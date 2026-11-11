//! A relay: runs in the process of a tursoraft node, subscribes to the
//! changes the node applies ([`Node::subscribe`]), and sends them to one
//! read-only follower Durable Object over a WebSocket.
//!
//! The relay opens the WebSocket to a Worker URL, and the Worker hands it to
//! the follower, which accepts it with the Hibernation API: the follower is
//! the WebSocket server, so it can leave memory between messages.
//!
//! Each connection starts with a full copy of the database at a known log
//! index, then sends the change sets applied after that index, in log
//! order. It copies again, on the same connection, after [`Event::Reset`],
//! [`Event::Lagged`], or a schema change other than CREATE TABLE, CREATE
//! INDEX, CREATE VIEW and triggers; and on a new connection after any
//! failure. Nothing is resumed: there is no stored history of events.
//!
//! The follower runs no triggers: change sets already hold the rows the
//! source's triggers wrote, so the relay sends no CREATE TRIGGER.
//!
//! See `edge/README.md` for the follower and the Worker.

pub mod protocol;

mod copy;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tursoraft_node::{ChangeKind, Changes, Event, Node, RowChange, Subscription, Value};

use protocol::{CopyRow, FollowerMessage, Kind, RelayMessage, Row, Values};

/// Where and how a relay sends. Its `Debug` leaves the secret out, so that
/// logging a config does not reveal it.
#[derive(Clone)]
pub struct Config {
    /// The Worker's relay endpoint, such as
    /// `wss://edge.example.workers.dev/relay?follower=weur`.
    pub url: String,
    /// Sent as `Authorization: Bearer <secret>`; the Worker's `RELAY_SECRET`.
    pub secret: String,
    /// Most messages sent and not yet acknowledged.
    pub window: usize,
    /// Most change sets in one message.
    pub max_events: usize,
    /// About the most bytes of row values in one message. One change set is
    /// never split, so a message holding a large one is larger.
    pub max_bytes: usize,
    /// How often an idle relay checks for entries that sent no event (blank,
    /// membership, or only rejected change sets), so that the follower's
    /// position reaches them.
    pub tick: Duration,
    /// How long an unacknowledged message may wait before the connection is
    /// given up.
    pub ack_timeout: Duration,
    /// The wait before connecting again after a failure. It doubles with
    /// each failure in a row, up to `max_retry`.
    pub retry: Duration,
    pub max_retry: Duration,
}

impl Config {
    pub fn new(url: impl Into<String>, secret: impl Into<String>) -> Config {
        Config {
            url: url.into(),
            secret: secret.into(),
            window: 16,
            max_events: 256,
            max_bytes: 512 * 1024,
            tick: Duration::from_millis(200),
            ack_timeout: Duration::from_secs(30),
            retry: Duration::from_millis(500),
            max_retry: Duration::from_secs(30),
        }
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("url", &self.url)
            .field("secret", &"<hidden>")
            .field("window", &self.window)
            .field("max_events", &self.max_events)
            .field("max_bytes", &self.max_bytes)
            .field("tick", &self.tick)
            .field("ack_timeout", &self.ack_timeout)
            .field("retry", &self.retry)
            .field("max_retry", &self.max_retry)
            .finish()
    }
}

/// What a relay has done since it started.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    /// WebSocket connections opened, and those that ended in a failure.
    pub connects: u64,
    pub failures: u64,
    /// Full copies sent completely, and their rows.
    pub copies: u64,
    pub copied_rows: u64,
    /// Copies started again because of each reason.
    pub resets: u64,
    pub lags: u64,
    pub schema_changes: u64,
    /// Change sets sent, and messages acknowledged.
    pub events: u64,
    pub acks: u64,
}

/// A running relay. Dropping it stops it.
pub struct Relay {
    task: JoinHandle<()>,
    shared: Arc<Shared>,
}

#[derive(Default)]
struct Shared {
    stats: Mutex<Stats>,
    /// The `through` of the follower's last acknowledgement.
    through: watch::Sender<u64>,
    streams: AtomicU64,
}

impl Relay {
    /// Starts relaying `node`'s changes to the follower at `config.url`, on
    /// the current tokio runtime.
    pub fn start(node: Node, config: Config) -> Relay {
        let shared = Arc::new(Shared::default());
        let task = tokio::spawn(run(node, config, shared.clone()));
        Relay { task, shared }
    }

    pub fn stats(&self) -> Stats {
        self.shared.stats.lock().unwrap().clone()
    }

    /// The log index the follower has acknowledged applying every entry up
    /// to, as it changes.
    pub fn follower_through(&self) -> watch::Receiver<u64> {
        self.shared.through.subscribe()
    }

    /// Waits until the follower has acknowledged applying `index`.
    pub async fn wait_follower(&self, index: u64, timeout: Duration) -> Result<(), u64> {
        let mut rx = self.follower_through();
        let reached = tokio::time::timeout(timeout, rx.wait_for(|t| *t >= index))
            .await
            .is_ok_and(|r| r.is_ok());
        if reached {
            Ok(())
        } else {
            Err(*rx.borrow())
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Why a session ended.
enum Stop {
    /// The node shut down; nothing more will come.
    NodeGone,
    Failed(String),
}

impl From<String> for Stop {
    fn from(e: String) -> Stop {
        Stop::Failed(e)
    }
}

/// Why the relay copies again on the same connection.
enum Restart {
    Reset,
    Lagged,
    Schema(String),
}

async fn run(node: Node, config: Config, shared: Arc<Shared>) {
    let mut wait = config.retry;
    loop {
        let copies = shared.stats.lock().unwrap().copies;
        match session(&node, &config, &shared).await {
            Stop::NodeGone => {
                tracing::info!("relay: node {} shut down; stopping", node.id());
                return;
            }
            Stop::Failed(e) => {
                // A session that got as far as a copy was not a failure in a row.
                if shared.stats.lock().unwrap().copies > copies {
                    wait = config.retry;
                }
                tracing::warn!("relay: {e}; connecting again in {wait:?}");
                shared.stats.lock().unwrap().failures += 1;
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(config.max_retry.max(config.retry));
            }
        }
    }
}

/// One connection: a full copy, then changes, and again after a restart.
async fn session(node: &Node, config: &Config, shared: &Shared) -> Stop {
    let mut conn = match Conn::open(config, shared).await {
        Ok(conn) => conn,
        Err(e) => return Stop::Failed(e),
    };
    loop {
        let (_, events) = node.subscribe();
        let index = match send_copy(node, &mut conn, shared).await {
            Ok(index) => index,
            Err(e) => return e,
        };
        let restart = match follow(node, &mut conn, events, index, config, shared).await {
            Ok(restart) => restart,
            Err(e) => return e,
        };
        {
            let mut stats = shared.stats.lock().unwrap();
            match restart {
                Restart::Reset => stats.resets += 1,
                Restart::Lagged => stats.lags += 1,
                Restart::Schema(what) => {
                    tracing::info!("relay: schema change ({what}); copying again");
                    stats.schema_changes += 1;
                }
            }
        }
        if let Err(e) = conn.flush(shared).await {
            return Stop::Failed(e);
        }
    }
}

/// Starts a new stream with a full copy read now, and returns its index.
async fn send_copy(node: &Node, conn: &mut Conn, shared: &Shared) -> Result<u64, Stop> {
    let copy = copy::read(node).await?;
    conn.new_stream(node.id(), shared);
    let begin = RelayMessage::CopyBegin {
        stream: &conn.stream,
        seq: conn.next_seq,
        index: copy.index,
        schema: copy.schema.clone(),
        tables: copy.tables.clone(),
    };
    let begin = serde_json::to_string(&begin).unwrap();
    conn.send(begin, shared).await?;
    let mut rows = copy.rows.rows.iter().filter_map(|row| match row[0] {
        Value::Integer(table) if table >= 0 => Some((table as usize, row)),
        _ => None,
    });
    let mut copied = 0;
    let mut next = rows.next();
    while let Some((table, _)) = next {
        let width = copy.tables[table].columns + 1;
        let mut chunk = Vec::new();
        let mut bytes = 0;
        while let Some((t, row)) = next {
            if t != table || (bytes >= conn.max_bytes && !chunk.is_empty()) {
                break;
            }
            bytes += size(&row[1..1 + width]);
            chunk.push(CopyRow(&row[1..1 + width]));
            next = rows.next();
        }
        copied += chunk.len() as u64;
        let message = RelayMessage::CopyRows {
            stream: &conn.stream,
            seq: conn.next_seq,
            table,
            rows: chunk,
        };
        let message = serde_json::to_string(&message).unwrap();
        conn.send(message, shared).await?;
    }
    let end = RelayMessage::CopyEnd {
        stream: &conn.stream,
        seq: conn.next_seq,
    };
    let end = serde_json::to_string(&end).unwrap();
    conn.send(end, shared).await?;
    let mut stats = shared.stats.lock().unwrap();
    stats.copies += 1;
    stats.copied_rows += copied;
    tracing::info!(
        "relay: copied {copied} rows at log index {} to the follower",
        copy.index
    );
    Ok(copy.index)
}

/// Sends the change sets after `index` until the copy must start again.
async fn follow(
    node: &Node,
    conn: &mut Conn,
    mut events: Subscription,
    index: u64,
    config: &Config,
    shared: &Shared,
) -> Result<Restart, Stop> {
    // Every entry up to `sent_through` has been sent completely, and every
    // change set up to `last` has been sent.
    let mut sent_through = index;
    let mut last = (index, usize::MAX);
    let mut tick = tokio::time::interval(config.tick);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let first = tokio::select! {
            biased;
            // Read even when nothing is in flight, to notice a close.
            reply = conn.reply(shared) => {
                reply?;
                continue;
            }
            event = events.recv() => Some(event.ok_or(Stop::NodeGone)?),
            _ = tick.tick() => {
                conn.check_timeout(config)?;
                None
            }
        };
        // Every event of the entries up to `known` has been put into the
        // subscription by now, so once it is empty they have all arrived.
        let known = sent_index(node);
        let mut batch: Vec<Changes> = Vec::new();
        let mut bytes = 0;
        let mut drained = true;
        let mut next = first;
        loop {
            let event = match next.take() {
                Some(event) => event,
                None if batch.len() >= config.max_events || bytes >= config.max_bytes => {
                    drained = false;
                    break;
                }
                None => match events.try_recv() {
                    Some(event) => event,
                    None => break,
                },
            };
            match event {
                Event::Changes(changes) => {
                    if (changes.index, changes.position) > last {
                        bytes += changes
                            .rows
                            .iter()
                            .map(|r| {
                                r.before.as_deref().map_or(0, size)
                                    + r.after.as_deref().map_or(0, size)
                            })
                            .sum::<usize>();
                        last = (changes.index, changes.position);
                        batch.push(changes);
                    }
                }
                Event::Reset { .. } => return Ok(Restart::Reset),
                Event::Lagged { .. } => return Ok(Restart::Lagged),
            }
        }
        let through = if drained {
            known.max(sent_through)
        } else {
            // Events come in log order, so the entries before the last
            // event's entry are complete.
            (last.0 - 1).max(sent_through)
        };
        if batch.is_empty() && through == sent_through {
            continue;
        }
        let mut wire = Vec::with_capacity(batch.len());
        for changes in &batch {
            let mut rows = Vec::with_capacity(changes.rows.len());
            for row in changes.rows.iter() {
                match wire_row(row) {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => {}
                    Err(what) => return Ok(Restart::Schema(what)),
                }
            }
            wire.push(protocol::Event {
                index: changes.index,
                position: changes.position,
                rows,
            });
        }
        let message = RelayMessage::Changes {
            stream: &conn.stream,
            seq: conn.next_seq,
            events: wire,
            through,
        };
        let message = serde_json::to_string(&message).unwrap();
        conn.send(message, shared).await?;
        shared.stats.lock().unwrap().events += batch.len() as u64;
        sent_through = through;
    }
}

/// The last entry or snapshot index whose events this node has put into
/// every subscription. A new subscription starts right after it.
fn sent_index(node: &Node) -> u64 {
    node.subscribe().0
}

/// The wire form of one row change. `None` for a change the follower does
/// not need; `Err` for a schema change it cannot replay.
fn wire_row(row: &RowChange) -> Result<Option<Row<'_>>, String> {
    if row.table.eq_ignore_ascii_case("sqlite_schema") {
        return schema_row(row);
    }
    if copy::is_internal(&row.table) {
        return Ok(None);
    }
    Ok(Some(Row::Change {
        table: &row.table,
        kind: match row.kind {
            ChangeKind::Insert => Kind::Insert,
            ChangeKind::Update => Kind::Update,
            ChangeKind::Delete => Kind::Delete,
        },
        rowid: row.rowid.to_string(),
        before: row.before.as_deref().map(Values),
        after: row.after.as_deref().map(Values),
    }))
}

/// A change of `sqlite_schema`, whose rows are `type, name, tbl_name,
/// rootpage, sql`. Creating a table, index or view is replayed as its SQL.
/// Triggers are left out, as are automatic indexes (no SQL) and internal
/// tables. Anything else, such as ALTER TABLE or DROP, makes a new copy.
fn schema_row(row: &RowChange) -> Result<Option<Row<'_>>, String> {
    let image = row.after.as_ref().or(row.before.as_ref());
    let text = |i: usize| match image.and_then(|v| v.get(i)) {
        Some(Value::Text(t)) => Some(t.as_str()),
        _ => None,
    };
    let (kind, name) = (text(0).unwrap_or(""), text(1).unwrap_or(""));
    if kind == "trigger" || copy::is_internal(name) {
        return Ok(None);
    }
    match (row.kind, kind, text(4)) {
        (ChangeKind::Insert, "table" | "index" | "view", Some(sql)) => Ok(Some(Row::Sql { sql })),
        (ChangeKind::Insert, "index", None) => Ok(None),
        (kind, object, _) => Err(format!("{kind:?} of {object} {name}")),
    }
}

/// About how many bytes `values` take in a message.
fn size(values: &[Value]) -> usize {
    values
        .iter()
        .map(|v| match v {
            Value::Text(t) => t.len() + 2,
            Value::Blob(b) => b.len() * 4 / 3 + 8,
            _ => 24,
        })
        .sum()
}

/// One WebSocket to the follower, and the messages sent on it that are not
/// acknowledged yet.
struct Conn {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    stream: String,
    next_seq: u64,
    /// (seq, when sent) of each unacknowledged message, oldest first.
    in_flight: VecDeque<(u64, Instant)>,
    window: usize,
    max_bytes: usize,
}

impl Conn {
    async fn open(config: &Config, shared: &Shared) -> Result<Conn, String> {
        let mut request = config
            .url
            .as_str()
            .into_client_request()
            .map_err(|e| format!("relay URL {}: {e}", config.url))?;
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {}", config.secret)
                .parse()
                .map_err(|e| format!("the secret does not fit in a header: {e}"))?,
        );
        let (ws, _) = tokio::time::timeout(
            config.ack_timeout,
            tokio_tungstenite::connect_async(request),
        )
        .await
        .map_err(|_| format!("connecting to {} timed out", config.url))?
        .map_err(|e| format!("connecting to {}: {e}", config.url))?;
        shared.stats.lock().unwrap().connects += 1;
        let mut conn = Conn {
            ws,
            stream: String::new(),
            next_seq: 1,
            in_flight: VecDeque::new(),
            window: config.window.max(1),
            max_bytes: config.max_bytes,
        };
        match tokio::time::timeout(config.ack_timeout, conn.next_message()).await {
            Ok(Ok(FollowerMessage::Hello { through, instance })) => {
                tracing::info!(
                    "relay: connected to follower {instance}, which has applied through {through}"
                );
                Ok(conn)
            }
            Ok(Ok(other)) => Err(format!("expected hello, got {other:?}")),
            Ok(Err(e)) => Err(e),
            Err(_) => Err("the follower sent no hello".into()),
        }
    }

    /// Starts a new stream: a new id, numbered from 1 again.
    fn new_stream(&mut self, node_id: u64, shared: &Shared) {
        let n = shared.streams.fetch_add(1, Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        self.stream = format!("{node_id}-{nanos:x}-{n}");
        self.next_seq = 1;
    }

    /// Sends one message, after waiting for acknowledgements while the
    /// window is full.
    async fn send(&mut self, message: String, shared: &Shared) -> Result<(), String> {
        while self.in_flight.len() >= self.window {
            self.reply(shared).await?;
        }
        self.ws
            .send(Message::text(message))
            .await
            .map_err(|e| format!("sending: {e}"))?;
        self.in_flight.push_back((self.next_seq, Instant::now()));
        self.next_seq += 1;
        Ok(())
    }

    /// Waits until every message sent has been acknowledged.
    async fn flush(&mut self, shared: &Shared) -> Result<(), String> {
        while !self.in_flight.is_empty() {
            self.reply(shared).await?;
        }
        Ok(())
    }

    /// Receives one acknowledgement. An error reply ends the connection.
    async fn reply(&mut self, shared: &Shared) -> Result<(), String> {
        match self.next_message().await? {
            FollowerMessage::Ack { seq, through } => {
                let Some((expected, _)) = self.in_flight.pop_front() else {
                    return Err(format!("acknowledgement of {seq}, but nothing was sent"));
                };
                if seq != expected {
                    return Err(format!("acknowledgement of {seq}, expected {expected}"));
                }
                shared.through.send_if_modified(|t| {
                    let changed = *t != through;
                    *t = through;
                    changed
                });
                shared.stats.lock().unwrap().acks += 1;
                Ok(())
            }
            FollowerMessage::Error { seq, message } => {
                Err(format!("the follower refused message {seq}: {message}"))
            }
            other => Err(format!("unexpected {other:?}")),
        }
    }

    async fn next_message(&mut self) -> Result<FollowerMessage, String> {
        loop {
            let frame = match self.ws.next().await {
                None => return Err("the follower closed the connection".into()),
                Some(Err(e)) => return Err(format!("receiving: {e}")),
                Some(Ok(frame)) => frame,
            };
            match frame {
                Message::Text(text) => {
                    return serde_json::from_str(&text)
                        .map_err(|e| format!("bad message {text:?}: {e}"))
                }
                Message::Close(frame) => {
                    return Err(format!("the follower closed the connection: {frame:?}"))
                }
                _ => continue,
            }
        }
    }

    fn check_timeout(&self, config: &Config) -> Result<(), String> {
        match self.in_flight.front() {
            Some((seq, sent)) if sent.elapsed() > config.ack_timeout => Err(format!(
                "message {seq} unacknowledged after {:?}",
                config.ack_timeout
            )),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn a_config_does_not_show_its_secret() {
        let config = Config::new("wss://edge.example/relay", "s3cret-value");
        let shown = format!("{config:?} {config:#?}");
        assert!(!shown.contains("s3cret-value"), "{shown}");
        assert!(shown.contains("wss://edge.example/relay"), "{shown}");
    }
}
