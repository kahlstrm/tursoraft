//! Change subscriptions: an application in the same process as a node
//! receives the row changes of every change set this node applies, in log
//! order, after the entry's database transaction has committed.
//!
//! Apply hands each applied change set's bytes to one broadcast channel and
//! moves on: it never waits for a subscriber and does not decode anything.
//! Each subscriber reads the channel at its own pace. The first subscriber
//! to receive a change set decodes its rows, and the others share that copy.
//! A subscriber that falls more than [`crate::Options::subscriber_queue`]
//! events behind loses the oldest ones and next receives [`Event::Lagged`].

use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::broadcast::{self, error::RecvError, error::TryRecvError};
use tursoraft_changeset::turso_core::types::ImmutableRecordRef;
pub use tursoraft_changeset::ChangeKind;
use tursoraft_changeset::ChangeSet;
use tursoraft_statemachine::{Entry, Outcome};

use crate::Value;

/// What a subscriber receives; see [`crate::Node::subscribe`].
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// One change set this node applied. A read on this node started after
    /// this event sees its changes.
    Changes(Changes),
    /// This node replaced its whole database with a snapshot taken at
    /// `index`. The change sets up to `index` that it skipped send no
    /// events. Everything derived from the database may be stale: read it
    /// again, then go on with the events after this one.
    Reset { index: u64 },
    /// This subscriber did not keep up and lost the `missed` oldest events
    /// it had not received. Handle it as [`Event::Reset`].
    Lagged { missed: u64 },
}

/// The changes of one applied change set: one transaction.
#[derive(Debug, Clone, PartialEq)]
pub struct Changes {
    /// The log index of the entry that holds the change set.
    pub index: u64,
    /// The change set's position among the proposals of that entry. Rejected
    /// change sets send no event, so positions may skip.
    pub position: usize,
    /// The rows the transaction changed, in the order it changed them.
    /// Shared by every subscriber that received this event.
    pub rows: Arc<[RowChange]>,
}

/// One row the transaction inserted, updated or deleted.
///
/// Schema changes are changes of the table `sqlite_schema`, whose rows are
/// `type, name, tbl_name, rootpage, sql`: `CREATE TABLE` inserts a row, and
/// `ALTER TABLE` updates the `sql` of one. These images are as the node that
/// ran the transaction captured them, so `rootpage` may differ from this
/// node's file.
#[derive(Debug, Clone, PartialEq)]
pub struct RowChange {
    pub table: String,
    pub kind: ChangeKind,
    /// The rowid on the node that ran the transaction. For a table whose
    /// PRIMARY KEY is not an INTEGER PRIMARY KEY, other nodes may store the
    /// row under another rowid; identify it by its key columns.
    pub rowid: i64,
    /// The whole row before the change, in the table's column order. Present
    /// for updates and deletes.
    pub before: Option<Vec<Value>>,
    /// The whole row after the change, in the table's column order. Present
    /// for inserts and updates.
    pub after: Option<Vec<Value>>,
}

/// A stream of [`Event`]s from one node; see [`crate::Node::subscribe`].
/// Dropping it ends the subscription.
pub struct Subscription {
    rx: broadcast::Receiver<Notice>,
}

impl Subscription {
    /// The next event. `None` once the node has shut down and every event
    /// sent before has been received.
    pub async fn recv(&mut self) -> Option<Event> {
        match self.rx.recv().await {
            Ok(notice) => Some(notice.event()),
            Err(RecvError::Lagged(missed)) => Some(Event::Lagged { missed }),
            Err(RecvError::Closed) => None,
        }
    }

    /// The next event if one is waiting, without waiting.
    pub fn try_recv(&mut self) -> Option<Event> {
        match self.rx.try_recv() {
            Ok(notice) => Some(notice.event()),
            Err(TryRecvError::Lagged(missed)) => Some(Event::Lagged { missed }),
            Err(TryRecvError::Empty | TryRecvError::Closed) => None,
        }
    }
}

/// What apply puts into the channel.
#[derive(Clone)]
enum Notice {
    Applied(Arc<Applied>),
    Reset(u64),
}

struct Applied {
    index: u64,
    position: usize,
    /// The encoded change set, as proposed.
    change_set: Vec<u8>,
    rows: OnceLock<Arc<[RowChange]>>,
}

impl Notice {
    fn event(&self) -> Event {
        match self {
            Notice::Applied(a) => Event::Changes(Changes {
                index: a.index,
                position: a.position,
                rows: a.rows.get_or_init(|| decode(&a.change_set)).clone(),
            }),
            Notice::Reset(index) => Event::Reset { index: *index },
        }
    }
}

/// Apply decoded this change set and every record in it before it reported
/// the change set as applied, so decoding cannot fail here.
fn decode(bytes: &[u8]) -> Arc<[RowChange]> {
    let change_set = ChangeSet::decode(bytes).expect("an applied change set decodes");
    let values = |record: Option<Vec<u8>>| {
        record.map(|bytes| {
            ImmutableRecordRef::from_bin_record(&bytes)
                .get_values_owned()
                .expect("an applied row record decodes")
                .iter()
                .map(Value::from_turso)
                .collect()
        })
    };
    change_set
        .changes
        .into_iter()
        .map(|c| RowChange {
            table: c.table,
            kind: c.kind,
            rowid: c.id,
            before: values(c.before),
            after: values(c.after),
        })
        .collect()
}

/// The sending side, shared by the node and its state machine.
pub(crate) struct Feed {
    state: Mutex<FeedState>,
}

struct FeedState {
    /// The last entry or snapshot index whose events have been sent.
    index: u64,
    /// `None` once the node has shut down.
    tx: Option<broadcast::Sender<Notice>>,
}

impl Feed {
    pub(crate) fn new(applied_index: u64, queue: usize) -> Feed {
        Feed {
            state: Mutex::new(FeedState {
                index: applied_index,
                tx: Some(broadcast::channel(queue).0),
            }),
        }
    }

    /// A new subscription and the index whose events it starts after.
    pub(crate) fn subscribe(&self) -> (u64, Subscription) {
        let state = self.state.lock().unwrap();
        let rx = match &state.tx {
            Some(tx) => tx.subscribe(),
            // Shut down: a receiver whose sender is already gone.
            None => broadcast::channel(1).0.subscribe(),
        };
        (state.index, Subscription { rx })
    }

    /// Sends the change sets of `entry` that `outcomes` says were applied.
    /// Call after the entry has committed, in log order.
    pub(crate) fn applied(&self, entry: &Entry, outcomes: &[Outcome]) {
        let mut state = self.state.lock().unwrap();
        state.index = state.index.max(entry.index);
        let Some(tx) = state.tx.as_ref().filter(|tx| tx.receiver_count() > 0) else {
            return;
        };
        for (position, (proposal, outcome)) in entry.proposals.iter().zip(outcomes).enumerate() {
            if *outcome == Outcome::Applied {
                // No receiver is not an error: one may have just dropped.
                let _ = tx.send(Notice::Applied(Arc::new(Applied {
                    index: entry.index,
                    position,
                    change_set: proposal.clone(),
                    rows: OnceLock::new(),
                })));
            }
        }
    }

    /// Sends [`Event::Reset`]. Call after the snapshot is installed.
    pub(crate) fn reset(&self, index: u64) {
        let mut state = self.state.lock().unwrap();
        state.index = index;
        if let Some(tx) = &state.tx {
            let _ = tx.send(Notice::Reset(index));
        }
    }

    /// Ends every subscription once it has received the events sent so far.
    pub(crate) fn close(&self) {
        self.state.lock().unwrap().tx = None;
    }
}
