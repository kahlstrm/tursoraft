//! The bank-transfer workload and the checker of what a cluster kept.
//!
//! Accounts start with [`INITIAL_BALANCE`] each. A transfer moves an amount
//! from one account to another and records itself in `transfers` under a key
//! the client chose, in one transaction of two updates and one insert. So the
//! total of all balances never changes, and each balance equals its starting
//! value plus what the recorded transfers moved into it minus what they moved
//! out. A transfer that is only partly there breaks that equation for its two
//! accounts.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tursoraft_node::{Client, CommitMode, Error, ReadLevel, Statement, Value};
use tursoraft_raft::NodeId;

use crate::cluster::{rows, Cluster};

pub const INITIAL_BALANCE: i64 = 1000;

/// Creates the two tables and accounts `1..=accounts`, in one transaction.
pub fn schema(accounts: i64) -> Vec<Statement> {
    let values: Vec<String> = (1..=accounts)
        .map(|id| format!("({id}, {INITIAL_BALANCE})"))
        .collect();
    vec![
        Statement::from("CREATE TABLE accounts(id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)"),
        Statement::from(
            "CREATE TABLE transfers(key TEXT PRIMARY KEY, src INTEGER NOT NULL, \
             dst INTEGER NOT NULL, amount INTEGER NOT NULL)",
        ),
        Statement::new(
            format!(
                "INSERT INTO accounts(id, balance) VALUES {}",
                values.join(", ")
            ),
            Vec::new(),
        ),
    ]
}

/// One transfer: two updates and one insert.
pub fn transfer(key: &str, src: i64, dst: i64, amount: i64) -> Vec<Statement> {
    vec![
        Statement::new(
            "UPDATE accounts SET balance = balance - ? WHERE id = ?",
            vec![Value::Integer(amount), Value::Integer(src)],
        ),
        Statement::new(
            "UPDATE accounts SET balance = balance + ? WHERE id = ?",
            vec![Value::Integer(amount), Value::Integer(dst)],
        ),
        Statement::new(
            "INSERT INTO transfers(key, src, dst, amount) VALUES (?, ?, ?, ?)",
            vec![
                Value::Text(key.to_string()),
                Value::Integer(src),
                Value::Integer(dst),
                Value::Integer(amount),
            ],
        ),
    ]
}

/// One transfer a client sent, and what it was told.
#[derive(Debug, Clone)]
pub struct Attempt {
    pub key: String,
    /// The node the client sent it to.
    pub node: NodeId,
    pub started: Instant,
    pub finished: Instant,
    /// The log index of the acknowledged write.
    pub result: Result<u64, Error>,
}

/// What an answer says about a transfer's effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Told {
    Acknowledged,
    /// `Transaction`, `Rejected`, `Unavailable` or `Aborted`: it had no effect.
    NoEffect,
    /// `OutcomeUnknown`, or any other error: it may have taken effect.
    /// (`Aborted` counts as without effect, as `NoEffect` above.)
    Unknown,
}

impl Attempt {
    pub fn told(&self) -> Told {
        match &self.result {
            Ok(_) => Told::Acknowledged,
            Err(
                Error::Transaction(_)
                | Error::Rejected(_)
                | Error::Unavailable(_)
                | Error::Aborted(_),
            ) => Told::NoEffect,
            Err(_) => Told::Unknown,
        }
    }

    pub fn latency(&self) -> Duration {
        self.finished - self.started
    }
}

/// Totals one reader saw, with how often it saw each.
#[derive(Debug, Clone, Default)]
pub struct ReadTally {
    pub totals: BTreeMap<i64, u64>,
    pub errors: u64,
}

impl ReadTally {
    pub fn reads(&self) -> u64 {
        self.totals.values().sum()
    }
}

/// Who writes and who reads during a [`Load`].
#[derive(Debug, Clone)]
pub struct LoadSpec {
    /// Prefix of every transfer key, unique per load in one cluster.
    pub tag: String,
    /// Writers per node.
    pub writers: Vec<(NodeId, usize)>,
    pub mode: CommitMode,
    /// Run each transfer as an interactive transaction ([`Client::begin`]):
    /// read both balances, then write the new balances as values, then
    /// record the transfer. A transfer that committed on a stale read would
    /// overwrite another one's balance change, which the balance check
    /// notices.
    pub interactive: bool,
    /// Nodes with a reader that sums all balances in one `SELECT`.
    pub readers: Vec<NodeId>,
    /// A node with a reader that sums the balances in two `SELECT`s, half
    /// the accounts each: a reader that can see half a transfer, to show that
    /// the total check notices.
    pub split_reader: Option<NodeId>,
    pub accounts: i64,
    /// Client request timeout.
    pub timeout: Duration,
}

impl LoadSpec {
    pub fn new(tag: &str, writers: Vec<(NodeId, usize)>, mode: CommitMode, accounts: i64) -> Self {
        LoadSpec {
            tag: tag.to_string(),
            writers,
            mode,
            interactive: false,
            readers: Vec::new(),
            split_reader: None,
            accounts,
            timeout: Duration::from_secs(30),
        }
    }
}

/// Writers and readers running against a cluster until [`Load::stop`].
pub struct Load {
    stop: Arc<AtomicBool>,
    attempts: Arc<Mutex<Vec<Attempt>>>,
    reads: BTreeMap<NodeId, Arc<Mutex<ReadTally>>>,
    split: Option<Arc<Mutex<ReadTally>>>,
    tasks: Vec<JoinHandle<()>>,
}

/// What a [`Load`] did.
#[derive(Debug, Clone, Default)]
pub struct Finished {
    pub attempts: Vec<Attempt>,
    pub reads: BTreeMap<NodeId, ReadTally>,
    pub split: Option<ReadTally>,
}

impl Load {
    pub fn start(c: &Cluster, spec: &LoadSpec) -> Load {
        let stop = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for &(node, count) in &spec.writers {
            for w in 0..count {
                let client = c.client(node).with_timeout(spec.timeout);
                let prefix = format!("{}-n{node}-w{w}", spec.tag);
                let seed = (node * 1_000_003 + w as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                tasks.push(tokio::spawn(write_loop(
                    client,
                    node,
                    prefix,
                    (spec.mode, spec.interactive),
                    spec.accounts,
                    seed | 1,
                    stop.clone(),
                    attempts.clone(),
                )));
            }
        }
        let mut reads = BTreeMap::new();
        for &node in &spec.readers {
            let tally = Arc::new(Mutex::new(ReadTally::default()));
            let client = c.client(node).with_timeout(Duration::from_secs(5));
            tasks.push(tokio::spawn(read_loop(
                client,
                None,
                stop.clone(),
                tally.clone(),
            )));
            reads.insert(node, tally);
        }
        let split = spec.split_reader.map(|node| {
            let tally = Arc::new(Mutex::new(ReadTally::default()));
            let client = c.client(node).with_timeout(Duration::from_secs(5));
            tasks.push(tokio::spawn(read_loop(
                client,
                Some(spec.accounts / 2),
                stop.clone(),
                tally.clone(),
            )));
            tally
        });
        Load {
            stop,
            attempts,
            reads,
            split,
            tasks,
        }
    }

    /// The attempts so far.
    pub fn attempts(&self) -> Vec<Attempt> {
        self.attempts.lock().unwrap().clone()
    }

    /// Stops issuing transfers and reads, and waits for the ones in flight.
    pub async fn stop(self) -> Finished {
        self.stop.store(true, Relaxed);
        for t in self.tasks {
            t.await.unwrap();
        }
        let attempts = self.attempts.lock().unwrap().clone();
        Finished {
            attempts,
            reads: self
                .reads
                .into_iter()
                .map(|(n, t)| (n, t.lock().unwrap().clone()))
                .collect(),
            split: self.split.map(|t| t.lock().unwrap().clone()),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_loop(
    client: Client,
    node: NodeId,
    prefix: String,
    (mode, interactive): (CommitMode, bool),
    accounts: i64,
    seed: u64,
    stop: Arc<AtomicBool>,
    attempts: Arc<Mutex<Vec<Attempt>>>,
) {
    let mut rng = seed;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut seq = 0u64;
    while !stop.load(Relaxed) {
        let key = format!("{prefix}-{seq}");
        seq += 1;
        let src = 1 + (next() % accounts as u64) as i64;
        let dst = 1 + (src + (next() % (accounts as u64 - 1)) as i64) % accounts;
        let amount = 1 + (next() % 97) as i64;
        let started = Instant::now();
        let result = if interactive {
            interactive_transfer(&client, &key, src, dst, amount, mode).await
        } else {
            client
                .execute(transfer(&key, src, dst, amount), mode)
                .await
                .map(|w| w.index)
        };
        let failed = result.is_err();
        attempts.lock().unwrap().push(Attempt {
            key,
            node,
            started,
            finished: Instant::now(),
            result,
        });
        if failed {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// One transfer as an interactive transaction: reads both balances, writes
/// the new ones as values, records the transfer, commits. Rolls back if a
/// statement fails, so the node's write lock is not held until the hold
/// limit.
async fn interactive_transfer(
    client: &Client,
    key: &str,
    src: i64,
    dst: i64,
    amount: i64,
    mode: CommitMode,
) -> Result<u64, Error> {
    let mut tx = client.begin().await?;
    if let Err(e) = interactive_statements(&mut tx, key, src, dst, amount).await {
        let _ = tx.rollback().await;
        return Err(e);
    }
    tx.commit(mode).await.map(|w| w.index)
}

async fn interactive_statements(
    tx: &mut tursoraft_node::ClientTransaction<'_>,
    key: &str,
    src: i64,
    dst: i64,
    amount: i64,
) -> Result<(), Error> {
    let read = tx
        .execute(Statement::new(
            "SELECT id, balance FROM accounts WHERE id IN (?, ?)",
            vec![Value::Integer(src), Value::Integer(dst)],
        ))
        .await?;
    let balance = |id: i64| {
        read.rows
            .iter()
            .find(|r| r[0] == Value::Integer(id))
            .map(|r| match r[1] {
                Value::Integer(b) => b,
                ref v => panic!("not a balance: {v:?}"),
            })
            .expect("the account exists")
    };
    let (from, to) = (balance(src), balance(dst));
    for (id, value) in [(src, from - amount), (dst, to + amount)] {
        tx.execute(Statement::new(
            "UPDATE accounts SET balance = ? WHERE id = ?",
            vec![Value::Integer(value), Value::Integer(id)],
        ))
        .await?;
    }
    tx.execute(transfer(key, src, dst, amount).pop().unwrap())
        .await?;
    Ok(())
}

/// Sums all balances with [`ReadLevel::Local`] until stopped. With
/// `split = Some(k)`, sums accounts `<= k` and `> k` in two reads.
async fn read_loop(
    client: Client,
    split: Option<i64>,
    stop: Arc<AtomicBool>,
    tally: Arc<Mutex<ReadTally>>,
) {
    let sum = |sql: String| {
        let client = &client;
        async move {
            match client
                .query(Statement::new(sql, Vec::new()), ReadLevel::Local)
                .await
            {
                Ok(r) => match r.rows.first().and_then(|row| row.first()) {
                    Some(Value::Integer(n)) => Some(*n),
                    _ => None,
                },
                Err(_) => None,
            }
        }
    };
    while !stop.load(Relaxed) {
        let total = match split {
            None => sum("SELECT sum(balance) FROM accounts".into()).await,
            Some(k) => {
                let low = sum(format!("SELECT sum(balance) FROM accounts WHERE id <= {k}")).await;
                let high = sum(format!("SELECT sum(balance) FROM accounts WHERE id > {k}")).await;
                low.zip(high).map(|(a, b)| a + b)
            }
        };
        {
            let mut t = tally.lock().unwrap();
            match total {
                Some(total) => *t.totals.entry(total).or_default() += 1,
                None => t.errors += 1,
            }
        }
        if total.is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// The balances and the recorded transfers of one node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BankState {
    pub balances: BTreeMap<i64, i64>,
    /// Key to (src, dst, amount).
    pub transfers: BTreeMap<String, (i64, i64, i64)>,
}

/// Reads one node's balances and transfers with [`ReadLevel::Local`], in one
/// statement, so that both come from the same read transaction while the
/// node applies entries.
pub async fn read_state(client: &Client) -> BankState {
    let int = |v: &Value| match v {
        Value::Integer(n) => *n,
        v => panic!("not an integer: {v:?}"),
    };
    let all = rows(
        client,
        "SELECT 'a', id, balance, NULL, NULL FROM accounts \
         UNION ALL SELECT 't', key, src, dst, amount FROM transfers",
        ReadLevel::Local,
    )
    .await;
    let mut state = BankState::default();
    for r in &all {
        match (&r[0], &r[1]) {
            (Value::Text(t), id) if t == "a" => {
                state.balances.insert(int(id), int(&r[2]));
            }
            (Value::Text(t), Value::Text(key)) if t == "t" => {
                state
                    .transfers
                    .insert(key.clone(), (int(&r[2]), int(&r[3]), int(&r[4])));
            }
            _ => panic!("unexpected row {r:?}"),
        }
    }
    state
}

/// What one node's state says about the attempts.
#[derive(Debug, Clone, Default)]
pub struct Verdict {
    pub acknowledged: usize,
    pub no_effect: usize,
    pub unknown: usize,
    /// Unknown outcomes whose transfer is there.
    pub unknown_present: usize,
    /// Acknowledged transfers that are not there.
    pub missing_acknowledged: Vec<String>,
    /// Transfers that were reported to have had no effect, yet are there.
    pub present_without_effect: Vec<String>,
    /// Transfers no attempt sent, yet there.
    pub present_never_sent: Vec<String>,
    pub total: i64,
    pub expected_total: i64,
    /// Accounts whose balance is not what the recorded transfers add up to:
    /// (id, expected, actual).
    pub wrong_balances: Vec<(i64, i64, Option<i64>)>,
}

impl Verdict {
    /// Everything wrong except missing acknowledged transfers, which async
    /// mode may lose.
    pub fn violations(&self) -> Vec<String> {
        let mut out = Vec::new();
        if !self.present_without_effect.is_empty() {
            out.push(format!(
                "transfers reported as without effect are there: {:?}",
                self.present_without_effect
            ));
        }
        if !self.present_never_sent.is_empty() {
            out.push(format!(
                "transfers nobody sent are there: {:?}",
                self.present_never_sent
            ));
        }
        if self.total != self.expected_total {
            out.push(format!(
                "the total balance is {}, not {}",
                self.total, self.expected_total
            ));
        }
        if !self.wrong_balances.is_empty() {
            out.push(format!(
                "balances that the transfers do not add up to (id, expected, actual): {:?}",
                self.wrong_balances
            ));
        }
        out
    }

    /// [`Verdict::violations`] that can be checked while transfers are still
    /// being sent: the ones that do not need every sent key to be known.
    pub fn violations_during_load(&self) -> Vec<String> {
        let mut v = self.clone();
        v.present_never_sent.clear();
        v.violations()
    }

    pub fn summary(&self) -> String {
        format!(
            "{} acknowledged ({} missing), {} unknown ({} there), {} without effect; total {}",
            self.acknowledged,
            self.missing_acknowledged.len(),
            self.unknown,
            self.unknown_present,
            self.no_effect,
            self.total
        )
    }
}

/// Checks a node's state against what the clients were told.
pub fn check(state: &BankState, attempts: &[Attempt], accounts: i64) -> Verdict {
    let mut v = Verdict {
        expected_total: accounts * INITIAL_BALANCE,
        ..Verdict::default()
    };
    let mut sent = BTreeSet::new();
    for a in attempts {
        sent.insert(a.key.as_str());
        let present = state.transfers.contains_key(&a.key);
        match a.told() {
            Told::Acknowledged => {
                v.acknowledged += 1;
                if !present {
                    v.missing_acknowledged.push(a.key.clone());
                }
            }
            Told::NoEffect => {
                v.no_effect += 1;
                if present {
                    v.present_without_effect.push(a.key.clone());
                }
            }
            Told::Unknown => {
                v.unknown += 1;
                v.unknown_present += present as usize;
            }
        }
    }
    v.present_never_sent = state
        .transfers
        .keys()
        .filter(|k| !sent.contains(k.as_str()))
        .cloned()
        .collect();
    let mut expected: BTreeMap<i64, i64> = (1..=accounts).map(|id| (id, INITIAL_BALANCE)).collect();
    for id in state.balances.keys() {
        expected.entry(*id).or_insert(0);
    }
    for (src, dst, amount) in state.transfers.values() {
        *expected.entry(*src).or_default() -= amount;
        *expected.entry(*dst).or_default() += amount;
    }
    for (id, exp) in expected {
        let actual = state.balances.get(&id).copied();
        if actual != Some(exp) {
            v.wrong_balances.push((id, exp, actual));
        }
    }
    v.total = state.balances.values().sum();
    v
}

#[cfg(test)]
mod tests {
    //! The checker fed states that break each property once.

    use super::*;

    fn attempt(key: &str, result: Result<u64, Error>) -> Attempt {
        let now = Instant::now();
        Attempt {
            key: key.into(),
            node: 1,
            started: now,
            finished: now,
            result,
        }
    }

    /// Three accounts after transfers a (1->2, 10) and b (2->3, 5), both
    /// acknowledged, c unknown and absent, d without effect and absent.
    fn good() -> (BankState, Vec<Attempt>) {
        let state = BankState {
            balances: [(1, 990), (2, 1005), (3, 1005)].into(),
            transfers: [("a".to_string(), (1, 2, 10)), ("b".to_string(), (2, 3, 5))].into(),
        };
        let attempts = vec![
            attempt("a", Ok(5)),
            attempt("b", Ok(6)),
            attempt("c", Err(Error::OutcomeUnknown("lost".into()))),
            attempt("d", Err(Error::Unavailable("no leader".into()))),
        ];
        (state, attempts)
    }

    #[test]
    fn a_consistent_state_passes() {
        let (state, attempts) = good();
        let v = check(&state, &attempts, 3);
        assert!(v.violations().is_empty(), "{:?}", v.violations());
        assert!(v.missing_acknowledged.is_empty());
        assert_eq!((v.acknowledged, v.unknown, v.no_effect), (2, 1, 1));
    }

    #[test]
    fn a_lost_acknowledged_transfer_is_reported() {
        let (mut state, attempts) = good();
        // Transfer b lost wholly: its row and both of its updates.
        state.transfers.remove("b");
        state.balances.insert(2, 1010);
        state.balances.insert(3, 1000);
        let v = check(&state, &attempts, 3);
        assert_eq!(v.missing_acknowledged, vec!["b".to_string()]);
        assert!(v.violations().is_empty());
    }

    #[test]
    fn half_a_transfer_is_reported() {
        let (mut state, attempts) = good();
        // An unknown transfer whose row is there but whose updates are not.
        state.transfers.insert("c".into(), (3, 1, 7));
        let v = check(&state, &attempts, 3);
        assert_eq!(v.unknown_present, 1);
        assert_eq!(
            v.wrong_balances,
            vec![(1, 997, Some(990)), (3, 998, Some(1005))]
        );
        // Only one of b's updates: the total changes too.
        let (mut state, attempts) = good();
        state.balances.insert(3, 1000);
        let v = check(&state, &attempts, 3);
        assert_eq!(v.violations().len(), 2, "{:?}", v.violations());
    }

    #[test]
    fn a_transfer_that_committed_on_a_stale_read_is_reported() {
        // b (2->3, 5) read account 2 before a (1->2, 10) and wrote its
        // balance as a value: a's +10 on account 2 is overwritten.
        let (mut state, attempts) = good();
        state.balances.insert(2, 995);
        let v = check(&state, &attempts, 3);
        assert_eq!(v.wrong_balances, vec![(2, 1005, Some(995))]);
        assert_eq!(v.violations().len(), 2, "{:?}", v.violations());
    }

    #[test]
    fn a_transfer_without_effect_that_is_there_is_reported() {
        let (mut state, attempts) = good();
        state.transfers.insert("d".into(), (1, 3, 1));
        state.balances.insert(1, 989);
        state.balances.insert(3, 1006);
        let v = check(&state, &attempts, 3);
        assert_eq!(v.present_without_effect, vec!["d".to_string()]);
        state.transfers.insert("zz".into(), (1, 3, 0));
        let v = check(&state, &attempts, 3);
        assert_eq!(v.present_never_sent, vec!["zz".to_string()]);
    }
}
