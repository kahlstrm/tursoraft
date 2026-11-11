//! Moving a running cluster's voters and leader from one region to another.
//!
//! The layout has three nodes in Europe and three in Asia, with round trips
//! from cloudping.co (AWS, P50, the two directions averaged):
//!
//! | node | place | AWS region |
//! |---:|---|---|
//! | 1 | Frankfurt | eu-central-1 |
//! | 2 | Paris | eu-west-3 |
//! | 3 | London | eu-west-2 |
//! | 4 | Singapore | ap-southeast-1 |
//! | 5 | Hong Kong | ap-east-1 |
//! | 6 | Tokyo | ap-northeast-1 |
//!
//! A move ([`move_voters`]) uses the three membership calls of the Raft
//! client: the new voters are already non-voting followers
//! (`add_follower`); it waits until they have applied the leader's last
//! entry, makes them the voters with one `change_voters` (openraft goes
//! through a joint configuration of old and new voters, and keeps the old
//! voters as non-voting followers), then hands leadership to the chosen new
//! voter with `transfer_leader`.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use tursoraft_node::Error;
use tursoraft_raft::{DelayTable, NodeId, PairRtt};

use crate::bank::Attempt;
use crate::cluster::{ms, Cluster};

pub const EUROPE: [NodeId; 3] = [1, 2, 3];
pub const ASIA: [NodeId; 3] = [4, 5, 6];
pub const ALL: [NodeId; 6] = [1, 2, 3, 4, 5, 6];
pub const PLACES: [&str; 6] = [
    "Frankfurt",
    "Paris",
    "London",
    "Singapore",
    "Hong Kong",
    "Tokyo",
];

/// Round trips in ms, from cloudping.co's P50 grid (fetched 2026-11-11),
/// each the mean of the two directions.
pub const RTTS: [(NodeId, NodeId, f64); 15] = [
    (1, 2, 10.1),
    (1, 3, 14.4),
    (2, 3, 9.9),
    (4, 5, 35.1),
    (4, 6, 70.1),
    (5, 6, 52.5),
    (1, 4, 164.8),
    (1, 5, 193.3),
    (1, 6, 229.4),
    (2, 4, 158.8),
    (2, 5, 196.5),
    (2, 6, 219.8),
    (3, 4, 164.0),
    (3, 5, 199.4),
    (3, 6, 213.9),
];

pub fn place(id: NodeId) -> &'static str {
    PLACES[id as usize - 1]
}

/// The delay table of [`RTTS`], scaled by `scale` (1.0 for the real values).
pub fn delays(scale: f64) -> DelayTable {
    DelayTable {
        default_rtt_ms: 0.0,
        pairs: RTTS
            .iter()
            .map(|&(a, b, rtt_ms)| PairRtt {
                a,
                b,
                rtt_ms: rtt_ms * scale,
            })
            .collect(),
    }
}

/// How long each step of a move took.
#[derive(Debug, Clone, Default)]
pub struct Moved {
    /// Until every new voter had applied the leader's last entry. Zero when
    /// the move did not wait for it.
    pub catch_up: Duration,
    /// `change_voters`, until it returned success.
    pub change: Duration,
    /// `change_voters` calls made; more than one after a failed call.
    pub change_calls: usize,
    /// `transfer_leader`, until every running new voter names the new leader.
    pub transfer: Duration,
    pub total: Duration,
    /// Errors of membership calls that were tried again.
    pub retried: Vec<String>,
}

impl Moved {
    pub fn summary(&self) -> String {
        format!(
            "catch-up {:.0} ms, change_voters {:.0} ms ({} calls), transfer_leader {:.0} ms, total {:.0} ms{}",
            ms(self.catch_up),
            ms(self.change),
            self.change_calls,
            ms(self.transfer),
            ms(self.total),
            if self.retried.is_empty() {
                String::new()
            } else {
                format!("; retried after {:?}", self.retried)
            }
        )
    }
}

/// Makes exactly `voters` the voters and `leader` the leader, through the
/// node `leader` (which forwards each call to whoever leads). With
/// `catch_up_first`, first waits until every new voter has applied the
/// leader's last entry. Calls that fail, such as after the leader died, are
/// tried again until `change_voters` succeeds and the running `voters` name
/// `leader`; so calling it again finishes a move that was cut short.
pub async fn move_voters(
    c: &Cluster,
    voters: &[NodeId],
    leader: NodeId,
    catch_up_first: bool,
) -> Moved {
    assert!(voters.contains(&leader));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(120);
    let target: BTreeSet<NodeId> = voters.iter().copied().collect();
    let client = c.client(leader).with_timeout(Duration::from_secs(30));
    let mut m = Moved::default();
    if catch_up_first {
        // A node that was just added may not know the leader yet.
        let current = loop {
            if let Some(l) = client.raft().status().await.unwrap().leader {
                break l;
            }
            assert!(Instant::now() < deadline, "node {leader} knows no leader");
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let last = c.status(current).await.last_log_index.unwrap();
        for id in voters {
            c.client(*id)
                .raft()
                .wait_applied(last, Duration::from_secs(120))
                .await
                .unwrap_or_else(|e| panic!("node {id} did not catch up to {last}: {e}"));
        }
        m.catch_up = start.elapsed();
    }
    let t = Instant::now();
    loop {
        m.change_calls += 1;
        match client.raft().change_voters(target.clone()).await {
            Ok(_) => break,
            Err(e) => m.retried.push(format!("change_voters: {e}")),
        }
        assert!(Instant::now() < deadline, "the move did not finish: {m:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    m.change = t.elapsed();
    let t = Instant::now();
    let running = c.running();
    let up: Vec<NodeId> = voters
        .iter()
        .copied()
        .filter(|id| running.contains(id))
        .collect();
    loop {
        match client.raft().transfer_leader(leader).await {
            Ok(()) if c.wait_leader(&up).await == leader => break,
            Ok(()) => m
                .retried
                .push("transfer_leader: leader changed again".into()),
            Err(e) => m.retried.push(format!("transfer_leader: {e}")),
        }
        assert!(Instant::now() < deadline, "the move did not finish: {m:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    m.transfer = t.elapsed();
    m.total = start.elapsed();
    m
}

/// What writers were told in a time window.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Seen {
    /// Attempts acknowledged in the window.
    pub acked: usize,
    pub rejected: usize,
    pub unavailable: usize,
    pub unknown: usize,
    /// `Aborted` and `Transaction`.
    pub other: usize,
    /// The longest time in the window in which no attempt was acknowledged,
    /// counting the window's start and end as acknowledgements.
    pub longest_gap: Duration,
}

impl Seen {
    /// Of the attempts that `include` selects and that finished in
    /// `[from, to]`.
    pub fn of(
        attempts: &[Attempt],
        include: impl Fn(&Attempt) -> bool,
        from: Instant,
        to: Instant,
    ) -> Seen {
        let mut s = Seen::default();
        let mut acks = vec![from, to];
        for a in attempts
            .iter()
            .filter(|a| include(a) && a.finished >= from && a.finished <= to)
        {
            match &a.result {
                Ok(_) => {
                    s.acked += 1;
                    acks.push(a.finished);
                }
                Err(Error::Rejected(_)) => s.rejected += 1,
                Err(Error::Unavailable(_)) => s.unavailable += 1,
                Err(Error::Aborted(_) | Error::Transaction(_)) => s.other += 1,
                Err(_) => s.unknown += 1,
            }
        }
        acks.sort();
        s.longest_gap = acks
            .windows(2)
            .map(|w| w[1] - w[0])
            .max()
            .unwrap_or_default();
        s
    }

    pub fn summary(&self) -> String {
        format!(
            "{} acknowledged, {} rejected, {} unavailable, {} unknown, {} aborted; longest without an acknowledgement {:.0} ms",
            self.acked,
            self.rejected,
            self.unavailable,
            self.unknown,
            self.other,
            ms(self.longest_gap)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(start: Instant, from_ms: u64, to_ms: u64, result: Result<u64, Error>) -> Attempt {
        Attempt {
            key: String::new(),
            node: 1,
            started: start + Duration::from_millis(from_ms),
            finished: start + Duration::from_millis(to_ms),
            result,
        }
    }

    #[test]
    fn the_longest_gap_counts_the_window_edges_and_only_acknowledgements() {
        let t = Instant::now();
        let attempts = vec![
            at(t, 0, 100, Ok(1)),
            at(t, 100, 400, Err(Error::OutcomeUnknown("x".into()))),
            at(t, 150, 450, Err(Error::Unavailable("x".into()))),
            at(t, 400, 700, Ok(2)),
            // Before the window: not counted.
            at(t, 0, 10, Err(Error::Rejected("x".into()))),
        ];
        let s = Seen::of(
            &attempts,
            |_| true,
            t + Duration::from_millis(50),
            t + Duration::from_millis(1000),
        );
        assert_eq!(
            (s.acked, s.unknown, s.unavailable, s.rejected),
            (2, 1, 1, 0)
        );
        // 100 -> 700 is longer than 700 -> 1000 and 50 -> 100.
        assert_eq!(s.longest_gap, Duration::from_millis(600));
        let none = Seen::of(&attempts, |a| a.node == 2, t, t + Duration::from_secs(1));
        assert_eq!(none.longest_gap, Duration::from_secs(1));
    }
}
