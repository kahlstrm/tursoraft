//! Adaptor from openraft's state machine API to [`StateMachine`].
//!
//! Each openraft entry becomes one [`Entry`]: normal entries carry their
//! proposals; blank and membership entries carry none. `Entry::raft_meta` is
//! the bincode encoding of [`RaftMeta`], the entry's log id and the membership
//! in effect after it. The state machine stores it with the entry, so
//! [`StateMachine::applied`] and every [`Snapshot`] give back both.

use std::io;
use std::sync::{Arc, Mutex};

use futures_util::{Stream, StreamExt};
use openraft::alias::{SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend};
use serde::{Deserialize, Serialize};
use tursoraft_statemachine::{Applied, Entry, Outcome, Snapshot, StateMachine};

use crate::{LogId, TypeConfig};

type Membership = StoredMembershipOf<TypeConfig>;
type RaftSnapshot = SnapshotOf<TypeConfig, Vec<u8>>;

/// What this crate keeps in [`Entry::raft_meta`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RaftMeta {
    pub log_id: LogId,
    pub membership: Membership,
}

impl RaftMeta {
    fn decode(bytes: &[u8]) -> io::Result<RaftMeta> {
        bincode::deserialize(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

/// [`StateMachine`] with its error type turned into `io::Error`, so that
/// [`crate::Raft`] does not depend on the application's state machine type.
trait DynStateMachine: Send {
    fn applied(&mut self) -> io::Result<Option<Applied>>;
    fn apply(&mut self, entry: &Entry) -> io::Result<Vec<Outcome>>;
    fn snapshot(&mut self) -> io::Result<Snapshot>;
    fn install_snapshot(&mut self, snapshot: &Snapshot) -> io::Result<()>;
}

impl<S: StateMachine> DynStateMachine for S {
    fn applied(&mut self) -> io::Result<Option<Applied>> {
        StateMachine::applied(self).map_err(io::Error::other)
    }
    fn apply(&mut self, entry: &Entry) -> io::Result<Vec<Outcome>> {
        StateMachine::apply(self, entry).map_err(io::Error::other)
    }
    fn snapshot(&mut self) -> io::Result<Snapshot> {
        StateMachine::snapshot(self).map_err(io::Error::other)
    }
    fn install_snapshot(&mut self, snapshot: &Snapshot) -> io::Result<()> {
        StateMachine::install_snapshot(self, snapshot).map_err(io::Error::other)
    }
}

type Shared = Arc<Mutex<Box<dyn DynStateMachine>>>;

/// openraft's view of the application's [`StateMachine`].
pub struct StateMachineAdaptor {
    sm: Shared,
    /// Membership after the last applied entry.
    membership: Membership,
    current_snapshot: Arc<Mutex<Option<RaftSnapshot>>>,
}

impl StateMachineAdaptor {
    pub(crate) async fn new<S: StateMachine>(sm: S) -> io::Result<Self> {
        let sm: Shared = Arc::new(Mutex::new(Box::new(sm)));
        let applied = blocking(&sm, |sm| sm.applied()).await?;
        let membership = match applied {
            Some(a) => RaftMeta::decode(&a.raft_meta)?.membership,
            None => Membership::default(),
        };
        Ok(StateMachineAdaptor {
            sm,
            membership,
            current_snapshot: Arc::new(Mutex::new(None)),
        })
    }
}

/// Runs `f` on the state machine on a blocking thread.
async fn blocking<T, F>(sm: &Shared, f: F) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut dyn DynStateMachine) -> io::Result<T> + Send + 'static,
{
    let sm = sm.clone();
    tokio::task::spawn_blocking(move || f(&mut **sm.lock().unwrap()))
        .await
        .map_err(io::Error::other)?
}

impl RaftStateMachine<TypeConfig> for StateMachineAdaptor {
    type SnapshotData = Vec<u8>;
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(&mut self) -> io::Result<(Option<LogId>, Membership)> {
        match blocking(&self.sm, |sm| sm.applied()).await? {
            None => Ok((None, Membership::default())),
            Some(a) => {
                let meta = RaftMeta::decode(&a.raft_meta)?;
                Ok((Some(meta.log_id), meta.membership))
            }
        }
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = io::Result<EntryResponder<TypeConfig>>> + Unpin + OptionalSend,
    {
        let mut membership = self.membership.clone();
        let mut batch = Vec::new();
        let mut responders = Vec::new();
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let proposals = match entry.payload {
                EntryPayload::Blank => Vec::new(),
                EntryPayload::Normal(proposals) => proposals.0,
                EntryPayload::Membership(m) => {
                    membership = Membership::new(Some(entry.log_id), m);
                    Vec::new()
                }
            };
            let meta = RaftMeta {
                log_id: entry.log_id,
                membership: membership.clone(),
            };
            batch.push(Entry {
                index: entry.log_id.index,
                raft_meta: bincode::serialize(&meta).expect("RaftMeta serializes"),
                proposals,
            });
            responders.push(responder);
        }
        let outcomes = blocking(&self.sm, move |sm| {
            let mut outcomes = Vec::with_capacity(batch.len());
            for entry in &batch {
                let out = sm.apply(entry)?;
                assert!(
                    out.is_empty() || out.len() == entry.proposals.len(),
                    "StateMachine::apply returned {} outcomes for {} proposals at index {}",
                    out.len(),
                    entry.proposals.len(),
                    entry.index
                );
                outcomes.push(out);
            }
            Ok(outcomes)
        })
        .await?;
        self.membership = membership;
        for (responder, outcome) in responders.into_iter().zip(outcomes) {
            if let Some(responder) = responder {
                responder.send(outcome);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            sm: self.sm.clone(),
            current_snapshot: self.current_snapshot.clone(),
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        bytes: Vec<u8>,
    ) -> io::Result<()> {
        let decoded: Snapshot = bincode::deserialize(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let inner_meta = RaftMeta::decode(&decoded.applied.raft_meta)?;
        assert_eq!(
            Some(inner_meta.log_id),
            meta.last_log_id,
            "snapshot data does not match its metadata"
        );
        blocking(&self.sm, move |sm| sm.install_snapshot(&decoded)).await?;
        self.membership = meta.last_membership.clone();
        *self.current_snapshot.lock().unwrap() = Some(RaftSnapshot {
            meta: meta.clone(),
            snapshot: bytes,
        });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> io::Result<Option<RaftSnapshot>> {
        Ok(self.current_snapshot.lock().unwrap().clone())
    }
}

pub struct SnapshotBuilder {
    sm: Shared,
    current_snapshot: Arc<Mutex<Option<RaftSnapshot>>>,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    type SnapshotData = Vec<u8>;

    async fn build_snapshot(&mut self) -> io::Result<RaftSnapshot> {
        let snapshot = blocking(&self.sm, |sm| sm.snapshot()).await?;
        let Applied { index, raft_meta } = &snapshot.applied;
        let raft_meta = RaftMeta::decode(raft_meta)?;
        assert_eq!(
            *index, raft_meta.log_id.index,
            "Applied::index does not match its raft_meta"
        );
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: Some(raft_meta.log_id),
            last_membership: raft_meta.membership,
        };
        let bytes = bincode::serialize(&snapshot).map_err(io::Error::other)?;
        let built = RaftSnapshot {
            meta,
            snapshot: bytes,
        };
        *self.current_snapshot.lock().unwrap() = Some(built.clone());
        Ok(built)
    }
}
