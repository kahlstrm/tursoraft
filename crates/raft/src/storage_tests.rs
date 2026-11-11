use std::fs::OpenOptions;
use std::future::Future;
use std::io::{self, Write};

use openraft::storage::{RaftLogReader, RaftLogStorage, RaftLogStorageExt};
use openraft::testing::log::{StoreBuilder, Suite};
use openraft::{EntryPayload, StorageError};
use tempfile::TempDir;

use crate::log_store::LogStore;
use crate::state_machine::StateMachineAdaptor;
use crate::{CountingOptions, CountingStateMachine, LogEntry, LogId, Proposals, TypeConfig, Vote};

type Sm = StateMachineAdaptor;
type OpenraftSuite = Suite<TypeConfig, LogStore, Sm, Builder, TempDir>;

struct Builder;

impl StoreBuilder<TypeConfig, LogStore, Sm, TempDir> for Builder {
    async fn build(&self) -> Result<(TempDir, LogStore, Sm), StorageError<TypeConfig>> {
        let dir = TempDir::new().unwrap();
        // Small segments, so the suite's purges and truncations cross files.
        let log = LogStore::open(dir.path(), 256).unwrap();
        let sm = CountingStateMachine::new(CountingOptions {
            path: Some(dir.path().join("sm")),
            ..Default::default()
        })
        .unwrap();
        let sm = StateMachineAdaptor::new(sm).await.unwrap();
        Ok((dir, log, sm))
    }
}

fn run_suite_test<F, Fut>(test: F)
where
    F: FnOnce(LogStore, Sm) -> Fut,
    Fut: Future<Output = io::Result<()>>,
{
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (_dir, log, sm) = Builder.build().await.unwrap();
        test(log, sm).await.unwrap();
    });
}

/// openraft's storage tests, one `#[test]` each.
macro_rules! openraft_suite {
    ($($name:ident),* $(,)?) => {
        mod openraft_suite {
            use super::*;
            $(
                #[test]
                fn $name() {
                    run_suite_test(OpenraftSuite::$name);
                }
            )*

            #[test]
            fn transfer_snapshot() {
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(OpenraftSuite::transfer_snapshot(&Builder))
                    .unwrap();
            }
        }
    };
}

openraft_suite!(
    last_membership_in_log_initial,
    last_membership_in_log,
    last_membership_in_log_multi_step,
    get_membership_initial,
    get_membership_from_log_and_empty_sm,
    get_membership_from_empty_log_and_sm,
    get_membership_from_log_le_sm_last_applied,
    get_membership_from_log_gt_sm_last_applied_1,
    get_membership_from_log_gt_sm_last_applied_2,
    get_initial_state_without_init,
    get_initial_state_membership_from_empty_log_and_sm,
    get_initial_state_membership_from_sm_inlog_is_smaller,
    get_initial_state_membership_from_log_insm_is_smaller,
    get_initial_state_with_state,
    get_initial_state_last_log_gt_sm,
    get_initial_state_last_log_lt_sm,
    get_initial_state_last_log_lt_sm_past_snapshot,
    get_initial_state_sm_lt_purged,
    get_initial_state_inverted_log_order,
    get_initial_state_conflict_at_same_index,
    get_initial_state_log_ids,
    get_initial_state_re_apply_committed,
    save_vote,
    get_log_entries,
    limited_get_log_entries,
    leader_bounded_stream,
    entries_stream,
    try_get_log_entry,
    log_reader_reads_new_entries,
    initial_logs,
    get_log_state,
    get_log_id,
    last_id_in_log,
    last_applied_state,
    purge_logs_upto_0,
    purge_logs_upto_5,
    purge_logs_upto_20,
    delete_logs_after_11,
    delete_logs_after_5,
    delete_logs_after_0,
    delete_logs_after_none,
    append_to_log,
    snapshot_meta,
    snapshot_meta_optional,
    apply_single,
    apply_multiple,
);

fn log_id(index: u64) -> LogId {
    LogId::new(
        openraft::impls::leader_id_adv::LeaderId {
            term: 1,
            node_id: 1,
        },
        index,
    )
}

fn entry(index: u64, proposal: &[u8]) -> LogEntry {
    LogEntry {
        log_id: log_id(index),
        payload: EntryPayload::Normal(Proposals(vec![proposal.to_vec()])),
    }
}

async fn indexes(log: &mut LogStore) -> Vec<u64> {
    log.try_get_log_entries(0..)
        .await
        .unwrap()
        .iter()
        .map(|e| e.log_id.index)
        .collect()
}

/// Reopening a log gives back what was flushed, without a torn last record,
/// after truncations and purges across segment files.
#[tokio::test]
async fn reopen_after_truncate_purge_and_torn_write() {
    let dir = TempDir::new().unwrap();
    {
        let mut log = LogStore::open(dir.path(), 200).unwrap();
        log.save_vote(&Vote::new(3, 2)).await.unwrap();
        log.blocking_append((1..=20).map(|i| entry(i, &[i as u8; 40])))
            .await
            .unwrap();
        log.truncate_after(Some(log_id(14))).await.unwrap();
        log.blocking_append((15..=17).map(|i| entry(i, b"second term")))
            .await
            .unwrap();
        log.purge(log_id(6)).await.unwrap();
    }
    // A record cut short by a crash during a write.
    let last = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "seg"))
        .max()
        .unwrap();
    OpenOptions::new()
        .append(true)
        .open(&last)
        .unwrap()
        .write_all(&[60, 0, 0, 0, 1, 2])
        .unwrap();

    let mut log = LogStore::open(dir.path(), 200).unwrap();
    assert_eq!(log.read_vote().await.unwrap(), Some(Vote::new(3, 2)));
    assert_eq!(indexes(&mut log).await, (7..=17).collect::<Vec<_>>());
    let state = log.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(6));
    let e16 = log.try_get_log_entries(16..17).await.unwrap();
    assert_eq!(
        e16[0].payload,
        EntryPayload::Normal(Proposals(vec![b"second term".to_vec()]))
    );

    // The cut-off tail is gone, so the next append continues the log.
    log.blocking_append([entry(18, b"after reopen")])
        .await
        .unwrap();
    drop(log);
    let mut log = LogStore::open(dir.path(), 200).unwrap();
    assert_eq!(indexes(&mut log).await, (7..=18).collect::<Vec<_>>());
}
