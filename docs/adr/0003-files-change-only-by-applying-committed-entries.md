# 0003: A node changes its database file only by applying committed log entries

Status: Accepted
Date: 2026-11-11
Decided by: the coordinating agent; Kalle Ahlström can veto

## Context

A transaction has to run somewhere before its change set exists. If the node
that runs it also commits it to its file at once, that file is ahead of the
log, and the write has to be undone when the entry never reaches a majority.

Each transaction also chooses when it is acknowledged: `quorum` (after a
majority has stored the entry) or `async` (earlier, accepting that the write
can be lost).

## Decision

- The node that runs a transaction captures its change set and rolls the
  transaction back.
- Every node, the leader included, changes its file only when it applies an
  entry that Raft has committed.
- An `async` write is acknowledged after the leader has flushed the log entry
  to its own disk, before any other node has it. The leader's file is still
  unchanged at that point.
- A read is exactly one SELECT statement. Anything else is refused on the
  read path, so that a read cannot change the file outside the log.

## Alternatives rejected

- **The leader commits first and replicates after.** Faster on the leader,
  because the write is done once. It needs an undo in the database file when
  a write is lost, or a rebuild of the node from a snapshot, as LiteFS does.
- **Acknowledge `async` writes after the leader has changed its file.** That
  breaks the rule above for the one mode where writes can be lost.

## Consequences

- No reader on any node sees a write that a majority has not stored.
- A lost `async` write leaves nothing to undo. Under leader kills nothing was
  half applied and the nodes stayed equal (`crates/proof/tests/failures.rs`).
- No write acknowledged in `quorum` mode was lost in any run with leader
  kills, pauses and hand-overs.
- A leader killed in `async` mode lost up to 159 acknowledged transfers at a
  100 ms round trip, about half of what it acknowledged in the last round
  trip.
- The leader does each write twice: once to capture it and once to apply it.
- A transaction that runs before an earlier one is applied would not see it.
  ADR 0005 is the answer to that.
