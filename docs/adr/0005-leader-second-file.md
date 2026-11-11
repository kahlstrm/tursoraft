# 0005: The leader captures on a second database file that runs ahead

Status: Accepted
Date: 2026-11-11
Decided by: the implementing agent, by reasoning and not by measuring the
alternatives; Kalle Ahlström can veto

## Context

Under ADR 0003 the leader's file changes only when an entry is applied. A
transaction captured on that file does not see transactions that are still on
their way to a majority. So the leader could capture a group of transactions
only after it had applied the group before, which is one group per round
trip:

- 16 `quorum` writers at the leader waited two round trips each.
- One `async` writer waited a full round trip per write, so `async` gained
  nothing for a caller that writes back-to-back.

## Decision

- The leader keeps a second database file next to its own
  (`<name>.ahead.db`). It runs each group of transactions on that file for
  real, so the next group sees the one before, and hands each group to Raft
  before it captures the next.
- The leader's own file still changes only by applying committed entries.
- The second file is thrown away and copied again from the real one when the
  node becomes leader and whenever a group fails.
- A change set captured on the second file is marked as having read writes
  that were not applied yet. Each database file stores the log position of
  the last rejection. A marked change set is rejected when anything was
  rejected after the position it was based on.
- Up to 256 groups may be on their way at once (`Options::max_pending_groups`).
  Setting it to 1 turns the second file off.
- The second file starts its WAL over once it holds 500 frames.

## Alternatives rejected

- **Replay all pending change sets onto the leader's file at every
  capture.** The cost grows with the number of groups on their way.
- **Let the leader commit its own capture.** The leader would then change its
  file in a different way from every other node, against ADR 0003.
- **A finer rule after a rejection**, which rejects only transactions that
  read the rejected rows. It would have to record what each transaction read.

## Consequences

- One `async` writer at the leader waits 1.5–2.0 ms per write at a 10 ms and
  at a 100 ms round trip, where it waited 13.8 and 104.6 ms.
- 16 `quorum` writers wait one round trip, where they waited two.
- The leader stores the database twice, and copies it at every change of
  leader and after every failed group.
- After a rejection, every later transaction that read unapplied writes is
  rejected, also one that did not read the rejected row.
- A group that is lost with a change of leader takes the later groups with
  it.
- Without the WAL restart, `async` writes with voters 0–1 ms apart slowed
  down over time. The value 500 is not tuned.
- A change set proposed through the Raft client directly, and not through
  `execute` or an interactive commit, is missing from the second file for
  about 30 ms. Only test code proposes that way. The test that shows it is
  kept and marked as ignored.
