# 0002: Replicate the rows a transaction changed, not WAL frames or SQL text

Status: Accepted
Date: 2026-11-11
Decided by: the coordinating agent; Kalle Ahlström can veto

## Context

Something has to go into the Raft log for every transaction. Turso offers
three candidates:

- Change data capture (CDC): `PRAGMA capture_data_changes_conn('full')`
  records the row before and after every change, and schema changes. The
  connection that runs a transaction can read these records before it
  commits (`crates/changeset/tests/cdc_capture.rs`).
- Raw WAL frames, behind Turso's `conn_raw_api` feature.
- The SQL text of the statements.

## Decision

A log entry holds change sets. A change set is the rows one transaction
changed, read from CDC, with the row before and the row after for updates and
deletes, and the log position the transaction read from.

The database runs in WAL mode, not MVCC mode. Foreign keys are enforced on
every node, at capture and at apply.

## Alternatives rejected

- **WAL frames, as dqlite does.** The leader would write the frames to its
  own WAL before a majority has them, so a reader on the leader could see a
  write that is later lost. Frames are also 4 KiB pages: two transactions
  that change unrelated rows on one page would look like a conflict, which
  rules out the check of ADR 0004.
- **SQL text, as rqlite does.** `random()`, `datetime('now')` and similar
  give different results on different nodes unless every statement is
  rewritten.
- **The sync engine's replay code for rows.** It writes each row as "insert,
  or overwrite if the key exists", so the last writer wins silently. Rows are
  written by our own code with plain INSERT, UPDATE and DELETE. Schema
  changes still go through the sync engine's code.
- **MVCC mode.** Replaying changes onto an MVCC database is untested in
  Turso.

## Consequences

- Change sets are small: 41 bytes for an insert of one small row, 54 for an
  update of one column, 34 for a delete, 142 for a bank transfer of two
  updates and one insert.
- Functions that are not deterministic give the same result on every node,
  because their results are replicated and not the calls.
- `WITHOUT ROWID` tables cannot be replicated. Turso refuses CDC on them.
- Turso's CDC records nothing for CREATE and DROP of triggers and views.
  Capture adds those records itself by comparing the schema before and after
  the statement.
- A change set and a snapshot are each held in memory whole. That is fine for
  a prototype and not for large databases.
- CDC is labelled "Early Preview" in Turso's manual, and Turso is before 1.0.
  The workspace takes Turso from Git at one fixed commit.
- Raw WAL frames stay the fallback if CDC turns out to be unusable.
