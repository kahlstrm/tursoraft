# 0006: Change subscriptions drop events for a slow subscriber and never slow apply

Status: Accepted
Date: 2026-11-11
Decided by: the coordinating agent, four choices that Kalle Ahlström can
each veto

## Context

An application in the same process as a node wants to know which rows
changed, for example to update a page that is open in a browser. Every node
applies the same committed change sets in the same order, so every node can
tell its application the same thing.

Events are not stored anywhere. The Raft log is cut after every snapshot
(every 5,000 entries), so it cannot serve as a history of events either.

## Decision

`Node::subscribe()` returns the applied log index the subscription starts
after, and a stream of events.

1. **After apply, never before.** A change set is sent to subscribers after
   the node has committed it to its file, and only when it was applied, not
   when it was rejected. One event holds all rows of one transaction.
2. **A slow subscriber loses events.** Each subscriber has a queue
   (`Options::subscriber_queue`, 1,024 by default). When it is full the
   oldest events are dropped, and the subscriber gets `Lagged` with the
   number it missed. Apply never waits.
3. **A snapshot sends one `Reset`.** When a node catches up by installing a
   snapshot, subscribers get one `Reset` and no row changes for the entries
   the snapshot covers.
4. **Nothing is resumed.** After a restart the application subscribes again.

After `Lagged` or `Reset`, and at the start, the application reads the
database, and ignores events up to the index it read at.

Rows arrive as the values before and after, in the table's column order,
without column names. A schema change arrives as a change of the table
`sqlite_schema`.

## Alternatives rejected

- **Make the node wait for its slowest subscriber.** One slow application
  would hold back the node, and through a voter the whole cluster.
- **After a snapshot, compare the old file with the new one row by row.** It
  costs a full scan of both.
- **Resume from a log position.** It could only reach back to the last
  snapshot.
- **Decode rows during apply.** Rows are decoded by the first subscriber that
  receives an event, so a node without subscribers pays nothing for decoding.

## Consequences

- An application must be able to build its state again from the database at
  any time. Subscriptions are a way to learn early, not a log to rely on.
- Applying did not get measurably slower: with 0, 1 and 16 subscribers per
  node the time per change set stayed within the spread of the tree without
  subscriptions (`crates/node/bench-results/subscriptions.md`).
- A subscriber that lags holds up to `subscriber_queue` change sets in memory
  per node.
- After `DROP TABLE` a subscriber gets the delete of the `sqlite_schema` row
  and no deletes of the table's rows.
- For a table without an INTEGER PRIMARY KEY, the row id in an event is the
  one of the node that captured the change and may differ from this node's
  file.
- Anything outside the node's process, such as the follower of ADR 0008, has
  to take a full copy after it reconnects.
