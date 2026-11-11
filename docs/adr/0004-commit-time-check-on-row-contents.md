# 0004: Transactions run where they start and are checked at commit by comparing row contents

Status: Accepted
Date: 2026-11-11
Decided by: the coordinating agent; Kalle Ahlström can veto

## Context

When every statement of a transaction is sent to the leader, a transaction of
five statements from an instance 220 ms away costs five round trips. Aurora
DSQL avoids this: the transaction runs locally and crosses the network once,
at commit, where it is checked against the transactions that committed in the
meantime.

Change sets (ADR 0002) carry the row a transaction read before it changed it,
so such a check can be made per row.

## Decision

- An interactive transaction runs on the file of the instance that started
  it. At commit the instance sends the leader one message: the change set and
  the log position the transaction read from.
- Before a change set is written, each row it touches is compared with the
  row as it is now:
  - An update or delete is rejected when the row it read differs from the
    current row, or is gone.
  - An insert is rejected when a row with its key already exists.
  - A change set captured before any schema change is rejected.
- The decision uses only what is in the log and in the database file, so
  every node decides the same way, also after a restart or a snapshot.
- The leader makes the same check when the change set arrives, against its
  second file (ADR 0005), so conflicts mostly stay out of the log.

## Alternatives rejected

- **Send each statement to the leader.** One round trip per statement. It
  stays in the code as `Node::execute` and is the baseline.
- **Store a "last written at" log position for every row.** It would catch a
  row that was changed and changed back. It costs a second write per row of
  every table.
- **Record what each transaction read and check that too.** That would give
  serializable transactions. CDC does not capture reads.

## Consequences

- A five-statement transaction from 220 ms away costs 239 ms, where sending
  each statement costs 1,172 ms.
- Isolation is snapshot isolation. Two transactions that each read the row
  the other writes both commit (write skew).
- A row that was changed and changed back passes the check.
- The check is stricter than one node running the same SQL: two transactions
  that set one row to the same value conflict, and so does any transaction
  captured before a schema change on an unrelated table.
- Two instances that insert into the same table and let the database pick the
  key get the same key, and the second is rejected. Applications choose their
  own keys, or run the transaction again.
- An application has to handle a rejected commit, usually by running the
  transaction again.
- A far instance loses most conflicts on rows that many instances write,
  because its change set arrives last. In the store's flash sale with one
  counter row per product, the instances in Ashburn and Singapore won 1 and 0
  checkouts while Falkenstein won 127. One row per unit for sale, or letting
  the leader reserve, removes this; see the `reserve` setting of
  `tursoraft-shop`.
- The check adds one key lookup per row: 1.08–1.21 ms to apply a bank
  transfer, where it took 1.02–1.07 ms.
