# Architecture decision records

One file per decision that shaped tursoraft. Each record says what was known
when the decision was made, what was decided, what was rejected and why, and
what follows from it. The numbers in a record were measured on one machine
with injected network delay unless the record says otherwise.

A record is not edited when a decision changes. A new record replaces it, and
the old one gets `Status: replaced by NNNN`.

| # | Decision | Status |
|---|---|---|
| [0001](0001-raft-with-voters-close-together.md) | Raft, with the voters close together and far instances that do not vote | Accepted |
| [0002](0002-replicate-changed-rows.md) | Replicate the rows a transaction changed, not WAL frames or SQL text | Accepted |
| [0003](0003-files-change-only-by-applying-committed-entries.md) | A node changes its database file only by applying committed log entries | Accepted |
| [0004](0004-commit-time-check-on-row-contents.md) | Transactions run where they start and are checked at commit by comparing row contents | Accepted |
| [0005](0005-leader-second-file.md) | The leader captures on a second database file that runs ahead | Accepted |
| [0006](0006-change-subscriptions-never-slow-apply.md) | Change subscriptions drop events for a slow subscriber and never slow apply | Accepted |
| [0007](0007-lost-directory-is-refused.md) | A node that lost its Raft directory is refused under its old id | Accepted |
| [0008](0008-reads-only-followers-in-durable-objects.md) | Followers in Durable Objects hold a reads-only copy fed from change subscriptions | Accepted, not built yet |

## Template

```markdown
# NNNN: the decision, as a sentence

Status: Accepted | Proposed | replaced by NNNN
Date: YYYY-MM-DD
Decided by: who

## Context

What was known, with the measurements that mattered.

## Decision

What tursoraft does.

## Alternatives rejected

Each alternative and the reason it lost.

## Consequences

What this costs, what it rules out, and what an application has to do.
```
