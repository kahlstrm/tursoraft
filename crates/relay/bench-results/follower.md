# A follower Durable Object under Miniflare

These numbers come from one machine running a local Miniflare. They say
nothing about Cloudflare's network, its storage, or where a DO runs.

Setup: an Amp orb with 8 CPUs and 15 GB of memory. Release build. Three
voters in one process, each on its own tokio runtime of 2 workers, with
0 ms between them. Miniflare 4.20260730.0 and workerd 1.20260730.1 ran
in a child process (`edge/test/serve.mjs`). The relay talked to the Worker
over a WebSocket on 127.0.0.1. The tree is tursoraft b4380b0 with the
relay commits.

```sh
cargo test -p tursoraft-relay --release --test measure -- --ignored --nocapture --test-threads=1
COPY_ROWS=1000000 cargo test -p tursoraft-relay --release --test measure -- --ignored --nocapture full_copy
(cd edge && npm run bench)
```

## From the leader's acknowledgement to the follower having applied the write

Each run made 500 writes, one at a time, with 2 ms between them. Each write
updated one row. A write's time is measured from the moment
`Node::execute` returned on the leader (`CommitMode::Quorum`) until the
relay received the follower's acknowledgement. The follower sends that
acknowledgement after its `transactionSync` has returned, so the time is an
upper bound.

| relay on | writes | median ms | p90 ms | p99 ms | max ms |
|---|---|---|---|---|---|
| the leader | 500 | 1.08 | 1.32 | 1.91 | 5.17 |
| a follower voter | 500 | 1.18 | 1.57 | 1.94 | 2.87 |

## One follower and 16 writers

The 16 writers sent quorum writes to the leader, 300 transactions each, and
the relay ran on a follower voter. "lag at the end" is the time from the
last write's acknowledgement until the follower had applied it. In both runs
the follower kept up: it was about 1 ms behind, so the cluster set the rate,
not the follower.

| rows per transaction | transactions | cluster tx/s | follower done after s | follower tx/s | follower rows/s | lag at the end ms | messages | change sets per message |
|---|---|---|---|---|---|---|---|---|
| 1 | 4800 | 3409 | 1.41 | 3407 | 3407 | 1 | 534 | 9.0 |
| 10 | 4800 | 2369 | 2.03 | 2368 | 23680 | 1 | 610 | 7.9 |

## What one follower can apply

`edge/test/bench.mjs` sends `changes` messages straight to the follower,
with no cluster and no relay. It keeps up to 16 unacknowledged messages, as
the relay does. Each change set inserts rows of about 120 bytes.

| change sets per message | rows per change set | transactions | s | tx/s | rows/s |
|---|---|---|---|---|---|
| 1 | 1 | 20000 | 15.52 | 1289 | 1289 |
| 16 | 1 | 20000 | 1.44 | 13881 | 13881 |
| 256 | 1 | 20000 | 0.31 | 63566 | 63566 |
| 16 | 10 | 20000 | 4.11 | 4863 | 48629 |
| 256 | 10 | 20000 | 1.89 | 10574 | 105736 |

Each message costs about 0.8 ms, mostly for its `transactionSync` and its
reply. The relay therefore batches every event that is waiting into one
message, up to 256 change sets or about 512 KB.

## A full copy

Rows of `items(id INTEGER PRIMARY KEY, sku TEXT, price INTEGER, title TEXT)`,
about 120 bytes of values each. The time runs from starting the relay until
the follower acknowledged the copy's last message. That includes connecting,
the relay's SELECT of the whole database, sending in chunks of about 512 KB,
writing to the staging tables, and the swap.

| rows | bytes of row values | database and WAL files MB | copy s | rows/s | messages |
|---|---|---|---|---|---|
| 10000 | 1200000 | 2.3 | 0.11 | 89110 | 8 |
| 100000 | 12000000 | 13.8 | 0.95 | 104762 | 40 |
| 1000000 | 120000000 | 133.5 | 7.64, 8.12 (two runs) | 130853, 123131 | 363 |

The relay holds the whole result of the copy's SELECT in memory. On
Cloudflare a DO has 128 MB, and Miniflare did not enforce that here. The
follower only ever holds one message of about 512 KB, plus what SQLite
caches.
