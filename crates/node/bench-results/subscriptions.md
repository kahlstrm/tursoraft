# What change subscriptions cost

`tests/subscribe_cost.rs`, release build, an Amp orb with 8 CPUs, 0 ms
round trip, 3 voters in one process (one tokio runtime of 2 workers per
node), pipelining on (256). 16 writers on the leader, 500 `INSERT`s each
(8000 transactions, 1 row of about 115 bytes each), `CommitMode::Quorum`.
Subscribers are on every node; each reads every row of every event, so each
event is decoded once per node. Apply time is `Stats::apply_micros`, which
includes sending to subscribers.

Tree: tursoraft 81e0cd4 (Turso 0c9fe0dd), with and without the
subscription commits. The baseline is the same test file, with the function
that starts subscribers replaced by one that starts none. The runs
alternated baseline and change, 5 rounds; the table gives the median of the
5 and, for throughput, the range.

| tree | subscribers per node | tx/s (median, range) | leader apply µs per change set | follower apply µs per change set | leader apply µs per entry | change sets per entry | events missed |
|---|---|---|---|---|---|---|---|
| 81e0cd4 | (none) | 2836 (2814–2980) | 173.8 | 173.5 | 1405 | 8.2 | – |
| with subscriptions | 0 | 2879 (2850–2963) | 171.0 | 170.7 | 1414 | 8.3 | 0 |
| with subscriptions | 1 | 2985 (2814–3027) | 164.5 | 168.7 | 1433 | 8.6 | 0 |
| with subscriptions | 16 | 2944 (2795–3126) | 159.8 | 160.6 | 1501 | 9.4 | 0 |

The differences are within the spread between rounds of the same tree (the
baseline's leader apply time ranged from 162.6 to 177.7 µs per change set).
More subscribers came with larger entries (more change sets per entry),
which lowers the time per change set and raises it per entry.

Timed alone, in a scratch release test with the node's `Feed` and an entry
of 8 one-row change sets, sending cost per change set: 17 ns with no
subscriber, 0.46 µs with 1 and 1.1 µs with 16. The last two include
draining the subscribers on the same thread and decoding each change set
once (0.23 µs for a one-row change set).
