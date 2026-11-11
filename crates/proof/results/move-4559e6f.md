# Benchmark `move` section on the integration tree 4559e6f

Command: `cargo run --release -p tursoraft-proof -- bench --only move` (all six nodes in one orb; delays from the delay table).

tursoraft bench: 100 accounts, transfer = 2 updates + 1 insert; warm-up 1s, window 4s..12s and at least 200 transfers; max_pending_groups [1, 256]

## Moving the voters and the leader from Europe to Asia

Nodes 1 Frankfurt, 2 Paris, 3 London, 4 Singapore, 5 Hong Kong, 6 Tokyo. Before: voters Europe, leader Frankfurt, Asia non-voting followers. After: voters Asia, leader Singapore, Europe non-voting followers. A move: wait until the new voters applied the leader's last entry, `change_voters`, `transfer_leader`. Timeouts: heartbeat 458 ms, election 1374..2748 ms.

Round trips from cloudping.co (AWS P50, both directions averaged) / as measured here (ms):

| pair | configured | measured |
|---|---:|---:|
| Frankfurt – Paris | 10.1 | 10.5 |
| Frankfurt – London | 14.4 | 14.2 |
| Paris – London | 9.9 | 9.7 |
| Singapore – Hong Kong | 35.1 | 34.8 |
| Singapore – Tokyo | 70.1 | 70.1 |
| Hong Kong – Tokyo | 52.5 | 52.9 |
| Frankfurt – Singapore | 164.8 | 165.2 |
| Frankfurt – Hong Kong | 193.3 | 193.2 |
| Frankfurt – Tokyo | 229.4 | 229.3 |
| Paris – Singapore | 158.8 | 158.2 |
| Paris – Hong Kong | 196.5 | 196.4 |
| Paris – Tokyo | 219.8 | 219.6 |
| London – Singapore | 164 | 163.3 |
| London – Hong Kong | 199.4 | 199.5 |
| London – Tokyo | 213.9 | 213.0 |

### Write latency before and after the move

One cell: `writers` writers in Frankfurt and as many in Singapore at once. `interactive`: `Client::begin`, read both balances, write them as values, commit in quorum mode.

| pending | voters | mode | writers per place | writer in | p50 ms | p99 ms | n | tx/s | tx/entry | lag p50 ms | lag max ms | behind max | rej | unk | unav | resets |
|---:|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | Europe | Quorum | 1 | Frankfurt | 14.6 | 30.6 | 255 | 63 | 1.0 | 18.4 | 129.5 | 10 | 0 | 0 | 0 | 0 |
| 1 | Europe | Quorum | 1 | Singapore | 189.1 | 195.4 | 21 | 5 | 1.0 | 18.4 | 129.5 | 10 | 0 | 0 | 0 | 0 |
| 1 | Europe | Quorum | 16 | Frankfurt | 33.1 | 44.7 | 1920 | 476 | 9.3 | 17.7 | 129.1 | 9 | 0 | 0 | 0 | 0 |
| 1 | Europe | Quorum | 16 | Singapore | 197.1 | 212.3 | 320 | 79 | 9.3 | 17.7 | 129.1 | 9 | 0 | 0 | 0 | 0 |
| 1 | Europe | Async | 1 | Frankfurt | 14.3 | 16.6 | 280 | 69 | 1.1 | 18.8 | 129.0 | 10 | 0 | 0 | 0 | 0 |
| 1 | Europe | Async | 1 | Singapore | 172.6 | 178.7 | 24 | 6 | 1.1 | 18.8 | 129.0 | 10 | 0 | 0 | 0 | 0 |
| 1 | Europe | Async | 16 | Frankfurt | 18.4 | 26.0 | 3440 | 853 | 17.5 | 18.8 | 130.6 | 8 | 0 | 0 | 0 | 0 |
| 1 | Europe | Async | 16 | Singapore | 183.0 | 194.3 | 352 | 87 | 17.5 | 18.8 | 130.6 | 8 | 0 | 0 | 0 | 0 |
| 1 | Europe | interactive | 1 | Frankfurt | 16.2 | 27.8 | 236 | 59 | 1.0 | 14.3 | 129.0 | 8 | 10 | 0 | 0 | 0 |
| 1 | Europe | interactive | 1 | Singapore | 186.5 | 194.4 | 12 | 3 | 1.0 | 14.3 | 129.0 | 8 | 10 | 0 | 0 | 0 |
| 1 | Europe | interactive | 16 | Frankfurt | 33.2 | 115.0 | 1138 | 283 | 6.4 | 13.8 | 138.6 | 9 | 784 | 0 | 0 | 0 |
| 1 | Europe | interactive | 16 | Singapore | 193.6 | 201.6 | 28 | 7 | 6.4 | 13.8 | 138.6 | 9 | 784 | 0 | 0 | 0 |
| 1 | Asia | Quorum | 1 | Frankfurt | 238.6 | 243.6 | 34 | 4 | 1.0 | 70.2 | 112.4 | 3 | 0 | 0 | 0 | 0 |
| 1 | Asia | Quorum | 1 | Singapore | 40.1 | 81.1 | 172 | 21 | 1.0 | 70.2 | 112.4 | 3 | 0 | 0 | 0 | 0 |
| 1 | Asia | Quorum | 16 | Frankfurt | 226.7 | 255.1 | 272 | 68 | 10.9 | 74.0 | 123.8 | 3 | 0 | 0 | 0 | 0 |
| 1 | Asia | Quorum | 16 | Singapore | 84.6 | 96.3 | 767 | 190 | 10.9 | 74.0 | 123.8 | 3 | 0 | 0 | 0 | 0 |
| 1 | Asia | Async | 1 | Frankfurt | 198.9 | 208.2 | 35 | 5 | 1.2 | 71.0 | 117.9 | 3 | 0 | 0 | 0 | 0 |
| 1 | Asia | Async | 1 | Singapore | 39.9 | 43.4 | 172 | 25 | 1.2 | 71.0 | 117.9 | 3 | 0 | 0 | 0 | 0 |
| 1 | Asia | Async | 16 | Frankfurt | 176.0 | 216.6 | 352 | 87 | 19.5 | 42.6 | 122.2 | 3 | 0 | 0 | 0 | 0 |
| 1 | Asia | Async | 16 | Singapore | 43.5 | 48.1 | 1488 | 369 | 19.5 | 42.6 | 122.2 | 3 | 0 | 0 | 0 | 0 |
| 1 | Asia | interactive | 1 | Frankfurt | 236.9 | 247.0 | 31 | 3 | 1.0 | 73.0 | 119.9 | 3 | 8 | 0 | 0 | 0 |
| 1 | Asia | interactive | 1 | Singapore | 42.2 | 82.6 | 174 | 20 | 1.0 | 73.0 | 119.9 | 3 | 8 | 0 | 0 | 0 |
| 1 | Asia | interactive | 16 | Frankfurt | 227.8 | 245.9 | 52 | 13 | 10.6 | 71.1 | 112.8 | 3 | 564 | 0 | 0 | 0 |
| 1 | Asia | interactive | 16 | Singapore | 78.4 | 85.4 | 598 | 148 | 10.6 | 71.1 | 112.8 | 3 | 564 | 0 | 0 | 0 |
| 256 | Europe | Quorum | 1 | Frankfurt | 14.5 | 16.3 | 278 | 69 | 1.0 | 19.2 | 128.9 | 11 | 0 | 0 | 0 | 0 |
| 256 | Europe | Quorum | 1 | Singapore | 179.7 | 184.5 | 22 | 5 | 1.0 | 19.2 | 128.9 | 11 | 0 | 0 | 0 | 0 |
| 256 | Europe | Quorum | 16 | Frankfurt | 17.7 | 25.8 | 3568 | 886 | 4.1 | 19.5 | 129.7 | 43 | 0 | 0 | 0 | 0 |
| 256 | Europe | Quorum | 16 | Singapore | 182.6 | 187.8 | 352 | 87 | 4.1 | 19.5 | 129.7 | 43 | 0 | 0 | 0 | 0 |
| 256 | Europe | Async | 1 | Frankfurt | 2.6 | 5.5 | 1463 | 363 | 1.0 | 19.2 | 128.9 | 51 | 0 | 0 | 0 | 0 |
| 256 | Europe | Async | 1 | Singapore | 167.5 | 169.5 | 25 | 6 | 1.0 | 19.2 | 128.9 | 51 | 0 | 0 | 0 | 0 |
| 256 | Europe | Async | 16 | Frankfurt | 6.8 | 14.8 | 8843 | 2187 | 10.9 | 19.3 | 129.6 | 29 | 0 | 0 | 0 | 0 |
| 256 | Europe | Async | 16 | Singapore | 173.2 | 182.4 | 369 | 91 | 10.9 | 19.3 | 129.6 | 29 | 0 | 0 | 0 | 0 |
| 256 | Europe | interactive | 1 | Frankfurt | 18.5 | 21.7 | 216 | 54 | 1.0 | 15.1 | 129.0 | 9 | 10 | 0 | 0 | 0 |
| 256 | Europe | interactive | 1 | Singapore | 183.9 | 187.0 | 14 | 4 | 1.0 | 15.1 | 129.0 | 9 | 10 | 0 | 0 | 0 |
| 256 | Europe | interactive | 16 | Frankfurt | 42.3 | 223.0 | 904 | 225 | 1.2 | 18.1 | 203.8 | 31 | 619 | 0 | 0 | 0 |
| 256 | Europe | interactive | 16 | Singapore | 188.0 | 265.7 | 65 | 16 | 1.2 | 18.1 | 203.8 | 31 | 619 | 0 | 0 | 0 |
| 256 | Asia | Quorum | 1 | Frankfurt | 206.5 | 211.0 | 34 | 5 | 1.0 | 73.4 | 119.1 | 4 | 0 | 0 | 0 | 0 |
| 256 | Asia | Quorum | 1 | Singapore | 41.1 | 44.0 | 171 | 24 | 1.0 | 73.4 | 119.1 | 4 | 0 | 0 | 0 | 0 |
| 256 | Asia | Quorum | 16 | Frankfurt | 208.9 | 220.2 | 306 | 76 | 1.9 | 105.8 | 140.2 | 37 | 0 | 0 | 0 | 0 |
| 256 | Asia | Quorum | 16 | Singapore | 42.5 | 53.6 | 1509 | 375 | 1.9 | 105.8 | 140.2 | 37 | 0 | 0 | 0 | 0 |
| 256 | Asia | Async | 1 | Frankfurt | 169.0 | 177.1 | 24 | 6 | 1.0 | 112.9 | 121.3 | 38 | 0 | 0 | 0 | 0 |
| 256 | Asia | Async | 1 | Singapore | 3.4 | 6.4 | 1123 | 279 | 1.0 | 112.9 | 121.3 | 38 | 0 | 0 | 0 | 0 |
| 256 | Asia | Async | 16 | Frankfurt | 173.7 | 182.0 | 368 | 91 | 10.3 | 105.4 | 123.5 | 27 | 0 | 0 | 0 | 0 |
| 256 | Asia | Async | 16 | Singapore | 8.6 | 17.0 | 7236 | 1793 | 10.3 | 105.4 | 123.5 | 27 | 0 | 0 | 0 | 0 |
| 256 | Asia | interactive | 1 | Frankfurt | 211.4 | 221.6 | 34 | 4 | 1.0 | 76.4 | 121.8 | 3 | 7 | 0 | 0 | 0 |
| 256 | Asia | interactive | 1 | Singapore | 45.5 | 54.3 | 171 | 22 | 1.0 | 76.4 | 121.8 | 3 | 7 | 0 | 0 | 0 |
| 256 | Asia | interactive | 16 | Frankfurt | 214.9 | 263.0 | 57 | 14 | 1.2 | 100.5 | 121.1 | 24 | 793 | 0 | 0 | 0 |
| 256 | Asia | interactive | 16 | Singapore | 63.4 | 159.1 | 709 | 177 | 1.2 | 100.5 | 121.1 | 24 | 793 | 0 | 0 | 0 |

### What writers see during a move

2 quorum, 2 async and 2 interactive writers in Frankfurt and in Singapore run through the move. `gap`: the longest time in which a place got no acknowledgement, from the start of the move to 1 s after its end; `gap before`: the same over an equally long window just before the move (at most 2.5 s). Counts are of answers in the move window.

| pending | to | catch-up ms | change_voters ms | transfer_leader ms | total ms | writer in | gap ms | gap before ms | acked | rejected | unavailable | unknown |
|---:|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | Asia | 121 | 556 | 245 | 922 | Frankfurt | 377 | 16 | 73 | 9 | 0 | 0 |
| 1 | Asia | 121 | 556 | 245 | 922 | Singapore | 282 | 177 | 104 | 10 | 0 | 0 |
| 1 | Europe | 123 | 506 | 195 | 824 | Frankfurt | 218 | 205 | 228 | 42 | 0 | 0 |
| 1 | Europe | 123 | 506 | 195 | 824 | Singapore | 350 | 40 | 43 | 15 | 0 | 0 |
| 1 | Asia | 128 | 559 | 245 | 932 | Frankfurt | 368 | 20 | 84 | 6 | 0 | 0 |
| 1 | Asia | 128 | 559 | 245 | 932 | Singapore | 282 | 176 | 104 | 9 | 0 | 0 |
| 1 | Europe | 96 | 500 | 194 | 790 | Frankfurt | 335 | 206 | 216 | 44 | 0 | 0 |
| 1 | Europe | 96 | 500 | 194 | 790 | Singapore | 362 | 43 | 36 | 16 | 0 | 0 |
| 1 | Asia | 128 | 561 | 241 | 930 | Frankfurt | 370 | 17 | 76 | 6 | 0 | 0 |
| 1 | Asia | 128 | 561 | 241 | 930 | Singapore | 461 | 172 | 105 | 8 | 0 | 0 |
| 1 | Europe | 120 | 499 | 194 | 812 | Frankfurt | 375 | 207 | 222 | 46 | 0 | 0 |
| 1 | Europe | 120 | 499 | 194 | 812 | Singapore | 517 | 43 | 35 | 13 | 0 | 0 |
| 256 | Asia | 123 | 567 | 243 | 933 | Frankfurt | 557 | 11 | 322 | 57 | 0 | 0 |
| 256 | Asia | 123 | 567 | 243 | 933 | Singapore | 276 | 171 | 317 | 33 | 0 | 0 |
| 256 | Europe | 121 | 500 | 193 | 814 | Frankfurt | 240 | 170 | 361 | 33 | 0 | 0 |
| 256 | Europe | 121 | 500 | 193 | 814 | Singapore | 258 | 20 | 240 | 43 | 0 | 0 |
| 256 | Asia | 126 | 563 | 240 | 929 | Frankfurt | 545 | 13 | 273 | 37 | 0 | 0 |
| 256 | Asia | 126 | 563 | 240 | 929 | Singapore | 200 | 172 | 282 | 39 | 0 | 0 |
| 256 | Europe | 115 | 513 | 187 | 815 | Frankfurt | 154 | 169 | 330 | 46 | 0 | 0 |
| 256 | Europe | 115 | 513 | 187 | 815 | Singapore | 239 | 13 | 228 | 36 | 0 | 0 |
| 256 | Asia | 127 | 557 | 242 | 927 | Frankfurt | 335 | 18 | 224 | 43 | 0 | 0 |
| 256 | Asia | 127 | 557 | 242 | 927 | Singapore | 175 | 172 | 236 | 36 | 0 | 0 |
| 256 | Europe | 119 | 499 | 194 | 813 | Frankfurt | 201 | 172 | 351 | 30 | 0 | 0 |
| 256 | Europe | 119 | 499 | 194 | 813 | Singapore | 289 | 14 | 239 | 35 | 0 | 0 |

### Moving to new nodes that start empty

The database holds about 20 MB of 4 KB blobs. The Asian nodes run but are not members. At the start of the move they are added as non-voting followers, then moved to as above, either after waiting until they applied the leader's last entry or at once. 2 quorum and 2 async writers in Frankfurt. `gap`: the longest time without an acknowledgement in Frankfurt from the start of the move to 1 s after its end; `gap before`: the same over at most 2.5 s just before the move. `db MB`: the leader's database file and its WAL.

| pending | catch up first | db MB | add ms | catch-up ms | change_voters ms | transfer_leader ms | total ms | quorum gap ms | gap before ms | async gap ms | unavailable | unknown |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | yes | 28 | 37 | 1322 | 562 | 244 | 2128 | 513 | 34 | 542 | 0 | 0 |
| 1 | no | 28 | 41 | 0 | 1066 | 256 | 1322 | 650 | 17 | 651 | 0 | 0 |
| 256 | yes | 28 | 42 | 1181 | 559 | 257 | 1997 | 553 | 37 | 641 | 0 | 0 |
| 256 | no | 28 | 42 | 0 | 1050 | 250 | 1300 | 693 | 31 | 782 | 0 | 0 |

(move took 248 s)

total 248 s
