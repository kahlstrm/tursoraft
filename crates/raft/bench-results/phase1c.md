### fdatasync of this orb (append a record, then fdatasync)

| record | threads | samples | p50 ms | p99 ms |
|---|---|---|---|---|
| 200 B | 1 | 2000 | 0.39 | 0.81 |
| 200 B | 3 | 6000 | 0.55 | 1.35 |
| 200 B | 7 | 14000 | 0.75 | 1.42 |
| 4096 B | 1 | 2000 | 0.44 | 0.87 |
| 4096 B | 3 | 6000 | 0.70 | 1.64 |
| 4096 B | 7 | 14000 | 0.88 | 1.57 |
| 65536 B | 1 | 2000 | 0.78 | 1.69 |
| 65536 B | 3 | 6000 | 0.93 | 1.92 |
| 65536 B | 7 | 14000 | 1.47 | 2.88 |

### Voters × round trip (1 writer, batching off, 200 B, at the leader)

| RTT ms | voters | non-voting | size | writers | batching | in flight | at | quorum RTT ms | p50 ms | p99 ms | p50 − quorum RTT | writes/s | entries/write | voter AppendEntries with entries per write | samples | window s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 3 | 0 | 200 | 1 | off | - | leader | 0.0 | 1.17 | 2.39 | 1.17 | 805 | 1.000 | 2.000 | 8050 | 10 |
| 0 | 5 | 0 | 200 | 1 | off | - | leader | 0.0 | 1.43 | 2.63 | 1.43 | 667 | 1.000 | 4.000 | 6669 | 10 |
| 0 | 7 | 0 | 200 | 1 | off | - | leader | 0.0 | 1.35 | 2.25 | 1.35 | 719 | 1.000 | 6.000 | 7193 | 10 |
| 1 | 3 | 0 | 200 | 1 | off | - | leader | 1.0 | 2.09 | 4.49 | 1.09 | 439 | 1.000 | 2.000 | 4386 | 10 |
| 1 | 5 | 0 | 200 | 1 | off | - | leader | 1.0 | 2.34 | 4.98 | 1.34 | 408 | 1.000 | 4.000 | 4078 | 10 |
| 1 | 7 | 0 | 200 | 1 | off | - | leader | 1.0 | 2.29 | 4.16 | 1.29 | 418 | 1.000 | 6.000 | 4177 | 10 |
| 10 | 3 | 0 | 200 | 1 | off | - | leader | 10.0 | 11.95 | 13.26 | 1.95 | 83 | 1.000 | 2.000 | 834 | 10 |
| 10 | 5 | 0 | 200 | 1 | off | - | leader | 10.0 | 12.29 | 14.07 | 2.29 | 82 | 1.000 | 4.000 | 818 | 10 |
| 10 | 7 | 0 | 200 | 1 | off | - | leader | 10.0 | 12.25 | 13.74 | 2.25 | 82 | 1.000 | 6.000 | 815 | 10 |
| 50 | 3 | 0 | 200 | 1 | off | - | leader | 50.0 | 52.16 | 54.00 | 2.16 | 19 | 1.000 | 2.000 | 958 | 50 |
| 50 | 5 | 0 | 200 | 1 | off | - | leader | 50.0 | 52.09 | 53.46 | 2.09 | 19 | 1.000 | 4.000 | 959 | 50 |
| 50 | 7 | 0 | 200 | 1 | off | - | leader | 50.0 | 52.18 | 53.62 | 2.18 | 19 | 1.000 | 6.000 | 959 | 50 |
| 100 | 3 | 0 | 200 | 1 | off | - | leader | 100.0 | 102.38 | 103.99 | 2.38 | 10 | 1.000 | 2.000 | 977 | 100 |
| 100 | 5 | 0 | 200 | 1 | off | - | leader | 100.0 | 102.18 | 103.66 | 2.18 | 10 | 1.000 | 4.000 | 978 | 100 |
| 100 | 7 | 0 | 200 | 1 | off | - | leader | 100.0 | 102.20 | 103.67 | 2.20 | 10 | 1.000 | 6.000 | 978 | 100 |
| 250 | 3 | 0 | 200 | 1 | off | - | leader | 250.0 | 252.10 | 253.58 | 2.10 | 4 | 1.000 | 2.000 | 794 | 200 |
| 250 | 5 | 0 | 200 | 1 | off | - | leader | 250.0 | 252.29 | 254.18 | 2.29 | 4 | 1.000 | 4.000 | 793 | 200 |
| 250 | 7 | 0 | 200 | 1 | off | - | leader | 250.0 | 252.25 | 253.82 | 2.25 | 4 | 1.000 | 6.000 | 793 | 200 |

### Writers × batching × round trip (3 voters, 200 B, at the leader)

| RTT ms | voters | non-voting | size | writers | batching | in flight | at | quorum RTT ms | p50 ms | p99 ms | p50 − quorum RTT | writes/s | entries/write | voter AppendEntries with entries per write | samples | window s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 3 | 0 | 200 | 1 | off | - | leader | 0.0 | 1.17 | 2.39 | 1.17 | 805 | 1.000 | 2.000 | 8050 | 10 |
| 1 | 3 | 0 | 200 | 1 | off | - | leader | 1.0 | 2.09 | 4.49 | 1.09 | 439 | 1.000 | 2.000 | 4386 | 10 |
| 10 | 3 | 0 | 200 | 1 | off | - | leader | 10.0 | 11.95 | 13.26 | 1.95 | 83 | 1.000 | 2.000 | 834 | 10 |
| 50 | 3 | 0 | 200 | 1 | off | - | leader | 50.0 | 52.16 | 54.00 | 2.16 | 19 | 1.000 | 2.000 | 958 | 50 |
| 100 | 3 | 0 | 200 | 1 | off | - | leader | 100.0 | 102.38 | 103.99 | 2.38 | 10 | 1.000 | 2.000 | 977 | 100 |
| 250 | 3 | 0 | 200 | 1 | off | - | leader | 250.0 | 252.10 | 253.58 | 2.10 | 4 | 1.000 | 2.000 | 794 | 200 |
| 0 | 3 | 0 | 200 | 1 | on | 2 | leader | 0.0 | 1.06 | 1.57 | 1.06 | 915 | 1.000 | 2.000 | 9154 | 10 |
| 0 | 3 | 0 | 200 | 16 | off | - | leader | 0.0 | 11.08 | 14.51 | 11.08 | 1443 | 1.000 | 1.983 | 14428 | 10 |
| 0 | 3 | 0 | 200 | 16 | on | 2 | leader | 0.0 | 2.37 | 3.86 | 2.37 | 6571 | 0.188 | 0.376 | 65714 | 10 |
| 0 | 3 | 0 | 200 | 128 | off | - | leader | 0.0 | 38.19 | 349.66 | 38.19 | 2417 | 1.004 | 0.000 | 24170 | 10 LEADER CHANGED |
| 0 | 3 | 0 | 200 | 128 | on | 2 | leader | 0.0 | 2.85 | 4.56 | 2.85 | 43489 | 0.024 | 0.048 | 434889 | 10 |
| 1 | 3 | 0 | 200 | 1 | on | 2 | leader | 1.0 | 2.47 | 4.36 | 1.47 | 390 | 1.000 | 2.000 | 3900 | 10 |
| 1 | 3 | 0 | 200 | 16 | off | - | leader | 1.0 | 10.61 | 15.67 | 9.61 | 1431 | 1.000 | 1.776 | 14312 | 10 |
| 1 | 3 | 0 | 200 | 16 | on | 2 | leader | 1.0 | 3.03 | 5.45 | 2.03 | 5058 | 0.192 | 0.383 | 50581 | 10 |
| 1 | 3 | 0 | 200 | 128 | off | - | leader | 1.0 | 37.25 | 399.04 | 36.25 | 2653 | 1.000 | 0.000 | 26531 | 10 LEADER CHANGED |
| 1 | 3 | 0 | 200 | 128 | on | 2 | leader | 1.0 | 3.36 | 5.73 | 2.36 | 36641 | 0.025 | 0.049 | 366408 | 10 |
| 10 | 3 | 0 | 200 | 1 | on | 2 | leader | 10.0 | 12.21 | 13.25 | 2.21 | 83 | 1.000 | 2.000 | 828 | 10 |
| 10 | 3 | 0 | 200 | 16 | off | - | leader | 10.0 | 11.66 | 14.04 | 1.66 | 1361 | 1.000 | 1.708 | 13612 | 10 |
| 10 | 3 | 0 | 200 | 16 | on | 2 | leader | 10.0 | 17.31 | 24.67 | 7.31 | 910 | 0.189 | 0.378 | 9100 | 10 |
| 10 | 3 | 0 | 200 | 128 | off | - | leader | 10.0 | 49.31 | 505.48 | 39.31 | 1990 | 1.000 | 1.081 | 19905 | 10 |
| 10 | 3 | 0 | 200 | 128 | on | 2 | leader | 10.0 | 17.62 | 24.92 | 7.62 | 7134 | 0.024 | 0.048 | 71344 | 10 |
| 50 | 3 | 0 | 200 | 1 | on | 2 | leader | 50.0 | 52.19 | 53.77 | 2.19 | 19 | 1.000 | 2.000 | 957 | 50 |
| 50 | 3 | 0 | 200 | 16 | off | - | leader | 50.0 | 51.66 | 53.39 | 1.66 | 309 | 1.000 | 1.852 | 3088 | 10 |
| 50 | 3 | 0 | 200 | 16 | on | 2 | leader | 50.0 | 99.62 | 105.03 | 49.62 | 202 | 0.191 | 0.381 | 2024 | 10 |
| 50 | 3 | 0 | 200 | 128 | off | - | leader | 50.0 | 74.52 | 315.13 | 24.52 | 1607 | 1.000 | 1.546 | 16074 | 10 |
| 50 | 3 | 0 | 200 | 128 | on | 2 | leader | 50.0 | 99.17 | 105.76 | 49.17 | 1584 | 0.024 | 0.048 | 15842 | 10 |
| 100 | 3 | 0 | 200 | 1 | on | 2 | leader | 100.0 | 102.23 | 103.61 | 2.23 | 10 | 1.000 | 2.000 | 978 | 100 |
| 100 | 3 | 0 | 200 | 16 | off | - | leader | 100.0 | 101.90 | 103.92 | 1.90 | 157 | 1.000 | 1.846 | 3136 | 20 |
| 100 | 3 | 0 | 200 | 16 | on | 2 | leader | 100.0 | 199.42 | 205.29 | 99.42 | 103 | 0.190 | 0.380 | 2061 | 20 |
| 100 | 3 | 0 | 200 | 128 | off | - | leader | 100.0 | 102.38 | 130.81 | 2.38 | 1179 | 1.000 | 1.793 | 23589 | 20 |
| 100 | 3 | 0 | 200 | 128 | on | 2 | leader | 100.0 | 200.73 | 206.55 | 100.73 | 797 | 0.025 | 0.048 | 15946 | 20 |
| 250 | 3 | 0 | 200 | 1 | on | 2 | leader | 250.0 | 251.99 | 253.23 | 1.99 | 4 | 1.000 | 2.000 | 794 | 200 |
| 250 | 3 | 0 | 200 | 16 | off | - | leader | 250.0 | 252.06 | 253.98 | 2.06 | 64 | 1.000 | 1.788 | 3184 | 50 |
| 250 | 3 | 0 | 200 | 16 | on | 2 | leader | 250.0 | 499.74 | 505.85 | 249.74 | 42 | 0.190 | 0.380 | 2093 | 50 |
| 250 | 3 | 0 | 200 | 128 | off | - | leader | 250.0 | 252.03 | 254.83 | 2.03 | 508 | 1.000 | 1.743 | 25397 | 50 |
| 250 | 3 | 0 | 200 | 128 | on | 2 | leader | 250.0 | 501.59 | 507.11 | 251.59 | 323 | 0.025 | 0.049 | 16132 | 50 |

### Proposal size (3 voters, batching on, at the leader)

| RTT ms | voters | non-voting | size | writers | batching | in flight | at | quorum RTT ms | p50 ms | p99 ms | p50 − quorum RTT | writes/s | entries/write | voter AppendEntries with entries per write | samples | window s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 3 | 0 | 200 | 1 | on | 2 | leader | 0.0 | 1.04 | 1.54 | 1.04 | 934 | 1.000 | 2.000 | 9343 | 10 |
| 0 | 3 | 0 | 200 | 16 | on | 2 | leader | 0.0 | 2.27 | 3.40 | 2.27 | 6934 | 0.188 | 0.376 | 69344 | 10 |
| 0 | 3 | 0 | 4096 | 1 | on | 2 | leader | 0.0 | 1.39 | 1.95 | 1.39 | 706 | 1.000 | 2.000 | 7055 | 10 |
| 0 | 3 | 0 | 4096 | 16 | on | 2 | leader | 0.0 | 2.96 | 4.85 | 2.96 | 5228 | 0.188 | 0.376 | 52278 | 10 |
| 0 | 3 | 0 | 65536 | 1 | on | 2 | leader | 0.0 | 2.25 | 2.87 | 2.25 | 438 | 1.000 | 2.000 | 4382 | 10 |
| 0 | 3 | 0 | 65536 | 16 | on | 2 | leader | 0.0 | 12.85 | 24.95 | 12.85 | 1305 | 0.227 | 0.296 | 13050 | 10 |
| 100 | 3 | 0 | 200 | 1 | on | 2 | leader | 100.0 | 102.18 | 103.81 | 2.18 | 10 | 1.000 | 2.000 | 978 | 100 |
| 100 | 3 | 0 | 200 | 16 | on | 2 | leader | 100.0 | 199.02 | 205.26 | 99.02 | 103 | 0.190 | 0.378 | 2065 | 20 |
| 100 | 3 | 0 | 4096 | 1 | on | 2 | leader | 100.0 | 102.31 | 103.57 | 2.31 | 10 | 1.000 | 2.000 | 977 | 100 |
| 100 | 3 | 0 | 4096 | 16 | on | 2 | leader | 100.0 | 195.65 | 214.11 | 95.65 | 103 | 0.188 | 0.376 | 2065 | 20 |
| 100 | 3 | 0 | 65536 | 1 | on | 2 | leader | 100.0 | 102.77 | 104.25 | 2.77 | 10 | 1.000 | 2.000 | 973 | 100 |
| 100 | 3 | 0 | 65536 | 16 | on | 2 | leader | 100.0 | 169.49 | 228.42 | 69.49 | 99 | 0.190 | 0.381 | 1975 | 20 |

### Non-voting followers (3 voters, 200 B, batching on, at the leader)

| RTT ms | voters | non-voting | size | writers | batching | in flight | at | quorum RTT ms | p50 ms | p99 ms | p50 − quorum RTT | writes/s | entries/write | voter AppendEntries with entries per write | samples | window s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 3 | 0 | 200 | 1 | on | 2 | leader | 10.0 | 11.57 | 13.05 | 1.57 | 86 | 1.000 | 2.000 | 858 | 10 |
| 10 | 3 | 0 | 200 | 16 | on | 2 | leader | 10.0 | 16.79 | 23.91 | 6.79 | 933 | 0.189 | 0.377 | 9330 | 10 |
| 10 | 3 | 2 | 200 | 1 | on | 2 | leader | 10.0 | 11.60 | 12.91 | 1.60 | 86 | 1.000 | 2.000 | 862 | 10 |
| 10 | 3 | 2 | 200 | 16 | on | 2 | leader | 10.0 | 16.85 | 23.62 | 6.85 | 937 | 0.188 | 0.377 | 9373 | 10 |
| 10 | 3 | 4 | 200 | 1 | on | 2 | leader | 10.0 | 11.66 | 13.11 | 1.66 | 86 | 0.999 | 1.998 | 859 | 10 |
| 10 | 3 | 4 | 200 | 16 | on | 2 | leader | 10.0 | 16.78 | 23.85 | 6.78 | 934 | 0.188 | 0.376 | 9338 | 10 |
| 10 | 3 | 10 | 200 | 1 | on | 2 | leader | 10.0 | 11.83 | 13.49 | 1.83 | 84 | 1.000 | 2.000 | 839 | 10 |
| 10 | 3 | 10 | 200 | 16 | on | 2 | leader | 10.0 | 17.64 | 24.55 | 7.64 | 902 | 0.188 | 0.375 | 9019 | 10 |
| 100 | 3 | 0 | 200 | 1 | on | 2 | leader | 100.0 | 101.90 | 103.41 | 1.90 | 10 | 1.000 | 2.000 | 981 | 100 |
| 100 | 3 | 0 | 200 | 16 | on | 2 | leader | 100.0 | 194.73 | 204.63 | 94.73 | 103 | 0.192 | 0.383 | 2055 | 20 |
| 100 | 3 | 2 | 200 | 1 | on | 2 | leader | 100.0 | 101.65 | 102.92 | 1.65 | 10 | 1.000 | 2.000 | 984 | 100 |
| 100 | 3 | 2 | 200 | 16 | on | 2 | leader | 100.0 | 194.53 | 204.37 | 94.53 | 104 | 0.189 | 0.378 | 2085 | 20 |
| 100 | 3 | 4 | 200 | 1 | on | 2 | leader | 100.0 | 101.55 | 102.99 | 1.55 | 10 | 1.000 | 2.000 | 985 | 100 |
| 100 | 3 | 4 | 200 | 16 | on | 2 | leader | 100.0 | 199.34 | 204.16 | 99.34 | 103 | 0.192 | 0.384 | 2053 | 20 |
| 100 | 3 | 10 | 200 | 1 | on | 2 | leader | 100.0 | 101.90 | 103.24 | 1.90 | 10 | 1.000 | 2.000 | 982 | 100 |
| 100 | 3 | 10 | 200 | 16 | on | 2 | leader | 100.0 | 193.67 | 205.45 | 93.67 | 103 | 0.190 | 0.379 | 2061 | 20 |

### Where the write is issued (3 voters, 200 B, batching on)

| RTT ms | voters | non-voting | size | writers | batching | in flight | at | quorum RTT ms | p50 ms | p99 ms | p50 − quorum RTT | writes/s | entries/write | voter AppendEntries with entries per write | samples | window s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 3 | 0 | 200 | 1 | on | 2 | farthest (node 3, 10 ms) | 10.0 | 21.93 | 23.53 | 11.93 | 46 | 1.000 | 2.000 | 914 | 20 |
| 10 | 3 | 0 | 200 | 1 | on | 2 | leader | 10.0 | 11.53 | 12.69 | 1.53 | 86 | 1.000 | 2.000 | 865 | 10 |
| 10 | 3 | 0 | 200 | 16 | on | 2 | farthest (node 3, 10 ms) | 10.0 | 22.67 | 33.49 | 12.67 | 690 | 0.239 | 0.478 | 6895 | 10 |
| 10 | 3 | 0 | 200 | 16 | on | 2 | leader | 10.0 | 16.75 | 23.28 | 6.75 | 942 | 0.188 | 0.377 | 9420 | 10 |
| 100 | 3 | 0 | 200 | 1 | on | 2 | farthest (node 3, 100 ms) | 100.0 | 202.50 | 204.76 | 102.50 | 5 | 1.000 | 2.000 | 988 | 200 |
| 100 | 3 | 0 | 200 | 1 | on | 2 | leader | 100.0 | 101.71 | 103.04 | 1.71 | 10 | 1.000 | 2.000 | 983 | 100 |
| 100 | 3 | 0 | 200 | 16 | on | 2 | farthest (node 3, 100 ms) | 100.0 | 204.22 | 305.74 | 104.22 | 76 | 0.228 | 0.455 | 3033 | 40 |
| 100 | 3 | 0 | 200 | 16 | on | 2 | leader | 100.0 | 194.73 | 205.67 | 94.73 | 104 | 0.188 | 0.377 | 2082 | 20 |

### Batcher limit on entries in flight (3 voters, 200 B)

| RTT ms | voters | non-voting | size | writers | batching | in flight | at | quorum RTT ms | p50 ms | p99 ms | p50 − quorum RTT | writes/s | entries/write | voter AppendEntries with entries per write | samples | window s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 3 | 0 | 200 | 16 | on | 1 | leader | 10.0 | 23.60 | 25.92 | 13.60 | 677 | 0.125 | 0.250 | 6768 | 10 |
| 10 | 3 | 0 | 200 | 16 | on | 8 | leader | 10.0 | 12.52 | 16.74 | 2.52 | 1249 | 0.566 | 1.111 | 12494 | 10 |
| 10 | 3 | 0 | 200 | 16 | on | 64 | leader | 10.0 | 11.56 | 14.34 | 1.56 | 1371 | 0.992 | 1.619 | 13711 | 10 |
| 10 | 3 | 0 | 200 | 128 | on | 1 | leader | 10.0 | 23.64 | 26.01 | 13.64 | 5402 | 0.016 | 0.031 | 54021 | 10 |
| 10 | 3 | 0 | 200 | 128 | on | 8 | leader | 10.0 | 13.19 | 18.08 | 3.19 | 9493 | 0.072 | 0.140 | 94934 | 10 |
| 10 | 3 | 0 | 200 | 128 | on | 64 | leader | 10.0 | 43.24 | 110.04 | 33.24 | 2953 | 0.510 | 0.926 | 29529 | 10 |
| 100 | 3 | 0 | 200 | 16 | on | 1 | leader | 100.0 | 204.40 | 206.73 | 104.40 | 78 | 0.125 | 0.250 | 1569 | 20 |
| 100 | 3 | 0 | 200 | 16 | on | 8 | leader | 100.0 | 103.41 | 199.09 | 3.41 | 138 | 0.568 | 1.104 | 2762 | 20 |
| 100 | 3 | 0 | 200 | 16 | on | 64 | leader | 100.0 | 102.02 | 104.09 | 2.02 | 157 | 0.995 | 1.615 | 3134 | 20 |
| 100 | 3 | 0 | 200 | 128 | on | 1 | leader | 100.0 | 204.29 | 206.28 | 104.29 | 627 | 0.016 | 0.031 | 12545 | 20 |
| 100 | 3 | 0 | 200 | 128 | on | 8 | leader | 100.0 | 105.17 | 197.34 | 5.17 | 1085 | 0.072 | 0.138 | 21698 | 20 |
| 100 | 3 | 0 | 200 | 128 | on | 64 | leader | 100.0 | 102.75 | 112.81 | 2.75 | 1236 | 0.509 | 0.992 | 24730 | 20 |

### Delay in the node's network layer vs. a TCP proxy (3 voters, 200 B, batching on)

| RTT ms | voters | non-voting | size | writers | batching | in flight | at | quorum RTT ms | p50 ms | p99 ms | p50 − quorum RTT | writes/s | entries/write | voter AppendEntries with entries per write | samples | window s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 3 | 0 | 200 | 1 | on | 2 | leader | 10.0 | 12.35 | 14.31 | 2.35 | 81 | 1.000 | 2.000 | 808 | 10 |
| 10 (proxy) | 3 | 0 | 200 | 1 | on | 2 | leader | 10.0 | 12.14 | 13.23 | 2.14 | 82 | 1.000 | 2.000 | 823 | 10 |
| 10 | 3 | 0 | 200 | 16 | on | 2 | leader | 10.0 | 17.58 | 24.64 | 7.58 | 900 | 0.190 | 0.379 | 9000 | 10 |
| 10 (proxy) | 3 | 0 | 200 | 16 | on | 2 | leader | 10.0 | 17.42 | 25.33 | 7.42 | 894 | 0.192 | 0.383 | 8942 | 10 |
| 100 | 3 | 0 | 200 | 1 | on | 2 | leader | 100.0 | 102.29 | 103.62 | 2.29 | 10 | 1.000 | 2.000 | 978 | 100 |
| 100 (proxy) | 3 | 0 | 200 | 1 | on | 2 | leader | 100.0 | 102.32 | 103.89 | 2.32 | 10 | 1.000 | 2.000 | 977 | 100 |
| 100 | 3 | 0 | 200 | 16 | on | 2 | leader | 100.0 | 199.48 | 205.18 | 99.48 | 102 | 0.192 | 0.383 | 2046 | 20 |
| 100 (proxy) | 3 | 0 | 200 | 16 | on | 2 | leader | 100.0 | 202.69 | 206.11 | 102.69 | 98 | 0.200 | 0.398 | 1961 | 20 |

### AWS regions (cloudping.co P50; leader in Frankfurt, 200 B, batching on)

| RTT ms | voters | non-voting | size | writers | batching | in flight | at | quorum RTT ms | p50 ms | p99 ms | p50 − quorum RTT | writes/s | entries/write | voter AppendEntries with entries per write | samples | window s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| aws_a | 3 | 4 | 200 | 1 | on | 2 | farthest (node 7, 255.7 ms) | 14.4 | 273.26 | 276.09 | 258.86 | 4 | 1.000 | 2.000 | 732 | 200 |
| aws_a | 3 | 4 | 200 | 1 | on | 2 | leader | 14.4 | 17.23 | 20.28 | 2.83 | 58 | 1.000 | 2.000 | 836 | 14 |
| aws_a | 3 | 4 | 200 | 16 | on | 2 | farthest (node 7, 255.7 ms) | 14.4 | 273.42 | 277.53 | 259.02 | 58 | 0.946 | 1.447 | 3160 | 54 |
| aws_a | 3 | 4 | 200 | 16 | on | 2 | leader | 14.4 | 26.21 | 36.53 | 11.81 | 605 | 0.189 | 0.377 | 6047 | 10 |
| aws_b | 3 | 4 | 200 | 1 | on | 2 | farthest (node 7, 255.7 ms) | 91.5 | 350.10 | 352.37 | 258.60 | 3 | 1.000 | 2.000 | 571 | 200 |
| aws_b | 3 | 4 | 200 | 1 | on | 2 | leader | 91.5 | 93.85 | 95.87 | 2.35 | 11 | 1.000 | 2.000 | 974 | 92 |
| aws_b | 3 | 4 | 200 | 16 | on | 2 | farthest (node 7, 255.7 ms) | 91.5 | 373.74 | 415.37 | 282.24 | 43 | 0.492 | 0.984 | 2582 | 60 |
| aws_b | 3 | 4 | 200 | 16 | on | 2 | leader | 91.5 | 177.82 | 188.65 | 86.32 | 113 | 0.189 | 0.377 | 2068 | 18 |
| aws_c | 3 | 6 | 200 | 1 | on | 2 | farthest (node 7, 255.7 ms) | 1.0 | 258.97 | 261.08 | 257.97 | 4 | 0.999 | 1.997 | 773 | 200 |
| aws_c | 3 | 6 | 200 | 1 | on | 2 | leader | 1.0 | 2.09 | 4.24 | 1.09 | 447 | 1.000 | 2.000 | 4472 | 10 |
| aws_c | 3 | 6 | 200 | 16 | on | 2 | farthest (node 7, 255.7 ms) | 1.0 | 259.51 | 264.26 | 258.51 | 62 | 0.758 | 1.056 | 3168 | 51 |
| aws_c | 3 | 6 | 200 | 16 | on | 2 | leader | 1.0 | 4.64 | 9.25 | 3.64 | 3311 | 0.191 | 0.378 | 33114 | 10 |

### Follower lag behind the leader's log (sampled every 5 ms during the window)

| cell | follower | voter | RTT to leader ms | applied lag entries mean / max | stored lag entries mean / max | applied lag ms p50 / p99 / max |
|---|---|---|---|---|---|---|
| `aws_a/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=farthest` | 2 Ireland | yes | 21.6 | 0.1 / 1 | 0.1 / 1 | 0.0 / 5.4 / 10.8 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=farthest` | 3 London | yes | 14.4 | 0.1 / 1 | 0.1 / 1 | 0.0 / 0.0 / 5.8 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=farthest` | 4 N. Virginia | no | 91.5 | 0.2 / 1 | 0.3 / 1 | 0.0 / 40.2 / 45.6 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=farthest` | 5 Oregon | no | 146.3 | 0.3 / 1 | 0.5 / 1 | 0.0 / 69.6 / 71.1 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=farthest` | 6 Singapore | no | 164.8 | 0.4 / 1 | 0.6 / 1 | 0.0 / 79.1 / 81.0 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=farthest` | 7 Sydney | no | 255.7 | 0.5 / 1 | 0.9 / 1 | 0.0 / 124.4 / 126.1 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=leader` | 2 Ireland | yes | 21.6 | 1.6 / 2 | 1.3 / 2 | 0.0 / 10.6 / 11.1 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=leader` | 3 London | yes | 14.4 | 1.4 / 2 | 0.9 / 1 | 0.0 / 5.8 / 8.6 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=leader` | 4 N. Virginia | no | 91.5 | 3.6 / 4 | 5.4 / 6 | 35.0 / 45.4 / 46.2 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=leader` | 5 Oregon | no | 146.3 | 5.2 / 6 | 8.6 / 10 | 61.0 / 70.8 / 76.4 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=leader` | 6 Singapore | no | 164.8 | 5.7 / 7 | 9.6 / 11 | 70.4 / 80.8 / 81.9 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=leader` | 7 Sydney | no | 255.7 | 8.4 / 9 | 14.9 / 16 | 115.7 / 125.9 / 130.9 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=farthest` | 2 Ireland | yes | 21.6 | 1.5 / 4 | 1.3 / 4 | 0.0 / 10.1 / 11.2 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=farthest` | 3 London | yes | 14.4 | 1.3 / 4 | 0.9 / 2 | 0.0 / 5.6 / 9.3 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=farthest` | 4 N. Virginia | no | 91.5 | 3.5 / 8 | 5.2 / 12 | 29.5 / 45.0 / 48.5 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=farthest` | 5 Oregon | no | 146.3 | 5.0 / 11 | 8.2 / 16 | 55.2 / 70.7 / 74.2 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=farthest` | 6 Singapore | no | 164.8 | 5.5 / 12 | 9.2 / 16 | 65.0 / 80.6 / 82.0 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=farthest` | 7 Sydney | no | 255.7 | 8.0 / 16 | 14.2 / 16 | 110.2 / 125.6 / 128.6 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=leader` | 2 Ireland | yes | 21.6 | 3.2 / 4 | 2.7 / 4 | 4.6 / 10.8 / 19.4 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=leader` | 3 London | yes | 14.4 | 2.8 / 4 | 1.9 / 2 | 0.0 / 6.0 / 7.6 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=leader` | 4 N. Virginia | no | 91.5 | 7.2 / 8 | 10.7 / 12 | 39.4 / 45.7 / 48.9 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=leader` | 5 Oregon | no | 146.3 | 10.4 / 12 | 17.0 / 19 | 65.2 / 71.0 / 75.2 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=leader` | 6 Singapore | no | 164.8 | 11.4 / 13 | 19.1 / 21 | 75.0 / 81.0 / 89.2 |
| `aws_a/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=leader` | 7 Sydney | no | 255.7 | 16.6 / 18 | 29.5 / 32 | 120.0 / 126.0 / 139.5 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=farthest` | 2 Ireland | no | 21.6 | 0.3 / 1 | 0.1 / 1 | 0.0 / 4.6 / 10.2 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=farthest` | 3 London | no | 14.4 | 0.3 / 1 | 0.0 / 1 | 0.0 / 0.0 / 6.2 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=farthest` | 4 N. Virginia | yes | 91.5 | 0.4 / 1 | 0.3 / 1 | 0.0 / 39.9 / 45.7 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=farthest` | 5 Oregon | no | 146.3 | 0.5 / 1 | 0.4 / 1 | 0.0 / 65.6 / 71.0 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=farthest` | 6 Singapore | yes | 164.8 | 0.5 / 1 | 0.5 / 1 | 0.0 / 75.3 / 80.8 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=farthest` | 7 Sydney | no | 255.7 | 0.6 / 1 | 0.7 / 1 | 0.0 / 120.7 / 130.1 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=leader` | 2 Ireland | no | 21.6 | 1.1 / 2 | 0.2 / 1 | 0.0 / 9.4 / 11.6 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=leader` | 3 London | no | 14.4 | 1.1 / 2 | 0.2 / 1 | 0.0 / 5.4 / 6.2 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=leader` | 4 N. Virginia | yes | 91.5 | 1.5 / 2 | 1.0 / 1 | 0.0 / 44.5 / 46.0 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=leader` | 5 Oregon | no | 146.3 | 1.8 / 2 | 1.6 / 2 | 24.7 / 70.2 / 71.3 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=leader` | 6 Singapore | yes | 164.8 | 1.9 / 2 | 1.8 / 2 | 34.7 / 80.1 / 84.6 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=1/batching=on/at=leader` | 7 Sydney | no | 255.7 | 2.4 / 3 | 2.7 / 3 | 79.7 / 125.2 / 126.7 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=farthest` | 2 Ireland | no | 21.6 | 2.2 / 3 | 0.5 / 2 | 0.0 / 9.8 / 11.0 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=farthest` | 3 London | no | 14.4 | 2.1 / 3 | 0.3 / 1 | 0.0 / 5.4 / 6.5 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=farthest` | 4 N. Virginia | yes | 91.5 | 2.9 / 4 | 2.0 / 2 | 19.8 / 44.5 / 46.0 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=farthest` | 5 Oregon | no | 146.3 | 3.5 / 4 | 3.1 / 4 | 45.6 / 70.3 / 71.4 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=farthest` | 6 Singapore | yes | 164.8 | 3.7 / 4 | 3.5 / 4 | 55.3 / 80.2 / 83.9 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=farthest` | 7 Sydney | no | 255.7 | 4.7 / 6 | 5.4 / 6 | 100.9 / 125.3 / 130.2 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=leader` | 2 Ireland | no | 21.6 | 2.2 / 4 | 0.5 / 2 | 0.0 / 10.1 / 11.1 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=leader` | 3 London | no | 14.4 | 2.2 / 4 | 0.3 / 2 | 0.0 / 5.5 / 6.2 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=leader` | 4 N. Virginia | yes | 91.5 | 3.0 / 4 | 2.0 / 2 | 0.0 / 45.1 / 45.9 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=leader` | 5 Oregon | no | 146.3 | 3.6 / 4 | 3.2 / 4 | 25.7 / 70.5 / 71.9 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=leader` | 6 Singapore | yes | 164.8 | 3.8 / 4 | 3.6 / 4 | 35.7 / 80.4 / 81.3 |
| `aws_b/voters=3/learners=4/aws/size=200/writers=16/batching=on/at=leader` | 7 Sydney | no | 255.7 | 4.7 / 6 | 5.5 / 6 | 80.9 / 125.5 / 126.5 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=farthest` | 2 Ireland | no | 21.6 | 0.1 / 1 | 0.1 / 1 | 0.0 / 5.5 / 10.7 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=farthest` | 3 London | no | 14.4 | 0.0 / 1 | 0.1 / 1 | 0.0 / 3.5 / 6.3 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=farthest` | 4 N. Virginia | no | 91.5 | 0.2 / 1 | 0.4 / 1 | 0.0 / 40.2 / 49.1 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=farthest` | 5 Oregon | no | 146.3 | 0.3 / 1 | 0.6 / 1 | 0.0 / 69.5 / 74.5 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=farthest` | 6 Singapore | no | 164.8 | 0.3 / 1 | 0.6 / 1 | 0.0 / 79.1 / 81.1 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=farthest` | 7 Sydney | no | 255.7 | 0.5 / 1 | 1.0 / 2 | 0.0 / 124.5 / 126.1 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=farthest` | 8 Frankfurt | yes | 1.0 | 0.0 / 1 | 0.0 / 1 | 0.0 / 0.0 / 0.0 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=farthest` | 9 Frankfurt | yes | 1.0 | 0.0 / 1 | 0.0 / 1 | 0.0 / 0.0 / 0.0 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=leader` | 2 Ireland | no | 21.6 | 5.4 / 9 | 9.8 / 14 | 5.5 / 10.7 / 11.0 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=leader` | 3 London | no | 14.4 | 3.9 / 7 | 6.6 / 10 | 4.8 / 6.1 / 6.3 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=leader` | 4 N. Virginia | no | 91.5 | 21.1 / 28 | 41.2 / 50 | 40.4 / 45.7 / 46.2 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=leader` | 5 Oregon | no | 146.3 | 33.4 / 42 | 65.7 / 79 | 70.0 / 71.1 / 71.4 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=leader` | 6 Singapore | no | 164.8 | 37.5 / 47 | 74.0 / 88 | 79.8 / 81.1 / 81.3 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=leader` | 7 Sydney | no | 255.7 | 57.9 / 70 | 114.8 / 132 | 124.9 / 126.1 / 130.6 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=leader` | 8 Frankfurt | yes | 1.0 | 0.9 / 2 | 0.6 / 2 | 0.0 / 0.0 / 0.0 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=1/batching=on/at=leader` | 9 Frankfurt | yes | 1.0 | 0.9 / 2 | 0.6 / 2 | 0.0 / 0.0 / 0.0 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=farthest` | 2 Ireland | no | 21.6 | 0.6 / 10 | 1.1 / 13 | 0.0 / 10.2 / 11.1 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=farthest` | 3 London | no | 14.4 | 0.5 / 8 | 0.7 / 10 | 0.0 / 5.5 / 7.3 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=farthest` | 4 N. Virginia | no | 91.5 | 2.3 / 16 | 4.4 / 16 | 0.0 / 45.0 / 48.8 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=farthest` | 5 Oregon | no | 146.3 | 3.5 / 16 | 6.9 / 16 | 0.0 / 70.5 / 75.1 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=farthest` | 6 Singapore | no | 164.8 | 4.0 / 16 | 7.8 / 16 | 0.0 / 80.4 / 81.5 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=farthest` | 7 Sydney | no | 255.7 | 6.1 / 16 | 12.0 / 17 | 44.2 / 125.4 / 129.3 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=farthest` | 8 Frankfurt | yes | 1.0 | 0.1 / 4 | 0.1 / 3 | 0.0 / 0.0 / 0.0 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=farthest` | 9 Frankfurt | yes | 1.0 | 0.2 / 4 | 0.1 / 2 | 0.0 / 0.0 / 0.0 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=leader` | 2 Ireland | no | 21.6 | 8.8 / 13 | 15.2 / 21 | 8.9 / 10.9 / 15.7 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=leader` | 3 London | no | 14.4 | 6.5 / 10 | 10.6 / 15 | 4.9 / 6.1 / 11.5 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=leader` | 4 N. Virginia | no | 91.5 | 30.9 / 40 | 59.6 / 74 | 43.8 / 45.8 / 50.4 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=leader` | 5 Oregon | no | 146.3 | 48.1 / 62 | 94.7 / 115 | 70.0 / 71.2 / 75.7 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=leader` | 6 Singapore | no | 164.8 | 54.0 / 68 | 106.3 / 129 | 79.9 / 81.2 / 85.4 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=leader` | 7 Sydney | no | 255.7 | 82.7 / 101 | 164.0 / 195 | 125.0 / 126.2 / 130.7 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=leader` | 8 Frankfurt | yes | 1.0 | 2.3 / 7 | 1.9 / 5 | 0.0 / 0.0 / 4.9 |
| `aws_c/voters=3/learners=6/aws/size=200/writers=16/batching=on/at=leader` | 9 Frankfurt | yes | 1.0 | 2.4 / 5 | 1.9 / 4 | 0.0 / 0.0 / 5.1 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=1/batching=on/at=leader` | 2  | yes | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.3 / 8.0 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=1/batching=on/at=leader` | 3  | yes | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.4 / 8.0 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=1/batching=on/at=leader` | 4  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.4 / 8.0 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=1/batching=on/at=leader` | 5  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.3 / 8.0 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=1/batching=on/at=leader` | 6  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.4 / 8.0 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=1/batching=on/at=leader` | 7  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.4 / 8.0 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=1/batching=on/at=leader` | 8  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.4 / 8.0 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=1/batching=on/at=leader` | 9  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.4 / 8.0 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=1/batching=on/at=leader` | 10  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.4 / 8.0 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=1/batching=on/at=leader` | 11  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.4 / 8.0 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=1/batching=on/at=leader` | 12  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.4 / 8.0 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=1/batching=on/at=leader` | 13  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.4 / 8.0 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=16/batching=on/at=leader` | 2  | yes | 10.0 | 2.8 / 4 | 2.0 / 3 | 0.0 / 5.5 / 6.3 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=16/batching=on/at=leader` | 3  | yes | 10.0 | 2.8 / 4 | 2.0 / 3 | 0.0 / 5.4 / 6.3 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=16/batching=on/at=leader` | 4  | no | 10.0 | 2.9 / 4 | 2.0 / 3 | 0.0 / 5.5 / 6.3 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=16/batching=on/at=leader` | 5  | no | 10.0 | 2.8 / 4 | 2.0 / 3 | 0.0 / 5.4 / 6.1 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=16/batching=on/at=leader` | 6  | no | 10.0 | 2.8 / 4 | 1.9 / 3 | 0.0 / 5.5 / 6.3 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=16/batching=on/at=leader` | 7  | no | 10.0 | 2.8 / 4 | 1.9 / 4 | 0.0 / 5.4 / 6.1 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=16/batching=on/at=leader` | 8  | no | 10.0 | 2.8 / 4 | 2.0 / 3 | 0.0 / 5.6 / 10.1 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=16/batching=on/at=leader` | 9  | no | 10.0 | 2.8 / 5 | 2.0 / 3 | 0.0 / 5.5 / 10.7 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=16/batching=on/at=leader` | 10  | no | 10.0 | 2.8 / 4 | 2.0 / 8 | 0.0 / 5.6 / 10.1 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=16/batching=on/at=leader` | 11  | no | 10.0 | 2.8 / 4 | 2.0 / 3 | 0.0 / 5.5 / 10.1 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=16/batching=on/at=leader` | 12  | no | 10.0 | 2.8 / 4 | 2.0 / 3 | 0.0 / 5.5 / 10.1 |
| `learners/voters=3/learners=10/rtt=10/size=200/writers=16/batching=on/at=leader` | 13  | no | 10.0 | 2.8 / 5 | 1.9 / 3 | 0.0 / 5.5 / 6.1 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=1/batching=on/at=leader` | 2  | yes | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.6 / 51.2 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=1/batching=on/at=leader` | 3  | yes | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.6 / 50.8 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=1/batching=on/at=leader` | 4  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.5 / 50.9 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=1/batching=on/at=leader` | 5  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.6 / 50.9 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=1/batching=on/at=leader` | 6  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.6 / 50.8 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=1/batching=on/at=leader` | 7  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.6 / 51.2 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=1/batching=on/at=leader` | 8  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.6 / 51.3 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=1/batching=on/at=leader` | 9  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.6 / 50.9 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=1/batching=on/at=leader` | 10  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.6 / 51.3 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=1/batching=on/at=leader` | 11  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.5 / 51.1 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=1/batching=on/at=leader` | 12  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.5 / 50.8 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=1/batching=on/at=leader` | 13  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.6 / 50.8 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=16/batching=on/at=leader` | 2  | yes | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 49.0 / 51.1 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=16/batching=on/at=leader` | 3  | yes | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 46.0 / 50.7 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=16/batching=on/at=leader` | 4  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 46.0 / 51.0 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=16/batching=on/at=leader` | 5  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 46.0 / 50.7 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=16/batching=on/at=leader` | 6  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 46.0 / 51.0 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=16/batching=on/at=leader` | 7  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 46.1 / 51.0 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=16/batching=on/at=leader` | 8  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 46.1 / 51.1 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=16/batching=on/at=leader` | 9  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 49.0 / 55.2 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=16/batching=on/at=leader` | 10  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 46.0 / 51.1 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=16/batching=on/at=leader` | 11  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 46.0 / 51.0 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=16/batching=on/at=leader` | 12  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 46.0 / 51.1 |
| `learners/voters=3/learners=10/rtt=100/size=200/writers=16/batching=on/at=leader` | 13  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 48.7 / 51.1 |
| `learners/voters=3/learners=2/rtt=10/size=200/writers=1/batching=on/at=leader` | 2  | yes | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.3 / 6.3 |
| `learners/voters=3/learners=2/rtt=10/size=200/writers=1/batching=on/at=leader` | 3  | yes | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.3 / 6.3 |
| `learners/voters=3/learners=2/rtt=10/size=200/writers=1/batching=on/at=leader` | 4  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.3 / 6.3 |
| `learners/voters=3/learners=2/rtt=10/size=200/writers=1/batching=on/at=leader` | 5  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.3 / 6.3 |
| `learners/voters=3/learners=2/rtt=10/size=200/writers=16/batching=on/at=leader` | 2  | yes | 10.0 | 2.9 / 4 | 2.0 / 4 | 0.0 / 5.3 / 5.7 |
| `learners/voters=3/learners=2/rtt=10/size=200/writers=16/batching=on/at=leader` | 3  | yes | 10.0 | 2.9 / 4 | 2.0 / 4 | 0.0 / 5.4 / 10.0 |
| `learners/voters=3/learners=2/rtt=10/size=200/writers=16/batching=on/at=leader` | 4  | no | 10.0 | 2.9 / 4 | 2.0 / 4 | 0.0 / 5.4 / 10.0 |
| `learners/voters=3/learners=2/rtt=10/size=200/writers=16/batching=on/at=leader` | 5  | no | 10.0 | 2.9 / 4 | 2.0 / 4 | 0.0 / 5.3 / 5.8 |
| `learners/voters=3/learners=2/rtt=100/size=200/writers=1/batching=on/at=leader` | 2  | yes | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.5 / 50.7 |
| `learners/voters=3/learners=2/rtt=100/size=200/writers=1/batching=on/at=leader` | 3  | yes | 100.0 | 1.5 / 2 | 1.0 / 1 | 0.0 / 45.5 / 50.7 |
| `learners/voters=3/learners=2/rtt=100/size=200/writers=1/batching=on/at=leader` | 4  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.5 / 50.7 |
| `learners/voters=3/learners=2/rtt=100/size=200/writers=1/batching=on/at=leader` | 5  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.5 / 50.7 |
| `learners/voters=3/learners=2/rtt=100/size=200/writers=16/batching=on/at=leader` | 2  | yes | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 45.8 / 50.4 |
| `learners/voters=3/learners=2/rtt=100/size=200/writers=16/batching=on/at=leader` | 3  | yes | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 45.8 / 50.4 |
| `learners/voters=3/learners=2/rtt=100/size=200/writers=16/batching=on/at=leader` | 4  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 45.8 / 50.4 |
| `learners/voters=3/learners=2/rtt=100/size=200/writers=16/batching=on/at=leader` | 5  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 45.9 / 50.5 |
| `learners/voters=3/learners=4/rtt=10/size=200/writers=1/batching=on/at=leader` | 2  | yes | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.3 / 5.9 |
| `learners/voters=3/learners=4/rtt=10/size=200/writers=1/batching=on/at=leader` | 3  | yes | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.4 / 5.9 |
| `learners/voters=3/learners=4/rtt=10/size=200/writers=1/batching=on/at=leader` | 4  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.3 / 5.9 |
| `learners/voters=3/learners=4/rtt=10/size=200/writers=1/batching=on/at=leader` | 5  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.3 / 5.9 |
| `learners/voters=3/learners=4/rtt=10/size=200/writers=1/batching=on/at=leader` | 6  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.3 / 5.8 |
| `learners/voters=3/learners=4/rtt=10/size=200/writers=1/batching=on/at=leader` | 7  | no | 10.0 | 1.4 / 2 | 0.9 / 2 | 0.0 / 5.3 / 5.7 |
| `learners/voters=3/learners=4/rtt=10/size=200/writers=16/batching=on/at=leader` | 2  | yes | 10.0 | 2.9 / 4 | 2.0 / 3 | 0.0 / 5.3 / 6.5 |
| `learners/voters=3/learners=4/rtt=10/size=200/writers=16/batching=on/at=leader` | 3  | yes | 10.0 | 2.9 / 4 | 2.0 / 3 | 0.0 / 5.3 / 6.5 |
| `learners/voters=3/learners=4/rtt=10/size=200/writers=16/batching=on/at=leader` | 4  | no | 10.0 | 2.9 / 4 | 2.0 / 3 | 0.0 / 5.3 / 5.9 |
| `learners/voters=3/learners=4/rtt=10/size=200/writers=16/batching=on/at=leader` | 5  | no | 10.0 | 2.9 / 4 | 2.0 / 3 | 0.0 / 5.4 / 6.5 |
| `learners/voters=3/learners=4/rtt=10/size=200/writers=16/batching=on/at=leader` | 6  | no | 10.0 | 2.9 / 4 | 2.0 / 3 | 0.0 / 5.3 / 6.0 |
| `learners/voters=3/learners=4/rtt=10/size=200/writers=16/batching=on/at=leader` | 7  | no | 10.0 | 2.9 / 4 | 2.0 / 3 | 0.0 / 5.3 / 6.6 |
| `learners/voters=3/learners=4/rtt=100/size=200/writers=1/batching=on/at=leader` | 2  | yes | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.5 / 50.8 |
| `learners/voters=3/learners=4/rtt=100/size=200/writers=1/batching=on/at=leader` | 3  | yes | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.5 / 50.8 |
| `learners/voters=3/learners=4/rtt=100/size=200/writers=1/batching=on/at=leader` | 4  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.5 / 51.4 |
| `learners/voters=3/learners=4/rtt=100/size=200/writers=1/batching=on/at=leader` | 5  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.5 / 50.8 |
| `learners/voters=3/learners=4/rtt=100/size=200/writers=1/batching=on/at=leader` | 6  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.5 / 51.0 |
| `learners/voters=3/learners=4/rtt=100/size=200/writers=1/batching=on/at=leader` | 7  | no | 100.0 | 1.5 / 2 | 1.0 / 2 | 0.0 / 45.5 / 51.4 |
| `learners/voters=3/learners=4/rtt=100/size=200/writers=16/batching=on/at=leader` | 2  | yes | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 45.9 / 50.8 |
| `learners/voters=3/learners=4/rtt=100/size=200/writers=16/batching=on/at=leader` | 3  | yes | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 45.9 / 50.7 |
| `learners/voters=3/learners=4/rtt=100/size=200/writers=16/batching=on/at=leader` | 4  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 45.9 / 50.9 |
| `learners/voters=3/learners=4/rtt=100/size=200/writers=16/batching=on/at=leader` | 5  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 46.0 / 50.8 |
| `learners/voters=3/learners=4/rtt=100/size=200/writers=16/batching=on/at=leader` | 6  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 46.0 / 50.8 |
| `learners/voters=3/learners=4/rtt=100/size=200/writers=16/batching=on/at=leader` | 7  | no | 100.0 | 3.0 / 4 | 2.0 / 3 | 0.0 / 46.0 / 50.7 |

### Failover: SIGKILL of the leader to the next acknowledged write (3 voters, timeouts from for_max_rtt)

| RTT ms | heartbeat ms | election timeout ms | trials | kill → new leader seen p50 ms | kill → ack min | p50 | max ms |
|---|---|---|---|---|---|---|---|
| 0 | 50 | 150–300 | 10 | 501 | 466 | 509 | 549 |
| 1 | 50 | 150–300 | 10 | 544 | 466 | 551 | 594 |
| 10 | 50 | 150–300 | 10 | 578 | 555 | 611 | 645 |
| 50 | 100 | 300–600 | 10 | 1087 | 1062 | 1199 | 1305 |
| 100 | 200 | 600–1200 | 10 | 2159 | 2198 | 2369 | 2622 |
| 250 | 500 | 1500–3000 | 10 | 5452 | 5263 | 5965 | 6433 |

### Leader transfer (3 voters, 8 writers at the old leader; ms from the call, p50 of the trials)

| RTT ms | in flight | trials | target is leader | old leader sees it | write issued at call at new leader | write issued when seen, at new | at old | longest gap between acks |
|---|---|---|---|---|---|---|---|---|
| 0 | 2 | 6 | 6 | 7 | 13 | 9 | 9 | 12 |
| 0 | 64 | 6 | 9 | 13 | 13 | 16 | 16 | 7 |
| 10 | 2 | 6 | 21 | 28 | 55 | 54 | 61 | 60 |
| 10 | 64 | 6 | 21 | 33 | 56 | 55 | 55 | 64 |
| 100 | 2 | 6 | 157 | 213 | 466 | 463 | 513 | 380 |
| 100 | 64 | 6 | 156 | 211 | 371 | 370 | 414 | 478 |

