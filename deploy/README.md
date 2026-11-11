# Deploying the store

`regions.json` describes the cluster: one machine per node, the machine type,
the ports, the WireGuard network and the Raft timeouts. Three scripts use it:

| Script | What it does | Needs |
| --- | --- | --- |
| `local.sh` | Runs every node on this machine, with the regions' round trips injected by tursoraft's delay table, then the load and the checker | Rust, `jq`, `curl` |
| `hetzner.sh` | Creates and deletes the machines on Hetzner Cloud. The only provider-specific part | `hcloud`, `jq`, `HCLOUD_TOKEN` |
| `cluster.sh` | Everything once the machines exist: WireGuard, the binary as a systemd service, forming the cluster, the load, the checker | SSH as root to each machine, `jq`, Rust to build the binary |

## On one machine

```bash
deploy/local.sh --reserve counter --duration 60    # or units, or leader
```

It prints, per instance and request kind, the count, the median and
99th-percentile latency and the outcomes, then the checker's report, and
exits with status 1 if the checker found a violation. `--break oversell` or
`--break ack-rejected` starts every instance with a deliberate defect, to see
the checker fail.

## On Hetzner Cloud

```bash
export HCLOUD_TOKEN=...              # a read-write token of an empty project
deploy/hetzner.sh plan               # the machines and their hourly price
deploy/hetzner.sh up                 # asks before it creates anything; writes deploy/state/hosts
deploy/cluster.sh install            # builds the release binary, installs WireGuard and the service
deploy/cluster.sh rtt                # the measured round trips between the machines
deploy/cluster.sh init --reserve counter
deploy/cluster.sh load --duration 300 --shoppers 16 --flash-at 60 --flash-shoppers 8
deploy/cluster.sh reset              # empty every instance; then init with another design
deploy/hetzner.sh list               # what runs, and what it has cost so far
deploy/hetzner.sh down               # delete everything with the label tursoraft-shop=1
```

`load` runs the load generator on every machine against its own instance at
the same time, copies the results to `deploy/state/results/<time>/`, and runs
the checker on the first voter, which reaches every node's admin port over
WireGuard. The shop is public on port 8080 of each machine
(`http://<ip>:8080/`, `/products/0` for the flash-sale product with live
stock).

`init` adds every node to the cluster, so every instance must be running
then; `install` starts them all. A node whose data directory is deleted
cannot rejoin under its old id (the other nodes refuse it), so to start over
use `reset`, which wipes every node, not one machine alone.

The machines need nothing but a Debian image with SSH. The binary is built
where the scripts run and copied over, so that machine must have the same or
an older glibc than the image (Debian 13 has glibc 2.41).

## Network and security

tursoraft's Raft and client traffic is plain TCP with no encryption and no
authentication. The nodes therefore listen on WireGuard addresses
(`10.77.0.ID`), and the provider's firewall admits only SSH, WireGuard and the
shop's HTTP port. The admin API (forming the cluster, setting up the store,
exporting all data) listens on the WireGuard address only. The shop's HTTP
API has no authentication either: anyone who finds the address can fill carts
and place orders.

## Raft timeouts on a real network

On real machines the delay table stays empty, so the Raft timeouts in
`regions.json` are used as they are. The network layer gives each
AppendEntries answer `heartbeat_ms + 2 × (round trip in the delay table)` to
arrive (`crates/raft/src/network.rs`), which with an empty table is the
heartbeat alone. So the heartbeat must be longer than the round trip to the
farthest node, followers included, and not only to the farthest voter as the
comment on `RaftTimeouts` says. With Singapore about 165–200 ms from Europe,
`regions.json` sets a heartbeat of 400 ms and an election timeout of 2–4 s.
A failover therefore takes several seconds.
