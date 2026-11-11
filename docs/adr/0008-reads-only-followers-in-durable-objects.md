# 0008: Followers in Durable Objects hold a reads-only copy fed from change subscriptions

Status: Accepted, not built yet
Date: 2026-11-11
Decided by: Kalle Ahlström

## Context

The wish: pages rendered on the server, close to the user, on a platform that
bills CPU only when a request comes in or a change arrives.

A tursoraft node cannot be that. The three voters run all the time, because a
Raft leader sends heartbeats around the clock, and a node needs inbound TCP
and a disk that stays.

Cloudflare Workers with SQLite-backed Durable Objects (DOs) are billed that
way. A DO that waits on a WebSocket accepted with the hibernation API is not
billed, is removed from memory after about 10 s without events, and is
started again by the next message.

Cloudflare D1 already is a primary DO with replica DOs in every region. So a
design made only of DOs rebuilds D1.

## Decision

- A relay in a node's process subscribes to the node's changes (ADR 0006) and
  sends them over a WebSocket to a follower DO.
- The follower DO keeps a copy of the tables in its own SQLite. It is not a
  tursoraft node and takes no part in Raft.
- A Worker renders HTML from that copy.
- The copy is reads only. Writes go to a tursoraft node.
- After a reconnect, a restart of the relay, a `Reset` or missed events, the
  DO takes a full copy again. A read never sees a half-copied database, and a
  read can name a log position and wait until the DO has reached it.

## Alternatives rejected

- **Raft inside DOs.** Voters must be long-lived and reachable, and
  heartbeats would keep every DO awake and billed.
- **Primary and replica DOs of our own.** That is D1.
- **Feed the DO from the Raft log.** The log holds rejected change sets too
  and is cut after every snapshot. The subscription already gives the
  applied changes in order.
- **Local transactions in the DO.** Triggers work in DO SQLite, so the rows a
  transaction changed can be captured there (checked on local `workerd`).
  This is possible and not agreed. It is the step that would go beyond D1.
- **Fly.io Machines that are suspended when idle.** They run the real node
  and cost almost nothing asleep, but a sleeping Machine receives nothing and
  is stale when it wakes.
- **bunny.net Magic Containers.** They run the real node in many regions and
  bill CPU by use, but RAM is billed all the time, and whether containers in
  different regions can connect to each other was not checked.

## Consequences

- Reads at the edge are local to the DO. They can be stale by the time the
  relay and the WebSocket take, unless the read names a log position.
- Every replicated row is billed as a row written in the DO. From
  Cloudflare's price table: a bank transfer writes three rows, so 100
  transfers per second cost about $730 a month for each follower DO. This
  fits a site that mostly reads.
- The follower is a second implementation, in TypeScript, of applying
  changes. It has to be tested against the node's tables.
- The DO and the Worker run unchanged on `workerd`, the open-source runtime,
  on any Linux machine. What that cannot give is Cloudflare's placement of
  DOs near users and billing per request.
- Measuring real latency and cost needs a Cloudflare account. Until then it
  is tested on one machine only.
