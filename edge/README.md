# edge: a reads-only follower in a Cloudflare Durable Object

A Worker renders HTML pages from a follower Durable Object (DO). The follower
keeps a copy of a tursoraft database in the DO's own SQLite. A relay in a
tursoraft node's process (`crates/relay`) sends it the changes the node applies.
See ADR 0008 in `docs/adr/`.

```text
tursoraft node ── Node::subscribe ── relay ══ WebSocket ══> Worker /relay ──> Follower DO ──> SQLite
                                                           Worker /, /products/<id> ──read──┘
```

- The relay opens the WebSocket to the Worker, and the follower accepts it
  with the Hibernation API. The follower is the WebSocket server, so it can
  leave memory between messages.
- Each connection starts with a full copy at a known log index, followed by
  the change sets applied after that index. The relay copies again after
  `Reset`, `Lagged`, a schema change other than CREATE TABLE, CREATE INDEX or
  CREATE VIEW, and after any failure or reconnect. Nothing is resumed.
- The follower applies each message, and its position, in one
  `transactionSync`. It skips any change set at or below its stored (log
  index, position).
- A copy is staged in separate tables and swapped in by the copy's last
  message, so a read never sees half of it.
- Foreign keys are off while the follower applies, and no triggers are
  created. A change set already holds the rows that the source's cascades
  and triggers wrote.
- Integers travel as decimal text and are bound with `CAST(? AS INTEGER)`,
  so values beyond 2^53 stay exact. The wire format is in `src/protocol.ts`
  and `crates/relay/src/protocol.rs`.

## Files

| File | What |
|---|---|
| `src/follower.ts` | The follower DO: applies relay messages, reads, waits for a log index |
| `src/index.ts` | The Worker: routes, the secret, `?at=` and `?wait_ms=` |
| `src/pages.ts` | Every page and all SQL that pages run. Change only this file to point the pages at another schema |
| `src/protocol.ts` | Messages and the wire form of values |
| `wrangler.toml` | Deployment config (not deployed yet) |
| `test/edge.mjs` | Starts the Worker under Miniflare |
| `test/serve.mjs` | The same, for the Rust tests and `deploy/local.sh --edge` |
| `test/follower.test.mjs` | Tests of the follower with hand-written relay messages |
| `test/bench.mjs` | How fast one follower applies change sets |

## Routes

| Route | |
|---|---|
| `GET /`, `GET /products/<id>` | Pages. `?at=<log index>` waits until the follower has applied that index (at most `?wait_ms=`, default 5000, capped at 30000; then 504). The response header `x-follower-through` is the index the page was read at |
| `GET /relay?follower=<name>` | A relay's WebSocket. Needs the secret |
| `GET /admin/status`, `GET /admin/dump`, `POST /admin/close-relays`, `POST /admin/forget` | Position, every table with exact values, dropping the relays' sockets, and deleting the copy and the position (for a cluster that started over: the follower refuses a copy older than what it has). Need the secret |

The secret is sent as `Authorization: Bearer <RELAY_SECRET>`. `?follower=`
picks the follower DO by name; it defaults to `FOLLOWER_NAME`.

## Test

```bash
npm ci
npm test            # tsc, then the tests in test/*.test.mjs under Miniflare
npm run bench       # change sets per second for one follower
```

The end-to-end tests run a tursoraft cluster with a relay against this
Worker. They need `npm ci` here first:

```bash
cargo test -p tursoraft-relay
cargo test -p tursoraft-relay --release --test measure -- --ignored --nocapture --test-threads=1
```

## Deploy (not done yet)

```bash
npx wrangler secret put RELAY_SECRET
npx wrangler deploy
```

Then start a relay with
`tursoraft_relay::Relay::start(node, Config::new("wss://<host>/relay?follower=<name>", secret))`.
Add `locationHint` when the follower is first created, to place it in a region.
Nothing does that yet.
