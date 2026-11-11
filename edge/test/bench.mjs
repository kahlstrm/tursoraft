// How fast one follower Durable Object applies change sets, without a
// cluster or a relay: this script sends `changes` messages straight to the
// follower under Miniflare, 16 unacknowledged at a time as the relay does,
// and times them until the last acknowledgement. One machine, local
// Miniflare: it says nothing about Cloudflare.
//
//   npm run bench

import { startEdge } from "./edge.mjs";

const SECRET = "bench-secret";
const TRANSACTIONS = 20_000;
const WINDOW = 16;
const body = "x".repeat(100);

const edge = await startEdge({ secret: SECRET });
console.log("| change sets per message | rows per change set | transactions | s | tx/s | rows/s |");
console.log("|---|---|---|---|---|---|");
for (const [perMessage, rows] of [
  [1, 1],
  [16, 1],
  [256, 1],
  [16, 10],
  [256, 10],
]) {
  await run(`bench-${perMessage}-${rows}`, perMessage, rows);
}
await edge.dispose();

async function run(name, perMessage, rows) {
  const res = await edge.mf.dispatchFetch(`${edge.url}/relay?follower=${name}`, {
    headers: { Upgrade: "websocket", authorization: `Bearer ${SECRET}` },
  });
  const ws = res.webSocket;
  let replies = 0;
  let onReply = null;
  ws.addEventListener("message", (e) => {
    const reply = JSON.parse(e.data);
    if (reply.t === "error") throw new Error(reply.message);
    replies += 1;
    if (onReply) onReply();
  });
  ws.accept();
  const until = async (n) => {
    while (replies < n) await new Promise((r) => (onReply = r));
  };
  await until(1); // hello
  const stream = name;
  let seq = 0;
  const send = (message) => ws.send(JSON.stringify({ stream, seq: ++seq, ...message }));
  send({
    t: "copy_begin",
    index: 1,
    schema: [
      {
        type: "table",
        name: "events",
        sql: "CREATE TABLE events(id INTEGER PRIMARY KEY, writer INTEGER, n INTEGER, body TEXT)",
      },
    ],
    tables: [{ name: "events", columns: 4 }],
  });
  send({ t: "copy_end" });
  await until(3);
  const started = performance.now();
  let id = 0;
  let index = 1;
  for (let tx = 0; tx < TRANSACTIONS; tx += perMessage) {
    const events = [];
    for (let k = 0; k < perMessage; k++) {
      index += 1;
      const changes = [];
      for (let r = 0; r < rows; r++) {
        id += 1;
        changes.push({
          table: "events",
          kind: "insert",
          rowid: String(id),
          after: [{ i: String(id) }, { i: "1" }, { i: String(id) }, body],
        });
      }
      events.push({ index, position: 0, rows: changes });
    }
    send({ t: "changes", events, through: index });
    await until(seq + 1 - WINDOW);
  }
  await until(seq + 1);
  const s = (performance.now() - started) / 1000;
  console.log(
    `| ${perMessage} | ${rows} | ${TRANSACTIONS} | ${s.toFixed(2)} | ${(TRANSACTIONS / s).toFixed(0)} | ${((TRANSACTIONS * rows) / s).toFixed(0)} |`,
  );
  ws.close(1000, "done");
}
