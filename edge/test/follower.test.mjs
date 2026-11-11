// The follower Durable Object and the Worker under Miniflare, driven by
// hand-written relay messages. The end-to-end tests with a tursoraft
// cluster are in crates/relay/tests/edge.rs.

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { startEdge } from "./edge.mjs";

const SECRET = "test-secret";
let edge;

before(async () => {
  edge = await startEdge({ secret: SECRET });
});
after(() => edge.dispose());

/// A relay's connection to follower `name`. `send` returns the reply.
async function connect(name, { secret = SECRET } = {}) {
  const res = await edge.mf.dispatchFetch(`${edge.url}/relay?follower=${name}`, {
    headers: { Upgrade: "websocket", authorization: `Bearer ${secret}` },
  });
  if (res.status !== 101) return { status: res.status };
  const ws = res.webSocket;
  const replies = [];
  let waiting = null;
  ws.addEventListener("message", (e) => {
    replies.push(JSON.parse(e.data));
    if (waiting) waiting();
  });
  ws.accept();
  const next = async () => {
    while (replies.length === 0) await new Promise((r) => (waiting = r));
    return replies.shift();
  };
  const hello = await next();
  assert.equal(hello.t, "hello");
  let stream = null;
  let seq = 0;
  return {
    status: 101,
    hello,
    ws,
    next,
    /// Sends `message` as the next of the current stream; a copy_begin
    /// starts a new stream.
    async send(message) {
      if (message.t === "copy_begin") {
        stream = `s${Math.random()}`;
        seq = 0;
      }
      seq += 1;
      ws.send(JSON.stringify({ stream, seq, ...message }));
      return next();
    },
    /// Sends a raw message without numbering it.
    async sendRaw(message) {
      ws.send(JSON.stringify(message));
      return next();
    },
  };
}

async function get(path, { secret } = {}) {
  const headers = secret ? { authorization: `Bearer ${secret}` } : {};
  const res = await edge.mf.dispatchFetch(`${edge.url}${path}`, { headers });
  return { status: res.status, text: await res.text(), through: res.headers.get("x-follower-through") };
}

async function dump(name) {
  const res = await get(`/admin/dump?follower=${name}`, { secret: SECRET });
  assert.equal(res.status, 200, res.text);
  return JSON.parse(res.text);
}

const PRODUCTS =
  "CREATE TABLE products(product_id INTEGER PRIMARY KEY, title TEXT NOT NULL, price_cents INTEGER NOT NULL, stock INTEGER NOT NULL)";

/// Copies a database with products 1 and 2 at log index `index`.
async function copyProducts(relay, index) {
  for (const message of [
    {
      t: "copy_begin",
      index,
      schema: [{ type: "table", name: "products", sql: PRODUCTS }],
      tables: [{ name: "products", columns: 4 }],
    },
    {
      t: "copy_rows",
      table: 0,
      rows: [
        [{ i: "1" }, { i: "1" }, "Tea", { i: "250" }, { i: "10" }],
        [{ i: "2" }, { i: "2" }, "Coffee", { i: "300" }, { i: "5" }],
      ],
    },
    { t: "copy_end" },
  ]) {
    const reply = await relay.send(message);
    assert.equal(reply.t, "ack", JSON.stringify(reply));
  }
}

const insert = (table, rowid, after) => ({ table, kind: "insert", rowid: String(rowid), after });

test("the relay endpoint and the admin pages need the secret", async () => {
  assert.equal((await connect("auth", { secret: "wrong" })).status, 401);
  const res = await edge.mf.dispatchFetch(`${edge.url}/relay?follower=auth`, {
    headers: { Upgrade: "websocket" },
  });
  assert.equal(res.status, 401);
  assert.equal((await get("/admin/status?follower=auth")).status, 401);
  assert.equal((await get("/admin/status?follower=auth", { secret: "wrong" })).status, 401);
  assert.equal((await connect("auth")).status, 101);
});

test("values are stored exactly, through a copy and through changes", async () => {
  const relay = await connect("values");
  const values = [
    { i: "9223372036854775807" },
    { i: "-9223372036854775808" },
    { i: "9007199254740993" },
    { i: "-42" },
    0.1,
    -2.5e-300,
    3,
    { r: "inf" },
    { r: "-inf" },
    "",
    "O'Brien \"x\" ä 😀",
    "123",
    { b: "AAEC/wA=" },
    { b: "" },
    null,
  ];
  const sql = "CREATE TABLE vals(k INTEGER PRIMARY KEY, v, r REAL, n INTEGER)";
  let reply = await relay.send({
    t: "copy_begin",
    index: 10,
    schema: [{ type: "table", name: "vals", sql }],
    tables: [{ name: "vals", columns: 4 }],
  });
  assert.equal(reply.t, "ack");
  reply = await relay.send({
    t: "copy_rows",
    table: 0,
    rows: values.map((v, k) => [{ i: String(k) }, { i: String(k) }, v, null, null]),
  });
  assert.equal(reply.t, "ack");
  assert.equal((await relay.send({ t: "copy_end" })).t, "ack");
  reply = await relay.send({
    t: "changes",
    events: [
      {
        index: 11,
        position: 0,
        rows: values.map((v, k) => insert("vals", 100 + k, [{ i: String(100 + k) }, v, null, null])),
      },
    ],
    through: 11,
  });
  assert.equal(reply.t, "ack", JSON.stringify(reply));
  const { tables } = await dump("values");
  const byKey = new Map(tables.vals.rows.map((r) => [r[0].i, r[1]]));
  values.forEach((v, k) => {
    // A real that holds an integer comes back as a plain number.
    assert.deepEqual(byKey.get(String(k)), v, `copied value ${k}`);
    assert.deepEqual(byKey.get(String(100 + k)), v, `changed value ${k}`);
  });
});

test("a change set at or below the stored position is skipped", async () => {
  const relay = await connect("dupes");
  await copyProducts(relay, 10);
  const tea = insert("products", 3, [{ i: "3" }, "Cocoa", { i: "100" }, { i: "1" }]);
  const pie = insert("products", 4, [{ i: "4" }, "Pie", { i: "500" }, { i: "2" }]);
  const events = [
    // At the copy's index: the copy has it.
    { index: 10, position: 3, rows: [insert("products", 1, [{ i: "1" }, "Again", { i: "1" }, { i: "1" }])] },
    { index: 11, position: 0, rows: [tea] },
  ];
  assert.equal((await relay.send({ t: "changes", events, through: 10 })).t, "ack");
  // The relay sends (11, 0) again with the next one.
  const again = [events[1], { index: 11, position: 2, rows: [pie] }];
  const reply = await relay.send({ t: "changes", events: again, through: 11 });
  assert.equal(reply.t, "ack", JSON.stringify(reply));
  const { tables, through, lastIndex, lastPosition } = await dump("dupes");
  assert.deepEqual(
    tables.products.rows.map((r) => r[1]),
    ["Tea", "Coffee", "Cocoa", "Pie"],
  );
  assert.deepEqual([through, lastIndex, lastPosition], [11, 11, 2]);
});

test("a copy is not visible until its last message", async () => {
  const relay = await connect("default");
  await copyProducts(relay, 10);
  let page = await get("/");
  assert.equal(page.status, 200);
  assert.match(page.text, /Tea/);
  assert.equal(page.through, "10");
  // A new copy without Tea, sent halfway.
  assert.equal(
    (
      await relay.send({
        t: "copy_begin",
        index: 20,
        schema: [{ type: "table", name: "products", sql: PRODUCTS }],
        tables: [{ name: "products", columns: 4 }],
      })
    ).t,
    "ack",
  );
  assert.equal(
    (
      await relay.send({
        t: "copy_rows",
        table: 0,
        rows: [[{ i: "7" }, { i: "7" }, "Juice", { i: "200" }, { i: "3" }]],
      })
    ).t,
    "ack",
  );
  page = await get("/");
  assert.match(page.text, /Tea/);
  assert.doesNotMatch(page.text, /Juice/);
  assert.equal(page.through, "10");
  const status = JSON.parse((await get("/admin/status", { secret: SECRET })).text);
  assert.equal(status.copying, true);
  assert.equal((await relay.send({ t: "copy_end" })).t, "ack");
  page = await get("/");
  assert.doesNotMatch(page.text, /Tea/);
  assert.match(page.text, /Juice/);
  assert.equal(page.through, "20");
});

test("after a failed message the stream is refused until a new copy", async () => {
  const relay = await connect("broken");
  await copyProducts(relay, 10);
  const missing = { table: "products", kind: "delete", rowid: "9", before: [{ i: "9" }, "No", { i: "1" }, { i: "1" }] };
  const cocoa = insert("products", 3, [{ i: "3" }, "Cocoa", { i: "100" }, { i: "1" }]);
  let reply = await relay.send({
    t: "changes",
    events: [{ index: 11, position: 0, rows: [cocoa, missing] }],
    through: 11,
  });
  assert.equal(reply.t, "error");
  assert.match(reply.message, /no such row/);
  // Nothing of it was kept, and the next message of the stream is refused.
  reply = await relay.send({ t: "changes", events: [], through: 12 });
  assert.equal(reply.t, "error");
  assert.match(reply.message, /expected message 4/);
  let state = await dump("broken");
  assert.equal(state.through, 10);
  assert.equal(state.tables.products.rows.length, 2);
  // A message of another stream, or out of order, is refused as well.
  reply = await relay.sendRaw({ t: "changes", stream: "other", seq: 4, events: [], through: 12 });
  assert.equal(reply.t, "error");
  reply = await relay.sendRaw({ t: "copy_end", stream: "other", seq: 2 });
  assert.equal(reply.t, "error");
  // A copy that would take the follower back is refused.
  reply = await relay.send({ t: "copy_begin", index: 9, schema: [], tables: [] });
  assert.equal(reply.t, "error");
  assert.match(reply.message, /take this follower back/);
  await copyProducts(relay, 12);
  state = await dump("broken");
  assert.equal(state.through, 12);
});

test("foreign-key actions do not run while applying", async () => {
  const relay = await connect("cascade");
  await copyProducts(relay, 1);
  const parent = "CREATE TABLE parent(id INTEGER PRIMARY KEY)";
  const child = "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE)";
  let reply = await relay.send({
    t: "changes",
    events: [
      {
        index: 2,
        position: 0,
        rows: [
          { sql: parent },
          { sql: child },
          insert("parent", 1, [{ i: "1" }]),
          insert("child", 10, [{ i: "10" }, { i: "1" }]),
          // A child whose parent the source does not have: allowed here.
          insert("child", 11, [{ i: "11" }, { i: "99" }]),
        ],
      },
    ],
    through: 2,
  });
  assert.equal(reply.t, "ack", JSON.stringify(reply));
  // The source deleted the parent and its cascade deleted the child; the
  // change set holds both deletes.
  reply = await relay.send({
    t: "changes",
    events: [
      {
        index: 3,
        position: 0,
        rows: [
          { table: "parent", kind: "delete", rowid: "1", before: [{ i: "1" }] },
          { table: "child", kind: "delete", rowid: "10", before: [{ i: "10" }, { i: "1" }] },
        ],
      },
    ],
    through: 3,
  });
  assert.equal(reply.t, "ack", JSON.stringify(reply));
  const { tables } = await dump("cascade");
  assert.deepEqual(tables.child.rows, [[{ i: "11" }, { i: "99" }]]);
});

test("a read waits until the follower has applied its log index", async () => {
  const relay = await connect("waits");
  await copyProducts(relay, 5);
  const read = get("/products/3?follower=waits&at=6&wait_ms=10000");
  let done = false;
  read.then(() => (done = true));
  await new Promise((r) => setTimeout(r, 300));
  assert.equal(done, false, "the read did not wait");
  // Entry 6 arrives in two messages; the read waits for the whole entry.
  const cocoa = insert("products", 3, [{ i: "3" }, "Cocoa <hot>", { i: "100" }, { i: "1" }]);
  assert.equal(
    (await relay.send({ t: "changes", events: [{ index: 6, position: 0, rows: [] }], through: 5 })).t,
    "ack",
  );
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(done, false, "the read did not wait for the whole entry");
  assert.equal(
    (await relay.send({ t: "changes", events: [{ index: 6, position: 1, rows: [cocoa] }], through: 6 })).t,
    "ack",
  );
  const page = await read;
  assert.equal(page.status, 200, page.text);
  assert.match(page.text, /<h1>Cocoa &lt;hot&gt;<\/h1>/);
  assert.equal(page.through, "6");
  const late = await get("/?follower=waits&at=7&wait_ms=200");
  assert.equal(late.status, 504);
  assert.equal((await get("/products/99?follower=waits")).status, 404);
  assert.equal((await get("/nowhere?follower=waits")).status, 404);
});

test("the follower keeps its position and its socket when it leaves memory", async () => {
  const relay = await connect("evicted");
  await copyProducts(relay, 5);
  const before = JSON.parse((await get("/admin/status?follower=evicted", { secret: SECRET })).text);
  await edge.evict("evicted");
  const cocoa = insert("products", 3, [{ i: "3" }, "Cocoa", { i: "100" }, { i: "1" }]);
  const reply = await relay.send({
    t: "changes",
    events: [{ index: 6, position: 0, rows: [cocoa] }],
    through: 6,
  });
  assert.equal(reply.t, "ack", JSON.stringify(reply));
  const after = JSON.parse((await get("/admin/status?follower=evicted", { secret: SECRET })).text);
  assert.notEqual(after.instance, before.instance);
  assert.equal(after.constructed, before.constructed + 1);
  assert.equal(after.through, 6);
  assert.equal(after.relays, 1);
});
