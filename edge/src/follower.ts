// The follower Durable Object: a read-only copy of a tursoraft database in
// the object's own SQLite.
//
// A relay in a tursoraft node's process opens a WebSocket to the Worker,
// which hands it to this object. The object accepts it with the Hibernation
// API, so it can leave memory between messages while the socket stays open.
// Each message runs in one `transactionSync` together with the object's
// position, and is acknowledged only after that. The object runs no Raft and
// no Turso: it copies rows that Turso already accepted.
//
// A full copy is written to staging tables and swapped in by its last
// message's transaction, so reads never see half a copy; until then they
// see the data from before. Triggers are never created here, and foreign-key
// actions are off while applying, because a change set already holds the
// rows that the source's triggers and cascades wrote.

import { DurableObject } from "cloudflare:workers";
import {
  bind,
  exactColumn,
  fromSql,
  quote,
  type FollowerMessage,
  type RelayMessage,
  type RowChange,
  type SchemaObject,
  type Value,
} from "./protocol";

export interface Env {
  FOLLOWER: DurableObjectNamespace<Follower>;
  /// The shared secret a relay and an admin must present.
  RELAY_SECRET?: string;
  /// The follower that pages read from unless the request names another.
  FOLLOWER_NAME?: string;
}

/// This object's own tables. The relay sends no tables whose names start
/// with `tursoraft_` or `sqlite_`, and DO SQL refuses names that start with
/// `_cf_`.
const STATE = "tursoraft_follower";
const STAGE = "tursoraft_copy_";
const OWN_PREFIXES = ["sqlite_", "_cf_", "tursoraft_"];

/// The position a full copy leaves: after every change set of its entry.
const WHOLE_ENTRY = Number.MAX_SAFE_INTEGER;

/// Where this follower is. Every entry up to `through` is applied
/// completely; (`lastIndex`, `lastPosition`) is the last change set applied.
export type Position = { through: number; lastIndex: number; lastPosition: number };

type State = Position & {
  /// The relay stream whose messages are accepted, and its last message.
  stream: string | null;
  seq: number;
  /// The copy being staged: its index, schema and tables, as JSON.
  copyIndex: number | null;
  copySchema: string | null;
  copyTables: string | null;
  constructed: number;
};

/// Every copied table with exact values, and the position it was read at.
/// Tables without a PRIMARY KEY have their rowid as the first value.
export type Dump = Position & {
  schema: { type: string; name: string }[];
  tables: Record<string, { rowid: boolean; rows: Value[][] }>;
};

export type FollowerStatus = Position & {
  copying: boolean;
  /// Random per constructor run: it changes when the object left memory.
  instance: string;
  /// How many times the constructor has run, stored.
  constructed: number;
  relays: number;
};

type TableInfo = {
  name: string;
  columns: string[];
  /// The PRIMARY KEY columns, or none when rows are identified by rowid.
  key: number[];
  rowidName: string;
};

export class Follower extends DurableObject<Env> {
  private readonly sql: SqlStorage;
  private readonly instance = crypto.randomUUID();
  private tables = new Map<string, TableInfo>();
  private waiters: { index: number; resolve: () => void }[] = [];

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS ${STATE}(
         id INTEGER PRIMARY KEY CHECK (id = 0),
         through INTEGER NOT NULL, last_index INTEGER NOT NULL, last_position INTEGER NOT NULL,
         stream TEXT, seq INTEGER NOT NULL,
         copy_index INTEGER, copy_schema TEXT, copy_tables TEXT,
         constructed INTEGER NOT NULL)`,
    );
    this.sql.exec(`INSERT OR IGNORE INTO ${STATE} VALUES (0, 0, 0, -1, NULL, 0, NULL, NULL, NULL, 0)`);
    this.sql.exec(`UPDATE ${STATE} SET constructed = constructed + 1`);
  }

  /// The relay's WebSocket, passed on by the Worker after it checked the
  /// secret. A new relay replaces the one before.
  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("expected a WebSocket upgrade", { status: 426 });
    }
    for (const old of this.ctx.getWebSockets()) old.close(4000, "another relay connected");
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    const hello: FollowerMessage = {
      t: "hello",
      through: this.state().through,
      instance: this.instance,
    };
    server.send(JSON.stringify(hello));
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws: WebSocket, data: string | ArrayBuffer): void {
    let seq = -1;
    let reply: FollowerMessage;
    try {
      const msg = JSON.parse(
        typeof data === "string" ? data : new TextDecoder().decode(data),
      ) as RelayMessage;
      seq = msg.seq;
      // Has no effect inside a transaction, so it is set before.
      this.sql.exec("PRAGMA foreign_keys = OFF");
      const through = this.ctx.storage.transactionSync(() => this.handle(msg));
      reply = { t: "ack", seq, through };
    } catch (e) {
      // Rolled back: cached table shapes may describe tables that are gone.
      this.tables.clear();
      const message = e instanceof Error ? e.message : String(e);
      console.error(`follower: message ${seq} failed: ${message}`);
      reply = { t: "error", seq, message };
    }
    ws.send(JSON.stringify(reply));
    this.wake();
  }

  webSocketClose(ws: WebSocket, code: number, reason: string): void {
    // 1005 and 1006 say that no code was sent; they may not be sent back.
    ws.close(code === 1005 || code === 1006 ? 1000 : code, reason);
  }

  /// Runs one statement once this follower has applied log index `at`
  /// (when given), and returns its rows and the position they were read
  /// at. Waits at most `timeoutMs`, then returns `null` for the rows.
  async read(
    sql: string,
    params: SqlStorageValue[],
    at: number | null,
    timeoutMs: number,
  ): Promise<{ through: number; rows: Record<string, SqlStorageValue>[] | null }> {
    if (at !== null && !(await this.waitFor(at, timeoutMs))) {
      return { through: this.state().through, rows: null };
    }
    // Synchronous from here on, so no message is applied in between.
    const through = this.state().through;
    return { through, rows: this.sql.exec(sql, ...params).toArray() };
  }

  /// Every copied table, once this follower has applied `at` (when given).
  /// `null` if it did not within `timeoutMs`.
  async dump(at: number | null, timeoutMs: number): Promise<Dump | null> {
    if (at !== null && !(await this.waitFor(at, timeoutMs))) return null;
    const state = this.state();
    const schema = this.userObjects();
    const tables: Dump["tables"] = {};
    for (const { type, name } of schema) {
      if (type !== "table") continue;
      const info = this.tableInfo(name);
      const rowid = info.key.length === 0;
      const exprs = info.columns.map((c) => exactColumn(quote(c)));
      if (rowid) exprs.unshift(exactColumn(info.rowidName));
      const rows: Value[][] = [];
      for (const raw of this.sql.exec(`SELECT ${exprs.join(", ")} FROM ${quote(name)}`).raw()) {
        const row: Value[] = [];
        for (let i = 0; i < raw.length; i += 2) row.push(fromSql(raw[i], raw[i + 1]));
        rows.push(row);
      }
      tables[name] = { rowid, rows };
    }
    return {
      through: state.through,
      lastIndex: state.lastIndex,
      lastPosition: state.lastPosition,
      schema,
      tables,
    };
  }

  status(): FollowerStatus {
    const s = this.state();
    return {
      through: s.through,
      lastIndex: s.lastIndex,
      lastPosition: s.lastPosition,
      copying: s.copyIndex !== null,
      instance: this.instance,
      constructed: s.constructed,
      relays: this.ctx.getWebSockets().length,
    };
  }

  /// Closes the relays' WebSockets, as a network failure would.
  closeRelays(): number {
    const sockets = this.ctx.getWebSockets();
    for (const ws of sockets) ws.close(4001, "closed by an admin");
    return sockets.length;
  }

  /// Deletes every copied table and the position, and closes the relays'
  /// WebSockets, so that the next copy may be at any log index. For a
  /// cluster that started over from an empty database: otherwise this
  /// follower refuses its copies, which are at lower indexes than its own.
  forget(): number {
    this.ctx.storage.transactionSync(() => {
      this.dropStaging();
      const live = this.userObjects();
      for (const { name } of live.filter((o) => o.type === "view")) {
        this.sql.exec(`DROP VIEW ${quote(name)}`);
      }
      for (const { name } of live.filter((o) => o.type === "table")) {
        this.sql.exec(`DROP TABLE ${quote(name)}`);
      }
      this.sql.exec(
        `UPDATE ${STATE} SET through = 0, last_index = 0, last_position = -1, stream = NULL,
           seq = 0, copy_index = NULL, copy_schema = NULL, copy_tables = NULL`,
      );
    });
    this.tables.clear();
    return this.closeRelays();
  }

  /// Applies one relay message and returns the new `through`.
  private handle(msg: RelayMessage): number {
    if (this.sql.exec<{ foreign_keys: number }>("PRAGMA foreign_keys").one().foreign_keys !== 0) {
      throw new Error("foreign keys are on: cascades would run twice");
    }
    const state = this.state();
    if (msg.t === "copy_begin") {
      if (msg.seq !== 1) throw new Error(`stream ${msg.stream} begins with message ${msg.seq}`);
    } else if (msg.stream !== state.stream || msg.seq !== state.seq + 1) {
      throw new Error(
        `expected message ${state.seq + 1} of stream ${state.stream}, got ${msg.seq} of ${msg.stream}`,
      );
    }
    this.sql.exec(`UPDATE ${STATE} SET stream = ?, seq = ?`, msg.stream, msg.seq);
    switch (msg.t) {
      case "copy_begin": {
        if (msg.index < state.through) {
          throw new Error(
            `a copy at log index ${msg.index} would take this follower back from ${state.through}`,
          );
        }
        this.dropStaging();
        msg.tables.forEach((table, i) => {
          const columns = ["r", ...Array.from({ length: table.columns }, (_, c) => `c${c}`)];
          this.sql.exec(`CREATE TABLE ${STAGE}${i}(${columns.join(", ")})`);
        });
        this.sql.exec(
          `UPDATE ${STATE} SET copy_index = ?, copy_schema = ?, copy_tables = ?`,
          msg.index,
          JSON.stringify(msg.schema),
          JSON.stringify(msg.tables),
        );
        return state.through;
      }
      case "copy_rows": {
        this.expectCopy(state, true);
        for (const row of msg.rows) {
          const [marks, params] = bindAll(row);
          this.sql.exec(`INSERT INTO ${STAGE}${msg.table} VALUES (${marks.join(", ")})`, ...params);
        }
        return state.through;
      }
      case "copy_end": {
        this.expectCopy(state, true);
        return this.swapInCopy(state);
      }
      case "changes": {
        this.expectCopy(state, false);
        let { lastIndex, lastPosition } = state;
        for (const event of msg.events) {
          // At or below the stored position: already applied.
          const applied =
            event.index <= state.through ||
            event.index < lastIndex ||
            (event.index === lastIndex && event.position <= lastPosition);
          if (applied) continue;
          for (const row of event.rows) this.applyRow(row);
          lastIndex = event.index;
          lastPosition = event.position;
        }
        const through = Math.max(state.through, msg.through);
        this.sql.exec(
          `UPDATE ${STATE} SET through = ?, last_index = ?, last_position = ?`,
          through,
          lastIndex,
          lastPosition,
        );
        return through;
      }
      default:
        throw new Error(`unknown message ${JSON.stringify((msg as { t: unknown }).t)}`);
    }
  }

  /// Replaces every copied object with the staged copy.
  private swapInCopy(state: State): number {
    const schema = JSON.parse(state.copySchema!) as SchemaObject[];
    const staged = JSON.parse(state.copyTables!) as { name: string; columns: number }[];
    const live = this.userObjects();
    for (const { name } of live.filter((o) => o.type === "view")) {
      this.sql.exec(`DROP VIEW ${quote(name)}`);
    }
    for (const { name } of live.filter((o) => o.type === "table")) {
      this.sql.exec(`DROP TABLE ${quote(name)}`);
    }
    this.tables.clear();
    for (const o of schema.filter((o) => o.type === "table")) this.sql.exec(o.sql);
    staged.forEach(({ name, columns }, i) => {
      const info = this.tableInfo(name);
      if (info.columns.length !== columns) {
        throw new Error(
          `table ${name} has ${info.columns.length} columns here and ${columns} in the copy`,
        );
      }
      const target = info.columns.map(quote);
      const source = info.columns.map((_, c) => `c${c}`);
      if (info.key.length === 0) {
        target.push(info.rowidName);
        source.push("r");
      }
      this.sql.exec(
        `INSERT INTO ${quote(name)}(${target.join(", ")}) SELECT ${source.join(", ")} FROM ${STAGE}${i}`,
      );
    });
    this.dropStaging();
    for (const o of schema.filter((o) => o.type === "index")) this.sql.exec(o.sql);
    for (const o of schema.filter((o) => o.type === "view")) this.sql.exec(o.sql);
    this.sql.exec(
      `UPDATE ${STATE} SET through = ?, last_index = ?, last_position = ?,
         copy_index = NULL, copy_schema = NULL, copy_tables = NULL`,
      state.copyIndex,
      state.copyIndex,
      WHOLE_ENTRY,
    );
    return state.copyIndex!;
  }

  private applyRow(row: RowChange): void {
    if ("sql" in row) {
      this.sql.exec(row.sql);
      this.tables.clear();
      return;
    }
    const table = this.tableInfo(row.table);
    const name = quote(table.name);
    const here = () => `${row.kind} of ${table.name} rowid ${row.rowid}`;
    switch (row.kind) {
      case "insert": {
        const after = image(row.after, here);
        const columns = table.columns.slice(0, after.length).map(quote);
        const values = [...after];
        if (table.key.length === 0) {
          columns.push(table.rowidName);
          values.push({ i: row.rowid });
        }
        const [marks, params] = bindAll(values);
        this.sql.exec(
          `INSERT INTO ${name}(${columns.join(", ")}) VALUES (${marks.join(", ")})`,
          ...params,
        );
        return;
      }
      case "update": {
        const [set, setParams] = bindAll(image(row.after, here));
        const assignments = set.map((mark, i) => `${quote(table.columns[i])} = ${mark}`);
        const [where, whereParams] = identity(table, row.rowid, image(row.before, here));
        const cursor = this.sql.exec(
          `UPDATE ${name} SET ${assignments.join(", ")} WHERE ${where}`,
          ...setParams,
          ...whereParams,
        );
        if (cursor.rowsWritten === 0) throw new Error(`${here()}: no such row here`);
        return;
      }
      case "delete": {
        const [where, params] = identity(table, row.rowid, image(row.before, here));
        const cursor = this.sql.exec(`DELETE FROM ${name} WHERE ${where}`, ...params);
        if (cursor.rowsWritten === 0) throw new Error(`${here()}: no such row here`);
        return;
      }
    }
  }

  private tableInfo(name: string): TableInfo {
    const cached = this.tables.get(name);
    if (cached) return cached;
    const info = this.sql
      .exec<{ name: string; pk: number }>(
        "SELECT name, pk FROM pragma_table_info(?) ORDER BY cid",
        name,
      )
      .toArray();
    if (info.length === 0) throw new Error(`no such table: ${name}`);
    const columns = info.map((c) => c.name);
    const key = info
      .map((c, i) => [c.pk, i] as const)
      .filter(([pk]) => pk > 0)
      .sort((a, b) => a[0] - b[0])
      .map(([, i]) => i);
    const rowidName = ["rowid", "_rowid_", "oid"].find(
      (alias) => !columns.some((c) => c.toLowerCase() === alias),
    );
    if (rowidName === undefined) throw new Error(`table ${name} shadows every name of the rowid`);
    const table = { name, columns, key, rowidName };
    this.tables.set(name, table);
    return table;
  }

  /// The source's tables, indexes and views here, without this object's
  /// own and without automatic indexes.
  private userObjects(): { type: string; name: string }[] {
    return this.sql
      .exec<{ type: string; name: string }>(
        "SELECT type, name FROM sqlite_schema WHERE type IN ('table', 'index', 'view') ORDER BY rowid",
      )
      .toArray()
      .filter((o) => !OWN_PREFIXES.some((p) => o.name.startsWith(p)))
      .map((o) => ({ type: o.type, name: o.name }));
  }

  private dropStaging(): void {
    const staged = this.sql
      .exec<{ name: string }>("SELECT name FROM sqlite_schema WHERE type = 'table'")
      .toArray()
      .filter((t) => t.name.startsWith(STAGE));
    for (const { name } of staged) this.sql.exec(`DROP TABLE ${quote(name)}`);
  }

  private expectCopy(state: State, copying: boolean): void {
    if ((state.copyIndex !== null) !== copying) {
      throw new Error(copying ? "no copy is in progress" : "a copy is in progress");
    }
  }

  private state(): State {
    const row = this.sql
      .exec<{
        through: number;
        last_index: number;
        last_position: number;
        stream: string | null;
        seq: number;
        copy_index: number | null;
        copy_schema: string | null;
        copy_tables: string | null;
        constructed: number;
      }>(`SELECT * FROM ${STATE}`)
      .one();
    return {
      through: row.through,
      lastIndex: row.last_index,
      lastPosition: row.last_position,
      stream: row.stream,
      seq: row.seq,
      copyIndex: row.copy_index,
      copySchema: row.copy_schema,
      copyTables: row.copy_tables,
      constructed: row.constructed,
    };
  }

  /// Whether this follower has applied log index `index` within
  /// `timeoutMs`.
  private waitFor(index: number, timeoutMs: number): Promise<boolean> {
    if (this.state().through >= index) return Promise.resolve(true);
    return new Promise((resolve) => {
      const waiter = {
        index,
        resolve: () => {
          clearTimeout(timer);
          resolve(true);
        },
      };
      const timer = setTimeout(() => {
        this.waiters = this.waiters.filter((w) => w !== waiter);
        resolve(false);
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }

  private wake(): void {
    if (this.waiters.length === 0) return;
    const through = this.state().through;
    const ready = this.waiters.filter((w) => w.index <= through);
    this.waiters = this.waiters.filter((w) => w.index > through);
    for (const w of ready) w.resolve();
  }
}

/// The WHERE clause that finds the row an update or delete changes: by its
/// PRIMARY KEY in `before`, or by rowid in a table without one. The rowid of
/// a table with another PRIMARY KEY may differ between nodes.
function identity(table: TableInfo, rowid: string, before: Value[]): [string, SqlStorageValue[]] {
  if (table.key.length === 0) return [`${table.rowidName} = CAST(? AS INTEGER)`, [rowid]];
  const predicates: string[] = [];
  const params: SqlStorageValue[] = [];
  for (const k of table.key) {
    const [mark, param] = bind(before[k] ?? null);
    predicates.push(`${quote(table.columns[k])} IS ${mark}`);
    params.push(param);
  }
  return [predicates.join(" AND "), params];
}

function bindAll(values: Value[]): [string[], SqlStorageValue[]] {
  const marks: string[] = [];
  const params: SqlStorageValue[] = [];
  for (const v of values) {
    const [mark, param] = bind(v);
    marks.push(mark);
    params.push(param);
  }
  return [marks, params];
}

function image(values: Value[] | undefined, here: () => string): Value[] {
  if (values === undefined) throw new Error(`${here()} has no row image`);
  return values;
}
