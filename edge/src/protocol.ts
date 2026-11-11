// The messages between a tursoraft relay and a follower Durable Object, and
// how row values travel in them. The relay's side is
// `crates/relay/src/protocol.rs`; keep the two in step.
//
// Every message is one JSON text frame. The follower answers each with an
// `ack` once the message's transaction has committed, or with an `error`
// when nothing of it was kept.

/// One SQL value. Integers travel as decimal text, because JSON numbers are
/// doubles in JavaScript and DO SQL returns doubles too.
export type Value =
  | null
  | number // REAL, finite
  | string // TEXT
  | { i: string } // INTEGER
  | { b: string } // BLOB, base64
  | { r: "inf" | "-inf" }; // REAL, infinite

/// One row a transaction changed, with whole rows in the table's column
/// order. `rowid` is the rowid on the node that ran the transaction; it
/// identifies rows only in tables without a PRIMARY KEY.
export type RowChange =
  | { sql: string } // CREATE TABLE, CREATE INDEX or CREATE VIEW
  | {
      table: string;
      kind: "insert" | "update" | "delete";
      rowid: string;
      before?: Value[];
      after?: Value[];
    };

/// The changes of one transaction at (log index, position in the entry).
export type Event = { index: number; position: number; rows: RowChange[] };

/// One object of the source's schema, as `sqlite_schema` holds it.
export type SchemaObject = { type: "table" | "index" | "view"; name: string; sql: string };

// A stream is one full copy and the changes after it. Its messages are
// numbered from 1 by `seq`, and the follower applies them only in that
// order: after a failed message it refuses the rest of the stream, and the
// relay starts a new one.
export type RelayMessage =
  | {
      t: "copy_begin";
      stream: string;
      seq: number;
      /// The log index the copy was read at.
      index: number;
      schema: SchemaObject[];
      /// The tables whose rows follow, and the number of columns of each.
      tables: { name: string; columns: number }[];
    }
  | {
      t: "copy_rows";
      stream: string;
      seq: number;
      /// Position in `tables` of copy_begin.
      table: number;
      /// Each row is [rowid, column values...].
      rows: Value[][];
    }
  | { t: "copy_end"; stream: string; seq: number }
  | {
      t: "changes";
      stream: string;
      seq: number;
      events: Event[];
      /// Every entry up to this log index has been sent completely.
      through: number;
    };

export type FollowerMessage =
  | { t: "hello"; through: number; instance: string }
  | { t: "ack"; seq: number; through: number }
  | { t: "error"; seq: number; message: string };

/// A placeholder and its parameter for binding `value` in DO SQL. JavaScript
/// numbers always bind as REAL there, so integers bind as text and are cast.
export function bind(value: Value): [string, SqlStorageValue] {
  if (value === null) return ["?", null];
  if (typeof value === "number") return ["CAST(? AS REAL)", value];
  if (typeof value === "string") return ["?", value];
  if ("i" in value) return ["CAST(? AS INTEGER)", value.i];
  if ("b" in value) return ["?", fromBase64(value.b).buffer as ArrayBuffer];
  return ["CAST(? AS REAL)", value.r === "inf" ? Infinity : -Infinity];
}

/// The SQL expression that reads column `c` so that [`fromSql`] can rebuild
/// its exact value: integers as text, everything else as it is.
export function exactColumn(c: string): string {
  return `typeof(${c}), CASE typeof(${c}) WHEN 'integer' THEN CAST(${c} AS TEXT) ELSE ${c} END`;
}

/// The value read by [`exactColumn`].
export function fromSql(type: SqlStorageValue, v: SqlStorageValue): Value {
  switch (type) {
    case "null":
      return null;
    case "integer":
      return { i: v as string };
    case "real": {
      const n = v as number;
      if (n === Infinity) return { r: "inf" };
      if (n === -Infinity) return { r: "-inf" };
      return n;
    }
    case "text":
      return v as string;
    case "blob":
      return { b: toBase64(new Uint8Array(v as ArrayBuffer)) };
    default:
      throw new Error(`unknown SQL type ${String(type)}`);
  }
}

export function quote(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

function fromBase64(s: string): Uint8Array {
  const bin = atob(s);
  const bytes = new Uint8Array(new ArrayBuffer(bin.length));
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

function toBase64(bytes: Uint8Array): string {
  let bin = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(bin);
}
