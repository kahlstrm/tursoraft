//! The messages between a relay and a follower Durable Object, and how row
//! values travel in them. The follower's side is `edge/src/protocol.ts`;
//! keep the two in step.
//!
//! Every message is one JSON text frame. A stream is one full copy and the
//! changes after it; its messages are numbered from 1 by `seq`. The follower
//! applies each message in one transaction and answers with an `ack`, or
//! with an `error` when it kept nothing of the message. After an error it
//! refuses the rest of the stream, so the relay starts a new one.
//!
//! Values: SQL integers travel as `{"i": "<decimal>"}`, because JSON numbers
//! are doubles in JavaScript and lose integers above 2^53. Blobs travel as
//! `{"b": "<base64>"}`, infinite reals as `{"r": "inf"}` or `{"r": "-inf"}`,
//! finite reals as JSON numbers, text as JSON strings, and NULL as `null`.

use base64::Engine;
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize, Serializer};
use tursoraft_node::Value;

#[derive(Debug, Serialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum RelayMessage<'a> {
    /// Starts a stream with a full copy read at log `index`. The follower
    /// keeps serving its old data until `CopyEnd`.
    CopyBegin {
        stream: &'a str,
        seq: u64,
        index: u64,
        schema: Vec<SchemaObject>,
        tables: Vec<CopyTable>,
    },
    /// Rows of table number `table` of `CopyBegin::tables`.
    CopyRows {
        stream: &'a str,
        seq: u64,
        table: usize,
        rows: Vec<CopyRow<'a>>,
    },
    /// Replaces the follower's data with the copy, in one transaction.
    CopyEnd { stream: &'a str, seq: u64 },
    /// Change sets in log order. Every entry up to `through` has been sent
    /// completely, in this message or before it.
    Changes {
        stream: &'a str,
        seq: u64,
        events: Vec<Event<'a>>,
        through: u64,
    },
}

/// One table, index or view of the source, as `sqlite_schema` holds it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SchemaObject {
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CopyTable {
    pub name: String,
    pub columns: usize,
}

/// The rowid on the source, then the row's values in column order.
#[derive(Debug)]
pub struct CopyRow<'a>(pub &'a [Value]);

/// One transaction's changes, at (log index, position in the entry).
#[derive(Debug, Serialize)]
pub struct Event<'a> {
    pub index: u64,
    pub position: usize,
    pub rows: Vec<Row<'a>>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Row<'a> {
    /// CREATE TABLE, CREATE INDEX or CREATE VIEW, to run as it is.
    Sql { sql: &'a str },
    Change {
        table: &'a str,
        kind: Kind,
        /// The rowid on the node that ran the transaction, as text.
        rowid: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        before: Option<Values<'a>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        after: Option<Values<'a>>,
    },
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Insert,
    Update,
    Delete,
}

/// A row image in the wire form of its values.
#[derive(Debug)]
pub struct Values<'a>(pub &'a [Value]);

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum FollowerMessage {
    /// Sent once the follower accepted the WebSocket. `through` is where it
    /// is; `instance` changes each time its constructor runs.
    Hello { through: u64, instance: String },
    /// Message `seq` committed; the follower has applied every entry up to
    /// `through`.
    Ack { seq: u64, through: u64 },
    /// Message `seq` failed and nothing of it was kept.
    Error { seq: i64, message: String },
}

/// One value in its wire form.
pub struct Wire<'a>(pub &'a Value);

impl Serialize for Wire<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let tagged = |s: S, key: &'static str, value: &str| {
            let mut st = s.serialize_struct("Value", 1)?;
            st.serialize_field(key, value)?;
            st.end()
        };
        match self.0 {
            Value::Null => s.serialize_unit(),
            Value::Integer(i) => tagged(s, "i", &i.to_string()),
            Value::Real(f) if f.is_finite() => s.serialize_f64(*f),
            Value::Real(f) if *f > 0.0 => tagged(s, "r", "inf"),
            Value::Real(_) => tagged(s, "r", "-inf"),
            Value::Text(t) => s.serialize_str(t),
            Value::Blob(b) => tagged(s, "b", &base64::engine::general_purpose::STANDARD.encode(b)),
        }
    }
}

impl Serialize for Values<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for v in self.0 {
            seq.serialize_element(&Wire(v))?;
        }
        seq.end()
    }
}

impl Serialize for CopyRow<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        Values(self.0).serialize(s)
    }
}

/// The value a wire form stands for, or `None` if it is not one.
pub fn decode_value(v: &serde_json::Value) -> Option<Value> {
    use serde_json::Value as J;
    Some(match v {
        J::Null => Value::Null,
        J::Number(n) => Value::Real(n.as_f64()?),
        J::String(s) => Value::Text(s.clone()),
        J::Object(o) if o.len() == 1 => {
            let (key, inner) = o.iter().next()?;
            let inner = inner.as_str()?;
            match key.as_str() {
                "i" => Value::Integer(inner.parse().ok()?),
                "b" => Value::Blob(
                    base64::engine::general_purpose::STANDARD
                        .decode(inner)
                        .ok()?,
                ),
                "r" if inner == "inf" => Value::Real(f64::INFINITY),
                "r" if inner == "-inf" => Value::Real(f64::NEG_INFINITY),
                _ => return None,
            }
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_survive_the_wire_exactly() {
        let values = [
            Value::Null,
            Value::Integer(i64::MAX),
            Value::Integer(i64::MIN),
            Value::Integer((1 << 53) + 1),
            Value::Integer(-7),
            Value::Real(0.1),
            Value::Real(-1.5e300),
            Value::Real(5.0),
            Value::Real(f64::INFINITY),
            Value::Real(f64::NEG_INFINITY),
            Value::Text("{\"i\": \"1\"} ä 😀".into()),
            Value::Blob(vec![0, 1, 255]),
            Value::Blob(vec![]),
        ];
        let json = serde_json::to_string(&Values(&values)).unwrap();
        let back: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        let back: Vec<Value> = back.iter().map(|v| decode_value(v).unwrap()).collect();
        assert_eq!(back, values, "{json}");
        // A real that holds an integer stays a real: 5.0 is not {"i": "5"}.
        assert!(json.contains("5.0"), "{json}");
    }
}
