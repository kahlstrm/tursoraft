//! The change-set format: one transaction's row changes as read from
//! `turso_cdc`, encoded to bytes.
//!
//! Encoding, version 3 (all integers are LEB128 varints; `id` is zigzag):
//!
//! ```text
//! version: u8 = 3
//! base_index: varint
//! schema_epoch: varint
//! flags: u8   bit 0 reads_pending
//! table_count: varint, then per table: len: varint, UTF-8 bytes
//! change_count: varint, then per change:
//!     tag: u8   bits 0-1 kind (0 insert, 1 update, 2 delete),
//!               bit 2 before present, bit 3 after present, bit 4 updates present
//!     table: varint index into the table list
//!     id: zigzag varint
//!     before, after, updates when present: len: varint, bytes
//! ```
//!
//! `before`, `after` and `updates` are kept exactly as CDC wrote them: SQLite
//! binary records (see `turso_sync_engine::types::parse_bin_record`).
//!
//! Version 2 is version 3 without the flags byte; it decodes with
//! `reads_pending` false.

use crate::Error;

const VERSION: u8 = 3;

/// One transaction's changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeSet {
    /// The applied log index of the database when the transaction ran: its
    /// reads saw the entries up to this index, and, if `reads_pending` is
    /// set, the change sets after it up to this one.
    pub base_index: u64,
    /// The number of applied change sets that had changed the schema, as the
    /// transaction saw it. A change set applies only at the same schema epoch.
    pub schema_epoch: u64,
    /// The transaction read the effects of change sets after `base_index`
    /// that were not applied yet: earlier transactions of its batch, or groups
    /// still on their way through the log. Such a change set is rejected if
    /// any change set after `base_index` was rejected before it, since it read
    /// a state that never existed.
    pub reads_pending: bool,
    /// The row changes in the order the transaction made them. Schema changes
    /// are changes of the table `sqlite_schema`.
    pub changes: Vec<RowChange>,
}

/// One row change, as one `turso_cdc` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowChange {
    pub kind: ChangeKind,
    pub table: String,
    /// The row's rowid (the CDC `id` column).
    pub id: i64,
    /// The row before the change, as a SQLite record. Present for updates and
    /// deletes.
    pub before: Option<Vec<u8>>,
    /// The row after the change, as a SQLite record. Present for inserts and
    /// updates.
    pub after: Option<Vec<u8>>,
    /// For updates: a record with one changed flag per column followed by the
    /// new value of each changed column.
    pub updates: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    Insert,
    Update,
    Delete,
}

impl ChangeSet {
    /// The rows this change set wrote, as (table, rowid), in change order. A
    /// row written twice appears twice. For a table whose PRIMARY KEY is not
    /// the rowid, the rowid is the capturing node's and may differ on other
    /// nodes; the key columns in the images identify the row.
    pub fn written_rows(&self) -> impl Iterator<Item = (&str, i64)> {
        self.changes.iter().map(|c| (c.table.as_str(), c.id))
    }

    /// Whether the change set changes the schema (writes `sqlite_schema`).
    pub fn changes_schema(&self) -> bool {
        self.changes.iter().any(|c| c.table == "sqlite_schema")
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut tables: Vec<&str> = Vec::new();
        let mut table_of_change = Vec::with_capacity(self.changes.len());
        for change in &self.changes {
            let index = match tables.iter().position(|t| *t == change.table) {
                Some(index) => index,
                None => {
                    tables.push(&change.table);
                    tables.len() - 1
                }
            };
            table_of_change.push(index);
        }

        let mut out = vec![VERSION];
        put_varint(&mut out, self.base_index);
        put_varint(&mut out, self.schema_epoch);
        out.push(u8::from(self.reads_pending));
        put_varint(&mut out, tables.len() as u64);
        for table in &tables {
            put_bytes(&mut out, table.as_bytes());
        }
        put_varint(&mut out, self.changes.len() as u64);
        for (change, table) in self.changes.iter().zip(table_of_change) {
            let kind = match change.kind {
                ChangeKind::Insert => 0,
                ChangeKind::Update => 1,
                ChangeKind::Delete => 2,
            };
            let tag = kind
                | (u8::from(change.before.is_some()) << 2)
                | (u8::from(change.after.is_some()) << 3)
                | (u8::from(change.updates.is_some()) << 4);
            out.push(tag);
            put_varint(&mut out, table as u64);
            put_varint(&mut out, ((change.id << 1) ^ (change.id >> 63)) as u64);
            for image in [&change.before, &change.after, &change.updates]
                .into_iter()
                .flatten()
            {
                put_bytes(&mut out, image);
            }
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<ChangeSet, Error> {
        let mut r = Reader { bytes, pos: 0 };
        let version = r.byte()?;
        if version != 2 && version != VERSION {
            return Err(Error::Decode(format!("unknown version {version}")));
        }
        let base_index = r.varint()?;
        let schema_epoch = r.varint()?;
        let reads_pending = match version {
            2 => false,
            _ => match r.byte()? {
                0 => false,
                1 => true,
                flags => return Err(Error::Decode(format!("unknown flags {flags:#x}"))),
            },
        };
        let table_count = r.varint()?;
        let mut tables = Vec::new();
        for _ in 0..table_count {
            let name = std::str::from_utf8(r.bytes()?)
                .map_err(|e| Error::Decode(format!("table name: {e}")))?;
            tables.push(name.to_string());
        }
        let change_count = r.varint()?;
        let mut changes = Vec::new();
        for _ in 0..change_count {
            let tag = r.byte()?;
            let kind = match tag & 0b11 {
                0 => ChangeKind::Insert,
                1 => ChangeKind::Update,
                2 => ChangeKind::Delete,
                k => return Err(Error::Decode(format!("unknown change kind {k}"))),
            };
            if tag >> 5 != 0 {
                return Err(Error::Decode(format!("unknown tag bits {tag:#x}")));
            }
            let table = tables
                .get(r.varint()? as usize)
                .ok_or_else(|| Error::Decode("table index out of range".to_string()))?
                .clone();
            let zigzag = r.varint()?;
            let id = ((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64);
            let mut image = |bit: u8| -> Result<Option<Vec<u8>>, Error> {
                Ok(if tag & (1 << bit) != 0 {
                    Some(r.bytes()?.to_vec())
                } else {
                    None
                })
            };
            let before = image(2)?;
            let after = image(3)?;
            let updates = image(4)?;
            changes.push(RowChange {
                kind,
                table,
                id,
                before,
                after,
                updates,
            });
        }
        if r.pos != bytes.len() {
            return Err(Error::Decode("trailing bytes".to_string()));
        }
        Ok(ChangeSet {
            base_index,
            schema_epoch,
            reads_pending,
            changes,
        })
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn byte(&mut self) -> Result<u8, Error> {
        let b = *self
            .bytes
            .get(self.pos)
            .ok_or_else(|| Error::Decode("unexpected end".to_string()))?;
        self.pos += 1;
        Ok(b)
    }

    fn varint(&mut self) -> Result<u64, Error> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            v |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(Error::Decode("varint too long".to_string()))
    }

    fn bytes(&mut self) -> Result<&'a [u8], Error> {
        let len = self.varint()? as usize;
        let end = self
            .pos
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| Error::Decode("unexpected end".to_string()))?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_reverses_encode() {
        let set = ChangeSet {
            base_index: 300,
            schema_epoch: 7,
            reads_pending: true,
            changes: vec![
                RowChange {
                    kind: ChangeKind::Insert,
                    table: "t".to_string(),
                    id: -5,
                    before: None,
                    after: Some(vec![1, 2, 3]),
                    updates: None,
                },
                RowChange {
                    kind: ChangeKind::Update,
                    table: "sqlite_schema".to_string(),
                    id: i64::MAX,
                    before: Some(vec![]),
                    after: Some(vec![9; 200]),
                    updates: Some(vec![7]),
                },
                RowChange {
                    kind: ChangeKind::Delete,
                    table: "t".to_string(),
                    id: i64::MIN,
                    before: Some(vec![4]),
                    after: None,
                    updates: None,
                },
            ],
        };
        let bytes = set.encode();
        assert_eq!(ChangeSet::decode(&bytes).unwrap(), set);
        // Version 2 has no flags byte (here at offset 4) and reads as not
        // reading pending change sets.
        let mut v2 = bytes.clone();
        v2[0] = 2;
        assert_eq!(v2.remove(4), 1);
        let old = ChangeSet::decode(&v2).unwrap();
        assert!(!old.reads_pending);
        assert_eq!(old.changes, set.changes);
        assert!(ChangeSet::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(ChangeSet::decode(&longer).is_err());
    }
}
