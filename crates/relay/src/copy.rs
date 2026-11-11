//! A full copy of a node's database: the schema and every row, read in one
//! statement so that all of it is at one log index.

use tursoraft_node::{Node, ReadLevel, Rows, Statement, Value};

use crate::protocol::{CopyTable, SchemaObject};

/// The database of a node at one log index.
pub(crate) struct Copy {
    pub index: u64,
    /// Tables, indexes and views to create, in the order the source created
    /// them. No triggers: the source's change sets already hold what its
    /// triggers wrote.
    pub schema: Vec<SchemaObject>,
    pub tables: Vec<CopyTable>,
    /// Every row of every table: `[table number, rowid, values..., NULL...]`.
    pub rows: Rows,
}

/// Reads a copy. The schema is read first to build the statement, and that
/// statement reads the schema again with the rows; a schema change between
/// the two reads makes it try again.
pub(crate) async fn read(node: &Node) -> Result<Copy, String> {
    for _ in 0..10 {
        let schema = schema(node).await?;
        let mut tables = Vec::new();
        // Each arm: its result columns and its FROM clause.
        let mut arms: Vec<(Vec<String>, String)> = vec![
            (
                vec!["-1".into(), "log_index".into()],
                "tursoraft_applied".into(),
            ),
            (
                vec!["-2".into(), "type".into(), "name".into(), "sql".into()],
                "sqlite_schema".into(),
            ),
        ];
        for object in schema.iter().filter(|o| o.kind == "table") {
            let columns = columns(node, &object.name).await?;
            let rowid = ["rowid", "_rowid_", "oid"]
                .into_iter()
                .find(|alias| !columns.iter().any(|c| c.eq_ignore_ascii_case(alias)))
                .ok_or_else(|| format!("table {} shadows every name of the rowid", object.name))?;
            let mut exprs = vec![tables.len().to_string(), rowid.to_string()];
            exprs.extend(columns.iter().map(|c| quote(c)));
            arms.push((exprs, quote(&object.name)));
            tables.push(CopyTable {
                name: object.name.clone(),
                columns: columns.len(),
            });
        }
        // Each arm is padded with NULLs to the widest one.
        let width = arms.iter().map(|(exprs, _)| exprs.len()).max().unwrap();
        let sql = arms
            .into_iter()
            .map(|(mut exprs, from)| {
                exprs.resize(width, "NULL".into());
                format!("SELECT {} FROM {from}", exprs.join(", "))
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let rows = node
            .query(Statement::from(sql.as_str()), ReadLevel::Local)
            .await
            .map_err(|e| format!("reading the copy: {e}"))?;
        let mut index = None;
        let mut seen = Vec::new();
        for row in &rows.rows {
            match (&row[0], &row[1]) {
                (Value::Integer(-1), Value::Integer(i)) => index = Some(*i as u64),
                (Value::Integer(-2), _) => {
                    if let Some(object) = schema_object(&row[1], &row[2], &row[3]) {
                        seen.push(object);
                    }
                }
                _ => {}
            }
        }
        if seen != schema {
            continue;
        }
        return Ok(Copy {
            index: index.ok_or("the node has no applied index")?,
            schema,
            tables,
            rows,
        });
    }
    Err("the schema kept changing while it was read".into())
}

/// The source's tables, indexes and views that a follower copies.
async fn schema(node: &Node) -> Result<Vec<SchemaObject>, String> {
    let rows = node
        .query(
            Statement::from("SELECT type, name, sql FROM sqlite_schema"),
            ReadLevel::Local,
        )
        .await
        .map_err(|e| format!("reading the schema: {e}"))?;
    Ok(rows
        .rows
        .iter()
        .filter_map(|r| schema_object(&r[0], &r[1], &r[2]))
        .collect())
}

fn schema_object(kind: &Value, name: &Value, sql: &Value) -> Option<SchemaObject> {
    let (Value::Text(kind), Value::Text(name), Value::Text(sql)) = (kind, name, sql) else {
        // Automatic indexes have no SQL; the follower's CREATE TABLE makes them.
        return None;
    };
    let copied = matches!(kind.as_str(), "table" | "index" | "view") && !is_internal(name);
    copied.then(|| SchemaObject {
        kind: kind.clone(),
        name: name.clone(),
        sql: sql.clone(),
    })
}

/// Tables of SQLite, Turso and tursoraft itself, which followers do not copy.
pub(crate) fn is_internal(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    ["sqlite_", "turso_cdc", "tursoraft_"]
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

async fn columns(node: &Node, table: &str) -> Result<Vec<String>, String> {
    let rows = node
        .query(
            Statement::from(
                format!(
                    "SELECT name FROM pragma_table_info('{}') ORDER BY cid",
                    table.replace('\'', "''")
                )
                .as_str(),
            ),
            ReadLevel::Local,
        )
        .await
        .map_err(|e| format!("reading the columns of {table}: {e}"))?;
    rows.rows
        .into_iter()
        .map(|mut r| match r.pop() {
            Some(Value::Text(name)) => Ok(name),
            other => Err(format!("column name of {table}: {other:?}")),
        })
        .collect()
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
