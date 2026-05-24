//! Built-in virtual tables: rows materialized from Rust iterators instead of
//! the B+ tree. Used for PG system catalog views (`pg_timezone_*`,
//! `information_schema.*`).

use std::sync::Arc;

use citadel::Database;
use rustc_hash::FxHashSet;

use crate::error::Result;
use crate::schema::SchemaManager;
use crate::types::{DataType, QueryResult, Value};

pub trait VirtualTable: Send + Sync {
    fn name(&self) -> &str;
    fn scan(&self, db: &Database, schema: &SchemaManager) -> Result<QueryResult>;
}

pub fn register_builtins(schema: &mut SchemaManager) {
    let entries: [Arc<dyn VirtualTable>; 6] = [
        Arc::new(PgTimezoneNames),
        Arc::new(PgTimezoneAbbrevs),
        Arc::new(InfoSchemaTables),
        Arc::new(InfoSchemaColumns),
        Arc::new(InfoSchemaKeyColumnUsage),
        Arc::new(InfoSchemaTableConstraints),
    ];
    for vt in entries {
        schema.register_virtual(vt);
    }
}

pub struct PgTimezoneNames;
impl VirtualTable for PgTimezoneNames {
    fn name(&self) -> &str {
        "pg_timezone_names"
    }
    fn scan(&self, _db: &Database, _schema: &SchemaManager) -> Result<QueryResult> {
        let columns = vec![
            "name".to_string(),
            "utc_offset".to_string(),
            "is_dst".to_string(),
        ];
        let now = jiff::Timestamp::now();
        let db = jiff::tz::db();
        let mut rows = Vec::new();
        for name in db.available() {
            if let Ok(tz) = db.get(name.as_str()) {
                let info = tz.to_offset_info(now);
                let utc_offset = Value::Interval {
                    months: 0,
                    days: 0,
                    micros: i64::from(info.offset().seconds()) * 1_000_000,
                };
                rows.push(vec![
                    Value::Text(name.to_string().into()),
                    utc_offset,
                    Value::Boolean(info.dst().is_dst()),
                ]);
            }
        }
        Ok(QueryResult { columns, rows })
    }
}

pub struct PgTimezoneAbbrevs;
impl VirtualTable for PgTimezoneAbbrevs {
    fn name(&self) -> &str {
        "pg_timezone_abbrevs"
    }
    fn scan(&self, _db: &Database, _schema: &SchemaManager) -> Result<QueryResult> {
        let columns = vec![
            "abbrev".to_string(),
            "utc_offset".to_string(),
            "is_dst".to_string(),
        ];
        let now = jiff::Timestamp::now();
        let db = jiff::tz::db();
        let mut seen: FxHashSet<String> = FxHashSet::default();
        let mut rows = Vec::new();
        for name in db.available() {
            if let Ok(tz) = db.get(name.as_str()) {
                let info = tz.to_offset_info(now);
                let abbrev = info.abbreviation().to_string();
                if !seen.insert(abbrev.clone()) {
                    continue;
                }
                let utc_offset = Value::Interval {
                    months: 0,
                    days: 0,
                    micros: i64::from(info.offset().seconds()) * 1_000_000,
                };
                rows.push(vec![
                    Value::Text(abbrev.into()),
                    utc_offset,
                    Value::Boolean(info.dst().is_dst()),
                ]);
            }
        }
        Ok(QueryResult { columns, rows })
    }
}

pub struct InfoSchemaTables;
impl VirtualTable for InfoSchemaTables {
    fn name(&self) -> &str {
        "information_schema.tables"
    }
    fn scan(&self, _db: &Database, schema: &SchemaManager) -> Result<QueryResult> {
        let columns = vec![
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "table_type".to_string(),
        ];
        let mut rows = Vec::new();
        for ts in schema.all_schemas() {
            rows.push(vec![
                Value::Text("citadel".into()),
                Value::Text("public".into()),
                Value::Text(ts.name.clone().into()),
                Value::Text("BASE TABLE".into()),
            ]);
        }
        for vn in schema.view_names() {
            rows.push(vec![
                Value::Text("citadel".into()),
                Value::Text("public".into()),
                Value::Text(vn.to_string().into()),
                Value::Text("VIEW".into()),
            ]);
        }
        rows.sort_by(|a, b| match (&a[2], &b[2]) {
            (Value::Text(x), Value::Text(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal,
        });
        Ok(QueryResult { columns, rows })
    }
}

pub struct InfoSchemaColumns;
impl VirtualTable for InfoSchemaColumns {
    fn name(&self) -> &str {
        "information_schema.columns"
    }
    fn scan(&self, _db: &Database, schema: &SchemaManager) -> Result<QueryResult> {
        let columns = vec![
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "column_name".to_string(),
            "ordinal_position".to_string(),
            "column_default".to_string(),
            "is_nullable".to_string(),
            "data_type".to_string(),
        ];
        let mut rows = Vec::new();
        let mut schemas: Vec<_> = schema.all_schemas().collect();
        schemas.sort_by(|a, b| a.name.cmp(&b.name));
        for ts in schemas {
            for col in &ts.columns {
                rows.push(vec![
                    Value::Text("citadel".into()),
                    Value::Text("public".into()),
                    Value::Text(ts.name.clone().into()),
                    Value::Text(col.name.clone().into()),
                    Value::Integer(i64::from(col.position) + 1),
                    col.default_sql
                        .as_deref()
                        .map(|s| Value::Text(s.to_string().into()))
                        .unwrap_or(Value::Null),
                    Value::Text(if col.nullable {
                        "YES".into()
                    } else {
                        "NO".into()
                    }),
                    Value::Text(data_type_name(&col.data_type).into()),
                ]);
            }
        }
        Ok(QueryResult { columns, rows })
    }
}

pub struct InfoSchemaKeyColumnUsage;
impl VirtualTable for InfoSchemaKeyColumnUsage {
    fn name(&self) -> &str {
        "information_schema.key_column_usage"
    }
    fn scan(&self, _db: &Database, schema: &SchemaManager) -> Result<QueryResult> {
        let columns = vec![
            "constraint_catalog".to_string(),
            "constraint_schema".to_string(),
            "constraint_name".to_string(),
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "column_name".to_string(),
            "ordinal_position".to_string(),
            "referenced_table_name".to_string(),
            "referenced_column_name".to_string(),
        ];
        let mut rows = Vec::new();
        let mut schemas: Vec<_> = schema.all_schemas().collect();
        schemas.sort_by(|a, b| a.name.cmp(&b.name));
        for ts in schemas {
            for (i, &col_pos) in ts.primary_key_columns.iter().enumerate() {
                let col = &ts.columns[col_pos as usize];
                rows.push(vec![
                    Value::Text("citadel".into()),
                    Value::Text("public".into()),
                    Value::Text(format!("{}_pkey", ts.name).into()),
                    Value::Text("citadel".into()),
                    Value::Text("public".into()),
                    Value::Text(ts.name.clone().into()),
                    Value::Text(col.name.clone().into()),
                    Value::Integer((i + 1) as i64),
                    Value::Null,
                    Value::Null,
                ]);
            }
            for fk in &ts.foreign_keys {
                let cname = fk
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_fkey", ts.name));
                for (i, col_pos) in fk.columns.iter().enumerate() {
                    let col = &ts.columns[*col_pos as usize];
                    let ref_col = fk.referred_columns.get(i).cloned().unwrap_or_default();
                    rows.push(vec![
                        Value::Text("citadel".into()),
                        Value::Text("public".into()),
                        Value::Text(cname.clone().into()),
                        Value::Text("citadel".into()),
                        Value::Text("public".into()),
                        Value::Text(ts.name.clone().into()),
                        Value::Text(col.name.clone().into()),
                        Value::Integer((i + 1) as i64),
                        Value::Text(fk.foreign_table.clone().into()),
                        Value::Text(ref_col.into()),
                    ]);
                }
            }
        }
        Ok(QueryResult { columns, rows })
    }
}

pub struct InfoSchemaTableConstraints;
impl VirtualTable for InfoSchemaTableConstraints {
    fn name(&self) -> &str {
        "information_schema.table_constraints"
    }
    fn scan(&self, _db: &Database, schema: &SchemaManager) -> Result<QueryResult> {
        let columns = vec![
            "constraint_catalog".to_string(),
            "constraint_schema".to_string(),
            "constraint_name".to_string(),
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "constraint_type".to_string(),
        ];
        let mut rows = Vec::new();
        let mut schemas: Vec<_> = schema.all_schemas().collect();
        schemas.sort_by(|a, b| a.name.cmp(&b.name));
        for ts in schemas {
            if !ts.primary_key_columns.is_empty() {
                rows.push(constraint_row(
                    &format!("{}_pkey", ts.name),
                    &ts.name,
                    "PRIMARY KEY",
                ));
            }
            for fk in &ts.foreign_keys {
                let cname = fk
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_fkey", ts.name));
                rows.push(constraint_row(&cname, &ts.name, "FOREIGN KEY"));
            }
            for chk in &ts.check_constraints {
                let cname = chk
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_check", ts.name));
                rows.push(constraint_row(&cname, &ts.name, "CHECK"));
            }
            for col in &ts.columns {
                if col.check_expr.is_some() {
                    let cname = col
                        .check_name
                        .clone()
                        .unwrap_or_else(|| format!("{}_{}_check", ts.name, col.name));
                    rows.push(constraint_row(&cname, &ts.name, "CHECK"));
                }
            }
            for idx in &ts.indices {
                if idx.unique {
                    rows.push(constraint_row(&idx.name, &ts.name, "UNIQUE"));
                }
            }
        }
        Ok(QueryResult { columns, rows })
    }
}

fn constraint_row(name: &str, table: &str, kind: &str) -> Vec<Value> {
    vec![
        Value::Text("citadel".into()),
        Value::Text("public".into()),
        Value::Text(name.to_string().into()),
        Value::Text("citadel".into()),
        Value::Text("public".into()),
        Value::Text(table.to_string().into()),
        Value::Text(kind.to_string().into()),
    ]
}

fn data_type_name(dt: &DataType) -> &'static str {
    match dt {
        DataType::Integer => "INTEGER",
        DataType::Real => "REAL",
        DataType::Text => "TEXT",
        DataType::Blob => "BLOB",
        DataType::Boolean => "BOOLEAN",
        DataType::Date => "DATE",
        DataType::Time => "TIME",
        DataType::Timestamp => "TIMESTAMP",
        DataType::Interval => "INTERVAL",
        DataType::Json => "JSON",
        DataType::Jsonb => "JSONB",
        DataType::Null => "NULL",
        DataType::TsVector => "TSVECTOR",
        DataType::TsQuery => "TSQUERY",
    }
}
