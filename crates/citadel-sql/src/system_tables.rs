//! Built-in virtual tables: rows materialized from Rust iterators instead of
//! the B+ tree. Used for PG system catalog views (`pg_timezone_*`,
//! `information_schema.*`).

use std::sync::Arc;

use rustc_hash::FxHashSet;

use crate::error::Result;
use crate::schema::SchemaManager;
use crate::types::{DataType, QueryResult, Value};

pub trait VirtualTable: Send + Sync {
    fn name(&self) -> &str;
    fn scan(&self, schema: &SchemaManager) -> Result<QueryResult>;
}

pub fn register_builtins(schema: &mut SchemaManager) {
    let entries: [Arc<dyn VirtualTable>; 9] = [
        Arc::new(PgTimezoneNames),
        Arc::new(PgTimezoneAbbrevs),
        Arc::new(InfoSchemaTables),
        Arc::new(InfoSchemaColumns),
        Arc::new(InfoSchemaKeyColumnUsage),
        Arc::new(InfoSchemaTableConstraints),
        Arc::new(InfoSchemaTriggers),
        Arc::new(CitadelTriggersStatus),
        Arc::new(PgMatviews),
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
    fn scan(&self, _schema: &SchemaManager) -> Result<QueryResult> {
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
    fn scan(&self, _schema: &SchemaManager) -> Result<QueryResult> {
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
    fn scan(&self, schema: &SchemaManager) -> Result<QueryResult> {
        let columns = vec![
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "table_type".to_string(),
        ];
        let mut rows = Vec::new();
        for ts in schema.all_schemas() {
            // Listed separately below as MATERIALIZED VIEW.
            if schema.get_matview(&ts.name).is_some() {
                continue;
            }
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
        for mv in schema.all_matviews() {
            rows.push(vec![
                Value::Text("citadel".into()),
                Value::Text("public".into()),
                Value::Text(mv.name.clone().into()),
                Value::Text("MATERIALIZED VIEW".into()),
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
    fn scan(&self, schema: &SchemaManager) -> Result<QueryResult> {
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
    fn scan(&self, schema: &SchemaManager) -> Result<QueryResult> {
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
    fn scan(&self, schema: &SchemaManager) -> Result<QueryResult> {
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
        DataType::Array => "ARRAY",
        DataType::Vector { .. } => "VECTOR",
    }
}

/// One row per event for multi-event triggers (per SQL spec).
pub struct InfoSchemaTriggers;
impl VirtualTable for InfoSchemaTriggers {
    fn name(&self) -> &str {
        "information_schema.triggers"
    }
    fn scan(&self, schema: &SchemaManager) -> Result<QueryResult> {
        let columns = vec![
            "trigger_catalog".to_string(),
            "trigger_schema".to_string(),
            "trigger_name".to_string(),
            "event_manipulation".to_string(),
            "event_object_catalog".to_string(),
            "event_object_schema".to_string(),
            "event_object_table".to_string(),
            "action_order".to_string(),
            "action_condition".to_string(),
            "action_statement".to_string(),
            "action_orientation".to_string(),
            "action_timing".to_string(),
            "action_reference_old_table".to_string(),
            "action_reference_new_table".to_string(),
            "action_reference_old_row".to_string(),
            "action_reference_new_row".to_string(),
            "created".to_string(),
        ];
        let mut all: Vec<&crate::types::TriggerDef> = schema.all_triggers().collect();
        all.sort_by(|a, b| a.target.cmp(&b.target).then(a.name.cmp(&b.name)));
        let mut order_in_group: rustc_hash::FxHashMap<(String, String, String, String), i64> =
            rustc_hash::FxHashMap::default();
        let mut rows = Vec::new();
        for td in all {
            for ev in &td.events {
                let event_name = match ev {
                    crate::parser::TriggerEvent::Insert => "INSERT".to_string(),
                    crate::parser::TriggerEvent::Update(_) => "UPDATE".to_string(),
                    crate::parser::TriggerEvent::Delete => "DELETE".to_string(),
                };
                let timing_name = match td.timing {
                    crate::parser::TriggerTiming::Before => "BEFORE".to_string(),
                    crate::parser::TriggerTiming::After => "AFTER".to_string(),
                    crate::parser::TriggerTiming::InsteadOf => "INSTEAD OF".to_string(),
                };
                let orientation = match td.granularity {
                    crate::parser::TriggerGranularity::ForEachRow => "ROW".to_string(),
                    crate::parser::TriggerGranularity::ForEachStatement => "STATEMENT".to_string(),
                };
                let key = (
                    td.target.clone(),
                    event_name.clone(),
                    timing_name.clone(),
                    orientation.clone(),
                );
                let order = order_in_group.entry(key).or_insert(0);
                *order += 1;
                let order_val = *order;
                let action_condition = match &td.when_sql {
                    Some(s) => Value::Text(s.clone().into()),
                    None => Value::Null,
                };
                let old_table_alias = td
                    .referencing
                    .as_ref()
                    .and_then(|r| r.old_table_alias.clone());
                let new_table_alias = td
                    .referencing
                    .as_ref()
                    .and_then(|r| r.new_table_alias.clone());
                rows.push(vec![
                    Value::Text("citadel".into()),
                    Value::Text("public".into()),
                    Value::Text(td.name.clone().into()),
                    Value::Text(event_name.into()),
                    Value::Text("citadel".into()),
                    Value::Text("public".into()),
                    Value::Text(td.target.clone().into()),
                    Value::Integer(order_val),
                    action_condition,
                    Value::Text(td.body_sql.clone().into()),
                    Value::Text(orientation.into()),
                    Value::Text(timing_name.into()),
                    old_table_alias
                        .map(|s| Value::Text(s.into()))
                        .unwrap_or(Value::Null),
                    new_table_alias
                        .map(|s| Value::Text(s.into()))
                        .unwrap_or(Value::Null),
                    Value::Null,
                    Value::Null,
                    Value::Timestamp(td.created_at_micros),
                ]);
            }
        }
        Ok(QueryResult { columns, rows })
    }
}

/// Surfaces `enabled` status — PG hides this from `information_schema.triggers`.
pub struct CitadelTriggersStatus;
impl VirtualTable for CitadelTriggersStatus {
    fn name(&self) -> &str {
        "citadel_triggers_status"
    }
    fn scan(&self, schema: &SchemaManager) -> Result<QueryResult> {
        let columns = vec![
            "trigger_name".to_string(),
            "table_name".to_string(),
            "enabled".to_string(),
        ];
        let mut all: Vec<&crate::types::TriggerDef> = schema.all_triggers().collect();
        all.sort_by(|a, b| a.target.cmp(&b.target).then(a.name.cmp(&b.name)));
        let rows = all
            .into_iter()
            .map(|td| {
                vec![
                    Value::Text(td.name.clone().into()),
                    Value::Text(td.target.clone().into()),
                    Value::Boolean(td.enabled),
                ]
            })
            .collect();
        Ok(QueryResult { columns, rows })
    }
}

/// `matviewowner` and `tablespace` are constants — citadel has no permission/storage concept.
pub struct PgMatviews;
impl VirtualTable for PgMatviews {
    fn name(&self) -> &str {
        "pg_matviews"
    }
    fn scan(&self, schema: &SchemaManager) -> Result<QueryResult> {
        let columns = vec![
            "schemaname".to_string(),
            "matviewname".to_string(),
            "matviewowner".to_string(),
            "tablespace".to_string(),
            "hasindexes".to_string(),
            "ispopulated".to_string(),
            "definition".to_string(),
        ];
        let mut entries: Vec<&crate::types::MatviewDef> = schema.all_matviews().collect();
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let rows = entries
            .into_iter()
            .map(|mv| {
                let hasindexes = schema
                    .get(&mv.backing_table)
                    .map(|ts| !ts.indices.is_empty())
                    .unwrap_or(false);
                vec![
                    Value::Text("public".into()),
                    Value::Text(mv.name.clone().into()),
                    Value::Text("citadel".into()),
                    Value::Null,
                    Value::Boolean(hasindexes),
                    Value::Boolean(mv.with_data),
                    Value::Text(mv.select_sql.clone().into()),
                ]
            })
            .collect();
        Ok(QueryResult { columns, rows })
    }
}
