//! Built-in virtual tables: rows materialized from Rust iterators instead of
//! the B+ tree. Used for PG system catalog views (`pg_timezone_*`,
//! `information_schema.*`).

use std::sync::Arc;

use rustc_hash::FxHashSet;

use crate::error::{Result, SqlError};
use crate::executor::helpers::sort_vec_by;
use crate::schema::SchemaManager;
use crate::types::{DataType, QueryResult, Value};

pub trait VirtualTable: Send + Sync {
    fn name(&self) -> &str;

    /// Materialize this table for one read operation. Implementations should poll
    /// `cancel` inside every long loop; cancellation is cooperative, so the executor
    /// cannot interrupt code that does not poll.
    fn scan(
        &self,
        schema: &SchemaManager,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<QueryResult>;
}

fn check_cancel(cancel: Option<&citadel::CancelToken>) -> Result<()> {
    match cancel {
        Some(token) => token.check().map_err(SqlError::Storage),
        None => Ok(()),
    }
}

fn check_cancel_at(cancel: Option<&citadel::CancelToken>, work: usize) -> Result<()> {
    if work.is_multiple_of(64) {
        check_cancel(cancel)?;
    }
    Ok(())
}

pub fn register_builtins(schema: &mut SchemaManager) {
    let entries: [(Arc<dyn VirtualTable>, &[&str]); 9] = [
        (Arc::new(PgTimezoneNames), PgTimezoneNames::COLUMNS),
        (Arc::new(PgTimezoneAbbrevs), PgTimezoneAbbrevs::COLUMNS),
        (Arc::new(InfoSchemaTables), InfoSchemaTables::COLUMNS),
        (Arc::new(InfoSchemaColumns), InfoSchemaColumns::COLUMNS),
        (
            Arc::new(InfoSchemaKeyColumnUsage),
            InfoSchemaKeyColumnUsage::COLUMNS,
        ),
        (
            Arc::new(InfoSchemaTableConstraints),
            InfoSchemaTableConstraints::COLUMNS,
        ),
        (Arc::new(InfoSchemaTriggers), InfoSchemaTriggers::COLUMNS),
        (
            Arc::new(CitadelTriggersStatus),
            CitadelTriggersStatus::COLUMNS,
        ),
        (Arc::new(PgMatviews), PgMatviews::COLUMNS),
    ];
    for (vt, columns) in entries {
        schema.register_builtin_virtual(vt, columns);
    }
}

pub struct PgTimezoneNames;
impl PgTimezoneNames {
    const COLUMNS: &[&str] = &["name", "utc_offset", "is_dst"];
}

impl VirtualTable for PgTimezoneNames {
    fn name(&self) -> &str {
        "pg_timezone_names"
    }
    fn scan(
        &self,
        _schema: &SchemaManager,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<QueryResult> {
        check_cancel(cancel)?;
        let columns = Self::COLUMNS
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let now = jiff::Timestamp::now();
        let db = jiff::tz::db();
        let mut rows = Vec::new();
        for (work, name) in db.available().enumerate() {
            check_cancel_at(cancel, work)?;
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
        check_cancel(cancel)?;
        Ok(QueryResult { columns, rows })
    }
}

pub struct PgTimezoneAbbrevs;
impl PgTimezoneAbbrevs {
    const COLUMNS: &[&str] = &["abbrev", "utc_offset", "is_dst"];
}

impl VirtualTable for PgTimezoneAbbrevs {
    fn name(&self) -> &str {
        "pg_timezone_abbrevs"
    }
    fn scan(
        &self,
        _schema: &SchemaManager,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<QueryResult> {
        check_cancel(cancel)?;
        let columns = Self::COLUMNS
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let now = jiff::Timestamp::now();
        let db = jiff::tz::db();
        let mut seen: FxHashSet<String> = FxHashSet::default();
        let mut rows = Vec::new();
        for (work, name) in db.available().enumerate() {
            check_cancel_at(cancel, work)?;
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
        check_cancel(cancel)?;
        Ok(QueryResult { columns, rows })
    }
}

pub struct InfoSchemaTables;
impl InfoSchemaTables {
    const COLUMNS: &[&str] = &["table_catalog", "table_schema", "table_name", "table_type"];
}

impl VirtualTable for InfoSchemaTables {
    fn name(&self) -> &str {
        "information_schema.tables"
    }
    fn scan(
        &self,
        schema: &SchemaManager,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<QueryResult> {
        check_cancel(cancel)?;
        let columns = Self::COLUMNS
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let mut rows = Vec::new();
        let mut work = 0;
        for ts in schema.all_schemas() {
            check_cancel_at(cancel, work)?;
            work += 1;
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
            check_cancel_at(cancel, work)?;
            work += 1;
            rows.push(vec![
                Value::Text("citadel".into()),
                Value::Text("public".into()),
                Value::Text(vn.to_string().into()),
                Value::Text("VIEW".into()),
            ]);
        }
        for mv in schema.all_matviews() {
            check_cancel_at(cancel, work)?;
            work += 1;
            rows.push(vec![
                Value::Text("citadel".into()),
                Value::Text("public".into()),
                Value::Text(mv.name.clone().into()),
                Value::Text("MATERIALIZED VIEW".into()),
            ]);
        }
        rows = sort_vec_by(rows, cancel, |a, b| match (&a[2], &b[2]) {
            (Value::Text(x), Value::Text(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal,
        })?;
        check_cancel(cancel)?;
        Ok(QueryResult { columns, rows })
    }
}

pub struct InfoSchemaColumns;
impl InfoSchemaColumns {
    const COLUMNS: &[&str] = &[
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "column_default",
        "is_nullable",
        "data_type",
    ];
}

impl VirtualTable for InfoSchemaColumns {
    fn name(&self) -> &str {
        "information_schema.columns"
    }
    fn scan(
        &self,
        schema: &SchemaManager,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<QueryResult> {
        check_cancel(cancel)?;
        let columns = Self::COLUMNS
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let mut rows = Vec::new();
        let schemas: Vec<_> = schema.all_schemas().collect();
        let schemas = sort_vec_by(schemas, cancel, |a, b| a.name.cmp(&b.name))?;
        let mut work = 0;
        for ts in schemas {
            for col in &ts.columns {
                check_cancel_at(cancel, work)?;
                work += 1;
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
        check_cancel(cancel)?;
        Ok(QueryResult { columns, rows })
    }
}

pub struct InfoSchemaKeyColumnUsage;
impl InfoSchemaKeyColumnUsage {
    const COLUMNS: &[&str] = &[
        "constraint_catalog",
        "constraint_schema",
        "constraint_name",
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "referenced_table_name",
        "referenced_column_name",
    ];
}

impl VirtualTable for InfoSchemaKeyColumnUsage {
    fn name(&self) -> &str {
        "information_schema.key_column_usage"
    }
    fn scan(
        &self,
        schema: &SchemaManager,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<QueryResult> {
        check_cancel(cancel)?;
        let columns = Self::COLUMNS
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let mut rows = Vec::new();
        let schemas: Vec<_> = schema.all_schemas().collect();
        let schemas = sort_vec_by(schemas, cancel, |a, b| a.name.cmp(&b.name))?;
        let mut work = 0;
        for ts in schemas {
            for (i, &col_pos) in ts.primary_key_columns.iter().enumerate() {
                check_cancel_at(cancel, work)?;
                work += 1;
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
                    check_cancel_at(cancel, work)?;
                    work += 1;
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
        check_cancel(cancel)?;
        Ok(QueryResult { columns, rows })
    }
}

pub struct InfoSchemaTableConstraints;
impl InfoSchemaTableConstraints {
    const COLUMNS: &[&str] = &[
        "constraint_catalog",
        "constraint_schema",
        "constraint_name",
        "table_catalog",
        "table_schema",
        "table_name",
        "constraint_type",
    ];
}

impl VirtualTable for InfoSchemaTableConstraints {
    fn name(&self) -> &str {
        "information_schema.table_constraints"
    }
    fn scan(
        &self,
        schema: &SchemaManager,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<QueryResult> {
        check_cancel(cancel)?;
        let columns = Self::COLUMNS
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let mut rows = Vec::new();
        let schemas: Vec<_> = schema.all_schemas().collect();
        let schemas = sort_vec_by(schemas, cancel, |a, b| a.name.cmp(&b.name))?;
        let mut work = 0;
        for ts in schemas {
            check_cancel_at(cancel, work)?;
            work += 1;
            if !ts.primary_key_columns.is_empty() {
                rows.push(constraint_row(
                    &format!("{}_pkey", ts.name),
                    &ts.name,
                    "PRIMARY KEY",
                ));
            }
            for fk in &ts.foreign_keys {
                check_cancel_at(cancel, work)?;
                work += 1;
                let cname = fk
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_fkey", ts.name));
                rows.push(constraint_row(&cname, &ts.name, "FOREIGN KEY"));
            }
            for chk in &ts.check_constraints {
                check_cancel_at(cancel, work)?;
                work += 1;
                let cname = chk
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_check", ts.name));
                rows.push(constraint_row(&cname, &ts.name, "CHECK"));
            }
            for col in &ts.columns {
                check_cancel_at(cancel, work)?;
                work += 1;
                if col.check_expr.is_some() {
                    let cname = col
                        .check_name
                        .clone()
                        .unwrap_or_else(|| format!("{}_{}_check", ts.name, col.name));
                    rows.push(constraint_row(&cname, &ts.name, "CHECK"));
                }
            }
            for idx in &ts.indices {
                check_cancel_at(cancel, work)?;
                work += 1;
                if idx.unique {
                    rows.push(constraint_row(&idx.name, &ts.name, "UNIQUE"));
                }
            }
        }
        check_cancel(cancel)?;
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
impl InfoSchemaTriggers {
    const COLUMNS: &[&str] = &[
        "trigger_catalog",
        "trigger_schema",
        "trigger_name",
        "event_manipulation",
        "event_object_catalog",
        "event_object_schema",
        "event_object_table",
        "action_order",
        "action_condition",
        "action_statement",
        "action_orientation",
        "action_timing",
        "action_reference_old_table",
        "action_reference_new_table",
        "action_reference_old_row",
        "action_reference_new_row",
        "created",
    ];
}

impl VirtualTable for InfoSchemaTriggers {
    fn name(&self) -> &str {
        "information_schema.triggers"
    }
    fn scan(
        &self,
        schema: &SchemaManager,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<QueryResult> {
        check_cancel(cancel)?;
        let columns = Self::COLUMNS
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let all: Vec<&crate::types::TriggerDef> = schema.all_triggers().collect();
        let all = sort_vec_by(all, cancel, |a, b| {
            a.target.cmp(&b.target).then(a.name.cmp(&b.name))
        })?;
        let mut order_in_group: rustc_hash::FxHashMap<(String, String, String, String), i64> =
            rustc_hash::FxHashMap::default();
        let mut rows = Vec::new();
        let mut work = 0;
        for td in all {
            for ev in &td.events {
                check_cancel_at(cancel, work)?;
                work += 1;
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
        check_cancel(cancel)?;
        Ok(QueryResult { columns, rows })
    }
}

/// Surfaces `enabled` status — PG hides this from `information_schema.triggers`.
pub struct CitadelTriggersStatus;
impl CitadelTriggersStatus {
    const COLUMNS: &[&str] = &["trigger_name", "table_name", "enabled"];
}

impl VirtualTable for CitadelTriggersStatus {
    fn name(&self) -> &str {
        "citadel_triggers_status"
    }
    fn scan(
        &self,
        schema: &SchemaManager,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<QueryResult> {
        check_cancel(cancel)?;
        let columns = Self::COLUMNS
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let all: Vec<&crate::types::TriggerDef> = schema.all_triggers().collect();
        let all = sort_vec_by(all, cancel, |a, b| {
            a.target.cmp(&b.target).then(a.name.cmp(&b.name))
        })?;
        let mut rows = Vec::with_capacity(all.len());
        for (work, td) in all.into_iter().enumerate() {
            check_cancel_at(cancel, work)?;
            rows.push(vec![
                Value::Text(td.name.clone().into()),
                Value::Text(td.target.clone().into()),
                Value::Boolean(td.enabled),
            ]);
        }
        check_cancel(cancel)?;
        Ok(QueryResult { columns, rows })
    }
}

/// `matviewowner` and `tablespace` are constants — citadel has no permission/storage concept.
pub struct PgMatviews;
impl PgMatviews {
    const COLUMNS: &[&str] = &[
        "schemaname",
        "matviewname",
        "matviewowner",
        "tablespace",
        "hasindexes",
        "ispopulated",
        "definition",
    ];
}

impl VirtualTable for PgMatviews {
    fn name(&self) -> &str {
        "pg_matviews"
    }
    fn scan(
        &self,
        schema: &SchemaManager,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<QueryResult> {
        check_cancel(cancel)?;
        let columns = Self::COLUMNS
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let entries: Vec<&crate::types::MatviewDef> = schema.all_matviews().collect();
        let entries = sort_vec_by(entries, cancel, |a, b| a.name.cmp(&b.name))?;
        let mut rows = Vec::with_capacity(entries.len());
        for (work, mv) in entries.into_iter().enumerate() {
            check_cancel_at(cancel, work)?;
            let hasindexes = schema
                .get(&mv.backing_table)
                .map(|ts| !ts.indices.is_empty())
                .unwrap_or(false);
            rows.push(vec![
                Value::Text("public".into()),
                Value::Text(mv.name.clone().into()),
                Value::Text("citadel".into()),
                Value::Null,
                Value::Boolean(hasindexes),
                Value::Boolean(mv.with_data),
                Value::Text(mv.select_sql.clone().into()),
            ]);
        }
        check_cancel(cancel)?;
        Ok(QueryResult { columns, rows })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TimezoneOverride;

    impl VirtualTable for TimezoneOverride {
        fn name(&self) -> &str {
            "PG_TIMEZONE_NAMES"
        }

        fn scan(
            &self,
            _schema: &SchemaManager,
            _cancel: Option<&citadel::CancelToken>,
        ) -> Result<QueryResult> {
            panic!("schema admission must not scan virtual data")
        }
    }

    #[test]
    fn replacing_a_builtin_clears_its_columns_across_catalog_admission() {
        let db = citadel::DatabaseBuilder::new("")
            .passphrase(b"virtual-column-metadata")
            .argon2_profile(citadel::Argon2Profile::Iot)
            .create_in_memory()
            .unwrap();
        let conn = crate::Connection::open(&db).unwrap();
        let mut schema = SchemaManager::empty();
        register_builtins(&mut schema);
        assert_eq!(
            schema.virtual_columns("pg_timezone_names"),
            Some(PgTimezoneNames::COLUMNS)
        );
        let replacement: Arc<dyn VirtualTable> = Arc::new(TimezoneOverride);
        schema.register_virtual(Arc::clone(&replacement));
        assert!(schema.virtual_columns("pg_timezone_names").is_none());

        // The first admission merges locally registered sources into the
        // persisted schema; the second reloads a changed catalog snapshot.
        for statement in [
            "CREATE TABLE first_table (id INTEGER PRIMARY KEY)",
            "CREATE TABLE second_table (id INTEGER PRIMARY KEY)",
        ] {
            conn.execute(statement).unwrap();
            let mut read = db.begin_read();
            schema.admit_read(&db, &mut read).unwrap();
            assert!(Arc::ptr_eq(
                schema.get_virtual("pg_timezone_names").unwrap(),
                &replacement,
            ));
            assert!(schema.virtual_columns("pg_timezone_names").is_none());
            assert_eq!(
                schema.virtual_columns("information_schema.tables"),
                Some(InfoSchemaTables::COLUMNS)
            );
        }

        conn.execute("CREATE TABLE third_table (id INTEGER PRIMARY KEY)")
            .unwrap();
        let mut write = db.begin_write().unwrap();
        schema.admit_owned_write(&mut write).unwrap();
        assert!(Arc::ptr_eq(
            schema.get_virtual("pg_timezone_names").unwrap(),
            &replacement,
        ));
        assert!(schema.virtual_columns("pg_timezone_names").is_none());
        write.abort();
    }
}
