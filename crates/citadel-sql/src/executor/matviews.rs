use citadel::Database;
use citadel_txn::write_txn::WriteTxn;

use crate::error::{Result, SqlError};
use crate::parser::{
    CreateMatviewStmt, DropMatviewStmt, Expr, QueryBody, RefreshMatviewStmt, SelectColumn,
    SelectQuery, SelectStmt,
};
use crate::schema::SchemaManager;
use crate::types::{ColumnDef, DataType, ExecutionResult, MatviewDef, TableSchema, Value};

pub(super) fn exec_create_matview_in_txn(
    wtx: &mut WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &CreateMatviewStmt,
) -> Result<ExecutionResult> {
    let name_lower = stmt.name.to_ascii_lowercase();

    if schema.get_matview(&name_lower).is_some() {
        if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::Unsupported(format!(
            "materialized view '{}' already exists",
            stmt.name
        )));
    }
    if schema.contains(&name_lower) || schema.get_view(&name_lower).is_some() {
        return Err(SqlError::Unsupported(format!(
            "name '{}' already in use",
            stmt.name
        )));
    }

    reject_non_deterministic(&stmt.select_parsed)?;

    let qr = super::cte::exec_select_query_in_txn(wtx, schema, &stmt.select_parsed)?;
    let (column_names, rows) = match qr {
        ExecutionResult::Query(q) => (q.columns, q.rows),
        _ => {
            return Err(SqlError::Unsupported(
                "matview body did not return a result set".into(),
            ));
        }
    };

    let backing_table = MatviewDef::backing_table_name(&name_lower);
    let collations = super::dml::query_output_collations(
        schema,
        &super::CteContext::default(),
        &stmt.select_parsed,
        column_names.len(),
    );
    TableSchema::validate_column_count(column_names.len())?;
    crate::encoding::validate_row_column_count(column_names.len().saturating_sub(1))?;
    let columns = derive_columns(&column_names, &rows, &collations)?;
    if columns.is_empty() {
        return Err(SqlError::Unsupported(
            "materialized view must project at least one column".into(),
        ));
    }
    // First column = PK. Non-unique → DuplicateKey at populate; user reorders/projects.
    let mut backing_schema = TableSchema::new(
        backing_table.clone(),
        columns,
        vec![0],
        vec![],
        vec![],
        vec![],
    );

    if let Some(index) =
        super::constraint_indexes::primary_key_index_to_add(&backing_schema, |name| {
            schema
                .all_schemas()
                .any(|table| table.index_by_name(name).is_some())
        })
    {
        backing_schema.indices.push(index);
    }

    with_matview_savepoint(wtx, schema, |wtx, schema| {
        wtx.create_table(backing_table.as_bytes())
            .map_err(SqlError::Storage)?;
        super::ddl::create_index_tables(wtx, &backing_schema)?;
        if stmt.with_data {
            populate_backing_table(wtx, &backing_schema, &rows)?;
        }
        SchemaManager::save_schema(wtx, &backing_schema)?;
        schema.register(backing_schema);
        let mv = MatviewDef {
            name: name_lower.clone(),
            select_sql: stmt.select_sql.clone(),
            backing_table,
            with_data: stmt.with_data,
            created_at_micros: crate::datetime::txn_or_clock_micros(),
        };
        SchemaManager::save_matview(wtx, &mv)?;
        schema.register_matview(mv);
        Ok(ExecutionResult::Ok)
    })
}

pub(super) fn exec_refresh_matview(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &RefreshMatviewStmt,
) -> Result<ExecutionResult> {
    debug_assert!(stmt.concurrently);
    let mut rtx = db.begin_read();
    schema.admit_read(db, &mut rtx)?;
    super::reject_legacy_volatile_schema(schema)?;
    let name_lower = stmt.name.to_ascii_lowercase();

    let mv_snapshot = {
        let mv = schema
            .get_matview(&name_lower)
            .ok_or_else(|| SqlError::TableNotFound(stmt.name.clone()))?
            .clone();
        if !mv.with_data {
            return Err(SqlError::Unsupported(format!(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY cannot be used when the materialized view '{}' is not populated",
                stmt.name
            )));
        }
        let backing = schema
            .get(&mv.backing_table)
            .ok_or_else(|| SqlError::TableNotFound(mv.backing_table.clone()))?;
        if !backing.indices.iter().any(|idx| idx.unique) {
            return Err(SqlError::Unsupported(format!(
                "cannot refresh materialized view '{}' concurrently — it requires a UNIQUE index",
                stmt.name
            )));
        }
        mv
    };

    let source_mv = mv_snapshot.try_serialize()?;
    let source_backing = schema
        .get(&mv_snapshot.backing_table)
        .ok_or_else(|| SqlError::TableNotFound(mv_snapshot.backing_table.clone()))?
        .try_serialize()?;
    let parsed = crate::parser::parse_sql(&mv_snapshot.select_sql)?;
    let sq = match parsed {
        crate::parser::Statement::Select(sq) => *sq,
        _ => {
            return Err(SqlError::Unsupported(
                "stored matview body is not SELECT".into(),
            ));
        }
    };
    reject_non_deterministic(&sq)?;
    let rows = {
        let qr = super::cte::exec_select_query_with_read(&mut rtx, schema, &sq)?;
        match qr {
            ExecutionResult::Query(q) => {
                TableSchema::validate_column_count(q.columns.len())?;
                crate::encoding::validate_row_column_count(q.columns.len().saturating_sub(1))?;
                q.rows
            }
            _ => Vec::new(),
        }
    };

    drop(rtx);
    #[cfg(test)]
    catalog_tests::after_refresh_read(db);
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let admission_snapshot = schema.admit_write(db, &mut wtx)?;
    let dml_snapshot = schema.save_dml_snapshot();
    let mut committed = false;
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        super::reject_legacy_volatile_schema(schema)?;

        let mv = schema
            .get_matview(&name_lower)
            .ok_or_else(|| SqlError::TableNotFound(stmt.name.clone()))?
            .clone();
        let backing = schema
            .get(&mv.backing_table)
            .ok_or_else(|| SqlError::TableNotFound(mv.backing_table.clone()))?;
        if mv.try_serialize()? != source_mv || backing.try_serialize()? != source_backing {
            return Err(SqlError::InvalidValue(
                "materialized view definition changed during concurrent refresh".into(),
            ));
        }
        if !backing.indices.iter().any(|idx| idx.unique) {
            return Err(SqlError::Unsupported(format!(
                "cannot refresh materialized view '{}' concurrently — it requires a UNIQUE index",
                stmt.name
            )));
        }

        let backing = backing.clone();
        with_matview_savepoint(&mut wtx, schema, |wtx, schema| {
            diff_merge_concurrent(wtx, schema, &backing, &rows)
        })?;
        super::helpers::drain_deferred_fk_checks(&mut wtx, schema)?;
        super::commit_with_ann_publication(wtx, schema)?;
        committed = true;
        Ok(ExecutionResult::Ok)
    }));
    if !committed {
        if let Some(snapshot) = admission_snapshot {
            schema.restore_snapshot(snapshot);
        } else {
            schema.restore_dml_snapshot(dml_snapshot);
        }
    }
    match outcome {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

pub(super) fn exec_refresh_matview_in_txn(
    wtx: &mut WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &RefreshMatviewStmt,
) -> Result<ExecutionResult> {
    let name_lower = stmt.name.to_ascii_lowercase();
    let mv = schema
        .get_matview(&name_lower)
        .ok_or_else(|| SqlError::TableNotFound(stmt.name.clone()))?
        .clone();

    if stmt.concurrently && !mv.with_data {
        return Err(SqlError::Unsupported(format!(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY cannot be used when the materialized view '{}' is not populated",
            stmt.name
        )));
    }

    if stmt.concurrently {
        let backing = schema
            .get(&mv.backing_table)
            .ok_or_else(|| SqlError::TableNotFound(mv.backing_table.clone()))?;
        if !backing.indices.iter().any(|idx| idx.unique) {
            return Err(SqlError::Unsupported(format!(
                "cannot refresh materialized view '{}' concurrently — it requires a UNIQUE index",
                stmt.name
            )));
        }
    }

    let parsed = crate::parser::parse_sql(&mv.select_sql)?;
    let sq = match parsed {
        crate::parser::Statement::Select(sq) => *sq,
        _ => {
            return Err(SqlError::Unsupported(
                "stored matview body is not SELECT".into(),
            ));
        }
    };
    reject_non_deterministic(&sq)?;
    let qr = super::cte::exec_select_query_in_txn(wtx, schema, &sq)?;
    let rows = match qr {
        ExecutionResult::Query(q) => {
            TableSchema::validate_column_count(q.columns.len())?;
            crate::encoding::validate_row_column_count(q.columns.len().saturating_sub(1))?;
            q.rows
        }
        _ => Vec::new(),
    };

    let backing = schema
        .get(&mv.backing_table)
        .ok_or_else(|| SqlError::TableNotFound(mv.backing_table.clone()))?
        .clone();
    with_matview_savepoint(wtx, schema, |wtx, schema| {
        if stmt.concurrently {
            diff_merge_concurrent(wtx, schema, &backing, &rows)?;
        } else {
            let had_rows = wtx
                .table_entry_count(backing.name.as_bytes())
                .map_err(SqlError::Storage)?
                != 0;
            mark_backing_changed(wtx, schema, &backing)?;
            wtx.table_truncate(backing.name.as_bytes())
                .map_err(SqlError::Storage)?;
            for index in &backing.indices {
                wtx.table_truncate(&TableSchema::index_table_name(&backing.name, &index.name))
                    .map_err(SqlError::Storage)?;
            }
            #[cfg(test)]
            mutation_tests::after_removal();
            populate_backing_table(wtx, &backing, &rows)?;
            if had_rows {
                check_inbound_references(wtx, schema, &backing)?;
            }
        }
        if !mv.with_data {
            let mut updated = mv.clone();
            updated.with_data = true;
            SchemaManager::save_matview(wtx, &updated)?;
            schema.register_matview(updated);
        }
        Ok(ExecutionResult::Ok)
    })
}

pub(super) fn exec_drop_matview_in_txn(
    wtx: &mut WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &DropMatviewStmt,
) -> Result<ExecutionResult> {
    let name_lower = stmt.name.to_ascii_lowercase();
    let mv = match schema.get_matview(&name_lower) {
        Some(m) => m.clone(),
        None => {
            if stmt.if_exists {
                return Ok(ExecutionResult::Ok);
            }
            return Err(SqlError::TableNotFound(stmt.name.clone()));
        }
    };

    let mut dependents: Vec<String> = Vec::new();
    for (vname, vd) in schema
        .view_names()
        .iter()
        .filter_map(|n| schema.get_view(n).map(|v| (n.to_string(), v)))
    {
        if references_matview(&vd.sql, &name_lower) {
            dependents.push(format!("view '{vname}'"));
        }
    }
    for other_mv in schema.all_matviews() {
        if other_mv.name != name_lower && references_matview(&other_mv.select_sql, &name_lower) {
            dependents.push(format!("materialized view '{}'", other_mv.name));
        }
    }
    if !dependents.is_empty() && !stmt.cascade {
        return Err(SqlError::Unsupported(format!(
            "cannot drop materialized view '{}': depended on by {}",
            stmt.name,
            dependents.join(", ")
        )));
    }

    if stmt.cascade {
        let view_dependents: Vec<String> = schema
            .view_names()
            .iter()
            .filter_map(|n| {
                let v = schema.get_view(n)?;
                if references_matview(&v.sql, &name_lower) {
                    Some(n.to_string())
                } else {
                    None
                }
            })
            .collect();
        for vn in view_dependents {
            SchemaManager::delete_view(wtx, &vn)?;
            schema.remove_view(&vn);
        }
        let mv_dependents: Vec<String> = schema
            .all_matviews()
            .filter(|m| m.name != name_lower && references_matview(&m.select_sql, &name_lower))
            .map(|m| m.name.clone())
            .collect();
        for mvn in mv_dependents {
            let inner_stmt = DropMatviewStmt {
                name: mvn,
                if_exists: true,
                cascade: true,
            };
            exec_drop_matview_in_txn(wtx, schema, &inner_stmt)?;
        }
    }

    super::ddl::exec_drop_table_in_txn(
        wtx,
        schema,
        &crate::parser::DropTableStmt {
            name: mv.backing_table.clone(),
            if_exists: false,
        },
    )?;
    SchemaManager::delete_matview(wtx, &name_lower)?;
    schema.remove_matview(&name_lower);
    Ok(ExecutionResult::Ok)
}

/// Keep the backing rows, all index trees, and catalog publication atomic even
/// when called with a public caller-owned writer.
fn with_matview_savepoint<T>(
    wtx: &mut WriteTxn<'_>,
    schema: &mut SchemaManager,
    mutate: impl FnOnce(&mut WriteTxn<'_>, &mut SchemaManager) -> Result<T>,
) -> Result<T> {
    let catalog = schema.save_snapshot();
    let savepoint = wtx.begin_savepoint();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let result = mutate(wtx, schema)?;
        super::helpers::check_cancel(wtx.cancel_token())?;
        Ok(result)
    }));
    match outcome {
        Ok(Ok(result)) => Ok(result),
        failure => {
            wtx.restore_snapshot(savepoint);
            schema.restore_snapshot(catalog);
            match failure {
                Ok(Err(error)) => Err(error),
                Err(payload) => std::panic::resume_unwind(payload),
                Ok(Ok(_)) => unreachable!(),
            }
        }
    }
}

fn row_primary_key<'a>(table: &TableSchema, row: &'a [Value]) -> Result<&'a Value> {
    if row.len() != table.columns.len() {
        return Err(SqlError::InvalidValue(
            "materialized view row width changed".into(),
        ));
    }
    let key = row
        .first()
        .ok_or_else(|| SqlError::Unsupported("matview row has no columns".into()))?;
    if key.is_null() {
        return Err(SqlError::NotNullViolation(
            "matview primary-key column produced NULL — first column of SELECT must be NOT NULL"
                .into(),
        ));
    }
    Ok(key)
}

fn populate_backing_table(
    wtx: &mut WriteTxn<'_>,
    backing: &TableSchema,
    rows: &[Vec<Value>],
) -> Result<()> {
    let mut key_buf = Vec::with_capacity(32);
    let mut value_buf = Vec::with_capacity(256);
    for row in rows {
        super::helpers::check_cancel(wtx.cancel_token())?;
        let pk_val = row_primary_key(backing, row)?;
        encode_pk_key(pk_val, &mut key_buf);
        crate::encoding::encode_row_into(&row[1..], &mut value_buf);
        if !wtx
            .table_insert(backing.name.as_bytes(), &key_buf, &value_buf)
            .map_err(SqlError::Storage)?
        {
            return Err(SqlError::DuplicateKey);
        }
        super::helpers::insert_index_entries(wtx, backing, row, std::slice::from_ref(pk_val))?;
    }
    Ok(())
}

fn mark_backing_changed(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    backing: &TableSchema,
) -> Result<()> {
    super::helpers::check_cancel(wtx.cancel_token())?;
    schema.mark_dml(&backing.name);
    if backing.has_ann_index() {
        super::ann_persist::purge_segment(wtx, &backing.name)?;
    }
    Ok(())
}

/// REFRESH replaces a relation, rather than applying row-level referential
/// actions. Check surviving references against the complete final relation.
/// Initially-deferred constraints retain their ordinary commit-time semantics.
fn check_inbound_references(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    backing: &TableSchema,
) -> Result<()> {
    let mut checked = rustc_hash::FxHashSet::default();
    for (name, _) in schema.child_fks_for(&backing.name) {
        if !checked.insert(name) {
            continue;
        }
        let child = schema
            .get(name)
            .ok_or_else(|| SqlError::TableNotFound(name.into()))?;
        let foreign_keys = child
            .foreign_keys
            .iter()
            .filter(|fk| fk.foreign_table == backing.name)
            .collect::<Vec<_>>();
        super::fk::check_table_references(wtx, schema, child, &foreign_keys)?;
    }
    Ok(())
}

fn encode_pk_key(val: &Value, buf: &mut Vec<u8>) {
    buf.clear();
    match val {
        Value::Integer(i) => crate::encoding::encode_int_key_into(*i, buf),
        other => crate::encoding::encode_key_value_into(other, buf),
    }
}

fn diff_merge_concurrent(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    backing: &TableSchema,
    new_rows: &[Vec<Value>],
) -> Result<()> {
    use rustc_hash::FxHashMap;
    let mut new_by_key: FxHashMap<Vec<u8>, &Vec<Value>> = FxHashMap::default();
    let mut key_buf = Vec::with_capacity(32);
    for row in new_rows {
        super::helpers::check_cancel(wtx.cancel_token())?;
        encode_pk_key(row_primary_key(backing, row)?, &mut key_buf);
        if new_by_key.insert(key_buf.clone(), row).is_some() {
            return Err(SqlError::DuplicateKey);
        }
    }

    // Preserve unchanged rows and their index entries. Retain the old image for
    // changed rows so every obsolete UNIQUE value is removed before any final
    // value is inserted (a valid swap must not fail on an intermediate state).
    let mut changes = Vec::new();
    let mut decode_error = None;
    wtx.table_scan_from(backing.name.as_bytes(), b"", |key, value| {
        match decode_existing_row(key, value) {
            Ok(existing) => {
                let replacement = new_by_key.remove(key);
                if replacement != Some(&existing) {
                    changes.push((key.to_vec(), existing, replacement));
                }
            }
            Err(error) => {
                decode_error = Some(error);
                return Ok(false);
            }
        }
        Ok(true)
    })
    .map_err(SqlError::Storage)?;
    if let Some(error) = decode_error {
        return Err(error);
    }
    if changes.is_empty() && new_by_key.is_empty() {
        return Ok(());
    }

    mark_backing_changed(wtx, schema, backing)?;
    for (key, old, replacement) in &changes {
        super::helpers::check_cancel(wtx.cancel_token())?;
        super::helpers::delete_index_entries(wtx, backing, old, &old[..1])?;
        if replacement.is_none() {
            wtx.table_delete(backing.name.as_bytes(), key)
                .map_err(SqlError::Storage)?;
        }
    }
    #[cfg(test)]
    mutation_tests::after_removal();
    let mut value_buf = Vec::new();
    for (key, _, replacement) in &changes {
        if let Some(row) = replacement {
            super::helpers::check_cancel(wtx.cancel_token())?;
            crate::encoding::encode_row_into(&row[1..], &mut value_buf);
            wtx.table_insert(backing.name.as_bytes(), key, &value_buf)
                .map_err(SqlError::Storage)?;
            super::helpers::insert_index_entries(wtx, backing, row, &row[..1])?;
        }
    }
    for (key, row) in new_by_key {
        super::helpers::check_cancel(wtx.cancel_token())?;
        crate::encoding::encode_row_into(&row[1..], &mut value_buf);
        if !wtx
            .table_insert(backing.name.as_bytes(), &key, &value_buf)
            .map_err(SqlError::Storage)?
        {
            return Err(SqlError::DuplicateKey);
        }
        super::helpers::insert_index_entries(wtx, backing, row, &row[..1])?;
    }
    if !changes.is_empty() {
        check_inbound_references(wtx, schema, backing)?;
    }
    Ok(())
}

fn decode_existing_row(key: &[u8], value: &[u8]) -> Result<Vec<Value>> {
    let pk = decode_pk_value(key)?;
    let mut row = vec![pk];
    let rest = crate::encoding::decode_row(value)?;
    row.extend(rest);
    Ok(row)
}

fn decode_pk_value(key: &[u8]) -> Result<Value> {
    if let Ok(v) = crate::encoding::decode_pk_integer(key) {
        return Ok(Value::Integer(v));
    }
    let (val, _) = crate::encoding::decode_key_value(key)?;
    Ok(val)
}

fn derive_columns(
    column_names: &[String],
    rows: &[Vec<Value>],
    collations: &[crate::types::Collation],
) -> Result<Vec<ColumnDef>> {
    let mut seen = rustc_hash::FxHashSet::default();
    column_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let lower = name.to_ascii_lowercase();
            if !seen.insert(lower.clone()) {
                return Err(SqlError::DuplicateColumn(name.clone()));
            }
            let data_type = rows
                .iter()
                .find_map(|row| {
                    let v = row.get(i)?;
                    if v.is_null() {
                        None
                    } else {
                        Some(v.data_type())
                    }
                })
                .unwrap_or(DataType::Text);
            Ok(ColumnDef {
                name: lower,
                data_type,
                nullable: i != 0,
                position: i as u16,
                default_expr: None,
                default_sql: None,
                check_expr: None,
                check_sql: None,
                check_name: None,
                is_with_timezone: false,
                generated_expr: None,
                generated_sql: None,
                generated_kind: None,
                collation: collations
                    .get(i)
                    .copied()
                    .unwrap_or(crate::types::Collation::Binary),
            })
        })
        .collect()
}

fn reject_non_deterministic(sq: &SelectQuery) -> Result<()> {
    fn walk_body(body: &QueryBody) -> Result<()> {
        match body {
            QueryBody::Select(sel) => walk_select(sel),
            QueryBody::Compound(c) => {
                walk_body(&c.left)?;
                walk_body(&c.right)
            }
            QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_) => Err(
                SqlError::Unsupported("matview body must be a SELECT statement".into()),
            ),
        }
    }
    fn walk_select(sel: &SelectStmt) -> Result<()> {
        for col in &sel.columns {
            if let SelectColumn::Expr { expr, .. } = col {
                walk_expr(expr)?;
            }
        }
        if let Some(w) = &sel.where_clause {
            walk_expr(w)?;
        }
        for g in &sel.group_by {
            walk_expr(g)?;
        }
        if let Some(h) = &sel.having {
            walk_expr(h)?;
        }
        for o in &sel.order_by {
            walk_expr(&o.expr)?;
        }
        Ok(())
    }
    fn walk_expr(expr: &Expr) -> Result<()> {
        match expr {
            Expr::Function { name, args, .. } => {
                if crate::eval::is_volatile_function_expr(&name.to_ascii_uppercase(), args) {
                    return Err(SqlError::Unsupported(format!(
                        "non-deterministic function '{}' in matview definition",
                        name.to_ascii_lowercase()
                    )));
                }
                for a in args {
                    walk_expr(a)?;
                }
                Ok(())
            }
            Expr::BinaryOp { left, right, .. } => {
                walk_expr(left)?;
                walk_expr(right)
            }
            Expr::UnaryOp { expr, .. } => walk_expr(expr),
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(o) = operand {
                    walk_expr(o)?;
                }
                for (w, t) in conditions {
                    walk_expr(w)?;
                    walk_expr(t)?;
                }
                if let Some(e) = else_result {
                    walk_expr(e)?;
                }
                Ok(())
            }
            Expr::Coalesce(args) => {
                for a in args {
                    walk_expr(a)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    walk_body(&sq.body)
}

fn references_matview(sql: &str, name: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    let needle = name.to_ascii_lowercase();
    for prefix in ["from ", "join ", "into ", "update ", "table "] {
        if let Some(idx) = lower.find(prefix) {
            let after = &lower[idx + prefix.len()..];
            if let Some(token) = after
                .split(|c: char| !c.is_alphanumeric() && c != '_')
                .next()
            {
                if token == needle {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
#[path = "matviews_tests.rs"]
mod tests;

#[cfg(test)]
mod catalog_tests {
    use super::*;
    use crate::Connection;
    use citadel::{Argon2Profile, DatabaseBuilder};
    type RefreshHook = Box<dyn FnOnce(&Database)>;
    thread_local! { static AFTER_READ: std::cell::RefCell<Option<RefreshHook>> = const { std::cell::RefCell::new(None) }; }
    pub(super) fn after_refresh_read(db: &Database) {
        let hook = AFTER_READ.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook(db);
        }
    }
    fn fixture() -> Database {
        let db = DatabaseBuilder::new("")
            .passphrase(b"refresh-catalog")
            .argon2_profile(Argon2Profile::Iot)
            .create_in_memory()
            .unwrap();
        {
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE source (id INTEGER PRIMARY KEY, value INTEGER)")
                .unwrap();
            conn.execute("INSERT INTO source VALUES (1, 10)").unwrap();
            conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, value FROM source")
                .unwrap();
            conn.execute("CREATE UNIQUE INDEX mv_key ON mv(id)")
                .unwrap();
        }
        db
    }
    #[test]
    fn concurrent_refresh_rechecks_its_backing_definition_before_writes() {
        let db = fixture();
        let conn = Connection::open(&db).unwrap();
        conn.execute("INSERT INTO source VALUES (2, 20)").unwrap();
        AFTER_READ.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(|db| {
                let other = Connection::open(db).unwrap();
                other
                    .execute("CREATE UNIQUE INDEX mv_value ON mv(value)")
                    .unwrap();
            }))
        });
        let error = conn
            .execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
            .unwrap_err();
        assert!(error.to_string().contains("definition changed"));
        assert_eq!(
            conn.query("SELECT id FROM mv ORDER BY id").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
    }
    #[test]
    fn concurrent_refresh_keeps_its_read_snapshot_when_only_source_data_changes() {
        let db = fixture();
        let conn = Connection::open(&db).unwrap();
        AFTER_READ.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(|db| {
                let other = Connection::open(db).unwrap();
                other.execute("INSERT INTO source VALUES (2, 20)").unwrap();
            }))
        });
        conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
            .unwrap();
        assert_eq!(
            conn.query("SELECT id FROM mv ORDER BY id").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
        conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
            .unwrap();
        assert_eq!(
            conn.query("SELECT id FROM mv ORDER BY id")
                .unwrap()
                .rows
                .len(),
            2
        );
    }
}

#[cfg(test)]
#[path = "matviews_mutation_tests.rs"]
mod mutation_tests;
