use citadel::Database;
use citadel_txn::write_txn::WriteTxn;

use crate::error::{Result, SqlError};
use crate::parser::{
    CreateMatviewStmt, DropMatviewStmt, Expr, QueryBody, RefreshMatviewStmt, SelectColumn,
    SelectQuery, SelectStmt,
};
use crate::schema::SchemaManager;
use crate::types::{ColumnDef, DataType, ExecutionResult, MatviewDef, TableSchema, Value};

pub(super) fn exec_create_matview(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &CreateMatviewStmt,
) -> Result<ExecutionResult> {
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let r = exec_create_matview_in_txn(&mut wtx, schema, stmt)?;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(r)
}

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
    let columns = derive_columns(&column_names, &rows);
    if columns.is_empty() {
        return Err(SqlError::Unsupported(
            "materialized view must project at least one column".into(),
        ));
    }
    // First column = PK. Non-unique → DuplicateKey at populate; user reorders/projects.
    let backing_schema = TableSchema::new(
        backing_table.clone(),
        columns,
        vec![0],
        vec![],
        vec![],
        vec![],
    );

    wtx.create_table(backing_table.as_bytes())
        .map_err(SqlError::Storage)?;
    SchemaManager::save_schema(wtx, &backing_schema)?;
    schema.register(backing_schema);

    if stmt.with_data {
        populate_backing_table(wtx, &backing_table, &rows)?;
    }

    let mv = MatviewDef {
        name: name_lower.clone(),
        select_sql: stmt.select_sql.clone(),
        backing_table: backing_table.clone(),
        with_data: stmt.with_data,
        created_at_micros: crate::datetime::txn_or_clock_micros(),
    };
    SchemaManager::save_matview(wtx, &mv)?;
    schema.register_matview(mv);
    Ok(ExecutionResult::Ok)
}

pub(super) fn exec_refresh_matview(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &RefreshMatviewStmt,
) -> Result<ExecutionResult> {
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let r = exec_refresh_matview_in_txn(&mut wtx, schema, stmt)?;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(r)
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
        ExecutionResult::Query(q) => q.rows,
        _ => Vec::new(),
    };

    if stmt.concurrently {
        diff_merge_concurrent(wtx, &mv, &rows)?;
    } else {
        wtx.table_truncate(mv.backing_table.as_bytes())
            .map_err(SqlError::Storage)?;
        populate_backing_table(wtx, &mv.backing_table, &rows)?;
    }

    Ok(ExecutionResult::Ok)
}

pub(super) fn exec_drop_matview(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &DropMatviewStmt,
) -> Result<ExecutionResult> {
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let r = exec_drop_matview_in_txn(&mut wtx, schema, stmt)?;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(r)
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

    wtx.drop_table(mv.backing_table.as_bytes())
        .map_err(SqlError::Storage)?;
    SchemaManager::delete_matview(wtx, &name_lower)?;
    schema.remove_matview(&name_lower);
    schema.remove(&mv.backing_table);
    Ok(ExecutionResult::Ok)
}

fn populate_backing_table(
    wtx: &mut WriteTxn<'_>,
    backing_table: &str,
    rows: &[Vec<Value>],
) -> Result<()> {
    let mut key_buf = Vec::with_capacity(32);
    let mut value_buf = Vec::with_capacity(256);
    for row in rows {
        let pk_val = row
            .first()
            .ok_or_else(|| SqlError::Unsupported("matview row has no columns".into()))?;
        if pk_val.is_null() {
            return Err(SqlError::NotNullViolation(
                "matview primary-key column produced NULL — first column of SELECT must be NOT NULL"
                    .into(),
            ));
        }
        encode_pk_key(pk_val, &mut key_buf);
        let non_pk: Vec<Value> = row.iter().skip(1).cloned().collect();
        crate::encoding::encode_row_into(&non_pk, &mut value_buf);
        let inserted = wtx
            .table_insert(backing_table.as_bytes(), &key_buf, &value_buf)
            .map_err(SqlError::Storage)?;
        if !inserted {
            return Err(SqlError::DuplicateKey);
        }
        key_buf.clear();
        value_buf.clear();
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
    mv: &MatviewDef,
    new_rows: &[Vec<Value>],
) -> Result<()> {
    use rustc_hash::FxHashMap;

    let mut new_by_key: FxHashMap<Vec<u8>, &Vec<Value>> = FxHashMap::default();
    let mut key_buf = Vec::with_capacity(32);
    for row in new_rows {
        let pk_val = row
            .first()
            .ok_or_else(|| SqlError::Unsupported("matview row has no columns".into()))?;
        if pk_val.is_null() {
            return Err(SqlError::NotNullViolation(
                "matview PK column produced NULL".into(),
            ));
        }
        encode_pk_key(pk_val, &mut key_buf);
        new_by_key.insert(key_buf.clone(), row);
        key_buf.clear();
    }

    let mut deletes: Vec<Vec<u8>> = Vec::new();
    let mut updates: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut decode_err: Option<SqlError> = None;
    wtx.table_for_each(mv.backing_table.as_bytes(), |key, value| {
        match decode_existing_row(key, value) {
            Ok(existing) => match new_by_key.remove(key) {
                None => deletes.push(key.to_vec()),
                Some(new_row) => {
                    if new_row != &existing {
                        let non_pk: Vec<Value> = new_row.iter().skip(1).cloned().collect();
                        let mut buf = Vec::new();
                        crate::encoding::encode_row_into(&non_pk, &mut buf);
                        updates.push((key.to_vec(), buf));
                    }
                }
            },
            Err(e) => decode_err = Some(e),
        }
        Ok(())
    })
    .map_err(SqlError::Storage)?;
    if let Some(e) = decode_err {
        return Err(e);
    }

    for storage_key in &deletes {
        wtx.table_delete(mv.backing_table.as_bytes(), storage_key)
            .map_err(SqlError::Storage)?;
    }
    for (storage_key, value) in &updates {
        wtx.table_insert(mv.backing_table.as_bytes(), storage_key, value)
            .map_err(SqlError::Storage)?;
    }
    let mut val_buf = Vec::new();
    for (storage_key, row) in &new_by_key {
        let non_pk: Vec<Value> = row.iter().skip(1).cloned().collect();
        crate::encoding::encode_row_into(&non_pk, &mut val_buf);
        wtx.table_insert(mv.backing_table.as_bytes(), storage_key, &val_buf)
            .map_err(SqlError::Storage)?;
        val_buf.clear();
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

fn derive_columns(column_names: &[String], rows: &[Vec<Value>]) -> Vec<ColumnDef> {
    column_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
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
            ColumnDef {
                name: name.to_ascii_lowercase(),
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
                collation: crate::types::Collation::Binary,
            }
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
                let lower = name.to_ascii_lowercase();
                if matches!(
                    lower.as_str(),
                    "now"
                        | "random"
                        | "current_timestamp"
                        | "current_date"
                        | "current_time"
                        | "localtimestamp"
                        | "localtime"
                ) {
                    return Err(SqlError::Unsupported(format!(
                        "non-deterministic function '{lower}' in matview definition"
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
