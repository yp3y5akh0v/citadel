//! SQL executor: DDL and DML operations.

use std::collections::{BTreeMap, HashMap};

use citadel::Database;

use crate::encoding::{
    decode_column_raw, decode_columns, decode_columns_into, decode_composite_key, decode_key_value,
    decode_pk_integer, decode_pk_into, decode_row_into, encode_composite_key,
    encode_composite_key_into, encode_row, encode_row_into, row_non_pk_count, RawColumn,
};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, referenced_columns, ColumnMap};
use crate::parser::*;
use crate::planner::{self, ScanPlan};
use crate::schema::SchemaManager;
use crate::types::*;

// ── Index helpers ────────────────────────────────────────────────────

fn encode_index_key(idx: &IndexDef, row: &[Value], pk_values: &[Value]) -> Vec<u8> {
    let indexed_values: Vec<Value> = idx
        .columns
        .iter()
        .map(|&col_idx| row[col_idx as usize].clone())
        .collect();

    if idx.unique {
        let any_null = indexed_values.iter().any(|v| v.is_null());
        if !any_null {
            return encode_composite_key(&indexed_values);
        }
    }

    let mut all_values = indexed_values;
    all_values.extend_from_slice(pk_values);
    encode_composite_key(&all_values)
}

fn encode_index_value(idx: &IndexDef, row: &[Value], pk_values: &[Value]) -> Vec<u8> {
    if idx.unique {
        let indexed_values: Vec<Value> = idx
            .columns
            .iter()
            .map(|&col_idx| row[col_idx as usize].clone())
            .collect();
        let any_null = indexed_values.iter().any(|v| v.is_null());
        if !any_null {
            return encode_composite_key(pk_values);
        }
    }
    vec![]
}

fn insert_index_entries(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    row: &[Value],
    pk_values: &[Value],
) -> Result<()> {
    for idx in &table_schema.indices {
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
        let key = encode_index_key(idx, row, pk_values);
        let value = encode_index_value(idx, row, pk_values);

        let is_new = wtx
            .table_insert(&idx_table, &key, &value)
            .map_err(SqlError::Storage)?;

        if idx.unique && !is_new {
            let indexed_values: Vec<Value> = idx
                .columns
                .iter()
                .map(|&col_idx| row[col_idx as usize].clone())
                .collect();
            let any_null = indexed_values.iter().any(|v| v.is_null());
            if !any_null {
                return Err(SqlError::UniqueViolation(idx.name.clone()));
            }
        }
    }
    Ok(())
}

fn delete_index_entries(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    row: &[Value],
    pk_values: &[Value],
) -> Result<()> {
    for idx in &table_schema.indices {
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
        let key = encode_index_key(idx, row, pk_values);
        wtx.table_delete(&idx_table, &key)
            .map_err(SqlError::Storage)?;
    }
    Ok(())
}

fn index_columns_changed(idx: &IndexDef, old_row: &[Value], new_row: &[Value]) -> bool {
    idx.columns
        .iter()
        .any(|&col_idx| old_row[col_idx as usize] != new_row[col_idx as usize])
}

/// Execute a parsed SQL statement in auto-commit mode.
pub fn execute(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &Statement,
    params: &[Value],
) -> Result<ExecutionResult> {
    match stmt {
        Statement::CreateTable(ct) => exec_create_table(db, schema, ct),
        Statement::DropTable(dt) => exec_drop_table(db, schema, dt),
        Statement::CreateIndex(ci) => exec_create_index(db, schema, ci),
        Statement::DropIndex(di) => exec_drop_index(db, schema, di),
        Statement::AlterTable(at) => exec_alter_table(db, schema, at),
        Statement::Insert(ins) => exec_insert(db, schema, ins, params),
        Statement::Select(sq) => exec_select_query(db, schema, sq),
        Statement::Update(upd) => exec_update(db, schema, upd),
        Statement::Delete(del) => exec_delete(db, schema, del),
        Statement::Explain(inner) => explain(schema, inner),
        Statement::Begin | Statement::Commit | Statement::Rollback => Err(SqlError::Unsupported(
            "transaction control in auto-commit mode".into(),
        )),
    }
}

/// Execute a parsed SQL statement within an existing write transaction.
pub fn execute_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &Statement,
    params: &[Value],
) -> Result<ExecutionResult> {
    match stmt {
        Statement::CreateTable(ct) => exec_create_table_in_txn(wtx, schema, ct),
        Statement::DropTable(dt) => exec_drop_table_in_txn(wtx, schema, dt),
        Statement::CreateIndex(ci) => exec_create_index_in_txn(wtx, schema, ci),
        Statement::DropIndex(di) => exec_drop_index_in_txn(wtx, schema, di),
        Statement::AlterTable(at) => exec_alter_table_in_txn(wtx, schema, at),
        Statement::Insert(ins) => {
            let mut bufs = InsertBufs::new();
            exec_insert_in_txn(wtx, schema, ins, params, &mut bufs)
        }
        Statement::Select(sq) => exec_select_query_in_txn(wtx, schema, sq),
        Statement::Update(upd) => exec_update_in_txn(wtx, schema, upd),
        Statement::Delete(del) => exec_delete_in_txn(wtx, schema, del),
        Statement::Explain(inner) => explain(schema, inner),
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            Err(SqlError::Unsupported("nested transaction control".into()))
        }
    }
}

// ── EXPLAIN ──────────────────────────────────────────────────────────

pub fn explain(schema: &SchemaManager, stmt: &Statement) -> Result<ExecutionResult> {
    let lines = match stmt {
        Statement::Select(sq) => {
            let mut lines = Vec::new();
            let cte_names: Vec<&str> = sq.ctes.iter().map(|c| c.name.as_str()).collect();
            for cte in &sq.ctes {
                lines.push(format!("WITH {} AS", cte.name));
                lines.extend(
                    explain_query_body_cte(schema, &cte.body, &cte_names)?
                        .into_iter()
                        .map(|l| format!("  {l}")),
                );
            }
            lines.extend(explain_query_body_cte(schema, &sq.body, &cte_names)?);
            lines
        }
        Statement::Insert(ins) => match &ins.source {
            InsertSource::Values(rows) => {
                vec![format!(
                    "INSERT INTO {} ({} rows)",
                    ins.table.to_ascii_lowercase(),
                    rows.len()
                )]
            }
            InsertSource::Select(sq) => {
                let mut lines = vec![format!(
                    "INSERT INTO {} ... SELECT",
                    ins.table.to_ascii_lowercase()
                )];
                let cte_names: Vec<&str> = sq.ctes.iter().map(|c| c.name.as_str()).collect();
                for cte in &sq.ctes {
                    lines.push(format!("  WITH {} AS", cte.name));
                    lines.extend(
                        explain_query_body_cte(schema, &cte.body, &cte_names)?
                            .into_iter()
                            .map(|l| format!("    {l}")),
                    );
                }
                lines.extend(explain_query_body_cte(schema, &sq.body, &cte_names)?);
                lines
            }
        },
        Statement::Update(upd) => explain_dml(schema, &upd.table, &upd.where_clause, "UPDATE")?,
        Statement::Delete(del) => {
            explain_dml(schema, &del.table, &del.where_clause, "DELETE FROM")?
        }
        Statement::AlterTable(at) => {
            let desc = match &at.op {
                AlterTableOp::AddColumn { column, .. } => {
                    format!("ALTER TABLE {} ADD COLUMN {}", at.table, column.name)
                }
                AlterTableOp::DropColumn { name, .. } => {
                    format!("ALTER TABLE {} DROP COLUMN {}", at.table, name)
                }
                AlterTableOp::RenameColumn {
                    old_name, new_name, ..
                } => {
                    format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO {}",
                        at.table, old_name, new_name
                    )
                }
                AlterTableOp::RenameTable { new_name } => {
                    format!("ALTER TABLE {} RENAME TO {}", at.table, new_name)
                }
            };
            vec![desc]
        }
        Statement::Explain(_) => {
            return Err(SqlError::Unsupported("EXPLAIN EXPLAIN".into()));
        }
        _ => {
            return Err(SqlError::Unsupported(
                "EXPLAIN for this statement type".into(),
            ));
        }
    };

    let rows = lines
        .into_iter()
        .map(|line| vec![Value::Text(line.into())])
        .collect();
    Ok(ExecutionResult::Query(QueryResult {
        columns: vec!["plan".into()],
        rows,
    }))
}

fn explain_dml(
    schema: &SchemaManager,
    table: &str,
    where_clause: &Option<Expr>,
    verb: &str,
) -> Result<Vec<String>> {
    let lower = table.to_ascii_lowercase();
    let table_schema = schema
        .get(&lower)
        .ok_or_else(|| SqlError::TableNotFound(table.to_string()))?;
    let plan = planner::plan_select(table_schema, where_clause);
    let scan_line = format_scan_line(&lower, &None, &plan, table_schema);
    Ok(vec![format!("{verb} {}", scan_line)])
}

fn explain_query_body_cte(
    schema: &SchemaManager,
    body: &QueryBody,
    cte_names: &[&str],
) -> Result<Vec<String>> {
    match body {
        QueryBody::Select(sel) => explain_select_cte(schema, sel, cte_names),
        QueryBody::Compound(comp) => {
            let op_name = match (&comp.op, comp.all) {
                (SetOp::Union, true) => "UNION ALL",
                (SetOp::Union, false) => "UNION",
                (SetOp::Intersect, true) => "INTERSECT ALL",
                (SetOp::Intersect, false) => "INTERSECT",
                (SetOp::Except, true) => "EXCEPT ALL",
                (SetOp::Except, false) => "EXCEPT",
            };
            let mut lines = vec![op_name.to_string()];
            let left_lines = explain_query_body_cte(schema, &comp.left, cte_names)?;
            for l in left_lines {
                lines.push(format!("  {l}"));
            }
            let right_lines = explain_query_body_cte(schema, &comp.right, cte_names)?;
            for l in right_lines {
                lines.push(format!("  {l}"));
            }
            Ok(lines)
        }
    }
}

fn explain_select_cte(
    schema: &SchemaManager,
    stmt: &SelectStmt,
    cte_names: &[&str],
) -> Result<Vec<String>> {
    let mut lines = Vec::new();

    if stmt.from.is_empty() {
        lines.push("CONSTANT ROW".into());
        return Ok(lines);
    }

    let lower_from = stmt.from.to_ascii_lowercase();

    if cte_names
        .iter()
        .any(|n| n.eq_ignore_ascii_case(&lower_from))
    {
        lines.push(format!("SCAN CTE {lower_from}"));
        for join in &stmt.joins {
            let jname = join.table.name.to_ascii_lowercase();
            if cte_names.iter().any(|n| n.eq_ignore_ascii_case(&jname)) {
                lines.push(format!("SCAN CTE {jname}"));
            } else {
                let js = schema
                    .get(&jname)
                    .ok_or_else(|| SqlError::TableNotFound(join.table.name.clone()))?;
                let jp = planner::plan_select(js, &None);
                lines.push(format_scan_line(&jname, &join.table.alias, &jp, js));
            }
        }
        if !stmt.joins.is_empty() {
            lines.push("NESTED LOOP".into());
        }
        if !stmt.group_by.is_empty() {
            lines.push("GROUP BY".into());
        }
        if stmt.distinct {
            lines.push("DISTINCT".into());
        }
        if !stmt.order_by.is_empty() {
            lines.push("SORT".into());
        }
        if stmt.limit.is_some() {
            lines.push("LIMIT".into());
        }
        return Ok(lines);
    }

    let from_schema = schema
        .get(&lower_from)
        .ok_or_else(|| SqlError::TableNotFound(stmt.from.clone()))?;

    if stmt.joins.is_empty() {
        let plan = planner::plan_select(from_schema, &stmt.where_clause);
        lines.push(format_scan_line(
            &lower_from,
            &stmt.from_alias,
            &plan,
            from_schema,
        ));
    } else {
        let from_plan = planner::plan_select(from_schema, &None);
        lines.push(format_scan_line(
            &lower_from,
            &stmt.from_alias,
            &from_plan,
            from_schema,
        ));

        for join in &stmt.joins {
            let inner_lower = join.table.name.to_ascii_lowercase();
            if cte_names
                .iter()
                .any(|n| n.eq_ignore_ascii_case(&inner_lower))
            {
                lines.push(format!("SCAN CTE {inner_lower}"));
            } else {
                let inner_schema = schema
                    .get(&inner_lower)
                    .ok_or_else(|| SqlError::TableNotFound(join.table.name.clone()))?;
                let inner_plan = planner::plan_select(inner_schema, &None);
                lines.push(format_scan_line(
                    &inner_lower,
                    &join.table.alias,
                    &inner_plan,
                    inner_schema,
                ));
            }
        }

        let join_type_str = match stmt.joins.last().map(|j| j.join_type) {
            Some(JoinType::Left) => "LEFT JOIN",
            Some(JoinType::Right) => "RIGHT JOIN",
            Some(JoinType::Cross) => "CROSS JOIN",
            _ => "NESTED LOOP",
        };
        lines.push(join_type_str.into());
    }

    if stmt.where_clause.is_some() && stmt.joins.is_empty() {
        let plan = planner::plan_select(from_schema, &stmt.where_clause);
        if matches!(plan, ScanPlan::SeqScan) {
            lines.push("FILTER".into());
        }
    }

    if let Some(ref w) = stmt.where_clause {
        if !stmt.joins.is_empty() && has_subquery(w) {
            lines.push("SUBQUERY".into());
        }
    }

    explain_subqueries(stmt, &mut lines);

    if !stmt.group_by.is_empty() {
        lines.push("GROUP BY".into());
    }

    if stmt.distinct {
        lines.push("DISTINCT".into());
    }

    if !stmt.order_by.is_empty() {
        lines.push("SORT".into());
    }

    if let Some(ref offset_expr) = stmt.offset {
        if let Ok(n) = eval_const_int(offset_expr) {
            lines.push(format!("OFFSET {n}"));
        } else {
            lines.push("OFFSET".into());
        }
    }

    if let Some(ref limit_expr) = stmt.limit {
        if let Ok(n) = eval_const_int(limit_expr) {
            lines.push(format!("LIMIT {n}"));
        } else {
            lines.push("LIMIT".into());
        }
    }

    Ok(lines)
}

fn explain_subqueries(stmt: &SelectStmt, lines: &mut Vec<String>) {
    let mut count = 0;
    if let Some(ref w) = stmt.where_clause {
        count += count_subqueries(w);
    }
    if let Some(ref h) = stmt.having {
        count += count_subqueries(h);
    }
    for col in &stmt.columns {
        if let SelectColumn::Expr { expr, .. } = col {
            count += count_subqueries(expr);
        }
    }
    for _ in 0..count {
        lines.push("SUBQUERY".into());
    }
}

fn count_subqueries(expr: &Expr) -> usize {
    match expr {
        Expr::InSubquery { expr: e, .. } => 1 + count_subqueries(e),
        Expr::ScalarSubquery(_) => 1,
        Expr::Exists { .. } => 1,
        Expr::BinaryOp { left, right, .. } => count_subqueries(left) + count_subqueries(right),
        Expr::UnaryOp { expr: e, .. } => count_subqueries(e),
        Expr::IsNull(e) | Expr::IsNotNull(e) => count_subqueries(e),
        Expr::Function { args, .. } => args.iter().map(count_subqueries).sum(),
        Expr::Between {
            expr: e, low, high, ..
        } => count_subqueries(e) + count_subqueries(low) + count_subqueries(high),
        Expr::Like {
            expr: e, pattern, ..
        } => count_subqueries(e) + count_subqueries(pattern),
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let mut n = 0;
            if let Some(op) = operand {
                n += count_subqueries(op);
            }
            for (c, r) in conditions {
                n += count_subqueries(c) + count_subqueries(r);
            }
            if let Some(el) = else_result {
                n += count_subqueries(el);
            }
            n
        }
        Expr::Coalesce(args) => args.iter().map(count_subqueries).sum(),
        Expr::Cast { expr: e, .. } => count_subqueries(e),
        Expr::InList { expr: e, list, .. } => {
            count_subqueries(e) + list.iter().map(count_subqueries).sum::<usize>()
        }
        _ => 0,
    }
}

fn format_scan_line(
    table_name: &str,
    alias: &Option<String>,
    plan: &ScanPlan,
    table_schema: &TableSchema,
) -> String {
    let alias_part = match alias {
        Some(a) if !a.eq_ignore_ascii_case(table_name) => {
            format!(" AS {}", a.to_ascii_lowercase())
        }
        _ => String::new(),
    };

    let desc = planner::describe_plan(plan, table_schema);

    if desc.is_empty() {
        format!("SCAN TABLE {table_name}{alias_part}")
    } else {
        format!("SEARCH TABLE {table_name}{alias_part} {desc}")
    }
}

// ── FK validation helper ────────────────────────────────────────────

/// Validate FK references at DDL time: parent table must exist,
/// referred columns must match PK or be covered by a UNIQUE index.
fn validate_foreign_keys(
    schema: &SchemaManager,
    table_schema: &TableSchema,
    foreign_keys: &[ForeignKeySchemaEntry],
) -> Result<()> {
    for fk in foreign_keys {
        // Self-referencing FK: parent is the table being created
        let parent = if fk.foreign_table == table_schema.name {
            table_schema
        } else {
            schema.get(&fk.foreign_table).ok_or_else(|| {
                SqlError::Unsupported(format!(
                    "FOREIGN KEY references non-existent table '{}'",
                    fk.foreign_table
                ))
            })?
        };

        // Collect referred column indices in parent
        let ref_col_indices: Vec<u16> = fk
            .referred_columns
            .iter()
            .map(|rc| {
                parent
                    .column_index(rc)
                    .map(|i| i as u16)
                    .ok_or_else(|| SqlError::ColumnNotFound(rc.clone()))
            })
            .collect::<Result<_>>()?;

        if fk.columns.len() != ref_col_indices.len() {
            return Err(SqlError::Unsupported(format!(
                "FOREIGN KEY on '{}': column count mismatch",
                table_schema.name
            )));
        }

        // Check if referred columns are the PK
        let is_pk = parent.primary_key_columns == ref_col_indices;

        // Or covered by a UNIQUE index
        let has_unique = !is_pk
            && parent
                .indices
                .iter()
                .any(|idx| idx.unique && idx.columns == ref_col_indices);

        if !is_pk && !has_unique {
            return Err(SqlError::Unsupported(format!(
                "FOREIGN KEY on '{}': referred columns in '{}' are not PRIMARY KEY or UNIQUE",
                table_schema.name, fk.foreign_table
            )));
        }
    }
    Ok(())
}

/// Create auto-index on child FK columns. Returns updated schema with new indices.
fn create_fk_auto_indices(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    mut table_schema: TableSchema,
) -> Result<TableSchema> {
    let fks: Vec<(Vec<u16>, String)> = table_schema
        .foreign_keys
        .iter()
        .enumerate()
        .map(|(i, fk)| {
            let name = fk
                .name
                .as_ref()
                .map(|n| format!("__fk_{}_{}", table_schema.name, n))
                .unwrap_or_else(|| format!("__fk_{}_{}", table_schema.name, i));
            (fk.columns.clone(), name)
        })
        .collect();

    for (cols, idx_name) in fks {
        // Skip if an index already covers these columns
        let already_covered = table_schema.indices.iter().any(|idx| idx.columns == cols);
        if already_covered {
            continue;
        }

        let idx_def = IndexDef {
            name: idx_name.clone(),
            columns: cols,
            unique: false,
        };
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx_name);
        wtx.create_table(&idx_table).map_err(SqlError::Storage)?;
        // Table is empty at CREATE TABLE time - no rows to populate
        table_schema.indices.push(idx_def);
    }
    Ok(table_schema)
}

// ── DDL ─────────────────────────────────────────────────────────────

fn exec_create_table(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &CreateTableStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if schema.contains(&lower_name) {
        if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::TableAlreadyExists(stmt.name.clone()));
    }

    if stmt.primary_key.is_empty() {
        return Err(SqlError::PrimaryKeyRequired);
    }

    let mut seen = std::collections::HashSet::new();
    for col in &stmt.columns {
        let lower = col.name.to_ascii_lowercase();
        if !seen.insert(lower.clone()) {
            return Err(SqlError::DuplicateColumn(col.name.clone()));
        }
    }

    let columns: Vec<ColumnDef> = stmt
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| ColumnDef {
            name: c.name.to_ascii_lowercase(),
            data_type: c.data_type,
            nullable: c.nullable,
            position: i as u16,
            default_expr: c.default_expr.clone(),
            default_sql: c.default_sql.clone(),
            check_expr: c.check_expr.clone(),
            check_sql: c.check_sql.clone(),
            check_name: c.check_name.clone(),
        })
        .collect();

    let primary_key_columns: Vec<u16> = stmt
        .primary_key
        .iter()
        .map(|pk_name| {
            let lower = pk_name.to_ascii_lowercase();
            columns
                .iter()
                .position(|c| c.name == lower)
                .map(|i| i as u16)
                .ok_or_else(|| SqlError::ColumnNotFound(pk_name.clone()))
        })
        .collect::<Result<_>>()?;

    let check_constraints: Vec<TableCheckDef> = stmt
        .check_constraints
        .iter()
        .map(|tc| TableCheckDef {
            name: tc.name.clone(),
            expr: tc.expr.clone(),
            sql: tc.sql.clone(),
        })
        .collect();

    let foreign_keys: Vec<ForeignKeySchemaEntry> = stmt
        .foreign_keys
        .iter()
        .map(|fk| {
            let col_indices: Vec<u16> = fk
                .columns
                .iter()
                .map(|cn| {
                    let lower = cn.to_ascii_lowercase();
                    columns
                        .iter()
                        .position(|c| c.name == lower)
                        .map(|i| i as u16)
                        .ok_or_else(|| SqlError::ColumnNotFound(cn.clone()))
                })
                .collect::<Result<_>>()?;
            Ok(ForeignKeySchemaEntry {
                name: fk.name.clone(),
                columns: col_indices,
                foreign_table: fk.foreign_table.to_ascii_lowercase(),
                referred_columns: fk
                    .referred_columns
                    .iter()
                    .map(|s| s.to_ascii_lowercase())
                    .collect(),
            })
        })
        .collect::<Result<_>>()?;

    let table_schema = TableSchema::new(
        lower_name.clone(),
        columns,
        primary_key_columns,
        vec![],
        check_constraints,
        foreign_keys,
    );

    // Validate FK references at DDL time
    validate_foreign_keys(schema, &table_schema, &table_schema.foreign_keys)?;

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    SchemaManager::ensure_schema_table(&mut wtx)?;
    wtx.create_table(lower_name.as_bytes())
        .map_err(SqlError::Storage)?;

    // Auto-create FK indices on child columns
    let table_schema = create_fk_auto_indices(&mut wtx, table_schema)?;

    SchemaManager::save_schema(&mut wtx, &table_schema)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.register(table_schema);
    Ok(ExecutionResult::Ok)
}

fn exec_drop_table(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &DropTableStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if !schema.contains(&lower_name) {
        if stmt.if_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::TableNotFound(stmt.name.clone()));
    }

    // FK guard: reject if another table's FK references this table
    for (child_table, _fk) in schema.child_fks_for(&lower_name) {
        if child_table != lower_name {
            return Err(SqlError::ForeignKeyViolation(format!(
                "cannot drop table '{}': referenced by foreign key in '{}'",
                lower_name, child_table
            )));
        }
    }

    let table_schema = schema.get(&lower_name).unwrap();
    let idx_tables: Vec<Vec<u8>> = table_schema
        .indices
        .iter()
        .map(|idx| TableSchema::index_table_name(&lower_name, &idx.name))
        .collect();

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    for idx_table in &idx_tables {
        wtx.drop_table(idx_table).map_err(SqlError::Storage)?;
    }
    wtx.drop_table(lower_name.as_bytes())
        .map_err(SqlError::Storage)?;
    SchemaManager::delete_schema(&mut wtx, &lower_name)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.remove(&lower_name);
    Ok(ExecutionResult::Ok)
}

fn exec_create_table_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &CreateTableStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if schema.contains(&lower_name) {
        if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::TableAlreadyExists(stmt.name.clone()));
    }

    if stmt.primary_key.is_empty() {
        return Err(SqlError::PrimaryKeyRequired);
    }

    let mut seen = std::collections::HashSet::new();
    for col in &stmt.columns {
        let lower = col.name.to_ascii_lowercase();
        if !seen.insert(lower.clone()) {
            return Err(SqlError::DuplicateColumn(col.name.clone()));
        }
    }

    let columns: Vec<ColumnDef> = stmt
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| ColumnDef {
            name: c.name.to_ascii_lowercase(),
            data_type: c.data_type,
            nullable: c.nullable,
            position: i as u16,
            default_expr: c.default_expr.clone(),
            default_sql: c.default_sql.clone(),
            check_expr: c.check_expr.clone(),
            check_sql: c.check_sql.clone(),
            check_name: c.check_name.clone(),
        })
        .collect();

    let primary_key_columns: Vec<u16> = stmt
        .primary_key
        .iter()
        .map(|pk_name| {
            let lower = pk_name.to_ascii_lowercase();
            columns
                .iter()
                .position(|c| c.name == lower)
                .map(|i| i as u16)
                .ok_or_else(|| SqlError::ColumnNotFound(pk_name.clone()))
        })
        .collect::<Result<_>>()?;

    let check_constraints: Vec<TableCheckDef> = stmt
        .check_constraints
        .iter()
        .map(|tc| TableCheckDef {
            name: tc.name.clone(),
            expr: tc.expr.clone(),
            sql: tc.sql.clone(),
        })
        .collect();

    let foreign_keys: Vec<ForeignKeySchemaEntry> = stmt
        .foreign_keys
        .iter()
        .map(|fk| {
            let col_indices: Vec<u16> = fk
                .columns
                .iter()
                .map(|cn| {
                    let lower = cn.to_ascii_lowercase();
                    columns
                        .iter()
                        .position(|c| c.name == lower)
                        .map(|i| i as u16)
                        .ok_or_else(|| SqlError::ColumnNotFound(cn.clone()))
                })
                .collect::<Result<_>>()?;
            Ok(ForeignKeySchemaEntry {
                name: fk.name.clone(),
                columns: col_indices,
                foreign_table: fk.foreign_table.to_ascii_lowercase(),
                referred_columns: fk
                    .referred_columns
                    .iter()
                    .map(|s| s.to_ascii_lowercase())
                    .collect(),
            })
        })
        .collect::<Result<_>>()?;

    let table_schema = TableSchema::new(
        lower_name.clone(),
        columns,
        primary_key_columns,
        vec![],
        check_constraints,
        foreign_keys,
    );

    validate_foreign_keys(schema, &table_schema, &table_schema.foreign_keys)?;

    SchemaManager::ensure_schema_table(wtx)?;
    wtx.create_table(lower_name.as_bytes())
        .map_err(SqlError::Storage)?;

    let table_schema = create_fk_auto_indices(wtx, table_schema)?;

    SchemaManager::save_schema(wtx, &table_schema)?;

    schema.register(table_schema);
    Ok(ExecutionResult::Ok)
}

fn exec_drop_table_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &DropTableStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if !schema.contains(&lower_name) {
        if stmt.if_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::TableNotFound(stmt.name.clone()));
    }

    // FK guard
    for (child_table, _fk) in schema.child_fks_for(&lower_name) {
        if child_table != lower_name {
            return Err(SqlError::ForeignKeyViolation(format!(
                "cannot drop table '{}': referenced by foreign key in '{}'",
                lower_name, child_table
            )));
        }
    }

    let table_schema = schema.get(&lower_name).unwrap();
    let idx_tables: Vec<Vec<u8>> = table_schema
        .indices
        .iter()
        .map(|idx| TableSchema::index_table_name(&lower_name, &idx.name))
        .collect();

    for idx_table in &idx_tables {
        wtx.drop_table(idx_table).map_err(SqlError::Storage)?;
    }
    wtx.drop_table(lower_name.as_bytes())
        .map_err(SqlError::Storage)?;
    SchemaManager::delete_schema(wtx, &lower_name)?;

    schema.remove(&lower_name);
    Ok(ExecutionResult::Ok)
}

fn exec_create_index(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &CreateIndexStmt,
) -> Result<ExecutionResult> {
    let lower_table = stmt.table_name.to_ascii_lowercase();
    let lower_idx = stmt.index_name.to_ascii_lowercase();

    let table_schema = schema
        .get(&lower_table)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table_name.clone()))?;

    if table_schema.index_by_name(&lower_idx).is_some() {
        if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::IndexAlreadyExists(stmt.index_name.clone()));
    }

    let col_indices: Vec<u16> = stmt
        .columns
        .iter()
        .map(|col_name| {
            let lower = col_name.to_ascii_lowercase();
            table_schema
                .column_index(&lower)
                .map(|i| i as u16)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))
        })
        .collect::<Result<_>>()?;

    let idx_def = IndexDef {
        name: lower_idx.clone(),
        columns: col_indices,
        unique: stmt.unique,
    };

    let idx_table = TableSchema::index_table_name(&lower_table, &lower_idx);

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    SchemaManager::ensure_schema_table(&mut wtx)?;
    wtx.create_table(&idx_table).map_err(SqlError::Storage)?;

    // Populate index from existing rows
    let pk_indices = table_schema.pk_indices();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    {
        let mut scan_err: Option<SqlError> = None;
        wtx.table_for_each(lower_table.as_bytes(), |key, value| {
            match decode_full_row(table_schema, key, value) {
                Ok(row) => rows.push(row),
                Err(e) => scan_err = Some(e),
            }
            Ok(())
        })
        .map_err(SqlError::Storage)?;
        if let Some(e) = scan_err {
            return Err(e);
        }
    }

    for row in &rows {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        let key = encode_index_key(&idx_def, row, &pk_values);
        let value = encode_index_value(&idx_def, row, &pk_values);
        let is_new = wtx
            .table_insert(&idx_table, &key, &value)
            .map_err(SqlError::Storage)?;
        if idx_def.unique && !is_new {
            let indexed_values: Vec<Value> = idx_def
                .columns
                .iter()
                .map(|&col_idx| row[col_idx as usize].clone())
                .collect();
            let any_null = indexed_values.iter().any(|v| v.is_null());
            if !any_null {
                return Err(SqlError::UniqueViolation(stmt.index_name.clone()));
            }
        }
    }

    let mut updated_schema = table_schema.clone();
    updated_schema.indices.push(idx_def);
    SchemaManager::save_schema(&mut wtx, &updated_schema)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.register(updated_schema);
    Ok(ExecutionResult::Ok)
}

fn exec_drop_index(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &DropIndexStmt,
) -> Result<ExecutionResult> {
    let lower_idx = stmt.index_name.to_ascii_lowercase();

    let (table_name, _idx_pos) = match find_index_in_schemas(schema, &lower_idx) {
        Some(found) => found,
        None => {
            if stmt.if_exists {
                return Ok(ExecutionResult::Ok);
            }
            return Err(SqlError::IndexNotFound(stmt.index_name.clone()));
        }
    };

    let idx_table = TableSchema::index_table_name(&table_name, &lower_idx);

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    wtx.drop_table(&idx_table).map_err(SqlError::Storage)?;

    let table_schema = schema.get(&table_name).unwrap();
    let mut updated_schema = table_schema.clone();
    updated_schema.indices.retain(|i| i.name != lower_idx);
    SchemaManager::save_schema(&mut wtx, &updated_schema)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.register(updated_schema);
    Ok(ExecutionResult::Ok)
}

fn exec_create_index_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &CreateIndexStmt,
) -> Result<ExecutionResult> {
    let lower_table = stmt.table_name.to_ascii_lowercase();
    let lower_idx = stmt.index_name.to_ascii_lowercase();

    let table_schema = schema
        .get(&lower_table)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table_name.clone()))?;

    if table_schema.index_by_name(&lower_idx).is_some() {
        if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::IndexAlreadyExists(stmt.index_name.clone()));
    }

    let col_indices: Vec<u16> = stmt
        .columns
        .iter()
        .map(|col_name| {
            let lower = col_name.to_ascii_lowercase();
            table_schema
                .column_index(&lower)
                .map(|i| i as u16)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))
        })
        .collect::<Result<_>>()?;

    let idx_def = IndexDef {
        name: lower_idx.clone(),
        columns: col_indices,
        unique: stmt.unique,
    };

    let idx_table = TableSchema::index_table_name(&lower_table, &lower_idx);

    SchemaManager::ensure_schema_table(wtx)?;
    wtx.create_table(&idx_table).map_err(SqlError::Storage)?;

    let pk_indices = table_schema.pk_indices();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    {
        let mut scan_err: Option<SqlError> = None;
        wtx.table_for_each(lower_table.as_bytes(), |key, value| {
            match decode_full_row(table_schema, key, value) {
                Ok(row) => rows.push(row),
                Err(e) => scan_err = Some(e),
            }
            Ok(())
        })
        .map_err(SqlError::Storage)?;
        if let Some(e) = scan_err {
            return Err(e);
        }
    }

    for row in &rows {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        let key = encode_index_key(&idx_def, row, &pk_values);
        let value = encode_index_value(&idx_def, row, &pk_values);
        let is_new = wtx
            .table_insert(&idx_table, &key, &value)
            .map_err(SqlError::Storage)?;
        if idx_def.unique && !is_new {
            let indexed_values: Vec<Value> = idx_def
                .columns
                .iter()
                .map(|&col_idx| row[col_idx as usize].clone())
                .collect();
            let any_null = indexed_values.iter().any(|v| v.is_null());
            if !any_null {
                return Err(SqlError::UniqueViolation(stmt.index_name.clone()));
            }
        }
    }

    let mut updated_schema = table_schema.clone();
    updated_schema.indices.push(idx_def);
    SchemaManager::save_schema(wtx, &updated_schema)?;

    schema.register(updated_schema);
    Ok(ExecutionResult::Ok)
}

fn exec_drop_index_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &DropIndexStmt,
) -> Result<ExecutionResult> {
    let lower_idx = stmt.index_name.to_ascii_lowercase();

    let (table_name, _idx_pos) = match find_index_in_schemas(schema, &lower_idx) {
        Some(found) => found,
        None => {
            if stmt.if_exists {
                return Ok(ExecutionResult::Ok);
            }
            return Err(SqlError::IndexNotFound(stmt.index_name.clone()));
        }
    };

    let idx_table = TableSchema::index_table_name(&table_name, &lower_idx);
    wtx.drop_table(&idx_table).map_err(SqlError::Storage)?;

    let table_schema = schema.get(&table_name).unwrap();
    let mut updated_schema = table_schema.clone();
    updated_schema.indices.retain(|i| i.name != lower_idx);
    SchemaManager::save_schema(wtx, &updated_schema)?;

    schema.register(updated_schema);
    Ok(ExecutionResult::Ok)
}

// ── ALTER TABLE ──────────────────────────────────────────────────────

fn exec_alter_table(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &AlterTableStmt,
) -> Result<ExecutionResult> {
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    SchemaManager::ensure_schema_table(&mut wtx)?;
    alter_table_impl(&mut wtx, schema, stmt)?;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::Ok)
}

fn exec_alter_table_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &AlterTableStmt,
) -> Result<ExecutionResult> {
    SchemaManager::ensure_schema_table(wtx)?;
    alter_table_impl(wtx, schema, stmt)?;
    Ok(ExecutionResult::Ok)
}

fn alter_table_impl(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &AlterTableStmt,
) -> Result<()> {
    let lower_name = stmt.table.to_ascii_lowercase();
    if lower_name == "_schema" {
        return Err(SqlError::Unsupported("cannot alter internal table".into()));
    }
    match &stmt.op {
        AlterTableOp::AddColumn {
            column,
            foreign_key,
            if_not_exists,
        } => alter_add_column(
            wtx,
            schema,
            &lower_name,
            column,
            foreign_key.as_ref(),
            *if_not_exists,
        ),
        AlterTableOp::DropColumn { name, if_exists } => {
            alter_drop_column(wtx, schema, &lower_name, name, *if_exists)
        }
        AlterTableOp::RenameColumn { old_name, new_name } => {
            alter_rename_column(wtx, schema, &lower_name, old_name, new_name)
        }
        AlterTableOp::RenameTable { new_name } => {
            alter_rename_table(wtx, schema, &lower_name, new_name)
        }
    }
}

fn alter_add_column(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    table_name: &str,
    col_spec: &ColumnSpec,
    fk_def: Option<&ForeignKeyDef>,
    if_not_exists: bool,
) -> Result<()> {
    let table_schema = schema
        .get(table_name)
        .ok_or_else(|| SqlError::TableNotFound(table_name.into()))?;

    let col_lower = col_spec.name.to_ascii_lowercase();

    if table_schema.column_index(&col_lower).is_some() {
        if if_not_exists {
            return Ok(());
        }
        return Err(SqlError::DuplicateColumn(col_spec.name.clone()));
    }

    if col_spec.is_primary_key {
        return Err(SqlError::Unsupported(
            "cannot add PRIMARY KEY column via ALTER TABLE".into(),
        ));
    }

    if !col_spec.nullable && col_spec.default_expr.is_none() {
        let count = wtx.table_entry_count(table_name.as_bytes()).unwrap_or(0);
        if count > 0 {
            return Err(SqlError::Unsupported(
                "cannot add NOT NULL column without DEFAULT to non-empty table".into(),
            ));
        }
    }

    if let Some(ref check) = col_spec.check_expr {
        if has_subquery(check) {
            return Err(SqlError::Unsupported("subquery in CHECK constraint".into()));
        }
    }

    let new_pos = table_schema.columns.len() as u16;
    let new_col = ColumnDef {
        name: col_lower.clone(),
        data_type: col_spec.data_type,
        nullable: col_spec.nullable,
        position: new_pos,
        default_expr: col_spec.default_expr.clone(),
        default_sql: col_spec.default_sql.clone(),
        check_expr: col_spec.check_expr.clone(),
        check_sql: col_spec.check_sql.clone(),
        check_name: col_spec.check_name.clone(),
    };

    let mut new_schema = table_schema.clone();
    new_schema.columns.push(new_col);

    if let Some(fk) = fk_def {
        let col_idx = new_pos;
        let fk_entry = ForeignKeySchemaEntry {
            name: fk.name.clone(),
            columns: vec![col_idx],
            foreign_table: fk.foreign_table.to_ascii_lowercase(),
            referred_columns: fk
                .referred_columns
                .iter()
                .map(|s| s.to_ascii_lowercase())
                .collect(),
        };
        new_schema.foreign_keys.push(fk_entry);
    }

    new_schema = new_schema.rebuild();

    if fk_def.is_some() {
        validate_foreign_keys(schema, &new_schema, &new_schema.foreign_keys)?;
        new_schema = create_fk_auto_indices(wtx, new_schema)?;
    }

    SchemaManager::save_schema(wtx, &new_schema)?;
    schema.register(new_schema);
    Ok(())
}

fn alter_drop_column(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    table_name: &str,
    col_name: &str,
    if_exists: bool,
) -> Result<()> {
    let table_schema = schema
        .get(table_name)
        .ok_or_else(|| SqlError::TableNotFound(table_name.into()))?;

    let col_lower = col_name.to_ascii_lowercase();
    let drop_pos = match table_schema.column_index(&col_lower) {
        Some(pos) => pos,
        None => {
            if if_exists {
                return Ok(());
            }
            return Err(SqlError::ColumnNotFound(col_name.into()));
        }
    };
    let drop_pos_u16 = drop_pos as u16;

    if table_schema.primary_key_columns.contains(&drop_pos_u16) {
        return Err(SqlError::Unsupported(
            "cannot drop primary key column".into(),
        ));
    }

    for idx in &table_schema.indices {
        if idx.columns.contains(&drop_pos_u16) {
            return Err(SqlError::Unsupported(format!(
                "column '{}' is indexed by '{}'; drop the index first",
                col_lower, idx.name
            )));
        }
    }

    for fk in &table_schema.foreign_keys {
        if fk.columns.contains(&drop_pos_u16) {
            return Err(SqlError::Unsupported(format!(
                "column '{}' is part of a foreign key",
                col_lower
            )));
        }
    }

    for (child_table, fk) in schema.child_fks_for(table_name) {
        if child_table == table_name {
            continue; // self-ref already checked above
        }
        if fk.referred_columns.iter().any(|rc| rc == &col_lower) {
            return Err(SqlError::Unsupported(format!(
                "column '{}' is referenced by a foreign key in '{}'",
                col_lower, child_table
            )));
        }
    }

    for tc in &table_schema.check_constraints {
        if tc.sql.to_ascii_lowercase().contains(&col_lower) {
            return Err(SqlError::Unsupported(format!(
                "column '{}' is used in CHECK constraint '{}'",
                col_lower,
                tc.name.as_deref().unwrap_or("<unnamed>")
            )));
        }
    }

    // O(1): schema-only change. Old rows keep the dropped column's data
    // in the encoding; decode paths skip it via decode_col_mapping().
    // New rows encode NULL at the dead physical slot.
    let new_schema = table_schema.without_column(drop_pos);

    SchemaManager::save_schema(wtx, &new_schema)?;
    schema.register(new_schema);
    Ok(())
}

fn alter_rename_column(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    table_name: &str,
    old_name: &str,
    new_name: &str,
) -> Result<()> {
    let table_schema = schema
        .get(table_name)
        .ok_or_else(|| SqlError::TableNotFound(table_name.into()))?;

    let old_lower = old_name.to_ascii_lowercase();
    let new_lower = new_name.to_ascii_lowercase();

    let col_pos = table_schema
        .column_index(&old_lower)
        .ok_or_else(|| SqlError::ColumnNotFound(old_name.into()))?;

    if table_schema.column_index(&new_lower).is_some() {
        return Err(SqlError::DuplicateColumn(new_name.into()));
    }

    let mut new_schema = table_schema.clone();
    new_schema.columns[col_pos].name = new_lower.clone();

    // Update CHECK constraint SQL text that references the old column name
    for col in &mut new_schema.columns {
        if let Some(ref sql) = col.check_sql {
            if sql.to_ascii_lowercase().contains(&old_lower) {
                let updated = sql.replace(&old_lower, &new_lower);
                col.check_sql = Some(updated.clone());
                // Re-parse the updated expression
                if let Ok(parsed) = crate::parser::parse_sql_expr(&updated) {
                    col.check_expr = Some(parsed);
                }
            }
        }
    }
    for tc in &mut new_schema.check_constraints {
        if tc.sql.to_ascii_lowercase().contains(&old_lower) {
            tc.sql = tc.sql.replace(&old_lower, &new_lower);
            if let Ok(parsed) = crate::parser::parse_sql_expr(&tc.sql) {
                tc.expr = parsed;
            }
        }
    }

    // Update FK referred_columns if this table is referenced by child FKs
    // (only for self-referencing FKs - cross-table FKs reference by name,
    // so they'll be updated when child tables are loaded)
    for fk in &mut new_schema.foreign_keys {
        if fk.foreign_table == table_name {
            for rc in &mut fk.referred_columns {
                if *rc == old_lower {
                    *rc = new_lower.clone();
                }
            }
        }
    }

    SchemaManager::save_schema(wtx, &new_schema)?;
    schema.register(new_schema);
    Ok(())
}

fn alter_rename_table(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    old_name: &str,
    new_name: &str,
) -> Result<()> {
    let new_lower = new_name.to_ascii_lowercase();

    if new_lower == "_schema" {
        return Err(SqlError::Unsupported(
            "cannot rename to internal table name".into(),
        ));
    }

    let table_schema = schema
        .get(old_name)
        .ok_or_else(|| SqlError::TableNotFound(old_name.into()))?
        .clone();

    if schema.contains(&new_lower) {
        return Err(SqlError::TableAlreadyExists(new_name.into()));
    }

    wtx.rename_table(old_name.as_bytes(), new_lower.as_bytes())
        .map_err(SqlError::Storage)?;

    let idx_renames: Vec<(Vec<u8>, Vec<u8>)> = table_schema
        .indices
        .iter()
        .map(|idx| {
            let old_idx = TableSchema::index_table_name(old_name, &idx.name);
            let new_idx = TableSchema::index_table_name(&new_lower, &idx.name);
            (old_idx, new_idx)
        })
        .collect();
    for (old_idx, new_idx) in &idx_renames {
        wtx.rename_table(old_idx, new_idx)
            .map_err(SqlError::Storage)?;
    }

    let child_tables: Vec<String> = schema
        .child_fks_for(old_name)
        .iter()
        .filter(|(child, _)| *child != old_name)
        .map(|(child, _)| child.to_string())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    for child_table in &child_tables {
        let mut updated_child = schema.get(child_table).unwrap().clone();
        for fk in &mut updated_child.foreign_keys {
            if fk.foreign_table == old_name {
                fk.foreign_table = new_lower.clone();
            }
        }
        SchemaManager::save_schema(wtx, &updated_child)?;
        schema.register(updated_child);
    }

    SchemaManager::delete_schema(wtx, old_name)?;
    let mut new_schema = table_schema;
    new_schema.name = new_lower.clone();

    // Update self-referencing FKs
    for fk in &mut new_schema.foreign_keys {
        if fk.foreign_table == old_name {
            fk.foreign_table = new_lower.clone();
        }
    }

    SchemaManager::save_schema(wtx, &new_schema)?;
    schema.remove(old_name);
    schema.register(new_schema);
    Ok(())
}

fn find_index_in_schemas(schema: &SchemaManager, index_name: &str) -> Option<(String, usize)> {
    for table_name in schema.table_names() {
        if let Some(ts) = schema.get(table_name) {
            if let Some(pos) = ts.indices.iter().position(|i| i.name == index_name) {
                return Some((table_name.to_string(), pos));
            }
        }
    }
    None
}

// ── Index scan helpers ───────────────────────────────────────────────

fn extract_pk_key(
    idx_key: &[u8],
    idx_value: &[u8],
    is_unique: bool,
    num_index_cols: usize,
    num_pk_cols: usize,
) -> Result<Vec<u8>> {
    if is_unique && !idx_value.is_empty() {
        Ok(idx_value.to_vec())
    } else {
        let total_cols = num_index_cols + num_pk_cols;
        let all_values = decode_composite_key(idx_key, total_cols)?;
        let pk_values = &all_values[num_index_cols..];
        Ok(encode_composite_key(pk_values))
    }
}

fn check_range_conditions(
    idx_key: &[u8],
    num_prefix_cols: usize,
    range_conds: &[(BinOp, Value)],
    num_index_cols: usize,
) -> Result<RangeCheck> {
    if range_conds.is_empty() {
        return Ok(RangeCheck::Match);
    }

    let num_to_decode = num_prefix_cols + 1;
    if num_to_decode > num_index_cols {
        return Ok(RangeCheck::Match);
    }

    // Decode just enough columns to check the range column
    let mut pos = 0;
    for _ in 0..num_prefix_cols {
        let (_, n) = decode_key_value(&idx_key[pos..])?;
        pos += n;
    }
    let (range_val, _) = decode_key_value(&idx_key[pos..])?;

    let mut exceeds_upper = false;
    let mut below_lower = false;

    for (op, val) in range_conds {
        match op {
            BinOp::Lt => {
                if range_val >= *val {
                    exceeds_upper = true;
                }
            }
            BinOp::LtEq => {
                if range_val > *val {
                    exceeds_upper = true;
                }
            }
            BinOp::Gt => {
                if range_val <= *val {
                    below_lower = true;
                }
            }
            BinOp::GtEq => {
                if range_val < *val {
                    below_lower = true;
                }
            }
            _ => {}
        }
    }

    if exceeds_upper {
        Ok(RangeCheck::ExceedsUpper)
    } else if below_lower {
        Ok(RangeCheck::BelowLower)
    } else {
        Ok(RangeCheck::Match)
    }
}

enum RangeCheck {
    Match,
    BelowLower,
    ExceedsUpper,
}

/// Collect rows via ReadTxn using the scan plan.
fn collect_rows_read(
    db: &Database,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
    limit: Option<usize>,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;
    let columns = &table_schema.columns;

    match plan {
        ScanPlan::SeqScan => {
            let simple_pred = where_clause
                .as_ref()
                .and_then(|expr| try_simple_predicate(expr, table_schema));

            if let Some(ref pred) = simple_pred {
                let mut rtx = db.begin_read();
                let entry_count =
                    rtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0) as usize;
                let mut rows = Vec::with_capacity(entry_count / 4);
                let mut scan_err: Option<SqlError> = None;
                rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
                    match pred.matches_raw(key, value) {
                        Ok(true) => match decode_full_row(table_schema, key, value) {
                            Ok(row) => rows.push(row),
                            Err(e) => {
                                scan_err = Some(e);
                                return false;
                            }
                        },
                        Ok(false) => {}
                        Err(e) => {
                            scan_err = Some(e);
                            return false;
                        }
                    }
                    scan_err.is_none() && limit.map_or(true, |n| rows.len() < n)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
                return Ok((rows, true));
            }

            let mut rtx = db.begin_read();
            let entry_count = rtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0) as usize;
            let capacity = if where_clause.is_some() {
                entry_count / 4
            } else {
                entry_count
            };
            let mut rows = Vec::with_capacity(capacity);
            let mut scan_err: Option<SqlError> = None;

            let col_map = ColumnMap::new(columns);
            let partial_ctx = where_clause.as_ref().and_then(|expr| {
                let needed = referenced_columns(expr, columns);
                if needed.len() < columns.len() {
                    Some(PartialDecodeCtx::new(table_schema, &needed))
                } else {
                    None
                }
            });

            rtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                match (&where_clause, &partial_ctx) {
                    (Some(expr), Some(ctx)) => match ctx.decode(key, value) {
                        Ok(partial) => match eval_expr(expr, &col_map, &partial) {
                            Ok(val) if is_truthy(&val) => match ctx.complete(partial, key, value) {
                                Ok(row) => rows.push(row),
                                Err(e) => scan_err = Some(e),
                            },
                            Err(e) => scan_err = Some(e),
                            _ => {}
                        },
                        Err(e) => scan_err = Some(e),
                    },
                    (Some(expr), None) => match decode_full_row(table_schema, key, value) {
                        Ok(row) => match eval_expr(expr, &col_map, &row) {
                            Ok(val) if is_truthy(&val) => rows.push(row),
                            Err(e) => scan_err = Some(e),
                            _ => {}
                        },
                        Err(e) => scan_err = Some(e),
                    },
                    _ => match decode_full_row(table_schema, key, value) {
                        Ok(row) => rows.push(row),
                        Err(e) => scan_err = Some(e),
                    },
                }
                let keep_going = scan_err.is_none() && limit.map_or(true, |n| rows.len() < n);
                Ok(keep_going)
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok((rows, where_clause.is_some()))
        }

        ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(&pk_values);
            let mut rtx = db.begin_read();
            match rtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                Some(value) => {
                    let row = decode_full_row(table_schema, &key, &value)?;
                    if let Some(ref expr) = where_clause {
                        let col_map = ColumnMap::new(columns);
                        match eval_expr(expr, &col_map, &row) {
                            Ok(val) if is_truthy(&val) => Ok((vec![row], true)),
                            _ => Ok((vec![], true)),
                        }
                    } else {
                        Ok((vec![row], false))
                    }
                }
                None => Ok((vec![], true)),
            }
        }

        ScanPlan::IndexScan {
            idx_table,
            prefix,
            num_prefix_cols,
            range_conds,
            is_unique,
            index_columns,
            ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = index_columns.len();
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();

            {
                let mut rtx = db.begin_read();
                let mut scan_err: Option<SqlError> = None;
                rtx.table_scan_from(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols)
                    {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    Ok(true)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
            }

            let mut rows = Vec::new();
            let mut rtx = db.begin_read();
            let col_map = ColumnMap::new(columns);
            for pk_key in &pk_keys {
                if let Some(value) = rtx
                    .table_get(lower_name.as_bytes(), pk_key)
                    .map_err(SqlError::Storage)?
                {
                    let row = decode_full_row(table_schema, pk_key, &value)?;
                    if let Some(ref expr) = where_clause {
                        match eval_expr(expr, &col_map, &row) {
                            Ok(val) if is_truthy(&val) => rows.push(row),
                            _ => {}
                        }
                    } else {
                        rows.push(row);
                    }
                }
            }
            Ok((rows, where_clause.is_some()))
        }
    }
}

/// Collect rows via WriteTxn using the scan plan.
fn collect_rows_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
    limit: Option<usize>,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;
    let columns = &table_schema.columns;

    match plan {
        ScanPlan::SeqScan => {
            let simple_pred = where_clause
                .as_ref()
                .and_then(|expr| try_simple_predicate(expr, table_schema));

            if let Some(ref pred) = simple_pred {
                let mut rows = Vec::new();
                let mut scan_err: Option<SqlError> = None;
                wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                    match pred.matches_raw(key, value) {
                        Ok(true) => match decode_full_row(table_schema, key, value) {
                            Ok(row) => rows.push(row),
                            Err(e) => scan_err = Some(e),
                        },
                        Ok(false) => {}
                        Err(e) => scan_err = Some(e),
                    }
                    let keep_going = scan_err.is_none() && limit.map_or(true, |n| rows.len() < n);
                    Ok(keep_going)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
                return Ok((rows, true));
            }

            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;

            let col_map = ColumnMap::new(columns);
            let partial_ctx = where_clause.as_ref().and_then(|expr| {
                let needed = referenced_columns(expr, columns);
                if needed.len() < columns.len() {
                    Some(PartialDecodeCtx::new(table_schema, &needed))
                } else {
                    None
                }
            });

            wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                match (&where_clause, &partial_ctx) {
                    (Some(expr), Some(ctx)) => match ctx.decode(key, value) {
                        Ok(partial) => match eval_expr(expr, &col_map, &partial) {
                            Ok(val) if is_truthy(&val) => match ctx.complete(partial, key, value) {
                                Ok(row) => rows.push(row),
                                Err(e) => scan_err = Some(e),
                            },
                            Err(e) => scan_err = Some(e),
                            _ => {}
                        },
                        Err(e) => scan_err = Some(e),
                    },
                    (Some(expr), None) => match decode_full_row(table_schema, key, value) {
                        Ok(row) => match eval_expr(expr, &col_map, &row) {
                            Ok(val) if is_truthy(&val) => rows.push(row),
                            Err(e) => scan_err = Some(e),
                            _ => {}
                        },
                        Err(e) => scan_err = Some(e),
                    },
                    _ => match decode_full_row(table_schema, key, value) {
                        Ok(row) => rows.push(row),
                        Err(e) => scan_err = Some(e),
                    },
                }
                let keep_going = scan_err.is_none() && limit.map_or(true, |n| rows.len() < n);
                Ok(keep_going)
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok((rows, where_clause.is_some()))
        }

        ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(&pk_values);
            match wtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                Some(value) => {
                    let row = decode_full_row(table_schema, &key, &value)?;
                    if let Some(ref expr) = where_clause {
                        let col_map = ColumnMap::new(columns);
                        match eval_expr(expr, &col_map, &row) {
                            Ok(val) if is_truthy(&val) => Ok((vec![row], true)),
                            _ => Ok((vec![], true)),
                        }
                    } else {
                        Ok((vec![row], false))
                    }
                }
                None => Ok((vec![], true)),
            }
        }

        ScanPlan::IndexScan {
            idx_table,
            prefix,
            num_prefix_cols,
            range_conds,
            is_unique,
            index_columns,
            ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = index_columns.len();
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();

            {
                let mut scan_err: Option<SqlError> = None;
                wtx.table_scan_from(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols)
                    {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    Ok(true)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
            }

            let mut rows = Vec::new();
            let col_map = ColumnMap::new(columns);
            for pk_key in &pk_keys {
                if let Some(value) = wtx
                    .table_get(lower_name.as_bytes(), pk_key)
                    .map_err(SqlError::Storage)?
                {
                    let row = decode_full_row(table_schema, pk_key, &value)?;
                    if let Some(ref expr) = where_clause {
                        match eval_expr(expr, &col_map, &row) {
                            Ok(val) if is_truthy(&val) => rows.push(row),
                            _ => {}
                        }
                    } else {
                        rows.push(row);
                    }
                }
            }
            Ok((rows, where_clause.is_some()))
        }
    }
}

/// Collect (encoded_key, full_row) pairs via ReadTxn using the scan plan.
/// Used by DELETE and UPDATE which need the encoded PK key.
fn collect_keyed_rows_read(
    db: &Database,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;

    match plan {
        ScanPlan::SeqScan => {
            let mut rows = Vec::new();
            let mut rtx = db.begin_read();
            let mut scan_err: Option<SqlError> = None;
            rtx.table_for_each(lower_name.as_bytes(), |key, value| {
                match decode_full_row(table_schema, key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(e) => scan_err = Some(e),
                }
                Ok(())
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok(rows)
        }

        ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(&pk_values);
            let mut rtx = db.begin_read();
            match rtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                Some(value) => {
                    let row = decode_full_row(table_schema, &key, &value)?;
                    Ok(vec![(key, row)])
                }
                None => Ok(vec![]),
            }
        }

        ScanPlan::IndexScan {
            idx_table,
            prefix,
            num_prefix_cols,
            range_conds,
            is_unique,
            index_columns,
            ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = index_columns.len();
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();

            {
                let mut rtx = db.begin_read();
                let mut scan_err: Option<SqlError> = None;
                rtx.table_scan_from(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols)
                    {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    Ok(true)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
            }

            let mut rows = Vec::new();
            let mut rtx = db.begin_read();
            for pk_key in &pk_keys {
                if let Some(value) = rtx
                    .table_get(lower_name.as_bytes(), pk_key)
                    .map_err(SqlError::Storage)?
                {
                    rows.push((
                        pk_key.clone(),
                        decode_full_row(table_schema, pk_key, &value)?,
                    ));
                }
            }
            Ok(rows)
        }
    }
}

/// Collect (encoded_key, full_row) pairs via WriteTxn using the scan plan.
fn collect_keyed_rows_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;

    match plan {
        ScanPlan::SeqScan => {
            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;
            wtx.table_for_each(lower_name.as_bytes(), |key, value| {
                match decode_full_row(table_schema, key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(e) => scan_err = Some(e),
                }
                Ok(())
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok(rows)
        }

        ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(&pk_values);
            match wtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                Some(value) => {
                    let row = decode_full_row(table_schema, &key, &value)?;
                    Ok(vec![(key, row)])
                }
                None => Ok(vec![]),
            }
        }

        ScanPlan::IndexScan {
            idx_table,
            prefix,
            num_prefix_cols,
            range_conds,
            is_unique,
            index_columns,
            ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = index_columns.len();
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();

            {
                let mut scan_err: Option<SqlError> = None;
                wtx.table_scan_from(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols)
                    {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    Ok(true)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
            }

            let mut rows = Vec::new();
            for pk_key in &pk_keys {
                if let Some(value) = wtx
                    .table_get(lower_name.as_bytes(), pk_key)
                    .map_err(SqlError::Storage)?
                {
                    rows.push((
                        pk_key.clone(),
                        decode_full_row(table_schema, pk_key, &value)?,
                    ));
                }
            }
            Ok(rows)
        }
    }
}

// ── DML ─────────────────────────────────────────────────────────────

fn exec_insert(
    db: &Database,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
) -> Result<ExecutionResult> {
    let empty_ctes = CteContext::new();
    let materialized;
    let stmt = if insert_has_subquery(stmt) {
        materialized = materialize_insert(stmt, &mut |sub| {
            exec_subquery_read(db, schema, sub, &empty_ctes)
        })?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let insert_columns = if stmt.columns.is_empty() {
        table_schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
    } else {
        stmt.columns
            .iter()
            .map(|c| c.to_ascii_lowercase())
            .collect()
    };

    let col_indices: Vec<usize> = insert_columns
        .iter()
        .map(|name| {
            table_schema
                .column_index(name)
                .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))
        })
        .collect::<Result<_>>()?;

    // Pre-compute defaults: (column_position, default_expr) for columns NOT in insert list
    let defaults: Vec<(usize, &Expr)> = table_schema
        .columns
        .iter()
        .filter(|c| c.default_expr.is_some() && !col_indices.contains(&(c.position as usize)))
        .map(|c| (c.position as usize, c.default_expr.as_ref().unwrap()))
        .collect();

    // Pre-build ColumnMap for CHECK evaluation
    let has_checks = table_schema.has_checks();
    let check_col_map = if has_checks {
        Some(ColumnMap::new(&table_schema.columns))
    } else {
        None
    };

    let select_rows = match &stmt.source {
        InsertSource::Select(sq) => {
            let insert_ctes = materialize_all_ctes(&sq.ctes, sq.recursive, &mut |body, ctx| {
                exec_query_body_read(db, schema, body, ctx)
            })?;
            let qr = exec_query_body_read(db, schema, &sq.body, &insert_ctes)?;
            Some(qr.rows)
        }
        InsertSource::Values(_) => None,
    };

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let mut count: u64 = 0;

    let pk_indices = table_schema.pk_indices();
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let phys_count = table_schema.physical_non_pk_count();
    let mut row = vec![Value::Null; table_schema.columns.len()];
    let mut pk_values: Vec<Value> = vec![Value::Null; pk_indices.len()];
    let mut value_values: Vec<Value> = vec![Value::Null; phys_count];
    let mut key_buf: Vec<u8> = Vec::with_capacity(64);
    let mut value_buf: Vec<u8> = Vec::with_capacity(256);
    let mut fk_key_buf: Vec<u8> = Vec::with_capacity(64);

    let values = match &stmt.source {
        InsertSource::Values(rows) => Some(rows.as_slice()),
        InsertSource::Select(_) => None,
    };
    let sel_rows = select_rows.as_deref();

    let total = match (values, sel_rows) {
        (Some(rows), _) => rows.len(),
        (_, Some(rows)) => rows.len(),
        _ => 0,
    };

    if let Some(sel) = sel_rows {
        if !sel.is_empty() && sel[0].len() != insert_columns.len() {
            return Err(SqlError::InvalidValue(format!(
                "INSERT ... SELECT column count mismatch: expected {}, got {}",
                insert_columns.len(),
                sel[0].len()
            )));
        }
    }

    for idx in 0..total {
        for v in row.iter_mut() {
            *v = Value::Null;
        }

        if let Some(value_rows) = values {
            let value_row = &value_rows[idx];
            if value_row.len() != insert_columns.len() {
                return Err(SqlError::InvalidValue(format!(
                    "expected {} values, got {}",
                    insert_columns.len(),
                    value_row.len()
                )));
            }
            for (i, expr) in value_row.iter().enumerate() {
                let val = if let Expr::Parameter(n) = expr {
                    params
                        .get(n - 1)
                        .cloned()
                        .ok_or_else(|| SqlError::Parse(format!("unbound parameter ${n}")))?
                } else {
                    eval_const_expr(expr)?
                };
                let col_idx = col_indices[i];
                let col = &table_schema.columns[col_idx];
                let got_type = val.data_type();
                row[col_idx] = if val.is_null() {
                    Value::Null
                } else {
                    val.coerce_into(col.data_type)
                        .ok_or_else(|| SqlError::TypeMismatch {
                            expected: col.data_type.to_string(),
                            got: got_type.to_string(),
                        })?
                };
            }
        } else if let Some(sel) = sel_rows {
            let sel_row = &sel[idx];
            for (i, val) in sel_row.iter().enumerate() {
                let col_idx = col_indices[i];
                let col = &table_schema.columns[col_idx];
                let got_type = val.data_type();
                row[col_idx] = if val.is_null() {
                    Value::Null
                } else {
                    val.clone().coerce_into(col.data_type).ok_or_else(|| {
                        SqlError::TypeMismatch {
                            expected: col.data_type.to_string(),
                            got: got_type.to_string(),
                        }
                    })?
                };
            }
        }

        // Apply DEFAULT for omitted columns
        for &(pos, def_expr) in &defaults {
            let val = eval_const_expr(def_expr)?;
            let col = &table_schema.columns[pos];
            if val.is_null() {
                // row[pos] already Null from init
            } else {
                let got_type = val.data_type();
                row[pos] =
                    val.coerce_into(col.data_type)
                        .ok_or_else(|| SqlError::TypeMismatch {
                            expected: col.data_type.to_string(),
                            got: got_type.to_string(),
                        })?;
            }
        }

        for col in &table_schema.columns {
            if !col.nullable && row[col.position as usize].is_null() {
                return Err(SqlError::NotNullViolation(col.name.clone()));
            }
        }

        // CHECK constraints
        if let Some(ref col_map) = check_col_map {
            for col in &table_schema.columns {
                if let Some(ref check) = col.check_expr {
                    let result = eval_expr(check, col_map, &row)?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(&tc.expr, col_map, &row)?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

        // FK child-side validation
        for fk in &table_schema.foreign_keys {
            let any_null = fk.columns.iter().any(|&ci| row[ci as usize].is_null());
            if any_null {
                continue; // MATCH SIMPLE: skip if any FK col is NULL
            }
            let fk_vals: Vec<Value> = fk
                .columns
                .iter()
                .map(|&ci| row[ci as usize].clone())
                .collect();
            fk_key_buf.clear();
            encode_composite_key_into(&fk_vals, &mut fk_key_buf);
            let found = wtx
                .table_get(fk.foreign_table.as_bytes(), &fk_key_buf)
                .map_err(SqlError::Storage)?;
            if found.is_none() {
                let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                return Err(SqlError::ForeignKeyViolation(name.to_string()));
            }
        }

        for (j, &i) in pk_indices.iter().enumerate() {
            pk_values[j] = std::mem::replace(&mut row[i], Value::Null);
        }
        encode_composite_key_into(&pk_values, &mut key_buf);

        for (j, &i) in non_pk.iter().enumerate() {
            value_values[enc_pos[j] as usize] = std::mem::replace(&mut row[i], Value::Null);
        }
        encode_row_into(&value_values, &mut value_buf);

        if key_buf.len() > citadel_core::MAX_KEY_SIZE {
            return Err(SqlError::KeyTooLarge {
                size: key_buf.len(),
                max: citadel_core::MAX_KEY_SIZE,
            });
        }
        if value_buf.len() > citadel_core::MAX_INLINE_VALUE_SIZE {
            return Err(SqlError::RowTooLarge {
                size: value_buf.len(),
                max: citadel_core::MAX_INLINE_VALUE_SIZE,
            });
        }

        let is_new = wtx
            .table_insert(stmt.table.as_bytes(), &key_buf, &value_buf)
            .map_err(SqlError::Storage)?;
        if !is_new {
            return Err(SqlError::DuplicateKey);
        }

        if !table_schema.indices.is_empty() {
            for (j, &i) in pk_indices.iter().enumerate() {
                row[i] = pk_values[j].clone();
            }
            for (j, &i) in non_pk.iter().enumerate() {
                row[i] = std::mem::replace(&mut value_values[enc_pos[j] as usize], Value::Null);
            }
            insert_index_entries(&mut wtx, table_schema, &row, &pk_values)?;
        }
        count += 1;
    }

    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

fn has_subquery(expr: &Expr) -> bool {
    crate::parser::has_subquery(expr)
}

fn stmt_has_subquery(stmt: &SelectStmt) -> bool {
    if let Some(ref w) = stmt.where_clause {
        if has_subquery(w) {
            return true;
        }
    }
    if let Some(ref h) = stmt.having {
        if has_subquery(h) {
            return true;
        }
    }
    for col in &stmt.columns {
        if let SelectColumn::Expr { expr, .. } = col {
            if has_subquery(expr) {
                return true;
            }
        }
    }
    for ob in &stmt.order_by {
        if has_subquery(&ob.expr) {
            return true;
        }
    }
    for join in &stmt.joins {
        if let Some(ref on_expr) = join.on_clause {
            if has_subquery(on_expr) {
                return true;
            }
        }
    }
    false
}

fn materialize_expr(
    expr: &Expr,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<Expr> {
    match expr {
        Expr::InSubquery {
            expr: e,
            subquery,
            negated,
        } => {
            let inner = materialize_expr(e, exec_sub)?;
            let qr = exec_sub(subquery)?;
            if !qr.columns.is_empty() && qr.columns.len() != 1 {
                return Err(SqlError::SubqueryMultipleColumns);
            }
            let mut values = std::collections::HashSet::new();
            let mut has_null = false;
            for row in &qr.rows {
                if row[0].is_null() {
                    has_null = true;
                } else {
                    values.insert(row[0].clone());
                }
            }
            Ok(Expr::InSet {
                expr: Box::new(inner),
                values,
                has_null,
                negated: *negated,
            })
        }
        Expr::ScalarSubquery(subquery) => {
            let qr = exec_sub(subquery)?;
            if qr.rows.len() > 1 {
                return Err(SqlError::SubqueryMultipleRows);
            }
            let val = if qr.rows.is_empty() {
                Value::Null
            } else {
                qr.rows[0][0].clone()
            };
            Ok(Expr::Literal(val))
        }
        Expr::Exists { subquery, negated } => {
            let qr = exec_sub(subquery)?;
            let exists = !qr.rows.is_empty();
            let result = if *negated { !exists } else { exists };
            Ok(Expr::Literal(Value::Boolean(result)))
        }
        Expr::InList {
            expr: e,
            list,
            negated,
        } => {
            let inner = materialize_expr(e, exec_sub)?;
            let items = list
                .iter()
                .map(|item| materialize_expr(item, exec_sub))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::InList {
                expr: Box::new(inner),
                list: items,
                negated: *negated,
            })
        }
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(materialize_expr(left, exec_sub)?),
            op: *op,
            right: Box::new(materialize_expr(right, exec_sub)?),
        }),
        Expr::UnaryOp { op, expr: e } => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(materialize_expr(e, exec_sub)?),
        }),
        Expr::IsNull(e) => Ok(Expr::IsNull(Box::new(materialize_expr(e, exec_sub)?))),
        Expr::IsNotNull(e) => Ok(Expr::IsNotNull(Box::new(materialize_expr(e, exec_sub)?))),
        Expr::InSet {
            expr: e,
            values,
            has_null,
            negated,
        } => Ok(Expr::InSet {
            expr: Box::new(materialize_expr(e, exec_sub)?),
            values: values.clone(),
            has_null: *has_null,
            negated: *negated,
        }),
        Expr::Between {
            expr: e,
            low,
            high,
            negated,
        } => Ok(Expr::Between {
            expr: Box::new(materialize_expr(e, exec_sub)?),
            low: Box::new(materialize_expr(low, exec_sub)?),
            high: Box::new(materialize_expr(high, exec_sub)?),
            negated: *negated,
        }),
        Expr::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => {
            let esc = escape
                .as_ref()
                .map(|es| materialize_expr(es, exec_sub).map(Box::new))
                .transpose()?;
            Ok(Expr::Like {
                expr: Box::new(materialize_expr(e, exec_sub)?),
                pattern: Box::new(materialize_expr(pattern, exec_sub)?),
                escape: esc,
                negated: *negated,
            })
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let op = operand
                .as_ref()
                .map(|e| materialize_expr(e, exec_sub).map(Box::new))
                .transpose()?;
            let conds = conditions
                .iter()
                .map(|(c, r)| {
                    Ok((
                        materialize_expr(c, exec_sub)?,
                        materialize_expr(r, exec_sub)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let else_r = else_result
                .as_ref()
                .map(|e| materialize_expr(e, exec_sub).map(Box::new))
                .transpose()?;
            Ok(Expr::Case {
                operand: op,
                conditions: conds,
                else_result: else_r,
            })
        }
        Expr::Coalesce(args) => {
            let materialized = args
                .iter()
                .map(|a| materialize_expr(a, exec_sub))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::Coalesce(materialized))
        }
        Expr::Cast { expr: e, data_type } => Ok(Expr::Cast {
            expr: Box::new(materialize_expr(e, exec_sub)?),
            data_type: *data_type,
        }),
        Expr::Function { name, args } => {
            let materialized = args
                .iter()
                .map(|a| materialize_expr(a, exec_sub))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::Function {
                name: name.clone(),
                args: materialized,
            })
        }
        other => Ok(other.clone()),
    }
}

fn materialize_stmt(
    stmt: &SelectStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<SelectStmt> {
    let where_clause = stmt
        .where_clause
        .as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    let having = stmt
        .having
        .as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    let columns = stmt
        .columns
        .iter()
        .map(|c| match c {
            SelectColumn::AllColumns => Ok(SelectColumn::AllColumns),
            SelectColumn::Expr { expr, alias } => Ok(SelectColumn::Expr {
                expr: materialize_expr(expr, exec_sub)?,
                alias: alias.clone(),
            }),
        })
        .collect::<Result<Vec<_>>>()?;
    let order_by = stmt
        .order_by
        .iter()
        .map(|ob| {
            Ok(OrderByItem {
                expr: materialize_expr(&ob.expr, exec_sub)?,
                descending: ob.descending,
                nulls_first: ob.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let joins = stmt
        .joins
        .iter()
        .map(|j| {
            let on_clause = j
                .on_clause
                .as_ref()
                .map(|e| materialize_expr(e, exec_sub))
                .transpose()?;
            Ok(JoinClause {
                join_type: j.join_type,
                table: j.table.clone(),
                on_clause,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let group_by = stmt
        .group_by
        .iter()
        .map(|e| materialize_expr(e, exec_sub))
        .collect::<Result<Vec<_>>>()?;
    Ok(SelectStmt {
        columns,
        from: stmt.from.clone(),
        from_alias: stmt.from_alias.clone(),
        joins,
        distinct: stmt.distinct,
        where_clause,
        order_by,
        limit: stmt.limit.clone(),
        offset: stmt.offset.clone(),
        group_by,
        having,
    })
}

type CteContext = HashMap<String, QueryResult>;
type ScanTableFn<'a> = &'a mut dyn FnMut(&str) -> Result<(TableSchema, Vec<Vec<Value>>)>;

fn exec_subquery_read(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<QueryResult> {
    match exec_select(db, schema, stmt, ctes)? {
        ExecutionResult::Query(qr) => Ok(qr),
        _ => Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        }),
    }
}

fn exec_subquery_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<QueryResult> {
    match exec_select_in_txn(wtx, schema, stmt, ctes)? {
        ExecutionResult::Query(qr) => Ok(qr),
        _ => Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        }),
    }
}

fn update_has_subquery(stmt: &UpdateStmt) -> bool {
    stmt.where_clause.as_ref().is_some_and(has_subquery)
        || stmt.assignments.iter().any(|(_, e)| has_subquery(e))
}

fn materialize_update(
    stmt: &UpdateStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<UpdateStmt> {
    let where_clause = stmt
        .where_clause
        .as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    let assignments = stmt
        .assignments
        .iter()
        .map(|(name, expr)| Ok((name.clone(), materialize_expr(expr, exec_sub)?)))
        .collect::<Result<Vec<_>>>()?;
    Ok(UpdateStmt {
        table: stmt.table.clone(),
        assignments,
        where_clause,
    })
}

fn delete_has_subquery(stmt: &DeleteStmt) -> bool {
    stmt.where_clause.as_ref().is_some_and(has_subquery)
}

fn materialize_delete(
    stmt: &DeleteStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<DeleteStmt> {
    let where_clause = stmt
        .where_clause
        .as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    Ok(DeleteStmt {
        table: stmt.table.clone(),
        where_clause,
    })
}

fn insert_has_subquery(stmt: &InsertStmt) -> bool {
    match &stmt.source {
        InsertSource::Values(rows) => rows.iter().any(|row| row.iter().any(has_subquery)),
        InsertSource::Select(sq) => {
            sq.ctes.iter().any(|c| query_body_has_subquery(&c.body))
                || query_body_has_subquery(&sq.body)
        }
    }
}

fn query_body_has_subquery(body: &QueryBody) -> bool {
    match body {
        QueryBody::Select(sel) => stmt_has_subquery(sel),
        QueryBody::Compound(comp) => {
            query_body_has_subquery(&comp.left) || query_body_has_subquery(&comp.right)
        }
    }
}

fn materialize_insert(
    stmt: &InsertStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<InsertStmt> {
    let source = match &stmt.source {
        InsertSource::Values(rows) => {
            let mat = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|e| materialize_expr(e, exec_sub))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            InsertSource::Values(mat)
        }
        InsertSource::Select(sq) => {
            let ctes = sq
                .ctes
                .iter()
                .map(|c| {
                    Ok(CteDefinition {
                        name: c.name.clone(),
                        column_aliases: c.column_aliases.clone(),
                        body: materialize_query_body(&c.body, exec_sub)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let body = materialize_query_body(&sq.body, exec_sub)?;
            InsertSource::Select(Box::new(SelectQuery {
                ctes,
                recursive: sq.recursive,
                body,
            }))
        }
    };
    Ok(InsertStmt {
        table: stmt.table.clone(),
        columns: stmt.columns.clone(),
        source,
    })
}

fn materialize_query_body(
    body: &QueryBody,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<QueryBody> {
    match body {
        QueryBody::Select(sel) => Ok(QueryBody::Select(Box::new(materialize_stmt(
            sel, exec_sub,
        )?))),
        QueryBody::Compound(comp) => Ok(QueryBody::Compound(CompoundSelect {
            op: comp.op.clone(),
            all: comp.all,
            left: Box::new(materialize_query_body(&comp.left, exec_sub)?),
            right: Box::new(materialize_query_body(&comp.right, exec_sub)?),
            order_by: comp.order_by.clone(),
            limit: comp.limit.clone(),
            offset: comp.offset.clone(),
        })),
    }
}

fn exec_query_body(
    db: &Database,
    schema: &SchemaManager,
    body: &QueryBody,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    match body {
        QueryBody::Select(sel) => exec_select(db, schema, sel, ctes),
        QueryBody::Compound(comp) => exec_compound_select(db, schema, comp, ctes),
    }
}

fn exec_query_body_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    body: &QueryBody,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    match body {
        QueryBody::Select(sel) => exec_select_in_txn(wtx, schema, sel, ctes),
        QueryBody::Compound(comp) => exec_compound_select_in_txn(wtx, schema, comp, ctes),
    }
}

fn exec_query_body_read(
    db: &Database,
    schema: &SchemaManager,
    body: &QueryBody,
    ctes: &CteContext,
) -> Result<QueryResult> {
    match exec_query_body(db, schema, body, ctes)? {
        ExecutionResult::Query(qr) => Ok(qr),
        _ => Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        }),
    }
}

fn exec_query_body_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    body: &QueryBody,
    ctes: &CteContext,
) -> Result<QueryResult> {
    match exec_query_body_in_txn(wtx, schema, body, ctes)? {
        ExecutionResult::Query(qr) => Ok(qr),
        _ => Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        }),
    }
}

fn exec_compound_select(
    db: &Database,
    schema: &SchemaManager,
    comp: &CompoundSelect,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let left_qr = match exec_query_body(db, schema, &comp.left, ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => QueryResult {
            columns: vec![],
            rows: vec![],
        },
    };
    let right_qr = match exec_query_body(db, schema, &comp.right, ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => QueryResult {
            columns: vec![],
            rows: vec![],
        },
    };
    apply_set_operation(comp, left_qr, right_qr)
}

fn exec_compound_select_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    comp: &CompoundSelect,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let left_qr = match exec_query_body_in_txn(wtx, schema, &comp.left, ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => QueryResult {
            columns: vec![],
            rows: vec![],
        },
    };
    let right_qr = match exec_query_body_in_txn(wtx, schema, &comp.right, ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => QueryResult {
            columns: vec![],
            rows: vec![],
        },
    };
    apply_set_operation(comp, left_qr, right_qr)
}

// ── CTE support ──────────────────────────────────────────────────────

fn exec_select_query(
    db: &Database,
    schema: &SchemaManager,
    sq: &SelectQuery,
) -> Result<ExecutionResult> {
    if let Some(fused) = try_fuse_cte(sq) {
        let empty = CteContext::new();
        return exec_query_body(db, schema, &fused, &empty);
    }
    let ctes = materialize_all_ctes(&sq.ctes, sq.recursive, &mut |body, ctx| {
        exec_query_body_read(db, schema, body, ctx)
    })?;
    exec_query_body(db, schema, &sq.body, &ctes)
}

fn exec_select_query_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    sq: &SelectQuery,
) -> Result<ExecutionResult> {
    if let Some(fused) = try_fuse_cte(sq) {
        let empty = CteContext::new();
        return exec_query_body_in_txn(wtx, schema, &fused, &empty);
    }
    let ctes = materialize_all_ctes(&sq.ctes, sq.recursive, &mut |body, ctx| {
        exec_query_body_write(wtx, schema, body, ctx)
    })?;
    exec_query_body_in_txn(wtx, schema, &sq.body, &ctes)
}

/// Inline a single simple CTE into a direct query against the real table.
fn try_fuse_cte(sq: &SelectQuery) -> Option<QueryBody> {
    if sq.ctes.len() != 1 || sq.recursive {
        return None;
    }
    let cte = &sq.ctes[0];
    if !cte.column_aliases.is_empty() {
        return None;
    }

    let inner = match &cte.body {
        QueryBody::Select(s) => s.as_ref(),
        _ => return None,
    };

    if !inner.joins.is_empty()
        || !inner.group_by.is_empty()
        || inner.distinct
        || inner.having.is_some()
        || inner.limit.is_some()
        || inner.offset.is_some()
        || !inner.order_by.is_empty()
        || stmt_has_subquery(inner)
    {
        return None;
    }

    let all_simple_refs = inner.columns.iter().all(|c| match c {
        SelectColumn::AllColumns => true,
        SelectColumn::Expr { expr, alias } => alias.is_none() && matches!(expr, Expr::Column(_)),
    });
    if !all_simple_refs {
        return None;
    }

    let outer = match &sq.body {
        QueryBody::Select(s) => s.as_ref(),
        _ => return None,
    };
    if !outer.from.eq_ignore_ascii_case(&cte.name) || !outer.joins.is_empty() {
        return None;
    }

    let merged_where = match (&inner.where_clause, &outer.where_clause) {
        (Some(iw), Some(ow)) => Some(Expr::BinaryOp {
            left: Box::new(iw.clone()),
            op: BinOp::And,
            right: Box::new(ow.clone()),
        }),
        (Some(w), None) | (None, Some(w)) => Some(w.clone()),
        (None, None) => None,
    };

    let fused = SelectStmt {
        columns: outer.columns.clone(),
        from: inner.from.clone(),
        from_alias: inner.from_alias.clone(),
        joins: vec![],
        distinct: outer.distinct,
        where_clause: merged_where,
        order_by: outer.order_by.clone(),
        limit: outer.limit.clone(),
        offset: outer.offset.clone(),
        group_by: outer.group_by.clone(),
        having: outer.having.clone(),
    };

    Some(QueryBody::Select(Box::new(fused)))
}

fn materialize_all_ctes(
    defs: &[CteDefinition],
    recursive: bool,
    exec_body: &mut dyn FnMut(&QueryBody, &CteContext) -> Result<QueryResult>,
) -> Result<CteContext> {
    let mut ctx = CteContext::new();
    for cte in defs {
        let qr = if recursive && cte_body_references_self(&cte.body, &cte.name) {
            materialize_recursive_cte(cte, &ctx, exec_body)?
        } else {
            materialize_cte(cte, &ctx, exec_body)?
        };
        ctx.insert(cte.name.clone(), qr);
    }
    Ok(ctx)
}

fn materialize_cte(
    cte: &CteDefinition,
    ctx: &CteContext,
    exec_body: &mut dyn FnMut(&QueryBody, &CteContext) -> Result<QueryResult>,
) -> Result<QueryResult> {
    let mut qr = exec_body(&cte.body, ctx)?;
    if !cte.column_aliases.is_empty() {
        if cte.column_aliases.len() != qr.columns.len() {
            return Err(SqlError::CteColumnAliasMismatch {
                name: cte.name.clone(),
                expected: cte.column_aliases.len(),
                got: qr.columns.len(),
            });
        }
        qr.columns = cte.column_aliases.clone();
    }
    Ok(qr)
}

const MAX_RECURSIVE_ITERATIONS: usize = 10_000;

fn materialize_recursive_cte(
    cte: &CteDefinition,
    ctx: &CteContext,
    exec_body: &mut dyn FnMut(&QueryBody, &CteContext) -> Result<QueryResult>,
) -> Result<QueryResult> {
    let (anchor_body, recursive_body, union_all) = match &cte.body {
        QueryBody::Compound(comp) if matches!(comp.op, SetOp::Union) => {
            (&*comp.left, &*comp.right, comp.all)
        }
        _ => return Err(SqlError::RecursiveCteNoUnion(cte.name.clone())),
    };

    let anchor_qr = exec_body(anchor_body, ctx)?;
    let columns = if !cte.column_aliases.is_empty() {
        if cte.column_aliases.len() != anchor_qr.columns.len() {
            return Err(SqlError::CteColumnAliasMismatch {
                name: cte.name.clone(),
                expected: cte.column_aliases.len(),
                got: anchor_qr.columns.len(),
            });
        }
        cte.column_aliases.clone()
    } else {
        anchor_qr.columns
    };

    let mut accumulated = anchor_qr.rows;
    let mut working_rows = accumulated.clone();
    let mut seen = if !union_all {
        let mut s = std::collections::HashSet::new();
        for row in &accumulated {
            s.insert(row.clone());
        }
        Some(s)
    } else {
        None
    };

    let cte_key = cte.name.clone();

    let fast_sel = match recursive_body {
        QueryBody::Select(sel)
            if sel.from.eq_ignore_ascii_case(&cte_key)
                && sel.joins.is_empty()
                && sel.group_by.is_empty()
                && !sel.distinct
                && sel.having.is_none()
                && sel.limit.is_none()
                && sel.offset.is_none()
                && sel.order_by.is_empty()
                && !stmt_has_subquery(sel) =>
        {
            Some(sel.as_ref())
        }
        _ => None,
    };

    if let Some(sel) = fast_sel {
        let cte_cols: Vec<ColumnDef> = columns
            .iter()
            .enumerate()
            .map(|(i, name)| ColumnDef {
                name: name.clone(),
                data_type: DataType::Null,
                nullable: true,
                position: i as u16,
                default_expr: None,
                default_sql: None,
                check_expr: None,
                check_sql: None,
                check_name: None,
            })
            .collect();
        let col_map = ColumnMap::new(&cte_cols);
        let ncols = sel.columns.len();

        for iteration in 0..MAX_RECURSIVE_ITERATIONS {
            if working_rows.is_empty() {
                break;
            }

            let mut step_rows = Vec::with_capacity(working_rows.len());
            for row in &working_rows {
                if let Some(ref w) = sel.where_clause {
                    match eval_expr(w, &col_map, row) {
                        Ok(val) if is_truthy(&val) => {}
                        Ok(_) => continue,
                        Err(e) => return Err(e),
                    }
                }
                let mut out = Vec::with_capacity(ncols);
                for col in &sel.columns {
                    match col {
                        SelectColumn::Expr { expr, .. } => {
                            out.push(eval_expr(expr, &col_map, row)?);
                        }
                        SelectColumn::AllColumns => {
                            out.extend_from_slice(row);
                        }
                    }
                }
                step_rows.push(out);
            }

            if step_rows.is_empty() {
                break;
            }

            let new_rows = if let Some(ref mut seen_set) = seen {
                step_rows
                    .into_iter()
                    .filter(|r| seen_set.insert(r.clone()))
                    .collect::<Vec<_>>()
            } else {
                step_rows
            };

            if new_rows.is_empty() {
                break;
            }

            accumulated.extend_from_slice(&new_rows);
            working_rows = new_rows;

            if iteration == MAX_RECURSIVE_ITERATIONS - 1 {
                return Err(SqlError::RecursiveCteMaxIterations(
                    cte_key.clone(),
                    MAX_RECURSIVE_ITERATIONS,
                ));
            }
        }
    } else {
        let mut iter_ctx = ctx.clone();
        iter_ctx.insert(
            cte_key.clone(),
            QueryResult {
                columns: columns.clone(),
                rows: working_rows,
            },
        );

        for iteration in 0..MAX_RECURSIVE_ITERATIONS {
            if iter_ctx.get(&cte_key).unwrap().rows.is_empty() {
                break;
            }

            let iter_qr = exec_body(recursive_body, &iter_ctx)?;
            if iter_qr.rows.is_empty() {
                break;
            }

            let new_rows = if let Some(ref mut seen_set) = seen {
                iter_qr
                    .rows
                    .into_iter()
                    .filter(|r| seen_set.insert(r.clone()))
                    .collect::<Vec<_>>()
            } else {
                iter_qr.rows
            };

            if new_rows.is_empty() {
                break;
            }

            accumulated.extend_from_slice(&new_rows);
            iter_ctx.get_mut(&cte_key).unwrap().rows = new_rows;

            if iteration == MAX_RECURSIVE_ITERATIONS - 1 {
                return Err(SqlError::RecursiveCteMaxIterations(
                    cte_key.clone(),
                    MAX_RECURSIVE_ITERATIONS,
                ));
            }
        }

        iter_ctx.remove(&cte_key);
    }

    Ok(QueryResult {
        columns,
        rows: accumulated,
    })
}

fn cte_body_references_self(body: &QueryBody, name: &str) -> bool {
    match body {
        QueryBody::Select(sel) => {
            sel.from.eq_ignore_ascii_case(name)
                || sel
                    .joins
                    .iter()
                    .any(|j| j.table.name.eq_ignore_ascii_case(name))
        }
        QueryBody::Compound(comp) => {
            cte_body_references_self(&comp.left, name)
                || cte_body_references_self(&comp.right, name)
        }
    }
}

fn build_cte_schema(name: &str, qr: &QueryResult) -> TableSchema {
    let columns: Vec<ColumnDef> = qr
        .columns
        .iter()
        .enumerate()
        .map(|(i, col_name)| ColumnDef {
            name: col_name.clone(),
            data_type: DataType::Null,
            nullable: true,
            position: i as u16,
            default_expr: None,
            default_sql: None,
            check_expr: None,
            check_sql: None,
            check_name: None,
        })
        .collect();
    TableSchema::new(name.into(), columns, vec![], vec![], vec![], vec![])
}

fn exec_select_from_cte(
    cte_result: &QueryResult,
    stmt: &SelectStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<ExecutionResult> {
    let cte_schema = build_cte_schema(&stmt.from, cte_result);
    let actual_stmt;
    let s = if stmt_has_subquery(stmt) {
        actual_stmt = materialize_stmt(stmt, exec_sub)?;
        &actual_stmt
    } else {
        stmt
    };

    let has_aggregates = s.columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    });

    if has_aggregates || !s.group_by.is_empty() {
        if let Some(ref where_expr) = s.where_clause {
            let col_map = ColumnMap::new(&cte_schema.columns);
            let filtered: Vec<Vec<Value>> = cte_result
                .rows
                .iter()
                .filter(|row| match eval_expr(where_expr, &col_map, row) {
                    Ok(val) => is_truthy(&val),
                    _ => false,
                })
                .cloned()
                .collect();
            return exec_aggregate(&cte_schema.columns, &filtered, s);
        }
        return exec_aggregate(&cte_schema.columns, &cte_result.rows, s);
    }

    process_select(&cte_schema.columns, cte_result.rows.clone(), s, false)
}

fn exec_select_join_with_ctes(
    stmt: &SelectStmt,
    ctes: &CteContext,
    scan_table: ScanTableFn<'_>,
) -> Result<ExecutionResult> {
    let (from_schema, from_rows) = resolve_table_or_cte(&stmt.from, ctes, scan_table)?;
    let from_alias = table_alias_or_name(&stmt.from, &stmt.from_alias);

    let mut tables: Vec<(String, TableSchema)> = vec![(from_alias.clone(), from_schema)];
    let mut join_rows: Vec<Vec<Vec<Value>>> = Vec::new();

    for join in &stmt.joins {
        let jname = &join.table.name;
        let (js, jrows) = resolve_table_or_cte(jname, ctes, scan_table)?;
        let jalias = table_alias_or_name(jname, &join.table.alias);
        tables.push((jalias, js));
        join_rows.push(jrows);
    }

    let mut outer_rows = from_rows;
    let mut cur_tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), &tables[0].1)];

    for (ji, join) in stmt.joins.iter().enumerate() {
        let inner_schema = &tables[ji + 1].1;
        let inner_alias = &tables[ji + 1].0;
        let inner_rows = &join_rows[ji];

        let mut preview_tables = cur_tables.clone();
        preview_tables.push((inner_alias.clone(), inner_schema));
        let combined_cols = build_joined_columns(&preview_tables);

        let outer_col_count = if outer_rows.is_empty() {
            cur_tables.iter().map(|(_, s)| s.columns.len()).sum()
        } else {
            outer_rows[0].len()
        };
        let inner_col_count = inner_schema.columns.len();

        outer_rows = exec_join_step(
            outer_rows,
            inner_rows,
            join,
            &combined_cols,
            outer_col_count,
            inner_col_count,
            None,
            None,
        );
        cur_tables.push((inner_alias.clone(), inner_schema));
    }

    let joined_cols = build_joined_columns(&cur_tables);
    process_select(&joined_cols, outer_rows, stmt, false)
}

fn resolve_table_or_cte(
    name: &str,
    ctes: &CteContext,
    scan_table: ScanTableFn<'_>,
) -> Result<(TableSchema, Vec<Vec<Value>>)> {
    let lower = name.to_ascii_lowercase();
    if let Some(cte_result) = ctes.get(&lower) {
        let schema = build_cte_schema(&lower, cte_result);
        Ok((schema, cte_result.rows.clone()))
    } else {
        scan_table(&lower)
    }
}

fn scan_table_read(
    db: &Database,
    schema: &SchemaManager,
    name: &str,
) -> Result<(TableSchema, Vec<Vec<Value>>)> {
    let table_schema = schema
        .get(name)
        .ok_or_else(|| SqlError::TableNotFound(name.to_string()))?;
    let (rows, _) = collect_rows_read(db, table_schema, &None, None)?;
    Ok((table_schema.clone(), rows))
}

fn scan_table_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    name: &str,
) -> Result<(TableSchema, Vec<Vec<Value>>)> {
    let table_schema = schema
        .get(name)
        .ok_or_else(|| SqlError::TableNotFound(name.to_string()))?;
    let (rows, _) = collect_rows_write(wtx, table_schema, &None, None)?;
    Ok((table_schema.clone(), rows))
}

fn apply_set_operation(
    comp: &CompoundSelect,
    left_qr: QueryResult,
    right_qr: QueryResult,
) -> Result<ExecutionResult> {
    if !left_qr.columns.is_empty()
        && !right_qr.columns.is_empty()
        && left_qr.columns.len() != right_qr.columns.len()
    {
        return Err(SqlError::CompoundColumnCountMismatch {
            left: left_qr.columns.len(),
            right: right_qr.columns.len(),
        });
    }

    let columns = left_qr.columns;

    let mut rows = match (&comp.op, comp.all) {
        (SetOp::Union, true) => {
            let mut rows = left_qr.rows;
            rows.extend(right_qr.rows);
            rows
        }
        (SetOp::Union, false) => {
            let mut seen = std::collections::HashSet::new();
            let mut rows = Vec::new();
            for row in left_qr.rows.into_iter().chain(right_qr.rows) {
                if seen.insert(row.clone()) {
                    rows.push(row);
                }
            }
            rows
        }
        (SetOp::Intersect, true) => {
            let mut right_counts: std::collections::HashMap<Vec<Value>, usize> =
                std::collections::HashMap::new();
            for row in &right_qr.rows {
                *right_counts.entry(row.clone()).or_insert(0) += 1;
            }
            let mut rows = Vec::new();
            for row in left_qr.rows {
                if let Some(count) = right_counts.get_mut(&row) {
                    if *count > 0 {
                        *count -= 1;
                        rows.push(row);
                    }
                }
            }
            rows
        }
        (SetOp::Intersect, false) => {
            let right_set: std::collections::HashSet<Vec<Value>> =
                right_qr.rows.into_iter().collect();
            let mut seen = std::collections::HashSet::new();
            let mut rows = Vec::new();
            for row in left_qr.rows {
                if right_set.contains(&row) && seen.insert(row.clone()) {
                    rows.push(row);
                }
            }
            rows
        }
        (SetOp::Except, true) => {
            let mut right_counts: std::collections::HashMap<Vec<Value>, usize> =
                std::collections::HashMap::new();
            for row in &right_qr.rows {
                *right_counts.entry(row.clone()).or_insert(0) += 1;
            }
            let mut rows = Vec::new();
            for row in left_qr.rows {
                if let Some(count) = right_counts.get_mut(&row) {
                    if *count > 0 {
                        *count -= 1;
                        continue;
                    }
                }
                rows.push(row);
            }
            rows
        }
        (SetOp::Except, false) => {
            let right_set: std::collections::HashSet<Vec<Value>> =
                right_qr.rows.into_iter().collect();
            let mut seen = std::collections::HashSet::new();
            let mut rows = Vec::new();
            for row in left_qr.rows {
                if !right_set.contains(&row) && seen.insert(row.clone()) {
                    rows.push(row);
                }
            }
            rows
        }
    };

    if !comp.order_by.is_empty() {
        let col_defs: Vec<crate::types::ColumnDef> = columns
            .iter()
            .enumerate()
            .map(|(i, name)| crate::types::ColumnDef {
                name: name.clone(),
                data_type: crate::types::DataType::Null,
                nullable: true,
                position: i as u16,
                default_expr: None,
                default_sql: None,
                check_expr: None,
                check_sql: None,
                check_name: None,
            })
            .collect();
        sort_rows(&mut rows, &comp.order_by, &col_defs)?;
    }

    if let Some(ref offset_expr) = comp.offset {
        let offset = eval_const_int(offset_expr)?.max(0) as usize;
        if offset < rows.len() {
            rows = rows.split_off(offset);
        } else {
            rows.clear();
        }
    }

    if let Some(ref limit_expr) = comp.limit {
        let limit = eval_const_int(limit_expr)?.max(0) as usize;
        rows.truncate(limit);
    }

    Ok(ExecutionResult::Query(QueryResult { columns, rows }))
}

fn exec_select(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if stmt_has_subquery(stmt) {
        materialized =
            materialize_stmt(stmt, &mut |sub| exec_subquery_read(db, schema, sub, ctes))?;
        &materialized
    } else {
        stmt
    };

    if stmt.from.is_empty() {
        return exec_select_no_from(stmt);
    }

    let lower_name = stmt.from.to_ascii_lowercase();

    if let Some(cte_result) = ctes.get(&lower_name) {
        if stmt.joins.is_empty() {
            return exec_select_from_cte(cte_result, stmt, &mut |sub| {
                exec_subquery_read(db, schema, sub, ctes)
            });
        } else {
            return exec_select_join_with_ctes(stmt, ctes, &mut |name| {
                scan_table_read(db, schema, name)
            });
        }
    }

    if !ctes.is_empty()
        && stmt
            .joins
            .iter()
            .any(|j| ctes.contains_key(&j.table.name.to_ascii_lowercase()))
    {
        return exec_select_join_with_ctes(stmt, ctes, &mut |name| {
            scan_table_read(db, schema, name)
        });
    }

    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.from.clone()))?;

    if !stmt.joins.is_empty() {
        return exec_select_join(db, schema, stmt);
    }

    if let Some(result) = try_count_star_shortcut(stmt, || {
        let mut rtx = db.begin_read();
        rtx.table_entry_count(lower_name.as_bytes())
            .map_err(SqlError::Storage)
    })? {
        return Ok(result);
    }

    if let Some(plan) = StreamAggPlan::try_new(stmt, table_schema)? {
        let mut states: Vec<AggState> = plan.ops.iter().map(|(op, _)| AggState::new(op)).collect();
        let mut scan_err: Option<SqlError> = None;
        let mut rtx = db.begin_read();
        if stmt.where_clause.is_none() {
            rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
                plan.feed_row_raw(key, value, &mut states, &mut scan_err)
            })
            .map_err(SqlError::Storage)?;
        } else {
            let col_map = ColumnMap::new(&table_schema.columns);
            rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
                plan.feed_row(
                    key,
                    value,
                    table_schema,
                    &col_map,
                    &stmt.where_clause,
                    &mut states,
                    &mut scan_err,
                )
            })
            .map_err(SqlError::Storage)?;
        }
        if let Some(e) = scan_err {
            return Err(e);
        }
        return Ok(plan.finish(states));
    }

    if let Some(plan) = StreamGroupByPlan::try_new(stmt, table_schema)? {
        let lower = lower_name.clone();
        let mut rtx = db.begin_read();
        return plan
            .execute_scan(|cb| rtx.table_scan_raw(lower.as_bytes(), |key, value| cb(key, value)));
    }

    if let Some(plan) = TopKScanPlan::try_new(stmt, table_schema)? {
        let lower = lower_name.clone();
        let mut rtx = db.begin_read();
        return plan.execute_scan(table_schema, stmt, |cb| {
            rtx.table_scan_raw(lower.as_bytes(), |key, value| cb(key, value))
        });
    }

    let scan_limit = compute_scan_limit(stmt);
    let (rows, predicate_applied) =
        collect_rows_read(db, table_schema, &stmt.where_clause, scan_limit)?;
    process_select(&table_schema.columns, rows, stmt, predicate_applied)
}

fn compute_scan_limit(stmt: &SelectStmt) -> Option<usize> {
    if !stmt.order_by.is_empty()
        || !stmt.group_by.is_empty()
        || stmt.distinct
        || stmt.having.is_some()
    {
        return None;
    }
    let has_aggregates = stmt.columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    });
    if has_aggregates {
        return None;
    }
    let limit = stmt.limit.as_ref()?;
    let limit_val = eval_const_int(limit).ok()?.max(0) as usize;
    let offset_val = stmt
        .offset
        .as_ref()
        .and_then(|e| eval_const_int(e).ok())
        .unwrap_or(0)
        .max(0) as usize;
    Some(limit_val.saturating_add(offset_val))
}

fn try_count_star_shortcut(
    stmt: &SelectStmt,
    get_count: impl FnOnce() -> Result<u64>,
) -> Result<Option<ExecutionResult>> {
    if stmt.columns.len() != 1
        || stmt.where_clause.is_some()
        || !stmt.group_by.is_empty()
        || stmt.having.is_some()
    {
        return Ok(None);
    }
    let col = match &stmt.columns[0] {
        SelectColumn::Expr { expr, alias } => (expr, alias),
        _ => return Ok(None),
    };
    if !matches!(col.0, Expr::CountStar) {
        return Ok(None);
    }
    let count = get_count()? as i64;
    let col_name = col.1.as_deref().unwrap_or("COUNT(*)").to_string();
    Ok(Some(ExecutionResult::Query(QueryResult {
        columns: vec![col_name],
        rows: vec![vec![Value::Integer(count)]],
    })))
}

enum StreamAgg {
    CountStar,
    Count(usize),
    Sum(usize),
    Avg(usize),
    Min(usize),
    Max(usize),
}

enum RawAggTarget {
    CountStar,
    Pk(usize),
    NonPk(usize),
}

enum AggState {
    CountStar(i64),
    Count(i64),
    Sum {
        int_sum: i64,
        real_sum: f64,
        has_real: bool,
        all_null: bool,
    },
    Avg {
        sum: f64,
        count: i64,
    },
    Min(Option<Value>),
    Max(Option<Value>),
}

impl AggState {
    fn new(op: &StreamAgg) -> Self {
        match op {
            StreamAgg::CountStar => AggState::CountStar(0),
            StreamAgg::Count(_) => AggState::Count(0),
            StreamAgg::Sum(_) => AggState::Sum {
                int_sum: 0,
                real_sum: 0.0,
                has_real: false,
                all_null: true,
            },
            StreamAgg::Avg(_) => AggState::Avg { sum: 0.0, count: 0 },
            StreamAgg::Min(_) => AggState::Min(None),
            StreamAgg::Max(_) => AggState::Max(None),
        }
    }

    fn feed_val(&mut self, val: &Value) -> Result<()> {
        match self {
            AggState::CountStar(c) => {
                *c += 1;
            }
            AggState::Count(c) => {
                if !val.is_null() {
                    *c += 1;
                }
            }
            AggState::Sum {
                int_sum,
                real_sum,
                has_real,
                all_null,
            } => match val {
                Value::Integer(i) => {
                    *int_sum += i;
                    *all_null = false;
                }
                Value::Real(r) => {
                    *real_sum += r;
                    *has_real = true;
                    *all_null = false;
                }
                Value::Null => {}
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric".into(),
                        got: val.data_type().to_string(),
                    })
                }
            },
            AggState::Avg { sum, count } => match val {
                Value::Integer(i) => {
                    *sum += *i as f64;
                    *count += 1;
                }
                Value::Real(r) => {
                    *sum += r;
                    *count += 1;
                }
                Value::Null => {}
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric".into(),
                        got: val.data_type().to_string(),
                    })
                }
            },
            AggState::Min(cur) => {
                if !val.is_null() {
                    *cur = Some(match cur.take() {
                        None => val.clone(),
                        Some(m) => {
                            if val < &m {
                                val.clone()
                            } else {
                                m
                            }
                        }
                    });
                }
            }
            AggState::Max(cur) => {
                if !val.is_null() {
                    *cur = Some(match cur.take() {
                        None => val.clone(),
                        Some(m) => {
                            if val > &m {
                                val.clone()
                            } else {
                                m
                            }
                        }
                    });
                }
            }
        }
        Ok(())
    }

    fn feed_raw(&mut self, raw: &RawColumn) -> Result<()> {
        match self {
            AggState::CountStar(c) => {
                *c += 1;
            }
            AggState::Count(c) => {
                if !matches!(raw, RawColumn::Null) {
                    *c += 1;
                }
            }
            AggState::Sum {
                int_sum,
                real_sum,
                has_real,
                all_null,
            } => match raw {
                RawColumn::Integer(i) => {
                    *int_sum += i;
                    *all_null = false;
                }
                RawColumn::Real(r) => {
                    *real_sum += r;
                    *has_real = true;
                    *all_null = false;
                }
                RawColumn::Null => {}
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric".into(),
                        got: "non-numeric".into(),
                    })
                }
            },
            AggState::Avg { sum, count } => match raw {
                RawColumn::Integer(i) => {
                    *sum += *i as f64;
                    *count += 1;
                }
                RawColumn::Real(r) => {
                    *sum += r;
                    *count += 1;
                }
                RawColumn::Null => {}
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric".into(),
                        got: "non-numeric".into(),
                    })
                }
            },
            AggState::Min(cur) => {
                if !matches!(raw, RawColumn::Null) {
                    let val = raw.to_value();
                    *cur = Some(match cur.take() {
                        None => val,
                        Some(m) => {
                            if val < m {
                                val
                            } else {
                                m
                            }
                        }
                    });
                }
            }
            AggState::Max(cur) => {
                if !matches!(raw, RawColumn::Null) {
                    let val = raw.to_value();
                    *cur = Some(match cur.take() {
                        None => val,
                        Some(m) => {
                            if val > m {
                                val
                            } else {
                                m
                            }
                        }
                    });
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> Value {
        match self {
            AggState::CountStar(c) | AggState::Count(c) => Value::Integer(c),
            AggState::Sum {
                int_sum,
                real_sum,
                has_real,
                all_null,
            } => {
                if all_null {
                    Value::Null
                } else if has_real {
                    Value::Real(real_sum + int_sum as f64)
                } else {
                    Value::Integer(int_sum)
                }
            }
            AggState::Avg { sum, count } => {
                if count == 0 {
                    Value::Null
                } else {
                    Value::Real(sum / count as f64)
                }
            }
            AggState::Min(v) | AggState::Max(v) => v.unwrap_or(Value::Null),
        }
    }
}

struct StreamAggPlan {
    ops: Vec<(StreamAgg, String)>,
    partial_ctx: Option<PartialDecodeCtx>,
    raw_targets: Vec<RawAggTarget>,
    num_pk_cols: usize,
    nonpk_agg_defaults: Vec<Option<Value>>,
}

impl StreamAggPlan {
    fn try_new(stmt: &SelectStmt, table_schema: &TableSchema) -> Result<Option<Self>> {
        if !stmt.group_by.is_empty() || stmt.having.is_some() || !stmt.joins.is_empty() {
            return Ok(None);
        }

        let col_map = ColumnMap::new(&table_schema.columns);
        let mut ops: Vec<(StreamAgg, String)> = Vec::new();
        for sel_col in &stmt.columns {
            let (expr, alias) = match sel_col {
                SelectColumn::Expr { expr, alias } => (expr, alias),
                _ => return Ok(None),
            };
            let name = alias
                .as_deref()
                .unwrap_or(&expr_display_name(expr))
                .to_string();
            match expr {
                Expr::CountStar => ops.push((StreamAgg::CountStar, name)),
                Expr::Function {
                    name: func_name,
                    args,
                } if args.len() == 1 => {
                    let func = func_name.to_ascii_uppercase();
                    let col_idx = match resolve_simple_col(&args[0], &col_map) {
                        Some(idx) => idx,
                        None => return Ok(None),
                    };
                    match func.as_str() {
                        "COUNT" => ops.push((StreamAgg::Count(col_idx), name)),
                        "SUM" => ops.push((StreamAgg::Sum(col_idx), name)),
                        "AVG" => ops.push((StreamAgg::Avg(col_idx), name)),
                        "MIN" => ops.push((StreamAgg::Min(col_idx), name)),
                        "MAX" => ops.push((StreamAgg::Max(col_idx), name)),
                        _ => return Ok(None),
                    }
                }
                _ => return Ok(None),
            }
        }

        let mut needed: Vec<usize> = ops
            .iter()
            .filter_map(|(op, _)| match op {
                StreamAgg::CountStar => None,
                StreamAgg::Count(i)
                | StreamAgg::Sum(i)
                | StreamAgg::Avg(i)
                | StreamAgg::Min(i)
                | StreamAgg::Max(i) => Some(*i),
            })
            .collect();
        if let Some(ref where_expr) = stmt.where_clause {
            needed.extend(referenced_columns(where_expr, &table_schema.columns));
        }
        needed.sort_unstable();
        needed.dedup();

        let partial_ctx = if needed.len() < table_schema.columns.len() {
            Some(PartialDecodeCtx::new(table_schema, &needed))
        } else {
            None
        };

        let non_pk = table_schema.non_pk_indices();
        let enc_pos = table_schema.encoding_positions();
        let raw_targets: Vec<RawAggTarget> = ops
            .iter()
            .map(|(op, _)| match op {
                StreamAgg::CountStar => RawAggTarget::CountStar,
                StreamAgg::Count(idx)
                | StreamAgg::Sum(idx)
                | StreamAgg::Avg(idx)
                | StreamAgg::Min(idx)
                | StreamAgg::Max(idx) => {
                    if let Some(pk_pos) = table_schema
                        .primary_key_columns
                        .iter()
                        .position(|&i| i as usize == *idx)
                    {
                        RawAggTarget::Pk(pk_pos)
                    } else {
                        let nonpk_order = non_pk.iter().position(|&i| i == *idx).unwrap();
                        RawAggTarget::NonPk(enc_pos[nonpk_order] as usize)
                    }
                }
            })
            .collect();

        let num_pk_cols = table_schema.primary_key_columns.len();

        let mapping = table_schema.decode_col_mapping();
        let nonpk_agg_defaults: Vec<Option<Value>> = raw_targets
            .iter()
            .map(|t| match t {
                RawAggTarget::NonPk(phys_idx) => {
                    let schema_col = mapping[*phys_idx];
                    if schema_col == usize::MAX {
                        return None;
                    }
                    table_schema.columns[schema_col]
                        .default_expr
                        .as_ref()
                        .and_then(|expr| eval_const_expr(expr).ok())
                }
                _ => None,
            })
            .collect();

        Ok(Some(Self {
            ops,
            partial_ctx,
            raw_targets,
            num_pk_cols,
            nonpk_agg_defaults,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn feed_row(
        &self,
        key: &[u8],
        value: &[u8],
        table_schema: &TableSchema,
        col_map: &ColumnMap,
        where_clause: &Option<Expr>,
        states: &mut [AggState],
        scan_err: &mut Option<SqlError>,
    ) -> bool {
        let row = match &self.partial_ctx {
            Some(ctx) => match ctx.decode(key, value) {
                Ok(r) => r,
                Err(e) => {
                    *scan_err = Some(e);
                    return false;
                }
            },
            None => match decode_full_row(table_schema, key, value) {
                Ok(r) => r,
                Err(e) => {
                    *scan_err = Some(e);
                    return false;
                }
            },
        };

        if let Some(expr) = where_clause {
            match eval_expr(expr, col_map, &row) {
                Ok(val) if !is_truthy(&val) => return true,
                Err(e) => {
                    *scan_err = Some(e);
                    return false;
                }
                _ => {}
            }
        }

        for (i, (op, _)) in self.ops.iter().enumerate() {
            let val = match op {
                StreamAgg::CountStar => &Value::Null,
                StreamAgg::Count(idx)
                | StreamAgg::Sum(idx)
                | StreamAgg::Avg(idx)
                | StreamAgg::Min(idx)
                | StreamAgg::Max(idx) => &row[*idx],
            };
            if let Err(e) = states[i].feed_val(val) {
                *scan_err = Some(e);
                return false;
            }
        }
        true
    }

    fn feed_row_raw(
        &self,
        key: &[u8],
        value: &[u8],
        states: &mut [AggState],
        scan_err: &mut Option<SqlError>,
    ) -> bool {
        for (i, target) in self.raw_targets.iter().enumerate() {
            let raw = match target {
                RawAggTarget::CountStar => {
                    if let Err(e) = states[i].feed_raw(&RawColumn::Null) {
                        *scan_err = Some(e);
                        return false;
                    }
                    continue;
                }
                RawAggTarget::Pk(pk_pos) => {
                    if self.num_pk_cols == 1 && *pk_pos == 0 {
                        match decode_pk_integer(key) {
                            Ok(v) => RawColumn::Integer(v),
                            Err(e) => {
                                *scan_err = Some(e);
                                return false;
                            }
                        }
                    } else {
                        match decode_composite_key(key, self.num_pk_cols) {
                            Ok(pk) => RawColumn::Integer(match &pk[*pk_pos] {
                                Value::Integer(i) => *i,
                                _ => {
                                    *scan_err =
                                        Some(SqlError::InvalidValue("PK not integer".into()));
                                    return false;
                                }
                            }),
                            Err(e) => {
                                *scan_err = Some(e);
                                return false;
                            }
                        }
                    }
                }
                RawAggTarget::NonPk(idx) => {
                    let stored = row_non_pk_count(value);
                    if *idx >= stored {
                        if let Some(ref default) = self.nonpk_agg_defaults[i] {
                            if let Err(e) = states[i].feed_val(default) {
                                *scan_err = Some(e);
                                return false;
                            }
                        } else if let Err(e) = states[i].feed_raw(&RawColumn::Null) {
                            *scan_err = Some(e);
                            return false;
                        }
                        continue;
                    }
                    match decode_column_raw(value, *idx) {
                        Ok(v) => v,
                        Err(e) => {
                            *scan_err = Some(e);
                            return false;
                        }
                    }
                }
            };
            if let Err(e) = states[i].feed_raw(&raw) {
                *scan_err = Some(e);
                return false;
            }
        }
        true
    }

    fn finish(self, states: Vec<AggState>) -> ExecutionResult {
        let col_names: Vec<String> = self.ops.iter().map(|(_, name)| name.clone()).collect();
        let result_row: Vec<Value> = states.into_iter().map(|s| s.finish()).collect();
        ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: vec![result_row],
        })
    }
}

fn resolve_simple_col(expr: &Expr, col_map: &ColumnMap) -> Option<usize> {
    match expr {
        Expr::Column(name) => col_map.resolve(name).ok(),
        Expr::QualifiedColumn { table, column } => col_map.resolve_qualified(table, column).ok(),
        _ => None,
    }
}

enum GroupByOutputCol {
    GroupKey,
    Agg(usize),
}

struct StreamGroupByPlan {
    group_target: RawAggTarget,
    num_pk_cols: usize,
    agg_ops: Vec<StreamAgg>,
    raw_targets: Vec<RawAggTarget>,
    output: Vec<(GroupByOutputCol, String)>,
    where_pred: Option<SimplePredicate>,
}

impl StreamGroupByPlan {
    fn try_new(stmt: &SelectStmt, schema: &TableSchema) -> Result<Option<Self>> {
        if stmt.group_by.len() != 1
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || !stmt.order_by.is_empty()
            || stmt.limit.is_some()
        {
            return Ok(None);
        }

        let where_pred = stmt
            .where_clause
            .as_ref()
            .map(|expr| try_simple_predicate(expr, schema));
        // If WHERE exists but isn't a simple predicate, bail out
        if stmt.where_clause.is_some() && where_pred.as_ref().unwrap().is_none() {
            return Ok(None);
        }
        let where_pred = where_pred.flatten();

        let col_map = ColumnMap::new(&schema.columns);

        let group_col_idx = match &stmt.group_by[0] {
            Expr::Column(name) => col_map.resolve(name).ok(),
            _ => None,
        };
        let group_col_idx = match group_col_idx {
            Some(idx) => idx,
            None => return Ok(None),
        };

        if schema.columns[group_col_idx].data_type != DataType::Integer {
            return Ok(None);
        }

        let non_pk = schema.non_pk_indices();
        let enc_pos = schema.encoding_positions();
        let group_target = if let Some(pk_pos) = schema
            .primary_key_columns
            .iter()
            .position(|&i| i as usize == group_col_idx)
        {
            RawAggTarget::Pk(pk_pos)
        } else {
            let nonpk_order = non_pk.iter().position(|&i| i == group_col_idx).unwrap();
            RawAggTarget::NonPk(enc_pos[nonpk_order] as usize)
        };

        let mut agg_ops = Vec::new();
        let mut raw_targets = Vec::new();
        let mut output = Vec::new();

        for sel_col in &stmt.columns {
            let (expr, alias) = match sel_col {
                SelectColumn::Expr { expr, alias } => (expr, alias),
                _ => return Ok(None),
            };
            let name = alias
                .as_deref()
                .unwrap_or(&expr_display_name(expr))
                .to_string();

            if let Some(idx) = resolve_simple_col(expr, &col_map) {
                if idx == group_col_idx {
                    output.push((GroupByOutputCol::GroupKey, name));
                    continue;
                }
            }

            match expr {
                Expr::CountStar => {
                    let agg_idx = agg_ops.len();
                    agg_ops.push(StreamAgg::CountStar);
                    raw_targets.push(RawAggTarget::CountStar);
                    output.push((GroupByOutputCol::Agg(agg_idx), name));
                }
                Expr::Function {
                    name: func_name,
                    args,
                } if args.len() == 1 => {
                    let func = func_name.to_ascii_uppercase();
                    let col_idx = match resolve_simple_col(&args[0], &col_map) {
                        Some(idx) => idx,
                        None => return Ok(None),
                    };
                    let target = if let Some(pk_pos) = schema
                        .primary_key_columns
                        .iter()
                        .position(|&i| i as usize == col_idx)
                    {
                        RawAggTarget::Pk(pk_pos)
                    } else {
                        let nonpk_order = non_pk.iter().position(|&i| i == col_idx).unwrap();
                        RawAggTarget::NonPk(enc_pos[nonpk_order] as usize)
                    };
                    let agg_idx = agg_ops.len();
                    match func.as_str() {
                        "COUNT" => agg_ops.push(StreamAgg::Count(col_idx)),
                        "SUM" => agg_ops.push(StreamAgg::Sum(col_idx)),
                        "AVG" => agg_ops.push(StreamAgg::Avg(col_idx)),
                        "MIN" => agg_ops.push(StreamAgg::Min(col_idx)),
                        "MAX" => agg_ops.push(StreamAgg::Max(col_idx)),
                        _ => return Ok(None),
                    }
                    raw_targets.push(target);
                    output.push((GroupByOutputCol::Agg(agg_idx), name));
                }
                _ => return Ok(None),
            }
        }

        Ok(Some(Self {
            group_target,
            num_pk_cols: schema.primary_key_columns.len(),
            agg_ops,
            raw_targets,
            output,
            where_pred,
        }))
    }

    fn execute_scan(
        &self,
        scan: impl FnOnce(
            &mut dyn FnMut(&[u8], &[u8]) -> bool,
        ) -> std::result::Result<(), citadel::Error>,
    ) -> Result<ExecutionResult> {
        let mut groups: HashMap<i64, Vec<AggState>> = HashMap::new();
        let mut null_group: Option<Vec<AggState>> = None;
        let mut scan_err: Option<SqlError> = None;

        scan(&mut |key, value| {
            if let Some(ref pred) = self.where_pred {
                match pred.matches_raw(key, value) {
                    Ok(true) => {}
                    Ok(false) => return true,
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                }
            }

            let group_key: Option<i64> = match &self.group_target {
                RawAggTarget::Pk(pk_pos) => {
                    if self.num_pk_cols == 1 && *pk_pos == 0 {
                        match decode_pk_integer(key) {
                            Ok(v) => Some(v),
                            Err(e) => {
                                scan_err = Some(e);
                                return false;
                            }
                        }
                    } else {
                        match decode_composite_key(key, self.num_pk_cols) {
                            Ok(pk) => match &pk[*pk_pos] {
                                Value::Integer(i) => Some(*i),
                                Value::Null => None,
                                _ => {
                                    scan_err = Some(SqlError::InvalidValue(
                                        "GROUP BY key not integer".into(),
                                    ));
                                    return false;
                                }
                            },
                            Err(e) => {
                                scan_err = Some(e);
                                return false;
                            }
                        }
                    }
                }
                RawAggTarget::NonPk(idx) => match decode_column_raw(value, *idx) {
                    Ok(RawColumn::Integer(i)) => Some(i),
                    Ok(RawColumn::Null) => None,
                    Ok(_) => {
                        scan_err = Some(SqlError::InvalidValue("GROUP BY key not integer".into()));
                        return false;
                    }
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                },
                RawAggTarget::CountStar => unreachable!(),
            };

            let states = match group_key {
                Some(k) => groups
                    .entry(k)
                    .or_insert_with(|| self.agg_ops.iter().map(AggState::new).collect()),
                None => null_group
                    .get_or_insert_with(|| self.agg_ops.iter().map(AggState::new).collect()),
            };

            for (i, target) in self.raw_targets.iter().enumerate() {
                let raw = match target {
                    RawAggTarget::CountStar => {
                        if let Err(e) = states[i].feed_raw(&RawColumn::Null) {
                            scan_err = Some(e);
                            return false;
                        }
                        continue;
                    }
                    RawAggTarget::Pk(pk_pos) => {
                        if self.num_pk_cols == 1 && *pk_pos == 0 {
                            match decode_pk_integer(key) {
                                Ok(v) => RawColumn::Integer(v),
                                Err(e) => {
                                    scan_err = Some(e);
                                    return false;
                                }
                            }
                        } else {
                            match decode_composite_key(key, self.num_pk_cols) {
                                Ok(pk) => match &pk[*pk_pos] {
                                    Value::Integer(i) => RawColumn::Integer(*i),
                                    _ => {
                                        scan_err = Some(SqlError::InvalidValue(
                                            "agg column not integer".into(),
                                        ));
                                        return false;
                                    }
                                },
                                Err(e) => {
                                    scan_err = Some(e);
                                    return false;
                                }
                            }
                        }
                    }
                    RawAggTarget::NonPk(idx) => match decode_column_raw(value, *idx) {
                        Ok(v) => v,
                        Err(e) => {
                            scan_err = Some(e);
                            return false;
                        }
                    },
                };
                if let Err(e) = states[i].feed_raw(&raw) {
                    scan_err = Some(e);
                    return false;
                }
            }
            true
        })
        .map_err(SqlError::Storage)?;

        if let Some(e) = scan_err {
            return Err(e);
        }

        let col_names: Vec<String> = self.output.iter().map(|(_, name)| name.clone()).collect();
        let null_extra = if null_group.is_some() { 1 } else { 0 };
        let mut result_rows: Vec<Vec<Value>> = Vec::with_capacity(groups.len() + null_extra);
        if let Some(states) = null_group {
            let mut row = Vec::with_capacity(self.output.len());
            let finished: Vec<Value> = states.into_iter().map(|s| s.finish()).collect();
            for (col, _) in &self.output {
                match col {
                    GroupByOutputCol::GroupKey => row.push(Value::Null),
                    GroupByOutputCol::Agg(idx) => row.push(finished[*idx].clone()),
                }
            }
            result_rows.push(row);
        }
        for (group_key, states) in groups {
            let mut row = Vec::with_capacity(self.output.len());
            let finished: Vec<Value> = states.into_iter().map(|s| s.finish()).collect();
            for (col, _) in &self.output {
                match col {
                    GroupByOutputCol::GroupKey => row.push(Value::Integer(group_key)),
                    GroupByOutputCol::Agg(idx) => row.push(finished[*idx].clone()),
                }
            }
            result_rows.push(row);
        }

        Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: result_rows,
        }))
    }
}

struct TopKScanPlan {
    sort_target: RawAggTarget,
    num_pk_cols: usize,
    descending: bool,
    nulls_first: bool,
    keep: usize,
}

impl TopKScanPlan {
    fn try_new(stmt: &SelectStmt, schema: &TableSchema) -> Result<Option<Self>> {
        if stmt.order_by.len() != 1
            || stmt.limit.is_none()
            || stmt.where_clause.is_some()
            || !stmt.group_by.is_empty()
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || stmt.distinct
        {
            return Ok(None);
        }

        let has_aggregates = stmt.columns.iter().any(|c| match c {
            SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
            _ => false,
        });
        if has_aggregates {
            return Ok(None);
        }

        let ob = &stmt.order_by[0];
        let col_map = ColumnMap::new(&schema.columns);
        let col_idx = match resolve_simple_col(&ob.expr, &col_map) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let non_pk = schema.non_pk_indices();
        let enc_pos_arr = schema.encoding_positions();
        let sort_target = if let Some(pk_pos) = schema
            .primary_key_columns
            .iter()
            .position(|&i| i as usize == col_idx)
        {
            RawAggTarget::Pk(pk_pos)
        } else {
            let nonpk_order = non_pk.iter().position(|&i| i == col_idx).unwrap();
            RawAggTarget::NonPk(enc_pos_arr[nonpk_order] as usize)
        };

        let limit = eval_const_int(stmt.limit.as_ref().unwrap())?.max(0) as usize;
        let offset = stmt
            .offset
            .as_ref()
            .map(eval_const_int)
            .transpose()?
            .unwrap_or(0)
            .max(0) as usize;
        let keep = limit.saturating_add(offset);
        if keep == 0 {
            return Ok(None);
        }

        Ok(Some(Self {
            sort_target,
            num_pk_cols: schema.primary_key_columns.len(),
            descending: ob.descending,
            nulls_first: ob.nulls_first.unwrap_or(!ob.descending),
            keep,
        }))
    }

    fn execute_scan(
        &self,
        schema: &TableSchema,
        stmt: &SelectStmt,
        scan: impl FnOnce(
            &mut dyn FnMut(&[u8], &[u8]) -> bool,
        ) -> std::result::Result<(), citadel::Error>,
    ) -> Result<ExecutionResult> {
        use std::cmp::Ordering;
        use std::collections::BinaryHeap;

        struct Candidate {
            sort_key: Value,
            raw_key: Vec<u8>,
            raw_value: Vec<u8>,
        }

        struct CandWrapper {
            c: Candidate,
            descending: bool,
            nulls_first: bool,
        }

        impl PartialEq for CandWrapper {
            fn eq(&self, other: &Self) -> bool {
                self.cmp(other) == Ordering::Equal
            }
        }
        impl Eq for CandWrapper {}

        impl PartialOrd for CandWrapper {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        // Max-heap: worst candidate on top for eviction.
        impl Ord for CandWrapper {
            fn cmp(&self, other: &Self) -> Ordering {
                let ord = match (self.c.sort_key.is_null(), other.c.sort_key.is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => {
                        if self.nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (false, true) => {
                        if self.nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    (false, false) => self.c.sort_key.cmp(&other.c.sort_key),
                };
                if self.descending {
                    ord.reverse()
                } else {
                    ord
                }
            }
        }

        let k = self.keep;
        let mut heap: BinaryHeap<CandWrapper> = BinaryHeap::with_capacity(k + 1);
        let mut scan_err: Option<SqlError> = None;

        scan(&mut |key, value| {
            let sort_key: Value = match &self.sort_target {
                RawAggTarget::Pk(pk_pos) => {
                    if self.num_pk_cols == 1 && *pk_pos == 0 {
                        match decode_pk_integer(key) {
                            Ok(v) => Value::Integer(v),
                            Err(e) => {
                                scan_err = Some(e);
                                return false;
                            }
                        }
                    } else {
                        match decode_composite_key(key, self.num_pk_cols) {
                            Ok(mut pk) => std::mem::replace(&mut pk[*pk_pos], Value::Null),
                            Err(e) => {
                                scan_err = Some(e);
                                return false;
                            }
                        }
                    }
                }
                RawAggTarget::NonPk(idx) => match decode_column_raw(value, *idx) {
                    Ok(raw) => raw.to_value(),
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                },
                RawAggTarget::CountStar => unreachable!(),
            };

            // Heap full and can't beat worst - skip
            if heap.len() >= k {
                if let Some(top) = heap.peek() {
                    let ord = match (sort_key.is_null(), top.c.sort_key.is_null()) {
                        (true, true) => Ordering::Equal,
                        (true, false) => {
                            if self.nulls_first {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                        (false, true) => {
                            if self.nulls_first {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        }
                        (false, false) => sort_key.cmp(&top.c.sort_key),
                    };
                    let cmp = if self.descending { ord.reverse() } else { ord };
                    if cmp != Ordering::Less {
                        return true;
                    }
                }
            }

            let cand = CandWrapper {
                c: Candidate {
                    sort_key,
                    raw_key: key.to_vec(),
                    raw_value: value.to_vec(),
                },
                descending: self.descending,
                nulls_first: self.nulls_first,
            };

            if heap.len() < k {
                heap.push(cand);
            } else if let Some(mut top) = heap.peek_mut() {
                *top = cand;
            }

            true
        })
        .map_err(SqlError::Storage)?;

        if let Some(e) = scan_err {
            return Err(e);
        }

        let mut winners: Vec<CandWrapper> = heap.into_vec();
        winners.sort();

        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(winners.len());
        for w in &winners {
            rows.push(decode_full_row(schema, &w.c.raw_key, &w.c.raw_value)?);
        }

        if let Some(ref offset_expr) = stmt.offset {
            let offset = eval_const_int(offset_expr)?.max(0) as usize;
            if offset < rows.len() {
                rows = rows.split_off(offset);
            } else {
                rows.clear();
            }
        }
        if let Some(ref limit_expr) = stmt.limit {
            let limit = eval_const_int(limit_expr)?.max(0) as usize;
            rows.truncate(limit);
        }

        let (col_names, projected) = project_rows(&schema.columns, &stmt.columns, rows)?;
        Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: projected,
        }))
    }
}

struct SimplePredicate {
    is_pk: bool,
    pk_pos: usize,
    nonpk_idx: usize,
    op: BinOp,
    literal: Value,
    num_pk_cols: usize,
    precomputed_int: Option<i64>,
    default_int: Option<i64>,
    default_val: Option<Value>,
}

impl SimplePredicate {
    fn matches_raw(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        if let Some(target) = self.precomputed_int {
            return Ok(self.match_nonpk_int_inline(value, target));
        }
        let raw = if self.is_pk {
            if self.num_pk_cols == 1 {
                RawColumn::Integer(decode_pk_integer(key)?)
            } else {
                let pk = decode_composite_key(key, self.num_pk_cols)?;
                match &pk[self.pk_pos] {
                    Value::Integer(i) => RawColumn::Integer(*i),
                    Value::Real(r) => RawColumn::Real(*r),
                    Value::Boolean(b) => RawColumn::Boolean(*b),
                    _ => {
                        return Ok(raw_matches_op_value(
                            &pk[self.pk_pos],
                            self.op,
                            &self.literal,
                        ))
                    }
                }
            }
        } else if self.nonpk_idx >= row_non_pk_count(value) {
            return Ok(match &self.default_val {
                Some(d) => raw_matches_op_value(d, self.op, &self.literal),
                None => false,
            });
        } else {
            decode_column_raw(value, self.nonpk_idx)?
        };
        Ok(raw_matches_op(&raw, self.op, &self.literal))
    }

    #[inline(always)]
    fn match_nonpk_int_inline(&self, data: &[u8], target: i64) -> bool {
        let col_count = u16::from_le_bytes(data[0..2].try_into().unwrap()) as usize;

        if self.nonpk_idx >= col_count {
            return match self.default_int {
                Some(v) => match self.op {
                    BinOp::Eq => v == target,
                    BinOp::NotEq => v != target,
                    BinOp::Lt => v < target,
                    BinOp::Gt => v > target,
                    BinOp::LtEq => v <= target,
                    BinOp::GtEq => v >= target,
                    _ => false,
                },
                None => false,
            };
        }

        let bm_bytes = col_count.div_ceil(8);

        // NULL -> false (SQL NULL semantics)
        if data[2 + self.nonpk_idx / 8] & (1 << (self.nonpk_idx % 8)) != 0 {
            return false;
        }

        let mut pos = 2 + bm_bytes;

        // Skip preceding non-null columns by reading their length
        for col in 0..self.nonpk_idx {
            if data[2 + col / 8] & (1 << (col % 8)) == 0 {
                let len = u32::from_le_bytes(data[pos + 1..pos + 5].try_into().unwrap()) as usize;
                pos += 5 + len;
            }
        }

        // Read i64 directly: skip type_tag(1) + len(4), read 8 bytes
        let v = i64::from_le_bytes(data[pos + 5..pos + 13].try_into().unwrap());

        match self.op {
            BinOp::Eq => v == target,
            BinOp::NotEq => v != target,
            BinOp::Lt => v < target,
            BinOp::Gt => v > target,
            BinOp::LtEq => v <= target,
            BinOp::GtEq => v >= target,
            _ => false,
        }
    }
}

fn try_simple_predicate(expr: &Expr, schema: &TableSchema) -> Option<SimplePredicate> {
    let (col_name, op, literal) = match expr {
        Expr::BinaryOp { left, op, right } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(name), Expr::Literal(lit)) => (name.as_str(), *op, lit),
            (Expr::Literal(lit), Expr::Column(name)) => (name.as_str(), flip_cmp_op(*op)?, lit),
            _ => return None,
        },
        _ => return None,
    };

    if !matches!(
        op,
        BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::Gt | BinOp::LtEq | BinOp::GtEq
    ) {
        return None;
    }

    let col_idx = schema.column_index(col_name)?;
    let non_pk = schema.non_pk_indices();

    if let Some(pk_pos) = schema
        .primary_key_columns
        .iter()
        .position(|&i| i as usize == col_idx)
    {
        Some(SimplePredicate {
            is_pk: true,
            pk_pos,
            nonpk_idx: 0,
            op,
            literal: literal.clone(),
            num_pk_cols: schema.primary_key_columns.len(),
            precomputed_int: None,
            default_int: None,
            default_val: None,
        })
    } else {
        let nonpk_order = non_pk.iter().position(|&i| i == col_idx)?;
        let nonpk_idx = schema.encoding_positions()[nonpk_order] as usize;
        let precomputed_int = match literal {
            Value::Integer(i) => Some(*i),
            _ => None,
        };
        let default_val = schema.columns[col_idx]
            .default_expr
            .as_ref()
            .and_then(|expr| eval_const_expr(expr).ok());
        let default_int = default_val.as_ref().and_then(|v| match v {
            Value::Integer(i) => Some(*i),
            _ => None,
        });
        Some(SimplePredicate {
            is_pk: false,
            pk_pos: 0,
            nonpk_idx,
            op,
            literal: literal.clone(),
            num_pk_cols: schema.primary_key_columns.len(),
            precomputed_int,
            default_int,
            default_val,
        })
    }
}

fn flip_cmp_op(op: BinOp) -> Option<BinOp> {
    match op {
        BinOp::Eq => Some(BinOp::Eq),
        BinOp::NotEq => Some(BinOp::NotEq),
        BinOp::Lt => Some(BinOp::Gt),
        BinOp::Gt => Some(BinOp::Lt),
        BinOp::LtEq => Some(BinOp::GtEq),
        BinOp::GtEq => Some(BinOp::LtEq),
        _ => None,
    }
}

fn raw_matches_op(raw: &RawColumn, op: BinOp, literal: &Value) -> bool {
    // SQL NULL semantics: any comparison involving NULL yields NULL (falsy)
    if matches!(raw, RawColumn::Null) || literal.is_null() {
        return false;
    }
    match op {
        BinOp::Eq => raw.eq_value(literal),
        BinOp::NotEq => !raw.eq_value(literal),
        BinOp::Lt => raw.cmp_value(literal) == Some(std::cmp::Ordering::Less),
        BinOp::Gt => raw.cmp_value(literal) == Some(std::cmp::Ordering::Greater),
        BinOp::LtEq => raw
            .cmp_value(literal)
            .is_some_and(|o| o != std::cmp::Ordering::Greater),
        BinOp::GtEq => raw
            .cmp_value(literal)
            .is_some_and(|o| o != std::cmp::Ordering::Less),
        _ => false,
    }
}

fn raw_matches_op_value(val: &Value, op: BinOp, literal: &Value) -> bool {
    match op {
        BinOp::Eq => val == literal,
        BinOp::NotEq => val != literal && !val.is_null(),
        BinOp::Lt => val < literal,
        BinOp::Gt => val > literal,
        BinOp::LtEq => val <= literal,
        BinOp::GtEq => val >= literal,
        _ => false,
    }
}

fn exec_select_no_from(stmt: &SelectStmt) -> Result<ExecutionResult> {
    let empty_cols: Vec<ColumnDef> = vec![];
    let empty_row: Vec<Value> = vec![];
    let (col_names, projected) = project_rows(&empty_cols, &stmt.columns, vec![empty_row])?;
    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: projected,
    }))
}

fn process_select(
    columns: &[ColumnDef],
    mut rows: Vec<Vec<Value>>,
    stmt: &SelectStmt,
    predicate_applied: bool,
) -> Result<ExecutionResult> {
    if !predicate_applied {
        if let Some(ref where_expr) = stmt.where_clause {
            let col_map = ColumnMap::new(columns);
            rows.retain(|row| match eval_expr(where_expr, &col_map, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            });
        }
    }

    let has_aggregates = stmt.columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    });

    if has_aggregates || !stmt.group_by.is_empty() {
        return exec_aggregate(columns, &rows, stmt);
    }

    if stmt.distinct {
        let (col_names, mut projected) = project_rows(columns, &stmt.columns, rows)?;

        let mut seen = std::collections::HashSet::new();
        projected.retain(|row| seen.insert(row.clone()));

        if !stmt.order_by.is_empty() {
            let output_cols = build_output_columns(&stmt.columns, columns);
            sort_rows(&mut projected, &stmt.order_by, &output_cols)?;
        }

        if let Some(ref offset_expr) = stmt.offset {
            let offset = eval_const_int(offset_expr)?.max(0) as usize;
            if offset < projected.len() {
                projected = projected.split_off(offset);
            } else {
                projected.clear();
            }
        }

        if let Some(ref limit_expr) = stmt.limit {
            let limit = eval_const_int(limit_expr)?.max(0) as usize;
            projected.truncate(limit);
        }

        return Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: projected,
        }));
    }

    if !stmt.order_by.is_empty() {
        if let Some(ref limit_expr) = stmt.limit {
            let limit = eval_const_int(limit_expr)?.max(0) as usize;
            let offset = match stmt.offset {
                Some(ref e) => eval_const_int(e)?.max(0) as usize,
                None => 0,
            };
            let keep = limit.saturating_add(offset);
            if keep == 0 {
                rows.clear();
            } else if keep < rows.len() {
                topk_rows(&mut rows, &stmt.order_by, columns, keep)?;
                rows.truncate(keep);
            } else {
                sort_rows(&mut rows, &stmt.order_by, columns)?;
            }
        } else {
            sort_rows(&mut rows, &stmt.order_by, columns)?;
        }
    }

    if let Some(ref offset_expr) = stmt.offset {
        let offset = eval_const_int(offset_expr)?.max(0) as usize;
        if offset < rows.len() {
            rows = rows.split_off(offset);
        } else {
            rows.clear();
        }
    }

    if let Some(ref limit_expr) = stmt.limit {
        let limit = eval_const_int(limit_expr)?.max(0) as usize;
        rows.truncate(limit);
    }

    let (col_names, projected) = project_rows(columns, &stmt.columns, rows)?;

    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: projected,
    }))
}

fn resolve_table_name<'a>(schema: &'a SchemaManager, name: &str) -> Result<&'a TableSchema> {
    schema
        .get(name)
        .ok_or_else(|| SqlError::TableNotFound(name.to_string()))
}

fn build_joined_columns(tables: &[(String, &TableSchema)]) -> Vec<ColumnDef> {
    let mut result = Vec::new();
    let mut pos: u16 = 0;

    for (alias, schema) in tables {
        for col in &schema.columns {
            result.push(ColumnDef {
                name: format!("{}.{}", alias.to_ascii_lowercase(), col.name),
                data_type: col.data_type,
                nullable: col.nullable,
                position: pos,
                default_expr: None,
                default_sql: None,
                check_expr: None,
                check_sql: None,
                check_name: None,
            });
            pos += 1;
        }
    }

    result
}

fn extract_equi_join_keys(
    on_expr: &Expr,
    combined_cols: &[ColumnDef],
    outer_col_count: usize,
) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();

    fn flatten<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
        match e {
            Expr::BinaryOp {
                left,
                op: BinOp::And,
                right,
            } => {
                flatten(left, out);
                flatten(right, out);
            }
            _ => out.push(e),
        }
    }
    let mut conjuncts = Vec::new();
    flatten(on_expr, &mut conjuncts);

    for expr in conjuncts {
        if let Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = expr
        {
            if let (Some(l_idx), Some(r_idx)) = (
                resolve_col_idx(left, combined_cols),
                resolve_col_idx(right, combined_cols),
            ) {
                if l_idx < outer_col_count && r_idx >= outer_col_count {
                    pairs.push((l_idx, r_idx - outer_col_count));
                } else if r_idx < outer_col_count && l_idx >= outer_col_count {
                    pairs.push((r_idx, l_idx - outer_col_count));
                }
            }
        }
    }

    pairs
}

fn resolve_col_idx(expr: &Expr, columns: &[ColumnDef]) -> Option<usize> {
    match expr {
        Expr::Column(name) => {
            let matches: Vec<usize> = columns
                .iter()
                .enumerate()
                .filter(|(_, c)| {
                    c.name == *name
                        || (c.name.len() > name.len()
                            && c.name.as_bytes()[c.name.len() - name.len() - 1] == b'.'
                            && c.name.ends_with(name.as_str()))
                })
                .map(|(i, _)| i)
                .collect();
            if matches.len() == 1 {
                Some(matches[0])
            } else {
                None
            }
        }
        Expr::QualifiedColumn { table, column } => {
            let qualified = format!("{table}.{column}");
            columns.iter().position(|c| c.name == qualified)
        }
        _ => None,
    }
}

fn hash_key(row: &[Value], col_indices: &[usize]) -> Vec<Value> {
    col_indices.iter().map(|&i| row[i].clone()).collect()
}

fn count_conjuncts(expr: &Expr) -> usize {
    match expr {
        Expr::BinaryOp {
            op: BinOp::And,
            left,
            right,
        } => count_conjuncts(left) + count_conjuncts(right),
        _ => 1,
    }
}

fn combine_row(outer: &[Value], inner: &[Value], cap: usize) -> Vec<Value> {
    let mut combined = Vec::with_capacity(cap);
    combined.extend(outer.iter().cloned());
    combined.extend(inner.iter().cloned());
    combined
}

struct CombineProjection {
    slots: Vec<(usize, bool)>,
}

fn combine_row_projected(outer: &[Value], inner: &[Value], proj: &CombineProjection) -> Vec<Value> {
    proj.slots
        .iter()
        .map(|&(idx, is_inner)| {
            if is_inner {
                inner[idx].clone()
            } else {
                outer[idx].clone()
            }
        })
        .collect()
}

fn build_combine_projection(
    needed_combined: &[usize],
    outer_col_count: usize,
) -> CombineProjection {
    CombineProjection {
        slots: needed_combined
            .iter()
            .map(|&ci| {
                if ci < outer_col_count {
                    (ci, false)
                } else {
                    (ci - outer_col_count, true)
                }
            })
            .collect(),
    }
}

fn build_projected_columns(full_cols: &[ColumnDef], needed_combined: &[usize]) -> Vec<ColumnDef> {
    needed_combined
        .iter()
        .enumerate()
        .map(|(new_pos, &old_pos)| {
            let orig = &full_cols[old_pos];
            ColumnDef {
                name: orig.name.clone(),
                data_type: orig.data_type,
                nullable: orig.nullable,
                position: new_pos as u16,
                default_expr: None,
                default_sql: None,
                check_expr: None,
                check_sql: None,
                check_name: None,
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn try_integer_join(
    outer_rows: Vec<Vec<Value>>,
    inner_rows: &[Vec<Value>],
    join_type: &JoinType,
    outer_key_col: usize,
    inner_key_col: usize,
    outer_col_count: usize,
    inner_col_count: usize,
    outer_is_sorted: bool,
    projection: Option<&CombineProjection>,
) -> std::result::Result<Vec<Vec<Value>>, Vec<Vec<Value>>> {
    let cap = projection.map_or(outer_col_count + inner_col_count, |p| p.slots.len());

    if outer_is_sorted && matches!(join_type, JoinType::Inner | JoinType::Cross) {
        let mut sorted_inner: Vec<(i64, usize)> = Vec::with_capacity(inner_rows.len());
        let mut needs_sort = false;
        let mut prev = i64::MIN;
        for (i, r) in inner_rows.iter().enumerate() {
            match r[inner_key_col] {
                Value::Integer(k) => {
                    if k < prev {
                        needs_sort = true;
                    }
                    prev = k;
                    sorted_inner.push((k, i));
                }
                Value::Null => {}
                _ => return Err(outer_rows),
            }
        }
        if needs_sort {
            sorted_inner.sort_unstable_by_key(|&(k, _)| k);
        }

        let mut result = Vec::with_capacity(outer_rows.len());
        let mut j = 0;
        for mut outer in outer_rows {
            let ok = match outer[outer_key_col] {
                Value::Integer(i) => i,
                _ => continue,
            };
            while j < sorted_inner.len() && sorted_inner[j].0 < ok {
                j += 1;
            }
            let mut kk = j;
            while kk < sorted_inner.len() && sorted_inner[kk].0 == ok {
                let is_last = kk + 1 >= sorted_inner.len() || sorted_inner[kk + 1].0 != ok;
                let inner = &inner_rows[sorted_inner[kk].1];
                if let Some(proj) = projection {
                    if is_last {
                        result.push(
                            proj.slots
                                .iter()
                                .map(|&(idx, is_inner)| {
                                    if is_inner {
                                        inner[idx].clone()
                                    } else {
                                        std::mem::take(&mut outer[idx])
                                    }
                                })
                                .collect(),
                        );
                    } else {
                        result.push(combine_row_projected(&outer, inner, proj));
                    }
                } else if is_last {
                    outer.extend(inner.iter().cloned());
                    result.push(outer);
                    break;
                } else {
                    result.push(combine_row(&outer, inner, cap));
                }
                kk += 1;
            }
        }
        return Ok(result);
    }

    let mut inner_map: HashMap<i64, Vec<usize>> = HashMap::with_capacity(inner_rows.len());
    for (idx, inner) in inner_rows.iter().enumerate() {
        match &inner[inner_key_col] {
            Value::Integer(k) => inner_map.entry(*k).or_default().push(idx),
            Value::Null => {}
            _ => return Err(outer_rows),
        }
    }

    let mut result = Vec::with_capacity(inner_rows.len());

    match join_type {
        JoinType::Inner | JoinType::Cross => {
            for mut outer in outer_rows {
                if let Value::Integer(k) = outer[outer_key_col] {
                    if let Some(indices) = inner_map.get(&k) {
                        if let Some(proj) = projection {
                            for &idx in indices {
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                            }
                            let last_idx = *indices.last().unwrap();
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                        }
                    }
                }
            }
        }
        JoinType::Left => {
            for mut outer in outer_rows {
                if let Value::Integer(k) = outer[outer_key_col] {
                    if let Some(indices) = inner_map.get(&k) {
                        if let Some(proj) = projection {
                            for &idx in indices {
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                            }
                            let last_idx = *indices.last().unwrap();
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                        }
                        continue;
                    }
                }
                if let Some(proj) = projection {
                    let null_inner = vec![Value::Null; inner_col_count];
                    result.push(combine_row_projected(&outer, &null_inner, proj));
                } else {
                    outer.resize(cap, Value::Null);
                    result.push(outer);
                }
            }
        }
        JoinType::Right => {
            let mut inner_matched = vec![false; inner_rows.len()];
            for mut outer in outer_rows {
                if let Value::Integer(k) = outer[outer_key_col] {
                    if let Some(indices) = inner_map.get(&k) {
                        if let Some(proj) = projection {
                            for &idx in indices {
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                                inner_matched[idx] = true;
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                                inner_matched[idx] = true;
                            }
                            let last_idx = *indices.last().unwrap();
                            inner_matched[last_idx] = true;
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                        }
                    }
                }
            }
            for (j, inner) in inner_rows.iter().enumerate() {
                if !inner_matched[j] {
                    if let Some(proj) = projection {
                        let null_outer = vec![Value::Null; outer_col_count];
                        result.push(combine_row_projected(&null_outer, inner, proj));
                    } else {
                        let mut padded = Vec::with_capacity(cap);
                        padded.resize(outer_col_count, Value::Null);
                        padded.extend(inner.iter().cloned());
                        result.push(padded);
                    }
                }
            }
        }
    }

    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn exec_join_step(
    mut outer_rows: Vec<Vec<Value>>,
    inner_rows: &[Vec<Value>],
    join: &JoinClause,
    combined_cols: &[ColumnDef],
    outer_col_count: usize,
    inner_col_count: usize,
    outer_pk_col: Option<usize>,
    projection: Option<&CombineProjection>,
) -> Vec<Vec<Value>> {
    let equi_pairs = join
        .on_clause
        .as_ref()
        .map(|on| extract_equi_join_keys(on, combined_cols, outer_col_count))
        .unwrap_or_default();

    let is_pure_equi = join.on_clause.as_ref().map_or(true, |on| {
        !equi_pairs.is_empty() && count_conjuncts(on) == equi_pairs.len()
    });

    let effective_proj = if is_pure_equi { projection } else { None };

    if equi_pairs.len() == 1 && is_pure_equi {
        let (outer_key_col, inner_key_col) = equi_pairs[0];
        let outer_is_sorted = outer_pk_col == Some(outer_key_col);
        match try_integer_join(
            outer_rows,
            inner_rows,
            &join.join_type,
            outer_key_col,
            inner_key_col,
            outer_col_count,
            inner_col_count,
            outer_is_sorted,
            effective_proj,
        ) {
            Ok(result) => return result,
            Err(rows) => outer_rows = rows,
        }
    }

    let outer_key_cols: Vec<usize> = equi_pairs.iter().map(|&(o, _)| o).collect();
    let inner_key_cols: Vec<usize> = equi_pairs.iter().map(|&(_, i)| i).collect();

    let mut inner_map: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();
    for (idx, inner) in inner_rows.iter().enumerate() {
        inner_map
            .entry(hash_key(inner, &inner_key_cols))
            .or_default()
            .push(idx);
    }

    let cap = effective_proj.map_or(outer_col_count + inner_col_count, |p| p.slots.len());
    let mut result = Vec::new();

    if is_pure_equi {
        match join.join_type {
            JoinType::Inner | JoinType::Cross => {
                for mut outer in outer_rows {
                    let key = hash_key(&outer, &outer_key_cols);
                    if let Some(indices) = inner_map.get(&key) {
                        if let Some(proj) = effective_proj {
                            for &idx in indices {
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                            }
                            let last_idx = *indices.last().unwrap();
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                        }
                    }
                }
            }
            JoinType::Left => {
                for mut outer in outer_rows {
                    let key = hash_key(&outer, &outer_key_cols);
                    if let Some(indices) = inner_map.get(&key) {
                        if let Some(proj) = effective_proj {
                            for &idx in indices {
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                            }
                            let last_idx = *indices.last().unwrap();
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                        }
                    } else if let Some(proj) = effective_proj {
                        let null_inner = vec![Value::Null; inner_col_count];
                        result.push(combine_row_projected(&outer, &null_inner, proj));
                    } else {
                        outer.resize(cap, Value::Null);
                        result.push(outer);
                    }
                }
            }
            JoinType::Right => {
                let mut inner_matched = vec![false; inner_rows.len()];
                for mut outer in outer_rows {
                    let key = hash_key(&outer, &outer_key_cols);
                    if let Some(indices) = inner_map.get(&key) {
                        if let Some(proj) = effective_proj {
                            for &idx in indices {
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                                inner_matched[idx] = true;
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                                inner_matched[idx] = true;
                            }
                            let last_idx = *indices.last().unwrap();
                            inner_matched[last_idx] = true;
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                        }
                    }
                }
                for (j, inner) in inner_rows.iter().enumerate() {
                    if !inner_matched[j] {
                        if let Some(proj) = effective_proj {
                            let null_outer = vec![Value::Null; outer_col_count];
                            result.push(combine_row_projected(&null_outer, inner, proj));
                        } else {
                            let mut padded = Vec::with_capacity(cap);
                            padded.resize(outer_col_count, Value::Null);
                            padded.extend(inner.iter().cloned());
                            result.push(padded);
                        }
                    }
                }
            }
        }
    } else {
        let combined_map = ColumnMap::new(combined_cols);
        let on_matches = |combined: &[Value]| -> bool {
            match join.on_clause {
                Some(ref on_expr) => eval_expr(on_expr, &combined_map, combined)
                    .map(|v| is_truthy(&v))
                    .unwrap_or(false),
                None => true,
            }
        };

        match join.join_type {
            JoinType::Inner | JoinType::Cross => {
                for outer in &outer_rows {
                    let key = hash_key(outer, &outer_key_cols);
                    if let Some(indices) = inner_map.get(&key) {
                        for &idx in indices {
                            let combined = combine_row(outer, &inner_rows[idx], cap);
                            if on_matches(&combined) {
                                result.push(combined);
                            }
                        }
                    }
                }
            }
            JoinType::Left => {
                for outer in &outer_rows {
                    let key = hash_key(outer, &outer_key_cols);
                    let mut matched = false;
                    if let Some(indices) = inner_map.get(&key) {
                        for &idx in indices {
                            let combined = combine_row(outer, &inner_rows[idx], cap);
                            if on_matches(&combined) {
                                result.push(combined);
                                matched = true;
                            }
                        }
                    }
                    if !matched {
                        let mut padded = Vec::with_capacity(cap);
                        padded.extend(outer.iter().cloned());
                        padded.resize(cap, Value::Null);
                        result.push(padded);
                    }
                }
            }
            JoinType::Right => {
                let mut inner_matched = vec![false; inner_rows.len()];
                for outer in &outer_rows {
                    let key = hash_key(outer, &outer_key_cols);
                    if let Some(indices) = inner_map.get(&key) {
                        for &idx in indices {
                            let combined = combine_row(outer, &inner_rows[idx], cap);
                            if on_matches(&combined) {
                                result.push(combined);
                                inner_matched[idx] = true;
                            }
                        }
                    }
                }
                for (j, inner) in inner_rows.iter().enumerate() {
                    if !inner_matched[j] {
                        let mut padded = Vec::with_capacity(cap);
                        padded.resize(outer_col_count, Value::Null);
                        padded.extend(inner.iter().cloned());
                        result.push(padded);
                    }
                }
            }
        }
    }

    result
}

fn table_alias_or_name(name: &str, alias: &Option<String>) -> String {
    match alias {
        Some(a) => a.to_ascii_lowercase(),
        None => name.to_ascii_lowercase(),
    }
}

fn collect_all_rows_raw(
    rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    table_schema: &TableSchema,
) -> Result<Vec<Vec<Value>>> {
    let lower_name = &table_schema.name;
    let entry_count = rtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0) as usize;
    let mut rows = Vec::with_capacity(entry_count);
    let mut scan_err: Option<SqlError> = None;
    rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
        match decode_full_row(table_schema, key, value) {
            Ok(row) => rows.push(row),
            Err(e) => {
                scan_err = Some(e);
                return false;
            }
        }
        true
    })
    .map_err(SqlError::Storage)?;
    if let Some(e) = scan_err {
        return Err(e);
    }
    Ok(rows)
}

fn collect_all_rows_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
) -> Result<Vec<Vec<Value>>> {
    collect_rows_write(wtx, table_schema, &None, None).map(|(rows, _)| rows)
}

fn has_ambiguous_bare_ref(expr: &Expr, columns: &[ColumnDef]) -> bool {
    match expr {
        Expr::Column(name) => {
            let lower = name.to_ascii_lowercase();
            columns
                .iter()
                .filter(|c| c.name == lower || c.name.ends_with(&format!(".{lower}")))
                .count()
                > 1
        }
        Expr::BinaryOp { left, right, .. } => {
            has_ambiguous_bare_ref(left, columns) || has_ambiguous_bare_ref(right, columns)
        }
        Expr::UnaryOp { expr: inner, .. } | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            has_ambiguous_bare_ref(inner, columns)
        }
        Expr::Function { args, .. } | Expr::Coalesce(args) => {
            args.iter().any(|a| has_ambiguous_bare_ref(a, columns))
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            has_ambiguous_bare_ref(e, columns)
                || has_ambiguous_bare_ref(low, columns)
                || has_ambiguous_bare_ref(high, columns)
        }
        Expr::InList { expr: e, list, .. } => {
            has_ambiguous_bare_ref(e, columns)
                || list.iter().any(|a| has_ambiguous_bare_ref(a, columns))
        }
        Expr::Like {
            expr: e,
            pattern,
            escape,
            ..
        } => {
            has_ambiguous_bare_ref(e, columns)
                || has_ambiguous_bare_ref(pattern, columns)
                || escape
                    .as_ref()
                    .is_some_and(|esc| has_ambiguous_bare_ref(esc, columns))
        }
        Expr::Cast { expr: inner, .. } => has_ambiguous_bare_ref(inner, columns),
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|o| has_ambiguous_bare_ref(o, columns))
                || conditions.iter().any(|(w, t)| {
                    has_ambiguous_bare_ref(w, columns) || has_ambiguous_bare_ref(t, columns)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| has_ambiguous_bare_ref(e, columns))
        }
        _ => false,
    }
}

struct JoinColumnPlan {
    per_table: Vec<Vec<usize>>,
    output_combined: Vec<usize>,
}

fn compute_join_needed_columns(
    stmt: &SelectStmt,
    tables: &[(String, &TableSchema)],
) -> Option<JoinColumnPlan> {
    for sel in &stmt.columns {
        if matches!(sel, SelectColumn::AllColumns) {
            return None;
        }
    }

    let combined_cols = build_joined_columns(tables);

    for sel in &stmt.columns {
        if let SelectColumn::Expr { expr, .. } = sel {
            if has_ambiguous_bare_ref(expr, &combined_cols) {
                return None;
            }
        }
    }

    let mut output_combined: Vec<usize> = Vec::new();
    for sel in &stmt.columns {
        if let SelectColumn::Expr { expr, .. } = sel {
            output_combined.extend(referenced_columns(expr, &combined_cols));
        }
    }
    if let Some(w) = &stmt.where_clause {
        output_combined.extend(referenced_columns(w, &combined_cols));
    }
    for ob in &stmt.order_by {
        output_combined.extend(referenced_columns(&ob.expr, &combined_cols));
    }
    for gb in &stmt.group_by {
        output_combined.extend(referenced_columns(gb, &combined_cols));
    }
    if let Some(h) = &stmt.having {
        output_combined.extend(referenced_columns(h, &combined_cols));
    }
    output_combined.sort_unstable();
    output_combined.dedup();

    let mut needed_combined = output_combined.clone();
    for join in &stmt.joins {
        if let Some(on_expr) = &join.on_clause {
            needed_combined.extend(referenced_columns(on_expr, &combined_cols));
        }
    }
    needed_combined.sort_unstable();
    needed_combined.dedup();

    let mut offsets = Vec::with_capacity(tables.len() + 1);
    offsets.push(0usize);
    for (_, s) in tables {
        offsets.push(offsets.last().unwrap() + s.columns.len());
    }

    let mut per_table: Vec<Vec<usize>> = tables.iter().map(|_| Vec::new()).collect();
    for &ci in &needed_combined {
        for (t, _) in tables.iter().enumerate() {
            let start = offsets[t];
            let end = offsets[t + 1];
            if ci >= start && ci < end {
                per_table[t].push(ci - start);
                break;
            }
        }
    }

    Some(JoinColumnPlan {
        per_table,
        output_combined,
    })
}

fn collect_rows_partial(
    rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    table_schema: &TableSchema,
    needed: &[usize],
) -> Result<Vec<Vec<Value>>> {
    if needed.is_empty() || needed.len() == table_schema.columns.len() {
        return collect_all_rows_raw(rtx, table_schema);
    }
    let ctx = PartialDecodeCtx::new(table_schema, needed);
    let lower_name = &table_schema.name;
    let entry_count = rtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0) as usize;
    let mut rows = Vec::with_capacity(entry_count);
    let mut scan_err: Option<SqlError> = None;
    rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
        match ctx.decode(key, value) {
            Ok(row) => rows.push(row),
            Err(e) => {
                scan_err = Some(e);
                return false;
            }
        }
        true
    })
    .map_err(SqlError::Storage)?;
    if let Some(e) = scan_err {
        return Err(e);
    }
    Ok(rows)
}

fn collect_rows_partial_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    needed: &[usize],
) -> Result<Vec<Vec<Value>>> {
    if needed.is_empty() || needed.len() == table_schema.columns.len() {
        return collect_all_rows_write(wtx, table_schema);
    }
    let ctx = PartialDecodeCtx::new(table_schema, needed);
    let lower_name = &table_schema.name;
    let entry_count = wtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0) as usize;
    let mut rows = Vec::with_capacity(entry_count);
    let mut scan_err: Option<SqlError> = None;
    wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
        match ctx.decode(key, value) {
            Ok(row) => rows.push(row),
            Err(e) => {
                scan_err = Some(e);
                return Ok(false);
            }
        }
        Ok(true)
    })
    .map_err(SqlError::Storage)?;
    if let Some(e) = scan_err {
        return Err(e);
    }
    Ok(rows)
}

fn exec_select_join(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let from_schema = resolve_table_name(schema, &stmt.from)?;
    let from_alias = table_alias_or_name(&stmt.from, &stmt.from_alias);

    let mut all_tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), from_schema)];
    for join in &stmt.joins {
        let inner_schema = resolve_table_name(schema, &join.table.name)?;
        let inner_alias = table_alias_or_name(&join.table.name, &join.table.alias);
        all_tables.push((inner_alias, inner_schema));
    }
    let (needed_per_table, output_combined) = match compute_join_needed_columns(stmt, &all_tables) {
        Some(plan) => (Some(plan.per_table), Some(plan.output_combined)),
        None => (None, None),
    };

    let mut rtx = db.begin_read();
    let mut outer_rows = match &needed_per_table {
        Some(n) if !n.is_empty() => collect_rows_partial(&mut rtx, from_schema, &n[0])?,
        _ => collect_all_rows_raw(&mut rtx, from_schema)?,
    };

    let mut tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), from_schema)];
    let mut cur_outer_pk_col: Option<usize> = if from_schema.primary_key_columns.len() == 1 {
        Some(from_schema.primary_key_columns[0] as usize)
    } else {
        None
    };

    let num_joins = stmt.joins.len();
    let mut last_combined_cols: Option<Vec<ColumnDef>> = None;
    for (ji, join) in stmt.joins.iter().enumerate() {
        let inner_schema = resolve_table_name(schema, &join.table.name)?;
        let inner_alias = table_alias_or_name(&join.table.name, &join.table.alias);
        let inner_rows = match &needed_per_table {
            Some(n) if ji + 1 < n.len() => {
                collect_rows_partial(&mut rtx, inner_schema, &n[ji + 1])?
            }
            _ => collect_all_rows_raw(&mut rtx, inner_schema)?,
        };

        let mut preview_tables = tables.clone();
        preview_tables.push((inner_alias.clone(), inner_schema));
        let combined_cols = build_joined_columns(&preview_tables);

        let outer_col_count = if outer_rows.is_empty() {
            tables.iter().map(|(_, s)| s.columns.len()).sum()
        } else {
            outer_rows[0].len()
        };
        let inner_col_count = inner_schema.columns.len();

        let is_last = ji == num_joins - 1;
        let proj = if is_last {
            output_combined
                .as_ref()
                .map(|oc| build_combine_projection(oc, outer_col_count))
        } else {
            None
        };

        outer_rows = exec_join_step(
            outer_rows,
            &inner_rows,
            join,
            &combined_cols,
            outer_col_count,
            inner_col_count,
            cur_outer_pk_col,
            proj.as_ref(),
        );
        last_combined_cols = Some(combined_cols);
        tables.push((inner_alias, inner_schema));
        cur_outer_pk_col = None;
    }
    drop(rtx);

    let joined_cols = last_combined_cols.unwrap_or_else(|| build_joined_columns(&tables));
    if let Some(ref oc) = output_combined {
        let actual_width = outer_rows.first().map_or(0, |r| r.len());
        if actual_width == oc.len() {
            let projected_cols = build_projected_columns(&joined_cols, oc);
            return process_select(&projected_cols, outer_rows, stmt, false);
        }
    }
    process_select(&joined_cols, outer_rows, stmt, false)
}

fn exec_select_join_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let from_schema = resolve_table_name(schema, &stmt.from)?;
    let from_alias = table_alias_or_name(&stmt.from, &stmt.from_alias);

    let mut all_tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), from_schema)];
    for join in &stmt.joins {
        let inner_schema = resolve_table_name(schema, &join.table.name)?;
        let inner_alias = table_alias_or_name(&join.table.name, &join.table.alias);
        all_tables.push((inner_alias, inner_schema));
    }
    let (needed_per_table, output_combined) = match compute_join_needed_columns(stmt, &all_tables) {
        Some(plan) => (Some(plan.per_table), Some(plan.output_combined)),
        None => (None, None),
    };

    let mut outer_rows = match &needed_per_table {
        Some(n) if !n.is_empty() => collect_rows_partial_write(wtx, from_schema, &n[0])?,
        _ => collect_all_rows_write(wtx, from_schema)?,
    };

    let mut tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), from_schema)];
    let mut cur_outer_pk_col: Option<usize> = if from_schema.primary_key_columns.len() == 1 {
        Some(from_schema.primary_key_columns[0] as usize)
    } else {
        None
    };

    let num_joins = stmt.joins.len();
    for (ji, join) in stmt.joins.iter().enumerate() {
        let inner_schema = resolve_table_name(schema, &join.table.name)?;
        let inner_alias = table_alias_or_name(&join.table.name, &join.table.alias);
        let inner_rows = match &needed_per_table {
            Some(n) if ji + 1 < n.len() => {
                collect_rows_partial_write(wtx, inner_schema, &n[ji + 1])?
            }
            _ => collect_all_rows_write(wtx, inner_schema)?,
        };

        let mut preview_tables = tables.clone();
        preview_tables.push((inner_alias.clone(), inner_schema));
        let combined_cols = build_joined_columns(&preview_tables);

        let outer_col_count = if outer_rows.is_empty() {
            tables.iter().map(|(_, s)| s.columns.len()).sum()
        } else {
            outer_rows[0].len()
        };
        let inner_col_count = inner_schema.columns.len();

        let is_last = ji == num_joins - 1;
        let proj = if is_last {
            output_combined
                .as_ref()
                .map(|oc| build_combine_projection(oc, outer_col_count))
        } else {
            None
        };

        outer_rows = exec_join_step(
            outer_rows,
            &inner_rows,
            join,
            &combined_cols,
            outer_col_count,
            inner_col_count,
            cur_outer_pk_col,
            proj.as_ref(),
        );
        tables.push((inner_alias, inner_schema));
        cur_outer_pk_col = None;
    }

    let joined_cols = build_joined_columns(&tables);
    if let Some(ref oc) = output_combined {
        let actual_width = outer_rows.first().map_or(0, |r| r.len());
        if actual_width == oc.len() {
            let projected_cols = build_projected_columns(&joined_cols, oc);
            return process_select(&projected_cols, outer_rows, stmt, false);
        }
    }
    process_select(&joined_cols, outer_rows, stmt, false)
}

fn exec_update(
    db: &Database,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if update_has_subquery(stmt) {
        materialized = materialize_update(stmt, &mut |sub| {
            exec_subquery_read(db, schema, sub, &HashMap::new())
        })?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let col_map = ColumnMap::new(&table_schema.columns);
    let all_candidates = collect_keyed_rows_read(db, table_schema, &stmt.where_clause)?;
    let matching_rows: Vec<(Vec<u8>, Vec<Value>)> = all_candidates
        .into_iter()
        .filter(|(_, row)| match &stmt.where_clause {
            Some(where_expr) => match eval_expr(where_expr, &col_map, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            },
            None => true,
        })
        .collect();

    if matching_rows.is_empty() {
        return Ok(ExecutionResult::RowsAffected(0));
    }

    struct UpdateChange {
        old_key: Vec<u8>,
        new_key: Vec<u8>,
        new_value: Vec<u8>,
        pk_changed: bool,
        old_row: Vec<Value>,
        new_row: Vec<Value>,
    }

    let pk_indices = table_schema.pk_indices();
    let mut changes: Vec<UpdateChange> = Vec::new();

    for (old_key, row) in &matching_rows {
        let mut new_row = row.clone();
        let mut pk_changed = false;

        // Evaluate all SET expressions against the original row (SQL standard).
        let mut evaluated: Vec<(usize, Value)> = Vec::with_capacity(stmt.assignments.len());
        for (col_name, expr) in &stmt.assignments {
            let col_idx = table_schema
                .column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let new_val = eval_expr(expr, &col_map, row)?;
            let col = &table_schema.columns[col_idx];

            let got_type = new_val.data_type();
            let coerced = if new_val.is_null() {
                if !col.nullable {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
                Value::Null
            } else {
                new_val
                    .coerce_into(col.data_type)
                    .ok_or_else(|| SqlError::TypeMismatch {
                        expected: col.data_type.to_string(),
                        got: got_type.to_string(),
                    })?
            };

            evaluated.push((col_idx, coerced));
        }

        for (col_idx, coerced) in evaluated {
            if table_schema.primary_key_columns.contains(&(col_idx as u16)) {
                pk_changed = true;
            }
            new_row[col_idx] = coerced;
        }

        // CHECK constraints on new_row
        if table_schema.has_checks() {
            for col in &table_schema.columns {
                if let Some(ref check) = col.check_expr {
                    let result = eval_expr(check, &col_map, &new_row)?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(&tc.expr, &col_map, &new_row)?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| new_row[i].clone()).collect();
        let new_key = encode_composite_key(&pk_values);

        let non_pk = table_schema.non_pk_indices();
        let enc_pos = table_schema.encoding_positions();
        let phys_count = table_schema.physical_non_pk_count();
        let mut value_values = vec![Value::Null; phys_count];
        for (j, &i) in non_pk.iter().enumerate() {
            value_values[enc_pos[j] as usize] = new_row[i].clone();
        }
        let new_value = encode_row(&value_values);

        changes.push(UpdateChange {
            old_key: old_key.clone(),
            new_key,
            new_value,
            pk_changed,
            old_row: row.clone(),
            new_row,
        });
    }

    {
        use std::collections::HashSet;
        let mut new_keys: HashSet<Vec<u8>> = HashSet::new();
        for c in &changes {
            if c.pk_changed && c.new_key != c.old_key && !new_keys.insert(c.new_key.clone()) {
                return Err(SqlError::DuplicateKey);
            }
        }
    }

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;

    // FK child-side: validate new FK values exist in parent
    if !table_schema.foreign_keys.is_empty() {
        for c in &changes {
            for fk in &table_schema.foreign_keys {
                let fk_changed = fk
                    .columns
                    .iter()
                    .any(|&ci| c.old_row[ci as usize] != c.new_row[ci as usize]);
                if !fk_changed {
                    continue;
                }
                let any_null = fk
                    .columns
                    .iter()
                    .any(|&ci| c.new_row[ci as usize].is_null());
                if any_null {
                    continue;
                }
                let fk_vals: Vec<Value> = fk
                    .columns
                    .iter()
                    .map(|&ci| c.new_row[ci as usize].clone())
                    .collect();
                let fk_key = encode_composite_key(&fk_vals);
                let found = wtx
                    .table_get(fk.foreign_table.as_bytes(), &fk_key)
                    .map_err(SqlError::Storage)?;
                if found.is_none() {
                    let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                    return Err(SqlError::ForeignKeyViolation(name.to_string()));
                }
            }
        }
    }

    // FK parent-side: if PK changed, check no child references old PK
    let child_fks = schema.child_fks_for(&lower_name);
    if !child_fks.is_empty() {
        for c in &changes {
            if !c.pk_changed {
                continue;
            }
            let old_pk: Vec<Value> = pk_indices.iter().map(|&i| c.old_row[i].clone()).collect();
            let old_pk_key = encode_composite_key(&old_pk);
            for &(child_table, fk) in &child_fks {
                let child_schema = schema.get(child_table).unwrap();
                let fk_idx = child_schema
                    .indices
                    .iter()
                    .find(|idx| idx.columns == fk.columns);
                if let Some(idx) = fk_idx {
                    let idx_table = TableSchema::index_table_name(child_table, &idx.name);
                    let mut has_child = false;
                    wtx.table_scan_from(&idx_table, &old_pk_key, |key, _| {
                        if key.starts_with(&old_pk_key) {
                            has_child = true;
                            Ok(false) // stop scanning
                        } else {
                            Ok(false) // past prefix, stop
                        }
                    })
                    .map_err(SqlError::Storage)?;
                    if has_child {
                        return Err(SqlError::ForeignKeyViolation(format!(
                            "cannot update PK in '{}': referenced by '{}'",
                            lower_name, child_table
                        )));
                    }
                }
            }
        }
    }

    for c in &changes {
        let old_pk: Vec<Value> = pk_indices.iter().map(|&i| c.old_row[i].clone()).collect();

        for idx in &table_schema.indices {
            if index_columns_changed(idx, &c.old_row, &c.new_row) || c.pk_changed {
                let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
                let old_idx_key = encode_index_key(idx, &c.old_row, &old_pk);
                wtx.table_delete(&idx_table, &old_idx_key)
                    .map_err(SqlError::Storage)?;
            }
        }

        if c.pk_changed {
            wtx.table_delete(lower_name.as_bytes(), &c.old_key)
                .map_err(SqlError::Storage)?;
        }
    }

    for c in &changes {
        let new_pk: Vec<Value> = pk_indices.iter().map(|&i| c.new_row[i].clone()).collect();

        if c.pk_changed {
            let is_new = wtx
                .table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
                .map_err(SqlError::Storage)?;
            if !is_new {
                return Err(SqlError::DuplicateKey);
            }
        } else {
            wtx.table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
                .map_err(SqlError::Storage)?;
        }

        for idx in &table_schema.indices {
            if index_columns_changed(idx, &c.old_row, &c.new_row) || c.pk_changed {
                let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
                let new_idx_key = encode_index_key(idx, &c.new_row, &new_pk);
                let new_idx_val = encode_index_value(idx, &c.new_row, &new_pk);
                let is_new = wtx
                    .table_insert(&idx_table, &new_idx_key, &new_idx_val)
                    .map_err(SqlError::Storage)?;
                if idx.unique && !is_new {
                    let indexed_values: Vec<Value> = idx
                        .columns
                        .iter()
                        .map(|&col_idx| c.new_row[col_idx as usize].clone())
                        .collect();
                    let any_null = indexed_values.iter().any(|v| v.is_null());
                    if !any_null {
                        return Err(SqlError::UniqueViolation(idx.name.clone()));
                    }
                }
            }
        }
    }

    let count = changes.len() as u64;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

fn exec_delete(
    db: &Database,
    schema: &SchemaManager,
    stmt: &DeleteStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if delete_has_subquery(stmt) {
        materialized = materialize_delete(stmt, &mut |sub| {
            exec_subquery_read(db, schema, sub, &HashMap::new())
        })?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let col_map = ColumnMap::new(&table_schema.columns);
    let all_candidates = collect_keyed_rows_read(db, table_schema, &stmt.where_clause)?;
    let rows_to_delete: Vec<(Vec<u8>, Vec<Value>)> = all_candidates
        .into_iter()
        .filter(|(_, row)| match &stmt.where_clause {
            Some(where_expr) => match eval_expr(where_expr, &col_map, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            },
            None => true,
        })
        .collect();

    if rows_to_delete.is_empty() {
        return Ok(ExecutionResult::RowsAffected(0));
    }

    let pk_indices = table_schema.pk_indices();
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;

    // FK parent-side: check no child rows reference deleted PKs
    let child_fks = schema.child_fks_for(&lower_name);
    if !child_fks.is_empty() {
        for (_key, row) in &rows_to_delete {
            let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
            let pk_key = encode_composite_key(&pk_values);
            for &(child_table, fk) in &child_fks {
                let child_schema = schema.get(child_table).unwrap();
                let fk_idx = child_schema
                    .indices
                    .iter()
                    .find(|idx| idx.columns == fk.columns);
                if let Some(idx) = fk_idx {
                    let idx_table = TableSchema::index_table_name(child_table, &idx.name);
                    let mut has_child = false;
                    wtx.table_scan_from(&idx_table, &pk_key, |key, _| {
                        if key.starts_with(&pk_key) {
                            has_child = true;
                            Ok(false)
                        } else {
                            Ok(false)
                        }
                    })
                    .map_err(SqlError::Storage)?;
                    if has_child {
                        return Err(SqlError::ForeignKeyViolation(format!(
                            "cannot delete from '{}': referenced by '{}'",
                            lower_name, child_table
                        )));
                    }
                }
            }
        }
    }

    for (key, row) in &rows_to_delete {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        delete_index_entries(&mut wtx, table_schema, row, &pk_values)?;
        wtx.table_delete(lower_name.as_bytes(), key)
            .map_err(SqlError::Storage)?;
    }
    let count = rows_to_delete.len() as u64;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

#[derive(Default)]
pub struct InsertBufs {
    row: Vec<Value>,
    pk_values: Vec<Value>,
    value_values: Vec<Value>,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
    col_indices: Vec<usize>,
    fk_key_buf: Vec<u8>,
}

impl InsertBufs {
    pub fn new() -> Self {
        Self {
            row: Vec::new(),
            pk_values: Vec::new(),
            value_values: Vec::new(),
            key_buf: Vec::with_capacity(64),
            value_buf: Vec::with_capacity(256),
            col_indices: Vec::new(),
            fk_key_buf: Vec::with_capacity(64),
        }
    }
}

pub fn exec_insert_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
    bufs: &mut InsertBufs,
) -> Result<ExecutionResult> {
    let empty_ctes = CteContext::new();
    let materialized;
    let stmt = if insert_has_subquery(stmt) {
        materialized = materialize_insert(stmt, &mut |sub| {
            exec_subquery_write(wtx, schema, sub, &empty_ctes)
        })?;
        &materialized
    } else {
        stmt
    };

    let table_schema = schema
        .get(&stmt.table)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let default_columns;
    let insert_columns: &[String] = if stmt.columns.is_empty() {
        default_columns = table_schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();
        &default_columns
    } else {
        &stmt.columns
    };

    bufs.col_indices.clear();
    for name in insert_columns {
        bufs.col_indices.push(
            table_schema
                .column_index(name)
                .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))?,
        );
    }

    // Pre-compute defaults
    let defaults: Vec<(usize, &Expr)> = table_schema
        .columns
        .iter()
        .filter(|c| c.default_expr.is_some() && !bufs.col_indices.contains(&(c.position as usize)))
        .map(|c| (c.position as usize, c.default_expr.as_ref().unwrap()))
        .collect();

    let has_checks = table_schema.has_checks();
    let check_col_map = if has_checks {
        Some(ColumnMap::new(&table_schema.columns))
    } else {
        None
    };

    let pk_indices = table_schema.pk_indices();
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let phys_count = table_schema.physical_non_pk_count();
    let dropped = table_schema.dropped_non_pk_slots();

    bufs.row.resize(table_schema.columns.len(), Value::Null);
    bufs.pk_values.resize(pk_indices.len(), Value::Null);
    bufs.value_values.resize(phys_count, Value::Null);

    let select_rows = match &stmt.source {
        InsertSource::Select(sq) => {
            let insert_ctes = materialize_all_ctes(&sq.ctes, sq.recursive, &mut |body, ctx| {
                exec_query_body_write(wtx, schema, body, ctx)
            })?;
            let qr = exec_query_body_write(wtx, schema, &sq.body, &insert_ctes)?;
            Some(qr.rows)
        }
        InsertSource::Values(_) => None,
    };

    let mut count: u64 = 0;

    let values = match &stmt.source {
        InsertSource::Values(rows) => Some(rows.as_slice()),
        InsertSource::Select(_) => None,
    };
    let sel_rows = select_rows.as_deref();

    let total = match (values, sel_rows) {
        (Some(rows), _) => rows.len(),
        (_, Some(rows)) => rows.len(),
        _ => 0,
    };

    if let Some(sel) = sel_rows {
        if !sel.is_empty() && sel[0].len() != insert_columns.len() {
            return Err(SqlError::InvalidValue(format!(
                "INSERT ... SELECT column count mismatch: expected {}, got {}",
                insert_columns.len(),
                sel[0].len()
            )));
        }
    }

    for idx in 0..total {
        for v in bufs.row.iter_mut() {
            *v = Value::Null;
        }

        if let Some(value_rows) = values {
            let value_row = &value_rows[idx];
            if value_row.len() != insert_columns.len() {
                return Err(SqlError::InvalidValue(format!(
                    "expected {} values, got {}",
                    insert_columns.len(),
                    value_row.len()
                )));
            }
            for (i, expr) in value_row.iter().enumerate() {
                let val = if let Expr::Parameter(n) = expr {
                    params
                        .get(n - 1)
                        .cloned()
                        .ok_or_else(|| SqlError::Parse(format!("unbound parameter ${n}")))?
                } else {
                    eval_const_expr(expr)?
                };
                let col_idx = bufs.col_indices[i];
                let col = &table_schema.columns[col_idx];
                let got_type = val.data_type();
                bufs.row[col_idx] = if val.is_null() {
                    Value::Null
                } else {
                    val.coerce_into(col.data_type)
                        .ok_or_else(|| SqlError::TypeMismatch {
                            expected: col.data_type.to_string(),
                            got: got_type.to_string(),
                        })?
                };
            }
        } else if let Some(sel) = sel_rows {
            let sel_row = &sel[idx];
            for (i, val) in sel_row.iter().enumerate() {
                let col_idx = bufs.col_indices[i];
                let col = &table_schema.columns[col_idx];
                let got_type = val.data_type();
                bufs.row[col_idx] = if val.is_null() {
                    Value::Null
                } else {
                    val.clone().coerce_into(col.data_type).ok_or_else(|| {
                        SqlError::TypeMismatch {
                            expected: col.data_type.to_string(),
                            got: got_type.to_string(),
                        }
                    })?
                };
            }
        }

        // Apply DEFAULT for omitted columns
        for &(pos, def_expr) in &defaults {
            let val = eval_const_expr(def_expr)?;
            let col = &table_schema.columns[pos];
            if val.is_null() {
                // bufs.row[pos] already Null from init
            } else {
                let got_type = val.data_type();
                bufs.row[pos] =
                    val.coerce_into(col.data_type)
                        .ok_or_else(|| SqlError::TypeMismatch {
                            expected: col.data_type.to_string(),
                            got: got_type.to_string(),
                        })?;
            }
        }

        for col in &table_schema.columns {
            if !col.nullable && bufs.row[col.position as usize].is_null() {
                return Err(SqlError::NotNullViolation(col.name.clone()));
            }
        }

        // CHECK constraints
        if let Some(ref col_map) = check_col_map {
            for col in &table_schema.columns {
                if let Some(ref check) = col.check_expr {
                    let result = eval_expr(check, col_map, &bufs.row)?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(&tc.expr, col_map, &bufs.row)?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

        // FK child-side validation
        for fk in &table_schema.foreign_keys {
            let any_null = fk.columns.iter().any(|&ci| bufs.row[ci as usize].is_null());
            if any_null {
                continue;
            }
            let fk_vals: Vec<Value> = fk
                .columns
                .iter()
                .map(|&ci| bufs.row[ci as usize].clone())
                .collect();
            bufs.fk_key_buf.clear();
            encode_composite_key_into(&fk_vals, &mut bufs.fk_key_buf);
            let found = wtx
                .table_get(fk.foreign_table.as_bytes(), &bufs.fk_key_buf)
                .map_err(SqlError::Storage)?;
            if found.is_none() {
                let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                return Err(SqlError::ForeignKeyViolation(name.to_string()));
            }
        }

        for (j, &i) in pk_indices.iter().enumerate() {
            bufs.pk_values[j] = std::mem::replace(&mut bufs.row[i], Value::Null);
        }
        encode_composite_key_into(&bufs.pk_values, &mut bufs.key_buf);

        for &slot in dropped {
            bufs.value_values[slot as usize] = Value::Null;
        }
        for (j, &i) in non_pk.iter().enumerate() {
            bufs.value_values[enc_pos[j] as usize] =
                std::mem::replace(&mut bufs.row[i], Value::Null);
        }
        encode_row_into(&bufs.value_values, &mut bufs.value_buf);

        if bufs.key_buf.len() > citadel_core::MAX_KEY_SIZE {
            return Err(SqlError::KeyTooLarge {
                size: bufs.key_buf.len(),
                max: citadel_core::MAX_KEY_SIZE,
            });
        }
        if bufs.value_buf.len() > citadel_core::MAX_INLINE_VALUE_SIZE {
            return Err(SqlError::RowTooLarge {
                size: bufs.value_buf.len(),
                max: citadel_core::MAX_INLINE_VALUE_SIZE,
            });
        }

        let is_new = wtx
            .table_insert(stmt.table.as_bytes(), &bufs.key_buf, &bufs.value_buf)
            .map_err(SqlError::Storage)?;
        if !is_new {
            return Err(SqlError::DuplicateKey);
        }

        if !table_schema.indices.is_empty() {
            for (j, &i) in pk_indices.iter().enumerate() {
                bufs.row[i] = bufs.pk_values[j].clone();
            }
            for (j, &i) in non_pk.iter().enumerate() {
                bufs.row[i] =
                    std::mem::replace(&mut bufs.value_values[enc_pos[j] as usize], Value::Null);
            }
            insert_index_entries(wtx, table_schema, &bufs.row, &bufs.pk_values)?;
        }
        count += 1;
    }

    Ok(ExecutionResult::RowsAffected(count))
}

fn exec_select_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if stmt_has_subquery(stmt) {
        materialized =
            materialize_stmt(stmt, &mut |sub| exec_subquery_write(wtx, schema, sub, ctes))?;
        &materialized
    } else {
        stmt
    };

    if stmt.from.is_empty() {
        return exec_select_no_from(stmt);
    }

    let lower_name = stmt.from.to_ascii_lowercase();

    if let Some(cte_result) = ctes.get(&lower_name) {
        if stmt.joins.is_empty() {
            return exec_select_from_cte(cte_result, stmt, &mut |sub| {
                exec_subquery_write(wtx, schema, sub, ctes)
            });
        } else {
            return exec_select_join_with_ctes(stmt, ctes, &mut |name| {
                scan_table_write(wtx, schema, name)
            });
        }
    }

    if !ctes.is_empty()
        && stmt
            .joins
            .iter()
            .any(|j| ctes.contains_key(&j.table.name.to_ascii_lowercase()))
    {
        return exec_select_join_with_ctes(stmt, ctes, &mut |name| {
            scan_table_write(wtx, schema, name)
        });
    }

    if !stmt.joins.is_empty() {
        return exec_select_join_in_txn(wtx, schema, stmt);
    }

    let lower_name = stmt.from.to_ascii_lowercase();
    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.from.clone()))?;

    if let Some(result) = try_count_star_shortcut(stmt, || {
        wtx.table_entry_count(lower_name.as_bytes())
            .map_err(SqlError::Storage)
    })? {
        return Ok(result);
    }

    if let Some(plan) = StreamAggPlan::try_new(stmt, table_schema)? {
        let mut states: Vec<AggState> = plan.ops.iter().map(|(op, _)| AggState::new(op)).collect();
        let mut scan_err: Option<SqlError> = None;
        if stmt.where_clause.is_none() {
            wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                Ok(plan.feed_row_raw(key, value, &mut states, &mut scan_err))
            })
            .map_err(SqlError::Storage)?;
        } else {
            let col_map = ColumnMap::new(&table_schema.columns);
            wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                Ok(plan.feed_row(
                    key,
                    value,
                    table_schema,
                    &col_map,
                    &stmt.where_clause,
                    &mut states,
                    &mut scan_err,
                ))
            })
            .map_err(SqlError::Storage)?;
        }
        if let Some(e) = scan_err {
            return Err(e);
        }
        return Ok(plan.finish(states));
    }

    if let Some(plan) = StreamGroupByPlan::try_new(stmt, table_schema)? {
        let lower = lower_name.clone();
        return plan.execute_scan(|cb| {
            wtx.table_scan_from(lower.as_bytes(), b"", |key, value| Ok(cb(key, value)))
        });
    }

    if let Some(plan) = TopKScanPlan::try_new(stmt, table_schema)? {
        let lower = lower_name.clone();
        return plan.execute_scan(table_schema, stmt, |cb| {
            wtx.table_scan_from(lower.as_bytes(), b"", |key, value| Ok(cb(key, value)))
        });
    }

    let scan_limit = compute_scan_limit(stmt);
    let (rows, predicate_applied) =
        collect_rows_write(wtx, table_schema, &stmt.where_clause, scan_limit)?;
    process_select(&table_schema.columns, rows, stmt, predicate_applied)
}

fn exec_update_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if update_has_subquery(stmt) {
        materialized = materialize_update(stmt, &mut |sub| {
            exec_subquery_write(wtx, schema, sub, &HashMap::new())
        })?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let col_map = ColumnMap::new(&table_schema.columns);
    let all_candidates = collect_keyed_rows_write(wtx, table_schema, &stmt.where_clause)?;
    let matching_rows: Vec<(Vec<u8>, Vec<Value>)> = all_candidates
        .into_iter()
        .filter(|(_, row)| match &stmt.where_clause {
            Some(where_expr) => match eval_expr(where_expr, &col_map, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            },
            None => true,
        })
        .collect();

    if matching_rows.is_empty() {
        return Ok(ExecutionResult::RowsAffected(0));
    }

    struct UpdateChange {
        old_key: Vec<u8>,
        new_key: Vec<u8>,
        new_value: Vec<u8>,
        pk_changed: bool,
        old_row: Vec<Value>,
        new_row: Vec<Value>,
    }

    let pk_indices = table_schema.pk_indices();
    let mut changes: Vec<UpdateChange> = Vec::new();

    for (old_key, row) in &matching_rows {
        let mut new_row = row.clone();
        let mut pk_changed = false;

        // Evaluate all SET expressions against the original row (SQL standard).
        let mut evaluated: Vec<(usize, Value)> = Vec::with_capacity(stmt.assignments.len());
        for (col_name, expr) in &stmt.assignments {
            let col_idx = table_schema
                .column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let new_val = eval_expr(expr, &col_map, row)?;
            let col = &table_schema.columns[col_idx];

            let got_type = new_val.data_type();
            let coerced = if new_val.is_null() {
                if !col.nullable {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
                Value::Null
            } else {
                new_val
                    .coerce_into(col.data_type)
                    .ok_or_else(|| SqlError::TypeMismatch {
                        expected: col.data_type.to_string(),
                        got: got_type.to_string(),
                    })?
            };

            evaluated.push((col_idx, coerced));
        }

        for (col_idx, coerced) in evaluated {
            if table_schema.primary_key_columns.contains(&(col_idx as u16)) {
                pk_changed = true;
            }
            new_row[col_idx] = coerced;
        }

        // CHECK constraints on new_row
        if table_schema.has_checks() {
            for col in &table_schema.columns {
                if let Some(ref check) = col.check_expr {
                    let result = eval_expr(check, &col_map, &new_row)?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(&tc.expr, &col_map, &new_row)?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| new_row[i].clone()).collect();
        let new_key = encode_composite_key(&pk_values);

        let non_pk = table_schema.non_pk_indices();
        let enc_pos = table_schema.encoding_positions();
        let phys_count = table_schema.physical_non_pk_count();
        let mut value_values = vec![Value::Null; phys_count];
        for (j, &i) in non_pk.iter().enumerate() {
            value_values[enc_pos[j] as usize] = new_row[i].clone();
        }
        let new_value = encode_row(&value_values);

        changes.push(UpdateChange {
            old_key: old_key.clone(),
            new_key,
            new_value,
            pk_changed,
            old_row: row.clone(),
            new_row,
        });
    }

    {
        use std::collections::HashSet;
        let mut new_keys: HashSet<Vec<u8>> = HashSet::new();
        for c in &changes {
            if c.pk_changed && c.new_key != c.old_key && !new_keys.insert(c.new_key.clone()) {
                return Err(SqlError::DuplicateKey);
            }
        }
    }

    // FK child-side: validate new FK values exist in parent
    if !table_schema.foreign_keys.is_empty() {
        for c in &changes {
            for fk in &table_schema.foreign_keys {
                let fk_changed = fk
                    .columns
                    .iter()
                    .any(|&ci| c.old_row[ci as usize] != c.new_row[ci as usize]);
                if !fk_changed {
                    continue;
                }
                let any_null = fk
                    .columns
                    .iter()
                    .any(|&ci| c.new_row[ci as usize].is_null());
                if any_null {
                    continue;
                }
                let fk_vals: Vec<Value> = fk
                    .columns
                    .iter()
                    .map(|&ci| c.new_row[ci as usize].clone())
                    .collect();
                let fk_key = encode_composite_key(&fk_vals);
                let found = wtx
                    .table_get(fk.foreign_table.as_bytes(), &fk_key)
                    .map_err(SqlError::Storage)?;
                if found.is_none() {
                    let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                    return Err(SqlError::ForeignKeyViolation(name.to_string()));
                }
            }
        }
    }

    // FK parent-side: if PK changed, check no child references old PK
    let child_fks = schema.child_fks_for(&lower_name);
    if !child_fks.is_empty() {
        for c in &changes {
            if !c.pk_changed {
                continue;
            }
            let old_pk: Vec<Value> = pk_indices.iter().map(|&i| c.old_row[i].clone()).collect();
            let old_pk_key = encode_composite_key(&old_pk);
            for &(child_table, fk) in &child_fks {
                let child_schema = schema.get(child_table).unwrap();
                let fk_idx = child_schema
                    .indices
                    .iter()
                    .find(|idx| idx.columns == fk.columns);
                if let Some(idx) = fk_idx {
                    let idx_table = TableSchema::index_table_name(child_table, &idx.name);
                    let mut has_child = false;
                    wtx.table_scan_from(&idx_table, &old_pk_key, |key, _| {
                        if key.starts_with(&old_pk_key) {
                            has_child = true;
                            Ok(false)
                        } else {
                            Ok(false)
                        }
                    })
                    .map_err(SqlError::Storage)?;
                    if has_child {
                        return Err(SqlError::ForeignKeyViolation(format!(
                            "cannot update PK in '{}': referenced by '{}'",
                            lower_name, child_table
                        )));
                    }
                }
            }
        }
    }

    for c in &changes {
        let old_pk: Vec<Value> = pk_indices.iter().map(|&i| c.old_row[i].clone()).collect();

        for idx in &table_schema.indices {
            if index_columns_changed(idx, &c.old_row, &c.new_row) || c.pk_changed {
                let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
                let old_idx_key = encode_index_key(idx, &c.old_row, &old_pk);
                wtx.table_delete(&idx_table, &old_idx_key)
                    .map_err(SqlError::Storage)?;
            }
        }

        if c.pk_changed {
            wtx.table_delete(lower_name.as_bytes(), &c.old_key)
                .map_err(SqlError::Storage)?;
        }
    }

    for c in &changes {
        let new_pk: Vec<Value> = pk_indices.iter().map(|&i| c.new_row[i].clone()).collect();

        if c.pk_changed {
            let is_new = wtx
                .table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
                .map_err(SqlError::Storage)?;
            if !is_new {
                return Err(SqlError::DuplicateKey);
            }
        } else {
            wtx.table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
                .map_err(SqlError::Storage)?;
        }

        for idx in &table_schema.indices {
            if index_columns_changed(idx, &c.old_row, &c.new_row) || c.pk_changed {
                let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
                let new_idx_key = encode_index_key(idx, &c.new_row, &new_pk);
                let new_idx_val = encode_index_value(idx, &c.new_row, &new_pk);
                let is_new = wtx
                    .table_insert(&idx_table, &new_idx_key, &new_idx_val)
                    .map_err(SqlError::Storage)?;
                if idx.unique && !is_new {
                    let indexed_values: Vec<Value> = idx
                        .columns
                        .iter()
                        .map(|&col_idx| c.new_row[col_idx as usize].clone())
                        .collect();
                    let any_null = indexed_values.iter().any(|v| v.is_null());
                    if !any_null {
                        return Err(SqlError::UniqueViolation(idx.name.clone()));
                    }
                }
            }
        }
    }

    let count = changes.len() as u64;
    Ok(ExecutionResult::RowsAffected(count))
}

fn exec_delete_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &DeleteStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if delete_has_subquery(stmt) {
        materialized = materialize_delete(stmt, &mut |sub| {
            exec_subquery_write(wtx, schema, sub, &HashMap::new())
        })?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let col_map = ColumnMap::new(&table_schema.columns);
    let all_candidates = collect_keyed_rows_write(wtx, table_schema, &stmt.where_clause)?;
    let rows_to_delete: Vec<(Vec<u8>, Vec<Value>)> = all_candidates
        .into_iter()
        .filter(|(_, row)| match &stmt.where_clause {
            Some(where_expr) => match eval_expr(where_expr, &col_map, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            },
            None => true,
        })
        .collect();

    if rows_to_delete.is_empty() {
        return Ok(ExecutionResult::RowsAffected(0));
    }

    let pk_indices = table_schema.pk_indices();

    // FK parent-side: check no child rows reference deleted PKs
    let child_fks = schema.child_fks_for(&lower_name);
    if !child_fks.is_empty() {
        for (_key, row) in &rows_to_delete {
            let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
            let pk_key = encode_composite_key(&pk_values);
            for &(child_table, fk) in &child_fks {
                let child_schema = schema.get(child_table).unwrap();
                let fk_idx = child_schema
                    .indices
                    .iter()
                    .find(|idx| idx.columns == fk.columns);
                if let Some(idx) = fk_idx {
                    let idx_table = TableSchema::index_table_name(child_table, &idx.name);
                    let mut has_child = false;
                    wtx.table_scan_from(&idx_table, &pk_key, |key, _| {
                        if key.starts_with(&pk_key) {
                            has_child = true;
                            Ok(false)
                        } else {
                            Ok(false)
                        }
                    })
                    .map_err(SqlError::Storage)?;
                    if has_child {
                        return Err(SqlError::ForeignKeyViolation(format!(
                            "cannot delete from '{}': referenced by '{}'",
                            lower_name, child_table
                        )));
                    }
                }
            }
        }
    }

    for (key, row) in &rows_to_delete {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        delete_index_entries(wtx, table_schema, row, &pk_values)?;
        wtx.table_delete(lower_name.as_bytes(), key)
            .map_err(SqlError::Storage)?;
    }
    let count = rows_to_delete.len() as u64;
    Ok(ExecutionResult::RowsAffected(count))
}

// ── Aggregation ─────────────────────────────────────────────────────

fn exec_aggregate(
    columns: &[ColumnDef],
    rows: &[Vec<Value>],
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let col_map = ColumnMap::new(columns);
    let groups: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = if stmt.group_by.is_empty() {
        let mut m = BTreeMap::new();
        m.insert(vec![], rows.iter().collect());
        m
    } else {
        let mut m: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = BTreeMap::new();
        for row in rows {
            let group_key: Vec<Value> = stmt
                .group_by
                .iter()
                .map(|expr| eval_expr(expr, &col_map, row))
                .collect::<Result<_>>()?;
            m.entry(group_key).or_default().push(row);
        }
        m
    };

    let mut result_rows = Vec::new();
    let output_cols = build_output_columns(&stmt.columns, columns);

    for group_rows in groups.values() {
        let mut result_row = Vec::new();

        for sel_col in &stmt.columns {
            match sel_col {
                SelectColumn::AllColumns => {
                    return Err(SqlError::Unsupported("SELECT * with GROUP BY".into()));
                }
                SelectColumn::Expr { expr, .. } => {
                    let val = eval_aggregate_expr(expr, &col_map, group_rows)?;
                    result_row.push(val);
                }
            }
        }

        if let Some(ref having) = stmt.having {
            let passes = match eval_aggregate_expr(having, &col_map, group_rows) {
                Ok(val) => is_truthy(&val),
                Err(SqlError::ColumnNotFound(_)) => {
                    let output_map = ColumnMap::new(&output_cols);
                    match eval_expr(having, &output_map, &result_row) {
                        Ok(val) => is_truthy(&val),
                        Err(_) => false,
                    }
                }
                Err(e) => return Err(e),
            };
            if !passes {
                continue;
            }
        }

        result_rows.push(result_row);
    }

    if stmt.distinct {
        let mut seen = std::collections::HashSet::new();
        result_rows.retain(|row| seen.insert(row.clone()));
    }

    if !stmt.order_by.is_empty() {
        let output_cols = build_output_columns(&stmt.columns, columns);
        sort_rows(&mut result_rows, &stmt.order_by, &output_cols)?;
    }

    if let Some(ref offset_expr) = stmt.offset {
        let offset = eval_const_int(offset_expr)?.max(0) as usize;
        if offset < result_rows.len() {
            result_rows = result_rows.split_off(offset);
        } else {
            result_rows.clear();
        }
    }
    if let Some(ref limit_expr) = stmt.limit {
        let limit = eval_const_int(limit_expr)?.max(0) as usize;
        result_rows.truncate(limit);
    }

    let col_names = stmt
        .columns
        .iter()
        .map(|c| match c {
            SelectColumn::AllColumns => "*".into(),
            SelectColumn::Expr { alias: Some(a), .. } => a.clone(),
            SelectColumn::Expr { expr, .. } => expr_display_name(expr),
        })
        .collect();

    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: result_rows,
    }))
}

fn eval_aggregate_expr(
    expr: &Expr,
    col_map: &ColumnMap,
    group_rows: &[&Vec<Value>],
) -> Result<Value> {
    match expr {
        Expr::CountStar => Ok(Value::Integer(group_rows.len() as i64)),

        Expr::Function { name, args } if is_aggregate_function(name, args.len()) => {
            let func = name.to_ascii_uppercase();
            if args.len() != 1 {
                return Err(SqlError::Unsupported(format!(
                    "{func} with {} args",
                    args.len()
                )));
            }
            let arg = &args[0];
            let values: Vec<Value> = group_rows
                .iter()
                .map(|row| eval_expr(arg, col_map, row))
                .collect::<Result<_>>()?;

            match func.as_str() {
                "COUNT" => {
                    let count = values.iter().filter(|v| !v.is_null()).count();
                    Ok(Value::Integer(count as i64))
                }
                "SUM" => {
                    let mut int_sum: i64 = 0;
                    let mut real_sum: f64 = 0.0;
                    let mut has_real = false;
                    let mut all_null = true;
                    for v in &values {
                        match v {
                            Value::Integer(i) => {
                                int_sum += i;
                                all_null = false;
                            }
                            Value::Real(r) => {
                                real_sum += r;
                                has_real = true;
                                all_null = false;
                            }
                            Value::Null => {}
                            _ => {
                                return Err(SqlError::TypeMismatch {
                                    expected: "numeric".into(),
                                    got: v.data_type().to_string(),
                                })
                            }
                        }
                    }
                    if all_null {
                        return Ok(Value::Null);
                    }
                    if has_real {
                        Ok(Value::Real(real_sum + int_sum as f64))
                    } else {
                        Ok(Value::Integer(int_sum))
                    }
                }
                "AVG" => {
                    let mut sum: f64 = 0.0;
                    let mut count: i64 = 0;
                    for v in &values {
                        match v {
                            Value::Integer(i) => {
                                sum += *i as f64;
                                count += 1;
                            }
                            Value::Real(r) => {
                                sum += r;
                                count += 1;
                            }
                            Value::Null => {}
                            _ => {
                                return Err(SqlError::TypeMismatch {
                                    expected: "numeric".into(),
                                    got: v.data_type().to_string(),
                                })
                            }
                        }
                    }
                    if count == 0 {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Real(sum / count as f64))
                    }
                }
                "MIN" => {
                    let mut min: Option<&Value> = None;
                    for v in &values {
                        if v.is_null() {
                            continue;
                        }
                        min = Some(match min {
                            None => v,
                            Some(m) => {
                                if v < m {
                                    v
                                } else {
                                    m
                                }
                            }
                        });
                    }
                    Ok(min.cloned().unwrap_or(Value::Null))
                }
                "MAX" => {
                    let mut max: Option<&Value> = None;
                    for v in &values {
                        if v.is_null() {
                            continue;
                        }
                        max = Some(match max {
                            None => v,
                            Some(m) => {
                                if v > m {
                                    v
                                } else {
                                    m
                                }
                            }
                        });
                    }
                    Ok(max.cloned().unwrap_or(Value::Null))
                }
                _ => Err(SqlError::Unsupported(format!("aggregate function: {func}"))),
            }
        }

        Expr::Column(_) | Expr::QualifiedColumn { .. } => {
            if let Some(first) = group_rows.first() {
                eval_expr(expr, col_map, first)
            } else {
                Ok(Value::Null)
            }
        }

        Expr::Literal(v) => Ok(v.clone()),

        Expr::BinaryOp { left, op, right } => {
            let l = eval_aggregate_expr(left, col_map, group_rows)?;
            let r = eval_aggregate_expr(right, col_map, group_rows)?;
            eval_expr(
                &Expr::BinaryOp {
                    left: Box::new(Expr::Literal(l)),
                    op: *op,
                    right: Box::new(Expr::Literal(r)),
                },
                col_map,
                &[],
            )
        }

        Expr::UnaryOp { op, expr: e } => {
            let v = eval_aggregate_expr(e, col_map, group_rows)?;
            eval_expr(
                &Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(Expr::Literal(v)),
                },
                col_map,
                &[],
            )
        }

        Expr::IsNull(e) => {
            let v = eval_aggregate_expr(e, col_map, group_rows)?;
            Ok(Value::Boolean(v.is_null()))
        }

        Expr::IsNotNull(e) => {
            let v = eval_aggregate_expr(e, col_map, group_rows)?;
            Ok(Value::Boolean(!v.is_null()))
        }

        Expr::Cast { expr: e, data_type } => {
            let v = eval_aggregate_expr(e, col_map, group_rows)?;
            eval_expr(
                &Expr::Cast {
                    expr: Box::new(Expr::Literal(v)),
                    data_type: *data_type,
                },
                col_map,
                &[],
            )
        }

        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let op_val = operand
                .as_ref()
                .map(|e| eval_aggregate_expr(e, col_map, group_rows))
                .transpose()?;
            if let Some(ov) = &op_val {
                for (cond, result) in conditions {
                    let cv = eval_aggregate_expr(cond, col_map, group_rows)?;
                    if !ov.is_null() && !cv.is_null() && *ov == cv {
                        return eval_aggregate_expr(result, col_map, group_rows);
                    }
                }
            } else {
                for (cond, result) in conditions {
                    let cv = eval_aggregate_expr(cond, col_map, group_rows)?;
                    if is_truthy(&cv) {
                        return eval_aggregate_expr(result, col_map, group_rows);
                    }
                }
            }
            match else_result {
                Some(e) => eval_aggregate_expr(e, col_map, group_rows),
                None => Ok(Value::Null),
            }
        }

        Expr::Coalesce(args) => {
            for arg in args {
                let v = eval_aggregate_expr(arg, col_map, group_rows)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }

        Expr::Between {
            expr: e,
            low,
            high,
            negated,
        } => {
            let v = eval_aggregate_expr(e, col_map, group_rows)?;
            let lo = eval_aggregate_expr(low, col_map, group_rows)?;
            let hi = eval_aggregate_expr(high, col_map, group_rows)?;
            eval_expr(
                &Expr::Between {
                    expr: Box::new(Expr::Literal(v)),
                    low: Box::new(Expr::Literal(lo)),
                    high: Box::new(Expr::Literal(hi)),
                    negated: *negated,
                },
                col_map,
                &[],
            )
        }

        Expr::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => {
            let v = eval_aggregate_expr(e, col_map, group_rows)?;
            let p = eval_aggregate_expr(pattern, col_map, group_rows)?;
            let esc = escape
                .as_ref()
                .map(|es| eval_aggregate_expr(es, col_map, group_rows))
                .transpose()?;
            let esc_box = esc.map(|v| Box::new(Expr::Literal(v)));
            eval_expr(
                &Expr::Like {
                    expr: Box::new(Expr::Literal(v)),
                    pattern: Box::new(Expr::Literal(p)),
                    escape: esc_box,
                    negated: *negated,
                },
                col_map,
                &[],
            )
        }

        Expr::Function { name, args } => {
            let evaluated: Vec<Value> = args
                .iter()
                .map(|a| eval_aggregate_expr(a, col_map, group_rows))
                .collect::<Result<_>>()?;
            let literal_args: Vec<Expr> = evaluated.into_iter().map(Expr::Literal).collect();
            eval_expr(
                &Expr::Function {
                    name: name.clone(),
                    args: literal_args,
                },
                col_map,
                &[],
            )
        }

        _ => Err(SqlError::Unsupported(format!(
            "expression in aggregate: {expr:?}"
        ))),
    }
}

fn is_aggregate_function(name: &str, arg_count: usize) -> bool {
    let u = name.to_ascii_uppercase();
    matches!(u.as_str(), "COUNT" | "SUM" | "AVG")
        || (matches!(u.as_str(), "MIN" | "MAX") && arg_count == 1)
}

fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::CountStar => true,
        Expr::Function { name, args } => {
            is_aggregate_function(name, args.len()) || args.iter().any(is_aggregate_expr)
        }
        Expr::BinaryOp { left, right, .. } => is_aggregate_expr(left) || is_aggregate_expr(right),
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => is_aggregate_expr(expr),
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand.as_ref().is_some_and(|e| is_aggregate_expr(e))
                || conditions
                    .iter()
                    .any(|(c, r)| is_aggregate_expr(c) || is_aggregate_expr(r))
                || else_result.as_ref().is_some_and(|e| is_aggregate_expr(e))
        }
        Expr::Coalesce(args) => args.iter().any(is_aggregate_expr),
        Expr::Between {
            expr, low, high, ..
        } => is_aggregate_expr(expr) || is_aggregate_expr(low) || is_aggregate_expr(high),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            is_aggregate_expr(expr)
                || is_aggregate_expr(pattern)
                || escape.as_ref().is_some_and(|e| is_aggregate_expr(e))
        }
        _ => false,
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

struct PartialDecodeCtx {
    pk_positions: Vec<(usize, usize)>,
    nonpk_targets: Vec<usize>,
    nonpk_schema: Vec<usize>,
    num_cols: usize,
    num_pk_cols: usize,
    remaining_pk: Vec<(usize, usize)>,
    remaining_nonpk_targets: Vec<usize>,
    remaining_nonpk_schema: Vec<usize>,
    nonpk_defaults: Vec<(usize, usize, Value)>,
    remaining_defaults: Vec<(usize, usize, Value)>,
}

impl PartialDecodeCtx {
    fn new(schema: &TableSchema, needed: &[usize]) -> Self {
        let non_pk = schema.non_pk_indices();
        let enc_pos = schema.encoding_positions();
        let mut pk_positions = Vec::new();
        let mut nonpk_targets = Vec::new();
        let mut nonpk_schema = Vec::new();

        for &col in needed {
            if let Some(pk_pos) = schema
                .primary_key_columns
                .iter()
                .position(|&i| i as usize == col)
            {
                pk_positions.push((pk_pos, col));
            } else if let Some(nonpk_order) = non_pk.iter().position(|&i| i == col) {
                nonpk_targets.push(enc_pos[nonpk_order] as usize);
                nonpk_schema.push(col);
            }
        }

        let needed_set: std::collections::HashSet<usize> = needed.iter().copied().collect();
        let mut remaining_pk = Vec::new();
        for (pk_pos, &pk_col) in schema.primary_key_columns.iter().enumerate() {
            if !needed_set.contains(&(pk_col as usize)) {
                remaining_pk.push((pk_pos, pk_col as usize));
            }
        }
        let mut remaining_nonpk_targets = Vec::new();
        let mut remaining_nonpk_schema = Vec::new();
        for (nonpk_order, &col) in non_pk.iter().enumerate() {
            if !needed_set.contains(&col) {
                remaining_nonpk_targets.push(enc_pos[nonpk_order] as usize);
                remaining_nonpk_schema.push(col);
            }
        }

        let mut nonpk_defaults = Vec::new();
        for (&phys_pos, &schema_col) in nonpk_targets.iter().zip(nonpk_schema.iter()) {
            if let Some(ref expr) = schema.columns[schema_col].default_expr {
                if let Ok(val) = eval_const_expr(expr) {
                    nonpk_defaults.push((phys_pos, schema_col, val));
                }
            }
        }
        let mut remaining_defaults = Vec::new();
        for (&phys_pos, &schema_col) in remaining_nonpk_targets
            .iter()
            .zip(remaining_nonpk_schema.iter())
        {
            if let Some(ref expr) = schema.columns[schema_col].default_expr {
                if let Ok(val) = eval_const_expr(expr) {
                    remaining_defaults.push((phys_pos, schema_col, val));
                }
            }
        }

        Self {
            pk_positions,
            nonpk_targets,
            nonpk_schema,
            num_cols: schema.columns.len(),
            num_pk_cols: schema.primary_key_columns.len(),
            remaining_pk,
            remaining_nonpk_targets,
            remaining_nonpk_schema,
            nonpk_defaults,
            remaining_defaults,
        }
    }

    fn decode(&self, key: &[u8], value: &[u8]) -> Result<Vec<Value>> {
        let mut row = vec![Value::Null; self.num_cols];

        if self.pk_positions.len() == 1 && self.num_pk_cols == 1 {
            let (_, schema_col) = self.pk_positions[0];
            let (v, _) = decode_key_value(key)?;
            row[schema_col] = v;
        } else if !self.pk_positions.is_empty() {
            let mut pk_values = decode_composite_key(key, self.num_pk_cols)?;
            for &(pk_pos, schema_col) in &self.pk_positions {
                row[schema_col] = std::mem::take(&mut pk_values[pk_pos]);
            }
        }

        if !self.nonpk_targets.is_empty() {
            decode_columns_into(value, &self.nonpk_targets, &self.nonpk_schema, &mut row)?;
        }

        if !self.nonpk_defaults.is_empty() {
            let stored = row_non_pk_count(value);
            for (nonpk_idx, schema_col, default) in &self.nonpk_defaults {
                if *nonpk_idx >= stored {
                    row[*schema_col] = default.clone();
                }
            }
        }

        Ok(row)
    }

    fn complete(&self, mut row: Vec<Value>, key: &[u8], value: &[u8]) -> Result<Vec<Value>> {
        if !self.remaining_pk.is_empty() {
            let mut pk_values = decode_composite_key(key, self.num_pk_cols)?;
            for &(pk_pos, schema_col) in &self.remaining_pk {
                row[schema_col] = std::mem::take(&mut pk_values[pk_pos]);
            }
        }
        if !self.remaining_nonpk_targets.is_empty() {
            let mut values = decode_columns(value, &self.remaining_nonpk_targets)?;
            for (i, &schema_col) in self.remaining_nonpk_schema.iter().enumerate() {
                row[schema_col] = std::mem::take(&mut values[i]);
            }
        }
        if !self.remaining_defaults.is_empty() {
            let stored = row_non_pk_count(value);
            for (nonpk_idx, schema_col, default) in &self.remaining_defaults {
                if *nonpk_idx >= stored {
                    row[*schema_col] = default.clone();
                }
            }
        }
        Ok(row)
    }
}

fn decode_full_row(schema: &TableSchema, key: &[u8], value: &[u8]) -> Result<Vec<Value>> {
    let mut row = vec![Value::Null; schema.columns.len()];
    decode_pk_into(
        key,
        schema.primary_key_columns.len(),
        &mut row,
        schema.pk_indices(),
    )?;
    let mapping = schema.decode_col_mapping();
    let stored_count = row_non_pk_count(value);
    decode_row_into(value, &mut row, mapping)?;
    // Fill defaults for physical positions beyond stored count
    // (columns added after this row was written)
    if stored_count < mapping.len() {
        for &logical_idx in mapping.iter().skip(stored_count) {
            if logical_idx != usize::MAX {
                if let Some(ref expr) = schema.columns[logical_idx].default_expr {
                    row[logical_idx] = eval_const_expr(expr)?;
                }
            }
        }
    }
    Ok(row)
}

/// Evaluate a constant expression (no column references).
fn eval_const_expr(expr: &Expr) -> Result<Value> {
    static EMPTY: std::sync::OnceLock<ColumnMap> = std::sync::OnceLock::new();
    let empty = EMPTY.get_or_init(|| ColumnMap::new(&[]));
    eval_expr(expr, empty, &[])
}

fn eval_const_int(expr: &Expr) -> Result<i64> {
    match eval_const_expr(expr)? {
        Value::Integer(i) => Ok(i),
        other => Err(SqlError::TypeMismatch {
            expected: "INTEGER".into(),
            got: other.data_type().to_string(),
        }),
    }
}

fn sort_rows(
    rows: &mut [Vec<Value>],
    order_by: &[OrderByItem],
    columns: &[ColumnDef],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let col_map = ColumnMap::new(columns);
    let mut indices: Vec<usize> = (0..rows.len()).collect();

    if let Some(col_idx) = try_resolve_flat_sort_col(order_by, &col_map) {
        let desc = order_by[0].descending;
        let nulls_first = order_by[0].nulls_first.unwrap_or(!desc);
        indices.sort_by(|&a, &b| {
            compare_flat_key(&rows[a][col_idx], &rows[b][col_idx], desc, nulls_first)
        });
    } else {
        let keys = extract_sort_keys(rows, order_by, &col_map);
        indices.sort_by(|&a, &b| compare_sort_keys(&keys[a], &keys[b], order_by));
    }

    let sorted: Vec<Vec<Value>> = indices
        .iter()
        .map(|&i| std::mem::take(&mut rows[i]))
        .collect();
    rows.iter_mut()
        .zip(sorted)
        .for_each(|(slot, row)| *slot = row);
    Ok(())
}

fn topk_rows(
    rows: &mut [Vec<Value>],
    order_by: &[OrderByItem],
    columns: &[ColumnDef],
    k: usize,
) -> Result<()> {
    let col_map = ColumnMap::new(columns);
    let mut indices: Vec<usize> = (0..rows.len()).collect();

    if let Some(col_idx) = try_resolve_flat_sort_col(order_by, &col_map) {
        let desc = order_by[0].descending;
        let nulls_first = order_by[0].nulls_first.unwrap_or(!desc);
        let cmp = |&a: &usize, &b: &usize| {
            compare_flat_key(&rows[a][col_idx], &rows[b][col_idx], desc, nulls_first)
        };
        indices.select_nth_unstable_by(k - 1, cmp);
        indices[..k].sort_by(cmp);
    } else {
        let keys = extract_sort_keys(rows, order_by, &col_map);
        let cmp = |&a: &usize, &b: &usize| compare_sort_keys(&keys[a], &keys[b], order_by);
        indices.select_nth_unstable_by(k - 1, cmp);
        indices[..k].sort_by(cmp);
    }

    let sorted: Vec<Vec<Value>> = indices[..k]
        .iter()
        .map(|&i| std::mem::take(&mut rows[i]))
        .collect();
    rows[..k]
        .iter_mut()
        .zip(sorted)
        .for_each(|(slot, row)| *slot = row);
    Ok(())
}

fn try_resolve_flat_sort_col(order_by: &[OrderByItem], col_map: &ColumnMap) -> Option<usize> {
    if order_by.len() != 1 {
        return None;
    }
    match &order_by[0].expr {
        Expr::Column(name) => col_map.resolve(&name.to_ascii_lowercase()).ok(),
        _ => None,
    }
}

fn compare_flat_key(a: &Value, b: &Value, desc: bool, nulls_first: bool) -> std::cmp::Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => {
            if nulls_first {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        }
        (false, true) => {
            if nulls_first {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }
        (false, false) => {
            let cmp = a.cmp(b);
            if desc {
                cmp.reverse()
            } else {
                cmp
            }
        }
    }
}

fn extract_sort_keys(
    rows: &[Vec<Value>],
    order_by: &[OrderByItem],
    col_map: &ColumnMap,
) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| {
            order_by
                .iter()
                .map(|item| eval_expr(&item.expr, col_map, row).unwrap_or(Value::Null))
                .collect()
        })
        .collect()
}

fn compare_sort_keys(a: &[Value], b: &[Value], order_by: &[OrderByItem]) -> std::cmp::Ordering {
    for (i, item) in order_by.iter().enumerate() {
        let nulls_first = item.nulls_first.unwrap_or(!item.descending);
        let ord = match (a[i].is_null(), b[i].is_null()) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            (false, false) => {
                let cmp = a[i].cmp(&b[i]);
                if item.descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

fn try_build_index_map(
    select_cols: &[SelectColumn],
    columns: &[ColumnDef],
) -> Option<Vec<(String, usize)>> {
    let col_map = ColumnMap::new(columns);
    let mut map = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for sel in select_cols {
        match sel {
            SelectColumn::AllColumns => {
                for col in columns {
                    let idx = col.position as usize;
                    if !seen.insert(idx) {
                        return None;
                    }
                    map.push((col.name.clone(), idx));
                }
            }
            SelectColumn::Expr { expr, alias } => {
                let idx = match expr {
                    Expr::Column(name) => col_map.resolve(name).ok()?,
                    Expr::QualifiedColumn { table, column } => {
                        col_map.resolve_qualified(table, column).ok()?
                    }
                    _ => return None,
                };
                if !seen.insert(idx) {
                    return None;
                }
                let name = alias.clone().unwrap_or_else(|| expr_display_name(expr));
                map.push((name, idx));
            }
        }
    }
    Some(map)
}

fn project_rows(
    columns: &[ColumnDef],
    select_cols: &[SelectColumn],
    mut rows: Vec<Vec<Value>>,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    // Fast path: SELECT * - zero clones
    if select_cols.len() == 1 && matches!(select_cols[0], SelectColumn::AllColumns) {
        let col_names = columns.iter().map(|c| c.name.clone()).collect();
        return Ok((col_names, rows));
    }

    // Fast path: all simple column refs - use mem::take, zero clones
    if let Some(map) = try_build_index_map(select_cols, columns) {
        let col_names: Vec<String> = map.iter().map(|(n, _)| n.clone()).collect();
        // Identity: columns already in the right order - return as-is
        if map.len() == columns.len() && map.iter().enumerate().all(|(i, &(_, idx))| idx == i) {
            return Ok((col_names, rows));
        }
        let projected = rows
            .iter_mut()
            .map(|row| {
                map.iter()
                    .map(|&(_, idx)| std::mem::take(&mut row[idx]))
                    .collect()
            })
            .collect();
        return Ok((col_names, projected));
    }

    // Fallback: expression evaluation (requires cloning)
    let mut col_names = Vec::new();
    type Projector = Box<dyn Fn(&[Value]) -> Result<Value>>;
    let mut projectors: Vec<Projector> = Vec::new();
    let col_map = std::sync::Arc::new(ColumnMap::new(columns));

    for sel_col in select_cols {
        match sel_col {
            SelectColumn::AllColumns => {
                for col in columns {
                    let idx = col.position as usize;
                    col_names.push(col.name.clone());
                    projectors.push(Box::new(move |row: &[Value]| Ok(row[idx].clone())));
                }
            }
            SelectColumn::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| expr_display_name(expr));
                col_names.push(name);
                let expr = expr.clone();
                let map = col_map.clone();
                projectors.push(Box::new(move |row: &[Value]| eval_expr(&expr, &map, row)));
            }
        }
    }

    let projected = rows
        .iter()
        .map(|row| {
            projectors
                .iter()
                .map(|p| p(row))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((col_names, projected))
}

fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::QualifiedColumn { table, column } => format!("{table}.{column}"),
        Expr::Literal(v) => format!("{v}"),
        Expr::CountStar => "COUNT(*)".into(),
        Expr::Function { name, args } => {
            let arg_strs: Vec<String> = args.iter().map(expr_display_name).collect();
            format!("{name}({})", arg_strs.join(", "))
        }
        Expr::BinaryOp { left, op, right } => {
            format!(
                "{} {} {}",
                expr_display_name(left),
                op_symbol(op),
                expr_display_name(right)
            )
        }
        _ => "?".into(),
    }
}

fn op_symbol(op: &BinOp) -> &'static str {
    match op {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Eq => "=",
        BinOp::NotEq => "<>",
        BinOp::Lt => "<",
        BinOp::Gt => ">",
        BinOp::LtEq => "<=",
        BinOp::GtEq => ">=",
        BinOp::And => "AND",
        BinOp::Or => "OR",
        BinOp::Concat => "||",
    }
}

fn build_output_columns(select_cols: &[SelectColumn], columns: &[ColumnDef]) -> Vec<ColumnDef> {
    let mut out = Vec::new();
    for (i, col) in select_cols.iter().enumerate() {
        let (name, data_type) = match col {
            SelectColumn::AllColumns => (format!("col{i}"), DataType::Null),
            SelectColumn::Expr {
                alias: Some(a),
                expr,
            } => (a.clone(), infer_expr_type(expr, columns)),
            SelectColumn::Expr { expr, .. } => {
                (expr_display_name(expr), infer_expr_type(expr, columns))
            }
        };
        out.push(ColumnDef {
            name,
            data_type,
            nullable: true,
            position: i as u16,
            default_expr: None,
            default_sql: None,
            check_expr: None,
            check_sql: None,
            check_name: None,
        });
    }
    out
}

fn infer_expr_type(expr: &Expr, columns: &[ColumnDef]) -> DataType {
    match expr {
        Expr::Column(name) => columns
            .iter()
            .find(|c| c.name == *name)
            .map(|c| c.data_type)
            .unwrap_or(DataType::Null),
        Expr::QualifiedColumn { table, column } => {
            let qualified = format!("{table}.{column}");
            columns
                .iter()
                .find(|c| c.name == qualified)
                .map(|c| c.data_type)
                .unwrap_or(DataType::Null)
        }
        Expr::Literal(v) => v.data_type(),
        Expr::CountStar => DataType::Integer,
        Expr::Function { name, .. } => match name.to_ascii_uppercase().as_str() {
            "COUNT" => DataType::Integer,
            "AVG" => DataType::Real,
            "SUM" | "MIN" | "MAX" => DataType::Null,
            _ => DataType::Null,
        },
        _ => DataType::Null,
    }
}
