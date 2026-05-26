use citadel::Database;

use crate::error::{Result, SqlError};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::helpers::*;

pub(super) fn collect_column_refs(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Column(name) => out.push(name.to_ascii_lowercase()),
        Expr::QualifiedColumn { column, .. } => out.push(column.to_ascii_lowercase()),
        Expr::BinaryOp { left, right, .. } => {
            collect_column_refs(left, out);
            collect_column_refs(right, out);
        }
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } => collect_column_refs(expr, out),
        Expr::Function { args, .. } => {
            for a in args {
                collect_column_refs(a, out);
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            if let Some(o) = operand {
                collect_column_refs(o, out);
            }
            for (c, r) in conditions {
                collect_column_refs(c, out);
                collect_column_refs(r, out);
            }
            if let Some(e) = else_result {
                collect_column_refs(e, out);
            }
        }
        Expr::Coalesce(items) => {
            for i in items {
                collect_column_refs(i, out);
            }
        }
        _ => {}
    }
}

pub(super) fn validate_no_chained_generated(columns: &[ColumnDef]) -> Result<()> {
    let generated_names: rustc_hash::FxHashSet<String> = columns
        .iter()
        .filter(|c| c.generated_kind.is_some())
        .map(|c| c.name.to_ascii_lowercase())
        .collect();
    if generated_names.is_empty() {
        return Ok(());
    }
    for col in columns {
        if let Some(expr) = &col.generated_expr {
            let mut refs = Vec::new();
            collect_column_refs(expr, &mut refs);
            for r in refs {
                if generated_names.contains(&r) && r != col.name.to_ascii_lowercase() {
                    return Err(SqlError::GeneratedColumnReference(col.name.clone()));
                }
            }
        }
    }
    Ok(())
}

/// Validate FK references: parent must exist, referred columns must be PK or UNIQUE.
pub(super) fn validate_foreign_keys(
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

        let is_pk = parent.primary_key_columns == ref_col_indices;
        let has_unique = !is_pk
            && parent
                .indices
                .iter()
                .any(|idx| idx.unique && idx.columns_vec() == ref_col_indices);

        if !is_pk && !has_unique {
            return Err(SqlError::Unsupported(format!(
                "FOREIGN KEY on '{}': referred columns in '{}' are not PRIMARY KEY or UNIQUE",
                table_schema.name, fk.foreign_table
            )));
        }
    }
    Ok(())
}

/// Create auto-indexes from inline `UNIQUE` constraints parsed from CREATE TABLE.
pub(super) fn create_unique_auto_indices(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    mut table_schema: TableSchema,
    stmt: &CreateTableStmt,
) -> Result<TableSchema> {
    for (i, uq) in stmt.unique_indices.iter().enumerate() {
        let col_idxs: Vec<u16> = uq
            .columns
            .iter()
            .map(|name| {
                table_schema
                    .column_index(name)
                    .map(|idx| idx as u16)
                    .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))
            })
            .collect::<Result<_>>()?;

        if col_idxs == table_schema.primary_key_columns {
            continue;
        }

        if table_schema
            .indices
            .iter()
            .any(|idx| idx.unique && idx.columns_vec() == col_idxs)
        {
            continue;
        }

        let idx_name = uq
            .name
            .clone()
            .unwrap_or_else(|| format!("__uq_{}_{}", table_schema.name, i));

        if uq.name.is_some()
            && table_schema
                .index_by_name(&idx_name.to_ascii_lowercase())
                .is_some()
        {
            return Err(SqlError::IndexAlreadyExists(idx_name));
        }

        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx_name);
        wtx.create_table(&idx_table).map_err(SqlError::Storage)?;
        let collations: Vec<crate::types::Collation> = col_idxs
            .iter()
            .map(|&i| table_schema.columns[i as usize].collation)
            .collect();
        table_schema.indices.push(IndexDef::from_column_lists(
            idx_name,
            col_idxs,
            collations,
            true,
            None,
            None,
            crate::types::IndexKind::default(),
        ));
    }
    Ok(table_schema)
}

/// Create auto-index on child FK columns. Returns updated schema with new indices.
pub(super) fn create_fk_auto_indices(
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
        let already_covered = table_schema
            .indices
            .iter()
            .any(|idx| idx.columns_vec() == cols);
        if already_covered {
            continue;
        }

        let idx_def = IndexDef::from_column_lists(
            idx_name.clone(),
            cols,
            vec![],
            false,
            None,
            None,
            IndexKind::default(),
        );
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx_name);
        wtx.create_table(&idx_table).map_err(SqlError::Storage)?;
        // Table is empty at CREATE TABLE time - no rows to populate
        table_schema.indices.push(idx_def);
    }
    Ok(table_schema)
}

pub(super) fn exec_create_table(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &CreateTableStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if schema.get_view(&lower_name).is_some() {
        return Err(SqlError::ViewAlreadyExists(stmt.name.clone()));
    }

    if schema.contains(&lower_name) {
        if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::TableAlreadyExists(stmt.name.clone()));
    }

    if stmt.primary_key.is_empty() {
        return Err(SqlError::PrimaryKeyRequired);
    }

    let mut seen = rustc_hash::FxHashSet::default();
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
            is_with_timezone: false,
            generated_expr: c.generated_expr.clone(),
            generated_sql: c.generated_sql.clone(),
            generated_kind: c.generated_kind,
            collation: c.collation,
        })
        .collect();

    validate_no_chained_generated(&columns)?;

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
                on_delete: fk.on_delete,
                on_update: fk.on_update,
                deferrable: fk.deferrable,
                initially_deferred: fk.initially_deferred,
            })
        })
        .collect::<Result<_>>()?;

    let mut table_schema = TableSchema::new(
        lower_name.clone(),
        columns,
        primary_key_columns,
        vec![],
        check_constraints,
        foreign_keys,
    );
    if stmt.strict {
        table_schema.flags |= crate::types::TABLE_FLAG_STRICT;
    }

    validate_foreign_keys(schema, &table_schema, &table_schema.foreign_keys)?;

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    SchemaManager::ensure_schema_table(&mut wtx)?;
    wtx.create_table(lower_name.as_bytes())
        .map_err(SqlError::Storage)?;

    let table_schema = create_unique_auto_indices(&mut wtx, table_schema, stmt)?;
    let table_schema = create_fk_auto_indices(&mut wtx, table_schema)?;

    SchemaManager::save_schema(&mut wtx, &table_schema)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.register(table_schema);
    Ok(ExecutionResult::Ok)
}

pub(super) fn exec_drop_table(
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
    let storage_name = table_schema.name.clone();
    let idx_tables: Vec<Vec<u8>> = table_schema
        .indices
        .iter()
        .map(|idx| TableSchema::index_table_name(&storage_name, &idx.name))
        .collect();

    let trigger_names: Vec<String> = schema
        .triggers_for(&storage_name)
        .iter()
        .map(|t| t.name.clone())
        .collect();

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    for idx_table in &idx_tables {
        wtx.drop_table(idx_table).map_err(SqlError::Storage)?;
    }
    wtx.drop_table(storage_name.as_bytes())
        .map_err(SqlError::Storage)?;
    for tname in &trigger_names {
        SchemaManager::delete_trigger(&mut wtx, tname)?;
    }
    SchemaManager::delete_schema(&mut wtx, &storage_name)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.remove_triggers_for(&storage_name);
    schema.remove(&storage_name);
    Ok(ExecutionResult::Ok)
}

pub(super) fn exec_create_table_in_txn(
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

    let mut seen = rustc_hash::FxHashSet::default();
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
            is_with_timezone: false,
            generated_expr: c.generated_expr.clone(),
            generated_sql: c.generated_sql.clone(),
            generated_kind: c.generated_kind,
            collation: c.collation,
        })
        .collect();

    validate_no_chained_generated(&columns)?;

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
                on_delete: fk.on_delete,
                on_update: fk.on_update,
                deferrable: fk.deferrable,
                initially_deferred: fk.initially_deferred,
            })
        })
        .collect::<Result<_>>()?;

    let mut table_schema = TableSchema::new(
        lower_name.clone(),
        columns,
        primary_key_columns,
        vec![],
        check_constraints,
        foreign_keys,
    );
    if stmt.strict {
        table_schema.flags |= crate::types::TABLE_FLAG_STRICT;
    }

    validate_foreign_keys(schema, &table_schema, &table_schema.foreign_keys)?;

    SchemaManager::ensure_schema_table(wtx)?;
    wtx.create_table(lower_name.as_bytes())
        .map_err(SqlError::Storage)?;

    let table_schema = create_unique_auto_indices(wtx, table_schema, stmt)?;
    let table_schema = create_fk_auto_indices(wtx, table_schema)?;

    SchemaManager::save_schema(wtx, &table_schema)?;

    schema.register(table_schema);
    Ok(ExecutionResult::Ok)
}

pub(super) fn exec_drop_table_in_txn(
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

    let trigger_names: Vec<String> = schema
        .triggers_for(&lower_name)
        .iter()
        .map(|t| t.name.clone())
        .collect();

    for idx_table in &idx_tables {
        wtx.drop_table(idx_table).map_err(SqlError::Storage)?;
    }
    wtx.drop_table(lower_name.as_bytes())
        .map_err(SqlError::Storage)?;
    for tname in &trigger_names {
        SchemaManager::delete_trigger(wtx, tname)?;
    }
    SchemaManager::delete_schema(wtx, &lower_name)?;

    schema.remove_triggers_for(&lower_name);
    schema.remove(&lower_name);
    Ok(ExecutionResult::Ok)
}

pub(super) fn exec_truncate(
    db: &Database,
    schema: &SchemaManager,
    stmt: &TruncateStmt,
) -> Result<ExecutionResult> {
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let count = truncate_tables(&mut wtx, schema, stmt)?;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

pub(super) fn exec_truncate_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &TruncateStmt,
) -> Result<ExecutionResult> {
    let count = truncate_tables(wtx, schema, stmt)?;
    Ok(ExecutionResult::RowsAffected(count))
}

fn truncate_tables(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &TruncateStmt,
) -> Result<u64> {
    let lower: Vec<String> = stmt.tables.iter().map(|t| t.to_ascii_lowercase()).collect();
    let listed: rustc_hash::FxHashSet<&str> = lower.iter().map(String::as_str).collect();

    for name in &lower {
        if !schema.contains(name) {
            return Err(SqlError::TableNotFound(name.clone()));
        }
        for (child, _fk) in schema.child_fks_for(name) {
            if !listed.contains(child) {
                return Err(SqlError::ForeignKeyViolation(format!(
                    "cannot truncate table '{}': referenced by foreign key in '{}'",
                    name, child
                )));
            }
        }
    }

    let mut total = 0u64;
    for name in &lower {
        let table_schema = schema.get(name).unwrap();
        let count = wtx
            .table_truncate(name.as_bytes())
            .map_err(SqlError::Storage)?;
        for idx in &table_schema.indices {
            let idx_table = TableSchema::index_table_name(name, &idx.name);
            wtx.table_truncate(&idx_table).map_err(SqlError::Storage)?;
        }
        total += count;
    }
    Ok(total)
}

fn build_index_def_for_create(
    stmt: &CreateIndexStmt,
    table_schema: &TableSchema,
    name: String,
) -> Result<IndexDef> {
    let mut keys: Vec<crate::types::IndexKey> = Vec::with_capacity(stmt.columns.len());
    for (i, raw_name) in stmt.columns.iter().enumerate() {
        if let Some(Some((expr, sql))) = stmt.key_exprs.get(i) {
            keys.push(crate::types::IndexKey::Expr {
                expr: expr.clone(),
                original_sql: sql.clone(),
            });
            continue;
        }
        let lower = raw_name.to_ascii_lowercase();
        let col_idx = table_schema
            .column_index(&lower)
            .ok_or_else(|| SqlError::ColumnNotFound(raw_name.clone()))?
            as u16;
        if matches!(
            table_schema.columns[col_idx as usize].generated_kind,
            Some(crate::parser::GeneratedKind::Virtual)
        ) {
            return Err(SqlError::Unsupported(format!(
                "cannot CREATE INDEX on VIRTUAL generated column '{}'",
                table_schema.columns[col_idx as usize].name
            )));
        }
        let explicit_collate = stmt
            .collations
            .get(i)
            .copied()
            .unwrap_or(crate::types::Collation::Binary);
        let collate = if explicit_collate != crate::types::Collation::Binary {
            explicit_collate
        } else {
            table_schema.columns[col_idx as usize].collation
        };
        keys.push(crate::types::IndexKey::Column {
            idx: col_idx,
            collate,
        });
    }
    Ok(IndexDef {
        name,
        keys,
        unique: stmt.unique,
        predicate_sql: stmt.predicate_sql.clone(),
        predicate_expr: stmt.predicate_expr.clone(),
        kind: stmt.kind,
    })
}

pub(super) fn exec_create_index(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &CreateIndexStmt,
) -> Result<ExecutionResult> {
    let lower_table = stmt.table_name.to_ascii_lowercase();
    let lower_idx = stmt.index_name.to_ascii_lowercase();

    let table_schema = schema
        .get(&lower_table)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table_name.clone()))?;
    // Resolve through any TEMP alias: storage operations must use the schema's actual `name`.
    let storage_table = table_schema.name.clone();

    if table_schema.index_by_name(&lower_idx).is_some() {
        if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::IndexAlreadyExists(stmt.index_name.clone()));
    }

    let idx_def = build_index_def_for_create(stmt, table_schema, lower_idx.clone())?;

    let idx_table = TableSchema::index_table_name(&storage_table, &lower_idx);

    let pk_indices = table_schema.pk_indices();
    // CONCURRENTLY: pre-scan rows under a ReadTxn (no write lock held). Other writers can
    // proceed during the scan. If a writer commits between our snapshot and the merge,
    // we re-scan under the WriteTxn to ensure correctness.
    let (mut rows, prescan_gen): (Vec<Vec<Value>>, Option<u64>) = if stmt.concurrently {
        let mut rtx = db.begin_read();
        let g1 = rtx.commit_generation();
        let mut prescan: Vec<Vec<Value>> = Vec::new();
        let mut scan_err: Option<SqlError> = None;
        rtx.table_for_each(storage_table.as_bytes(), |key, value| {
            match decode_full_row(table_schema, key, value) {
                Ok(row) => prescan.push(row),
                Err(e) => scan_err = Some(e),
            }
            Ok(())
        })
        .map_err(SqlError::Storage)?;
        drop(rtx);
        if let Some(e) = scan_err {
            return Err(e);
        }
        (prescan, Some(g1))
    } else {
        (Vec::new(), None)
    };

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    SchemaManager::ensure_schema_table(&mut wtx)?;
    wtx.create_table(&idx_table).map_err(SqlError::Storage)?;

    // For non-concurrent path, scan under the write lock. For concurrent path, re-scan only
    // if commit_generation moved (a writer committed between our snapshot and now).
    if let Some(g1) = prescan_gen {
        let g_now = db.manager().commit_generation();
        if g_now != g1 {
            rows.clear();
            let mut scan_err: Option<SqlError> = None;
            wtx.table_for_each(storage_table.as_bytes(), |key, value| {
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
    } else {
        let mut scan_err: Option<SqlError> = None;
        wtx.table_for_each(storage_table.as_bytes(), |key, value| {
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

    if let crate::types::IndexKind::Inverted(inv_kind) = idx_def.kind {
        let col_idx = idx_def.column_positions_iter().next().ok_or_else(|| {
            SqlError::Unsupported("inverted index requires at least one column key".into())
        })? as usize;
        let col_type = table_schema.columns[col_idx].data_type;
        match inv_kind {
            crate::types::InvertedKind::Gin(_) => {
                if !matches!(
                    col_type,
                    crate::types::DataType::Json | crate::types::DataType::Jsonb
                ) {
                    return Err(SqlError::Unsupported(
                        "GIN index requires a JSON or JSONB column".into(),
                    ));
                }
            }
            crate::types::InvertedKind::Fts { .. } => {
                if !matches!(
                    col_type,
                    crate::types::DataType::Text | crate::types::DataType::TsVector
                ) {
                    return Err(SqlError::Unsupported(format!(
                        "FTS index requires a TEXT or TSVECTOR column, got {col_type}"
                    )));
                }
            }
        }
        if idx_def.unique {
            return Err(SqlError::Unsupported(
                "UNIQUE not supported on inverted indexes".into(),
            ));
        }
    }

    for row in &rows {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        if let crate::types::IndexKind::Inverted(inv_kind) = idx_def.kind {
            let value = &row[idx_def.column_positions_iter().next().unwrap() as usize];
            if !value.is_null() {
                let entries =
                    super::helpers::extract_inverted_entries_with_values(value, inv_kind)?;
                let pk_encoded = crate::encoding::encode_composite_key(&pk_values);
                for (entry, val_bytes) in entries {
                    let full_key = super::helpers::build_inverted_key(&entry, &pk_encoded);
                    wtx.table_insert(&idx_table, &full_key, &val_bytes)
                        .map_err(SqlError::Storage)?;
                }
            }
            continue;
        }
        let key = encode_index_key_with_schema(&idx_def, row, &pk_values, table_schema);
        let value = encode_index_value(&idx_def, row, &pk_values);
        let is_new = wtx
            .table_insert(&idx_table, &key, &value)
            .map_err(SqlError::Storage)?;
        if idx_def.unique && !is_new {
            let indexed_values: Vec<Value> = idx_def
                .column_positions_iter()
                .map(|col_idx| row[col_idx as usize].clone())
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

pub(super) fn exec_drop_index(
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

pub(super) fn exec_create_index_in_txn(
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

    let idx_def = build_index_def_for_create(stmt, table_schema, lower_idx.clone())?;

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
        let key = encode_index_key_with_schema(&idx_def, row, &pk_values, table_schema);
        let value = encode_index_value(&idx_def, row, &pk_values);
        let is_new = wtx
            .table_insert(&idx_table, &key, &value)
            .map_err(SqlError::Storage)?;
        if idx_def.unique && !is_new {
            let indexed_values: Vec<Value> = idx_def
                .column_positions_iter()
                .map(|col_idx| row[col_idx as usize].clone())
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

pub(super) fn exec_drop_index_in_txn(
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

pub(super) fn exec_create_view(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &CreateViewStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if schema.contains(&lower_name) {
        return Err(SqlError::TableAlreadyExists(stmt.name.clone()));
    }

    let replacing = if let Some(existing) = schema.get_view(&lower_name) {
        if stmt.or_replace {
            true
        } else if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        } else {
            return Err(SqlError::ViewAlreadyExists(existing.name.clone()));
        }
    } else {
        false
    };

    let parsed = crate::parser::parse_sql(&stmt.sql)?;
    if !matches!(parsed, Statement::Select(_)) {
        return Err(SqlError::Parse(
            "view body must be a SELECT statement".into(),
        ));
    }

    let view_def = ViewDef {
        name: lower_name.clone(),
        sql: stmt.sql.clone(),
        column_aliases: stmt.column_aliases.clone(),
    };

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    SchemaManager::ensure_views_table(&mut wtx)?;
    if replacing {
        SchemaManager::delete_view(&mut wtx, &lower_name)?;
    }
    SchemaManager::save_view(&mut wtx, &view_def)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.register_view(view_def);
    Ok(ExecutionResult::Ok)
}

pub(super) fn exec_create_view_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &CreateViewStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if schema.contains(&lower_name) {
        return Err(SqlError::TableAlreadyExists(stmt.name.clone()));
    }

    let replacing = if let Some(existing) = schema.get_view(&lower_name) {
        if stmt.or_replace {
            true
        } else if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        } else {
            return Err(SqlError::ViewAlreadyExists(existing.name.clone()));
        }
    } else {
        false
    };

    let parsed = crate::parser::parse_sql(&stmt.sql)?;
    if !matches!(parsed, Statement::Select(_)) {
        return Err(SqlError::Parse(
            "view body must be a SELECT statement".into(),
        ));
    }

    let view_def = ViewDef {
        name: lower_name.clone(),
        sql: stmt.sql.clone(),
        column_aliases: stmt.column_aliases.clone(),
    };

    SchemaManager::ensure_views_table(wtx)?;
    if replacing {
        SchemaManager::delete_view(wtx, &lower_name)?;
    }
    SchemaManager::save_view(wtx, &view_def)?;

    schema.register_view(view_def);
    Ok(ExecutionResult::Ok)
}

pub(super) fn exec_drop_view(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &DropViewStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if schema.get_view(&lower_name).is_none() {
        if stmt.if_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::ViewNotFound(stmt.name.clone()));
    }

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    SchemaManager::delete_view(&mut wtx, &lower_name)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.remove_view(&lower_name);
    Ok(ExecutionResult::Ok)
}

pub(super) fn exec_drop_view_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &DropViewStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if schema.get_view(&lower_name).is_none() {
        if stmt.if_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::ViewNotFound(stmt.name.clone()));
    }

    SchemaManager::delete_view(wtx, &lower_name)?;

    schema.remove_view(&lower_name);
    Ok(ExecutionResult::Ok)
}

pub(super) fn exec_alter_table(
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

pub(super) fn exec_alter_table_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &AlterTableStmt,
) -> Result<ExecutionResult> {
    SchemaManager::ensure_schema_table(wtx)?;
    alter_table_impl(wtx, schema, stmt)?;
    Ok(ExecutionResult::Ok)
}

pub(super) fn alter_table_impl(
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
        AlterTableOp::DisableTrigger { name } => {
            alter_set_trigger_enabled(wtx, schema, name, false)
        }
        AlterTableOp::EnableTrigger { name } => alter_set_trigger_enabled(wtx, schema, name, true),
        AlterTableOp::DisableAllTriggers => {
            alter_set_all_triggers_enabled(wtx, schema, &lower_name, false)
        }
        AlterTableOp::EnableAllTriggers => {
            alter_set_all_triggers_enabled(wtx, schema, &lower_name, true)
        }
    }
}

fn alter_set_trigger_enabled(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    name: &str,
    enabled: bool,
) -> Result<()> {
    let trig = schema
        .find_trigger(name)
        .map(|(_, t)| t.clone())
        .ok_or_else(|| SqlError::Unsupported(format!("trigger '{name}' not found")))?;
    let mut updated = trig.clone();
    updated.enabled = enabled;
    SchemaManager::save_trigger(wtx, &updated)?;
    schema.set_trigger_enabled(name, enabled);
    Ok(())
}

fn alter_set_all_triggers_enabled(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    target: &str,
    enabled: bool,
) -> Result<()> {
    let triggers: Vec<crate::types::TriggerDef> = schema.triggers_for(target).to_vec();
    for mut t in triggers {
        t.enabled = enabled;
        SchemaManager::save_trigger(wtx, &t)?;
    }
    schema.set_all_triggers_enabled(target, enabled);
    Ok(())
}

pub(super) fn alter_add_column(
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

    if !col_spec.nullable && col_spec.default_expr.is_none() && col_spec.generated_kind.is_none() {
        let count = wtx.table_entry_count(table_name.as_bytes()).unwrap_or(0);
        if count > 0 {
            return Err(SqlError::Unsupported(
                "cannot add NOT NULL column without DEFAULT to non-empty table".into(),
            ));
        }
    }

    if matches!(
        col_spec.generated_kind,
        Some(crate::parser::GeneratedKind::Stored)
    ) {
        let count = wtx.table_entry_count(table_name.as_bytes()).unwrap_or(0);
        if count > 0 {
            return Err(SqlError::Unsupported(
                "cannot add STORED generated column to non-empty table; use VIRTUAL or recreate the table".into(),
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
        is_with_timezone: false,
        generated_expr: col_spec.generated_expr.clone(),
        generated_sql: col_spec.generated_sql.clone(),
        generated_kind: col_spec.generated_kind,
        collation: col_spec.collation,
    };

    let mut new_schema = table_schema.clone();
    new_schema.columns.push(new_col);

    validate_no_chained_generated(&new_schema.columns)?;

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
            on_delete: fk.on_delete,
            on_update: fk.on_update,
            deferrable: fk.deferrable,
            initially_deferred: fk.initially_deferred,
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

pub(super) fn alter_drop_column(
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
        if idx.column_positions_iter().any(|c| c == drop_pos_u16) {
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

    for col in &table_schema.columns {
        if col.position == drop_pos_u16 {
            continue;
        }
        if let Some(expr) = &col.generated_expr {
            let mut refs = Vec::new();
            collect_column_refs(expr, &mut refs);
            if refs.iter().any(|r| r == &col_lower) {
                return Err(SqlError::Unsupported(format!(
                    "column '{}' is referenced by generated column '{}'",
                    col_lower, col.name
                )));
            }
        }
    }

    // Schema-only: rows keep the dead slot; decode skips via col_mapping.
    let new_schema = table_schema.without_column(drop_pos);

    SchemaManager::save_schema(wtx, &new_schema)?;
    schema.register(new_schema);
    Ok(())
}

pub(super) fn alter_rename_column(
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

    // Update self-referencing FK referred_columns (cross-table FKs resolve by name at load)
    for fk in &mut new_schema.foreign_keys {
        if fk.foreign_table == table_name {
            for rc in &mut fk.referred_columns {
                if *rc == old_lower {
                    *rc = new_lower.clone();
                }
            }
        }
    }

    for col in &mut new_schema.columns {
        if let Some(ref sql) = col.generated_sql {
            if sql.to_ascii_lowercase().contains(&old_lower) {
                let updated = sql.replace(&old_lower, &new_lower);
                col.generated_sql = Some(updated.clone());
                if let Ok(parsed) = crate::parser::parse_sql_expr(&updated) {
                    col.generated_expr = Some(parsed);
                }
            }
        }
    }

    SchemaManager::save_schema(wtx, &new_schema)?;
    schema.register(new_schema);
    Ok(())
}

pub(super) fn alter_rename_table(
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
        .collect::<rustc_hash::FxHashSet<_>>()
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

pub(super) fn find_index_in_schemas(
    schema: &SchemaManager,
    index_name: &str,
) -> Option<(String, usize)> {
    for table_name in schema.table_names() {
        if let Some(ts) = schema.get(table_name) {
            if let Some(pos) = ts.indices.iter().position(|i| i.name == index_name) {
                return Some((table_name.to_string(), pos));
            }
        }
    }
    None
}
