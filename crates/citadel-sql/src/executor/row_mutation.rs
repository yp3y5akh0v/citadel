//! Shared row mutation semantics and iterative foreign-key actions.

use citadel_txn::write_txn::WriteTxn;

use crate::encoding::encode_composite_key;
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, EvalCtx};
use crate::parser::{Expr, ReferentialAction, SelectColumn, TriggerTiming, UpdateStmt};
use crate::schema::SchemaManager;
use crate::types::{ExecutionResult, ForeignKeySchemaEntry, IndexDef, TableSchema, Value};

use super::helpers::*;
use super::triggers::{self, FireEvent};

type KeyedRow = (Vec<u8>, Vec<Value>);

struct RowChange {
    key: Vec<u8>,
    old: Vec<Value>,
    new: Option<Vec<Value>>,
}

enum Mutation {
    Delete,
    Update(Vec<(String, Expr)>),
    /// Conflict expressions have already been evaluated against OLD/EXCLUDED.
    EvaluatedUpdate,
}

struct Operation<'a> {
    table: &'a TableSchema,
    changed_columns: Vec<String>,
    mutation: Mutation,
    refresh_rows: bool,
    has_children: bool,
    has_triggers: bool,
    rows: std::vec::IntoIter<RowChange>,
    old_rows: Vec<Vec<Value>>,
    new_rows: Vec<Vec<Value>>,
    capture_rows: bool,
    returning_rows: Vec<ReturningRow>,
    statement_triggers: bool,
    count: u64,
    root: bool,
}

impl Operation<'_> {
    fn event(&self) -> FireEvent<'_> {
        if !matches!(self.mutation, Mutation::Delete) {
            FireEvent::Update {
                changed_columns: &self.changed_columns,
            }
        } else {
            FireEvent::Delete
        }
    }
}

struct MutationResult {
    count: u64,
    rows: Vec<ReturningRow>,
}

impl MutationResult {
    fn project(
        self,
        table: &TableSchema,
        returning: Option<&[SelectColumn]>,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<ExecutionResult> {
        match returning {
            Some(columns) => Ok(ExecutionResult::Query(project_returning(
                table, columns, &self.rows, cancel,
            )?)),
            None => Ok(ExecutionResult::RowsAffected(self.count)),
        }
    }
}

struct NoActionCheck<'a> {
    child: &'a TableSchema,
    fk: &'a ForeignKeySchemaEntry,
    reference: super::fk::ReferenceKey,
    key: Vec<u8>,
}

/// The UPDATE half of one INSERT ... ON CONFLICT statement. Rows enter the
/// mutation engine immediately, while transitions and NO ACTION checks retain
/// statement scope across all input rows.
pub(super) struct ConflictUpdates<'a> {
    schema: &'a SchemaManager,
    table: &'a TableSchema,
    changed_columns: Vec<String>,
    has_children: bool,
    has_triggers: bool,
    statement_triggers: bool,
    old_rows: Vec<Vec<Value>>,
    new_rows: Vec<Vec<Value>>,
    no_action_checks: Vec<NoActionCheck<'a>>,
}

impl<'a> ConflictUpdates<'a> {
    pub(super) fn begin(
        wtx: &mut WriteTxn<'_>,
        schema: &'a SchemaManager,
        table: &'a TableSchema,
        changed_columns: Vec<String>,
    ) -> Result<Self> {
        let updates = Self {
            schema,
            table,
            changed_columns,
            has_children: !schema.child_fks_for(&table.name).is_empty(),
            has_triggers: triggers::has_update_triggers(schema, &table.name),
            statement_triggers: triggers::has_statement_update_triggers(schema, &table.name),
            old_rows: Vec::new(),
            new_rows: Vec::new(),
            no_action_checks: Vec::new(),
        };
        updates.fire_statement(wtx, TriggerTiming::Before)?;
        Ok(updates)
    }

    pub(super) fn can_fuse(&self) -> bool {
        !self.has_children && !self.has_triggers
    }

    pub(super) fn apply(
        &mut self,
        wtx: &mut WriteTxn<'_>,
        key: &[u8],
        old: &[Value],
        new: Vec<Value>,
        capture_rows: bool,
    ) -> Result<Option<(Vec<Value>, Vec<Value>)>> {
        self.schema.mark_dml(&self.table.name);
        let capture = capture_rows || self.statement_triggers;
        let operation = Operation {
            table: self.table,
            changed_columns: self.changed_columns.clone(),
            mutation: Mutation::EvaluatedUpdate,
            // The conflict lookup immediately precedes expression evaluation.
            // BEFORE row triggers are checked for same-row changes in apply_row.
            refresh_rows: false,
            has_children: self.has_children,
            has_triggers: self.has_triggers,
            rows: vec![RowChange {
                key: key.to_vec(),
                old: old.to_vec(),
                new: Some(new),
            }]
            .into_iter(),
            old_rows: Vec::new(),
            new_rows: Vec::new(),
            capture_rows: capture,
            returning_rows: Vec::new(),
            statement_triggers: false,
            count: 0,
            root: true,
        };
        let mut result = run(wtx, self.schema, operation, &mut self.no_action_checks)?;
        if !capture {
            return Ok(None);
        }
        let Some((Some(old), Some(new))) = result.rows.pop() else {
            return Err(SqlError::Unsupported(
                "conflict update did not produce a row".into(),
            ));
        };
        if self.statement_triggers {
            if !capture_rows {
                self.old_rows.push(old);
                self.new_rows.push(new);
                return Ok(None);
            }
            self.old_rows.push(old.clone());
            self.new_rows.push(new.clone());
        }
        Ok(capture_rows.then_some((old, new)))
    }

    pub(super) fn after_rows(&self, wtx: &mut WriteTxn<'_>) -> Result<()> {
        self.fire_statement(wtx, TriggerTiming::After)
    }

    pub(super) fn finish(self, wtx: &mut WriteTxn<'_>) -> Result<()> {
        check_no_action(wtx, self.schema, self.no_action_checks)
    }

    fn fire_statement(&self, wtx: &mut WriteTxn<'_>, timing: TriggerTiming) -> Result<()> {
        if self.statement_triggers {
            triggers::fire_statement_triggers(
                wtx,
                self.schema,
                &self.table.name,
                timing,
                FireEvent::Update {
                    changed_columns: &self.changed_columns,
                },
                &self.table.columns,
                &self.old_rows,
                &self.new_rows,
            )?;
        }
        Ok(())
    }
}

struct ParentChange<'a> {
    table: &'a TableSchema,
    key: Vec<u8>,
    old: Option<Vec<Value>>,
    new: Option<Vec<Value>>,
}

enum Work<'a> {
    Row(Operation<'a>),
    AfterRow(Operation<'a>, RowChange),
    ForeignKeys(ParentChange<'a>, usize),
}

pub(super) fn update_rows(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    table: &TableSchema,
    stmt: &UpdateStmt,
    rows: Vec<KeyedRow>,
) -> Result<ExecutionResult> {
    let operation = prepare_operation(
        wtx,
        schema,
        table,
        Some(&stmt.assignments),
        stmt.returning.is_some(),
        rows,
        true,
    )?;
    let mut checks = Vec::new();
    let result = run(wtx, schema, operation, &mut checks)?;
    check_no_action(wtx, schema, checks)?;
    result.project(table, stmt.returning.as_deref(), wtx.cancel_token())
}

pub(super) fn delete_rows(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    table: &TableSchema,
    returning: Option<Vec<SelectColumn>>,
    rows: Vec<KeyedRow>,
) -> Result<ExecutionResult> {
    let operation = prepare_operation(wtx, schema, table, None, returning.is_some(), rows, true)?;
    let mut checks = Vec::new();
    let result = run(wtx, schema, operation, &mut checks)?;
    check_no_action(wtx, schema, checks)?;
    result.project(table, returning.as_deref(), wtx.cancel_token())
}

fn prepare_operation<'a>(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    table: &'a TableSchema,
    assignments: Option<&[(String, Expr)]>,
    capture_rows: bool,
    rows: Vec<KeyedRow>,
    root: bool,
) -> Result<Operation<'a>> {
    check_cancel(wtx.cancel_token())?;
    schema.mark_dml(&table.name);
    if table.has_ann_index() {
        super::ann_persist::purge_segment(wtx, &table.name)?;
    }
    let changed_columns = assignments.map_or_else(Vec::new, |a| {
        a.iter().map(|(name, _)| name.clone()).collect()
    });
    let is_update = assignments.is_some();
    let statement_triggers = if is_update {
        triggers::has_statement_update_triggers(schema, &table.name)
    } else {
        triggers::has_statement_delete_triggers(schema, &table.name)
    };
    let mut changes = Vec::with_capacity(rows.len());
    let mut old_rows = Vec::new();
    let mut new_rows = Vec::new();
    for (key, old) in rows {
        check_cancel(wtx.cancel_token())?;
        let new = assignments
            .map(|a| evaluate_update(table, a, &old, wtx.cancel_token()))
            .transpose()?;
        if statement_triggers {
            old_rows.push(old.clone());
            if let Some(new) = &new {
                new_rows.push(new.clone());
            }
        }
        changes.push(RowChange { key, old, new });
    }
    let has_children = !schema.child_fks_for(&table.name).is_empty();
    let has_triggers = if is_update {
        triggers::has_update_triggers(schema, &table.name)
    } else {
        triggers::has_delete_triggers(schema, &table.name)
    };
    let operation = Operation {
        table,
        changed_columns,
        mutation: assignments.map_or(Mutation::Delete, |a| Mutation::Update(a.to_vec())),
        refresh_rows: has_children || has_triggers || !table.foreign_keys.is_empty(),
        has_children,
        has_triggers,
        count: 0,
        rows: changes.into_iter(),
        old_rows,
        new_rows,
        capture_rows,
        returning_rows: Vec::new(),
        statement_triggers,
        root,
    };
    if statement_triggers {
        triggers::fire_statement_triggers(
            wtx,
            schema,
            &table.name,
            TriggerTiming::Before,
            operation.event(),
            &table.columns,
            &operation.old_rows,
            &operation.new_rows,
        )?;
    }
    Ok(operation)
}

fn evaluate_update(
    table: &TableSchema,
    assignments: &[(String, Expr)],
    old: &[Value],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Value>> {
    let mut new = old.to_vec();
    let col_map = table.column_map();
    for (name, expr) in assignments {
        let position = table
            .column_index(name)
            .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))?;
        let column = &table.columns[position];
        if column.generated_kind.is_some() {
            return Err(SqlError::CannotUpdateGeneratedColumn(column.name.clone()));
        }
        let value = eval_expr(expr, &EvalCtx::new(col_map, old).with_cancel(cancel))?;
        new[position] = coerce_update_column(value, column, table.is_strict())?;
    }
    for column in &table.columns {
        if column.generated_kind.is_some() {
            let value = eval_expr(
                column.generated_expr.as_ref().unwrap(),
                &EvalCtx::new(col_map, &new).with_cancel(cancel),
            )?;
            new[column.position as usize] = coerce_update_column(value, column, table.is_strict())?;
        }
    }
    Ok(new)
}

fn coerce_update_column(
    value: Value,
    column: &crate::types::ColumnDef,
    strict: bool,
) -> Result<Value> {
    if value.is_null() {
        Ok(Value::Null)
    } else {
        coerce_for_column(value, column, strict)
    }
}

fn validate_update(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    table: &TableSchema,
    changed_columns: &[String],
    old: &[Value],
    new: &[Value],
) -> Result<()> {
    let col_map = table.column_map();
    for column in &table.columns {
        if !column.nullable && new[column.position as usize].is_null() {
            return Err(SqlError::NotNullViolation(column.name.clone()));
        }
        if let Some(expr) = &column.check_expr {
            let value = eval_expr(
                expr,
                &EvalCtx::new(col_map, new).with_cancel(wtx.cancel_token()),
            )?;
            if !is_truthy(&value) && !value.is_null() {
                return Err(SqlError::CheckViolation(
                    column.check_name.as_deref().unwrap_or(&column.name).into(),
                ));
            }
        }
    }
    for constraint in &table.check_constraints {
        let value = eval_expr(
            &constraint.expr,
            &EvalCtx::new(col_map, new).with_cancel(wtx.cancel_token()),
        )?;
        if !is_truthy(&value) && !value.is_null() {
            return Err(SqlError::CheckViolation(
                constraint.name.as_deref().unwrap_or(&constraint.sql).into(),
            ));
        }
    }
    let pk_changed = table.pk_indices().iter().any(|&i| !old[i].bit_eq(&new[i]));
    let mut key = Vec::new();
    for fk in &table.foreign_keys {
        let assigned_or_changed = fk.columns.iter().any(|&i| {
            !old[i as usize].bit_eq(&new[i as usize])
                || changed_columns
                    .iter()
                    .any(|name| table.columns[i as usize].name.eq_ignore_ascii_case(name))
        });
        if assigned_or_changed || pk_changed {
            super::fk::check_row_reference(wtx, schema, table, fk, new, &mut key)?;
        }
    }

    Ok(())
}

fn apply_row(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    operation: &Operation<'_>,
    row: &RowChange,
) -> Result<()> {
    let table = operation.table;
    if operation.has_triggers
        && triggers::has_row_triggers(
            schema,
            &table.name,
            TriggerTiming::Before,
            operation.event(),
        )
    {
        // OLD can contain an evaluated default for a field absent on disk.
        // Compare physical rows so checking trigger side effects does not
        // evaluate volatile defaults a second time.
        let before = wtx
            .table_get(table.name.as_bytes(), &row.key)
            .map_err(SqlError::Storage)?;
        triggers::fire_row_triggers(
            wtx,
            schema,
            &table.name,
            TriggerTiming::Before,
            operation.event(),
            Some(row.old.clone()),
            row.new.clone(),
            &table.columns,
        )?;
        let after = wtx
            .table_get(table.name.as_bytes(), &row.key)
            .map_err(SqlError::Storage)?;
        if before.is_none() || before != after {
            return Err(SqlError::Unsupported(
                "a BEFORE trigger cannot modify or delete the row being processed".into(),
            ));
        }
    }
    if let Some(new) = &row.new {
        validate_update(
            wtx,
            schema,
            table,
            &operation.changed_columns,
            &row.old,
            new,
        )?;
    }
    if operation.has_children {
        check_restrict(
            wtx,
            schema,
            table,
            &row.key,
            Some(&row.old),
            row.new.as_deref(),
        )?;
    }
    let old_pk: Vec<Value> = table
        .pk_indices()
        .iter()
        .map(|&i| row.old[i].clone())
        .collect();
    match &row.new {
        None => {
            delete_index_entries(wtx, table, &row.old, &old_pk)?;
            wtx.table_delete(table.name.as_bytes(), &row.key)
                .map_err(SqlError::Storage)?;
        }
        Some(new) => {
            let new_pk: Vec<Value> = table.pk_indices().iter().map(|&i| new[i].clone()).collect();
            let new_key = encode_composite_key(&new_pk);
            if new_key.len() > citadel_core::MAX_KEY_SIZE {
                return Err(SqlError::KeyTooLarge {
                    size: new_key.len(),
                    max: citadel_core::MAX_KEY_SIZE,
                });
            }
            let mut values = Vec::new();
            let bytes = encode_stored_row(table, new, &mut values);
            if bytes.len() > citadel_core::MAX_VALUE_SIZE {
                return Err(SqlError::RowTooLarge {
                    size: bytes.len(),
                    max: citadel_core::MAX_VALUE_SIZE,
                });
            }
            let pk_changed = new_key != row.key;
            let col_map = any_partial_index(table).then(|| table.column_map());
            let actions = table
                .indices
                .iter()
                .map(|index| {
                    partial_idx_update_actions_with_cancel(
                        index,
                        &row.old,
                        new,
                        index_columns_changed(index, &row.old, new, table),
                        pk_changed,
                        col_map,
                        wtx.cancel_token(),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            for (index, &(delete, _)) in table.indices.iter().zip(&actions) {
                if delete {
                    delete_index_entry(wtx, table, index, &row.old, &old_pk)?;
                }
            }
            if pk_changed {
                wtx.table_delete(table.name.as_bytes(), &row.key)
                    .map_err(SqlError::Storage)?;
            }
            let inserted = wtx
                .table_insert(table.name.as_bytes(), &new_key, &bytes)
                .map_err(SqlError::Storage)?;
            if pk_changed && !inserted {
                return Err(SqlError::DuplicateKey);
            }
            for (index, &(_, insert)) in table.indices.iter().zip(&actions) {
                if insert {
                    insert_index_entry(wtx, table, index, new, &new_pk)?;
                }
            }
        }
    }
    Ok(())
}

fn referenced_key(
    table: &TableSchema,
    fk: &ForeignKeySchemaEntry,
    fallback: &[u8],
    row: Option<&[Value]>,
) -> Result<Vec<u8>> {
    let Some(row) = row else {
        return Ok(fallback.to_vec());
    };
    let values = fk
        .referred_columns
        .iter()
        .map(|name| {
            table
                .column_index(name)
                .map(|i| row[i].clone())
                .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(encode_composite_key(&values))
}

fn reference_changed(
    table: &TableSchema,
    fk: &ForeignKeySchemaEntry,
    old: Option<&[Value]>,
    new: Option<&[Value]>,
) -> Result<bool> {
    let Some(old) = old else {
        return Ok(true);
    };
    let reference = super::fk::ReferenceKey::new(table, fk)?;
    let mut changed = new.is_none();
    for (position, name) in fk.referred_columns.iter().enumerate() {
        let i = table
            .column_index(name)
            .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))?;
        // MATCH SIMPLE: a NULL in the referenced key never owns child rows.
        if old[i].is_null() {
            return Ok(false);
        }
        changed |= new.is_some_and(|row| !reference.value_equal(position, &old[i], &row[i]));
    }
    Ok(changed)
}

fn check_restrict(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    table: &TableSchema,
    key: &[u8],
    old: Option<&[Value]>,
    new: Option<&[Value]>,
) -> Result<()> {
    for (child_name, fk) in schema.child_fks_for(&table.name) {
        let action = if new.is_some() {
            fk.on_update
        } else {
            fk.on_delete
        };
        if action != ReferentialAction::Restrict || !reference_changed(table, fk, old, new)? {
            continue;
        }
        let child = schema
            .get(child_name)
            .ok_or_else(|| SqlError::TableNotFound(child_name.into()))?;
        let reference_key = super::fk::ReferenceKey::new(table, fk)?;
        let index = find_cascading_idx(child, fk, &reference_key).ok_or_else(|| {
            SqlError::ForeignKeyViolation(format!(
                "no index backs the foreign key on '{child_name}'"
            ))
        })?;
        let reference = referenced_key(table, fk, key, old)?;
        let mut hits = FkChildHits::default();
        scan_fk_index_keys(wtx, child, index, &reference_key, &reference, &mut hits)?;
        // Deleting a self-referencing row removes its own reference.
        let references_other_row = hits
            .entries()
            .any(|(_, pk)| new.is_some() || child.name != table.name || pk != key);
        if references_other_row {
            return Err(SqlError::ForeignKeyViolation(format!(
                "cannot change '{}': referenced by '{child_name}'",
                table.name
            )));
        }
    }
    Ok(())
}

fn run<'a>(
    wtx: &mut WriteTxn<'_>,
    schema: &'a SchemaManager,
    operation: Operation<'a>,
    no_action_checks: &mut Vec<NoActionCheck<'a>>,
) -> Result<MutationResult> {
    let mut work = vec![Work::Row(operation)];
    let mut completed = None;
    while let Some(task) = work.pop() {
        check_cancel(wtx.cancel_token())?;
        match task {
            Work::Row(mut operation) => {
                if let Some(mut row) = operation.rows.next() {
                    // Earlier FK actions or triggers may have changed another
                    // selected row. Keep its current values and index entries.
                    if operation.refresh_rows {
                        let Some(value) = wtx
                            .table_get(operation.table.name.as_bytes(), &row.key)
                            .map_err(SqlError::Storage)?
                        else {
                            work.push(Work::Row(operation));
                            continue;
                        };
                        let current = decode_full_row_with_cancel(
                            operation.table,
                            &row.key,
                            &value,
                            wtx.cancel_token(),
                        )?;
                        if current.len() != row.old.len()
                            || current.iter().zip(&row.old).any(|(a, b)| !a.bit_eq(b))
                        {
                            row.new = match &operation.mutation {
                                Mutation::Delete => None,
                                Mutation::Update(assignments) => Some(evaluate_update(
                                    operation.table,
                                    assignments,
                                    &current,
                                    wtx.cancel_token(),
                                )?),
                                Mutation::EvaluatedUpdate => {
                                    return Err(SqlError::Unsupported(
                                        "conflict row changed after expression evaluation".into(),
                                    ))
                                }
                            };
                            row.old = current;
                        }
                    }
                    apply_row(wtx, schema, &operation, &row)?;
                    operation.count += 1;
                    let parent = operation.has_children.then(|| ParentChange {
                        table: operation.table,
                        key: row.key.clone(),
                        old: Some(row.old.clone()),
                        new: row.new.clone(),
                    });
                    work.push(Work::AfterRow(operation, row));
                    if let Some(parent) = parent {
                        work.push(Work::ForeignKeys(parent, 0));
                    }
                } else {
                    if operation.statement_triggers {
                        operation.old_rows.truncate(operation.count as usize);
                        operation.new_rows.truncate(operation.count as usize);
                        triggers::fire_statement_triggers(
                            wtx,
                            schema,
                            &operation.table.name,
                            TriggerTiming::After,
                            operation.event(),
                            &operation.table.columns,
                            &operation.old_rows,
                            &operation.new_rows,
                        )?;
                    }
                    if operation.root {
                        completed = Some(MutationResult {
                            count: operation.count,
                            rows: operation.returning_rows,
                        });
                    }
                }
            }
            Work::AfterRow(mut operation, mut row) => {
                // A self-referencing cascade can update this row again. Expose
                // that stored result to AFTER triggers, transitions and RETURNING.
                if operation.has_children
                    && (operation.capture_rows
                        || operation.statement_triggers
                        || operation.has_triggers)
                {
                    if let Some(new) = &row.new {
                        let key = encode_composite_key(
                            &operation
                                .table
                                .pk_indices()
                                .iter()
                                .map(|&i| new[i].clone())
                                .collect::<Vec<_>>(),
                        );
                        if let Some(value) = wtx
                            .table_get(operation.table.name.as_bytes(), &key)
                            .map_err(SqlError::Storage)?
                        {
                            row.new = Some(decode_full_row_with_cancel(
                                operation.table,
                                &key,
                                &value,
                                wtx.cancel_token(),
                            )?);
                        }
                    }
                }
                if operation.statement_triggers {
                    let position = operation.count as usize - 1;
                    operation.old_rows[position] = row.old.clone();
                    if let Some(new) = &row.new {
                        operation.new_rows[position] = new.clone();
                    }
                }
                if operation.has_triggers {
                    triggers::fire_row_triggers(
                        wtx,
                        schema,
                        &operation.table.name,
                        TriggerTiming::After,
                        operation.event(),
                        Some(row.old.clone()),
                        row.new.clone(),
                        &operation.table.columns,
                    )?;
                }
                if operation.capture_rows {
                    operation.returning_rows.push((Some(row.old), row.new));
                }
                work.push(Work::Row(operation));
            }
            Work::ForeignKeys(parent, position) => {
                let children = schema.child_fks_for(&parent.table.name);
                let Some(&(child_name, fk)) = children.get(position) else {
                    continue;
                };
                if !reference_changed(
                    parent.table,
                    fk,
                    parent.old.as_deref(),
                    parent.new.as_deref(),
                )? {
                    work.push(Work::ForeignKeys(parent, position + 1));
                    continue;
                }
                let child = schema
                    .get(child_name)
                    .ok_or_else(|| SqlError::TableNotFound(child_name.into()))?;
                let reference_key = super::fk::ReferenceKey::new(parent.table, fk)?;
                let index = find_cascading_idx(child, fk, &reference_key).ok_or_else(|| {
                    SqlError::ForeignKeyViolation(format!(
                        "no index backs the foreign key on '{child_name}'"
                    ))
                })?;
                let key = referenced_key(parent.table, fk, &parent.key, parent.old.as_deref())?;
                let mut hits = FkChildHits::default();
                scan_fk_index_keys(wtx, child, index, &reference_key, &key, &mut hits)?;
                let action = if parent.new.is_some() {
                    fk.on_update
                } else {
                    fk.on_delete
                };
                if hits.is_empty() {
                    work.push(Work::ForeignKeys(parent, position + 1));
                    continue;
                }
                if action == ReferentialAction::NoAction {
                    no_action_checks.push(NoActionCheck {
                        child,
                        fk,
                        reference: reference_key,
                        key,
                    });
                    work.push(Work::ForeignKeys(parent, position + 1));
                    continue;
                }
                if action == ReferentialAction::Restrict {
                    return Err(SqlError::ForeignKeyViolation(format!(
                        "cannot change '{}': referenced by '{child_name}'",
                        parent.table.name
                    )));
                }
                if action == ReferentialAction::Cascade
                    && parent.new.is_none()
                    && child.indices.len() == 1
                    && schema.child_fks_for(&child.name).is_empty()
                    && !triggers::has_delete_triggers(schema, &child.name)
                {
                    schema.mark_dml(&child.name);
                    let index_table = TableSchema::index_table_name(&child.name, &index.name);
                    // Leaf children have no observable row actions. Their sole
                    // backing index supplies both encoded keys without decoding.
                    if !try_truncate_leaf_children(wtx, child, index, &index_table, &hits)? {
                        for (index_key, key) in hits.entries() {
                            check_cancel(wtx.cancel_token())?;
                            wtx.table_delete(&index_table, index_key)
                                .map_err(SqlError::Storage)?;
                            wtx.table_delete(child.name.as_bytes(), key)
                                .map_err(SqlError::Storage)?;
                        }
                    }
                    work.push(Work::ForeignKeys(parent, position + 1));
                    continue;
                }
                let rows = fetch_child_rows(wtx, child, &hits)?;
                let assignments = if action == ReferentialAction::Cascade && parent.new.is_none() {
                    None
                } else {
                    Some(
                        fk.columns
                            .iter()
                            .enumerate()
                            .map(|(i, &column)| {
                                let definition = &child.columns[column as usize];
                                let expression = match action {
                                    ReferentialAction::SetNull => Expr::Literal(Value::Null),
                                    ReferentialAction::SetDefault => definition
                                        .default_expr
                                        .clone()
                                        .unwrap_or(Expr::Literal(Value::Null)),
                                    ReferentialAction::Cascade => {
                                        let name = &fk.referred_columns[i];
                                        let p =
                                            parent.table.column_index(name).ok_or_else(|| {
                                                SqlError::ColumnNotFound(name.clone())
                                            })?;
                                        Expr::Literal(parent.new.as_ref().unwrap()[p].clone())
                                    }
                                    _ => unreachable!(),
                                };
                                Ok((definition.name.clone(), expression))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )
                };
                let operation = prepare_operation(
                    wtx,
                    schema,
                    child,
                    assignments.as_deref(),
                    false,
                    rows,
                    false,
                )?;
                work.push(Work::ForeignKeys(parent, position + 1));
                work.push(Work::Row(operation));
            }
        }
    }
    completed.ok_or_else(|| SqlError::Unsupported("row mutation did not complete".into()))
}

fn check_no_action(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    checks: Vec<NoActionCheck<'_>>,
) -> Result<()> {
    for NoActionCheck {
        child,
        fk,
        reference: reference_key,
        key,
    } in checks
    {
        let index = find_cascading_idx(child, fk, &reference_key).ok_or_else(|| {
            SqlError::ForeignKeyViolation(format!(
                "no index backs the foreign key on '{}'",
                child.name
            ))
        })?;
        let mut hits = FkChildHits::default();
        scan_fk_index_keys(wtx, child, index, &reference_key, &key, &mut hits)?;
        let mut reference_key = Vec::new();
        for (_, row) in fetch_child_rows(wtx, child, &hits)? {
            super::fk::check_row_reference(wtx, schema, child, fk, &row, &mut reference_key)?;
        }
    }
    Ok(())
}

/// The caller has excluded child triggers, descendants and additional indexes.
/// Keep the original hit scan and its budget charges before proving coverage.
fn try_truncate_leaf_children(
    wtx: &mut WriteTxn<'_>,
    child: &TableSchema,
    index: &IndexDef,
    index_table: &[u8],
    hits: &FkChildHits,
) -> Result<bool> {
    // A non-NULL UNIQUE prefix has at most one child. For a singleton, two
    // keyed deletes already do no more tree mutations than two truncations.
    if index.unique || hits.len() < 2 {
        return Ok(false);
    }
    let count = hits.len() as u64;
    if wtx.table_entry_count(child.name.as_bytes())? != count
        || wtx.table_entry_count(index_table)? != count
    {
        return Ok(false);
    }
    // Nonunique index hits have distinct complete primary-key suffixes. Their
    // existence plus the physical entry count proves every base row is covered;
    // tombstones or unrelated rows make this proof fail. Checking existence also
    // prevents a dangling index entry from standing in for an unindexed row.
    for (_, key) in hits.entries() {
        if !wtx.table_contains_key(child.name.as_bytes(), key)? {
            return Ok(false);
        }
    }
    // Preserve the leaf-child lane's index-before-base mutation order. The
    // statement guard makes any failure after the first mutation uncommittable.
    wtx.table_truncate(index_table)?;
    wtx.table_truncate(child.name.as_bytes())?;
    Ok(true)
}

#[cfg(test)]
#[path = "row_mutation_tests.rs"]
mod tests;
