use citadel::Database;

use crate::error::{Result, SqlError};
use crate::parser::{
    CreateTriggerStmt, DropTriggerStmt, Statement, TriggerEvent, TriggerGranularity, TriggerTiming,
};
use crate::schema::SchemaManager;
use crate::types::{ExecutionResult, TriggerDef, Value};

pub(super) const MAX_TRIGGER_DEPTH: usize = 32;

thread_local! {
    static TRIGGER_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn enter_trigger() -> Result<TriggerGuard> {
    TRIGGER_DEPTH.with(|c| {
        let d = c.get();
        if d >= MAX_TRIGGER_DEPTH {
            return Err(SqlError::Unsupported(format!(
                "trigger recursion limit ({MAX_TRIGGER_DEPTH}) exceeded"
            )));
        }
        c.set(d + 1);
        Ok(TriggerGuard)
    })
}

struct TriggerGuard;
impl Drop for TriggerGuard {
    fn drop(&mut self) {
        TRIGGER_DEPTH.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

pub(super) fn exec_create_trigger(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &CreateTriggerStmt,
) -> Result<ExecutionResult> {
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let result = exec_create_trigger_in_txn(&mut wtx, schema, stmt)?;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(result)
}

pub(super) fn exec_create_trigger_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &CreateTriggerStmt,
) -> Result<ExecutionResult> {
    validate_trigger_shape(stmt, schema)?;

    if schema.find_trigger(&stmt.name).is_some() {
        if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::Unsupported(format!(
            "trigger '{}' already exists",
            stmt.name
        )));
    }

    let target_storage = match schema.get(&stmt.target) {
        Some(ts) => ts.name.clone(),
        None => match schema.get_view(&stmt.target.to_ascii_lowercase()) {
            Some(_) => stmt.target.to_ascii_lowercase(),
            None => return Err(SqlError::TableNotFound(stmt.target.clone())),
        },
    };

    let td = TriggerDef {
        name: stmt.name.to_ascii_lowercase(),
        timing: stmt.timing,
        events: stmt.events.clone(),
        target: target_storage,
        granularity: stmt.granularity,
        referencing: stmt.referencing.clone(),
        when_sql: stmt.when_sql.clone(),
        body_sql: stmt.body_sql.clone(),
        enabled: true,
        created_at_micros: crate::datetime::txn_or_clock_micros(),
    };
    SchemaManager::save_trigger(wtx, &td)?;
    schema.register_trigger(td);
    Ok(ExecutionResult::Ok)
}

pub(super) fn exec_drop_trigger(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &DropTriggerStmt,
) -> Result<ExecutionResult> {
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let result = exec_drop_trigger_in_txn(&mut wtx, schema, stmt)?;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(result)
}

pub(super) fn exec_drop_trigger_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &DropTriggerStmt,
) -> Result<ExecutionResult> {
    if schema.find_trigger(&stmt.name).is_none() {
        if stmt.if_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::Unsupported(format!(
            "trigger '{}' not found",
            stmt.name
        )));
    }
    SchemaManager::delete_trigger(wtx, &stmt.name)?;
    schema.remove_trigger(&stmt.name);
    Ok(ExecutionResult::Ok)
}

fn validate_trigger_shape(stmt: &CreateTriggerStmt, schema: &SchemaManager) -> Result<()> {
    let target_is_view = schema.get_view(&stmt.target.to_ascii_lowercase()).is_some();
    let target_is_table = schema.get(&stmt.target).is_some();

    match stmt.timing {
        TriggerTiming::InsteadOf => {
            if !target_is_view {
                return Err(SqlError::Unsupported(
                    "INSTEAD OF triggers can only be created on views".into(),
                ));
            }
            if stmt.granularity == TriggerGranularity::ForEachStatement {
                return Err(SqlError::Unsupported(
                    "INSTEAD OF triggers must be FOR EACH ROW".into(),
                ));
            }
        }
        TriggerTiming::Before | TriggerTiming::After => {
            if !target_is_table && !target_is_view {
                return Err(SqlError::TableNotFound(stmt.target.clone()));
            }
        }
    }

    for ev in &stmt.events {
        match ev {
            TriggerEvent::Insert | TriggerEvent::Update(_) | TriggerEvent::Delete => {}
        }
    }

    for body_stmt in &stmt.body {
        match body_stmt {
            Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::Select(_) => {}
            other => {
                return Err(SqlError::Unsupported(format!(
                    "trigger body may only contain INSERT/UPDATE/DELETE/SELECT, got: {other:?}"
                )));
            }
        }
    }

    Ok(())
}

#[derive(Default, Clone)]
pub(crate) struct TriggerBindings {
    pub old_row: Option<Vec<Value>>,
    pub new_row: Option<Vec<Value>>,
    pub old_columns: Vec<crate::types::ColumnDef>,
    pub new_columns: Vec<crate::types::ColumnDef>,
}

impl TriggerBindings {
    pub fn for_row_insert(new: Vec<Value>, cols: Vec<crate::types::ColumnDef>) -> Self {
        Self {
            old_row: None,
            new_row: Some(new),
            old_columns: vec![],
            new_columns: cols,
        }
    }
    pub fn for_row_update(
        old: Vec<Value>,
        new: Vec<Value>,
        cols: Vec<crate::types::ColumnDef>,
    ) -> Self {
        Self {
            old_row: Some(old),
            new_row: Some(new),
            old_columns: cols.clone(),
            new_columns: cols,
        }
    }
    pub fn for_row_delete(old: Vec<Value>, cols: Vec<crate::types::ColumnDef>) -> Self {
        Self {
            old_row: Some(old),
            new_row: None,
            old_columns: cols,
            new_columns: vec![],
        }
    }
}

thread_local! {
    static CURRENT_BINDINGS: std::cell::RefCell<Option<TriggerBindings>> =
        const { std::cell::RefCell::new(None) };
}

/// Restores any prior bindings on drop so nested triggers stack correctly.
fn with_bindings<R>(bindings: TriggerBindings, f: impl FnOnce() -> R) -> R {
    let prev = CURRENT_BINDINGS.with(|cell| cell.replace(Some(bindings)));
    struct Restore(Option<TriggerBindings>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURRENT_BINDINGS.with(|cell| *cell.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(prev);
    f()
}

pub(crate) fn current_bindings() -> Option<TriggerBindings> {
    CURRENT_BINDINGS.with(|cell| cell.borrow().clone())
}

/// `UPDATE OF col_list` narrows: fires only when an affected column appears in the list.
fn event_matches(trigger_ev: &TriggerEvent, fired: FireEvent<'_>) -> bool {
    match (trigger_ev, fired) {
        (TriggerEvent::Insert, FireEvent::Insert) => true,
        (TriggerEvent::Delete, FireEvent::Delete) => true,
        (TriggerEvent::Update(cols), FireEvent::Update { changed_columns }) => {
            if cols.is_empty() {
                true
            } else {
                cols.iter()
                    .any(|c| changed_columns.iter().any(|cc| cc.eq_ignore_ascii_case(c)))
            }
        }
        _ => false,
    }
}

#[derive(Clone, Copy)]
pub(crate) enum FireEvent<'a> {
    Insert,
    Update { changed_columns: &'a [String] },
    Delete,
}

/// Synthetic ColumnDef list for a view's OLD/NEW binding — types default to Text since
/// OLD/NEW lookups return the underlying `Value` directly and never consult them.
pub(crate) fn view_columns_from_aliases(aliases: &[String]) -> Vec<crate::types::ColumnDef> {
    aliases
        .iter()
        .enumerate()
        .map(|(i, name)| crate::types::ColumnDef {
            name: name.to_ascii_lowercase(),
            data_type: crate::types::DataType::Text,
            nullable: true,
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
        })
        .collect()
}

pub(crate) fn has_instead_of(schema: &SchemaManager, target: &str, event: FireEvent<'_>) -> bool {
    schema.triggers_for(target).iter().any(|t| {
        t.enabled
            && t.timing == TriggerTiming::InsteadOf
            && t.granularity == TriggerGranularity::ForEachRow
            && t.events.iter().any(|e| event_matches(e, event))
    })
}

/// Returns `Ok(true)` if an INSTEAD OF trigger handled the event — caller skips real DML.
#[allow(clippy::too_many_arguments)]
pub(crate) fn fire_row_triggers(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    target: &str,
    timing: TriggerTiming,
    event: FireEvent<'_>,
    old_row: Option<Vec<Value>>,
    new_row: Option<Vec<Value>>,
    table_cols: &[crate::types::ColumnDef],
) -> Result<bool> {
    let _guard = enter_trigger()?;
    let candidates: Vec<TriggerDef> = schema
        .triggers_for(target)
        .iter()
        .filter(|t| {
            t.enabled
                && t.timing == timing
                && t.granularity == TriggerGranularity::ForEachRow
                && t.events.iter().any(|e| event_matches(e, event))
        })
        .cloned()
        .collect();

    if candidates.is_empty() {
        return Ok(false);
    }

    let bindings = match (old_row.clone(), new_row.clone()) {
        (None, Some(n)) => TriggerBindings::for_row_insert(n, table_cols.to_vec()),
        (Some(o), Some(n)) => TriggerBindings::for_row_update(o, n, table_cols.to_vec()),
        (Some(o), None) => TriggerBindings::for_row_delete(o, table_cols.to_vec()),
        (None, None) => TriggerBindings::default(),
    };

    let mut handled_instead_of = false;
    for td in &candidates {
        let pre_when_pass = match &td.when_sql {
            None => true,
            Some(when_sql) => evaluate_when(when_sql, &bindings)?,
        };
        if !pre_when_pass {
            continue;
        }
        with_bindings(bindings.clone(), || -> Result<()> {
            execute_trigger_body(wtx, schema, &td.body_sql)
        })?;
        if td.timing == TriggerTiming::InsteadOf {
            handled_instead_of = true;
        }
    }
    Ok(handled_instead_of)
}

fn evaluate_when(when_sql: &str, bindings: &TriggerBindings) -> Result<bool> {
    let when_expr = crate::parser::parse_sql_expr(when_sql)?;
    let cols_for_eval = if !bindings.new_columns.is_empty() {
        &bindings.new_columns
    } else {
        &bindings.old_columns
    };
    let col_map = crate::eval::ColumnMap::new(cols_for_eval);
    let row_for_default: &[Value] = bindings
        .new_row
        .as_deref()
        .or(bindings.old_row.as_deref())
        .unwrap_or(&[]);
    let ctx = crate::eval::EvalCtx::with_old_new(
        &col_map,
        row_for_default,
        bindings.old_row.as_deref(),
        bindings.new_row.as_deref(),
    );
    let val = crate::eval::eval_expr(&when_expr, &ctx)?;
    Ok(crate::eval::is_truthy(&val))
}

/// `REFERENCING NEW/OLD TABLE AS` is materialized as ephemeral real tables in `wtx`
/// so any scan path resolves them; aliases live on a per-thread stack for the body's run.
#[allow(clippy::too_many_arguments)]
pub(crate) fn fire_statement_triggers(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    target: &str,
    timing: TriggerTiming,
    event: FireEvent<'_>,
    table_cols: &[crate::types::ColumnDef],
    old_rows: &[Vec<Value>],
    new_rows: &[Vec<Value>],
) -> Result<()> {
    let candidates: Vec<TriggerDef> = schema
        .triggers_for(target)
        .iter()
        .filter(|t| {
            t.enabled
                && t.timing == timing
                && t.granularity == TriggerGranularity::ForEachStatement
                && t.events.iter().any(|e| event_matches(e, event))
        })
        .cloned()
        .collect();

    if candidates.is_empty() {
        return Ok(());
    }

    for td in &candidates {
        let _guard = enter_trigger()?;
        let mut storages: Vec<Vec<u8>> = Vec::new();
        let mut aliases: rustc_hash::FxHashMap<String, String> = rustc_hash::FxHashMap::default();
        if let Some(ref tt) = td.referencing {
            if let Some(new_alias) = tt.new_table_alias.as_ref() {
                let storage = format!(
                    "__trans_{}_{}_new",
                    td.name.to_ascii_lowercase(),
                    transition_seq()
                );
                provision_transition_table(wtx, schema, &storage, target, table_cols, new_rows)?;
                aliases.insert(new_alias.to_ascii_lowercase(), storage.clone());
                storages.push(storage.into_bytes());
            }
            if let Some(old_alias) = tt.old_table_alias.as_ref() {
                let storage = format!(
                    "__trans_{}_{}_old",
                    td.name.to_ascii_lowercase(),
                    transition_seq()
                );
                provision_transition_table(wtx, schema, &storage, target, table_cols, old_rows)?;
                aliases.insert(old_alias.to_ascii_lowercase(), storage.clone());
                storages.push(storage.into_bytes());
            }
        }
        let _trans_guard = if aliases.is_empty() {
            None
        } else {
            Some(crate::schema::push_transition_tables(aliases.clone()))
        };

        let body_res = execute_trigger_body(wtx, schema, &td.body_sql);

        drop(_trans_guard);
        for storage in &storages {
            let storage_str = std::str::from_utf8(storage).map_err(|_| {
                SqlError::Unsupported("invalid utf8 in transition storage name".into())
            })?;
            schema.unregister_transition_schema(storage_str);
            let _ = wtx.drop_table(storage);
        }
        body_res?;
    }
    Ok(())
}

fn transition_seq() -> u64 {
    thread_local! {
        static SEQ: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }
    SEQ.with(|c| {
        let v = c.get() + 1;
        c.set(v);
        v
    })
}

fn provision_transition_table(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    storage_name: &str,
    target_name: &str,
    table_cols: &[crate::types::ColumnDef],
    rows: &[Vec<Value>],
) -> Result<()> {
    wtx.create_table(storage_name.as_bytes())
        .map_err(SqlError::Storage)?;
    let ts = match schema.get(target_name) {
        Some(t) => crate::types::TableSchema::new(
            storage_name.to_string(),
            t.columns.clone(),
            vec![],
            vec![],
            vec![],
            vec![],
        ),
        None => crate::types::TableSchema::new(
            storage_name.to_string(),
            table_cols.to_vec(),
            vec![],
            vec![],
            vec![],
            vec![],
        ),
    };
    schema.register_transition_schema(storage_name.to_string(), ts);

    // Bypass the regular INSERT path: no constraints, no triggers, row index = storage key.
    let mut key_buf = Vec::with_capacity(8);
    let mut value_buf = Vec::with_capacity(256);
    for (i, row) in rows.iter().enumerate() {
        crate::encoding::encode_int_key_into(i as i64, &mut key_buf);
        crate::encoding::encode_row_into(row, &mut value_buf);
        wtx.table_insert(storage_name.as_bytes(), &key_buf, &value_buf)
            .map_err(SqlError::Storage)?;
        key_buf.clear();
        value_buf.clear();
    }
    Ok(())
}

fn execute_trigger_body(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    body_sql: &str,
) -> Result<()> {
    let stmts = crate::parser::parse_sql_multi(body_sql)?;
    for stmt in stmts {
        match &stmt {
            Statement::Insert(ins) => {
                super::dml::exec_insert_in_txn(wtx, schema, ins, &[])?;
            }
            Statement::Update(upd) => {
                super::write::exec_update_in_txn(wtx, schema, upd)?;
            }
            Statement::Delete(del) => {
                super::write::exec_delete_in_txn(wtx, schema, del)?;
            }
            Statement::Select(sq) => {
                super::cte::exec_select_query_in_txn(wtx, schema, sq)?;
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "trigger body cannot execute {other:?}"
                )));
            }
        }
    }
    Ok(())
}
