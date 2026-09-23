//! SQL executor: DDL and DML operations.

mod aggregate;
pub(crate) mod ann_persist;
mod ann_topk;
pub(crate) mod compile;
pub(crate) mod constraint_indexes;
mod correlated;
mod cte;
mod ddl;
mod dml;
mod explain;
mod fk;
pub(crate) mod helpers;
mod index_build;
mod insert_copy;
mod join;
pub(crate) mod matviews;
mod result_cache;
mod row_mutation;
mod scan;
mod select;
mod topk;
pub(crate) mod triggers;
mod view;
mod window;
pub(crate) mod write;
pub use ann_persist::AnnSegmentInfo;
pub use ann_topk::AnnIndexSource;
pub(crate) use ann_topk::{ann_cache_status, commit_with_ann_publication, persist_ann_index};
pub(crate) use compile::{compile, CompiledPlan};
use cte::*;
use ddl::*;
use dml::*;
use explain::*;
use join::*;
use scan::*;
use select::*;
use view::*;
use window::*;
use write::*;

use citadel::Database;
use citadel_txn::manager::ScanMeasurement;
use citadel_txn::read_txn::ReadTxn;
use rustc_hash::FxHashMap;
use std::sync::Arc;

use crate::error::{Result, SqlError};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

pub(crate) fn stmt_mutates(stmt: &Statement) -> bool {
    match stmt {
        Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::Truncate(_)
        | Statement::CreateTable(_)
        | Statement::DropTable(_)
        | Statement::AlterTable(_)
        | Statement::CreateIndex(_)
        | Statement::DropIndex(_)
        | Statement::CreateView(_)
        | Statement::DropView(_)
        | Statement::CreateTrigger(_)
        | Statement::DropTrigger(_)
        | Statement::CreateMaterializedView(_)
        | Statement::RefreshMaterializedView(_)
        | Statement::DropMaterializedView(_) => true,
        Statement::Select(query) => {
            query.ctes.iter().any(|cte| query_body_mutates(&cte.body))
                || query_body_mutates(&query.body)
        }
        Statement::Explain {
            inner,
            analyze: true,
        } => stmt_mutates(inner),
        _ => false,
    }
}

pub(crate) fn stmt_mutates_schema(stmt: &Statement) -> bool {
    match stmt {
        Statement::CreateTable(_)
        | Statement::DropTable(_)
        | Statement::AlterTable(_)
        | Statement::CreateIndex(_)
        | Statement::DropIndex(_)
        | Statement::CreateView(_)
        | Statement::DropView(_)
        | Statement::CreateTrigger(_)
        | Statement::DropTrigger(_)
        | Statement::CreateMaterializedView(_)
        | Statement::RefreshMaterializedView(_)
        | Statement::DropMaterializedView(_) => true,
        Statement::Explain {
            inner,
            analyze: true,
        } => stmt_mutates_schema(inner),
        _ => false,
    }
}

pub(crate) fn reject_legacy_volatile_schema(schema: &SchemaManager) -> Result<()> {
    match schema.legacy_volatile_definition() {
        Some(definition) => Err(SqlError::Unsupported(format!(
            "legacy catalog recovery required: {definition}; only DROP INDEX, DROP TABLE, or ALTER TABLE DROP COLUMN is allowed until every volatile persisted expression is removed"
        ))),
        None => Ok(()),
    }
}

fn legacy_recovery_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::DropIndex(_)
            | Statement::DropTable(_)
            | Statement::Commit
            | Statement::Rollback
            // Connection resolves this and permits only an evaluation-equivalent
            // no-op while recovery mode is active. Direct executor callers have
            // no session state and reject it as unsupported later.
            | Statement::SetTimezone { .. }
    ) || matches!(
        stmt,
        Statement::AlterTable(alter)
            if matches!(&alter.op, AlterTableOp::DropColumn { .. })
    )
}

pub(crate) fn guard_legacy_volatile_schema(schema: &SchemaManager, stmt: &Statement) -> Result<()> {
    if legacy_recovery_statement(stmt) {
        Ok(())
    } else {
        reject_legacy_volatile_schema(schema)
    }
}

fn query_body_mutates(body: &QueryBody) -> bool {
    match body {
        QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_) => true,
        QueryBody::Compound(compound) => {
            query_body_mutates(&compound.left) || query_body_mutates(&compound.right)
        }
        QueryBody::Select(_) => false,
    }
}

/// A materialized CTE, derived table or view: its rows, plus each column's collation.
/// `QueryResult` is public API and carries names only, so collations ride here; a set
/// operation needs them to decide when two rows are the same row.
pub(super) struct CteRows {
    pub(super) result: QueryResult,
    pub(super) collations: Vec<Collation>,
}

impl CteRows {
    pub(super) fn new(result: QueryResult, collations: Vec<Collation>) -> Self {
        Self { result, collations }
    }

    /// Rows whose columns carry no collation of their own: a virtual table, a table
    /// function, or a RETURNING clause.
    pub(super) fn binary(result: QueryResult) -> Self {
        let collations = vec![Collation::Binary; result.columns.len()];
        Self::new(result, collations)
    }

    /// A relation's column names and collations with no rows. Enough to resolve what a
    /// reference to it collates as, which is all the collation resolver reads.
    pub(super) fn shape(columns: Vec<String>, collations: Vec<Collation>) -> Self {
        Self::new(
            QueryResult {
                columns,
                rows: Vec::new(),
            },
            collations,
        )
    }

    /// Replace the rows, keeping the collations - the columns they were projected from have
    /// not changed. Used by a recursive CTE, which refills itself each iteration.
    pub(super) fn with_rows(&self, rows: Vec<Vec<Value>>) -> Self {
        Self::new(
            QueryResult {
                columns: self.result.columns.clone(),
                rows,
            },
            self.collations.clone(),
        )
    }

    pub(super) fn collation_at(&self, idx: usize) -> Collation {
        self.collations
            .get(idx)
            .copied()
            .unwrap_or(Collation::Binary)
    }

    /// Store immutable materialized rows in a context without making every
    /// derived-query or nested-CTE context clone copy the complete row set.
    pub(super) fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }
}

#[cfg(test)]
thread_local! {
    /// Deterministic cancellation injection for CTE row materialization. These
    /// loops have no callback or I/O boundary a test could otherwise use to
    /// trip the token after work has started.
    static CANCEL_ON_NTH_CTE_ROW: std::cell::RefCell<Option<(citadel::CancelToken, usize)>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) struct CancelOnNthCteRowGuard {
    previous: Option<(citadel::CancelToken, usize)>,
}

#[cfg(test)]
impl Drop for CancelOnNthCteRowGuard {
    fn drop(&mut self) {
        CANCEL_ON_NTH_CTE_ROW.with(|slot| {
            *slot.borrow_mut() = self.previous.take();
        });
    }
}

#[cfg(test)]
pub(super) fn cancel_on_nth_cte_row(
    token: citadel::CancelToken,
    nth: usize,
) -> CancelOnNthCteRowGuard {
    assert!(nth > 0);
    let previous = CANCEL_ON_NTH_CTE_ROW.with(|slot| slot.borrow_mut().replace((token, nth)));
    CancelOnNthCteRowGuard { previous }
}

#[inline]
fn check_cte_cancel_at(cancel: Option<&citadel::CancelToken>, row_idx: usize) -> Result<()> {
    #[cfg(test)]
    CANCEL_ON_NTH_CTE_ROW.with(|slot| {
        let fire = {
            let mut armed = slot.borrow_mut();
            match armed.as_mut() {
                Some((token, remaining)) if *remaining == 1 => {
                    let token = token.clone();
                    *armed = None;
                    Some(token)
                }
                Some((_, remaining)) => {
                    *remaining -= 1;
                    None
                }
                None => None,
            }
        };
        if let Some(token) = fire {
            token.cancel();
        }
    });

    helpers::check_cancel_at(cancel, row_idx)
}

fn clone_cte_rows_with_cancel(
    rows: &[Vec<Value>],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<Value>>> {
    if cancel.is_none() {
        return Ok(rows.to_vec());
    }

    let mut cloned = Vec::with_capacity(rows.len());
    for (row_idx, row) in rows.iter().enumerate() {
        check_cte_cancel_at(cancel, row_idx)?;
        cloned.push(row.clone());
    }
    helpers::check_cancel(cancel)?;
    Ok(cloned)
}

fn extend_cte_rows_with_cancel(
    target: &mut Vec<Vec<Value>>,
    rows: &[Vec<Value>],
    cancel: Option<&citadel::CancelToken>,
) -> Result<()> {
    if cancel.is_none() {
        target.extend_from_slice(rows);
        return Ok(());
    }

    target.reserve(rows.len());
    for (row_idx, row) in rows.iter().enumerate() {
        check_cte_cancel_at(cancel, row_idx)?;
        target.push(row.clone());
    }
    helpers::check_cancel(cancel)
}

type CteContext = FxHashMap<String, Arc<CteRows>>;
type ScanTableFn<'a> = &'a mut dyn FnMut(&str) -> Result<(TableSchema, Vec<Vec<Value>>)>;

/// What the post-scan phases need besides the rows.
#[derive(Clone, Copy)]
pub(super) struct SelectCtx<'a> {
    pub columns: &'a [ColumnDef],
    pub stmt: &'a SelectStmt,
    /// The scan already applied the WHERE clause, so the filter phase is a
    /// no-op rather than a second evaluation of the same predicate.
    pub predicate_applied: bool,
    pub cancel: Option<&'a citadel::CancelToken>,
}

impl<'a> SelectCtx<'a> {
    /// `cancel` is required, not optional: an optional token is how a path ends
    /// up silently uncancellable.
    pub fn new(
        columns: &'a [ColumnDef],
        stmt: &'a SelectStmt,
        cancel: Option<&'a citadel::CancelToken>,
    ) -> Self {
        Self {
            columns,
            stmt,
            predicate_applied: false,
            cancel,
        }
    }

    /// Only the few paths whose scan already applied the WHERE clause set this.
    pub fn predicate_applied(mut self, applied: bool) -> Self {
        self.predicate_applied = applied;
        self
    }

    pub fn cancelled(&self) -> bool {
        self.cancel.is_some_and(|t| t.is_cancelled())
    }

    pub fn check(&self) -> Result<()> {
        if self.cancelled() {
            return Err(SqlError::Storage(citadel_core::Error::Interrupted));
        }
        Ok(())
    }
}

/// Time a span of work and the rows it scanned.
///
/// The operation-local guard excludes scans from other threads/connections and
/// removes itself on every exit, including an execution error.
struct Span {
    started: std::time::Instant,
    scans: ScanMeasurement,
}

impl Span {
    fn open(scans: ScanMeasurement) -> Self {
        Self {
            started: std::time::Instant::now(),
            scans,
        }
    }

    fn close(self, result: &ExecutionResult) -> explain::Measured {
        explain::Measured::observed(self.started.elapsed(), result, self.scans.rows_scanned())
    }
}

/// `Err(Interrupted)` when the token is already tripped. Checked at the door
/// because not every statement reaches a scan loop: a metadata `COUNT(*)`, a
/// cache hit, a point lookup and a `VALUES` insert all commit without one.
fn check_cancelled(token: Option<&citadel::CancelToken>) -> Result<()> {
    match token {
        Some(t) => t.check().map_err(SqlError::Storage),
        None => Ok(()),
    }
}

/// A missing descriptor has no estimate. Every other storage failure remains
/// visible: treating corruption, tampering, or cancellation as an unknown row
/// count would make EXPLAIN falsely look successful.
fn explain_row_count<F>(read: F) -> Result<Option<u64>>
where
    F: FnOnce() -> citadel_core::Result<u64>,
{
    #[cfg(test)]
    CANCEL_ON_NEXT_EXPLAIN_ROW_COUNT.with(|slot| {
        if let Some(token) = slot.borrow_mut().take() {
            token.cancel();
        }
    });

    match read() {
        Ok(rows) => Ok(Some(rows)),
        Err(citadel_core::Error::TableNotFound(_)) => Ok(None),
        Err(error) => Err(SqlError::Storage(error)),
    }
}

#[cfg(test)]
thread_local! {
    /// Trips a statement token after the executor's entry check but before
    /// EXPLAIN reads its first catalog row count.
    static CANCEL_ON_NEXT_EXPLAIN_ROW_COUNT: std::cell::RefCell<Option<citadel::CancelToken>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
struct CancelOnNextExplainRowCountGuard {
    previous: Option<citadel::CancelToken>,
}

#[cfg(test)]
impl Drop for CancelOnNextExplainRowCountGuard {
    fn drop(&mut self) {
        CANCEL_ON_NEXT_EXPLAIN_ROW_COUNT.with(|slot| {
            *slot.borrow_mut() = self.previous.take();
        });
    }
}

#[cfg(test)]
fn cancel_on_next_explain_row_count(
    token: citadel::CancelToken,
) -> CancelOnNextExplainRowCountGuard {
    let previous = CANCEL_ON_NEXT_EXPLAIN_ROW_COUNT.with(|slot| slot.borrow_mut().replace(token));
    CancelOnNextExplainRowCountGuard { previous }
}

#[cfg(test)]
thread_local! {
    static PAUSE_AFTER_EXPLAIN_PLAN: std::cell::RefCell<Option<(
        std::sync::Arc<std::sync::Barrier>,
        std::sync::Arc<std::sync::Barrier>,
    )>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn pause_after_explain_plan() {
    PAUSE_AFTER_EXPLAIN_PLAN.with(|slot| {
        if let Some((planned, resume)) = slot.borrow().as_ref() {
            planned.wait();
            resume.wait();
        }
    });
}

#[cfg(test)]
struct ExplainPlanPauseGuard;

#[cfg(test)]
impl Drop for ExplainPlanPauseGuard {
    fn drop(&mut self) {
        PAUSE_AFTER_EXPLAIN_PLAN.with(|slot| slot.borrow_mut().take());
    }
}

#[cfg(test)]
fn pause_next_explain_after_plan(
    planned: std::sync::Arc<std::sync::Barrier>,
    resume: std::sync::Arc<std::sync::Barrier>,
) -> ExplainPlanPauseGuard {
    PAUSE_AFTER_EXPLAIN_PLAN.with(|slot| {
        assert!(slot.borrow_mut().replace((planned, resume)).is_none());
    });
    ExplainPlanPauseGuard
}

pub fn execute(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &Statement,
    params: &[Value],
) -> Result<ExecutionResult> {
    check_cancelled(db.cancel_token().as_ref())?;
    if matches!(stmt, Statement::CreateIndex(index) if index.concurrently) {
        // The concurrent builder admits its own read and short write views.
        if let Statement::CreateIndex(index) = stmt {
            return exec_create_index(db, schema, index);
        }
    }
    if let Statement::RefreshMaterializedView(refresh) = stmt {
        if refresh.concurrently {
            return matviews::exec_refresh_matview(db, schema, refresh);
        }
    }
    if !stmt_mutates(stmt) {
        let mut rtx = db.begin_read();
        schema.admit_read(db, &mut rtx)?;
        return execute_with_admitted_read(&mut rtx, schema, stmt, params);
    }
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let admission_snapshot = schema.admit_write(db, &mut wtx)?;
    let mut schema_snapshot =
        admission_snapshot.or_else(|| stmt_mutates_schema(stmt).then(|| schema.save_snapshot()));
    let mut dml_snapshot = Some(schema.save_dml_snapshot());
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let result = execute_in_admitted_txn(&mut wtx, schema, stmt, params)?;
        helpers::drain_deferred_fk_checks(&mut wtx, schema)?;
        commit_with_ann_publication(wtx, schema)?;
        Ok(result)
    }));
    if !matches!(&outcome, Ok(Ok(_))) {
        if let Some(snapshot) = schema_snapshot.take() {
            schema.restore_snapshot(snapshot);
        } else if let Some(snapshot) = dml_snapshot.take() {
            schema.restore_dml_snapshot(snapshot);
        }
    }
    match outcome {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

/// Execute against the caller's read snapshot. A stale immutable catalog is
/// rejected before evaluation; Connection admits it from that same snapshot.
pub fn execute_with_read(
    rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &Statement,
    params: &[Value],
) -> Result<ExecutionResult> {
    check_cancelled(rtx.cancel_token())?;
    schema.validate_read_catalog(rtx)?;
    execute_with_admitted_read(rtx, schema, stmt, params)
}

pub(crate) fn execute_with_admitted_read(
    rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &Statement,
    _params: &[Value],
) -> Result<ExecutionResult> {
    guard_legacy_volatile_schema(schema, stmt)?;
    check_cancelled(rtx.cancel_token())?;
    match stmt {
        Statement::Select(sq) => cte::exec_select_query_with_read(rtx, schema, sq),
        Statement::Explain { inner, analyze } => {
            if *analyze && stmt_mutates(inner) {
                return Err(SqlError::Unsupported(
                    "EXPLAIN ANALYZE of a mutating statement inside a read-only transaction".into(),
                ));
            }
            let mut plan = explain(
                &mut ExplainCtx {
                    schema,
                    rows: &mut |t: &str| explain_row_count(|| rtx.table_entry_count(t.as_bytes())),
                },
                inner,
            )?;
            if *analyze {
                #[cfg(test)]
                pause_after_explain_plan();
                let span = Span::open(rtx.measure_scans());
                let result = execute_with_admitted_read(rtx, schema, inner, _params)?;
                let measured = span.close(&result);
                explain::attach_measurement(&mut plan, &measured);
            }
            Ok(plan)
        }
        Statement::CreateTable(_)
        | Statement::DropTable(_)
        | Statement::CreateIndex(_)
        | Statement::DropIndex(_)
        | Statement::CreateView(_)
        | Statement::DropView(_)
        | Statement::AlterTable(_)
        | Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::Truncate(_)
        | Statement::CreateTrigger(_)
        | Statement::DropTrigger(_)
        | Statement::CreateMaterializedView(_)
        | Statement::RefreshMaterializedView(_)
        | Statement::DropMaterializedView(_) => Err(SqlError::Unsupported(
            "cannot execute mutating statement inside a read-only transaction".into(),
        )),
        Statement::Begin { .. }
        | Statement::Commit
        | Statement::Rollback
        | Statement::Savepoint(_)
        | Statement::ReleaseSavepoint(_)
        | Statement::RollbackTo(_)
        | Statement::SetTimezone { .. } => Err(SqlError::Unsupported(
            "transaction / session control handled by Connection".into(),
        )),
    }
}

/// Insert through an immutable catalog already matching the caller's writer.
/// Stale catalogs are refused before mutation; `execute_in_txn` can refresh a
/// mutable catalog from that same writer.
pub fn exec_insert_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
) -> Result<ExecutionResult> {
    wtx.check_usable().map_err(SqlError::Storage)?;
    check_cancelled(wtx.cancel_token())?;
    schema.validate_write_catalog(wtx)?;
    constraint_indexes::require_constraint_indexes(schema)?;
    dml::exec_insert_in_admitted_txn(wtx, schema, stmt, params)
}

pub(crate) use dml::exec_insert_in_admitted_txn;

/// Execute a parsed SQL statement within an existing write transaction.
pub fn execute_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &Statement,
    params: &[Value],
) -> Result<ExecutionResult> {
    wtx.check_usable().map_err(SqlError::Storage)?;
    check_cancelled(wtx.cancel_token())?;
    schema.admit_owned_write(wtx)?;
    execute_in_admitted_txn(wtx, schema, stmt, params)
}

pub(crate) fn execute_in_admitted_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &Statement,
    params: &[Value],
) -> Result<ExecutionResult> {
    guard_legacy_volatile_schema(schema, stmt)?;
    wtx.check_usable().map_err(SqlError::Storage)?;
    // Refused at the door, so nothing ran and there is nothing to poison.
    check_cancelled(wtx.cancel_token())?;
    let mutation_marker = wtx.mutation_marker();
    let mut outcome = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        execute_in_txn_inner(wtx, schema, stmt, params)
    })) {
        Ok(outcome) => outcome,
        Err(payload) => {
            if stmt_mutates(stmt) {
                wtx.mark_failed();
            }
            std::panic::resume_unwind(payload)
        }
    };
    if outcome.is_ok() && stmt_mutates_schema(stmt) {
        if let Err(error) = schema.bind_write_catalog(wtx) {
            outcome = Err(error);
        }
    }
    // The caller still owns this uncommitted transaction. Catch a cancel that
    // arrived after the final inner-loop check but before success was returned.
    if outcome.is_ok() {
        if let Err(err) = check_cancelled(wtx.cancel_token()) {
            outcome = Err(err);
        }
    }
    // A mutating statement may fail after successful low-level writes. This
    // API is public, so protect callers that do not go through `Connection`.
    if stmt_mutates(stmt) && wtx.mutated_since(mutation_marker) {
        if let Err(error) = &outcome {
            mark_write_statement_failed(wtx, error);
        }
    }
    outcome
}

pub(crate) fn mark_write_statement_failed(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    error: &SqlError,
) {
    if matches!(error, SqlError::Storage(citadel_core::Error::Interrupted)) {
        wtx.mark_cancelled();
    } else {
        wtx.mark_failed();
    }
}

fn execute_in_txn_inner(
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
        Statement::CreateView(cv) => exec_create_view_in_txn(wtx, schema, cv),
        Statement::DropView(dv) => exec_drop_view_in_txn(wtx, schema, dv),
        Statement::AlterTable(at) => exec_alter_table_in_txn(wtx, schema, at),
        Statement::Insert(ins) => exec_insert_in_admitted_txn(wtx, schema, ins, params),
        Statement::Select(sq) => exec_select_query_in_txn(wtx, schema, sq),
        Statement::Update(upd) => exec_update_in_txn(wtx, schema, upd),
        Statement::Delete(del) => exec_delete_in_txn(wtx, schema, del),
        Statement::Truncate(t) => exec_truncate_in_txn(wtx, schema, t),
        Statement::Explain { inner, analyze } => {
            // Validate and capture the plan before ANALYZE can change the
            // transaction. Unsupported plans therefore have no side effects.
            let mut plan = explain(
                &mut ExplainCtx {
                    schema,
                    rows: &mut |t: &str| explain_row_count(|| wtx.table_entry_count(t.as_bytes())),
                },
                inner,
            )?;
            if *analyze {
                let span = Span::open(wtx.measure_scans());
                let result = execute_in_admitted_txn(wtx, schema, inner, params)?;
                let measured = span.close(&result);
                explain::attach_measurement(&mut plan, &measured);
            }
            Ok(plan)
        }
        Statement::CreateTrigger(ct) => triggers::exec_create_trigger_in_txn(wtx, schema, ct),
        Statement::DropTrigger(dt) => triggers::exec_drop_trigger_in_txn(wtx, schema, dt),
        Statement::CreateMaterializedView(mv) => {
            matviews::exec_create_matview_in_txn(wtx, schema, mv)
        }
        Statement::RefreshMaterializedView(rmv) => {
            matviews::exec_refresh_matview_in_txn(wtx, schema, rmv)
        }
        Statement::DropMaterializedView(dmv) => {
            matviews::exec_drop_matview_in_txn(wtx, schema, dmv)
        }
        Statement::Begin { .. }
        | Statement::Commit
        | Statement::Rollback
        | Statement::Savepoint(_)
        | Statement::ReleaseSavepoint(_)
        | Statement::RollbackTo(_)
        | Statement::SetTimezone { .. } => {
            Err(SqlError::Unsupported("nested transaction control".into()))
        }
    }
}

pub(super) fn scan_table_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    name: &str,
) -> Result<(TableSchema, Vec<Vec<Value>>)> {
    let table_schema = schema
        .get(name)
        .ok_or_else(|| SqlError::TableNotFound(name.to_string()))?;
    let (rows, _) = collect_rows_with_read(rtx, table_schema, &None, None)?;
    Ok((table_schema.clone(), rows))
}

pub(super) fn scan_table_with_read_or_view(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    name: &str,
) -> Result<(TableSchema, Vec<Vec<Value>>)> {
    if let Some(ts) = schema.get(name) {
        let (rows, _) = collect_rows_with_read(rtx, ts, &None, None)?;
        return Ok((ts.clone(), rows));
    }
    if let Some(vd) = schema.get_view(name) {
        let qr = exec_view_with_read(rtx, schema, vd)?;
        let vs = build_view_schema(name, &qr)?;
        return Ok((vs, qr.result.rows));
    }
    if let Some(vt) = schema.get_virtual(name) {
        // A virtual table invents its columns rather than reading them from a relation, so
        // none of them carries a collation.
        let rows = CteRows::binary(vt.scan(schema, rtx.cancel_token())?);
        let vs = build_view_schema(name, &rows)?;
        return Ok((vs, rows.result.rows));
    }
    Err(SqlError::TableNotFound(name.to_string()))
}

pub(super) fn scan_table_write(
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

pub(super) fn scan_table_write_or_view(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    name: &str,
) -> Result<(TableSchema, Vec<Vec<Value>>)> {
    if let Some(ts) = schema.get(name) {
        let (rows, _) = collect_rows_write(wtx, ts, &None, None)?;
        return Ok((ts.clone(), rows));
    }
    if let Some(vd) = schema.get_view(name) {
        let qr = exec_view_write(wtx, schema, vd)?;
        let vs = build_view_schema(name, &qr)?;
        return Ok((vs, qr.result.rows));
    }
    Err(SqlError::TableNotFound(name.to_string()))
}

pub(super) fn resolve_table_or_cte(
    name: &str,
    ctes: &CteContext,
    scan_table: ScanTableFn<'_>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<(TableSchema, Vec<Vec<Value>>)> {
    let lower = name.to_ascii_lowercase();
    if let Some(cte) = ctes.get(&lower) {
        let schema = build_cte_schema(&lower, cte)?;
        Ok((
            schema,
            clone_cte_rows_with_cancel(&cte.result.rows, cancel)?,
        ))
    } else {
        scan_table(&lower)
    }
}

pub(super) fn exec_select_join_with_ctes(
    stmt: &SelectStmt,
    ctes: &CteContext,
    scan_table: ScanTableFn<'_>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<ExecutionResult> {
    let (from_schema, from_rows) = resolve_table_or_cte(&stmt.from, ctes, scan_table, cancel)?;
    let from_alias = table_alias_or_name(&stmt.from, &stmt.from_alias);

    let mut tables: Vec<(String, TableSchema)> = vec![(from_alias.clone(), from_schema)];
    let mut join_rows: Vec<Vec<Vec<Value>>> = Vec::new();

    for join in &stmt.joins {
        let jname = &join.table.name;
        let (js, jrows) = resolve_table_or_cte(jname, ctes, scan_table, cancel)?;
        let jalias = table_alias_or_name(jname, &join.table.alias);
        tables.push((jalias, js));
        join_rows.push(jrows);
    }

    let mut outer_rows = from_rows;
    let mut cur_tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), &tables[0].1)];

    for (ji, join) in stmt.joins.iter().enumerate() {
        let inner_schema = &tables[ji + 1].1;
        let inner_alias = &tables[ji + 1].0;
        let inner_rows = &mut join_rows[ji];

        let mut preview_tables = cur_tables.clone();
        preview_tables.push((inner_alias.clone(), inner_schema));
        let combined_cols = build_joined_columns(&preview_tables);

        let outer_col_count = if outer_rows.is_empty() {
            cur_tables.iter().map(|(_, s)| s.columns.len()).sum()
        } else {
            outer_rows[0].len()
        };
        let inner_col_count = inner_schema.columns.len();

        let equi = compute_equi_join_meta(join, &combined_cols, outer_col_count);
        outer_rows = exec_join_step(
            outer_rows,
            inner_rows,
            join,
            &combined_cols,
            outer_col_count,
            inner_col_count,
            None,
            None,
            &equi,
            cancel,
        )?;
        cur_tables.push((inner_alias.clone(), inner_schema));
    }

    let joined_cols = build_joined_columns(&cur_tables);
    process_select(outer_rows, SelectCtx::new(&joined_cols, stmt, cancel))
}

#[cfg(test)]
mod scan_span_tests {
    use super::*;
    use citadel::{Argon2Profile, DatabaseBuilder};

    #[test]
    fn span_does_not_charge_another_connections_thread() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("scan-span.citadel"))
            .passphrase(b"scan-span-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE scan_rows (id INTEGER PRIMARY KEY)")
            .unwrap();
        let values = (0..64)
            .map(|value| format!("({value})"))
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(&format!("INSERT INTO scan_rows VALUES {values}"))
            .unwrap();

        let telemetry_before = db.rows_scanned();
        let span = Span::open(db.measure_scans());
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let other = crate::Connection::open(&db).unwrap();
                    other.execute("SELECT * FROM scan_rows").unwrap();
                })
                .join()
                .expect("other connection panicked");
        });
        assert!(
            db.rows_scanned() > telemetry_before,
            "the other connection did not exercise global scan telemetry"
        );

        let measured = span.close(&ExecutionResult::Ok);
        assert_eq!(
            measured.scanned, None,
            "Span attributed another thread's scans to this operation"
        );
    }
}

#[cfg(test)]
mod cancellation_entry_tests {
    use super::*;
    use citadel::{Argon2Profile, CancelToken, DatabaseBuilder};

    fn assert_interrupted<T>(outcome: Result<T>) {
        let error = match outcome {
            Err(error) => error,
            Ok(_) => panic!("pre-cancelled direct entry executed"),
        };
        assert!(
            matches!(error, SqlError::Storage(citadel_core::Error::Interrupted)),
            "direct entry returned {error:?}"
        );
    }

    #[test]
    fn every_public_executor_entry_refuses_a_pre_cancelled_token() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("entry-cancel.citadel"))
            .passphrase(b"entry-cancel-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let mut schema = SchemaManager::empty();
        // These statements would return Unsupported/TableNotFound if an entry
        // check were removed, so an inner scan cannot mask the regression.
        let control = crate::parser::parse_sql("SET TIME ZONE 'UTC'").unwrap();
        let missing_insert = crate::parser::parse_sql("INSERT INTO missing VALUES (1)").unwrap();
        let Statement::Insert(insert) = &missing_insert else {
            panic!("expected INSERT")
        };

        let token = CancelToken::new();
        token.cancel();
        db.set_cancel(Some(token));

        assert_interrupted(execute(&db, &mut schema, &control, &[]));

        let mut rtx = db.begin_read();
        assert_interrupted(execute_with_read(&mut rtx, &schema, &control, &[]));
        drop(rtx);

        let mut wtx = db.begin_write().unwrap();
        assert_interrupted(execute_in_txn(&mut wtx, &mut schema, &control, &[]));
        assert_interrupted(exec_insert_in_txn(&mut wtx, &schema, insert, &[]));
        wtx.abort();
    }
}

#[cfg(test)]
mod legacy_catalog_entry_tests {
    use super::*;
    use citadel::{Argon2Profile, DatabaseBuilder};

    fn assert_recovery_required<T>(outcome: Result<T>) {
        let error = match outcome {
            Err(error) => error,
            Ok(_) => panic!("legacy volatile catalog reached a public executor lane"),
        };
        let message = error.to_string();
        assert!(message.contains("legacy catalog recovery required"));
        assert!(message.contains("legacy_random_idx"));
        assert!(message.contains("volatile function RANDOM()"));
    }

    #[test]
    fn every_public_executor_entry_enforces_legacy_recovery_mode() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("entry-legacy.citadel"))
            .passphrase(b"entry-legacy-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE legacy_random (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE INDEX legacy_random_idx ON legacy_random (id)")
            .unwrap();

        // Simulate a catalog written before immutable expression-index
        // validation existed.
        let mut legacy = conn.table_schema("legacy_random").unwrap();
        let index = legacy
            .indices
            .iter_mut()
            .find(|index| index.name == "legacy_random_idx")
            .unwrap();
        index.keys = vec![crate::types::IndexKey::Expr {
            expr: crate::parser::parse_sql_expr("RANDOM()").unwrap(),
            original_sql: "RANDOM()".into(),
        }];
        drop(conn);
        let mut wtx = db.begin_write().unwrap();
        SchemaManager::save_schema(&mut wtx, &legacy).unwrap();
        wtx.commit().unwrap();

        let mut schema = SchemaManager::load(&db).unwrap();
        let select = crate::parser::parse_sql("SELECT 1").unwrap();
        assert_recovery_required(execute(&db, &mut schema, &select, &[]));

        let mut rtx = db.begin_read();
        assert_recovery_required(execute_with_read(&mut rtx, &schema, &select, &[]));
        drop(rtx);

        let insert_stmt = crate::parser::parse_sql("INSERT INTO legacy_random VALUES (1)").unwrap();
        let Statement::Insert(insert) = &insert_stmt else {
            panic!("expected INSERT")
        };
        let mut wtx = db.begin_write().unwrap();
        assert_recovery_required(execute_in_txn(&mut wtx, &mut schema, &select, &[]));
        assert_recovery_required(exec_insert_in_txn(&mut wtx, &schema, insert, &[]));
        wtx.abort();

        let drop_index = crate::parser::parse_sql("DROP INDEX legacy_random_idx").unwrap();
        execute(&db, &mut schema, &drop_index, &[]).unwrap();
        assert!(schema.legacy_volatile_definition().is_none());
        let ExecutionResult::Query(result) = execute(&db, &mut schema, &select, &[]).unwrap()
        else {
            panic!("expected query result after recovery")
        };
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
    }
}

#[cfg(test)]
mod explain_cancel_tests {
    use super::*;
    use citadel::{Argon2Profile, CancelToken, Database, DatabaseBuilder};

    fn explained_table() -> (tempfile::TempDir, Database, SchemaManager, Statement) {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("explain-cancel.citadel"))
            .passphrase(b"explain-cancel-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE explained (id INTEGER PRIMARY KEY)")
            .unwrap();
        drop(conn);
        let schema = SchemaManager::load(&db).unwrap();
        let statement = crate::parser::parse_sql("EXPLAIN SELECT * FROM explained").unwrap();
        (dir, db, schema, statement)
    }

    fn assert_interrupted(error: SqlError) {
        assert!(
            matches!(error, SqlError::Storage(citadel_core::Error::Interrupted)),
            "EXPLAIN returned {error:?}"
        );
    }

    #[test]
    fn autocommit_explain_preserves_a_row_count_interruption() {
        let (_dir, db, mut schema, statement) = explained_table();
        let token = CancelToken::new();
        db.set_cancel(Some(token.clone()));
        let _cancel = cancel_on_next_explain_row_count(token);

        let error = execute(&db, &mut schema, &statement, &[])
            .expect_err("the interrupted row-count lookup was reported as an unknown estimate");

        assert_interrupted(error);
    }

    #[test]
    fn interrupted_explain_analyze_does_not_commit_its_mutation() {
        let (_dir, db, _schema, _) = explained_table();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("INSERT INTO explained VALUES (1)").unwrap();
        drop(conn);
        let mut schema = SchemaManager::load(&db).unwrap();
        let statement =
            crate::parser::parse_sql("EXPLAIN ANALYZE UPDATE explained SET id = 2 WHERE id = 1")
                .unwrap();
        let token = CancelToken::new();
        db.set_cancel(Some(token.clone()));
        let _cancel = cancel_on_next_explain_row_count(token);

        let error = execute(&db, &mut schema, &statement, &[])
            .expect_err("reporting succeeded after its token was cancelled");
        assert_interrupted(error);

        db.set_cancel(None);
        let conn = crate::Connection::open(&db).unwrap();
        let rows = conn.query("SELECT id FROM explained").unwrap();
        assert_eq!(rows.rows, vec![vec![Value::Integer(1)]]);
    }

    #[test]
    fn read_transaction_explain_preserves_a_row_count_interruption() {
        let (_dir, db, schema, statement) = explained_table();
        let token = CancelToken::new();
        db.set_cancel(Some(token.clone()));
        let mut rtx = db.begin_read();
        let _cancel = cancel_on_next_explain_row_count(token);

        let error = execute_with_read(&mut rtx, &schema, &statement, &[])
            .expect_err("the interrupted row-count lookup was reported as an unknown estimate");

        assert_interrupted(error);
    }

    #[test]
    fn autocommit_explain_analyze_uses_one_read_snapshot() {
        let (_dir, db, _schema, _) = explained_table();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("INSERT INTO explained VALUES (1)").unwrap();
        drop(conn);

        let mut schema = SchemaManager::load(&db).unwrap();
        let statement =
            crate::parser::parse_sql("EXPLAIN ANALYZE SELECT * FROM explained").unwrap();
        let planned = std::sync::Arc::new(std::sync::Barrier::new(2));
        let resume = std::sync::Arc::new(std::sync::Barrier::new(2));
        let _pause = pause_next_explain_after_plan(planned.clone(), resume.clone());

        let result = std::thread::scope(|scope| {
            let writer = scope.spawn(|| {
                planned.wait();
                let conn = crate::Connection::open(&db).unwrap();
                conn.execute("INSERT INTO explained VALUES (2)").unwrap();
                resume.wait();
            });
            let result = execute(&db, &mut schema, &statement, &[]).unwrap();
            writer.join().expect("writer panicked");
            result
        });

        let ExecutionResult::Query(query) = result else {
            panic!("expected EXPLAIN rows")
        };
        let Value::Text(line) = &query.rows[0][0] else {
            panic!("expected a text plan")
        };
        assert!(line.contains("rows=1"), "plan changed snapshots: {line}");
        assert!(
            line.contains("emitted=1"),
            "execution changed snapshots: {line}"
        );

        let conn = crate::Connection::open(&db).unwrap();
        assert_eq!(conn.query("SELECT * FROM explained").unwrap().rows.len(), 2);
    }

    #[test]
    fn a_missing_row_count_remains_an_unknown_estimate() {
        let rows = explain_row_count(|| {
            Err(citadel_core::Error::TableNotFound(
                "stale-schema-table".into(),
            ))
        })
        .unwrap();

        assert_eq!(rows, None);
    }

    #[test]
    fn an_integrity_failure_is_not_hidden_as_an_unknown_estimate() {
        let error = explain_row_count(|| Err(citadel_core::Error::DatabaseCorrupted))
            .expect_err("EXPLAIN hid a storage integrity failure");

        assert!(matches!(
            error,
            SqlError::Storage(citadel_core::Error::DatabaseCorrupted)
        ));
    }
}
