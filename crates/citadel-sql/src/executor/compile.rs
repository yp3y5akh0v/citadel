use std::sync::Arc;

use citadel_txn::read_txn::ReadTxn;
use citadel_txn::write_txn::WriteTxn;

use crate::error::Result;
use crate::parser::Statement;
use crate::schema::SchemaManager;
use crate::types::{ExecutionResult, QueryResult, Value};

pub(crate) enum ActiveTxnRef<'a, 'db: 'a> {
    Read(&'a mut ReadTxn<'db>),
    Write(&'a mut WriteTxn<'db>),
}

/// An unsupported streaming shape returns its exact admitted snapshot so the
/// buffered executor can continue without reopening a newer view.
pub(crate) enum StreamAttempt<'db> {
    Streaming(Box<dyn RowSourceIter + 'db>),
    Buffered(ReadTxn<'db>),
}

pub(crate) trait CompiledPlan: Send + Sync {
    fn execute(
        &self,
        schema: &SchemaManager,
        stmt: &Statement,
        params: &[Value],
        txn: ActiveTxnRef<'_, '_>,
    ) -> Result<ExecutionResult>;

    /// Attempt to stream, retaining the admitted snapshot when unsupported.
    /// Storage errors are returned; they must not trigger a fresh-snapshot retry.
    fn try_stream<'db>(
        &self,
        rtx: ReadTxn<'db>,
        _schema: &SchemaManager,
        _stmt: &Statement,
        _params: &[Value],
    ) -> Result<StreamAttempt<'db>> {
        Ok(StreamAttempt::Buffered(rtx))
    }

    /// Zero-copy materialized collect; `None` if the plan cannot fast-collect.
    fn try_collect(
        &self,
        _rtx: &mut ReadTxn<'_>,
        _schema: &SchemaManager,
        _stmt: &Statement,
        _params: &[Value],
    ) -> Option<Result<QueryResult>> {
        None
    }

    /// `false` when `execute` reads `params` directly without `resolve_scoped_param`,
    /// letting the caller skip `with_scoped_params`.
    fn uses_scoped_params(&self) -> bool {
        true
    }

    /// Positive proof that every execution and fallback is independent of
    /// temporal/JSONPath session context. Scoped parameters and statement
    /// guards are separate and must still run. Unknown plans require context.
    fn can_skip_session_context(&self) -> bool {
        false
    }

    /// `false` when the plan never reads the txn clock (no NOW(),
    /// CURRENT_TIMESTAMP, etc.). Lets the caller skip the
    /// `with_txn_clock` thread-local wrapper.
    fn needs_txn_clock(&self) -> bool {
        true
    }
}

/// Internal trait: object-safe streaming source over decoded rows.
pub(crate) trait RowSourceIter {
    fn next_row(&mut self) -> Result<Option<Vec<Value>>>;
    fn columns(&self) -> &[String];
}

pub(crate) fn compile(schema: &SchemaManager, stmt: &Statement) -> Option<Arc<dyn CompiledPlan>> {
    match stmt {
        Statement::Select(sq) => super::select::CompiledSelect::try_compile(schema, sq)
            .map(|c| Arc::new(c) as Arc<dyn CompiledPlan>),
        Statement::Insert(ins) => super::dml::CompiledInsert::try_compile(schema, ins)
            .map(|c| Arc::new(c) as Arc<dyn CompiledPlan>),
        Statement::Update(upd) => super::write::CompiledUpdate::try_compile(schema, upd)
            .ok()
            .flatten()
            .map(|c| Arc::new(c) as Arc<dyn CompiledPlan>),
        Statement::Delete(del) => super::write::CompiledDelete::try_compile(schema, del)
            .map(|c| Arc::new(c) as Arc<dyn CompiledPlan>),
        _ => None,
    }
}

#[cfg(test)]
#[path = "compile_tests.rs"]
mod tests;
