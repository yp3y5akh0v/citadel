use std::sync::Arc;

use citadel::Database;
use citadel_txn::read_txn::ReadTxn;
use citadel_txn::write_txn::WriteTxn;

use crate::error::Result;
use crate::parser::Statement;
use crate::schema::SchemaManager;
use crate::types::{ExecutionResult, QueryResult, Value};

pub enum ActiveTxnRef<'a, 'db: 'a> {
    None,
    Read(&'a mut ReadTxn<'db>),
    Write(&'a mut WriteTxn<'db>),
}

pub trait CompiledPlan: Send + Sync {
    fn execute(
        &self,
        db: &Database,
        schema: &SchemaManager,
        stmt: &Statement,
        params: &[Value],
        txn: ActiveTxnRef<'_, '_>,
    ) -> Result<ExecutionResult>;

    /// Attempt to produce a streaming row source. Returns `None` if this plan
    /// cannot stream the given statement — caller falls back to `execute`.
    fn try_stream<'db>(
        &self,
        _db: &'db Database,
        _schema: &SchemaManager,
        _stmt: &Statement,
        _params: &[Value],
    ) -> Option<Box<dyn RowSourceIter + 'db>> {
        None
    }

    /// Zero-copy materialized collect; `None` if the plan cannot fast-collect.
    fn try_collect(
        &self,
        _db: &Database,
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

    /// `false` when the plan never reads the txn clock (no NOW(),
    /// CURRENT_TIMESTAMP, etc.). Lets the caller skip the
    /// `with_txn_clock` thread-local wrapper.
    fn needs_txn_clock(&self) -> bool {
        true
    }
}

/// Internal trait: object-safe streaming source over decoded rows.
pub trait RowSourceIter {
    fn next_row(&mut self) -> Result<Option<Vec<Value>>>;
    fn columns(&self) -> &[String];
    /// Upper bound on remaining rows, for output pre-sizing. 0 = unknown.
    fn size_hint(&self) -> usize {
        0
    }
}

pub fn compile(schema: &SchemaManager, stmt: &Statement) -> Option<Arc<dyn CompiledPlan>> {
    match stmt {
        Statement::Select(sq) => super::select::CompiledSelect::try_compile(schema, sq)
            .map(|c| Arc::new(c) as Arc<dyn CompiledPlan>),
        Statement::Insert(ins) => super::dml::CompiledInsert::try_compile(schema, ins)
            .map(|c| Arc::new(c) as Arc<dyn CompiledPlan>),
        Statement::Update(upd) => super::write::CompiledUpdate::try_compile(schema, upd)
            .ok()
            .flatten()
            .map(|c| Arc::new(c) as Arc<dyn CompiledPlan>),
        Statement::Delete(del) => super::dml::CompiledDelete::try_compile(schema, del)
            .map(|c| Arc::new(c) as Arc<dyn CompiledPlan>),
        _ => None,
    }
}

#[cfg(test)]
#[path = "compile_tests.rs"]
mod tests;
