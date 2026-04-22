use std::sync::Arc;

use citadel::Database;
use citadel_txn::write_txn::WriteTxn;

use crate::error::Result;
use crate::parser::Statement;
use crate::schema::SchemaManager;
use crate::types::{ExecutionResult, Value};

pub trait CompiledPlan: Send + Sync {
    fn execute(
        &self,
        db: &Database,
        schema: &SchemaManager,
        stmt: &Statement,
        params: &[Value],
        wtx: Option<&mut WriteTxn<'_>>,
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
}

/// Internal trait: object-safe streaming source over decoded rows.
pub trait RowSourceIter {
    fn next_row(&mut self) -> Result<Option<Vec<Value>>>;
    fn columns(&self) -> &[String];
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
