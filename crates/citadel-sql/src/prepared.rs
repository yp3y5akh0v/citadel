//! Prepared statements: parse + compile once, execute many times with parameters.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::connection::Connection;
use crate::error::{Result, SqlError};
use crate::executor::compile::RowSourceIter;
use crate::executor::helpers::expr_display_name;
use crate::executor::{self, CompiledPlan};
use crate::parser::{QueryBody, SelectColumn, SelectQuery, SelectStmt, Statement};
use crate::schema::SchemaManager;
use crate::types::{ExecutionResult, QueryResult, Value};

/// A prepared SQL statement bound to a `Connection`.
pub struct PreparedStatement<'c, 'db> {
    conn: &'c Connection<'db>,
    sql: String,
    ast: Arc<Statement>,
    compiled: Option<Arc<dyn CompiledPlan>>,
    schema_gen: u64,
    param_count: usize,
    columns: Vec<String>,
    column_index: FxHashMap<String, usize>,
    readonly: bool,
    is_explain: bool,
}

struct Compiled {
    ast: Arc<Statement>,
    plan: Option<Arc<dyn CompiledPlan>>,
    schema_gen: u64,
    param_count: usize,
    columns: Vec<String>,
}

impl<'c, 'db> PreparedStatement<'c, 'db> {
    pub(crate) fn new(conn: &'c Connection<'db>, sql: &str) -> Result<Self> {
        let c = compile_for_sql(conn, sql)?;
        let readonly = matches!(*c.ast, Statement::Select(_) | Statement::Explain(_));
        let is_explain = matches!(*c.ast, Statement::Explain(_));
        let mut column_index =
            FxHashMap::with_capacity_and_hasher(c.columns.len(), Default::default());
        for (i, name) in c.columns.iter().enumerate() {
            column_index.entry(name.clone()).or_insert(i);
        }
        Ok(Self {
            conn,
            sql: sql.to_string(),
            ast: c.ast,
            compiled: c.plan,
            schema_gen: c.schema_gen,
            param_count: c.param_count,
            columns: c.columns,
            column_index,
            readonly,
            is_explain,
        })
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Number of positional parameters (`$1`, `$2`, ...) this statement expects.
    pub fn param_count(&self) -> usize {
        self.param_count
    }

    /// Alias of [`Self::param_count`] matching rusqlite's name.
    pub fn parameter_count(&self) -> usize {
        self.param_count
    }

    /// Number of output columns. Zero for non-SELECT statements.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Output column names in declaration order.
    pub fn column_names(&self) -> &[String] {
        &self.columns
    }

    /// Output column name at index `i`, if any.
    pub fn column_name(&self, i: usize) -> Option<&str> {
        self.columns.get(i).map(|s| s.as_str())
    }

    /// Position of the column named `name`, if present.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_index.get(name).copied()
    }

    /// True if the statement is read-only (SELECT or EXPLAIN).
    pub fn readonly(&self) -> bool {
        self.readonly
    }

    /// True if the statement is an EXPLAIN.
    pub fn is_explain(&self) -> bool {
        self.is_explain
    }

    /// Execute the statement; returns rows affected (0 for SELECT/DDL).
    pub fn execute(&self, params: &[Value]) -> Result<u64> {
        match self.run(params)? {
            ExecutionResult::RowsAffected(n) => Ok(n),
            ExecutionResult::Query(_) | ExecutionResult::Ok => Ok(0),
        }
    }

    /// Execute and return a stepping `Rows<'_>` iterator.
    pub fn query(&self, params: &[Value]) -> Result<Rows<'_>> {
        if params.len() != self.param_count {
            return Err(SqlError::ParameterCountMismatch {
                expected: self.param_count,
                got: params.len(),
            });
        }
        if self.conn.inner.borrow().schema.generation() == self.schema_gen {
            if let Some(plan) = &self.compiled {
                if let Some(stream) = try_stream_via_plan(self, plan.as_ref(), params) {
                    return Ok(Rows::streaming(stream));
                }
            }
        }
        let (columns, rows) = match self.run(params)? {
            ExecutionResult::Query(qr) => (qr.columns, qr.rows),
            ExecutionResult::RowsAffected(_) | ExecutionResult::Ok => {
                (self.columns.clone(), Vec::new())
            }
        };
        Ok(Rows::materialized(columns, rows))
    }

    /// Execute and return the fully-materialized `QueryResult`.
    pub fn query_collect(&self, params: &[Value]) -> Result<QueryResult> {
        match self.run(params)? {
            ExecutionResult::Query(qr) => Ok(qr),
            ExecutionResult::RowsAffected(n) => Ok(QueryResult {
                columns: vec!["rows_affected".into()],
                rows: vec![vec![Value::Integer(n as i64)]],
            }),
            ExecutionResult::Ok => Ok(QueryResult {
                columns: vec![],
                rows: vec![],
            }),
        }
    }

    /// Run the query and pass the first row to `f`.
    pub fn query_row<T, F>(&self, params: &[Value], f: F) -> Result<T>
    where
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        let mut rows = self.query(params)?;
        match rows.next()? {
            Some(row) => f(&row),
            None => Err(SqlError::QueryReturnedNoRows),
        }
    }

    /// True if the query returns at least one row (DML returns `n > 0`).
    pub fn exists(&self, params: &[Value]) -> Result<bool> {
        if params.len() != self.param_count {
            return Err(SqlError::ParameterCountMismatch {
                expected: self.param_count,
                got: params.len(),
            });
        }
        if self.conn.inner.borrow().schema.generation() == self.schema_gen {
            if let Some(plan) = &self.compiled {
                if let Some(mut stream) = try_stream_via_plan(self, plan.as_ref(), params) {
                    return Ok(stream.next_row()?.is_some());
                }
            }
        }
        match self.run(params)? {
            ExecutionResult::Query(qr) => Ok(!qr.rows.is_empty()),
            ExecutionResult::RowsAffected(n) => Ok(n > 0),
            ExecutionResult::Ok => Ok(false),
        }
    }

    fn run(&self, params: &[Value]) -> Result<ExecutionResult> {
        if params.len() != self.param_count {
            return Err(SqlError::ParameterCountMismatch {
                expected: self.param_count,
                got: params.len(),
            });
        }
        let mut inner = self.conn.inner.borrow_mut();
        if inner.schema.generation() == self.schema_gen {
            return inner.execute_prepared(self.conn.db, &self.ast, self.compiled.as_ref(), params);
        }
        let c = compile_inside(&mut inner, &self.sql)?;
        if c.param_count != self.param_count {
            return Err(SqlError::ParameterCountMismatch {
                expected: self.param_count,
                got: c.param_count,
            });
        }
        inner.execute_prepared(self.conn.db, &c.ast, c.plan.as_ref(), params)
    }
}

/// Stepping iterator over query rows. Obtained from [`PreparedStatement::query`].
pub struct Rows<'a> {
    source: RowSource<'a>,
    columns: Vec<String>,
    buf: Vec<Value>,
}

enum RowSource<'a> {
    Materialized(std::vec::IntoIter<Vec<Value>>),
    Streaming(Box<dyn RowSourceIter + 'a>),
}

impl<'a> Rows<'a> {
    fn materialized(columns: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            source: RowSource::Materialized(rows.into_iter()),
            columns,
            buf: Vec::new(),
        }
    }

    fn streaming(source: Box<dyn RowSourceIter + 'a>) -> Self {
        let columns = source.columns().to_vec();
        Self {
            source: RowSource::Streaming(source),
            columns,
            buf: Vec::new(),
        }
    }

    /// Step to the next row, if any.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Row<'_>>> {
        let next: Option<Vec<Value>> = match &mut self.source {
            RowSource::Materialized(iter) => iter.next(),
            RowSource::Streaming(stream) => stream.next_row()?,
        };
        match next {
            Some(values) => {
                self.buf = values;
                Ok(Some(Row {
                    columns: &self.columns,
                    values: &self.buf,
                }))
            }
            None => Ok(None),
        }
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn column_names(&self) -> &[String] {
        &self.columns
    }

    /// Drain all remaining rows into a [`QueryResult`].
    pub fn collect(mut self) -> Result<QueryResult> {
        let mut rows = Vec::new();
        while let Some(row) = self.next()? {
            rows.push(row.to_vec());
        }
        Ok(QueryResult {
            columns: self.columns,
            rows,
        })
    }
}

/// A single row produced by [`Rows::next`].
pub struct Row<'a> {
    columns: &'a [String],
    values: &'a [Value],
}

impl<'a> Row<'a> {
    /// Value at column index `i`, if present.
    pub fn get(&self, i: usize) -> Option<&Value> {
        self.values.get(i)
    }

    /// Value of the column named `name`, if present.
    pub fn get_by_name(&self, name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|c| c == name)
            .and_then(|i| self.values.get(i))
    }

    pub fn column_count(&self) -> usize {
        self.values.len()
    }

    /// Name of the column at index `i`, if present.
    pub fn column_name(&self, i: usize) -> Option<&str> {
        self.columns.get(i).map(|s| s.as_str())
    }

    pub fn as_slice(&self) -> &[Value] {
        self.values
    }

    pub fn to_vec(&self) -> Vec<Value> {
        self.values.to_vec()
    }
}

fn compile_for_sql(conn: &Connection<'_>, sql: &str) -> Result<Compiled> {
    let mut inner = conn.inner.borrow_mut();
    compile_inside(&mut inner, sql)
}

fn compile_inside(
    inner: &mut crate::connection::ConnectionInner<'_>,
    sql: &str,
) -> Result<Compiled> {
    let (ast, param_count) = inner.get_or_parse(sql)?;
    let schema_gen = inner.schema.generation();
    let plan = executor::compile(&inner.schema, &ast);
    if let Some(p) = &plan {
        if let Some(entry) = inner.stmt_cache.get_mut(sql) {
            entry.compiled = Some(Arc::clone(p));
        }
    }
    let columns = derive_columns(&ast, &inner.schema);
    Ok(Compiled {
        ast,
        plan,
        schema_gen,
        param_count,
        columns,
    })
}

fn derive_columns(stmt: &Statement, schema: &SchemaManager) -> Vec<String> {
    match stmt {
        Statement::Select(sq) => derive_select_columns(sq, schema),
        Statement::Explain(_) => vec!["plan".into()],
        _ => Vec::new(),
    }
}

fn derive_select_columns(sq: &SelectQuery, schema: &SchemaManager) -> Vec<String> {
    derive_body_columns(&sq.body, schema)
}

fn derive_body_columns(body: &QueryBody, schema: &SchemaManager) -> Vec<String> {
    match body {
        QueryBody::Select(sel) => derive_from_select_stmt(sel, schema),
        QueryBody::Compound(cs) => derive_body_columns(&cs.left, schema),
        QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_) => Vec::new(),
    }
}

fn try_stream_via_plan<'db>(
    stmt: &PreparedStatement<'_, 'db>,
    plan: &dyn CompiledPlan,
    params: &[Value],
) -> Option<Box<dyn RowSourceIter + 'db>> {
    let inner = stmt.conn.inner.borrow();
    if inner.active_txn_is_some() {
        return None;
    }
    plan.try_stream(stmt.conn.db, &inner.schema, &stmt.ast, params)
}

fn derive_from_select_stmt(sel: &SelectStmt, schema: &SchemaManager) -> Vec<String> {
    let lower = sel.from.to_ascii_lowercase();
    let table_columns = schema.get(&lower).map(|ts| ts.columns.as_slice());
    let mut out = Vec::new();
    for col in &sel.columns {
        match col {
            SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                if let Some(cols) = table_columns {
                    for c in cols {
                        out.push(c.name.clone());
                    }
                }
            }
            SelectColumn::Expr { alias: Some(a), .. } => out.push(a.clone()),
            SelectColumn::Expr { expr, alias: None } => out.push(expr_display_name(expr)),
        }
    }
    out
}
