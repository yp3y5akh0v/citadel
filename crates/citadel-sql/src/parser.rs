//! SQL parser: converts SQL strings into our internal AST.

use sqlparser::ast as sp;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::{Result, SqlError};
use crate::types::{DataType, Value};

// ── Internal AST ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    CreateView(CreateViewStmt),
    DropView(DropViewStmt),
    AlterTable(Box<AlterTableStmt>),
    Insert(InsertStmt),
    Select(Box<SelectQuery>),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    ReleaseSavepoint(String),
    RollbackTo(String),
    Explain(Box<Statement>),
}

#[derive(Debug, Clone)]
pub struct AlterTableStmt {
    pub table: String,
    pub op: AlterTableOp,
}

#[derive(Debug, Clone)]
pub enum AlterTableOp {
    AddColumn {
        column: Box<ColumnSpec>,
        foreign_key: Option<ForeignKeyDef>,
        if_not_exists: bool,
    },
    DropColumn {
        name: String,
        if_exists: bool,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    RenameTable {
        new_name: String,
    },
}

#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub primary_key: Vec<String>,
    pub if_not_exists: bool,
    pub check_constraints: Vec<TableCheckConstraint>,
    pub foreign_keys: Vec<ForeignKeyDef>,
}

#[derive(Debug, Clone)]
pub struct TableCheckConstraint {
    pub name: Option<String>,
    pub expr: Expr,
    pub sql: String,
}

#[derive(Debug, Clone)]
pub struct ForeignKeyDef {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub foreign_table: String,
    pub referred_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub default_expr: Option<Expr>,
    pub default_sql: Option<String>,
    pub check_expr: Option<Expr>,
    pub check_sql: Option<String>,
    pub check_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropIndexStmt {
    pub index_name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateViewStmt {
    pub name: String,
    pub sql: String,
    pub column_aliases: Vec<String>,
    pub or_replace: bool,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropViewStmt {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Select(Box<SelectQuery>),
}

#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table: String,
    pub columns: Vec<String>,
    pub source: InsertSource,
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Cross,
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableRef,
    pub on_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub columns: Vec<SelectColumn>,
    pub from: String,
    pub from_alias: Option<String>,
    pub joins: Vec<JoinClause>,
    pub distinct: bool,
    pub where_clause: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum SetOp {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone)]
pub struct CompoundSelect {
    pub op: SetOp,
    pub all: bool,
    pub left: Box<QueryBody>,
    pub right: Box<QueryBody>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum QueryBody {
    Select(Box<SelectStmt>),
    Compound(Box<CompoundSelect>),
}

#[derive(Debug, Clone)]
pub struct CteDefinition {
    pub name: String,
    pub column_aliases: Vec<String>,
    pub body: QueryBody,
}

#[derive(Debug, Clone)]
pub struct SelectQuery {
    pub ctes: Vec<CteDefinition>,
    pub recursive: bool,
    pub body: QueryBody,
}

#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
    AllColumns,
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expr: Expr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(Value),
    Column(String),
    QualifiedColumn {
        table: String,
        column: String,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Function {
        name: String,
        args: Vec<Expr>,
    },
    CountStar,
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<SelectStmt>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    Exists {
        subquery: Box<SelectStmt>,
        negated: bool,
    },
    ScalarSubquery(Box<SelectStmt>),
    InSet {
        expr: Box<Expr>,
        values: std::collections::HashSet<Value>,
        has_null: bool,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        negated: bool,
    },
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    Coalesce(Vec<Expr>),
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    Parameter(usize),
    WindowFunction {
        name: String,
        args: Vec<Expr>,
        spec: WindowSpec,
    },
}

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub frame: Option<WindowFrame>,
}

#[derive(Debug, Clone)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start: WindowFrameBound,
    pub end: WindowFrameBound,
}

#[derive(Debug, Clone, Copy)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(Box<Expr>),
    CurrentRow,
    Following(Box<Expr>),
    UnboundedFollowing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    And,
    Or,
    Concat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

// ── Expression utilities ────────────────────────────────────────────

pub fn has_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::ScalarSubquery(_) => true,
        Expr::BinaryOp { left, right, .. } => has_subquery(left) || has_subquery(right),
        Expr::UnaryOp { expr, .. } => has_subquery(expr),
        Expr::IsNull(e) | Expr::IsNotNull(e) => has_subquery(e),
        Expr::InList { expr, list, .. } => has_subquery(expr) || list.iter().any(has_subquery),
        Expr::InSet { expr, .. } => has_subquery(expr),
        Expr::Between {
            expr, low, high, ..
        } => has_subquery(expr) || has_subquery(low) || has_subquery(high),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            has_subquery(expr)
                || has_subquery(pattern)
                || escape.as_ref().is_some_and(|e| has_subquery(e))
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand.as_ref().is_some_and(|e| has_subquery(e))
                || conditions
                    .iter()
                    .any(|(c, r)| has_subquery(c) || has_subquery(r))
                || else_result.as_ref().is_some_and(|e| has_subquery(e))
        }
        Expr::Coalesce(args) | Expr::Function { args, .. } => args.iter().any(has_subquery),
        Expr::Cast { expr, .. } => has_subquery(expr),
        _ => false,
    }
}

/// Parse a SQL expression string back into an internal Expr.
/// Used for deserializing stored DEFAULT/CHECK expressions from schema.
pub fn parse_sql_expr(sql: &str) -> Result<Expr> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|e| SqlError::Parse(e.to_string()))?;
    let sp_expr = parser
        .parse_expr()
        .map_err(|e| SqlError::Parse(e.to_string()))?;
    convert_expr(&sp_expr)
}

// ── Parser entry point ──────────────────────────────────────────────

pub fn parse_sql(sql: &str) -> Result<Statement> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Parse(e.to_string()))?;

    if stmts.is_empty() {
        return Err(SqlError::Parse("empty SQL".into()));
    }
    if stmts.len() > 1 {
        return Err(SqlError::Unsupported("multiple statements".into()));
    }

    convert_statement(stmts.into_iter().next().unwrap())
}

// ── Parameter utilities ─────────────────────────────────────────────

/// Returns the number of distinct parameters in a statement (max $N found).
pub fn count_params(stmt: &Statement) -> usize {
    let mut max_idx = 0usize;
    visit_exprs_stmt(stmt, &mut |e| {
        if let Expr::Parameter(n) = e {
            max_idx = max_idx.max(*n);
        }
    });
    max_idx
}

/// Replace all `Expr::Parameter(n)` with `Expr::Literal(params[n-1])`.
pub fn bind_params(
    stmt: &Statement,
    params: &[crate::types::Value],
) -> crate::error::Result<Statement> {
    bind_stmt(stmt, params)
}

fn bind_stmt(stmt: &Statement, params: &[crate::types::Value]) -> crate::error::Result<Statement> {
    match stmt {
        Statement::Select(sq) => Ok(Statement::Select(Box::new(bind_select_query(sq, params)?))),
        Statement::Insert(ins) => {
            let source = match &ins.source {
                InsertSource::Values(rows) => {
                    let bound = rows
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(|e| bind_expr(e, params))
                                .collect::<crate::error::Result<Vec<_>>>()
                        })
                        .collect::<crate::error::Result<Vec<_>>>()?;
                    InsertSource::Values(bound)
                }
                InsertSource::Select(sq) => {
                    InsertSource::Select(Box::new(bind_select_query(sq, params)?))
                }
            };
            Ok(Statement::Insert(InsertStmt {
                table: ins.table.clone(),
                columns: ins.columns.clone(),
                source,
            }))
        }
        Statement::Update(upd) => {
            let assignments = upd
                .assignments
                .iter()
                .map(|(col, e)| Ok((col.clone(), bind_expr(e, params)?)))
                .collect::<crate::error::Result<Vec<_>>>()?;
            let where_clause = upd
                .where_clause
                .as_ref()
                .map(|e| bind_expr(e, params))
                .transpose()?;
            Ok(Statement::Update(UpdateStmt {
                table: upd.table.clone(),
                assignments,
                where_clause,
            }))
        }
        Statement::Delete(del) => {
            let where_clause = del
                .where_clause
                .as_ref()
                .map(|e| bind_expr(e, params))
                .transpose()?;
            Ok(Statement::Delete(DeleteStmt {
                table: del.table.clone(),
                where_clause,
            }))
        }
        Statement::Explain(inner) => Ok(Statement::Explain(Box::new(bind_stmt(inner, params)?))),
        other => Ok(other.clone()),
    }
}

fn bind_select(
    sel: &SelectStmt,
    params: &[crate::types::Value],
) -> crate::error::Result<SelectStmt> {
    let columns = sel
        .columns
        .iter()
        .map(|c| match c {
            SelectColumn::AllColumns => Ok(SelectColumn::AllColumns),
            SelectColumn::Expr { expr, alias } => Ok(SelectColumn::Expr {
                expr: bind_expr(expr, params)?,
                alias: alias.clone(),
            }),
        })
        .collect::<crate::error::Result<Vec<_>>>()?;
    let joins = sel
        .joins
        .iter()
        .map(|j| {
            let on_clause = j
                .on_clause
                .as_ref()
                .map(|e| bind_expr(e, params))
                .transpose()?;
            Ok(JoinClause {
                join_type: j.join_type,
                table: j.table.clone(),
                on_clause,
            })
        })
        .collect::<crate::error::Result<Vec<_>>>()?;
    let where_clause = sel
        .where_clause
        .as_ref()
        .map(|e| bind_expr(e, params))
        .transpose()?;
    let order_by = sel
        .order_by
        .iter()
        .map(|o| {
            Ok(OrderByItem {
                expr: bind_expr(&o.expr, params)?,
                descending: o.descending,
                nulls_first: o.nulls_first,
            })
        })
        .collect::<crate::error::Result<Vec<_>>>()?;
    let limit = sel
        .limit
        .as_ref()
        .map(|e| bind_expr(e, params))
        .transpose()?;
    let offset = sel
        .offset
        .as_ref()
        .map(|e| bind_expr(e, params))
        .transpose()?;
    let group_by = sel
        .group_by
        .iter()
        .map(|e| bind_expr(e, params))
        .collect::<crate::error::Result<Vec<_>>>()?;
    let having = sel
        .having
        .as_ref()
        .map(|e| bind_expr(e, params))
        .transpose()?;

    Ok(SelectStmt {
        columns,
        from: sel.from.clone(),
        from_alias: sel.from_alias.clone(),
        joins,
        distinct: sel.distinct,
        where_clause,
        order_by,
        limit,
        offset,
        group_by,
        having,
    })
}

fn bind_query_body(
    body: &QueryBody,
    params: &[crate::types::Value],
) -> crate::error::Result<QueryBody> {
    match body {
        QueryBody::Select(sel) => Ok(QueryBody::Select(Box::new(bind_select(sel, params)?))),
        QueryBody::Compound(comp) => {
            let order_by = comp
                .order_by
                .iter()
                .map(|o| {
                    Ok(OrderByItem {
                        expr: bind_expr(&o.expr, params)?,
                        descending: o.descending,
                        nulls_first: o.nulls_first,
                    })
                })
                .collect::<crate::error::Result<Vec<_>>>()?;
            let limit = comp
                .limit
                .as_ref()
                .map(|e| bind_expr(e, params))
                .transpose()?;
            let offset = comp
                .offset
                .as_ref()
                .map(|e| bind_expr(e, params))
                .transpose()?;
            Ok(QueryBody::Compound(Box::new(CompoundSelect {
                op: comp.op.clone(),
                all: comp.all,
                left: Box::new(bind_query_body(&comp.left, params)?),
                right: Box::new(bind_query_body(&comp.right, params)?),
                order_by,
                limit,
                offset,
            })))
        }
    }
}

fn bind_select_query(
    sq: &SelectQuery,
    params: &[crate::types::Value],
) -> crate::error::Result<SelectQuery> {
    let ctes = sq
        .ctes
        .iter()
        .map(|cte| {
            Ok(CteDefinition {
                name: cte.name.clone(),
                column_aliases: cte.column_aliases.clone(),
                body: bind_query_body(&cte.body, params)?,
            })
        })
        .collect::<crate::error::Result<Vec<_>>>()?;
    let body = bind_query_body(&sq.body, params)?;
    Ok(SelectQuery {
        ctes,
        recursive: sq.recursive,
        body,
    })
}

fn bind_expr(expr: &Expr, params: &[crate::types::Value]) -> crate::error::Result<Expr> {
    match expr {
        Expr::Parameter(n) => {
            if *n == 0 || *n > params.len() {
                return Err(SqlError::ParameterCountMismatch {
                    expected: *n,
                    got: params.len(),
                });
            }
            Ok(Expr::Literal(params[*n - 1].clone()))
        }
        Expr::Literal(_) | Expr::Column(_) | Expr::QualifiedColumn { .. } | Expr::CountStar => {
            Ok(expr.clone())
        }
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(bind_expr(left, params)?),
            op: *op,
            right: Box::new(bind_expr(right, params)?),
        }),
        Expr::UnaryOp { op, expr: e } => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(bind_expr(e, params)?),
        }),
        Expr::IsNull(e) => Ok(Expr::IsNull(Box::new(bind_expr(e, params)?))),
        Expr::IsNotNull(e) => Ok(Expr::IsNotNull(Box::new(bind_expr(e, params)?))),
        Expr::Function { name, args } => {
            let args = args
                .iter()
                .map(|a| bind_expr(a, params))
                .collect::<crate::error::Result<Vec<_>>>()?;
            Ok(Expr::Function {
                name: name.clone(),
                args,
            })
        }
        Expr::InSubquery {
            expr: e,
            subquery,
            negated,
        } => Ok(Expr::InSubquery {
            expr: Box::new(bind_expr(e, params)?),
            subquery: Box::new(bind_select(subquery, params)?),
            negated: *negated,
        }),
        Expr::InList {
            expr: e,
            list,
            negated,
        } => {
            let list = list
                .iter()
                .map(|l| bind_expr(l, params))
                .collect::<crate::error::Result<Vec<_>>>()?;
            Ok(Expr::InList {
                expr: Box::new(bind_expr(e, params)?),
                list,
                negated: *negated,
            })
        }
        Expr::Exists { subquery, negated } => Ok(Expr::Exists {
            subquery: Box::new(bind_select(subquery, params)?),
            negated: *negated,
        }),
        Expr::ScalarSubquery(sq) => Ok(Expr::ScalarSubquery(Box::new(bind_select(sq, params)?))),
        Expr::InSet {
            expr: e,
            values,
            has_null,
            negated,
        } => Ok(Expr::InSet {
            expr: Box::new(bind_expr(e, params)?),
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
            expr: Box::new(bind_expr(e, params)?),
            low: Box::new(bind_expr(low, params)?),
            high: Box::new(bind_expr(high, params)?),
            negated: *negated,
        }),
        Expr::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => Ok(Expr::Like {
            expr: Box::new(bind_expr(e, params)?),
            pattern: Box::new(bind_expr(pattern, params)?),
            escape: escape
                .as_ref()
                .map(|esc| bind_expr(esc, params).map(Box::new))
                .transpose()?,
            negated: *negated,
        }),
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let operand = operand
                .as_ref()
                .map(|e| bind_expr(e, params).map(Box::new))
                .transpose()?;
            let conditions = conditions
                .iter()
                .map(|(cond, then)| Ok((bind_expr(cond, params)?, bind_expr(then, params)?)))
                .collect::<crate::error::Result<Vec<_>>>()?;
            let else_result = else_result
                .as_ref()
                .map(|e| bind_expr(e, params).map(Box::new))
                .transpose()?;
            Ok(Expr::Case {
                operand,
                conditions,
                else_result,
            })
        }
        Expr::Coalesce(args) => {
            let args = args
                .iter()
                .map(|a| bind_expr(a, params))
                .collect::<crate::error::Result<Vec<_>>>()?;
            Ok(Expr::Coalesce(args))
        }
        Expr::Cast { expr: e, data_type } => Ok(Expr::Cast {
            expr: Box::new(bind_expr(e, params)?),
            data_type: *data_type,
        }),
        Expr::WindowFunction { name, args, spec } => {
            let args = args
                .iter()
                .map(|a| bind_expr(a, params))
                .collect::<crate::error::Result<Vec<_>>>()?;
            let partition_by = spec
                .partition_by
                .iter()
                .map(|e| bind_expr(e, params))
                .collect::<crate::error::Result<Vec<_>>>()?;
            let order_by = spec
                .order_by
                .iter()
                .map(|o| {
                    Ok(OrderByItem {
                        expr: bind_expr(&o.expr, params)?,
                        descending: o.descending,
                        nulls_first: o.nulls_first,
                    })
                })
                .collect::<crate::error::Result<Vec<_>>>()?;
            let frame = match &spec.frame {
                Some(f) => Some(WindowFrame {
                    units: f.units,
                    start: bind_frame_bound(&f.start, params)?,
                    end: bind_frame_bound(&f.end, params)?,
                }),
                None => None,
            };
            Ok(Expr::WindowFunction {
                name: name.clone(),
                args,
                spec: WindowSpec {
                    partition_by,
                    order_by,
                    frame,
                },
            })
        }
    }
}

fn bind_frame_bound(
    bound: &WindowFrameBound,
    params: &[crate::types::Value],
) -> crate::error::Result<WindowFrameBound> {
    match bound {
        WindowFrameBound::Preceding(e) => {
            Ok(WindowFrameBound::Preceding(Box::new(bind_expr(e, params)?)))
        }
        WindowFrameBound::Following(e) => {
            Ok(WindowFrameBound::Following(Box::new(bind_expr(e, params)?)))
        }
        other => Ok(other.clone()),
    }
}

fn visit_exprs_stmt(stmt: &Statement, visitor: &mut impl FnMut(&Expr)) {
    match stmt {
        Statement::Select(sq) => {
            for cte in &sq.ctes {
                visit_exprs_query_body(&cte.body, visitor);
            }
            visit_exprs_query_body(&sq.body, visitor);
        }
        Statement::Insert(ins) => match &ins.source {
            InsertSource::Values(rows) => {
                for row in rows {
                    for e in row {
                        visit_expr(e, visitor);
                    }
                }
            }
            InsertSource::Select(sq) => {
                for cte in &sq.ctes {
                    visit_exprs_query_body(&cte.body, visitor);
                }
                visit_exprs_query_body(&sq.body, visitor);
            }
        },
        Statement::Update(upd) => {
            for (_, e) in &upd.assignments {
                visit_expr(e, visitor);
            }
            if let Some(w) = &upd.where_clause {
                visit_expr(w, visitor);
            }
        }
        Statement::Delete(del) => {
            if let Some(w) = &del.where_clause {
                visit_expr(w, visitor);
            }
        }
        Statement::Explain(inner) => visit_exprs_stmt(inner, visitor),
        _ => {}
    }
}

fn visit_exprs_query_body(body: &QueryBody, visitor: &mut impl FnMut(&Expr)) {
    match body {
        QueryBody::Select(sel) => visit_exprs_select(sel, visitor),
        QueryBody::Compound(comp) => {
            visit_exprs_query_body(&comp.left, visitor);
            visit_exprs_query_body(&comp.right, visitor);
            for o in &comp.order_by {
                visit_expr(&o.expr, visitor);
            }
            if let Some(l) = &comp.limit {
                visit_expr(l, visitor);
            }
            if let Some(o) = &comp.offset {
                visit_expr(o, visitor);
            }
        }
    }
}

fn visit_exprs_select(sel: &SelectStmt, visitor: &mut impl FnMut(&Expr)) {
    for col in &sel.columns {
        if let SelectColumn::Expr { expr, .. } = col {
            visit_expr(expr, visitor);
        }
    }
    for j in &sel.joins {
        if let Some(on) = &j.on_clause {
            visit_expr(on, visitor);
        }
    }
    if let Some(w) = &sel.where_clause {
        visit_expr(w, visitor);
    }
    for o in &sel.order_by {
        visit_expr(&o.expr, visitor);
    }
    if let Some(l) = &sel.limit {
        visit_expr(l, visitor);
    }
    if let Some(o) = &sel.offset {
        visit_expr(o, visitor);
    }
    for g in &sel.group_by {
        visit_expr(g, visitor);
    }
    if let Some(h) = &sel.having {
        visit_expr(h, visitor);
    }
}

fn visit_expr(expr: &Expr, visitor: &mut impl FnMut(&Expr)) {
    visitor(expr);
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            visit_expr(left, visitor);
            visit_expr(right, visitor);
        }
        Expr::UnaryOp { expr: e, .. } | Expr::IsNull(e) | Expr::IsNotNull(e) => {
            visit_expr(e, visitor);
        }
        Expr::Function { args, .. } | Expr::Coalesce(args) => {
            for a in args {
                visit_expr(a, visitor);
            }
        }
        Expr::InSubquery {
            expr: e, subquery, ..
        } => {
            visit_expr(e, visitor);
            visit_exprs_select(subquery, visitor);
        }
        Expr::InList { expr: e, list, .. } => {
            visit_expr(e, visitor);
            for l in list {
                visit_expr(l, visitor);
            }
        }
        Expr::Exists { subquery, .. } => visit_exprs_select(subquery, visitor),
        Expr::ScalarSubquery(sq) => visit_exprs_select(sq, visitor),
        Expr::InSet { expr: e, .. } => visit_expr(e, visitor),
        Expr::Between {
            expr: e, low, high, ..
        } => {
            visit_expr(e, visitor);
            visit_expr(low, visitor);
            visit_expr(high, visitor);
        }
        Expr::Like {
            expr: e,
            pattern,
            escape,
            ..
        } => {
            visit_expr(e, visitor);
            visit_expr(pattern, visitor);
            if let Some(esc) = escape {
                visit_expr(esc, visitor);
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            if let Some(op) = operand {
                visit_expr(op, visitor);
            }
            for (cond, then) in conditions {
                visit_expr(cond, visitor);
                visit_expr(then, visitor);
            }
            if let Some(el) = else_result {
                visit_expr(el, visitor);
            }
        }
        Expr::Cast { expr: e, .. } => visit_expr(e, visitor),
        Expr::WindowFunction { args, spec, .. } => {
            for a in args {
                visit_expr(a, visitor);
            }
            for p in &spec.partition_by {
                visit_expr(p, visitor);
            }
            for o in &spec.order_by {
                visit_expr(&o.expr, visitor);
            }
            if let Some(ref frame) = spec.frame {
                if let WindowFrameBound::Preceding(e) | WindowFrameBound::Following(e) =
                    &frame.start
                {
                    visit_expr(e, visitor);
                }
                if let WindowFrameBound::Preceding(e) | WindowFrameBound::Following(e) = &frame.end
                {
                    visit_expr(e, visitor);
                }
            }
        }
        Expr::Literal(_)
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. }
        | Expr::CountStar
        | Expr::Parameter(_) => {}
    }
}

// ── Statement conversion ────────────────────────────────────────────

fn convert_statement(stmt: sp::Statement) -> Result<Statement> {
    match stmt {
        sp::Statement::CreateTable(ct) => convert_create_table(ct),
        sp::Statement::CreateIndex(ci) => convert_create_index(ci),
        sp::Statement::Drop {
            object_type: sp::ObjectType::Table,
            if_exists,
            names,
            ..
        } => {
            if names.len() != 1 {
                return Err(SqlError::Unsupported("multi-table DROP".into()));
            }
            Ok(Statement::DropTable(DropTableStmt {
                name: object_name_to_string(&names[0]),
                if_exists,
            }))
        }
        sp::Statement::Drop {
            object_type: sp::ObjectType::Index,
            if_exists,
            names,
            ..
        } => {
            if names.len() != 1 {
                return Err(SqlError::Unsupported("multi-index DROP".into()));
            }
            Ok(Statement::DropIndex(DropIndexStmt {
                index_name: object_name_to_string(&names[0]),
                if_exists,
            }))
        }
        sp::Statement::CreateView(cv) => convert_create_view(cv),
        sp::Statement::Drop {
            object_type: sp::ObjectType::View,
            if_exists,
            names,
            ..
        } => {
            if names.len() != 1 {
                return Err(SqlError::Unsupported("multi-view DROP".into()));
            }
            Ok(Statement::DropView(DropViewStmt {
                name: object_name_to_string(&names[0]),
                if_exists,
            }))
        }
        sp::Statement::AlterTable(at) => convert_alter_table(at),
        sp::Statement::Insert(insert) => convert_insert(insert),
        sp::Statement::Query(query) => convert_query(*query),
        sp::Statement::Update(update) => convert_update(update),
        sp::Statement::Delete(delete) => convert_delete(delete),
        sp::Statement::StartTransaction { .. } => Ok(Statement::Begin),
        sp::Statement::Commit { chain: true, .. } => {
            Err(SqlError::Unsupported("COMMIT AND CHAIN".into()))
        }
        sp::Statement::Commit { .. } => Ok(Statement::Commit),
        sp::Statement::Rollback { chain: true, .. } => {
            Err(SqlError::Unsupported("ROLLBACK AND CHAIN".into()))
        }
        sp::Statement::Rollback {
            savepoint: Some(name),
            ..
        } => Ok(Statement::RollbackTo(name.value.to_ascii_lowercase())),
        sp::Statement::Rollback { .. } => Ok(Statement::Rollback),
        sp::Statement::Savepoint { name } => {
            Ok(Statement::Savepoint(name.value.to_ascii_lowercase()))
        }
        sp::Statement::ReleaseSavepoint { name } => {
            Ok(Statement::ReleaseSavepoint(name.value.to_ascii_lowercase()))
        }
        sp::Statement::Explain {
            statement, analyze, ..
        } => {
            if analyze {
                return Err(SqlError::Unsupported("EXPLAIN ANALYZE".into()));
            }
            let inner = convert_statement(*statement)?;
            Ok(Statement::Explain(Box::new(inner)))
        }
        _ => Err(SqlError::Unsupported(format!("statement type: {}", stmt))),
    }
}

/// Parse column options (NOT NULL, DEFAULT, CHECK, FK) from a sqlparser ColumnDef.
/// Returns (ColumnSpec, Option<ForeignKeyDef>, was_inline_pk).
fn convert_column_def(
    col_def: &sp::ColumnDef,
) -> Result<(ColumnSpec, Option<ForeignKeyDef>, bool)> {
    let col_name = col_def.name.value.clone();
    let data_type = convert_data_type(&col_def.data_type)?;
    let mut nullable = true;
    let mut is_primary_key = false;
    let mut default_expr = None;
    let mut default_sql = None;
    let mut check_expr = None;
    let mut check_sql = None;
    let mut check_name = None;
    let mut fk_def = None;

    for opt in &col_def.options {
        match &opt.option {
            sp::ColumnOption::NotNull => nullable = false,
            sp::ColumnOption::Null => nullable = true,
            sp::ColumnOption::PrimaryKey(_) => {
                is_primary_key = true;
                nullable = false;
            }
            sp::ColumnOption::Default(expr) => {
                default_sql = Some(expr.to_string());
                default_expr = Some(convert_expr(expr)?);
            }
            sp::ColumnOption::Check(check) => {
                check_sql = Some(check.expr.to_string());
                let converted = convert_expr(&check.expr)?;
                if has_subquery(&converted) {
                    return Err(SqlError::Unsupported("subquery in CHECK constraint".into()));
                }
                check_expr = Some(converted);
                check_name = check.name.as_ref().map(|n| n.value.clone());
            }
            sp::ColumnOption::ForeignKey(fk) => {
                convert_fk_actions(&fk.on_delete, &fk.on_update)?;
                let ftable = object_name_to_string(&fk.foreign_table).to_ascii_lowercase();
                let referred: Vec<String> = fk
                    .referred_columns
                    .iter()
                    .map(|i| i.value.to_ascii_lowercase())
                    .collect();
                fk_def = Some(ForeignKeyDef {
                    name: fk.name.as_ref().map(|n| n.value.clone()),
                    columns: vec![col_name.to_ascii_lowercase()],
                    foreign_table: ftable,
                    referred_columns: referred,
                });
            }
            _ => {}
        }
    }

    let spec = ColumnSpec {
        name: col_name,
        data_type,
        nullable,
        is_primary_key,
        default_expr,
        default_sql,
        check_expr,
        check_sql,
        check_name,
    };
    Ok((spec, fk_def, is_primary_key))
}

fn convert_create_table(ct: sp::CreateTable) -> Result<Statement> {
    let name = object_name_to_string(&ct.name);
    let if_not_exists = ct.if_not_exists;

    let mut columns = Vec::new();
    let mut inline_pk: Vec<String> = Vec::new();
    let mut foreign_keys: Vec<ForeignKeyDef> = Vec::new();

    for col_def in &ct.columns {
        let (spec, fk_def, was_pk) = convert_column_def(col_def)?;
        if was_pk {
            inline_pk.push(spec.name.clone());
        }
        if let Some(fk) = fk_def {
            foreign_keys.push(fk);
        }
        columns.push(spec);
    }

    // Check table-level constraints
    let mut check_constraints: Vec<TableCheckConstraint> = Vec::new();

    for constraint in &ct.constraints {
        match constraint {
            sp::TableConstraint::PrimaryKey(pk_constraint) => {
                for idx_col in &pk_constraint.columns {
                    let col_name = match &idx_col.column.expr {
                        sp::Expr::Identifier(ident) => ident.value.clone(),
                        _ => continue,
                    };
                    if !inline_pk.contains(&col_name) {
                        inline_pk.push(col_name.clone());
                    }
                    if let Some(col) = columns.iter_mut().find(|c| c.name == col_name) {
                        col.nullable = false;
                        col.is_primary_key = true;
                    }
                }
            }
            sp::TableConstraint::Check(check) => {
                let sql = check.expr.to_string();
                let converted = convert_expr(&check.expr)?;
                if has_subquery(&converted) {
                    return Err(SqlError::Unsupported("subquery in CHECK constraint".into()));
                }
                check_constraints.push(TableCheckConstraint {
                    name: check.name.as_ref().map(|n| n.value.clone()),
                    expr: converted,
                    sql,
                });
            }
            sp::TableConstraint::ForeignKey(fk) => {
                convert_fk_actions(&fk.on_delete, &fk.on_update)?;
                let cols: Vec<String> = fk
                    .columns
                    .iter()
                    .map(|i| i.value.to_ascii_lowercase())
                    .collect();
                let ftable = object_name_to_string(&fk.foreign_table).to_ascii_lowercase();
                let referred: Vec<String> = fk
                    .referred_columns
                    .iter()
                    .map(|i| i.value.to_ascii_lowercase())
                    .collect();
                foreign_keys.push(ForeignKeyDef {
                    name: fk.name.as_ref().map(|n| n.value.clone()),
                    columns: cols,
                    foreign_table: ftable,
                    referred_columns: referred,
                });
            }
            _ => {}
        }
    }

    Ok(Statement::CreateTable(CreateTableStmt {
        name,
        columns,
        primary_key: inline_pk,
        if_not_exists,
        check_constraints,
        foreign_keys,
    }))
}

fn convert_alter_table(at: sp::AlterTable) -> Result<Statement> {
    let table = object_name_to_string(&at.name);
    if at.operations.len() != 1 {
        return Err(SqlError::Unsupported(
            "ALTER TABLE with multiple operations".into(),
        ));
    }
    let op = match at.operations.into_iter().next().unwrap() {
        sp::AlterTableOperation::AddColumn {
            column_def,
            if_not_exists,
            ..
        } => {
            let (spec, fk, _was_pk) = convert_column_def(&column_def)?;
            AlterTableOp::AddColumn {
                column: Box::new(spec),
                foreign_key: fk,
                if_not_exists,
            }
        }
        sp::AlterTableOperation::DropColumn {
            column_names,
            if_exists,
            ..
        } => {
            if column_names.len() != 1 {
                return Err(SqlError::Unsupported(
                    "DROP COLUMN with multiple columns".into(),
                ));
            }
            AlterTableOp::DropColumn {
                name: column_names.into_iter().next().unwrap().value,
                if_exists,
            }
        }
        sp::AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => AlterTableOp::RenameColumn {
            old_name: old_column_name.value,
            new_name: new_column_name.value,
        },
        sp::AlterTableOperation::RenameTable { table_name } => {
            let new_name = match table_name {
                sp::RenameTableNameKind::To(name) | sp::RenameTableNameKind::As(name) => {
                    object_name_to_string(&name)
                }
            };
            AlterTableOp::RenameTable { new_name }
        }
        other => {
            return Err(SqlError::Unsupported(format!(
                "ALTER TABLE operation: {other}"
            )));
        }
    };
    Ok(Statement::AlterTable(Box::new(AlterTableStmt {
        table,
        op,
    })))
}

fn convert_fk_actions(
    on_delete: &Option<sp::ReferentialAction>,
    on_update: &Option<sp::ReferentialAction>,
) -> Result<()> {
    for action in [on_delete, on_update] {
        match action {
            None
            | Some(sp::ReferentialAction::Restrict)
            | Some(sp::ReferentialAction::NoAction) => {}
            Some(other) => {
                return Err(SqlError::Unsupported(format!(
                    "FOREIGN KEY action: {other}"
                )));
            }
        }
    }
    Ok(())
}

fn convert_create_index(ci: sp::CreateIndex) -> Result<Statement> {
    let index_name = ci
        .name
        .as_ref()
        .map(object_name_to_string)
        .ok_or_else(|| SqlError::Parse("index name required".into()))?;

    let table_name = object_name_to_string(&ci.table_name);

    let columns: Vec<String> = ci
        .columns
        .iter()
        .map(|idx_col| match &idx_col.column.expr {
            sp::Expr::Identifier(ident) => Ok(ident.value.clone()),
            other => Err(SqlError::Unsupported(format!("expression index: {other}"))),
        })
        .collect::<Result<_>>()?;

    if columns.is_empty() {
        return Err(SqlError::Parse(
            "index must have at least one column".into(),
        ));
    }

    Ok(Statement::CreateIndex(CreateIndexStmt {
        index_name,
        table_name,
        columns,
        unique: ci.unique,
        if_not_exists: ci.if_not_exists,
    }))
}

fn convert_create_view(cv: sp::CreateView) -> Result<Statement> {
    let name = object_name_to_string(&cv.name);

    if cv.materialized {
        return Err(SqlError::Unsupported("MATERIALIZED VIEW".into()));
    }

    let sql = cv.query.to_string();

    // Validate the SQL is parseable as a SELECT
    let dialect = GenericDialect {};
    let test = Parser::parse_sql(&dialect, &sql).map_err(|e| SqlError::Parse(e.to_string()))?;
    if test.is_empty() {
        return Err(SqlError::Parse("empty view definition".into()));
    }
    match &test[0] {
        sp::Statement::Query(_) => {}
        _ => {
            return Err(SqlError::Parse(
                "view body must be a SELECT statement".into(),
            ))
        }
    }

    let column_aliases: Vec<String> = cv
        .columns
        .iter()
        .map(|c| c.name.value.to_ascii_lowercase())
        .collect();

    Ok(Statement::CreateView(CreateViewStmt {
        name,
        sql,
        column_aliases,
        or_replace: cv.or_replace,
        if_not_exists: cv.if_not_exists,
    }))
}

fn convert_insert(insert: sp::Insert) -> Result<Statement> {
    let table = match &insert.table {
        sp::TableObject::TableName(name) => object_name_to_string(name).to_ascii_lowercase(),
        _ => return Err(SqlError::Unsupported("INSERT into non-table object".into())),
    };

    let columns: Vec<String> = insert
        .columns
        .iter()
        .map(|c| c.value.to_ascii_lowercase())
        .collect();

    let query = insert
        .source
        .ok_or_else(|| SqlError::Parse("INSERT requires VALUES or SELECT".into()))?;

    let source = match *query.body {
        sp::SetExpr::Values(sp::Values { rows, .. }) => {
            let mut result = Vec::new();
            for row in rows {
                let mut exprs = Vec::new();
                for expr in row {
                    exprs.push(convert_expr(&expr)?);
                }
                result.push(exprs);
            }
            InsertSource::Values(result)
        }
        _ => {
            let (ctes, recursive) = if let Some(ref with) = query.with {
                convert_with(with)?
            } else {
                (vec![], false)
            };
            let body = convert_query_body(&query)?;
            InsertSource::Select(Box::new(SelectQuery {
                ctes,
                recursive,
                body,
            }))
        }
    };

    Ok(Statement::Insert(InsertStmt {
        table,
        columns,
        source,
    }))
}

fn convert_select_body(select: &sp::Select) -> Result<SelectStmt> {
    let distinct = match &select.distinct {
        Some(sp::Distinct::Distinct) => true,
        Some(sp::Distinct::On(_)) => {
            return Err(SqlError::Unsupported("DISTINCT ON".into()));
        }
        _ => false,
    };

    // FROM clause
    let (from, from_alias, joins) = if select.from.is_empty() {
        (String::new(), None, vec![])
    } else if select.from.len() == 1 {
        let table_with_joins = &select.from[0];
        let (name, alias) = match &table_with_joins.relation {
            sp::TableFactor::Table { name, alias, .. } => {
                let table_name = object_name_to_string(name);
                let alias_str = alias.as_ref().map(|a| a.name.value.clone());
                (table_name, alias_str)
            }
            _ => return Err(SqlError::Unsupported("non-table FROM source".into())),
        };
        let j = table_with_joins
            .joins
            .iter()
            .map(convert_join)
            .collect::<Result<Vec<_>>>()?;
        (name, alias, j)
    } else {
        return Err(SqlError::Unsupported("comma-separated FROM tables".into()));
    };

    // Projection
    let columns: Vec<SelectColumn> = select
        .projection
        .iter()
        .map(convert_select_item)
        .collect::<Result<_>>()?;

    // WHERE
    let where_clause = select.selection.as_ref().map(convert_expr).transpose()?;

    // GROUP BY
    let group_by = match &select.group_by {
        sp::GroupByExpr::Expressions(exprs, _) => {
            exprs.iter().map(convert_expr).collect::<Result<_>>()?
        }
        sp::GroupByExpr::All(_) => {
            return Err(SqlError::Unsupported("GROUP BY ALL".into()));
        }
    };

    // HAVING
    let having = select.having.as_ref().map(convert_expr).transpose()?;

    Ok(SelectStmt {
        columns,
        from,
        from_alias,
        joins,
        distinct,
        where_clause,
        order_by: vec![],
        limit: None,
        offset: None,
        group_by,
        having,
    })
}

fn convert_set_expr(set_expr: &sp::SetExpr) -> Result<QueryBody> {
    match set_expr {
        sp::SetExpr::Select(sel) => Ok(QueryBody::Select(Box::new(convert_select_body(sel)?))),
        sp::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let set_op = match op {
                sp::SetOperator::Union => SetOp::Union,
                sp::SetOperator::Intersect => SetOp::Intersect,
                sp::SetOperator::Except | sp::SetOperator::Minus => SetOp::Except,
            };
            let all = match set_quantifier {
                sp::SetQuantifier::All => true,
                sp::SetQuantifier::None | sp::SetQuantifier::Distinct => false,
                _ => {
                    return Err(SqlError::Unsupported("BY NAME set operations".into()));
                }
            };
            Ok(QueryBody::Compound(Box::new(CompoundSelect {
                op: set_op,
                all,
                left: Box::new(convert_set_expr(left)?),
                right: Box::new(convert_set_expr(right)?),
                order_by: vec![],
                limit: None,
                offset: None,
            })))
        }
        _ => Err(SqlError::Unsupported("unsupported set expression".into())),
    }
}

fn convert_query_body(query: &sp::Query) -> Result<QueryBody> {
    let mut body = convert_set_expr(&query.body)?;

    // ORDER BY
    let order_by = if let Some(ref ob) = query.order_by {
        match &ob.kind {
            sp::OrderByKind::Expressions(exprs) => exprs
                .iter()
                .map(convert_order_by_expr)
                .collect::<Result<_>>()?,
            sp::OrderByKind::All { .. } => {
                return Err(SqlError::Unsupported("ORDER BY ALL".into()));
            }
        }
    } else {
        vec![]
    };

    // LIMIT / OFFSET
    let (limit, offset) = match &query.limit_clause {
        Some(sp::LimitClause::LimitOffset { limit, offset, .. }) => {
            let l = limit.as_ref().map(convert_expr).transpose()?;
            let o = offset
                .as_ref()
                .map(|o| convert_expr(&o.value))
                .transpose()?;
            (l, o)
        }
        Some(sp::LimitClause::OffsetCommaLimit { limit, offset }) => {
            let l = Some(convert_expr(limit)?);
            let o = Some(convert_expr(offset)?);
            (l, o)
        }
        None => (None, None),
    };

    match &mut body {
        QueryBody::Select(sel) => {
            sel.order_by = order_by;
            sel.limit = limit;
            sel.offset = offset;
        }
        QueryBody::Compound(comp) => {
            comp.order_by = order_by;
            comp.limit = limit;
            comp.offset = offset;
        }
    }

    Ok(body)
}

fn convert_subquery(query: &sp::Query) -> Result<SelectStmt> {
    if query.with.is_some() {
        return Err(SqlError::Unsupported("CTEs in subqueries".into()));
    }
    match convert_query_body(query)? {
        QueryBody::Select(s) => Ok(*s),
        QueryBody::Compound(_) => Err(SqlError::Unsupported(
            "UNION/INTERSECT/EXCEPT in subqueries".into(),
        )),
    }
}

fn convert_with(with: &sp::With) -> Result<(Vec<CteDefinition>, bool)> {
    let mut names = std::collections::HashSet::new();
    let mut ctes = Vec::new();
    for cte in &with.cte_tables {
        let name = cte.alias.name.value.to_ascii_lowercase();
        if !names.insert(name.clone()) {
            return Err(SqlError::DuplicateCteName(name));
        }
        let column_aliases: Vec<String> = cte
            .alias
            .columns
            .iter()
            .map(|c| c.name.value.to_ascii_lowercase())
            .collect();
        let body = convert_query_body(&cte.query)?;
        ctes.push(CteDefinition {
            name,
            column_aliases,
            body,
        });
    }
    Ok((ctes, with.recursive))
}

fn convert_query(query: sp::Query) -> Result<Statement> {
    let (ctes, recursive) = if let Some(ref with) = query.with {
        convert_with(with)?
    } else {
        (vec![], false)
    };
    let body = convert_query_body(&query)?;
    Ok(Statement::Select(Box::new(SelectQuery {
        ctes,
        recursive,
        body,
    })))
}

fn convert_join(join: &sp::Join) -> Result<JoinClause> {
    let (join_type, constraint) = match &join.join_operator {
        sp::JoinOperator::Inner(c) => (JoinType::Inner, Some(c)),
        sp::JoinOperator::Join(c) => (JoinType::Inner, Some(c)),
        sp::JoinOperator::CrossJoin(c) => (JoinType::Cross, Some(c)),
        sp::JoinOperator::Left(c) => (JoinType::Left, Some(c)),
        sp::JoinOperator::LeftSemi(c) => (JoinType::Left, Some(c)),
        sp::JoinOperator::LeftAnti(c) => (JoinType::Left, Some(c)),
        sp::JoinOperator::Right(c) => (JoinType::Right, Some(c)),
        sp::JoinOperator::RightSemi(c) => (JoinType::Right, Some(c)),
        sp::JoinOperator::RightAnti(c) => (JoinType::Right, Some(c)),
        other => return Err(SqlError::Unsupported(format!("join type: {other:?}"))),
    };

    let (name, alias) = match &join.relation {
        sp::TableFactor::Table { name, alias, .. } => {
            let table_name = object_name_to_string(name);
            let alias_str = alias.as_ref().map(|a| a.name.value.clone());
            (table_name, alias_str)
        }
        _ => return Err(SqlError::Unsupported("non-table JOIN source".into())),
    };

    let on_clause = match constraint {
        Some(sp::JoinConstraint::On(expr)) => Some(convert_expr(expr)?),
        Some(sp::JoinConstraint::None) | None => None,
        Some(other) => return Err(SqlError::Unsupported(format!("join constraint: {other:?}"))),
    };

    Ok(JoinClause {
        join_type,
        table: TableRef { name, alias },
        on_clause,
    })
}

fn convert_update(update: sp::Update) -> Result<Statement> {
    let table = match &update.table.relation {
        sp::TableFactor::Table { name, .. } => object_name_to_string(name),
        _ => return Err(SqlError::Unsupported("non-table UPDATE target".into())),
    };

    let assignments = update
        .assignments
        .iter()
        .map(|a| {
            let col = match &a.target {
                sp::AssignmentTarget::ColumnName(name) => object_name_to_string(name),
                _ => return Err(SqlError::Unsupported("tuple assignment".into())),
            };
            let expr = convert_expr(&a.value)?;
            Ok((col, expr))
        })
        .collect::<Result<_>>()?;

    let where_clause = update.selection.as_ref().map(convert_expr).transpose()?;

    Ok(Statement::Update(UpdateStmt {
        table,
        assignments,
        where_clause,
    }))
}

fn convert_delete(delete: sp::Delete) -> Result<Statement> {
    let table_name = match &delete.from {
        sp::FromTable::WithFromKeyword(tables) => {
            if tables.len() != 1 {
                return Err(SqlError::Unsupported("multi-table DELETE".into()));
            }
            match &tables[0].relation {
                sp::TableFactor::Table { name, .. } => object_name_to_string(name),
                _ => return Err(SqlError::Unsupported("non-table DELETE target".into())),
            }
        }
        sp::FromTable::WithoutKeyword(tables) => {
            if tables.len() != 1 {
                return Err(SqlError::Unsupported("multi-table DELETE".into()));
            }
            match &tables[0].relation {
                sp::TableFactor::Table { name, .. } => object_name_to_string(name),
                _ => return Err(SqlError::Unsupported("non-table DELETE target".into())),
            }
        }
    };

    let where_clause = delete.selection.as_ref().map(convert_expr).transpose()?;

    Ok(Statement::Delete(DeleteStmt {
        table: table_name,
        where_clause,
    }))
}

// ── Expression conversion ───────────────────────────────────────────

fn convert_expr(expr: &sp::Expr) -> Result<Expr> {
    match expr {
        sp::Expr::Value(v) => convert_value(&v.value),
        sp::Expr::Identifier(ident) => Ok(Expr::Column(ident.value.to_ascii_lowercase())),
        sp::Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                Ok(Expr::QualifiedColumn {
                    table: parts[0].value.to_ascii_lowercase(),
                    column: parts[1].value.to_ascii_lowercase(),
                })
            } else {
                Ok(Expr::Column(
                    parts.last().unwrap().value.to_ascii_lowercase(),
                ))
            }
        }
        sp::Expr::BinaryOp { left, op, right } => {
            let bin_op = convert_bin_op(op)?;
            Ok(Expr::BinaryOp {
                left: Box::new(convert_expr(left)?),
                op: bin_op,
                right: Box::new(convert_expr(right)?),
            })
        }
        sp::Expr::UnaryOp { op, expr } => {
            let unary_op = match op {
                sp::UnaryOperator::Minus => UnaryOp::Neg,
                sp::UnaryOperator::Not => UnaryOp::Not,
                _ => return Err(SqlError::Unsupported(format!("unary op: {op}"))),
            };
            Ok(Expr::UnaryOp {
                op: unary_op,
                expr: Box::new(convert_expr(expr)?),
            })
        }
        sp::Expr::IsNull(e) => Ok(Expr::IsNull(Box::new(convert_expr(e)?))),
        sp::Expr::IsNotNull(e) => Ok(Expr::IsNotNull(Box::new(convert_expr(e)?))),
        sp::Expr::Nested(e) => convert_expr(e),
        sp::Expr::Function(func) => convert_function(func),
        sp::Expr::InSubquery {
            expr: e,
            subquery,
            negated,
        } => {
            let inner_expr = convert_expr(e)?;
            let stmt = convert_subquery(subquery)?;
            Ok(Expr::InSubquery {
                expr: Box::new(inner_expr),
                subquery: Box::new(stmt),
                negated: *negated,
            })
        }
        sp::Expr::InList {
            expr: e,
            list,
            negated,
        } => {
            let inner_expr = convert_expr(e)?;
            let items = list.iter().map(convert_expr).collect::<Result<Vec<_>>>()?;
            Ok(Expr::InList {
                expr: Box::new(inner_expr),
                list: items,
                negated: *negated,
            })
        }
        sp::Expr::Exists { subquery, negated } => {
            let stmt = convert_subquery(subquery)?;
            Ok(Expr::Exists {
                subquery: Box::new(stmt),
                negated: *negated,
            })
        }
        sp::Expr::Subquery(query) => {
            let stmt = convert_subquery(query)?;
            Ok(Expr::ScalarSubquery(Box::new(stmt)))
        }
        sp::Expr::Between {
            expr: e,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(convert_expr(e)?),
            low: Box::new(convert_expr(low)?),
            high: Box::new(convert_expr(high)?),
            negated: *negated,
        }),
        sp::Expr::Like {
            expr: e,
            negated,
            pattern,
            escape_char,
            ..
        } => {
            let esc = escape_char
                .as_ref()
                .map(convert_escape_value)
                .transpose()?
                .map(Box::new);
            Ok(Expr::Like {
                expr: Box::new(convert_expr(e)?),
                pattern: Box::new(convert_expr(pattern)?),
                escape: esc,
                negated: *negated,
            })
        }
        sp::Expr::ILike {
            expr: e,
            negated,
            pattern,
            escape_char,
            ..
        } => {
            let esc = escape_char
                .as_ref()
                .map(convert_escape_value)
                .transpose()?
                .map(Box::new);
            Ok(Expr::Like {
                expr: Box::new(convert_expr(e)?),
                pattern: Box::new(convert_expr(pattern)?),
                escape: esc,
                negated: *negated,
            })
        }
        sp::Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let op = operand
                .as_ref()
                .map(|e| convert_expr(e))
                .transpose()?
                .map(Box::new);
            let conds: Vec<(Expr, Expr)> = conditions
                .iter()
                .map(|cw| Ok((convert_expr(&cw.condition)?, convert_expr(&cw.result)?)))
                .collect::<Result<_>>()?;
            let else_r = else_result
                .as_ref()
                .map(|e| convert_expr(e))
                .transpose()?
                .map(Box::new);
            Ok(Expr::Case {
                operand: op,
                conditions: conds,
                else_result: else_r,
            })
        }
        sp::Expr::Cast {
            expr: e,
            data_type: dt,
            ..
        } => {
            let target = convert_data_type(dt)?;
            Ok(Expr::Cast {
                expr: Box::new(convert_expr(e)?),
                data_type: target,
            })
        }
        sp::Expr::Substring {
            expr: e,
            substring_from,
            substring_for,
            ..
        } => {
            let mut args = vec![convert_expr(e)?];
            if let Some(from) = substring_from {
                args.push(convert_expr(from)?);
            }
            if let Some(f) = substring_for {
                args.push(convert_expr(f)?);
            }
            Ok(Expr::Function {
                name: "SUBSTR".into(),
                args,
            })
        }
        sp::Expr::Trim {
            expr: e,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            let fn_name = match trim_where {
                Some(sp::TrimWhereField::Leading) => "LTRIM",
                Some(sp::TrimWhereField::Trailing) => "RTRIM",
                _ => "TRIM",
            };
            let mut args = vec![convert_expr(e)?];
            if let Some(what) = trim_what {
                args.push(convert_expr(what)?);
            } else if let Some(chars) = trim_characters {
                if let Some(first) = chars.first() {
                    args.push(convert_expr(first)?);
                }
            }
            Ok(Expr::Function {
                name: fn_name.into(),
                args,
            })
        }
        sp::Expr::Ceil { expr: e, .. } => Ok(Expr::Function {
            name: "CEIL".into(),
            args: vec![convert_expr(e)?],
        }),
        sp::Expr::Floor { expr: e, .. } => Ok(Expr::Function {
            name: "FLOOR".into(),
            args: vec![convert_expr(e)?],
        }),
        sp::Expr::Position { expr: e, r#in } => Ok(Expr::Function {
            name: "INSTR".into(),
            args: vec![convert_expr(r#in)?, convert_expr(e)?],
        }),
        _ => Err(SqlError::Unsupported(format!("expression: {expr}"))),
    }
}

fn convert_value(val: &sp::Value) -> Result<Expr> {
    match val {
        sp::Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(Expr::Literal(Value::Integer(i)))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(Expr::Literal(Value::Real(f)))
            } else {
                Err(SqlError::InvalidValue(format!("cannot parse number: {n}")))
            }
        }
        sp::Value::SingleQuotedString(s) => Ok(Expr::Literal(Value::Text(s.as_str().into()))),
        sp::Value::Boolean(b) => Ok(Expr::Literal(Value::Boolean(*b))),
        sp::Value::Null => Ok(Expr::Literal(Value::Null)),
        sp::Value::Placeholder(s) => {
            let idx_str = s
                .strip_prefix('$')
                .ok_or_else(|| SqlError::Parse(format!("invalid placeholder: {s}")))?;
            let idx: usize = idx_str
                .parse()
                .map_err(|_| SqlError::Parse(format!("invalid placeholder index: {s}")))?;
            if idx == 0 {
                return Err(SqlError::Parse("placeholder index must be >= 1".into()));
            }
            Ok(Expr::Parameter(idx))
        }
        _ => Err(SqlError::Unsupported(format!("value type: {val}"))),
    }
}

fn convert_escape_value(val: &sp::Value) -> Result<Expr> {
    match val {
        sp::Value::SingleQuotedString(s) => Ok(Expr::Literal(Value::Text(s.as_str().into()))),
        _ => Err(SqlError::Unsupported(format!("ESCAPE value: {val}"))),
    }
}

fn convert_bin_op(op: &sp::BinaryOperator) -> Result<BinOp> {
    match op {
        sp::BinaryOperator::Plus => Ok(BinOp::Add),
        sp::BinaryOperator::Minus => Ok(BinOp::Sub),
        sp::BinaryOperator::Multiply => Ok(BinOp::Mul),
        sp::BinaryOperator::Divide => Ok(BinOp::Div),
        sp::BinaryOperator::Modulo => Ok(BinOp::Mod),
        sp::BinaryOperator::Eq => Ok(BinOp::Eq),
        sp::BinaryOperator::NotEq => Ok(BinOp::NotEq),
        sp::BinaryOperator::Lt => Ok(BinOp::Lt),
        sp::BinaryOperator::Gt => Ok(BinOp::Gt),
        sp::BinaryOperator::LtEq => Ok(BinOp::LtEq),
        sp::BinaryOperator::GtEq => Ok(BinOp::GtEq),
        sp::BinaryOperator::And => Ok(BinOp::And),
        sp::BinaryOperator::Or => Ok(BinOp::Or),
        sp::BinaryOperator::StringConcat => Ok(BinOp::Concat),
        _ => Err(SqlError::Unsupported(format!("binary op: {op}"))),
    }
}

fn convert_function(func: &sp::Function) -> Result<Expr> {
    let name = object_name_to_string(&func.name).to_ascii_uppercase();

    let (args, is_count_star) = match &func.args {
        sp::FunctionArguments::List(list) => {
            if list.args.is_empty() && name == "COUNT" {
                (vec![], true)
            } else {
                let mut count_star = false;
                let args = list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e)) => convert_expr(e),
                        sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Wildcard) => {
                            if name == "COUNT" {
                                count_star = true;
                                Ok(Expr::CountStar)
                            } else {
                                Err(SqlError::Unsupported(format!("{name}(*)")))
                            }
                        }
                        _ => Err(SqlError::Unsupported(format!(
                            "function arg type in {name}"
                        ))),
                    })
                    .collect::<Result<Vec<_>>>()?;
                if name == "COUNT" && args.len() == 1 && count_star {
                    (vec![], true)
                } else {
                    (args, false)
                }
            }
        }
        sp::FunctionArguments::None => {
            if name == "COUNT" {
                (vec![], true)
            } else {
                (vec![], false)
            }
        }
        sp::FunctionArguments::Subquery(_) => {
            return Err(SqlError::Unsupported("subquery in function".into()));
        }
    };

    // Window function: check OVER before any other special handling
    if let Some(over) = &func.over {
        let spec = match over {
            sp::WindowType::WindowSpec(ws) => convert_window_spec(ws)?,
            sp::WindowType::NamedWindow(_) => {
                return Err(SqlError::Unsupported("named windows".into()));
            }
        };
        return Ok(Expr::WindowFunction { name, args, spec });
    }

    // Non-window special forms
    if is_count_star {
        return Ok(Expr::CountStar);
    }

    if name == "COALESCE" {
        if args.is_empty() {
            return Err(SqlError::Parse(
                "COALESCE requires at least one argument".into(),
            ));
        }
        return Ok(Expr::Coalesce(args));
    }

    if name == "NULLIF" {
        if args.len() != 2 {
            return Err(SqlError::Parse(
                "NULLIF requires exactly two arguments".into(),
            ));
        }
        return Ok(Expr::Case {
            operand: None,
            conditions: vec![(
                Expr::BinaryOp {
                    left: Box::new(args[0].clone()),
                    op: BinOp::Eq,
                    right: Box::new(args[1].clone()),
                },
                Expr::Literal(Value::Null),
            )],
            else_result: Some(Box::new(args[0].clone())),
        });
    }

    if name == "IIF" {
        if args.len() != 3 {
            return Err(SqlError::Parse(
                "IIF requires exactly three arguments".into(),
            ));
        }
        return Ok(Expr::Case {
            operand: None,
            conditions: vec![(args[0].clone(), args[1].clone())],
            else_result: Some(Box::new(args[2].clone())),
        });
    }

    Ok(Expr::Function { name, args })
}

fn convert_window_spec(ws: &sp::WindowSpec) -> Result<WindowSpec> {
    let partition_by = ws
        .partition_by
        .iter()
        .map(convert_expr)
        .collect::<Result<Vec<_>>>()?;
    let order_by = ws
        .order_by
        .iter()
        .map(convert_order_by_expr)
        .collect::<Result<Vec<_>>>()?;
    let frame = ws
        .window_frame
        .as_ref()
        .map(convert_window_frame)
        .transpose()?;
    Ok(WindowSpec {
        partition_by,
        order_by,
        frame,
    })
}

fn convert_window_frame(wf: &sp::WindowFrame) -> Result<WindowFrame> {
    let units = match wf.units {
        sp::WindowFrameUnits::Rows => WindowFrameUnits::Rows,
        sp::WindowFrameUnits::Range => WindowFrameUnits::Range,
        sp::WindowFrameUnits::Groups => {
            return Err(SqlError::Unsupported("GROUPS window frame".into()));
        }
    };
    let start = convert_window_frame_bound(&wf.start_bound)?;
    let end = match &wf.end_bound {
        Some(b) => convert_window_frame_bound(b)?,
        None => WindowFrameBound::CurrentRow,
    };
    Ok(WindowFrame { units, start, end })
}

fn convert_window_frame_bound(b: &sp::WindowFrameBound) -> Result<WindowFrameBound> {
    match b {
        sp::WindowFrameBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        sp::WindowFrameBound::Preceding(None) => Ok(WindowFrameBound::UnboundedPreceding),
        sp::WindowFrameBound::Preceding(Some(e)) => {
            Ok(WindowFrameBound::Preceding(Box::new(convert_expr(e)?)))
        }
        sp::WindowFrameBound::Following(None) => Ok(WindowFrameBound::UnboundedFollowing),
        sp::WindowFrameBound::Following(Some(e)) => {
            Ok(WindowFrameBound::Following(Box::new(convert_expr(e)?)))
        }
    }
}

fn convert_select_item(item: &sp::SelectItem) -> Result<SelectColumn> {
    match item {
        sp::SelectItem::Wildcard(_) => Ok(SelectColumn::AllColumns),
        sp::SelectItem::UnnamedExpr(e) => {
            let expr = convert_expr(e)?;
            Ok(SelectColumn::Expr { expr, alias: None })
        }
        sp::SelectItem::ExprWithAlias { expr, alias } => {
            let expr = convert_expr(expr)?;
            Ok(SelectColumn::Expr {
                expr,
                alias: Some(alias.value.clone()),
            })
        }
        sp::SelectItem::QualifiedWildcard(_, _) => {
            Err(SqlError::Unsupported("qualified wildcard (table.*)".into()))
        }
    }
}

fn convert_order_by_expr(expr: &sp::OrderByExpr) -> Result<OrderByItem> {
    let e = convert_expr(&expr.expr)?;
    let descending = expr.options.asc.map(|asc| !asc).unwrap_or(false);
    let nulls_first = expr.options.nulls_first;

    Ok(OrderByItem {
        expr: e,
        descending,
        nulls_first,
    })
}

// ── Data type conversion ────────────────────────────────────────────

fn convert_data_type(dt: &sp::DataType) -> Result<DataType> {
    match dt {
        sp::DataType::Int(_)
        | sp::DataType::Integer(_)
        | sp::DataType::BigInt(_)
        | sp::DataType::SmallInt(_)
        | sp::DataType::TinyInt(_)
        | sp::DataType::Int2(_)
        | sp::DataType::Int4(_)
        | sp::DataType::Int8(_) => Ok(DataType::Integer),

        sp::DataType::Real
        | sp::DataType::Double(..)
        | sp::DataType::DoublePrecision
        | sp::DataType::Float(_)
        | sp::DataType::Float4
        | sp::DataType::Float64 => Ok(DataType::Real),

        sp::DataType::Varchar(_)
        | sp::DataType::Text
        | sp::DataType::Char(_)
        | sp::DataType::Character(_)
        | sp::DataType::String(_) => Ok(DataType::Text),

        sp::DataType::Blob(_) | sp::DataType::Bytea => Ok(DataType::Blob),

        sp::DataType::Boolean | sp::DataType::Bool => Ok(DataType::Boolean),

        _ => Err(SqlError::Unsupported(format!("data type: {dt}"))),
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn object_name_to_string(name: &sp::ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|p| match p {
            sp::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_table() {
        let stmt = parse_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)",
        )
        .unwrap();

        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.name, "users");
                assert_eq!(ct.columns.len(), 3);
                assert_eq!(ct.columns[0].name, "id");
                assert_eq!(ct.columns[0].data_type, DataType::Integer);
                assert!(ct.columns[0].is_primary_key);
                assert!(!ct.columns[0].nullable);
                assert_eq!(ct.columns[1].name, "name");
                assert_eq!(ct.columns[1].data_type, DataType::Text);
                assert!(!ct.columns[1].nullable);
                assert_eq!(ct.columns[2].name, "age");
                assert!(ct.columns[2].nullable);
                assert_eq!(ct.primary_key, vec!["id"]);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_create_table_if_not_exists() {
        let stmt = parse_sql("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY)").unwrap();
        match stmt {
            Statement::CreateTable(ct) => assert!(ct.if_not_exists),
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_drop_table() {
        let stmt = parse_sql("DROP TABLE users").unwrap();
        match stmt {
            Statement::DropTable(dt) => {
                assert_eq!(dt.name, "users");
                assert!(!dt.if_exists);
            }
            _ => panic!("expected DropTable"),
        }
    }

    #[test]
    fn parse_drop_table_if_exists() {
        let stmt = parse_sql("DROP TABLE IF EXISTS users").unwrap();
        match stmt {
            Statement::DropTable(dt) => assert!(dt.if_exists),
            _ => panic!("expected DropTable"),
        }
    }

    #[test]
    fn parse_insert() {
        let stmt =
            parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.table, "users");
                assert_eq!(ins.columns, vec!["id", "name"]);
                let values = match &ins.source {
                    InsertSource::Values(v) => v,
                    _ => panic!("expected Values"),
                };
                assert_eq!(values.len(), 2);
                assert!(matches!(values[0][0], Expr::Literal(Value::Integer(1))));
                assert!(matches!(&values[0][1], Expr::Literal(Value::Text(s)) if s == "Alice"));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_select_all() {
        let stmt = parse_sql("SELECT * FROM users").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert_eq!(sel.from, "users");
                    assert!(matches!(sel.columns[0], SelectColumn::AllColumns));
                    assert!(sel.where_clause.is_none());
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_where() {
        let stmt = parse_sql("SELECT id, name FROM users WHERE age > 18").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert_eq!(sel.columns.len(), 2);
                    assert!(sel.where_clause.is_some());
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_order_limit() {
        let stmt = parse_sql("SELECT * FROM users ORDER BY name ASC LIMIT 10 OFFSET 5").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert_eq!(sel.order_by.len(), 1);
                    assert!(!sel.order_by[0].descending);
                    assert!(sel.limit.is_some());
                    assert!(sel.offset.is_some());
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_update() {
        let stmt = parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
        match stmt {
            Statement::Update(upd) => {
                assert_eq!(upd.table, "users");
                assert_eq!(upd.assignments.len(), 1);
                assert_eq!(upd.assignments[0].0, "name");
                assert!(upd.where_clause.is_some());
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn parse_delete() {
        let stmt = parse_sql("DELETE FROM users WHERE id = 1").unwrap();
        match stmt {
            Statement::Delete(del) => {
                assert_eq!(del.table, "users");
                assert!(del.where_clause.is_some());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn parse_aggregate() {
        let stmt = parse_sql("SELECT COUNT(*), SUM(age) FROM users").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert_eq!(sel.columns.len(), 2);
                    match &sel.columns[0] {
                        SelectColumn::Expr {
                            expr: Expr::CountStar,
                            ..
                        } => {}
                        other => panic!("expected CountStar, got {other:?}"),
                    }
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_group_by_having() {
        let stmt = parse_sql(
            "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5",
        )
        .unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert_eq!(sel.group_by.len(), 1);
                    assert!(sel.having.is_some());
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_expressions() {
        let stmt = parse_sql("SELECT id + 1, -price, NOT active FROM items").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert_eq!(sel.columns.len(), 3);
                    // id + 1
                    match &sel.columns[0] {
                        SelectColumn::Expr {
                            expr: Expr::BinaryOp { op: BinOp::Add, .. },
                            ..
                        } => {}
                        other => panic!("expected BinaryOp Add, got {other:?}"),
                    }
                    // -price
                    match &sel.columns[1] {
                        SelectColumn::Expr {
                            expr:
                                Expr::UnaryOp {
                                    op: UnaryOp::Neg, ..
                                },
                            ..
                        } => {}
                        other => panic!("expected UnaryOp Neg, got {other:?}"),
                    }
                    // NOT active
                    match &sel.columns[2] {
                        SelectColumn::Expr {
                            expr:
                                Expr::UnaryOp {
                                    op: UnaryOp::Not, ..
                                },
                            ..
                        } => {}
                        other => panic!("expected UnaryOp Not, got {other:?}"),
                    }
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_is_null() {
        let stmt = parse_sql("SELECT * FROM t WHERE x IS NULL").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert!(matches!(sel.where_clause, Some(Expr::IsNull(_))));
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_inner_join() {
        let stmt = parse_sql("SELECT * FROM a JOIN b ON a.id = b.id").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert_eq!(sel.from, "a");
                    assert_eq!(sel.joins.len(), 1);
                    assert_eq!(sel.joins[0].join_type, JoinType::Inner);
                    assert_eq!(sel.joins[0].table.name, "b");
                    assert!(sel.joins[0].on_clause.is_some());
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_inner_join_explicit() {
        let stmt = parse_sql("SELECT * FROM a INNER JOIN b ON a.id = b.a_id").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert_eq!(sel.joins.len(), 1);
                    assert_eq!(sel.joins[0].join_type, JoinType::Inner);
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_cross_join() {
        let stmt = parse_sql("SELECT * FROM a CROSS JOIN b").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert_eq!(sel.joins.len(), 1);
                    assert_eq!(sel.joins[0].join_type, JoinType::Cross);
                    assert!(sel.joins[0].on_clause.is_none());
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_left_join() {
        let stmt = parse_sql("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert_eq!(sel.joins.len(), 1);
                    assert_eq!(sel.joins[0].join_type, JoinType::Left);
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_table_alias() {
        let stmt = parse_sql("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert_eq!(sel.from, "users");
                    assert_eq!(sel.from_alias.as_deref(), Some("u"));
                    assert_eq!(sel.joins[0].table.name, "orders");
                    assert_eq!(sel.joins[0].table.alias.as_deref(), Some("o"));
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_multi_join() {
        let stmt =
            parse_sql("SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert_eq!(sel.joins.len(), 2);
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_qualified_column() {
        let stmt = parse_sql("SELECT u.id, u.name FROM users u").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => match &sel.columns[0] {
                    SelectColumn::Expr {
                        expr: Expr::QualifiedColumn { table, column },
                        ..
                    } => {
                        assert_eq!(table, "u");
                        assert_eq!(column, "id");
                    }
                    other => panic!("expected QualifiedColumn, got {other:?}"),
                },
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn reject_subquery() {
        assert!(parse_sql("SELECT * FROM (SELECT 1)").is_err());
    }

    #[test]
    fn parse_type_mapping() {
        let stmt = parse_sql(
            "CREATE TABLE t (a INT PRIMARY KEY, b BIGINT, c SMALLINT, d REAL, e DOUBLE PRECISION, f VARCHAR(255), g BOOLEAN, h BLOB, i BYTEA)"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, DataType::Integer); // INT
                assert_eq!(ct.columns[1].data_type, DataType::Integer); // BIGINT
                assert_eq!(ct.columns[2].data_type, DataType::Integer); // SMALLINT
                assert_eq!(ct.columns[3].data_type, DataType::Real); // REAL
                assert_eq!(ct.columns[4].data_type, DataType::Real); // DOUBLE
                assert_eq!(ct.columns[5].data_type, DataType::Text); // VARCHAR
                assert_eq!(ct.columns[6].data_type, DataType::Boolean); // BOOLEAN
                assert_eq!(ct.columns[7].data_type, DataType::Blob); // BLOB
                assert_eq!(ct.columns[8].data_type, DataType::Blob); // BYTEA
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_boolean_literals() {
        let stmt = parse_sql("INSERT INTO t (a, b) VALUES (true, false)").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                let values = match &ins.source {
                    InsertSource::Values(v) => v,
                    _ => panic!("expected Values"),
                };
                assert!(matches!(values[0][0], Expr::Literal(Value::Boolean(true))));
                assert!(matches!(values[0][1], Expr::Literal(Value::Boolean(false))));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_null_literal() {
        let stmt = parse_sql("INSERT INTO t (a) VALUES (NULL)").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                let values = match &ins.source {
                    InsertSource::Values(v) => v,
                    _ => panic!("expected Values"),
                };
                assert!(matches!(values[0][0], Expr::Literal(Value::Null)));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_alias() {
        let stmt = parse_sql("SELECT id AS user_id FROM users").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => match &sel.columns[0] {
                    SelectColumn::Expr { alias: Some(a), .. } => assert_eq!(a, "user_id"),
                    other => panic!("expected alias, got {other:?}"),
                },
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_begin() {
        let stmt = parse_sql("BEGIN").unwrap();
        assert!(matches!(stmt, Statement::Begin));
    }

    #[test]
    fn parse_begin_transaction() {
        let stmt = parse_sql("BEGIN TRANSACTION").unwrap();
        assert!(matches!(stmt, Statement::Begin));
    }

    #[test]
    fn parse_commit() {
        let stmt = parse_sql("COMMIT").unwrap();
        assert!(matches!(stmt, Statement::Commit));
    }

    #[test]
    fn parse_rollback() {
        let stmt = parse_sql("ROLLBACK").unwrap();
        assert!(matches!(stmt, Statement::Rollback));
    }

    #[test]
    fn parse_savepoint() {
        let stmt = parse_sql("SAVEPOINT sp1").unwrap();
        match stmt {
            Statement::Savepoint(name) => assert_eq!(name, "sp1"),
            other => panic!("expected Savepoint, got {other:?}"),
        }
    }

    #[test]
    fn parse_savepoint_case_insensitive() {
        let stmt = parse_sql("SAVEPOINT My_SP").unwrap();
        match stmt {
            Statement::Savepoint(name) => assert_eq!(name, "my_sp"),
            other => panic!("expected Savepoint, got {other:?}"),
        }
    }

    #[test]
    fn parse_release_savepoint() {
        let stmt = parse_sql("RELEASE SAVEPOINT sp1").unwrap();
        match stmt {
            Statement::ReleaseSavepoint(name) => assert_eq!(name, "sp1"),
            other => panic!("expected ReleaseSavepoint, got {other:?}"),
        }
    }

    #[test]
    fn parse_release_without_savepoint_keyword() {
        let stmt = parse_sql("RELEASE sp1").unwrap();
        match stmt {
            Statement::ReleaseSavepoint(name) => assert_eq!(name, "sp1"),
            other => panic!("expected ReleaseSavepoint, got {other:?}"),
        }
    }

    #[test]
    fn parse_rollback_to_savepoint() {
        let stmt = parse_sql("ROLLBACK TO SAVEPOINT sp1").unwrap();
        match stmt {
            Statement::RollbackTo(name) => assert_eq!(name, "sp1"),
            other => panic!("expected RollbackTo, got {other:?}"),
        }
    }

    #[test]
    fn parse_rollback_to_without_savepoint_keyword() {
        let stmt = parse_sql("ROLLBACK TO sp1").unwrap();
        match stmt {
            Statement::RollbackTo(name) => assert_eq!(name, "sp1"),
            other => panic!("expected RollbackTo, got {other:?}"),
        }
    }

    #[test]
    fn parse_rollback_to_case_insensitive() {
        let stmt = parse_sql("ROLLBACK TO My_SP").unwrap();
        match stmt {
            Statement::RollbackTo(name) => assert_eq!(name, "my_sp"),
            other => panic!("expected RollbackTo, got {other:?}"),
        }
    }

    #[test]
    fn parse_commit_and_chain_rejected() {
        let err = parse_sql("COMMIT AND CHAIN").unwrap_err();
        assert!(matches!(err, SqlError::Unsupported(_)));
    }

    #[test]
    fn parse_rollback_and_chain_rejected() {
        let err = parse_sql("ROLLBACK AND CHAIN").unwrap_err();
        assert!(matches!(err, SqlError::Unsupported(_)));
    }

    #[test]
    fn parse_select_distinct() {
        let stmt = parse_sql("SELECT DISTINCT name FROM users").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert!(sel.distinct);
                    assert_eq!(sel.columns.len(), 1);
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_without_distinct() {
        let stmt = parse_sql("SELECT name FROM users").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert!(!sel.distinct);
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_distinct_all_columns() {
        let stmt = parse_sql("SELECT DISTINCT * FROM users").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => {
                    assert!(sel.distinct);
                    assert!(matches!(sel.columns[0], SelectColumn::AllColumns));
                }
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn reject_distinct_on() {
        assert!(parse_sql("SELECT DISTINCT ON (id) * FROM users").is_err());
    }

    #[test]
    fn parse_create_index() {
        let stmt = parse_sql("CREATE INDEX idx_name ON users (name)").unwrap();
        match stmt {
            Statement::CreateIndex(ci) => {
                assert_eq!(ci.index_name, "idx_name");
                assert_eq!(ci.table_name, "users");
                assert_eq!(ci.columns, vec!["name"]);
                assert!(!ci.unique);
                assert!(!ci.if_not_exists);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn parse_create_unique_index() {
        let stmt = parse_sql("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap();
        match stmt {
            Statement::CreateIndex(ci) => {
                assert!(ci.unique);
                assert_eq!(ci.columns, vec!["email"]);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn parse_create_index_if_not_exists() {
        let stmt = parse_sql("CREATE INDEX IF NOT EXISTS idx_x ON t (a)").unwrap();
        match stmt {
            Statement::CreateIndex(ci) => assert!(ci.if_not_exists),
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn parse_create_index_multi_column() {
        let stmt = parse_sql("CREATE INDEX idx_multi ON t (a, b, c)").unwrap();
        match stmt {
            Statement::CreateIndex(ci) => {
                assert_eq!(ci.columns, vec!["a", "b", "c"]);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn parse_drop_index() {
        let stmt = parse_sql("DROP INDEX idx_name").unwrap();
        match stmt {
            Statement::DropIndex(di) => {
                assert_eq!(di.index_name, "idx_name");
                assert!(!di.if_exists);
            }
            _ => panic!("expected DropIndex"),
        }
    }

    #[test]
    fn parse_drop_index_if_exists() {
        let stmt = parse_sql("DROP INDEX IF EXISTS idx_name").unwrap();
        match stmt {
            Statement::DropIndex(di) => {
                assert!(di.if_exists);
            }
            _ => panic!("expected DropIndex"),
        }
    }

    #[test]
    fn parse_explain_select() {
        let stmt = parse_sql("EXPLAIN SELECT * FROM users WHERE id = 1").unwrap();
        match stmt {
            Statement::Explain(inner) => {
                assert!(matches!(*inner, Statement::Select(_)));
            }
            _ => panic!("expected Explain"),
        }
    }

    #[test]
    fn parse_explain_insert() {
        let stmt = parse_sql("EXPLAIN INSERT INTO t (a) VALUES (1)").unwrap();
        assert!(matches!(stmt, Statement::Explain(_)));
    }

    #[test]
    fn reject_explain_analyze() {
        assert!(parse_sql("EXPLAIN ANALYZE SELECT * FROM t").is_err());
    }

    #[test]
    fn parse_parameter_placeholder() {
        let stmt = parse_sql("SELECT * FROM t WHERE id = $1").unwrap();
        match stmt {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => match &sel.where_clause {
                    Some(Expr::BinaryOp { right, .. }) => {
                        assert!(matches!(right.as_ref(), Expr::Parameter(1)));
                    }
                    other => panic!("expected BinaryOp with Parameter, got {other:?}"),
                },
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_multiple_parameters() {
        let stmt = parse_sql("INSERT INTO t (a, b) VALUES ($1, $2)").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                let values = match &ins.source {
                    InsertSource::Values(v) => v,
                    _ => panic!("expected Values"),
                };
                assert!(matches!(values[0][0], Expr::Parameter(1)));
                assert!(matches!(values[0][1], Expr::Parameter(2)));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_insert_select() {
        let stmt =
            parse_sql("INSERT INTO t2 (id, name) SELECT id, name FROM t1 WHERE id > 5").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.table, "t2");
                assert_eq!(ins.columns, vec!["id", "name"]);
                match &ins.source {
                    InsertSource::Select(sq) => match &sq.body {
                        QueryBody::Select(sel) => {
                            assert_eq!(sel.from, "t1");
                            assert_eq!(sel.columns.len(), 2);
                            assert!(sel.where_clause.is_some());
                        }
                        _ => panic!("expected QueryBody::Select"),
                    },
                    _ => panic!("expected InsertSource::Select"),
                }
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_insert_select_no_columns() {
        let stmt = parse_sql("INSERT INTO t2 SELECT * FROM t1").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.table, "t2");
                assert!(ins.columns.is_empty());
                assert!(matches!(&ins.source, InsertSource::Select(_)));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn reject_zero_parameter() {
        assert!(parse_sql("SELECT $0 FROM t").is_err());
    }

    #[test]
    fn count_params_basic() {
        let stmt = parse_sql("SELECT * FROM t WHERE a = $1 AND b = $3").unwrap();
        assert_eq!(count_params(&stmt), 3);
    }

    #[test]
    fn count_params_none() {
        let stmt = parse_sql("SELECT * FROM t WHERE a = 1").unwrap();
        assert_eq!(count_params(&stmt), 0);
    }

    #[test]
    fn bind_params_basic() {
        let stmt = parse_sql("SELECT * FROM t WHERE id = $1").unwrap();
        let bound = bind_params(&stmt, &[Value::Integer(42)]).unwrap();
        match bound {
            Statement::Select(sq) => match sq.body {
                QueryBody::Select(sel) => match &sel.where_clause {
                    Some(Expr::BinaryOp { right, .. }) => {
                        assert!(matches!(right.as_ref(), Expr::Literal(Value::Integer(42))));
                    }
                    other => panic!("expected BinaryOp with Literal, got {other:?}"),
                },
                _ => panic!("expected QueryBody::Select"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn bind_params_out_of_range() {
        let stmt = parse_sql("SELECT * FROM t WHERE id = $2").unwrap();
        let result = bind_params(&stmt, &[Value::Integer(1)]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_table_constraint_pk() {
        let stmt = parse_sql("CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a))").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.primary_key, vec!["a"]);
                assert!(ct.columns[0].is_primary_key);
                assert!(!ct.columns[0].nullable);
            }
            _ => panic!("expected CreateTable"),
        }
    }
}
