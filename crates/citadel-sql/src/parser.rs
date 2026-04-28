//! SQL parser: converts SQL strings into the internal AST.

use sqlparser::ast as sp;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::{Result, SqlError};
use crate::types::{DataType, Value};

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
    SetTimezone(String),
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
    pub unique_indices: Vec<UniqueIndexDef>,
}

#[derive(Debug, Clone)]
pub struct UniqueIndexDef {
    pub name: Option<String>,
    pub columns: Vec<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratedKind {
    Stored,
    Virtual,
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
    pub generated_expr: Option<Expr>,
    pub generated_sql: Option<String>,
    pub generated_kind: Option<GeneratedKind>,
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
    pub on_conflict: Option<OnConflictClause>,
    pub returning: Option<Vec<SelectColumn>>,
}

#[derive(Debug, Clone)]
pub struct OnConflictClause {
    pub target: Option<ConflictTarget>,
    pub action: OnConflictAction,
}

#[derive(Debug, Clone)]
pub enum ConflictTarget {
    Columns(Vec<String>),
    Constraint(String),
}

#[derive(Debug, Clone)]
pub enum OnConflictAction {
    DoNothing,
    DoUpdate {
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
    },
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
    pub returning: Option<Vec<SelectColumn>>,
}

#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Expr>,
    pub returning: Option<Vec<SelectColumn>>,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
    AllColumns,
    AllFromOld,
    AllFromNew,
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
        /// True for aggregate forms like `COUNT(DISTINCT x)`.
        distinct: bool,
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
        values: rustc_hash::FxHashSet<Value>,
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

/// Parse one or more `;`-separated SQL statements.
pub fn parse_sql_multi(sql: &str) -> Result<Vec<Statement>> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Parse(e.to_string()))?;

    if stmts.is_empty() {
        return Err(SqlError::Parse("empty SQL".into()));
    }

    stmts.into_iter().map(convert_statement).collect()
}

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
        sp::Statement::Set(sp::Set::SetTimeZone { value, .. }) => {
            // Accept a string literal or bare identifier (PG allows `SET TIME ZONE UTC`).
            let zone = match value {
                sp::Expr::Value(v) => match &v.value {
                    sp::Value::SingleQuotedString(s) => s.clone(),
                    sp::Value::DoubleQuotedString(s) => s.clone(),
                    other => other.to_string(),
                },
                sp::Expr::Identifier(ident) => ident.value.clone(),
                other => {
                    return Err(SqlError::Parse(format!(
                        "SET TIME ZONE expects a string literal or identifier, got: {other}"
                    )))
                }
            };
            Ok(Statement::SetTimezone(zone))
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

/// Parse column options (NOT NULL, DEFAULT, CHECK, FK, UNIQUE) from a sqlparser ColumnDef.
/// Returns (ColumnSpec, Option<ForeignKeyDef>, was_inline_pk, was_unique).
fn convert_column_def(
    col_def: &sp::ColumnDef,
) -> Result<(ColumnSpec, Option<ForeignKeyDef>, bool, bool)> {
    let col_name = col_def.name.value.clone();
    let data_type = convert_data_type(&col_def.data_type)?;
    let mut nullable = true;
    let mut is_primary_key = false;
    let mut is_unique = false;
    let mut default_expr = None;
    let mut default_sql = None;
    let mut check_expr = None;
    let mut check_sql = None;
    let mut check_name = None;
    let mut generated_expr = None;
    let mut generated_sql = None;
    let mut generated_kind = None;
    let mut fk_def = None;

    for opt in &col_def.options {
        match &opt.option {
            sp::ColumnOption::NotNull => nullable = false,
            sp::ColumnOption::Null => nullable = true,
            sp::ColumnOption::PrimaryKey(_) => {
                is_primary_key = true;
                nullable = false;
            }
            sp::ColumnOption::Unique(_) => is_unique = true,
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
            sp::ColumnOption::Generated {
                generation_expr,
                generation_expr_mode,
                sequence_options: _,
                ..
            } => {
                let Some(expr) = generation_expr else {
                    return Err(SqlError::Unsupported(
                        "identity columns not yet supported; use INTEGER PRIMARY KEY for autoincrement".into(),
                    ));
                };
                let mode = generation_expr_mode.unwrap_or(sp::GeneratedExpressionMode::Virtual);
                let converted = convert_expr(expr)?;
                reject_aggregate_or_window(expr, "GENERATED")?;
                if has_subquery(&converted) {
                    return Err(SqlError::Unsupported(
                        "subquery in GENERATED expression".into(),
                    ));
                }
                reject_volatile_in_generated(&converted)?;
                generated_sql = Some(expr.to_string());
                generated_expr = Some(converted);
                generated_kind = Some(match mode {
                    sp::GeneratedExpressionMode::Stored => GeneratedKind::Stored,
                    sp::GeneratedExpressionMode::Virtual => GeneratedKind::Virtual,
                });
            }
            _ => {}
        }
    }

    if generated_kind.is_some() {
        if default_expr.is_some() {
            return Err(SqlError::Unsupported(
                "DEFAULT and GENERATED cannot be combined".into(),
            ));
        }
        if is_primary_key {
            return Err(SqlError::Unsupported(
                "GENERATED column cannot be PRIMARY KEY".into(),
            ));
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
        generated_expr,
        generated_sql,
        generated_kind,
    };
    Ok((spec, fk_def, is_primary_key, is_unique))
}

fn reject_volatile_in_generated(expr: &Expr) -> Result<()> {
    fn walk(e: &Expr) -> Result<()> {
        match e {
            Expr::Function { name, args, .. } => {
                let upper = name.to_ascii_uppercase();
                if matches!(
                    upper.as_str(),
                    "RANDOM"
                        | "NOW"
                        | "CURRENT_TIMESTAMP"
                        | "CURRENT_DATE"
                        | "CURRENT_TIME"
                        | "CLOCK_TIMESTAMP"
                        | "STATEMENT_TIMESTAMP"
                        | "TRANSACTION_TIMESTAMP"
                        | "LOCALTIMESTAMP"
                        | "LOCALTIME"
                ) {
                    return Err(SqlError::Unsupported(format!(
                        "volatile function {name}() not allowed in GENERATED expression"
                    )));
                }
                for a in args {
                    walk(a)?;
                }
                Ok(())
            }
            Expr::BinaryOp { left, right, .. } => {
                walk(left)?;
                walk(right)
            }
            Expr::UnaryOp { expr, .. } => walk(expr),
            Expr::Cast { expr, .. } => walk(expr),
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(o) = operand {
                    walk(o)?;
                }
                for (cond, res) in conditions {
                    walk(cond)?;
                    walk(res)?;
                }
                if let Some(e) = else_result {
                    walk(e)?;
                }
                Ok(())
            }
            Expr::Coalesce(items) => items.iter().try_for_each(walk),
            _ => Ok(()),
        }
    }
    walk(expr)
}

fn convert_create_table(ct: sp::CreateTable) -> Result<Statement> {
    let name = object_name_to_string(&ct.name);
    let if_not_exists = ct.if_not_exists;

    let mut columns = Vec::new();
    let mut inline_pk: Vec<String> = Vec::new();
    let mut foreign_keys: Vec<ForeignKeyDef> = Vec::new();
    let mut unique_indices: Vec<UniqueIndexDef> = Vec::new();

    for col_def in &ct.columns {
        let (spec, fk_def, was_pk, was_unique) = convert_column_def(col_def)?;
        if was_pk {
            inline_pk.push(spec.name.clone());
        }
        if let Some(fk) = fk_def {
            foreign_keys.push(fk);
        }
        if was_unique && !was_pk {
            unique_indices.push(UniqueIndexDef {
                name: None,
                columns: vec![spec.name.to_ascii_lowercase()],
            });
        }
        columns.push(spec);
    }

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
            sp::TableConstraint::Unique(u) => {
                let cols: Vec<String> = u
                    .columns
                    .iter()
                    .filter_map(|idx_col| match &idx_col.column.expr {
                        sp::Expr::Identifier(ident) => Some(ident.value.to_ascii_lowercase()),
                        _ => None,
                    })
                    .collect();
                if !cols.is_empty() {
                    unique_indices.push(UniqueIndexDef {
                        name: u.name.as_ref().map(|n| n.value.clone()),
                        columns: cols,
                    });
                }
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
        unique_indices,
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
            let (spec, fk, _was_pk, _was_unique) = convert_column_def(&column_def)?;
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

    let on_conflict = insert.on.as_ref().map(convert_on_insert).transpose()?;
    let returning = convert_returning(insert.returning.as_deref())?;

    Ok(Statement::Insert(InsertStmt {
        table,
        columns,
        source,
        on_conflict,
        returning,
    }))
}

fn convert_on_insert(on: &sp::OnInsert) -> Result<OnConflictClause> {
    match on {
        sp::OnInsert::OnConflict(oc) => {
            let target = oc
                .conflict_target
                .as_ref()
                .map(convert_conflict_target)
                .transpose()?;
            let action = convert_on_conflict_action(&oc.action)?;
            Ok(OnConflictClause { target, action })
        }
        sp::OnInsert::DuplicateKeyUpdate(_) => Err(SqlError::Parse(
            "ON DUPLICATE KEY UPDATE is MySQL-specific; use ON CONFLICT".into(),
        )),
        _ => Err(SqlError::Parse("unsupported ON INSERT clause".into())),
    }
}

fn convert_conflict_target(target: &sp::ConflictTarget) -> Result<ConflictTarget> {
    match target {
        sp::ConflictTarget::Columns(cols) => Ok(ConflictTarget::Columns(
            cols.iter().map(|c| c.value.to_ascii_lowercase()).collect(),
        )),
        sp::ConflictTarget::OnConstraint(name) => {
            if name.0.len() > 1 {
                return Err(SqlError::Parse(
                    "qualified constraint names not supported".into(),
                ));
            }
            Ok(ConflictTarget::Constraint(
                object_name_to_string(name).to_ascii_lowercase(),
            ))
        }
    }
}

fn convert_on_conflict_action(action: &sp::OnConflictAction) -> Result<OnConflictAction> {
    match action {
        sp::OnConflictAction::DoNothing => Ok(OnConflictAction::DoNothing),
        sp::OnConflictAction::DoUpdate(du) => {
            let assignments = du
                .assignments
                .iter()
                .map(|a| {
                    let col = match &a.target {
                        sp::AssignmentTarget::ColumnName(name) => {
                            object_name_to_string(name).to_ascii_lowercase()
                        }
                        _ => {
                            return Err(SqlError::Unsupported(
                                "tuple assignment in ON CONFLICT".into(),
                            ))
                        }
                    };
                    let expr = convert_expr(&a.value)?;
                    Ok((col, expr))
                })
                .collect::<Result<_>>()?;
            let where_clause = du.selection.as_ref().map(convert_expr).transpose()?;
            Ok(OnConflictAction::DoUpdate {
                assignments,
                where_clause,
            })
        }
    }
}

fn convert_select_body(select: &sp::Select) -> Result<SelectStmt> {
    let distinct = match &select.distinct {
        Some(sp::Distinct::Distinct) => true,
        Some(sp::Distinct::On(_)) => {
            return Err(SqlError::Unsupported("DISTINCT ON".into()));
        }
        _ => false,
    };

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

    let columns: Vec<SelectColumn> = select
        .projection
        .iter()
        .map(convert_select_item)
        .collect::<Result<_>>()?;

    let where_clause = select.selection.as_ref().map(convert_expr).transpose()?;

    let group_by = match &select.group_by {
        sp::GroupByExpr::Expressions(exprs, _) => {
            exprs.iter().map(convert_expr).collect::<Result<_>>()?
        }
        sp::GroupByExpr::All(_) => {
            return Err(SqlError::Unsupported("GROUP BY ALL".into()));
        }
    };

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
    let mut names = rustc_hash::FxHashSet::default();
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
    let returning = convert_returning(update.returning.as_deref())?;

    Ok(Statement::Update(UpdateStmt {
        table,
        assignments,
        where_clause,
        returning,
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
    let returning = convert_returning(delete.returning.as_deref())?;

    Ok(Statement::Delete(DeleteStmt {
        table: table_name,
        where_clause,
        returning,
    }))
}

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
                distinct: false,
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
                distinct: false,
            })
        }
        sp::Expr::Ceil { expr: e, .. } => Ok(Expr::Function {
            name: "CEIL".into(),
            args: vec![convert_expr(e)?],
            distinct: false,
        }),
        sp::Expr::Floor { expr: e, .. } => Ok(Expr::Function {
            name: "FLOOR".into(),
            args: vec![convert_expr(e)?],
            distinct: false,
        }),
        sp::Expr::Position { expr: e, r#in } => Ok(Expr::Function {
            name: "INSTR".into(),
            args: vec![convert_expr(r#in)?, convert_expr(e)?],
            distinct: false,
        }),
        // Typed literal: `DATE '2024-01-15'`, `TIMESTAMP '...'`, etc.
        sp::Expr::TypedString(ts) => {
            let raw = match &ts.value.value {
                sp::Value::SingleQuotedString(s) => s.clone(),
                sp::Value::DoubleQuotedString(s) => s.clone(),
                other => other.to_string(),
            };
            convert_typed_string(&ts.data_type, &raw)
        }
        // INTERVAL '...' — sqlparser emits Expr::Interval with a boxed Expr value + field qualifiers
        sp::Expr::Interval(iv) => convert_interval_expr(iv),
        // EXTRACT(field FROM src)
        sp::Expr::Extract { field, expr: e, .. } => {
            let field_name = match field {
                sp::DateTimeField::Year => "year",
                sp::DateTimeField::Month => "month",
                sp::DateTimeField::Week(_) => "week",
                sp::DateTimeField::Day => "day",
                sp::DateTimeField::Date => "day",
                sp::DateTimeField::Hour => "hour",
                sp::DateTimeField::Minute => "minute",
                sp::DateTimeField::Second => "second",
                sp::DateTimeField::Millisecond => "milliseconds",
                sp::DateTimeField::Microsecond => "microseconds",
                sp::DateTimeField::Microseconds => "microseconds",
                sp::DateTimeField::Milliseconds => "milliseconds",
                sp::DateTimeField::Dow => "dow",
                sp::DateTimeField::Isodow => "isodow",
                sp::DateTimeField::Doy => "doy",
                sp::DateTimeField::Epoch => "epoch",
                sp::DateTimeField::Quarter => "quarter",
                sp::DateTimeField::Decade => "decade",
                sp::DateTimeField::Century => "century",
                sp::DateTimeField::Millennium => "millennium",
                sp::DateTimeField::Isoyear => "isoyear",
                sp::DateTimeField::Julian => "julian",
                other => {
                    return Err(SqlError::InvalidExtractField(format!("{other:?}")));
                }
            };
            Ok(Expr::Function {
                name: "EXTRACT".into(),
                args: vec![
                    Expr::Literal(Value::Text(field_name.into())),
                    convert_expr(e)?,
                ],
                distinct: false,
            })
        }
        // `AT TIME ZONE 'zone'` operator — desugars to AT_TIMEZONE(ts, zone) scalar function.
        sp::Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => Ok(Expr::Function {
            name: "AT_TIMEZONE".into(),
            args: vec![convert_expr(timestamp)?, convert_expr(time_zone)?],
            distinct: false,
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

fn convert_typed_string(dt: &sp::DataType, value: &str) -> Result<Expr> {
    let s = value.trim_matches('\'');
    match dt {
        sp::DataType::Date => {
            let d = crate::datetime::parse_date(s)?;
            Ok(Expr::Literal(Value::Date(d)))
        }
        sp::DataType::Time(_, _) => {
            let t = crate::datetime::parse_time(s)?;
            Ok(Expr::Literal(Value::Time(t)))
        }
        sp::DataType::Timestamp(_, _) => {
            let t = crate::datetime::parse_timestamp(s)?;
            Ok(Expr::Literal(Value::Timestamp(t)))
        }
        sp::DataType::Interval { .. } => {
            let (months, days, micros) = crate::datetime::parse_interval(s)?;
            Ok(Expr::Literal(Value::Interval {
                months,
                days,
                micros,
            }))
        }
        _ => {
            let target = convert_data_type(dt)?;
            Ok(Expr::Cast {
                expr: Box::new(Expr::Literal(Value::Text(s.into()))),
                data_type: target,
            })
        }
    }
}

fn convert_interval_expr(iv: &sp::Interval) -> Result<Expr> {
    let raw = match iv.value.as_ref() {
        sp::Expr::Value(v) => match &v.value {
            sp::Value::SingleQuotedString(s) => s.clone(),
            sp::Value::Number(n, _) => n.clone(),
            other => {
                return Err(SqlError::InvalidIntervalLiteral(format!(
                    "unsupported inner value: {other}"
                )))
            }
        },
        other => {
            return Err(SqlError::InvalidIntervalLiteral(format!(
                "unsupported inner expr: {other}"
            )))
        }
    };

    // SQL-standard form `INTERVAL '5' DAY` — append the unit to the literal.
    let with_unit = if let Some(field) = &iv.leading_field {
        let unit_name = match field {
            sp::DateTimeField::Year => "years",
            sp::DateTimeField::Month => "months",
            sp::DateTimeField::Week(_) => "weeks",
            sp::DateTimeField::Day => "days",
            sp::DateTimeField::Hour => "hours",
            sp::DateTimeField::Minute => "minutes",
            sp::DateTimeField::Second => "seconds",
            _ => {
                return Err(SqlError::InvalidIntervalLiteral(format!(
                    "unsupported leading field: {field:?}"
                )))
            }
        };
        format!("{raw} {unit_name}")
    } else {
        raw
    };

    let (months, days, micros) = crate::datetime::parse_interval(&with_unit)?;
    Ok(Expr::Literal(Value::Interval {
        months,
        days,
        micros,
    }))
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

    let (args, is_count_star, distinct) = match &func.args {
        sp::FunctionArguments::List(list) => {
            let distinct = matches!(
                list.duplicate_treatment,
                Some(sp::DuplicateTreatment::Distinct)
            );
            if list.args.is_empty() && name == "COUNT" {
                (vec![], true, distinct)
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
                    (vec![], true, distinct)
                } else {
                    (args, false, distinct)
                }
            }
        }
        sp::FunctionArguments::None => {
            if name == "COUNT" {
                (vec![], true, false)
            } else {
                (vec![], false, false)
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

    Ok(Expr::Function {
        name,
        args,
        distinct,
    })
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

fn convert_returning(items: Option<&[sp::SelectItem]>) -> Result<Option<Vec<SelectColumn>>> {
    match items {
        None => Ok(None),
        Some(items) => {
            let cols = items
                .iter()
                .map(convert_returning_item)
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(cols))
        }
    }
}

fn convert_returning_item(item: &sp::SelectItem) -> Result<SelectColumn> {
    match item {
        sp::SelectItem::Wildcard(_) => Ok(SelectColumn::AllColumns),
        sp::SelectItem::UnnamedExpr(e) => {
            reject_aggregate_or_window(e, "RETURNING")?;
            Ok(SelectColumn::Expr {
                expr: convert_expr(e)?,
                alias: None,
            })
        }
        sp::SelectItem::ExprWithAlias { expr, alias } => {
            reject_aggregate_or_window(expr, "RETURNING")?;
            Ok(SelectColumn::Expr {
                expr: convert_expr(expr)?,
                alias: Some(alias.value.clone()),
            })
        }
        sp::SelectItem::QualifiedWildcard(kind, _) => match kind {
            sp::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                let s = object_name_to_string(name);
                if s.eq_ignore_ascii_case("old") {
                    Ok(SelectColumn::AllFromOld)
                } else if s.eq_ignore_ascii_case("new") {
                    Ok(SelectColumn::AllFromNew)
                } else {
                    Err(SqlError::Unsupported(format!(
                        "RETURNING {s}.* — only old.* and new.* qualified wildcards allowed"
                    )))
                }
            }
            sp::SelectItemQualifiedWildcardKind::Expr(_) => {
                Err(SqlError::Unsupported("expression.* in RETURNING".into()))
            }
        },
    }
}

fn reject_aggregate_or_window(expr: &sp::Expr, ctx: &str) -> Result<()> {
    use sp::Expr as E;
    match expr {
        E::Function(f) => {
            if f.over.is_some() {
                return Err(SqlError::Unsupported(format!(
                    "window functions are not allowed in {ctx}"
                )));
            }
            let name = f
                .name
                .0
                .last()
                .map(|p| match p {
                    sp::ObjectNamePart::Identifier(id) => id.value.to_ascii_uppercase(),
                    _ => String::new(),
                })
                .unwrap_or_default();
            if matches!(
                name.as_str(),
                "COUNT"
                    | "SUM"
                    | "AVG"
                    | "MIN"
                    | "MAX"
                    | "GROUP_CONCAT"
                    | "STRING_AGG"
                    | "ARRAY_AGG"
                    | "BIT_AND"
                    | "BIT_OR"
                    | "BOOL_AND"
                    | "BOOL_OR"
                    | "EVERY"
                    | "STDDEV"
                    | "STDDEV_POP"
                    | "STDDEV_SAMP"
                    | "VARIANCE"
                    | "VAR_POP"
                    | "VAR_SAMP"
            ) {
                return Err(SqlError::Unsupported(format!(
                    "aggregate functions are not allowed in {ctx}"
                )));
            }
            for arg in walk_function_args(f) {
                reject_aggregate_or_window(arg, ctx)?;
            }
            Ok(())
        }
        E::BinaryOp { left, right, .. } => {
            reject_aggregate_or_window(left, ctx)?;
            reject_aggregate_or_window(right, ctx)
        }
        E::UnaryOp { expr, .. } => reject_aggregate_or_window(expr, ctx),
        E::Cast { expr, .. } => reject_aggregate_or_window(expr, ctx),
        E::Nested(e) => reject_aggregate_or_window(e, ctx),
        E::Case {
            conditions,
            else_result,
            ..
        } => {
            for cwt in conditions {
                reject_aggregate_or_window(&cwt.condition, ctx)?;
                reject_aggregate_or_window(&cwt.result, ctx)?;
            }
            if let Some(e) = else_result {
                reject_aggregate_or_window(e, ctx)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn walk_function_args(f: &sp::Function) -> Vec<&sp::Expr> {
    use sp::FunctionArguments as FA;
    let mut out = Vec::new();
    if let FA::List(args) = &f.args {
        for a in &args.args {
            if let sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e)) = a {
                out.push(e);
            }
        }
    }
    out
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

        sp::DataType::Date => Ok(DataType::Date),
        sp::DataType::Time(_, _) => Ok(DataType::Time),
        sp::DataType::Timestamp(_, _) => Ok(DataType::Timestamp),
        sp::DataType::Interval { .. } => Ok(DataType::Interval),

        _ => Err(SqlError::Unsupported(format!("data type: {dt}"))),
    }
}

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
#[path = "parser_tests.rs"]
mod tests;
