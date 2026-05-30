//! SQL parser: converts SQL strings into the internal AST.

use sqlparser::ast as sp;

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
    CreateMaterializedView(Box<CreateMatviewStmt>),
    RefreshMaterializedView(RefreshMatviewStmt),
    DropMaterializedView(DropMatviewStmt),
    CreateTrigger(Box<CreateTriggerStmt>),
    DropTrigger(DropTriggerStmt),
    AlterTable(Box<AlterTableStmt>),
    Insert(InsertStmt),
    Select(Box<SelectQuery>),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    Truncate(TruncateStmt),
    Begin { access_mode: BeginAccessMode },
    Commit,
    Rollback,
    Savepoint(String),
    ReleaseSavepoint(String),
    RollbackTo(String),
    SetTimezone(String),
    Explain(Box<Statement>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeginAccessMode {
    Default,
    ReadWrite,
    ReadOnly,
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
    DisableTrigger {
        name: String,
    },
    EnableTrigger {
        name: String,
    },
    DisableAllTriggers,
    EnableAllTriggers,
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
    pub strict: bool,
    pub temporary: bool,
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
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
    pub deferrable: bool,
    pub initially_deferred: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReferentialAction {
    NoAction = 0,
    Restrict = 1,
    Cascade = 2,
    SetNull = 3,
    SetDefault = 4,
}

impl ReferentialAction {
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::NoAction),
            1 => Some(Self::Restrict),
            2 => Some(Self::Cascade),
            3 => Some(Self::SetNull),
            4 => Some(Self::SetDefault),
            _ => None,
        }
    }
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
    pub collation: crate::types::Collation,
}

#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct TruncateStmt {
    pub tables: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table_name: String,
    /// Per-key column name for `IndexColSpec::Column` entries; ignored for `Expr` entries
    /// (those resolve through `key_exprs[i]`).
    pub columns: Vec<String>,
    /// Parallel to `columns`: `None` means the key is a column reference, `Some((expr, sql))`
    /// means the key is an expression. Empty Vec = all-column index.
    pub key_exprs: Vec<Option<(Expr, String)>>,
    pub unique: bool,
    pub if_not_exists: bool,
    pub predicate_sql: Option<String>,
    pub predicate_expr: Option<Expr>,
    pub collations: Vec<crate::types::Collation>,
    pub kind: crate::types::IndexKind,
    /// ANN-only: filter-column names from `WITH (filters = '...')`, resolved to
    /// schema column indices in `build_index_def_for_create`. Empty otherwise.
    pub ann_filter_cols: Vec<String>,
    pub concurrently: bool,
}

#[derive(Debug, Clone)]
pub struct CreateMatviewStmt {
    pub name: String,
    pub select_sql: String,
    pub select_parsed: SelectQuery,
    pub with_data: bool,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct RefreshMatviewStmt {
    pub name: String,
    pub concurrently: bool,
}

#[derive(Debug, Clone)]
pub struct DropMatviewStmt {
    pub name: String,
    pub if_exists: bool,
    pub cascade: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update(Vec<String>),
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerGranularity {
    ForEachRow,
    ForEachStatement,
}

#[derive(Debug, Clone)]
pub struct TransitionTables {
    pub new_table_alias: Option<String>,
    pub old_table_alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreateTriggerStmt {
    pub name: String,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub target: String,
    pub granularity: TriggerGranularity,
    pub referencing: Option<TransitionTables>,
    pub when_sql: Option<String>,
    pub when_expr: Option<Expr>,
    pub body_sql: String,
    pub body: Vec<Statement>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropTriggerStmt {
    pub name: String,
    pub table: Option<String>,
    pub if_exists: bool,
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
    pub args: Option<Vec<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DerivedTable {
    pub query: Box<SelectQuery>,
    pub lateral: bool,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Cross,
    Left,
    Right,
    FullOuter,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableRef,
    pub subquery: Option<Box<DerivedTable>>,
    pub on_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub columns: Vec<SelectColumn>,
    pub from: String,
    pub from_alias: Option<String>,
    pub from_subquery: Option<Box<DerivedTable>>,
    pub from_args: Option<Vec<Expr>>,
    pub from_json_table: Option<Box<JsonTableSpec>>,
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
pub struct JsonTableSpec {
    pub source: Expr,
    pub root_path: String,
    pub columns: Vec<JsonTableCol>,
}

#[derive(Debug, Clone)]
pub enum JsonTableCol {
    Named {
        name: String,
        ty: DataType,
        path: String,
        exists: bool,
    },
    Ordinality {
        name: String,
    },
    Nested {
        path: String,
        columns: Vec<JsonTableCol>,
    },
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
    Insert(Box<InsertStmt>),
    Update(Box<UpdateStmt>),
    Delete(Box<DeleteStmt>),
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
    Collate {
        expr: Box<Expr>,
        collation: crate::types::Collation,
    },
    TypedNullRecord(String),
    ArrayLiteral(Vec<Expr>),
    Quantified {
        left: Box<Expr>,
        op: BinOp,
        quantifier: Quantifier,
        right: QuantifiedRhs,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quantifier {
    Any,
    All,
}

#[derive(Debug, Clone)]
pub enum QuantifiedRhs {
    Subquery(Box<SelectStmt>),
    Array(Box<Expr>),
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
    JsonGet,
    JsonGetText,
    JsonPath,
    JsonPathText,
    JsonContains,
    JsonContainedBy,
    JsonHasKey,
    JsonHasAnyKey,
    JsonHasAllKeys,
    JsonDeletePath,
    JsonPathExists,
    JsonPathMatch,
    /// `@?_tz` — tz-aware variant of `@?`.
    JsonPathExistsTz,
    /// `@@_tz` — tz-aware variant of `@@`.
    JsonPathMatchTz,
    VectorL2,
    VectorInner,
    VectorCosine,
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
        Expr::ArrayLiteral(elems) => elems.iter().any(has_subquery),
        Expr::Quantified { left, right, .. } => {
            has_subquery(left)
                || match right {
                    QuantifiedRhs::Subquery(_) => true,
                    QuantifiedRhs::Array(e) => has_subquery(e),
                }
        }
        _ => false,
    }
}

pub fn parse_sql_expr(sql: &str) -> Result<Expr> {
    let sp_expr = crate::dialect::parse_expr(sql).map_err(|e| SqlError::Parse(e.to_string()))?;
    convert_expr(&sp_expr)
}

pub fn parse_sql(sql: &str) -> Result<Statement> {
    if let Some(stmt) = try_parse_refresh_matview(sql) {
        return stmt;
    }
    let (rewritten, no_data_flags) = strip_matview_with_no_data(sql);
    let stmts =
        crate::dialect::parse_statements(&rewritten).map_err(|e| SqlError::Parse(e.to_string()))?;

    if stmts.is_empty() {
        return Err(SqlError::Parse("empty SQL".into()));
    }
    if stmts.len() > 1 {
        return Err(SqlError::Unsupported("multiple statements".into()));
    }

    let mut converted = convert_statement(stmts.into_iter().next().unwrap())?;
    apply_no_data_flags(std::slice::from_mut(&mut converted), &no_data_flags);
    Ok(converted)
}

pub fn parse_sql_multi(sql: &str) -> Result<Vec<Statement>> {
    let (rewritten, no_data_flags) = strip_matview_with_no_data(sql);
    let mut out: Vec<Statement> = Vec::new();
    for (start, end) in split_statement_spans(&rewritten) {
        let stmt_sql = &rewritten[start..end];
        if let Some(parsed) = try_parse_refresh_matview(stmt_sql) {
            out.push(parsed?);
        } else {
            let raw = crate::dialect::parse_statements(stmt_sql)
                .map_err(|e| SqlError::Parse(e.to_string()))?;
            for s in raw {
                out.push(convert_statement(s)?);
            }
        }
    }
    if out.is_empty() {
        return Err(SqlError::Parse("empty SQL".into()));
    }
    apply_no_data_flags(&mut out, &no_data_flags);
    Ok(out)
}

fn split_statement_spans(sql: &str) -> Vec<(usize, usize)> {
    let bytes = sql.as_bytes();
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut stmt_start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < bytes.len() {
                i += 2;
            }
            continue;
        }
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }
        if b == b'"' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }
        if b == b'$' {
            let mut j = i + 1;
            while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'$' {
                let tag_len = j - i + 1;
                i = j + 1;
                while i + tag_len <= bytes.len() {
                    if bytes[i..i + tag_len] == bytes[(j - tag_len + 1)..=j] {
                        i += tag_len;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            i += 1;
            continue;
        }
        if b == b';' {
            if !sql[stmt_start..i].trim().is_empty() {
                spans.push((stmt_start, i));
            }
            i += 1;
            stmt_start = i;
            continue;
        }
        i += 1;
    }
    if stmt_start < bytes.len() && !sql[stmt_start..].trim().is_empty() {
        spans.push((stmt_start, bytes.len()));
    }
    spans
}

fn apply_no_data_flags(stmts: &mut [Statement], flags: &[bool]) {
    let mut iter = flags.iter();
    for stmt in stmts.iter_mut() {
        if let Statement::CreateMaterializedView(boxed) = stmt {
            if let Some(&no_data) = iter.next() {
                boxed.with_data = !no_data;
            }
        }
    }
}

/// sqlparser 0.61 doesn't natively parse REFRESH MATERIALIZED VIEW [CONCURRENTLY] <name>.
fn try_parse_refresh_matview(sql: &str) -> Option<Result<Statement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    let after = lower.strip_prefix("refresh materialized view")?;
    let after = after.trim_start();
    let (concurrently, rest_lower) = match after.strip_prefix("concurrently") {
        Some(r) if r.starts_with(char::is_whitespace) => (true, r.trim_start()),
        _ => (false, after),
    };
    if rest_lower.is_empty() {
        return Some(Err(SqlError::Parse(
            "REFRESH MATERIALIZED VIEW requires a name".into(),
        )));
    }
    let name = rest_lower
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches(|c: char| c == '"' || c == ';')
        .to_string();
    if name.is_empty() {
        return Some(Err(SqlError::Parse(
            "REFRESH MATERIALIZED VIEW requires a name".into(),
        )));
    }
    Some(Ok(Statement::RefreshMaterializedView(RefreshMatviewStmt {
        name,
        concurrently,
    })))
}

fn strip_matview_with_no_data(sql: &str) -> (String, Vec<bool>) {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut flags: Vec<bool> = Vec::new();
    let mut stmt_start = 0usize;
    let mut i = 0usize;

    let push_one = |out: &mut String, i: &mut usize, bytes: &[u8], sql: &str| {
        let b = bytes[*i];
        if b < 0x80 {
            out.push(b as char);
            *i += 1;
        } else {
            let len = if b >= 0xF0 {
                4
            } else if b >= 0xE0 {
                3
            } else if b >= 0xC0 {
                2
            } else {
                1
            };
            let end = (*i + len).min(bytes.len());
            out.push_str(&sql[*i..end]);
            *i = end;
        }
    };

    while i < bytes.len() {
        let b = bytes[i];
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                push_one(&mut out, &mut i, bytes, sql);
            }
            continue;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            out.push('/');
            out.push('*');
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                push_one(&mut out, &mut i, bytes, sql);
            }
            if i + 1 < bytes.len() {
                out.push('*');
                out.push('/');
                i += 2;
            }
            continue;
        }
        if b == b'\'' {
            out.push('\'');
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        out.push('\'');
                        out.push('\'');
                        i += 2;
                    } else {
                        out.push('\'');
                        i += 1;
                        break;
                    }
                } else {
                    push_one(&mut out, &mut i, bytes, sql);
                }
            }
            continue;
        }
        if b == b'"' {
            out.push('"');
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        out.push('"');
                        out.push('"');
                        i += 2;
                    } else {
                        out.push('"');
                        i += 1;
                        break;
                    }
                } else {
                    push_one(&mut out, &mut i, bytes, sql);
                }
            }
            continue;
        }
        if b == b'$' {
            let mut j = i + 1;
            while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'$' {
                let tag_len = j - i + 1;
                out.push_str(&sql[i..=j]);
                i = j + 1;
                while i + tag_len <= bytes.len() {
                    if bytes[i..i + tag_len] == bytes[(j - tag_len + 1)..=j] {
                        out.push_str(&sql[i..i + tag_len]);
                        i += tag_len;
                        break;
                    }
                    push_one(&mut out, &mut i, bytes, sql);
                }
                continue;
            }
            out.push('$');
            i += 1;
            continue;
        }
        if b == b';' {
            let stmt_text = &out[stmt_start..];
            if statement_is_create_matview(stmt_text) {
                match find_with_no_data_suffix(stmt_text) {
                    Some(truncate_to) => {
                        out.truncate(stmt_start + truncate_to);
                        flags.push(true);
                    }
                    None => flags.push(false),
                }
            }
            out.push(';');
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                out.push(bytes[i] as char);
                i += 1;
            }
            stmt_start = out.len();
            continue;
        }
        push_one(&mut out, &mut i, bytes, sql);
    }

    let stmt_text = &out[stmt_start..];
    if !stmt_text.trim().is_empty() && statement_is_create_matview(stmt_text) {
        match find_with_no_data_suffix(stmt_text) {
            Some(truncate_to) => {
                out.truncate(stmt_start + truncate_to);
                flags.push(true);
            }
            None => flags.push(false),
        }
    }

    (out, flags)
}

fn statement_is_create_matview(stmt: &str) -> bool {
    let stripped = strip_leading_ws_and_comments(stmt);
    let lower = stripped.to_ascii_lowercase();
    let s = lower.as_str();
    let after_create = match strip_kw_lower(s, "create") {
        Some(r) => r,
        None => return false,
    };
    let after_orrep = if let Some(r) = strip_kw_lower(after_create, "or") {
        if let Some(r2) = strip_kw_lower(r, "replace") {
            r2
        } else {
            after_create
        }
    } else {
        after_create
    };
    let after_temp = if let Some(r) = strip_kw_lower(after_orrep, "temporary") {
        r
    } else if let Some(r) = strip_kw_lower(after_orrep, "temp") {
        r
    } else {
        after_orrep
    };
    let after_mv = match strip_kw_lower(after_temp, "materialized") {
        Some(r) => r,
        None => return false,
    };
    strip_kw_lower(after_mv, "view").is_some()
}

fn strip_leading_ws_and_comments(s: &str) -> &str {
    let mut rest = s;
    loop {
        let trimmed = rest.trim_start();
        if let Some(after) = trimmed.strip_prefix("--") {
            if let Some(nl) = after.find('\n') {
                rest = &after[nl..];
                continue;
            }
            return "";
        }
        if let Some(after) = trimmed.strip_prefix("/*") {
            if let Some(end) = after.find("*/") {
                rest = &after[end + 2..];
                continue;
            }
            return "";
        }
        return trimmed;
    }
}

fn strip_kw_lower<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    let rest = s.strip_prefix(kw)?;
    if rest.is_empty() || rest.starts_with(char::is_whitespace) {
        Some(rest.trim_start())
    } else {
        None
    }
}

fn find_with_no_data_suffix(stmt: &str) -> Option<usize> {
    let bytes = stmt.as_bytes();
    let mut end = bytes.len();
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    if end < 4 || !bytes[end - 4..end].eq_ignore_ascii_case(b"data") {
        return None;
    }
    let mut p = end - 4;
    let ws_start = p;
    while p > 0 && bytes[p - 1].is_ascii_whitespace() {
        p -= 1;
    }
    if p == ws_start {
        return None;
    }
    if p < 2 || !bytes[p - 2..p].eq_ignore_ascii_case(b"no") {
        return None;
    }
    p -= 2;
    let ws_start = p;
    while p > 0 && bytes[p - 1].is_ascii_whitespace() {
        p -= 1;
    }
    if p == ws_start {
        return None;
    }
    if p < 4 || !bytes[p - 4..p].eq_ignore_ascii_case(b"with") {
        return None;
    }
    p -= 4;
    let ws_start = p;
    while p > 0 && bytes[p - 1].is_ascii_whitespace() {
        p -= 1;
    }
    if p == ws_start {
        return None;
    }
    Some(p)
}

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
        QueryBody::Insert(ins) => visit_exprs_stmt(&Statement::Insert((**ins).clone()), visitor),
        QueryBody::Update(upd) => visit_exprs_stmt(&Statement::Update((**upd).clone()), visitor),
        QueryBody::Delete(del) => visit_exprs_stmt(&Statement::Delete((**del).clone()), visitor),
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
        Expr::Collate { expr: e, .. } => visit_expr(e, visitor),
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
        Expr::ArrayLiteral(elems) => {
            for e in elems {
                visit_expr(e, visitor);
            }
        }
        Expr::Quantified { left, right, .. } => {
            visit_expr(left, visitor);
            match right {
                QuantifiedRhs::Subquery(sq) => visit_exprs_select(sq, visitor),
                QuantifiedRhs::Array(e) => visit_expr(e, visitor),
            }
        }
        Expr::Literal(_)
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. }
        | Expr::CountStar
        | Expr::Parameter(_)
        | Expr::TypedNullRecord(_) => {}
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
        sp::Statement::Drop {
            object_type: sp::ObjectType::MaterializedView,
            if_exists,
            names,
            cascade,
            ..
        } => {
            if names.len() != 1 {
                return Err(SqlError::Unsupported("multi-matview DROP".into()));
            }
            Ok(Statement::DropMaterializedView(DropMatviewStmt {
                name: object_name_to_string(&names[0]),
                if_exists,
                cascade,
            }))
        }
        sp::Statement::CreateTrigger(ct) => convert_create_trigger(ct),
        sp::Statement::DropTrigger(dt) => Ok(Statement::DropTrigger(DropTriggerStmt {
            name: object_name_to_string(&dt.trigger_name),
            table: dt.table_name.as_ref().map(object_name_to_string),
            if_exists: dt.if_exists,
        })),
        sp::Statement::AlterTable(at) => convert_alter_table(at),
        sp::Statement::Insert(insert) => convert_insert(insert),
        sp::Statement::Query(query) => convert_query(*query),
        sp::Statement::Update(update) => convert_update(update),
        sp::Statement::Delete(delete) => convert_delete(delete),
        sp::Statement::Truncate(t) => convert_truncate(t),
        sp::Statement::StartTransaction { modes, .. } => {
            let mut access_mode = BeginAccessMode::Default;
            for mode in modes {
                if let sp::TransactionMode::AccessMode(am) = mode {
                    access_mode = match am {
                        sp::TransactionAccessMode::ReadOnly => BeginAccessMode::ReadOnly,
                        sp::TransactionAccessMode::ReadWrite => BeginAccessMode::ReadWrite,
                    };
                }
            }
            Ok(Statement::Begin { access_mode })
        }
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
    let mut collation = crate::types::Collation::Binary;

    for opt in &col_def.options {
        match &opt.option {
            sp::ColumnOption::Collation(name) => {
                let coll_name = object_name_to_string(name);
                collation = crate::types::Collation::from_name(&coll_name).ok_or_else(|| {
                    SqlError::Unsupported(format!(
                        "collation '{coll_name}' not supported (BINARY/NOCASE/RTRIM only)"
                    ))
                })?;
            }
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
                let (on_delete, on_update) = convert_fk_actions(&fk.on_delete, &fk.on_update)?;
                let (deferrable, initially_deferred) =
                    convert_fk_characteristics(&fk.characteristics);
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
                    on_delete,
                    on_update,
                    deferrable,
                    initially_deferred,
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
        collation,
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
    let strict = ct.strict;
    let temporary = ct.temporary;

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
                let (on_delete, on_update) = convert_fk_actions(&fk.on_delete, &fk.on_update)?;
                let (deferrable, initially_deferred) =
                    convert_fk_characteristics(&fk.characteristics);
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
                    on_delete,
                    on_update,
                    deferrable,
                    initially_deferred,
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
        strict,
        temporary,
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
        sp::AlterTableOperation::DisableTrigger { name } => {
            if name.value.eq_ignore_ascii_case("all") {
                AlterTableOp::DisableAllTriggers
            } else {
                AlterTableOp::DisableTrigger {
                    name: name.value.to_ascii_lowercase(),
                }
            }
        }
        sp::AlterTableOperation::EnableTrigger { name } => {
            if name.value.eq_ignore_ascii_case("all") {
                AlterTableOp::EnableAllTriggers
            } else {
                AlterTableOp::EnableTrigger {
                    name: name.value.to_ascii_lowercase(),
                }
            }
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
) -> Result<(ReferentialAction, ReferentialAction)> {
    Ok((convert_fk_action(on_delete)?, convert_fk_action(on_update)?))
}

fn convert_fk_action(action: &Option<sp::ReferentialAction>) -> Result<ReferentialAction> {
    match action {
        None | Some(sp::ReferentialAction::NoAction) => Ok(ReferentialAction::NoAction),
        Some(sp::ReferentialAction::Restrict) => Ok(ReferentialAction::Restrict),
        Some(sp::ReferentialAction::Cascade) => Ok(ReferentialAction::Cascade),
        Some(sp::ReferentialAction::SetNull) => Ok(ReferentialAction::SetNull),
        Some(sp::ReferentialAction::SetDefault) => Ok(ReferentialAction::SetDefault),
    }
}

fn convert_fk_characteristics(ch: &Option<sp::ConstraintCharacteristics>) -> (bool, bool) {
    let Some(c) = ch else {
        return (false, false);
    };
    let deferrable = c.deferrable.unwrap_or(false);
    let initially_deferred = matches!(c.initially, Some(sp::DeferrableInitial::Deferred));
    (deferrable, initially_deferred)
}

fn convert_create_index(ci: sp::CreateIndex) -> Result<Statement> {
    let index_name = ci
        .name
        .as_ref()
        .map(object_name_to_string)
        .ok_or_else(|| SqlError::Parse("index name required".into()))?;

    let table_name = object_name_to_string(&ci.table_name);

    let mut columns: Vec<String> = Vec::with_capacity(ci.columns.len());
    let mut collations: Vec<crate::types::Collation> = Vec::with_capacity(ci.columns.len());
    let mut key_exprs: Vec<Option<(Expr, String)>> = Vec::with_capacity(ci.columns.len());
    for idx_col in &ci.columns {
        let (name, coll, expr_entry) = match &idx_col.column.expr {
            sp::Expr::Identifier(ident) => {
                (ident.value.clone(), crate::types::Collation::Binary, None)
            }
            sp::Expr::Collate {
                expr: inner,
                collation,
            } => match inner.as_ref() {
                sp::Expr::Identifier(ident) => {
                    let coll_name = object_name_to_string(collation);
                    let coll = crate::types::Collation::from_name(&coll_name).ok_or_else(|| {
                        SqlError::Unsupported(format!(
                            "collation '{coll_name}' not supported (BINARY/NOCASE/RTRIM only)"
                        ))
                    })?;
                    (ident.value.clone(), coll, None)
                }
                inner_expr => {
                    let sql = inner_expr.to_string();
                    let expr = convert_expr(inner_expr)?;
                    (
                        sql.clone(),
                        crate::types::Collation::Binary,
                        Some((expr, sql)),
                    )
                }
            },
            other => {
                let sql = other.to_string();
                let expr = convert_expr(other)?;
                (
                    sql.clone(),
                    crate::types::Collation::Binary,
                    Some((expr, sql)),
                )
            }
        };
        columns.push(name);
        collations.push(coll);
        key_exprs.push(expr_entry);
    }

    if columns.is_empty() {
        return Err(SqlError::Parse(
            "index must have at least one column".into(),
        ));
    }

    let (predicate_sql, predicate_expr) = match &ci.predicate {
        Some(sp_expr) => {
            let expr = convert_expr(sp_expr)?;
            validate_partial_index_predicate(&expr)?;
            (Some(sp_expr.to_string()), Some(expr))
        }
        None => (None, None),
    };

    let mut ann_filter_cols: Vec<String> = Vec::new();
    let kind = match &ci.using {
        None => crate::types::IndexKind::BTree,
        Some(sp::IndexType::BTree) => crate::types::IndexKind::BTree,
        Some(sp::IndexType::GIN) => {
            let ops = parse_gin_with_ops(&ci.with)?;
            crate::types::IndexKind::Inverted(crate::types::InvertedKind::Gin(ops))
        }
        Some(sp::IndexType::Custom(ident)) if ident.value.eq_ignore_ascii_case("fts") => {
            let config_id = parse_fts_with_config(&ci.with)?;
            crate::types::IndexKind::Inverted(crate::types::InvertedKind::Fts { config_id })
        }
        Some(sp::IndexType::Custom(ident)) if ident.value.eq_ignore_ascii_case("ann") => {
            let (metric, filter_cols) = parse_ann_with_opts(&ci.with)?;
            ann_filter_cols = filter_cols;
            crate::types::IndexKind::Inverted(crate::types::InvertedKind::Ann { metric })
        }
        Some(other) => {
            return Err(SqlError::Unsupported(format!(
                "index method {other}; supported: BTREE, GIN, FTS, ANN"
            )));
        }
    };

    Ok(Statement::CreateIndex(CreateIndexStmt {
        index_name,
        table_name,
        columns,
        key_exprs,
        unique: ci.unique,
        if_not_exists: ci.if_not_exists,
        predicate_sql,
        predicate_expr,
        collations,
        kind,
        ann_filter_cols,
        concurrently: ci.concurrently,
    }))
}

fn parse_gin_with_ops(with: &[sp::Expr]) -> Result<crate::types::GinOpsClass> {
    use crate::types::GinOpsClass;
    let mut ops_name: Option<String> = None;
    for expr in with {
        match expr {
            sp::Expr::BinaryOp {
                left,
                op: sp::BinaryOperator::Eq,
                right,
            } => {
                let key = match left.as_ref() {
                    sp::Expr::Identifier(id) => id.value.to_ascii_lowercase(),
                    other => {
                        return Err(SqlError::Unsupported(format!(
                            "GIN WITH option key: {other}"
                        )));
                    }
                };
                if key != "ops" {
                    return Err(SqlError::Unsupported(format!(
                        "GIN WITH option: unknown key '{key}' (only 'ops' supported)"
                    )));
                }
                let val = match right.as_ref() {
                    sp::Expr::Value(v) => match &v.value {
                        sp::Value::SingleQuotedString(s) => s.clone(),
                        sp::Value::DoubleQuotedString(s) => s.clone(),
                        other => {
                            return Err(SqlError::Parse(format!(
                                "GIN ops value must be a string literal, got: {other}"
                            )))
                        }
                    },
                    sp::Expr::Identifier(id) => id.value.clone(),
                    other => {
                        return Err(SqlError::Parse(format!(
                            "GIN ops value must be a string literal, got: {other}"
                        )));
                    }
                };
                ops_name = Some(val);
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "GIN WITH option must be `key = value`, got: {other}"
                )));
            }
        }
    }
    let lower = ops_name.as_deref().map(|s| s.to_ascii_lowercase());
    match lower.as_deref() {
        None | Some("jsonb_ops") => Ok(GinOpsClass::JsonbOps),
        Some("jsonb_path_ops") => Ok(GinOpsClass::JsonbPathOps),
        Some(other) => Err(SqlError::Unsupported(format!(
            "GIN opclass '{other}'; supported: jsonb_ops, jsonb_path_ops"
        ))),
    }
}

fn parse_fts_with_config(with: &[sp::Expr]) -> Result<u8> {
    let mut config_name: Option<String> = None;
    for expr in with {
        match expr {
            sp::Expr::BinaryOp {
                left,
                op: sp::BinaryOperator::Eq,
                right,
            } => {
                let key = match left.as_ref() {
                    sp::Expr::Identifier(id) => id.value.to_ascii_lowercase(),
                    other => {
                        return Err(SqlError::Unsupported(format!(
                            "FTS WITH option key: {other}"
                        )));
                    }
                };
                if key != "config" {
                    return Err(SqlError::Unsupported(format!(
                        "FTS WITH option: unknown key '{key}' (only 'config' supported)"
                    )));
                }
                let val = match right.as_ref() {
                    sp::Expr::Value(v) => match &v.value {
                        sp::Value::SingleQuotedString(s) => s.clone(),
                        sp::Value::DoubleQuotedString(s) => s.clone(),
                        other => {
                            return Err(SqlError::Parse(format!(
                                "FTS config value must be a string literal, got: {other}"
                            )))
                        }
                    },
                    sp::Expr::Identifier(id) => id.value.clone(),
                    other => {
                        return Err(SqlError::Parse(format!(
                            "FTS config value must be a string literal, got: {other}"
                        )));
                    }
                };
                config_name = Some(val);
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "FTS WITH option must be `key = value`, got: {other}"
                )));
            }
        }
    }
    let name = config_name.unwrap_or_else(|| "english".to_string());
    Ok(crate::fts::TokenizerKind::from_name(&name)?.as_config_id())
}

/// Parse the ANN `WITH (...)` options. Supported keys: `metric` (l2|inner|cosine)
/// and `filters` (comma-separated low-cardinality column names pushed into the
/// PRISM cell filter). Returns the metric and the raw, lowercased filter names.
fn parse_ann_with_opts(with: &[sp::Expr]) -> Result<(crate::types::AnnMetric, Vec<String>)> {
    use crate::types::AnnMetric;
    let mut metric_name: Option<String> = None;
    let mut filter_cols: Vec<String> = Vec::new();
    for expr in with {
        match expr {
            sp::Expr::BinaryOp {
                left,
                op: sp::BinaryOperator::Eq,
                right,
            } => {
                let key = match left.as_ref() {
                    sp::Expr::Identifier(id) => id.value.to_ascii_lowercase(),
                    other => {
                        return Err(SqlError::Unsupported(format!(
                            "ANN WITH option key: {other}"
                        )));
                    }
                };
                let val = match right.as_ref() {
                    sp::Expr::Value(v) => match &v.value {
                        sp::Value::SingleQuotedString(s) => s.clone(),
                        sp::Value::DoubleQuotedString(s) => s.clone(),
                        other => {
                            return Err(SqlError::Parse(format!(
                                "ANN '{key}' value must be a string literal, got: {other}"
                            )))
                        }
                    },
                    sp::Expr::Identifier(id) => id.value.clone(),
                    other => {
                        return Err(SqlError::Parse(format!(
                            "ANN '{key}' value must be a string literal, got: {other}"
                        )));
                    }
                };
                match key.as_str() {
                    "metric" => metric_name = Some(val),
                    "filters" => {
                        filter_cols = val
                            .split(',')
                            .map(|s| s.trim().to_ascii_lowercase())
                            .filter(|s| !s.is_empty())
                            .collect();
                    }
                    other => {
                        return Err(SqlError::Unsupported(format!(
                            "ANN WITH option: unknown key '{other}' (supported: metric, filters)"
                        )));
                    }
                }
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "ANN WITH option must be `key = value`, got: {other}"
                )));
            }
        }
    }
    let lower = metric_name.as_deref().map(|s| s.to_ascii_lowercase());
    let metric = match lower.as_deref() {
        None | Some("l2") => AnnMetric::L2,
        Some("inner") | Some("inner_product") | Some("ip") => AnnMetric::Inner,
        Some("cosine") => AnnMetric::Cosine,
        Some(other) => {
            return Err(SqlError::Unsupported(format!(
                "ANN metric '{other}'; supported: l2, inner, cosine"
            )))
        }
    };
    Ok((metric, filter_cols))
}

fn validate_partial_index_predicate(expr: &Expr) -> Result<()> {
    let mut bad: Option<&'static str> = None;
    visit_expr(expr, &mut |e| {
        if bad.is_some() {
            return;
        }
        match e {
            Expr::ScalarSubquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                bad = Some("subqueries");
            }
            Expr::CountStar => bad = Some("aggregates"),
            Expr::WindowFunction { .. } => bad = Some("window functions"),
            Expr::Parameter(_) => bad = Some("bound parameters"),
            Expr::QualifiedColumn { .. } => bad = Some("cross-table references"),
            Expr::Function { name, .. } => {
                if is_aggregate_function(name) {
                    bad = Some("aggregates");
                } else if !is_immutable_function(name) {
                    bad = Some("non-deterministic functions");
                }
            }
            _ => {}
        }
    });
    if let Some(reason) = bad {
        return Err(SqlError::Unsupported(format!(
            "partial index predicate cannot contain {reason}"
        )));
    }
    Ok(())
}

fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max" | "total" | "group_concat" | "string_agg"
    )
}

fn is_immutable_function(name: &str) -> bool {
    !matches!(
        name.to_ascii_lowercase().as_str(),
        "now"
            | "current_timestamp"
            | "current_date"
            | "current_time"
            | "localtimestamp"
            | "localtime"
            | "random"
            | "rand"
    )
}

fn convert_create_trigger(ct: sp::CreateTrigger) -> Result<Statement> {
    let name = object_name_to_string(&ct.name);
    let target = object_name_to_string(&ct.table_name);

    let timing = match ct.period {
        Some(sp::TriggerPeriod::Before) => TriggerTiming::Before,
        Some(sp::TriggerPeriod::After) => TriggerTiming::After,
        Some(sp::TriggerPeriod::InsteadOf) => TriggerTiming::InsteadOf,
        _ => {
            return Err(SqlError::Parse(
                "CREATE TRIGGER requires BEFORE, AFTER, or INSTEAD OF".into(),
            ));
        }
    };

    let mut events: Vec<TriggerEvent> = Vec::with_capacity(ct.events.len());
    for ev in &ct.events {
        let mapped = match ev {
            sp::TriggerEvent::Insert => TriggerEvent::Insert,
            sp::TriggerEvent::Delete => TriggerEvent::Delete,
            sp::TriggerEvent::Update(cols) => {
                TriggerEvent::Update(cols.iter().map(|i| i.value.to_ascii_lowercase()).collect())
            }
            sp::TriggerEvent::Truncate => {
                return Err(SqlError::Unsupported(
                    "TRUNCATE triggers are not supported".into(),
                ));
            }
        };
        events.push(mapped);
    }
    if events.is_empty() {
        return Err(SqlError::Parse(
            "CREATE TRIGGER requires at least one event (INSERT/UPDATE/DELETE)".into(),
        ));
    }

    let granularity = match ct.trigger_object {
        Some(sp::TriggerObjectKind::For(sp::TriggerObject::Statement))
        | Some(sp::TriggerObjectKind::ForEach(sp::TriggerObject::Statement)) => {
            TriggerGranularity::ForEachStatement
        }
        // FOR EACH ROW is the default per SQLite semantics when omitted.
        _ => TriggerGranularity::ForEachRow,
    };

    if timing == TriggerTiming::InsteadOf && granularity == TriggerGranularity::ForEachStatement {
        return Err(SqlError::Unsupported(
            "INSTEAD OF triggers must be FOR EACH ROW".into(),
        ));
    }

    let mut referencing: Option<TransitionTables> = None;
    if !ct.referencing.is_empty() {
        let mut new_alias: Option<String> = None;
        let mut old_alias: Option<String> = None;
        for r in &ct.referencing {
            let alias = object_name_to_string(&r.transition_relation_name);
            match r.refer_type {
                sp::TriggerReferencingType::NewTable => new_alias = Some(alias),
                sp::TriggerReferencingType::OldTable => old_alias = Some(alias),
            }
        }
        referencing = Some(TransitionTables {
            new_table_alias: new_alias,
            old_table_alias: old_alias,
        });
    }

    let when_expr = ct.condition.as_ref().map(convert_expr).transpose()?;
    let when_sql = ct.condition.as_ref().map(|e| e.to_string());

    let inner_statements: &[sp::Statement] = match &ct.statements {
        Some(sp::ConditionalStatements::Sequence { statements })
        | Some(sp::ConditionalStatements::BeginEnd(sp::BeginEndStatements {
            statements, ..
        })) => statements,
        None => {
            return Err(SqlError::Parse(
                "CREATE TRIGGER body must contain BEGIN ... END or one or more statements".into(),
            ));
        }
    };
    let body: Vec<Statement> = inner_statements
        .iter()
        .cloned()
        .map(convert_statement)
        .collect::<Result<Vec<_>>>()?;
    let body_sql: String = inner_statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(";");

    Ok(Statement::CreateTrigger(Box::new(CreateTriggerStmt {
        name,
        timing,
        events,
        target,
        granularity,
        referencing,
        when_sql,
        when_expr,
        body_sql,
        body,
        if_not_exists: false,
    })))
}

fn convert_create_view(cv: sp::CreateView) -> Result<Statement> {
    let name = object_name_to_string(&cv.name);
    let sql = cv.query.to_string();

    let parsed_select = parse_select_query(&sql)?;

    if cv.materialized {
        return Ok(Statement::CreateMaterializedView(Box::new(
            CreateMatviewStmt {
                name,
                select_sql: sql,
                select_parsed: parsed_select,
                with_data: true,
                if_not_exists: cv.if_not_exists,
            },
        )));
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

fn parse_select_query(sql: &str) -> Result<SelectQuery> {
    let stmts = parse_sql_multi(sql)?;
    if stmts.len() != 1 {
        return Err(SqlError::Parse(
            "matview body must be a single SELECT statement".into(),
        ));
    }
    match stmts.into_iter().next().unwrap() {
        Statement::Select(sq) => Ok(*sq),
        _ => Err(SqlError::Parse(
            "matview body must be a SELECT statement".into(),
        )),
    }
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

    let (from, from_alias, from_subquery, from_args, from_json_table, joins) =
        if select.from.is_empty() {
            (String::new(), None, None, None, None, vec![])
        } else {
            let first_twj = &select.from[0];
            let (first_name, first_alias, first_sub, first_args, first_jt) =
                convert_from_relation(&first_twj.relation)?;
            let mut joins: Vec<JoinClause> = first_twj
                .joins
                .iter()
                .map(convert_join)
                .collect::<Result<Vec<_>>>()?;
            for extra_twj in &select.from[1..] {
                let (extra_name, extra_alias, extra_sub, extra_args, extra_jt) =
                    convert_from_relation(&extra_twj.relation)?;
                if extra_jt.is_some() {
                    return Err(SqlError::Unsupported(
                        "JSON_TABLE in extra FROM positions not supported".into(),
                    ));
                }
                joins.push(JoinClause {
                    join_type: JoinType::Cross,
                    table: TableRef {
                        name: extra_name,
                        alias: extra_alias,
                        args: extra_args,
                    },
                    subquery: extra_sub,
                    on_clause: None,
                });
                for j in &extra_twj.joins {
                    joins.push(convert_join(j)?);
                }
            }
            (
                first_name,
                first_alias,
                first_sub,
                first_args,
                first_jt,
                joins,
            )
        };
    for j in &joins {
        if let Some(sub) = &j.subquery {
            if sub.lateral && matches!(j.join_type, JoinType::Right | JoinType::FullOuter) {
                return Err(SqlError::Unsupported(
                    "LATERAL is not allowed on the right side of RIGHT JOIN or FULL OUTER JOIN"
                        .into(),
                ));
            }
        }
    }

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
        from_subquery,
        from_args,
        from_json_table,
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

type FromRelation = (
    String,
    Option<String>,
    Option<Box<DerivedTable>>,
    Option<Vec<Expr>>,
    Option<Box<JsonTableSpec>>,
);

fn convert_from_relation(relation: &sp::TableFactor) -> Result<FromRelation> {
    match relation {
        sp::TableFactor::Table {
            name, alias, args, ..
        } => {
            let table_name = object_name_to_string(name);
            let alias_str = alias.as_ref().map(|a| a.name.value.clone());
            let args_converted = match args {
                Some(table_args) => {
                    let mut converted = Vec::with_capacity(table_args.args.len());
                    for arg in &table_args.args {
                        match arg {
                            sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e)) => {
                                converted.push(convert_expr(e)?);
                            }
                            _ => {
                                return Err(SqlError::Unsupported(
                                    "non-positional table function argument".into(),
                                ));
                            }
                        }
                    }
                    Some(converted)
                }
                None => None,
            };
            Ok((table_name, alias_str, None, args_converted, None))
        }
        sp::TableFactor::Derived {
            lateral,
            subquery,
            alias,
            ..
        } => {
            let alias_name = match alias {
                Some(a) => a.name.value.clone(),
                None => return Err(SqlError::Unsupported("derived table requires alias".into())),
            };
            let inner = convert_select_query(subquery)?;
            for cte in &inner.ctes {
                if matches!(
                    &cte.body,
                    QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_)
                ) {
                    return Err(SqlError::Unsupported(
                        "WITH-DML inside subqueries (PG forbids)".into(),
                    ));
                }
            }
            let derived = DerivedTable {
                query: Box::new(inner),
                lateral: *lateral,
                alias: alias_name.clone(),
            };
            Ok((alias_name, None, Some(Box::new(derived)), None, None))
        }
        sp::TableFactor::JsonTable {
            json_expr,
            json_path,
            columns,
            alias,
        } => {
            let alias_name = match alias {
                Some(a) => a.name.value.clone(),
                None => "json_table".to_string(),
            };
            let source = convert_expr(json_expr)?;
            let root_path = json_path_value_to_string(json_path)?;
            let cols = columns
                .iter()
                .map(convert_json_table_column)
                .collect::<Result<Vec<_>>>()?;
            let spec = JsonTableSpec {
                source,
                root_path,
                columns: cols,
            };
            Ok((alias_name, None, None, None, Some(Box::new(spec))))
        }
        _ => Err(SqlError::Unsupported("non-table FROM source".into())),
    }
}

fn json_path_value_to_string(v: &sp::Value) -> Result<String> {
    use sp::Value as V;
    match v {
        V::SingleQuotedString(s)
        | V::DoubleQuotedString(s)
        | V::DollarQuotedString(sp::DollarQuotedString { value: s, .. })
        | V::TripleSingleQuotedString(s)
        | V::TripleDoubleQuotedString(s) => Ok(s.clone()),
        other => Err(SqlError::Unsupported(format!(
            "JSON_TABLE path must be a string literal, got: {other}"
        ))),
    }
}

fn convert_json_table_column(c: &sp::JsonTableColumn) -> Result<JsonTableCol> {
    match c {
        sp::JsonTableColumn::Named(n) => {
            let path = json_path_value_to_string(&n.path)?;
            Ok(JsonTableCol::Named {
                name: n.name.value.clone(),
                ty: convert_data_type(&n.r#type)?,
                path,
                exists: n.exists,
            })
        }
        sp::JsonTableColumn::ForOrdinality(ident) => Ok(JsonTableCol::Ordinality {
            name: ident.value.clone(),
        }),
        sp::JsonTableColumn::Nested(n) => {
            let path = json_path_value_to_string(&n.path)?;
            let columns = n
                .columns
                .iter()
                .map(convert_json_table_column)
                .collect::<Result<Vec<_>>>()?;
            Ok(JsonTableCol::Nested { path, columns })
        }
    }
}

fn convert_set_expr(set_expr: &sp::SetExpr) -> Result<QueryBody> {
    match set_expr {
        sp::SetExpr::Select(sel) => Ok(QueryBody::Select(Box::new(convert_select_body(sel)?))),
        sp::SetExpr::Insert(stmt) => match convert_statement(stmt.clone())? {
            Statement::Insert(ins) => Ok(QueryBody::Insert(Box::new(ins))),
            _ => Err(SqlError::Parse("expected INSERT in WITH-DML body".into())),
        },
        sp::SetExpr::Update(stmt) => match convert_statement(stmt.clone())? {
            Statement::Update(upd) => Ok(QueryBody::Update(Box::new(upd))),
            _ => Err(SqlError::Parse("expected UPDATE in WITH-DML body".into())),
        },
        sp::SetExpr::Delete(stmt) => match convert_statement(stmt.clone())? {
            Statement::Delete(del) => Ok(QueryBody::Delete(Box::new(del))),
            _ => Err(SqlError::Parse("expected DELETE in WITH-DML body".into())),
        },
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
        QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_) => {
            if !order_by.is_empty() || limit.is_some() || offset.is_some() {
                return Err(SqlError::Parse(
                    "ORDER BY / LIMIT / OFFSET not allowed on DML CTE body".into(),
                ));
            }
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
        QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_) => Err(
            SqlError::Unsupported("WITH-DML inside subqueries (PG forbids)".into()),
        ),
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
    let sq = convert_select_query(&query)?;
    Ok(Statement::Select(Box::new(sq)))
}

fn convert_select_query(query: &sp::Query) -> Result<SelectQuery> {
    let (ctes, recursive) = if let Some(ref with) = query.with {
        convert_with(with)?
    } else {
        (vec![], false)
    };
    let body = convert_query_body(query)?;
    Ok(SelectQuery {
        ctes,
        recursive,
        body,
    })
}

fn convert_join(join: &sp::Join) -> Result<JoinClause> {
    let (join_type, constraint) = match &join.join_operator {
        sp::JoinOperator::Inner(c) => (JoinType::Inner, Some(c)),
        sp::JoinOperator::Join(c) => (JoinType::Inner, Some(c)),
        sp::JoinOperator::CrossJoin(c) => (JoinType::Cross, Some(c)),
        sp::JoinOperator::Left(c) | sp::JoinOperator::LeftOuter(c) => (JoinType::Left, Some(c)),
        sp::JoinOperator::LeftSemi(c) => (JoinType::Left, Some(c)),
        sp::JoinOperator::LeftAnti(c) => (JoinType::Left, Some(c)),
        sp::JoinOperator::Right(c) | sp::JoinOperator::RightOuter(c) => (JoinType::Right, Some(c)),
        sp::JoinOperator::RightSemi(c) => (JoinType::Right, Some(c)),
        sp::JoinOperator::RightAnti(c) => (JoinType::Right, Some(c)),
        sp::JoinOperator::FullOuter(c) => (JoinType::FullOuter, Some(c)),
        other => return Err(SqlError::Unsupported(format!("join type: {other:?}"))),
    };

    let (name, alias, subquery, args, json_table) = convert_from_relation(&join.relation)?;
    if json_table.is_some() {
        return Err(SqlError::Unsupported(
            "JSON_TABLE on right side of JOIN".into(),
        ));
    }

    let on_clause = match constraint {
        Some(sp::JoinConstraint::On(expr)) => Some(convert_expr(expr)?),
        Some(sp::JoinConstraint::None) | None => None,
        Some(other) => return Err(SqlError::Unsupported(format!("join constraint: {other:?}"))),
    };

    Ok(JoinClause {
        join_type,
        table: TableRef { name, alias, args },
        subquery,
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

fn convert_truncate(t: sp::Truncate) -> Result<Statement> {
    if matches!(t.cascade, Some(sp::CascadeOption::Cascade)) {
        return Err(SqlError::Unsupported("TRUNCATE CASCADE".into()));
    }
    if t.if_exists {
        return Err(SqlError::Unsupported("TRUNCATE IF EXISTS".into()));
    }
    if t.partitions.is_some() {
        return Err(SqlError::Unsupported("TRUNCATE PARTITION".into()));
    }
    if t.on_cluster.is_some() {
        return Err(SqlError::Unsupported("TRUNCATE ON CLUSTER".into()));
    }
    if t.table_names.is_empty() {
        return Err(SqlError::Parse(
            "TRUNCATE requires at least one table".into(),
        ));
    }

    let tables: Vec<String> = t
        .table_names
        .iter()
        .map(|tt| object_name_to_string(&tt.name))
        .collect();

    Ok(Statement::Truncate(TruncateStmt { tables }))
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
        sp::Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => convert_quantified(left, compare_op, right, Quantifier::Any),
        sp::Expr::AllOp {
            left,
            compare_op,
            right,
        } => convert_quantified(left, compare_op, right, Quantifier::All),
        sp::Expr::Array(sp::Array { elem, .. }) => {
            let elems: Result<Vec<Expr>> = elem.iter().map(convert_expr).collect();
            Ok(Expr::ArrayLiteral(elems?))
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
            if let (sp::DataType::Custom(name, modifiers), sp::Expr::Value(v)) = (dt, e.as_ref()) {
                if modifiers.is_empty() && name.0.len() == 1 && matches!(v.value, sp::Value::Null) {
                    if let sp::ObjectNamePart::Identifier(id) = &name.0[0] {
                        return Ok(Expr::TypedNullRecord(id.value.clone()));
                    }
                }
            }
            let target = convert_data_type(dt)?;
            let inner = convert_expr(e)?;
            if matches!(target, DataType::Json | DataType::Jsonb) {
                if let Expr::Literal(Value::Text(s)) = &inner {
                    let v = if matches!(target, DataType::Json) {
                        crate::json::validate_text(s.as_str())?;
                        Value::Json(s.clone())
                    } else {
                        crate::json::text_to_jsonb(s.as_str())?
                    };
                    return Ok(Expr::Literal(v));
                }
            }
            Ok(Expr::Cast {
                expr: Box::new(inner),
                data_type: target,
            })
        }
        sp::Expr::Collate {
            expr: e,
            collation: name,
        } => {
            let coll_name = object_name_to_string(name);
            let coll = crate::types::Collation::from_name(&coll_name).ok_or_else(|| {
                SqlError::Unsupported(format!(
                    "collation '{coll_name}' not supported (BINARY/NOCASE/RTRIM only)"
                ))
            })?;
            Ok(Expr::Collate {
                expr: Box::new(convert_expr(e)?),
                collation: coll,
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
        sp::Expr::TypedString(ts) => {
            let raw = match &ts.value.value {
                sp::Value::SingleQuotedString(s) => s.clone(),
                sp::Value::DoubleQuotedString(s) => s.clone(),
                other => other.to_string(),
            };
            convert_typed_string(&ts.data_type, &raw)
        }
        sp::Expr::Interval(iv) => convert_interval_expr(iv),
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

fn convert_quantified(
    left: &sp::Expr,
    compare_op: &sp::BinaryOperator,
    right: &sp::Expr,
    quantifier: Quantifier,
) -> Result<Expr> {
    let left_expr = convert_expr(left)?;
    let op = convert_bin_op(compare_op)?;
    if !matches!(
        op,
        BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::Gt | BinOp::LtEq | BinOp::GtEq
    ) {
        return Err(SqlError::Unsupported(format!(
            "ANY/ALL only supports comparison operators, got {op:?}"
        )));
    }
    let rhs = match right {
        sp::Expr::Subquery(query) => {
            let stmt = convert_subquery(query)?;
            QuantifiedRhs::Subquery(Box::new(stmt))
        }
        other => QuantifiedRhs::Array(Box::new(convert_expr(other)?)),
    };
    Ok(Expr::Quantified {
        left: Box::new(left_expr),
        op,
        quantifier,
        right: rhs,
    })
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
        sp::BinaryOperator::Arrow => Ok(BinOp::JsonGet),
        sp::BinaryOperator::LongArrow => Ok(BinOp::JsonGetText),
        sp::BinaryOperator::HashArrow => Ok(BinOp::JsonPath),
        sp::BinaryOperator::HashLongArrow => Ok(BinOp::JsonPathText),
        sp::BinaryOperator::AtArrow => Ok(BinOp::JsonContains),
        sp::BinaryOperator::ArrowAt => Ok(BinOp::JsonContainedBy),
        sp::BinaryOperator::Question => Ok(BinOp::JsonHasKey),
        sp::BinaryOperator::QuestionPipe => Ok(BinOp::JsonHasAnyKey),
        sp::BinaryOperator::QuestionAnd => Ok(BinOp::JsonHasAllKeys),
        sp::BinaryOperator::HashMinus => Ok(BinOp::JsonDeletePath),
        sp::BinaryOperator::AtQuestion => Ok(BinOp::JsonPathExists),
        sp::BinaryOperator::AtAt => Ok(BinOp::JsonPathMatch),
        sp::BinaryOperator::Custom(s) if s == "@?_tz" => Ok(BinOp::JsonPathExistsTz),
        sp::BinaryOperator::Custom(s) if s == "@@_tz" => Ok(BinOp::JsonPathMatchTz),
        sp::BinaryOperator::LtDashGt => Ok(BinOp::VectorL2),
        sp::BinaryOperator::Spaceship => Ok(BinOp::VectorCosine),
        sp::BinaryOperator::Custom(s) if s == "<#>" => Ok(BinOp::VectorInner),
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

    if let Some(over) = &func.over {
        let spec = match over {
            sp::WindowType::WindowSpec(ws) => convert_window_spec(ws)?,
            sp::WindowType::NamedWindow(_) => {
                return Err(SqlError::Unsupported("named windows".into()));
            }
        };
        return Ok(Expr::WindowFunction { name, args, spec });
    }

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

        sp::DataType::JSON => Ok(DataType::Json),
        sp::DataType::JSONB => Ok(DataType::Jsonb),

        sp::DataType::TsVector => Ok(DataType::TsVector),
        sp::DataType::TsQuery => Ok(DataType::TsQuery),

        sp::DataType::Custom(name, modifiers) => {
            if name.0.len() == 1 {
                if let sp::ObjectNamePart::Identifier(id) = &name.0[0] {
                    if id.value.eq_ignore_ascii_case("vector") {
                        if modifiers.len() != 1 {
                            return Err(SqlError::Parse(
                                "VECTOR requires exactly one dimension argument".into(),
                            ));
                        }
                        let dim: u16 = modifiers[0].parse().map_err(|_| {
                            SqlError::Parse(format!(
                                "VECTOR dimension must be a positive integer, got '{}'",
                                modifiers[0]
                            ))
                        })?;
                        if dim == 0 {
                            return Err(SqlError::Parse("VECTOR dimension must be >= 1".into()));
                        }
                        return Ok(DataType::Vector { dim });
                    }
                }
            }
            Err(SqlError::Unsupported(format!("data type: {dt}")))
        }

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
