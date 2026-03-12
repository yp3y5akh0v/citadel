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
    Insert(InsertStmt),
    Select(SelectStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub primary_key: Vec<String>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_primary_key: bool,
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
pub struct InsertStmt {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
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
    QualifiedColumn { table: String, column: String },
    BinaryOp { left: Box<Expr>, op: BinOp, right: Box<Expr> },
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Function { name: String, args: Vec<Expr> },
    CountStar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add, Sub, Mul, Div, Mod,
    Eq, NotEq, Lt, Gt, LtEq, GtEq,
    And, Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

// ── Parser entry point ──────────────────────────────────────────────

pub fn parse_sql(sql: &str) -> Result<Statement> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql)
        .map_err(|e| SqlError::Parse(e.to_string()))?;

    if stmts.is_empty() {
        return Err(SqlError::Parse("empty SQL".into()));
    }
    if stmts.len() > 1 {
        return Err(SqlError::Unsupported("multiple statements".into()));
    }

    convert_statement(stmts.into_iter().next().unwrap())
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
        sp::Statement::Insert(insert) => convert_insert(insert),
        sp::Statement::Query(query) => convert_query(*query),
        sp::Statement::Update(update) => convert_update(update),
        sp::Statement::Delete(delete) => convert_delete(delete),
        sp::Statement::StartTransaction { .. } => Ok(Statement::Begin),
        sp::Statement::Commit { .. } => Ok(Statement::Commit),
        sp::Statement::Rollback { .. } => Ok(Statement::Rollback),
        _ => Err(SqlError::Unsupported(format!(
            "statement type: {}",
            stmt
        ))),
    }
}

fn convert_create_table(ct: sp::CreateTable) -> Result<Statement> {
    let name = object_name_to_string(&ct.name);
    let if_not_exists = ct.if_not_exists;

    let mut columns = Vec::new();
    let mut inline_pk: Vec<String> = Vec::new();

    for col_def in &ct.columns {
        let col_name = col_def.name.value.clone();
        let data_type = convert_data_type(&col_def.data_type)?;
        let mut nullable = true;
        let mut is_primary_key = false;

        for opt in &col_def.options {
            match &opt.option {
                sp::ColumnOption::NotNull => nullable = false,
                sp::ColumnOption::Null => nullable = true,
                sp::ColumnOption::PrimaryKey(_) => {
                    is_primary_key = true;
                    nullable = false;
                    inline_pk.push(col_name.clone());
                }
                _ => {}
            }
        }

        columns.push(ColumnSpec {
            name: col_name,
            data_type,
            nullable,
            is_primary_key,
        });
    }

    // Check table-level constraints for PRIMARY KEY
    for constraint in &ct.constraints {
        if let sp::TableConstraint::PrimaryKey(pk_constraint) = constraint {
            for idx_col in &pk_constraint.columns {
                // IndexColumn has a `column: OrderByExpr` field; extract ident from the expr
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
    }

    Ok(Statement::CreateTable(CreateTableStmt {
        name,
        columns,
        primary_key: inline_pk,
        if_not_exists,
    }))
}

fn convert_create_index(ci: sp::CreateIndex) -> Result<Statement> {
    let index_name = ci.name
        .as_ref()
        .map(object_name_to_string)
        .ok_or_else(|| SqlError::Parse("index name required".into()))?;

    let table_name = object_name_to_string(&ci.table_name);

    let columns: Vec<String> = ci.columns.iter().map(|idx_col| {
        match &idx_col.column.expr {
            sp::Expr::Identifier(ident) => Ok(ident.value.clone()),
            other => Err(SqlError::Unsupported(format!("expression index: {other}"))),
        }
    }).collect::<Result<_>>()?;

    if columns.is_empty() {
        return Err(SqlError::Parse("index must have at least one column".into()));
    }

    Ok(Statement::CreateIndex(CreateIndexStmt {
        index_name,
        table_name,
        columns,
        unique: ci.unique,
        if_not_exists: ci.if_not_exists,
    }))
}

fn convert_insert(insert: sp::Insert) -> Result<Statement> {
    let table = match &insert.table {
        sp::TableObject::TableName(name) => object_name_to_string(name),
        _ => return Err(SqlError::Unsupported("INSERT into non-table object".into())),
    };

    let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();

    let source = insert.source.ok_or_else(|| {
        SqlError::Parse("INSERT requires VALUES".into())
    })?;

    let values = match *source.body {
        sp::SetExpr::Values(sp::Values { rows, .. }) => {
            let mut result = Vec::new();
            for row in rows {
                let mut exprs = Vec::new();
                for expr in row {
                    exprs.push(convert_expr(&expr)?);
                }
                result.push(exprs);
            }
            result
        }
        _ => return Err(SqlError::Unsupported("INSERT ... SELECT".into())),
    };

    Ok(Statement::Insert(InsertStmt {
        table,
        columns,
        values,
    }))
}

fn convert_query(query: sp::Query) -> Result<Statement> {
    let select = match *query.body {
        sp::SetExpr::Select(sel) => *sel,
        _ => return Err(SqlError::Unsupported("UNION/INTERSECT/EXCEPT".into())),
    };

    let distinct = match &select.distinct {
        Some(sp::Distinct::Distinct) => true,
        Some(sp::Distinct::On(_)) => {
            return Err(SqlError::Unsupported("DISTINCT ON".into()));
        }
        _ => false,
    };

    // FROM clause
    if select.from.len() != 1 {
        if select.from.is_empty() {
            return Err(SqlError::Parse("SELECT requires FROM".into()));
        }
        return Err(SqlError::Unsupported("comma-separated FROM tables".into()));
    }

    let table_with_joins = &select.from[0];
    let (from, from_alias) = match &table_with_joins.relation {
        sp::TableFactor::Table { name, alias, .. } => {
            let table_name = object_name_to_string(name);
            let alias_str = alias.as_ref().map(|a| a.name.value.clone());
            (table_name, alias_str)
        }
        _ => return Err(SqlError::Unsupported("non-table FROM source".into())),
    };

    let joins = table_with_joins.joins.iter()
        .map(|j| convert_join(j))
        .collect::<Result<Vec<_>>>()?;

    // Projection
    let columns: Vec<SelectColumn> = select.projection.iter()
        .map(convert_select_item)
        .collect::<Result<_>>()?;

    // WHERE
    let where_clause = select.selection.as_ref()
        .map(convert_expr)
        .transpose()?;

    // ORDER BY
    let order_by = if let Some(ref ob) = query.order_by {
        match &ob.kind {
            sp::OrderByKind::Expressions(exprs) => {
                exprs.iter().map(convert_order_by_expr).collect::<Result<_>>()?
            }
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
            let o = offset.as_ref().map(|o| convert_expr(&o.value)).transpose()?;
            (l, o)
        }
        Some(sp::LimitClause::OffsetCommaLimit { limit, offset }) => {
            let l = Some(convert_expr(limit)?);
            let o = Some(convert_expr(offset)?);
            (l, o)
        }
        None => (None, None),
    };

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

    Ok(Statement::Select(SelectStmt {
        columns,
        from,
        from_alias,
        joins,
        distinct,
        where_clause,
        order_by,
        limit,
        offset,
        group_by,
        having,
    }))
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

    let assignments = update.assignments.iter()
        .map(|a| {
            let col = match &a.target {
                sp::AssignmentTarget::ColumnName(name) => object_name_to_string(name),
                _ => return Err(SqlError::Unsupported("tuple assignment".into())),
            };
            let expr = convert_expr(&a.value)?;
            Ok((col, expr))
        })
        .collect::<Result<_>>()?;

    let where_clause = update.selection.as_ref()
        .map(convert_expr)
        .transpose()?;

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

    let where_clause = delete.selection.as_ref()
        .map(convert_expr)
        .transpose()?;

    Ok(Statement::Delete(DeleteStmt {
        table: table_name,
        where_clause,
    }))
}

// ── Expression conversion ───────────────────────────────────────────

fn convert_expr(expr: &sp::Expr) -> Result<Expr> {
    match expr {
        sp::Expr::Value(v) => convert_value(&v.value),
        sp::Expr::Identifier(ident) => Ok(Expr::Column(ident.value.clone())),
        sp::Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                Ok(Expr::QualifiedColumn {
                    table: parts[0].value.clone(),
                    column: parts[1].value.clone(),
                })
            } else {
                Ok(Expr::Column(parts.last().unwrap().value.clone()))
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
        sp::Value::SingleQuotedString(s) => Ok(Expr::Literal(Value::Text(s.clone()))),
        sp::Value::Boolean(b) => Ok(Expr::Literal(Value::Boolean(*b))),
        sp::Value::Null => Ok(Expr::Literal(Value::Null)),
        _ => Err(SqlError::Unsupported(format!("value type: {val}"))),
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
        _ => Err(SqlError::Unsupported(format!("binary op: {op}"))),
    }
}

fn convert_function(func: &sp::Function) -> Result<Expr> {
    let name = object_name_to_string(&func.name).to_ascii_uppercase();

    // COUNT(*)
    match &func.args {
        sp::FunctionArguments::List(list) => {
            if list.args.is_empty() && name == "COUNT" {
                return Ok(Expr::CountStar);
            }
            let args = list.args.iter()
                .map(|arg| match arg {
                    sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e)) => convert_expr(e),
                    sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Wildcard) => {
                        if name == "COUNT" {
                            Ok(Expr::CountStar)
                        } else {
                            Err(SqlError::Unsupported(format!("{name}(*)")))
                        }
                    }
                    _ => Err(SqlError::Unsupported(format!("function arg type in {name}"))),
                })
                .collect::<Result<Vec<_>>>()?;

            // If this is COUNT(*), the args will contain CountStar
            if name == "COUNT" && args.len() == 1 && matches!(args[0], Expr::CountStar) {
                return Ok(Expr::CountStar);
            }

            Ok(Expr::Function { name, args })
        }
        sp::FunctionArguments::None => {
            if name == "COUNT" {
                Ok(Expr::CountStar)
            } else {
                Ok(Expr::Function { name, args: vec![] })
            }
        }
        sp::FunctionArguments::Subquery(_) => {
            Err(SqlError::Unsupported("subquery in function".into()))
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

        sp::DataType::Blob(_)
        | sp::DataType::Bytea => Ok(DataType::Blob),

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
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)"
        ).unwrap();

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
        let stmt = parse_sql(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
        ).unwrap();

        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.table, "users");
                assert_eq!(ins.columns, vec!["id", "name"]);
                assert_eq!(ins.values.len(), 2);
                assert!(matches!(ins.values[0][0], Expr::Literal(Value::Integer(1))));
                assert!(matches!(&ins.values[0][1], Expr::Literal(Value::Text(s)) if s == "Alice"));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_select_all() {
        let stmt = parse_sql("SELECT * FROM users").unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.from, "users");
                assert!(matches!(sel.columns[0], SelectColumn::AllColumns));
                assert!(sel.where_clause.is_none());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_where() {
        let stmt = parse_sql("SELECT id, name FROM users WHERE age > 18").unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.columns.len(), 2);
                assert!(sel.where_clause.is_some());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_order_limit() {
        let stmt = parse_sql(
            "SELECT * FROM users ORDER BY name ASC LIMIT 10 OFFSET 5"
        ).unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.order_by.len(), 1);
                assert!(!sel.order_by[0].descending);
                assert!(sel.limit.is_some());
                assert!(sel.offset.is_some());
            }
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
            Statement::Select(sel) => {
                assert_eq!(sel.columns.len(), 2);
                match &sel.columns[0] {
                    SelectColumn::Expr { expr: Expr::CountStar, .. } => {}
                    other => panic!("expected CountStar, got {other:?}"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_group_by_having() {
        let stmt = parse_sql(
            "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5"
        ).unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.group_by.len(), 1);
                assert!(sel.having.is_some());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_expressions() {
        let stmt = parse_sql("SELECT id + 1, -price, NOT active FROM items").unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.columns.len(), 3);
                // id + 1
                match &sel.columns[0] {
                    SelectColumn::Expr { expr: Expr::BinaryOp { op: BinOp::Add, .. }, .. } => {}
                    other => panic!("expected BinaryOp Add, got {other:?}"),
                }
                // -price
                match &sel.columns[1] {
                    SelectColumn::Expr { expr: Expr::UnaryOp { op: UnaryOp::Neg, .. }, .. } => {}
                    other => panic!("expected UnaryOp Neg, got {other:?}"),
                }
                // NOT active
                match &sel.columns[2] {
                    SelectColumn::Expr { expr: Expr::UnaryOp { op: UnaryOp::Not, .. }, .. } => {}
                    other => panic!("expected UnaryOp Not, got {other:?}"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_is_null() {
        let stmt = parse_sql("SELECT * FROM t WHERE x IS NULL").unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert!(matches!(sel.where_clause, Some(Expr::IsNull(_))));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_inner_join() {
        let stmt = parse_sql("SELECT * FROM a JOIN b ON a.id = b.id").unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.from, "a");
                assert_eq!(sel.joins.len(), 1);
                assert_eq!(sel.joins[0].join_type, JoinType::Inner);
                assert_eq!(sel.joins[0].table.name, "b");
                assert!(sel.joins[0].on_clause.is_some());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_inner_join_explicit() {
        let stmt = parse_sql("SELECT * FROM a INNER JOIN b ON a.id = b.a_id").unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.joins.len(), 1);
                assert_eq!(sel.joins[0].join_type, JoinType::Inner);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_cross_join() {
        let stmt = parse_sql("SELECT * FROM a CROSS JOIN b").unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.joins.len(), 1);
                assert_eq!(sel.joins[0].join_type, JoinType::Cross);
                assert!(sel.joins[0].on_clause.is_none());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_left_join() {
        let stmt = parse_sql("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id").unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.joins.len(), 1);
                assert_eq!(sel.joins[0].join_type, JoinType::Left);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_table_alias() {
        let stmt = parse_sql("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id").unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.from, "users");
                assert_eq!(sel.from_alias.as_deref(), Some("u"));
                assert_eq!(sel.joins[0].table.name, "orders");
                assert_eq!(sel.joins[0].table.alias.as_deref(), Some("o"));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_multi_join() {
        let stmt = parse_sql(
            "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id"
        ).unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.joins.len(), 2);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_qualified_column() {
        let stmt = parse_sql("SELECT u.id, u.name FROM users u").unwrap();
        match stmt {
            Statement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr { expr: Expr::QualifiedColumn { table, column }, .. } => {
                        assert_eq!(table, "u");
                        assert_eq!(column, "id");
                    }
                    other => panic!("expected QualifiedColumn, got {other:?}"),
                }
            }
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
                assert_eq!(ct.columns[3].data_type, DataType::Real);    // REAL
                assert_eq!(ct.columns[4].data_type, DataType::Real);    // DOUBLE
                assert_eq!(ct.columns[5].data_type, DataType::Text);    // VARCHAR
                assert_eq!(ct.columns[6].data_type, DataType::Boolean); // BOOLEAN
                assert_eq!(ct.columns[7].data_type, DataType::Blob);    // BLOB
                assert_eq!(ct.columns[8].data_type, DataType::Blob);    // BYTEA
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_boolean_literals() {
        let stmt = parse_sql("INSERT INTO t (a, b) VALUES (true, false)").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert!(matches!(ins.values[0][0], Expr::Literal(Value::Boolean(true))));
                assert!(matches!(ins.values[0][1], Expr::Literal(Value::Boolean(false))));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_null_literal() {
        let stmt = parse_sql("INSERT INTO t (a) VALUES (NULL)").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert!(matches!(ins.values[0][0], Expr::Literal(Value::Null)));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_alias() {
        let stmt = parse_sql("SELECT id AS user_id FROM users").unwrap();
        match stmt {
            Statement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr { alias: Some(a), .. } => assert_eq!(a, "user_id"),
                    other => panic!("expected alias, got {other:?}"),
                }
            }
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
    fn parse_select_distinct() {
        let stmt = parse_sql("SELECT DISTINCT name FROM users").unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert!(sel.distinct);
                assert_eq!(sel.columns.len(), 1);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_without_distinct() {
        let stmt = parse_sql("SELECT name FROM users").unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert!(!sel.distinct);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_distinct_all_columns() {
        let stmt = parse_sql("SELECT DISTINCT * FROM users").unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert!(sel.distinct);
                assert!(matches!(sel.columns[0], SelectColumn::AllColumns));
            }
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
    fn parse_table_constraint_pk() {
        let stmt = parse_sql(
            "CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a))"
        ).unwrap();
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
