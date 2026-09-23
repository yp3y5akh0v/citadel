use citadel_sql::dialect::CitadelDialect;
use sqlparser::ast::{BinaryOperator, ColumnOption, Expr, Statement};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

fn expression(sql: &str) -> Expr {
    // Exercise Citadel directly: a GenericDialect retry must not mask errors.
    let dialect = CitadelDialect::new();
    let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
    let expression = parser.parse_expr().unwrap();
    assert_eq!(parser.peek_token().token, Token::EOF);
    expression
}

fn collated(expr: &Expr, expected_expr: &str, expected_collation: &str) {
    let Expr::Collate { expr, collation } = expr else {
        panic!("expected COLLATE, got {expr:?}");
    };
    assert_eq!(expr.to_string(), expected_expr);
    assert_eq!(collation.to_string(), expected_collation);
}

#[test]
fn qualified_collation_binds_on_both_sides_of_comparison() {
    let Expr::BinaryOp { left, op, right } =
        expression("t.name COLLATE BINARY=r.name COLLATE NOCASE")
    else {
        panic!("expected comparison");
    };
    assert_eq!(op, BinaryOperator::Eq);
    collated(&left, "t.name", "BINARY");
    collated(&right, "r.name", "NOCASE");
}

#[test]
fn delimited_qualified_names_need_no_extra_whitespace() {
    let Expr::BinaryOp { left, op, right } = expression(r#""t"."name"COLLATE"NOCASE"="r"."name""#)
    else {
        panic!("expected comparison");
    };
    assert_eq!(op, BinaryOperator::Eq);
    collated(&left, r#""t"."name""#, r#""NOCASE""#);
    assert_eq!(right.to_string(), r#""r"."name""#);
}

#[test]
fn collation_binds_more_tightly_than_concatenation_and_comparison() {
    for sql in [
        "t.name COLLATE NOCASE||r.name='ab'",
        "t.name||r.name COLLATE NOCASE='ab'",
    ] {
        let Expr::BinaryOp { left, op, right } = expression(sql) else {
            panic!("expected comparison");
        };
        assert_eq!(op, BinaryOperator::Eq);
        assert_eq!(right.to_string(), "'ab'");
        let Expr::BinaryOp { left, op, right } = *left else {
            panic!("expected concatenation");
        };
        assert_eq!(op, BinaryOperator::StringConcat);
        if sql.starts_with("t.name COLLATE") {
            collated(&left, "t.name", "NOCASE");
            assert_eq!(right.to_string(), "r.name");
        } else {
            assert_eq!(left.to_string(), "t.name");
            collated(&right, "r.name", "NOCASE");
        }
    }
}

#[test]
fn casts_and_parenthesized_qualified_collations_parse() {
    let Expr::Collate { expr, collation } = expression("t.name::TEXT COLLATE BINARY") else {
        panic!("expected collated cast");
    };
    assert!(matches!(*expr, Expr::Cast { .. }));
    assert_eq!(collation.to_string(), "BINARY");
    let Expr::Nested(expr) = expression("(t.name COLLATE BINARY)") else {
        panic!("expected nested collation");
    };
    collated(&expr, "t.name", "BINARY");
}

#[test]
fn column_definition_collation_remains_a_column_option() {
    let statements = Parser::parse_sql(
        &CitadelDialect::new(),
        "CREATE TABLE t (name TEXT DEFAULT 'A' COLLATE NOCASE)",
    )
    .unwrap();
    let Statement::CreateTable(table) = &statements[0] else {
        panic!("expected CREATE TABLE");
    };
    let options = &table.columns[0].options;
    assert!(options.iter().any(|option| matches!(&option.option,
        ColumnOption::Default(Expr::Value(value)) if value.to_string() == "'A'")));
    assert!(options.iter().any(|option| matches!(&option.option,
        ColumnOption::Collation(name) if name.to_string() == "NOCASE")));
}

#[test]
fn correlated_where_with_qualified_collation_parses_without_retry() {
    Parser::parse_sql(
        &CitadelDialect::new(),
        "SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM refs r WHERE r.id=t.id AND (((t.name COLLATE BINARY) = r.name) OR r.id=99))",
    )
    .unwrap();
}

#[test]
fn incomplete_infix_collation_reports_an_error() {
    let dialect = CitadelDialect::new();
    let mut parser = Parser::new(&dialect)
        .try_with_sql("t.name COLLATE")
        .unwrap();
    assert!(parser.parse_expr().is_err());
}
