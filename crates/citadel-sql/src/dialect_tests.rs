use super::*;

#[test]
fn parses_pg_jsonb_question_operator() {
    let stmts = parse_statements("SELECT data ? 'role' FROM t").unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn parses_pg_jsonb_question_pipe_operator() {
    let stmts = parse_statements("SELECT data ?| ARRAY['a','b'] FROM t").unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn parses_pg_jsonb_contains_operator() {
    let stmts =
        parse_statements("SELECT id FROM t WHERE data @> '{\"role\":\"admin\"}'::jsonb").unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn parses_truncate_cascade() {
    parse_statements("TRUNCATE TABLE t CASCADE").unwrap();
}

#[test]
fn parse_expr_pg_arrow() {
    parse_expr("data ->> 'name'").unwrap();
}

#[test]
fn parse_expr_arithmetic() {
    parse_expr("1 + 2 * 3").unwrap();
}

#[test]
fn invalid_sql_returns_error() {
    assert!(parse_statements("NOT VALID SQL").is_err());
}
