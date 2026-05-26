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

#[test]
fn parses_at_question_tz_operator() {
    parse_statements("SELECT data @?_tz '$.x' FROM t").unwrap();
}

#[test]
fn parses_at_at_tz_operator() {
    parse_statements("SELECT data @@_tz '$.x ? (@ > 0)' FROM t").unwrap();
}

#[test]
fn parses_at_tz_with_space_before_suffix() {
    parse_statements("SELECT data @? _tz '$.x' FROM t").unwrap();
}

#[test]
fn parses_at_question_tz_inside_complex_where() {
    parse_statements(
        "SELECT id FROM t WHERE data @? '$.a' AND data @?_tz '$.b.timestamp() ? (@ > \"2026-01-01T00:00:00Z\")'",
    )
    .unwrap();
}

#[test]
fn at_tz_inside_string_literal_not_rewritten() {
    let stmts = parse_statements("SELECT '@?_tz' AS x FROM t").unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn falls_back_to_plain_at_when_no_underscore_tz() {
    parse_statements("SELECT data @? '$.x' FROM t").unwrap();
    parse_statements("SELECT data @@ '$.x ? (@ > 0)' FROM t").unwrap();
}
