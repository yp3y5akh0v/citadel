use super::*;
use crate::schema::SchemaManager;
use crate::types::{ColumnDef, DataType, QueryResult, TableSchema};

fn parse_query(sql: &str) -> crate::parser::SelectQuery {
    match crate::parser::parse_sql(sql).unwrap() {
        Statement::Select(q) => *q,
        other => panic!("expected select, got {other:?}"),
    }
}

fn schema_with_t() -> SchemaManager {
    let mut s = SchemaManager::empty();
    let mut id = ColumnDef {
        name: "id".into(),
        data_type: DataType::Integer,
        nullable: false,
        position: 0,
        default_expr: None,
        default_sql: None,
        check_expr: None,
        check_sql: None,
        check_name: None,
        is_with_timezone: false,
        generated_expr: None,
        generated_sql: None,
        generated_kind: None,
        collation: crate::types::Collation::Binary,
    };
    let mut v = id.clone();
    id.name = "id".into();
    v.name = "v".into();
    v.nullable = true;
    v.position = 1;
    s.register(TableSchema::new(
        "t".into(),
        vec![id, v],
        vec![0],
        vec![],
        vec![],
        vec![],
    ));
    s
}

fn cacheable(schema: &SchemaManager, sql: &str) -> bool {
    is_result_cacheable(schema, &parse_query(sql))
}

#[test]
fn cacheable_accepts_pure_reads() {
    let s = schema_with_t();
    assert!(cacheable(&s, "SELECT 1"));
    assert!(cacheable(&s, "SELECT SUM(v) FROM t"));
    assert!(cacheable(
        &s,
        "SELECT v FROM t WHERE v > $1 ORDER BY v LIMIT 3"
    ));
    assert!(cacheable(
        &s,
        "WITH big AS (SELECT v FROM t WHERE v > 10) SELECT COUNT(*) FROM big"
    ));
    assert!(cacheable(&s, "SELECT DATE('2024-01-01')"));
    assert!(cacheable(&s, "SELECT v FROM t UNION SELECT v FROM t"));
}

#[test]
fn cacheable_refuses_volatile_and_unknown() {
    let s = schema_with_t();
    assert!(!cacheable(&s, "SELECT RANDOM()"));
    assert!(!cacheable(&s, "SELECT NOW()"));
    assert!(!cacheable(&s, "SELECT CLOCK_TIMESTAMP()"));
    assert!(!cacheable(&s, "SELECT DATE('now')"));
    assert!(!cacheable(&s, "SELECT DATE(v) FROM t"));
    assert!(!cacheable(&s, "SELECT v FROM missing_table"));
    assert!(!cacheable(&s, "SELECT v FROM t WHERE v > RANDOM()"));
    assert!(!cacheable(
        &s,
        "SELECT ROW_NUMBER() OVER (ORDER BY RANDOM()) FROM t"
    ));
    assert!(!cacheable(
        &s,
        "SELECT ROW_NUMBER() OVER (PARTITION BY RANDOM() ORDER BY v) FROM t"
    ));
    assert!(!cacheable(
        &s,
        "WITH x AS (SELECT NOW() AS n) SELECT n FROM x"
    ));
}

#[test]
fn jsonpath_cacheability_is_path_aware() {
    let s = schema_with_t();
    assert!(cacheable(
        &s,
        r#"SELECT JSONB_PATH_QUERY_FIRST('{"profile":{"id":1}}'::JSONB, '$.profile')"#
    ));
    assert!(cacheable(
        &s,
        r#"SELECT '{"active":true}'::JSONB @? '$.active'"#
    ));
    assert!(cacheable(
        &s,
        r#"SELECT JSONB_PATH_MATCH('{"priority":1}'::JSONB, '$.priority == 1')"#
    ));
    assert!(cacheable(
        &s,
        r#"SELECT '{"priority":1}'::JSONB @@ '$.priority == 1'"#
    ));
    assert!(cacheable(
        &s,
        r#"SELECT JSONB_PATH_QUERY_FIRST('"2024-01-01"'::JSONB, '$.date()')"#
    ));
    assert!(cacheable(
        &s,
        r#"SELECT JSONB_PATH_MATCH('{"a":"2024-01-01","b":"2024-01-02"}'::JSONB, '$.a.date() < $.b.date()')"#
    ));
    assert!(cacheable(
        &s,
        r#"SELECT JSONB_PATH_MATCH('{"priority":1}'::JSONB, '$.priority == $minimum', '{"minimum":1}'::JSONB)"#
    ));

    // TimestampTz-to-TimeTz is the one cast that reads session context even
    // through a standard entry point.
    assert!(!cacheable(
        &s,
        r#"SELECT JSONB_PATH_QUERY_FIRST('"2024-01-01T00:00:00+00:00"'::JSONB, '$.time_tz()')"#
    ));
    // `_TZ` entry points and paths supplied at runtime always fail closed.
    assert!(!cacheable(
        &s,
        r#"SELECT JSONB_PATH_EXISTS_TZ('{"active":true}'::JSONB, '$.active')"#
    ));
    assert!(!cacheable(
        &s,
        r#"SELECT JSONB_PATH_EXISTS('{"active":true}'::JSONB, $1)"#
    ));
}

#[test]
fn text_search_overloads_are_cacheable_without_treating_queries_as_json_paths() {
    let s = schema_with_t();
    for constructor in [
        "TO_TSQUERY",
        "PLAINTO_TSQUERY",
        "PHRASETO_TSQUERY",
        "WEBSEARCH_TO_TSQUERY",
    ] {
        for sql in [
            format!("SELECT id FROM t WHERE v @@ {constructor}($1)"),
            format!("SELECT {constructor}('rust') @@ TO_TSVECTOR('rust')"),
            format!("SELECT id FROM t WHERE v @@ {constructor}('english', $1)"),
        ] {
            assert!(cacheable(&s, &sql), "{sql}");
        }
    }
    for sql in [
        "SELECT id, TS_RANK(v, TO_TSQUERY('rust & database')) AS r FROM t \
         WHERE v @@ TO_TSQUERY('rust & database') ORDER BY r DESC LIMIT 10",
        "SELECT id FROM t WHERE TO_TSVECTOR('rust database') @@ $1",
        "SELECT id FROM t WHERE 'rust database' @@ $1",
        "SELECT id FROM t WHERE CAST(v AS TEXT) @@ $1",
        "SELECT id FROM t WHERE CAST(v AS TSVECTOR) @@ $1",
        "SELECT id FROM t WHERE v @@ CAST($1 AS TSQUERY)",
        "SELECT id FROM t WHERE v @@ (TO_TSQUERY($1) COLLATE BINARY)",
        "SELECT id FROM t WHERE (CAST(v AS TEXT) COLLATE BINARY) @@ $1",
    ] {
        assert!(cacheable(&s, sql), "{sql}");
    }
}

#[test]
fn text_search_type_proofs_preserve_jsonpath_and_child_volatility_guards() {
    let s = schema_with_t();
    for sql in [
        "SELECT id FROM t WHERE v @@ $1",
        "SELECT id FROM t WHERE CAST(v AS JSONB) @@ $1",
        "SELECT id FROM t WHERE (CAST(v AS JSONB) COLLATE BINARY) @@ $1",
        "SELECT id FROM t WHERE v @@ CAST(TO_TSQUERY('rust') AS TEXT)",
        "SELECT id FROM t WHERE CAST(v AS JSONB) @@ CAST(TO_TSQUERY('rust') AS TEXT)",
        "SELECT id FROM t WHERE CAST(v AS JSONB) @? CAST(TO_TSQUERY('rust') AS TEXT)",
        r#"SELECT id FROM t WHERE CAST(v AS JSONB) @@ '$.time_tz().string() == "17:04:56+10:00"'"#,
        "SELECT id FROM t WHERE v @@ TO_TSQUERY(CAST(RANDOM() AS TEXT))",
        "SELECT id FROM t WHERE TO_TSVECTOR(CAST(NOW() AS TEXT)) @@ $1",
        "SELECT id FROM t WHERE v @@ TO_TSQUERY(CAST(\
         JSONB_PATH_QUERY_FIRST_TZ(CAST(v AS JSONB), '$.time().string()') AS TEXT))",
        "SELECT id FROM t WHERE v @@ CAST(\
         JSONB_PATH_QUERY_FIRST_TZ(CAST(v AS JSONB), '$.time().string()') AS TSQUERY)",
    ] {
        assert!(!cacheable(&s, sql), "{sql}");
    }
}

#[test]
fn params_match_is_bit_exact() {
    assert!(params_match(
        &[Value::Real(1.5), Value::Integer(2)],
        &[Value::Real(1.5), Value::Integer(2)]
    ));
    // -0.0 == 0.0 numerically, but results can differ textually.
    assert!(!params_match(&[Value::Real(0.0)], &[Value::Real(-0.0)]));
    // NaN != NaN numerically, but identical bits are the same param.
    assert!(params_match(
        &[Value::Real(f64::NAN)],
        &[Value::Real(f64::NAN)]
    ));
    // Cross-type numeric equality must not conflate keys.
    assert!(!params_match(&[Value::Integer(1)], &[Value::Real(1.0)]));
    assert!(!params_match(&[Value::Integer(1)], &[]));
}

#[test]
fn slot_serves_only_same_generation_and_params() {
    let slot = ResultCacheSlot::new();
    let result = QueryResult {
        columns: vec!["n".into()],
        rows: vec![vec![Value::Integer(42)]],
    };
    slot.store(7, &[Value::Integer(1)], &result);

    assert_eq!(
        slot.lookup(7, &[Value::Integer(1)]).map(|q| q.rows),
        Some(vec![vec![Value::Integer(42)]])
    );
    assert!(slot.lookup(8, &[Value::Integer(1)]).is_none());
    assert!(slot.lookup(7, &[Value::Integer(2)]).is_none());
}

#[test]
fn oversized_results_and_params_are_not_stored() {
    let slot = ResultCacheSlot::new();
    let big_text = "x".repeat(RESULT_CACHE_MAX_BYTES + 1);
    let big = QueryResult {
        columns: vec!["t".into()],
        rows: vec![vec![Value::Text(big_text.clone().into())]],
    };
    slot.store(1, &[], &big);
    assert!(slot.lookup(1, &[]).is_none());

    let small = QueryResult {
        columns: vec!["n".into()],
        rows: vec![vec![Value::Integer(1)]],
    };
    slot.store(1, &[Value::Text(big_text.into())], &small);
    assert!(slot.lookup(1, &[]).is_none());
}

#[test]
fn oversized_arrays_are_rejected_by_the_budgeted_walk() {
    let too_many_inline_values =
        Value::Array(vec![Value::Null; RESULT_CACHE_MAX_BYTES / 32 + 1].into());
    let result = QueryResult {
        columns: vec!["a".into()],
        rows: vec![vec![too_many_inline_values]],
    };

    assert!(!within_cap(&[], &result));
}

#[test]
fn cache_budget_counts_nested_array_contents_at_the_boundary() {
    let value = Value::Array(
        vec![
            Value::Integer(7),
            Value::Array(vec![Value::Blob(vec![0; 7]), Value::Text("x".repeat(25).into())].into()),
            Value::Array(Vec::new().into()),
        ]
        .into(),
    );
    // Three outer slots, two nested slots, then the blob and heap text.
    let required = 3 * 32 + 2 * 32 + 7 + 25;
    let mut exact = required;
    assert!(value_fits(&value, &mut exact));
    assert_eq!(exact, 0);

    let mut short = required - 1;
    assert!(!value_fits(&value, &mut short));
}
