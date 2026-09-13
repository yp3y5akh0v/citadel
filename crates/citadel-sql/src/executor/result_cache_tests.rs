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

#[test]
fn cacheability_includes_lazy_schema_defaults() {
    for (expression, expected) in [
        ("42", true),
        ("$1", false),
        ("COALESCE($1, 42)", false),
        ("DATE('2024-01-01')", true),
        ("CURRENT_DATE", false),
        ("CLOCK_TIMESTAMP()", false),
        ("DATE(CAST('now' AS TEXT))", false),
        ("DATE($1)", false),
        ("RANDOM()", false),
        (
            r#"JSONB_PATH_QUERY_FIRST_TZ('"12:00:00"'::JSONB, '$.time_tz()')"#,
            false,
        ),
        (
            r#"JSONB_PATH_QUERY_FIRST('"2023-08-15T12:34:56+05:30"'::JSONB, '$.time_tz()')"#,
            false,
        ),
        (r#"JSONB_PATH_QUERY_FIRST('{"n":1}'::JSONB, '$.n')"#, true),
    ] {
        let mut schema = schema_with_t();
        let mut table = schema.get("t").unwrap().clone();
        let query = parse_query(&format!("SELECT {expression}"));
        let crate::parser::QueryBody::Select(select) = query.body else {
            unreachable!()
        };
        let crate::parser::SelectColumn::Expr { expr, .. } = &select.columns[0] else {
            unreachable!()
        };
        table.columns[1].default_expr = Some(expr.clone());
        schema.register(table);
        assert_eq!(
            cacheable(&schema, "SELECT v FROM t"),
            expected,
            "{expression}"
        );
    }
}

#[test]
fn self_referencing_schema_default_is_not_recursed_during_cache_admission() {
    let mut schema = schema_with_t();
    let mut table = schema.get("t").unwrap().clone();
    let query = parse_query("SELECT ((SELECT v FROM t) COLLATE BINARY)");
    let crate::parser::QueryBody::Select(select) = query.body else {
        unreachable!()
    };
    let crate::parser::SelectColumn::Expr { expr, .. } = &select.columns[0] else {
        unreachable!()
    };
    table.columns[1].default_expr = Some(expr.clone());
    schema.register(table);
    assert!(!cacheable(&schema, "SELECT v FROM t"));
}

#[test]
fn cacheability_keeps_cte_names_in_their_query_scope() {
    let mut schema = schema_with_t();
    let mut table = schema.get("t").unwrap().clone();
    table.columns[1].default_expr = Some(crate::parser::parse_sql_expr("CURRENT_DATE").unwrap());
    schema.register(table);
    for sql in [
        "SELECT t.v FROM (WITH t AS (SELECT 1 AS id) SELECT id FROM t) a JOIN t ON a.id=t.id",
        "WITH t AS (SELECT v FROM t) SELECT v FROM t",
        "WITH RECURSIVE t(v) AS (SELECT v FROM t UNION ALL SELECT v FROM t WHERE 0) SELECT v FROM t",
        "SELECT t.v FROM (WITH t AS (SELECT 1 AS id) SELECT id FROM t) a, t",
    ] {
        assert!(!cacheable(&schema, sql), "{sql}");
    }
    for sql in [
        "WITH t AS (SELECT 1 AS v) SELECT v FROM t",
        "WITH t AS (SELECT 1 AS v) SELECT a.v FROM (WITH t AS (SELECT 2 AS v) SELECT v FROM t) a JOIN t ON a.v=t.v",
        "WITH RECURSIVE t(v) AS (SELECT 1 UNION ALL SELECT v+1 FROM t WHERE v<3) SELECT v FROM t",
    ] {
        assert!(cacheable(&schema, sql), "{sql}");
    }
}

#[test]
fn cacheability_resolves_views_without_the_callers_cte_namespace() {
    let mut schema = schema_with_t();
    let mut table = schema.get("t").unwrap().clone();
    table.columns[1].default_expr = Some(crate::parser::parse_sql_expr("CURRENT_DATE").unwrap());
    schema.register(table);
    schema.register_view(crate::types::ViewDef {
        name: "contextual_view".into(),
        sql: "SELECT id, v FROM t".into(),
        column_aliases: Vec::new(),
    });
    assert!(!cacheable(
        &schema,
        "WITH t AS (SELECT 1 AS id) SELECT v FROM contextual_view"
    ));
    schema.register_view(crate::types::ViewDef {
        name: "pure_view".into(),
        sql: "SELECT 1 AS x".into(),
        column_aliases: Vec::new(),
    });
    assert!(cacheable(
        &schema,
        "WITH t AS (SELECT 1 AS v) SELECT a.x, t.v FROM pure_view a JOIN t ON a.x=t.v"
    ));
}
