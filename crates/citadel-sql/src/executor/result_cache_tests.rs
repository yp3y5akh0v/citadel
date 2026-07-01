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
        "WITH x AS (SELECT NOW() AS n) SELECT n FROM x"
    ));
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
