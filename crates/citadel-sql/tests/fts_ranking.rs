use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::fts::{parse_tsquery, TsVectorBuilder, Weight};
use citadel_sql::{Connection, Value};
use std::sync::Arc;

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn tsvector(lexs: &[(&[u8], u16, Weight)]) -> Arc<[u8]> {
    let mut b = TsVectorBuilder::new();
    for (lex, p, w) in lexs {
        b.push(lex, *p, *w);
    }
    b.build()
}

#[test]
fn ts_rank_zero_for_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = tsvector(&[(b"cat", 1, Weight::D)]);
    let q = parse_tsquery("dog").unwrap().encode();
    let stmt = conn.prepare("SELECT ts_rank($1, $2)").unwrap();
    let rows = stmt
        .query_collect(&[Value::TsVector(v), Value::TsQuery(q)])
        .unwrap();
    let r = match &rows.rows[0][0] {
        Value::Real(x) => *x,
        _ => panic!(),
    };
    assert!(r.abs() < 1e-9, "expected 0, got {r}");
}

#[test]
fn ts_rank_higher_for_higher_weight() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v_a = tsvector(&[(b"cat", 1, Weight::A)]);
    let v_d = tsvector(&[(b"cat", 1, Weight::D)]);
    let q = parse_tsquery("cat").unwrap().encode();
    let stmt = conn.prepare("SELECT ts_rank($1, $2)").unwrap();
    let r_a = match &stmt
        .query_collect(&[Value::TsVector(v_a), Value::TsQuery(q.clone())])
        .unwrap()
        .rows[0][0]
    {
        Value::Real(x) => *x,
        _ => panic!(),
    };
    let r_d = match &stmt
        .query_collect(&[Value::TsVector(v_d), Value::TsQuery(q)])
        .unwrap()
        .rows[0][0]
    {
        Value::Real(x) => *x,
        _ => panic!(),
    };
    assert!(r_a > r_d, "weight A should outrank D: a={r_a}, d={r_d}");
}

#[test]
fn ts_rank_more_terms_more_rank() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v_match_one = tsvector(&[(b"cat", 1, Weight::D)]);
    let v_match_two = tsvector(&[(b"cat", 1, Weight::D), (b"dog", 2, Weight::D)]);
    let q = parse_tsquery("cat & dog").unwrap().encode();
    let stmt = conn.prepare("SELECT ts_rank($1, $2)").unwrap();
    let r1 = match &stmt
        .query_collect(&[Value::TsVector(v_match_one), Value::TsQuery(q.clone())])
        .unwrap()
        .rows[0][0]
    {
        Value::Real(x) => *x,
        _ => panic!(),
    };
    let r2 = match &stmt
        .query_collect(&[Value::TsVector(v_match_two), Value::TsQuery(q)])
        .unwrap()
        .rows[0][0]
    {
        Value::Real(x) => *x,
        _ => panic!(),
    };
    assert!(
        r2 > r1,
        "matching both terms should outrank one: r1={r1}, r2={r2}"
    );
}

#[test]
fn ts_rank_cd_rewards_proximity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v_close = tsvector(&[(b"cat", 1, Weight::D), (b"dog", 2, Weight::D)]);
    let v_far = tsvector(&[(b"cat", 1, Weight::D), (b"dog", 100, Weight::D)]);
    let q = parse_tsquery("cat & dog").unwrap().encode();
    let stmt = conn.prepare("SELECT ts_rank_cd($1, $2)").unwrap();
    let r_close = match &stmt
        .query_collect(&[Value::TsVector(v_close), Value::TsQuery(q.clone())])
        .unwrap()
        .rows[0][0]
    {
        Value::Real(x) => *x,
        _ => panic!(),
    };
    let r_far = match &stmt
        .query_collect(&[Value::TsVector(v_far), Value::TsQuery(q)])
        .unwrap()
        .rows[0][0]
    {
        Value::Real(x) => *x,
        _ => panic!(),
    };
    assert!(
        r_close > r_far,
        "closer cover should rank higher: close={r_close}, far={r_far}"
    );
}

#[test]
fn ts_rank_norm_squash_to_unit_interval() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = tsvector(&[(b"cat" as &[u8], 1, Weight::A)]);
    let q = parse_tsquery("cat").unwrap().encode();
    // norm bit 32 squashes to [0, 1)
    let stmt = conn.prepare("SELECT ts_rank($1, $2, 32)").unwrap();
    let r = match &stmt
        .query_collect(&[Value::TsVector(v), Value::TsQuery(q)])
        .unwrap()
        .rows[0][0]
    {
        Value::Real(x) => *x,
        _ => panic!(),
    };
    assert!((0.0..1.0).contains(&r), "expected (0, 1), got {r}");
}

#[test]
fn length_tsvector_counts_distinct_lexemes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = tsvector(&[
        (b"cat", 1, Weight::D),
        (b"cat", 2, Weight::D),
        (b"dog", 3, Weight::D),
    ]);
    let stmt = conn.prepare("SELECT length($1)").unwrap();
    let rows = stmt.query_collect(&[Value::TsVector(v)]).unwrap();
    assert_eq!(rows.rows[0][0], Value::Integer(2));
}

#[test]
fn numnode_counts_ast_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // `a & b | c` = Or(And(a, b), c) → 5 nodes
    let q = parse_tsquery("a & b | c").unwrap().encode();
    let stmt = conn.prepare("SELECT numnode($1)").unwrap();
    let rows = stmt.query_collect(&[Value::TsQuery(q)]).unwrap();
    assert_eq!(rows.rows[0][0], Value::Integer(5));
}

#[test]
fn to_tsquery_parses_input() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT to_tsquery('cat & dog')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    if let Value::TsQuery(_) = &rows.rows[0][0] {
        // ok
    } else {
        panic!("expected TsQuery value, got {:?}", rows.rows[0][0]);
    }
}

#[test]
fn tokenizer_functions_work() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    for sql in [
        "SELECT to_tsvector('hello world')",
        "SELECT plainto_tsquery('cat dog')",
        "SELECT phraseto_tsquery('cat dog')",
        "SELECT websearch_to_tsquery('cat OR dog')",
        "SELECT ts_lexize('english', 'running')",
    ] {
        let res = conn
            .prepare(sql)
            .unwrap()
            .query_collect(&[])
            .unwrap_or_else(|e| panic!("expected {sql} to succeed, got {e}"));
        let v = &res.rows[0][0];
        assert!(!matches!(v, Value::Null), "{sql} returned NULL");
    }
}
