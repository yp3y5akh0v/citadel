use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::fts::{parse_tsquery, TsVectorBuilder, Weight};
use citadel_sql::{Connection, Value};
use std::sync::Arc;

type WeightedLexeme<'a> = (&'a [u8], u16, Weight);

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn tsvector(lexs: &[WeightedLexeme<'_>]) -> Arc<[u8]> {
    let mut b = TsVectorBuilder::new();
    for (lex, p, w) in lexs {
        b.push(lex, *p, *w).unwrap();
    }
    b.build()
}

#[test]
fn ts_rank_zero_for_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = tsvector(&[(b"cat", 1, Weight::D)]);
    let q = parse_tsquery("dog").unwrap().encode().unwrap();
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
    let q = parse_tsquery("cat").unwrap().encode().unwrap();
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
    let q = parse_tsquery("cat & dog").unwrap().encode().unwrap();
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
    let q = parse_tsquery("cat & dog").unwrap().encode().unwrap();
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
    let q = parse_tsquery("cat").unwrap().encode().unwrap();
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
    let q = parse_tsquery("a & b | c").unwrap().encode().unwrap();
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

fn seed_rank_comparison_tables(conn: &Connection<'_>) {
    let docs: &[(i64, &[WeightedLexeme<'_>])] = &[
        (
            1,
            &[
                (b"cat", 1, Weight::A),
                (b"cat", 2, Weight::A),
                (b"cat", 3, Weight::A),
                (b"dog", 4, Weight::D),
                (b"fox", 5, Weight::B),
            ],
        ),
        (
            2,
            &[
                (b"cat", 1, Weight::D),
                (b"dog", 2, Weight::A),
                (b"dog", 3, Weight::A),
                (b"dog", 4, Weight::A),
                (b"fox", 5, Weight::D),
            ],
        ),
        (
            3,
            &[
                (b"cat", 1, Weight::D),
                (b"dog", 2, Weight::D),
                (b"fox", 3, Weight::D),
            ],
        ),
        (
            4,
            &[
                (b"cat", 1, Weight::D),
                (b"dog", 2, Weight::D),
                (b"fox", 3, Weight::D),
            ],
        ),
        (5, &[(b"cat", 1, Weight::A)]),
        (6, &[(b"dog", 1, Weight::A)]),
        (
            7,
            &[
                (b"cat", 1, Weight::D),
                (b"dog", 2, Weight::D),
                (b"fox", 3, Weight::D),
            ],
        ),
        (8, &[(b"cat", 1, Weight::A), (b"dog", 2, Weight::A)]),
        (9, &[(b"cat", 1, Weight::D), (b"dog", 2, Weight::D)]),
        (10, &[(b"cat", 1, Weight::D), (b"dog", 2, Weight::D)]),
    ];
    for table in ["rank_scan", "rank_index"] {
        conn.execute(&format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, body TSVECTOR)"
        ))
        .unwrap();
        for &(id, lexemes) in docs {
            conn.query_params(
                &format!("INSERT INTO {table} VALUES ($1, $2)"),
                &[Value::Integer(id), Value::TsVector(tsvector(lexemes))],
            )
            .unwrap();
        }
    }
    conn.execute("CREATE INDEX rank_body ON rank_index USING fts (body)")
        .unwrap();
}

fn rank_comparison_rows(
    conn: &Connection<'_>,
    table: &str,
    rank_query: &str,
    filter_query: &str,
    direction: &str,
    limit: i64,
) -> Vec<Vec<Value>> {
    // SQL leaves equal ranks unordered. The fast lane chooses the lowest PK;
    // make that tie policy explicit in the unindexed scalar reference.
    let tie_break = if table == "rank_scan" { ", id ASC" } else { "" };
    conn.query(&format!(
        "SELECT id, ts_rank(body, to_tsquery('simple', '{rank_query}')) AS r \
         FROM {table} WHERE body @@ to_tsquery('simple', '{filter_query}') \
         ORDER BY r {direction}{tie_break} LIMIT {limit}"
    ))
    .unwrap()
    .rows
}

#[test]
fn indexed_rank_matches_scalar_query_semantics_and_exact_scores() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed_rank_comparison_tables(&conn);
    for (rank_query, filter_query) in [
        ("cat & dog", "cat & dog"),
        ("cat", "cat & dog"),
        ("dog & dog & cat", "cat & dog"),
        ("fox & dog & cat", "cat & dog & fox"),
        ("fox", "cat"),
        ("cat:A", "cat & dog"),
        ("cat | fox", "cat & dog"),
        ("cat <-> dog", "cat & dog"),
        ("cat:*", "cat & dog"),
    ] {
        for direction in ["ASC", "DESC"] {
            let expected =
                rank_comparison_rows(&conn, "rank_scan", rank_query, filter_query, direction, 100);
            let actual = rank_comparison_rows(
                &conn,
                "rank_index",
                rank_query,
                filter_query,
                direction,
                100,
            );
            assert!(!expected.is_empty());
            assert_eq!(
                actual, expected,
                "rank={rank_query}, filter={filter_query}, {direction}"
            );
        }
    }
}

#[test]
fn indexed_rank_topk_honors_direction_ties_and_large_limits() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed_rank_comparison_tables(&conn);
    for direction in ["ASC", "DESC"] {
        for limit in [1, 3, i64::MAX] {
            let expected = rank_comparison_rows(
                &conn,
                "rank_scan",
                "cat & dog",
                "cat & dog",
                direction,
                limit,
            );
            let actual = rank_comparison_rows(
                &conn,
                "rank_index",
                "cat & dog",
                "cat & dog",
                direction,
                limit,
            );
            assert_eq!(actual, expected, "{direction}, LIMIT {limit}");
        }
    }
    let ascending = rank_comparison_rows(&conn, "rank_index", "cat & dog", "cat & dog", "ASC", 3);
    assert_eq!(
        ascending
            .iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Integer(3), Value::Integer(4), Value::Integer(7)]
    );
}

#[test]
fn indexed_rank_ordinals_preserve_rows_and_projection_names() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed_rank_comparison_tables(&conn);
    for (projection, rank_ordinal) in [
        ("id, ts_rank(body, to_tsquery('simple', 'cat & dog'))", 2),
        ("ts_rank(body, to_tsquery('simple', 'cat & dog')), id", 1),
        (
            "d.id, ts_rank(d.body, to_tsquery('simple', 'cat & dog'))",
            2,
        ),
        (
            "d.id AS document, ts_rank(d.body, to_tsquery('simple', 'cat & dog')) AS \"Score\"",
            2,
        ),
    ] {
        for direction in ["ASC", "DESC"] {
            let query = |table: &str| {
                let tie_break = if table == "rank_scan" {
                    ", d.id ASC"
                } else {
                    ""
                };
                conn.query(&format!(
                    "SELECT {projection} FROM {table} AS d \
                     WHERE d.body @@ to_tsquery('simple', 'cat & dog') \
                     ORDER BY {rank_ordinal} {direction}{tie_break} LIMIT 3"
                ))
                .unwrap()
            };
            let expected = query("rank_scan");
            let actual = query("rank_index");
            assert_eq!(expected.rows.len(), 3);
            assert_eq!(actual.columns, expected.columns, "{projection}");
            assert_eq!(
                actual.rows, expected.rows,
                "{projection}, ORDER BY {rank_ordinal} {direction}"
            );
        }
    }
}

#[test]
fn indexed_rank_multiple_projections_use_their_own_queries() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed_rank_comparison_tables(&conn);
    let query = |table: &str| {
        conn.query(&format!(
            "SELECT id, ts_rank(body, to_tsquery('cat')) AS c, \
         ts_rank(body, to_tsquery('dog')) AS d FROM {table} \
         WHERE body @@ to_tsquery('cat & dog') ORDER BY d DESC LIMIT 100"
        ))
        .unwrap()
        .rows
    };
    assert_eq!(query("rank_index"), query("rank_scan"));
}

#[test]
fn indexed_text_source_does_not_hide_ts_rank_type_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE text_docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("INSERT INTO text_docs VALUES (1, 'cat dog')")
        .unwrap();
    let query = "SELECT id, ts_rank(body, to_tsquery('cat')) AS r FROM text_docs \
                 WHERE body @@ to_tsquery('cat') ORDER BY r DESC LIMIT 1";
    for indexed in [false, true] {
        if indexed {
            conn.execute("CREATE INDEX text_body ON text_docs USING fts (body)")
                .unwrap();
        }
        assert!(
            matches!(conn.query(query), Err(citadel_sql::SqlError::TypeMismatch { expected, .. })
            if expected == "TSVECTOR")
        );
    }
}

#[test]
fn indexed_rank_row_dependent_query_matches_scalar_evaluation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed_rank_comparison_tables(&conn);
    let query = |table: &str| {
        conn.query(&format!(
        "SELECT id, ts_rank(body, to_tsquery(CASE WHEN id = 1 THEN 'cat' ELSE 'dog' END)) AS r \
         FROM {table} WHERE body @@ to_tsquery('cat & dog') ORDER BY r DESC LIMIT 100"
    ))
        .unwrap()
        .rows
    };
    assert_eq!(query("rank_index"), query("rank_scan"));
}

#[test]
fn indexed_rank_malformed_query_preserves_scalar_projection_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed_rank_comparison_tables(&conn);
    for (filter, limit) in [("absent", 10), ("cat", 0), ("cat", 10)] {
        let query = |table: &str| {
            conn.query_params(
                &format!(
                    "SELECT id, ts_rank(body, $1) AS r FROM {table} \
             WHERE body @@ to_tsquery('{filter}') ORDER BY r DESC LIMIT {limit}"
                ),
                &[Value::TsQuery(Arc::from([255_u8]))],
            )
        };
        match (query("rank_index"), query("rank_scan")) {
            (Ok(actual), Ok(expected)) => assert_eq!(actual.rows, expected.rows),
            (Err(actual), Err(expected)) => assert_eq!(actual.to_string(), expected.to_string()),
            (actual, expected) => {
                panic!("filter={filter}, LIMIT {limit}: {actual:?} != {expected:?}")
            }
        }
    }
}
