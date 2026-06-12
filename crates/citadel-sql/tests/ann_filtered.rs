//! Filtered ANN: `WHERE col = v / IN (...)` pushed into the PRISM cell filter,
//! plus recheck of non-pushable predicates, validated against brute-force truth.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

const DIM: usize = 8;

/// Deterministic pseudo-random vector for row `i`.
fn vec_for(i: u64) -> Vec<f32> {
    (0..DIM)
        .map(|d| {
            let x = (i.wrapping_mul(2654435761).wrapping_add(d as u64 * 40503) % 1000) as f32;
            x / 1000.0
        })
        .collect()
}

fn vec_literal(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{x}")).collect();
    format!("'[{}]'::VECTOR({})", parts.join(", "), DIM)
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum()
}

struct Row {
    id: u64,
    category: i64,
    score: f64,
    v: Vec<f32>,
}

/// Build a table `t(id, category, score, v)` with `n` rows across `cats`
/// categories and an ANN index whose filter column is `category`.
fn seed(conn: &Connection<'_>, n: u64, cats: i64) -> Vec<Row> {
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, score REAL, v VECTOR(8))",
    )
    .unwrap();
    let mut rows = Vec::new();
    for i in 1..=n {
        let category = (i as i64) % cats;
        let score = (i as f64 % 10.0) / 10.0;
        let v = vec_for(i);
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, {category}, {score}, {})",
            vec_literal(&v)
        ))
        .unwrap();
        rows.push(Row {
            id: i,
            category,
            score,
            v,
        });
    }
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2', filters = 'category')")
        .unwrap();
    rows
}

fn query_ids(conn: &Connection<'_>, sql: &str) -> Vec<i64> {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(qr) => qr
            .rows
            .iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i,
                other => panic!("expected Integer id, got {other:?}"),
            })
            .collect(),
        _ => panic!("expected query result"),
    }
}

#[test]
fn filtered_eq_returns_only_matching_category() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let rows = seed(&conn, 300, 5);
    let q = vec_for(7);

    let ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM t WHERE category = 2 ORDER BY v <-> {} LIMIT 10",
            vec_literal(&q)
        ),
    );
    assert!(!ids.is_empty());
    for id in &ids {
        let row = rows.iter().find(|r| r.id == *id as u64).unwrap();
        assert_eq!(row.category, 2, "row {id} is not category 2");
    }
}

#[test]
fn filtered_eq_nearest_matches_brute_force() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let rows = seed(&conn, 300, 5);
    let q = vec_for(123);

    // Brute-force nearest within category 3.
    let mut cat3: Vec<&Row> = rows.iter().filter(|r| r.category == 3).collect();
    cat3.sort_by(|a, b| l2(&a.v, &q).partial_cmp(&l2(&b.v, &q)).unwrap());
    let truth_nearest = cat3[0].id as i64;

    let ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM t WHERE category = 3 ORDER BY v <-> {} LIMIT 5",
            vec_literal(&q)
        ),
    );
    assert_eq!(ids[0], truth_nearest, "nearest in category 3 mismatch");
}

#[test]
fn filtered_in_list_restricts_to_set() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let rows = seed(&conn, 300, 6);
    let q = vec_for(50);

    let ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM t WHERE category IN (1, 4) ORDER BY v <-> {} LIMIT 15",
            vec_literal(&q)
        ),
    );
    assert!(!ids.is_empty());
    for id in &ids {
        let row = rows.iter().find(|r| r.id == *id as u64).unwrap();
        assert!(
            row.category == 1 || row.category == 4,
            "row {id} category {} not in (1,4)",
            row.category
        );
    }
}

#[test]
fn pushdown_plus_recheck_residual() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let rows = seed(&conn, 400, 5);
    let q = vec_for(9);

    // category = 1 pushes into PRISM; score > 0.5 is a recheck residual.
    let ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM t WHERE category = 1 AND score > 0.5 ORDER BY v <-> {} LIMIT 10",
            vec_literal(&q)
        ),
    );
    assert!(!ids.is_empty());
    for id in &ids {
        let row = rows.iter().find(|r| r.id == *id as u64).unwrap();
        assert_eq!(row.category, 1, "row {id} not category 1");
        assert!(row.score > 0.5, "row {id} score {} not > 0.5", row.score);
    }
}

#[test]
fn absent_filter_value_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let _ = seed(&conn, 100, 5);
    let q = vec_for(1);

    let ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM t WHERE category = 999 ORDER BY v <-> {} LIMIT 10",
            vec_literal(&q)
        ),
    );
    assert!(ids.is_empty(), "category 999 should match no rows");
}

#[test]
fn non_pushable_predicate_still_correct_via_exact_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let rows = seed(&conn, 200, 5);
    let q = vec_for(11);

    // score is not a filter column, so the ANN plan declines to the exact scan.
    let ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM t WHERE score > 0.8 ORDER BY v <-> {} LIMIT 10",
            vec_literal(&q)
        ),
    );
    assert!(!ids.is_empty());
    for id in &ids {
        let row = rows.iter().find(|r| r.id == *id as u64).unwrap();
        assert!(row.score > 0.8, "row {id} score {} not > 0.8", row.score);
    }
}

#[test]
fn filtered_recall_matches_brute_force() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let rows = seed(&conn, 500, 4);

    let k = 10;
    let mut total_recall = 0.0_f64;
    let trials = 10;
    for t in 0..trials {
        let q = vec_for(1000 + t);
        // Brute-force top-k within category 0.
        let mut cat0: Vec<&Row> = rows.iter().filter(|r| r.category == 0).collect();
        cat0.sort_by(|a, b| l2(&a.v, &q).partial_cmp(&l2(&b.v, &q)).unwrap());
        let truth: std::collections::HashSet<i64> =
            cat0.iter().take(k).map(|r| r.id as i64).collect();

        let ids = query_ids(
            &conn,
            &format!(
                "SELECT id FROM t WHERE category = 0 ORDER BY v <-> {} LIMIT {k}",
                vec_literal(&q)
            ),
        );
        let hits = ids.iter().filter(|id| truth.contains(id)).count();
        total_recall += hits as f64 / truth.len() as f64;
    }
    let mean = total_recall / trials as f64;
    assert!(mean >= 0.9, "filtered recall@{k} = {mean:.3} below 0.9");
}

#[test]
fn filtered_index_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let rows;
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        rows = seed(&conn, 200, 5);
    }
    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let q = vec_for(3);
    let ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM t WHERE category = 2 ORDER BY v <-> {} LIMIT 8",
            vec_literal(&q)
        ),
    );
    assert!(!ids.is_empty());
    for id in &ids {
        let row = rows.iter().find(|r| r.id == *id as u64).unwrap();
        assert_eq!(row.category, 2, "row {id} not category 2 after reopen");
    }
}

#[test]
fn filtered_eq_real_literal_on_integer_column_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn, 300, 5);
    let q = vec_for(7);

    let int_ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM t WHERE category = 2 ORDER BY v <-> {} LIMIT 10",
            vec_literal(&q)
        ),
    );
    let real_ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM t WHERE category = 2.0 ORDER BY v <-> {} LIMIT 10",
            vec_literal(&q)
        ),
    );
    assert!(!int_ids.is_empty());
    assert_eq!(int_ids, real_ids, "category = 2.0 must match category = 2");
}

#[test]
fn filtered_eq_fractional_literal_matches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn, 100, 5);
    let q = vec_for(7);

    let ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM t WHERE category = 1.5 ORDER BY v <-> {} LIMIT 10",
            vec_literal(&q)
        ),
    );
    assert!(ids.is_empty(), "no integer category equals 1.5");
}

#[test]
fn filtered_eq_null_returns_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn, 100, 5);
    // Rows whose category IS NULL must not be returned by `= NULL`.
    let v = vec_for(1001);
    conn.execute(&format!(
        "INSERT INTO t VALUES (1001, NULL, 0.5, {})",
        vec_literal(&v)
    ))
    .unwrap();
    let q = vec_for(7);

    let ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM t WHERE category = NULL ORDER BY v <-> {} LIMIT 10",
            vec_literal(&q)
        ),
    );
    assert!(ids.is_empty(), "category = NULL is never true in SQL");
}

#[test]
fn filtered_in_mixed_numeric_literals_matches_both_arms() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let rows = seed(&conn, 300, 5);
    let q = vec_for(50);

    // Brute-force top-20 across categories {1, 2}.
    let mut both: Vec<&Row> = rows
        .iter()
        .filter(|r| r.category == 1 || r.category == 2)
        .collect();
    both.sort_by(|a, b| l2(&a.v, &q).partial_cmp(&l2(&b.v, &q)).unwrap());
    let truth: Vec<i64> = both.iter().take(20).map(|r| r.id as i64).collect();

    let ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM t WHERE category IN (1, 2.0) ORDER BY v <-> {} LIMIT 20",
            vec_literal(&q)
        ),
    );
    assert_eq!(ids, truth, "IN (1, 2.0) must cover both categories");
}

#[test]
fn filtered_eq_nocase_text_matches_across_cases() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, tag TEXT COLLATE NOCASE, v VECTOR(8))")
        .unwrap();
    let tags = ["Foo", "foo", "FOO", "bar", "fOo", "BAR"];
    for i in 1..=60u64 {
        let v = vec_for(i);
        conn.execute(&format!(
            "INSERT INTO s VALUES ({i}, '{}', {})",
            tags[(i % 6) as usize],
            vec_literal(&v)
        ))
        .unwrap();
    }
    conn.execute("CREATE INDEX ix_sv ON s USING ann (v) WITH (metric = 'l2', filters = 'tag')")
        .unwrap();
    let q = vec_for(3);

    let ids = query_ids(
        &conn,
        &format!(
            "SELECT id FROM s WHERE tag = 'fOO' ORDER BY v <-> {} LIMIT 60",
            vec_literal(&q)
        ),
    );
    // Every Foo-cased row (40 of 60) matches under NOCASE; no bar row does.
    assert_eq!(ids.len(), 40, "NOCASE eq must match every casing of 'foo'");
    for id in &ids {
        assert_ne!(*id as u64 % 6, 3, "row {id} is 'bar'");
        assert_ne!(*id as u64 % 6, 5, "row {id} is 'BAR'");
    }
}
