use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn seed(conn: &Connection<'_>) {
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(3))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, '[1.0, 0.0, 0.0]'::VECTOR(3))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, '[0.0, 1.0, 0.0]'::VECTOR(3))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, '[0.0, 0.0, 1.0]'::VECTOR(3))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (4, '[0.9, 0.1, 0.0]'::VECTOR(3))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (5, '[1.0, 1.0, 1.0]'::VECTOR(3))")
        .unwrap();
}

#[test]
fn order_by_l2_limit_returns_top_k() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    let qr = match conn
        .execute("SELECT id FROM t ORDER BY v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 3")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!("expected query result"),
    };
    let ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        })
        .collect();
    assert_eq!(ids[0], 1, "closest to [1,0,0] is itself (id=1)");
    assert_eq!(ids[1], 4, "next closest is [0.9, 0.1, 0]");
    assert_eq!(ids.len(), 3);
}

#[test]
fn order_by_cosine_limit_returns_top_k() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    let qr = match conn
        .execute("SELECT id FROM t ORDER BY v <=> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 2")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    let ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids[0], 1);
    assert_eq!(ids[1], 4);
}

#[test]
fn order_by_inner_product_limit_returns_top_k() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    let qr = match conn
        .execute("SELECT id FROM t ORDER BY v <#> '[1.0, 1.0, 1.0]'::VECTOR(3) LIMIT 1")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    let ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids[0], 5, "highest IP with [1,1,1] is [1,1,1] itself");
}

#[test]
fn select_distance_column_returns_real() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    let qr = match conn
        .execute("SELECT id, v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) AS d FROM t ORDER BY d LIMIT 2")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    assert_eq!(qr.rows.len(), 2);
    let d0 = match &qr.rows[0][1] {
        Value::Real(r) => *r,
        other => panic!("expected Real, got {other:?}"),
    };
    assert!(d0 < 0.5, "closest distance should be small, got {d0}");
}

#[test]
fn order_by_distance_works_without_ann_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    let qr = match conn
        .execute("SELECT id FROM t ORDER BY v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 5")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    assert_eq!(qr.rows.len(), 5);
}

#[test]
fn insert_after_index_build_invalidates_cache() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2')")
        .unwrap();

    let _ = conn
        .execute("SELECT id FROM t ORDER BY v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 1")
        .unwrap();
    assert!(
        conn.ann_cache_status("t", "v").unwrap().is_some(),
        "cache should be populated"
    );

    conn.execute("INSERT INTO t VALUES (99, '[1.0, 0.0, 0.0]'::VECTOR(3))")
        .unwrap();
    assert!(
        conn.ann_cache_status("t", "v").unwrap().is_none(),
        "auto-commit INSERT should evict the stale ANN cache"
    );

    let qr = match conn
        .execute("SELECT id FROM t ORDER BY v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 2")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    let ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    assert!(
        ids.contains(&99),
        "rebuilt cache must include the post-INSERT row (got {ids:?})"
    );
}

#[test]
fn ann_in_write_txn_scans_live_view_not_stale_cache() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(3))")
        .unwrap();
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    for (id, vec) in [
        (1, "[1.0, 0.0, 0.0]"),
        (2, "[0.0, 1.0, 0.0]"),
        (3, "[0.0, 0.0, 1.0]"),
        (4, "[0.9, 0.1, 0.0]"),
        (5, "[1.0, 1.0, 1.0]"),
    ] {
        conn.execute(&format!("INSERT INTO t VALUES ({id}, '{vec}'::VECTOR(3))"))
            .unwrap();
    }

    // Prime the cache via the read path.
    let _ = conn
        .execute("SELECT id FROM t ORDER BY v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 1")
        .unwrap();
    assert!(
        conn.ann_cache_status("t", "v").unwrap().is_some(),
        "read-path query caches the index"
    );

    conn.execute("BEGIN").unwrap();
    // Uncommitted closest row: ANN must stream the live view, not the stale cache.
    conn.execute("INSERT INTO t VALUES (6, '[0.99, 0.0, 0.0]'::VECTOR(3))")
        .unwrap();
    let qr = match conn
        .execute("SELECT id FROM t ORDER BY v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 3")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    conn.execute("COMMIT").unwrap();

    let ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    assert_eq!(
        ids,
        vec![1, 6, 4],
        "in-txn ANN sees uncommitted id=6 in distance order"
    );
}

#[test]
fn rollback_keeps_cache_intact() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    let _ = conn
        .execute("SELECT id FROM t ORDER BY v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 1")
        .unwrap();
    assert!(conn.ann_cache_status("t", "v").unwrap().is_some());

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (99, '[0.5, 0.5, 0.0]'::VECTOR(3))")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();
    assert!(
        conn.ann_cache_status("t", "v").unwrap().is_some(),
        "rolled-back DML must not invalidate the cache"
    );
}

#[test]
fn explicit_commit_invalidates_cache() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    let _ = conn
        .execute("SELECT id FROM t ORDER BY v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 1")
        .unwrap();
    assert!(conn.ann_cache_status("t", "v").unwrap().is_some());

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (99, '[1.0, 0.0, 0.0]'::VECTOR(3))")
        .unwrap();
    conn.execute("COMMIT").unwrap();
    assert!(
        conn.ann_cache_status("t", "v").unwrap().is_none(),
        "explicit COMMIT after DML must evict the ANN cache"
    );
}

#[test]
fn ann_query_survives_close_and_reopen_with_passphrase() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let initial_ids: Vec<i64> = {
        let db = DatabaseBuilder::new(&db_path)
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(3))")
            .unwrap();
        for (id, vec) in [
            (1, "[1.0, 0.0, 0.0]"),
            (2, "[0.0, 1.0, 0.0]"),
            (3, "[0.0, 0.0, 1.0]"),
            (4, "[0.9, 0.1, 0.0]"),
            (5, "[1.0, 1.0, 1.0]"),
            (6, "[0.95, 0.05, 0.0]"),
        ] {
            conn.execute(&format!("INSERT INTO t VALUES ({id}, '{vec}'::VECTOR(3))"))
                .unwrap();
        }
        conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2')")
            .unwrap();

        let qr = match conn
            .execute("SELECT id FROM t ORDER BY v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 3")
            .unwrap()
        {
            ExecutionResult::Query(qr) => qr,
            _ => panic!(),
        };
        qr.rows
            .iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i,
                _ => panic!(),
            })
            .collect()
    };

    let db = DatabaseBuilder::new(&db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    assert_eq!(
        db.sql_cache_len(),
        0,
        "freshly-reopened DB starts with an empty shared cache"
    );

    let conn = Connection::open(&db).unwrap();
    let qr = match conn
        .execute("SELECT id FROM t ORDER BY v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 3")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    let reopened_ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    assert_eq!(
        reopened_ids, initial_ids,
        "ANN top-k must be stable across encrypted close/reopen"
    );
    assert_eq!(
        db.sql_cache_len(),
        1,
        "first query after reopen rebuilds the cache"
    );
}

#[test]
fn ann_rejects_wrong_passphrase_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    {
        let db = DatabaseBuilder::new(&db_path)
            .passphrase(b"correct-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(2))")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, '[1.0, 0.0]'::VECTOR(2))")
            .unwrap();
    }
    let err = DatabaseBuilder::new(&db_path)
        .passphrase(b"wrong-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .expect_err("wrong passphrase must fail to open");
    let lower = format!("{err:?}").to_ascii_lowercase();
    assert!(
        lower.contains("keyfileintegrity")
            || lower.contains("badpassphrase")
            || lower.contains("mac"),
        "expected key-file MAC failure, got {err:?}"
    );
}

#[test]
fn ann_query_on_empty_table_returns_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(3))")
        .unwrap();
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    // Empty table must return [], not error.
    let qr = match conn
        .execute("SELECT id FROM t ORDER BY v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 5")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    assert!(qr.rows.is_empty());
}

#[test]
fn order_by_distance_works_with_ann_index_present() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    let qr = match conn
        .execute("SELECT id FROM t ORDER BY v <-> '[1.0, 0.0, 0.0]'::VECTOR(3) LIMIT 3")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    let ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids[0], 1);
    assert_eq!(ids[1], 4);
}
