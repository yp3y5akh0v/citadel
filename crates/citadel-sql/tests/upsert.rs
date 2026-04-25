use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn rows_affected(result: ExecutionResult) -> u64 {
    match result {
        ExecutionResult::RowsAffected(n) => n,
        other => panic!("expected RowsAffected, got {other:?}"),
    }
}

fn query(conn: &Connection, sql: &str) -> QueryResult {
    conn.query(sql).unwrap()
}

#[test]
fn do_nothing_pk_conflict_skips_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'original')")
        .unwrap();

    let affected = rows_affected(
        conn.execute("INSERT INTO t VALUES (1, 'new') ON CONFLICT (id) DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected, 0);

    let qr = query(&conn, "SELECT v FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("original".into()));
}

#[test]
fn do_nothing_new_row_inserts_normally() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let affected = rows_affected(
        conn.execute("INSERT INTO t VALUES (1, 'hello') ON CONFLICT (id) DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected, 1);

    let qr = query(&conn, "SELECT v FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
}

#[test]
fn do_nothing_no_target_works_on_any_unique_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();

    let affected_pk = rows_affected(
        conn.execute("INSERT INTO t VALUES (1, 'b@x') ON CONFLICT DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected_pk, 0);

    let affected_email = rows_affected(
        conn.execute("INSERT INTO t VALUES (2, 'a@x') ON CONFLICT DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected_email, 0);

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn do_nothing_on_unique_index_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();

    let affected = rows_affected(
        conn.execute("INSERT INTO t VALUES (2, 'a@x') ON CONFLICT (email) DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected, 0);

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn do_nothing_multi_row_values_mixed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 'skip'), (2, 'b'), (3, 'c') ON CONFLICT (id) DO NOTHING",
        )
        .unwrap(),
    );
    assert_eq!(affected, 2);

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn do_nothing_insert_select_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("INSERT INTO src VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        .unwrap();
    conn.execute("INSERT INTO dst VALUES (1, 'existing'), (2, 'existing')")
        .unwrap();

    let affected = rows_affected(
        conn.execute("INSERT INTO dst SELECT id, v FROM src ON CONFLICT (id) DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected, 1);

    let qr = query(&conn, "SELECT v FROM dst WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("existing".into()));
}

#[test]
fn do_nothing_null_in_unique_column_inserts() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();

    let affected = rows_affected(
        conn.execute("INSERT INTO t VALUES (2, NULL) ON CONFLICT (email) DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected, 1);

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn do_nothing_inside_explicit_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    conn.execute("BEGIN").unwrap();
    rows_affected(
        conn.execute("INSERT INTO t VALUES (1, 'skip') ON CONFLICT (id) DO NOTHING")
            .unwrap(),
    );
    rows_affected(
        conn.execute("INSERT INTO t VALUES (2, 'b') ON CONFLICT (id) DO NOTHING")
            .unwrap(),
    );
    conn.execute("COMMIT").unwrap();

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn do_nothing_inside_savepoint_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a') ON CONFLICT (id) DO NOTHING")
        .unwrap();
    conn.execute("SAVEPOINT s1").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b') ON CONFLICT (id) DO NOTHING")
        .unwrap();
    conn.execute("ROLLBACK TO s1").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn do_update_pk_conflict_sets_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'old')").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 'ignored') ON CONFLICT (id) DO UPDATE SET v = 'new'",
        )
        .unwrap(),
    );
    assert_eq!(affected, 1);

    let qr = query(&conn, "SELECT v FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("new".into()));
}

#[test]
fn do_update_pk_conflict_increment_counter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE counters (k TEXT PRIMARY KEY, c INTEGER)")
        .unwrap();

    for _ in 0..5 {
        conn.execute(
            "INSERT INTO counters VALUES ('hits', 1) \
             ON CONFLICT (k) DO UPDATE SET c = c + 1",
        )
        .unwrap();
    }

    let qr = query(&conn, "SELECT c FROM counters WHERE k = 'hits'");
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn do_update_uses_excluded_column_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'old')").unwrap();

    conn.execute(
        "INSERT INTO t VALUES (1, 'proposed') ON CONFLICT (id) DO UPDATE SET v = excluded.v",
    )
    .unwrap();

    let qr = query(&conn, "SELECT v FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("proposed".into()));
}

#[test]
fn do_update_mixed_existing_and_excluded_in_expr() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    conn.execute("INSERT INTO t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET c = c + excluded.c")
        .unwrap();

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(15));
}

#[test]
fn do_update_unqualified_col_refers_to_existing_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100, 200)").unwrap();

    conn.execute("INSERT INTO t VALUES (1, 1, 2) ON CONFLICT (id) DO UPDATE SET a = b")
        .unwrap();

    let qr = query(&conn, "SELECT a, b FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(200));
    assert_eq!(qr.rows[0][1], Value::Integer(200));
}

#[test]
fn do_update_where_true_fires() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET c = 99 WHERE c < 10",
        )
        .unwrap(),
    );
    assert_eq!(affected, 1);

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(99));
}

#[test]
fn do_update_where_false_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 50)").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET c = 999 WHERE c < 10",
        )
        .unwrap(),
    );
    assert_eq!(affected, 0);

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(50));
}

#[test]
fn do_update_where_null_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER, flag INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5, NULL)").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 0, NULL) \
             ON CONFLICT (id) DO UPDATE SET c = 100 WHERE flag = 1",
        )
        .unwrap(),
    );
    assert_eq!(affected, 0);
}

#[test]
fn do_update_on_unique_index_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, hits INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a@x', 0)").unwrap();

    conn.execute(
        "INSERT INTO t VALUES (99, 'a@x', 1) \
         ON CONFLICT (email) DO UPDATE SET hits = hits + 1",
    )
    .unwrap();

    let qr = query(&conn, "SELECT id, hits FROM t WHERE email = 'a@x'");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(1));
    let count = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(count.rows[0][0], Value::Integer(1));
}

#[test]
fn do_update_rejects_not_null_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = NULL")
        .expect_err("NOT NULL should fire after DO UPDATE");
    assert!(matches!(err, SqlError::NotNullViolation(_)));
}

#[test]
fn do_update_rejects_check_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER CHECK (c >= 0))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET c = -1")
        .expect_err("CHECK should fire after DO UPDATE");
    assert!(matches!(err, SqlError::CheckViolation(_)));
}

#[test]
fn do_update_rejects_fk_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    let err = conn
        .execute("INSERT INTO child VALUES (10, 1) ON CONFLICT (id) DO UPDATE SET pid = 999")
        .expect_err("FK should fire after DO UPDATE");
    assert!(matches!(err, SqlError::ForeignKeyViolation(_)));
}

#[test]
fn do_update_rejects_conflicting_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b@x')").unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (2, 'ignored') ON CONFLICT (id) DO UPDATE SET email = 'a@x'")
        .expect_err("updated email collides with existing unique value");
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn do_update_case_insensitive_column_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (ID INTEGER PRIMARY KEY, V TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    conn.execute(
        "INSERT INTO t VALUES (1, 'ignored') ON CONFLICT (id) DO UPDATE SET v = Excluded.V",
    )
    .unwrap();

    let qr = query(&conn, "SELECT v FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("ignored".into()));
}

#[test]
fn do_update_excluded_not_found_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET v = excluded.nope")
        .expect_err("excluded.nope does not exist");
    assert!(matches!(err, SqlError::ColumnNotFound(_)));
}

#[test]
fn do_update_error_rolls_back_statement() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER CHECK (c >= 0))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();

    let _ = conn
        .execute(
            "INSERT INTO t VALUES (2, 10), (1, 0) \
             ON CONFLICT (id) DO UPDATE SET c = -1",
        )
        .expect_err("second row's CHECK fires and aborts");

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn on_constraint_named_unique_index_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX t_email_idx ON t (email)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (2, 'a@x') ON CONFLICT ON CONSTRAINT t_email_idx DO NOTHING",
        )
        .unwrap(),
    );
    assert_eq!(affected, 0);

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn on_constraint_rejects_unknown_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1, 'a') ON CONFLICT ON CONSTRAINT missing_idx DO NOTHING")
        .expect_err("unknown constraint should error");
    assert!(matches!(err, SqlError::Plan(_)));
}

#[test]
fn multi_row_values_second_row_conflicts_with_first() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();

    conn.execute(
        "INSERT INTO t VALUES (1, 10), (1, 20) \
         ON CONFLICT (id) DO UPDATE SET c = c + excluded.c",
    )
    .unwrap();

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(30));
}

#[test]
fn insert_select_on_conflict_do_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE src (k TEXT PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE dst (k TEXT PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES ('a', 10), ('b', 20)")
        .unwrap();
    conn.execute("INSERT INTO dst VALUES ('a', 1)").unwrap();

    conn.execute(
        "INSERT INTO dst SELECT k, v FROM src \
         ON CONFLICT (k) DO UPDATE SET v = excluded.v",
    )
    .unwrap();

    let qr = query(&conn, "SELECT v FROM dst WHERE k = 'a'");
    assert_eq!(qr.rows[0][0], Value::Integer(10));
    let qr = query(&conn, "SELECT v FROM dst WHERE k = 'b'");
    assert_eq!(qr.rows[0][0], Value::Integer(20));
}

#[test]
fn rejects_on_conflict_without_target_do_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1, 'a') ON CONFLICT DO UPDATE SET v = 'b'")
        .expect_err("DO UPDATE without target should error");
    assert!(matches!(err, SqlError::Plan(_)));
}

#[test]
fn rejects_conflict_target_not_matching_any_unique() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1, 'a') ON CONFLICT (name) DO NOTHING")
        .expect_err("should reject target without matching unique constraint");
    match err {
        SqlError::Plan(msg) => assert!(msg.contains("does not match any unique constraint")),
        other => panic!("expected Plan error, got {other:?}"),
    }
}

#[test]
fn prepared_upsert_reused_across_calls() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();

    let stmt = conn
        .prepare(
            "INSERT INTO t VALUES ($1, 1) \
             ON CONFLICT (id) DO UPDATE SET c = c + 1",
        )
        .unwrap();

    for _ in 0..3 {
        stmt.execute(&[Value::Integer(1)]).unwrap();
    }
    stmt.execute(&[Value::Integer(2)]).unwrap();

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    let qr = query(&conn, "SELECT c FROM t WHERE id = 2");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn prepared_upsert_excluded_with_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let stmt = conn
        .prepare(
            "INSERT INTO t VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET c = c + excluded.c",
        )
        .unwrap();

    stmt.execute(&[Value::Integer(1), Value::Integer(5)])
        .unwrap();
    stmt.execute(&[Value::Integer(1), Value::Integer(3)])
        .unwrap();

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(18));
}

#[test]
fn prepared_upsert_do_nothing_with_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let stmt = conn
        .prepare("INSERT INTO t VALUES ($1, 'x') ON CONFLICT (id) DO NOTHING")
        .unwrap();

    for i in 0..5 {
        stmt.execute(&[Value::Integer(i % 3)]).unwrap();
    }

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn multi_row_values_mixed_new_and_conflict_do_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 5), (2, 20), (3, 30) \
             ON CONFLICT (id) DO UPDATE SET c = c + excluded.c",
        )
        .unwrap(),
    );
    assert_eq!(affected, 3);

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(105));
    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn do_update_counter_fast_path_correct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE ct (k TEXT PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO ct VALUES ('hot', 0)").unwrap();

    for _ in 0..200 {
        conn.execute("INSERT INTO ct VALUES ('hot', 1) ON CONFLICT (k) DO UPDATE SET c = c + 1")
            .unwrap();
    }

    let qr = query(&conn, "SELECT c FROM ct WHERE k = 'hot'");
    assert_eq!(qr.rows[0][0], Value::Integer(200));
}

#[test]
fn do_update_counter_crosses_varint_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 126)").unwrap();

    for _ in 0..10 {
        conn.execute("INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET c = c + 1")
            .unwrap();
    }

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(136));
}

#[test]
fn do_update_counter_subtract_fast_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    for _ in 0..30 {
        conn.execute("INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET c = c - 2")
            .unwrap();
    }

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(40));
}

#[test]
fn multi_row_error_mid_batch_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER CHECK (c >= 0))")
        .unwrap();

    let _ = conn
        .execute(
            "INSERT INTO t VALUES (1, 10), (2, -1), (3, 30) \
             ON CONFLICT (id) DO NOTHING",
        )
        .expect_err("row 2 violates CHECK");

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}
