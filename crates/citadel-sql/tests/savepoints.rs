//! Integration tests for SQL SAVEPOINT / RELEASE / ROLLBACK TO.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"savepoint-test")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"savepoint-test")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

fn assert_ok(r: ExecutionResult) {
    match r {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn assert_rows(r: ExecutionResult, expected: u64) {
    match r {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, expected),
        other => panic!("expected RowsAffected({expected}), got {other:?}"),
    }
}

fn count(conn: &mut Connection<'_>, sql: &str) -> i64 {
    let qr = conn.query(sql).unwrap();
    match &qr.rows[0][0] {
        Value::Integer(n) => *n,
        v => panic!("expected integer count, got {v:?}"),
    }
}

fn setup(conn: &mut Connection<'_>) {
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
}

// ═══════════════════════════════════════════════════════════════════
// Mechanics & guards
// ═══════════════════════════════════════════════════════════════════

#[test]
fn savepoint_outside_txn_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn.execute("SAVEPOINT sp1").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn release_outside_txn_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn.execute("RELEASE SAVEPOINT sp1").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn rollback_to_outside_txn_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn.execute("ROLLBACK TO sp1").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn commit_clears_savepoint_stack() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    assert_ok(conn.execute("BEGIN").unwrap());
    assert_ok(conn.execute("SAVEPOINT sp1").unwrap());
    assert_ok(conn.execute("COMMIT").unwrap());

    assert_ok(conn.execute("BEGIN").unwrap());
    let err = conn.execute("ROLLBACK TO sp1").unwrap_err();
    assert!(matches!(err, SqlError::SavepointNotFound(_)));
}

#[test]
fn rollback_clears_savepoint_stack() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    assert_ok(conn.execute("BEGIN").unwrap());
    assert_ok(conn.execute("SAVEPOINT sp1").unwrap());
    assert_ok(conn.execute("ROLLBACK").unwrap());

    assert_ok(conn.execute("BEGIN").unwrap());
    let err = conn.execute("RELEASE sp1").unwrap_err();
    assert!(matches!(err, SqlError::SavepointNotFound(_)));
}

#[test]
fn release_nonexistent_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    assert_ok(conn.execute("BEGIN").unwrap());
    let err = conn.execute("RELEASE SAVEPOINT ghost").unwrap_err();
    assert!(matches!(err, SqlError::SavepointNotFound(_)));
}

#[test]
fn rollback_to_nonexistent_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    assert_ok(conn.execute("BEGIN").unwrap());
    let err = conn.execute("ROLLBACK TO ghost").unwrap_err();
    assert!(matches!(err, SqlError::SavepointNotFound(_)));
}

// ═══════════════════════════════════════════════════════════════════
// Single-level semantics
// ═══════════════════════════════════════════════════════════════════

#[test]
fn release_keeps_work() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp1").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
        .unwrap();
    conn.execute("RELEASE SAVEPOINT sp1").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn rollback_to_discards_post_savepoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'keep')")
        .unwrap();
    conn.execute("SAVEPOINT sp1").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'drop')")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (3, 'drop')")
        .unwrap();
    conn.execute("ROLLBACK TO sp1").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("keep".into()));
}

#[test]
fn rollback_to_preserves_savepoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp1").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
        .unwrap();
    conn.execute("ROLLBACK TO sp1").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'b')")
        .unwrap();
    conn.execute("ROLLBACK TO sp1").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn pre_savepoint_work_persists() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'pre')")
        .unwrap();
    conn.execute("SAVEPOINT sp1").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'post')")
        .unwrap();
    conn.execute("ROLLBACK TO sp1").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT val FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("pre".into()));
}

// ═══════════════════════════════════════════════════════════════════
// Nesting
// ═══════════════════════════════════════════════════════════════════

#[test]
fn release_inner_keeps_outer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT a").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
        .unwrap();
    conn.execute("SAVEPOINT b").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'b')")
        .unwrap();
    conn.execute("RELEASE b").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (3, 'c')")
        .unwrap();
    conn.execute("ROLLBACK TO a").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn rollback_to_inner_keeps_work_up_to_inner() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'outer')")
        .unwrap();
    conn.execute("SAVEPOINT a").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'mid')")
        .unwrap();
    conn.execute("SAVEPOINT b").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (3, 'inner')")
        .unwrap();
    conn.execute("ROLLBACK TO b").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
}

#[test]
fn rollback_to_outer_discards_middle_and_inner() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT a").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
        .unwrap();
    conn.execute("SAVEPOINT b").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'b')")
        .unwrap();
    conn.execute("SAVEPOINT c").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (3, 'c')")
        .unwrap();
    conn.execute("ROLLBACK TO a").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);

    assert_ok(conn.execute("BEGIN").unwrap());
    let err = conn.execute("RELEASE b").unwrap_err();
    assert!(matches!(err, SqlError::SavepointNotFound(_)));
}

#[test]
fn shadowed_duplicate_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'outer')")
        .unwrap();
    conn.execute("SAVEPOINT sp").unwrap(); // shadows outer
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'inner')")
        .unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();

    let qr = conn.query("SELECT val FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("outer".into()));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn shadowed_release_peels_inner() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'outer')")
        .unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'inner')")
        .unwrap();
    conn.execute("RELEASE sp").unwrap(); // peels inner
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
}

// ═══════════════════════════════════════════════════════════════════
// DDL across savepoints
// ═══════════════════════════════════════════════════════════════════

#[test]
fn create_table_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t2 (id) VALUES (1)").unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    let err = conn.query("SELECT * FROM t2").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn drop_table_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn create_index_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("CREATE INDEX idx_val ON t (val)").unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    let err = conn.execute("DROP INDEX idx_val").unwrap_err();
    assert!(matches!(err, SqlError::IndexNotFound(_)));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn alter_table_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN extra INTEGER")
        .unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    let err = conn.query("SELECT extra FROM t").unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)));
}

#[test]
fn ddl_and_dml_rolled_back_together() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("CREATE TABLE nested (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO nested (id) VALUES (1), (2), (3)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (10, 'x')")
        .unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    let err = conn.query("SELECT * FROM nested").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
}

// ═══════════════════════════════════════════════════════════════════
// Index maintenance
// ═══════════════════════════════════════════════════════════════════

#[test]
fn index_insert_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("CREATE INDEX idx_val ON t (val)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'x')")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'x')")
        .unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        count(&mut conn, "SELECT COUNT(*) FROM t WHERE val = 'x'"),
        0
    );
}

#[test]
fn unique_index_respected_across_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("CREATE UNIQUE INDEX uq_val ON t (val)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'existing')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'existing')")
        .unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    let err = conn
        .execute("INSERT INTO t (id, val) VALUES (3, 'existing')")
        .unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════
// Error recovery
// ═══════════════════════════════════════════════════════════════════

#[test]
fn recover_from_unique_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'seed')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    let err = conn
        .execute("INSERT INTO t (id, val) VALUES (1, 'dup')")
        .unwrap_err();
    assert!(matches!(err, SqlError::DuplicateKey));
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'ok')")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 2);
}

#[test]
fn recover_from_check_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE tc (id INTEGER PRIMARY KEY, n INTEGER CHECK(n > 0))")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    let err = conn
        .execute("INSERT INTO tc (id, n) VALUES (1, -5)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(_)));
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("INSERT INTO tc (id, n) VALUES (2, 10)")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM tc"), 1);
}

#[test]
fn recover_from_fk_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    let err = conn
        .execute("INSERT INTO child (id, pid) VALUES (1, 99)")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(_)));
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("INSERT INTO parent (id) VALUES (5)").unwrap();
    conn.execute("INSERT INTO child (id, pid) VALUES (1, 5)")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM child"), 1);
}

#[test]
fn partial_batch_insert_recovered() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("INSERT INTO t (id, val) VALUES (5, 'blocker')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    let err = conn
        .execute(
            "INSERT INTO t (id, val) VALUES (1, 'a'), (2, 'b'), (5, 'dup'), (3, 'c'), (4, 'd')",
        )
        .unwrap_err();
    assert!(matches!(err, SqlError::DuplicateKey));
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 5);
}

// ═══════════════════════════════════════════════════════════════════
// Case handling, syntax variants
// ═══════════════════════════════════════════════════════════════════

#[test]
fn case_insensitive_savepoint_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT My_SP").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'x')")
        .unwrap();
    conn.execute("ROLLBACK TO my_sp").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn release_without_savepoint_keyword() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp1").unwrap();
    conn.execute("RELEASE sp1").unwrap(); // without SAVEPOINT keyword
    conn.execute("COMMIT").unwrap();
}

#[test]
fn rollback_to_without_savepoint_keyword() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp1").unwrap();
    conn.execute("ROLLBACK TO sp1").unwrap(); // without SAVEPOINT keyword
    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════
// Cache / buffer correctness
// ═══════════════════════════════════════════════════════════════════

#[test]
fn stmt_cache_invalidated_after_rollback_to() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'pre')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    // Prime compiled_update cache against pre-rollback schema.
    conn.execute("UPDATE t SET val = 'mid' WHERE id = 1")
        .unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN extra INTEGER")
        .unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    // Must re-plan: `extra` no longer exists after rollback.
    conn.execute("UPDATE t SET val = 'post' WHERE id = 1")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("post".into()));
}

#[test]
fn batch_insert_after_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    assert_rows(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'a'), (2, 'b')")
            .unwrap(),
        2,
    );
    conn.execute("ROLLBACK TO sp").unwrap();
    assert_rows(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'a'), (2, 'b')")
            .unwrap(),
        2,
    );
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 2);
}

// ═══════════════════════════════════════════════════════════════════
// Persistence
// ═══════════════════════════════════════════════════════════════════

#[test]
fn nested_savepoints_with_release_persist() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup(&mut conn);

        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'base')")
            .unwrap();
        conn.execute("SAVEPOINT a").unwrap();
        conn.execute("INSERT INTO t (id, val) VALUES (2, 'a_row')")
            .unwrap();
        conn.execute("SAVEPOINT b").unwrap();
        conn.execute("INSERT INTO t (id, val) VALUES (3, 'b_row')")
            .unwrap();
        conn.execute("ROLLBACK TO b").unwrap();
        conn.execute("INSERT INTO t (id, val) VALUES (4, 'after_b_rollback')")
            .unwrap();
        conn.execute("RELEASE a").unwrap();
        conn.execute("COMMIT").unwrap();
    }

    let db = open_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][1], Value::Text("base".into()));
    assert_eq!(qr.rows[1][1], Value::Text("a_row".into()));
    assert_eq!(qr.rows[2][1], Value::Text("after_b_rollback".into()));
}

#[test]
fn savepoint_then_outer_rollback_persists_nothing() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup(&mut conn);

        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
            .unwrap();
        conn.execute("SAVEPOINT sp").unwrap();
        conn.execute("INSERT INTO t (id, val) VALUES (2, 'b')")
            .unwrap();
        conn.execute("RELEASE sp").unwrap();
        conn.execute("ROLLBACK").unwrap();
    }

    let db = open_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
}

// ═══════════════════════════════════════════════════════════════════
// Smoke / end-to-end flows
// ═══════════════════════════════════════════════════════════════════

#[test]
fn smoke_simple_flow() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
        .unwrap();
    conn.execute("SAVEPOINT sp1").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'b')")
        .unwrap();
    conn.execute("ROLLBACK TO sp1").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn smoke_update_delete_mix() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    for i in 1..=5 {
        conn.execute(&format!("INSERT INTO t (id, val) VALUES ({i}, 'v{i}')"))
            .unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("UPDATE t SET val = 'updated' WHERE id > 2")
        .unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 5);
    for (i, row) in qr.rows.iter().enumerate() {
        let id = i + 1;
        assert_eq!(row[0], Value::Integer(id as i64));
        assert_eq!(row[1], Value::Text(format!("v{id}").into()));
    }
}

#[test]
fn smoke_many_nested_savepoints() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    for i in 1..=10 {
        conn.execute(&format!("SAVEPOINT s{i}")).unwrap();
        conn.execute(&format!("INSERT INTO t (id, val) VALUES ({i}, 'row{i}')"))
            .unwrap();
    }
    conn.execute("ROLLBACK TO s5").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 4);
    for (i, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer((i + 1) as i64));
    }
}

// ═══════════════════════════════════════════════════════════════════
// AND CHAIN rejection (parser-level)
// ═══════════════════════════════════════════════════════════════════

#[test]
fn commit_and_chain_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("BEGIN").unwrap();
    let err = conn.execute("COMMIT AND CHAIN").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(_)));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn rollback_and_chain_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("BEGIN").unwrap();
    let err = conn.execute("ROLLBACK AND CHAIN").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(_)));
    conn.execute("ROLLBACK").unwrap();
}

// ═══════════════════════════════════════════════════════════════════
// Real-world scenarios
// ═══════════════════════════════════════════════════════════════════

#[test]
fn real_bank_transfer_with_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, balance INTEGER NOT NULL CHECK(balance >= 0), frozen INTEGER NOT NULL)",
    )
    .unwrap();
    conn.execute("INSERT INTO accounts (id, name, balance, frozen) VALUES (1, 'Alice', 1000, 0)")
        .unwrap();
    conn.execute("INSERT INTO accounts (id, name, balance, frozen) VALUES (2, 'Bob', 500, 1)")
        .unwrap();
    conn.execute("INSERT INTO accounts (id, name, balance, frozen) VALUES (3, 'Carol', 200, 0)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    assert_rows(
        conn.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
            .unwrap(),
        1,
    );
    conn.execute("SAVEPOINT try_bob").unwrap();
    let frozen = conn
        .query("SELECT frozen FROM accounts WHERE id = 2")
        .unwrap();
    if matches!(frozen.rows[0][0], Value::Integer(1)) {
        conn.execute("ROLLBACK TO try_bob").unwrap();
        conn.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 3")
            .unwrap();
    } else {
        conn.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
            .unwrap();
        conn.execute("RELEASE try_bob").unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let qr = conn
        .query("SELECT id, balance FROM accounts ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows[0][1], Value::Integer(900));
    assert_eq!(qr.rows[1][1], Value::Integer(500));
    assert_eq!(qr.rows[2][1], Value::Integer(300));
    let total = count(&mut conn, "SELECT SUM(balance) FROM accounts");
    assert_eq!(total, 1700);
}

#[test]
fn real_bulk_import_with_batch_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER CHECK(qty > 0))")
        .unwrap();

    let batches: &[&[(i64, i64)]] = &[
        &[(1, 10), (2, 20), (3, 30)],
        &[(4, 40), (5, -5), (6, 60)], // bad batch (qty -5 fails CHECK)
        &[(7, 70), (8, 80)],
        &[(9, 90), (10, 100)],
    ];

    conn.execute("BEGIN").unwrap();
    let mut imported = 0;
    let mut skipped_batches = 0;
    for batch in batches {
        conn.execute("SAVEPOINT batch_sp").unwrap();
        let mut ok = true;
        for &(id, qty) in *batch {
            if let Err(e) =
                conn.execute(&format!("INSERT INTO items (id, qty) VALUES ({id}, {qty})"))
            {
                ok = false;
                assert!(matches!(e, SqlError::CheckViolation(_)));
                break;
            }
        }
        if ok {
            conn.execute("RELEASE batch_sp").unwrap();
            imported += batch.len();
        } else {
            conn.execute("ROLLBACK TO batch_sp").unwrap();
            conn.execute("RELEASE batch_sp").unwrap();
            skipped_batches += 1;
        }
    }
    conn.execute("COMMIT").unwrap();

    assert_eq!(skipped_batches, 1);
    assert_eq!(imported, 7);
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM items"), 7);
    let ids = conn.query("SELECT id FROM items ORDER BY id").unwrap();
    let got: Vec<i64> = ids
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(got, vec![1, 2, 3, 7, 8, 9, 10]);
}

#[test]
fn real_fk_cascade_partial_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL)")
        .unwrap();
    conn.execute(
        "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL REFERENCES orders(id), sku TEXT NOT NULL, qty INTEGER NOT NULL)",
    )
    .unwrap();
    conn.execute("CREATE UNIQUE INDEX uq_order_sku ON order_items (order_id, sku)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO orders (id, customer) VALUES (1, 'Alice')")
        .unwrap();
    conn.execute("INSERT INTO order_items (id, order_id, sku, qty) VALUES (1, 1, 'A', 1)")
        .unwrap();
    conn.execute("SAVEPOINT add_more").unwrap();
    conn.execute("INSERT INTO order_items (id, order_id, sku, qty) VALUES (2, 1, 'B', 2)")
        .unwrap();
    let err = conn
        .execute("INSERT INTO order_items (id, order_id, sku, qty) VALUES (3, 1, 'B', 99)")
        .unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
    conn.execute("ROLLBACK TO add_more").unwrap();
    conn.execute("INSERT INTO order_items (id, order_id, sku, qty) VALUES (2, 1, 'B', 2)")
        .unwrap();
    conn.execute("INSERT INTO order_items (id, order_id, sku, qty) VALUES (3, 1, 'C', 3)")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn
        .query("SELECT sku, qty FROM order_items ORDER BY sku")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Text("A".into()));
    assert_eq!(qr.rows[1][0], Value::Text("B".into()));
    assert_eq!(qr.rows[2][0], Value::Text("C".into()));
}

#[test]
fn real_join_query_after_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, uid INTEGER, tag TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_p_uid ON p (uid)").unwrap();

    conn.execute("INSERT INTO u (id, name) VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    conn.execute("INSERT INTO p (id, uid, tag) VALUES (10, 1, 'x'), (11, 1, 'y'), (12, 2, 'z')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO p (id, uid, tag) VALUES (13, 1, 'DROPME'), (14, 2, 'DROPME')")
        .unwrap();
    conn.execute("UPDATE u SET name = 'ALICE-MOD' WHERE id = 1")
        .unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    let qr = conn
        .query(
            "SELECT u.name, COUNT(p.id) FROM u LEFT JOIN p ON p.uid = u.id GROUP BY u.name ORDER BY u.name",
        )
        .unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Text("bob".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(1));
}

#[test]
fn real_view_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'a'), (2, 'b')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    let qr = conn.query("SELECT COUNT(*) FROM v").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    let err = conn.query("SELECT * FROM v").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn real_deep_nesting_30_levels() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    for i in 1..=30 {
        conn.execute(&format!("SAVEPOINT sp{i}")).unwrap();
        conn.execute(&format!("INSERT INTO t (id, val) VALUES ({i}, 'lvl{i}')"))
            .unwrap();
    }
    conn.execute("ROLLBACK TO sp5").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 4);
    for (i, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer((i + 1) as i64));
    }
}

#[test]
fn real_large_rollback_to() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    for i in 1..=50 {
        conn.execute(&format!("INSERT INTO t (id, val) VALUES ({i}, 'base{i}')"))
            .unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    for i in 100..=2100 {
        conn.execute(&format!("INSERT INTO t (id, val) VALUES ({i}, 'post{i}')"))
            .unwrap();
    }
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1000000, 'final')")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 51);
    let ids: Vec<i64> = conn
        .query("SELECT id FROM t WHERE id > 100 ORDER BY id")
        .unwrap()
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![1_000_000]);
}

#[test]
fn real_triple_shadow_peel() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'l1')")
        .unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'l2')")
        .unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (3, 'l3')")
        .unwrap();

    conn.execute("RELEASE sp").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 3);

    conn.execute("ROLLBACK TO sp").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);

    conn.execute("RELEASE sp").unwrap();

    conn.execute("ROLLBACK TO sp").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);

    conn.execute("COMMIT").unwrap();
}

#[test]
fn real_repeated_rollback_to_same() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'base')")
        .unwrap();
    conn.execute("SAVEPOINT sp").unwrap();

    for attempt in 1..=5 {
        conn.execute(&format!(
            "INSERT INTO t (id, val) VALUES ({attempt}0, 'attempt')"
        ))
        .unwrap();
        conn.execute(&format!(
            "INSERT INTO t (id, val) VALUES ({attempt}1, 'more')"
        ))
        .unwrap();
        assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 3);
        conn.execute("ROLLBACK TO sp").unwrap();
        assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
    }

    conn.execute("COMMIT").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
    let qr = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("base".into()));
}

#[test]
fn real_reader_isolation_during_savepoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut writer = Connection::open(&db).unwrap();
    setup(&mut writer);
    writer
        .execute("INSERT INTO t (id, val) VALUES (1, 'committed')")
        .unwrap();

    let mut reader = Connection::open(&db).unwrap();
    let before = reader.query("SELECT val FROM t").unwrap();
    assert_eq!(before.rows[0][0], Value::Text("committed".into()));

    writer.execute("BEGIN").unwrap();
    writer.execute("SAVEPOINT sp").unwrap();
    writer
        .execute("INSERT INTO t (id, val) VALUES (2, 'uncommitted')")
        .unwrap();

    let during = reader.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(during.rows[0][0], Value::Integer(1));

    writer.execute("ROLLBACK TO sp").unwrap();
    writer.execute("COMMIT").unwrap();

    let after = reader.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(after.rows[0][0], Value::Integer(1));
}

#[test]
fn real_multicol_unique_index_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE m (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX uq_ab ON m (a, b)")
        .unwrap();
    conn.execute("INSERT INTO m (id, a, b) VALUES (1, 1, 1), (2, 1, 2), (3, 2, 1)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("DELETE FROM m WHERE id = 1").unwrap();
    conn.execute("INSERT INTO m (id, a, b) VALUES (4, 1, 1)")
        .unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();

    // Nested savepoint around the failing INSERT so the unique violation
    // leaves the txn in a clean state for the retry below.
    conn.execute("SAVEPOINT try").unwrap();
    let err = conn
        .execute("INSERT INTO m (id, a, b) VALUES (5, 1, 1)")
        .unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
    conn.execute("ROLLBACK TO try").unwrap();
    conn.execute("INSERT INTO m (id, a, b) VALUES (5, 3, 3)")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM m"), 4);
}

#[test]
fn real_aggregate_after_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)")
        .unwrap();
    for i in 1..=20 {
        let cat = if i % 2 == 0 { "even" } else { "odd" };
        conn.execute(&format!(
            "INSERT INTO s (id, category, amount) VALUES ({i}, '{cat}', {})",
            i * 10
        ))
        .unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("UPDATE s SET amount = amount * 100").unwrap();
    conn.execute("DELETE FROM s WHERE id > 10").unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();

    let qr = conn
        .query("SELECT category, SUM(amount), COUNT(*) FROM s GROUP BY category ORDER BY category")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("even".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(1100));
    assert_eq!(qr.rows[0][2], Value::Integer(10));
    assert_eq!(qr.rows[1][0], Value::Text("odd".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(1000));
    assert_eq!(qr.rows[1][2], Value::Integer(10));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn real_prepared_insert_across_savepoints() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    for i in 1..=5 {
        conn.execute_params(
            "INSERT INTO t (id, val) VALUES ($1, $2)",
            &[Value::Integer(i), Value::Text(format!("v{i}").into())],
        )
        .unwrap();
    }
    conn.execute("SAVEPOINT sp").unwrap();
    for i in 6..=10 {
        conn.execute_params(
            "INSERT INTO t (id, val) VALUES ($1, $2)",
            &[Value::Integer(i), Value::Text(format!("v{i}").into())],
        )
        .unwrap();
    }
    conn.execute("ROLLBACK TO sp").unwrap();
    for i in 6..=8 {
        conn.execute_params(
            "INSERT INTO t (id, val) VALUES ($1, $2)",
            &[Value::Integer(i), Value::Text(format!("new{i}").into())],
        )
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 8);
    assert_eq!(qr.rows[5][1], Value::Text("new6".into()));
    assert_eq!(qr.rows[6][1], Value::Text("new7".into()));
    assert_eq!(qr.rows[7][1], Value::Text("new8".into()));
}

#[test]
fn real_mixed_workload() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, type TEXT NOT NULL, amount INTEGER NOT NULL)",
    )
    .unwrap();
    conn.execute("CREATE INDEX idx_type ON events (type)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO events (id, type, amount) VALUES (1, 'seed', 10)")
        .unwrap();
    conn.execute("SAVEPOINT phase1").unwrap();
    conn.execute("INSERT INTO events (id, type, amount) VALUES (2, 'a', 20)")
        .unwrap();
    conn.execute("INSERT INTO events (id, type, amount) VALUES (3, 'a', 30)")
        .unwrap();

    conn.execute("SAVEPOINT phase2").unwrap();
    conn.execute("ALTER TABLE events ADD COLUMN tag TEXT")
        .unwrap();
    conn.execute("UPDATE events SET tag = 'auto' WHERE id > 1")
        .unwrap();
    let qr = conn
        .query("SELECT COUNT(*) FROM events WHERE tag = 'auto'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));

    conn.execute("ROLLBACK TO phase2").unwrap();
    let err = conn.query("SELECT tag FROM events").unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)));

    conn.execute("INSERT INTO events (id, type, amount) VALUES (4, 'b', 40)")
        .unwrap();
    conn.execute("RELEASE phase1").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn
        .query("SELECT COUNT(*), SUM(amount) FROM events")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(4));
    assert_eq!(qr.rows[0][1], Value::Integer(100));
}

#[test]
fn real_savepoint_name_matches_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT t").unwrap(); // same name as table
    conn.execute("INSERT INTO t (id) VALUES (2)").unwrap();
    conn.execute("ROLLBACK TO t").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn real_complex_error_recovery_flow() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE inv (sku TEXT PRIMARY KEY, qty INTEGER NOT NULL CHECK(qty >= 0))",
        )
        .unwrap();

        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO inv (sku, qty) VALUES ('A', 100)")
            .unwrap();
        conn.execute("INSERT INTO inv (sku, qty) VALUES ('B', 50)")
            .unwrap();

        conn.execute("SAVEPOINT alloc").unwrap();
        let err = conn
            .execute("UPDATE inv SET qty = qty - 150 WHERE sku = 'A'")
            .unwrap_err();
        assert!(matches!(err, SqlError::CheckViolation(_)));
        conn.execute("ROLLBACK TO alloc").unwrap();

        conn.execute("UPDATE inv SET qty = qty - 100 WHERE sku = 'A'")
            .unwrap();

        conn.execute("SAVEPOINT ship_b").unwrap();
        conn.execute("UPDATE inv SET qty = 0 WHERE sku = 'B'")
            .unwrap();
        conn.execute("ROLLBACK TO ship_b").unwrap();
        conn.execute("UPDATE inv SET qty = 10 WHERE sku = 'B'")
            .unwrap();

        conn.execute("RELEASE alloc").unwrap();
        conn.execute("COMMIT").unwrap();
    }

    let db = open_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT sku, qty FROM inv ORDER BY sku").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("A".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(0));
    assert_eq!(qr.rows[1][0], Value::Text("B".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(10));
}
