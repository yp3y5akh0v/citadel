use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}


fn assert_ok(result: ExecutionResult) {
    match result {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn assert_rows_affected(result: ExecutionResult, expected: u64) {
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, expected),
        other => panic!("expected RowsAffected({expected}), got {other:?}"),
    }
}

// ── Basic transaction control ────────────────────────────────────────

#[test]
fn begin_commit_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    assert!(!conn.in_transaction());
    assert_ok(conn.execute("BEGIN").unwrap());
    assert!(conn.in_transaction());

    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (2, 'b')").unwrap(), 1);

    assert_ok(conn.execute("COMMIT").unwrap());
    assert!(!conn.in_transaction());

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][1], Value::Text("a".into()));
    assert_eq!(qr.rows[1][1], Value::Text("b".into()));
}

#[test]
fn begin_rollback_discards_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'original')").unwrap(), 1);

    assert_ok(conn.execute("BEGIN").unwrap());
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (2, 'new')").unwrap(), 1);
    assert_rows_affected(conn.execute("UPDATE t SET val = 'modified' WHERE id = 1").unwrap(), 1);
    assert_ok(conn.execute("ROLLBACK").unwrap());

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("original".into()));
}

#[test]
fn begin_transaction_keyword() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // "BEGIN TRANSACTION" should work the same as "BEGIN"
    assert_ok(conn.execute("BEGIN TRANSACTION").unwrap());
    assert!(conn.in_transaction());
    assert_ok(conn.execute("ROLLBACK").unwrap());
    assert!(!conn.in_transaction());
}

// ── Read-your-writes ────────────────────────────────────────────────

#[test]
fn read_your_writes_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'hello')").unwrap(), 1);

    // SELECT within the same transaction should see the uncommitted insert
    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("hello".into()));

    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (2, 'world')").unwrap(), 1);

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);

    conn.execute("COMMIT").unwrap();
}

#[test]
fn read_after_update_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'before')").unwrap(), 1);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("UPDATE t SET val = 'after' WHERE id = 1").unwrap(), 1);

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("after".into()));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn read_after_delete_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (2, 'b')").unwrap(), 1);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("DELETE FROM t WHERE id = 1").unwrap(), 1);

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));

    conn.execute("COMMIT").unwrap();
}

// ── DDL in transactions ─────────────────────────────────────────────

#[test]
fn create_table_in_transaction_committed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'test')").unwrap(), 1);
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn create_table_in_transaction_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'test')").unwrap(), 1);
    conn.execute("ROLLBACK").unwrap();

    // Table should not exist after rollback
    let err = conn.execute("SELECT * FROM t").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn drop_table_in_transaction_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'keep')").unwrap(), 1);

    conn.execute("BEGIN").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // Table should still exist after rollback
    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("keep".into()));
}

#[test]
fn create_and_drop_same_table_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("COMMIT").unwrap();

    // Table should not exist
    let err = conn.execute("SELECT * FROM t").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

// ── Error handling in transactions ──────────────────────────────────

#[test]
fn error_begin_while_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    let err = conn.execute("BEGIN").unwrap_err();
    assert!(matches!(err, SqlError::TransactionAlreadyActive));
    // Original transaction should still be active
    assert!(conn.in_transaction());
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn error_commit_without_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let err = conn.execute("COMMIT").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn error_rollback_without_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let err = conn.execute("ROLLBACK").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn statement_error_keeps_transaction_active() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT NOT NULL)").unwrap();

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'ok')").unwrap(), 1);

    // This should fail (NOT NULL violation) but not kill the transaction
    let err = conn.execute("INSERT INTO t (id, val) VALUES (2, NULL)").unwrap_err();
    assert!(matches!(err, SqlError::NotNullViolation(_)));
    assert!(conn.in_transaction());

    // Previous successful insert should still be visible
    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);

    // Can still commit the valid changes
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn duplicate_key_error_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);

    // Duplicate key should fail
    let err = conn.execute("INSERT INTO t (id) VALUES (1)").unwrap_err();
    assert!(matches!(err, SqlError::DuplicateKey));
    assert!(conn.in_transaction());

    // Can rollback after error
    conn.execute("ROLLBACK").unwrap();
}

// ── Multi-table transactions ────────────────────────────────────────

#[test]
fn multi_table_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)").unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER, name TEXT)").unwrap();

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO orders (id, total) VALUES (1, 100)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO items (id, order_id, name) VALUES (1, 1, 'Widget')").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO items (id, order_id, name) VALUES (2, 1, 'Gadget')").unwrap(), 1);
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM orders").unwrap();
    assert_eq!(qr.rows.len(), 1);
    let qr = conn.query("SELECT * FROM items ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn multi_table_transaction_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)").unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER, name TEXT)").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO orders (id, total) VALUES (1, 50)").unwrap(), 1);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO orders (id, total) VALUES (2, 100)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO items (id, order_id, name) VALUES (1, 2, 'Widget')").unwrap(), 1);
    conn.execute("ROLLBACK").unwrap();

    // Only the pre-transaction order should exist
    let qr = conn.query("SELECT * FROM orders").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));

    let qr = conn.query("SELECT * FROM items").unwrap();
    assert_eq!(qr.rows.len(), 0);
}

// ── Persistence ─────────────────────────────────────────────────────

#[test]
fn committed_transaction_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        conn.execute("BEGIN").unwrap();
        assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'persisted')").unwrap(), 1);
        assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (2, 'also persisted')").unwrap(), 1);
        conn.execute("COMMIT").unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(qr.rows.len(), 2);
        assert_eq!(qr.rows[0][1], Value::Text("persisted".into()));
    }
}

#[test]
fn rolled_back_transaction_not_persisted() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'keep')").unwrap(), 1);

        conn.execute("BEGIN").unwrap();
        assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (2, 'discard')").unwrap(), 1);
        conn.execute("ROLLBACK").unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT * FROM t").unwrap();
        assert_eq!(qr.rows.len(), 1);
        assert_eq!(qr.rows[0][1], Value::Text("keep".into()));
    }
}

// ── Drop without commit (auto-rollback) ─────────────────────────────

#[test]
fn drop_connection_with_active_transaction_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());

    {
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'keep')").unwrap(), 1);
    }

    {
        let mut conn = Connection::open(&db).unwrap();

        conn.execute("BEGIN").unwrap();
        assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (2, 'should_disappear')").unwrap(), 1);
        // Drop without COMMIT or ROLLBACK
    }

    {
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT * FROM t").unwrap();
        assert_eq!(qr.rows.len(), 1);
        assert_eq!(qr.rows[0][1], Value::Text("keep".into()));
    }
}

// ── Aggregation in transactions ─────────────────────────────────────

#[test]
fn aggregation_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 10)").unwrap(), 1);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (2, 20)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (3, 30)").unwrap(), 1);

    let qr = conn.query("SELECT COUNT(*), SUM(val) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    assert_eq!(qr.rows[0][1], Value::Integer(60));

    conn.execute("COMMIT").unwrap();
}

// ── Multiple transactions in sequence ───────────────────────────────

#[test]
fn sequential_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    // Transaction 1: insert and commit
    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'first')").unwrap(), 1);
    conn.execute("COMMIT").unwrap();

    // Transaction 2: insert and rollback
    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (2, 'second')").unwrap(), 1);
    conn.execute("ROLLBACK").unwrap();

    // Transaction 3: insert and commit
    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (3, 'third')").unwrap(), 1);
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

// ── Complex scenarios ───────────────────────────────────────────────

#[test]
fn insert_update_delete_in_single_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (2, 'b')").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (3, 'c')").unwrap(), 1);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (4, 'd')").unwrap(), 1);
    assert_rows_affected(conn.execute("UPDATE t SET val = 'updated' WHERE id = 2").unwrap(), 1);
    assert_rows_affected(conn.execute("DELETE FROM t WHERE id = 3").unwrap(), 1);
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Text("a".into())]);
    assert_eq!(qr.rows[1], vec![Value::Integer(2), Value::Text("updated".into())]);
    assert_eq!(qr.rows[2], vec![Value::Integer(4), Value::Text("d".into())]);
}

#[test]
fn order_by_limit_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

    conn.execute("BEGIN").unwrap();
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t (id, val) VALUES ({i}, {})", i * 10)).unwrap();
    }

    let qr = conn.query("SELECT * FROM t ORDER BY val DESC LIMIT 3").unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][1], Value::Integer(100));
    assert_eq!(qr.rows[1][1], Value::Integer(90));
    assert_eq!(qr.rows[2][1], Value::Integer(80));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn where_filter_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, status) VALUES (1, 'active')").unwrap();
    conn.execute("INSERT INTO t (id, status) VALUES (2, 'inactive')").unwrap();
    conn.execute("INSERT INTO t (id, status) VALUES (3, 'active')").unwrap();

    let qr = conn.query("SELECT id FROM t WHERE status = 'active' ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn auto_commit_still_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // Without BEGIN, each statement is auto-committed
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id, val) VALUES (1, 'auto')").unwrap(), 1);

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("auto".into()));
}

#[test]
fn group_by_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT NOT NULL, amount INTEGER NOT NULL)"
    ).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO sales (id, category, amount) VALUES (1, 'A', 10)").unwrap();
    conn.execute("INSERT INTO sales (id, category, amount) VALUES (2, 'B', 20)").unwrap();
    conn.execute("INSERT INTO sales (id, category, amount) VALUES (3, 'A', 30)").unwrap();
    conn.execute("INSERT INTO sales (id, category, amount) VALUES (4, 'B', 40)").unwrap();

    let qr = conn.query(
        "SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY category"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("A".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(40));
    assert_eq!(qr.rows[1][0], Value::Text("B".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(60));

    conn.execute("COMMIT").unwrap();
}
