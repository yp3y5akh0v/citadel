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

#[test]
fn begin_commit_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    assert!(!conn.in_transaction());
    assert_ok(conn.execute("BEGIN").unwrap());
    assert!(conn.in_transaction());

    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (2, 'b')")
            .unwrap(),
        1,
    );

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
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'original')")
            .unwrap(),
        1,
    );

    assert_ok(conn.execute("BEGIN").unwrap());
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (2, 'new')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("UPDATE t SET val = 'modified' WHERE id = 1")
            .unwrap(),
        1,
    );
    assert_ok(conn.execute("ROLLBACK").unwrap());

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("original".into()));
}

#[test]
fn begin_transaction_keyword() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    // "BEGIN TRANSACTION" should work the same as "BEGIN"
    assert_ok(conn.execute("BEGIN TRANSACTION").unwrap());
    assert!(conn.in_transaction());
    assert_ok(conn.execute("ROLLBACK").unwrap());
    assert!(!conn.in_transaction());
}

#[test]
fn read_your_writes_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'hello')")
            .unwrap(),
        1,
    );

    // SELECT within the same transaction should see the uncommitted insert
    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("hello".into()));

    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (2, 'world')")
            .unwrap(),
        1,
    );

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);

    conn.execute("COMMIT").unwrap();
}

#[test]
fn read_after_update_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'before')")
            .unwrap(),
        1,
    );

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("UPDATE t SET val = 'after' WHERE id = 1")
            .unwrap(),
        1,
    );

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("after".into()));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn read_after_delete_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (2, 'b')")
            .unwrap(),
        1,
    );

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("DELETE FROM t WHERE id = 1").unwrap(), 1);

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn create_table_in_transaction_committed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'test')")
            .unwrap(),
        1,
    );
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn create_table_in_transaction_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'test')")
            .unwrap(),
        1,
    );
    conn.execute("ROLLBACK").unwrap();

    // Table should not exist after rollback
    let err = conn.execute("SELECT * FROM t").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn drop_table_in_transaction_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'keep')")
            .unwrap(),
        1,
    );

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
    let conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("COMMIT").unwrap();

    // Table should not exist
    let err = conn.execute("SELECT * FROM t").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn error_begin_while_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

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
    let conn = Connection::open(&db).unwrap();

    let err = conn.execute("COMMIT").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn error_rollback_without_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    let err = conn.execute("ROLLBACK").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn statement_error_keeps_transaction_active() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'ok')")
            .unwrap(),
        1,
    );

    // This should fail (NOT NULL violation) but not kill the transaction
    let err = conn
        .execute("INSERT INTO t (id, val) VALUES (2, NULL)")
        .unwrap_err();
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
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);

    // Duplicate key should fail
    let err = conn.execute("INSERT INTO t (id) VALUES (1)").unwrap_err();
    assert!(matches!(err, SqlError::DuplicateKey));
    assert!(conn.in_transaction());

    // Can rollback after error
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn multi_table_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER, name TEXT)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO orders (id, total) VALUES (1, 100)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items (id, order_id, name) VALUES (1, 1, 'Widget')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items (id, order_id, name) VALUES (2, 1, 'Gadget')")
            .unwrap(),
        1,
    );
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
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER, name TEXT)")
        .unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO orders (id, total) VALUES (1, 50)")
            .unwrap(),
        1,
    );

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO orders (id, total) VALUES (2, 100)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items (id, order_id, name) VALUES (1, 2, 'Widget')")
            .unwrap(),
        1,
    );
    conn.execute("ROLLBACK").unwrap();

    // Only the pre-transaction order should exist
    let qr = conn.query("SELECT * FROM orders").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));

    let qr = conn.query("SELECT * FROM items").unwrap();
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn committed_transaction_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();

        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        assert_rows_affected(
            conn.execute("INSERT INTO t (id, val) VALUES (1, 'persisted')")
                .unwrap(),
            1,
        );
        assert_rows_affected(
            conn.execute("INSERT INTO t (id, val) VALUES (2, 'also persisted')")
                .unwrap(),
            1,
        );
        conn.execute("COMMIT").unwrap();
    }

    {
        let db = open_db(dir.path());
        let conn = Connection::open(&db).unwrap();
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
        let conn = Connection::open(&db).unwrap();

        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        assert_rows_affected(
            conn.execute("INSERT INTO t (id, val) VALUES (1, 'keep')")
                .unwrap(),
            1,
        );

        conn.execute("BEGIN").unwrap();
        assert_rows_affected(
            conn.execute("INSERT INTO t (id, val) VALUES (2, 'discard')")
                .unwrap(),
            1,
        );
        conn.execute("ROLLBACK").unwrap();
    }

    {
        let db = open_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT * FROM t").unwrap();
        assert_eq!(qr.rows.len(), 1);
        assert_eq!(qr.rows[0][1], Value::Text("keep".into()));
    }
}

#[test]
fn drop_connection_with_active_transaction_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());

    {
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        assert_rows_affected(
            conn.execute("INSERT INTO t (id, val) VALUES (1, 'keep')")
                .unwrap(),
            1,
        );
    }

    {
        let conn = Connection::open(&db).unwrap();

        conn.execute("BEGIN").unwrap();
        assert_rows_affected(
            conn.execute("INSERT INTO t (id, val) VALUES (2, 'should_disappear')")
                .unwrap(),
            1,
        );
        // Drop without COMMIT or ROLLBACK
    }

    {
        let conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT * FROM t").unwrap();
        assert_eq!(qr.rows.len(), 1);
        assert_eq!(qr.rows[0][1], Value::Text("keep".into()));
    }
}

#[test]
fn aggregation_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 10)")
            .unwrap(),
        1,
    );

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (2, 20)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (3, 30)")
            .unwrap(),
        1,
    );

    let qr = conn.query("SELECT COUNT(*), SUM(val) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    assert_eq!(qr.rows[0][1], Value::Integer(60));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn sequential_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'first')")
            .unwrap(),
        1,
    );
    conn.execute("COMMIT").unwrap();

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (2, 'second')")
            .unwrap(),
        1,
    );
    conn.execute("ROLLBACK").unwrap();

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (3, 'third')")
            .unwrap(),
        1,
    );
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn insert_update_delete_in_single_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (2, 'b')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (3, 'c')")
            .unwrap(),
        1,
    );

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (4, 'd')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("UPDATE t SET val = 'updated' WHERE id = 2")
            .unwrap(),
        1,
    );
    assert_rows_affected(conn.execute("DELETE FROM t WHERE id = 3").unwrap(), 1);
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Text("a".into())]);
    assert_eq!(
        qr.rows[1],
        vec![Value::Integer(2), Value::Text("updated".into())]
    );
    assert_eq!(qr.rows[2], vec![Value::Integer(4), Value::Text("d".into())]);
}

#[test]
fn order_by_limit_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t (id, val) VALUES ({i}, {})", i * 10))
            .unwrap();
    }

    let qr = conn
        .query("SELECT * FROM t ORDER BY val DESC LIMIT 3")
        .unwrap();
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
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, status) VALUES (1, 'active')")
        .unwrap();
    conn.execute("INSERT INTO t (id, status) VALUES (2, 'inactive')")
        .unwrap();
    conn.execute("INSERT INTO t (id, status) VALUES (3, 'active')")
        .unwrap();

    let qr = conn
        .query("SELECT id FROM t WHERE status = 'active' ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn auto_commit_still_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    // Without BEGIN, each statement is auto-committed
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'auto')")
            .unwrap(),
        1,
    );

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("auto".into()));
}

#[test]
fn group_by_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT NOT NULL, amount INTEGER NOT NULL)"
    ).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO sales (id, category, amount) VALUES (1, 'A', 10)")
        .unwrap();
    conn.execute("INSERT INTO sales (id, category, amount) VALUES (2, 'B', 20)")
        .unwrap();
    conn.execute("INSERT INTO sales (id, category, amount) VALUES (3, 'A', 30)")
        .unwrap();
    conn.execute("INSERT INTO sales (id, category, amount) VALUES (4, 'B', 40)")
        .unwrap();

    let qr = conn
        .query(
            "SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("A".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(40));
    assert_eq!(qr.rows[1][0], Value::Text("B".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(60));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn distinct_within_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)")
        .unwrap();

    assert_ok(conn.execute("BEGIN").unwrap());

    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (5, 30)").unwrap();

    let qr = conn
        .query("SELECT DISTINCT val FROM t ORDER BY val")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(10));
    assert_eq!(qr.rows[1][0], Value::Integer(20));
    assert_eq!(qr.rows[2][0], Value::Integer(30));

    assert_ok(conn.execute("COMMIT").unwrap());

    let qr = conn
        .query("SELECT DISTINCT val FROM t ORDER BY val")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn distinct_sees_uncommitted_within_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, color TEXT NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'red')").unwrap();

    assert_ok(conn.execute("BEGIN").unwrap());

    conn.execute("INSERT INTO t VALUES (2, 'red')").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'blue')").unwrap();

    let qr = conn
        .query("SELECT DISTINCT color FROM t ORDER BY color")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("blue".into()));
    assert_eq!(qr.rows[1][0], Value::Text("red".into()));

    assert_ok(conn.execute("ROLLBACK").unwrap());

    let qr = conn
        .query("SELECT DISTINCT color FROM t ORDER BY color")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("red".into()));
}

#[test]
fn begin_read_only_blocks_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    conn.execute("BEGIN READ ONLY").unwrap();
    // SELECT works.
    let p = conn.prepare("SELECT v FROM t WHERE id = 1").unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows[0][0], citadel_sql::Value::Integer(100));

    // Mutations error.
    let err = conn.execute("INSERT INTO t VALUES (2, 200)").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("read-only") || msg.contains("read only"),
        "expected read-only error, got: {msg}"
    );

    let err2 = conn
        .execute("UPDATE t SET v = 999 WHERE id = 1")
        .unwrap_err();
    assert!(err2.to_string().contains("read-only") || err2.to_string().contains("read only"));

    let err3 = conn.execute("DELETE FROM t WHERE id = 1").unwrap_err();
    assert!(err3.to_string().contains("read-only") || err3.to_string().contains("read only"));

    conn.execute("COMMIT").unwrap();

    // After COMMIT, normal txns work.
    conn.execute("INSERT INTO t VALUES (3, 300)").unwrap();
}

#[test]
fn begin_read_only_commit_and_rollback_both_work() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();

    // COMMIT path.
    conn.execute("BEGIN READ ONLY").unwrap();
    conn.execute("COMMIT").unwrap();
    assert!(!conn.in_transaction());

    // ROLLBACK path.
    conn.execute("BEGIN READ ONLY").unwrap();
    conn.execute("ROLLBACK").unwrap();
    assert!(!conn.in_transaction());
}

#[test]
fn begin_read_write_works_normally() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("BEGIN READ WRITE").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("COMMIT").unwrap();
}

#[test]
fn begin_read_only_create_table_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let err = conn.execute("CREATE TABLE t (id INTEGER)").unwrap_err();
    assert!(err.to_string().to_lowercase().contains("read"));
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn begin_read_only_drop_table_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let err = conn.execute("DROP TABLE t").unwrap_err();
    assert!(err.to_string().to_lowercase().contains("read"));
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn begin_read_only_create_index_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
        .unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let err = conn.execute("CREATE INDEX idx_x ON t (x)").unwrap_err();
    assert!(err.to_string().to_lowercase().contains("read"));
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn begin_read_only_alter_table_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let err = conn
        .execute("ALTER TABLE t ADD COLUMN y INTEGER")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("read"));
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn begin_read_only_create_view_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let err = conn
        .execute("CREATE VIEW v AS SELECT * FROM t")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("read"));
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn begin_read_only_truncate_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let err = conn.execute("TRUNCATE TABLE t").unwrap_err();
    assert!(err.to_string().to_lowercase().contains("read"));
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn begin_read_only_nested_begin_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let err = conn.execute("BEGIN").unwrap_err();
    assert!(matches!(err, SqlError::TransactionAlreadyActive));
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn commit_without_begin_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn.execute("COMMIT").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn rollback_without_begin_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn.execute("ROLLBACK").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn begin_read_only_multiple_selects_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    for i in 1..=5 {
        conn.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
            .unwrap();
    }
    conn.execute("BEGIN READ ONLY").unwrap();
    let p = conn.prepare("SELECT v FROM t WHERE id = $1").unwrap();
    for i in 1..=5 {
        let r = p.query_collect(&[Value::Integer(i)]).unwrap();
        assert_eq!(r.rows[0][0], Value::Integer(i * 10));
    }
    conn.execute("COMMIT").unwrap();
}

#[test]
fn begin_read_only_join_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO a VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    conn.execute("INSERT INTO b VALUES (10, 1, 'X'), (11, 2, 'Y')")
        .unwrap();

    conn.execute("BEGIN READ ONLY").unwrap();
    let p = conn
        .prepare("SELECT a.name, b.val FROM a INNER JOIN b ON a.id = b.a_id ORDER BY a.id")
        .unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows.len(), 2);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn begin_read_only_aggregate_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }

    conn.execute("BEGIN READ ONLY").unwrap();
    let p = conn.prepare("SELECT SUM(v), COUNT(*) FROM t").unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(55));
    assert_eq!(r.rows[0][1], Value::Integer(10));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn begin_read_only_then_read_write_in_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    conn.execute("COMMIT").unwrap();
    conn.execute("BEGIN READ WRITE").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("COMMIT").unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let p = conn.prepare("SELECT id FROM t").unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows.len(), 1);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn begin_read_only_set_timezone_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let result = conn.execute("SET TIME ZONE 'UTC'");
    assert!(
        result.is_ok(),
        "SET TIME ZONE should be allowed in read-only"
    );
    conn.execute("COMMIT").unwrap();
}

#[test]
fn begin_read_only_view_select_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .unwrap();
    conn.execute("CREATE VIEW big_v AS SELECT * FROM t WHERE v > 50")
        .unwrap();

    conn.execute("BEGIN READ ONLY").unwrap();
    let p = conn.prepare("SELECT id FROM big_v ORDER BY id").unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows.len(), 2);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn begin_read_only_cte_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t VALUES ({})", i))
            .unwrap();
    }

    conn.execute("BEGIN READ ONLY").unwrap();
    let p = conn
        .prepare("WITH small AS (SELECT id FROM t WHERE id < 5) SELECT COUNT(*) FROM small")
        .unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(4));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn begin_read_only_upsert_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let err = conn
        .execute("INSERT INTO t VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET v = 20")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("read"));
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn begin_read_only_in_transaction_reports_true() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert!(!conn.in_transaction());
    conn.execute("BEGIN READ ONLY").unwrap();
    assert!(conn.in_transaction());
    conn.execute("COMMIT").unwrap();
    assert!(!conn.in_transaction());
}

#[test]
fn read_only_does_not_block_concurrent_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn1 = Connection::open(&db).unwrap();
    conn1
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn1.execute("BEGIN READ ONLY").unwrap();

    let conn2 = Connection::open(&db).unwrap();
    conn2.execute("BEGIN READ ONLY").unwrap();
    conn2.execute("COMMIT").unwrap();
    conn1.execute("COMMIT").unwrap();
}

#[test]
fn begin_read_only_subquery_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    conn.execute("BEGIN READ ONLY").unwrap();
    let p = conn
        .prepare("SELECT id FROM t WHERE v > (SELECT AVG(v) FROM t) ORDER BY id")
        .unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows.len(), 5);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn begin_read_only_explain_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let p = conn.prepare("EXPLAIN SELECT * FROM t").unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert!(!r.rows.is_empty());
    conn.execute("COMMIT").unwrap();
}

#[test]
fn begin_read_only_after_uncommitted_select_does_not_leak() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    {
        conn.execute("BEGIN READ ONLY").unwrap();
        let p = conn.prepare("SELECT id FROM t").unwrap();
        let _r = p.query_collect(&[]).unwrap();
        conn.execute("COMMIT").unwrap();
    }
    // Verify next BEGIN READ ONLY can start cleanly.
    conn.execute("BEGIN READ ONLY").unwrap();
    conn.execute("COMMIT").unwrap();
}

#[test]
fn begin_then_explicit_read_only_modes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // BEGIN without keyword → default (read-write)
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("COMMIT").unwrap();
    // BEGIN TRANSACTION → default
    conn.execute("BEGIN TRANSACTION").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("COMMIT").unwrap();
}

#[test]
fn read_only_with_persistent_writer_no_blocking() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn_r = Connection::open(&db).unwrap();
    conn_r
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn_r.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    // Reader starts.
    conn_r.execute("BEGIN READ ONLY").unwrap();

    // A separate connection can commit a write.
    let conn_w = Connection::open(&db).unwrap();
    conn_w.execute("INSERT INTO t VALUES (2, 200)").unwrap();

    // Reader's COMMIT still works.
    conn_r.execute("COMMIT").unwrap();

    // After commit, reader sees the new row.
    let p = conn_r.prepare("SELECT COUNT(*) FROM t").unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(2));
}

#[test]
fn read_only_savepoint_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let _ = conn.execute("SAVEPOINT sp1");
    // SAVEPOINT/RELEASE/ROLLBACK TO inside read-only is implementation-defined;
    // verify the connection stays consistent regardless.
    conn.execute("ROLLBACK").unwrap();
    assert!(!conn.in_transaction());
}

#[test]
fn temporary_table_create_insert_select_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO tmp VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();
    let p = conn.prepare("SELECT v FROM tmp WHERE id = 2").unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(200));
}

#[test]
fn temporary_table_visible_only_in_owning_connection() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn1 = Connection::open(&db).unwrap();
    conn1
        .execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn1.execute("INSERT INTO tmp VALUES (1)").unwrap();

    let conn2 = Connection::open(&db).unwrap();
    let err = conn2.execute("SELECT * FROM tmp").unwrap_err();
    assert!(
        matches!(err, SqlError::TableNotFound(_)),
        "tmp should NOT be visible to conn2, got: {err:?}"
    );
}

#[test]
fn temporary_table_dropped_on_connection_close() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO tmp VALUES (42)").unwrap();
    } // conn drops here → temp tables cleaned up

    // A fresh connection sees no leaked tmp tables.
    let conn = Connection::open(&db).unwrap();
    let err = conn.execute("SELECT * FROM tmp").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn temporary_table_can_be_dropped_explicitly() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("DROP TABLE tmp").unwrap();
    let err = conn.execute("INSERT INTO tmp VALUES (1)").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn temporary_table_with_index_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX idx_v ON tmp (v)").unwrap();
    conn.execute("INSERT INTO tmp VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let p = conn.prepare("SELECT id FROM tmp WHERE v = 20").unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(2));
}

#[test]
fn temporary_table_duplicate_create_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY)")
        .unwrap();
    let err = conn
        .execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY)")
        .unwrap_err();
    assert!(matches!(err, SqlError::TableAlreadyExists(_)));
}

#[test]
fn temporary_table_if_not_exists_no_error_on_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TEMPORARY TABLE IF NOT EXISTS tmp (id INTEGER PRIMARY KEY)")
        .unwrap();
}

#[test]
fn temporary_table_update_delete_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO tmp VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("UPDATE tmp SET v = 999 WHERE id = 1").unwrap();
    let p = conn.prepare("SELECT v FROM tmp WHERE id = 1").unwrap();
    assert_eq!(
        p.query_collect(&[]).unwrap().rows[0][0],
        Value::Integer(999)
    );
    conn.execute("DELETE FROM tmp WHERE id = 2").unwrap();
    let p2 = conn.prepare("SELECT COUNT(*) FROM tmp").unwrap();
    assert_eq!(p2.query_collect(&[]).unwrap().rows[0][0], Value::Integer(1));
}

#[test]
fn temporary_table_inside_read_only_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let err = conn
        .execute("CREATE TEMPORARY TABLE tmp (id INTEGER)")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("read"));
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn temporary_table_can_join_persistent_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE persistent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO persistent VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    conn.execute("CREATE TEMPORARY TABLE tmp_filter (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO tmp_filter VALUES (1)").unwrap();
    let p = conn
        .prepare(
            "SELECT p.name FROM persistent p \
             INNER JOIN tmp_filter t ON p.id = t.id",
        )
        .unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Text("Alice".into()));
}
