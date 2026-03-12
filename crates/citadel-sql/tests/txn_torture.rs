//! Torture tests for BEGIN / COMMIT / ROLLBACK transaction control.
//!
//! These tests exhaustively cover edge cases, error recovery, DDL/DML
//! interleaving, read-your-writes semantics, and schema consistency.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"torture-test")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"torture-test")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

fn setup_table(conn: &mut Connection<'_>) {
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)").unwrap();
}

fn count_rows(conn: &mut Connection<'_>, table: &str) -> i64 {
    let qr = conn.query(&format!("SELECT COUNT(*) FROM {table}")).unwrap();
    match &qr.rows[0][0] {
        Value::Integer(n) => *n,
        _ => panic!("expected integer count"),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Section 1: Read-your-writes exhaustive combinations
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn ryw_insert_then_select_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();
    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn ryw_insert_then_select_with_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'b', 20)").unwrap();

    let qr = conn.query("SELECT * FROM t WHERE num > 15").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn ryw_update_then_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'before', 10)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET val = 'after' WHERE id = 1").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("after".into()));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn ryw_delete_then_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'b', 20)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn ryw_insert_update_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'original', 10)").unwrap();
    conn.execute("UPDATE t SET val = 'modified' WHERE id = 1").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("modified".into()));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn ryw_insert_delete_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'ephemeral', 10)").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 0);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn ryw_delete_all_then_reinsert() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM t").unwrap();
    assert_eq!(count_rows(&mut conn, "t"), 0);
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'new', 20)").unwrap();
    assert_eq!(count_rows(&mut conn, "t"), 1);
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn ryw_multiple_updates_same_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'v0', 0)").unwrap();

    conn.execute("BEGIN").unwrap();
    for i in 1..=10 {
        conn.execute(&format!("UPDATE t SET val = 'v{i}', num = {i} WHERE id = 1")).unwrap();
    }

    let qr = conn.query("SELECT val, num FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("v10".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(10));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn ryw_insert_many_then_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    for i in 1..=50 {
        conn.execute(&format!("INSERT INTO t (id, val, num) VALUES ({i}, 'x', {i})")).unwrap();
    }

    let qr = conn.query("SELECT COUNT(*), SUM(num), MIN(num), MAX(num), AVG(num) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(50));
    assert_eq!(qr.rows[0][1], Value::Integer(1275)); // 50*51/2
    assert_eq!(qr.rows[0][2], Value::Integer(1));
    assert_eq!(qr.rows[0][3], Value::Integer(50));
    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Section 2: DDL + DML interleaving in transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn ddl_create_insert_select_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'in-txn')").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);

    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn ddl_create_insert_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'gone')").unwrap();
    conn.execute("ROLLBACK").unwrap();

    let err = conn.query("SELECT * FROM t").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn ddl_create_two_tables_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)").unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)").unwrap();
    conn.execute("INSERT INTO a (id) VALUES (1)").unwrap();
    conn.execute("INSERT INTO b (id) VALUES (2)").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "a"), 1);
    assert_eq!(count_rows(&mut conn, "b"), 1);
}

#[test]
fn ddl_create_two_tables_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)").unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)").unwrap();
    conn.execute("ROLLBACK").unwrap();

    assert!(conn.query("SELECT * FROM a").is_err());
    assert!(conn.query("SELECT * FROM b").is_err());
}

#[test]
fn ddl_drop_rollback_restores_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'keep')").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'keep')").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("ROLLBACK").unwrap();

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][1], Value::Text("keep".into()));
}

#[test]
fn ddl_drop_recreate_same_name_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'old')").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").unwrap();
    conn.execute("INSERT INTO t (id, name) VALUES (1, 'new')").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT name FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("new".into()));
}

#[test]
fn ddl_drop_recreate_same_name_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'original')").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    conn.execute("INSERT INTO t (id, name) VALUES (2, 'new_schema')").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // Original table with original schema should be restored
    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("original".into()));
}

#[test]
fn ddl_create_if_not_exists_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

    conn.execute("BEGIN").unwrap();
    // Should not error
    conn.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)").unwrap();
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 1);
}

#[test]
fn ddl_drop_if_exists_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    // Should not error even though table doesn't exist
    conn.execute("DROP TABLE IF EXISTS nonexistent").unwrap();
    conn.execute("COMMIT").unwrap();
}

#[test]
fn ddl_create_table_already_exists_error_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

    conn.execute("BEGIN").unwrap();
    let err = conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap_err();
    assert!(matches!(err, SqlError::TableAlreadyExists(_)));
    // Transaction should still be active
    assert!(conn.in_transaction());
    conn.execute("ROLLBACK").unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Section 3: Error recovery within transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn error_recovery_not_null_then_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT NOT NULL)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'ok')").unwrap();

    // This fails but doesn't kill the transaction
    let err = conn.execute("INSERT INTO t (id, val) VALUES (2, NULL)").unwrap_err();
    assert!(matches!(err, SqlError::NotNullViolation(_)));
    assert!(conn.in_transaction());

    // Insert a valid row after the error
    conn.execute("INSERT INTO t (id, val) VALUES (3, 'also_ok')").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn error_recovery_duplicate_key_then_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    let err = conn.execute("INSERT INTO t (id) VALUES (1)").unwrap_err();
    assert!(matches!(err, SqlError::DuplicateKey));
    assert!(conn.in_transaction());

    conn.execute("INSERT INTO t (id) VALUES (2)").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 2);
}

#[test]
fn error_recovery_type_mismatch_then_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, flag BOOLEAN NOT NULL)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, flag) VALUES (1, true)").unwrap();

    let err = conn.execute("INSERT INTO t (id, flag) VALUES (2, 'not_a_bool')").unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
    assert!(conn.in_transaction());

    conn.execute("INSERT INTO t (id, flag) VALUES (3, false)").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 2);
}

#[test]
fn error_recovery_table_not_found_then_create() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    let err = conn.execute("INSERT INTO t (id) VALUES (1)").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
    assert!(conn.in_transaction());

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 1);
}

#[test]
fn error_recovery_multiple_errors_then_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT NOT NULL)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'ok')").unwrap();

    // Error 1
    assert!(conn.execute("INSERT INTO t (id, val) VALUES (2, NULL)").is_err());
    // Error 2
    assert!(conn.execute("INSERT INTO t (id, val) VALUES (1, 'dup')").is_err());
    // Error 3
    assert!(conn.execute("SELECT * FROM nonexistent").is_err());

    assert!(conn.in_transaction());
    conn.execute("INSERT INTO t (id, val) VALUES (4, 'after_errors')").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 2);
}

#[test]
fn error_recovery_multiple_errors_then_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    conn.execute("INSERT INTO t (id) VALUES (100)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    // Multiple errors
    assert!(conn.execute("INSERT INTO t (id) VALUES (1)").is_err());
    assert!(conn.execute("INSERT INTO t (id) VALUES (1)").is_err());

    conn.execute("ROLLBACK").unwrap();

    // Only pre-transaction row
    assert_eq!(count_rows(&mut conn, "t"), 1);
    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(100));
}

// ═══════════════════════════════════════════════════════════════════════
// Section 4: Sequential transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn ten_sequential_commit_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    for i in 0..10 {
        conn.execute("BEGIN").unwrap();
        for j in 0..5 {
            let id = i * 5 + j;
            conn.execute(&format!(
                "INSERT INTO t (id, val, num) VALUES ({id}, 'batch{i}', {j})"
            )).unwrap();
        }
        conn.execute("COMMIT").unwrap();
    }

    assert_eq!(count_rows(&mut conn, "t"), 50);
}

#[test]
fn alternating_commit_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    for i in 0..20 {
        conn.execute("BEGIN").unwrap();
        conn.execute(&format!("INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})")).unwrap();
        if i % 2 == 0 {
            conn.execute("COMMIT").unwrap();
        } else {
            conn.execute("ROLLBACK").unwrap();
        }
    }

    // Only even-numbered rows committed
    assert_eq!(count_rows(&mut conn, "t"), 10);
    let qr = conn.query("SELECT id FROM t ORDER BY id").unwrap();
    for (idx, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer((idx * 2) as i64));
    }
}

#[test]
fn commit_rollback_commit_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    // Txn 1: commit
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 1)").unwrap();
    conn.execute("COMMIT").unwrap();

    // Txn 2: rollback
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'b', 2)").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // Txn 3: commit
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (3, 'c', 3)").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn auto_commit_between_explicit_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    // Auto-commit insert
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'auto1', 1)").unwrap();

    // Explicit transaction
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'explicit', 2)").unwrap();
    conn.execute("COMMIT").unwrap();

    // Another auto-commit
    conn.execute("INSERT INTO t (id, val, num) VALUES (3, 'auto2', 3)").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 3);
}

// ═══════════════════════════════════════════════════════════════════════
// Section 5: Multi-table atomicity
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn multi_table_all_or_nothing_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)").unwrap();
    conn.execute("CREATE TABLE txn_log (id INTEGER PRIMARY KEY, account_id INTEGER, amount INTEGER)").unwrap();
    conn.execute("INSERT INTO accounts (id, balance) VALUES (1, 100)").unwrap();
    conn.execute("INSERT INTO accounts (id, balance) VALUES (2, 200)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE accounts SET balance = 50 WHERE id = 1").unwrap();
    conn.execute("UPDATE accounts SET balance = 250 WHERE id = 2").unwrap();
    conn.execute("INSERT INTO txn_log (id, account_id, amount) VALUES (1, 1, -50)").unwrap();
    conn.execute("INSERT INTO txn_log (id, account_id, amount) VALUES (2, 2, 50)").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT balance FROM accounts ORDER BY id").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(50));
    assert_eq!(qr.rows[1][0], Value::Integer(250));
    assert_eq!(count_rows(&mut conn, "txn_log"), 2);
}

#[test]
fn multi_table_all_or_nothing_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)").unwrap();
    conn.execute("CREATE TABLE txn_log (id INTEGER PRIMARY KEY, account_id INTEGER, amount INTEGER)").unwrap();
    conn.execute("INSERT INTO accounts (id, balance) VALUES (1, 100)").unwrap();
    conn.execute("INSERT INTO accounts (id, balance) VALUES (2, 200)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE accounts SET balance = 50 WHERE id = 1").unwrap();
    conn.execute("UPDATE accounts SET balance = 250 WHERE id = 2").unwrap();
    conn.execute("INSERT INTO txn_log (id, account_id, amount) VALUES (1, 1, -50)").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // Everything reverted
    let qr = conn.query("SELECT balance FROM accounts ORDER BY id").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(100));
    assert_eq!(qr.rows[1][0], Value::Integer(200));
    assert_eq!(count_rows(&mut conn, "txn_log"), 0);
}

#[test]
fn create_multiple_tables_insert_into_all_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    for i in 0..5 {
        conn.execute(&format!("CREATE TABLE t{i} (id INTEGER PRIMARY KEY, val TEXT)")).unwrap();
        for j in 0..3 {
            let id = i * 3 + j;
            conn.execute(&format!("INSERT INTO t{i} (id, val) VALUES ({id}, 'v{j}')")).unwrap();
        }
    }
    conn.execute("COMMIT").unwrap();

    for i in 0..5 {
        assert_eq!(count_rows(&mut conn, &format!("t{i}")), 3);
    }
}

#[test]
fn create_multiple_tables_insert_into_all_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    for i in 0..5 {
        conn.execute(&format!("CREATE TABLE t{i} (id INTEGER PRIMARY KEY)")).unwrap();
        conn.execute(&format!("INSERT INTO t{i} (id) VALUES (1)")).unwrap();
    }
    conn.execute("ROLLBACK").unwrap();

    for i in 0..5 {
        assert!(conn.query(&format!("SELECT * FROM t{i}")).is_err());
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Section 6: Boundary conditions and edge cases
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn empty_transaction_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("COMMIT").unwrap();
    assert!(!conn.in_transaction());
}

#[test]
fn empty_transaction_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("ROLLBACK").unwrap();
    assert!(!conn.in_transaction());
}

#[test]
fn select_only_transaction_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();

    conn.execute("BEGIN").unwrap();
    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn double_begin_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    let err = conn.execute("BEGIN").unwrap_err();
    assert!(matches!(err, SqlError::TransactionAlreadyActive));
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn commit_without_begin() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let err = conn.execute("COMMIT").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn rollback_without_begin() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let err = conn.execute("ROLLBACK").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn double_commit_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("COMMIT").unwrap();
    let err = conn.execute("COMMIT").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn double_rollback_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("ROLLBACK").unwrap();
    let err = conn.execute("ROLLBACK").unwrap_err();
    assert!(matches!(err, SqlError::NoActiveTransaction));
}

#[test]
fn begin_after_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 1)").unwrap();
    conn.execute("COMMIT").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'b', 2)").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 2);
}

#[test]
fn begin_after_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'gone', 1)").unwrap();
    conn.execute("ROLLBACK").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'kept', 2)").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 1);
    let qr = conn.query("SELECT id FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

// ═══════════════════════════════════════════════════════════════════════
// Section 7: Type coercion in transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn type_coercion_integer_to_real_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, price REAL)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, price) VALUES (1, 42)").unwrap();

    let qr = conn.query("SELECT price FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Real(42.0));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn all_types_insert_select_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, txt TEXT, num REAL, flag BOOLEAN)"
    ).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, txt, num, flag) VALUES (1, 'hello', 3.14, true)").unwrap();
    conn.execute("INSERT INTO t (id, txt, num, flag) VALUES (2, NULL, NULL, NULL)").unwrap();

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows[0][1], Value::Text("hello".into()));
    assert_eq!(qr.rows[0][2], Value::Real(3.14));
    assert_eq!(qr.rows[0][3], Value::Boolean(true));
    assert_eq!(qr.rows[1][1], Value::Null);
    assert_eq!(qr.rows[1][2], Value::Null);
    assert_eq!(qr.rows[1][3], Value::Null);

    conn.execute("COMMIT").unwrap();
}

#[test]
fn null_handling_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, NULL)").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'not_null')").unwrap();

    let qr = conn.query("SELECT * FROM t WHERE val IS NULL").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));

    let qr = conn.query("SELECT * FROM t WHERE val IS NOT NULL").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));

    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Section 8: ORDER BY, LIMIT, OFFSET in transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn order_by_asc_desc_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    for i in 1..=5 {
        conn.execute(&format!("INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})")).unwrap();
    }

    let qr = conn.query("SELECT num FROM t ORDER BY num ASC").unwrap();
    let nums: Vec<i64> = qr.rows.iter().map(|r| match &r[0] { Value::Integer(n) => *n, _ => panic!() }).collect();
    assert_eq!(nums, vec![1, 2, 3, 4, 5]);

    let qr = conn.query("SELECT num FROM t ORDER BY num DESC").unwrap();
    let nums: Vec<i64> = qr.rows.iter().map(|r| match &r[0] { Value::Integer(n) => *n, _ => panic!() }).collect();
    assert_eq!(nums, vec![5, 4, 3, 2, 1]);

    conn.execute("COMMIT").unwrap();
}

#[test]
fn limit_offset_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})")).unwrap();
    }

    let qr = conn.query("SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 2").unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    assert_eq!(qr.rows[1][0], Value::Integer(4));
    assert_eq!(qr.rows[2][0], Value::Integer(5));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn order_by_with_nulls_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', NULL)").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'b', 10)").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (3, 'c', 5)").unwrap();

    let qr = conn.query("SELECT id, num FROM t ORDER BY num ASC").unwrap();
    // NULLs first in ASC order
    assert_eq!(qr.rows[0][1], Value::Null);
    assert_eq!(qr.rows[1][1], Value::Integer(5));
    assert_eq!(qr.rows[2][1], Value::Integer(10));

    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Section 9: Aggregation in transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn count_star_in_empty_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(0));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn sum_avg_min_max_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'b', 20)").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (3, 'c', 30)").unwrap();

    let qr = conn.query("SELECT SUM(num), AVG(num), MIN(num), MAX(num) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(60));
    assert_eq!(qr.rows[0][2], Value::Integer(10));
    assert_eq!(qr.rows[0][3], Value::Integer(30));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn group_by_having_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, cat TEXT NOT NULL, qty INTEGER NOT NULL)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO sales (id, cat, qty) VALUES (1, 'A', 10)").unwrap();
    conn.execute("INSERT INTO sales (id, cat, qty) VALUES (2, 'A', 20)").unwrap();
    conn.execute("INSERT INTO sales (id, cat, qty) VALUES (3, 'B', 5)").unwrap();
    conn.execute("INSERT INTO sales (id, cat, qty) VALUES (4, 'B', 3)").unwrap();
    conn.execute("INSERT INTO sales (id, cat, qty) VALUES (5, 'C', 100)").unwrap();

    let qr = conn.query(
        "SELECT cat, SUM(qty) AS total FROM sales GROUP BY cat HAVING SUM(qty) > 10 ORDER BY cat"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("A".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(30));
    assert_eq!(qr.rows[1][0], Value::Text("C".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(100));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn aggregate_after_update_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'b', 20)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET num = 100 WHERE id = 1").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (3, 'c', 30)").unwrap();

    let qr = conn.query("SELECT SUM(num) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(150)); // 100 + 20 + 30
    conn.execute("COMMIT").unwrap();
}

#[test]
fn aggregate_after_delete_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'b', 20)").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (3, 'c', 30)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();

    let qr = conn.query("SELECT COUNT(*), SUM(num) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[0][1], Value::Integer(40)); // 10 + 30
    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Section 10: Persistence after commit/rollback
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn persistence_commit_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_table(&mut conn);

        conn.execute("BEGIN").unwrap();
        for i in 1..=100 {
            conn.execute(&format!(
                "INSERT INTO t (id, val, num) VALUES ({i}, 'row{i}', {i})"
            )).unwrap();
        }
        conn.execute("COMMIT").unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        assert_eq!(count_rows(&mut conn, "t"), 100);

        let qr = conn.query("SELECT SUM(num) FROM t").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(5050));
    }
}

#[test]
fn persistence_rollback_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_table(&mut conn);
        conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'kept', 1)").unwrap();

        conn.execute("BEGIN").unwrap();
        for i in 2..=50 {
            conn.execute(&format!(
                "INSERT INTO t (id, val, num) VALUES ({i}, 'gone', {i})"
            )).unwrap();
        }
        conn.execute("ROLLBACK").unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        assert_eq!(count_rows(&mut conn, "t"), 1);
    }
}

#[test]
fn persistence_ddl_commit_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        conn.execute("BEGIN").unwrap();
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY)").unwrap();
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY)").unwrap();
        conn.execute("INSERT INTO t1 (id) VALUES (1)").unwrap();
        conn.execute("INSERT INTO t2 (id) VALUES (2)").unwrap();
        conn.execute("COMMIT").unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        assert_eq!(count_rows(&mut conn, "t1"), 1);
        assert_eq!(count_rows(&mut conn, "t2"), 1);
    }
}

#[test]
fn persistence_ddl_rollback_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        conn.execute("BEGIN").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
        conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();
        conn.execute("ROLLBACK").unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let err = conn.query("SELECT * FROM t").unwrap_err();
        assert!(matches!(err, SqlError::TableNotFound(_)));
    }
}

#[test]
fn persistence_drop_rollback_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'survive')").unwrap();

        conn.execute("BEGIN").unwrap();
        conn.execute("DROP TABLE t").unwrap();
        conn.execute("ROLLBACK").unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT val FROM t").unwrap();
        assert_eq!(qr.rows.len(), 1);
        assert_eq!(qr.rows[0][0], Value::Text("survive".into()));
    }
}

#[test]
fn persistence_drop_without_commit_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        conn.execute("INSERT INTO t (id, val) VALUES (1, 'survive')").unwrap();

        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t (id, val) VALUES (2, 'gone')").unwrap();
        // Drop Connection without COMMIT or ROLLBACK
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        assert_eq!(count_rows(&mut conn, "t"), 1);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Section 11: Large data in transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn large_transaction_200_rows_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    for i in 0..200 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'row{i}', {})", i * 2
        )).unwrap();
    }
    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 200);
    let qr = conn.query("SELECT SUM(num) FROM t").unwrap();
    // Sum of 0,2,4,...,398 = 2 * (0+1+...+199) = 2 * 199*200/2 = 39800
    assert_eq!(qr.rows[0][0], Value::Integer(39800));
}

#[test]
fn large_transaction_200_rows_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    for i in 0..200 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})"
        )).unwrap();
    }
    conn.execute("ROLLBACK").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 0);
}

#[test]
fn large_update_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    for i in 0..100 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'old', {i})"
        )).unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET val = 'new', num = num + 1000").unwrap();

    let qr = conn.query("SELECT MIN(num), MAX(num) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1000));
    assert_eq!(qr.rows[0][1], Value::Integer(1099));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn large_delete_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    for i in 0..100 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})"
        )).unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM t WHERE num < 50").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 50);
    conn.execute("COMMIT").unwrap();
    assert_eq!(count_rows(&mut conn, "t"), 50);
}

// ═══════════════════════════════════════════════════════════════════════
// Section 12: CREATE/DROP cycles
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn create_drop_cycle_in_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    for i in 0..10 {
        conn.execute("BEGIN").unwrap();
        conn.execute("CREATE TABLE tmp (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        conn.execute(&format!("INSERT INTO tmp (id, val) VALUES ({i}, 'cycle')")).unwrap();
        conn.execute("COMMIT").unwrap();

        let qr = conn.query("SELECT * FROM tmp").unwrap();
        assert_eq!(qr.rows.len(), 1);

        conn.execute("BEGIN").unwrap();
        conn.execute("DROP TABLE tmp").unwrap();
        conn.execute("COMMIT").unwrap();

        assert!(conn.query("SELECT * FROM tmp").is_err());
    }
}

#[test]
fn create_drop_cycle_with_rollbacks() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // Create and commit
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    conn.execute("COMMIT").unwrap();

    // Try to drop but rollback
    conn.execute("BEGIN").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // Table should still exist
    assert_eq!(count_rows(&mut conn, "t"), 1);

    // Actually drop
    conn.execute("BEGIN").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("COMMIT").unwrap();

    // Create again
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'new')").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("new".into()));
}

// ═══════════════════════════════════════════════════════════════════════
// Section 13: Expression evaluation in transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn arithmetic_expressions_in_txn_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();

    let qr = conn.query("SELECT num + 5, num * 2, num - 3 FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(15));
    assert_eq!(qr.rows[0][1], Value::Integer(20));
    assert_eq!(qr.rows[0][2], Value::Integer(7));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn update_with_expression_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET num = num + 5 WHERE id = 1").unwrap();

    let qr = conn.query("SELECT num FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(15));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn complex_where_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    for i in 1..=20 {
        let val = if i % 2 == 0 { "even" } else { "odd" };
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, '{val}', {i})"
        )).unwrap();
    }

    let qr = conn.query("SELECT COUNT(*) FROM t WHERE val = 'even' AND num > 10").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5)); // 12, 14, 16, 18, 20

    let qr = conn.query("SELECT COUNT(*) FROM t WHERE val = 'odd' OR num > 18").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(11)); // 10 odd + 20(even but >18)

    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Section 14: Schema consistency edge cases
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn schema_consistent_after_create_rollback_create_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // Create with schema A, rollback
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // Create with schema B, commit
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)").unwrap();
    conn.execute("INSERT INTO t (id, value) VALUES (1, 42)").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT value FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(42));

    // Name column should not exist (schema B doesn't have it)
    let err = conn.query("SELECT name FROM t").unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)));
}

#[test]
fn tables_list_consistent_after_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE existing (id INTEGER PRIMARY KEY)").unwrap();

    let tables_before: Vec<String> = conn.tables().iter().map(|s| s.to_string()).collect();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE tmp1 (id INTEGER PRIMARY KEY)").unwrap();
    conn.execute("CREATE TABLE tmp2 (id INTEGER PRIMARY KEY)").unwrap();
    conn.execute("ROLLBACK").unwrap();

    let tables_after: Vec<String> = conn.tables().iter().map(|s| s.to_string()).collect();
    assert_eq!(tables_before, tables_after);
}

#[test]
fn schema_reloaded_after_rollback_with_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'data')").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    // During the transaction, t is gone from schema
    assert!(conn.query("SELECT * FROM t").is_err());
    conn.execute("ROLLBACK").unwrap();

    // After rollback, schema should be reloaded and t should be back
    let qr = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("data".into()));
}

// ═══════════════════════════════════════════════════════════════════════
// Section 15: UPDATE edge cases in transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn update_pk_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET id = 100 WHERE id = 1").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(100));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn update_pk_conflict_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'b', 20)").unwrap();

    conn.execute("BEGIN").unwrap();
    let err = conn.execute("UPDATE t SET id = 2 WHERE id = 1").unwrap_err();
    assert!(matches!(err, SqlError::DuplicateKey));
    assert!(conn.in_transaction());
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn update_no_match_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();

    conn.execute("BEGIN").unwrap();
    let result = conn.execute("UPDATE t SET val = 'x' WHERE id = 999").unwrap();
    assert!(matches!(result, ExecutionResult::RowsAffected(0)));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn update_all_rows_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    for i in 1..=5 {
        conn.execute(&format!("INSERT INTO t (id, val, num) VALUES ({i}, 'old', {i})")).unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET val = 'new'").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM t WHERE val = 'new'").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5));
    let qr = conn.query("SELECT COUNT(*) FROM t WHERE val = 'old'").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(0));

    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Section 16: DELETE edge cases in transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn delete_all_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})")).unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM t").unwrap();
    assert_eq!(count_rows(&mut conn, "t"), 0);
    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 0);
}

#[test]
fn delete_all_then_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})")).unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM t").unwrap();
    assert_eq!(count_rows(&mut conn, "t"), 0);
    conn.execute("ROLLBACK").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 10);
}

#[test]
fn delete_no_match_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 10)").unwrap();

    conn.execute("BEGIN").unwrap();
    let result = conn.execute("DELETE FROM t WHERE id = 999").unwrap();
    assert!(matches!(result, ExecutionResult::RowsAffected(0)));
    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "t"), 1);
}

// ═══════════════════════════════════════════════════════════════════════
// Section 17: Mixed auto-commit and explicit transaction
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn auto_commit_inserts_visible_in_next_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'auto', 1)").unwrap();

    conn.execute("BEGIN").unwrap();
    let qr = conn.query("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("auto".into()));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn committed_txn_visible_in_auto_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'explicit', 1)").unwrap();
    conn.execute("COMMIT").unwrap();

    // Auto-commit select should see it
    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn rolled_back_txn_not_visible_in_auto_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'gone', 1)").unwrap();
    conn.execute("ROLLBACK").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 0);
}

// ═══════════════════════════════════════════════════════════════════════
// Section 18: in_transaction() state tracking
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn in_transaction_state_tracking() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert!(!conn.in_transaction());

    conn.execute("BEGIN").unwrap();
    assert!(conn.in_transaction());

    conn.execute("COMMIT").unwrap();
    assert!(!conn.in_transaction());

    conn.execute("BEGIN").unwrap();
    assert!(conn.in_transaction());

    conn.execute("ROLLBACK").unwrap();
    assert!(!conn.in_transaction());
}

#[test]
fn in_transaction_after_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert!(conn.in_transaction());

    // Error should not change state
    assert!(conn.execute("INSERT INTO nonexistent (id) VALUES (1)").is_err());
    assert!(conn.in_transaction());

    conn.execute("ROLLBACK").unwrap();
    assert!(!conn.in_transaction());
}

// ═══════════════════════════════════════════════════════════════════════
// Section 19: Composite primary key in transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn composite_pk_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (a INTEGER, b TEXT, val TEXT, PRIMARY KEY (a, b))"
    ).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (a, b, val) VALUES (1, 'x', 'first')").unwrap();
    conn.execute("INSERT INTO t (a, b, val) VALUES (1, 'y', 'second')").unwrap();
    conn.execute("INSERT INTO t (a, b, val) VALUES (2, 'x', 'third')").unwrap();

    let qr = conn.query("SELECT * FROM t ORDER BY a, b").unwrap();
    assert_eq!(qr.rows.len(), 3);

    // Duplicate composite PK should fail
    let err = conn.execute("INSERT INTO t (a, b, val) VALUES (1, 'x', 'dup')").unwrap_err();
    assert!(matches!(err, SqlError::DuplicateKey));

    conn.execute("COMMIT").unwrap();
}

#[test]
fn composite_pk_update_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (a INTEGER, b TEXT, val TEXT, PRIMARY KEY (a, b))"
    ).unwrap();
    conn.execute("INSERT INTO t (a, b, val) VALUES (1, 'x', 'original')").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET b = 'y' WHERE a = 1 AND b = 'x'").unwrap();

    let qr = conn.query("SELECT a, b, val FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("y".into()));

    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Section 20: Stress scenarios
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn stress_50_sequential_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    for i in 0..50 {
        conn.execute("BEGIN").unwrap();
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'stress', {i})"
        )).unwrap();
        conn.execute("COMMIT").unwrap();
    }

    assert_eq!(count_rows(&mut conn, "t"), 50);
}

#[test]
fn stress_rollback_heavy() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    let mut committed = 0;
    for i in 0..100 {
        conn.execute("BEGIN").unwrap();
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})"
        )).unwrap();
        if i % 5 == 0 {
            conn.execute("COMMIT").unwrap();
            committed += 1;
        } else {
            conn.execute("ROLLBACK").unwrap();
        }
    }

    assert_eq!(count_rows(&mut conn, "t"), committed);
}

#[test]
fn stress_mixed_operations_in_large_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_table(&mut conn);

    // Pre-populate
    for i in 0..50 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'pre', {i})"
        )).unwrap();
    }

    conn.execute("BEGIN").unwrap();

    // Insert more
    for i in 50..100 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'new', {i})"
        )).unwrap();
    }

    // Update some
    conn.execute("UPDATE t SET val = 'updated' WHERE num < 25").unwrap();

    // Delete some
    conn.execute("DELETE FROM t WHERE num >= 75").unwrap();

    // Verify intermediate state
    assert_eq!(count_rows(&mut conn, "t"), 75); // 100 - 25 (75..99)

    let qr = conn.query("SELECT COUNT(*) FROM t WHERE val = 'updated'").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(25));

    conn.execute("COMMIT").unwrap();

    // Verify final state
    assert_eq!(count_rows(&mut conn, "t"), 75);
}

#[test]
fn stress_create_drop_10_tables_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    for i in 0..10 {
        conn.execute(&format!("CREATE TABLE t{i} (id INTEGER PRIMARY KEY, val TEXT)")).unwrap();
        conn.execute(&format!("INSERT INTO t{i} (id, val) VALUES (1, 'data{i}')")).unwrap();
    }
    conn.execute("COMMIT").unwrap();

    for i in 0..10 {
        assert_eq!(count_rows(&mut conn, &format!("t{i}")), 1);
    }

    // Drop all in one transaction
    conn.execute("BEGIN").unwrap();
    for i in 0..10 {
        conn.execute(&format!("DROP TABLE t{i}")).unwrap();
    }
    conn.execute("COMMIT").unwrap();

    for i in 0..10 {
        assert!(conn.query(&format!("SELECT * FROM t{i}")).is_err());
    }
}

#[test]
fn stress_alternating_tables_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

    conn.execute("BEGIN").unwrap();
    for i in 0..50 {
        if i % 2 == 0 {
            conn.execute(&format!("INSERT INTO a (id, val) VALUES ({i}, {i})")).unwrap();
        } else {
            conn.execute(&format!("INSERT INTO b (id, val) VALUES ({i}, {i})")).unwrap();
        }
    }
    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "a"), 25);
    assert_eq!(count_rows(&mut conn, "b"), 25);
}
