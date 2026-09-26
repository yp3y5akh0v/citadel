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

fn assert_rows(result: &ExecutionResult, expected: u64) {
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(*n, expected),
        other => panic!("expected RowsAffected({expected}), got {other:?}"),
    }
}

fn assert_ok(result: &ExecutionResult) {
    assert!(
        matches!(result, ExecutionResult::Ok),
        "expected Ok, got {result:?}"
    );
}

fn assert_query_rows(result: &ExecutionResult, expected_rows: usize) {
    match result {
        ExecutionResult::Query(qr) => assert_eq!(qr.rows.len(), expected_rows),
        other => panic!("expected Query, got {other:?}"),
    }
}

#[test]
fn script_single_statement() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    assert_eq!(exec.completed.len(), 1);
    assert_ok(&exec.completed[0]);
    assert!(exec.error.is_none());
}

#[test]
fn script_multi_ddl_dml_select() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO t VALUES (1, 'a');
         INSERT INTO t VALUES (2, 'b');
         SELECT * FROM t ORDER BY id",
    );
    assert_eq!(exec.completed.len(), 4);
    assert_ok(&exec.completed[0]);
    assert_rows(&exec.completed[1], 1);
    assert_rows(&exec.completed[2], 1);
    assert_query_rows(&exec.completed[3], 2);
    assert!(exec.error.is_none());
}

#[test]
fn script_trailing_semicolon() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    let exec = conn.execute_script("SELECT * FROM t;");
    assert_eq!(exec.completed.len(), 1);
    assert_query_rows(&exec.completed[0], 0);
    assert!(exec.error.is_none());
}

#[test]
fn script_empty_returns_parse_error() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script("");
    assert!(exec.completed.is_empty());
    assert!(matches!(exec.error, Some(SqlError::Parse(_))));
}

#[test]
fn script_parse_error_whole_script() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script("SELECT 1; GARBAGE;");
    assert!(exec.completed.is_empty());
    assert!(matches!(exec.error, Some(SqlError::Parse(_))));
}

#[test]
fn script_runtime_error_mid_script() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");
    let exec = conn.execute_script("INSERT INTO t VALUES (1, 10); INSERT INTO t VALUES (1, 20)");
    assert_eq!(exec.completed.len(), 1);
    assert_rows(&exec.completed[0], 1);
    assert!(exec.error.is_some());

    let verify = conn.execute_script("SELECT v FROM t WHERE id = 1");
    assert_query_rows(&verify.completed[0], 1);
}

#[test]
fn script_with_comments() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script("-- before\nSELECT 1;\n-- between\nSELECT 2;\n-- after");
    assert_eq!(exec.completed.len(), 2);
    assert!(exec.error.is_none());
}

#[test]
fn script_semicolon_in_string() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT NOT NULL)");
    let exec = conn.execute_script("INSERT INTO t VALUES (1, 'a;b;c')");
    assert_eq!(exec.completed.len(), 1);
    assert_rows(&exec.completed[0], 1);

    let q = conn.execute_script("SELECT s FROM t WHERE id = 1");
    match &q.completed[0] {
        ExecutionResult::Query(qr) => {
            assert_eq!(qr.rows.len(), 1);
            match &qr.rows[0][0] {
                Value::Text(s) => assert_eq!(s.as_str(), "a;b;c"),
                other => panic!("expected Text, got {other:?}"),
            }
        }
        other => panic!("expected Query, got {other:?}"),
    }
}

#[test]
fn script_transaction_spans_script() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    let exec =
        conn.execute_script("BEGIN; INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); COMMIT;");
    assert_eq!(exec.completed.len(), 4);
    assert!(exec.error.is_none());
    assert!(!conn.in_transaction());

    let q = conn.execute_script("SELECT id FROM t ORDER BY id");
    assert_query_rows(&q.completed[0], 2);
}

#[test]
fn script_savepoint_in_script() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    let exec = conn.execute_script(
        "BEGIN; \
         SAVEPOINT sp1; \
         INSERT INTO t VALUES (1); \
         ROLLBACK TO sp1; \
         COMMIT;",
    );
    assert_eq!(exec.completed.len(), 5);
    assert!(exec.error.is_none());

    let q = conn.execute_script("SELECT COUNT(*) FROM t");
    match &q.completed[0] {
        ExecutionResult::Query(qr) => match &qr.rows[0][0] {
            Value::Integer(n) => assert_eq!(*n, 0),
            other => panic!("expected Integer, got {other:?}"),
        },
        other => panic!("expected Query, got {other:?}"),
    }
}

#[test]
fn script_error_in_explicit_txn_leaves_txn_open() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");
    let exec =
        conn.execute_script("BEGIN; INSERT INTO t VALUES (1, 10); INSERT INTO t VALUES (1, 20);");
    assert_eq!(exec.completed.len(), 2);
    assert!(exec.error.is_some());
    assert!(conn.in_transaction());

    let _ = conn.execute_script("ROLLBACK");
}
