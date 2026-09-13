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
fn add_nullable_column_to_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(conn.execute("ALTER TABLE t ADD COLUMN val TEXT").unwrap());

    conn.execute("INSERT INTO t (id, val) VALUES (1, 'hello')")
        .unwrap();
    let qr = conn.query("SELECT id, val FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("hello".into()));
}

#[test]
fn add_nullable_column_to_nonempty_table_old_rows_get_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    conn.execute("INSERT INTO t (id) VALUES (2)").unwrap();

    assert_ok(conn.execute("ALTER TABLE t ADD COLUMN val TEXT").unwrap());

    let qr = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][1], Value::Null);
    assert_eq!(qr.rows[1][1], Value::Null);
}

#[test]
fn add_column_with_default_old_rows_get_default() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    conn.execute("INSERT INTO t (id) VALUES (2)").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 42")
            .unwrap(),
    );

    let qr = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows[0][1], Value::Integer(42));
    assert_eq!(qr.rows[1][1], Value::Integer(42));
}

#[test]
fn add_not_null_with_default_to_nonempty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN val INTEGER NOT NULL DEFAULT 99")
            .unwrap(),
    );

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(99));
}

#[test]
fn add_not_null_without_default_to_nonempty_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    let err = conn
        .execute("ALTER TABLE t ADD COLUMN val INTEGER NOT NULL")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported(ref msg) if msg.contains("NOT NULL")),
        "got: {err:?}"
    );
}

#[test]
fn add_not_null_without_default_to_empty_ok() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN val INTEGER NOT NULL")
            .unwrap(),
    );

    conn.execute("INSERT INTO t (id, val) VALUES (1, 10)")
        .unwrap();
    let qr = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(10));
}

#[test]
fn add_duplicate_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    let err = conn
        .execute("ALTER TABLE t ADD COLUMN val TEXT")
        .unwrap_err();
    assert!(matches!(err, SqlError::DuplicateColumn(_)), "got: {err:?}");
}

#[test]
fn add_column_if_not_exists_on_existing_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN IF NOT EXISTS val TEXT")
            .unwrap(),
    );
}

#[test]
fn add_column_to_nonexistent_table_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    let err = conn
        .execute("ALTER TABLE missing ADD COLUMN val TEXT")
        .unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)), "got: {err:?}");
}

#[test]
fn add_column_select_star_returns_new_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN val TEXT DEFAULT 'hi'")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.columns.len(), 2);
    assert_eq!(qr.rows[0][1], Value::Text("hi".into()));

    conn.execute("INSERT INTO t (id, val) VALUES (2, 'world')")
        .unwrap();
    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows[1][1], Value::Text("world".into()));
}

#[test]
fn add_column_with_check_constraint() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN val INTEGER CHECK (val > 0)")
            .unwrap(),
    );

    conn.execute("INSERT INTO t (id, val) VALUES (1, 5)")
        .unwrap();
    let err = conn
        .execute("INSERT INTO t (id, val) VALUES (2, -1)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(_)), "got: {err:?}");
}

#[test]
fn add_column_persistence_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        assert_ok(
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .unwrap(),
        );
        conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();
        assert_ok(
            conn.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99")
                .unwrap(),
        );
    }
    {
        let db = open_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(99));

        conn.execute("INSERT INTO t (id, val) VALUES (2, 50)")
            .unwrap();
        let qr = conn.query("SELECT val FROM t WHERE id = 2").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(50));
    }
}

#[test]
fn add_column_in_txn_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("BEGIN").unwrap();
    assert_ok(conn.execute("ALTER TABLE t ADD COLUMN val TEXT").unwrap());
    conn.execute("ROLLBACK").unwrap();

    let err = conn
        .execute("INSERT INTO t (id, val) VALUES (1, 'x')")
        .unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)), "got: {err:?}");
}

#[test]
fn add_primary_key_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    let err = conn
        .execute("ALTER TABLE t ADD COLUMN pk INTEGER PRIMARY KEY")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported(ref msg) if msg.contains("PRIMARY KEY")),
        "got: {err:?}"
    );
}

#[test]
fn drop_non_pk_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t (id, a, b) VALUES (1, 'hello', 10)")
        .unwrap();
    conn.execute("INSERT INTO t (id, a, b) VALUES (2, 'world', 20)")
        .unwrap();

    assert_ok(conn.execute("ALTER TABLE t DROP COLUMN a").unwrap());

    let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(qr.columns.len(), 2);
    assert_eq!(qr.columns[0], "id");
    assert_eq!(qr.columns[1], "b");
    assert_eq!(qr.rows[0][1], Value::Integer(10));
    assert_eq!(qr.rows[1][1], Value::Integer(20));
}

#[test]
fn drop_column_positions_compacted() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'x', 10, 1.5)")
        .unwrap();

    assert_ok(conn.execute("ALTER TABLE t DROP COLUMN b").unwrap());

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.columns, vec!["id", "a", "c"]);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("x".into()));
    assert_eq!(qr.rows[0][2], Value::Real(1.5));
}

#[test]
fn drop_pk_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    let err = conn.execute("ALTER TABLE t DROP COLUMN id").unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported(ref msg) if msg.contains("primary key")),
        "got: {err:?}"
    );
}

#[test]
fn drop_indexed_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_val ON t (val)").unwrap());

    let err = conn.execute("ALTER TABLE t DROP COLUMN val").unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported(ref msg) if msg.contains("indexed")),
        "got: {err:?}"
    );
}

#[test]
fn drop_fk_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
        )
        .unwrap(),
    );

    // FK auto-index is created, so "indexed" check fires first
    let err = conn
        .execute("ALTER TABLE child DROP COLUMN pid")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported(ref msg) if msg.contains("indexed") || msg.contains("foreign key")),
        "got: {err:?}"
    );
}

#[test]
fn drop_column_referenced_by_check_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, CHECK (a + b > 0))",
        )
        .unwrap(),
    );

    let err = conn.execute("ALTER TABLE t DROP COLUMN a").unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported(ref msg) if msg.contains("CHECK")),
        "got: {err:?}"
    );
}

#[test]
fn drop_nonexistent_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    let err = conn
        .execute("ALTER TABLE t DROP COLUMN missing")
        .unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)), "got: {err:?}");
}

#[test]
fn drop_column_if_exists_nonexistent_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("ALTER TABLE t DROP COLUMN IF EXISTS missing")
            .unwrap(),
    );
}

#[test]
fn drop_column_from_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    assert_ok(conn.execute("ALTER TABLE t DROP COLUMN val").unwrap());

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.columns, vec!["id"]);
}

#[test]
fn drop_column_indexes_on_remaining_columns_still_work() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_c ON t (c)").unwrap());

    conn.execute("INSERT INTO t VALUES (1, 'x', 10, 1.5)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'y', 20, 2.5)")
        .unwrap();

    assert_ok(conn.execute("ALTER TABLE t DROP COLUMN a").unwrap());

    // Index on c should still work for lookups
    let qr = conn.query("SELECT id, c FROM t WHERE c = 2.5").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn drop_column_persistence_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        assert_ok(
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)")
                .unwrap(),
        );
        conn.execute("INSERT INTO t VALUES (1, 'hello', 42)")
            .unwrap();
        assert_ok(conn.execute("ALTER TABLE t DROP COLUMN a").unwrap());
    }
    {
        let db = open_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT * FROM t").unwrap();
        assert_eq!(qr.columns, vec!["id", "b"]);
        assert_eq!(qr.rows[0][1], Value::Integer(42));
    }
}

#[test]
fn drop_column_in_txn_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'hi')").unwrap();

    conn.execute("BEGIN").unwrap();
    assert_ok(conn.execute("ALTER TABLE t DROP COLUMN val").unwrap());
    conn.execute("ROLLBACK").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("hi".into()));
}

#[test]
fn drop_column_with_check_and_default() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER DEFAULT 10 CHECK (a > 0), b TEXT)",
        )
        .unwrap(),
    );
    conn.execute("INSERT INTO t (id, b) VALUES (1, 'x')")
        .unwrap();

    assert_ok(conn.execute("ALTER TABLE t DROP COLUMN a").unwrap());

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.columns, vec!["id", "b"]);
    assert_eq!(qr.rows[0][1], Value::Text("x".into()));

    conn.execute("INSERT INTO t (id, b) VALUES (2, 'y')")
        .unwrap();
}

#[test]
fn drop_column_then_insert_correct_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL)")
            .unwrap(),
    );
    assert_ok(conn.execute("ALTER TABLE t DROP COLUMN b").unwrap());

    conn.execute("INSERT INTO t (id, a, c) VALUES (1, 'hello', 1.5)")
        .unwrap();
    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.columns, vec!["id", "a", "c"]);
    assert_eq!(qr.rows[0][2], Value::Real(1.5));
}

#[test]
fn rename_column_select_with_new_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, old_name TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE t RENAME COLUMN old_name TO new_name")
            .unwrap(),
    );

    let qr = conn.query("SELECT new_name FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
}

#[test]
fn rename_column_old_name_no_longer_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, old_name TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE t RENAME COLUMN old_name TO new_name")
            .unwrap(),
    );

    let err = conn.query("SELECT old_name FROM t").unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)), "got: {err:?}");
}

#[test]
fn rename_indexed_column_index_still_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_val ON t (val)").unwrap());

    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 200)").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE t RENAME COLUMN val TO amount")
            .unwrap(),
    );

    let qr = conn.query("SELECT id FROM t WHERE amount = 200").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn rename_to_existing_name_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)")
            .unwrap(),
    );

    let err = conn
        .execute("ALTER TABLE t RENAME COLUMN a TO b")
        .unwrap_err();
    assert!(matches!(err, SqlError::DuplicateColumn(_)), "got: {err:?}");
}

#[test]
fn rename_nonexistent_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    let err = conn
        .execute("ALTER TABLE t RENAME COLUMN missing TO new_name")
        .unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)), "got: {err:?}");
}

#[test]
fn rename_column_persistence_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        assert_ok(
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .unwrap(),
        );
        conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
        assert_ok(
            conn.execute("ALTER TABLE t RENAME COLUMN val TO name")
                .unwrap(),
        );
    }
    {
        let db = open_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
    }
}

#[test]
fn rename_fk_column_fk_still_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
        )
        .unwrap(),
    );
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (1, 1)").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE child RENAME COLUMN pid TO parent_id")
            .unwrap(),
    );

    let err = conn
        .execute("INSERT INTO child VALUES (2, 999)")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::ForeignKeyViolation(_)),
        "got: {err:?}"
    );
}

#[test]
fn rename_column_in_txn_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    assert_ok(
        conn.execute("ALTER TABLE t RENAME COLUMN val TO new_val")
            .unwrap(),
    );
    conn.execute("ROLLBACK").unwrap();

    conn.execute("INSERT INTO t (id, val) VALUES (1, 'hi')")
        .unwrap();
    let qr = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("hi".into()));
}

#[test]
fn rename_table_old_errors_new_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE old_t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO old_t VALUES (1, 'hello')")
        .unwrap();

    assert_ok(conn.execute("ALTER TABLE old_t RENAME TO new_t").unwrap());

    let err = conn.query("SELECT * FROM old_t").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)), "got: {err:?}");

    let qr = conn.query("SELECT * FROM new_t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("hello".into()));
}

#[test]
fn rename_table_data_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 10))
            .unwrap();
    }

    assert_ok(conn.execute("ALTER TABLE t RENAME TO t2").unwrap());

    let qr = conn.query("SELECT COUNT(*) FROM t2").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(10));

    let qr = conn.query("SELECT SUM(val) FROM t2").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(550));
}

#[test]
fn rename_table_indexes_work() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_val ON t (val)").unwrap());

    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 200)").unwrap();

    assert_ok(conn.execute("ALTER TABLE t RENAME TO t2").unwrap());

    let qr = conn.query("SELECT id FROM t2 WHERE val = 200").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn rename_to_existing_table_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );

    let err = conn.execute("ALTER TABLE t1 RENAME TO t2").unwrap_err();
    assert!(
        matches!(err, SqlError::TableAlreadyExists(_)),
        "got: {err:?}"
    );
}

#[test]
fn rename_nonexistent_table_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    let err = conn
        .execute("ALTER TABLE missing RENAME TO new_name")
        .unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)), "got: {err:?}");
}

#[test]
fn rename_table_fks_in_other_tables_updated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
        )
        .unwrap(),
    );
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (1, 1)").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE parent RENAME TO parent2")
            .unwrap(),
    );

    // FK should still be enforced - child references parent2 now
    let err = conn
        .execute("INSERT INTO child VALUES (2, 999)")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::ForeignKeyViolation(_)),
        "got: {err:?}"
    );

    conn.execute("INSERT INTO parent2 VALUES (2)").unwrap();
    conn.execute("INSERT INTO child VALUES (2, 2)").unwrap();
}

#[test]
fn rename_table_persistence_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        assert_ok(
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .unwrap(),
        );
        conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
        assert_ok(conn.execute("ALTER TABLE t RENAME TO t_renamed").unwrap());
    }
    {
        let db = open_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        let qr = conn
            .query("SELECT val FROM t_renamed WHERE id = 1")
            .unwrap();
        assert_eq!(qr.rows[0][0], Value::Text("hello".into()));

        let err = conn.query("SELECT * FROM t").unwrap_err();
        assert!(matches!(err, SqlError::TableNotFound(_)));
    }
}

#[test]
fn rename_table_in_txn_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'hi')").unwrap();

    conn.execute("BEGIN").unwrap();
    assert_ok(conn.execute("ALTER TABLE t RENAME TO t_new").unwrap());
    conn.execute("ROLLBACK").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("hi".into()));
}

#[test]
fn rename_table_with_self_referencing_fk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES t(id))")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 1)").unwrap();

    assert_ok(conn.execute("ALTER TABLE t RENAME TO tree").unwrap());

    let err = conn
        .execute("INSERT INTO tree VALUES (3, 999)")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::ForeignKeyViolation(_)),
        "got: {err:?}"
    );

    conn.execute("INSERT INTO tree VALUES (3, 2)").unwrap();
}

#[test]
fn explain_alter_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap(),
    );

    let qr = conn
        .query("EXPLAIN ALTER TABLE t ADD COLUMN x INTEGER")
        .unwrap();
    let line = match &qr.rows[0][0] {
        Value::Text(s) => s.to_string(),
        other => panic!("expected text, got {other:?}"),
    };
    assert!(line.contains("ADD COLUMN"), "got: {line}");

    let qr = conn.query("EXPLAIN ALTER TABLE t RENAME TO t2").unwrap();
    let line = match &qr.rows[0][0] {
        Value::Text(s) => s.to_string(),
        other => panic!("expected text, got {other:?}"),
    };
    assert!(line.contains("RENAME TO"), "got: {line}");
}

#[test]
fn add_then_drop_same_column_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN temp INTEGER DEFAULT 5")
            .unwrap(),
    );
    let qr = conn.query("SELECT temp FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5));

    assert_ok(conn.execute("ALTER TABLE t DROP COLUMN temp").unwrap());
    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.columns, vec!["id"]);
}

#[test]
fn chained_in_txn_create_add_insert_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN val TEXT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.columns, vec!["id", "val"]);

    conn.execute("ALTER TABLE t DROP COLUMN val").unwrap();
    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.columns, vec!["id"]);

    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.columns, vec!["id"]);
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn multiple_sequential_add_columns_all_defaults_correct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN a INTEGER DEFAULT 10")
            .unwrap(),
    );
    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN b TEXT DEFAULT 'hi'")
            .unwrap(),
    );
    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN c REAL DEFAULT 3.5")
            .unwrap(),
    );

    let qr = conn.query("SELECT a, b, c FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(10));
    assert_eq!(qr.rows[0][1], Value::Text("hi".into()));
    assert_eq!(qr.rows[0][2], Value::Real(3.5));
}

#[test]
fn add_column_with_default_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT (2 + 3)")
            .unwrap(),
    );

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn large_table_drop_column_all_rows_rewritten() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)")
            .unwrap(),
    );
    for i in 1..=1000 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {}, 'row{i}')", i * 10))
            .unwrap();
    }

    assert_ok(conn.execute("ALTER TABLE t DROP COLUMN b").unwrap());

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1000));

    let qr = conn.query("SELECT a FROM t WHERE id = 500").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5000));
}

#[test]
fn add_column_sum_avg_count_on_new_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    for i in 1..=5 {
        conn.execute(&format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 10")
            .unwrap(),
    );

    let qr = conn.query("SELECT SUM(val) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(50));

    let qr = conn.query("SELECT COUNT(val) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn add_column_where_on_new_column_integer_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    for i in 1..=5 {
        conn.execute(&format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 10")
            .unwrap(),
    );

    let qr = conn.query("SELECT id FROM t WHERE val = 10").unwrap();
    assert_eq!(qr.rows.len(), 5);

    let qr = conn.query("SELECT id FROM t WHERE val > 5").unwrap();
    assert_eq!(qr.rows.len(), 5);

    let qr = conn.query("SELECT id FROM t WHERE val < 5").unwrap();
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn add_column_select_projection_only_new_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'y')").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN b INTEGER DEFAULT 42")
            .unwrap(),
    );

    let qr = conn.query("SELECT b FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(42));
    assert_eq!(qr.rows[1][0], Value::Integer(42));
}

#[test]
fn add_column_join_using_new_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 VALUES (1)").unwrap();
    conn.execute("INSERT INTO t2 VALUES (1, 100)").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE t1 ADD COLUMN join_key INTEGER DEFAULT 1")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT t1.id, t2.val FROM t1 JOIN t2 ON t1.join_key = t2.id")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Integer(100));
}

#[test]
fn add_column_with_default_then_update_old_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 10")
            .unwrap(),
    );

    // Read default
    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(10));

    // Update materializes the value
    assert_rows_affected(
        conn.execute("UPDATE t SET val = 99 WHERE id = 1").unwrap(),
        1,
    );

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(99));
}

fn for_added_default_update_modes(setup: &[&str], check: impl Fn(&Connection<'_>, bool)) {
    for prepared in [true, false] {
        for explicit_transaction in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            for sql in setup {
                conn.execute(sql).unwrap();
            }
            if explicit_transaction {
                conn.execute("BEGIN").unwrap();
            }
            check(&conn, prepared);
            if explicit_transaction {
                conn.execute("COMMIT").unwrap();
            }
        }
    }
}

fn execute_added_default_update(conn: &Connection<'_>, prepared: bool, sql: &str, count: u64) {
    if prepared {
        assert_eq!(conn.prepare(sql).unwrap().execute(&[]).unwrap(), count);
    } else {
        assert_rows_affected(conn.execute(sql).unwrap(), count);
    }
}

#[test]
fn update_added_default_target_reads_missing_but_preserves_stored_null() {
    for_added_default_update_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY)",
            "INSERT INTO t VALUES (1), (2)",
            "ALTER TABLE t ADD COLUMN v INTEGER DEFAULT 10",
            "INSERT INTO t VALUES (3, NULL), (4, 30)",
        ],
        |conn, prepared| {
            execute_added_default_update(
                conn,
                prepared,
                "UPDATE t SET v = v + 1 WHERE id >= 1 AND id <= 4",
                4,
            );
            assert_eq!(
                conn.query("SELECT v FROM t ORDER BY id").unwrap().rows,
                vec![
                    vec![Value::Integer(11)],
                    vec![Value::Integer(11)],
                    vec![Value::Null],
                    vec![Value::Integer(31)],
                ]
            );
        },
    );
}

#[test]
fn update_added_default_rhs_and_simultaneous_assignments_use_old_values() {
    for_added_default_update_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL)",
            "INSERT INTO t VALUES (1, 3), (2, 4)",
            "ALTER TABLE t ADD COLUMN b INTEGER NOT NULL DEFAULT (2 * 5)",
        ],
        |conn, prepared| {
            execute_added_default_update(conn, prepared, "UPDATE t SET a = b + 1, b = a + 2", 2);
            assert_eq!(
                conn.query("SELECT a, b FROM t ORDER BY id").unwrap().rows,
                vec![
                    vec![Value::Integer(11), Value::Integer(5)],
                    vec![Value::Integer(11), Value::Integer(6)],
                ]
            );
        },
    );
}

#[test]
fn update_added_default_fixed_width_range_mixes_short_and_full_rows() {
    for_added_default_update_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL)",
            "INSERT INTO t VALUES (2, 20), (4, 40)",
            "ALTER TABLE t ADD COLUMN b INTEGER NOT NULL DEFAULT 10",
            "ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'untouched'",
            "INSERT INTO t VALUES (1, 10, 3, 'first'), (3, 30, 7, NULL)",
        ],
        |conn, prepared| {
            // A fixed-width SET target permits the fused range path, but the
            // alternating old rows must grow to store their RHS and untouched defaults.
            execute_added_default_update(
                conn,
                prepared,
                "UPDATE t SET a = a + b WHERE id >= 1 AND id <= 4",
                4,
            );
            assert_eq!(
                conn.query("SELECT * FROM t ORDER BY id").unwrap().rows,
                vec![
                    vec![
                        Value::Integer(1),
                        Value::Integer(13),
                        Value::Integer(3),
                        Value::Text("first".into())
                    ],
                    vec![
                        Value::Integer(2),
                        Value::Integer(30),
                        Value::Integer(10),
                        Value::Text("untouched".into())
                    ],
                    vec![
                        Value::Integer(3),
                        Value::Integer(37),
                        Value::Integer(7),
                        Value::Null
                    ],
                    vec![
                        Value::Integer(4),
                        Value::Integer(50),
                        Value::Integer(10),
                        Value::Text("untouched".into())
                    ],
                ]
            );
        },
    );
}

#[test]
fn update_added_default_cross_type_storage_rewrites_preserve_neighbors() {
    for default_sql in ["TRUE", "7.0", "(7 + 0)", "0"] {
        let add_default =
            format!("ALTER TABLE t ADD COLUMN d INTEGER NOT NULL DEFAULT {default_sql}");
        for_added_default_update_modes(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER NOT NULL, label TEXT)",
                "INSERT INTO t VALUES (1, 10, 'first'), (2, 20, 'second')",
                &add_default,
                "ALTER TABLE t ADD COLUMN sentinel INTEGER NOT NULL DEFAULT 42",
            ],
            |conn, prepared| {
                // Defaults normalize through INSERT's existing coercion policy.
                // Raw legacy TEXT-in-INTEGER neighbor coverage lives in
                // write_tests::legacy_cross_type_stored_update_rewrites_preserve_neighbors.
                execute_added_default_update(conn, prepared, "UPDATE t SET d = 9 WHERE id = 1", 1);
                let first = vec![
                    Value::Integer(1),
                    Value::Integer(10),
                    Value::Text("first".into()),
                    Value::Integer(9),
                    Value::Integer(42),
                ];
                assert_eq!(
                    conn.query("SELECT * FROM t WHERE id = 1").unwrap().rows,
                    vec![first.clone()],
                    "direct PK update with DEFAULT {default_sql}"
                );

                // Materialize d without assigning it, then exercise a fixed-width
                // range assignment over the resulting full stored row.
                execute_added_default_update(
                    conn,
                    prepared,
                    "UPDATE t SET x = x + 1 WHERE id = 2",
                    1,
                );
                execute_added_default_update(
                    conn,
                    prepared,
                    "UPDATE t SET d = 9 WHERE id >= 2 AND id <= 3",
                    1,
                );
                assert_eq!(
                    conn.query("SELECT * FROM t ORDER BY id").unwrap().rows,
                    vec![
                        first,
                        vec![
                            Value::Integer(2),
                            Value::Integer(21),
                            Value::Text("second".into()),
                            Value::Integer(9),
                            Value::Integer(42),
                        ],
                    ],
                    "materialized range update with DEFAULT {default_sql}"
                );
            },
        );
    }
}

#[test]
fn update_added_default_range_error_cannot_publish_a_full_row_prefix() {
    for prepared in [true, false] {
        for explicit_transaction in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let original_first = vec![vec![Value::Integer(1), Value::Integer(10), Value::Null]];
            {
                let db = create_db(dir.path());
                let conn = Connection::open(&db).unwrap();
                conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL)")
                    .unwrap();
                conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
                conn.execute(
                    "ALTER TABLE t ADD COLUMN b INTEGER DEFAULT (9223372036854775807 + 1)",
                )
                .unwrap();
                conn.execute("INSERT INTO t VALUES (1, 10, NULL)").unwrap();
                if explicit_transaction {
                    conn.execute("BEGIN").unwrap();
                }

                // The first row is patched in place. Materializing the next
                // row's untouched default fails before its grown row can be queued.
                let sql = "UPDATE t SET a = a + 1 WHERE id >= 1 AND id <= 2";
                let error = if prepared {
                    conn.prepare(sql).unwrap().execute(&[]).unwrap_err()
                } else {
                    conn.execute(sql).unwrap_err()
                };
                assert!(matches!(error, SqlError::IntegerOverflow), "got: {error:?}");
                if explicit_transaction {
                    assert!(matches!(
                        conn.execute("COMMIT").unwrap_err(),
                        SqlError::Storage(citadel_core::Error::TransactionFailed)
                    ));
                }
                assert_eq!(
                    conn.query("SELECT * FROM t WHERE id = 1").unwrap().rows,
                    original_first
                );
            }
            let db = open_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            assert_eq!(
                conn.query("SELECT * FROM t WHERE id = 1").unwrap().rows,
                original_first
            );
        }
    }
}

#[test]
fn update_added_default_residual_where_and_returning() {
    for_added_default_update_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL)",
            "INSERT INTO t VALUES (1, 3), (2, 4)",
            "ALTER TABLE t ADD COLUMN b INTEGER DEFAULT 10",
            "INSERT INTO t VALUES (3, 5, NULL)",
        ],
        |conn, prepared| {
            let sql =
                "UPDATE t SET a = a + b WHERE id >= 1 AND id <= 3 AND b = 10 RETURNING id, a, b";
            let rows = if prepared {
                conn.prepare(sql).unwrap().query_collect(&[]).unwrap().rows
            } else {
                conn.query(sql).unwrap().rows
            };
            assert_eq!(
                rows,
                vec![
                    vec![Value::Integer(1), Value::Integer(13), Value::Integer(10)],
                    vec![Value::Integer(2), Value::Integer(14), Value::Integer(10)],
                ]
            );
            assert_eq!(
                conn.query("SELECT a, b FROM t WHERE id = 3").unwrap().rows,
                vec![vec![Value::Integer(5), Value::Null]]
            );
        },
    );
}

#[test]
fn update_added_default_virtual_generated_input_and_returning() {
    for_added_default_update_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
            "INSERT INTO t VALUES (1, 3), (2, 4)",
            "ALTER TABLE t ADD COLUMN b INTEGER DEFAULT 10",
            "ALTER TABLE t ADD COLUMN g INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL",
        ],
        |conn, prepared| {
            let sql = "UPDATE t SET a = g + 1 WHERE id >= 1 AND id <= 2 RETURNING a, b, g";
            let rows = if prepared {
                conn.prepare(sql).unwrap().query_collect(&[]).unwrap().rows
            } else {
                conn.query(sql).unwrap().rows
            };
            assert_eq!(
                rows,
                vec![
                    vec![Value::Integer(14), Value::Integer(10), Value::Integer(24)],
                    vec![Value::Integer(15), Value::Integer(10), Value::Integer(25)],
                ]
            );
            assert_eq!(
                conn.query("SELECT a, b, g FROM t ORDER BY id")
                    .unwrap()
                    .rows,
                rows
            );
        },
    );
}

#[test]
fn update_added_default_preserves_intervening_same_bitmap_byte() {
    for_added_default_update_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)",
            "INSERT INTO t VALUES (1, 'old')",
            "ALTER TABLE t ADD COLUMN b INTEGER DEFAULT 10",
            "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 20",
        ],
        |conn, prepared| {
            execute_added_default_update(conn, prepared, "UPDATE t SET c = 99 WHERE id = 1", 1);
            assert_eq!(
                conn.query("SELECT a, b, c FROM t").unwrap().rows,
                vec![vec![
                    Value::Text("old".into()),
                    Value::Integer(10),
                    Value::Integer(99)
                ]]
            );
        },
    );
}

#[test]
fn update_added_default_preserves_intervening_new_bitmap_byte() {
    for_added_default_update_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER, f INTEGER)",
            "INSERT INTO t VALUES (1, 1, NULL, 3, 4, 5, 6), (2, 11, 12, 13, 14, 15, 16)",
            "ALTER TABLE t ADD COLUMN g INTEGER DEFAULT 70",
            "ALTER TABLE t ADD COLUMN h TEXT DEFAULT 'eight'",
            "ALTER TABLE t ADD COLUMN i INTEGER DEFAULT 90",
        ],
        |conn, prepared| {
            execute_added_default_update(
                conn,
                prepared,
                "UPDATE t SET i = 99 WHERE id >= 1 AND id <= 2",
                2,
            );
            assert_eq!(
                conn.query("SELECT b, f, g, h, i FROM t ORDER BY id").unwrap().rows,
                vec![
                    vec![Value::Null, Value::Integer(6), Value::Integer(70), Value::Text("eight".into()), Value::Integer(99)],
                    vec![Value::Integer(12), Value::Integer(16), Value::Integer(70), Value::Text("eight".into()), Value::Integer(99)],
                ]
            );
        },
    );
}

#[test]
fn update_added_default_after_nontrailing_drop() {
    for_added_default_update_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, dropped TEXT, a INTEGER)",
            "INSERT INTO t VALUES (1, 'gone', 4), (2, 'gone', 5)",
            "ALTER TABLE t DROP COLUMN dropped",
            "ALTER TABLE t ADD COLUMN b INTEGER DEFAULT 10",
            "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 20",
        ],
        |conn, prepared| {
            execute_added_default_update(conn, prepared, "UPDATE t SET a = a + b, c = c + 1", 2);
            assert_eq!(
                conn.query("SELECT a, b, c FROM t ORDER BY id")
                    .unwrap()
                    .rows,
                vec![
                    vec![Value::Integer(14), Value::Integer(10), Value::Integer(21)],
                    vec![Value::Integer(15), Value::Integer(10), Value::Integer(21)],
                ]
            );
        },
    );
}

#[test]
fn update_added_default_volatile_value_is_materialized_once_per_row() {
    for_added_default_update_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, copied INTEGER)",
            "INSERT INTO t VALUES (1, NULL), (2, NULL), (3, NULL)",
            "ALTER TABLE t ADD COLUMN token INTEGER DEFAULT (RANDOM())",
        ],
        |conn, prepared| {
            execute_added_default_update(conn, prepared, "UPDATE t SET copied = token", 3);
            let rows = conn
                .query("SELECT copied, token FROM t ORDER BY id")
                .unwrap()
                .rows;
            for row in &rows {
                assert!(matches!(row[0], Value::Integer(_)), "got {row:?}");
                assert_eq!(
                    row[0], row[1],
                    "RHS and stored default must use the same evaluation"
                );
            }
            assert!(rows.windows(2).any(|pair| pair[0][1] != pair[1][1]));
            assert_eq!(
                conn.query("SELECT copied, token FROM t ORDER BY id")
                    .unwrap()
                    .rows,
                rows
            );
        },
    );
}

#[test]
fn update_added_default_error_is_lazy_for_stored_values() {
    for_added_default_update_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
            "INSERT INTO t VALUES (1, 5)",
            "ALTER TABLE t ADD COLUMN b INTEGER DEFAULT (9223372036854775807 + 1)",
            "INSERT INTO t VALUES (2, 6, NULL)",
        ],
        |conn, prepared| {
            execute_added_default_update(conn, prepared, "UPDATE t SET a = a + 1 WHERE id = 2", 1);
            assert_eq!(
                conn.query("SELECT * FROM t WHERE id = 2").unwrap().rows,
                vec![vec![Value::Integer(2), Value::Integer(7), Value::Null]]
            );
            let sql = "UPDATE t SET a = a + b WHERE id = 1";
            let err = if prepared {
                conn.prepare(sql).unwrap().execute(&[]).unwrap_err()
            } else {
                conn.execute(sql).unwrap_err()
            };
            assert!(matches!(err, SqlError::IntegerOverflow), "got: {err:?}");
        },
    );
}

#[test]
fn update_added_default_rollback_snapshot_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN v INTEGER DEFAULT 10")
            .unwrap();
        let observer = Connection::open(&db).unwrap();
        observer.execute("BEGIN READ ONLY").unwrap();
        let original = vec![vec![Value::Integer(10)], vec![Value::Integer(10)]];
        assert_eq!(
            observer.query("SELECT v FROM t ORDER BY id").unwrap().rows,
            original
        );

        let stmt = conn
            .prepare("UPDATE t SET v = v + 1 WHERE id >= 1 AND id <= 2")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        assert_eq!(stmt.execute(&[]).unwrap(), 2);
        let updated = vec![vec![Value::Integer(11)], vec![Value::Integer(11)]];
        assert_eq!(
            conn.query("SELECT v FROM t ORDER BY id").unwrap().rows,
            updated
        );
        conn.execute("ROLLBACK").unwrap();
        assert_eq!(
            conn.query("SELECT v FROM t ORDER BY id").unwrap().rows,
            original
        );

        assert_eq!(stmt.execute(&[]).unwrap(), 2);
        assert_eq!(
            conn.query("SELECT v FROM t ORDER BY id").unwrap().rows,
            updated
        );
        assert_eq!(
            observer.query("SELECT v FROM t ORDER BY id").unwrap().rows,
            original
        );
        observer.execute("COMMIT").unwrap();
        assert_eq!(
            observer.query("SELECT v FROM t ORDER BY id").unwrap().rows,
            updated
        );
    }
    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_eq!(
        conn.query("SELECT v FROM t ORDER BY id").unwrap().rows,
        vec![vec![Value::Integer(11)], vec![Value::Integer(11)]]
    );
}

#[test]
fn alter_internal_schema_table_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    let err = conn
        .execute("ALTER TABLE _schema ADD COLUMN x TEXT")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported(ref msg) if msg.contains("internal")),
        "got: {err:?}"
    );
}

#[test]
fn add_column_with_fk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();

    assert_ok(
        conn.execute("ALTER TABLE child ADD COLUMN pid INTEGER REFERENCES parent(id)")
            .unwrap(),
    );

    // FK should be enforced
    let err = conn
        .execute("INSERT INTO child VALUES (1, 999)")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::ForeignKeyViolation(_)),
        "got: {err:?}"
    );

    // Valid FK works
    conn.execute("INSERT INTO child VALUES (1, 1)").unwrap();
}

#[test]
fn drop_column_referenced_by_other_table_fk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE UNIQUE INDEX idx_val ON parent (val)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, pval INTEGER REFERENCES parent(val))",
        )
        .unwrap(),
    );

    // val is indexed, so "indexed" check fires first; also referenced by child FK
    let err = conn
        .execute("ALTER TABLE parent DROP COLUMN val")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported(ref msg) if msg.contains("indexed") || msg.contains("referenced by a foreign key")),
        "got: {err:?}"
    );
}

#[test]
fn add_column_nullable_with_where_null_check() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2)").unwrap();

    assert_ok(conn.execute("ALTER TABLE t ADD COLUMN val TEXT").unwrap());

    // New row with value
    conn.execute("INSERT INTO t VALUES (3, 'hello')").unwrap();

    let qr = conn
        .query("SELECT id FROM t WHERE val IS NULL ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));

    let qr = conn
        .query("SELECT id FROM t WHERE val IS NOT NULL")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

fn as_int(v: &Value) -> i64 {
    match v {
        Value::Integer(i) => *i,
        other => panic!("expected Integer, got {other:?}"),
    }
}

#[test]
fn projected_scan_after_nontrailing_drop_column() {
    // Regression: a non-trailing DROP COLUMN left surviving columns at physical slots past
    // the live count, panicking the projected-scan decoder out-of-bounds.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, d TEXT)",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10, 20, 30, 'x')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 11, 21, 31, 'y')")
        .unwrap();

    // Non-trailing non-PK drop: surviving c, d now sit at physical slots > live count.
    assert_ok(conn.execute("ALTER TABLE t DROP COLUMN b").unwrap());

    let r = conn
        .prepare("SELECT a, c FROM t")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let mut got: Vec<(i64, i64)> = r
        .rows
        .iter()
        .map(|row| (as_int(&row[0]), as_int(&row[1])))
        .collect();
    got.sort();
    assert_eq!(got, vec![(10, 30), (11, 31)]);

    let r = conn
        .prepare("SELECT a, d FROM t")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let mut got: Vec<(i64, String)> = r
        .rows
        .iter()
        .map(|row| {
            let s = match &row[1] {
                Value::Text(s) => s.to_string(),
                v => panic!("d: {v:?}"),
            };
            (as_int(&row[0]), s)
        })
        .collect();
    got.sort();
    assert_eq!(got, vec![(10, "x".to_string()), (11, "y".to_string())]);

    let r = conn
        .prepare("SELECT id FROM t")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    let r = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(r.rows[0].len(), 4);
}

#[test]
fn projected_fastlane_fires_after_trailing_drop() {
    // The static-offset fast lane must still fire correctly when the dropped slot is
    // after every projected target, so offsets are unaffected.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10, 20, 30)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 11, 21, 31)")
        .unwrap();

    assert_ok(conn.execute("ALTER TABLE t DROP COLUMN c").unwrap());

    // a, b are fixed-width and precede the dropped slot -> fast lane resolves offsets.
    let r = conn
        .prepare("SELECT a, b FROM t")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let mut got: Vec<(i64, i64)> = r
        .rows
        .iter()
        .map(|row| (as_int(&row[0]), as_int(&row[1])))
        .collect();
    got.sort();
    assert_eq!(got, vec![(10, 20), (11, 21)]);
}
