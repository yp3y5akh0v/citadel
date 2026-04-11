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

// ── ADD COLUMN ───────────────────────────────────────────────────────

#[test]
fn add_nullable_column_to_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

    let err = conn
        .execute("ALTER TABLE missing ADD COLUMN val TEXT")
        .unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)), "got: {err:?}");
}

#[test]
fn add_column_select_star_returns_new_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
        let mut conn = Connection::open(&db).unwrap();
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
        let mut conn = Connection::open(&db).unwrap();
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
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("BEGIN").unwrap();
    assert_ok(conn.execute("ALTER TABLE t ADD COLUMN val TEXT").unwrap());
    conn.execute("ROLLBACK").unwrap();

    // Column should not exist
    let err = conn
        .execute("INSERT INTO t (id, val) VALUES (1, 'x')")
        .unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)), "got: {err:?}");
}

#[test]
fn add_primary_key_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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

// ── DROP COLUMN ──────────────────────────────────────────────────────

#[test]
fn drop_non_pk_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
        let mut conn = Connection::open(&db).unwrap();
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
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT * FROM t").unwrap();
        assert_eq!(qr.columns, vec!["id", "b"]);
        assert_eq!(qr.rows[0][1], Value::Integer(42));
    }
}

#[test]
fn drop_column_in_txn_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER DEFAULT 10 CHECK (a > 0), b TEXT)",
        )
        .unwrap(),
    );
    conn.execute("INSERT INTO t (id, b) VALUES (1, 'x')")
        .unwrap();

    assert_ok(conn.execute("ALTER TABLE t DROP COLUMN a").unwrap());

    // Only id and b remain
    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.columns, vec!["id", "b"]);
    assert_eq!(qr.rows[0][1], Value::Text("x".into()));

    // Insert should work without the dropped column's check
    conn.execute("INSERT INTO t (id, b) VALUES (2, 'y')")
        .unwrap();
}

#[test]
fn drop_column_then_insert_correct_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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

// ── RENAME COLUMN ────────────────────────────────────────────────────

#[test]
fn rename_column_select_with_new_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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

    // Index should still work under the new name
    let qr = conn.query("SELECT id FROM t WHERE amount = 200").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn rename_to_existing_name_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
        let mut conn = Connection::open(&db).unwrap();
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
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
    }
}

#[test]
fn rename_fk_column_fk_still_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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

    // Rename the FK column in child
    assert_ok(
        conn.execute("ALTER TABLE child RENAME COLUMN pid TO parent_id")
            .unwrap(),
    );

    // FK still enforced
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
    let mut conn = Connection::open(&db).unwrap();

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

    // Old name should work again
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'hi')")
        .unwrap();
    let qr = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("hi".into()));
}

// ── RENAME TABLE ─────────────────────────────────────────────────────

#[test]
fn rename_table_old_errors_new_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

    let err = conn
        .execute("ALTER TABLE missing RENAME TO new_name")
        .unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)), "got: {err:?}");
}

#[test]
fn rename_table_fks_in_other_tables_updated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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

    // Valid insert still works
    conn.execute("INSERT INTO parent2 VALUES (2)").unwrap();
    conn.execute("INSERT INTO child VALUES (2, 2)").unwrap();
}

#[test]
fn rename_table_persistence_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        assert_ok(
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .unwrap(),
        );
        conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
        assert_ok(conn.execute("ALTER TABLE t RENAME TO t_renamed").unwrap());
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
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
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'hi')").unwrap();

    conn.execute("BEGIN").unwrap();
    assert_ok(conn.execute("ALTER TABLE t RENAME TO t_new").unwrap());
    conn.execute("ROLLBACK").unwrap();

    // Old name should work again
    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("hi".into()));
}

#[test]
fn rename_table_with_self_referencing_fk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES t(id))")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 1)").unwrap();

    assert_ok(conn.execute("ALTER TABLE t RENAME TO tree").unwrap());

    // Self-referencing FK still enforced
    let err = conn
        .execute("INSERT INTO tree VALUES (3, 999)")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::ForeignKeyViolation(_)),
        "got: {err:?}"
    );

    // Valid self-ref still works
    conn.execute("INSERT INTO tree VALUES (3, 2)").unwrap();
}

#[test]
fn explain_alter_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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

// ── Edge cases & decode-path coverage ────────────────────────────────

#[test]
fn add_then_drop_same_column_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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

    // This exercises the SimplePredicate / match_nonpk_int_inline path
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
    let mut conn = Connection::open(&db).unwrap();

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

    // This exercises PartialDecodeCtx::decode() path
    let qr = conn.query("SELECT b FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(42));
    assert_eq!(qr.rows[1][0], Value::Integer(42));
}

#[test]
fn add_column_join_using_new_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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

#[test]
fn alter_internal_schema_table_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
    let mut conn = Connection::open(&db).unwrap();

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
