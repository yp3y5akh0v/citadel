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

fn setup_src(conn: &mut Connection) {
    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute(
            "INSERT INTO src (id, name, age) VALUES \
             (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 35)",
        )
        .unwrap(),
        3,
    );
}

// ── 1. Basic INSERT ... SELECT ──────────────────────────────────────

#[test]
fn basic_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_src(&mut conn);

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT * FROM src").unwrap(),
        3,
    );

    let qr = conn
        .query("SELECT id, name, age FROM dst ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][2], Value::Integer(30));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
    assert_eq!(qr.rows[1][1], Value::Text("Bob".into()));
    assert_eq!(qr.rows[1][2], Value::Integer(25));
    assert_eq!(qr.rows[2][0], Value::Integer(3));
    assert_eq!(qr.rows[2][1], Value::Text("Carol".into()));
    assert_eq!(qr.rows[2][2], Value::Integer(35));
}

// ── 2. INSERT ... SELECT with explicit columns + WHERE ──────────────

#[test]
fn insert_select_with_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_src(&mut conn);

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute(
            "INSERT INTO dst (id, name, age) SELECT id, name, age FROM src WHERE age > 25",
        )
        .unwrap(),
        2,
    );

    let qr = conn
        .query("SELECT id, name, age FROM dst ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][2], Value::Integer(30));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
    assert_eq!(qr.rows[1][1], Value::Text("Carol".into()));
    assert_eq!(qr.rows[1][2], Value::Integer(35));
}

// ── 3. INSERT ... SELECT with column reorder ────────────────────────

#[test]
fn insert_select_column_reorder() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_src(&mut conn);

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dst (age, id, name) SELECT age, id, name FROM src WHERE id = 1")
            .unwrap(),
        1,
    );

    let qr = conn
        .query("SELECT id, name, age FROM dst WHERE id = 1")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][2], Value::Integer(30));
}

// ── 4. Column count mismatch ────────────────────────────────────────

#[test]
fn insert_select_column_count_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_src(&mut conn);

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
            .unwrap(),
    );

    let err = conn
        .execute("INSERT INTO dst SELECT id, name FROM src")
        .unwrap_err();
    assert!(
        err.to_string().contains("column count mismatch"),
        "expected 'column count mismatch' error, got: {err}"
    );
}

// ── 5. Self-referential INSERT ... SELECT (snapshot semantics) ──────

#[test]
fn insert_select_self_referential() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_src(&mut conn);

    assert_rows_affected(
        conn.execute("INSERT INTO src SELECT id + 10, name, age FROM src")
            .unwrap(),
        3,
    );

    let qr = conn.query("SELECT id FROM src ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 6);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
    assert_eq!(qr.rows[2][0], Value::Integer(3));
    assert_eq!(qr.rows[3][0], Value::Integer(11));
    assert_eq!(qr.rows[4][0], Value::Integer(12));
    assert_eq!(qr.rows[5][0], Value::Integer(13));
}

// ── 6. INSERT ... SELECT with parameters ────────────────────────────

#[test]
fn insert_select_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_src(&mut conn);

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute_params(
            "INSERT INTO dst SELECT * FROM src WHERE id > $1",
            &[Value::Integer(1)],
        )
        .unwrap(),
        2,
    );

    let qr = conn.query("SELECT id FROM dst ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

// ── 7. Type coercion: INTEGER → REAL ────────────────────────────────

#[test]
fn insert_select_type_coercion() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO src (id, val) VALUES (1, 42)")
            .unwrap(),
        1,
    );

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, val REAL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT * FROM src").unwrap(),
        1,
    );

    let qr = conn.query("SELECT val FROM dst WHERE id = 1").unwrap();
    match &qr.rows[0][0] {
        Value::Real(r) => assert!((*r - 42.0).abs() < 1e-10),
        other => panic!("expected Real, got {other:?}"),
    }
}

// ── 8. Empty result set ─────────────────────────────────────────────

#[test]
fn insert_select_empty_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_src(&mut conn);

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT * FROM src WHERE 1 = 0")
            .unwrap(),
        0,
    );

    let qr = conn.query("SELECT COUNT(*) FROM dst").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

// ── 9. Default fills omitted column ─────────────────────────────────

#[test]
fn insert_select_with_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_src(&mut conn);

    assert_ok(
        conn.execute(
            "CREATE TABLE dst (\
                id INTEGER PRIMARY KEY, \
                name TEXT NOT NULL, \
                age INTEGER DEFAULT 99\
            )",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dst (id, name) SELECT id, name FROM src")
            .unwrap(),
        3,
    );

    let qr = conn
        .query("SELECT id, name, age FROM dst ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    for row in &qr.rows {
        assert_eq!(row[2], Value::Integer(99));
    }
}

// ── 10. INSERT ... SELECT inside transaction ────────────────────────

#[test]
fn insert_select_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_src(&mut conn);

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT * FROM src").unwrap(),
        3,
    );
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM dst").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

// ── 11. Duplicate primary key ───────────────────────────────────────

#[test]
fn insert_select_duplicate_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_src(&mut conn);

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO dst (id, name, age) VALUES (1, 'Existing', 50)")
        .unwrap();

    let err = conn
        .execute("INSERT INTO dst SELECT * FROM src")
        .unwrap_err();
    assert!(matches!(err, SqlError::DuplicateKey));
}

// ── 12. NOT NULL violation ──────────────────────────────────────────

#[test]
fn insert_select_not_null_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO src (id, val) VALUES (1, NULL)")
            .unwrap(),
        1,
    );

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );

    let err = conn
        .execute("INSERT INTO dst SELECT * FROM src")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::NotNullViolation(_)),
        "expected NotNullViolation, got {err:?}"
    );
}

// ── 13. Foreign key violation ───────────────────────────────────────

#[test]
fn insert_select_fk_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO parent (id) VALUES (1)").unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE child (\
                id INTEGER NOT NULL PRIMARY KEY, \
                parent_id INTEGER, \
                FOREIGN KEY (parent_id) REFERENCES parent(id)\
            )",
        )
        .unwrap(),
    );

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, parent_id INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO src (id, parent_id) VALUES (1, 999)")
            .unwrap(),
        1,
    );

    let err = conn
        .execute("INSERT INTO child SELECT * FROM src")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::ForeignKeyViolation(..)),
        "expected ForeignKeyViolation, got {err:?}"
    );
}

// ── 14. CHECK constraint violation ──────────────────────────────────

#[test]
fn insert_select_check_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, age INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO src (id, age) VALUES (1, -5)")
            .unwrap(),
        1,
    );

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, age INTEGER, CHECK (age > 0))")
            .unwrap(),
    );

    let err = conn
        .execute("INSERT INTO dst SELECT * FROM src")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::CheckViolation(..)),
        "expected CheckViolation, got {err:?}"
    );
}

// ── 15. INSERT ... SELECT with JOIN ─────────────────────────────────

#[test]
fn insert_select_with_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, age INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO a (id, age) VALUES (1, 30), (2, 25), (3, 35)")
            .unwrap(),
        3,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO b (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap(),
        2,
    );

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT a.id, b.name, a.age FROM a JOIN b ON a.id = b.id")
            .unwrap(),
        2,
    );

    let qr = conn
        .query("SELECT id, name, age FROM dst ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][2], Value::Integer(30));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
    assert_eq!(qr.rows[1][1], Value::Text("Bob".into()));
    assert_eq!(qr.rows[1][2], Value::Integer(25));
}
