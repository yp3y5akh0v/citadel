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

fn assert_rows_affected(result: ExecutionResult, expected: u64) {
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, expected),
        other => panic!("expected RowsAffected({expected}), got {other:?}"),
    }
}

fn assert_ok(result: ExecutionResult) {
    match result {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn setup_users_table(conn: &mut Connection) {
    assert_ok(conn.execute(
        "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT, email TEXT, age INTEGER)"
    ).unwrap());
}

fn insert_users(conn: &mut Connection) {
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@test.com', 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@test.com', 25)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (3, 'Charlie', 'charlie@test.com', 35)"
    ).unwrap(), 1);
}

// ═══════════════════════════════════════════════════════════════════
//  CREATE INDEX — basic
// ═══════════════════════════════════════════════════════════════════

#[test]
fn create_non_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);
    insert_users(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
}

#[test]
fn create_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);
    insert_users(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
}

#[test]
fn create_index_on_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@test.com', 30)"
    ).unwrap(), 1);

    let qr = conn.query("SELECT * FROM users").unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn create_multi_column_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);
    insert_users(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name_age ON users (name, age)").unwrap());
}

#[test]
fn create_unique_multi_column_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);
    insert_users(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_name_email ON users (name, email)").unwrap());
}

#[test]
fn create_index_if_not_exists_on_existing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("CREATE INDEX IF NOT EXISTS idx_name ON users (name)").unwrap());
}

#[test]
fn create_index_if_not_exists_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX IF NOT EXISTS idx_name ON users (name)").unwrap());
}

// ═══════════════════════════════════════════════════════════════════
//  CREATE INDEX — error cases
// ═══════════════════════════════════════════════════════════════════

#[test]
fn create_index_on_nonexistent_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let err = conn.execute("CREATE INDEX idx_name ON ghost (name)").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn create_index_on_nonexistent_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    let err = conn.execute("CREATE INDEX idx_bad ON users (nonexistent)").unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)));
}

#[test]
fn create_duplicate_index_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    let err = conn.execute("CREATE INDEX idx_name ON users (name)").unwrap_err();
    assert!(matches!(err, SqlError::IndexAlreadyExists(_)));
}

#[test]
fn create_unique_index_violates_existing_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'same@test.com', 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'same@test.com', 25)"
    ).unwrap(), 1);

    let err = conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn create_unique_index_allows_null_duplicates_in_existing_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', NULL, 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', NULL, 25)"
    ).unwrap(), 1);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
}

#[test]
fn create_index_case_insensitive_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX IDX_NAME ON users (name)").unwrap());
    let err = conn.execute("CREATE INDEX idx_name ON users (name)").unwrap_err();
    assert!(matches!(err, SqlError::IndexAlreadyExists(_)));
}

// ═══════════════════════════════════════════════════════════════════
//  DROP INDEX
// ═══════════════════════════════════════════════════════════════════

#[test]
fn drop_index_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("DROP INDEX idx_name").unwrap());
}

#[test]
fn drop_nonexistent_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    let err = conn.execute("DROP INDEX ghost_idx").unwrap_err();
    assert!(matches!(err, SqlError::IndexNotFound(_)));
}

#[test]
fn drop_index_if_exists_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("DROP INDEX IF EXISTS ghost_idx").unwrap());
}

#[test]
fn drop_index_if_exists_existing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("DROP INDEX IF EXISTS idx_name").unwrap());
}

#[test]
fn drop_and_recreate_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);
    insert_users(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("DROP INDEX idx_name").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
}

#[test]
fn drop_index_case_insensitive() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("DROP INDEX IDX_NAME").unwrap());
}

// ═══════════════════════════════════════════════════════════════════
//  DROP TABLE cascades index drops
// ═══════════════════════════════════════════════════════════════════

#[test]
fn drop_table_cascades_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);
    insert_users(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    assert_ok(conn.execute("DROP TABLE users").unwrap());

    setup_users_table(&mut conn);
    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
}

#[test]
fn drop_table_if_exists_with_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("DROP TABLE IF EXISTS users").unwrap());
    assert_ok(conn.execute("DROP TABLE IF EXISTS users").unwrap());
}

// ═══════════════════════════════════════════════════════════════════
//  INSERT — index maintenance
// ═══════════════════════════════════════════════════════════════════

#[test]
fn insert_populates_non_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'a@t.com', 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Alice', 'b@t.com', 25)"
    ).unwrap(), 1);

    let qr = conn.query("SELECT * FROM users WHERE name = 'Alice'").unwrap();
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn insert_populates_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@t.com', 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@t.com', 25)"
    ).unwrap(), 1);
}

#[test]
fn insert_violates_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'same@t.com', 30)"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'same@t.com', 25)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn insert_null_in_unique_index_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', NULL, 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', NULL, 25)"
    ).unwrap(), 1);

    let qr = conn.query("SELECT * FROM users WHERE email IS NULL").unwrap();
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn insert_multiple_nulls_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    for i in 1..=10 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'User{i}', NULL, {i})")
        ).unwrap(), 1);
    }

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(10));
}

#[test]
fn insert_with_multiple_indexes_on_same_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_age ON users (age)").unwrap());

    insert_users(&mut conn);

    let qr = conn.query("SELECT * FROM users").unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn insert_unique_violation_on_multicolumn_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_name_age ON users (name, age)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'a@t.com', 30)"
    ).unwrap(), 1);

    // Different name, same age — OK
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'b@t.com', 30)"
    ).unwrap(), 1);

    // Same name, different age — OK
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (3, 'Alice', 'c@t.com', 25)"
    ).unwrap(), 1);

    // Same name AND age — VIOLATION
    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Alice', 'd@t.com', 30)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn insert_multicolumn_unique_allows_partial_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_name_email ON users (name, email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', NULL, 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Alice', NULL, 25)"
    ).unwrap(), 1);
}

// ═══════════════════════════════════════════════════════════════════
//  DELETE — index maintenance
// ═══════════════════════════════════════════════════════════════════

#[test]
fn delete_removes_index_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    assert_rows_affected(conn.execute("DELETE FROM users WHERE id = 1").unwrap(), 1);

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
    ).unwrap(), 1);
}

#[test]
fn delete_all_rows_cleans_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    assert_rows_affected(conn.execute("DELETE FROM users").unwrap(), 3);

    insert_users(&mut conn);
    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn delete_with_where_cleans_correct_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    assert_rows_affected(conn.execute("DELETE FROM users WHERE age < 30").unwrap(), 1);

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'bob@test.com', 40)"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (5, 'Eve', 'alice@test.com', 22)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn delete_with_multiple_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_age ON users (age)").unwrap());

    insert_users(&mut conn);
    assert_rows_affected(conn.execute("DELETE FROM users WHERE id = 2").unwrap(), 1);

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@test.com', 25)"
    ).unwrap(), 1);
}

#[test]
fn delete_null_indexed_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', NULL, 30)"
    ).unwrap(), 1);

    assert_rows_affected(conn.execute("DELETE FROM users WHERE id = 1").unwrap(), 1);

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

// ═══════════════════════════════════════════════════════════════════
//  UPDATE — index maintenance
// ═══════════════════════════════════════════════════════════════════

#[test]
fn update_non_indexed_column_no_index_change() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    assert_rows_affected(conn.execute("UPDATE users SET age = 99 WHERE id = 1").unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn update_indexed_column_updates_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    assert_rows_affected(conn.execute(
        "UPDATE users SET email = 'newalice@test.com' WHERE id = 1"
    ).unwrap(), 1);

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (5, 'Eve', 'newalice@test.com', 22)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn update_causes_unique_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    let err = conn.execute(
        "UPDATE users SET email = 'bob@test.com' WHERE id = 1"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn update_pk_column_updates_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    assert_rows_affected(conn.execute("UPDATE users SET id = 100 WHERE id = 1").unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (200, 'Dave', 'alice@test.com', 40)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    let qr = conn.query("SELECT id, email FROM users WHERE id = 100").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("alice@test.com".into()));
}

#[test]
fn update_sets_indexed_column_to_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    assert_rows_affected(conn.execute(
        "UPDATE users SET email = NULL WHERE id = 1"
    ).unwrap(), 1);

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
    ).unwrap(), 1);
}

#[test]
fn update_from_null_to_value_unique_check() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', NULL, 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@test.com', 25)"
    ).unwrap(), 1);

    let err = conn.execute(
        "UPDATE users SET email = 'bob@test.com' WHERE id = 1"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    assert_rows_affected(conn.execute(
        "UPDATE users SET email = 'alice@test.com' WHERE id = 1"
    ).unwrap(), 1);
}

#[test]
fn update_multiple_rows_indexed_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    insert_users(&mut conn);

    assert_rows_affected(conn.execute("UPDATE users SET name = 'Everyone'").unwrap(), 3);

    let qr = conn.query("SELECT name FROM users").unwrap();
    for row in &qr.rows {
        assert_eq!(row[0], Value::Text("Everyone".into()));
    }
}

#[test]
fn update_multiple_rows_unique_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    let err = conn.execute("UPDATE users SET email = 'same@test.com'").unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn update_with_multiple_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_age ON users (age)").unwrap());

    insert_users(&mut conn);

    assert_rows_affected(conn.execute(
        "UPDATE users SET name = 'Alicia', email = 'alicia@test.com', age = 31 WHERE id = 1"
    ).unwrap(), 1);

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
    ).unwrap(), 1);
}

// ═══════════════════════════════════════════════════════════════════
//  Persistence — indexes survive reopen
// ═══════════════════════════════════════════════════════════════════

#[test]
fn index_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_users_table(&mut conn);
        insert_users(&mut conn);
        assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        let err = conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
        ).unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));

        assert_rows_affected(conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'dave@test.com', 40)"
        ).unwrap(), 1);
    }
}

#[test]
fn index_data_persists_after_insert_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_users_table(&mut conn);
        assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
        insert_users(&mut conn);
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(3));

        let err = conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'bob@test.com', 40)"
        ).unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));
    }
}

#[test]
fn create_index_reopen_then_insert() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_users_table(&mut conn);
        assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
        assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        insert_users(&mut conn);

        let err = conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
        ).unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));
    }
}

#[test]
fn create_index_reopen_then_drop_index() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_users_table(&mut conn);
        insert_users(&mut conn);
        assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        assert_ok(conn.execute("DROP INDEX idx_email").unwrap());

        assert_rows_affected(conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
        ).unwrap(), 1);
    }
}

#[test]
fn multiple_reopens_with_index_operations() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_users_table(&mut conn);
        insert_users(&mut conn);
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        assert_rows_affected(conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'dave@test.com', 40)"
        ).unwrap(), 1);
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(4));

        let err = conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (5, 'Eve', 'dave@test.com', 22)"
        ).unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Transaction interaction (BEGIN/COMMIT/ROLLBACK)
// ═══════════════════════════════════════════════════════════════════

#[test]
fn create_index_in_transaction_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);
    insert_users(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    conn.execute("COMMIT").unwrap();

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn create_index_in_transaction_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);
    insert_users(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    conn.execute("ROLLBACK").unwrap();

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
}

#[test]
fn drop_index_in_transaction_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);
    insert_users(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    conn.execute("BEGIN").unwrap();
    assert_ok(conn.execute("DROP INDEX idx_email").unwrap());
    conn.execute("ROLLBACK").unwrap();

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn insert_with_index_in_transaction_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@t.com', 30)"
    ).unwrap(), 1);
    conn.execute("ROLLBACK").unwrap();

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@t.com', 30)"
    ).unwrap(), 1);
}

#[test]
fn update_with_index_in_transaction_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute(
        "UPDATE users SET email = 'newalice@test.com' WHERE id = 1"
    ).unwrap(), 1);
    conn.execute("COMMIT").unwrap();

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
    ).unwrap(), 1);
}

#[test]
fn delete_with_index_in_transaction_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute("DELETE FROM users WHERE id = 1").unwrap(), 1);
    conn.execute("COMMIT").unwrap();

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
    ).unwrap(), 1);
}

#[test]
fn mixed_ddl_dml_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@t.com', 30)"
    ).unwrap(), 1);
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@t.com', 25)"
    ).unwrap(), 1);
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn create_index_then_unique_violation_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'a@t.com', 30)"
    ).unwrap(), 1);
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'a@t.com', 25)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

// ═══════════════════════════════════════════════════════════════════
//  DROP TABLE in transaction cascades indexes
// ═══════════════════════════════════════════════════════════════════

#[test]
fn drop_table_cascade_in_transaction_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);
    insert_users(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    conn.execute("BEGIN").unwrap();
    assert_ok(conn.execute("DROP TABLE users").unwrap());
    conn.execute("COMMIT").unwrap();

    setup_users_table(&mut conn);
    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
}

#[test]
fn drop_table_cascade_in_transaction_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);
    insert_users(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    conn.execute("BEGIN").unwrap();
    assert_ok(conn.execute("DROP TABLE users").unwrap());
    conn.execute("ROLLBACK").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

// ═══════════════════════════════════════════════════════════════════
//  Index on different column types
// ═══════════════════════════════════════════════════════════════════

#[test]
fn index_on_integer_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_age ON users (age)").unwrap());
    insert_users(&mut conn);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'd@t.com', 30)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn index_on_boolean_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE flags (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)"
    ).unwrap());

    assert_ok(conn.execute("CREATE INDEX idx_active ON flags (active)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO flags (id, active) VALUES (1, TRUE)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO flags (id, active) VALUES (2, FALSE)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO flags (id, active) VALUES (3, TRUE)"
    ).unwrap(), 1);

    let qr = conn.query("SELECT COUNT(*) FROM flags WHERE active = TRUE").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn index_on_real_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE measurements (id INTEGER NOT NULL PRIMARY KEY, value REAL)"
    ).unwrap());

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_value ON measurements (value)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO measurements (id, value) VALUES (1, 3.14)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO measurements (id, value) VALUES (2, 2.71)"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO measurements (id, value) VALUES (3, 3.14)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn index_on_text_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_name ON users (name)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'a@t.com', 30)"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Alice', 'b@t.com', 25)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn index_on_nullable_text_with_mixed_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'A', 'a@t.com', 20)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'B', NULL, 25)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (3, 'C', 'c@t.com', 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'D', NULL, 35)"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (5, 'E', 'a@t.com', 40)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (5, 'E', NULL, 40)"
    ).unwrap(), 1);

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn index_on_empty_string() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_name ON users (name)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, '', 'a@t.com', 30)"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, '', 'b@t.com', 25)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

// ═══════════════════════════════════════════════════════════════════
//  Edge cases
// ═══════════════════════════════════════════════════════════════════

#[test]
fn multiple_indexes_on_same_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name1 ON users (name)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_name2 ON users (name)").unwrap());

    insert_users(&mut conn);

    assert_ok(conn.execute("DROP INDEX idx_name1").unwrap());

    let qr = conn.query("SELECT * FROM users WHERE name = 'Alice'").unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn index_across_multiple_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute(
        "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER, total REAL)"
    ).unwrap());

    assert_ok(conn.execute("CREATE INDEX idx_user_name ON users (name)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_order_user ON orders (user_id)").unwrap());

    insert_users(&mut conn);
    assert_rows_affected(conn.execute(
        "INSERT INTO orders (id, user_id, total) VALUES (1, 1, 99.99)"
    ).unwrap(), 1);

    assert_ok(conn.execute("DROP TABLE orders").unwrap());

    let qr = conn.query("SELECT * FROM users WHERE name = 'Bob'").unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn create_drop_create_same_name_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    for _ in 0..5 {
        assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
        assert_ok(conn.execute("DROP INDEX idx_name").unwrap());
    }

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
}

#[test]
fn insert_delete_cycle_with_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    for i in 0..20 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'User', 'u@t.com', {i})")
        ).unwrap(), 1);
        assert_rows_affected(conn.execute(
            &format!("DELETE FROM users WHERE id = {i}")
        ).unwrap(), 1);
    }

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(0));

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (100, 'Final', 'u@t.com', 99)"
    ).unwrap(), 1);
}

#[test]
fn update_swap_indexed_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'a@t.com', 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'b@t.com', 25)"
    ).unwrap(), 1);

    assert_rows_affected(conn.execute(
        "UPDATE users SET email = 'temp@t.com' WHERE id = 1"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "UPDATE users SET email = 'a@t.com' WHERE id = 2"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "UPDATE users SET email = 'b@t.com' WHERE id = 1"
    ).unwrap(), 1);

    let qr = conn.query("SELECT email FROM users WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("b@t.com".into()));

    let qr = conn.query("SELECT email FROM users WHERE id = 2").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("a@t.com".into()));
}

#[test]
fn index_on_single_column_pk_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE simple (id INTEGER NOT NULL PRIMARY KEY)"
    ).unwrap());

    assert_ok(conn.execute("CREATE INDEX idx_id ON simple (id)").unwrap());

    assert_rows_affected(conn.execute("INSERT INTO simple (id) VALUES (1)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO simple (id) VALUES (2)").unwrap(), 1);
    assert_rows_affected(conn.execute("DELETE FROM simple WHERE id = 1").unwrap(), 1);

    let qr = conn.query("SELECT COUNT(*) FROM simple").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn unique_index_on_pk_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_pk ON users (id)").unwrap());
    insert_users(&mut conn);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Dup', 'dup@t.com', 99)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::DuplicateKey | SqlError::UniqueViolation(_)));
}

#[test]
fn index_with_all_null_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    for i in 1..=5 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'User{i}', NULL, {i})")
        ).unwrap(), 1);
    }

    assert_rows_affected(conn.execute("DELETE FROM users").unwrap(), 5);

    for i in 1..=5 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'User{i}', NULL, {i})")
        ).unwrap(), 1);
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Stress tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn stress_many_rows_with_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_age ON users (age)").unwrap());

    for i in 0..200 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'User{i}', 'u{i}@t.com', {})", i % 50)
        ).unwrap(), 1);
    }

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(200));

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (999, 'Dup', 'u0@t.com', 0)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn stress_many_indexes_on_one_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_age ON users (age)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_name_age ON users (name, age)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_email_name ON users (email, name)").unwrap());

    insert_users(&mut conn);

    assert_rows_affected(conn.execute(
        "UPDATE users SET name = 'Alicia', email = 'alicia@t.com', age = 31 WHERE id = 1"
    ).unwrap(), 1);

    assert_rows_affected(conn.execute("DELETE FROM users WHERE id = 2").unwrap(), 1);

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn stress_interleaved_crud_with_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    for i in 0..50 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'User{i}', 'u{i}@t.com', {})", i + 20)
        ).unwrap(), 1);
    }

    for i in (0..50).step_by(2) {
        assert_rows_affected(conn.execute(
            &format!("DELETE FROM users WHERE id = {i}")
        ).unwrap(), 1);
    }

    for i in (1..50).step_by(2) {
        assert_rows_affected(conn.execute(
            &format!("UPDATE users SET email = 'updated{i}@t.com' WHERE id = {i}")
        ).unwrap(), 1);
    }

    for i in (0..50).step_by(2) {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'New{i}', 'u{i}@t.com', {})", i + 100)
        ).unwrap(), 1);
    }

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(50));
}

#[test]
fn stress_create_populate_drop_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    for i in 0..50 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'User{i}', 'u{i}@t.com', {i})")
        ).unwrap(), 1);
    }

    for _ in 0..5 {
        assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

        let err = conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (999, 'Dup', 'u0@t.com', 0)"
        ).unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));

        assert_ok(conn.execute("DROP INDEX idx_email").unwrap());
    }
}

#[test]
fn stress_index_with_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    for batch in 0..10 {
        conn.execute("BEGIN").unwrap();
        for i in 0..5 {
            let id = batch * 5 + i;
            assert_rows_affected(conn.execute(
                &format!("INSERT INTO users (id, name, email, age) VALUES ({id}, 'U{id}', 'u{id}@t.com', {id})")
            ).unwrap(), 1);
        }
        conn.execute("COMMIT").unwrap();
    }

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(50));
}

#[test]
fn stress_index_persistence_cycle() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_users_table(&mut conn);
        assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

        for i in 0..20 {
            assert_rows_affected(conn.execute(
                &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'U{i}', 'u{i}@t.com', {i})")
            ).unwrap(), 1);
        }
    }

    for batch in 1..=5 {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        for i in 0..10 {
            let id = 20 + (batch - 1) * 10 + i;
            assert_rows_affected(conn.execute(
                &format!("INSERT INTO users (id, name, email, age) VALUES ({id}, 'U{id}', 'u{id}@t.com', {id})")
            ).unwrap(), 1);
        }
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(70));
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Composite key edge cases
// ═══════════════════════════════════════════════════════════════════

#[test]
fn composite_unique_index_enforces_full_combination() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE products (id INTEGER NOT NULL PRIMARY KEY, category TEXT, sku TEXT)"
    ).unwrap());

    assert_ok(conn.execute(
        "CREATE UNIQUE INDEX idx_cat_sku ON products (category, sku)"
    ).unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO products (id, category, sku) VALUES (1, 'electronics', 'ABC')"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO products (id, category, sku) VALUES (2, 'electronics', 'DEF')"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO products (id, category, sku) VALUES (3, 'clothing', 'ABC')"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO products (id, category, sku) VALUES (4, 'electronics', 'ABC')"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn composite_non_unique_index_allows_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE events (id INTEGER NOT NULL PRIMARY KEY, year INTEGER, month INTEGER)"
    ).unwrap());

    assert_ok(conn.execute(
        "CREATE INDEX idx_ym ON events (year, month)"
    ).unwrap());

    for i in 0..10 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO events (id, year, month) VALUES ({i}, 2024, 1)")
        ).unwrap(), 1);
    }

    let qr = conn.query("SELECT COUNT(*) FROM events").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(10));
}

// ═══════════════════════════════════════════════════════════════════
//  Data integrity: full CRUD cycle correctness
// ═══════════════════════════════════════════════════════════════════

#[test]
fn full_lifecycle_correctness() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    insert_users(&mut conn);

    let qr = conn.query("SELECT * FROM users ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
    assert_eq!(qr.rows[1][1], Value::Text("Bob".into()));
    assert_eq!(qr.rows[2][1], Value::Text("Charlie".into()));

    assert_rows_affected(conn.execute(
        "UPDATE users SET name = 'Alicia', email = 'alicia@test.com' WHERE id = 1"
    ).unwrap(), 1);

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (5, 'Eve', 'alicia@test.com', 22)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    assert_rows_affected(conn.execute("DELETE FROM users WHERE id = 2").unwrap(), 1);

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (5, 'Eve', 'bob@test.com', 22)"
    ).unwrap(), 1);

    assert_ok(conn.execute("DROP INDEX idx_name").unwrap());

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (6, 'Frank', 'charlie@test.com', 50)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(4));
}

#[test]
fn full_lifecycle_with_persistence() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_users_table(&mut conn);
        assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
        insert_users(&mut conn);
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        assert_rows_affected(conn.execute(
            "UPDATE users SET email = 'newalice@test.com' WHERE id = 1"
        ).unwrap(), 1);

        assert_rows_affected(conn.execute("DELETE FROM users WHERE id = 2").unwrap(), 1);
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(2));

        assert_rows_affected(conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
        ).unwrap(), 1);

        assert_rows_affected(conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (5, 'Eve', 'bob@test.com', 22)"
        ).unwrap(), 1);

        let err = conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (6, 'Frank', 'newalice@test.com', 50)"
        ).unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));

        let err = conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (7, 'Grace', 'charlie@test.com', 28)"
        ).unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Multi-column index with NULL combinations
// ═══════════════════════════════════════════════════════════════════

#[test]
fn composite_unique_null_in_first_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_name_email ON users (name, email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, NULL, 'a@t.com', 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, NULL, 'a@t.com', 25)"
    ).unwrap(), 1);
}

#[test]
fn composite_unique_null_in_second_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_name_email ON users (name, email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', NULL, 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Alice', NULL, 25)"
    ).unwrap(), 1);
}

#[test]
fn composite_unique_both_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_name_email ON users (name, email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, NULL, NULL, 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, NULL, NULL, 25)"
    ).unwrap(), 1);
}

// ═══════════════════════════════════════════════════════════════════
//  SELECT still works correctly with indexes present
// ═══════════════════════════════════════════════════════════════════

#[test]
fn select_with_index_returns_correct_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_age ON users (age)").unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    insert_users(&mut conn);

    let qr = conn.query("SELECT * FROM users ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][2], Value::Text("alice@test.com".into()));
    assert_eq!(qr.rows[0][3], Value::Integer(30));
}

#[test]
fn aggregation_with_index_present() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_age ON users (age)").unwrap());
    insert_users(&mut conn);

    let qr = conn.query("SELECT COUNT(*), SUM(age), AVG(age) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    assert_eq!(qr.rows[0][1], Value::Integer(90));
    assert_eq!(qr.rows[0][2], Value::Real(30.0));
}

#[test]
fn distinct_with_index_present() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_age ON users (age)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'a@t.com', 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'b@t.com', 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (3, 'Charlie', 'c@t.com', 25)"
    ).unwrap(), 1);

    let qr = conn.query("SELECT DISTINCT age FROM users ORDER BY age").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(25));
    assert_eq!(qr.rows[1][0], Value::Integer(30));
}

#[test]
fn order_by_with_index_present() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    insert_users(&mut conn);

    let qr = conn.query("SELECT name FROM users ORDER BY name").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Bob".into()));
    assert_eq!(qr.rows[2][0], Value::Text("Charlie".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  Error recovery: state consistency after failures
// ═══════════════════════════════════════════════════════════════════

#[test]
fn state_consistent_after_unique_violation_on_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'a@t.com', 30)"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'a@t.com', 25)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(
        conn.query("SELECT name FROM users WHERE id = 1").unwrap().rows[0][0],
        Value::Text("Alice".into())
    );

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'b@t.com', 25)"
    ).unwrap(), 1);

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn state_consistent_after_unique_violation_on_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    let err = conn.execute(
        "UPDATE users SET email = 'bob@test.com' WHERE id = 1"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    let qr = conn.query("SELECT email FROM users WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("alice@test.com".into()));

    let qr = conn.query("SELECT email FROM users WHERE id = 2").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("bob@test.com".into()));

    assert_rows_affected(conn.execute(
        "UPDATE users SET email = 'newalice@test.com' WHERE id = 1"
    ).unwrap(), 1);
}

#[test]
fn state_consistent_after_create_unique_index_failure() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'same@t.com', 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'same@t.com', 25)"
    ).unwrap(), 1);

    let err = conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (3, 'Charlie', 'same@t.com', 35)"
    ).unwrap(), 1);

    assert_ok(conn.execute("CREATE INDEX idx_email ON users (email)").unwrap());
}

#[test]
fn multiple_violations_dont_corrupt_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'a@t.com', 30)"
    ).unwrap(), 1);

    for i in 2..12 {
        let err = conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'X', 'a@t.com', {i})")
        ).unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));
    }

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'b@t.com', 25)"
    ).unwrap(), 1);
}

// ═══════════════════════════════════════════════════════════════════
//  Complex multi-step scenarios
// ═══════════════════════════════════════════════════════════════════

#[test]
fn update_pk_and_indexed_column_simultaneously() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());
    insert_users(&mut conn);

    assert_rows_affected(conn.execute(
        "UPDATE users SET id = 100, name = 'Alicia', email = 'alicia@t.com' WHERE id = 1"
    ).unwrap(), 1);

    let qr = conn.query("SELECT COUNT(*) FROM users WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(0));

    let qr = conn.query("SELECT name, email FROM users WHERE id = 100").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alicia".into()));
    assert_eq!(qr.rows[0][1], Value::Text("alicia@t.com".into()));

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'alice@test.com', 40)"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (5, 'Eve', 'alicia@t.com', 22)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn multi_row_insert_with_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'A', 'a@t.com', 20), (2, 'B', 'b@t.com', 25), (3, 'C', 'c@t.com', 30)"
    ).unwrap(), 3);

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'D', 'a@t.com', 35)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn multi_row_insert_duplicate_within_batch() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'A', 'same@t.com', 20), (2, 'B', 'same@t.com', 25)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn delete_no_matching_rows_with_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    assert_rows_affected(conn.execute("DELETE FROM users WHERE id = 999").unwrap(), 0);

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn update_no_matching_rows_with_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    assert_rows_affected(conn.execute(
        "UPDATE users SET email = 'new@t.com' WHERE id = 999"
    ).unwrap(), 0);

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn update_to_same_value_no_change() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    insert_users(&mut conn);

    assert_rows_affected(conn.execute(
        "UPDATE users SET email = 'alice@test.com' WHERE id = 1"
    ).unwrap(), 1);

    let qr = conn.query("SELECT email FROM users WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("alice@test.com".into()));
}

#[test]
fn complex_transaction_with_mixed_operations() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE products (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, sku TEXT, price REAL)"
    ).unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_sku ON products (sku)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_price ON products (price)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO products (id, name, sku, price) VALUES (1, 'Widget', 'WGT-001', 9.99)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO products (id, name, sku, price) VALUES (2, 'Gadget', 'GDG-001', 19.99)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO products (id, name, sku, price) VALUES (3, 'Doohickey', 'DHK-001', 29.99)"
    ).unwrap(), 1);

    assert_rows_affected(conn.execute(
        "UPDATE products SET price = 12.99 WHERE sku = 'WGT-001'"
    ).unwrap(), 1);

    assert_rows_affected(conn.execute("DELETE FROM products WHERE id = 2").unwrap(), 1);

    assert_rows_affected(conn.execute(
        "INSERT INTO products (id, name, sku, price) VALUES (4, 'Thingamajig', 'GDG-001', 39.99)"
    ).unwrap(), 1);

    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM products").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));

    let qr = conn.query("SELECT name FROM products WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("Widget".into()));

    let err = conn.execute(
        "INSERT INTO products (id, name, sku, price) VALUES (5, 'Copy', 'WGT-001', 1.99)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn complex_transaction_rollback_undoes_everything() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@t.com', 30)"
    ).unwrap(), 1);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@t.com', 25)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "UPDATE users SET email = 'newalice@t.com' WHERE id = 1"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (3, 'Charlie', 'charlie@t.com', 35)"
    ).unwrap(), 1);
    conn.execute("ROLLBACK").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));

    let qr = conn.query("SELECT email FROM users WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("alice@t.com".into()));

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@t.com', 25)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (3, 'Charlie', 'charlie@t.com', 35)"
    ).unwrap(), 1);
}

#[test]
fn multiple_tables_independent_index_operations() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, email TEXT)"
    ).unwrap());
    assert_ok(conn.execute(
        "CREATE TABLE products (id INTEGER NOT NULL PRIMARY KEY, sku TEXT)"
    ).unwrap());
    assert_ok(conn.execute(
        "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, ref_code TEXT)"
    ).unwrap());

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_user_email ON users (email)").unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_product_sku ON products (sku)").unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_order_ref ON orders (ref_code)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, email) VALUES (1, 'user@t.com')"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO products (id, sku) VALUES (1, 'SKU-001')"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO orders (id, ref_code) VALUES (1, 'ORD-001')"
    ).unwrap(), 1);

    assert_ok(conn.execute("DROP TABLE products").unwrap());

    let err = conn.execute(
        "INSERT INTO users (id, email) VALUES (2, 'user@t.com')"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    let err = conn.execute(
        "INSERT INTO orders (id, ref_code) VALUES (2, 'ORD-001')"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    assert_ok(conn.execute(
        "CREATE TABLE products (id INTEGER NOT NULL PRIMARY KEY, sku TEXT)"
    ).unwrap());
    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_product_sku ON products (sku)").unwrap());
    assert_rows_affected(conn.execute(
        "INSERT INTO products (id, sku) VALUES (1, 'SKU-001')"
    ).unwrap(), 1);
}

#[test]
fn index_correctness_after_bulk_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    for i in 0..20 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'User{i}', 'u{i}@t.com', {})", 20 + i)
        ).unwrap(), 1);
    }

    for i in 0..20 {
        assert_rows_affected(conn.execute(
            &format!("UPDATE users SET email = 'new{i}@t.com' WHERE id = {i}")
        ).unwrap(), 1);
    }

    for i in 0..20 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({}, 'Reuse{i}', 'u{i}@t.com', {})", 100 + i, 50 + i)
        ).unwrap(), 1);
    }

    for i in 0..20 {
        let err = conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({}, 'Dup', 'new{i}@t.com', 99)", 200 + i)
        ).unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));
    }

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(40));
}

#[test]
fn index_correctness_after_selective_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    for i in 0..30 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'User{i}', 'u{i}@t.com', {})", 20 + i)
        ).unwrap(), 1);
    }

    for i in (0..30).filter(|x| x % 3 == 0) {
        assert_rows_affected(conn.execute(
            &format!("DELETE FROM users WHERE id = {i}")
        ).unwrap(), 1);
    }

    for i in (0..30).filter(|x| x % 3 == 0) {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({}, 'Reuse', 'u{i}@t.com', 99)", 100 + i)
        ).unwrap(), 1);
    }

    for i in (0..30).filter(|x| x % 3 != 0) {
        let err = conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({}, 'Dup', 'u{i}@t.com', 99)", 200 + i)
        ).unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));
    }
}

#[test]
fn transition_null_to_non_null_and_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', NULL, 30)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', NULL, 25)"
    ).unwrap(), 1);

    assert_rows_affected(conn.execute(
        "UPDATE users SET email = 'a@t.com' WHERE id = 1"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (3, 'C', 'a@t.com', 20)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    assert_rows_affected(conn.execute(
        "UPDATE users SET email = NULL WHERE id = 1"
    ).unwrap(), 1);

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (3, 'C', 'a@t.com', 20)"
    ).unwrap(), 1);

    let err = conn.execute(
        "UPDATE users SET email = 'a@t.com' WHERE id = 1"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn index_with_negative_and_zero_integers() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_age ON users (age)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'A', 'a@t.com', -100)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (2, 'B', 'b@t.com', 0)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (3, 'C', 'c@t.com', 100)"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (4, 'D', 'd@t.com', -100)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (5, 'E', 'e@t.com', 0)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (6, 'F', 'f@t.com', 100)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn index_with_float_edge_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE data (id INTEGER NOT NULL PRIMARY KEY, val REAL)"
    ).unwrap());

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_val ON data (val)").unwrap());

    assert_rows_affected(conn.execute("INSERT INTO data (id, val) VALUES (1, 0.0)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO data (id, val) VALUES (2, -0.001)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO data (id, val) VALUES (3, 0.001)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO data (id, val) VALUES (4, 999999.999)").unwrap(), 1);

    let err = conn.execute("INSERT INTO data (id, val) VALUES (5, 0.0)").unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn index_with_very_long_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE data (id INTEGER NOT NULL PRIMARY KEY, val TEXT)"
    ).unwrap());

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_val ON data (val)").unwrap());

    let long_a = "a".repeat(500);
    let long_b = "b".repeat(500);

    assert_rows_affected(conn.execute(
        &format!("INSERT INTO data (id, val) VALUES (1, '{long_a}')")
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        &format!("INSERT INTO data (id, val) VALUES (2, '{long_b}')")
    ).unwrap(), 1);

    let err = conn.execute(
        &format!("INSERT INTO data (id, val) VALUES (3, '{long_a}')")
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

// ═══════════════════════════════════════════════════════════════════
//  Hardcore stress: large scale correctness verification
// ═══════════════════════════════════════════════════════════════════

#[test]
fn stress_500_rows_full_crud_cycle_with_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_name ON users (name)").unwrap());

    for i in 0..500 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'User{i}', 'u{i}@t.com', {})", i % 80)
        ).unwrap(), 1);
    }

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(500));

    for i in 0..100 {
        assert_rows_affected(conn.execute(
            &format!("DELETE FROM users WHERE id = {i}")
        ).unwrap(), 1);
    }

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(400));

    for i in 100..200 {
        assert_rows_affected(conn.execute(
            &format!("UPDATE users SET email = 'updated{i}@t.com' WHERE id = {i}")
        ).unwrap(), 1);
    }

    for i in 0..100 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({}, 'New{i}', 'u{i}@t.com', {})", 500 + i, i % 80)
        ).unwrap(), 1);
    }

    for i in 100..110 {
        let err = conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({}, 'Dup', 'updated{i}@t.com', 99)", 700 + i)
        ).unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));
    }

    for i in 100..110 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({}, 'Freed', 'u{i}@t.com', 99)", 800 + i)
        ).unwrap(), 1);
    }

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(510));
}

#[test]
fn stress_transaction_batches_with_index_verification() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());
    assert_ok(conn.execute("CREATE INDEX idx_age ON users (age)").unwrap());

    for batch in 0..20 {
        conn.execute("BEGIN").unwrap();
        for i in 0..10 {
            let id = batch * 10 + i;
            assert_rows_affected(conn.execute(
                &format!("INSERT INTO users (id, name, email, age) VALUES ({id}, 'U{id}', 'u{id}@t.com', {id})")
            ).unwrap(), 1);
        }
        conn.execute("COMMIT").unwrap();
    }

    assert_eq!(
        conn.query("SELECT COUNT(*) FROM users").unwrap().rows[0][0],
        Value::Integer(200)
    );

    for batch in 0..5 {
        conn.execute("BEGIN").unwrap();
        for i in 0..10 {
            let id = 200 + batch * 10 + i;
            assert_rows_affected(conn.execute(
                &format!("INSERT INTO users (id, name, email, age) VALUES ({id}, 'R{id}', 'r{id}@t.com', {id})")
            ).unwrap(), 1);
        }
        conn.execute("ROLLBACK").unwrap();
    }

    assert_eq!(
        conn.query("SELECT COUNT(*) FROM users").unwrap().rows[0][0],
        Value::Integer(200)
    );

    for i in 0..10 {
        let id = 200 + i;
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({id}, 'Final{id}', 'r{id}@t.com', {id})")
        ).unwrap(), 1);
    }

    assert_eq!(
        conn.query("SELECT COUNT(*) FROM users").unwrap().rows[0][0],
        Value::Integer(210)
    );
}

#[test]
fn stress_multiple_tables_multiple_indexes_persistence() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        assert_ok(conn.execute(
            "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, email TEXT, dept TEXT)"
        ).unwrap());
        assert_ok(conn.execute(
            "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER, status TEXT)"
        ).unwrap());

        assert_ok(conn.execute("CREATE UNIQUE INDEX idx_user_email ON users (email)").unwrap());
        assert_ok(conn.execute("CREATE INDEX idx_user_dept ON users (dept)").unwrap());
        assert_ok(conn.execute("CREATE INDEX idx_order_status ON orders (status)").unwrap());
        assert_ok(conn.execute("CREATE INDEX idx_order_user ON orders (user_id)").unwrap());

        for i in 0..50 {
            assert_rows_affected(conn.execute(
                &format!("INSERT INTO users (id, email, dept) VALUES ({i}, 'u{i}@t.com', 'dept{}')", i % 5)
            ).unwrap(), 1);
        }

        for i in 0..100 {
            assert_rows_affected(conn.execute(
                &format!("INSERT INTO orders (id, user_id, status) VALUES ({i}, {}, '{}')",
                    i % 50,
                    if i % 3 == 0 { "complete" } else if i % 3 == 1 { "pending" } else { "cancelled" })
            ).unwrap(), 1);
        }
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        assert_eq!(
            conn.query("SELECT COUNT(*) FROM users").unwrap().rows[0][0],
            Value::Integer(50)
        );
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM orders").unwrap().rows[0][0],
            Value::Integer(100)
        );

        let err = conn.execute("INSERT INTO users (id, email, dept) VALUES (999, 'u0@t.com', 'x')").unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));

        assert_ok(conn.execute("DROP INDEX idx_order_status").unwrap());

        assert_rows_affected(conn.execute("DELETE FROM orders WHERE id < 10").unwrap(), 10);
        assert_rows_affected(conn.execute("UPDATE users SET dept = 'fired' WHERE id < 5").unwrap(), 5);
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        assert_eq!(
            conn.query("SELECT COUNT(*) FROM users").unwrap().rows[0][0],
            Value::Integer(50)
        );
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM orders").unwrap().rows[0][0],
            Value::Integer(90)
        );

        let qr = conn.query("SELECT COUNT(*) FROM users WHERE dept = 'fired'").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(5));

        let err = conn.execute("INSERT INTO users (id, email, dept) VALUES (999, 'u10@t.com', 'x')").unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));

        assert_ok(conn.execute("CREATE INDEX idx_order_status ON orders (status)").unwrap());
    }
}

#[test]
fn index_population_from_existing_large_dataset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    for i in 0..300 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'U{i}', 'u{i}@t.com', {})", i % 60)
        ).unwrap(), 1);
    }

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    let err = conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (999, 'Dup', 'u0@t.com', 0)"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (300, 'New', 'new@t.com', 0)"
    ).unwrap(), 1);
}

#[test]
fn update_all_rows_shifts_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_table(&mut conn);

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap());

    for i in 0..20 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({i}, 'U{i}', 'u{i}@t.com', {i})")
        ).unwrap(), 1);
    }

    for i in 0..20 {
        assert_rows_affected(conn.execute(
            &format!("UPDATE users SET email = 'new_u{i}@t.com' WHERE id = {i}")
        ).unwrap(), 1);
    }

    for i in 0..20 {
        assert_rows_affected(conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({}, 'Old', 'u{i}@t.com', 99)", 100 + i)
        ).unwrap(), 1);

        let err = conn.execute(
            &format!("INSERT INTO users (id, name, email, age) VALUES ({}, 'Dup', 'new_u{i}@t.com', 99)", 200 + i)
        ).unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Composite PK table with indexes
// ═══════════════════════════════════════════════════════════════════

#[test]
fn index_on_composite_pk_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE enrollments (student_id INTEGER NOT NULL, course_id INTEGER NOT NULL, grade TEXT, PRIMARY KEY (student_id, course_id))"
    ).unwrap());

    assert_ok(conn.execute("CREATE INDEX idx_grade ON enrollments (grade)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO enrollments (student_id, course_id, grade) VALUES (1, 101, 'A')"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO enrollments (student_id, course_id, grade) VALUES (1, 102, 'B')"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO enrollments (student_id, course_id, grade) VALUES (2, 101, 'A')"
    ).unwrap(), 1);

    let qr = conn.query("SELECT COUNT(*) FROM enrollments WHERE grade = 'A'").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));

    assert_rows_affected(conn.execute(
        "DELETE FROM enrollments WHERE student_id = 1 AND course_id = 101"
    ).unwrap(), 1);

    let qr = conn.query("SELECT COUNT(*) FROM enrollments WHERE grade = 'A'").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn unique_index_on_composite_pk_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE enrollments (student_id INTEGER NOT NULL, course_id INTEGER NOT NULL, grade TEXT, PRIMARY KEY (student_id, course_id))"
    ).unwrap());

    assert_ok(conn.execute("CREATE UNIQUE INDEX idx_grade ON enrollments (grade)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO enrollments (student_id, course_id, grade) VALUES (1, 101, 'A')"
    ).unwrap(), 1);

    let err = conn.execute(
        "INSERT INTO enrollments (student_id, course_id, grade) VALUES (2, 101, 'A')"
    ).unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));

    assert_rows_affected(conn.execute(
        "INSERT INTO enrollments (student_id, course_id, grade) VALUES (2, 101, NULL)"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO enrollments (student_id, course_id, grade) VALUES (3, 101, NULL)"
    ).unwrap(), 1);
}

#[test]
fn update_composite_pk_with_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE enrollments (student_id INTEGER NOT NULL, course_id INTEGER NOT NULL, grade TEXT, PRIMARY KEY (student_id, course_id))"
    ).unwrap());

    assert_ok(conn.execute("CREATE INDEX idx_grade ON enrollments (grade)").unwrap());

    assert_rows_affected(conn.execute(
        "INSERT INTO enrollments (student_id, course_id, grade) VALUES (1, 101, 'B')"
    ).unwrap(), 1);
    assert_rows_affected(conn.execute(
        "INSERT INTO enrollments (student_id, course_id, grade) VALUES (1, 102, 'C')"
    ).unwrap(), 1);

    assert_rows_affected(conn.execute(
        "UPDATE enrollments SET grade = 'A' WHERE student_id = 1 AND course_id = 101"
    ).unwrap(), 1);

    let qr = conn.query("SELECT grade FROM enrollments WHERE student_id = 1 AND course_id = 101").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("A".into()));

    assert_rows_affected(conn.execute(
        "UPDATE enrollments SET student_id = 2 WHERE student_id = 1 AND course_id = 101"
    ).unwrap(), 1);

    let qr = conn.query("SELECT COUNT(*) FROM enrollments WHERE student_id = 2").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}
