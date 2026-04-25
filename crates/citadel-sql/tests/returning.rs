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

fn query(conn: &Connection, sql: &str) -> QueryResult {
    conn.query(sql).unwrap()
}

fn rows_affected(result: ExecutionResult) -> u64 {
    match result {
        ExecutionResult::RowsAffected(n) => n,
        ExecutionResult::Query(qr) => qr.rows.len() as u64,
        ExecutionResult::Ok => 0,
    }
}

fn setup_users(conn: &Connection) {
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, count INTEGER)")
        .unwrap();
}

#[test]
fn insert_returning_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Alice', 5) RETURNING *",
    );
    assert_eq!(qr.columns, vec!["id", "name", "count"]);
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][2], Value::Integer(5));
}

#[test]
fn insert_returning_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Alice', 5) RETURNING id, name",
    );
    assert_eq!(qr.columns, vec!["id", "name"]);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn insert_returning_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'alice', 5) RETURNING id + 1, UPPER(name) AS u",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[0][1], Value::Text("ALICE".into()));
}

#[test]
fn insert_returning_multi_row_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'A', 1), (2, 'B', 2), (3, 'C', 3) RETURNING id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
    assert_eq!(qr.rows[2][0], Value::Integer(3));
}

#[test]
fn insert_returning_qualified_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Alice', 5) RETURNING users.id",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn update_returning_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(
        &conn,
        "UPDATE users SET count = 10 WHERE id = 1 RETURNING *",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][2], Value::Integer(10));
}

#[test]
fn update_returning_no_match_yields_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "UPDATE users SET count = 10 WHERE id = 999 RETURNING *",
    );
    assert_eq!(qr.rows.len(), 0);
    assert_eq!(qr.columns, vec!["id", "name", "count"]);
}

#[test]
fn delete_returning_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(&conn, "DELETE FROM users WHERE id = 1 RETURNING *");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));

    let after = query(&conn, "SELECT * FROM users");
    assert_eq!(after.rows.len(), 0);
}

#[test]
fn delete_returning_subset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5), (2, 'Bob', 3)")
        .unwrap();

    let qr = query(&conn, "DELETE FROM users RETURNING id");
    assert_eq!(qr.columns, vec!["id"]);
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn update_returning_old_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(
        &conn,
        "UPDATE users SET count = count + 10 WHERE id = 1 RETURNING old.count AS was, new.count AS now_",
    );
    assert_eq!(qr.columns, vec!["was", "now_"]);
    assert_eq!(qr.rows[0][0], Value::Integer(5));
    assert_eq!(qr.rows[0][1], Value::Integer(15));
}

#[test]
fn update_returning_old_star_and_new_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(
        &conn,
        "UPDATE users SET count = count + 10 WHERE id = 1 RETURNING old.*, new.*",
    );
    assert_eq!(qr.rows[0].len(), 6);
    assert_eq!(qr.rows[0][2], Value::Integer(5));
    assert_eq!(qr.rows[0][5], Value::Integer(15));
}

#[test]
fn delete_returning_old_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(&conn, "DELETE FROM users WHERE id = 1 RETURNING old.*");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn insert_returning_new_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Alice', 5) RETURNING new.*",
    );
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn insert_returning_old_star_is_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Alice', 5) RETURNING old.*",
    );
    assert_eq!(qr.rows.len(), 1);
    for v in &qr.rows[0] {
        assert_eq!(*v, Value::Null);
    }
}

#[test]
fn upsert_do_nothing_on_conflict_returning_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Bob', 99) ON CONFLICT (id) DO NOTHING RETURNING *",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn upsert_do_nothing_no_conflict_returning_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Alice', 5) ON CONFLICT (id) DO NOTHING RETURNING *",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn upsert_do_update_returning_post_update_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'X', 100) ON CONFLICT (id) DO UPDATE SET count = count + 1 RETURNING count",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(6));
}

#[test]
fn upsert_do_update_returning_old_and_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'X', 100) ON CONFLICT (id) DO UPDATE SET count = count + 1 RETURNING old.count, new.count",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(5));
    assert_eq!(qr.rows[0][1], Value::Integer(6));
}

#[test]
fn returning_aggregate_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let err = conn
        .execute("INSERT INTO users VALUES (1, 'A', 5) RETURNING COUNT(*)")
        .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(ref msg) if msg.contains("aggregate")));
}

#[test]
fn returning_unknown_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let err = conn
        .execute("INSERT INTO users VALUES (1, 'A', 5) RETURNING does_not_exist")
        .unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)));
}

#[test]
fn prepared_insert_returning() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let stmt = conn
        .prepare("INSERT INTO users VALUES ($1, $2, $3) RETURNING id, name")
        .unwrap();
    let result = stmt
        .query_collect(&[Value::Integer(7), Value::Text("Eve".into()), Value::Integer(0)])
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(7));
    assert_eq!(result.rows[0][1], Value::Text("Eve".into()));
}

#[test]
fn execute_does_not_drop_returning_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let result = conn
        .execute("INSERT INTO users VALUES (1, 'A', 5) RETURNING *")
        .unwrap();
    assert_eq!(rows_affected(result), 1);
}
