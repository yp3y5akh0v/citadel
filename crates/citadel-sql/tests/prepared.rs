use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn query_result(result: ExecutionResult) -> QueryResult {
    match result {
        ExecutionResult::Query(qr) => qr,
        other => panic!("expected Query, got {other:?}"),
    }
}

fn setup_schema(conn: &mut Connection<'_>) {
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX idx_name ON users (name)")
        .unwrap();
}

fn insert_users(conn: &mut Connection<'_>) {
    conn.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 35)")
        .unwrap();
}

// ── Basic parameter parsing ─────────────────────────────────────────

#[test]
fn select_with_pk_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    let qr = query_result(
        conn.execute_params("SELECT name FROM users WHERE id = $1", &[Value::Integer(2)])
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Bob".into()));
}

#[test]
fn select_with_multiple_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT * FROM users WHERE age > $1 AND age < $2",
            &[Value::Integer(26), Value::Integer(34)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn insert_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    conn.execute_params(
        "INSERT INTO users (id, name, age) VALUES ($1, $2, $3)",
        &[
            Value::Integer(10),
            Value::Text("Dave".into()),
            Value::Integer(40),
        ],
    )
    .unwrap();

    let qr = query_result(
        conn.execute("SELECT name FROM users WHERE id = 10")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Text("Dave".into()));
}

#[test]
fn update_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    conn.execute_params(
        "UPDATE users SET name = $2 WHERE id = $1",
        &[Value::Integer(1), Value::Text("Alicia".into())],
    )
    .unwrap();

    let qr = query_result(conn.execute("SELECT name FROM users WHERE id = 1").unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("Alicia".into()));
}

#[test]
fn delete_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    conn.execute_params("DELETE FROM users WHERE id = $1", &[Value::Integer(2)])
        .unwrap();

    let qr = query_result(conn.execute("SELECT COUNT(*) FROM users").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

// ── Parameter count validation ──────────────────────────────────────

#[test]
fn too_few_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let result = conn.execute_params(
        "SELECT * FROM users WHERE id = $1 AND name = $2",
        &[Value::Integer(1)],
    );
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(err.contains("mismatch"));
}

#[test]
fn too_many_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let result = conn.execute_params(
        "SELECT * FROM users WHERE id = $1",
        &[Value::Integer(1), Value::Integer(2)],
    );
    assert!(result.is_err());
}

#[test]
fn no_params_with_execute_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    let qr = query_result(conn.execute_params("SELECT * FROM users", &[]).unwrap());
    assert_eq!(qr.rows.len(), 3);
}

// ── Cache behavior ──────────────────────────────────────────────────

#[test]
fn cache_reuse_same_sql() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    let sql = "SELECT * FROM users WHERE id = $1";

    let qr1 = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr1.rows[0][1], Value::Text("Alice".into()));

    let qr2 = query_result(conn.execute_params(sql, &[Value::Integer(3)]).unwrap());
    assert_eq!(qr2.rows[0][1], Value::Text("Carol".into()));
}

#[test]
fn schema_invalidation_on_create_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    let sql = "SELECT * FROM users WHERE age = $1";
    conn.execute_params(sql, &[Value::Integer(30)]).unwrap();

    conn.execute("CREATE INDEX idx_age ON users (age)").unwrap();

    let qr = query_result(conn.execute_params(sql, &[Value::Integer(25)]).unwrap());
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("Bob".into()));
}

#[test]
fn schema_invalidation_on_drop_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    conn.execute_params("SELECT * FROM users WHERE id = $1", &[Value::Integer(1)])
        .unwrap();

    conn.execute("DROP TABLE users").unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name, age) VALUES (1, 'NewAlice', 99)")
        .unwrap();

    let qr = query_result(
        conn.execute_params("SELECT name FROM users WHERE id = $1", &[Value::Integer(1)])
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Text("NewAlice".into()));
}

// ── Edge cases ──────────────────────────────────────────────────────

#[test]
fn null_parameter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    conn.execute_params(
        "INSERT INTO users (id, name, age) VALUES ($1, $2, $3)",
        &[Value::Integer(1), Value::Text("Eve".into()), Value::Null],
    )
    .unwrap();

    let qr = query_result(conn.execute("SELECT age FROM users WHERE id = 1").unwrap());
    assert_eq!(qr.rows[0][0], Value::Null);
}

#[test]
fn param_in_between() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT name FROM users WHERE age BETWEEN $1 AND $2",
            &[Value::Integer(26), Value::Integer(34)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
}

#[test]
fn param_in_like() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT name FROM users WHERE name LIKE $1",
            &[Value::Text("A%".into())],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
}

#[test]
fn param_in_in_list() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT name FROM users WHERE id IN ($1, $2) ORDER BY id",
            &[Value::Integer(1), Value::Integer(3)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Carol".into()));
}

#[test]
fn param_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute_params(
        "INSERT INTO users (id, name, age) VALUES ($1, $2, $3)",
        &[
            Value::Integer(1),
            Value::Text("TxnUser".into()),
            Value::Integer(20),
        ],
    )
    .unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = query_result(conn.execute("SELECT name FROM users WHERE id = 1").unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("TxnUser".into()));
}

#[test]
fn explain_with_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "EXPLAIN SELECT * FROM users WHERE id = $1",
            &[Value::Integer(5)],
        )
        .unwrap(),
    );
    let plan: Vec<String> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            _ => panic!(),
        })
        .collect();
    assert!(plan[0].contains("SEARCH TABLE users"));
    assert!(plan[0].contains("USING PRIMARY KEY"));
}

#[test]
fn query_params_convenience() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    let qr = conn
        .query_params("SELECT name FROM users WHERE id = $1", &[Value::Integer(2)])
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("Bob".into()));
}

#[test]
fn param_in_order_by_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT name FROM users ORDER BY name LIMIT $1",
            &[Value::Integer(2)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn backward_compat_execute_no_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);

    let qr = query_result(conn.execute("SELECT * FROM users").unwrap());
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn param_in_join_on_clause() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    insert_users(&mut conn);
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")
        .unwrap();
    conn.execute("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 99.0)")
        .unwrap();

    let qr = query_result(conn.execute_params(
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > $1",
        &[Value::Real(50.0)],
    ).unwrap());
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
}
