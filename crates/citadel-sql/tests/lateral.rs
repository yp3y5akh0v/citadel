use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
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

fn query(conn: &Connection, sql: &str) -> QueryResult {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(qr) => qr,
        other => panic!("expected Query, got {other:?}"),
    }
}

fn setup_categories_products(conn: &Connection) {
    assert_ok(
        conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE p (id INTEGER PRIMARY KEY, cat_id INTEGER, name TEXT, price INTEGER)",
        )
        .unwrap(),
    );
    conn.execute("INSERT INTO c VALUES (1, 'Books'), (2, 'Toys'), (3, 'Empty')")
        .unwrap();
    conn.execute("INSERT INTO p VALUES (10, 1, 'Rust', 50), (11, 1, 'SQL', 30), (12, 1, 'Go', 40), (13, 2, 'Lego', 100), (14, 2, 'Doll', 25)")
        .unwrap();
}

#[test]
fn lateral_top_n_per_group() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let qr = query(
        &conn,
        "SELECT c.id, p.name FROM c, LATERAL (
            SELECT name FROM p WHERE p.cat_id = c.id ORDER BY price DESC LIMIT 2
         ) p ORDER BY c.id, p.name",
    );
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(1));
    assert_eq!(qr.rows[2][0], Value::Integer(2));
    assert_eq!(qr.rows[3][0], Value::Integer(2));
}

#[test]
fn lateral_left_join_preserves_outer_when_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let qr = query(
        &conn,
        "SELECT c.id FROM c LEFT JOIN LATERAL (
            SELECT name FROM p WHERE p.cat_id = c.id LIMIT 1
         ) p ON true ORDER BY c.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[2][0], Value::Integer(3));
}

#[test]
fn lateral_cross_join_form() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let qr = query(
        &conn,
        "SELECT c.id, p.name FROM c CROSS JOIN LATERAL (
            SELECT name FROM p WHERE p.cat_id = c.id LIMIT 1
         ) p ORDER BY c.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
}

#[test]
fn lateral_non_equality_correlation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, budget INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, price INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO c VALUES (1, 50), (2, 200)")
        .unwrap();
    conn.execute("INSERT INTO p VALUES (10, 30), (11, 100), (12, 150)")
        .unwrap();

    let qr = query(
        &conn,
        "SELECT c.id, p.id FROM c, LATERAL (
            SELECT id FROM p WHERE p.price < c.budget
         ) p ORDER BY c.id, p.id",
    );
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(10));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
}

#[test]
fn non_lateral_derived_table_in_from() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let qr = query(
        &conn,
        "SELECT sub.cat_id, sub.cnt FROM (
            SELECT cat_id, COUNT(*) AS cnt FROM p GROUP BY cat_id
         ) sub ORDER BY sub.cat_id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(3));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
    assert_eq!(qr.rows[1][1], Value::Integer(2));
}

#[test]
fn non_lateral_derived_table_in_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let qr = query(
        &conn,
        "SELECT c.id, sub.cnt FROM c INNER JOIN (
            SELECT cat_id, COUNT(*) AS cnt FROM p GROUP BY cat_id
         ) sub ON c.id = sub.cat_id ORDER BY c.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(3));
}

#[test]
fn lateral_right_join_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let result = conn.execute(
        "SELECT * FROM c RIGHT JOIN LATERAL (SELECT name FROM p WHERE p.cat_id = c.id) p ON true",
    );
    assert!(matches!(result, Err(SqlError::Unsupported(_))));
}

#[test]
fn lateral_full_outer_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let result = conn.execute(
        "SELECT * FROM c FULL OUTER JOIN LATERAL (SELECT name FROM p WHERE p.cat_id = c.id) p ON true",
    );
    assert!(matches!(result, Err(SqlError::Unsupported(_))));
}
