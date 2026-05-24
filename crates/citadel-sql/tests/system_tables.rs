use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn pg_timezone_names_bare_table_form() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT COUNT(*) FROM pg_timezone_names")
        .unwrap();
    match &qr.rows[0][0] {
        Value::Integer(n) => assert!(*n > 100, "expected >100 zones, got {n}"),
        v => panic!("expected Integer count, got {v:?}"),
    }
}

#[test]
fn pg_timezone_names_filters_utc() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT name FROM pg_timezone_names WHERE name = 'UTC'")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn pg_timezone_abbrevs_bare_table_form() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT COUNT(*) FROM pg_timezone_abbrevs")
        .unwrap();
    match &qr.rows[0][0] {
        Value::Integer(n) => assert!(*n > 0, "expected >0 abbrevs, got {n}"),
        v => panic!("expected Integer count, got {v:?}"),
    }
}

#[test]
fn information_schema_tables_lists_user_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
        .unwrap();

    let qr = conn
        .query("SELECT table_name FROM information_schema.tables ORDER BY table_name")
        .unwrap();
    let names: Vec<String> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.to_string(),
            _ => panic!("expected text"),
        })
        .collect();
    assert!(names.contains(&"users".to_string()));
    assert!(names.contains(&"orders".to_string()));
}

#[test]
fn information_schema_tables_marks_views() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT id FROM t").unwrap();

    let qr = conn
        .query("SELECT table_type FROM information_schema.tables WHERE table_name = 'v'")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("VIEW".into()));
}

#[test]
fn information_schema_columns_lists_columns_with_ordinal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
        .unwrap();

    let qr = conn
        .query(
            "SELECT column_name, ordinal_position, is_nullable, data_type \
             FROM information_schema.columns \
             WHERE table_name = 't' \
             ORDER BY ordinal_position",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Text("id".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Text("name".into()));
    assert_eq!(qr.rows[1][2], Value::Text("NO".into()));
    assert_eq!(qr.rows[2][0], Value::Text("age".into()));
    assert_eq!(qr.rows[2][2], Value::Text("YES".into()));
}

#[test]
fn information_schema_key_column_usage_lists_pk_and_fk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE kids (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))",
    )
    .unwrap();

    let qr = conn
        .query(
            "SELECT table_name, column_name, referenced_table_name, referenced_column_name \
             FROM information_schema.key_column_usage \
             WHERE table_name = 'kids' AND referenced_table_name IS NOT NULL",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("parent_id".into()));
    assert_eq!(qr.rows[0][2], Value::Text("parents".into()));
    assert_eq!(qr.rows[0][3], Value::Text("id".into()));
}

#[test]
fn information_schema_table_constraints_lists_pk_fk_check() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE kids (\
             id INTEGER PRIMARY KEY, \
             parent_id INTEGER REFERENCES parents(id), \
             age INTEGER CHECK (age > 0))",
    )
    .unwrap();

    let qr = conn
        .query(
            "SELECT constraint_type FROM information_schema.table_constraints \
             WHERE table_name = 'kids' ORDER BY constraint_type",
        )
        .unwrap();
    let kinds: Vec<String> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.to_string(),
            _ => panic!("expected text"),
        })
        .collect();
    assert!(kinds.contains(&"PRIMARY KEY".to_string()));
    assert!(kinds.contains(&"FOREIGN KEY".to_string()));
    assert!(kinds.contains(&"CHECK".to_string()));
}

#[test]
fn information_schema_join_tables_columns_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, n TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, n TEXT, x REAL)")
        .unwrap();

    let qr = conn
        .query(
            "SELECT t.table_name, COUNT(c.column_name) AS ncol \
             FROM information_schema.tables t \
             JOIN information_schema.columns c ON t.table_name = c.table_name \
             WHERE t.table_name IN ('a','b') \
             GROUP BY t.table_name \
             ORDER BY t.table_name",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("a".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Text("b".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(3));
}
