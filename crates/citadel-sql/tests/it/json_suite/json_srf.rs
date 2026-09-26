use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn jsonb_array_elements_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT * FROM jsonb_array_elements(CAST('[10,20,30]' AS JSONB))")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn jsonb_array_elements_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT value FROM jsonb_array_elements_text(CAST('[\"a\",\"b\",\"c\"]' AS JSONB))")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Text("a".into()));
    assert_eq!(qr.rows[1][0], Value::Text("b".into()));
    assert_eq!(qr.rows[2][0], Value::Text("c".into()));
}

#[test]
fn jsonb_each_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT key FROM jsonb_each(CAST('{\"x\":1,\"y\":2}' AS JSONB)) ORDER BY key")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("x".into()));
    assert_eq!(qr.rows[1][0], Value::Text("y".into()));
}

#[test]
fn jsonb_object_keys_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT * FROM jsonb_object_keys(CAST('{\"a\":1,\"b\":2,\"c\":3}' AS JSONB)) ORDER BY 1")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Text("a".into()));
    assert_eq!(qr.rows[1][0], Value::Text("b".into()));
    assert_eq!(qr.rows[2][0], Value::Text("c".into()));
}

#[test]
fn srf_null_arg_returns_empty_set() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT * FROM jsonb_array_elements(CAST(NULL AS JSONB))")
        .unwrap();
    assert_eq!(qr.rows.len(), 0);
}

fn int(value: i64) -> Value {
    Value::Integer(value)
}

fn text(value: &str) -> Value {
    Value::Text(value.into())
}

/// A one-row table `t`, so a table function aliased `t` that a subquery
/// mistook for the table would count its own rows.
fn setup_function_sources(conn: &Connection) {
    for sql in [
        "CREATE TABLE t (a INTEGER PRIMARY KEY)",
        "INSERT INTO t VALUES (7)",
        "CREATE TABLE c (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO c VALUES (1, 'Books')",
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, doc JSON)",
        "INSERT INTO docs VALUES (1, '[1,2,3]'), (2, '[4]')",
    ] {
        conn.execute(sql).unwrap();
    }
}

#[test]
fn table_functions_read_the_same_in_every_transaction_and_scope() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_function_sources(&conn);
    let cases: Vec<(&str, Vec<Vec<Value>>)> = vec![
        (
            "SELECT value FROM json_array_elements_text('[\"a\",\"b\"]'::JSON) ORDER BY 1",
            vec![vec![text("a")], vec![text("b")]],
        ),
        (
            "SELECT * FROM JSON_TABLE(CAST('[{\"a\":1,\"b\":\"x\"},{\"a\":2,\"b\":\"y\"}]' AS JSONB), \
             '$[*]' COLUMNS (a INT PATH '$.a', b TEXT PATH '$.b')) AS jt ORDER BY 1",
            vec![vec![int(1), text("x")], vec![int(2), text("y")]],
        ),
        (
            "SELECT id, (SELECT COUNT(*) FROM json_array_elements(docs.doc)) FROM docs ORDER BY 1",
            vec![vec![int(1), int(3)], vec![int(2), int(1)]],
        ),
        (
            "SELECT j.value, d.y FROM json_array_elements_text('[\"1\",\"2\"]'::JSON) AS j, \
             LATERAL (SELECT CAST(j.value AS INTEGER) * 2 AS y) AS d ORDER BY 1",
            vec![vec![text("1"), int(2)], vec![text("2"), int(4)]],
        ),
        (
            "SELECT value, (SELECT COUNT(*) FROM t) \
             FROM json_array_elements_text('[\"p\",\"q\"]'::JSON) AS t ORDER BY 1",
            vec![vec![text("p"), int(1)], vec![text("q"), int(1)]],
        ),
        (
            "SELECT t.a, (SELECT COUNT(*) FROM t) FROM JSON_TABLE(CAST('[{\"a\":1},{\"a\":2}]' AS JSONB), \
             '$[*]' COLUMNS (a INT PATH '$.a')) AS t ORDER BY 1",
            vec![vec![int(1), int(1)], vec![int(2), int(1)]],
        ),
    ];
    for transaction in [false, true] {
        if transaction {
            conn.execute("BEGIN").unwrap();
        }
        for (sql, expected) in &cases {
            let rows = conn.query(sql).map(|qr| qr.rows);
            assert_eq!(
                rows.as_ref().ok(),
                Some(expected),
                "{sql} (transaction: {transaction}): {rows:?}"
            );
        }
        if transaction {
            conn.execute("COMMIT").unwrap();
        }
    }
}

#[test]
fn statements_that_write_read_table_functions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_function_sources(&conn);
    for sql in [
        "INSERT INTO t (a) SELECT CAST(value AS INTEGER) FROM json_array_elements_text('[\"8\",\"9\"]'::JSON)",
        "UPDATE c SET name = (SELECT MAX(value) FROM json_array_elements_text('[\"x\",\"y\"]'::JSON))",
        "DELETE FROM t WHERE a IN (SELECT CAST(value AS INTEGER) FROM json_array_elements_text('[\"8\"]'::JSON))",
    ] {
        conn.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    assert_eq!(
        conn.query("SELECT a FROM t ORDER BY 1").unwrap().rows,
        vec![vec![int(7)], vec![int(9)]]
    );
    assert_eq!(
        conn.query("SELECT name FROM c").unwrap().rows,
        vec![vec![text("y")]]
    );
}

#[test]
fn arguments_to_a_relation_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_function_sources(&conn);
    for transaction in [false, true] {
        if transaction {
            conn.execute("BEGIN").unwrap();
        }
        match conn.query("SELECT * FROM c(1)") {
            Err(SqlError::Unsupported(message)) => assert_eq!(message, "table function: c"),
            other => panic!("transaction {transaction}: {other:?}"),
        }
        let zones = conn.query("SELECT COUNT(*) FROM timezone_names()").unwrap();
        assert!(matches!(zones.rows[0][0], Value::Integer(n) if n > 0));
        if transaction {
            conn.execute("COMMIT").unwrap();
        }
    }
}
