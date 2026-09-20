use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"compiled-temp-insert-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

#[test]
fn prepared_insert_routes_integer_and_coercible_text_to_temp_without_base() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TEMP TABLE items (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    let insert = conn.prepare("INSERT INTO items VALUES ($1, $2)").unwrap();

    conn.execute("BEGIN").unwrap();
    // Text forces the checked bind/coercion path; integer parameters use the
    // compiled template. Both must resolve the same physical TEMP table.
    assert_eq!(
        insert
            .execute(&[Value::Text("2".into()), Value::Text("20".into())])
            .unwrap(),
        1
    );
    let integer_result = insert.execute(&[Value::Integer(1), Value::Integer(10)]);
    assert!(
        matches!(integer_result, Ok(1)),
        "compiled INSERT missed the TEMP table: {integer_result:?}"
    );
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        conn.query("SELECT id, n FROM items ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
        ]
    );
}

#[test]
fn prepared_insert_keeps_another_connections_base_unchanged_under_temp_shadowing() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TEMP TABLE items (id INTEGER PRIMARY KEY, marker TEXT) STRICT")
        .unwrap();
    let insert = conn
        .prepare("INSERT INTO items VALUES ($1, 'routed')")
        .unwrap();

    // This connection has no TEMP alias, so it can create a permanent table
    // with the same user-visible name without changing the TEMP contract.
    let base_conn = Connection::open(&db).unwrap();
    base_conn
        .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, marker TEXT) STRICT")
        .unwrap();
    base_conn
        .execute("INSERT INTO items VALUES (-1, 'base')")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    assert_eq!(insert.execute(&[Value::Text("2".into())]).unwrap(), 1);
    assert_eq!(insert.execute(&[Value::Integer(1)]).unwrap(), 1);
    conn.execute("COMMIT").unwrap();
    let temp_rows = conn
        .query("SELECT id, marker FROM items ORDER BY id")
        .unwrap()
        .rows;

    conn.execute("DROP TABLE items").unwrap();
    assert_eq!(
        base_conn
            .query("SELECT id, marker FROM items ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(-1), Value::Text("base".into())]],
        "the compiled integer path wrote through the TEMP shadow into the base table"
    );
    assert_eq!(
        temp_rows,
        vec![
            vec![Value::Integer(1), Value::Text("routed".into())],
            vec![Value::Integer(2), Value::Text("routed".into())],
        ]
    );

    // Discover the other connection's permanent table after dropping TEMP;
    // the same prepared statement must now resolve that table.
    conn.refresh_schema().unwrap();
    conn.execute("BEGIN").unwrap();
    assert_eq!(insert.execute(&[Value::Integer(3)]).unwrap(), 1);
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        conn.query("SELECT id, marker FROM items ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![Value::Integer(-1), Value::Text("base".into())],
            vec![Value::Integer(3), Value::Text("routed".into())],
        ]
    );
}
