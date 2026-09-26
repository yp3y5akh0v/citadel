use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"compiled-insert-scope")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

#[test]
fn compiled_insert_fallback_binds_trigger_parameters_instead_of_ambient_values() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    conn.execute("CREATE TRIGGER capture AFTER INSERT ON items FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.id, COALESCE($2, 0)); END").unwrap();
    let insert = conn.prepare("INSERT INTO items VALUES ($1,$2)").unwrap();
    // Establish the existing autocommit generic-expression binding contract
    // before comparing the compiled active-write path with the same plan.
    citadel_sql::eval::with_scoped_params(&[Value::Integer(99), Value::Integer(99)], || {
        assert_eq!(
            insert
                .execute(&[Value::Integer(10), Value::Integer(11)])
                .unwrap(),
            1
        );
        assert_eq!(
            citadel_sql::eval::resolve_scoped_param(2).unwrap(),
            Value::Integer(99)
        );
    });
    assert_eq!(
        conn.query("SELECT value FROM audit WHERE id=10")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(11)]]
    );
    conn.execute("BEGIN").unwrap();
    citadel_sql::eval::with_scoped_params(&[Value::Integer(99), Value::Integer(99)], || {
        assert_eq!(
            insert
                .execute(&[Value::Integer(1), Value::Integer(7)])
                .unwrap(),
            1
        );
        assert_eq!(
            conn.query("SELECT value FROM audit WHERE id=1")
                .unwrap()
                .rows,
            vec![vec![Value::Integer(7)]]
        );
        assert_eq!(
            citadel_sql::eval::resolve_scoped_param(2).unwrap(),
            Value::Integer(99)
        );
    });
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn compiled_insert_fallback_binds_expression_index_parameters() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX by_parameter ON items (CAST($2 AS INTEGER))")
        .unwrap();
    let insert = conn.prepare("INSERT INTO items VALUES ($1,$2)").unwrap();
    conn.execute("BEGIN").unwrap();
    citadel_sql::eval::with_scoped_params(&[Value::Integer(99), Value::Integer(99)], || {
        for (id, value) in [(1, 7), (2, 8)] {
            assert_eq!(
                insert
                    .execute(&[Value::Integer(id), Value::Integer(value)])
                    .unwrap(),
                1
            );
        }
        assert_eq!(
            citadel_sql::eval::resolve_scoped_param(2).unwrap(),
            Value::Integer(99)
        );
    });
    assert_eq!(
        conn.query("SELECT id,value FROM items ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![Value::Integer(1), Value::Integer(7)],
            vec![Value::Integer(2), Value::Integer(8)]
        ]
    );
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn compiled_insert_fallback_binds_lazy_parameters_in_referenced_parent_rows() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT COLLATE NOCASE UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO parent VALUES (1, 'Alpha')")
        .unwrap();
    conn.execute("ALTER TABLE parent ADD COLUMN d INTEGER DEFAULT $1")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, code TEXT REFERENCES parent(code))")
        .unwrap();
    let insert = conn.prepare("INSERT INTO child VALUES ($1,$2)").unwrap();
    conn.execute("BEGIN").unwrap();
    citadel_sql::eval::with_scoped_params(&[], || {
        assert_eq!(
            insert
                .execute(&[Value::Integer(2), Value::Text("Alpha".into())])
                .unwrap(),
            1
        );
        assert!(citadel_sql::eval::resolve_scoped_param(1).is_err());
    });
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM child").unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
    conn.execute("ROLLBACK").unwrap();
}
