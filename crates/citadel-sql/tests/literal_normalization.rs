use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"literal-normalization")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn missing_parameter(result: Result<ExecutionResult, SqlError>, expected: usize) {
    let error = result.unwrap_err();
    assert!(
        matches!(error, SqlError::ParameterCountMismatch { expected: n, got: 0 } if n == expected),
        "{error:?}"
    );
}

fn rejects_both(conn: &Connection<'_>, sql: &str, parameter: usize) {
    missing_parameter(conn.execute(sql), parameter);
    missing_parameter(conn.execute_params(sql, &[]), parameter);
}

#[test]
fn literal_values_do_not_bind_persisted_default_or_dependent_virtual_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("persisted.db");
    {
        let db = DatabaseBuilder::new(&path)
            .passphrase(b"literal-normalization")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, d INTEGER DEFAULT $1, v INTEGER GENERATED ALWAYS AS (d + 1) VIRTUAL)")
            .unwrap();
    }
    let db = DatabaseBuilder::new(&path)
        .passphrase(b"literal-normalization")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    rejects_both(&conn, "INSERT INTO items(id) VALUES (7)", 1);
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM items").unwrap().rows,
        vec![vec![Value::Integer(0)]]
    );
    conn.execute_params("INSERT INTO items(id) VALUES ($1)", &[Value::Integer(3)])
        .unwrap();
    assert_eq!(
        conn.query("SELECT id,d,v FROM items ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![
            Value::Integer(3),
            Value::Integer(3),
            Value::Integer(4)
        ]]
    );
}

#[test]
fn literal_values_do_not_bind_stored_or_indexed_virtual_generated_parameters() {
    for kind in ["STORED", "VIRTUAL"] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute(&format!("CREATE TABLE items (id INTEGER PRIMARY KEY, g INTEGER GENERATED ALWAYS AS (id + $1) {kind})"))
            .unwrap();
        if kind == "VIRTUAL" {
            // Make the virtual value an actual insert-time dependency.
            conn.execute("CREATE INDEX generated_key ON items((g + 0))")
                .unwrap();
        }
        rejects_both(&conn, "INSERT INTO items(id) VALUES (7)", 1);
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM items").unwrap().rows,
            vec![vec![Value::Integer(0)]]
        );
    }
}

#[test]
fn literal_values_do_not_bind_expression_index_parameters() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE INDEX parameter_key ON items (CAST($1 AS INTEGER))")
        .unwrap();
    rejects_both(&conn, "INSERT INTO items VALUES (7)", 1);
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM items").unwrap().rows,
        vec![vec![Value::Integer(0)]]
    );
}

#[test]
fn literal_admission_rechecks_altered_and_reloaded_schema() {
    for reload in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO items(id) VALUES (1)").unwrap();
        if reload {
            let other = Connection::open(&db).unwrap();
            other
                .execute("ALTER TABLE items ADD COLUMN d INTEGER DEFAULT $1")
                .unwrap();
            conn.refresh_schema().unwrap();
        } else {
            conn.execute("ALTER TABLE items ADD COLUMN d INTEGER DEFAULT $1")
                .unwrap();
        }
        rejects_both(&conn, "INSERT INTO items(id) VALUES (2)", 1);
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM items").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
    }
}

#[test]
fn literal_admission_rechecks_trigger_enable_disable() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    conn.execute("CREATE TRIGGER capture AFTER INSERT ON items FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.id, $2); END").unwrap();
    // Bare VALUES parameters use the trigger body's explicit empty slice. Keep
    // that existing error contract separate from generic expression evaluation.
    for result in [
        conn.execute("INSERT INTO items VALUES (9, 90)"),
        conn.execute_params("INSERT INTO items VALUES (9, 90)", &[]),
    ] {
        let error = result.unwrap_err();
        assert!(
            matches!(&error, SqlError::Parse(message) if message == "unbound parameter $2"),
            "{error:?}"
        );
    }
    conn.execute("DROP TRIGGER capture").unwrap();
    conn.execute("CREATE TRIGGER capture AFTER INSERT ON items FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.id, COALESCE($2, 0)); END").unwrap();
    conn.execute("ALTER TABLE items DISABLE TRIGGER capture")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 10)").unwrap();
    conn.execute("ALTER TABLE items ENABLE TRIGGER capture")
        .unwrap();
    rejects_both(&conn, "INSERT INTO items VALUES (2, 20)", 2);
    conn.execute("ALTER TABLE items DISABLE TRIGGER ALL")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (3, 30)").unwrap();
    conn.execute("ALTER TABLE items ENABLE TRIGGER ALL")
        .unwrap();
    rejects_both(&conn, "INSERT INTO items VALUES (4, 40)", 2);
    assert_eq!(
        conn.query("SELECT id FROM items ORDER BY id").unwrap().rows,
        vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
    );
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM audit").unwrap().rows,
        vec![vec![Value::Integer(0)]]
    );
}

#[test]
fn literal_values_do_not_bind_referenced_parent_lazy_parameters() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT COLLATE BINARY)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX parent_code ON parent(code COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO parent VALUES (1, 'Alpha')")
        .unwrap();
    conn.execute("ALTER TABLE parent ADD COLUMN d INTEGER DEFAULT $1")
        .unwrap();
    conn.execute("ALTER TABLE parent ADD COLUMN v INTEGER GENERATED ALWAYS AS (d + 1) VIRTUAL")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, code TEXT REFERENCES parent(code))")
        .unwrap();
    let parent = conn.table_schema("parent").unwrap();
    assert_eq!(parent.indices.len(), 1);
    let index = &parent.indices[0];
    assert!(index.unique && index.is_full_column_btree(&[1]));
    assert_eq!(
        parent.columns[1].collation,
        citadel_sql::types::Collation::Binary
    );
    assert_eq!(index.collation_at(0), citadel_sql::types::Collation::NoCase);
    // The broader index must recheck the parent's Binary equality, decoding
    // its physically missing default and dependent virtual column.
    rejects_both(&conn, "INSERT INTO child VALUES (2, 'Alpha')", 1);
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM child").unwrap().rows,
        vec![vec![Value::Integer(0)]]
    );
}

#[test]
fn exact_parent_equality_index_skips_unrelated_lazy_parameters() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT COLLATE NOCASE UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO parent VALUES (1, 'Alpha')")
        .unwrap();
    conn.execute("ALTER TABLE parent ADD COLUMN d INTEGER DEFAULT $1")
        .unwrap();
    conn.execute("ALTER TABLE parent ADD COLUMN v INTEGER GENERATED ALWAYS AS (d + 1) VIRTUAL")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, code TEXT REFERENCES parent(code))")
        .unwrap();
    // Both literal-normalized and explicit empty-binding routes can prove
    // parent membership without materializing unrelated parent columns.
    conn.execute("INSERT INTO child VALUES (2, 'ALPHA')")
        .unwrap();
    conn.execute_params("INSERT INTO child VALUES (3, 'alpha')", &[])
        .unwrap();
    assert_eq!(
        conn.query("SELECT id,code FROM child ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![Value::Integer(2), Value::Text("ALPHA".into())],
            vec![Value::Integer(3), Value::Text("alpha".into())],
        ]
    );
    assert!(matches!(
        conn.query("SELECT d FROM parent"),
        Err(SqlError::ParameterCountMismatch {
            expected: 1,
            got: 0
        })
    ));
}

#[test]
fn literal_normalization_preserves_parameter_free_defaults_generated_values_and_explicit_bindings()
{
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT, amount INTEGER DEFAULT 4, doubled INTEGER GENERATED ALWAYS AS (amount * 2) STORED, day DATE DEFAULT CURRENT_DATE)").unwrap();
    conn.execute("CREATE INDEX by_doubled ON items(doubled)")
        .unwrap();
    conn.execute("INSERT INTO items(id,label) VALUES (1, 'first')")
        .unwrap();
    conn.execute("INSERT INTO items(id,label) VALUES (2, 'it''s second')")
        .unwrap();
    conn.execute_params(
        "INSERT INTO items(id,label) VALUES ($1,$2)",
        &[Value::Integer(3), Value::Text("bound".into())],
    )
    .unwrap();
    assert_eq!(
        conn.query("SELECT id,label,amount,doubled FROM items ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Text("first".into()),
                Value::Integer(4),
                Value::Integer(8)
            ],
            vec![
                Value::Integer(2),
                Value::Text("it's second".into()),
                Value::Integer(4),
                Value::Integer(8)
            ],
            vec![
                Value::Integer(3),
                Value::Text("bound".into()),
                Value::Integer(4),
                Value::Integer(8)
            ],
        ]
    );
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM items WHERE day IS NOT NULL")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(3)]]
    );
}
