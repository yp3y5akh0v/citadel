use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, TableSchema, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("cascade.db"))
        .passphrase(b"cascade-test")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn index_entries(db: &citadel::Database, name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let table = TableSchema::index_table_name("child", name);
    let mut entries = Vec::new();
    db.begin_read()
        .table_for_each(&table, |key, value| {
            entries.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .unwrap();
    entries
}

fn delete_parent(conn: &Connection<'_>, prepared: bool) -> Result<u64, SqlError> {
    if prepared {
        conn.prepare("DELETE FROM parent WHERE id = $1")?
            .execute(&[Value::Integer(1)])
    } else {
        match conn.execute("DELETE FROM parent WHERE id = 1")? {
            ExecutionResult::RowsAffected(n) => Ok(n),
            other => panic!("unexpected delete result: {other:?}"),
        }
    }
}

#[test]
fn cascade_delete_maintains_each_index_with_the_same_fk_columns() {
    for definition in [
        "CREATE INDEX extra ON child (p)",
        "CREATE UNIQUE INDEX extra ON child (p)",
        "CREATE INDEX extra ON child (p) WHERE id > 0",
        "CREATE INDEX extra ON child (p, id + 1)",
    ] {
        for (prepared, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
                .unwrap();
            conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
            conn.execute(definition).unwrap();
            conn.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
            conn.execute("INSERT INTO child VALUES (20, 2)").unwrap();
            let expected = index_entries(&db, "extra");
            assert!(!expected.is_empty());
            conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
            assert_eq!(index_entries(&db, "extra").len(), expected.len() + 1);
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            assert_eq!(delete_parent(&conn, prepared).unwrap(), 1);
            if explicit {
                conn.execute("COMMIT").unwrap();
            }
            assert_eq!(
                index_entries(&db, "extra"),
                expected,
                "{definition}, prepared={prepared}, explicit={explicit}"
            );
            assert_eq!(
                conn.query("SELECT id FROM child ORDER BY id").unwrap().rows,
                vec![vec![Value::Integer(20)]]
            );
            conn.execute("INSERT INTO parent VALUES (1)").unwrap();
            conn.execute("INSERT INTO child VALUES (11, 1)").unwrap();
        }
    }
}

#[test]
fn cascade_delete_removes_fts_and_gin_postings() {
    for (prepared, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE, body TEXT, data JSONB)").unwrap();
        conn.execute("CREATE INDEX child_fts ON child USING fts (body)")
            .unwrap();
        conn.execute("CREATE INDEX child_gin ON child USING gin (data)")
            .unwrap();
        conn.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
        conn.execute(
            "INSERT INTO child VALUES (20, 2, 'shared retained', '{\"shared\":true,\"n\":20}')",
        )
        .unwrap();
        let expected_fts = index_entries(&db, "child_fts");
        let expected_gin = index_entries(&db, "child_gin");
        assert!(!expected_fts.is_empty() && !expected_gin.is_empty());
        conn.execute(
            "INSERT INTO child VALUES (10, 1, 'shared removed', '{\"shared\":true,\"n\":10}')",
        )
        .unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        assert_eq!(delete_parent(&conn, prepared).unwrap(), 1);
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
        assert_eq!(index_entries(&db, "child_fts"), expected_fts);
        assert_eq!(index_entries(&db, "child_gin"), expected_gin);
        assert!(conn
            .query("SELECT id FROM child WHERE body @@ to_tsquery('removed')")
            .unwrap()
            .rows
            .is_empty());
        assert_eq!(
            conn.query("SELECT id FROM child WHERE body @@ to_tsquery('shared')")
                .unwrap()
                .rows,
            vec![vec![Value::Integer(20)]]
        );
    }
}

#[test]
fn cascade_delete_fires_before_and_after_row_triggers_around_the_delete() {
    for (prepared, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
        conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, phase TEXT, remaining INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1), (20, 2)")
            .unwrap();
        conn.execute("CREATE TRIGGER child_before BEFORE DELETE ON child FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.id * 10, 'before', (SELECT COUNT(*) FROM child)); END").unwrap();
        conn.execute("CREATE TRIGGER child_after AFTER DELETE ON child FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.id * 10 + 1, 'after', (SELECT COUNT(*) FROM child)); END").unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        assert_eq!(delete_parent(&conn, prepared).unwrap(), 1);
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
        assert_eq!(
            conn.query("SELECT phase, remaining FROM log ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![Value::Text("before".into()), Value::Integer(2)],
                vec![Value::Text("after".into()), Value::Integer(1)],
            ]
        );
    }
}

#[test]
fn cascade_before_trigger_failure_preserves_parent_child_and_indexes() {
    for prepared in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
        conn.execute("CREATE UNIQUE INDEX extra ON child (p)")
            .unwrap();
        conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, n INTEGER CHECK (n > 0))")
            .unwrap();
        conn.execute("INSERT INTO parent VALUES (1)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
        let expected = index_entries(&db, "extra");
        conn.execute("CREATE TRIGGER child_before BEFORE DELETE ON child FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.id, 0); END").unwrap();
        assert!(matches!(
            delete_parent(&conn, prepared),
            Err(SqlError::CheckViolation(_))
        ));
        assert_eq!(
            conn.query("SELECT id FROM parent").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
        assert_eq!(
            conn.query("SELECT id FROM child").unwrap().rows,
            vec![vec![Value::Integer(10)]]
        );
        assert_eq!(index_entries(&db, "extra"), expected);
        assert!(conn.query("SELECT id FROM log").unwrap().rows.is_empty());
    }
}

#[test]
fn cascade_index_and_trigger_changes_rollback_with_the_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
    conn.execute("CREATE UNIQUE INDEX extra ON child (p)")
        .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TRIGGER child_before BEFORE DELETE ON child FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.id); END").unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    let expected = index_entries(&db, "extra");
    conn.execute("BEGIN").unwrap();
    assert_eq!(delete_parent(&conn, true).unwrap(), 1);
    assert!(conn.query("SELECT id FROM child").unwrap().rows.is_empty());
    assert_eq!(
        conn.query("SELECT id FROM log").unwrap().rows,
        vec![vec![Value::Integer(10)]]
    );
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(
        conn.query("SELECT id FROM child").unwrap().rows,
        vec![vec![Value::Integer(10)]]
    );
    assert_eq!(index_entries(&db, "extra"), expected);
    assert!(conn.query("SELECT id FROM log").unwrap().rows.is_empty());
}

#[test]
fn update_cascade_moves_primary_keys_through_a_deep_worklist() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE chain0 (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO chain0 VALUES (1)").unwrap();
    for depth in 1..128 {
        conn.execute(&format!("CREATE TABLE chain{depth} (id INTEGER PRIMARY KEY REFERENCES chain{}(id) ON UPDATE CASCADE ON DELETE CASCADE)", depth - 1)).unwrap();
        conn.execute(&format!("INSERT INTO chain{depth} VALUES (1)"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.prepare("UPDATE chain0 SET id = $1")
        .unwrap()
        .execute(&[Value::Integer(2)])
        .unwrap();
    for depth in 0..128 {
        assert_eq!(
            conn.query(&format!("SELECT id FROM chain{depth}"))
                .unwrap()
                .rows,
            vec![vec![Value::Integer(2)]]
        );
    }
    conn.execute("DELETE FROM chain0").unwrap();
    assert!(conn
        .query("SELECT id FROM chain127")
        .unwrap()
        .rows
        .is_empty());
}
