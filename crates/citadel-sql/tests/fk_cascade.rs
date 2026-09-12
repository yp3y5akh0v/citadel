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
        conn.execute("INSERT INTO child VALUES (10, 1), (20, 1)")
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
                vec![Value::Text("before".into()), Value::Integer(1)],
                vec![Value::Text("after".into()), Value::Integer(0)],
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

#[test]
fn complete_cascade_preserves_overflow_snapshots_savepoints_and_parent_returning() {
    for deferred in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        let timing = if deferred {
            "DEFERRABLE INITIALLY DEFERRED"
        } else {
            ""
        };
        conn.execute(&format!("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE {timing}, payload BLOB)")).unwrap();
        conn.execute("INSERT INTO parent VALUES (1)").unwrap();
        let payload = Value::Blob(vec![0x5a; 16_384]);
        let insert = conn
            .prepare("INSERT INTO child VALUES ($1, 1, $2)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        for id in 1..=8 {
            insert
                .execute(&[Value::Integer(id), payload.clone()])
                .unwrap();
        }
        conn.execute("COMMIT").unwrap();
        let mut old = db.begin_read();
        assert_eq!(old.table_entry_count(b"child").unwrap(), 8);
        conn.execute("BEGIN").unwrap();
        // For deferred constraints this identity remains queued after clearing;
        // commit must recognize that its final child row no longer exists.
        insert
            .execute(&[Value::Integer(9), payload.clone()])
            .unwrap();
        conn.execute("SAVEPOINT before_delete").unwrap();
        let delete = conn
            .prepare("DELETE FROM parent WHERE id = $1 RETURNING id")
            .unwrap();
        assert_eq!(
            delete.query_collect(&[Value::Integer(1)]).unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
        assert!(conn.query("SELECT id FROM child").unwrap().rows.is_empty());
        conn.execute("ROLLBACK TO SAVEPOINT before_delete").unwrap();
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM child").unwrap().rows,
            vec![vec![Value::Integer(9)]]
        );
        assert_eq!(delete_parent(&conn, true).unwrap(), 1);
        conn.execute("COMMIT").unwrap();
        assert!(index_entries(&db, "__fk_child_0").is_empty());
        let mut old_count = 0;
        old.table_for_each(b"child", |_, value| {
            assert!(value.len() > 16_384);
            old_count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(old_count, 8);
        assert!(db.manager().integrity_check().unwrap().is_ok());
    }
}

#[test]
fn complete_cascade_does_not_use_dangling_index_keys_as_coverage() {
    use citadel_sql::encoding::encode_composite_key;

    for missing_base_key in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
        conn.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
        conn.execute(if missing_base_key {
            "INSERT INTO child VALUES (10, 1), (20, 2)"
        } else {
            "INSERT INTO child VALUES (10, 1), (20, 1)"
        })
        .unwrap();
        let index_table = TableSchema::index_table_name("child", "__fk_child_0");
        let key =
            |parent, child| encode_composite_key(&[Value::Integer(parent), Value::Integer(child)]);
        let dangling = key(if missing_base_key { 1 } else { 2 }, 99);
        let mut wtx = db.begin_write().unwrap();
        if missing_base_key {
            assert!(wtx.table_delete(&index_table, &key(2, 20)).unwrap());
        }
        wtx.table_insert(&index_table, &dangling, &[]).unwrap();
        wtx.commit().unwrap();
        assert_eq!(delete_parent(&conn, true).unwrap(), 1);
        if missing_base_key {
            // Two hits and two base rows are insufficient: one hit points to
            // missing PK 99, so the unrelated unindexed row 20 must survive.
            assert_eq!(
                conn.query("SELECT id, p FROM child").unwrap().rows,
                vec![vec![Value::Integer(20), Value::Integer(2)]]
            );
            assert!(index_entries(&db, "__fk_child_0").is_empty());
        } else {
            assert!(conn.query("SELECT id FROM child").unwrap().rows.is_empty());
            assert_eq!(index_entries(&db, "__fk_child_0"), vec![(dangling, vec![])]);
        }
        // The deliberately inconsistent remaining index is not repaired here.
    }
}

#[test]
fn complete_cascade_preserves_composite_neighbors_nulls_and_descendant_actions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (a TEXT, b INTEGER, PRIMARY KEY(a, b))")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, FOREIGN KEY(a, b) REFERENCES parent(a, b) ON DELETE CASCADE)").unwrap();
    conn.execute("CREATE TABLE grandchild (id INTEGER PRIMARY KEY, child_id INTEGER REFERENCES child(id) ON DELETE CASCADE)").unwrap();
    conn.execute("INSERT INTO parent VALUES ('a', 1), ('a', 2), ('aa', 1)")
        .unwrap();
    conn.execute("INSERT INTO child VALUES (1, 'a', 1), (2, 'a', 1), (3, 'a', 2), (4, 'aa', 1), (5, NULL, 1)").unwrap();
    conn.execute("INSERT INTO grandchild VALUES (11, 1), (12, 2), (13, 3), (14, 4), (15, 5)")
        .unwrap();
    assert!(matches!(
        conn.execute("DELETE FROM parent WHERE a = 'a' AND b = 1")
            .unwrap(),
        ExecutionResult::RowsAffected(1)
    ));
    assert_eq!(
        conn.query("SELECT id FROM child ORDER BY id").unwrap().rows,
        vec![
            vec![Value::Integer(3)],
            vec![Value::Integer(4)],
            vec![Value::Integer(5)]
        ]
    );
    assert_eq!(
        conn.query("SELECT id FROM grandchild ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![Value::Integer(13)],
            vec![Value::Integer(14)],
            vec![Value::Integer(15)]
        ]
    );
    // A second parent statement resolves its own fresh hit set and counts.
    assert!(matches!(
        conn.execute("DELETE FROM parent").unwrap(),
        ExecutionResult::RowsAffected(2)
    ));
    assert_eq!(
        conn.query("SELECT id FROM child").unwrap().rows,
        vec![vec![Value::Integer(5)]]
    );
    assert_eq!(
        conn.query("SELECT id FROM grandchild").unwrap().rows,
        vec![vec![Value::Integer(15)]]
    );
    conn.execute("DELETE FROM child").unwrap();
    conn.execute("INSERT INTO parent VALUES ('a', 1)").unwrap();
    conn.execute("INSERT INTO child VALUES (6, 'a', 1), (7, 'a', 1)")
        .unwrap();
    conn.execute("INSERT INTO grandchild VALUES (16, 6), (17, 7)")
        .unwrap();
    assert!(matches!(
        conn.execute("DELETE FROM parent").unwrap(),
        ExecutionResult::RowsAffected(1)
    ));
    assert!(conn.query("SELECT id FROM child").unwrap().rows.is_empty());
    assert!(conn
        .query("SELECT id FROM grandchild")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn error_after_complete_cascade_rolls_back_parent_child_and_index() {
    for explicit in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
        conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, n INTEGER CHECK (n > 0))")
            .unwrap();
        conn.execute("INSERT INTO parent VALUES (1)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1), (20, 1)")
            .unwrap();
        conn.execute("CREATE TRIGGER parent_after AFTER DELETE ON parent FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.id, (SELECT COUNT(*) FROM child)); END").unwrap();
        let before = index_entries(&db, "__fk_child_0");
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        assert!(matches!(
            delete_parent(&conn, true),
            Err(SqlError::CheckViolation(_))
        ));
        if explicit {
            assert!(conn.execute("COMMIT").is_err());
        }
        assert_eq!(
            conn.query("SELECT id FROM parent").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
        assert_eq!(
            conn.query("SELECT id FROM child ORDER BY id").unwrap().rows,
            vec![vec![Value::Integer(10)], vec![Value::Integer(20)]]
        );
        assert_eq!(index_entries(&db, "__fk_child_0"), before);
        assert!(conn.query("SELECT id FROM log").unwrap().rows.is_empty());
    }
}
