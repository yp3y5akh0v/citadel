use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("fk-followup.db"))
        .passphrase(b"fk-followup-test")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn dropping_fk_backing_index_rejects_partial_or_expression_substitutes() {
    for definition in [
        "CREATE INDEX extra ON child (p) WHERE id < 0",
        "CREATE INDEX extra ON child (p, id + 1)",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
        conn.execute(definition).unwrap();
        assert!(
            matches!(
                conn.execute("DROP INDEX __fk_child_0"),
                Err(SqlError::Unsupported(_))
            ),
            "{definition}"
        );
        conn.execute("INSERT INTO parent VALUES (1)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
        conn.execute("DELETE FROM parent WHERE id = 1").unwrap();
        assert!(conn.query("SELECT id FROM child").unwrap().rows.is_empty());
    }
}

#[test]
fn dropping_fk_backing_index_rejects_an_inverted_substitute() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id TEXT PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p TEXT REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
    conn.execute("CREATE INDEX extra ON child USING fts (p)")
        .unwrap();
    assert!(matches!(
        conn.execute("DROP INDEX __fk_child_0"),
        Err(SqlError::Unsupported(_))
    ));
}

#[test]
fn set_default_requires_the_replacement_parent_to_exist() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER DEFAULT 99 REFERENCES parent(id) ON DELETE SET DEFAULT)").unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    assert!(matches!(
        conn.execute("DELETE FROM parent WHERE id = 1"),
        Err(SqlError::ForeignKeyViolation(_))
    ));
    assert_eq!(
        conn.query("SELECT p FROM child").unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
    assert_eq!(
        conn.query("SELECT id FROM parent").unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn update_cascade_recomputes_stored_generated_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON UPDATE CASCADE, g INTEGER GENERATED ALWAYS AS (p * 2) STORED)").unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child (id, p) VALUES (10, 1)")
        .unwrap();
    conn.execute("UPDATE parent SET id = 2 WHERE id = 1")
        .unwrap();
    assert_eq!(
        conn.query("SELECT p, g FROM child").unwrap().rows,
        vec![vec![Value::Integer(2), Value::Integer(4)]]
    );
}

#[test]
fn set_null_preserves_child_check_constraints() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER CHECK (p IS NOT NULL) REFERENCES parent(id) ON DELETE SET NULL)").unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    assert!(matches!(
        conn.execute("DELETE FROM parent WHERE id = 1"),
        Err(SqlError::CheckViolation(_))
    ));
    assert_eq!(
        conn.query("SELECT p FROM child").unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn update_cascade_moves_a_child_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY REFERENCES parent(id) ON UPDATE CASCADE)",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (1)").unwrap();
    conn.execute("UPDATE parent SET id = 2 WHERE id = 1")
        .unwrap();
    assert_eq!(
        conn.query("SELECT id FROM child").unwrap().rows,
        vec![vec![Value::Integer(2)]]
    );
    assert!(conn
        .query("SELECT id FROM child WHERE id = 1")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        conn.query("SELECT id FROM child WHERE id = 2")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(2)]]
    );
}

#[test]
fn set_null_dispatches_child_update_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE SET NULL)").unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, old_p INTEGER, new_p INTEGER)")
        .unwrap();
    conn.execute("CREATE TRIGGER child_after AFTER UPDATE ON child FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.id, OLD.p, NEW.p); END").unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    conn.execute("DELETE FROM parent WHERE id = 1").unwrap();
    assert_eq!(
        conn.query("SELECT old_p, new_p FROM log").unwrap().rows,
        vec![vec![Value::Integer(1), Value::Null]]
    );
}

#[test]
fn set_default_cannot_retain_the_deleted_parent_key() {
    for (prepared, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER DEFAULT 1 REFERENCES parent(id) ON DELETE SET DEFAULT)").unwrap();
        conn.execute("INSERT INTO parent VALUES (1)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        let result = if prepared {
            conn.prepare("DELETE FROM parent WHERE id = $1")
                .unwrap()
                .execute(&[Value::Integer(1)])
                .map(|_| ())
        } else {
            conn.execute("DELETE FROM parent WHERE id = 1").map(|_| ())
        };
        assert!(matches!(result, Err(SqlError::ForeignKeyViolation(_))));
        if explicit {
            conn.execute("ROLLBACK").unwrap();
        }
        assert_eq!(
            conn.query("SELECT id FROM parent").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
        assert_eq!(
            conn.query("SELECT p FROM child").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
    }
}

#[test]
fn cascade_update_triggers_observe_parent_mutation_order_and_rollback() {
    for (prepared, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON UPDATE CASCADE)").unwrap();
        conn.execute(
            "CREATE TABLE log (id INTEGER PRIMARY KEY, parent_key INTEGER, child_key INTEGER)",
        )
        .unwrap();
        conn.execute("INSERT INTO parent VALUES (1)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
        for (name, timing, table, event_id) in [
            ("parent_before", "BEFORE", "parent", 1),
            ("child_before", "BEFORE", "child", 2),
            ("child_after", "AFTER", "child", 3),
            ("parent_after", "AFTER", "parent", 4),
        ] {
            conn.execute(&format!("CREATE TRIGGER {name} {timing} UPDATE ON {table} FOR EACH ROW BEGIN INSERT INTO log VALUES ({event_id}, (SELECT MIN(id) FROM parent), (SELECT MIN(p) FROM child)); END")).unwrap();
        }
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        if prepared {
            assert_eq!(
                conn.prepare("UPDATE parent SET id = $1 WHERE id = $2")
                    .unwrap()
                    .execute(&[Value::Integer(2), Value::Integer(1)])
                    .unwrap(),
                1
            );
        } else {
            conn.execute("UPDATE parent SET id = 2 WHERE id = 1")
                .unwrap();
        }
        assert_eq!(
            conn.query("SELECT parent_key, child_key FROM log ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![Value::Integer(1), Value::Integer(1)],
                vec![Value::Integer(2), Value::Integer(1)],
                vec![Value::Integer(2), Value::Integer(2)],
                vec![Value::Integer(2), Value::Integer(2)],
            ]
        );
        if explicit {
            conn.execute("ROLLBACK").unwrap();
            assert_eq!(
                conn.query("SELECT id FROM parent").unwrap().rows,
                vec![vec![Value::Integer(1)]]
            );
            assert_eq!(
                conn.query("SELECT p FROM child").unwrap().rows,
                vec![vec![Value::Integer(1)]]
            );
            assert!(conn.query("SELECT id FROM log").unwrap().rows.is_empty());
        }
    }
}

#[test]
fn set_null_update_triggers_observe_the_deleted_parent_and_rollback() {
    for (prepared, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE SET NULL)").unwrap();
        conn.execute(
            "CREATE TABLE log (id INTEGER PRIMARY KEY, parent_count INTEGER, child_key INTEGER)",
        )
        .unwrap();
        conn.execute("INSERT INTO parent VALUES (1)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
        for (name, timing, event, table, event_id) in [
            ("parent_before", "BEFORE", "DELETE", "parent", 1),
            ("child_before", "BEFORE", "UPDATE", "child", 2),
            ("child_after", "AFTER", "UPDATE", "child", 3),
            ("parent_after", "AFTER", "DELETE", "parent", 4),
        ] {
            conn.execute(&format!("CREATE TRIGGER {name} {timing} {event} ON {table} FOR EACH ROW BEGIN INSERT INTO log VALUES ({event_id}, (SELECT COUNT(*) FROM parent), (SELECT MIN(p) FROM child)); END")).unwrap();
        }
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        if prepared {
            assert_eq!(
                conn.prepare("DELETE FROM parent WHERE id = $1")
                    .unwrap()
                    .execute(&[Value::Integer(1)])
                    .unwrap(),
                1
            );
        } else {
            conn.execute("DELETE FROM parent WHERE id = 1").unwrap();
        }
        assert_eq!(
            conn.query("SELECT parent_count, child_key FROM log ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![Value::Integer(1), Value::Integer(1)],
                vec![Value::Integer(0), Value::Integer(1)],
                vec![Value::Integer(0), Value::Null],
                vec![Value::Integer(0), Value::Null],
            ]
        );
        if explicit {
            conn.execute("ROLLBACK").unwrap();
            assert_eq!(
                conn.query("SELECT id FROM parent").unwrap().rows,
                vec![vec![Value::Integer(1)]]
            );
            assert_eq!(
                conn.query("SELECT p FROM child").unwrap().rows,
                vec![vec![Value::Integer(1)]]
            );
            assert!(conn.query("SELECT id FROM log").unwrap().rows.is_empty());
        }
    }
}

#[test]
fn before_parent_update_can_remove_restricting_children() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON UPDATE RESTRICT)").unwrap();
    conn.execute("CREATE TRIGGER parent_before BEFORE UPDATE ON parent FOR EACH ROW BEGIN DELETE FROM child WHERE p = OLD.id; END").unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    conn.execute("UPDATE parent SET id = 2 WHERE id = 1")
        .unwrap();
    assert_eq!(
        conn.query("SELECT id FROM parent").unwrap().rows,
        vec![vec![Value::Integer(2)]]
    );
    assert!(conn.query("SELECT id FROM child").unwrap().rows.is_empty());
}

#[test]
fn parent_update_finishes_each_rows_actions_before_mutating_the_next_row() {
    for (prepared, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON UPDATE CASCADE)").unwrap();
        conn.execute(
            "CREATE TABLE log (id INTEGER PRIMARY KEY, parent_sum INTEGER, child_sum INTEGER)",
        )
        .unwrap();
        conn.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1), (20, 2)")
            .unwrap();
        conn.execute("CREATE TRIGGER parent_before BEFORE UPDATE ON parent FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.id * 10, (SELECT SUM(id) FROM parent), (SELECT SUM(p) FROM child)); END").unwrap();
        conn.execute("CREATE TRIGGER parent_after AFTER UPDATE ON parent FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.id * 10 + 1, (SELECT SUM(id) FROM parent), (SELECT SUM(p) FROM child)); END").unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        if prepared {
            assert_eq!(
                conn.prepare("UPDATE parent SET id = id + 10")
                    .unwrap()
                    .execute(&[])
                    .unwrap(),
                2
            );
        } else {
            conn.execute("UPDATE parent SET id = id + 10").unwrap();
        }
        assert_eq!(
            conn.query("SELECT parent_sum, child_sum FROM log ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![Value::Integer(3), Value::Integer(3)],
                vec![Value::Integer(13), Value::Integer(13)],
                vec![Value::Integer(13), Value::Integer(13)],
                vec![Value::Integer(23), Value::Integer(23)],
            ]
        );
        if explicit {
            conn.execute("ROLLBACK").unwrap();
            assert_eq!(
                conn.query("SELECT id FROM parent ORDER BY id")
                    .unwrap()
                    .rows,
                vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
            );
            assert!(conn.query("SELECT id FROM log").unwrap().rows.is_empty());
        }
    }
}

#[test]
fn parent_delete_finishes_each_rows_actions_before_mutating_the_next_row() {
    for (prepared, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
        conn.execute(
            "CREATE TABLE log (id INTEGER PRIMARY KEY, parent_count INTEGER, child_count INTEGER)",
        )
        .unwrap();
        conn.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1), (20, 2)")
            .unwrap();
        conn.execute("CREATE TRIGGER parent_before BEFORE DELETE ON parent FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.id * 10, (SELECT COUNT(*) FROM parent), (SELECT COUNT(*) FROM child)); END").unwrap();
        conn.execute("CREATE TRIGGER parent_after AFTER DELETE ON parent FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.id * 10 + 1, (SELECT COUNT(*) FROM parent), (SELECT COUNT(*) FROM child)); END").unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        if prepared {
            assert_eq!(
                conn.prepare("DELETE FROM parent")
                    .unwrap()
                    .execute(&[])
                    .unwrap(),
                2
            );
        } else {
            conn.execute("DELETE FROM parent").unwrap();
        }
        assert_eq!(
            conn.query("SELECT parent_count, child_count FROM log ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![Value::Integer(2), Value::Integer(2)],
                vec![Value::Integer(1), Value::Integer(1)],
                vec![Value::Integer(1), Value::Integer(1)],
                vec![Value::Integer(0), Value::Integer(0)],
            ]
        );
        if explicit {
            conn.execute("ROLLBACK").unwrap();
            assert_eq!(
                conn.query("SELECT id FROM parent ORDER BY id")
                    .unwrap()
                    .rows,
                vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
            );
            assert!(conn.query("SELECT id FROM log").unwrap().rows.is_empty());
        }
    }
}

#[test]
fn collated_fk_backing_indexes_recheck_folded_text_candidates() {
    for (collation, parent_one, parent_two) in [("NOCASE", "Key", "key"), ("RTRIM", "key ", "key")]
    {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id TEXT PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p TEXT REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
        conn.execute(&format!(
            "CREATE INDEX extra ON child (p COLLATE {collation})"
        ))
        .unwrap();
        conn.execute("DROP INDEX __fk_child_0").unwrap();
        let parent_insert = conn.prepare("INSERT INTO parent VALUES ($1)").unwrap();
        let child_insert = conn.prepare("INSERT INTO child VALUES ($1, $2)").unwrap();
        for (id, key) in [(10, parent_one), (20, parent_two)] {
            parent_insert.execute(&[Value::Text(key.into())]).unwrap();
            child_insert
                .execute(&[Value::Integer(id), Value::Text(key.into())])
                .unwrap();
        }
        conn.prepare("DELETE FROM parent WHERE id = $1")
            .unwrap()
            .execute(&[Value::Text(parent_one.into())])
            .unwrap();
        assert_eq!(
            conn.query("SELECT id FROM child").unwrap().rows,
            vec![vec![Value::Integer(20)]]
        );
    }
}

#[test]
fn update_cascade_generated_overflow_rolls_back_parent_and_child() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON UPDATE CASCADE, g INTEGER GENERATED ALWAYS AS (p * 2) STORED)").unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child (id, p) VALUES (10, 1)")
        .unwrap();
    let err = conn
        .execute("UPDATE parent SET id = 9223372036854775807 WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::IntegerOverflow));
    assert_eq!(
        conn.query("SELECT id FROM parent").unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
    assert_eq!(
        conn.query("SELECT p, g FROM child").unwrap().rows,
        vec![vec![Value::Integer(1), Value::Integer(2)]]
    );
}

#[test]
fn descendant_trigger_changes_pending_cascade_rows_without_leaving_stale_indexes() {
    for (prepared, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
        conn.execute("CREATE TABLE grandchild (id INTEGER PRIMARY KEY, c INTEGER REFERENCES child(id) ON DELETE CASCADE)").unwrap();
        conn.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
        conn.execute("INSERT INTO child VALUES (30, 2)").unwrap();

        let index_table = citadel_sql::TableSchema::index_table_name("child", "__fk_child_0");
        let read_index = || {
            let mut entries = Vec::new();
            db.begin_read()
                .table_for_each(&index_table, |key, value| {
                    entries.push((key.to_vec(), value.to_vec()));
                    Ok(())
                })
                .unwrap();
            entries
        };
        let expected_index = read_index();
        assert_eq!(expected_index.len(), 1);

        conn.execute("INSERT INTO child VALUES (10, 1), (20, 1)")
            .unwrap();
        conn.execute("INSERT INTO grandchild VALUES (100, 10)")
            .unwrap();
        conn.execute("CREATE TRIGGER move_pending_child BEFORE DELETE ON grandchild FOR EACH ROW BEGIN UPDATE child SET p = 2 WHERE id = 20; END").unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        if prepared {
            conn.prepare("DELETE FROM parent WHERE id = $1")
                .unwrap()
                .execute(&[Value::Integer(1)])
                .unwrap();
        } else {
            conn.execute("DELETE FROM parent WHERE id = 1").unwrap();
        }
        if explicit {
            conn.execute("COMMIT").unwrap();
        }

        assert_eq!(
            conn.query("SELECT id, p FROM child ORDER BY id")
                .unwrap()
                .rows,
            vec![vec![Value::Integer(30), Value::Integer(2)]]
        );
        assert!(conn
            .query("SELECT id FROM grandchild")
            .unwrap()
            .rows
            .is_empty());
        assert_eq!(
            read_index(),
            expected_index,
            "prepared={prepared}, explicit={explicit}"
        );
    }
}

#[test]
fn null_unique_parent_values_do_not_match_unrelated_null_child_references() {
    for action in ["CASCADE", "RESTRICT", "NO ACTION"] {
        for statement in [
            "DELETE FROM parent WHERE id = 1",
            "UPDATE parent SET u = 8 WHERE id = 1",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, u INTEGER UNIQUE)")
                .unwrap();
            conn.execute(&format!(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(u) ON DELETE {action} ON UPDATE {action})"
            ))
            .unwrap();
            conn.execute("INSERT INTO parent VALUES (1, NULL), (7, 7)")
                .unwrap();
            conn.execute("INSERT INTO child VALUES (10, NULL), (20, 7)")
                .unwrap();
            conn.execute(statement).unwrap();
            assert_eq!(
                conn.query("SELECT id, p FROM child ORDER BY id")
                    .unwrap()
                    .rows,
                vec![
                    vec![Value::Integer(10), Value::Null],
                    vec![Value::Integer(20), Value::Integer(7)],
                ],
                "action={action}, statement={statement}"
            );
        }
    }
}

#[test]
fn same_row_before_trigger_mutation_fails_atomically_without_stale_indexes() {
    for trigger_action in [
        "UPDATE child SET p = 2 WHERE id = OLD.id",
        "DELETE FROM child WHERE id = OLD.id",
    ] {
        for explicit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
                .unwrap();
            conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
            conn.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
            conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
            // The WHEN guard prevents the nested DELETE case recursing forever.
            conn.execute("CREATE TABLE guard (id INTEGER PRIMARY KEY)")
                .unwrap();
            conn.execute(&format!("CREATE TRIGGER change_current BEFORE DELETE ON child FOR EACH ROW WHEN (SELECT COUNT(*) FROM guard) = 0 BEGIN INSERT INTO guard VALUES (1); {trigger_action}; END")).unwrap();
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            let error = conn.execute("DELETE FROM parent WHERE id = 1").unwrap_err();
            assert!(matches!(error, SqlError::Unsupported(_)), "{error:?}");
            if explicit {
                conn.execute("ROLLBACK").unwrap();
            }
            assert_eq!(
                conn.query("SELECT id FROM parent ORDER BY id")
                    .unwrap()
                    .rows,
                vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
            );
            assert_eq!(
                conn.query("SELECT id, p FROM child").unwrap().rows,
                vec![vec![Value::Integer(10), Value::Integer(1)]]
            );
            assert!(conn.query("SELECT id FROM guard").unwrap().rows.is_empty());
            let index = citadel_sql::TableSchema::index_table_name("child", "__fk_child_0");
            let mut count = 0;
            db.begin_read()
                .table_for_each(&index, |_, _| {
                    count += 1;
                    Ok(())
                })
                .unwrap();
            assert_eq!(count, 1);
        }
    }
}
