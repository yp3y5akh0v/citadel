use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn with_connection(check: impl FnOnce(&Connection<'_>)) {
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    check(&Connection::open(&db).unwrap());
}

fn parent_action(action: &str, rejects: bool) {
    for unique_reference in [false, true] {
        for prepared in [false, true] {
            with_connection(|conn| {
                conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code INTEGER UNIQUE)")
                    .unwrap();
                let column = if unique_reference { "code" } else { "id" };
                conn.execute(&format!("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_key INTEGER REFERENCES parent({column}) ON UPDATE {action})")).unwrap();
                conn.execute("INSERT INTO parent VALUES (1, 10)").unwrap();
                let old = if unique_reference { 10 } else { 1 };
                conn.execute(&format!("INSERT INTO child VALUES (5, {old})"))
                    .unwrap();
                let sql = format!("INSERT INTO parent VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET {column} = {column} + 1");
                let result = if prepared {
                    conn.prepare(&sql).unwrap().execute(&[])
                } else {
                    conn.execute(&sql).map(|result| match result {
                        ExecutionResult::RowsAffected(count) => count,
                        other => panic!("unexpected {other:?}"),
                    })
                };
                if rejects {
                    assert!(
                        matches!(result, Err(SqlError::ForeignKeyViolation(_))),
                        "{result:?}"
                    );
                    assert_eq!(
                        conn.query("SELECT * FROM parent").unwrap().rows,
                        vec![vec![Value::Integer(1), Value::Integer(10)]]
                    );
                } else {
                    assert_eq!(result.unwrap(), 1);
                }
                let expected = if rejects {
                    Value::Integer(old)
                } else if action == "SET NULL" {
                    Value::Null
                } else {
                    Value::Integer(old + 1)
                };
                assert_eq!(
                    conn.query("SELECT parent_key FROM child").unwrap().rows,
                    vec![vec![expected]]
                );
            });
        }
    }
}

#[test]
fn upsert_runs_parent_cascade_for_primary_and_unique_keys() {
    parent_action("CASCADE", false);
}

#[test]
fn upsert_runs_parent_restrict_for_primary_and_unique_keys() {
    parent_action("RESTRICT", true);
}

#[test]
fn upsert_runs_parent_set_null_for_primary_and_unique_keys() {
    parent_action("SET NULL", false);
}

#[test]
fn upsert_runs_before_update_once_with_old_and_excluded_values() {
    with_connection(|conn| {
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        conn.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, old_v INTEGER, new_v INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        conn.execute("CREATE TRIGGER before_upd BEFORE UPDATE ON t FOR EACH ROW BEGIN INSERT INTO audit VALUES (OLD.id, OLD.v, NEW.v); END").unwrap();
        let result = conn.query("INSERT INTO t VALUES (1, 7) ON CONFLICT (id) DO UPDATE SET v = v + excluded.v WHERE excluded.v > 0 RETURNING id, v").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Value::Integer(1), Value::Integer(17)]]
        );
        assert_eq!(
            conn.query("SELECT * FROM audit").unwrap().rows,
            vec![vec![
                Value::Integer(1),
                Value::Integer(10),
                Value::Integer(17)
            ]]
        );
        conn.execute("INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET v = 999 WHERE excluded.v > 0").unwrap();
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM audit").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
    });
}

#[test]
fn unique_conflict_update_keeps_assigned_names_for_update_of_triggers() {
    with_connection(|conn| {
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, v INTEGER, other INTEGER)",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a', 10, 20)")
            .unwrap();
        conn.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, calls INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO audit VALUES (1, 0)").unwrap();
        conn.execute("CREATE TRIGGER count_v AFTER UPDATE OF v ON t FOR EACH ROW BEGIN UPDATE audit SET calls = calls + 1 WHERE id = 1; END").unwrap();
        for assignment in ["v = v", "other = other + 1"] {
            let sql = format!("INSERT INTO t VALUES ($1, 'a', 0, 0) ON CONFLICT (email) DO UPDATE SET {assignment}");
            assert_eq!(
                conn.prepare(&sql)
                    .unwrap()
                    .execute(&[Value::Integer(9)])
                    .unwrap(),
                1
            );
        }
        assert_eq!(
            conn.query("SELECT calls FROM audit").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
        assert_eq!(
            conn.query("SELECT id, v, other FROM t").unwrap().rows,
            vec![vec![
                Value::Integer(1),
                Value::Integer(10),
                Value::Integer(21)
            ]]
        );
    });
}

#[test]
fn upsert_statement_triggers_capture_only_their_actual_rows_once() {
    with_connection(|conn| {
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        conn.execute("CREATE TABLE audit (event TEXT PRIMARY KEY, n INTEGER, total INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap();
        conn.execute("CREATE TRIGGER before_upd BEFORE UPDATE ON t FOR EACH STATEMENT BEGIN INSERT INTO audit VALUES ('before', (SELECT COUNT(*) FROM t), (SELECT SUM(v) FROM t)); END").unwrap();
        conn.execute("CREATE TRIGGER after_upd AFTER UPDATE ON t REFERENCING OLD TABLE AS o NEW TABLE AS n FOR EACH STATEMENT BEGIN INSERT INTO audit VALUES ('old', (SELECT COUNT(*) FROM o), (SELECT SUM(v) FROM o)); INSERT INTO audit VALUES ('new', (SELECT COUNT(*) FROM n), (SELECT SUM(v) FROM n)); END").unwrap();
        conn.execute("CREATE TRIGGER after_ins AFTER INSERT ON t REFERENCING NEW TABLE AS n FOR EACH STATEMENT BEGIN INSERT INTO audit VALUES ('insert', (SELECT COUNT(*) FROM n), (SELECT SUM(v) FROM n)); END").unwrap();
        let rows = conn.query("INSERT INTO t VALUES (1, 5), (2, 7), (3, 0), (4, 9) ON CONFLICT (id) DO UPDATE SET v = v + excluded.v WHERE excluded.v > 0 RETURNING id, v").unwrap().rows;
        assert_eq!(
            rows,
            vec![
                vec![Value::Integer(1), Value::Integer(15)],
                vec![Value::Integer(2), Value::Integer(27)],
                vec![Value::Integer(4), Value::Integer(9)]
            ]
        );
        assert_eq!(
            conn.query("SELECT * FROM audit ORDER BY event")
                .unwrap()
                .rows,
            vec![
                vec![
                    Value::Text("before".into()),
                    Value::Integer(3),
                    Value::Integer(60)
                ],
                vec![
                    Value::Text("insert".into()),
                    Value::Integer(1),
                    Value::Integer(9)
                ],
                vec![
                    Value::Text("new".into()),
                    Value::Integer(2),
                    Value::Integer(42)
                ],
                vec![
                    Value::Text("old".into()),
                    Value::Integer(2),
                    Value::Integer(30)
                ],
            ]
        );
    });
}

#[test]
fn upsert_rejects_before_trigger_that_deletes_target_and_rolls_back() {
    with_connection(|conn| {
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        conn.execute("CREATE TRIGGER before_upd BEFORE UPDATE ON t FOR EACH ROW BEGIN DELETE FROM t WHERE id = OLD.id; END").unwrap();
        let result = conn
            .execute("INSERT INTO t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET v = excluded.v");
        assert!(
            matches!(&result, Err(SqlError::Unsupported(reason)) if reason == "a BEFORE trigger cannot modify or delete the row being processed"),
            "{result:?}"
        );
        assert_eq!(
            conn.query("SELECT * FROM t").unwrap().rows,
            vec![vec![Value::Integer(1), Value::Integer(10)]]
        );
    });
}

#[test]
fn upsert_no_action_checks_the_final_statement_state() {
    with_connection(|conn| {
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, next_id INTEGER)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON UPDATE NO ACTION)").unwrap();
        conn.execute("INSERT INTO parent VALUES (1, 1)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
        conn.execute("INSERT INTO parent VALUES (1, 2), (1, 1) ON CONFLICT (id) DO UPDATE SET id = excluded.next_id").unwrap();
        assert_eq!(
            conn.query("SELECT id FROM parent ORDER BY id")
                .unwrap()
                .rows,
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
        );
        assert_eq!(
            conn.query("SELECT p FROM child").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
    });
    parent_action("NO ACTION", true);
}

#[test]
fn upsert_no_action_allows_after_insert_statement_trigger_to_repair_reference() {
    with_connection(|conn| {
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON UPDATE NO ACTION)").unwrap();
        conn.execute("INSERT INTO parent VALUES (1)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
        conn.execute("CREATE TRIGGER repair AFTER INSERT ON parent FOR EACH STATEMENT BEGIN UPDATE child SET p = 2 WHERE p = 1; END").unwrap();
        conn.execute("INSERT INTO parent VALUES (1) ON CONFLICT (id) DO UPDATE SET id = 2")
            .unwrap();
        assert_eq!(
            conn.query("SELECT p FROM child").unwrap().rows,
            vec![vec![Value::Integer(2)]]
        );
    });
}

#[test]
fn upsert_self_cascade_returns_the_stored_row() {
    with_connection(|conn| {
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, p INTEGER REFERENCES t(id) ON UPDATE CASCADE)",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
        let result = conn
            .query(
                "INSERT INTO t VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET id = 2 RETURNING id, p",
            )
            .unwrap();
        let expected = vec![vec![Value::Integer(2), Value::Integer(2)]];
        assert_eq!(result.rows, expected);
        assert_eq!(conn.query("SELECT * FROM t").unwrap().rows, expected);
    });
}

#[test]
fn upsert_nested_cascade_updates_descendant_unique_reference() {
    with_connection(|conn| {
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER UNIQUE REFERENCES parent(id) ON UPDATE CASCADE)").unwrap();
        conn.execute("CREATE TABLE grandchild (id INTEGER PRIMARY KEY, p INTEGER REFERENCES child(p) ON UPDATE CASCADE)").unwrap();
        conn.execute("INSERT INTO parent VALUES (1)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
        conn.execute("INSERT INTO grandchild VALUES (100, 1)")
            .unwrap();
        conn.execute("INSERT INTO parent VALUES (1) ON CONFLICT (id) DO UPDATE SET id = 2")
            .unwrap();
        assert_eq!(
            conn.query("SELECT p FROM child").unwrap().rows,
            vec![vec![Value::Integer(2)]]
        );
        assert_eq!(
            conn.query("SELECT p FROM grandchild").unwrap().rows,
            vec![vec![Value::Integer(2)]]
        );
    });
}

#[test]
fn failed_multirow_upsert_cannot_commit_partial_mutations() {
    with_connection(|conn| {
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON UPDATE RESTRICT)").unwrap();
        conn.execute("INSERT INTO parent VALUES (1, 10), (2, 20)")
            .unwrap();
        conn.execute("INSERT INTO child VALUES (10, 2)").unwrap();
        conn.execute("BEGIN").unwrap();
        let result = conn.execute("INSERT INTO parent VALUES (1, 7), (2, 8) ON CONFLICT (id) DO UPDATE SET id = id + 10, v = excluded.v");
        assert!(
            matches!(result, Err(SqlError::ForeignKeyViolation(_))),
            "{result:?}"
        );
        assert!(matches!(
            conn.execute("COMMIT"),
            Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
        ));
        // A refused COMMIT consumes the failed writer and rolls back its prefix.
        assert!(!conn.in_transaction());
        assert_eq!(
            conn.query("SELECT * FROM parent ORDER BY id").unwrap().rows,
            vec![
                vec![Value::Integer(1), Value::Integer(10)],
                vec![Value::Integer(2), Value::Integer(20)]
            ]
        );
        assert_eq!(
            conn.query("SELECT p FROM child").unwrap().rows,
            vec![vec![Value::Integer(2)]]
        );
    });
}

#[test]
fn upsert_statement_update_triggers_fire_for_zero_updated_rows() {
    with_connection(|conn| {
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        conn.execute("CREATE TABLE audit (event TEXT PRIMARY KEY, n INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        conn.execute("CREATE TRIGGER before_upd BEFORE UPDATE ON t FOR EACH STATEMENT BEGIN INSERT INTO audit VALUES ('before', 0); END").unwrap();
        conn.execute("CREATE TRIGGER after_upd AFTER UPDATE ON t REFERENCING NEW TABLE AS n FOR EACH STATEMENT BEGIN INSERT INTO audit VALUES ('after', (SELECT COUNT(*) FROM n)); END").unwrap();
        conn.execute(
            "INSERT INTO t VALUES (1, 7) ON CONFLICT (id) DO UPDATE SET v = excluded.v WHERE FALSE",
        )
        .unwrap();
        assert_eq!(
            conn.query("SELECT * FROM audit ORDER BY event")
                .unwrap()
                .rows,
            vec![
                vec![Value::Text("after".into()), Value::Integer(0)],
                vec![Value::Text("before".into()), Value::Integer(0)]
            ]
        );
    });
}

#[test]
fn before_update_preserves_volatile_missing_defaults_without_redecoding() {
    for sql in [
        "UPDATE t SET v = v + 1 WHERE id = 1 RETURNING v",
        "INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET v = v + 1 RETURNING v",
    ] {
        for prepared in [false, true] {
            with_connection(|conn| {
                conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
                    .unwrap();
                conn.execute("INSERT INTO t VALUES (1)").unwrap();
                conn.execute("ALTER TABLE t ADD COLUMN v INTEGER DEFAULT (RANDOM() % 1000000)")
                    .unwrap();
                conn.execute(
                    "CREATE TABLE audit (event TEXT PRIMARY KEY, old_v INTEGER, new_v INTEGER)",
                )
                .unwrap();
                for timing in ["BEFORE", "AFTER"] {
                    conn.execute(&format!("CREATE TRIGGER audit_{timing} {timing} UPDATE ON t FOR EACH ROW BEGIN INSERT INTO audit VALUES ('{timing}', OLD.v, NEW.v); END")).unwrap();
                }
                let returned = if prepared {
                    conn.prepare(sql).unwrap().query_collect(&[]).unwrap()
                } else {
                    conn.query(sql).unwrap()
                };
                let audit = conn
                    .query("SELECT old_v, new_v FROM audit ORDER BY event")
                    .unwrap();
                let Value::Integer(old) = audit.rows[0][0] else {
                    panic!("expected INTEGER default")
                };
                assert!((-999_999..=999_999).contains(&old));
                assert_eq!(
                    audit.rows,
                    vec![vec![Value::Integer(old), Value::Integer(old + 1)]; 2]
                );
                assert_eq!(returned.rows, vec![vec![Value::Integer(old + 1)]]);
                assert_eq!(conn.query("SELECT v FROM t").unwrap().rows, returned.rows);
            });
        }
    }
}

#[test]
fn before_update_physical_row_changes_are_rejected_and_rolled_back() {
    for sql in [
        "UPDATE t SET v = v + 1 WHERE id = 1",
        "INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET v = v + 1",
    ] {
        with_connection(|conn| {
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
            conn.execute("CREATE TABLE guard (id INTEGER PRIMARY KEY)")
                .unwrap();
            conn.execute("CREATE TRIGGER change_row BEFORE UPDATE ON t FOR EACH ROW WHEN NEW.v <> 100 BEGIN INSERT INTO guard VALUES (1); UPDATE t SET v = 100 WHERE id = OLD.id; END").unwrap();
            let result = conn.execute(sql);
            assert!(
                matches!(&result, Err(SqlError::Unsupported(reason)) if reason == "a BEFORE trigger cannot modify or delete the row being processed"),
                "{result:?}"
            );
            assert_eq!(
                conn.query("SELECT * FROM t").unwrap().rows,
                vec![vec![Value::Integer(1), Value::Integer(10)]]
            );
            assert!(conn.query("SELECT * FROM guard").unwrap().rows.is_empty());
        });
    }
}
