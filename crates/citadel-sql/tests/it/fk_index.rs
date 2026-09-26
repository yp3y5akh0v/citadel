use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("fk-index.db"))
        .passphrase(b"fk-index-test")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn partial_or_expression_unique_index_cannot_prove_parent_uniqueness() {
    for definition in [
        "CREATE UNIQUE INDEX parent_key ON parent (p) WHERE id < 0",
        "CREATE UNIQUE INDEX parent_key ON parent (p, id + 1)",
        "CREATE UNIQUE INDEX parent_key ON parent (id + 1, p)",
    ] {
        for in_transaction in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, p INTEGER)")
                .unwrap();
            conn.execute(definition).unwrap();
            conn.execute("INSERT INTO parent VALUES (1, 7), (2, 7)")
                .unwrap();
            if in_transaction {
                conn.execute("BEGIN").unwrap();
            }
            assert!(
                matches!(
                    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(p))"),
                    Err(SqlError::Unsupported(_))
                ),
                "{definition}; transaction={in_transaction}"
            );
            if in_transaction {
                conn.execute("ROLLBACK").unwrap();
            }
        }
    }
}

#[test]
fn full_column_index_can_replace_the_automatic_fk_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)")
        .unwrap();
    conn.execute("CREATE INDEX replacement ON child (p)")
        .unwrap();
    conn.execute("DROP INDEX __fk_child_0").unwrap();
    conn.execute("INSERT INTO parent VALUES (7), (8)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 7), (11, 8)")
        .unwrap();
    conn.execute("DELETE FROM parent WHERE id = 7").unwrap();
    assert_eq!(
        conn.query("SELECT id, p FROM child").unwrap().rows,
        vec![vec![Value::Integer(11), Value::Integer(8)]]
    );
    assert!(matches!(
        conn.execute("DROP INDEX replacement"),
        Err(SqlError::Unsupported(_))
    ));
}

#[test]
fn collated_replacement_index_preserves_binary_fk_equality() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id TEXT PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p TEXT REFERENCES parent(id) ON DELETE CASCADE ON UPDATE CASCADE)")
        .unwrap();
    conn.execute("CREATE INDEX replacement ON child (p COLLATE NOCASE)")
        .unwrap();
    conn.execute("DROP INDEX __fk_child_0").unwrap();
    conn.execute("INSERT INTO parent VALUES ('Alpha'), ('alpha')")
        .unwrap();
    conn.execute("INSERT INTO child VALUES (10, 'Alpha'), (11, 'alpha')")
        .unwrap();
    conn.execute("UPDATE parent SET id = 'Bravo' WHERE id = 'Alpha'")
        .unwrap();
    assert_eq!(
        conn.query("SELECT id, p FROM child ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![Value::Integer(10), Value::Text("Bravo".into())],
            vec![Value::Integer(11), Value::Text("alpha".into())],
        ]
    );
    conn.execute("DELETE FROM parent WHERE id = 'alpha'")
        .unwrap();
    assert_eq!(
        conn.query("SELECT id, p FROM child").unwrap().rows,
        vec![vec![Value::Integer(10), Value::Text("Bravo".into())]]
    );
}

#[test]
fn self_cascade_preserves_changes_to_rows_still_pending_in_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE nodes (id INTEGER PRIMARY KEY, p INTEGER REFERENCES nodes(id) ON UPDATE CASCADE)")
        .unwrap();
    conn.execute("INSERT INTO nodes VALUES (1, NULL), (2, 1)")
        .unwrap();
    conn.execute("UPDATE nodes SET id = id + 10").unwrap();
    assert_eq!(
        conn.query("SELECT id, p FROM nodes ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![Value::Integer(11), Value::Null],
            vec![Value::Integer(12), Value::Integer(11)],
        ]
    );
    assert_eq!(
        conn.query("SELECT id FROM nodes WHERE p = 11")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(12)]]
    );
}

#[test]
fn update_cascade_checks_recomputed_virtual_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON UPDATE CASCADE, v INTEGER GENERATED ALWAYS AS (p * 2) VIRTUAL, CHECK (v > 0))")
        .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child (id, p) VALUES (10, 1)")
        .unwrap();
    assert!(matches!(
        conn.execute("UPDATE parent SET id = -2 WHERE id = 1"),
        Err(SqlError::CheckViolation(_))
    ));
    assert_eq!(
        conn.query("SELECT p, v FROM child").unwrap().rows,
        vec![vec![Value::Integer(1), Value::Integer(2)]]
    );
    assert_eq!(
        conn.query("SELECT id FROM parent").unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn update_cascade_updates_indexes_on_virtual_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON UPDATE CASCADE, v INTEGER GENERATED ALWAYS AS (p * 2) VIRTUAL)")
        .unwrap();
    conn.execute("CREATE INDEX child_virtual ON child (v + 0)")
        .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child (id, p) VALUES (10, 1)")
        .unwrap();
    conn.execute("UPDATE parent SET id = 2 WHERE id = 1")
        .unwrap();
    assert_eq!(
        conn.query("SELECT id, p, v FROM child WHERE v + 0 = 4")
            .unwrap()
            .rows,
        vec![vec![
            Value::Integer(10),
            Value::Integer(2),
            Value::Integer(4)
        ]]
    );
    assert!(conn
        .query("SELECT id FROM child WHERE v + 0 = 2")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn no_action_checks_follow_sibling_cascades_in_either_fk_order() {
    for actions in [["NO ACTION", "CASCADE"], ["CASCADE", "NO ACTION"]] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute(&format!(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, a INTEGER REFERENCES parent(id) ON DELETE {}, b INTEGER REFERENCES parent(id) ON DELETE {})",
            actions[0], actions[1]
        ))
        .unwrap();
        conn.execute("INSERT INTO parent VALUES (1)").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1, 1)").unwrap();
        conn.execute("DELETE FROM parent WHERE id = 1").unwrap();
        assert!(conn.query("SELECT id FROM parent").unwrap().rows.is_empty());
        assert!(conn.query("SELECT id FROM child").unwrap().rows.is_empty());
    }
}

fn create_unique_parent_tables(conn: &Connection<'_>, deferred: bool) {
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code INTEGER UNIQUE)")
        .unwrap();
    let timing = if deferred {
        "DEFERRABLE INITIALLY DEFERRED"
    } else {
        ""
    };
    conn.execute(&format!(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, code INTEGER REFERENCES parent(code) ON DELETE CASCADE ON UPDATE CASCADE {timing})"
    ))
    .unwrap();
}

#[test]
fn unique_parent_checks_insert_update_and_prepared_insert() {
    for prepared in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        create_unique_parent_tables(&conn, false);
        conn.execute("INSERT INTO parent VALUES (1, 80), (2, 90)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        if prepared {
            let insert = conn.prepare("INSERT INTO child VALUES ($1, $2)").unwrap();
            insert
                .execute(&[Value::Integer(10), Value::Integer(80)])
                .unwrap();
            assert!(matches!(
                insert.execute(&[Value::Integer(11), Value::Integer(1)]),
                Err(SqlError::ForeignKeyViolation(_))
            ));
        } else {
            conn.execute("INSERT INTO child VALUES (10, 80)").unwrap();
            assert!(matches!(
                conn.execute("INSERT INTO child VALUES (11, 1)"),
                Err(SqlError::ForeignKeyViolation(_))
            ));
        }
        conn.execute("UPDATE child SET code = 90 WHERE id = 10")
            .unwrap();
        assert!(matches!(
            conn.execute("UPDATE child SET code = 1 WHERE id = 10"),
            Err(SqlError::ForeignKeyViolation(_))
        ));
        assert!(matches!(
            conn.execute(
                "INSERT INTO child VALUES (10, 80) ON CONFLICT (id) DO UPDATE SET code = 1"
            ),
            Err(SqlError::ForeignKeyViolation(_))
        ));
        conn.execute("COMMIT").unwrap();
        assert_eq!(
            conn.query("SELECT id, code FROM child").unwrap().rows,
            vec![vec![Value::Integer(10), Value::Integer(90)]]
        );
        conn.execute("UPDATE parent SET code = 91 WHERE id = 2")
            .unwrap();
        assert_eq!(
            conn.query("SELECT code FROM child").unwrap().rows,
            vec![vec![Value::Integer(91)]]
        );
        conn.execute("DELETE FROM parent WHERE id = 2").unwrap();
        assert!(conn.query("SELECT id FROM child").unwrap().rows.is_empty());
    }
}

#[test]
fn deferred_unique_reference_resolves_the_surviving_child_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    create_unique_parent_tables(&conn, true);
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 80)").unwrap();
    conn.execute("INSERT INTO parent VALUES (1, 80)").unwrap();
    conn.execute("COMMIT").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE child SET code = 777 WHERE id = 10")
        .unwrap();
    conn.execute("UPDATE child SET code = 90 WHERE id = 10")
        .unwrap();
    conn.execute("INSERT INTO parent VALUES (2, 90)").unwrap();
    conn.execute("INSERT INTO child VALUES (11, 888)").unwrap();
    conn.execute("DELETE FROM child WHERE id = 11").unwrap();
    conn.execute("UPDATE parent SET code = 91 WHERE id = 2")
        .unwrap();
    conn.execute("UPDATE parent SET code = 92 WHERE id = 2")
        .unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        conn.query("SELECT id, code FROM child").unwrap().rows,
        vec![vec![Value::Integer(10), Value::Integer(92)]]
    );
    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE child SET code = 1 WHERE id = 10")
        .unwrap();
    conn.execute("UPDATE child SET id = 12 WHERE id = 10")
        .unwrap();
    assert!(matches!(
        conn.execute("COMMIT"),
        Err(SqlError::ForeignKeyViolation(_))
    ));
}

#[test]
fn collated_unique_parent_rechecks_exact_values_and_cache_entries() {
    for (collation, exact, other) in [("NOCASE", "Alpha", "alpha"), ("RTRIM", "Alpha ", "Alpha")] {
        for deferred in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT)")
                .unwrap();
            conn.execute(&format!(
                "CREATE UNIQUE INDEX parent_code ON parent (code COLLATE {collation})"
            ))
            .unwrap();
            let timing = if deferred {
                "DEFERRABLE INITIALLY DEFERRED"
            } else {
                ""
            };
            conn.execute(&format!("CREATE TABLE child (id INTEGER PRIMARY KEY, code TEXT REFERENCES parent(code) {timing})")).unwrap();
            conn.execute(&format!("INSERT INTO parent VALUES (1, '{exact}')"))
                .unwrap();
            conn.execute("BEGIN").unwrap();
            conn.execute(&format!("INSERT INTO child VALUES (10, '{exact}')"))
                .unwrap();
            let result = conn.execute(&format!("INSERT INTO child VALUES (11, '{other}')"));
            if deferred {
                result.unwrap();
                assert!(matches!(
                    conn.execute("COMMIT"),
                    Err(SqlError::ForeignKeyViolation(_))
                ));
            } else {
                assert!(matches!(result, Err(SqlError::ForeignKeyViolation(_))));
                conn.execute("COMMIT").unwrap();
            }
        }
    }
}

#[test]
fn unique_parent_cache_distinguishes_multiple_referenced_indices() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, a INTEGER UNIQUE, b INTEGER UNIQUE)",
    )
    .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, a INTEGER REFERENCES parent(a), b INTEGER REFERENCES parent(b))").unwrap();
    conn.execute("INSERT INTO parent VALUES (1, 7, 8)").unwrap();
    conn.execute("BEGIN").unwrap();
    assert!(matches!(
        conn.execute("INSERT INTO child VALUES (10, 7, 7)"),
        Err(SqlError::ForeignKeyViolation(_))
    ));
    conn.execute("INSERT INTO child VALUES (10, 7, 8)").unwrap();
    conn.execute("COMMIT").unwrap();
}

#[test]
fn unique_parent_mutation_cannot_reuse_a_stale_positive_probe() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    create_unique_parent_tables(&conn, false);
    conn.execute("INSERT INTO parent VALUES (1, 7)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 7)").unwrap();
    conn.execute("UPDATE parent SET code = 8 WHERE id = 1")
        .unwrap();
    assert!(matches!(
        conn.execute("INSERT INTO child VALUES (11, 7)"),
        Err(SqlError::ForeignKeyViolation(_))
    ));
    conn.execute("INSERT INTO child VALUES (11, 8)").unwrap();
    conn.execute("COMMIT").unwrap();
}

#[test]
fn deferred_reference_survives_table_and_column_renames() {
    for (rename, insert_parent) in [
        (
            "ALTER TABLE child RENAME TO renamed_child",
            "INSERT INTO parent VALUES (1, 80)",
        ),
        (
            "ALTER TABLE child RENAME COLUMN code TO renamed_code",
            "INSERT INTO parent VALUES (1, 80)",
        ),
        (
            "ALTER TABLE parent RENAME TO renamed_parent",
            "INSERT INTO renamed_parent VALUES (1, 80)",
        ),
        (
            "ALTER TABLE parent RENAME COLUMN code TO renamed_code",
            "INSERT INTO parent VALUES (1, 80)",
        ),
    ] {
        for valid in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            create_unique_parent_tables(&conn, true);
            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO child VALUES (10, 80)").unwrap();
            conn.execute(rename).unwrap();
            if valid {
                conn.execute(insert_parent).unwrap();
                conn.execute("COMMIT").unwrap();
            } else {
                assert!(
                    matches!(
                        conn.execute("COMMIT"),
                        Err(SqlError::ForeignKeyViolation(_))
                    ),
                    "{rename}"
                );
            }
        }
    }
}

#[test]
fn deferred_reference_rename_preserves_savepoint_rollback() {
    for valid in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        create_unique_parent_tables(&conn, true);
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO child VALUES (10, 80)").unwrap();
        conn.execute("SAVEPOINT before_rename").unwrap();
        conn.execute("ALTER TABLE child RENAME TO renamed_child")
            .unwrap();
        conn.execute("ROLLBACK TO SAVEPOINT before_rename").unwrap();
        if valid {
            conn.execute("INSERT INTO parent VALUES (1, 80)").unwrap();
            conn.execute("COMMIT").unwrap();
        } else {
            assert!(matches!(
                conn.execute("COMMIT"),
                Err(SqlError::ForeignKeyViolation(_))
            ));
        }
    }
}

#[test]
fn immediate_self_references_accept_the_candidate_primary_key() {
    for prepared in [false, true] {
        for explicit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE self_row (id INTEGER PRIMARY KEY, p INTEGER REFERENCES self_row(id) ON UPDATE CASCADE)").unwrap();
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            if prepared {
                conn.prepare("INSERT INTO self_row VALUES ($1, $2)")
                    .unwrap()
                    .execute(&[Value::Integer(1), Value::Integer(1)])
                    .unwrap();
                conn.prepare("UPDATE self_row SET id = $1, p = $1 WHERE id = $2")
                    .unwrap()
                    .execute(&[Value::Integer(2), Value::Integer(1)])
                    .unwrap();
            } else {
                conn.execute("INSERT INTO self_row VALUES (1, 1)").unwrap();
                conn.execute("UPDATE self_row SET id = 2, p = 2 WHERE id = 1")
                    .unwrap();
            }
            assert_eq!(
                conn.query("SELECT id, p FROM self_row").unwrap().rows,
                vec![vec![Value::Integer(2), Value::Integer(2)]]
            );
            assert!(matches!(
                conn.execute("INSERT INTO self_row VALUES (3, 9)"),
                Err(SqlError::ForeignKeyViolation(_))
            ));
            assert!(matches!(
                conn.execute("UPDATE self_row SET id = 3, p = 9 WHERE id = 2"),
                Err(SqlError::ForeignKeyViolation(_))
            ));
            assert_eq!(
                conn.query("SELECT id, p FROM self_row").unwrap().rows,
                vec![vec![Value::Integer(2), Value::Integer(2)]]
            );
            if explicit {
                conn.execute("COMMIT").unwrap();
            }
        }
    }
}

#[test]
fn immediate_self_references_accept_only_the_exact_candidate_unique_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE self_row (id INTEGER PRIMARY KEY, code INTEGER UNIQUE, p INTEGER REFERENCES self_row(code) ON UPDATE CASCADE)").unwrap();
    conn.prepare("INSERT INTO self_row VALUES ($1,$2,$3)")
        .unwrap()
        .execute(&[Value::Integer(1), Value::Integer(7), Value::Integer(7)])
        .unwrap();
    conn.execute("UPDATE self_row SET code = 8, p = 8 WHERE id = 1")
        .unwrap();
    assert_eq!(
        conn.query("SELECT id, code, p FROM self_row").unwrap().rows,
        vec![vec![
            Value::Integer(1),
            Value::Integer(8),
            Value::Integer(8)
        ]]
    );
    assert!(matches!(
        conn.execute("INSERT INTO self_row VALUES (2,9,10)"),
        Err(SqlError::ForeignKeyViolation(_))
    ));
    assert!(matches!(
        conn.execute("UPDATE self_row SET code=9,p=10"),
        Err(SqlError::ForeignKeyViolation(_))
    ));
    assert_eq!(
        conn.query("SELECT code, p FROM self_row").unwrap().rows,
        vec![vec![Value::Integer(8), Value::Integer(8)]]
    );
}

#[test]
fn failed_self_reference_insert_rolls_back_before_next_parent_probe() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE self_row (id INTEGER PRIMARY KEY, u INTEGER UNIQUE, p INTEGER REFERENCES self_row(id))").unwrap();
    conn.execute("INSERT INTO self_row VALUES (1,7,NULL)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT before_conflict").unwrap();
    assert!(matches!(
        conn.execute("INSERT INTO self_row VALUES (2,7,2)"),
        Err(SqlError::UniqueViolation(_))
    ));
    conn.execute("ROLLBACK TO SAVEPOINT before_conflict")
        .unwrap();
    let error = conn
        .execute("INSERT INTO self_row VALUES (3,8,2)")
        .unwrap_err();
    assert!(
        matches!(error, SqlError::ForeignKeyViolation(_)),
        "{error:?}"
    );
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        conn.query("SELECT id FROM self_row").unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
}
#[test]
fn inline_self_unique_definitions_are_validated_before_any_storage_write() {
    for explicit in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        let error = conn.execute("CREATE TABLE bad_self (id INTEGER PRIMARY KEY, code INTEGER, p INTEGER REFERENCES bad_self(code))").unwrap_err();
        assert!(matches!(error, SqlError::Unsupported(_)), "{error:?}");
        // Failed validation must not leave storage behind or poison this transaction.
        conn.execute("CREATE TABLE bad_self (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE self_composite (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, pa INTEGER, pb INTEGER, UNIQUE(a,b), FOREIGN KEY(pa,pb) REFERENCES self_composite(a,b) ON UPDATE CASCADE)").unwrap();
        conn.execute("INSERT INTO self_composite VALUES (1,7,8,7,8)")
            .unwrap();
        conn.execute("UPDATE self_composite SET a=9,b=10,pa=9,pb=10")
            .unwrap();
        assert_eq!(
            conn.query("SELECT a,b,pa,pb FROM self_composite")
                .unwrap()
                .rows,
            vec![vec![
                Value::Integer(9),
                Value::Integer(10),
                Value::Integer(9),
                Value::Integer(10)
            ]]
        );
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
    }
}
