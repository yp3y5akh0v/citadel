use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, TableSchema, Value};

fn index_contents(db: &Database, table: &str, index: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut read = db.begin_read();
    let mut entries = Vec::new();
    read.table_for_each(
        &TableSchema::index_table_name(table, index),
        |key, value| {
            entries.push((key.to_vec(), value.to_vec()));
            Ok(())
        },
    )
    .unwrap();
    entries
}

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"matview-indexes")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn refresh(
    conn: &Connection<'_>,
    concurrent: bool,
) -> Result<citadel_sql::ExecutionResult, SqlError> {
    conn.execute(if concurrent {
        "REFRESH MATERIALIZED VIEW CONCURRENTLY mv"
    } else {
        "REFRESH MATERIALIZED VIEW mv"
    })
}

#[test]
fn collated_materialized_primary_key_is_enforced_during_creation() {
    for (collation, duplicate) in [("NOCASE", "a"), ("RTRIM", "A ")] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute(&format!(
            "CREATE TABLE src(id INTEGER PRIMARY KEY, s TEXT COLLATE {collation})"
        ))
        .unwrap();
        conn.execute(&format!("INSERT INTO src VALUES (1,'A'),(2,'{duplicate}')"))
            .unwrap();
        assert!(matches!(
            conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT s,id FROM src"),
            Err(SqlError::DuplicateKey)
        ));
        assert!(conn.table_schema("mv").is_none());
        let mut read = db.begin_read();
        assert!(read.table_root_stamp(b"mv").unwrap().is_none());
        assert!(read.table_get(b"_schema", b"mv").unwrap().is_none());
        drop(read);
        conn.execute("DELETE FROM src WHERE id=2").unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT s,id FROM src")
            .unwrap();
        assert_eq!(conn.table_schema("mv").unwrap().indices.len(), 1);
        drop(conn);
        let reopened = Connection::open(&db).unwrap();
        assert_eq!(
            reopened
                .query("SELECT id FROM mv WHERE s='A'")
                .unwrap()
                .rows,
            vec![vec![Value::Integer(1)]]
        );
    }
}

#[test]
fn unpopulated_collated_materialized_view_has_its_index_before_first_refresh() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY,s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1,'A'),(2,'a')")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT s,id FROM src WITH NO DATA")
        .unwrap();
    assert_eq!(conn.table_schema("mv").unwrap().indices.len(), 1);
    assert!(matches!(refresh(&conn, false), Err(SqlError::DuplicateKey)));
    assert!(conn.query("SELECT * FROM mv").unwrap().rows.is_empty());
    assert_eq!(
        conn.query("SELECT ispopulated FROM pg_matviews WHERE matviewname='mv'")
            .unwrap()
            .rows,
        vec![vec![Value::Boolean(false)]]
    );
    conn.execute("DELETE FROM src WHERE id=2").unwrap();
    refresh(&conn, false).unwrap();
    assert_eq!(
        conn.query("SELECT id FROM mv WHERE s='a'").unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn both_refresh_modes_maintain_secondary_expression_and_partial_indexes() {
    for concurrent in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY,k TEXT,n INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1,'Alpha',10),(2,'Beta',20),(3,'Gone',30)")
            .unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id,k,n FROM src")
            .unwrap();
        conn.execute("CREATE UNIQUE INDEX mv_id ON mv(id)").unwrap();
        conn.execute("CREATE INDEX mv_k ON mv(k)").unwrap();
        conn.execute("CREATE INDEX mv_lower ON mv(lower(k))")
            .unwrap();
        conn.execute("CREATE INDEX mv_partial ON mv(n) WHERE n>=20")
            .unwrap();
        conn.execute("DELETE FROM src WHERE id=3").unwrap();
        conn.execute("UPDATE src SET k='Changed',n=25 WHERE id=1")
            .unwrap();
        conn.execute("UPDATE src SET n=5 WHERE id=2").unwrap();
        conn.execute("INSERT INTO src VALUES (4,'New',40)").unwrap();
        refresh(&conn, concurrent).unwrap();
        for (predicate, expected) in [
            ("k='Changed'", vec![vec![Value::Integer(1)]]),
            ("k='Alpha'", vec![]),
            ("k='Gone'", vec![]),
            ("lower(k)='new'", vec![vec![Value::Integer(4)]]),
            ("lower(k)='alpha'", vec![]),
            (
                "n>=20",
                vec![vec![Value::Integer(1)], vec![Value::Integer(4)]],
            ),
        ] {
            assert_eq!(
                conn.query(&format!("SELECT id FROM mv WHERE {predicate} ORDER BY id"))
                    .unwrap()
                    .rows,
                expected,
                "concurrent={concurrent} {predicate}"
            );
        }
        // An independently built index is also the oracle for stale entries
        // that a heap recheck might otherwise hide from a query assertion.
        conn.execute("CREATE MATERIALIZED VIEW expected AS SELECT id,k,n FROM src")
            .unwrap();
        for (name, keys) in [("id", "id"), ("k", "k"), ("lower", "lower(k)")] {
            let unique = if name == "id" { "UNIQUE " } else { "" };
            conn.execute(&format!(
                "CREATE {unique}INDEX expected_{name} ON expected({keys})"
            ))
            .unwrap();
            assert_eq!(
                index_contents(&db, "mv", &format!("mv_{name}")),
                index_contents(&db, "expected", &format!("expected_{name}"))
            );
        }
        conn.execute("CREATE INDEX expected_partial ON expected(n) WHERE n>=20")
            .unwrap();
        assert_eq!(
            index_contents(&db, "mv", "mv_partial"),
            index_contents(&db, "expected", "expected_partial")
        );
        drop(conn);
        let reopened = Connection::open(&db).unwrap();
        assert_eq!(
            reopened
                .query("SELECT id FROM mv WHERE k='New'")
                .unwrap()
                .rows,
            vec![vec![Value::Integer(4)]]
        );
    }
}

#[test]
fn both_refresh_modes_allow_final_unique_swaps_and_reject_duplicate_final_values() {
    for concurrent in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY,n INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1,10),(2,20)")
            .unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id,n FROM src")
            .unwrap();
        conn.execute("CREATE UNIQUE INDEX mv_n ON mv(n)").unwrap();
        conn.execute("UPDATE src SET n=30-n").unwrap();
        refresh(&conn, concurrent).unwrap();
        let expected = vec![
            vec![Value::Integer(1), Value::Integer(20)],
            vec![Value::Integer(2), Value::Integer(10)],
        ];
        assert_eq!(
            conn.query("SELECT * FROM mv ORDER BY id").unwrap().rows,
            expected
        );
        assert_eq!(
            conn.query("SELECT id FROM mv WHERE n=10").unwrap().rows,
            vec![vec![Value::Integer(2)]]
        );
        conn.execute("UPDATE src SET n=7").unwrap();
        assert!(matches!(
            refresh(&conn, concurrent),
            Err(SqlError::UniqueViolation(_))
        ));
        assert_eq!(
            conn.query("SELECT * FROM mv ORDER BY id").unwrap().rows,
            expected
        );
        assert!(conn
            .query("SELECT id FROM mv WHERE n=7")
            .unwrap()
            .rows
            .is_empty());
        assert_eq!(
            conn.query("SELECT id FROM mv WHERE n=20").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
    }
}

#[test]
fn both_refresh_modes_reject_exact_and_collated_primary_duplicates_atomically() {
    for concurrent in [false, true] {
        for (collation, duplicate) in [("BINARY", "A"), ("NOCASE", "a"), ("RTRIM", "A ")] {
            let db = database();
            let conn = Connection::open(&db).unwrap();
            conn.execute(&format!(
                "CREATE TABLE src(id INTEGER PRIMARY KEY,s TEXT COLLATE {collation})"
            ))
            .unwrap();
            conn.execute("INSERT INTO src VALUES (1,'A')").unwrap();
            conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT s,id FROM src")
                .unwrap();
            conn.execute("CREATE UNIQUE INDEX mv_id ON mv(id)").unwrap();
            conn.execute(&format!("INSERT INTO src VALUES (2,'{duplicate}')"))
                .unwrap();
            assert!(
                matches!(refresh(&conn, concurrent), Err(SqlError::DuplicateKey)),
                "{concurrent} {collation}"
            );
            assert_eq!(
                conn.query("SELECT id FROM mv WHERE s='A'").unwrap().rows,
                vec![vec![Value::Integer(1)]]
            );
            assert!(conn
                .query("SELECT s FROM mv WHERE id=2")
                .unwrap()
                .rows
                .is_empty());
        }
    }
}

#[test]
fn refresh_checks_final_foreign_keys_without_applying_row_cascades() {
    for concurrent in [false, true] {
        for action in ["RESTRICT", "CASCADE"] {
            let db = database();
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY,n INTEGER)")
                .unwrap();
            conn.execute("INSERT INTO src VALUES (1,10),(2,20)")
                .unwrap();
            conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id,n FROM src")
                .unwrap();
            conn.execute("CREATE UNIQUE INDEX mv_n ON mv(n)").unwrap();
            conn.execute(&format!("CREATE TABLE child(id INTEGER PRIMARY KEY,p INTEGER REFERENCES mv(n) ON DELETE {action} ON UPDATE {action})")).unwrap();
            conn.execute("INSERT INTO child VALUES (1,10)").unwrap();
            conn.execute("UPDATE src SET n=30-n").unwrap();
            refresh(&conn, concurrent).unwrap();
            conn.execute("DELETE FROM src WHERE id=2").unwrap();
            assert!(matches!(
                refresh(&conn, concurrent),
                Err(SqlError::ForeignKeyViolation(_))
            ));
            assert_eq!(
                conn.query("SELECT id FROM mv WHERE n=10").unwrap().rows,
                vec![vec![Value::Integer(2)]]
            );
            assert_eq!(
                conn.query("SELECT p FROM child").unwrap().rows,
                vec![vec![Value::Integer(10)]]
            );
        }
    }
}

#[test]
fn deferred_references_are_checked_at_commit_against_surviving_children() {
    for concurrent in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1)").unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
            .unwrap();
        conn.execute("CREATE UNIQUE INDEX mv_id ON mv(id)").unwrap();
        conn.execute("CREATE TABLE child(id INTEGER PRIMARY KEY,p INTEGER REFERENCES mv(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
        conn.execute("INSERT INTO child VALUES (1,1)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("DELETE FROM src").unwrap();
        refresh(&conn, concurrent).unwrap();
        assert!(matches!(
            conn.execute("COMMIT"),
            Err(SqlError::ForeignKeyViolation(_))
        ));
        assert_eq!(
            conn.query("SELECT id FROM mv").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
        conn.execute("BEGIN").unwrap();
        conn.execute("DELETE FROM src").unwrap();
        refresh(&conn, concurrent).unwrap();
        conn.execute("DELETE FROM child").unwrap();
        conn.execute("COMMIT").unwrap();
        assert!(conn.query("SELECT id FROM mv").unwrap().rows.is_empty());
    }
}

#[test]
fn standalone_refresh_drains_deferred_foreign_key_checks_before_commit() {
    for concurrent in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1)").unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
            .unwrap();
        conn.execute("CREATE UNIQUE INDEX mv_id ON mv(id)").unwrap();
        conn.execute("CREATE TABLE child(id INTEGER PRIMARY KEY,p INTEGER REFERENCES mv(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
        conn.execute("INSERT INTO child VALUES (1,1)").unwrap();
        conn.execute("DELETE FROM src").unwrap();
        assert!(matches!(
            refresh(&conn, concurrent),
            Err(SqlError::ForeignKeyViolation(_))
        ));
        assert_eq!(
            conn.query("SELECT id FROM mv").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
        assert_eq!(index_contents(&db, "mv", "mv_id").len(), 1);
    }
}
