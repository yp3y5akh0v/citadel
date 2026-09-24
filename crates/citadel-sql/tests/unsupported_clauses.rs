use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"unsupported-clause-regression")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn setup(conn: &Connection<'_>) {
    conn.execute("CREATE TABLE q (id INTEGER PRIMARY KEY, done INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO q VALUES (1,1),(2,1),(3,0)")
        .unwrap();
    conn.execute("CREATE TABLE other (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO other VALUES (1)").unwrap();
}

fn rejects<T>(sql: &str, result: Result<T, SqlError>) {
    match result {
        Err(SqlError::Unsupported(_)) => {}
        Err(error) => panic!("{sql}: expected an unsupported-feature error, got {error:?}"),
        Ok(_) => panic!("{sql}: accepted unsupported semantics"),
    }
}

fn rejects_mutations(statements: &[&str]) {
    for &sql in statements {
        for explicit in [false, true] {
            let db = database();
            let conn = Connection::open(&db).unwrap();
            setup(&conn);
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            let before = conn.query("SELECT * FROM q ORDER BY id").unwrap().rows;
            let result = conn.execute(sql);
            let after = conn.query("SELECT * FROM q ORDER BY id").unwrap().rows;
            assert!(
                matches!(result, Err(SqlError::Unsupported(_))),
                "{sql}, explicit={explicit}: {result:?}; rows={after:?}"
            );
            assert_eq!(after, before, "{sql}, explicit={explicit}");
            rejects(sql, conn.prepare(sql));
            // Parser refusal must leave the caller's transaction usable.
            conn.execute("INSERT INTO q VALUES (4,0)").unwrap();
            if explicit {
                conn.execute("COMMIT").unwrap();
            }
            assert_eq!(
                conn.query("SELECT done FROM q WHERE id=4").unwrap().rows,
                vec![vec![Value::Integer(0)]]
            );
        }
    }
}

#[test]
fn delete_modifiers_are_rejected_before_mutation() {
    rejects_mutations(&[
        "DELETE FROM q WHERE done=1 LIMIT 1",
        "DELETE FROM q ORDER BY id LIMIT 1",
        "DELETE FROM q USING other WHERE q.id=other.id",
    ]);
}

#[test]
fn update_modifiers_are_rejected_before_mutation() {
    rejects_mutations(&[
        "UPDATE q SET done=2 WHERE done=1 LIMIT 1",
        "UPDATE q SET done=2 FROM other WHERE q.id=other.id",
    ]);
}

#[test]
fn insert_conflict_policies_are_not_silently_changed() {
    rejects_mutations(&[
        "INSERT OR IGNORE INTO q VALUES (4,0)",
        "INSERT OR REPLACE INTO q VALUES (1,2)",
    ]);
}

#[test]
fn semi_and_anti_joins_are_not_reinterpreted_as_outer_joins() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for join in ["LEFT SEMI", "LEFT ANTI", "RIGHT SEMI", "RIGHT ANTI"] {
        let sql = format!("SELECT q.id FROM q {join} JOIN other ON q.id=other.id ORDER BY q.id");
        let result = conn.query(&sql);
        assert!(
            matches!(result, Err(SqlError::Unsupported(_))),
            "{sql}: {result:?}"
        );
        rejects(&sql, conn.prepare(&sql));
    }
}

#[test]
fn unsupported_query_modifiers_are_checked_in_nested_sources() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for sql in [
        "SELECT TOP 1 id FROM q",
        "SELECT id FROM q FETCH FIRST 1 ROWS ONLY",
        "SELECT id FROM q QUALIFY id=1",
        "SELECT id FROM q FOR UPDATE",
        "SELECT * FROM (SELECT TOP 1 id FROM q) AS limited",
        "WITH limited AS (SELECT id FROM q FETCH FIRST 1 ROWS ONLY) SELECT * FROM limited",
    ] {
        rejects(sql, conn.query(sql));
        rejects(sql, conn.prepare(sql));
    }
}

#[test]
fn rejected_insert_source_does_not_write_a_partial_result() {
    rejects_mutations(&[
        "INSERT INTO q SELECT id+10,done FROM q FETCH FIRST 1 ROWS ONLY",
        "INSERT INTO q SELECT TOP 1 id+10,done FROM q",
    ]);
}
