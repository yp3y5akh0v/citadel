use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("delete-returning.db"))
        .passphrase(b"delete-returning-test")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn returning(conn: &Connection<'_>, sql: &str, prepared: bool) -> QueryResult {
    if prepared {
        conn.prepare(sql).unwrap().query_collect(&[]).unwrap()
    } else {
        conn.query(sql).unwrap()
    }
}

#[test]
fn empty_delete_returning_preserves_columns_in_explicit_and_cte_execution() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    for explicit in [false, true] {
        for prepared in [false, true] {
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            for predicate in ["", " WHERE id < 0"] {
                let result = returning(
                    &conn,
                    &format!("DELETE FROM src{predicate} RETURNING id AS deleted_id, val"),
                    prepared,
                );
                assert_eq!(result.columns, ["deleted_id", "val"]);
                assert!(result.rows.is_empty());
            }
            let sql = "WITH d AS (DELETE FROM src RETURNING *) INSERT INTO archive SELECT * FROM d";
            let result = if prepared {
                conn.prepare(sql)
                    .unwrap()
                    .execute(&[])
                    .map(ExecutionResult::RowsAffected)
            } else {
                conn.execute(sql)
            };
            assert!(matches!(result.unwrap(), ExecutionResult::RowsAffected(0)));
            if explicit {
                conn.execute("COMMIT").unwrap();
            }
        }
    }
    assert!(conn.query("SELECT * FROM archive").unwrap().rows.is_empty());
}

#[test]
fn empty_delete_still_fires_statement_triggers_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO counter VALUES (1, 0)").unwrap();
    conn.execute("CREATE TRIGGER before_del BEFORE DELETE ON t FOR EACH STATEMENT BEGIN UPDATE counter SET n=n+1 WHERE id=1; END").unwrap();
    conn.execute("CREATE TRIGGER after_del AFTER DELETE ON t FOR EACH STATEMENT BEGIN UPDATE counter SET n=n+10 WHERE id=1; END").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM t WHERE id<0").unwrap();
    let result = returning(&conn, "DELETE FROM t RETURNING id", true);
    assert_eq!(result.columns, ["id"]);
    assert!(result.rows.is_empty());
    assert_eq!(
        conn.query("SELECT n FROM counter").unwrap().rows,
        [vec![Value::Integer(22)]]
    );
    conn.execute("COMMIT").unwrap();
}

#[test]
fn full_delete_returning_projects_old_rows_cleans_indexes_and_restores_savepoints() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, txt TEXT, doubled INTEGER GENERATED ALWAYS AS (a*2) VIRTUAL)").unwrap();
    conn.execute("CREATE INDEX a_index ON t(a)").unwrap();
    let insert = conn
        .prepare("INSERT INTO t(id,a,txt) VALUES ($1,$2,$3)")
        .unwrap();
    let overflow = "x".repeat(12_000);
    for id in 0..40i64 {
        insert
            .execute(&[
                Value::Integer(id),
                if id == 3 {
                    Value::Null
                } else {
                    Value::Integer(id * 10)
                },
                Value::Text(overflow.clone().into()),
            ])
            .unwrap();
    }
    conn.execute("ALTER TABLE t ADD COLUMN added INTEGER DEFAULT 7")
        .unwrap();
    let expected = conn
        .query("SELECT id,a,txt,doubled,added FROM t ORDER BY id")
        .unwrap();
    let mut old_reader = db.begin_read();
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT kept").unwrap();
    let result = returning(
        &conn,
        "DELETE FROM t RETURNING old.*, new.id AS absent, COALESCE(a,0)+1 AS calc",
        true,
    );
    assert_eq!(
        result.columns,
        ["id", "a", "txt", "doubled", "added", "absent", "calc"]
    );
    let expected_projected: Vec<_> = expected
        .rows
        .iter()
        .map(|row| {
            let mut row = row.clone();
            let calc = match row[1] {
                Value::Integer(a) => a + 1,
                Value::Null => 1,
                _ => unreachable!(),
            };
            row.push(Value::Null);
            row.push(Value::Integer(calc));
            row
        })
        .collect();
    assert_eq!(result.rows, expected_projected);
    assert!(conn
        .query("SELECT id FROM t WHERE a=100")
        .unwrap()
        .rows
        .is_empty());
    conn.execute("ROLLBACK TO kept").unwrap();
    let restored = conn
        .query("SELECT id,a,txt,doubled,added FROM t ORDER BY id")
        .unwrap();
    assert_eq!(restored.columns, expected.columns);
    assert_eq!(restored.rows, expected.rows);
    assert_eq!(
        returning(&conn, "DELETE FROM t RETURNING *", false).rows,
        expected.rows
    );
    conn.execute("COMMIT").unwrap();
    let mut old_count = 0;
    old_reader
        .table_for_each(b"t", |_, _| {
            old_count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(old_count, 40);
    drop(old_reader);
    assert!(db.manager().integrity_check().unwrap().is_ok());
    assert!(conn
        .query("SELECT id FROM t WHERE a=100")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn failed_dml_cte_destination_restores_source_and_prior_explicit_work() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1,10),(2,20)")
        .unwrap();
    conn.execute("INSERT INTO archive VALUES (2,99)").unwrap();
    let sql = "WITH d AS (DELETE FROM src RETURNING *) INSERT INTO archive SELECT * FROM d";
    assert!(matches!(
        conn.prepare(sql).unwrap().execute(&[]),
        Err(SqlError::DuplicateKey)
    ));
    assert_eq!(
        conn.query("SELECT * FROM src ORDER BY id").unwrap().rows,
        [
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)]
        ]
    );
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO src VALUES (3,30)").unwrap();
    conn.execute("SAVEPOINT kept").unwrap();
    assert!(conn.execute(sql).is_err());
    conn.execute("ROLLBACK TO kept").unwrap();
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM src").unwrap().rows,
        [vec![Value::Integer(3)]]
    );
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        conn.query("SELECT * FROM archive").unwrap().rows,
        [vec![Value::Integer(2), Value::Integer(99)]]
    );
    assert!(db.manager().integrity_check().unwrap().is_ok());
}

#[test]
fn full_delete_returning_preserves_fk_actions_and_row_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p INTEGER REFERENCES p(id) ON DELETE CASCADE)",
    )
    .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TRIGGER seen AFTER DELETE ON c FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.id); END").unwrap();
    conn.execute("INSERT INTO p VALUES (1),(2)").unwrap();
    conn.execute("INSERT INTO c VALUES (10,1),(20,2)").unwrap();
    assert_eq!(
        returning(&conn, "DELETE FROM p RETURNING id", true).rows,
        [vec![Value::Integer(1)], vec![Value::Integer(2)]]
    );
    assert_eq!(
        conn.query("SELECT id FROM log ORDER BY id").unwrap().rows,
        [vec![Value::Integer(10)], vec![Value::Integer(20)]]
    );
    assert!(conn.query("SELECT * FROM c").unwrap().rows.is_empty());
}
