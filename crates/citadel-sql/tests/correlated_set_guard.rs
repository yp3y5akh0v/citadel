use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{executor, parser, schema::SchemaManager, Connection, SqlError, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"correlated-set-guard")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn fixture(conn: &Connection<'_>) {
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, only_outer INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, x INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1,10,1),(3,30,3)")
        .unwrap();
    conn.execute("INSERT INTO r VALUES (1,100),(2,200)")
        .unwrap();
}

fn rejects_capture<T>(result: Result<T, SqlError>) {
    match result {
        Err(SqlError::Unsupported(message)) => {
            assert_eq!(message, "correlated subqueries in UPDATE SET expressions")
        }
        Err(error) => panic!("expected unsupported correlated SET, got {error:?}"),
        Ok(_) => panic!("correlated SET must not be materialized once for all rows"),
    }
}

#[test]
fn correlated_set_is_rejected_before_mutation_on_every_execution_path() {
    for expr in [
        "(SELECT COUNT(*) FROM r WHERE r.id = t.id)",
        "(SELECT COUNT(*) FROM r WHERE r.id = only_outer)",
        "COALESCE((SELECT t.v), 0)",
        "CASE WHEN FALSE THEN (SELECT t.v) ELSE 7 END",
    ] {
        for mode in 0..5 {
            let db = database();
            let conn = Connection::open(&db).unwrap();
            fixture(&conn);
            let sql = format!("UPDATE t SET v = {expr}");
            match mode {
                0 => rejects_capture(conn.execute(&sql)),
                1 => rejects_capture(conn.prepare(&sql).unwrap().execute(&[])),
                2 => {
                    conn.execute("BEGIN").unwrap();
                    rejects_capture(conn.execute(&sql));
                    conn.execute("INSERT INTO r VALUES (4,400)").unwrap();
                    conn.execute("COMMIT").unwrap();
                }
                3 => rejects_capture(conn.execute_batch(&sql)),
                4 => {
                    let mut schema = SchemaManager::load(&db).unwrap();
                    let mut txn = db.begin_write().unwrap();
                    rejects_capture(executor::execute_in_txn(
                        &mut txn,
                        &mut schema,
                        &parser::parse_sql(&sql).unwrap(),
                        &[],
                    ));
                    txn.commit().unwrap();
                }
                _ => unreachable!(),
            }
            assert_eq!(
                conn.query("SELECT id,v FROM t ORDER BY id").unwrap().rows,
                vec![
                    vec![Value::Integer(1), Value::Integer(10)],
                    vec![Value::Integer(3), Value::Integer(30)],
                ],
                "{mode}: {sql}"
            );
        }
    }
}

#[test]
fn local_columns_and_aliases_in_set_subqueries_are_not_outer_captures() {
    for expr in [
        "(SELECT MAX(x) FROM r WHERE id > 0)",
        "(SELECT MAX(t.x) FROM r AS t WHERE t.id > 0)",
        "(SELECT MAX(t.v) FROM t WHERE t.id > 0)",
    ] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        fixture(&conn);
        conn.execute(&format!("UPDATE t SET v = {expr}")).unwrap();
        let expected = if expr.contains("FROM t WHERE") {
            30
        } else {
            200
        };
        assert_eq!(
            conn.query("SELECT v FROM t ORDER BY id").unwrap().rows,
            vec![
                vec![Value::Integer(expected)],
                vec![Value::Integer(expected)]
            ],
            "{expr}"
        );
    }
}

#[test]
fn uncorrelated_set_remains_supported_with_a_correlated_where() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    fixture(&conn);
    conn.execute(
        "UPDATE t SET v = (SELECT MAX(x) FROM r) \
         WHERE EXISTS (SELECT 1 FROM r WHERE r.id = t.id)",
    )
    .unwrap();
    assert_eq!(
        conn.query("SELECT id,v FROM t ORDER BY id").unwrap().rows,
        vec![
            vec![Value::Integer(1), Value::Integer(200)],
            vec![Value::Integer(3), Value::Integer(30)],
        ]
    );
}

#[test]
fn missing_outer_column_cannot_resolve_to_an_inner_column() {
    for sql in [
        "UPDATE t SET v = (SELECT MAX(x) FROM r WHERE t.x > 0)",
        "UPDATE t SET v = 7 WHERE EXISTS (SELECT 1 FROM r WHERE t.x > 0)",
        "DELETE FROM t WHERE EXISTS (SELECT 1 FROM r WHERE t.x > 0)",
    ] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        fixture(&conn);
        conn.execute("BEGIN").unwrap();
        let error = conn.execute(sql).unwrap_err();
        assert!(
            matches!(&error, SqlError::ColumnNotFound(name) if name == "t.x"),
            "{sql}: {error:?}"
        );
        conn.execute("COMMIT").unwrap();
        assert_eq!(
            conn.query("SELECT v FROM t ORDER BY id").unwrap().rows,
            vec![vec![Value::Integer(10)], vec![Value::Integer(30)]]
        );
    }
}

#[test]
fn builtin_virtual_columns_shadow_outer_columns_without_scanning_for_schema() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, table_name TEXT, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1,'different',0)")
        .unwrap();
    conn.execute(
        "UPDATE t SET v = (SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 't')",
    )
    .unwrap();
    assert_eq!(
        conn.query("SELECT v FROM t").unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
}
