use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{executor, parser, schema::SchemaManager, Connection, SqlError, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"join-function-sources")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn setup(conn: &Connection<'_>) {
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("CREATE TABLE json_array_elements (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO json_array_elements VALUES (99)")
        .unwrap();
}

fn rejects_function_source<T>(result: Result<T, SqlError>) {
    match result {
        Err(SqlError::Unsupported(message)) => {
            assert!(message.starts_with("table function on the right side of JOIN:"));
        }
        Err(error) => panic!("expected unsupported JOIN source, got {error:?}"),
        Ok(_) => panic!("JOIN function arguments must not be ignored"),
    }
}

#[test]
fn cached_and_prepared_joins_do_not_substitute_same_named_tables() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for sql in [
        "SELECT j.id FROM t JOIN json_array_elements('[1,2]'::JSON) j ON true",
        "SELECT j.id FROM t, json_array_elements('[1,2]'::JSON) j",
    ] {
        rejects_function_source(conn.execute(sql));
        rejects_function_source(conn.execute(sql));
        let prepared = conn.prepare(sql).unwrap();
        rejects_function_source(prepared.execute(&[]));
        rejects_function_source(prepared.query_collect(&[]));
        rejects_function_source(prepared.query(&[]));
    }
    assert_eq!(
        conn.query("SELECT j.id FROM t JOIN json_array_elements j ON true")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(99)]]
    );
}

#[test]
fn cte_derived_and_compound_joins_reject_function_sources() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for sql in [
        "WITH json_array_elements AS (SELECT 99 AS id) SELECT j.id FROM t JOIN json_array_elements('[1,2]'::JSON) j ON true",
        "SELECT d.id FROM (SELECT j.id FROM t JOIN json_array_elements('[1,2]'::JSON) j ON true) d",
        "SELECT j.id FROM t JOIN json_array_elements('[1,2]'::JSON) j ON true UNION ALL SELECT 5",
        "SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM t x JOIN json_array_elements('[1,2]'::JSON) j ON true)",
    ] {
        rejects_function_source(conn.execute(sql));
    }
}

#[test]
fn unknown_join_functions_are_explicit_and_do_not_poison_read_statements() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let sql = "SELECT t.id FROM t JOIN missing_function('[1,2]') j ON true";
    rejects_function_source(conn.execute(sql));
    conn.execute("BEGIN").unwrap();
    rejects_function_source(conn.execute(sql));
    rejects_function_source(
        conn.execute("SELECT j.id FROM t JOIN json_array_elements('[1,2]'::JSON) j ON true"),
    );
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(conn.query("SELECT id FROM t").unwrap().rows.len(), 2);
}

#[test]
fn public_ast_read_and_write_execution_validate_join_sources() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let mut statement =
        parser::parse_sql("SELECT j.id FROM t JOIN json_array_elements j ON true").unwrap();
    let parser::Statement::Select(query) = &mut statement else {
        unreachable!()
    };
    let parser::QueryBody::Select(select) = &mut query.body else {
        unreachable!()
    };
    select.joins[0].table.args = Some(vec![parser::Expr::Literal(Value::Text("[1,2]".into()))]);
    {
        let mut read = db.begin_read();
        let schema = SchemaManager::load_with_read(&db, &mut read).unwrap();
        rejects_function_source(executor::execute_with_read(
            &mut read,
            &schema,
            &statement,
            &[],
        ));
    }
    let mut schema = SchemaManager::load(&db).unwrap();
    let mut write = db.begin_write().unwrap();
    rejects_function_source(executor::execute_in_txn(
        &mut write,
        &mut schema,
        &statement,
        &[],
    ));
    write.abort();
}

#[test]
fn supported_primary_from_functions_still_execute() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    assert_eq!(
        conn.query("SELECT value FROM json_array_elements('[1,2]'::JSON)")
            .unwrap()
            .rows
            .len(),
        2
    );
    assert_eq!(
        conn.query("SELECT f.value FROM json_array_elements('[1,2]'::JSON) f JOIN t ON true")
            .unwrap()
            .rows
            .len(),
        2
    );
}
