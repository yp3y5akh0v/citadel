use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{executor, parser, schema::SchemaManager, Connection, SqlError, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"trigger-when-validation")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn rejects_when_subquery<T>(result: Result<T, SqlError>) {
    match result {
        Err(SqlError::Unsupported(message)) => assert_eq!(
            message,
            "subqueries are not supported in trigger WHEN conditions"
        ),
        Err(error) => panic!("expected unsupported trigger WHEN subquery, got {error:?}"),
        Ok(_) => panic!("trigger WHEN subquery must be rejected at creation"),
    }
}

#[test]
fn unsupported_when_conditions_never_register_a_trigger() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    for granularity in ["ROW", "STATEMENT"] {
        for condition in [
            "(SELECT id FROM parent) = 1",
            "EXISTS (SELECT 1 FROM parent)",
            "1 IN (SELECT id FROM parent)",
            "1 = ANY (SELECT id FROM parent)",
            "1 = ALL (SELECT id FROM parent)",
            "COALESCE((SELECT id FROM parent), 1) = 1",
            "CASE WHEN TRUE THEN TRUE ELSE EXISTS (SELECT 1 FROM parent) END",
        ] {
            let sql = format!(
                "CREATE TRIGGER invalid_when AFTER INSERT ON parent FOR EACH {granularity} \
                 WHEN ({condition}) BEGIN SELECT 1; END"
            );
            rejects_when_subquery(conn.execute(&sql));
            assert!(SchemaManager::load(&db)
                .unwrap()
                .find_trigger("invalid_when")
                .is_none());
        }
    }
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    assert_eq!(
        conn.query("SELECT id FROM parent").unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn prepared_create_rejects_when_subquery_before_registration() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    let prepared = conn
        .prepare(
            "CREATE TRIGGER invalid_when BEFORE INSERT ON parent FOR EACH ROW \
             WHEN EXISTS (SELECT 1 FROM parent) BEGIN SELECT 1; END",
        )
        .unwrap();
    rejects_when_subquery(prepared.execute(&[]));
    assert!(SchemaManager::load(&db)
        .unwrap()
        .find_trigger("invalid_when")
        .is_none());
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
}

#[test]
fn public_ast_validates_both_when_representations() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    for (when_sql, when_expr) in [
        (Some("EXISTS (SELECT 1 FROM parent)"), Some("TRUE")),
        (Some("TRUE"), Some("EXISTS (SELECT 1 FROM parent)")),
        (Some("EXISTS (SELECT 1 FROM parent)"), None),
        (None, Some("EXISTS (SELECT 1 FROM parent)")),
    ] {
        let mut statement = parser::parse_sql(
            "CREATE TRIGGER invalid_when AFTER INSERT ON parent FOR EACH ROW \
             BEGIN SELECT 1; END",
        )
        .unwrap();
        let parser::Statement::CreateTrigger(trigger) = &mut statement else {
            unreachable!()
        };
        trigger.when_sql = when_sql.map(str::to_owned);
        trigger.when_expr = when_expr.map(parser::parse_sql_expr).transpose().unwrap();
        let mut schema = SchemaManager::load(&db).unwrap();
        let mut write = db.begin_write().unwrap();
        rejects_when_subquery(executor::execute_in_txn(
            &mut write,
            &mut schema,
            &statement,
            &[],
        ));
        assert!(schema.find_trigger("invalid_when").is_none());
        write.abort();
    }
    assert!(SchemaManager::load(&db)
        .unwrap()
        .find_trigger("invalid_when")
        .is_none());
}

#[test]
fn scalar_when_conditions_preserve_subqueries_in_trigger_bodies() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER copy_positive AFTER INSERT ON parent FOR EACH ROW \
         WHEN NEW.id > 0 BEGIN INSERT INTO audit \
         SELECT id FROM parent WHERE id > 0 AND id IN (SELECT id FROM parent); END",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (-1)").unwrap();
    assert!(conn.query("SELECT id FROM audit").unwrap().rows.is_empty());
    conn.execute("INSERT INTO parent VALUES (2)").unwrap();
    assert_eq!(
        conn.query("SELECT id FROM audit").unwrap().rows,
        vec![vec![Value::Integer(2)]]
    );
}
