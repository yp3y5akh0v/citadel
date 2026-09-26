use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{
    parser::{parse_sql, Statement},
    types::Collation,
    Connection, SqlError, Value,
};

#[test]
fn create_index_ast_preserves_omitted_and_explicit_binary_clauses() {
    let Statement::CreateIndex(statement) = parse_sql("CREATE INDEX ix ON t(a, b COLLATE BINARY, c COLLATE NOCASE, d COLLATE RTRIM, (LOWER(e)) COLLATE BINARY)").unwrap() else { panic!("CREATE INDEX expected") };
    assert_eq!(
        statement.collations,
        vec![
            None,
            Some(Collation::Binary),
            Some(Collation::NoCase),
            Some(Collation::Rtrim),
            Some(Collation::Binary)
        ]
    );
    assert!(statement.key_exprs[4].is_some());
    assert!(matches!(
        parse_sql("CREATE INDEX bad ON t((LOWER(e)) COLLATE NOCASE)"),
        Err(SqlError::Unsupported(_))
    ));
}

#[test]
fn explicit_binary_unique_index_keeps_case_distinct_keys_and_query_semantics() {
    for concurrent in ["", "CONCURRENTLY "] {
        let db = DatabaseBuilder::new("")
            .passphrase(b"explicit-index-collation")
            .argon2_profile(Argon2Profile::Iot)
            .create_in_memory()
            .unwrap();
        let c = Connection::open(&db).unwrap();
        c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)")
            .unwrap();
        c.execute("INSERT INTO t VALUES(1,'Alpha'),(2,'alpha')")
            .unwrap();
        assert!(matches!(
            c.execute(&format!(
                "CREATE UNIQUE INDEX {concurrent}inherited ON t(name)"
            )),
            Err(SqlError::UniqueViolation(_))
        ));
        c.execute(&format!(
            "CREATE UNIQUE INDEX {concurrent}explicit_binary ON t(name COLLATE BINARY)"
        ))
        .unwrap();
        assert_eq!(
            c.table_schema("t")
                .unwrap()
                .index_by_name("explicit_binary")
                .unwrap()
                .collation_at(0),
            Collation::Binary
        );
        c.execute("INSERT INTO t VALUES(3,'ALPHA')").unwrap();
        assert!(matches!(
            c.execute("INSERT INTO t VALUES(4,'Alpha')"),
            Err(SqlError::UniqueViolation(_))
        ));
        assert_eq!(
            c.query("SELECT id FROM t WHERE name='alpha' ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![Value::Integer(1)],
                vec![Value::Integer(2)],
                vec![Value::Integer(3)]
            ]
        );
        assert_eq!(
            c.query("SELECT id FROM t WHERE name COLLATE BINARY='alpha'")
                .unwrap()
                .rows,
            vec![vec![Value::Integer(2)]]
        );
    }
}

#[test]
fn omitted_composite_key_inherits_but_binary_override_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("collation.cdl"))
        .passphrase(b"index-collation-reopen")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let c = Connection::open(&db).unwrap();
    c.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT COLLATE NOCASE, b TEXT COLLATE RTRIM)",
    )
    .unwrap();
    c.execute("CREATE INDEX mixed ON t(a, b COLLATE BINARY)")
        .unwrap();
    drop(c);
    drop(db);
    let db = DatabaseBuilder::new(dir.path().join("collation.cdl"))
        .passphrase(b"index-collation-reopen")
        .open()
        .unwrap();
    let c = Connection::open(&db).unwrap();
    let table = c.table_schema("t").unwrap();
    let index = table.index_by_name("mixed").unwrap();
    assert_eq!(index.collation_at(0), Collation::NoCase);
    assert_eq!(index.collation_at(1), Collation::Binary);
}

#[test]
fn direct_ast_rejects_unsupported_expression_collations() {
    for concurrent in ["", "CONCURRENTLY "] {
        let db = DatabaseBuilder::new("")
            .passphrase(b"direct-index-collation")
            .argon2_profile(Argon2Profile::Iot)
            .create_in_memory()
            .unwrap();
        let c = Connection::open(&db).unwrap();
        c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        c.execute("INSERT INTO t VALUES(1,'Alpha')").unwrap();
        drop(c);
        let mut schema = citadel_sql::schema::SchemaManager::load(&db).unwrap();
        let Statement::CreateIndex(mut statement) = parse_sql(&format!(
            "CREATE INDEX {concurrent}expression_key ON t((LOWER(name)))"
        ))
        .unwrap() else {
            panic!("CREATE INDEX expected")
        };
        for collation in [Collation::NoCase, Collation::Rtrim] {
            statement.collations[0] = Some(collation);
            assert!(matches!(
                citadel_sql::executor::execute(
                    &db,
                    &mut schema,
                    &Statement::CreateIndex(statement.clone()),
                    &[]
                ),
                Err(SqlError::Unsupported(message))
                    if message == "expression index keys require BINARY collation"
            ));
            assert!(schema
                .get("t")
                .unwrap()
                .index_by_name("expression_key")
                .is_none());
            assert!(citadel_sql::schema::SchemaManager::load(&db)
                .unwrap()
                .get("t")
                .unwrap()
                .index_by_name("expression_key")
                .is_none());
        }
        statement.collations[0] = Some(Collation::Binary);
        citadel_sql::executor::execute(&db, &mut schema, &Statement::CreateIndex(statement), &[])
            .unwrap();
        assert!(schema
            .get("t")
            .unwrap()
            .index_by_name("expression_key")
            .is_some());
    }
}
