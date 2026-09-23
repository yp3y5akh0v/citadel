use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::executor::{exec_insert_in_txn, execute, execute_in_txn};
use citadel_sql::parser::{parse_sql, Statement};
use citadel_sql::schema::SchemaManager;
use citadel_sql::{Connection, SqlError, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"collated-constraints")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

#[test]
fn collated_primary_keys_reject_equivalent_inserts_in_every_insert_route() {
    for (collation, original, equivalent) in
        [("NOCASE", "Alpha", "alpha"), ("RTRIM", "Alpha  ", "Alpha")]
    {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute(&format!(
            "CREATE TABLE names (name TEXT COLLATE {collation} PRIMARY KEY, n INTEGER)"
        ))
        .unwrap();
        conn.execute(&format!("INSERT INTO names VALUES ('{original}', 1)"))
            .unwrap();
        for sql in [
            format!("INSERT INTO names VALUES ('{equivalent}', 2)"),
            format!("INSERT INTO names VALUES ('unrelated', 3), ('{equivalent}', 2)"),
            format!("INSERT INTO names SELECT '{equivalent}', 2"),
        ] {
            assert!(
                matches!(conn.execute(&sql), Err(SqlError::DuplicateKey)),
                "{sql}"
            );
            assert_eq!(
                conn.query("SELECT n FROM names").unwrap().rows,
                vec![vec![Value::Integer(1)]]
            );
        }
        assert!(matches!(
            conn.prepare("INSERT INTO names VALUES ($1, $2)")
                .unwrap()
                .execute(&[Value::Text(equivalent.into()), Value::Integer(2)]),
            Err(SqlError::DuplicateKey)
        ));
        conn.execute("CREATE TABLE source (name TEXT PRIMARY KEY, n INTEGER)")
            .unwrap();
        conn.execute(&format!("INSERT INTO source VALUES ('{equivalent}', 2)"))
            .unwrap();
        assert!(matches!(
            conn.execute("INSERT INTO names SELECT * FROM source"),
            Err(SqlError::DuplicateKey)
        ));
    }
}

#[test]
fn composite_and_binary_primary_key_constraints_preserve_distinctions() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE names (tenant INTEGER, name TEXT COLLATE NOCASE, PRIMARY KEY(tenant,name))",
    )
    .unwrap();
    conn.execute("INSERT INTO names VALUES (1,'A'),(2,'a'),(1,'Ä'),(1,'ä')")
        .unwrap();
    assert!(matches!(
        conn.execute("INSERT INTO names VALUES (1,'a')"),
        Err(SqlError::DuplicateKey)
    ));
    conn.execute("CREATE TABLE binary_names (name TEXT PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO binary_names VALUES ('A'),('a'),('A ')")
        .unwrap();
    assert!(conn
        .table_schema("binary_names")
        .unwrap()
        .indices
        .is_empty());
}

#[test]
fn collated_pk_updates_handle_equivalent_spelling_and_rollback_collisions() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names (name TEXT COLLATE NOCASE PRIMARY KEY,n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO names VALUES ('A',1),('B',2)")
        .unwrap();
    conn.execute("UPDATE names SET name='a' WHERE n=1").unwrap();
    assert_eq!(
        conn.query("SELECT name FROM names WHERE n=1").unwrap().rows,
        vec![vec![Value::Text("a".into())]]
    );
    assert!(matches!(
        conn.execute("UPDATE names SET name='b' WHERE n=1"),
        Err(SqlError::DuplicateKey)
    ));
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT before_change").unwrap();
    conn.execute("UPDATE names SET name='C' WHERE n=1").unwrap();
    conn.execute("ROLLBACK TO before_change").unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        conn.query("SELECT name,n FROM names ORDER BY n")
            .unwrap()
            .rows,
        vec![
            vec![Value::Text("a".into()), Value::Integer(1)],
            vec![Value::Text("B".into()), Value::Integer(2)]
        ]
    );
}

#[test]
fn explicit_upsert_target_wins_over_unrelated_proposed_conflicts() {
    for stored_spelling in ["A", "a"] {
        for target in ["name", "email"] {
            for prepared in [false, true] {
                for transaction in [false, true] {
                    let db = database();
                    let conn = Connection::open(&db).unwrap();
                    conn.execute("CREATE TABLE people (name TEXT COLLATE NOCASE PRIMARY KEY,email TEXT UNIQUE,note TEXT)").unwrap();
                    conn.execute("INSERT INTO people VALUES ('A','x','one'),('B','y','two')")
                        .unwrap();
                    if transaction {
                        conn.execute("BEGIN").unwrap();
                    }
                    let sql = format!("INSERT INTO people VALUES ('{stored_spelling}','y','new') ON CONFLICT({target}) DO UPDATE SET note='ok' RETURNING name");
                    let result = if prepared {
                        conn.prepare(&sql).unwrap().query_collect(&[]).unwrap()
                    } else {
                        conn.query(&sql).unwrap()
                    };
                    assert_eq!(
                        result.rows,
                        vec![vec![Value::Text(
                            if target == "name" { "A" } else { "B" }.into()
                        )]],
                        "{sql}"
                    );
                    if transaction {
                        conn.execute("COMMIT").unwrap();
                    }
                    assert_eq!(
                        conn.query("SELECT note FROM people ORDER BY name")
                            .unwrap()
                            .rows,
                        if target == "name" {
                            vec![
                                vec![Value::Text("ok".into())],
                                vec![Value::Text("two".into())],
                            ]
                        } else {
                            vec![
                                vec![Value::Text("one".into())],
                                vec![Value::Text("ok".into())],
                            ]
                        }
                    );
                }
            }
        }
    }
}

#[test]
fn upsert_checks_final_constraints_and_collated_unique_arbiter() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE people (name TEXT COLLATE NOCASE PRIMARY KEY,email TEXT COLLATE NOCASE UNIQUE,n INTEGER)").unwrap();
    conn.execute("INSERT INTO people VALUES ('A','X',1),('B','Y',2)")
        .unwrap();
    conn.execute("INSERT INTO people VALUES ('a','y',0) ON CONFLICT(email) DO UPDATE SET n=9")
        .unwrap();
    assert!(matches!(conn.execute("INSERT INTO people VALUES ('a','y',0) ON CONFLICT(email) DO UPDATE SET name=excluded.name"), Err(SqlError::DuplicateKey)));
    for sql in [
        "INSERT INTO people VALUES ('a','y',0) ON CONFLICT DO NOTHING",
        "INSERT INTO people VALUES ('a','y',0) ON CONFLICT(name) DO NOTHING",
        "INSERT INTO people VALUES ('a','y',0) ON CONFLICT(email) DO NOTHING",
    ] {
        conn.execute(sql).unwrap();
    }
    assert_eq!(
        conn.query("SELECT name,n FROM people ORDER BY name")
            .unwrap()
            .rows,
        vec![
            vec![Value::Text("A".into()), Value::Integer(1)],
            vec![Value::Text("B".into()), Value::Integer(9)]
        ]
    );
}

fn strip_pk_indexes(db: &Database, table: &str) {
    let conn = Connection::open(db).unwrap();
    let mut schema = conn.table_schema(table).unwrap();
    drop(conn);
    let mut write = db.begin_write().unwrap();
    for index in schema.indices.drain(..) {
        write
            .drop_table(&citadel_sql::TableSchema::index_table_name(
                table,
                &index.name,
            ))
            .unwrap();
    }
    citadel_sql::schema::SchemaManager::save_schema(&mut write, &schema).unwrap();
    write.commit().unwrap();
}

#[test]
fn old_valid_catalog_is_reconciled_once_and_sound_open_needs_no_writer() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names(name TEXT COLLATE NOCASE PRIMARY KEY,n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO names VALUES ('A',1),('B',2)")
        .unwrap();
    drop(conn);
    strip_pk_indexes(&db, "names");
    let conn = Connection::open(&db).unwrap();
    assert_eq!(conn.table_schema("names").unwrap().indices.len(), 1);
    assert!(matches!(
        conn.execute("INSERT INTO names VALUES ('a',3)"),
        Err(SqlError::DuplicateKey)
    ));
    drop(conn);
    let writer = db.begin_write().unwrap();
    let reopened = Connection::open(&db).expect("sound admission must not request another writer");
    assert_eq!(reopened.table_schema("names").unwrap().indices.len(), 1);
    drop(reopened);
    drop(writer);
}

#[test]
fn ambiguous_old_catalog_fails_without_committing_catalog_or_row_changes() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names(name TEXT COLLATE NOCASE PRIMARY KEY,n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO names VALUES ('A',1)").unwrap();
    drop(conn);
    strip_pk_indexes(&db, "names");
    let mut write = db.begin_write().unwrap();
    write
        .table_insert(
            b"names",
            &citadel_sql::encoding::encode_composite_key(&[Value::Text("a".into())]),
            &citadel_sql::encoding::encode_row(&[Value::Integer(2)]),
        )
        .unwrap();
    write.commit().unwrap();
    let before = db.begin_read().table_get(b"_schema", b"names").unwrap();
    let error = match Connection::open(&db) {
        Ok(_) => panic!("ambiguous PK admitted"),
        Err(error) => error,
    };
    assert!(matches!(error, SqlError::UniqueViolation(_)), "{error:?}");
    let mut read = db.begin_read();
    assert_eq!(read.table_get(b"_schema", b"names").unwrap(), before);
    assert_eq!(read.table_entry_count(b"names").unwrap(), 2);
}

#[test]
fn dropping_last_pk_equality_index_is_rejected_and_exact_replacement_works() {
    let db = database();
    let first = Connection::open(&db).unwrap();
    first
        .execute("CREATE TABLE names(name TEXT COLLATE NOCASE PRIMARY KEY,n INTEGER)")
        .unwrap();
    first.execute("INSERT INTO names VALUES ('A',1)").unwrap();
    let original = first.table_schema("names").unwrap().indices[0].name.clone();
    let statement = first.prepare("INSERT INTO names VALUES ($1,$2)").unwrap();
    assert!(matches!(
        first.execute(&format!("DROP INDEX {original}")),
        Err(SqlError::Unsupported(_))
    ));
    first
        .execute("CREATE UNIQUE INDEX replacement ON names(name)")
        .unwrap();
    first.execute(&format!("DROP INDEX {original}")).unwrap();
    assert!(matches!(
        statement.execute(&[Value::Text("a".into()), Value::Integer(2)]),
        Err(SqlError::DuplicateKey)
    ));
    let second = Connection::open(&db).unwrap();
    assert_eq!(second.table_schema("names").unwrap().indices.len(), 1);
    assert!(matches!(
        second.execute("DROP INDEX replacement"),
        Err(SqlError::Unsupported(_))
    ));
}

#[test]
fn public_mutable_executors_upgrade_their_exact_writer_catalog() {
    for caller_owned in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE names(name TEXT COLLATE NOCASE PRIMARY KEY,n INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO names VALUES ('A',1)").unwrap();
        strip_pk_indexes(&db, "names");
        let mut schema = SchemaManager::load(&db).unwrap();
        let insert = parse_sql("INSERT INTO names VALUES ('B',2)").unwrap();
        if caller_owned {
            let mut wtx = db.begin_write().unwrap();
            execute_in_txn(&mut wtx, &mut schema, &insert, &[]).unwrap();
            wtx.commit().unwrap();
        } else {
            execute(&db, &mut schema, &insert, &[]).unwrap();
        }
        assert_eq!(schema.get("names").unwrap().indices.len(), 1);
        assert!(matches!(
            execute(
                &db,
                &mut schema,
                &parse_sql("INSERT INTO names VALUES ('a',3)").unwrap(),
                &[]
            ),
            Err(SqlError::DuplicateKey)
        ));
        assert_eq!(db.begin_read().table_entry_count(b"names").unwrap(), 2);
    }
}

#[test]
fn immutable_insert_refuses_unsound_metadata_without_mutating_writer() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names(name TEXT COLLATE NOCASE PRIMARY KEY)")
        .unwrap();
    strip_pk_indexes(&db, "names");
    let schema = SchemaManager::load(&db).unwrap();
    let Statement::Insert(insert) = parse_sql("INSERT INTO names VALUES ('A')").unwrap() else {
        panic!()
    };
    let mut wtx = db.begin_write().unwrap();
    let before = wtx.mutation_marker();
    assert!(matches!(
        exec_insert_in_txn(&mut wtx, &schema, &insert, &[]),
        Err(SqlError::InvalidValue(_))
    ));
    assert!(!wtx.mutated_since(before));
    assert_eq!(wtx.table_entry_count(b"names").unwrap(), 0);
    wtx.commit().unwrap();
    assert!(SchemaManager::load(&db)
        .unwrap()
        .get("names")
        .unwrap()
        .indices
        .is_empty());
}

#[test]
fn later_statement_failure_restores_catalog_before_admission_backfill() {
    for prepared in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE names(name TEXT COLLATE NOCASE PRIMARY KEY,n INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO names VALUES ('A',1)").unwrap();
        let insert = conn.prepare("INSERT INTO names VALUES ($1,$2)").unwrap();
        strip_pk_indexes(&db, "names");
        let result = if prepared {
            insert
                .execute(&[Value::Text("a".into()), Value::Integer(2)])
                .map(|_| ())
        } else {
            conn.execute("INSERT INTO names VALUES ('a',2)").map(|_| ())
        };
        assert!(matches!(result, Err(SqlError::DuplicateKey)));
        assert!(conn.table_schema("names").unwrap().indices.is_empty());
        assert!(SchemaManager::load(&db)
            .unwrap()
            .get("names")
            .unwrap()
            .indices
            .is_empty());
        conn.execute("INSERT INTO names VALUES ('B',2)").unwrap();
        assert_eq!(conn.table_schema("names").unwrap().indices.len(), 1);
    }
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names(name TEXT COLLATE NOCASE PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO names VALUES ('A')").unwrap();
    strip_pk_indexes(&db, "names");
    let mut schema = SchemaManager::load(&db).unwrap();
    assert!(matches!(
        execute(
            &db,
            &mut schema,
            &parse_sql("INSERT INTO names VALUES ('a')").unwrap(),
            &[]
        ),
        Err(SqlError::DuplicateKey)
    ));
    assert!(schema.get("names").unwrap().indices.is_empty());
}

#[test]
fn later_duplicate_backfill_rolls_back_every_tree_in_caller_owned_writer() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for name in ["a_valid", "z_invalid"] {
        conn.execute(&format!(
            "CREATE TABLE {name}(name TEXT COLLATE NOCASE PRIMARY KEY,n INTEGER)"
        ))
        .unwrap();
        conn.execute(&format!("INSERT INTO {name} VALUES ('A',1)"))
            .unwrap();
    }
    // Strip both in one raw transaction: opening SQL between strips would
    // intentionally reconcile the first table.
    let mut schema = SchemaManager::load(&db).unwrap();
    let mut raw = db.begin_write().unwrap();
    let mut removed = Vec::new();
    for name in ["a_valid", "z_invalid"] {
        let mut table = schema.get(name).unwrap().clone();
        for index in table.indices.drain(..) {
            let storage = citadel_sql::TableSchema::index_table_name(name, &index.name);
            raw.drop_table(&storage).unwrap();
            removed.push(storage);
        }
        SchemaManager::save_schema(&mut raw, &table).unwrap();
    }
    raw.table_insert(
        b"z_invalid",
        &citadel_sql::encoding::encode_composite_key(&[Value::Text("a".into())]),
        &citadel_sql::encoding::encode_row(&[Value::Integer(2)]),
    )
    .unwrap();
    raw.commit().unwrap();
    let mut wtx = db.begin_write().unwrap();
    let original = [
        wtx.table_get(b"_schema", b"a_valid").unwrap(),
        wtx.table_get(b"_schema", b"z_invalid").unwrap(),
    ];
    let error = execute_in_txn(
        &mut wtx,
        &mut schema,
        &parse_sql("INSERT INTO a_valid VALUES ('B',2)").unwrap(),
        &[],
    )
    .unwrap_err();
    assert!(matches!(error, SqlError::UniqueViolation(_)));
    assert_eq!(wtx.table_get(b"_schema", b"a_valid").unwrap(), original[0]);
    assert_eq!(
        wtx.table_get(b"_schema", b"z_invalid").unwrap(),
        original[1]
    );
    for storage in removed {
        assert!(wtx.table_root_stamp(&storage).unwrap().is_none());
    }
    assert!(schema.get("a_valid").unwrap().indices.is_empty());
    assert!(schema.get("z_invalid").unwrap().indices.is_empty());
    // Admission itself was atomic; the caller's otherwise usable writer can
    // still be committed without publishing partial constraint backfills.
    wtx.commit().unwrap();
}

#[test]
fn valid_existing_equality_index_is_reused_without_a_write() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names(name TEXT COLLATE NOCASE PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO names VALUES ('A')").unwrap();
    conn.execute("CREATE UNIQUE INDEX explicit_equality ON names(name)")
        .unwrap();
    let mut table = conn.table_schema("names").unwrap();
    let original = table.indices.remove(0);
    let mut raw = db.begin_write().unwrap();
    raw.drop_table(&citadel_sql::TableSchema::index_table_name(
        "names",
        &original.name,
    ))
    .unwrap();
    SchemaManager::save_schema(&mut raw, &table).unwrap();
    raw.commit().unwrap();
    let writer = db.begin_write().unwrap();
    let reopened = Connection::open(&db).unwrap();
    assert_eq!(
        reopened.table_schema("names").unwrap().indices[0].name,
        "explicit_equality"
    );
    drop(writer);
    assert!(matches!(
        reopened.execute("INSERT INTO names VALUES ('a')"),
        Err(SqlError::DuplicateKey)
    ));
}

#[test]
fn missing_declared_physical_index_is_not_silently_rebuilt() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names(name TEXT COLLATE NOCASE PRIMARY KEY)")
        .unwrap();
    let table = conn.table_schema("names").unwrap();
    let storage = citadel_sql::TableSchema::index_table_name("names", &table.indices[0].name);
    let mut raw = db.begin_write().unwrap();
    raw.drop_table(&storage).unwrap();
    raw.commit().unwrap();
    let reopened = Connection::open(&db).unwrap();
    assert!(matches!(
        reopened.execute("INSERT INTO names VALUES ('A')"),
        Err(SqlError::Storage(citadel_core::Error::TableNotFound(_)))
    ));
    assert!(db
        .begin_read()
        .table_root_stamp(&storage)
        .unwrap()
        .is_none());
    assert_eq!(db.begin_read().table_entry_count(b"names").unwrap(), 0);
}

#[test]
fn missing_index_admission_reports_busy_writer_and_persists_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("collated.cdl");
    {
        let db = DatabaseBuilder::new(&path)
            .passphrase(b"collated-persistence")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE names(name TEXT COLLATE RTRIM PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO names VALUES ('A  ')").unwrap();
        strip_pk_indexes(&db, "names");
        let writer = db.begin_write().unwrap();
        assert!(matches!(
            Connection::open(&db),
            Err(SqlError::Storage(
                citadel_core::Error::WriteTransactionActive
            ))
        ));
        drop(writer);
        let upgraded = Connection::open(&db).unwrap();
        assert_eq!(upgraded.table_schema("names").unwrap().indices.len(), 1);
    }
    let db = DatabaseBuilder::new(&path)
        .passphrase(b"collated-persistence")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    assert!(matches!(
        conn.execute("INSERT INTO names VALUES ('A')"),
        Err(SqlError::DuplicateKey)
    ));
    assert_eq!(
        conn.query("SELECT name FROM names").unwrap().rows,
        vec![vec![Value::Text("A  ".into())]]
    );
}

#[test]
fn unrelated_unique_constraint_never_becomes_the_primary_key_arbiter() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names(name TEXT PRIMARY KEY,email TEXT UNIQUE,n INTEGER)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX folded_name ON names(name COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO names VALUES ('A','x',1),('B','y',2)")
        .unwrap();
    assert!(matches!(
        conn.execute("INSERT INTO names VALUES ('a','z',3) ON CONFLICT(name) DO UPDATE SET n=9"),
        Err(SqlError::UniqueViolation(_))
    ));
    assert_eq!(conn.query("INSERT INTO names VALUES ('A','y',3) ON CONFLICT(email) DO UPDATE SET n=9 RETURNING name").unwrap().rows,
        vec![vec![Value::Text("B".into())]]);
}

#[test]
fn rtrim_composite_upsert_and_update_use_the_stored_physical_row() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names(tenant INTEGER,name TEXT COLLATE RTRIM,n INTEGER,PRIMARY KEY(tenant,name))").unwrap();
    conn.execute("INSERT INTO names VALUES (1,'A  ',1),(1,'B',2),(2,'A',3)")
        .unwrap();
    let upsert = conn.prepare("INSERT INTO names VALUES ($1,$2,9) ON CONFLICT(tenant,name) DO UPDATE SET n=excluded.n RETURNING name").unwrap();
    assert_eq!(
        upsert
            .query_collect(&[Value::Integer(1), Value::Text("A".into())])
            .unwrap()
            .rows,
        vec![vec![Value::Text("A  ".into())]]
    );
    conn.execute("UPDATE names SET name='A' WHERE tenant=1 AND n=9")
        .unwrap();
    assert!(matches!(
        conn.execute("UPDATE names SET name='B  ' WHERE tenant=1 AND n=9"),
        Err(SqlError::DuplicateKey)
    ));
    assert_eq!(
        conn.query("SELECT tenant,name,n FROM names ORDER BY tenant,name")
            .unwrap()
            .rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(9)
            ],
            vec![
                Value::Integer(1),
                Value::Text("B".into()),
                Value::Integer(2)
            ],
            vec![
                Value::Integer(2),
                Value::Text("A".into()),
                Value::Integer(3)
            ]
        ]
    );
}

#[test]
fn triggered_and_generated_rows_share_pk_enforcement_and_statement_rollback() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names(raw TEXT,name TEXT COLLATE NOCASE GENERATED ALWAYS AS (raw) STORED,PRIMARY KEY(name))").unwrap();
    conn.execute("INSERT INTO names(raw) VALUES ('A')").unwrap();
    assert!(matches!(
        conn.execute("INSERT INTO names(raw) VALUES ('a')"),
        Err(SqlError::DuplicateKey)
    ));
    conn.execute("CREATE TABLE source(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TRIGGER collision AFTER INSERT ON source FOR EACH ROW BEGIN INSERT INTO names(raw) VALUES ('a'); END").unwrap();
    assert!(matches!(
        conn.execute("INSERT INTO source VALUES (1)"),
        Err(SqlError::DuplicateKey)
    ));
    assert!(conn.query("SELECT id FROM source").unwrap().rows.is_empty());
    assert_eq!(
        conn.query("SELECT name FROM names").unwrap().rows,
        vec![vec![Value::Text("A".into())]]
    );
}

#[test]
fn stale_writer_maintains_new_pk_replacement_and_indexed_writes_do_not_scan_rows() {
    let db = database();
    let first = Connection::open(&db).unwrap();
    first
        .execute("CREATE TABLE names(name TEXT COLLATE NOCASE PRIMARY KEY,n INTEGER)")
        .unwrap();
    let original = first.table_schema("names").unwrap().indices[0].name.clone();
    let insert = first.prepare("INSERT INTO names VALUES ($1,$2)").unwrap();
    let second = Connection::open(&db).unwrap();
    second
        .execute("CREATE UNIQUE INDEX replacement ON names(name)")
        .unwrap();
    insert
        .execute(&[Value::Text("A".into()), Value::Integer(1)])
        .unwrap();
    second.execute(&format!("DROP INDEX {original}")).unwrap();
    assert!(matches!(
        insert.execute(&[Value::Text("a".into()), Value::Integer(2)]),
        Err(SqlError::DuplicateKey)
    ));
    // Warm the refreshed catalog once; the remaining indexed point writes
    // must not scan existing rows or catalog records on each data commit.
    insert
        .execute(&[Value::Text("warm".into()), Value::Integer(2)])
        .unwrap();
    for n in 0..8 {
        let scans = db.measure_scans();
        insert
            .execute(&[Value::Text(format!("row{n}").into()), Value::Integer(n)])
            .unwrap();
        assert_eq!(scans.rows_scanned(), 0);
    }
}
