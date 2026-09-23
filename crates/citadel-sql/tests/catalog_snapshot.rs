use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"catalog-snapshot")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn ids(conn: &Connection<'_>, sql: &str) -> Vec<Vec<Value>> {
    conn.query(sql).unwrap().rows
}

#[test]
fn stale_writer_maintains_an_index_created_by_another_connection() {
    for prepared in [false, true] {
        let db = database();
        let a = Connection::open(&db).unwrap();
        a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        a.execute("CREATE UNIQUE INDEX i ON t(v)").unwrap();
        let insert = a.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
        let b = Connection::open(&db).unwrap();
        b.execute("CREATE UNIQUE INDEX j ON t(v)").unwrap();
        if prepared {
            insert
                .execute(&[Value::Integer(1), Value::Text("x".into())])
                .unwrap();
        } else {
            a.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        }
        b.execute("DROP INDEX i").unwrap();
        let c = Connection::open(&db).unwrap();
        assert_eq!(
            ids(&c, "SELECT id FROM t WHERE v = 'x'"),
            vec![vec![Value::Integer(1)]]
        );
        assert!(c.execute("INSERT INTO t VALUES (2, 'x')").is_err());
        assert_eq!(
            ids(&c, "SELECT COUNT(*) FROM t"),
            vec![vec![Value::Integer(1)]]
        );
    }
}

#[test]
fn cached_and_prepared_plans_follow_drop_and_same_name_recreation() {
    let db = database();
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 10, 20), (2, 20, 10)")
        .unwrap();
    a.execute("CREATE INDEX idx ON t(x)").unwrap();
    let sql = "SELECT id FROM t WHERE x = 10 ORDER BY id";
    let prepared = a.prepare(sql).unwrap();
    assert_eq!(a.query(sql).unwrap().rows, vec![vec![Value::Integer(1)]]);
    let b = Connection::open(&db).unwrap();
    b.execute("DROP INDEX idx").unwrap();
    b.execute("CREATE INDEX idx ON t(y)").unwrap();
    assert_eq!(
        prepared.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
    assert_eq!(a.query(sql).unwrap().rows, vec![vec![Value::Integer(1)]]);
}

#[test]
fn old_read_transaction_keeps_its_catalog_and_data_snapshot() {
    let db = database();
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 7)").unwrap();
    let prepared = a.prepare("SELECT v FROM t ORDER BY id").unwrap();
    a.execute("BEGIN READ ONLY").unwrap();
    let b = Connection::open(&db).unwrap();
    b.execute("DROP TABLE t").unwrap();
    b.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    b.execute("INSERT INTO t VALUES (2, 'new')").unwrap();
    a.refresh_schema().unwrap();
    assert_eq!(
        prepared.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(7)]]
    );
    a.execute("COMMIT").unwrap();
    assert_eq!(
        prepared.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Text("new".into())]]
    );
}

#[test]
fn prepared_full_scan_collect_and_stream_use_the_admitted_definition() {
    for stream in [false, true] {
        let db = database();
        let a = Connection::open(&db).unwrap();
        a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        a.execute("INSERT INTO t VALUES (1, 7)").unwrap();
        let prepared = a.prepare("SELECT * FROM t").unwrap();
        let b = Connection::open(&db).unwrap();
        b.execute("DROP TABLE t").unwrap();
        b.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, extra INTEGER)")
            .unwrap();
        b.execute("INSERT INTO t VALUES (2, 'new', 9)").unwrap();
        let result = if stream {
            prepared.query(&[]).unwrap().collect().unwrap()
        } else {
            prepared.query_collect(&[]).unwrap()
        };
        assert_eq!(result.columns.len(), 3);
        assert_eq!(
            result.rows,
            vec![vec![
                Value::Integer(2),
                Value::Text("new".into()),
                Value::Integer(9)
            ]]
        );
    }
}

#[test]
fn newly_declared_child_foreign_key_blocks_a_stale_parent_delete() {
    for prepared in [false, true] {
        let db = database();
        let a = Connection::open(&db).unwrap();
        a.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        a.execute("INSERT INTO parent VALUES (1)").unwrap();
        let delete = a.prepare("DELETE FROM parent WHERE id = $1").unwrap();
        let b = Connection::open(&db).unwrap();
        b.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id))")
            .unwrap();
        b.execute("INSERT INTO child VALUES (2, 1)").unwrap();
        let result = if prepared {
            delete.execute(&[Value::Integer(1)]).map(|_| ())
        } else {
            a.execute("DELETE FROM parent WHERE id = 1").map(|_| ())
        };
        assert!(result.is_err());
        assert_eq!(
            ids(&b, "SELECT id FROM parent"),
            vec![vec![Value::Integer(1)]]
        );
    }
}

#[test]
fn data_only_commits_do_not_rescan_catalog_rows() {
    let db = database();
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    for i in 0..24 {
        a.execute(&format!("CREATE TABLE unused_{i} (id INTEGER PRIMARY KEY)"))
            .unwrap();
    }
    let b = Connection::open(&db).unwrap();
    let read = b.prepare("SELECT v FROM t WHERE id = $1").unwrap();
    // First call admits the populated catalog; subsequent data-only commits
    // must cost bounded catalog point reads, not catalog row scans.
    read.query_collect(&[Value::Integer(0)]).unwrap();
    for i in 0..8 {
        a.execute_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Integer(i), Value::Integer(i)],
        )
        .unwrap();
        let scans = db.measure_scans();
        assert_eq!(
            read.query_collect(&[Value::Integer(i)]).unwrap().rows,
            vec![vec![Value::Integer(i)]]
        );
        assert_eq!(
            scans.rows_scanned(),
            0,
            "catalog rows must not be scanned for a data commit"
        );
    }
}

#[test]
fn local_ddl_savepoint_rollback_and_failed_ddl_preserve_schema() {
    let db = database();
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 1), (2, 1)").unwrap();
    let b = Connection::open(&db).unwrap();
    b.execute("BEGIN").unwrap();
    b.execute("SAVEPOINT s").unwrap();
    b.execute("CREATE INDEX idx ON t(v)").unwrap();
    let statement = b
        .prepare("SELECT id FROM t WHERE v = 1 ORDER BY id")
        .unwrap();
    b.execute("ROLLBACK TO s").unwrap();
    assert_eq!(statement.query_collect(&[]).unwrap().rows.len(), 2);
    b.execute("COMMIT").unwrap();
    assert!(a.execute("CREATE UNIQUE INDEX bad ON t(v)").is_err());
    assert_eq!(ids(&b, "SELECT id FROM t WHERE v = 1 ORDER BY id").len(), 2);
    assert!(b.table_schema("t").unwrap().indices.is_empty());
}

#[test]
fn public_executor_refreshes_stale_writer_catalog_before_mutation() {
    use citadel_sql::{executor, parser, schema::SchemaManager};
    let db = database();
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let mut schema = SchemaManager::load(&db).unwrap();
    a.execute("CREATE UNIQUE INDEX idx ON t(v)").unwrap();
    let insert = parser::parse_sql("INSERT INTO t VALUES (1, 'x')").unwrap();
    let mut wtx = db.begin_write().unwrap();
    executor::execute_in_txn(&mut wtx, &mut schema, &insert, &[]).unwrap();
    wtx.commit().unwrap();
    assert_eq!(
        ids(&a, "SELECT id FROM t WHERE v = 'x'"),
        vec![vec![Value::Integer(1)]]
    );
    let duplicate = parser::parse_sql("INSERT INTO t VALUES (2, 'x')").unwrap();
    assert!(executor::execute(&db, &mut schema, &duplicate, &[]).is_err());
}

#[test]
fn public_read_executor_rejects_mismatched_catalog_and_accepts_exact_reload() {
    use citadel_sql::{executor, parser, schema::SchemaManager};
    let db = database();
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    let schema = SchemaManager::load(&db).unwrap();
    a.execute("DROP TABLE t").unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    let select = parser::parse_sql("SELECT v FROM t").unwrap();
    let mut rtx = db.begin_read();
    assert!(executor::execute_with_read(&mut rtx, &schema, &select, &[]).is_err());
    let exact = SchemaManager::load_with_read(&db, &mut rtx).unwrap();
    let result = executor::execute_with_read(&mut rtx, &exact, &select, &[]).unwrap();
    assert!(
        matches!(result, citadel_sql::ExecutionResult::Query(q) if q.rows == vec![vec![Value::Text("x".into())]])
    );
}

#[test]
fn streaming_rows_retain_their_own_old_catalog_snapshot() {
    let db = database();
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 7), (2, 8)").unwrap();
    let statement = a.prepare("SELECT * FROM t").unwrap();
    let rows = statement.query(&[]).unwrap();
    let b = Connection::open(&db).unwrap();
    b.execute("DROP TABLE t").unwrap();
    b.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    b.execute("INSERT INTO t VALUES (3, 'new')").unwrap();
    assert_eq!(
        rows.collect().unwrap().rows,
        vec![
            vec![Value::Integer(1), Value::Integer(7)],
            vec![Value::Integer(2), Value::Integer(8)]
        ]
    );
    assert_eq!(
        statement.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(3), Value::Text("new".into())]]
    );
}

#[test]
fn public_immutable_insert_refuses_stale_schema_without_touching_writer() {
    use citadel_sql::{executor, parser, schema::SchemaManager};
    let db = database();
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let mut schema = SchemaManager::load(&db).unwrap();
    a.execute("CREATE UNIQUE INDEX idx ON t(v)").unwrap();
    let statement = parser::parse_sql("INSERT INTO t VALUES (1, 'x')").unwrap();
    let parser::Statement::Insert(insert) = &statement else {
        unreachable!()
    };
    let mut wtx = db.begin_write().unwrap();
    let before = wtx.mutation_marker();
    assert!(executor::exec_insert_in_txn(&mut wtx, &schema, insert, &[]).is_err());
    assert!(!wtx.mutated_since(before));
    executor::execute_in_txn(&mut wtx, &mut schema, &statement, &[]).unwrap();
    wtx.commit().unwrap();
    assert_eq!(
        ids(&a, "SELECT id FROM t WHERE v = 'x'"),
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn alternating_public_caches_observe_same_writer_catalog_edits() {
    use citadel_sql::{executor, parser, schema::SchemaManager};
    for rollback_branch in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, w TEXT)")
            .unwrap();
        let mut a = SchemaManager::load(&db).unwrap();
        let mut b = SchemaManager::load(&db).unwrap();
        let mut wtx = db.begin_write().unwrap();
        let create_i = parser::parse_sql("CREATE UNIQUE INDEX i ON t(v)").unwrap();
        executor::execute_in_txn(&mut wtx, &mut a, &create_i, &[]).unwrap();
        if rollback_branch {
            let checkpoint = wtx.begin_savepoint();
            executor::execute_in_txn(
                &mut wtx,
                &mut b,
                &parser::parse_sql("CREATE INDEX discarded ON t(w)").unwrap(),
                &[],
            )
            .unwrap();
            wtx.restore_snapshot(checkpoint);
        }
        executor::execute_in_txn(
            &mut wtx,
            &mut b,
            &parser::parse_sql("CREATE UNIQUE INDEX j ON t(w)").unwrap(),
            &[],
        )
        .unwrap();
        let insert = parser::parse_sql("INSERT INTO t VALUES (1, 'x', 'y')").unwrap();
        let parser::Statement::Insert(values) = &insert else {
            unreachable!()
        };
        let marker = wtx.mutation_marker();
        assert!(executor::exec_insert_in_txn(&mut wtx, &a, values, &[]).is_err());
        assert!(!wtx.mutated_since(marker));
        executor::execute_in_txn(&mut wtx, &mut a, &insert, &[]).unwrap();
        wtx.commit().unwrap();
        assert_eq!(
            ids(&conn, "SELECT id FROM t WHERE w = 'y'"),
            vec![vec![Value::Integer(1)]]
        );
        assert!(conn.execute("INSERT INTO t VALUES (2, 'z', 'y')").is_err());
    }
}

#[test]
fn catalog_identity_rejects_other_database_transactions_and_rebinds_db_entry() {
    use citadel_sql::{executor, parser, schema::SchemaManager};
    let a = database();
    let b = database();
    for (db, collation) in [(&a, "INTEGER"), (&b, "TEXT")] {
        let conn = Connection::open(db).unwrap();
        conn.execute(&format!(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, value {collation})"
        ))
        .unwrap();
    }
    let mut schema = SchemaManager::load(&a).unwrap();
    let select = parser::parse_sql("SELECT * FROM t").unwrap();
    let insert = parser::parse_sql("INSERT INTO t VALUES (1, 'text')").unwrap();
    let parser::Statement::Insert(values) = &insert else {
        unreachable!()
    };
    let mut read = b.begin_read();
    assert!(SchemaManager::load_with_read(&a, &mut read).is_err());
    assert!(executor::execute_with_read(&mut read, &schema, &select, &[]).is_err());
    let mut write = b.begin_write().unwrap();
    let marker = write.mutation_marker();
    assert!(executor::exec_insert_in_txn(&mut write, &schema, values, &[]).is_err());
    assert!(executor::execute_in_txn(&mut write, &mut schema, &insert, &[]).is_err());
    assert!(!write.mutated_since(marker));
    write.abort();
    // The db-taking entry pairs the correct cache and clears prior DB overlays.
    schema.register_temp_alias("old_alias", "t".into());
    schema.mark_dml("old_only");
    executor::execute(&b, &mut schema, &select, &[]).unwrap();
    assert_eq!(schema.resolve_temp("old_alias"), "old_alias");
    assert!(!schema.has_dml_dirty());
    executor::execute(&b, &mut schema, &insert, &[]).unwrap();
    let conn = Connection::open(&b).unwrap();
    assert_eq!(
        ids(&conn, "SELECT value FROM t"),
        vec![vec![Value::Text("text".into())]]
    );
}

#[test]
fn public_catalog_edits_are_not_blessed_by_unchanged_storage_stamps() {
    use citadel_sql::{executor, parser, schema::SchemaManager};
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 7)").unwrap();
    let mut schema = SchemaManager::load(&db).unwrap();
    let mut table = schema.get("t").unwrap().clone();
    table.columns[1].name = "invented".into();
    schema.register(table);
    let invented = parser::parse_sql("SELECT invented FROM t").unwrap();
    assert!(executor::execute_with_read(&mut db.begin_read(), &schema, &invented, &[]).is_err());
    // The mutable db-taking entry must reload even though the catalog root did
    // not change, and execute according to the actual persisted definition.
    let actual = parser::parse_sql("SELECT v FROM t").unwrap();
    let result = executor::execute(&db, &mut schema, &actual, &[]).unwrap();
    assert!(
        matches!(result, citadel_sql::ExecutionResult::Query(q) if q.rows == vec![vec![Value::Integer(7)]])
    );
    assert_eq!(schema.get("t").unwrap().columns[1].name, "v");
}

#[test]
fn public_read_proof_avoids_catalog_scans_after_data_only_commits() {
    use citadel_sql::{executor, parser, schema::SchemaManager};
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    for i in 0..24 {
        conn.execute(&format!("CREATE TABLE u{i} (id INTEGER PRIMARY KEY)"))
            .unwrap();
    }
    let schema = SchemaManager::load(&db).unwrap();
    conn.execute("INSERT INTO t VALUES (1, 7)").unwrap();
    let statement = parser::parse_sql("SELECT v FROM t WHERE id = 1").unwrap();
    let mut read = db.begin_read();
    let scans = db.measure_scans();
    let result = executor::execute_with_read(&mut read, &schema, &statement, &[]).unwrap();
    assert!(
        matches!(result, citadel_sql::ExecutionResult::Query(q) if q.rows == vec![vec![Value::Integer(7)]])
    );
    assert_eq!(scans.rows_scanned(), 0);
}

#[test]
fn concurrent_index_resolves_names_after_admitting_the_owned_database() {
    use citadel_sql::{executor, parser, schema::SchemaManager};
    let a = database();
    let b = database();
    {
        let conn = Connection::open(&a).unwrap();
        conn.execute("CREATE TABLE old_physical (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
    }
    let mut schema = SchemaManager::load(&a).unwrap();
    schema.register_temp_alias("t", "old_physical".into());
    let conn = Connection::open(&b).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 7)").unwrap();
    let statement = parser::parse_sql("CREATE INDEX CONCURRENTLY idx ON t(v)").unwrap();
    executor::execute(&b, &mut schema, &statement, &[]).unwrap();
    assert_eq!(schema.resolve_temp("t"), "t");
    assert!(schema.get("t").unwrap().index_by_name("idx").is_some());
    assert_eq!(
        ids(&conn, "SELECT id FROM t WHERE v = 7"),
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn schema_snapshot_restores_the_cache_owner_with_catalog_provenance() {
    use citadel_sql::{executor, parser, schema::SchemaManager};
    let a = database();
    let b = database();
    let conn = Connection::open(&a).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (7)").unwrap();
    let schema_a = SchemaManager::load(&a).unwrap();
    let mut schema_b = SchemaManager::load(&b).unwrap();
    schema_b.restore_snapshot(schema_a.save_snapshot());
    assert!(std::sync::Arc::ptr_eq(
        &schema_b.sql_caches,
        &a.sql_cache_handle()
    ));
    assert!(!std::sync::Arc::ptr_eq(
        &schema_b.sql_caches,
        &b.sql_cache_handle()
    ));
    let select = parser::parse_sql("SELECT id FROM t").unwrap();
    let result = executor::execute_with_read(&mut a.begin_read(), &schema_b, &select, &[]).unwrap();
    assert!(
        matches!(result, citadel_sql::ExecutionResult::Query(q) if q.rows == vec![vec![Value::Integer(7)]])
    );
}

#[test]
fn public_admission_charges_catalog_values_and_refuses_before_mutation() {
    use citadel_sql::{executor, parser, schema::SchemaManager};
    use citadel_txn::ReadBudget;
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE source (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE copied (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    conn.execute("INSERT INTO source VALUES (1, 'copied payload')")
        .unwrap();
    let schema = SchemaManager::load(&db).unwrap();
    let stmt = parser::parse_sql("INSERT INTO copied SELECT * FROM source").unwrap();
    let parser::Statement::Insert(insert) = stmt else {
        unreachable!()
    };
    let mut txn = db.begin_write().unwrap();
    let mut catalog_bytes = 0;
    for name in [b"_schema".as_slice(), b"_views", b"_triggers", b"_matviews"] {
        match txn.table_for_each(name, |_, value| {
            catalog_bytes += value.len();
            Ok(())
        }) {
            Ok(()) | Err(citadel_core::Error::TableNotFound(_)) => {}
            Err(error) => panic!("{error}"),
        }
    }
    let mut source_bytes = 0;
    txn.table_for_each(b"source", |_, value| {
        source_bytes += value.len();
        Ok(())
    })
    .unwrap();
    assert!(catalog_bytes > 0 && source_bytes > 0);
    let total = catalog_bytes + source_bytes;
    for refused in [ReadBudget::new(0, total), ReadBudget::new(total, 0)] {
        let remaining = refused.remaining();
        txn.set_read_budget(Some(refused.clone()));
        assert!(matches!(
            executor::exec_insert_in_txn(&mut txn, &schema, &insert, &[]),
            Err(citadel_sql::SqlError::Storage(
                citadel_core::Error::ReadBudgetExceeded { .. }
            ))
        ));
        assert_eq!(refused.remaining(), remaining);
        txn.set_read_budget(None);
        assert_eq!(txn.table_entry_count(b"copied").unwrap(), 0);
    }
    let short = ReadBudget::new(total, total - 1);
    txn.set_read_budget(Some(short.clone()));
    assert!(matches!(
        executor::exec_insert_in_txn(&mut txn, &schema, &insert, &[]),
        Err(citadel_sql::SqlError::Storage(
            citadel_core::Error::ReadBudgetExceeded { .. }
        ))
    ));
    assert_eq!(short.remaining(), source_bytes - 1);
    txn.set_read_budget(None);
    assert_eq!(txn.table_entry_count(b"copied").unwrap(), 0);
    let budget = ReadBudget::new(total, total);
    txn.set_read_budget(Some(budget.clone()));
    assert!(matches!(
        executor::exec_insert_in_txn(&mut txn, &schema, &insert, &[]).unwrap(),
        citadel_sql::ExecutionResult::RowsAffected(1)
    ));
    assert_eq!(budget.remaining(), 0);
    txn.set_read_budget(None);
    txn.commit().unwrap();
    assert_eq!(
        ids(&conn, "SELECT id FROM copied"),
        vec![vec![Value::Integer(1)]]
    );

    // An exact immutable-read admission also accounts for materialized catalog
    // values if public cache mutation invalidated its cheap freshness proof.
    let mut schema = SchemaManager::load(&db).unwrap();
    schema.register(schema.get("source").unwrap().clone());
    let select = parser::parse_sql("SELECT * FROM source").unwrap();
    let mut txn = db.begin_read();
    txn.set_read_budget(Some(ReadBudget::new(0, 0)));
    assert!(matches!(
        executor::execute_with_read(&mut txn, &schema, &select, &[]),
        Err(citadel_sql::SqlError::Storage(
            citadel_core::Error::ReadBudgetExceeded { .. }
        ))
    ));
    let budget = ReadBudget::new(total, total);
    txn.set_read_budget(Some(budget.clone()));
    executor::execute_with_read(&mut txn, &schema, &select, &[]).unwrap();
    assert_eq!(budget.remaining(), 0);
}

#[test]
fn runtime_bound_columns_cannot_be_persisted_as_schema_constants() {
    use citadel_sql::{
        parser::{self, Expr},
        schema::SchemaManager,
        types::Collation,
    };
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let schema = SchemaManager::load(&db).unwrap();
    let mut table = schema.get("t").unwrap().clone();
    table.columns[1].default_sql = Some("'captured'".into());
    table.columns[1].default_expr = Some(Expr::BoundColumn {
        value: Value::Text("captured".into()),
        collation: Collation::NoCase,
    });
    assert!(matches!(
        table.try_serialize(),
        Err(citadel_sql::SqlError::InvalidValue(_))
    ));

    // A runtime capture must also be rejected below query input nodes, which
    // are not projection or WHERE expressions of the containing SELECT.
    for in_derived in [false, true] {
        let captured = table.columns[1].default_expr.clone().unwrap();
        let mut expr = parser::parse_sql_expr(if in_derived {
            "EXISTS (SELECT 1 FROM (SELECT 1 AS v) d)"
        } else {
            "EXISTS (SELECT 1 FROM json_array_elements('[1]'))"
        })
        .unwrap();
        let Expr::Exists { subquery, .. } = &mut expr else {
            unreachable!()
        };
        if in_derived {
            let parser::QueryBody::Select(inner) =
                &mut subquery.from_subquery.as_mut().unwrap().query.body
            else {
                unreachable!()
            };
            inner.columns = vec![parser::SelectColumn::Expr {
                expr: captured,
                alias: Some("v".into()),
            }];
        } else {
            subquery.from_args = Some(vec![captured]);
        }
        table.columns[1].default_expr = Some(expr);
        assert!(matches!(
            table.try_serialize(),
            Err(citadel_sql::SqlError::InvalidValue(_))
        ));
        table.columns[1].default_expr = Some(Expr::BoundColumn {
            value: Value::Text("captured".into()),
            collation: Collation::NoCase,
        });
    }
}

#[test]
fn own_ddl_retains_catalog_proof_without_scanning_catalog_rows() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1,7)").unwrap();
    for mode in 0..3 {
        let ddl = format!("CREATE INDEX by_v_{mode} ON t(v)");
        match mode {
            0 => {
                conn.execute(&ddl).unwrap();
            }
            1 => {
                conn.execute("BEGIN").unwrap();
                conn.execute(&ddl).unwrap();
                conn.execute("COMMIT").unwrap();
            }
            2 => {
                conn.execute_batch(&ddl).unwrap();
            }
            _ => unreachable!(),
        }
        let measured = db.measure_scans();
        assert_eq!(
            conn.query("SELECT v FROM t WHERE id=1").unwrap().rows,
            vec![vec![Value::Integer(7)]]
        );
        assert_eq!(
            measured.rows_scanned(),
            0,
            "own DDL mode {mode} unnecessarily rescanned catalogs"
        );
    }
}

#[test]
fn written_catalog_proof_accepts_only_matching_committed_read_roots() {
    use citadel_sql::{executor, parser, schema::SchemaManager};
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1,7)").unwrap();
    let mut schema = SchemaManager::load(&db).unwrap();
    let mut old_read = db.begin_read();
    let mut writer = db.begin_write().unwrap();
    executor::execute_in_txn(
        &mut writer,
        &mut schema,
        &parser::parse_sql("CREATE INDEX by_v ON t(v)").unwrap(),
        &[],
    )
    .unwrap();
    writer.commit().unwrap();
    let select = parser::parse_sql("SELECT v FROM t WHERE id=1").unwrap();
    assert!(matches!(
        executor::execute_with_read(&mut old_read, &schema, &select, &[]),
        Err(citadel_sql::SqlError::InvalidValue(_))
    ));
    let mut read = db.begin_read();
    let measured = db.measure_scans();
    assert!(matches!(
        executor::execute_with_read(&mut read, &schema, &select, &[]).unwrap(),
        citadel_sql::ExecutionResult::Query(_)
    ));
    assert_eq!(
        measured.rows_scanned(),
        0,
        "written proof was lost despite matching local generation and roots"
    );
}
