use super::*;
use crate::parser::{BinOp, Expr, GeneratedKind};
use crate::types::{Collation, ColumnDef, DataType, Value};

type IndexPrescanHook = Box<dyn FnOnce(&Database)>;

thread_local! {
    static INDEX_PRESCAN_HOOK: std::cell::RefCell<Option<IndexPrescanHook>> =
        const { std::cell::RefCell::new(None) };
}

struct IndexPrescanHookGuard {
    previous: Option<IndexPrescanHook>,
}

impl Drop for IndexPrescanHookGuard {
    fn drop(&mut self) {
        INDEX_PRESCAN_HOOK.with(|slot| {
            *slot.borrow_mut() = self.previous.take();
        });
    }
}

fn on_index_prescan(hook: impl FnOnce(&Database) + 'static) -> IndexPrescanHookGuard {
    let previous = INDEX_PRESCAN_HOOK.with(|slot| slot.borrow_mut().replace(Box::new(hook)));
    IndexPrescanHookGuard { previous }
}

pub(super) fn after_index_prescan(db: &Database) {
    let hook = INDEX_PRESCAN_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook(db);
    }
}

fn col(name: &str, dt: DataType) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: dt,
        nullable: true,
        position: 0,
        default_expr: None,
        default_sql: None,
        check_expr: None,
        check_sql: None,
        check_name: None,
        is_with_timezone: false,
        generated_expr: None,
        generated_sql: None,
        generated_kind: None,
        collation: Collation::Binary,
    }
}

fn i(n: i64) -> Value {
    Value::Integer(n)
}

fn repeated_primary_key_statement(columns: &str) -> CreateTableStmt {
    let sql = format!(
        "CREATE TABLE items (a INTEGER, z INTEGER, email TEXT UNIQUE, PRIMARY KEY ({columns}))"
    );
    let Statement::CreateTable(stmt) = crate::parser::parse_sql(&sql).unwrap() else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(stmt.primary_key, columns.split(", ").collect::<Vec<_>>());
    stmt
}

fn ddl_database() -> Database {
    citadel::DatabaseBuilder::new("")
        .passphrase(b"test-passphrase")
        .argon2_profile(citadel::Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn concurrent_index_statement(unique: bool) -> CreateIndexStmt {
    let unique = if unique { "UNIQUE " } else { "" };
    let sql = format!("CREATE {unique}INDEX CONCURRENTLY by_a ON items (a)");
    let Statement::CreateIndex(stmt) = crate::parser::parse_sql(&sql).unwrap() else {
        panic!("expected CREATE INDEX");
    };
    stmt
}

fn assert_concurrent_index_absent(db: &Database, schema: &SchemaManager) {
    assert!(schema.get("items").unwrap().index_by_name("by_a").is_none());
    assert!(SchemaManager::load(db)
        .unwrap()
        .get("items")
        .unwrap()
        .index_by_name("by_a")
        .is_none());
    let index_table = TableSchema::index_table_name("items", "by_a");
    assert!(db
        .begin_read()
        .table_root_stamp(&index_table)
        .unwrap()
        .is_none());
}

fn assert_index_keys(db: &Database, index: &str, expected: &[(i64, i64)]) {
    let index_table = TableSchema::index_table_name("items", index);
    let mut entries = Vec::new();
    db.begin_read()
        .table_for_each(&index_table, |key, value| {
            entries.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        entries,
        expected
            .iter()
            .map(|&(value, id)| (
                crate::encoding::encode_composite_key(&[i(value), i(id)]),
                Vec::new()
            ))
            .collect::<Vec<_>>()
    );
}

#[test]
fn concurrent_index_rebuilds_after_prescan_rows_change() {
    let db = ddl_database();
    {
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, a INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap();
    }
    let mut schema = SchemaManager::load(&db).unwrap();
    let _hook = on_index_prescan(|db| {
        let conn = crate::Connection::open(db).unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("UPDATE items SET a = 11 WHERE id = 1")
            .unwrap();
        conn.execute("DELETE FROM items WHERE id = 2").unwrap();
        conn.execute("INSERT INTO items VALUES (4, 40)").unwrap();
        conn.execute("COMMIT").unwrap();
    });

    exec_create_index(&db, &mut schema, &concurrent_index_statement(false)).unwrap();

    assert_index_keys(&db, "by_a", &[(11, 1), (30, 3), (40, 4)]);
    let conn = crate::Connection::open(&db).unwrap();
    for (a, ids) in [
        (10, vec![]),
        (11, vec![1]),
        (20, vec![]),
        (30, vec![3]),
        (40, vec![4]),
    ] {
        let rows = conn
            .query(&format!("SELECT id FROM items WHERE a = {a}"))
            .unwrap()
            .rows;
        assert_eq!(
            rows,
            ids.into_iter().map(|id| vec![i(id)]).collect::<Vec<_>>()
        );
    }
}

#[test]
fn concurrent_unique_index_rejects_duplicates_added_after_prescan() {
    let db = ddl_database();
    {
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, a INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, 10), (2, 20)")
            .unwrap();
    }
    let mut schema = SchemaManager::load(&db).unwrap();
    let before = db.manager().commit_generation();
    let _hook = on_index_prescan(|db| {
        crate::Connection::open(db)
            .unwrap()
            .execute("INSERT INTO items VALUES (3, 10)")
            .unwrap();
    });

    let error = exec_create_index(&db, &mut schema, &concurrent_index_statement(true)).unwrap_err();

    assert!(matches!(error, SqlError::UniqueViolation(name) if name == "by_a"));
    assert_eq!(db.manager().commit_generation(), before + 1);
    assert_concurrent_index_absent(&db, &schema);
    let conn = crate::Connection::open(&db).unwrap();
    assert_eq!(
        conn.query("SELECT id, a FROM items ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![i(1), i(10)], vec![i(2), i(20)], vec![i(3), i(10)]]
    );
    conn.execute("DELETE FROM items WHERE id = 3").unwrap();
    exec_create_index(&db, &mut schema, &concurrent_index_statement(true)).unwrap();
}

#[test]
fn concurrent_index_cancellation_after_prescan_prevents_publication() {
    let db = ddl_database();
    {
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, a INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, 10), (2, 20)")
            .unwrap();
    }
    let mut schema = SchemaManager::load(&db).unwrap();
    let before = db.manager().commit_generation();
    let token = citadel::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let hook_token = token.clone();
    let _hook = on_index_prescan(move |_| hook_token.cancel());

    let error =
        exec_create_index(&db, &mut schema, &concurrent_index_statement(false)).unwrap_err();

    assert!(token.is_cancelled());
    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
    db.set_cancel(None);
    assert_eq!(db.manager().commit_generation(), before);
    assert_concurrent_index_absent(&db, &schema);
    exec_create_index(&db, &mut schema, &concurrent_index_statement(false)).unwrap();
    assert!(SchemaManager::load(&db)
        .unwrap()
        .get("items")
        .unwrap()
        .index_by_name("by_a")
        .is_some());
}

#[test]
fn concurrent_index_preserves_columns_added_after_prescan() {
    let db = ddl_database();
    {
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, a INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, 10), (2, 20)")
            .unwrap();
    }
    let mut schema = SchemaManager::load(&db).unwrap();
    let _hook = on_index_prescan(|db| {
        let conn = crate::Connection::open(db).unwrap();
        conn.execute("ALTER TABLE items ADD COLUMN extra INTEGER DEFAULT 7")
            .unwrap();
        conn.execute("UPDATE items SET extra = 9 WHERE id = 2")
            .unwrap();
    });

    exec_create_index(&db, &mut schema, &concurrent_index_statement(false)).unwrap();

    let loaded = SchemaManager::load(&db).unwrap();
    let table = loaded.get("items").unwrap();
    assert_eq!(
        table
            .columns
            .iter()
            .map(|col| col.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "a", "extra"]
    );
    assert_eq!(table.index_by_name("by_a").unwrap().columns_vec(), [1]);
    assert_index_keys(&db, "by_a", &[(10, 1), (20, 2)]);
    let conn = crate::Connection::open(&db).unwrap();
    assert_eq!(
        conn.query("SELECT id, a, extra FROM items ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![i(1), i(10), i(7)], vec![i(2), i(20), i(9)]]
    );
}

#[test]
fn concurrent_index_preserves_other_index_created_after_prescan() {
    let db = ddl_database();
    {
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, 10, 100), (2, 20, 200)")
            .unwrap();
    }
    let mut schema = SchemaManager::load(&db).unwrap();
    let _hook = on_index_prescan(|db| {
        crate::Connection::open(db)
            .unwrap()
            .execute("CREATE INDEX by_b ON items (b)")
            .unwrap();
    });

    exec_create_index(&db, &mut schema, &concurrent_index_statement(false)).unwrap();

    let loaded = SchemaManager::load(&db).unwrap();
    let table = loaded.get("items").unwrap();
    assert_eq!(table.indices.len(), 2);
    assert_eq!(table.index_by_name("by_a").unwrap().columns_vec(), [1]);
    assert_eq!(table.index_by_name("by_b").unwrap().columns_vec(), [2]);
    assert_index_keys(&db, "by_a", &[(10, 1), (20, 2)]);
    assert_index_keys(&db, "by_b", &[(100, 1), (200, 2)]);
    let conn = crate::Connection::open(&db).unwrap();
    conn.execute("UPDATE items SET a = 11, b = 101 WHERE id = 1")
        .unwrap();
    assert_index_keys(&db, "by_a", &[(11, 1), (20, 2)]);
    assert_index_keys(&db, "by_b", &[(101, 1), (200, 2)]);
    for predicate in ["a = 11", "b = 101"] {
        assert_eq!(
            conn.query(&format!("SELECT id FROM items WHERE {predicate}"))
                .unwrap()
                .rows,
            vec![vec![i(1)]]
        );
    }
}

#[test]
fn stale_connection_index_build_uses_current_schema() {
    for (explicit, concurrently) in [(false, ""), (true, ""), (false, "CONCURRENTLY ")] {
        for column in ["a", "extra"] {
            let db = ddl_database();
            let updater = crate::Connection::open(&db).unwrap();
            updater
                .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, a INTEGER)")
                .unwrap();
            updater
                .execute("INSERT INTO items VALUES (1, 10), (2, 20)")
                .unwrap();
            let stale = crate::Connection::open(&db).unwrap();
            updater
                .execute("ALTER TABLE items ADD COLUMN extra INTEGER DEFAULT 7")
                .unwrap();
            updater
                .execute("UPDATE items SET extra = 9 WHERE id = 2")
                .unwrap();
            if explicit {
                stale.execute("BEGIN").unwrap();
            }

            stale
                .execute(&format!(
                    "CREATE INDEX {concurrently}by_value ON items ({column})"
                ))
                .unwrap();
            if explicit {
                stale.execute("COMMIT").unwrap();
            }

            let loaded = SchemaManager::load(&db).unwrap();
            let table = loaded.get("items").unwrap();
            assert_eq!(
                table
                    .columns
                    .iter()
                    .map(|col| col.name.as_str())
                    .collect::<Vec<_>>(),
                ["id", "a", "extra"]
            );
            let (position, expected) = if column == "a" {
                (1, [(10, 1), (20, 2)])
            } else {
                (2, [(7, 1), (9, 2)])
            };
            assert_eq!(
                table.index_by_name("by_value").unwrap().columns_vec(),
                [position]
            );
            assert_index_keys(&db, "by_value", &expected);
            let fresh = crate::Connection::open(&db).unwrap();
            assert_eq!(
                fresh
                    .query("SELECT id, a, extra FROM items ORDER BY id")
                    .unwrap()
                    .rows,
                vec![vec![i(1), i(10), i(7)], vec![i(2), i(20), i(9)]]
            );
        }
    }
}

#[test]
fn concurrent_index_respects_name_created_after_prescan() {
    for if_not_exists in [false, true] {
        let db = ddl_database();
        {
            let conn = crate::Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
                .unwrap();
            conn.execute("INSERT INTO items VALUES (1, 10, 100), (2, 20, 200)")
                .unwrap();
        }
        let mut schema = SchemaManager::load(&db).unwrap();
        let before = db.manager().commit_generation();
        let _hook = on_index_prescan(|db| {
            crate::Connection::open(db)
                .unwrap()
                .execute("CREATE INDEX by_a ON items (b)")
                .unwrap();
        });
        let mut stmt = concurrent_index_statement(false);
        stmt.if_not_exists = if_not_exists;

        let result = exec_create_index(&db, &mut schema, &stmt);

        if if_not_exists {
            assert!(matches!(result.unwrap(), ExecutionResult::Ok));
        } else {
            assert!(
                matches!(result.unwrap_err(), SqlError::IndexAlreadyExists(name) if name == "by_a")
            );
        }
        assert_eq!(db.manager().commit_generation(), before + 1);
        let loaded = SchemaManager::load(&db).unwrap();
        let table = loaded.get("items").unwrap();
        assert_eq!(table.indices.len(), 1);
        assert_eq!(table.index_by_name("by_a").unwrap().columns_vec(), [2]);
        assert_index_keys(&db, "by_a", &[(100, 1), (200, 2)]);
    }
}

#[test]
fn concurrent_index_rebuilds_after_target_recreation() {
    let db = ddl_database();
    {
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, a INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, 10)").unwrap();
    }
    let mut schema = SchemaManager::load(&db).unwrap();
    let _hook = on_index_prescan(|db| {
        let conn = crate::Connection::open(db).unwrap();
        conn.execute("DROP TABLE items").unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, b INTEGER, a INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (7, 70, 700)")
            .unwrap();
    });

    exec_create_index(&db, &mut schema, &concurrent_index_statement(false)).unwrap();

    let loaded = SchemaManager::load(&db).unwrap();
    let table = loaded.get("items").unwrap();
    assert_eq!(
        table
            .columns
            .iter()
            .map(|col| col.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "b", "a"]
    );
    assert_eq!(table.index_by_name("by_a").unwrap().columns_vec(), [2]);
    assert_index_keys(&db, "by_a", &[(700, 7)]);
    let conn = crate::Connection::open(&db).unwrap();
    assert_eq!(
        conn.query("SELECT id, b, a FROM items WHERE a = 700")
            .unwrap()
            .rows,
        vec![vec![i(7), i(70), i(700)]]
    );
}

#[test]
fn repeated_primary_key_declarations_preserve_names_for_validation() {
    let cases = [
        (
            "CREATE TABLE items (a INTEGER PRIMARY KEY, z INTEGER, PRIMARY KEY (a, z))",
            vec!["a", "a", "z"],
        ),
        (
            "CREATE TABLE items (a INTEGER PRIMARY KEY, z INTEGER, PRIMARY KEY (A, z))",
            vec!["a", "A", "z"],
        ),
        (
            "CREATE TABLE items (a INTEGER, z INTEGER, PRIMARY KEY (a), PRIMARY KEY (a, z))",
            vec!["a", "a", "z"],
        ),
    ];
    let columns = [col("a", DataType::Integer), col("z", DataType::Integer)];
    for (sql, expected) in cases {
        let Statement::CreateTable(stmt) = crate::parser::parse_sql(sql).unwrap() else {
            panic!("expected CREATE TABLE");
        };
        assert_eq!(stmt.primary_key, expected);
        assert!(matches!(
            resolve_primary_key_columns(&columns, &stmt.primary_key),
            Err(SqlError::DuplicateColumn(name)) if name.eq_ignore_ascii_case("a")
        ));
    }
}

#[test]
fn create_table_rejects_repeated_primary_key_columns_without_catalog_changes() {
    for columns in ["a, a, z", "a, A, z", "a, z, A"] {
        let db = ddl_database();
        let mut schema = SchemaManager::empty();
        let before = db.manager().commit_generation();
        let stmt = repeated_primary_key_statement(columns);

        let error = exec_create_table(&db, &mut schema, &stmt).unwrap_err();
        assert!(
            matches!(error, SqlError::DuplicateColumn(ref name) if name.eq_ignore_ascii_case("a"))
        );
        assert!(schema.table_names().is_empty());
        assert_eq!(db.manager().commit_generation(), before);
        assert!(db.begin_read().list_tables().unwrap().is_empty());
        assert!(SchemaManager::load(&db).unwrap().table_names().is_empty());

        let mut valid = stmt;
        valid.primary_key = vec!["a".into(), "z".into()];
        exec_create_table(&db, &mut schema, &valid).unwrap();
        assert_eq!(schema.get("items").unwrap().primary_key_columns, [0, 1]);
    }
}

#[test]
fn create_table_in_txn_rejects_repeated_primary_key_columns_without_catalog_changes() {
    for columns in ["a, a, z", "a, A, z", "a, z, A"] {
        let db = ddl_database();
        let mut schema = SchemaManager::empty();
        let stmt = repeated_primary_key_statement(columns);
        let mut wtx = db.begin_write().unwrap();

        let error = exec_create_table_in_txn(&mut wtx, &mut schema, &stmt).unwrap_err();
        assert!(
            matches!(error, SqlError::DuplicateColumn(ref name) if name.eq_ignore_ascii_case("a"))
        );
        assert!(schema.table_names().is_empty());
        assert!(wtx.table_root_stamp(b"items").unwrap().is_none());
        assert!(wtx.table_root_stamp(b"_schema").unwrap().is_none());
        wtx.commit().unwrap();
        assert!(db.begin_read().list_tables().unwrap().is_empty());
        assert!(SchemaManager::load(&db).unwrap().table_names().is_empty());

        let mut valid = stmt;
        valid.primary_key = vec!["a".into(), "z".into()];
        let mut wtx = db.begin_write().unwrap();
        exec_create_table_in_txn(&mut wtx, &mut schema, &valid).unwrap();
        wtx.commit().unwrap();
        let loaded = SchemaManager::load(&db).unwrap();
        assert_eq!(loaded.get("items").unwrap().primary_key_columns, [0, 1]);
    }
}

#[test]
fn collect_column_refs_simple_column() {
    let mut out = Vec::new();
    collect_column_refs(&Expr::Column("X".into()), &mut out);
    assert_eq!(out, vec!["x"]);
}

#[test]
fn collect_column_refs_qualified_uses_column_only() {
    let mut out = Vec::new();
    collect_column_refs(
        &Expr::QualifiedColumn {
            table: "T".into(),
            column: "Y".into(),
        },
        &mut out,
    );
    assert_eq!(out, vec!["y"]);
}

#[test]
fn collect_column_refs_binary_op() {
    let mut out = Vec::new();
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Mul,
        right: Box::new(Expr::Column("b".into())),
    };
    collect_column_refs(&e, &mut out);
    assert_eq!(out, vec!["a", "b"]);
}

#[test]
fn collect_column_refs_function_args() {
    let mut out = Vec::new();
    let e = Expr::Function {
        name: "ABS".into(),
        args: vec![Expr::Column("v".into())],
        distinct: false,
    };
    collect_column_refs(&e, &mut out);
    assert_eq!(out, vec!["v"]);
}

#[test]
fn collect_column_refs_literal_yields_empty() {
    let mut out = Vec::new();
    collect_column_refs(&Expr::Literal(i(1)), &mut out);
    assert!(out.is_empty());
}

#[test]
fn collect_column_refs_case_branches() {
    let mut out = Vec::new();
    let e = Expr::Case {
        operand: None,
        conditions: vec![(Expr::Column("c".into()), Expr::Column("r".into()))],
        else_result: Some(Box::new(Expr::Column("el".into()))),
    };
    collect_column_refs(&e, &mut out);
    assert_eq!(out, vec!["c", "r", "el"]);
}

#[test]
fn validate_no_chained_generated_no_generated_columns_ok() {
    let cs = vec![col("a", DataType::Integer), col("b", DataType::Integer)];
    assert!(validate_no_chained_generated(&cs).is_ok());
}

#[test]
fn validate_no_chained_generated_self_reference_ok() {
    let mut gen_col = col("g", DataType::Integer);
    gen_col.generated_kind = Some(GeneratedKind::Stored);
    gen_col.generated_expr = Some(Expr::Column("g".into()));
    let cs = vec![col("a", DataType::Integer), gen_col];
    assert!(validate_no_chained_generated(&cs).is_ok());
}

#[test]
fn validate_no_chained_generated_references_non_generated_ok() {
    let mut gen_col = col("g", DataType::Integer);
    gen_col.generated_kind = Some(GeneratedKind::Stored);
    gen_col.generated_expr = Some(Expr::Column("a".into()));
    let cs = vec![col("a", DataType::Integer), gen_col];
    assert!(validate_no_chained_generated(&cs).is_ok());
}

#[test]
fn validate_no_chained_generated_chain_rejected() {
    let mut g1 = col("g1", DataType::Integer);
    g1.generated_kind = Some(GeneratedKind::Stored);
    g1.generated_expr = Some(Expr::Column("a".into()));
    let mut g2 = col("g2", DataType::Integer);
    g2.generated_kind = Some(GeneratedKind::Stored);
    g2.generated_expr = Some(Expr::Column("g1".into()));
    let cs = vec![col("a", DataType::Integer), g1, g2];
    assert!(validate_no_chained_generated(&cs).is_err());
}
