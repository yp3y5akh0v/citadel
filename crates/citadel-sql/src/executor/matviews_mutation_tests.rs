use super::*;
use crate::Connection;
use citadel::{Argon2Profile, DatabaseBuilder};

thread_local! {
    static AFTER_REMOVAL: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

pub(super) fn after_removal() {
    if let Some(hook) = AFTER_REMOVAL.with(|slot| slot.borrow_mut().take()) {
        hook();
    }
}

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"matview-savepoint")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn contents(wtx: &mut WriteTxn<'_>, table: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    wtx.table_for_each(table, |key, value| {
        rows.push((key.to_vec(), value.to_vec()));
        Ok(())
    })
    .unwrap();
    rows
}

#[test]
fn cancelled_or_panicking_refresh_restores_all_trees_and_caller_writer() {
    for concurrent in [false, true] {
        for panic in [false, true] {
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
            let mut schema = SchemaManager::load(&db).unwrap();
            let mut wtx = db.begin_write().unwrap();
            schema.admit_owned_write(&mut wtx).unwrap();
            let backing = schema.get("mv").unwrap().clone();
            let index = TableSchema::index_table_name("mv", "mv_n");
            let before = [contents(&mut wtx, b"mv"), contents(&mut wtx, &index)];
            let marker = wtx.mutation_marker();
            let token = citadel::CancelToken::new();
            wtx.set_cancel(Some(token.clone()));
            AFTER_REMOVAL.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    if panic {
                        panic!("injected matview failure after index removal");
                    }
                    token.cancel();
                }))
            });
            let stmt = RefreshMatviewStmt {
                name: "mv".into(),
                concurrently: concurrent,
            };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                exec_refresh_matview_in_txn(&mut wtx, &mut schema, &stmt)
            }));
            if panic {
                assert!(result.is_err());
            } else {
                assert!(matches!(
                    result.unwrap(),
                    Err(SqlError::Storage(citadel_core::Error::Interrupted))
                ));
            }
            wtx.set_cancel(None);
            assert!(!wtx.mutated_since(marker));
            assert!(!schema.has_dml_dirty());
            assert_eq!(
                schema.get("mv").unwrap().try_serialize().unwrap(),
                backing.try_serialize().unwrap()
            );
            assert_eq!(contents(&mut wtx, b"mv"), before[0]);
            assert_eq!(contents(&mut wtx, &index), before[1]);
            wtx.commit().unwrap();
        }
    }
}

#[test]
fn concurrent_noop_preserves_row_and_index_roots_and_clean_dml_state() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY,n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1,10),(2,20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id,n FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX mv_n ON mv(n)").unwrap();
    let mut schema = SchemaManager::load(&db).unwrap();
    let mut wtx = db.begin_write().unwrap();
    schema.admit_owned_write(&mut wtx).unwrap();
    let index = TableSchema::index_table_name("mv", "mv_n");
    let before = [
        wtx.table_root_stamp(b"mv").unwrap(),
        wtx.table_root_stamp(&index).unwrap(),
    ];
    let marker = wtx.mutation_marker();
    exec_refresh_matview_in_txn(
        &mut wtx,
        &mut schema,
        &RefreshMatviewStmt {
            name: "mv".into(),
            concurrently: true,
        },
    )
    .unwrap();
    assert!(!wtx.mutated_since(marker));
    assert!(!schema.has_dml_dirty());
    assert_eq!(
        [
            wtx.table_root_stamp(b"mv").unwrap(),
            wtx.table_root_stamp(&index).unwrap()
        ],
        before
    );
    wtx.commit().unwrap();
}

#[test]
fn failed_initial_population_leaves_no_catalog_or_index_in_caller_writer() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY,s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1,'A'),(2,'a')")
        .unwrap();
    let mut schema = SchemaManager::load(&db).unwrap();
    let mut wtx = db.begin_write().unwrap();
    schema.admit_owned_write(&mut wtx).unwrap();
    let marker = wtx.mutation_marker();
    let crate::parser::Statement::CreateMaterializedView(stmt) =
        crate::parser::parse_sql("CREATE MATERIALIZED VIEW mv AS SELECT s,id FROM src").unwrap()
    else {
        panic!("CREATE MATVIEW expected")
    };
    assert!(matches!(
        exec_create_matview_in_txn(&mut wtx, &mut schema, &stmt),
        Err(SqlError::DuplicateKey)
    ));
    assert!(!wtx.mutated_since(marker));
    assert!(schema.get("mv").is_none());
    assert!(schema.get_matview("mv").is_none());
    assert!(wtx.table_root_stamp(b"mv").unwrap().is_none());
    assert!(wtx
        .table_root_stamp(&TableSchema::index_table_name("mv", "__pk_mv"))
        .unwrap()
        .is_none());
    assert!(wtx.table_get(b"_schema", b"mv").unwrap().is_none());
    wtx.commit().unwrap();
}
