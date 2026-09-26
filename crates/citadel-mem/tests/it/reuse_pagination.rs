use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{AtomInput, FetchQuery, MemoryEngine, MockEmbedder};
use citadel_sql::{Connection, Value};
use serde_json::json;

const TABLE: &str = "memory_atoms_d16_cosine_enc";

fn setup() -> (tempfile::TempDir, Arc<Database>, MemoryEngine) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("memory.cdl"))
            .passphrase(b"pagination-test")
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .create()
            .unwrap(),
    );
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    (dir, db, engine)
}

fn region(engine: &MemoryEngine, name: &str) {
    engine
        .create_encrypted_region(name, Arc::new(MockEmbedder::new(16)))
        .unwrap();
}

#[test]
fn small_encrypted_region_does_not_scan_unrelated_suffix() {
    let (_dir, db, engine) = setup();
    region(&engine, "small");
    region(&engine, "large");
    let ids = engine
        .remember_batch(
            "small",
            (0..8)
                .map(|i| AtomInput::new("turn", format!("small {i}")))
                .collect(),
        )
        .unwrap();
    engine
        .remember_batch(
            "large",
            (0..1024)
                .map(|i| AtomInput::new("turn", format!("large {i}")))
                .collect(),
        )
        .unwrap();
    let measured = db.measure_scans();
    let hits = engine.fetch_range("small", &FetchQuery::new(256)).unwrap();
    assert_eq!(hits.iter().map(|hit| hit.id).collect::<Vec<_>>(), ids);
    let first_work = measured.rows_scanned();
    assert!(first_work < 40, "first page scanned {first_work}");
    drop(measured);
    let measured = db.measure_scans();
    assert!(engine
        .fetch_range(
            "small",
            &FetchQuery::new(256).with_after_id(*ids.last().unwrap())
        )
        .unwrap()
        .is_empty());
    let terminal_work = measured.rows_scanned();
    assert!(terminal_work < 12, "terminal page scanned {terminal_work}");
    eprintln!("encrypted region rows visited: first={first_work}, terminal={terminal_work}, unrelated=1024");
}

#[test]
fn interleaved_cursors_keep_filters_and_skip_dead_keys() {
    let (_dir, db, engine) = setup();
    region(&engine, "a");
    region(&engine, "b");
    let mut expected = Vec::new();
    let mut dead = Vec::new();
    for i in 0..18 {
        let mut input = AtomInput::new(if i % 2 == 0 { "even" } else { "odd" }, format!("a {i}"))
            .with_created_at(1000 + i)
            .with_payload(json!({"keep": i % 3 == 0}));
        if i == 9 {
            input = input.with_expires_at(1);
        }
        let id = engine.remember("a", input).unwrap();
        if i == 0 || i == 3 {
            dead.push(id);
        } else if i % 3 == 0 && i != 9 {
            expected.push(id);
        }
        engine
            .remember("b", AtomInput::new("other", format!("b {i}")))
            .unwrap();
    }
    let conn = Connection::open(&db).unwrap();
    for id in dead {
        let rows = conn
            .query(&format!(
                "SELECT key_slot, key_gen FROM {TABLE} WHERE id = {id}"
            ))
            .unwrap()
            .rows;
        let (Value::Integer(slot), Value::Integer(generation)) = (&rows[0][0], &rows[0][1]) else {
            panic!("key binding")
        };
        db.atom_store_tombstone(*slot as u32, id as u64, *generation as u64)
            .unwrap();
    }
    let mut query = FetchQuery::new(1).with_payload_filter(json!({"keep": true}));
    let mut observed = Vec::new();
    loop {
        let page = engine.fetch_range("a", &query).unwrap();
        if page.is_empty() {
            break;
        }
        assert_eq!(page.len(), 1);
        query.after_id = Some(page[0].id);
        observed.push(page[0].id);
    }
    assert_eq!(observed, expected);
    // The same dead-key prefix must advance even without a payload filter.
    let all = engine.fetch_range("a", &FetchQuery::new(100)).unwrap();
    let mut plain = FetchQuery::new(1);
    let mut paged = Vec::new();
    loop {
        let page = engine.fetch_range("a", &plain).unwrap();
        if page.is_empty() {
            break;
        }
        plain.after_id = Some(page[0].id);
        paged.push(page[0].id);
    }
    assert_eq!(paged, all.iter().map(|hit| hit.id).collect::<Vec<_>>());
    let mut newest = FetchQuery::new(2).with_payload_filter(json!({"keep": true}));
    newest.newest = true;
    assert_eq!(
        engine
            .fetch_range("a", &newest)
            .unwrap()
            .iter()
            .map(|hit| hit.id)
            .collect::<Vec<_>>(),
        expected
            .into_iter()
            .rev()
            .take(2)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
    );
    assert!(engine
        .fetch_range("a", &FetchQuery::new(0))
        .unwrap()
        .is_empty());
}

#[test]
fn reopening_adds_cursor_index_without_changing_existing_atoms() {
    let (_dir, db, engine) = setup();
    region(&engine, "existing");
    let ids = engine
        .remember_batch(
            "existing",
            (0..5)
                .map(|i| AtomInput::new("turn", format!("existing {i}")))
                .collect(),
        )
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute(&format!("DROP INDEX {TABLE}_ri")).unwrap();
    drop(engine);
    for _ in 0..2 {
        let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
        engine
            .attach_existing_region("existing", Arc::new(MockEmbedder::new(16)))
            .unwrap();
        assert_eq!(
            engine
                .fetch_range("existing", &FetchQuery::new(100))
                .unwrap()
                .iter()
                .map(|hit| hit.id)
                .collect::<Vec<_>>(),
            ids
        );
        let check = Connection::open(&db).unwrap();
        let schema = check.table_schema(TABLE).unwrap();
        let index = schema.index_by_name(&format!("{TABLE}_ri")).unwrap();
        assert!(index.is_full_column_btree(&[1, 0]));
        assert_eq!(
            schema
                .indices
                .iter()
                .filter(|index| index.name == format!("{TABLE}_ri"))
                .count(),
            1
        );
    }
}

#[test]
fn reopening_rejects_a_reserved_cursor_index_with_wrong_definition() {
    for keys in [
        "(id, region_id)",
        "(region_id, id) WHERE kind = 'turn'",
        "(region_id, CAST(id AS TEXT))",
    ] {
        let (_dir, db, engine) = setup();
        region(&engine, "existing");
        drop(engine);
        let conn = Connection::open(&db).unwrap();
        conn.execute(&format!("DROP INDEX {TABLE}_ri")).unwrap();
        conn.execute(&format!("CREATE INDEX {TABLE}_ri ON {TABLE} {keys}"))
            .unwrap();
        let error = MemoryEngine::open(Arc::clone(&db))
            .err()
            .expect("invalid index must fail");
        assert!(error.to_string().contains("atom cursor index"), "{error}");
    }
}

#[test]
fn newest_fetch_batches_a_dead_key_tail_before_selecting_the_live_window() {
    let (_dir, db, engine) = setup();
    region(&engine, "tail");
    let ids = engine
        .remember_batch(
            "tail",
            (0..64)
                .map(|id| AtomInput::new("turn", format!("turn {id}")))
                .collect(),
        )
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    for id in &ids[16..] {
        let rows = conn
            .query(&format!(
                "SELECT key_slot, key_gen FROM {TABLE} WHERE id = {id}"
            ))
            .unwrap()
            .rows;
        let (Value::Integer(slot), Value::Integer(generation)) = (&rows[0][0], &rows[0][1]) else {
            panic!("key binding")
        };
        db.atom_store_tombstone(*slot as u32, *id as u64, *generation as u64)
            .unwrap();
    }
    let mut query = FetchQuery::new(1);
    query.newest = true;
    let measured = db.measure_scans();
    assert_eq!(
        engine
            .fetch_range("tail", &query)
            .unwrap()
            .iter()
            .map(|hit| hit.id)
            .collect::<Vec<_>>(),
        vec![ids[15]]
    );
    assert!(
        measured.rows_scanned() < 100,
        "newest tail scanned {} rows",
        measured.rows_scanned()
    );
}
