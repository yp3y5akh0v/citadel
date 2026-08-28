use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use citadel::{Database, DatabaseBuilder};
use citadel_mem::{
    AtomInput, AttestVerdict, FetchQuery, MemError, MemoryEngine, MemoryMaintenance, MockEmbedder,
    RecallQuery,
};
use citadel_sql::{Connection, Value};

const DIM: usize = 12;
const ENCRYPTED_TABLE: &str = "memory_atoms_d12_cosine_enc";

fn encrypted_database(path: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(path.join("vault.cdl"))
            .passphrase(b"maintenance-test")
            .enable_region_keys(true)
            .create()
            .unwrap(),
    )
}

fn atom_key_slot(db: &Database, atom_id: i64) -> u32 {
    let row = Connection::open(db)
        .unwrap()
        .query_params(
            &format!("SELECT key_slot FROM {ENCRYPTED_TABLE} WHERE id = $1"),
            &[Value::Integer(atom_id)],
        )
        .unwrap();
    match row.rows[0][0] {
        Value::Integer(slot) => u32::try_from(slot).unwrap(),
        ref other => panic!("key_slot is not an integer: {other:?}"),
    }
}

fn corrupt_atom_key_slot(db: &Database, slot: u32) {
    let path = db.atom_store_path();
    let block = citadel::core::REGION_STORE_BLOCK as u64;
    let copy_a = (2 + 2 * u64::from(slot)) * block;
    let copy_b = copy_a + block;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    for offset in [copy_a, copy_b] {
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x80;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&byte).unwrap();
    }
    file.sync_all().unwrap();
}

#[test]
fn maintenance_reads_attests_and_forgets_without_an_embedder() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("sealed", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    engine
        .create_region("plain", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let sealed = engine
        .remember("sealed", AtomInput::new("fact", "encrypted content"))
        .unwrap();
    let plain = engine
        .remember("plain", AtomInput::new("note", "plaintext content"))
        .unwrap();

    let maintenance = MemoryMaintenance::open(Arc::clone(&db)).unwrap();
    let regions = maintenance.regions().unwrap();
    assert_eq!(
        regions
            .iter()
            .map(|region| (region.name(), region.encrypted()))
            .collect::<Vec<_>>(),
        [("plain", false), ("sealed", true)]
    );
    assert_eq!(maintenance.count_region("sealed").unwrap(), 1);
    assert_eq!(
        maintenance
            .fetch_range("sealed", &FetchQuery::new(10))
            .unwrap()[0]
            .text,
        "encrypted content"
    );
    assert_eq!(
        maintenance.verify_atoms("sealed", &[sealed]).unwrap()[0].verdict,
        AttestVerdict::Authentic
    );
    assert_eq!(
        maintenance.verify_atoms("plain", &[plain]).unwrap()[0].verdict,
        AttestVerdict::PlaintextUnattested
    );
    assert_eq!(
        engine
            .recall("sealed", RecallQuery::by_text("encrypted", 10))
            .unwrap()[0]
            .id,
        sealed,
        "build the live engine's decrypted ANN cache before erasure"
    );

    let sealed_receipt = maintenance
        .forget_atoms("sealed", &[sealed], false)
        .unwrap();
    assert!(sealed_receipt.cryptographic_erasure);
    assert_eq!(sealed_receipt.erased_count, 1);
    assert!(sealed_receipt.readback_confirmed);
    assert_eq!(maintenance.count_region("sealed").unwrap(), 0);
    assert!(engine.fetch_one("sealed", sealed).unwrap().is_none());
    assert!(
        engine
            .recall("sealed", RecallQuery::by_text("encrypted", 10))
            .unwrap()
            .is_empty(),
        "maintenance erasure left a live engine serving its stale ANN cache"
    );

    let plain_receipt = maintenance.forget_atoms("plain", &[plain], false).unwrap();
    assert!(!plain_receipt.cryptographic_erasure);
    assert_eq!(plain_receipt.rows_deleted, 1);
}

#[test]
fn maintenance_open_does_not_create_a_memory_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());

    let error = match MemoryMaintenance::open(Arc::clone(&db)) {
        Ok(_) => panic!("maintenance must not manufacture a memory schema"),
        Err(error) => error,
    };
    assert!(
        matches!(error, MemError::Invalid(message) if message == "memory schema is not present")
    );

    let connection = Connection::open(&db).unwrap();
    assert!(connection.table_schema("memory_regions").is_none());
}

#[test]
fn forgotten_region_stays_in_inventory_but_never_reads_as_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = engine
        .create_encrypted_region("forgotten", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let atom = engine
        .remember("forgotten", AtomInput::new("fact", "must not read back"))
        .unwrap();
    let connection = Connection::open(&db).unwrap();
    let row = connection
        .query_params(
            "SELECT rsk_slot FROM memory_regions WHERE id = $1",
            &[Value::Integer(region_id)],
        )
        .unwrap();
    let Value::Integer(slot) = row.rows[0][0] else {
        panic!("region key slot must be an integer");
    };
    db.region_store_tombstone(
        slot as u32,
        region_id as u64,
        db.region_store_slot(slot as u32).unwrap().gen,
    )
    .unwrap();

    let maintenance = MemoryMaintenance::open(Arc::clone(&db)).unwrap();
    assert_eq!(maintenance.regions().unwrap()[0].name(), "forgotten");
    for result in [
        maintenance.count_region("forgotten").map(|_| ()),
        maintenance
            .fetch_range("forgotten", &FetchQuery::new(10))
            .map(|_| ()),
        maintenance.verify_atoms("forgotten", &[atom]).map(|_| ()),
        maintenance
            .forget_atoms("forgotten", &[atom], false)
            .map(|_| ()),
    ] {
        assert!(
            matches!(result, Err(MemError::RegionForgotten(ref region)) if region == "forgotten"),
            "forgotten content must fail instead of looking empty: {result:?}"
        );
    }
}

#[test]
fn inspection_of_missing_key_sidecars_never_creates_them() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_region("plain", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let connection = Connection::open(&db).unwrap();
    connection
        .execute_params(
            "INSERT INTO memory_regions \
             (id, name, embedding_dim, embedding_metric, model_id, encrypted, rsk_slot, \
              rsk_gen, created_at, metadata) \
             VALUES (99, 'orphaned', 12, 'cosine', 'lost-model', 1, 0, 1, \
                     CURRENT_TIMESTAMP, NULL)",
            &[],
        )
        .unwrap();
    assert!(!db.region_store_path().exists());
    assert!(!db.atom_store_path().exists());

    let maintenance = MemoryMaintenance::open(Arc::clone(&db)).unwrap();
    let inventory = maintenance.inventory().unwrap();
    let plain = inventory
        .iter()
        .find(|item| item.region().name() == "plain")
        .unwrap();
    assert_eq!(plain.live_atoms(), Some(0));
    let orphaned = inventory
        .iter()
        .find(|item| item.region().name() == "orphaned")
        .unwrap();
    assert_eq!(orphaned.live_atoms(), None);
    assert!(orphaned
        .unavailable()
        .is_some_and(|error| error.contains("forgotten")));

    for result in [
        maintenance.count_region("orphaned").map(|_| ()),
        maintenance
            .fetch_range("orphaned", &FetchQuery::new(10))
            .map(|_| ()),
        maintenance.verify_atoms("orphaned", &[1]).map(|_| ()),
    ] {
        assert!(matches!(result, Err(MemError::RegionForgotten(_))));
    }
    assert!(!db.region_store_path().exists());
    assert!(!db.atom_store_path().exists());
}

#[test]
fn empty_encrypted_region_does_not_create_an_atom_key_store_when_inspected() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("empty", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    assert!(db.region_store_path().exists());
    assert!(!db.atom_store_path().exists());

    let maintenance = MemoryMaintenance::open(Arc::clone(&db)).unwrap();
    let empty = maintenance
        .inventory()
        .unwrap()
        .into_iter()
        .find(|item| item.region().name() == "empty")
        .unwrap();
    assert_eq!(empty.live_atoms(), Some(0));
    assert_eq!(maintenance.count_region("empty").unwrap(), 0);
    assert!(maintenance
        .fetch_range("empty", &FetchQuery::new(10))
        .unwrap()
        .is_empty());
    assert_eq!(
        maintenance.verify_atoms("empty", &[77]).unwrap()[0].verdict,
        AttestVerdict::Missing
    );
    assert!(
        !db.atom_store_path().exists(),
        "model-free inspection created an empty atom-key sidecar"
    );
}

#[test]
fn missing_atom_sidecar_makes_a_nonempty_encrypted_region_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("missing-atoms", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    engine
        .remember(
            "missing-atoms",
            AtomInput::new("fact", "must not look empty"),
        )
        .unwrap();
    let atom_path = db.atom_store_path();
    std::fs::remove_file(&atom_path).unwrap();

    let maintenance = MemoryMaintenance::open(Arc::clone(&db)).unwrap();
    let item = maintenance
        .inventory()
        .unwrap()
        .into_iter()
        .find(|item| item.region().name() == "missing-atoms")
        .unwrap();
    assert_eq!(item.live_atoms(), None);
    assert!(item
        .unavailable()
        .is_some_and(|error| error.contains("atom key store is missing")));
    assert!(maintenance.count_region("missing-atoms").is_err());
    assert!(maintenance
        .fetch_range("missing-atoms", &FetchQuery::new(10))
        .is_err());
    assert!(
        !atom_path.exists(),
        "read-only maintenance recreated a missing atom-key sidecar"
    );
}

#[test]
fn one_invalid_region_key_slot_does_not_hide_healthy_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("healthy", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let damaged_id = engine
        .create_encrypted_region("damaged", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    engine
        .remember("healthy", AtomInput::new("fact", "readable"))
        .unwrap();
    engine
        .remember("damaged", AtomInput::new("fact", "isolated"))
        .unwrap();
    Connection::open(&db)
        .unwrap()
        .execute_params(
            "UPDATE memory_regions SET rsk_slot = $1 WHERE id = $2",
            &[
                Value::Integer(i64::from(u32::MAX)),
                Value::Integer(damaged_id),
            ],
        )
        .unwrap();

    let inventory = MemoryMaintenance::open(Arc::clone(&db))
        .unwrap()
        .inventory()
        .unwrap();
    let healthy = inventory
        .iter()
        .find(|item| item.region().name() == "healthy")
        .unwrap();
    assert_eq!(healthy.live_atoms(), Some(1));
    assert!(healthy.unavailable().is_none());
    let damaged = inventory
        .iter()
        .find(|item| item.region().name() == "damaged")
        .unwrap();
    assert_eq!(damaged.live_atoms(), None);
    assert!(damaged.unavailable().is_some());
}

#[test]
fn one_malformed_region_key_binding_does_not_hide_healthy_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("healthy", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let damaged_id = engine
        .create_encrypted_region("damaged", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    engine
        .remember("healthy", AtomInput::new("fact", "readable"))
        .unwrap();
    engine
        .remember("damaged", AtomInput::new("fact", "isolated"))
        .unwrap();
    Connection::open(&db)
        .unwrap()
        .execute_params(
            "UPDATE memory_regions SET rsk_gen = -1 WHERE id = $1",
            &[Value::Integer(damaged_id)],
        )
        .unwrap();

    let inventory = MemoryMaintenance::open(Arc::clone(&db))
        .unwrap()
        .inventory()
        .unwrap();
    let healthy = inventory
        .iter()
        .find(|item| item.region().name() == "healthy")
        .unwrap();
    assert_eq!(healthy.live_atoms(), Some(1));
    let damaged = inventory
        .iter()
        .find(|item| item.region().name() == "damaged")
        .unwrap();
    assert_eq!(damaged.live_atoms(), None);
    assert!(damaged.unavailable().is_some());
}

#[test]
fn one_invalid_atom_key_slot_does_not_hide_healthy_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("healthy", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    engine
        .create_encrypted_region("damaged", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    engine
        .remember("healthy", AtomInput::new("fact", "readable"))
        .unwrap();
    let damaged_atom = engine
        .remember("damaged", AtomInput::new("fact", "isolated"))
        .unwrap();
    corrupt_atom_key_slot(&db, atom_key_slot(&db, damaged_atom));

    let inventory = MemoryMaintenance::open(Arc::clone(&db))
        .unwrap()
        .inventory()
        .unwrap();
    let healthy = inventory
        .iter()
        .find(|item| item.region().name() == "healthy")
        .unwrap();
    assert_eq!(healthy.live_atoms(), Some(1));
    assert!(healthy.unavailable().is_none());
    let damaged = inventory
        .iter()
        .find(|item| item.region().name() == "damaged")
        .unwrap();
    assert_eq!(damaged.live_atoms(), None);
    assert!(damaged.unavailable().is_some());
}

#[test]
fn sealed_fetch_limit_does_not_read_a_later_corrupt_key_slot() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("ordered", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let first = engine
        .remember("ordered", AtomInput::new("fact", "first"))
        .unwrap();
    let second = engine
        .remember("ordered", AtomInput::new("fact", "second"))
        .unwrap();
    assert!(first < second);
    corrupt_atom_key_slot(&db, atom_key_slot(&db, second));

    let hits = MemoryMaintenance::open(Arc::clone(&db))
        .unwrap()
        .fetch_range("ordered", &FetchQuery::new(1))
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, first);
    assert_eq!(hits[0].text, "first");
}

#[test]
fn sealed_filtered_fetch_stops_before_a_later_corrupt_key_slot() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("filtered", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let first = engine
        .remember(
            "filtered",
            AtomInput::new("fact", "first").with_payload(serde_json::json!({"keep": true})),
        )
        .unwrap();
    let second = engine
        .remember(
            "filtered",
            AtomInput::new("fact", "second").with_payload(serde_json::json!({"keep": false})),
        )
        .unwrap();
    corrupt_atom_key_slot(&db, atom_key_slot(&db, second));

    let hits = MemoryMaintenance::open(Arc::clone(&db))
        .unwrap()
        .fetch_range(
            "filtered",
            &FetchQuery::new(1).with_payload_filter(serde_json::json!({"keep": true})),
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, first);
}

#[test]
fn verify_atoms_isolates_a_corrupt_key_slot_verdict() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("verify", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let healthy = engine
        .remember("verify", AtomInput::new("fact", "healthy"))
        .unwrap();
    let damaged = engine
        .remember("verify", AtomInput::new("fact", "damaged"))
        .unwrap();
    corrupt_atom_key_slot(&db, atom_key_slot(&db, damaged));

    let attestations = MemoryMaintenance::open(Arc::clone(&db))
        .unwrap()
        .verify_atoms("verify", &[healthy, damaged])
        .unwrap();
    assert_eq!(attestations.len(), 2);
    assert_eq!(attestations[0].atom_id, healthy);
    assert_eq!(attestations[0].verdict, AttestVerdict::Authentic);
    assert_eq!(attestations[1].atom_id, damaged);
    assert_eq!(attestations[1].verdict, AttestVerdict::Tampered);
}

#[test]
fn verify_atoms_isolates_a_malformed_row_binding_verdict() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("verify-row", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let healthy = engine
        .remember("verify-row", AtomInput::new("fact", "healthy"))
        .unwrap();
    let damaged = engine
        .remember("verify-row", AtomInput::new("fact", "damaged"))
        .unwrap();
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {ENCRYPTED_TABLE} SET key_gen = -1 WHERE id = $1"),
            &[Value::Integer(damaged)],
        )
        .unwrap();

    let attestations = MemoryMaintenance::open(Arc::clone(&db))
        .unwrap()
        .verify_atoms("verify-row", &[healthy, damaged])
        .unwrap();
    assert_eq!(attestations[0].verdict, AttestVerdict::Authentic);
    assert_eq!(attestations[1].verdict, AttestVerdict::Tampered);
}

#[test]
fn missing_region_sidecar_is_forgotten_and_never_recreated_by_maintenance() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("missing-region-key", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    engine
        .remember(
            "missing-region-key",
            AtomInput::new("fact", "unrecoverable"),
        )
        .unwrap();
    let region_path = db.region_store_path();
    drop(engine);
    drop(db);
    std::fs::remove_file(&region_path).unwrap();

    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("vault.cdl"))
            .passphrase(b"maintenance-test")
            .enable_region_keys(true)
            .open()
            .unwrap(),
    );
    let maintenance = MemoryMaintenance::open(Arc::clone(&db)).unwrap();
    assert!(matches!(
        maintenance.count_region("missing-region-key"),
        Err(MemError::RegionForgotten(ref name)) if name == "missing-region-key"
    ));
    assert!(
        !region_path.exists(),
        "model-free maintenance recreated a missing region-key sidecar"
    );
}

#[test]
fn maintenance_forgets_from_a_v2_schema_without_bootstrapping_new_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("legacy", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let atom = engine
        .remember("legacy", AtomInput::new("fact", "legacy encrypted atom"))
        .unwrap();
    drop(engine);

    let connection = Connection::open(&db).unwrap();
    connection
        .execute("DROP TABLE memory_similarity_policies")
        .unwrap();
    connection
        .execute("DROP TABLE memory_similarity_edges")
        .unwrap();
    assert!(connection
        .table_schema("memory_similarity_policies")
        .is_none());
    assert!(connection.table_schema("memory_similarity_edges").is_none());
    drop(connection);

    let maintenance = MemoryMaintenance::open(Arc::clone(&db)).unwrap();
    let receipt = maintenance.forget_atoms("legacy", &[atom], false).unwrap();
    assert!(receipt.cryptographic_erasure);
    assert!(receipt.readback_confirmed);
    assert_eq!(receipt.erased_count, 1);
    assert_eq!(receipt.rows_deleted, 1);
    assert_eq!(maintenance.count_region("legacy").unwrap(), 0);

    let connection = Connection::open(&db).unwrap();
    assert!(connection
        .table_schema("memory_similarity_policies")
        .is_none());
    assert!(connection.table_schema("memory_similarity_edges").is_none());
}

#[test]
fn incompatible_cleanup_schema_is_rejected_before_key_erasure() {
    let dir = tempfile::tempdir().unwrap();
    let db = encrypted_database(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("sealed", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let atom = engine
        .remember("sealed", AtomInput::new("fact", "must survive preflight"))
        .unwrap();
    let connection = Connection::open(&db).unwrap();
    let binding = connection
        .query_params(
            "SELECT key_slot FROM memory_atoms_d12_cosine_enc WHERE id = $1",
            &[Value::Integer(atom)],
        )
        .unwrap();
    let Value::Integer(slot) = binding.rows[0][0] else {
        panic!("atom key slot must be an integer");
    };
    connection.execute("DROP TABLE memory_idempotency").unwrap();
    connection
        .execute("CREATE TABLE memory_idempotency (wrong_column INTEGER PRIMARY KEY)")
        .unwrap();

    let error = MemoryMaintenance::open(Arc::clone(&db))
        .unwrap()
        .forget_atoms("sealed", &[atom], false)
        .expect_err("incompatible cleanup schema reached the erasure boundary");
    assert!(error.to_string().contains("memory_idempotency"));
    assert_eq!(
        db.atom_store_slot(slot as u32).unwrap().state,
        citadel::SlotState::Live
    );
    assert!(engine.fetch_one("sealed", atom).unwrap().is_some());
}
