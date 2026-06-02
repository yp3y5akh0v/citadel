//! Secure delete: `enable_secure_delete(true)` zero-fills freed pages once past all
//! readers. Same free-heavy workload off vs on: on must zero more pages, no corruption.

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_core::PAGE_SIZE;
use citadel_mem::{AtomInput, MemoryEngine, MockEmbedder, RecallQuery};
use std::sync::Arc;

const DIM: usize = 64;

fn build(path: &std::path::Path, secure: bool) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(path)
            .passphrase(b"test-passphrase")
            .enable_region_keys(true)
            .enable_secure_delete(secure)
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    )
}

/// Count fully-zeroed page-sized windows in the file (a proxy for zeroed freed pages).
fn count_zero_pages(path: &std::path::Path) -> usize {
    let bytes = std::fs::read(path).unwrap();
    bytes
        .chunks(PAGE_SIZE)
        .filter(|c| c.len() == PAGE_SIZE && c.iter().all(|&b| b == 0))
        .count()
}

/// Run an identical free-heavy workload and return the count of zeroed pages in the db
/// file. A survivor atom is asserted intact to catch any commit-path corruption.
fn run_workload(db_file: &std::path::Path, secure: bool) -> usize {
    {
        let db = build(db_file, secure);
        let eng = MemoryEngine::open(db).unwrap();
        eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(DIM)))
            .unwrap();

        let survivor = eng
            .remember("r", AtomInput::new("keep", "survivor secret stays intact"))
            .unwrap();

        // Fill many atoms (spanning many pages), then delete them all to free those pages.
        let fill: Vec<AtomInput> = (0..1000)
            .map(|i| AtomInput::new("fact", format!("ephemeral secret number {i}")))
            .collect();
        let ids = eng.remember_batch("r", fill).unwrap();
        eng.delete_atoms("r", &ids).unwrap();

        // Churn write txns (no readers) so the freed pages pass oldest_active and reclaim.
        for i in 0..10 {
            eng.remember("r", AtomInput::new("churn", format!("churn {i}")))
                .unwrap();
        }

        // Integrity: the survivor still recalls after all the freeing (+ possible zeroing).
        let hits = eng
            .recall("r", RecallQuery::by_text("survivor secret stays intact", 3))
            .unwrap();
        assert!(
            hits.iter().any(|h| h.id == survivor),
            "survivor atom must remain intact through secure-delete churn (no corruption)"
        );
    }
    count_zero_pages(db_file)
}

#[test]
fn secure_delete_zeroes_freed_pages_and_preserves_data() {
    let off_dir = tempfile::tempdir().unwrap();
    let on_dir = tempfile::tempdir().unwrap();
    let off = run_workload(&off_dir.path().join("m.db"), false);
    let on = run_workload(&on_dir.path().join("m.db"), true);
    assert!(
        on > off,
        "secure delete must zero more freed pages than the default path: on={on}, off={off}"
    );
}
