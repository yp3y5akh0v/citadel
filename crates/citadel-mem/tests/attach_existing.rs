//! Fail-if-absent attachment across reopens: never create anything on any path.

use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{AtomInput, MemError, MemoryEngine, MockEmbedder};

fn create_db(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(dir.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .create()
            .unwrap(),
    )
}

fn reopen_db(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(dir.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .open()
            .unwrap(),
    )
}

#[test]
fn attach_reopens_plaintext_and_encrypted_regions() {
    let dir = tempfile::tempdir().unwrap();
    let (plain_atom, sealed_atom) = {
        let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
        eng.create_region("plain", Arc::new(MockEmbedder::new(8)))
            .unwrap();
        eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(8)))
            .unwrap();
        (
            eng.remember("plain", AtomInput::new("fact", "plaintext survives"))
                .unwrap(),
            eng.remember("vault", AtomInput::new("fact", "sealed survives"))
                .unwrap(),
        )
    };

    // A NEW process image: fresh Database, fresh engine, no create calls.
    let eng = MemoryEngine::open(reopen_db(dir.path())).unwrap();
    eng.attach_existing_region("plain", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.attach_existing_region("vault", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert_eq!(
        eng.fetch_one("plain", plain_atom).unwrap().unwrap().text,
        "plaintext survives"
    );
    assert_eq!(
        eng.fetch_one("vault", sealed_atom).unwrap().unwrap().text,
        "sealed survives"
    );
}

/// COLD attach racing a drop: a fresh engine each iteration samples the full path.
#[test]
fn cold_attach_races_drop_for_plaintext_and_encrypted() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let owner = MemoryEngine::open(db.clone()).unwrap();

    for round in 0..25 {
        for encrypted in [false, true] {
            let name = format!("r{round}{}", u8::from(encrypted));
            let region_id = if encrypted {
                owner
                    .create_encrypted_region(&name, Arc::new(MockEmbedder::new(8)))
                    .unwrap()
            } else {
                owner
                    .create_region(&name, Arc::new(MockEmbedder::new(8)))
                    .unwrap()
            };
            let atom = owner
                .remember(&name, AtomInput::new("fact", "exact bytes"))
                .unwrap();

            // A brand-new engine over the same database: cold map.
            let cold = MemoryEngine::open(db.clone()).unwrap();
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let attacher = scope.spawn(|| {
                    barrier.wait();
                    if cold
                        .attach_existing_region(&name, Arc::new(MockEmbedder::new(8)))
                        .is_ok()
                    {
                        // Stale-handle hazard is a separate fix; only served bytes matter.
                        if let Ok(Some(hit)) = cold.fetch_one(&name, atom) {
                            assert_eq!(hit.text, "exact bytes", "no torn cold attach");
                        }
                    }
                });
                let dropper = scope.spawn(|| {
                    barrier.wait();
                    owner.drop_region(&name)
                });
                attacher.join().unwrap();
                match dropper.join().unwrap() {
                    Ok(()) => {}
                    Err(MemError::Core(citadel_core::Error::RegionInUse {
                        region_id: busy_region,
                    })) if busy_region == region_id as u64 => {
                        // Plaintext fetches reserve the region outside the lifecycle
                        // lock. The joined attacher has released that reservation.
                        owner.drop_region(&name).unwrap();
                    }
                    Err(error) => panic!("unexpected drop error for '{name}': {error:?}"),
                }
            });
            let absent = cold.attach_existing_region(&name, Arc::new(MockEmbedder::new(8)));
            assert!(
                matches!(&absent, Err(MemError::RegionNotFound(missing)) if missing == &name),
                "after the drop, cold attach must fail-if-absent: {absent:?}"
            );
        }
    }
}

/// Attach and drop share one lifecycle span: a half-attached region is impossible.
#[test]
fn attach_races_drop_without_tearing() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("vault", AtomInput::new("fact", "exact bytes"))
        .unwrap();

    std::thread::scope(|scope| {
        let attacher = scope.spawn(|| {
            for _ in 0..200 {
                // Dropped = clean Err; a successful attach serves the exact bytes.
                if eng
                    .attach_existing_region("vault", Arc::new(MockEmbedder::new(8)))
                    .is_ok()
                {
                    if let Ok(Some(hit)) = eng.fetch_one("vault", atom) {
                        assert_eq!(hit.text, "exact bytes", "no torn attach");
                    }
                }
            }
        });
        let dropper = scope.spawn(|| {
            eng.drop_region("vault").unwrap();
        });
        attacher.join().unwrap();
        dropper.join().unwrap();
    });

    assert!(
        eng.attach_existing_region("vault", Arc::new(MockEmbedder::new(8)))
            .is_err(),
        "after the drop, attach must fail-if-absent"
    );
}

#[test]
fn attach_never_creates_and_rejects_mismatched_embedders() {
    let dir = tempfile::tempdir().unwrap();
    {
        let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
        eng.create_region("plain", Arc::new(MockEmbedder::new(8)))
            .unwrap();
    }
    let eng = MemoryEngine::open(reopen_db(dir.path())).unwrap();

    // Absent name: error now, error again (nothing was materialized).
    assert!(eng
        .attach_existing_region("ghost", Arc::new(MockEmbedder::new(8)))
        .is_err());
    assert!(eng
        .attach_existing_region("ghost", Arc::new(MockEmbedder::new(8)))
        .is_err());

    // Wrong embedder dimension: the stored binding wins, loudly.
    assert!(eng
        .attach_existing_region("plain", Arc::new(MockEmbedder::new(16)))
        .is_err());
    // The failed attaches must not have poisoned the real one.
    eng.attach_existing_region("plain", Arc::new(MockEmbedder::new(8)))
        .unwrap();
}
