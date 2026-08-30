//! Cross-engine stale handles: every insertion re-verifies the region row.

use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{AtomInput, EvictionPolicy, MemError, MemoryEngine, MockEmbedder, RecallQuery};
use citadel_sql::{Connection, Value};

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

fn create_region(eng: &MemoryEngine, name: &str, encrypted: bool) -> i64 {
    if encrypted {
        eng.create_encrypted_region(name, Arc::new(MockEmbedder::new(8)))
            .unwrap();
        eng.attach_existing_region(name, Arc::new(MockEmbedder::new(8)))
            .unwrap()
    } else {
        eng.create_region(name, Arc::new(MockEmbedder::new(8)))
            .unwrap();
        eng.attach_existing_region(name, Arc::new(MockEmbedder::new(8)))
            .unwrap()
    }
}

fn tombstone_region_key_but_leave_row(db: &Database, name: &str) {
    let conn = Connection::open(db).unwrap();
    let qr = conn
        .query_params(
            "SELECT id, rsk_slot FROM memory_regions WHERE name = $1",
            &[Value::Text(name.into())],
        )
        .unwrap();
    let row = qr.rows.first().expect("region row remains present");
    let id = match row[0] {
        Value::Integer(id) => id,
        ref other => panic!("region id is not an integer: {other:?}"),
    };
    let slot = match row[1] {
        Value::Integer(slot) => u32::try_from(slot).unwrap(),
        ref other => panic!("rsk_slot is not an integer: {other:?}"),
    };
    db.region_store_tombstone(slot, id as u64, db.region_store_slot(slot).unwrap().gen)
        .unwrap();
}

/// Every insertion path refuses a stale handle: the write txn re-verifies the row.
#[test]
fn stale_writes_refuse_after_a_cross_engine_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let owner = MemoryEngine::open(db.clone()).unwrap();
    let writer = MemoryEngine::open(db.clone()).unwrap();

    type WriteOp = fn(&MemoryEngine, &str) -> Result<(), MemError>;
    let ops: [(&str, WriteOp); 3] = [
        ("remember", |e, r| {
            e.remember(r, AtomInput::new("fact", "late write"))
                .map(drop)
        }),
        ("remember_if_absent", |e, r| {
            e.remember_if_absent(r, AtomInput::new("fact", "late write"), &[], None)
                .map(drop)
        }),
        ("remember_batch", |e, r| {
            e.remember_batch(r, vec![AtomInput::new("fact", "late write")])
                .map(drop)
        }),
    ];
    for encrypted in [false, true] {
        for (name, op) in &ops {
            let region = format!("r-{name}-{}", u8::from(encrypted));
            create_region(&owner, &region, encrypted);
            owner
                .remember(&region, AtomInput::new("fact", "seed"))
                .unwrap();
            // The writer binds its own handle, then the owner drops.
            writer
                .attach_existing_region(&region, Arc::new(MockEmbedder::new(8)))
                .unwrap();
            owner.drop_region(&region).unwrap();

            let err = op(&writer, &region).unwrap_err();
            assert!(
                matches!(err, MemError::RegionNotFound(_)),
                "{name} (encrypted={encrypted}) must refuse a stale handle, got: {err}"
            );
        }
    }
}

/// Every surface binds the handle to the incarnation; drop/recreate must refuse.
#[test]
fn stale_plaintext_handles_refuse_reads_mutators_and_ann_persistence() {
    type RegionOp = fn(&MemoryEngine, &str, i64) -> Result<(), MemError>;
    let ops: [(&str, RegionOp); 14] = [
        ("fetch", |e, r, _| e.fetch(r, "fact", None, 10).map(drop)),
        ("fetch_one", |e, r, id| e.fetch_one(r, id).map(drop)),
        ("count", |e, r, _| e.count(r, "fact").map(drop)),
        ("recall", |e, r, _| {
            e.recall(r, RecallQuery::by_text("seed", 5)).map(drop)
        }),
        ("stored_embeddings", |e, r, _| {
            e.stored_embeddings_identity(r, "fact").map(drop)
        }),
        ("persist_ann", |e, r, _| e.persist_ann_index(r).map(drop)),
        ("ann_status", |e, r, _| e.ann_cache_status(r).map(drop)),
        ("update_payload", |e, r, id| {
            e.update_atom_payload(r, id, &serde_json::json!({"stale": true}))
        }),
        ("set_importance", |e, r, id| {
            e.set_importance(r, &[(id, 0.5)]).map(drop)
        }),
        ("evolve", |e, r, id| e.evolve(r, id, 1, 1.0).map(drop)),
        ("evict", |e, r, _| {
            e.evict(
                r,
                EvictionPolicy::LowScore {
                    score_threshold: 1.0,
                    confidence_threshold: 1.0,
                },
            )
            .map(drop)
        }),
        ("delete_atoms", |e, r, id| {
            e.delete_atoms(r, &[id]).map(drop)
        }),
        ("forget_atoms", |e, r, id| {
            e.forget_atoms(r, &[id], false).map(drop)
        }),
        ("verify_atoms", |e, r, id| {
            e.verify_atoms(r, &[id]).map(drop)
        }),
    ];

    for (name, op) in ops {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let owner = MemoryEngine::open(db.clone()).unwrap();
        let client = MemoryEngine::open(db).unwrap();
        let region = format!("plain-{name}");
        create_region(&owner, &region, false);
        let predecessor = owner
            .remember(&region, AtomInput::new("fact", "predecessor seed"))
            .unwrap();
        client
            .attach_existing_region(&region, Arc::new(MockEmbedder::new(8)))
            .unwrap();

        owner.drop_region(&region).unwrap();
        create_region(&owner, &region, false);
        owner
            .remember(&region, AtomInput::new("fact", "successor seed"))
            .unwrap();

        let error = op(&client, &region, predecessor).unwrap_err();
        assert!(
            matches!(error, MemError::RegionNotAttached(_)),
            "{name} accepted a stale plaintext incarnation: {error}"
        );
    }
}

/// Attach reconciles the persisted incarnation without needing a failed write.
#[test]
fn direct_attach_and_create_rebind_drop_recreate_for_both_region_kinds() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let owner = MemoryEngine::open(db.clone()).unwrap();
    let client = MemoryEngine::open(db.clone()).unwrap();

    for encrypted in [false, true] {
        for via_create in [false, true] {
            let region = format!("direct-{}-{}", u8::from(encrypted), u8::from(via_create));
            let old_id = create_region(&owner, &region, encrypted);
            assert_eq!(
                client
                    .attach_existing_region(&region, Arc::new(MockEmbedder::new(8)))
                    .unwrap(),
                old_id
            );

            owner.drop_region(&region).unwrap();
            let new_id = create_region(&owner, &region, encrypted);
            assert_ne!(old_id, new_id, "recreate must mint a new incarnation");

            let rebound = if via_create {
                if encrypted {
                    client
                        .create_encrypted_region(&region, Arc::new(MockEmbedder::new(8)))
                        .unwrap()
                } else {
                    client
                        .create_region(&region, Arc::new(MockEmbedder::new(8)))
                        .unwrap()
                }
            } else {
                client
                    .attach_existing_region(&region, Arc::new(MockEmbedder::new(8)))
                    .unwrap()
            };
            assert_eq!(rebound, new_id, "client must bind the successor id");

            client
                .remember(&region, AtomInput::new("fact", "successor write"))
                .unwrap();
            let hits = owner.fetch(&region, "fact", None, 10).unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].text, "successor write");
        }
    }
}

/// A failed attach clears the stale entry; reads must not stay apparently attached.
#[test]
fn absent_attach_evicts_the_local_stale_incarnation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let owner = MemoryEngine::open(db.clone()).unwrap();
    let client = MemoryEngine::open(db).unwrap();

    for encrypted in [false, true] {
        let region = format!("absent-{}", u8::from(encrypted));
        create_region(&owner, &region, encrypted);
        client
            .attach_existing_region(&region, Arc::new(MockEmbedder::new(8)))
            .unwrap();
        owner.drop_region(&region).unwrap();
        assert!(matches!(
            client.attach_existing_region(&region, Arc::new(MockEmbedder::new(8))),
            Err(MemError::RegionNotFound(_))
        ));
        assert!(matches!(
            client.fetch(&region, "fact", None, 1),
            Err(MemError::RegionNotFound(_))
        ));
    }
}

/// A key-erased region with a leftover row must refuse writes and allocate no ACK.
#[test]
fn tombstoned_region_key_with_live_row_cannot_be_resurrected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let owner = MemoryEngine::open(db.clone()).unwrap();
    let writer = MemoryEngine::open(db.clone()).unwrap();

    type WriteOp = fn(&MemoryEngine, &str) -> Result<(), MemError>;
    let ops: [(&str, WriteOp); 3] = [
        ("remember", |e, r| {
            e.remember(r, AtomInput::new("fact", "late write"))
                .map(drop)
        }),
        ("remember_if_absent", |e, r| {
            e.remember_if_absent(r, AtomInput::new("fact", "late write"), &[], None)
                .map(drop)
        }),
        ("remember_batch", |e, r| {
            e.remember_batch(r, vec![AtomInput::new("fact", "late write")])
                .map(drop)
        }),
    ];

    for (name, op) in ops {
        let region = format!("partial-{name}");
        create_region(&owner, &region, true);
        owner
            .remember(&region, AtomInput::new("fact", "seed"))
            .unwrap();
        writer
            .attach_existing_region(&region, Arc::new(MockEmbedder::new(8)))
            .unwrap();

        tombstone_region_key_but_leave_row(&db, &region);
        let live_acks = db.atom_store_live_owners().unwrap().len();
        let err = op(&writer, &region).unwrap_err();
        assert!(
            matches!(err, MemError::RegionNotAttached(_)),
            "{name} must reject a dead RSK with a surviving row, got: {err}"
        );
        assert_eq!(
            db.atom_store_live_owners().unwrap().len(),
            live_acks,
            "{name} allocated an ACK after region-key erasure"
        );
        let err = writer
            .attach_existing_region(&region, Arc::new(MockEmbedder::new(8)))
            .expect_err("a tombstoned region key cannot be reattached");
        assert!(
            matches!(err, MemError::RegionForgotten(_)),
            "{name} must expose the erased key when reattachment validates it, got: {err}"
        );
    }
}

/// Drop-and-recreate: the old handle can never write into the successor.
#[test]
fn stale_handles_never_write_into_a_recreated_region() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let owner = MemoryEngine::open(db.clone()).unwrap();
    let writer = MemoryEngine::open(db.clone()).unwrap();

    create_region(&owner, "vault", true);
    writer
        .attach_existing_region("vault", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    owner.drop_region("vault").unwrap();
    create_region(&owner, "vault", true);
    let successor = owner
        .remember("vault", AtomInput::new("fact", "successor atom"))
        .unwrap();

    // The drop invalidates every engine's stale handle before the name is recreated.
    let err = writer
        .remember("vault", AtomInput::new("fact", "stale write"))
        .unwrap_err();
    assert!(matches!(err, MemError::RegionNotAttached(_)), "got: {err}");

    // The engine remains detached until an explicit attach binds the successor.
    let err = writer
        .fetch("vault", "fact", None, 10)
        .expect_err("stale entry must be gone after the refused write");
    assert!(matches!(err, MemError::RegionNotAttached(_)), "got: {err}");
    writer
        .attach_existing_region("vault", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let rebound = writer
        .remember("vault", AtomInput::new("fact", "rebound write"))
        .unwrap();

    // The successor holds exactly its own atoms - the stale write left nothing.
    let hits = owner.fetch("vault", "fact", None, 10).unwrap();
    let mut texts: Vec<&str> = hits.iter().map(|h| h.text.as_str()).collect();
    texts.sort_unstable();
    assert_eq!(texts, ["rebound write", "successor atom"]);
    assert!(hits.iter().any(|h| h.id == successor));
    assert!(hits.iter().any(|h| h.id == rebound));
}

/// Retry transient writer or provenance-reservation collisions until `f` is terminal.
fn retry_busy<T>(f: impl Fn() -> Result<T, MemError>) -> Result<T, MemError> {
    for _ in 0..100_000 {
        match f() {
            Err(MemError::Core(citadel_core::Error::RegionInUse { .. })) => {
                std::thread::yield_now();
            }
            Err(e)
                if e.to_string()
                    .contains("write transaction is already active") =>
            {
                std::thread::yield_now();
            }
            terminal => return terminal,
        }
    }
    panic!("write-transaction collision never cleared");
}

/// Racing writes either land before the drop (and are swept) or refuse cleanly.
#[test]
fn racing_writes_never_leak_into_the_successor() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let owner = MemoryEngine::open(db.clone()).unwrap();

    for round in 0..25 {
        for encrypted in [false, true] {
            let region = format!("race{round}{}", u8::from(encrypted));
            create_region(&owner, &region, encrypted);
            let writer = MemoryEngine::open(db.clone()).unwrap();
            writer
                .attach_existing_region(&region, Arc::new(MockEmbedder::new(8)))
                .unwrap();

            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let w = scope.spawn(|| {
                    barrier.wait();
                    // Ok = landed pre-drop (swept); either unavailable state = refused.
                    let out =
                        retry_busy(|| writer.remember(&region, AtomInput::new("fact", "racer")));
                    if let Err(e) = out {
                        assert!(
                            matches!(
                                e,
                                MemError::RegionNotFound(_) | MemError::RegionNotAttached(_)
                            ),
                            "unexpected race outcome: {e}"
                        );
                    }
                });
                let d = scope.spawn(|| {
                    barrier.wait();
                    retry_busy(|| owner.drop_region(&region)).unwrap();
                });
                w.join().unwrap();
                d.join().unwrap();
            });

            create_region(&owner, &region, encrypted);
            owner
                .remember(&region, AtomInput::new("fact", "only me"))
                .unwrap();
            let hits = owner.fetch(&region, "fact", None, 10).unwrap();
            assert_eq!(
                hits.len(),
                1,
                "successor must hold exactly its own atom (round {round}, \
                 encrypted={encrypted})"
            );
            assert_eq!(hits[0].text, "only me");
        }
    }
}
