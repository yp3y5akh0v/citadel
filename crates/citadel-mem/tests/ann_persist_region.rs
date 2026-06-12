//! The region-level persisted-ANN wrapper: a plaintext region's index freezes
//! and reloads with identical recall results, sealed regions are categorically
//! refused (their index is RAM-only and zeroized with the region key), and the
//! status surface reports which index serves.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{
    AnnIndexSource, AtomInput, FusionWeights, MemoryEngine, MockEmbedder, RecallQuery,
};
use std::sync::Arc;

const DIM: usize = 16;

fn embedder() -> Arc<MockEmbedder> {
    Arc::new(MockEmbedder::new(DIM))
}

fn open_engine(dir: &std::path::Path, create: bool) -> MemoryEngine {
    let b = DatabaseBuilder::new(dir.join("m.db"))
        .passphrase(b"pw")
        .enable_region_keys(true)
        .argon2_profile(Argon2Profile::Iot);
    let db = if create {
        b.create().unwrap()
    } else {
        b.open().unwrap()
    };
    MemoryEngine::open(Arc::new(db)).unwrap()
}

fn semantic_query(text: &str) -> RecallQuery {
    RecallQuery {
        text: Some(text.to_string()),
        embedding: None,
        kinds: vec!["fact".to_string()],
        payload_filter: None,
        k: 8,
        weights: FusionWeights::semantic_only(),
        graph_expand: None,
    }
}

#[test]
fn plaintext_region_persists_and_reloads_with_identical_recall() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_region("corpus", embedder()).unwrap();
    for i in 0..120 {
        eng.remember(
            "corpus",
            AtomInput::new("fact", format!("theorem number {i} about topic {}", i % 7)),
        )
        .unwrap();
    }
    // Some atoms of another kind: the kind filter must keep working post-load.
    for i in 0..10 {
        eng.remember("corpus", AtomInput::new("note", format!("aside {i}")))
            .unwrap();
    }

    let truth: Vec<i64> = eng
        .recall("corpus", semantic_query("theorem about topic 3"))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert!(!truth.is_empty());

    let info = eng.persist_ann_index("corpus").unwrap();
    assert_eq!(info.n, 130, "all atoms of every kind are indexed");
    drop(eng);

    // Cold attach: recall identical, served by the LOADED segment.
    let eng = open_engine(dir.path(), false);
    eng.create_region("corpus", embedder()).unwrap();
    assert!(
        eng.ann_cache_status("corpus").unwrap().is_none(),
        "nothing cached before the first recall"
    );
    let again: Vec<i64> = eng
        .recall("corpus", semantic_query("theorem about topic 3"))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert_eq!(again, truth, "loaded-segment recall identical");
    match eng.ann_cache_status("corpus").unwrap() {
        Some(AnnIndexSource::Loaded { segment_b3 }) => {
            assert_eq!(segment_b3, info.segment_b3, "the exact persisted artifact");
        }
        other => panic!("expected Loaded, got {other:?}"),
    }

    // New atoms invalidate the segment transactionally; recall stays correct
    // and the status reports the honest rebuild.
    let new_id = eng
        .remember(
            "corpus",
            AtomInput::new("fact", "theorem about topic 3 but newer"),
        )
        .unwrap();
    let fresh: Vec<i64> = eng
        .recall("corpus", semantic_query("theorem about topic 3 but newer"))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert!(fresh.contains(&new_id), "post-persist atom is recallable");
    assert!(matches!(
        eng.ann_cache_status("corpus").unwrap(),
        Some(AnnIndexSource::Built { .. })
    ));
}

#[test]
fn sealed_region_persists_and_reloads_with_identical_recall() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    for i in 0..90 {
        eng.remember(
            "vault",
            AtomInput::new("fact", format!("secret theorem {i} about topic {}", i % 5)),
        )
        .unwrap();
    }
    let truth: Vec<i64> = eng
        .recall("vault", semantic_query("secret theorem about topic 2"))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert!(!truth.is_empty());

    let info = eng.persist_ann_index("vault").unwrap();
    assert_eq!(info.n, 90);
    drop(eng);

    // Cold attach: the first sealed recall LOADS the segment (no PRISM build)
    // and answers identically; the recall cache (text/payload) is rebuilt from
    // the same decrypt pass.
    let eng = open_engine(dir.path(), false);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    assert!(eng.ann_cache_status("vault").unwrap().is_none());
    let hits = eng
        .recall("vault", semantic_query("secret theorem about topic 2"))
        .unwrap();
    let again: Vec<i64> = hits.iter().map(|h| h.id).collect();
    assert_eq!(again, truth, "loaded sealed segment answers identically");
    assert!(
        hits.iter().all(|h| h.text.contains("secret theorem")),
        "decrypted content present in hits"
    );
    match eng.ann_cache_status("vault").unwrap() {
        Some(AnnIndexSource::Loaded { segment_b3 }) => {
            assert_eq!(segment_b3, info.segment_b3, "the exact sealed artifact");
        }
        other => panic!("expected Loaded, got {other:?}"),
    }
}

#[test]
fn sealed_forget_retires_the_segment_and_recall_stays_correct() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    let mut ids = Vec::new();
    for i in 0..40 {
        ids.push(
            eng.remember("vault", AtomInput::new("fact", format!("memory {i}")))
                .unwrap(),
        );
    }
    eng.persist_ann_index("vault").unwrap();

    // Crypto-erase one atom: the segment (whose SQ8 codes embed it) must be
    // retired with it, and the next recall must neither return the forgotten
    // atom nor serve the stale segment.
    let victim = ids[7];
    eng.forget_atoms("vault", &[victim], false).unwrap();
    let hits: Vec<i64> = eng
        .recall("vault", semantic_query("memory 7"))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert!(!hits.contains(&victim), "forgotten atom never resurfaces");
    assert!(
        matches!(
            eng.ann_cache_status("vault").unwrap(),
            Some(AnnIndexSource::Built { .. })
        ),
        "post-forget recall is served by an honest rebuild"
    );
}

#[test]
fn sealed_remember_after_persist_rebuilds_and_finds_the_new_atom() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    for i in 0..30 {
        eng.remember("vault", AtomInput::new("fact", format!("note {i}")))
            .unwrap();
    }
    eng.persist_ann_index("vault").unwrap();
    let new_id = eng
        .remember("vault", AtomInput::new("fact", "a brand new revelation"))
        .unwrap();
    let hits: Vec<i64> = eng
        .recall("vault", semantic_query("brand new revelation"))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert!(
        hits.contains(&new_id),
        "post-persist atom recallable: {hits:?}"
    );
}

#[test]
fn sealed_segment_survives_repeated_reopens_and_reconciles() {
    // The reconciler must spare the segment's row-less pseudo-atom key, not
    // tombstone it as an "interrupted insert" orphan.
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    for i in 0..30 {
        eng.remember("vault", AtomInput::new("fact", format!("note {i}")))
            .unwrap();
    }
    let info = eng.persist_ann_index("vault").unwrap();
    let truth: Vec<i64> = eng
        .recall("vault", semantic_query("note 12"))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    drop(eng);

    for round in 0..3 {
        let eng = open_engine(dir.path(), false);
        eng.create_encrypted_region("vault", embedder()).unwrap();
        let again: Vec<i64> = eng
            .recall("vault", semantic_query("note 12"))
            .unwrap()
            .iter()
            .map(|h| h.id)
            .collect();
        assert_eq!(again, truth, "round {round}: recall identical");
        match eng.ann_cache_status("vault").unwrap() {
            Some(AnnIndexSource::Loaded { segment_b3 }) => assert_eq!(segment_b3, info.segment_b3),
            other => panic!("round {round}: reconciler must spare the segment key: {other:?}"),
        }
        drop(eng);
    }
}

#[test]
fn sealed_segments_are_isolated_per_region() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_encrypted_region("a", embedder()).unwrap();
    eng.create_encrypted_region("b", embedder()).unwrap();
    let mut a_ids = Vec::new();
    for i in 0..25 {
        a_ids.push(
            eng.remember("a", AtomInput::new("fact", format!("alpha {i}")))
                .unwrap(),
        );
        eng.remember("b", AtomInput::new("fact", format!("beta {i}")))
            .unwrap();
    }
    eng.persist_ann_index("a").unwrap();
    let info_b = eng.persist_ann_index("b").unwrap();

    // Forgetting in A retires A's segment; B's must keep loading.
    eng.forget_atoms("a", &[a_ids[3]], false).unwrap();
    drop(eng);
    let eng = open_engine(dir.path(), false);
    eng.create_encrypted_region("a", embedder()).unwrap();
    eng.create_encrypted_region("b", embedder()).unwrap();
    let _ = eng.recall("a", semantic_query("alpha 5")).unwrap();
    let _ = eng.recall("b", semantic_query("beta 5")).unwrap();
    assert!(matches!(
        eng.ann_cache_status("a").unwrap(),
        Some(AnnIndexSource::Built { .. })
    ));
    match eng.ann_cache_status("b").unwrap() {
        Some(AnnIndexSource::Loaded { segment_b3 }) => assert_eq!(segment_b3, info_b.segment_b3),
        other => panic!("region B's segment must be untouched: {other:?}"),
    }
}

#[test]
fn sealed_second_persist_replaces_and_serves_the_new_segment() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    for i in 0..20 {
        eng.remember("vault", AtomInput::new("fact", format!("v1 {i}")))
            .unwrap();
    }
    let first = eng.persist_ann_index("vault").unwrap();
    eng.remember("vault", AtomInput::new("fact", "v2 addition"))
        .unwrap();
    let second = eng.persist_ann_index("vault").unwrap();
    assert_eq!(second.n, 21);
    assert_ne!(first.segment_b3, second.segment_b3);
    assert_ne!(first.content_fingerprint, second.content_fingerprint);
    drop(eng);

    let eng = open_engine(dir.path(), false);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    let hits: Vec<i64> = eng
        .recall("vault", semantic_query("v2 addition"))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert!(!hits.is_empty());
    match eng.ann_cache_status("vault").unwrap() {
        Some(AnnIndexSource::Loaded { segment_b3 }) => {
            assert_eq!(segment_b3, second.segment_b3, "the REPLACED segment serves");
        }
        other => panic!("expected Loaded, got {other:?}"),
    }
}

#[test]
fn sealed_chunk_loss_heals_and_next_persist_recovers() {
    // Crash-window shape: chunks gone (e.g. swept by the SQL purge) while the
    // key slot + meta survive. The load must HEAL (retire the orphan key,
    // carry the reason) and a later persist must work from scratch.
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("m.db"))
        .passphrase(b"pw")
        .enable_region_keys(true)
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let db = Arc::new(db);
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("vault", embedder()).unwrap();
    for i in 0..20 {
        eng.remember("vault", AtomInput::new("fact", format!("note {i}")))
            .unwrap();
    }
    eng.persist_ann_index("vault").unwrap();

    // Drop ONLY the chunk tree via raw KV, leaving meta + key slot live.
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.drop_table(b"__annseg_r1__memory_atoms_d16_cosine_enc")
            .unwrap();
        wtx.commit().unwrap();
    }

    let hits = eng.recall("vault", semantic_query("note 3")).unwrap();
    assert!(!hits.is_empty(), "recall survives the orphaned segment");
    match eng.ann_cache_status("vault").unwrap() {
        Some(AnnIndexSource::Built { refusal: Some(r) }) => {
            assert!(r.contains("chunk"), "refusal names the missing chunks: {r}");
        }
        other => panic!("expected Built with a refusal, got {other:?}"),
    }

    // Healed state must accept a fresh persist + reload.
    let info = eng.persist_ann_index("vault").unwrap();
    drop(eng);
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("vault", embedder()).unwrap();
    let _ = eng.recall("vault", semantic_query("note 3")).unwrap();
    match eng.ann_cache_status("vault").unwrap() {
        Some(AnnIndexSource::Loaded { segment_b3 }) => assert_eq!(segment_b3, info.segment_b3),
        other => panic!("post-heal persist must load: {other:?}"),
    }
}

#[test]
fn sealed_resurrected_ciphertext_is_useless_after_retirement() {
    // Crypto-erasure of the segment itself: save the ciphertext chunks AND the
    // meta rows, forget an atom (which retires = tombstones the segment key),
    // resurrect both - the segment must be REFUSED (its key is dead), the
    // forgotten atom must never resurface.
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("m.db"))
        .passphrase(b"pw")
        .enable_region_keys(true)
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let db = Arc::new(db);
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("vault", embedder()).unwrap();
    let mut ids = Vec::new();
    for i in 0..20 {
        ids.push(
            eng.remember("vault", AtomInput::new("fact", format!("memory {i}")))
                .unwrap(),
        );
    }
    eng.persist_ann_index("vault").unwrap();

    let seg_table = b"__annseg_r1__memory_atoms_d16_cosine_enc";
    let saved_chunks: Vec<(Vec<u8>, Vec<u8>)> = {
        let mut rtx = db.begin_read();
        let mut out = Vec::new();
        rtx.table_scan_from(seg_table, b"", &mut |k: &[u8], v: &[u8]| {
            out.push((k.to_vec(), v.to_vec()));
            Ok(true)
        })
        .unwrap();
        out
    };
    assert!(!saved_chunks.is_empty());
    let conn = citadel_sql::Connection::open(&db).unwrap();
    let saved_meta = match conn
        .execute("SELECT key, value FROM memory_meta WHERE key LIKE 'annseg_%'")
        .unwrap()
    {
        citadel_sql::ExecutionResult::Query(qr) => qr.rows,
        _ => panic!(),
    };
    assert_eq!(saved_meta.len(), 3);
    drop(conn);

    let victim = ids[5];
    eng.forget_atoms("vault", &[victim], false).unwrap();

    // Resurrect ciphertext + meta through raw channels.
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(seg_table).unwrap();
        for (k, v) in &saved_chunks {
            wtx.table_insert(seg_table, k, v).unwrap();
        }
        wtx.commit().unwrap();
    }
    let conn = citadel_sql::Connection::open(&db).unwrap();
    for row in &saved_meta {
        let (citadel_sql::Value::Text(k), citadel_sql::Value::Integer(v)) = (&row[0], &row[1])
        else {
            panic!("meta row shape");
        };
        conn.execute(&format!(
            "INSERT INTO memory_meta (key, value) VALUES ('{k}', {v})"
        ))
        .unwrap();
    }
    drop(conn);
    drop(eng);

    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("vault", embedder()).unwrap();
    let hits: Vec<i64> = eng
        .recall("vault", semantic_query("memory 5"))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert!(!hits.contains(&victim), "forgotten atom stays forgotten");
    match eng.ann_cache_status("vault").unwrap() {
        Some(AnnIndexSource::Built { refusal: Some(r) }) => {
            assert!(
                r.contains("slot") || r.contains("unwrap"),
                "the dead key refuses the resurrected ciphertext: {r}"
            );
        }
        other => panic!("expected a refused resurrection, got {other:?}"),
    }
}

#[test]
fn sealed_multi_chunk_segment_roundtrips() {
    // Enough sealed atoms that the ciphertext spans multiple 1 MB chunks,
    // exercising sealed chunk split + reassembly + AEAD over the whole body.
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    let inputs: Vec<citadel_mem::AtomInput> = (0..40000)
        .map(|i| citadel_mem::AtomInput::new("fact", format!("padded note {i} {}", "x".repeat(40))))
        .collect();
    eng.remember_batch("vault", inputs).unwrap();
    let info = eng.persist_ann_index("vault").unwrap();
    assert!(
        info.chunk_count >= 2,
        "the sealed fixture must span chunks, got {}",
        info.chunk_count
    );
    let truth: Vec<i64> = eng
        .recall("vault", semantic_query("padded note 4444"))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    drop(eng);

    let eng = open_engine(dir.path(), false);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    let again: Vec<i64> = eng
        .recall("vault", semantic_query("padded note 4444"))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert_eq!(again, truth, "multi-chunk sealed roundtrip identical");
    assert!(matches!(
        eng.ann_cache_status("vault").unwrap(),
        Some(AnnIndexSource::Loaded { .. })
    ));
}

#[test]
fn plaintext_evolve_and_payload_update_and_evict_purge() {
    // Every plaintext mutation flows through SQL, whose in-txn purge must
    // retire the segment - evolve (score UPDATE), payload update, and evict.
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_region("corpus", embedder()).unwrap();
    let mut last = 0;
    for i in 0..40 {
        last = eng
            .remember("corpus", AtomInput::new("fact", format!("entry {i}")))
            .unwrap();
    }

    for (what, mutate) in [
        (
            "evolve",
            Box::new(|e: &MemoryEngine| e.evolve("corpus", last, 5, f32::MAX).map(|_| ()))
                as Box<dyn Fn(&MemoryEngine) -> Result<(), citadel_mem::MemError>>,
        ),
        (
            "payload update",
            Box::new(|e: &MemoryEngine| {
                e.update_atom_payload("corpus", last, &serde_json::json!({"r": 1}))
            }),
        ),
        (
            "evict",
            Box::new(|e: &MemoryEngine| {
                e.evict(
                    "corpus",
                    citadel_mem::EvictionPolicy::LowScore {
                        score_threshold: -1.0,
                        confidence_threshold: -1.0,
                    },
                )
                .map(|_| ())
            }),
        ),
    ] {
        eng.persist_ann_index("corpus").unwrap();
        mutate(&eng).unwrap();
        let _ = eng.recall("corpus", semantic_query("entry 1")).unwrap();
        assert!(
            !matches!(
                eng.ann_cache_status("corpus").unwrap(),
                Some(AnnIndexSource::Loaded { .. })
            ),
            "{what} must retire the persisted segment"
        );
    }
}

#[test]
fn purge_region_eviction_retires_both_paths() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_region("plain", embedder()).unwrap();
    eng.create_encrypted_region("vault", embedder()).unwrap();
    for i in 0..20 {
        eng.remember("plain", AtomInput::new("fact", format!("p {i}")))
            .unwrap();
        eng.remember("vault", AtomInput::new("fact", format!("v {i}")))
            .unwrap();
    }
    eng.persist_ann_index("plain").unwrap();
    eng.persist_ann_index("vault").unwrap();
    eng.evict("plain", citadel_mem::EvictionPolicy::PurgeRegion)
        .unwrap();
    eng.evict("vault", citadel_mem::EvictionPolicy::PurgeRegion)
        .unwrap();
    assert!(eng
        .recall("plain", semantic_query("p 1"))
        .unwrap()
        .is_empty());
    assert!(eng
        .recall("vault", semantic_query("v 1"))
        .unwrap()
        .is_empty());
    // Purged regions must not serve any persisted segment.
    assert!(!matches!(
        eng.ann_cache_status("plain").unwrap(),
        Some(AnnIndexSource::Loaded { .. })
    ));
    assert!(!matches!(
        eng.ann_cache_status("vault").unwrap(),
        Some(AnnIndexSource::Loaded { .. })
    ));
}

#[test]
fn unknown_regions_error_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    assert!(eng.persist_ann_index("nope").is_err());
    assert!(eng.ann_cache_status("nope").is_err());
}

#[test]
fn sealed_forget_with_receipt_also_retires() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    let mut ids = Vec::new();
    for i in 0..20 {
        ids.push(
            eng.remember("vault", AtomInput::new("fact", format!("m {i}")))
                .unwrap(),
        );
    }
    eng.persist_ann_index("vault").unwrap();
    // The receipt-bearing variant goes through the same erase path.
    eng.forget_atoms("vault", &[ids[3]], true).unwrap();
    let _ = eng.recall("vault", semantic_query("m 5")).unwrap();
    assert!(!matches!(
        eng.ann_cache_status("vault").unwrap(),
        Some(AnnIndexSource::Loaded { .. })
    ));
}

#[test]
fn sealed_evolve_retires_the_segment() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    let mut last = 0;
    for i in 0..20 {
        last = eng
            .remember("vault", AtomInput::new("fact", format!("e {i}")))
            .unwrap();
    }
    eng.persist_ann_index("vault").unwrap();
    eng.evolve("vault", last, 3, f32::MAX).unwrap();
    let _ = eng.recall("vault", semantic_query("e 5")).unwrap();
    assert!(
        !matches!(
            eng.ann_cache_status("vault").unwrap(),
            Some(AnnIndexSource::Loaded { .. })
        ),
        "sealed evolve must retire the persisted segment"
    );
}

#[test]
fn sealed_empty_region_refuses_to_persist() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    let err = eng.persist_ann_index("vault").unwrap_err();
    assert!(err.to_string().contains("nothing to persist"), "{err}");
}

#[test]
fn sealed_evict_retires_the_segment() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    for i in 0..20 {
        eng.remember("vault", AtomInput::new("fact", format!("note {i}")))
            .unwrap();
    }
    eng.persist_ann_index("vault").unwrap();
    eng.evict(
        "vault",
        citadel_mem::EvictionPolicy::LowScore {
            score_threshold: 10.0,
            confidence_threshold: 10.0,
        },
    )
    .unwrap();
    let _ = eng.recall("vault", semantic_query("note 1")).unwrap();
    assert!(
        !matches!(
            eng.ann_cache_status("vault").unwrap(),
            Some(AnnIndexSource::Loaded { .. })
        ),
        "evict must retire the persisted segment"
    );
}

#[test]
fn sealed_payload_update_retires_the_segment() {
    let dir = tempfile::tempdir().unwrap();
    let eng = open_engine(dir.path(), true);
    eng.create_encrypted_region("vault", embedder()).unwrap();
    let mut last = 0;
    for i in 0..25 {
        last = eng
            .remember("vault", AtomInput::new("fact", format!("entry {i}")))
            .unwrap();
    }
    eng.persist_ann_index("vault").unwrap();
    eng.update_atom_payload("vault", last, &serde_json::json!({"status": "revised"}))
        .unwrap();
    let _ = eng.recall("vault", semantic_query("entry 24")).unwrap();
    assert!(
        matches!(
            eng.ann_cache_status("vault").unwrap(),
            Some(AnnIndexSource::Built { .. })
        ),
        "payload update retires the persisted segment"
    );
}
