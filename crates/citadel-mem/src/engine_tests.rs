use super::*;
use crate::embed::MockEmbedder;
use crate::error::MemError;
use crate::read_limits::MemoryReadLimits;
use crate::types::FusionWeights;
use citadel::{Argon2Profile, Database, DatabaseBuilder};
use std::sync::Arc;

const MOCK_MODEL_ID: &str = "mock-fnv1a-bow-v1";

fn create_db(path: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(path.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    )
}

fn open_db(path: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(path.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .open()
            .unwrap(),
    )
}

/// Probe chunk 0 in the raw segment tree; the SQL catalog never sees it.
fn seg_chunks_exist(db: &Database, region_id: RegionId, dim: u16) -> bool {
    let table = atoms_table(dim, EmbeddingMetric::Cosine, true);
    let seg = sealed_segment_table(&table, region_id);
    let mut rtx = db.begin_read();
    matches!(
        rtx.table_get(seg.as_bytes(), &0u32.to_be_bytes()),
        Ok(Some(_))
    )
}

/// Whether the segment chunk TREE exists at all (even holed or empty).
fn seg_tree_exists(db: &Database, region_id: RegionId, dim: u16) -> bool {
    let table = atoms_table(dim, EmbeddingMetric::Cosine, true);
    let seg = sealed_segment_table(&table, region_id);
    db.begin_read()
        .table_root_page(seg.as_bytes())
        .unwrap()
        .is_some()
}

fn assert_segment_retired(db: &Database, region_id: RegionId, dim: u16) {
    assert!(!seg_tree_exists(db, region_id, dim));
    let conn = Connection::open(db).unwrap();
    assert!(read_annseg_meta(&conn, region_id).unwrap().is_none());
}

fn open_enc_db(path: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(path.join("m.db"))
            .passphrase(b"test-passphrase")
            .enable_region_keys(true)
            .argon2_profile(Argon2Profile::Iot)
            .open()
            .unwrap(),
    )
}

fn create_enc_db(path: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(path.join("m.db"))
            .passphrase(b"test-passphrase")
            .enable_region_keys(true)
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    )
}

#[test]
fn recall_search_window_preserves_ann_breadth() {
    for (k, limit, expected) in [
        (1, 1, 64),
        (9, 9, 72),
        (50, 4096, 4096),
        (2000, 8000, 16000),
        (usize::MAX, usize::MAX, usize::MAX),
    ] {
        assert_eq!(recall_search_window(k, limit), expected);
    }
}

#[test]
fn create_region_is_idempotent_reattach() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    let e = Arc::new(MockEmbedder::new(8));
    let id1 = eng.create_region("notes", e.clone()).unwrap();
    let id2 = eng.create_region("notes", e).unwrap();
    assert_eq!(id1, id2, "re-attaching the same region returns the same id");
}

#[test]
fn a_persisted_region_requires_explicit_attachment_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let first = MemoryEngine::open(Arc::clone(&db)).unwrap();
    first
        .create_region("persisted", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    first
        .remember("persisted", AtomInput::new("fact", "retained"))
        .unwrap();
    drop(first);

    let reopened = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let error = reopened
        .count_region("persisted")
        .expect_err("a persisted region was treated as attached without an embedder");
    assert!(matches!(error, MemError::RegionNotAttached(ref name) if name == "persisted"));
    assert!(error.to_string().contains("attach_existing_region"));
    assert!(matches!(
        reopened.count_region("missing"),
        Err(MemError::RegionNotFound(ref name)) if name == "missing"
    ));

    reopened
        .attach_existing_region("persisted", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert_eq!(reopened.count_region("persisted").unwrap(), 1);
}

#[test]
fn create_region_rejects_dim_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("notes", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let err = eng
        .create_region("notes", Arc::new(MockEmbedder::new(16)))
        .unwrap_err();
    assert!(
        matches!(
            err,
            MemError::DimMismatch {
                expected: 8,
                got: 16,
                ..
            }
        ),
        "got {err:?}"
    );
}

#[test]
fn zero_dimension_is_rejected_before_create_attach_or_reembed_mutates_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let error = eng
        .create_region("zero", Arc::new(MockEmbedder::new(0)))
        .expect_err("zero-dimensional region was created");
    assert!(error.to_string().contains("at least 1"));
    assert_eq!(
        Connection::open(&db)
            .unwrap()
            .query("SELECT COUNT(*) FROM memory_regions")
            .unwrap()
            .rows[0][0],
        Value::Integer(0)
    );

    eng.create_region("valid", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert!(eng
        .attach_existing_region("valid", Arc::new(MockEmbedder::new(0)))
        .unwrap_err()
        .to_string()
        .contains("at least 1"));
    assert!(eng
        .reembed_region("valid", Arc::new(MockEmbedder::new(0)), None)
        .unwrap_err()
        .to_string()
        .contains("at least 1"));
    assert_eq!(
        Connection::open(&db)
            .unwrap()
            .query("SELECT embedding_dim FROM memory_regions WHERE name = 'valid'")
            .unwrap()
            .rows[0][0],
        Value::Integer(8)
    );
}

#[test]
fn model_provenance_is_normalized_and_placeholders_are_rejected_at_every_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_region(
        "normalized",
        Arc::new(ModelEmbedder {
            inner: MockEmbedder::new(8),
            model: "  model-a  ",
        }),
    )
    .unwrap();
    let stored_model = || {
        Connection::open(&db)
            .unwrap()
            .query("SELECT model_id FROM memory_regions WHERE name = 'normalized'")
            .unwrap()
            .rows[0][0]
            .clone()
    };
    assert_eq!(stored_model(), Value::Text("model-a".into()));

    let second = MemoryEngine::open(Arc::clone(&db)).unwrap();
    second
        .attach_existing_region(
            "normalized",
            Arc::new(ModelEmbedder {
                inner: MockEmbedder::new(8),
                model: "model-a",
            }),
        )
        .unwrap();
    second
        .reembed_region(
            "normalized",
            Arc::new(ModelEmbedder {
                inner: MockEmbedder::new(8),
                model: " model-b ",
            }),
            None,
        )
        .unwrap();
    assert_eq!(stored_model(), Value::Text("model-b".into()));
    second
        .reclassify_region("normalized", " model-c ".into())
        .unwrap();
    assert_eq!(stored_model(), Value::Text("model-c".into()));

    for placeholder in ["default", " UNKNOWN "] {
        let embedder = || {
            Arc::new(ModelEmbedder {
                inner: MockEmbedder::new(8),
                model: placeholder,
            }) as Arc<dyn Embedder>
        };
        assert!(second
            .create_region(&format!("bad-{placeholder:?}"), embedder())
            .unwrap_err()
            .to_string()
            .contains("placeholder"));
        assert!(second
            .attach_existing_region("normalized", embedder())
            .unwrap_err()
            .to_string()
            .contains("placeholder"));
        assert!(second
            .reembed_region("normalized", embedder(), None)
            .unwrap_err()
            .to_string()
            .contains("placeholder"));
        assert!(second
            .reclassify_region("normalized", placeholder.into())
            .unwrap_err()
            .to_string()
            .contains("placeholder"));
    }
    assert_eq!(stored_model(), Value::Text("model-c".into()));
}

#[test]
fn drop_region_then_recreate_allocates_new_id() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    let id1 = eng
        .create_region("notes", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.drop_region("notes").unwrap();
    let id2 = eng
        .create_region("notes", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert!(id2 > id1, "ids monotonic: {id1} then {id2}");
}

#[test]
fn drop_missing_region_is_ok() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.drop_region("ghost").unwrap();
}

#[test]
fn detaching_a_region_scrubs_the_shared_sealed_ann_cache() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("vault", AtomInput::new("fact", "cached secret"))
        .unwrap();
    eng.recall("vault", RecallQuery::by_text("cached secret", 5))
        .unwrap();

    let shared = eng.region_handle("vault").unwrap().ann;
    assert!(shared.read().unwrap().is_some());
    eng.drop_region("vault").unwrap();
    assert!(
        shared.read().unwrap().is_none(),
        "a stale RegionHandle must observe the cache scrub immediately"
    );
}

#[test]
fn decrypted_json_scrub_walks_nested_values_and_object_keys() {
    let mut payload = serde_json::json!({
        "secret-key": ["secret-value", {"nested-key": "nested-value"}],
        "public-number": 7
    });
    // Exact count (3 keys + 2 values) fails if recursion skips a nested container.
    let scrubbed = zeroize_json_strings(&mut payload);
    assert_eq!(
        scrubbed, 5,
        "every nested string value and object key must be visited and scrubbed"
    );
    assert_eq!(payload, serde_json::Value::Null);
}

#[test]
fn public_ranking_writes_reject_non_finite_values_before_sql() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("finite", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let first = eng
        .remember("finite", AtomInput::new("fact", "first"))
        .unwrap();
    let second = eng
        .remember("finite", AtomInput::new("fact", "second"))
        .unwrap();

    let err = eng
        .set_importance("finite", &[(first, 2.0), (second, f32::NAN)])
        .unwrap_err();
    assert!(matches!(err, MemError::Invalid(_)));
    assert!(eng
        .stored_atom_retrieval_state("finite")
        .unwrap()
        .iter()
        .all(|state| state.importance_bits() == 0.0f32.to_bits()));

    for weight in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        let err = eng
            .link(first, second, EdgeKind::Refines, weight)
            .unwrap_err();
        assert!(matches!(err, MemError::Invalid(_)));
    }
    let err = eng
        .link_with_evidence(
            first,
            second,
            EdgeKind::Refines,
            f32::INFINITY,
            Some(serde_json::json!({"quote": "must not persist"})),
        )
        .unwrap_err();
    assert!(matches!(err, MemError::Invalid(_)));
    assert!(eng
        .fetch_edges(Some(first), Some(second), None)
        .unwrap()
        .is_empty());
}

#[test]
fn atom_confidence_is_normalized_at_the_core_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("confidence", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    for confidence in [-0.01, 1.01] {
        let error = eng
            .remember(
                "confidence",
                AtomInput::new("fact", "must not persist").with_confidence(confidence),
            )
            .unwrap_err();
        assert!(matches!(error, MemError::Invalid(_)), "{error:?}");
    }
    assert_eq!(eng.count_region("confidence").unwrap(), 0);
}

#[test]
fn core_rejects_invalid_evolution_and_eviction_policies() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("validations", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("validations", AtomInput::new("fact", "must survive"))
        .unwrap();

    for max_distance in [-1.0, f32::NAN, f32::INFINITY] {
        let error = eng
            .evolve("validations", atom, 5, max_distance)
            .unwrap_err();
        assert!(matches!(error, MemError::Invalid(_)), "{error:?}");
    }
    let invalid_policies = [
        EvictionPolicy::Stale {
            older_than_micros: 0,
        },
        EvictionPolicy::Lru { keep_fraction: 0.0 },
        EvictionPolicy::Lru {
            keep_fraction: 1.01,
        },
        EvictionPolicy::Lru {
            keep_fraction: f32::NAN,
        },
        EvictionPolicy::LowImportance {
            importance_threshold: f32::INFINITY,
            confidence_threshold: 0.5,
        },
        EvictionPolicy::LowImportance {
            importance_threshold: 0.5,
            confidence_threshold: f32::NAN,
        },
        EvictionPolicy::LowImportance {
            importance_threshold: 0.5,
            confidence_threshold: -0.01,
        },
        EvictionPolicy::LowImportance {
            importance_threshold: 0.5,
            confidence_threshold: 1.01,
        },
    ];
    for policy in invalid_policies {
        let error = eng.evict("validations", policy).unwrap_err();
        assert!(matches!(error, MemError::Invalid(_)), "{error:?}");
    }
    assert!(eng.fetch_one("validations", atom).unwrap().is_some());
    assert!(eng.fetch_edges(Some(atom), None, None).unwrap().is_empty());
}

#[test]
fn region_edge_reads_require_two_live_local_endpoints_and_preserve_filters() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("alpha", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("beta", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a1 = eng.remember("alpha", AtomInput::new("note", "a1")).unwrap();
    let a2 = eng.remember("alpha", AtomInput::new("note", "a2")).unwrap();
    let a3 = eng.remember("alpha", AtomInput::new("note", "a3")).unwrap();
    let expired = eng
        .remember(
            "alpha",
            AtomInput::new("note", "expired").with_expires_at(micros_now() - 1),
        )
        .unwrap();
    let b1 = eng.remember("beta", AtomInput::new("note", "b1")).unwrap();
    let b2 = eng.remember("beta", AtomInput::new("note", "b2")).unwrap();

    let evidence = serde_json::json!({"quote": "alpha evidence"});
    eng.link_with_evidence(a1, a2, EdgeKind::Refines, 0.75, Some(evidence.clone()))
        .unwrap();
    eng.link(a2, a3, EdgeKind::Causes, 0.5).unwrap();
    eng.link(a1, b1, EdgeKind::Refines, 1.0).unwrap();
    eng.link(b1, a2, EdgeKind::Refines, 1.0).unwrap();
    eng.link(b1, b2, EdgeKind::Refines, 1.0).unwrap();
    eng.link(a1, expired, EdgeKind::Precedes, 1.0).unwrap();

    assert_eq!(
        eng.fetch_edges(Some(a1), Some(b1), None).unwrap().len(),
        1,
        "the global library reader keeps exposing cross-region edges"
    );
    let all = eng
        .fetch_edges_in_region("alpha", None, None, None, 10)
        .unwrap();
    assert_eq!(all.len(), 2);
    assert!(all.iter().any(|edge| {
        edge.src_id == a1
            && edge.dst_id == a2
            && edge.kind == EdgeKind::Refines
            && edge.evidence_ref.as_ref() == Some(&evidence)
    }));
    assert!(all
        .iter()
        .any(|edge| edge.src_id == a2 && edge.dst_id == a3));
    assert!(all.iter().all(|edge| {
        ![b1, b2, expired].contains(&edge.src_id) && ![b1, b2, expired].contains(&edge.dst_id)
    }));

    let by_src = eng
        .fetch_edges_in_region("alpha", Some(a1), None, None, 10)
        .unwrap();
    assert_eq!(by_src.len(), 1);
    assert_eq!(by_src[0].dst_id, a2);
    let by_dst = eng
        .fetch_edges_in_region("alpha", None, Some(a2), None, 10)
        .unwrap();
    assert_eq!(by_dst.len(), 1);
    assert_eq!(by_dst[0].src_id, a1);
    let by_kind = eng
        .fetch_edges_in_region("alpha", None, None, Some(EdgeKind::Causes), 10)
        .unwrap();
    assert_eq!(by_kind.len(), 1);
    assert_eq!((by_kind[0].src_id, by_kind[0].dst_id), (a2, a3));
    assert!(eng
        .fetch_edges_in_region("alpha", Some(a1), Some(b1), None, 10)
        .unwrap()
        .is_empty());
    let first = eng
        .fetch_edges_in_region("alpha", None, None, None, 1)
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(
        (first[0].src_id, first[0].dst_id, first[0].kind),
        (a1, a2, EdgeKind::Refines),
        "the SQL limit is applied after the stable edge order"
    );
    assert!(eng
        .fetch_edges_in_region("alpha", None, None, None, 0)
        .unwrap()
        .is_empty());
    let from_many = eng
        .fetch_edges_from_atoms_in_region("alpha", &[a1, a2], None, 10)
        .unwrap();
    assert_eq!(from_many.len(), 2);
    let between = eng
        .fetch_edges_between_atoms_in_region("alpha", &[a1, a2], None, 10)
        .unwrap();
    assert_eq!(between.len(), 1);
    assert_eq!((between[0].src_id, between[0].dst_id), (a1, a2));
    assert!(eng
        .fetch_edges_from_atoms_in_region("alpha", &[], None, 10)
        .unwrap()
        .is_empty());

    let page1 = eng
        .fetch_edges_page_in_region("alpha", None, None, None, None, 1)
        .unwrap();
    assert_eq!(page1.edges.len(), 1);
    let cursor = page1.next_after.expect("another live edge remains");
    assert_eq!(
        cursor,
        EdgeCursor {
            src_id: a1,
            dst_id: a2,
            kind: EdgeKind::Refines,
        }
    );
    let page2 = eng
        .fetch_edges_page_in_region("alpha", None, None, None, Some(cursor), 1)
        .unwrap();
    assert_eq!(page2.edges.len(), 1);
    assert_eq!((page2.edges[0].src_id, page2.edges[0].dst_id), (a2, a3));
    assert!(page2.next_after.is_none());
}

#[test]
fn region_edge_writes_require_two_live_local_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("alpha", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("beta", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let src = eng
        .remember("alpha", AtomInput::new("note", "source"))
        .unwrap();
    let dst = eng
        .remember("alpha", AtomInput::new("note", "destination"))
        .unwrap();
    let foreign = eng
        .remember("beta", AtomInput::new("note", "foreign"))
        .unwrap();
    let expired = eng
        .remember(
            "alpha",
            AtomInput::new("note", "expired").with_expires_at(micros_now() - 1),
        )
        .unwrap();

    eng.link_with_evidence_in_region(
        "alpha",
        src,
        dst,
        EdgeKind::Refines,
        0.75,
        Some(serde_json::json!({"quote": "local evidence"})),
    )
    .unwrap();
    for invalid in [foreign, expired, i64::MAX] {
        let error = eng
            .link_in_region("alpha", src, invalid, EdgeKind::Causes, 1.0)
            .unwrap_err();
        assert!(
            matches!(
                error,
                MemError::AtomNotLive { atom_id, ref region }
                    if atom_id == invalid && region == "alpha"
            ),
            "{error:?}"
        );
    }

    let edges = eng.fetch_edges(Some(src), None, None).unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!((edges[0].src_id, edges[0].dst_id), (src, dst));
    assert_eq!(
        edges[0].evidence_ref,
        Some(serde_json::json!({"quote": "local evidence"}))
    );
}

#[test]
fn region_edge_unlink_is_exact_idempotent_and_region_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("alpha", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("beta", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let src = eng
        .remember("alpha", AtomInput::new("note", "source"))
        .unwrap();
    let dst = eng
        .remember("alpha", AtomInput::new("note", "destination"))
        .unwrap();
    let foreign = eng
        .remember("beta", AtomInput::new("note", "foreign"))
        .unwrap();

    eng.link_in_region("alpha", src, dst, EdgeKind::Causes, 1.0)
        .unwrap();
    eng.link_in_region("alpha", src, dst, EdgeKind::Refines, 0.5)
        .unwrap();
    eng.link(src, foreign, EdgeKind::Causes, 1.0).unwrap();

    assert!(eng
        .unlink_in_region("alpha", src, dst, EdgeKind::Causes)
        .unwrap());
    assert!(!eng
        .unlink_in_region("alpha", src, dst, EdgeKind::Causes)
        .unwrap());
    let remaining = eng.fetch_edges(Some(src), None, None).unwrap();
    assert!(remaining
        .iter()
        .any(|edge| { edge.dst_id == dst && edge.kind == EdgeKind::Refines }));
    assert!(!remaining
        .iter()
        .any(|edge| { edge.dst_id == dst && edge.kind == EdgeKind::Causes }));

    let error = eng
        .unlink_in_region("alpha", src, foreign, EdgeKind::Causes)
        .unwrap_err();
    assert!(
        matches!(
            error,
            MemError::AtomNotLive { atom_id, ref region }
                if atom_id == foreign && region == "alpha"
        ),
        "{error:?}"
    );
    assert_eq!(
        eng.fetch_edges(Some(src), Some(foreign), Some(EdgeKind::Causes))
            .unwrap()
            .len(),
        1,
        "a region-scoped unlink must not remove a foreign edge"
    );
}

#[test]
fn region_edge_cycle_check_ignores_a_foreign_legacy_path() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("local", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("foreign", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let src = eng
        .remember("local", AtomInput::new("note", "src"))
        .unwrap();
    let dst = eng
        .remember("local", AtomInput::new("note", "dst"))
        .unwrap();
    let bridge = eng
        .remember("foreign", AtomInput::new("note", "bridge"))
        .unwrap();
    eng.link(dst, bridge, EdgeKind::DependsOn, 1.0).unwrap();
    eng.link(bridge, src, EdgeKind::DependsOn, 1.0).unwrap();

    assert!(matches!(
        eng.link(src, dst, EdgeKind::DependsOn, 1.0),
        Err(MemError::Cycle { .. })
    ));
    eng.link_in_region("local", src, dst, EdgeKind::DependsOn, 1.0)
        .unwrap();
    assert_eq!(
        eng.fetch_edges(Some(src), Some(dst), Some(EdgeKind::DependsOn))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn region_edge_cycle_check_ignores_a_stale_sealed_intermediary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("sealed", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let src = eng
        .remember("sealed", AtomInput::new("note", "src"))
        .unwrap();
    let dst = eng
        .remember("sealed", AtomInput::new("note", "dst"))
        .unwrap();
    let bridge = eng
        .remember("sealed", AtomInput::new("note", "bridge"))
        .unwrap();
    eng.link(dst, bridge, EdgeKind::DependsOn, 1.0).unwrap();
    eng.link(bridge, src, EdgeKind::DependsOn, 1.0).unwrap();

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {table} SET key_gen = key_gen + 1 WHERE id = $1"),
            &[Value::Integer(bridge)],
        )
        .unwrap();

    assert!(matches!(
        eng.link(src, dst, EdgeKind::DependsOn, 1.0),
        Err(MemError::Cycle { .. })
    ));
    eng.link_in_region("sealed", src, dst, EdgeKind::DependsOn, 1.0)
        .unwrap();
}

#[test]
fn region_edge_reads_reject_stale_sealed_endpoint_bindings() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("sealed", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let src = eng
        .remember("sealed", AtomInput::new("note", "source"))
        .unwrap();
    let stale = eng
        .remember("sealed", AtomInput::new("note", "stale"))
        .unwrap();
    let live = eng
        .remember("sealed", AtomInput::new("note", "live"))
        .unwrap();
    eng.link(src, stale, EdgeKind::Refines, 1.0).unwrap();
    eng.link(src, live, EdgeKind::Causes, 1.0).unwrap();

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {table} SET key_gen = key_gen + 1 WHERE id = $1"),
            &[Value::Integer(stale)],
        )
        .unwrap();

    assert_eq!(
        eng.fetch_edges(Some(src), None, None).unwrap().len(),
        2,
        "global edge storage is unchanged by a stale atom-key binding"
    );
    let scoped = eng
        .fetch_edges_in_region("sealed", Some(src), None, None, 10)
        .unwrap();
    assert_eq!(scoped.len(), 1);
    assert_eq!((scoped[0].src_id, scoped[0].dst_id), (src, live));
    let first_live = eng
        .fetch_edges_in_region("sealed", Some(src), None, None, 1)
        .unwrap();
    assert_eq!(first_live.len(), 1);
    assert_eq!(
        (first_live[0].src_id, first_live[0].dst_id),
        (src, live),
        "the SQL limit must be filled after stale sealed bindings are filtered"
    );
}

#[test]
fn region_edge_writes_reject_stale_sealed_endpoint_bindings() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("sealed", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let src = eng
        .remember("sealed", AtomInput::new("note", "source"))
        .unwrap();
    let stale = eng
        .remember("sealed", AtomInput::new("note", "stale"))
        .unwrap();

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {table} SET key_gen = key_gen + 1 WHERE id = $1"),
            &[Value::Integer(stale)],
        )
        .unwrap();

    let error = eng
        .link_in_region("sealed", src, stale, EdgeKind::Refines, 1.0)
        .unwrap_err();
    assert!(
        matches!(
            error,
            MemError::AtomNotLive { atom_id, ref region }
                if atom_id == stale && region == "sealed"
        ),
        "{error:?}"
    );
    assert!(eng
        .fetch_edges(Some(src), Some(stale), None)
        .unwrap()
        .is_empty());
}

#[test]
fn provenance_writes_require_live_source_atoms() {
    let plain_dir = tempfile::tempdir().unwrap();
    let plain = MemoryEngine::open(create_db(plain_dir.path())).unwrap();
    plain
        .create_region("plain", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let expired = plain
        .remember(
            "plain",
            AtomInput::new("fact", "expired source").with_expires_at(micros_now() - 1),
        )
        .unwrap();
    assert!(matches!(
        plain.remember_derived(
            "plain",
            AtomInput::new("fact", "plain derived"),
            &[expired],
            None,
        ),
        Err(MemError::AtomNotLive { atom_id, ref region })
            if atom_id == expired && region == "plain"
    ));
    assert!(matches!(
        plain.remember_if_absent(
            "plain",
            AtomInput::new("fact", "plain absent"),
            &[expired],
            None,
        ),
        Err(MemError::AtomNotLive { atom_id, ref region })
            if atom_id == expired && region == "plain"
    ));

    let sealed_dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(sealed_dir.path());
    let sealed = MemoryEngine::open(Arc::clone(&db)).unwrap();
    sealed
        .create_encrypted_region("sealed", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let stale = sealed
        .remember("sealed", AtomInput::new("fact", "stale source"))
        .unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {table} SET key_gen = key_gen + 1 WHERE id = $1"),
            &[Value::Integer(stale)],
        )
        .unwrap();
    assert!(matches!(
        sealed.remember_if_absent_keyed(
            "sealed",
            AtomInput::new("fact", "sealed keyed"),
            &[stale],
            None,
            "request-1",
        ),
        Err(MemError::AtomNotLive { atom_id, ref region })
            if atom_id == stale && region == "sealed"
    ));
    assert!(sealed
        .fetch_edges(None, Some(stale), Some(EdgeKind::DerivedFrom))
        .unwrap()
        .is_empty());
}

#[derive(Clone, Copy)]
struct SelectivelyNonFiniteEmbedder {
    passage: bool,
    query: bool,
}

impl Embedder for SelectivelyNonFiniteEmbedder {
    fn dim(&self) -> usize {
        8
    }

    fn metric(&self) -> EmbeddingMetric {
        EmbeddingMetric::Cosine
    }

    fn model_id(&self) -> &str {
        match (self.passage, self.query) {
            (true, false) => "nonfinite-passage",
            (false, true) => "nonfinite-query",
            (true, true) => "nonfinite-both",
            (false, false) => "finite-control",
        }
    }

    fn embed_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::embed::EmbedError> {
        let mut vectors = Vec::with_capacity(texts.len());
        for _ in texts {
            if cancel.is_some_and(citadel_core::CancelToken::is_cancelled) {
                return Err(crate::embed::EmbedError::Interrupted);
            }
            let mut vector = vec![0.0; self.dim()];
            if self.passage {
                vector[3] = f32::NAN;
            }
            vectors.push(vector);
        }
        Ok(vectors)
    }

    fn embed_queries_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::embed::EmbedError> {
        let mut vectors = Vec::with_capacity(texts.len());
        for _ in texts {
            if cancel.is_some_and(citadel_core::CancelToken::is_cancelled) {
                return Err(crate::embed::EmbedError::Interrupted);
            }
            let mut vector = vec![0.0; self.dim()];
            if self.query {
                vector[5] = f32::INFINITY;
            }
            vectors.push(vector);
        }
        Ok(vectors)
    }
}

#[test]
fn atom_inputs_and_passage_vectors_are_finite_before_plain_or_sealed_writes() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = if encrypted {
            create_enc_db(dir.path())
        } else {
            create_db(dir.path())
        };
        let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
        let embedder = Arc::new(SelectivelyNonFiniteEmbedder {
            passage: true,
            query: false,
        });
        if encrypted {
            eng.create_encrypted_region("finite-input", embedder)
                .unwrap();
        } else {
            eng.create_region("finite-input", embedder).unwrap();
        }

        for atom in [
            AtomInput::new("fact", "bad importance").with_importance(f32::NAN),
            AtomInput::new("fact", "bad confidence").with_confidence(f32::NEG_INFINITY),
        ] {
            assert!(matches!(
                eng.remember("finite-input", atom),
                Err(MemError::Invalid(_))
            ));
        }
        assert!(matches!(
            eng.remember("finite-input", AtomInput::new("fact", "bad passage")),
            Err(MemError::Invalid(_))
        ));
        assert!(matches!(
            eng.remember_batch(
                "finite-input",
                vec![
                    AtomInput::new("fact", "batch passage one"),
                    AtomInput::new("fact", "batch passage two"),
                ],
            ),
            Err(MemError::Invalid(_))
        ));
        assert!(matches!(
            eng.remember_batch(
                "finite-input",
                vec![
                    AtomInput::new("fact", "finite first"),
                    AtomInput::new("fact", "bad second").with_importance(f32::INFINITY),
                ],
            ),
            Err(MemError::Invalid(_))
        ));

        assert!(eng
            .stored_atom_retrieval_state("finite-input")
            .unwrap()
            .is_empty());
        if encrypted {
            assert!(
                db.atom_store_live_owners().unwrap().is_empty(),
                "rejected sealed inputs must not allocate an ACK slot"
            );
        }
    }
}

#[test]
fn query_vectors_are_finite_before_single_or_multi_recall_observation() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("explicit-query", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("explicit-query", AtomInput::new("fact", "seed"))
        .unwrap();

    let mut invalid = vec![0.0; 8];
    invalid[1] = f32::NAN;
    assert!(matches!(
        eng.recall(
            "explicit-query",
            RecallQuery::by_embedding(invalid.clone(), 1)
        ),
        Err(MemError::Invalid(_))
    ));
    assert!(eng.access_stats.lock().unwrap().is_empty());

    let multi = MultiRecallQuery::new(
        vec![
            RecallQuery::by_embedding(vec![0.0; 8], 1),
            RecallQuery::by_embedding(invalid, 1),
        ],
        1,
    );
    assert!(matches!(
        eng.recall_many("explicit-query", multi),
        Err(MemError::Invalid(_))
    ));
    assert!(
        eng.access_stats.lock().unwrap().is_empty(),
        "a bad later sub-query must reject the batch before the first access is observed"
    );

    for weights in [
        FusionWeights {
            semantic: f32::NAN,
            ..FusionWeights::default()
        },
        FusionWeights {
            keyword: f32::INFINITY,
            ..FusionWeights::default()
        },
        FusionWeights {
            recency: f32::NEG_INFINITY,
            ..FusionWeights::default()
        },
        FusionWeights {
            importance: f32::NAN,
            ..FusionWeights::default()
        },
    ] {
        assert!(matches!(
            eng.recall(
                "explicit-query",
                RecallQuery::by_embedding(vec![0.0; 8], 1).with_weights(weights)
            ),
            Err(MemError::Invalid(_))
        ));
    }

    for rrf_k in [0.0, -1.0, f32::NAN, f32::INFINITY] {
        assert!(matches!(
            eng.recall_many(
                "explicit-query",
                MultiRecallQuery::new(vec![RecallQuery::by_embedding(vec![0.0; 8], 1)], 1)
                    .with_rrf_k(rrf_k)
            ),
            Err(MemError::Invalid(_))
        ));
    }

    eng.set_reranker(
        Arc::new(crate::embed::MockReranker),
        RerankStrategy::Rrf { k: f32::NAN },
    );
    assert!(matches!(
        eng.recall("explicit-query", RecallQuery::by_embedding(vec![0.0; 8], 1)),
        Err(MemError::Invalid(_))
    ));
    eng.clear_reranker();
    assert!(
        eng.access_stats.lock().unwrap().is_empty(),
        "numeric validation must happen before recall records access"
    );

    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = if encrypted {
            create_enc_db(dir.path())
        } else {
            create_db(dir.path())
        };
        let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
        let embedder = Arc::new(SelectivelyNonFiniteEmbedder {
            passage: false,
            query: true,
        });
        if encrypted {
            eng.create_encrypted_region("text-query", embedder).unwrap();
        } else {
            eng.create_region("text-query", embedder).unwrap();
        }
        eng.remember("text-query", AtomInput::new("fact", "seed"))
            .unwrap();
        assert!(matches!(
            eng.recall("text-query", RecallQuery::by_text("needle", 1)),
            Err(MemError::Invalid(_))
        ));
        assert!(eng.access_stats.lock().unwrap().is_empty());
    }
}

#[test]
fn region_metadata_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let id1 = {
        let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
        eng.create_region("notes", Arc::new(MockEmbedder::new(8)))
            .unwrap()
    };
    let eng = MemoryEngine::open(open_db(dir.path())).unwrap();
    let id2 = eng
        .create_region("notes", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert_eq!(id1, id2, "region persists across reopen");
}

#[test]
fn stored_region_names_reads_sorted_persisted_live_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let writer = MemoryEngine::open(db.clone()).unwrap();
    writer
        .create_encrypted_region("Zebra", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    writer
        .create_region("alpha", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    writer
        .create_region(
            "Middle",
            Arc::new(MockEmbedder::with_metric(12, EmbeddingMetric::L2)),
        )
        .unwrap();

    let inventory = MemoryEngine::open(db.clone()).unwrap();
    assert_eq!(inventory.database_data_path(), db.data_path());
    let identities = inventory.stored_region_identities().unwrap();
    let fields: Vec<_> = identities
        .iter()
        .map(|identity| {
            (
                identity.name(),
                identity.encrypted(),
                identity.dim(),
                identity.metric(),
                identity.model_id(),
            )
        })
        .collect();
    assert_eq!(
        fields,
        vec![
            ("alpha", false, 8, EmbeddingMetric::Cosine, MOCK_MODEL_ID),
            ("middle", false, 12, EmbeddingMetric::L2, MOCK_MODEL_ID),
            ("zebra", true, 8, EmbeddingMetric::Cosine, MOCK_MODEL_ID),
        ],
        "identity inventory preserves every exact persisted reader binding"
    );
    assert_eq!(
        inventory.stored_region_names().unwrap(),
        vec!["alpha".to_owned(), "middle".to_owned(), "zebra".to_owned()],
        "inventory comes from canonical persisted rows, not local attachments"
    );
    let zebra = inventory
        .stored_region_identity("ZEBRA")
        .unwrap()
        .expect("the lookup is case-insensitive");
    assert_eq!(zebra.name(), "zebra");
    assert!(zebra.encrypted());
    assert_eq!(zebra.dim(), 8);
    assert_eq!(zebra.metric(), EmbeddingMetric::Cosine);
    assert_eq!(zebra.model_id(), MOCK_MODEL_ID);
    assert_eq!(inventory.stored_region_identity("absent").unwrap(), None);
    writer.drop_region("middle").unwrap();
    assert_eq!(
        inventory.stored_region_names().unwrap(),
        vec!["alpha".to_owned(), "zebra".to_owned()]
    );

    let conn = Connection::open(&db).unwrap();
    let zebra = writer.load_region_row(&conn, "zebra").unwrap().unwrap();
    drop(conn);
    db.region_store_tombstone(
        zebra.rsk_slot.unwrap(),
        zebra.id as u64,
        zebra.rsk_gen.unwrap(),
    )
    .unwrap();
    assert!(matches!(
        inventory.stored_region_names(),
        Err(MemError::RegionForgotten(name)) if name == "zebra"
    ));
}

#[test]
fn stored_atom_kinds_is_sorted_physical_inventory_without_decryption() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let writer = MemoryEngine::open(db.clone()).unwrap();
    writer
        .create_region("PlainKinds", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let sealed_id = writer
        .create_encrypted_region("SealedKinds", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let expired = micros_now() - 1;
    for atom in [
        AtomInput::new("zeta", "plain z"),
        AtomInput::new("alpha", "plain a"),
        AtomInput::new("expired-only", "plain expired").with_expires_at(expired),
        AtomInput::new("alpha", "plain duplicate"),
    ] {
        writer.remember("plainkinds", atom).unwrap();
    }
    writer
        .remember("sealedkinds", AtomInput::new("sealed-z", "sealed z"))
        .unwrap();
    let corrupt = writer
        .remember("sealedkinds", AtomInput::new("sealed-a", "sealed a"))
        .unwrap();
    writer
        .remember(
            "sealedkinds",
            AtomInput::new("sealed-expired", "sealed expired").with_expires_at(expired),
        )
        .unwrap();
    let residue = writer
        .remember(
            "sealedkinds",
            AtomInput::new("erased-residue", "sealed residue"),
        )
        .unwrap();

    // Inventory-only engine (no regions/embedders) opened while every ACK is live.
    let inventory = MemoryEngine::open(db.clone()).unwrap();
    let sealed_table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let residue_row = conn
        .query_params(
            &format!("SELECT key_slot FROM {sealed_table} WHERE id = $1"),
            &[Value::Integer(residue)],
        )
        .unwrap();
    let residue_slot = as_int(&residue_row.rows[0][0]).unwrap() as u32;
    conn.execute_params(
        &format!("UPDATE {sealed_table} SET sealed = $1 WHERE id = $2"),
        &[Value::Blob(vec![0xff]), Value::Integer(corrupt)],
    )
    .unwrap();
    drop(conn);
    db.atom_store_tombstone(
        residue_slot,
        residue as u64,
        db.atom_store_slot(residue_slot).unwrap().gen,
    )
    .unwrap();

    assert_eq!(
        inventory.stored_atom_kinds("PLAINKINDS").unwrap(),
        vec![
            "alpha".to_owned(),
            "expired-only".to_owned(),
            "zeta".to_owned()
        ]
    );
    assert_eq!(
        inventory.stored_atom_kinds("sealedkinds").unwrap(),
        vec![
            "erased-residue".to_owned(),
            "sealed-a".to_owned(),
            "sealed-expired".to_owned(),
            "sealed-z".to_owned()
        ],
        "expired, undecryptable, and erased-ACK rows remain physical kind inventory"
    );
    assert!(matches!(
        inventory.stored_atom_kinds("missing"),
        Err(MemError::RegionNotFound(name)) if name == "missing"
    ));

    let conn = Connection::open(&db).unwrap();
    let sealed = writer
        .load_region_row(&conn, "sealedkinds")
        .unwrap()
        .unwrap();
    drop(conn);
    db.region_store_tombstone(
        sealed.rsk_slot.unwrap(),
        sealed_id as u64,
        sealed.rsk_gen.unwrap(),
    )
    .unwrap();
    assert!(matches!(
        inventory.stored_atom_kinds("sealedkinds"),
        Err(MemError::RegionForgotten(name)) if name == "sealedkinds"
    ));
}

#[test]
fn stored_atom_retrieval_state_is_exact_physical_metadata_without_content() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let writer = MemoryEngine::open(db.clone()).unwrap();
    let region_id = writer
        .create_encrypted_region("RetrievalState", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    writer
        .create_region("PlainState", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let expired = micros_now() - 1;
    let future = micros_now() + 1_000_000;
    let exact_score = f32::from_bits(0x3eaa_aaab);
    let turn = writer
        .remember(
            "retrievalstate",
            AtomInput::new("turn", "turn content").with_importance(0.0),
        )
        .unwrap();
    let residue = writer
        .remember(
            "retrievalstate",
            AtomInput::new("marker", "residue content")
                .with_importance(-0.0)
                .with_expires_at(expired),
        )
        .unwrap();
    let derived = writer
        .remember(
            "retrievalstate",
            AtomInput::new("derived", "derived content")
                .with_importance(exact_score)
                .with_expires_at(future),
        )
        .unwrap();
    let plain = writer
        .remember(
            "plainstate",
            AtomInput::new("plain", "plain content").with_importance(0.0),
        )
        .unwrap();

    // Open the reader first: reopening after the tombstone would reconcile it.
    let inventory = MemoryEngine::open(db.clone()).unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let residue_row = conn
        .query_params(
            &format!("SELECT key_slot, key_gen FROM {table} WHERE id = $1"),
            &[Value::Integer(residue)],
        )
        .unwrap();
    let residue_slot = as_int(&residue_row.rows[0][0]).unwrap() as u32;
    conn.execute_params(
        &format!("UPDATE {table} SET sealed = $1 WHERE id = $2"),
        &[Value::Blob(vec![0xff]), Value::Integer(turn)],
    )
    .unwrap();
    drop(conn);
    db.atom_store_tombstone(
        residue_slot,
        residue as u64,
        db.atom_store_slot(residue_slot).unwrap().gen,
    )
    .unwrap();

    let states = inventory
        .stored_atom_retrieval_state("RETRIEVALSTATE")
        .unwrap();
    assert_eq!(states.len(), 3);
    assert_eq!(
        states
            .iter()
            .map(|state| state.atom_id())
            .collect::<Vec<_>>(),
        vec![turn, residue, derived]
    );
    assert_eq!(states[0].kind(), "turn");
    assert_eq!(states[0].importance_bits(), 0.0f32.to_bits());
    assert_eq!(states[0].expires_at(), None);
    assert_eq!(states[1].kind(), "marker");
    assert_eq!(states[1].importance_bits(), (-0.0f32).to_bits());
    assert_eq!(states[1].expires_at(), Some(expired));
    assert_eq!(states[2].kind(), "derived");
    assert_eq!(states[2].importance_bits(), exact_score.to_bits());
    assert_eq!(states[2].expires_at(), Some(future));

    let plain_states = inventory.stored_atom_retrieval_state("plainstate").unwrap();
    assert_eq!(plain_states.len(), 1);
    assert_eq!(plain_states[0].atom_id(), plain);
    assert_eq!(plain_states[0].kind(), "plain");
    assert_eq!(plain_states[0].importance_bits(), 0.0f32.to_bits());
    assert_eq!(plain_states[0].expires_at(), None);
    assert!(matches!(
        inventory.stored_atom_retrieval_state("missing"),
        Err(MemError::RegionNotFound(name)) if name == "missing"
    ));

    let conn = Connection::open(&db).unwrap();
    let region = writer
        .load_region_row(&conn, "retrievalstate")
        .unwrap()
        .unwrap();
    drop(conn);
    db.region_store_tombstone(
        region.rsk_slot.unwrap(),
        region_id as u64,
        region.rsk_gen.unwrap(),
    )
    .unwrap();
    assert!(matches!(
        inventory.stored_atom_retrieval_state("retrievalstate"),
        Err(MemError::RegionForgotten(name)) if name == "retrievalstate"
    ));
}

#[test]
fn supersedes_edges_reject_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let v1 = eng
        .remember("r", AtomInput::new("self_model", "v1"))
        .unwrap();
    let v2 = eng
        .remember("r", AtomInput::new("self_model", "v2"))
        .unwrap();

    eng.link(v2, v1, EdgeKind::Supersedes, 1.0).unwrap();
    assert!(matches!(
        eng.link(v1, v2, EdgeKind::Supersedes, 1.0),
        Err(MemError::Cycle { .. })
    ));
    assert!(matches!(
        eng.link(v1, v1, EdgeKind::Supersedes, 1.0),
        Err(MemError::Cycle { .. })
    ));
}

#[test]
fn reranker_reorders_recall_results() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("rr", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("rr", AtomInput::new("turn", "alpha beta gamma delta"))
        .unwrap();
    eng.remember("rr", AtomInput::new("turn", "zeta eta theta"))
        .unwrap();
    eng.remember("rr", AtomInput::new("turn", "beta gamma"))
        .unwrap();

    // Proves set_reranker -> recall uses the cross-encoder path (overlap wins).
    eng.set_reranker(
        Arc::new(crate::embed::MockReranker),
        RerankStrategy::Replace,
    );
    let hits = eng
        .recall("rr", RecallQuery::by_text("alpha beta gamma delta", 3))
        .unwrap();
    assert_eq!(hits[0].text, "alpha beta gamma delta", "best overlap first");
    assert!(
        hits[0].relevance >= hits[1].relevance,
        "relevance descending"
    );
}

#[test]
fn fetch_last_returns_highest_id_of_kind() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert!(
        eng.fetch_last("r", "audit").unwrap().is_none(),
        "no atoms yet"
    );

    eng.remember("r", AtomInput::new("audit", "first")).unwrap();
    let second = eng
        .remember("r", AtomInput::new("audit", "second").with_importance(0.75))
        .unwrap();
    eng.remember("r", AtomInput::new("note", "unrelated"))
        .unwrap();

    let last = eng.fetch_last("r", "audit").unwrap().unwrap();
    assert_eq!(last.id, second);
    assert_eq!(last.text, "second");
    assert_eq!(last.importance, 0.75);
    assert_eq!(last.relevance, None);
    assert_eq!(last.distance, None);
    assert_eq!(last.graph_depth, None);
}

#[test]
fn delete_atoms_removes_rows_and_incident_edges() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("r", AtomInput::new("note", "a")).unwrap();
    let b = eng.remember("r", AtomInput::new("note", "b")).unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();

    let report = eng.delete_atoms("r", &[a]).unwrap();
    assert_eq!(report.removed, 1);
    assert!(
        eng.fetch_one("r", a).unwrap().is_none(),
        "target is deleted"
    );
    assert!(
        eng.fetch_one("r", b).unwrap().is_some(),
        "other atom survives"
    );
    assert!(
        eng.fetch_edges(Some(a), None, None).unwrap().is_empty(),
        "incident edge is removed with the atom"
    );
}

#[test]
fn delete_atoms_force_deletes_immutable_atoms() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let t = eng
        .remember("r", AtomInput::new("llm_trace", "resp").immutable())
        .unwrap();
    assert_eq!(eng.delete_atoms("r", &[t]).unwrap().removed, 1);
    assert!(eng.fetch_one("r", t).unwrap().is_none());
}

#[test]
fn delete_atoms_empty_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert_eq!(eng.delete_atoms("r", &[]).unwrap().removed, 0);
}

#[test]
fn delete_atoms_is_region_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    // same dim+metric -> shared atoms table, separated only by region_id.
    eng.create_region("a", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("b", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let in_b = eng.remember("b", AtomInput::new("note", "keep")).unwrap();

    eng.delete_atoms("a", &[in_b]).unwrap();
    assert!(
        eng.fetch_one("b", in_b).unwrap().is_some(),
        "an id from another region is never matched"
    );
}

#[test]
fn delete_atoms_region_scope_preserves_foreign_edges() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("a", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("b", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("a", AtomInput::new("note", "a")).unwrap();
    let b = eng.remember("b", AtomInput::new("note", "b")).unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();

    // edges live in one global table, so the no-match delete leaves a->b intact.
    eng.delete_atoms("a", &[b]).unwrap();
    assert!(
        eng.fetch_one("b", b).unwrap().is_some(),
        "foreign atom survives"
    );
    assert_eq!(
        eng.fetch_edges(Some(a), Some(b), None).unwrap().len(),
        1,
        "an edge to a foreign-region atom is not deleted"
    );
}

#[test]
fn delete_atoms_removes_all_listed_ids_not_just_first() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("r", AtomInput::new("note", "a")).unwrap();
    let b = eng.remember("r", AtomInput::new("note", "b")).unwrap();
    let c = eng.remember("r", AtomInput::new("note", "c")).unwrap();
    let keep = eng.remember("r", AtomInput::new("note", "keep")).unwrap();

    let report = eng.delete_atoms("r", &[a, b, c]).unwrap();
    assert_eq!(
        report.removed, 3,
        "every listed id is deleted, not just the first"
    );
    for id in [a, b, c] {
        assert!(
            eng.fetch_one("r", id).unwrap().is_none(),
            "atom {id} in the multi-id list is deleted"
        );
    }
    assert!(
        eng.fetch_one("r", keep).unwrap().is_some(),
        "an unlisted atom survives a multi-id delete"
    );
}

// The erasure tests live in-crate (not tests/) because they reach the key store.

/// The headline adversary test: an adversary holding the passphrase (and thus the
/// REK, key file, and full DB image) recovers a sealed atom BEFORE forget by reading
/// the LIVE slot, unwrapping the RCK, deriving the seal keys, and opening the blob;
/// AFTER forget the slot is tombstoned, so the RCK cannot be unwrapped and the
/// (still-present) sealed bytes are permanently undecryptable.
#[test]
fn adversary_recovers_before_forget_then_fails_after() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let secret = "classified: rendezvous at the old pier at dawn";
    let atom_id = eng.remember("s", AtomInput::new("fact", secret)).unwrap();

    // The exact ciphertext an adversary sees: the page-decrypted `sealed` column.
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT sealed, key_slot FROM {table} WHERE id = $1"),
            &[Value::Integer(atom_id)],
        )
        .unwrap();
    let sealed = match &qr.rows[0][0] {
        Value::Blob(b) => b.clone(),
        other => panic!("sealed column is not a blob: {other:?}"),
    };
    let atom_slot = match &qr.rows[0][1] {
        Value::Integer(s) => *s as u32,
        other => panic!("key_slot is not an integer: {other:?}"),
    };
    let row = eng.load_region_row(&conn, "s").unwrap().unwrap();
    drop(conn);
    let slot = row.rsk_slot.expect("encrypted region records a key slot");

    // Adversary BEFORE forget: RCK -> atom-wrap key -> unwrap the atom's ACK -> open.
    let rec = db.region_store_slot(slot).unwrap();
    assert_eq!(rec.state, SlotState::Live);
    let mut rck = db.unwrap_region_key(&rec.wrapped).unwrap();
    let atom_wrap = derive_atom_wrap_key(&rck);
    rck.zeroize();
    let atom_rec = db.atom_store_slot(atom_slot).unwrap();
    let ack = atom_wrap.unwrap_atom_key(&atom_rec.wrapped).unwrap();
    let seal = derive_seal_keys(&ack);
    let blob = blob_seal::open(&seal, atom_id as u64, &sealed).unwrap();
    let (_emb, text, _payload) = decode_atom_blob(&blob).unwrap();
    assert_eq!(
        text, secret,
        "adversary with the passphrase recovers content while the region is live"
    );
    drop(seal); // the legitimate session ends; only on-disk state remains

    eng.drop_region("s").unwrap();

    // After forget: slot tombstoned -> RCK gone -> sealed bytes undecryptable.
    let rec2 = db.region_store_slot(slot).unwrap();
    assert_eq!(
        rec2.state,
        SlotState::Tombstone,
        "forget tombstones the key slot"
    );
    assert!(
        db.unwrap_region_key(&rec2.wrapped).is_err(),
        "the destroyed (zeroed) wrapped key cannot be unwrapped, so the RCK is gone"
    );
}

/// Per-atom adversary: forgetting ONE atom tombstones only its key slot, so
/// its sealed bytes become permanently undecryptable while the region key and
/// a sibling atom's key are untouched and the sibling still decrypts.
#[test]
fn forget_atom_destroys_only_its_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let secret = "the vault combination is 19-77-42";
    let target = eng.remember("s", AtomInput::new("fact", secret)).unwrap();
    let sibling = eng
        .remember("s", AtomInput::new("fact", "an ordinary sibling memory"))
        .unwrap();

    // Capture the adversary's view of the target: sealed bytes + its key slot.
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT sealed, key_slot FROM {table} WHERE id = $1"),
            &[Value::Integer(target)],
        )
        .unwrap();
    let sealed = match &qr.rows[0][0] {
        Value::Blob(b) => b.clone(),
        other => panic!("sealed column is not a blob: {other:?}"),
    };
    let target_slot = match &qr.rows[0][1] {
        Value::Integer(s) => *s as u32,
        other => panic!("key_slot is not an integer: {other:?}"),
    };
    let row = eng.load_region_row(&conn, "s").unwrap().unwrap();
    drop(conn);
    let region_slot = row.rsk_slot.expect("encrypted region records a key slot");

    let rec = db.region_store_slot(region_slot).unwrap();
    let mut rck = db.unwrap_region_key(&rec.wrapped).unwrap();
    let atom_wrap = derive_atom_wrap_key(&rck);
    rck.zeroize();

    // BEFORE forget: unwrap the target's ACK and recover its plaintext.
    let target_rec = db.atom_store_slot(target_slot).unwrap();
    assert_eq!(target_rec.state, SlotState::Live);
    let ack = atom_wrap.unwrap_atom_key(&target_rec.wrapped).unwrap();
    let blob = blob_seal::open(&derive_seal_keys(&ack), target as u64, &sealed).unwrap();
    assert_eq!(decode_atom_blob(&blob).unwrap().1, secret);

    eng.forget_atom("s", target).unwrap();

    // After forget: target's ACK destroyed, sealed bytes undecryptable;
    // region key untouched.
    let target_rec2 = db.atom_store_slot(target_slot).unwrap();
    assert_eq!(
        target_rec2.state,
        SlotState::Tombstone,
        "forget_atom tombstones the atom's key slot"
    );
    assert!(
        atom_wrap.unwrap_atom_key(&target_rec2.wrapped).is_err(),
        "the destroyed ACK cannot be unwrapped"
    );
    assert_eq!(
        target_rec2.wrapped,
        [0u8; citadel_core::WRAPPED_KEY_SIZE],
        "the wrapped ACK is explicitly zeroed on tombstone, not just made un-unwrappable by chance"
    );
    assert_eq!(
        db.region_store_slot(region_slot).unwrap().state,
        SlotState::Live,
        "forget_atom leaves the region key untouched"
    );
    assert!(
        eng.fetch_one("s", sibling).unwrap().is_some(),
        "the sibling atom still decrypts after the target is forgotten"
    );
}

#[test]
fn forget_atoms_encrypted_yields_verifiable_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("s", AtomInput::new("fact", "alpha")).unwrap();
    let b = eng.remember("s", AtomInput::new("fact", "beta")).unwrap();

    let r = eng.forget_atoms("s", &[a], false).unwrap();
    assert!(
        r.cryptographic_erasure,
        "encrypted region forgets by destroying keys"
    );
    assert_eq!(r.erased_count, 1);
    assert_eq!(r.slots_erased.len(), 1);
    assert_eq!(r.rows_deleted, 1);
    assert!(r.fsync && r.readback_confirmed);
    assert_eq!(r.algorithm, "AES-256-KW(RFC3394)");
    let slot = &r.slots_erased[0];
    assert_eq!(slot.atom_id, a);
    assert_eq!(
        slot.new_gen,
        slot.old_gen + 1,
        "the tombstone supersedes the live key"
    );
    assert!(
        eng.fetch_one("s", a).unwrap().is_none(),
        "target is forgotten"
    );
    assert!(eng.fetch_one("s", b).unwrap().is_some(), "sibling survives");
}

#[test]
fn maintenance_forget_returns_its_receipt_after_the_irreversible_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("s", AtomInput::new("fact", "erase me"))
        .unwrap();
    let maintenance = MemoryMaintenance::open(Arc::clone(&db)).unwrap();

    let fired = arm_cancel_after_key_erasure(&db);
    let result = maintenance.forget_atoms("s", &[atom], false);
    finish_key_erasure_probe(&db, &fired);
    let receipt = result.expect("cancellation after committed erasure must not hide its receipt");

    assert_eq!(receipt.erased_count, 1);
    assert_eq!(receipt.rows_deleted, 1);
    assert!(receipt.fsync && receipt.readback_confirmed);
    assert!(eng.fetch_one("s", atom).unwrap().is_none());
}

#[test]
fn maintenance_forget_preserves_the_segment_when_an_atom_is_reserved() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("s", AtomInput::new("fact", "held by callback"))
        .unwrap();
    eng.persist_ann_index("s").unwrap();
    let lifecycle = db.key_lifecycle_lock();
    let reservation = lifecycle.reserve_memory_atom_callbacks(&[atom as u64]);
    drop(lifecycle);

    let error = MemoryMaintenance::open(Arc::clone(&db))
        .unwrap()
        .forget_atoms("s", &[atom], false)
        .unwrap_err();
    assert!(matches!(
        error,
        MemError::Core(citadel_core::Error::AtomInUse { atom_id }) if atom_id == atom as u64
    ));
    assert!(seg_tree_exists(&db, region_id, 8));
    assert!(read_annseg_meta(&Connection::open(&db).unwrap(), region_id)
        .unwrap()
        .is_some());
    assert!(eng.fetch_one("s", atom).unwrap().is_some());
    drop(reservation);
}

#[test]
fn drop_region_preserves_its_keys_and_segment_when_an_atom_is_reserved() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("s", AtomInput::new("fact", "held by callback"))
        .unwrap();
    eng.persist_ann_index("s").unwrap();
    let lifecycle = db.key_lifecycle_lock();
    let reservation = lifecycle.reserve_memory_atom_callbacks(&[atom as u64]);
    drop(lifecycle);

    let error = eng.drop_region("s").unwrap_err();
    assert!(matches!(
        error,
        MemError::Core(citadel_core::Error::AtomInUse { atom_id }) if atom_id == atom as u64
    ));
    assert!(seg_tree_exists(&db, region_id, 8));
    assert!(read_annseg_meta(&Connection::open(&db).unwrap(), region_id)
        .unwrap()
        .is_some());
    assert!(eng.fetch_one("s", atom).unwrap().is_some());

    drop(reservation);
    eng.drop_region("s").unwrap();
    assert_segment_retired(&db, region_id, 8);
}

#[test]
fn drop_region_retry_skips_a_region_key_slot_reused_by_a_successor() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let old_region = eng
        .create_encrypted_region("old", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("old", AtomInput::new("fact", "old secret"))
        .unwrap();
    eng.persist_ann_index("old").unwrap();
    let old_row = eng
        .load_region_row(&Connection::open(&db).unwrap(), "old")
        .unwrap()
        .unwrap();
    let slot = old_row.rsk_slot.unwrap();
    let old_generation = old_row.rsk_gen.unwrap();
    db.region_store_tombstone(slot, old_region as u64, old_generation)
        .unwrap();

    let successor = eng
        .create_encrypted_region("successor", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let successor_atom = eng
        .remember("successor", AtomInput::new("fact", "successor secret"))
        .unwrap();
    let successor_row = eng
        .load_region_row(&Connection::open(&db).unwrap(), "successor")
        .unwrap()
        .unwrap();
    assert_eq!(successor_row.rsk_slot, Some(slot));
    assert!(successor_row.rsk_gen.unwrap() > old_generation);

    eng.drop_region("old").unwrap();
    assert!(eng
        .load_region_row(&Connection::open(&db).unwrap(), "old")
        .unwrap()
        .is_none());
    assert_eq!(
        eng.fetch_one("successor", successor_atom)
            .unwrap()
            .unwrap()
            .text,
        "successor secret"
    );
    let record = db.region_store_slot(slot).unwrap();
    assert_eq!(record.state, SlotState::Live);
    assert_eq!(record.region_id, successor as u64);
    assert_eq!(record.gen, successor_row.rsk_gen.unwrap());
    assert_segment_retired(&db, old_region, 8);
}

#[test]
fn maintenance_forget_finishes_after_segment_key_erasure_cancels() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("s", AtomInput::new("fact", "erase me"))
        .unwrap();
    eng.persist_ann_index("s").unwrap();
    let maintenance = MemoryMaintenance::open(Arc::clone(&db)).unwrap();

    let fired = arm_cancel_after_segment_key_erasure(&db);
    let result = maintenance.forget_atoms("s", &[atom], false);
    finish_segment_key_erasure_probe(&db, &fired);
    let receipt = result.unwrap();

    assert_eq!(receipt.erased_count, 1);
    assert_eq!(receipt.rows_deleted, 1);
    assert!(eng.fetch_one("s", atom).unwrap().is_none());
    assert_segment_retired(&db, region_id, 8);
}

#[test]
fn operational_forget_finishes_cleanup_when_cancelled_after_key_erasure() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("s", AtomInput::new("fact", "erase me"))
        .unwrap();

    let fired = arm_cancel_after_key_erasure(&db);
    let result = eng.forget_atoms("s", &[atom], false);
    finish_key_erasure_probe(&db, &fired);
    let receipt = result.unwrap();
    assert_eq!(receipt.erased_count, 1);
    assert_eq!(receipt.rows_deleted, 1);
    assert!(eng.fetch_one("s", atom).unwrap().is_none());
}

#[test]
fn operational_forget_finishes_after_segment_key_erasure_cancels() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("s", AtomInput::new("fact", "erase me"))
        .unwrap();
    eng.persist_ann_index("s").unwrap();

    let fired = arm_cancel_after_segment_key_erasure(&db);
    let result = eng.forget_atoms("s", &[atom], false);
    finish_segment_key_erasure_probe(&db, &fired);
    let receipt = result.unwrap();

    assert_eq!(receipt.erased_count, 1);
    assert_eq!(receipt.rows_deleted, 1);
    assert!(eng.fetch_one("s", atom).unwrap().is_none());
    assert_segment_retired(&db, region_id, 8);
}

#[test]
fn forget_rejects_an_out_of_range_row_key_slot_without_erasing_any_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("s", AtomInput::new("fact", "keep me"))
        .unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let (slot, _) = atom_binding(&db, &table, atom);
    let aliased = (1_i64 << 32) + i64::from(slot);
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {table} SET key_slot = $1 WHERE id = $2"),
            &[Value::Integer(aliased), Value::Integer(atom)],
        )
        .unwrap();

    let error = eng
        .forget_atoms("s", &[atom], false)
        .expect_err("an overflowing slot was truncated into a valid destructive binding");
    assert!(error.to_string().contains("key_slot is out of range"));
    assert_eq!(db.atom_store_slot(slot).unwrap().state, SlotState::Live);
    assert_eq!(
        Connection::open(&db)
            .unwrap()
            .query_params(
                &format!("SELECT COUNT(*) FROM {table} WHERE id = $1"),
                &[Value::Integer(atom)],
            )
            .unwrap()
            .rows[0][0],
        Value::Integer(1)
    );
}

#[test]
fn dependent_forget_finishes_cleanup_when_cancelled_after_key_erasure() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let root = eng.remember("s", AtomInput::new("fact", "root")).unwrap();
    let dependent = eng
        .remember_derived("s", AtomInput::new("fact", "dependent"), &[root], None)
        .unwrap();

    let fired = arm_cancel_after_key_erasure(&db);
    let result = eng.forget_atoms_with_dependents("s", &[root], false);
    finish_key_erasure_probe(&db, &fired);
    let receipt = result.unwrap();
    assert_eq!(receipt.erased_count, 2);
    assert_eq!(receipt.rows_deleted, 2);
    assert!(eng.fetch_one("s", root).unwrap().is_none());
    assert!(eng.fetch_one("s", dependent).unwrap().is_none());
}

#[test]
fn dependent_forget_closure_stops_at_its_core_work_limit_before_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_region("bounded", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let root = eng
        .remember("bounded", AtomInput::new("fact", "root"))
        .unwrap();
    let first = eng
        .remember_derived(
            "bounded",
            AtomInput::new("fact", "first dependent"),
            &[root],
            None,
        )
        .unwrap();
    let second = eng
        .remember_derived(
            "bounded",
            AtomInput::new("fact", "second dependent"),
            &[first],
            None,
        )
        .unwrap();
    let handle = eng.region_handle("bounded").unwrap();
    let conn = Connection::open(&db).unwrap();

    let error = dependent_closure(&conn, &handle, &[root], 2, None)
        .expect_err("the reverse-provenance closure exceeded its work limit");
    assert!(matches!(
        error,
        MemError::WorkLimitExceeded {
            operation: "dependent forget",
            limit: 2
        }
    ));
    for id in [root, first, second] {
        assert!(
            eng.fetch_one("bounded", id).unwrap().is_some(),
            "work-limit refusal partially deleted atom {id}"
        );
    }
}

#[test]
fn dependent_forget_finishes_after_segment_key_erasure_cancels() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let root = eng.remember("s", AtomInput::new("fact", "root")).unwrap();
    let dependent = eng
        .remember_derived("s", AtomInput::new("fact", "dependent"), &[root], None)
        .unwrap();
    eng.persist_ann_index("s").unwrap();

    let fired = arm_cancel_after_segment_key_erasure(&db);
    let result = eng.forget_atoms_with_dependents("s", &[root], false);
    finish_segment_key_erasure_probe(&db, &fired);
    let receipt = result.unwrap();

    assert_eq!(receipt.erased_count, 2);
    assert_eq!(receipt.rows_deleted, 2);
    assert!(eng.fetch_one("s", root).unwrap().is_none());
    assert!(eng.fetch_one("s", dependent).unwrap().is_none());
    assert_segment_retired(&db, region_id, 8);
}

#[test]
fn region_drop_finishes_cleanup_when_cancelled_after_key_erasure() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("s", AtomInput::new("fact", "erase me"))
        .unwrap();

    let fired = arm_cancel_after_key_erasure(&db);
    let result = eng.drop_region("s");
    finish_key_erasure_probe(&db, &fired);
    result.unwrap();
    let rows = Connection::open(&db)
        .unwrap()
        .query("SELECT id FROM memory_regions WHERE name = 's'")
        .unwrap();
    assert!(rows.rows.is_empty());
}

#[test]
fn region_drop_finishes_after_segment_key_erasure_cancels() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("s", AtomInput::new("fact", "erase me"))
        .unwrap();
    eng.persist_ann_index("s").unwrap();

    let fired = arm_cancel_after_segment_key_erasure(&db);
    let result = eng.drop_region("s");
    finish_segment_key_erasure_probe(&db, &fired);
    result.unwrap();

    let rows = Connection::open(&db)
        .unwrap()
        .query("SELECT id FROM memory_regions WHERE name = 's'")
        .unwrap();
    assert!(rows.rows.is_empty());
    assert_segment_retired(&db, region_id, 8);
}

#[test]
fn eviction_finishes_cleanup_when_cancelled_after_key_erasure() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("s", AtomInput::new("fact", "erase me"))
        .unwrap();

    let fired = arm_cancel_after_key_erasure(&db);
    let result = eng.evict("s", EvictionPolicy::PurgeRegion);
    finish_key_erasure_probe(&db, &fired);
    let report = result.unwrap();
    assert_eq!(report.removed, 1);
    assert!(eng.fetch_one("s", atom).unwrap().is_none());
}

#[test]
fn eviction_finishes_after_segment_key_erasure_cancels() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("s", AtomInput::new("fact", "erase me"))
        .unwrap();
    eng.persist_ann_index("s").unwrap();

    let fired = arm_cancel_after_segment_key_erasure(&db);
    let result = eng.evict("s", EvictionPolicy::PurgeRegion);
    finish_segment_key_erasure_probe(&db, &fired);
    let report = result.unwrap();

    assert_eq!(report.removed, 1);
    assert!(eng.fetch_one("s", atom).unwrap().is_none());
    assert_segment_retired(&db, region_id, 8);
}

#[test]
fn forget_atoms_plaintext_is_logical_delete_not_crypto_erasure() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("p", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("p", AtomInput::new("fact", "alpha")).unwrap();

    let r = eng.forget_atoms("p", &[a], false).unwrap();
    assert!(
        !r.cryptographic_erasure,
        "plaintext region is a logical delete, not cryptographic erasure"
    );
    assert_eq!(r.erased_count, 0);
    assert!(r.slots_erased.is_empty());
    assert!(!r.fsync && !r.readback_confirmed);
    assert_eq!(r.algorithm, "");
    assert_eq!(r.rows_deleted, 1, "the row is still deleted");
    assert!(eng.fetch_one("p", a).unwrap().is_none());
}

#[test]
fn forget_atoms_skips_immutable_unless_forced() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let t = eng
        .remember("s", AtomInput::new("fact", "protected").immutable())
        .unwrap();

    let r = eng.forget_atoms("s", &[t], false).unwrap();
    assert_eq!(r.immutable_skipped, vec![t], "immutable atom is skipped");
    assert_eq!(r.erased_count, 0);
    assert_eq!(r.rows_deleted, 0);
    assert!(
        eng.fetch_one("s", t).unwrap().is_some(),
        "immutable atom survives an unforced forget"
    );

    let r = eng.forget_atoms("s", &[t], true).unwrap();
    assert!(r.immutable_skipped.is_empty(), "force erases immutable too");
    assert_eq!(r.erased_count, 1);
    assert!(eng.fetch_one("s", t).unwrap().is_none());
}

#[test]
fn forget_atoms_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("s", AtomInput::new("fact", "alpha")).unwrap();

    assert_eq!(eng.forget_atoms("s", &[a], false).unwrap().erased_count, 1);
    // Forgetting an already-erased id destroys no further keys and deletes no rows.
    let again = eng.forget_atoms("s", &[a], false).unwrap();
    assert_eq!(again.erased_count, 0);
    assert_eq!(again.rows_deleted, 0);
}

/// Flip a byte of an atom's stored sealed ciphertext on disk (simulating tampering).
fn corrupt_sealed(db: &Arc<Database>, table: &str, id: AtomId) {
    let conn = Connection::open(db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT sealed FROM {table} WHERE id=$1"),
            &[Value::Integer(id)],
        )
        .unwrap();
    let mut sealed = match &qr.rows[0][0] {
        Value::Blob(b) => b.clone(),
        o => panic!("sealed is not a blob: {o:?}"),
    };
    sealed[0] ^= 0xff;
    conn.execute("BEGIN").unwrap();
    conn.execute_params(
        &format!("UPDATE {table} SET sealed=$1 WHERE id=$2"),
        &[Value::Blob(sealed), Value::Integer(id)],
    )
    .unwrap();
    conn.execute("COMMIT").unwrap();
}

/// `verify_atoms` re-authenticates sealed bytes off disk: an intact atom is
/// Authentic, and flipping a byte of stored ciphertext is caught as Tampered
/// (CTR is malleable, so only the HMAC catches it); the batch does not abort
/// on the bad atom.
#[test]
fn verify_atoms_detects_tampering_off_disk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("s", AtomInput::new("fact", "alpha")).unwrap();
    let b = eng.remember("s", AtomInput::new("fact", "beta")).unwrap();

    let v = eng.verify_atoms("s", &[a, b]).unwrap();
    assert_eq!(v.len(), 2);
    assert!(v
        .iter()
        .all(|x| x.verdict == AttestVerdict::Authentic && x.aad_bound));

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    corrupt_sealed(&db, &table, a);

    let v = eng.verify_atoms("s", &[a, b]).unwrap();
    assert_eq!(
        v[0].verdict,
        AttestVerdict::Tampered,
        "tampered atom detected"
    );
    assert!(
        v[0].aad_bound,
        "verdict came from an aad-bound HMAC recomputation"
    );
    assert_eq!(
        v[1].verdict,
        AttestVerdict::Authentic,
        "sibling unaffected, no abort on the bad atom"
    );
}

/// KeyErased is the crash-recovery state: `forget` destroys the key BEFORE
/// deleting the row, so a crash in between leaves the row present with its key
/// gone. verify must report KeyErased (content unrecoverable), distinct from a
/// never-stored id (Missing). A clean forget deletes the row too, reading as
/// Missing, so we simulate the partial state directly.
#[test]
fn verify_atoms_reports_key_erased_and_missing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("s", AtomInput::new("fact", "alpha")).unwrap();

    // Destroy the atom's key but leave its row (the crash-recovery state).
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT key_slot FROM {table} WHERE id=$1"),
            &[Value::Integer(a)],
        )
        .unwrap();
    let slot = match &qr.rows[0][0] {
        Value::Integer(s) => *s as u32,
        o => panic!("key_slot is not an integer: {o:?}"),
    };
    drop(conn);
    db.atom_store_tombstone(slot, a as u64, db.atom_store_slot(slot).unwrap().gen)
        .unwrap();

    let v = eng.verify_atoms("s", &[a, 9999]).unwrap();
    assert_eq!(
        v[0].verdict,
        AttestVerdict::KeyErased,
        "key destroyed but row present"
    );
    assert!(!v[0].aad_bound);
    assert_eq!(v[1].verdict, AttestVerdict::Missing, "never-stored id");
}

#[test]
fn stale_atom_key_binding_is_neither_counted_nor_decrypted() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("s", AtomInput::new("fact", "stale binding"))
        .unwrap();
    let maintenance = MemoryMaintenance::open(Arc::clone(&db)).unwrap();

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {table} SET key_gen = key_gen + 1 WHERE id = $1"),
            &[Value::Integer(atom)],
        )
        .unwrap();

    assert_eq!(eng.count_region("s").unwrap(), 0);
    assert!(eng
        .fetch_range("s", &FetchQuery::new(10))
        .unwrap()
        .is_empty());
    assert_eq!(maintenance.count_region("s").unwrap(), 0);
    assert!(maintenance
        .fetch_range("s", &FetchQuery::new(10))
        .unwrap()
        .is_empty());
    assert_eq!(
        maintenance.verify_atoms("s", &[atom]).unwrap()[0].verdict,
        AttestVerdict::KeyErased
    );
}

#[test]
fn a_warm_sealed_ann_cache_revalidates_the_row_key_binding() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember(
            "s",
            AtomInput::new("fact", "cached secret").with_embedding(unit(8, 0)),
        )
        .unwrap();
    let query = RecallQuery::by_embedding(unit(8, 0), 1);
    assert_eq!(eng.recall("s", query.clone()).unwrap()[0].id, atom);
    assert!(eng
        .region_handle("s")
        .unwrap()
        .ann
        .read()
        .unwrap()
        .as_ref()
        .is_some_and(|ann| ann.cached.contains_key(&atom)));

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {table} SET key_gen = key_gen + 1 WHERE id = $1"),
            &[Value::Integer(atom)],
        )
        .unwrap();

    assert!(
        eng.recall("s", query).unwrap().is_empty(),
        "a warm plaintext cache must not outlive its exact row/key binding"
    );
}

#[test]
fn a_warm_sealed_ann_cache_spends_the_materialized_budget_before_cloning() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let cached_text = "cached secret ".repeat(1_280);
    let cached_content_bytes =
        atom_content_bytes("fact", &cached_text, &serde_json::Value::Null, usize::MAX);
    let atom = eng
        .remember(
            "s",
            AtomInput::new("fact", cached_text).with_embedding(unit(8, 0)),
        )
        .unwrap();
    let query = RecallQuery::by_embedding(unit(8, 0), 1).with_superseded(true);
    assert_eq!(eng.recall("s", query.clone()).unwrap()[0].id, atom);
    assert!(eng
        .region_handle("s")
        .unwrap()
        .ann
        .read()
        .unwrap()
        .as_ref()
        .is_some_and(|ann| ann.cached.contains_key(&atom)));

    let error = eng
        .with_read_limits(
            MemoryReadLimits::new(1024 * 1024, 24 * 1024, 1024 * 1024),
            |eng| eng.recall("s", query),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        MemError::ReadLimitExceeded { size, .. } if size == cached_content_bytes
    ));
}

#[test]
fn warm_sealed_ann_only_spends_returned_budget_on_the_final_hit() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atoms = (0..64)
        .map(|index| {
            AtomInput::new("fact", format!("candidate {index:02} with bounded content"))
                .with_embedding(unit(8, index % 8))
        })
        .collect();
    eng.remember_batch("s", atoms).unwrap();
    let query = RecallQuery::by_embedding(unit(8, 0), 1).with_superseded(true);
    eng.recall("s", query.clone()).unwrap();

    let hits = eng
        .with_read_limits(MemoryReadLimits::new(1024 * 1024, 1024 * 1024, 64), |eng| {
            eng.recall("s", query)
        })
        .unwrap();
    assert_eq!(hits.len(), 1);
}

#[test]
fn fetch_page_does_not_charge_the_returned_budget_for_its_lookahead_atom() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("plain", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let first = eng
        .remember("plain", AtomInput::new("fact", "first"))
        .unwrap();
    eng.remember("plain", AtomInput::new("fact", "x".repeat(512)))
        .unwrap();

    let page = eng
        .with_read_limits(MemoryReadLimits::new(1024 * 1024, 1024 * 1024, 64), |eng| {
            eng.fetch_page("plain", &FetchQuery::new(1))
        })
        .unwrap();
    assert_eq!(page.atoms.len(), 1);
    assert_eq!(page.atoms[0].id, first);
    assert_eq!(page.next_after_id, Some(first));
}

#[test]
fn edge_page_does_not_charge_returned_budget_for_lookahead_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("plain", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let source = eng
        .remember("plain", AtomInput::new("fact", "source"))
        .unwrap();
    let first = eng
        .remember("plain", AtomInput::new("fact", "first"))
        .unwrap();
    let second = eng
        .remember("plain", AtomInput::new("fact", "second"))
        .unwrap();
    eng.link_in_region("plain", source, first, EdgeKind::Causes, 1.0)
        .unwrap();
    eng.link_with_evidence_in_region(
        "plain",
        source,
        second,
        EdgeKind::Causes,
        1.0,
        Some(serde_json::json!({"detail": "x".repeat(512)})),
    )
    .unwrap();

    let page = eng
        .with_read_limits(MemoryReadLimits::new(1024 * 1024, 1024 * 1024, 1), |eng| {
            eng.fetch_edges_page_in_region("plain", None, None, None, None, 1)
        })
        .unwrap();
    assert_eq!(page.edges.len(), 1);
    assert_eq!(
        (page.edges[0].src_id, page.edges[0].dst_id),
        (source, first)
    );
    assert!(page.next_after.is_some());
}

#[test]
fn profile_is_region_scoped_deterministic_and_does_not_charge_lookahead_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("profile", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("foreign", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let first = eng
        .remember(
            "profile",
            AtomInput::new("fact", "a").with_embedding(unit(8, 0)),
        )
        .unwrap();
    let second = eng
        .remember(
            "profile",
            AtomInput::new("fact", "b").with_embedding(unit(8, 1)),
        )
        .unwrap();
    let third = eng
        .remember(
            "profile",
            AtomInput::new("fact", "c").with_embedding(unit(8, 2)),
        )
        .unwrap();
    let foreign = eng
        .remember(
            "foreign",
            AtomInput::new("fact", "foreign").with_embedding(unit(8, 0)),
        )
        .unwrap();
    let foreign_peer = eng
        .remember(
            "foreign",
            AtomInput::new("fact", "foreign peer").with_embedding(unit(8, 1)),
        )
        .unwrap();
    eng.link_in_region("profile", first, second, EdgeKind::Causes, 1.0)
        .unwrap();
    eng.link_with_evidence_in_region(
        "profile",
        first,
        third,
        EdgeKind::Causes,
        1.0,
        Some(serde_json::json!({"detail": "x".repeat(512)})),
    )
    .unwrap();
    eng.link_in_region("foreign", foreign, foreign_peer, EdgeKind::Refines, 1.0)
        .unwrap();

    let query = RecallQuery::by_embedding(unit(8, 0), 3).with_superseded(true);
    let profile = eng
        .with_read_limits(
            MemoryReadLimits::new(1024 * 1024, 1024 * 1024, 128),
            |eng| eng.profile("profile", query.clone(), 1),
        )
        .unwrap();
    assert_eq!(profile.atoms.len(), 3);
    assert!(profile.atoms.iter().all(|atom| atom.id != foreign));
    assert!(profile.edges_truncated);
    assert_eq!(profile.edges.len(), 1);
    assert_eq!(
        (profile.edges[0].src_id, profile.edges[0].dst_id),
        (first, second)
    );

    let repeated = eng.profile("profile", query, 1).unwrap();
    assert_eq!(repeated.edges.len(), 1);
    assert_eq!(
        (repeated.edges[0].src_id, repeated.edges[0].dst_id),
        (first, second)
    );
}

#[test]
fn summary_page_charges_returned_kind_and_cursor_text() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("plain", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let first_kind = format!("a{}", "x".repeat(127));
    let second_kind = format!("z{}", "x".repeat(127));
    eng.remember("plain", AtomInput::new(&first_kind, "first"))
        .unwrap();
    eng.remember("plain", AtomInput::new(second_kind, "second"))
        .unwrap();

    let error = eng
        .with_read_limits(
            MemoryReadLimits::new(1024 * 1024, 1024 * 1024, first_kind.len()),
            |eng| eng.summarize_page("plain", &SummaryQuery::new(0, 1)),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        MemError::ReadLimitExceeded {
            size,
            remaining: 0,
            ..
        } if size == first_kind.len()
    ));
}

#[test]
fn persisted_name_and_kind_inventories_charge_returned_text() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    let region = format!("region-{}", "r".repeat(96));
    let kind = format!("kind-{}", "k".repeat(96));
    eng.create_region(&region, Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember(&region, AtomInput::new(&kind, "content"))
        .unwrap();
    let limits = MemoryReadLimits::new(1024 * 1024, 1024 * 1024, 32);

    assert!(matches!(
        eng.with_read_limits(limits, |eng| eng.stored_region_names()),
        Err(MemError::ReadLimitExceeded { .. })
    ));
    assert!(matches!(
        eng.with_read_limits(limits, |eng| eng.stored_atom_kinds(&region)),
        Err(MemError::ReadLimitExceeded { .. })
    ));
}

#[test]
fn endpoint_projection_does_not_materialize_unused_edge_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("plain", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let source = eng
        .remember("plain", AtomInput::new("fact", "source"))
        .unwrap();
    let destination = eng
        .remember("plain", AtomInput::new("fact", "destination"))
        .unwrap();
    eng.link_with_evidence_in_region(
        "plain",
        source,
        destination,
        EdgeKind::DerivedFrom,
        1.0,
        Some(serde_json::json!({"detail": "x".repeat(4 * 1024)})),
    )
    .unwrap();
    let limits = MemoryReadLimits::new(16 * 1024, 6_500, 16 * 1024);

    let endpoints = eng
        .with_read_limits(limits, |eng| {
            eng.fetch_edge_endpoints_from_atoms_in_region(
                "plain",
                &[source],
                Some(EdgeKind::DerivedFrom),
                1,
            )
        })
        .unwrap();
    assert_eq!(endpoints, vec![(source, destination)]);

    let error = eng
        .with_read_limits(limits, |eng| {
            eng.fetch_edges_from_atoms_in_region("plain", &[source], Some(EdgeKind::DerivedFrom), 1)
        })
        .unwrap_err();
    assert!(matches!(error, MemError::ReadLimitExceeded { .. }));
}

#[test]
fn exact_id_batch_reads_are_region_local_ordered_and_expiry_aware() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = if encrypted {
            create_enc_db(dir.path())
        } else {
            create_db(dir.path())
        };
        let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
        if encrypted {
            eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(8)))
                .unwrap();
        } else {
            eng.create_region("r", Arc::new(MockEmbedder::new(8)))
                .unwrap();
        }
        let future = micros_now() + 60_000_000;
        let first = eng
            .remember(
                "r",
                AtomInput::new("fact", "first")
                    .with_confidence(0.25)
                    .with_expires_at(future)
                    .with_embedding(unit(8, 0)),
            )
            .unwrap();
        let expired = eng
            .remember(
                "r",
                AtomInput::new("fact", "expired").with_expires_at(micros_now() - 1),
            )
            .unwrap();
        let second = eng.remember("r", AtomInput::new("fact", "second")).unwrap();

        let hits = eng
            .fetch_by_ids("r", &[second, i64::MAX, expired, first])
            .unwrap();
        assert_eq!(hits.len(), 4);
        assert_eq!(hits[0].as_ref().map(|hit| hit.id), Some(second));
        assert!(hits[1].is_none());
        assert!(hits[2].is_none());
        assert_eq!(hits[3].as_ref().map(|hit| hit.id), Some(first));
        assert_eq!(hits[3].as_ref().map(|hit| hit.confidence), Some(0.25));
        assert_eq!(
            hits[3].as_ref().and_then(|hit| hit.expires_at),
            Some(future)
        );

        let recalled = eng
            .recall(
                "r",
                RecallQuery::by_embedding(unit(8, 0), 10).with_superseded(true),
            )
            .unwrap();
        let recalled_first = recalled.iter().find(|hit| hit.id == first).unwrap();
        assert_eq!(recalled_first.confidence, 0.25);
        assert_eq!(recalled_first.expires_at, Some(future));

        let maintenance = MemoryMaintenance::open(db).unwrap();
        let maintained = maintenance.fetch_by_ids("r", &[first]).unwrap();
        assert_eq!(maintained[0].as_ref().map(|hit| hit.confidence), Some(0.25));
        assert_eq!(
            maintained[0].as_ref().and_then(|hit| hit.expires_at),
            Some(future)
        );

        assert!(matches!(
            eng.with_read_limits(MemoryReadLimits::new(1024 * 1024, 1024 * 1024, 1), |eng| {
                eng.fetch_by_ids("r", &[first])
            },),
            Err(MemError::ReadLimitExceeded { .. })
        ));
        assert!(matches!(
            maintenance.with_read_limits(
                MemoryReadLimits::new(1024 * 1024, 1024 * 1024, 1),
                |maintenance| maintenance.fetch_by_ids("r", &[first]),
            ),
            Err(MemError::ReadLimitExceeded { .. })
        ));

        assert!(matches!(
            eng.fetch_by_ids("r", &[first, first]),
            Err(MemError::Invalid(message)) if message.contains("duplicate id")
        ));
    }
}

#[test]
fn a_plain_overflow_value_is_refused_by_the_storage_budget() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("plain", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("plain", AtomInput::new("fact", "x".repeat(16 * 1024)))
        .unwrap();

    let error = eng
        .with_read_limits(MemoryReadLimits::new(128, 1024, 1024), |eng| {
            eng.fetch_range("plain", &FetchQuery::new(1))
        })
        .unwrap_err();
    assert!(matches!(error, MemError::ReadLimitExceeded { .. }));
}

#[test]
fn a_warm_sealed_ann_cache_rebuilds_after_raw_row_metadata_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember(
            "s",
            AtomInput::new("before", "cached secret").with_embedding(unit(8, 0)),
        )
        .unwrap();
    let query = RecallQuery::by_embedding(unit(8, 0), 1);
    assert_eq!(eng.recall("s", query.clone()).unwrap()[0].kind, "before");

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {table} SET kind = 'after' WHERE id = $1"),
            &[Value::Integer(atom)],
        )
        .unwrap();

    let hits = eng.recall("s", query).unwrap();
    assert_eq!(hits[0].id, atom);
    assert_eq!(
        hits[0].kind, "after",
        "cached plaintext metadata must be bound to its atom-table snapshot"
    );
}

#[test]
fn a_persisted_sealed_ann_rebuilds_after_raw_kind_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember(
            "s",
            AtomInput::new("before", "persisted secret").with_embedding(unit(8, 0)),
        )
        .unwrap();
    eng.persist_ann_index("s").unwrap();

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {table} SET kind = 'after' WHERE id = $1"),
            &[Value::Integer(atom)],
        )
        .unwrap();
    drop(eng);

    let reopened = MemoryEngine::open(Arc::clone(&db)).unwrap();
    reopened
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let hits = reopened
        .recall(
            "s",
            RecallQuery::by_embedding(unit(8, 0), 1).with_kinds(vec!["after".into()]),
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, atom);
    assert_eq!(hits[0].kind, "after");
    assert!(matches!(
        reopened.ann_cache_status("s").unwrap(),
        Some(AnnIndexSource::Built { .. })
    ));
}

/// Origin-binding: a blob replayed from another atom's row fails because the
/// HMAC is recomputed with the target id as authenticated data.
#[test]
fn verify_atoms_rejects_replayed_blob_from_another_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("s", AtomInput::new("fact", "alpha")).unwrap();
    let b = eng.remember("s", AtomInput::new("fact", "beta")).unwrap();

    // Copy a's sealed bytes into b's row: byte-valid, but bound to a's id, not b's.
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT sealed FROM {table} WHERE id=$1"),
            &[Value::Integer(a)],
        )
        .unwrap();
    let a_sealed = match &qr.rows[0][0] {
        Value::Blob(x) => x.clone(),
        o => panic!("sealed is not a blob: {o:?}"),
    };
    conn.execute("BEGIN").unwrap();
    conn.execute_params(
        &format!("UPDATE {table} SET sealed=$1 WHERE id=$2"),
        &[Value::Blob(a_sealed), Value::Integer(b)],
    )
    .unwrap();
    conn.execute("COMMIT").unwrap();
    drop(conn);

    let v = eng.verify_atoms("s", &[b]).unwrap();
    assert_eq!(
        v[0].verdict,
        AttestVerdict::Tampered,
        "a blob replayed from another row is rejected by the aad-bound MAC"
    );
}

/// Plaintext region: no per-atom MAC, so attestation is PlaintextUnattested
/// (never a false Authentic); absent ids are Missing.
#[test]
fn verify_atoms_plaintext_is_unattested() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("p", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("p", AtomInput::new("fact", "alpha")).unwrap();

    let v = eng.verify_atoms("p", &[a, 9999]).unwrap();
    assert_eq!(v[0].verdict, AttestVerdict::PlaintextUnattested);
    assert!(!v[0].aad_bound);
    assert_eq!(v[1].verdict, AttestVerdict::Missing);
}

/// `update_atom_payload` (encrypted): payload replaced; edges, embedding, and
/// seal integrity preserved; immutable/absent rejected.
#[test]
fn update_atom_payload_encrypted_preserves_embedding_edges_and_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember(
            "s",
            AtomInput::new("fact", "the sky is blue today")
                .with_payload(serde_json::json!({"v": 1})),
        )
        .unwrap();
    let b = eng
        .remember("s", AtomInput::new("fact", "sibling"))
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();

    let outcome = eng
        .update_atom_payload("s", a, &serde_json::json!({"v": 2, "note": "updated"}))
        .unwrap();
    assert!(outcome.changed);

    assert_eq!(
        eng.fetch_one("s", a).unwrap().unwrap().payload,
        serde_json::json!({"v": 2, "note": "updated"})
    );
    let hits = eng.recall("s", RecallQuery::by_text("sky", 5)).unwrap();
    assert!(hits.iter().any(|h| h.id == a));
    let edges = eng.fetch_edges(Some(a), None, None).unwrap();
    assert!(edges
        .iter()
        .any(|e| e.dst_id == b && e.kind == EdgeKind::DerivedFrom));
    assert_eq!(
        eng.verify_atoms("s", &[a]).unwrap()[0].verdict,
        AttestVerdict::Authentic
    );

    let imm = eng
        .remember("s", AtomInput::new("fact", "locked").immutable())
        .unwrap();
    assert!(eng
        .update_atom_payload("s", imm, &serde_json::json!({"x": 1}))
        .is_err());
    assert!(eng
        .update_atom_payload("s", 99999, &serde_json::json!({"x": 1}))
        .is_err());
}

#[test]
fn invalid_and_same_payload_updates_preserve_a_persisted_sealed_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let payload = serde_json::json!({"state": "unchanged", "n": 1});
    let atom = eng
        .remember(
            "s",
            AtomInput::new("fact", "persisted").with_payload(payload.clone()),
        )
        .unwrap();
    eng.persist_ann_index("s").unwrap();
    let meta = read_annseg_meta(&Connection::open(&db).unwrap(), region_id)
        .unwrap()
        .expect("persisted sealed segment");
    let epoch = db.cache_epoch();

    assert!(eng
        .update_atom_payload("s", i64::MAX, &serde_json::json!({"state": "new"}))
        .is_err());
    assert_eq!(db.cache_epoch(), epoch);
    assert!(seg_tree_exists(&db, region_id, 8));
    assert_eq!(
        read_annseg_meta(&Connection::open(&db).unwrap(), region_id)
            .unwrap()
            .unwrap(),
        meta
    );

    assert!(
        !eng.update_atom_payload("s", atom, &payload)
            .unwrap()
            .changed
    );
    assert_eq!(
        db.cache_epoch(),
        epoch,
        "same payload must not arm the epoch"
    );
    assert!(seg_tree_exists(&db, region_id, 8));
    assert_eq!(
        read_annseg_meta(&Connection::open(&db).unwrap(), region_id)
            .unwrap()
            .unwrap(),
        meta
    );

    assert!(
        eng.update_atom_payload("s", atom, &serde_json::json!({"state": "changed"}))
            .unwrap()
            .changed
    );
    assert!(db.cache_epoch() > epoch);
    assert_segment_retired(&db, region_id, 8);
}

#[test]
fn expired_atoms_reject_payload_updates_and_evolution_before_side_effects() {
    let expired_at = micros_now() - 1;

    let plain_dir = tempfile::tempdir().unwrap();
    let plain = MemoryEngine::open(create_db(plain_dir.path())).unwrap();
    plain
        .create_region("p", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let plain_expired = plain
        .remember(
            "p",
            AtomInput::new("fact", "expired plain")
                .with_payload(serde_json::json!({"v": 1}))
                .with_expires_at(expired_at),
        )
        .unwrap();
    assert!(plain
        .update_atom_payload("p", plain_expired, &serde_json::json!({"v": 2}))
        .is_err());
    assert!(plain.evolve("p", plain_expired, 1, f32::MAX).is_err());
    assert!(plain
        .fetch_edges(Some(plain_expired), None, None)
        .unwrap()
        .is_empty());

    let sealed_dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(sealed_dir.path());
    let sealed = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = sealed
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let sealed_expired = sealed
        .remember(
            "s",
            AtomInput::new("fact", "expired sealed")
                .with_payload(serde_json::json!({"v": 1}))
                .with_expires_at(expired_at),
        )
        .unwrap();
    sealed
        .remember("s", AtomInput::new("fact", "live sibling"))
        .unwrap();
    sealed.persist_ann_index("s").unwrap();
    let epoch = db.cache_epoch();
    let meta = read_annseg_meta(&Connection::open(&db).unwrap(), region_id)
        .unwrap()
        .expect("persisted sealed segment");

    assert!(sealed
        .update_atom_payload("s", sealed_expired, &serde_json::json!({"v": 2}))
        .is_err());
    assert!(sealed.evolve("s", sealed_expired, 1, f32::MAX).is_err());
    assert_eq!(db.cache_epoch(), epoch);
    assert!(seg_tree_exists(&db, region_id, 8));
    assert_eq!(
        read_annseg_meta(&Connection::open(&db).unwrap(), region_id)
            .unwrap()
            .unwrap(),
        meta
    );
}

#[test]
fn sealed_summary_pages_exact_live_kinds_and_rejects_stale_evolution_targets() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let stale = eng
        .remember("s", AtomInput::new("aardvark", "key-erased residue"))
        .unwrap();
    eng.remember("s", AtomInput::new("alpha", "first live alpha"))
        .unwrap();
    eng.remember("s", AtomInput::new("zeta", "live zeta"))
        .unwrap();
    eng.remember("s", AtomInput::new("alpha", "second live alpha"))
        .unwrap();

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let (slot, generation) = atom_binding(&db, &table, stale);
    db.atom_store_tombstone(slot, stale as u64, generation)
        .unwrap();

    let first = eng.summarize_page("s", &SummaryQuery::new(0, 1)).unwrap();
    assert_eq!(first.total, 3, "key-erased residue must not be counted");
    assert_eq!(first.kinds.len(), 1);
    assert_eq!(first.kinds[0].kind, "alpha");
    assert_eq!(first.kinds[0].count, 2);
    assert_eq!(first.next_after_kind.as_deref(), Some("alpha"));

    let second = eng
        .summarize_page("s", &SummaryQuery::new(0, 1).with_after_kind("alpha"))
        .unwrap();
    assert_eq!(second.total, 3);
    assert_eq!(second.kinds.len(), 1);
    assert_eq!(second.kinds[0].kind, "zeta");
    assert_eq!(second.next_after_kind, None);
    assert!(eng.summarize_page("s", &SummaryQuery::new(0, 0)).is_err());
    assert!(eng
        .summarize_page("s", &SummaryQuery::new(0, MAX_SUMMARY_KIND_LIMIT + 1),)
        .is_err());
    assert!(eng.evolve("s", stale, 1, f32::MAX).is_err());
}

/// `update_atom_payload` (plaintext): payload replaced; recall + edge
/// preserved; immutable/absent rejected.
#[test]
fn update_atom_payload_plaintext_preserves_recall_and_edges() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("p", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember(
            "p",
            AtomInput::new("fact", "the sky is blue today")
                .with_payload(serde_json::json!({"v": 1})),
        )
        .unwrap();
    let b = eng
        .remember("p", AtomInput::new("fact", "sibling"))
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();

    let outcome = eng
        .update_atom_payload("p", a, &serde_json::json!({"v": 9}))
        .unwrap();
    assert!(outcome.changed);
    assert!(
        !eng.update_atom_payload("p", a, &serde_json::json!({"v": 9}))
            .unwrap()
            .changed
    );

    assert_eq!(
        eng.fetch_one("p", a).unwrap().unwrap().payload,
        serde_json::json!({"v": 9})
    );
    let hits = eng.recall("p", RecallQuery::by_text("sky", 5)).unwrap();
    assert!(hits.iter().any(|h| h.id == a));
    let edges = eng.fetch_edges(Some(a), None, None).unwrap();
    assert!(edges
        .iter()
        .any(|e| e.dst_id == b && e.kind == EdgeKind::DerivedFrom));

    let imm = eng
        .remember("p", AtomInput::new("fact", "locked").immutable())
        .unwrap();
    assert!(eng
        .update_atom_payload("p", imm, &serde_json::json!({"x": 1}))
        .is_err());
    assert!(eng
        .update_atom_payload("p", 99999, &serde_json::json!({"x": 1}))
        .is_err());
}

/// A region whose key was destroyed while its row survives (the crash window between
/// key-destroy and row-delete) must refuse to attach with `RegionForgotten`.
#[test]
fn attaching_a_forgotten_region_yields_region_forgotten() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let region_id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("s", AtomInput::new("fact", "x")).unwrap();

    let conn = Connection::open(&db).unwrap();
    let slot = eng
        .load_region_row(&conn, "s")
        .unwrap()
        .unwrap()
        .rsk_slot
        .unwrap();
    drop(conn);

    // Destroy only the key, leaving the region row intact.
    db.region_store_tombstone(
        slot,
        region_id as u64,
        db.region_store_slot(slot).unwrap().gen,
    )
    .unwrap();

    // A fresh engine (empty in-process cache) must refuse to attach the region.
    let eng2 = MemoryEngine::open(db).unwrap();
    let err = eng2
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap_err();
    assert!(
        matches!(err, MemError::RegionForgotten(_)),
        "attaching a region whose key was destroyed must yield RegionForgotten, got: {err}"
    );
}

#[test]
fn cached_create_revalidates_destroyed_region_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let region_id = eng
        .create_encrypted_region("cached-create", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let slot = eng
        .load_region_row(&conn, "cached-create")
        .unwrap()
        .unwrap()
        .rsk_slot
        .unwrap();
    drop(conn);

    db.region_store_tombstone(
        slot,
        region_id as u64,
        db.region_store_slot(slot).unwrap().gen,
    )
    .unwrap();
    let err = eng
        .create_encrypted_region("cached-create", Arc::new(MockEmbedder::new(8)))
        .unwrap_err();
    assert!(matches!(err, MemError::RegionForgotten(_)), "got {err}");
    assert!(
        matches!(
            eng.region_handle("cached-create"),
            Err(MemError::RegionNotAttached(_))
        ),
        "the persisted row remains, but the failed fast path must evict its dead handle"
    );
}

#[test]
fn cached_attach_revalidates_destroyed_region_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let owner = MemoryEngine::open(db.clone()).unwrap();
    let region_id = owner
        .create_encrypted_region("cached-attach", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let client = MemoryEngine::open(db.clone()).unwrap();
    client
        .attach_existing_region("cached-attach", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let slot = owner
        .load_region_row(&conn, "cached-attach")
        .unwrap()
        .unwrap()
        .rsk_slot
        .unwrap();
    drop(conn);

    db.region_store_tombstone(
        slot,
        region_id as u64,
        db.region_store_slot(slot).unwrap().gen,
    )
    .unwrap();
    let err = client
        .attach_existing_region("cached-attach", Arc::new(MockEmbedder::new(8)))
        .unwrap_err();
    assert!(matches!(err, MemError::RegionForgotten(_)), "got {err}");
    assert!(
        matches!(
            client.region_handle("cached-attach"),
            Err(MemError::RegionNotAttached(_))
        ),
        "the persisted row remains, but the failed fast path must evict its dead handle"
    );
}

/// Multi-tenant residue isolation: forgetting one region must not let an adversary
/// (full secrets) recover it via any SIBLING region's still-live key, and the
/// forgotten region's wrapped key leaves no residue in the sidecar.
#[test]
fn adversary_full_image_reconstruction_with_live_siblings() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("victim", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_encrypted_region("survivor", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let survivor_secret = "survivor: lunch is at noon";
    let victim_atom = eng
        .remember(
            "victim",
            AtomInput::new("fact", "victim: vault code 31-7-19"),
        )
        .unwrap();
    eng.remember("survivor", AtomInput::new("fact", survivor_secret))
        .unwrap();

    // Capture the adversary's view BEFORE forget: victim's sealed ciphertext, its slot,
    // and its 40-byte wrapped key.
    let vtable = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT sealed FROM {vtable} WHERE id = $1"),
            &[Value::Integer(victim_atom)],
        )
        .unwrap();
    let victim_sealed = match &qr.rows[0][0] {
        Value::Blob(b) => b.clone(),
        o => panic!("sealed not blob: {o:?}"),
    };
    let victim_slot = eng
        .load_region_row(&conn, "victim")
        .unwrap()
        .unwrap()
        .rsk_slot
        .unwrap();
    drop(conn);
    let victim_wrapped = db.region_store_slot(victim_slot).unwrap().wrapped;
    let sidecar_path = db.region_store_path();

    eng.drop_region("victim").unwrap();

    // drop_region deletes the victim's atom rows, it does not merely orphan them.
    let conn = Connection::open(&db).unwrap();
    let cnt = conn
        .query_params(
            &format!("SELECT COUNT(*) FROM {vtable} WHERE id = $1"),
            &[Value::Integer(victim_atom)],
        )
        .unwrap();
    assert_eq!(
        as_int(&cnt.rows[0][0]).unwrap(),
        0,
        "victim atom row is deleted by drop_region, not left orphaned"
    );
    drop(conn);

    // Adversary, post-forget: try EVERY slot's key against the captured victim blob.
    let mut recovered = false;
    for slot in 0..citadel_core::REGION_STORE_PREALLOC_SLOTS {
        let rec = db.region_store_slot(slot).unwrap();
        if rec.state != SlotState::Live {
            continue;
        }
        if let Ok(mut rck) = db.unwrap_region_key(&rec.wrapped) {
            let seal = derive_seal_keys(&rck);
            rck.zeroize();
            if blob_seal::open(&seal, victim_atom as u64, &victim_sealed).is_ok() {
                recovered = true;
            }
        }
    }
    assert!(
        !recovered,
        "no surviving sibling key opens the forgotten victim ciphertext"
    );

    // Residue carve: the victim's pre-forget wrapped key is absent from the sidecar.
    let sidecar = std::fs::read(&sidecar_path).unwrap();
    assert!(
        !byte_window(&sidecar, &victim_wrapped),
        "victim wrapped-key residue absent"
    );

    // Collateral-free: the sibling still recalls its secret.
    let hits = eng
        .recall("survivor", crate::RecallQuery::by_text(survivor_secret, 3))
        .unwrap();
    assert!(
        hits.iter().any(|h| h.text == survivor_secret),
        "survivor unaffected"
    );
}

/// Recycling a tombstoned slot for a new region must bind a FRESH key, surface none
/// of the old region's content, and leave the old ciphertext permanently unopenable.
#[test]
fn engine_slot_recycle_after_forget_isolates_old_region() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();

    eng.create_encrypted_region("r1", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let s1 = "r1 secret: the password is hunter2";
    let r1_atom = eng.remember("r1", AtomInput::new("fact", s1)).unwrap();

    let r1table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT sealed FROM {r1table} WHERE id = $1"),
            &[Value::Integer(r1_atom)],
        )
        .unwrap();
    let r1_sealed = match &qr.rows[0][0] {
        Value::Blob(b) => b.clone(),
        o => panic!("{o:?}"),
    };
    let r1_slot = eng
        .load_region_row(&conn, "r1")
        .unwrap()
        .unwrap()
        .rsk_slot
        .unwrap();
    drop(conn);
    let r1_wrapped = db.region_store_slot(r1_slot).unwrap().wrapped;

    eng.drop_region("r1").unwrap();
    // The slot is tombstoned and its key is gone.
    let rec = db.region_store_slot(r1_slot).unwrap();
    assert_eq!(rec.state, SlotState::Tombstone);
    assert!(db.unwrap_region_key(&rec.wrapped).is_err());
    let sidecar = std::fs::read(db.region_store_path()).unwrap();
    assert!(
        !byte_window(&sidecar, &r1_wrapped),
        "r1 wrapped key gone after forget"
    );

    // R2 recycles the slot with a brand-new key.
    eng.create_encrypted_region("r2", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let r2_slot = eng
        .load_region_row(&conn, "r2")
        .unwrap()
        .unwrap()
        .rsk_slot
        .unwrap();
    drop(conn);
    assert_eq!(r2_slot, r1_slot, "R2 recycled R1's freed slot");
    let s2 = "r2 secret: the meeting is tuesday";
    eng.remember("r2", AtomInput::new("fact", s2)).unwrap();

    // R2 works on the recycled slot and shows nothing of R1.
    assert!(eng
        .recall("r2", crate::RecallQuery::by_text(s2, 5))
        .unwrap()
        .iter()
        .any(|h| h.text == s2));
    assert!(!eng
        .recall("r2", crate::RecallQuery::by_text(s1, 5))
        .unwrap()
        .iter()
        .any(|h| h.text == s1));

    // R2's fresh key cannot open R1's captured old ciphertext.
    let rec2 = db.region_store_slot(r2_slot).unwrap();
    let mut rck2 = db.unwrap_region_key(&rec2.wrapped).unwrap();
    let seal2 = derive_seal_keys(&rck2);
    rck2.zeroize();
    assert!(
        blob_seal::open(&seal2, r1_atom as u64, &r1_sealed).is_err(),
        "the recycled region's key must not open the forgotten region's ciphertext"
    );
}

#[test]
fn reconcile_reclaims_orphan_live_slot_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    {
        let _eng = MemoryEngine::open(db.clone()).unwrap();
        let (slot, _gen) = db
            .region_store_allocate_write(4242, &[0x7u8; citadel_core::WRAPPED_KEY_SIZE])
            .unwrap();
        assert_eq!(slot, 0, "first allocation lands in slot 0");
        assert_eq!(db.region_store_slot(0).unwrap().state, SlotState::Live);
    }
    let _eng2 = MemoryEngine::open(db.clone()).unwrap();
    assert_eq!(
        db.region_store_slot(0).unwrap().state,
        SlotState::Tombstone,
        "orphan LIVE slot reclaimed on open"
    );
}

/// An atom key slot left LIVE by an interrupted insert (key fsync'd, row never
/// committed) is reclaimed on the next open, mirroring the region-store reconcile.
#[test]
fn reconcile_reclaims_orphan_atom_live_slot_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let slot = {
        let _eng = MemoryEngine::open(db.clone()).unwrap();
        // Orphan: a LIVE atom key slot with no referencing atom row.
        let (slot, _gen) = db
            .atom_store_allocate_write(999, &[0x7u8; citadel_core::WRAPPED_KEY_SIZE])
            .unwrap();
        assert_eq!(db.atom_store_slot(slot).unwrap().state, SlotState::Live);
        slot
    };
    let _eng2 = MemoryEngine::open(db.clone()).unwrap();
    assert_eq!(
        db.atom_store_slot(slot).unwrap().state,
        SlotState::Tombstone,
        "orphan LIVE atom slot reclaimed on open"
    );
}

#[test]
fn reconcile_with_no_live_atom_keys_removes_rows_edges_and_ann() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let region_id = eng
        .create_encrypted_region("all-dead", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("all-dead", AtomInput::new("fact", "alpha"))
        .unwrap();
    let b = eng
        .remember("all-dead", AtomInput::new("fact", "beta"))
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();
    eng.persist_ann_index("all-dead").unwrap();

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let seg_table = sealed_segment_table(&table, region_id);
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .query_params(
            &format!("SELECT id, key_slot, key_gen FROM {table} WHERE region_id = $1"),
            &[Value::Integer(region_id)],
        )
        .unwrap();
    let mut bindings: Vec<(u32, u64, u64)> = rows
        .rows
        .iter()
        .map(|row| {
            (
                as_int(&row[1]).unwrap() as u32,
                as_int(&row[0]).unwrap() as u64,
                as_int(&row[2]).unwrap() as u64,
            )
        })
        .collect();
    let (slot, generation, pseudo_id) = read_annseg_meta(&conn, region_id)
        .unwrap()
        .expect("sealed ANN metadata");
    bindings.push((slot, pseudo_id, generation));
    drop(conn);

    db.atom_store_tombstone_batch(&bindings).unwrap();
    assert!(
        db.atom_store_live_owners().unwrap().is_empty(),
        "fixture must exercise the zero-live-key reconciliation branch"
    );
    drop(eng);

    let _reopened = MemoryEngine::open(db.clone()).unwrap();
    let conn = Connection::open(&db).unwrap();
    let atoms = conn
        .query_params(
            &format!("SELECT COUNT(*) FROM {table} WHERE region_id = $1"),
            &[Value::Integer(region_id)],
        )
        .unwrap();
    assert_eq!(as_int(&atoms.rows[0][0]).unwrap(), 0);
    let edges = conn
        .query_params(
            "SELECT COUNT(*) FROM memory_edges WHERE src_id IN ($1, $2) OR dst_id IN ($1, $2)",
            &[Value::Integer(a), Value::Integer(b)],
        )
        .unwrap();
    assert_eq!(as_int(&edges.rows[0][0]).unwrap(), 0);
    let meta = conn
        .query_params(
            "SELECT COUNT(*) FROM memory_meta WHERE key LIKE 'annseg_%'",
            &[],
        )
        .unwrap();
    assert_eq!(as_int(&meta.rows[0][0]).unwrap(), 0);
    assert!(
        !db.manager()
            .list_tables()
            .unwrap()
            .iter()
            .any(|(name, _)| name.as_slice() == seg_table.as_bytes()),
        "reopen cleanup must drop the row-less sealed ANN chunk table"
    );
}

#[test]
fn failed_sealed_inserts_tombstone_pending_keys_before_return() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let region_id = eng
        .create_encrypted_region("rollback", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let seed = eng
        .remember("rollback", AtomInput::new("fact", "seed"))
        .unwrap();
    let baseline = db.atom_store_live_owners().unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute_params(
        "UPDATE memory_meta SET value = $1 WHERE key = 'next_atom_id'",
        &[Value::Integer(seed)],
    )
    .unwrap();
    drop(conn);

    let assert_rolled_back = || {
        assert_eq!(
            db.atom_store_live_owners().unwrap(),
            baseline,
            "a failed call must not leave a second live owner until reopen"
        );
        let conn = Connection::open(&db).unwrap();
        let table = atoms_table(8, EmbeddingMetric::Cosine, true);
        let count = conn
            .query_params(
                &format!("SELECT COUNT(*) FROM {table} WHERE region_id = $1"),
                &[Value::Integer(region_id)],
            )
            .unwrap();
        assert_eq!(as_int(&count.rows[0][0]).unwrap(), 1);
        let next = conn
            .query_params(
                "SELECT value FROM memory_meta WHERE key = 'next_atom_id'",
                &[],
            )
            .unwrap();
        assert_eq!(as_int(&next.rows[0][0]).unwrap(), seed);
    };

    assert!(eng
        .remember("rollback", AtomInput::new("fact", "single failure"))
        .is_err());
    assert_rolled_back();

    assert!(eng
        .remember_if_absent(
            "rollback",
            AtomInput::new("fact", "if-absent failure"),
            &[],
            None,
        )
        .is_err());
    assert_rolled_back();

    assert!(eng
        .remember_batch(
            "rollback",
            vec![
                AtomInput::new("fact", "batch failure one"),
                AtomInput::new("fact", "batch failure two"),
                AtomInput::new("fact", "batch failure three"),
            ],
        )
        .is_err());
    assert_rolled_back();
    assert!(
        eng.fetch_one("rollback", seed).unwrap().is_some(),
        "the pre-existing slot with the duplicate owner remains live"
    );
}

#[test]
fn failed_encrypted_region_insert_tombstones_pending_region_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();

    FAILED_ENCRYPTED_REGION_WRAPPED_KEY.with(|captured| {
        *captured.borrow_mut() = None;
    });
    FAIL_ENCRYPTED_REGION_AFTER_SLOT.with(|fail| fail.set(true));
    let err = eng
        .create_encrypted_region("region-rollback", Arc::new(MockEmbedder::new(8)))
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("injected encrypted-region failure after key-slot allocation"),
        "wrong injected failure: {err}"
    );

    let wrapped = FAILED_ENCRYPTED_REGION_WRAPPED_KEY
        .with(|captured| captured.borrow_mut().take())
        .expect("fault point records the durable wrapped key for residue inspection");
    assert!(
        db.region_store_live_owners().unwrap().is_empty(),
        "the failed call must leave no live region-key owner"
    );
    let record = db.region_store_slot(0).unwrap();
    assert_eq!(record.state, SlotState::Tombstone);
    assert_eq!(record.wrapped, [0u8; WRAPPED_KEY_SIZE]);
    let sidecar = std::fs::read(db.region_store_path()).unwrap();
    assert!(
        !sidecar
            .windows(wrapped.len())
            .any(|window| window == wrapped.as_slice()),
        "both physical slot copies must be scrubbed before failure returns"
    );

    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .query_params(
            "SELECT COUNT(*) FROM memory_regions WHERE name = $1",
            &[Value::Text("region-rollback".into())],
        )
        .unwrap();
    assert_eq!(as_int(&rows.rows[0][0]).unwrap(), 0);
    drop(conn);

    // The raw RCK stays unexposed: Zeroizing ownership is the structural guarantee.
    eng.create_encrypted_region("region-rollback", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert_eq!(db.region_store_live_owners().unwrap().len(), 1);
}

#[test]
fn failed_derived_link_rolls_back_inserted_row_and_pending_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let region_id = eng
        .create_encrypted_region("derived-rollback", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let seed = eng
        .remember("derived-rollback", AtomInput::new("fact", "seed"))
        .unwrap();
    let baseline = db.atom_store_live_owners().unwrap();

    // Fail only the provenance INSERT, after the ACK and atom row are written.
    let conn = Connection::open(&db).unwrap();
    conn.execute("DROP TABLE memory_edges").unwrap();
    conn.execute(
        "CREATE TABLE memory_edges (\
         src_id INTEGER NOT NULL, dst_id INTEGER NOT NULL, kind TEXT NOT NULL, \
         weight REAL DEFAULT 1.0, PRIMARY KEY (src_id, dst_id, kind))",
    )
    .unwrap();
    drop(conn);

    assert!(eng
        .remember_derived(
            "derived-rollback",
            AtomInput::new("fact", "must roll back"),
            &[seed],
            Some(serde_json::json!({"quote": "seed"})),
        )
        .is_err());
    assert_eq!(db.atom_store_live_owners().unwrap(), baseline);
    let conn = Connection::open(&db).unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let count = conn
        .query_params(
            &format!("SELECT COUNT(*) FROM {table} WHERE region_id = $1"),
            &[Value::Integer(region_id)],
        )
        .unwrap();
    assert_eq!(
        as_int(&count.rows[0][0]).unwrap(),
        1,
        "the atom row shares the failed provenance transaction"
    );
}

#[test]
fn failed_sealed_ann_persist_cleans_key_chunks_and_metadata_before_return() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let region_id = eng
        .create_encrypted_region("ann-rollback", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("ann-rollback", AtomInput::new("fact", "alpha"))
        .unwrap();
    eng.remember("ann-rollback", AtomInput::new("fact", "beta"))
        .unwrap();
    let baseline = db.atom_store_live_owners().unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let seg_table = sealed_segment_table(&table, region_id);

    FAIL_SEALED_SEGMENT_AFTER_CHUNKS.with(|f| f.set(true));
    let err = eng.persist_ann_index("ann-rollback").unwrap_err();
    assert!(
        err.to_string().contains("after chunk commit"),
        "wrong injected failure: {err}"
    );
    assert_eq!(
        db.atom_store_live_owners().unwrap(),
        baseline,
        "the unpublished pseudo-key is tombstoned in the failing call"
    );
    let conn = Connection::open(&db).unwrap();
    assert!(read_annseg_meta(&conn, region_id).unwrap().is_none());
    assert!(
        !db.manager()
            .list_tables()
            .unwrap()
            .iter()
            .any(|(name, _)| name.as_slice() == seg_table.as_bytes()),
        "committed chunks are removed before returning the injected error"
    );
    drop(conn);

    eng.persist_ann_index("ann-rollback")
        .expect("a retry after rollback publishes a fresh segment");
}

#[test]
fn cancelled_sealed_decode_is_not_healed_as_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("ann-decode-cancel", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom_id = eng
        .remember(
            "ann-decode-cancel",
            AtomInput::new("fact", "survives a cancelled decode").with_embedding(unit(8, 0)),
        )
        .unwrap();
    let info = eng.persist_ann_index("ann-decode-cancel").unwrap();
    let conn = Connection::open(&db).unwrap();
    let binding = read_annseg_meta(&conn, region_id).unwrap().unwrap();
    drop(conn);
    drop(eng);

    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("ann-decode-cancel", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert!(eng.ann_cache_status("ann-decode-cancel").unwrap().is_none());

    // The hook lands immediately before the segment decoder. It also clears
    // the database token, proving the load observes its retained snapshot.
    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.recall(
        "ann-decode-cancel",
        RecallQuery::by_embedding(unit(8, 0), 1),
    ));
    assert!(db.cancel_token().is_none());

    let (segment_slot, segment_gen, pseudo_id) = binding;
    let record = db.atom_store_slot(segment_slot).unwrap();
    assert_eq!(record.state, SlotState::Live, "cancel must not retire key");
    assert_eq!(record.gen, segment_gen);
    assert_eq!(record.region_id, pseudo_id);
    assert!(seg_chunks_exist(&db, region_id, 8));
    let conn = Connection::open(&db).unwrap();
    assert_eq!(read_annseg_meta(&conn, region_id).unwrap(), Some(binding));
    drop(conn);
    assert!(eng.ann_cache_status("ann-decode-cancel").unwrap().is_none());

    let hits = eng
        .recall(
            "ann-decode-cancel",
            RecallQuery::by_embedding(unit(8, 0), 1),
        )
        .expect("the intact segment loads after cancellation is cleared");
    assert_eq!(hits[0].id, atom_id);
    match eng.ann_cache_status("ann-decode-cancel").unwrap() {
        Some(AnnIndexSource::Loaded { segment_b3 }) => assert_eq!(segment_b3, info.segment_b3),
        other => panic!("expected the same segment to load, got {other:?}"),
    }
}

#[test]
fn sealed_segment_kind_dictionary_must_match_attribute_codes() {
    fn inner(kinds: &[(&str, u32)], attribute_code: u32) -> Vec<u8> {
        let index = AnnIndex::build_with_attrs(
            vec![(7, vec![1.0, 0.0], vec![attribute_code])],
            1,
            Metric::L2,
            2,
        )
        .unwrap();
        let body = citadel_vector::segment::encode(&index);
        let mut inner = vec![0; 64];
        inner.extend_from_slice(&(kinds.len() as u32).to_le_bytes());
        for (kind, code) in kinds {
            inner.extend_from_slice(&(kind.len() as u32).to_le_bytes());
            inner.extend_from_slice(kind.as_bytes());
            inner.extend_from_slice(&code.to_le_bytes());
        }
        inner.extend_from_slice(&body);
        inner
    }

    assert!(parse_sealed_segment(&inner(&[("fact", 0)], 0), None)
        .unwrap()
        .is_some());
    assert!(parse_sealed_segment(&inner(&[], 0), None)
        .unwrap()
        .is_none());
    assert!(parse_sealed_segment(&inner(&[("fact", 1)], 0), None)
        .unwrap()
        .is_none());
    assert!(
        parse_sealed_segment(&inner(&[("fact", 0), ("fact", 1)], 0), None)
            .unwrap()
            .is_none()
    );
    assert!(parse_sealed_segment(&inner(&[("fact", 0)], 1), None)
        .unwrap()
        .is_none());
}

#[test]
fn reembed_ann_rebuild_does_not_swallow_the_database_cancel_token() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_region("ann-cancel", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("ann-cancel", AtomInput::new("fact", "alpha"))
        .unwrap();

    let token = citadel_core::CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));
    let err = eng
        .rebuild_region_ann(&atoms_table(8, EmbeddingMetric::Cosine, false), false, None)
        .expect_err("the Database token must stop the best-effort ANN rebuild");
    assert!(matches!(
        err,
        MemError::Core(citadel_core::Error::Interrupted)
    ));
    db.set_cancel(None);
}

/// Deleting an atom invalidates the cached PRISM index so it is not re-ranked later.
/// (`region_handle` shares the cached `ann` Arc, so we can observe the cache here.)
#[test]
fn delete_atoms_invalidates_ann_cache() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("s", AtomInput::new("fact", "alpha one"))
        .unwrap();
    eng.remember("s", AtomInput::new("fact", "beta two"))
        .unwrap();

    // First recall builds and caches the ephemeral index.
    eng.recall("s", RecallQuery::by_text("alpha one", 2))
        .unwrap();
    assert!(
        eng.region_handle("s")
            .unwrap()
            .ann
            .read()
            .unwrap()
            .is_some(),
        "the first sealed recall caches a PRISM index"
    );

    eng.delete_atoms("s", &[a]).unwrap();
    assert!(
        eng.region_handle("s")
            .unwrap()
            .ann
            .read()
            .unwrap()
            .is_none(),
        "delete_atoms invalidates the cached index so erased atoms are not re-ranked"
    );
}

#[test]
fn maintenance_forget_invalidates_ann_cache_in_a_live_engine() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("maintenance-cache", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = engine
        .remember(
            "maintenance-cache",
            AtomInput::new("fact", "erase this cached atom"),
        )
        .unwrap();

    engine
        .recall("maintenance-cache", RecallQuery::by_text("cached atom", 1))
        .unwrap();
    let cache = engine.region_handle("maintenance-cache").unwrap().ann;
    assert!(cache.read().unwrap().is_some());

    MemoryMaintenance::open(Arc::clone(&db))
        .unwrap()
        .forget_atoms("maintenance-cache", &[atom], false)
        .unwrap();

    assert!(
        cache.read().unwrap().is_none(),
        "model-free erasure left decrypted vectors in another engine's ANN cache"
    );
}

/// Dropping an encrypted region reclaims its atoms' key slots (tombstones +
/// frees them), rather than leaking them LIVE-but-dead in the atom key store.
#[test]
fn drop_region_reclaims_atom_key_slots() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("s", AtomInput::new("fact", "one")).unwrap();
    let b = eng.remember("s", AtomInput::new("fact", "two")).unwrap();

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let slot_of = |id: i64| -> u32 {
        let qr = conn
            .query_params(
                &format!("SELECT key_slot FROM {table} WHERE id = $1"),
                &[Value::Integer(id)],
            )
            .unwrap();
        as_int(&qr.rows[0][0]).unwrap() as u32
    };
    let (sa, sb) = (slot_of(a), slot_of(b));
    drop(conn);
    assert_eq!(db.atom_store_slot(sa).unwrap().state, SlotState::Live);
    assert_eq!(db.atom_store_slot(sb).unwrap().state, SlotState::Live);

    eng.drop_region("s").unwrap();

    assert_eq!(
        db.atom_store_slot(sa).unwrap().state,
        SlotState::Tombstone,
        "atom a's key slot is reclaimed, not leaked LIVE"
    );
    assert_eq!(
        db.atom_store_slot(sb).unwrap().state,
        SlotState::Tombstone,
        "atom b's key slot is reclaimed, not leaked LIVE"
    );
    assert!(
        db.atom_store_live_wrapped().unwrap().is_empty(),
        "no live atom keys remain after drop_region"
    );
}

/// The `_enc` table holds only the opaque `sealed` blob - no plaintext content column.
#[test]
fn enc_table_has_no_plaintext_content_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("enc", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("plain", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let enc = atoms_table(8, EmbeddingMetric::Cosine, true);
    let plain = atoms_table(8, EmbeddingMetric::Cosine, false);
    let conn = Connection::open(&db).unwrap();
    let columns = |t: &str| -> Vec<String> {
        conn.table_schema(t)
            .expect("table exists")
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect()
    };
    let has = |cols: &[String], c: &str| cols.iter().any(|n| n == c);
    let enc_cols = columns(&enc);
    let plain_cols = columns(&plain);
    assert!(
        has(&enc_cols, "sealed"),
        "sealed column present on the _enc table"
    );
    assert!(
        !has(&enc_cols, "text_content"),
        "_enc must NOT have text_content"
    );
    assert!(!has(&enc_cols, "payload"), "_enc must NOT have payload");
    assert!(!has(&enc_cols, "embedding"), "_enc must NOT have embedding");
    assert!(
        has(&plain_cols, "text_content"),
        "plaintext table keeps text_content"
    );
    assert!(
        has(&plain_cols, "embedding"),
        "plaintext table keeps embedding"
    );
    assert!(has(&plain_cols, "payload"), "plaintext table keeps payload");
}

#[test]
fn open_rejects_incompatible_memory_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    {
        let conn = Connection::open(&db).unwrap();
        let res = conn.execute_script(
            "CREATE TABLE memory_meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);\
             CREATE TABLE memory_regions (\
             id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, embedding_dim INTEGER NOT NULL,\
             embedding_metric TEXT NOT NULL, model_id TEXT NOT NULL, kek_epoch INTEGER NOT NULL,\
             created_at TIMESTAMP NOT NULL, metadata JSONB);",
        );
        assert!(res.error.is_none(), "schema setup failed: {:?}", res.error);
    }
    match MemoryEngine::open(db) {
        Ok(_) => panic!("incompatible schema must be rejected, but open succeeded"),
        Err(MemError::Invalid(m)) => {
            assert!(
                m.contains("incompatible memory schema"),
                "wrong rejection message: {m}"
            )
        }
        Err(e) => panic!("expected MemError::Invalid, got: {e:?}"),
    }
}

#[test]
fn json_contains_edge_cases() {
    use serde_json::json;
    // nested-object subset
    assert!(json_contains(
        &json!({"a": {"b": 1, "c": 2}}),
        &json!({"a": {"b": 1}})
    ));
    assert!(!json_contains(
        &json!({"a": {"b": 1}}),
        &json!({"a": {"b": 2}})
    ));
    // scalar contained in array
    assert!(json_contains(&json!([1, 2, 3]), &json!(2)));
    assert!(!json_contains(&json!([1, 2, 3]), &json!(9)));
    // order-independent array subset
    assert!(json_contains(&json!([1, 2, 3]), &json!([3, 1])));
    assert!(!json_contains(&json!([1, 2, 3]), &json!([3, 9])));
    // array of objects
    assert!(json_contains(
        &json!([{"x": 1}, {"y": 2}]),
        &json!([{"x": 1}])
    ));
    // empty needle is vacuously contained
    assert!(json_contains(&json!({"a": 1}), &json!({})));
    assert!(json_contains(&json!([1, 2]), &json!([])));
    // type mismatch and scalar equality
    assert!(!json_contains(&json!({"a": 1}), &json!([1])));
    assert!(json_contains(&json!("x"), &json!("x")));
    assert!(!json_contains(&json!("x"), &json!("y")));
}

#[test]
fn encode_decode_atom_blob_roundtrip_and_malformed() {
    let emb = vec![1.5f32, -2.0, 3.25];
    let blob = encode_atom_blob(&emb, "hello text", "{\"k\":1}");
    let (e, t, p) = decode_atom_blob(&blob).unwrap();
    assert_eq!(e, emb);
    assert_eq!(t, "hello text");
    assert_eq!(p, "{\"k\":1}");

    // empty everything round-trips
    let b0 = encode_atom_blob(&[], "", "");
    let (e0, t0, p0) = decode_atom_blob(&b0).unwrap();
    assert!(e0.is_empty() && t0.is_empty() && p0.is_empty());

    // malformed inputs return Err and never panic / over-allocate
    assert!(decode_atom_blob(&blob[..blob.len() - 1]).is_err());
    assert!(decode_atom_blob(&[]).is_err());
    assert!(decode_atom_blob(&[0x01]).is_err()); // partial dim header

    // invalid UTF-8 text
    let mut bad = Vec::new();
    bad.extend_from_slice(&0u16.to_le_bytes()); // dim 0
    bad.extend_from_slice(&2u32.to_le_bytes()); // text len 2
    bad.extend_from_slice(&[0xff, 0xff]); // not UTF-8
    bad.extend_from_slice(&0u32.to_le_bytes()); // payload len 0
    assert!(decode_atom_blob(&bad).is_err());

    // a huge length prefix is rejected by bounds check, not by a 4 GiB allocation
    let mut huge = Vec::new();
    huge.extend_from_slice(&0u16.to_le_bytes());
    huge.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
    huge.extend_from_slice(&[1, 2, 3]);
    assert!(decode_atom_blob(&huge).is_err());
}

#[test]
fn vector_only_atom_decoder_never_parses_text_or_payload() {
    let expected = vec![1.25f32, -9.5, 0.0];
    let mut blob = encode_atom_blob(&expected, "x", "y");
    let text_offset = 2 + expected.len() * 4 + 4;
    blob[text_offset] = 0xff;
    let payload_offset = text_offset + 1 + 4;
    blob[payload_offset] = 0xfe;

    assert!(
        decode_atom_blob(&blob).is_err(),
        "the full decoder rejects the deliberately non-UTF8 fields"
    );
    assert_eq!(
        decode_atom_embedding(&blob).unwrap(),
        expected,
        "the exact-vector path validates field framing without allocating strings"
    );

    blob.push(0);
    assert!(
        decode_atom_embedding(&blob).is_err(),
        "structural validation still rejects trailing plaintext"
    );
    assert!(decode_atom_embedding(&blob[..blob.len() - 2]).is_err());
}

#[test]
fn mmr_vector_open_validates_dimension_before_allocating_and_charges_the_copy() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    let atom_wrap = derive_atom_wrap_key(&[7u8; citadel_core::KEY_SIZE]);

    let (wrong, wrapped) = seal_atom(&atom_wrap, 41, &[1.0; 16], "text", "{}");
    let error = eng
        .with_read_limits(
            MemoryReadLimits::new(usize::MAX, wrong.len(), usize::MAX),
            |_| open_mmr_embedding(&atom_wrap, &wrapped, 41, &wrong, 8),
        )
        .unwrap_err();
    assert!(
        matches!(error, MemError::Invalid(_)) && error.to_string().contains("dim 16"),
        "the authenticated dimension must be rejected before a vector allocation: {error}"
    );

    let (valid, wrapped) = seal_atom(&atom_wrap, 42, &[1.0; 8], "text", "{}");
    let error = eng
        .with_read_limits(
            MemoryReadLimits::new(usize::MAX, valid.len() + 8 * 4 - 1, usize::MAX),
            |_| open_mmr_embedding(&atom_wrap, &wrapped, 42, &valid, 8),
        )
        .unwrap_err();
    assert!(
        matches!(error, MemError::ReadLimitExceeded { .. }),
        "the decoded vector allocation must be charged: {error}"
    );
}

#[test]
fn cached_mmr_redundancy_matches_a_recomputing_reference() {
    fn reference(query: &[f32], candidates: &[&[f32]], k: usize, lambda: f32) -> Vec<usize> {
        let take = k.min(candidates.len());
        if take == 0 {
            return Vec::new();
        }
        let first = maximal_marginal_relevance(query, candidates, 1, lambda, None).unwrap()[0];
        let query_scores = candidates
            .iter()
            .map(|candidate| cosine_similarity(query, candidate, None).unwrap())
            .collect::<Vec<_>>();
        let mut selected = vec![first];
        while selected.len() < take {
            let mut best = None;
            for (index, &query_score) in query_scores.iter().enumerate() {
                if selected.contains(&index) {
                    continue;
                }
                let redundancy = selected
                    .iter()
                    .map(|&chosen| {
                        cosine_similarity(candidates[index], candidates[chosen], None).unwrap()
                    })
                    .fold(f64::NEG_INFINITY, f64::max);
                let score =
                    f64::from(lambda) * query_score - (1.0 - f64::from(lambda)) * redundancy;
                if best.is_none_or(|(_, best_score)| score > best_score) {
                    best = Some((index, score));
                }
            }
            selected.push(best.expect("an unselected reference candidate remains").0);
        }
        selected
    }

    let query = [1.0, 0.1, -0.2];
    let owned = [
        [0.95, 0.05, -0.1],
        [0.8, 0.4, -0.2],
        [0.1, 1.0, 0.3],
        [-0.4, 0.2, 1.0],
        [0.3, -0.8, 0.5],
    ];
    let candidates = owned
        .iter()
        .map(|candidate| candidate.as_slice())
        .collect::<Vec<_>>();
    for lambda in [0.0, 0.25, 0.5, 0.9, 1.0] {
        assert_eq!(
            maximal_marginal_relevance(&query, &candidates, 4, lambda, None).unwrap(),
            reference(&query, &candidates, 4, lambda),
            "cached redundancy diverged at lambda={lambda}"
        );
    }
}

#[test]
fn text_only_atom_decoder_never_materializes_payload() {
    let embedding = [2.0f32, -4.0];
    let text = "dedup needle";
    let mut blob = encode_atom_blob(&embedding, text, "x");
    let payload_offset = 2 + embedding.len() * 4 + 4 + text.len() + 4;
    blob[payload_offset] = 0xff;

    assert!(
        decode_atom_blob(&blob).is_err(),
        "the full decoder rejects the deliberately non-UTF8 payload"
    );
    assert_eq!(
        decode_atom_text(&blob).unwrap(),
        text,
        "exact-text dedup validates payload framing without copying or parsing it"
    );
}

#[test]
fn vec_distance_matches_sql_metrics() {
    // L2 = sqrt(sum sq): [3,4] vs [0,0] -> 5
    assert!(
        (vec_distance(EmbeddingMetric::L2, &[3.0, 4.0], &[0.0, 0.0]).unwrap() - 5.0).abs() < 1e-5
    );
    // Inner = -dot: -([1,2].[3,4]) = -11
    assert!(
        (vec_distance(EmbeddingMetric::InnerProduct, &[1.0, 2.0], &[3.0, 4.0]).unwrap() - (-11.0))
            .abs()
            < 1e-5
    );
    // Cosine: identical -> 0, orthogonal -> 1
    assert!(
        vec_distance(EmbeddingMetric::Cosine, &[1.0, 0.0], &[1.0, 0.0])
            .unwrap()
            .abs()
            < 1e-6
    );
    assert!(
        (vec_distance(EmbeddingMetric::Cosine, &[1.0, 0.0], &[0.0, 1.0]).unwrap() - 1.0).abs()
            < 1e-6
    );
    assert_eq!(
        vec_distance(EmbeddingMetric::Cosine, &[3.0, 4.0], &[0.0, 0.0]),
        None,
        "cosine distance is undefined for a zero-norm vector"
    );
}

/// `search_sealed_index` sorts AnnIndex distances together with `vec_distance`
/// tail scores, so the two must agree unit-for-unit on every metric.
#[test]
fn ann_index_distances_match_vec_distance_for_all_metrics() {
    let rows: Vec<(u64, Vec<f32>)> = vec![
        (1, vec![1.0, 2.0, 3.0, 4.0]),
        (2, vec![-3.0, 0.5, 2.0, -1.0]),
        (3, vec![0.1, 0.2, 0.3, 0.4]),
    ];
    let q = [0.5f32, -1.0, 2.0, 0.25];
    for m in [
        EmbeddingMetric::L2,
        EmbeddingMetric::InnerProduct,
        EmbeddingMetric::Cosine,
    ] {
        let idx = AnnIndex::build(rows.clone(), ann_metric(m), 4).unwrap();
        let hits = idx.search(&q, rows.len()).expect("search");
        assert_eq!(hits.len(), rows.len());
        for (rid, d) in hits {
            let v = &rows.iter().find(|(id, _)| *id == rid).unwrap().1;
            let exact = vec_distance(m, &q, v).expect("fixture vectors have nonzero norms");
            assert!(
                (d - exact).abs() < 1e-4,
                "{m:?} row {rid}: index dist {d} vs exact {exact}"
            );
        }
    }
}

/// True if `needle` occurs as a contiguous window of `hay`.
fn byte_window(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

fn set_atom_age_and_access(
    db: &Arc<Database>,
    dim: u16,
    region_id: i64,
    atom_id: AtomId,
    created_micros: i64,
    access_count: i64,
) {
    let table = atoms_table(dim, EmbeddingMetric::Cosine, false);
    let conn = citadel_sql::Connection::open(db).unwrap();
    conn.execute_params(
        &format!(
            "UPDATE {table} SET created_at = $1, access_count = $2 \
             WHERE id = $3 AND region_id = $4"
        ),
        &[
            citadel_sql::Value::Timestamp(created_micros),
            citadel_sql::Value::Integer(access_count),
            citadel_sql::Value::Integer(atom_id),
            citadel_sql::Value::Integer(region_id),
        ],
    )
    .unwrap();
}

fn micros_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

#[test]
fn evolve_importance_pins_recency_decay_and_access_boost() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let region_id = eng
        .create_region("ev", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("ev", AtomInput::new("note", "alpha")).unwrap();

    let age_micros = 30i64 * 86_400 * 1_000_000;
    set_atom_age_and_access(&db, 8, region_id, a, micros_now() - age_micros, 19);

    let report = eng.evolve("ev", a, 0, 10.0).unwrap();
    assert_eq!(report.links_added, 0, "self is filtered, no neighbors");

    let recency = 0.5f32;
    let expected = recency * (1.0 + (19f32).ln_1p());
    assert!(
        (report.importance - expected).abs() < 1e-3,
        "importance {} should equal recency*ln1p boost {}",
        report.importance,
        expected
    );
    assert!(
        (expected - 1.997_86).abs() < 1e-2,
        "sanity: expected near 1.9979, got {expected}"
    );
}

#[test]
fn evolve_importance_zero_age_no_access_is_unity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let region_id = eng
        .create_region("ev0", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("ev0", AtomInput::new("note", "alpha"))
        .unwrap();

    set_atom_age_and_access(&db, 8, region_id, a, micros_now(), 0);
    let report = eng.evolve("ev0", a, 0, 10.0).unwrap();
    assert!(
        (report.importance - 1.0).abs() < 1e-4,
        "fresh, never-accessed atom has importance 1.0, got {}",
        report.importance
    );
}

fn atom_created_at(db: &Arc<Database>, table: &str, atom_id: AtomId) -> i64 {
    let conn = citadel_sql::Connection::open(db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT created_at FROM {table} WHERE id = $1"),
            &[citadel_sql::Value::Integer(atom_id)],
        )
        .unwrap();
    match qr.rows[0][0] {
        citadel_sql::Value::Timestamp(t) => t,
        ref v => panic!("created_at is not a timestamp: {v:?}"),
    }
}

#[test]
fn created_at_override_stores_event_time_on_the_plaintext_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_region("ev-t", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, false);

    let start = micros_now();
    let event = start - 90 * 86_400 * 1_000_000;
    let backdated = eng
        .remember(
            "ev-t",
            AtomInput::new("note", "alpha").with_created_at(event),
        )
        .unwrap();
    let fresh = eng
        .remember("ev-t", AtomInput::new("note", "beta"))
        .unwrap();
    let batched = eng
        .remember_batch(
            "ev-t",
            vec![AtomInput::new("note", "gamma").with_created_at(event + 1)],
        )
        .unwrap()[0];

    assert_eq!(atom_created_at(&db, &table, backdated), event);
    assert_eq!(atom_created_at(&db, &table, batched), event + 1);
    assert!(
        atom_created_at(&db, &table, fresh) >= start,
        "no override falls back to the ingest clock"
    );
    let hit = eng.fetch_one("ev-t", backdated).unwrap().unwrap();
    assert_eq!(
        hit.created_at, event,
        "fetch surfaces the stored event time"
    );
}

#[test]
fn created_at_override_stores_event_time_on_the_sealed_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("ev-s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);

    let event = micros_now() - 90 * 86_400 * 1_000_000;
    let single = eng
        .remember(
            "ev-s",
            AtomInput::new("note", "alpha").with_created_at(event),
        )
        .unwrap();
    let batched = eng
        .remember_batch(
            "ev-s",
            vec![AtomInput::new("note", "beta").with_created_at(event + 1)],
        )
        .unwrap()[0];

    assert_eq!(atom_created_at(&db, &table, single), event);
    assert_eq!(atom_created_at(&db, &table, batched), event + 1);
    let hit = eng.fetch_one("ev-s", single).unwrap().unwrap();
    assert_eq!(
        hit.created_at, event,
        "sealed fetch surfaces the event time"
    );
}

/// Counts which embedding side each call lands on (vectors come from the mock).
struct SideCountingEmbedder {
    inner: MockEmbedder,
    passages: std::sync::atomic::AtomicUsize,
    queries: std::sync::atomic::AtomicUsize,
}

struct BlockingQueryEmbedder {
    inner: MockEmbedder,
    armed: Arc<std::sync::atomic::AtomicBool>,
    entered: Arc<std::sync::atomic::AtomicBool>,
    release: Arc<std::sync::atomic::AtomicBool>,
}

struct ModelEmbedder {
    inner: MockEmbedder,
    model: &'static str,
}

impl Embedder for ModelEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }

    fn model_id(&self) -> &str {
        self.model
    }

    fn embed_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::EmbedError> {
        self.inner.embed_with_cancel(texts, cancel)
    }
}

impl Embedder for BlockingQueryEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }

    fn model_id(&self) -> &str {
        "blocking-query"
    }

    fn embed_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::EmbedError> {
        self.inner.embed_with_cancel(texts, cancel)
    }

    fn embed_queries_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::EmbedError> {
        use std::sync::atomic::Ordering;

        if self.armed.load(Ordering::SeqCst) {
            self.entered.store(true, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                if cancel.is_some_and(citadel_core::CancelToken::is_cancelled) {
                    return Err(crate::EmbedError::Interrupted);
                }
                std::thread::yield_now();
            }
        }
        self.inner.embed_with_cancel(texts, cancel)
    }
}

impl Embedder for SideCountingEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }

    fn model_id(&self) -> &str {
        "side-counting"
    }

    fn embed_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::EmbedError> {
        self.passages.fetch_add(texts.len(), Ordering::Relaxed);
        self.inner.embed_with_cancel(texts, cancel)
    }

    fn embed_queries_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::EmbedError> {
        self.queries.fetch_add(texts.len(), Ordering::Relaxed);
        self.inner.embed_with_cancel(texts, cancel)
    }
}

struct CancelAndClearEmbedder {
    inner: MockEmbedder,
    db: Arc<Database>,
    token: citadel_core::CancelToken,
    passages: std::sync::atomic::AtomicUsize,
    queries: std::sync::atomic::AtomicUsize,
}

impl Embedder for CancelAndClearEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }

    fn model_id(&self) -> &str {
        "cancel-and-clear"
    }

    fn embed_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::EmbedError> {
        self.passages.fetch_add(texts.len(), Ordering::Relaxed);
        let result = self.inner.embed_with_cancel(texts, cancel);
        self.token.cancel();
        self.db.set_cancel(None);
        result
    }

    fn embed_queries_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::EmbedError> {
        self.queries.fetch_add(texts.len(), Ordering::Relaxed);
        let result = self.inner.embed_with_cancel(texts, cancel);
        self.token.cancel();
        self.db.set_cancel(None);
        result
    }
}

struct CancelAndClearReranker {
    db: Arc<Database>,
    token: citadel_core::CancelToken,
    calls: std::sync::atomic::AtomicUsize,
}

struct BlockingReranker {
    entered: Arc<std::sync::atomic::AtomicBool>,
    release: Arc<std::sync::atomic::AtomicBool>,
    panic_after_release: bool,
}

struct CooperativeCancelEmbedder {
    inner: MockEmbedder,
    passage_calls: std::sync::atomic::AtomicUsize,
    query_calls: std::sync::atomic::AtomicUsize,
}

impl CooperativeCancelEmbedder {
    fn wait_for_cancel(
        calls: &std::sync::atomic::AtomicUsize,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::EmbedError> {
        use std::sync::atomic::Ordering;
        use std::time::{Duration, Instant};

        calls.fetch_add(1, Ordering::SeqCst);
        let Some(cancel) = cancel else {
            return Err(crate::EmbedError::Backend(
                "engine omitted the model cancellation token".into(),
            ));
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !cancel.is_cancelled() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        if cancel.is_cancelled() {
            Err(crate::EmbedError::Interrupted)
        } else {
            Err(crate::EmbedError::Backend(
                "timed out waiting for cooperative cancellation".into(),
            ))
        }
    }
}

impl Embedder for CooperativeCancelEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }

    fn model_id(&self) -> &str {
        "cooperative-cancel"
    }

    fn embed_with_cancel(
        &self,
        _texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::EmbedError> {
        Self::wait_for_cancel(&self.passage_calls, cancel)
    }

    fn embed_queries_with_cancel(
        &self,
        _texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::EmbedError> {
        Self::wait_for_cancel(&self.query_calls, cancel)
    }
}

struct CooperativeCancelReranker {
    calls: std::sync::atomic::AtomicUsize,
}

impl Reranker for CooperativeCancelReranker {
    fn model_id(&self) -> &str {
        "cooperative-cancel"
    }

    fn rerank_with_cancel(
        &self,
        _query: &str,
        _passages: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<f32>, crate::EmbedError> {
        use std::sync::atomic::Ordering;
        use std::time::{Duration, Instant};

        self.calls.fetch_add(1, Ordering::SeqCst);
        let Some(cancel) = cancel else {
            return Err(crate::EmbedError::Backend(
                "engine omitted the reranker cancellation token".into(),
            ));
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !cancel.is_cancelled() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        if cancel.is_cancelled() {
            Err(crate::EmbedError::Interrupted)
        } else {
            Err(crate::EmbedError::Backend(
                "timed out waiting for cooperative reranker cancellation".into(),
            ))
        }
    }
}

impl Reranker for BlockingReranker {
    fn model_id(&self) -> &str {
        "blocking-reranker"
    }

    fn rerank_with_cancel(
        &self,
        _query: &str,
        passages: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<f32>, crate::EmbedError> {
        use std::sync::atomic::Ordering;

        self.entered.store(true, Ordering::SeqCst);
        while !self.release.load(Ordering::SeqCst) {
            if cancel.is_some_and(citadel_core::CancelToken::is_cancelled) {
                return Err(crate::EmbedError::Interrupted);
            }
            std::thread::yield_now();
        }
        assert!(!self.panic_after_release, "injected reranker panic");
        Ok(vec![1.0; passages.len()])
    }
}

impl Reranker for CancelAndClearReranker {
    fn model_id(&self) -> &str {
        "cancel-and-clear-reranker"
    }

    fn rerank_with_cancel(
        &self,
        _query: &str,
        passages: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<f32>, crate::EmbedError> {
        if cancel.is_some_and(citadel_core::CancelToken::is_cancelled) {
            return Err(crate::EmbedError::Interrupted);
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        let scores = vec![1.0; passages.len()];
        self.token.cancel();
        self.db.set_cancel(None);
        Ok(scores)
    }
}

fn assert_mem_interrupted<T>(result: crate::Result<T>) {
    match result {
        Err(MemError::Core(citadel_core::Error::Interrupted)) => {}
        Err(err) => panic!("expected Interrupted, got {err:?}"),
        Ok(_) => panic!("expected Interrupted, got success"),
    }
}

fn wait_for_model_call(calls: &std::sync::atomic::AtomicUsize, operation: &str) {
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    let deadline = Instant::now() + Duration::from_secs(5);
    while calls.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "{operation} did not enter the cancellable model callback"
    );
}

fn arm_cancel_after_local_work(db: &Arc<Database>) {
    let token = citadel_core::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let db = Arc::clone(db);
    CANCEL_AFTER_LOCAL_WORK.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(slot.is_none(), "local cancellation hook was already armed");
        *slot = Some(Box::new(move || {
            token.cancel();
            // Prove the operation observes the snapshot it began with rather
            // than relying on a later database statement to see the token.
            db.set_cancel(None);
        }));
    });
}

fn arm_cancel_after_key_erasure(db: &Arc<Database>) -> Arc<std::sync::atomic::AtomicBool> {
    let token = citadel_core::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = Arc::clone(&fired);
    CANCEL_AFTER_KEY_ERASURE.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "key-erasure cancellation hook was already armed"
        );
        *slot = Some(Box::new(move || {
            observed.store(true, std::sync::atomic::Ordering::Relaxed);
            token.cancel();
        }));
    });
    fired
}

fn finish_key_erasure_probe(db: &Arc<Database>, fired: &std::sync::atomic::AtomicBool) {
    assert!(
        db.cancel_token().is_some_and(|token| token.is_cancelled()),
        "operation replaced the caller's tripped cancellation token"
    );
    db.set_cancel(None);
    let still_armed = CANCEL_AFTER_KEY_ERASURE.with(|slot| slot.borrow_mut().take().is_some());
    assert!(!still_armed, "atom or region key erasure was not reached");
    assert!(
        fired.load(std::sync::atomic::Ordering::Relaxed),
        "key-erasure hook did not fire"
    );
}

fn arm_cancel_after_segment_key_erasure(db: &Arc<Database>) -> Arc<std::sync::atomic::AtomicBool> {
    let token = citadel_core::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = Arc::clone(&fired);
    CANCEL_AFTER_SEGMENT_KEY_ERASURE.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "segment-key-erasure cancellation hook was already armed"
        );
        *slot = Some(Box::new(move || {
            observed.store(true, std::sync::atomic::Ordering::Relaxed);
            token.cancel();
        }));
    });
    fired
}

fn finish_segment_key_erasure_probe(db: &Arc<Database>, fired: &std::sync::atomic::AtomicBool) {
    assert!(
        db.cancel_token().is_some_and(|token| token.is_cancelled()),
        "operation replaced the caller's tripped cancellation token"
    );
    db.set_cancel(None);
    let still_armed =
        CANCEL_AFTER_SEGMENT_KEY_ERASURE.with(|slot| slot.borrow_mut().take().is_some());
    assert!(!still_armed, "persisted segment retirement was not reached");
    assert!(
        fired.load(std::sync::atomic::Ordering::Relaxed),
        "persisted segment key was not erased"
    );
}

#[test]
fn request_cancel_scope_restores_the_database_token_after_success_and_panic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let other_dir = tempfile::tempdir().unwrap();
    let other_db = create_db(other_dir.path());
    let other = MemoryEngine::open(Arc::clone(&other_db)).unwrap();

    let global = citadel_core::CancelToken::new();
    global.cancel();
    db.set_cancel(Some(global));

    let outer = citadel_core::CancelToken::new();
    eng.with_cancel_token(outer, |eng| {
        assert!(db
            .cancel_token()
            .is_some_and(|installed| !installed.is_cancelled()));

        let inner = citadel_core::CancelToken::new();
        inner.cancel();
        eng.with_cancel_token(inner, |_| {
            assert!(db
                .cancel_token()
                .is_some_and(|installed| installed.is_cancelled()));
        });
        assert!(db
            .cancel_token()
            .is_some_and(|installed| !installed.is_cancelled()));

        let other_token = citadel_core::CancelToken::new();
        other_token.cancel();
        other.with_cancel_token(other_token, |_| {
            assert!(db
                .cancel_token()
                .is_some_and(|installed| !installed.is_cancelled()));
            assert!(other_db
                .cancel_token()
                .is_some_and(|installed| installed.is_cancelled()));
        });
        assert!(other_db.cancel_token().is_none());
        assert!(db
            .cancel_token()
            .is_some_and(|installed| !installed.is_cancelled()));

        let replacement = citadel_core::CancelToken::new();
        replacement.cancel();
        db.set_cancel(Some(replacement));
        assert!(
            db.cancel_token()
                .is_some_and(|installed| !installed.is_cancelled()),
            "the explicit database token replaced the current local scope"
        );
    });
    assert!(db
        .cancel_token()
        .is_some_and(|installed| installed.is_cancelled()));

    db.set_cancel(None);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eng.with_cancel_token(citadel_core::CancelToken::new(), |_| {
            panic!("request handler panic must unwind through the cancellation lease")
        });
    }));
    assert!(panic.is_err());
    assert!(
        db.cancel_token().is_none(),
        "panic leaked a request token into later engine work"
    );

    eng.with_cancel_token(citadel_core::CancelToken::new(), |_| ());
    assert!(
        db.cancel_token().is_none(),
        "a poisoned execution lease did not recover"
    );
}

#[test]
fn request_cancel_scopes_are_thread_local_and_do_not_cancel_unscoped_work() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let first = Arc::new(MemoryEngine::open(Arc::clone(&db)).unwrap());
    let second = Arc::new(MemoryEngine::open(Arc::clone(&db)).unwrap());
    first
        .create_region("scope", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    second
        .attach_existing_region("scope", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    first
        .remember("scope", AtomInput::new("fact", "one"))
        .unwrap();

    let first_token = citadel_core::CancelToken::new();
    let release = Arc::new(AtomicBool::new(false));
    let (first_entered_tx, first_entered_rx) = mpsc::sync_channel(0);
    let (second_done_tx, second_done_rx) = mpsc::sync_channel(0);

    std::thread::scope(|scope| {
        let first = Arc::clone(&first);
        let token = first_token.clone();
        let worker_release = Arc::clone(&release);
        let first_thread = scope.spawn(move || {
            first.with_cancel_token(token.clone(), |first| {
                token.cancel();
                first_entered_tx.send(first.count("scope", "fact")).unwrap();
                while !worker_release.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
            });
        });
        let scoped_result = first_entered_rx.recv().unwrap();

        let second_worker = Arc::clone(&second);
        let second_thread = scope.spawn(move || {
            let result = second_worker
                .with_cancel_token(citadel_core::CancelToken::new(), |second| {
                    second.count("scope", "fact")
                });
            second_done_tx.send(result).unwrap();
        });

        let unscoped_result = second.count("scope", "fact");
        let concurrent_scope_result = second_done_rx.recv_timeout(Duration::from_secs(2));

        release.store(true, Ordering::Release);
        first_thread.join().unwrap();
        second_thread.join().unwrap();

        assert!(
            matches!(
                scoped_result,
                Err(MemError::Core(citadel_core::Error::Interrupted))
            ),
            "the scoped operation did not observe its own cancelled token"
        );
        assert_eq!(unscoped_result.unwrap(), 1);
        assert_eq!(
            concurrent_scope_result
                .expect("a second thread's cancellation scope was serialized")
                .unwrap(),
            1
        );
    });

    assert!(db.cancel_token().is_none());
    let global = citadel_core::CancelToken::new();
    global.cancel();
    db.set_cancel(Some(global));
    assert_mem_interrupted(second.count("scope", "fact"));
    db.set_cancel(None);
}

#[test]
fn local_read_postprocessing_observes_its_cancel_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_region("local-cancel", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember(
            "local-cancel",
            AtomInput::new("alpha", "first").with_embedding(unit(8, 0)),
        )
        .unwrap();
    let b = eng
        .remember(
            "local-cancel",
            AtomInput::new("beta", "second").with_embedding(unit(8, 1)),
        )
        .unwrap();
    eng.link(a, b, EdgeKind::Refines, 1.0).unwrap();

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.fetch_range("local-cancel", &FetchQuery::new(10)));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.recall("local-cancel", RecallQuery::by_embedding(unit(8, 0), 10)));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.profile(
        "local-cancel",
        RecallQuery::by_embedding(unit(8, 0), 10),
        10,
    ));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.fetch_edges(Some(a), None, None));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.fetch_edges_in_region("local-cancel", Some(a), None, None, 10));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.summarize("local-cancel", 0));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.stored_atom_kinds("local-cancel"));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.stored_region_names());

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.stored_atom_retrieval_state("local-cancel"));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.recall_mmr(
        "local-cancel",
        RecallQuery::by_embedding(unit(8, 0), 2),
        2,
        0.5,
    ));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.ann_cache_status("local-cancel"));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.ann_cache_status_current("local-cancel"));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.unlink_in_region("local-cancel", a, b, EdgeKind::Refines));
    assert_eq!(
        eng.fetch_edges(Some(a), Some(b), Some(EdgeKind::Refines))
            .unwrap()
            .len(),
        1,
        "a cancelled unlink must roll its deletion back"
    );
}

#[test]
fn empty_mutation_fast_paths_observe_their_cancel_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("empty-cancel", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let mutable = eng
        .remember(
            "empty-cancel",
            AtomInput::new("fact", "mutable")
                .with_payload(serde_json::json!({"tag": "keep"}))
                .with_embedding(unit(8, 0)),
        )
        .unwrap();
    let protected = eng
        .remember(
            "empty-cancel",
            AtomInput::new("fact", "protected")
                .immutable()
                .with_embedding(unit(8, 1)),
        )
        .unwrap();

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.count_region("empty-cancel"));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.evict(
        "empty-cancel",
        EvictionPolicy::PredicateMatch {
            predicate: serde_json::json!({"tag": "missing"}),
        },
    ));
    assert!(eng.fetch_one("empty-cancel", mutable).unwrap().is_some());

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.forget_atoms("empty-cancel", &[protected], false));
    assert!(eng.fetch_one("empty-cancel", protected).unwrap().is_some());
}

#[test]
fn destructive_operations_observe_their_cancel_snapshot_before_erasure() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("erase-cancel", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember(
            "erase-cancel",
            AtomInput::new("fact", "must survive").with_embedding(unit(8, 0)),
        )
        .unwrap();

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.drop_region("missing-region"));

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.persist_ann_index("erase-cancel"));
    assert!(eng.fetch_one("erase-cancel", atom).unwrap().is_some());

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.evict("erase-cancel", EvictionPolicy::PurgeRegion));
    assert!(eng.fetch_one("erase-cancel", atom).unwrap().is_some());

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.delete_atoms("erase-cancel", &[atom]));
    assert!(eng.fetch_one("erase-cancel", atom).unwrap().is_some());

    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.forget_atoms_with_dependents("erase-cancel", &[atom], true));
    assert!(eng.fetch_one("erase-cancel", atom).unwrap().is_some());

    eng.persist_ann_index("erase-cancel").unwrap();
    assert!(seg_chunks_exist(&db, region_id, 8));
    arm_cancel_after_local_work(&db);
    assert_mem_interrupted(eng.drop_region("erase-cancel"));
    assert!(eng.fetch_one("erase-cancel", atom).unwrap().is_some());
    assert!(
        seg_chunks_exist(&db, region_id, 8),
        "a cancelled drop must not retire the region's persisted ANN key first"
    );
}

#[test]
fn remember_embeds_passages_and_recall_embeds_queries() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    let counter = Arc::new(SideCountingEmbedder {
        inner: MockEmbedder::new(8),
        passages: std::sync::atomic::AtomicUsize::new(0),
        queries: std::sync::atomic::AtomicUsize::new(0),
    });
    eng.create_region("sides", counter.clone()).unwrap();

    eng.remember("sides", AtomInput::new("note", "alpha"))
        .unwrap();
    eng.remember_batch("sides", vec![AtomInput::new("note", "beta")])
        .unwrap();
    assert_eq!(counter.passages.load(Ordering::Relaxed), 2);
    assert_eq!(counter.queries.load(Ordering::Relaxed), 0);

    eng.recall("sides", RecallQuery::by_text("alpha", 1))
        .unwrap();
    assert_eq!(
        counter.queries.load(Ordering::Relaxed),
        1,
        "recall is query-side"
    );
    assert_eq!(counter.passages.load(Ordering::Relaxed), 2);
}

#[test]
fn recall_many_keeps_one_provenance_while_batch_queries_are_embedded() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let engine = Arc::new(MemoryEngine::open(Arc::clone(&db)).unwrap());
    let armed = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    engine
        .create_region(
            "stable-query",
            Arc::new(BlockingQueryEmbedder {
                inner: MockEmbedder::new(8),
                armed: Arc::clone(&armed),
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
            }),
        )
        .unwrap();
    engine
        .remember(
            "stable-query",
            AtomInput::new("note", "candidate").with_embedding(unit(8, 0)),
        )
        .unwrap();

    armed.store(true, Ordering::SeqCst);
    let worker = {
        let engine = Arc::clone(&engine);
        std::thread::spawn(move || {
            engine.recall_many(
                "stable-query",
                MultiRecallQuery::new(vec![RecallQuery::by_text("candidate", 1)], 1),
            )
        })
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !entered.load(Ordering::SeqCst) && Instant::now() < deadline {
        std::thread::yield_now();
    }
    if !entered.load(Ordering::SeqCst) {
        release.store(true, Ordering::SeqCst);
        let _ = worker.join();
        panic!("query embedder was not reached");
    }

    let error = engine
        .reclassify_region("stable-query", "different-model".into())
        .expect_err("provenance must not change under a batched query");
    assert!(matches!(
        error,
        MemError::Core(citadel_core::Error::RegionInUse { .. })
    ));
    release.store(true, Ordering::SeqCst);
    assert!(!worker.join().unwrap().unwrap().is_empty());
    engine
        .reclassify_region("stable-query", "different-model".into())
        .unwrap();
}

#[test]
fn encrypted_reembed_retires_the_persisted_ann_segment_before_publish() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = engine
        .create_encrypted_region(
            "segment-reembed",
            Arc::new(ModelEmbedder {
                inner: MockEmbedder::new(8),
                model: "model-a",
            }),
        )
        .unwrap();
    let atom = engine
        .remember("segment-reembed", AtomInput::new("note", "retained atom"))
        .unwrap();
    engine.persist_ann_index("segment-reembed").unwrap();
    let conn = Connection::open(&db).unwrap();
    let (slot, generation, _) = read_annseg_meta(&conn, region_id).unwrap().unwrap();
    drop(conn);
    assert!(seg_chunks_exist(&db, region_id, 8));

    FAIL_REEMBED_AFTER_SEGMENT_RETIRE.with(|fault| fault.set(true));
    let error = engine
        .reembed_region(
            "segment-reembed",
            Arc::new(ModelEmbedder {
                inner: MockEmbedder::new(8),
                model: "model-b",
            }),
            None,
        )
        .expect_err("fault point must stop before provenance publication");
    assert!(matches!(error, MemError::Invalid(message) if message.contains("segment retirement")));

    let record = db.atom_store_slot(slot).unwrap();
    assert_eq!(record.state, SlotState::Tombstone);
    assert!(record.gen > generation);
    assert!(!seg_tree_exists(&db, region_id, 8));
    let conn = Connection::open(&db).unwrap();
    assert!(read_annseg_meta(&conn, region_id).unwrap().is_none());
    let row = engine
        .load_region_row(&conn, "segment-reembed")
        .unwrap()
        .unwrap();
    assert_eq!(row.model_id, "model-a");
    let mark = engine.read_reembed_mark(&conn, region_id).unwrap().unwrap();
    assert!(mark.phase == ReembedPhase::Vectors);
    assert_eq!(mark.done_through, atom);
    drop(conn);

    engine
        .reembed_region(
            "segment-reembed",
            Arc::new(ModelEmbedder {
                inner: MockEmbedder::new(8),
                model: "model-b",
            }),
            None,
        )
        .unwrap();
    assert_eq!(
        engine
            .fetch_one("segment-reembed", atom)
            .unwrap()
            .unwrap()
            .id,
        atom
    );
}

#[test]
fn region_erasure_scrubs_dormant_caches_in_every_engine() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let writer = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let reader = MemoryEngine::open(Arc::clone(&db)).unwrap();
    writer
        .create_encrypted_region("shared-cache", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    reader
        .attach_existing_region("shared-cache", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    writer
        .remember("shared-cache", AtomInput::new("note", "secret"))
        .unwrap();
    reader
        .recall("shared-cache", RecallQuery::by_text("secret", 1))
        .unwrap();
    let stale_ann = reader.region_handle("shared-cache").unwrap().ann;
    assert!(stale_ann.read().unwrap().is_some());

    writer.drop_region("shared-cache").unwrap();

    assert!(stale_ann.read().unwrap().is_none());
    assert!(!reader.regions.lock().unwrap().contains_key("shared-cache"));
}

struct ReentrantDropEmbedder {
    inner: MockEmbedder,
    db: Arc<Database>,
    acquired: std::sync::mpsc::Sender<()>,
}

impl Embedder for ReentrantDropEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }

    fn model_id(&self) -> &str {
        self.inner.model_id()
    }

    fn embed_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> std::result::Result<Vec<Vec<f32>>, crate::EmbedError> {
        self.inner.embed_with_cancel(texts, cancel)
    }
}

impl Drop for ReentrantDropEmbedder {
    fn drop(&mut self) {
        let _keys = self.db.key_lifecycle_lock();
        let _edges = self.db.memory_edges_lock();
        let _ = self.acquired.send(());
    }
}

#[test]
fn region_drop_releases_shared_locks_before_dropping_the_embedder() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let engine = Arc::new(MemoryEngine::open(Arc::clone(&db)).unwrap());
    let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
    engine
        .create_region(
            "reentrant-drop",
            Arc::new(ReentrantDropEmbedder {
                inner: MockEmbedder::new(8),
                db,
                acquired: acquired_tx,
            }),
        )
        .unwrap();

    let worker = std::thread::spawn(move || engine.drop_region("reentrant-drop"));
    acquired_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("embedder destructor could not reacquire the shared edge lock");
    worker.join().unwrap().unwrap();
}

#[test]
fn plaintext_access_does_not_hold_the_global_key_lifecycle_lock() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let engine = Arc::new(MemoryEngine::open(Arc::clone(&db)).unwrap());
    engine
        .create_region("plain-reservation", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    let reader = {
        let engine = Arc::clone(&engine);
        std::thread::spawn(move || {
            let handle = engine.region_handle("plain-reservation").unwrap();
            engine.with_live_plain_access("plain-reservation", &handle, |_| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
        })
    };
    entered_rx.recv().unwrap();

    let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
    let probe = std::thread::spawn(move || {
        let _lifecycle = db.key_lifecycle_lock();
        acquired_tx.send(()).unwrap();
    });
    if acquired_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .is_err()
    {
        release_tx.send(()).unwrap();
        reader.join().unwrap().unwrap();
        probe.join().unwrap();
        panic!("a plaintext read retained the database-wide key-lifecycle lock");
    }

    release_tx.send(()).unwrap();
    reader.join().unwrap().unwrap();
    probe.join().unwrap();
}

#[test]
fn provenance_changes_scrub_dormant_caches_in_every_engine() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let writer = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let reader = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let model = |name| {
        Arc::new(ModelEmbedder {
            inner: MockEmbedder::new(8),
            model: name,
        }) as Arc<dyn Embedder>
    };

    writer
        .create_encrypted_region("shared-provenance", model("model-a"))
        .unwrap();
    reader
        .attach_existing_region("shared-provenance", model("model-a"))
        .unwrap();
    writer
        .remember("shared-provenance", AtomInput::new("note", "cached secret"))
        .unwrap();
    reader
        .recall(
            "shared-provenance",
            RecallQuery::by_text("cached secret", 1),
        )
        .unwrap();
    let stale_ann = reader.region_handle("shared-provenance").unwrap().ann;
    assert!(stale_ann.read().unwrap().is_some());

    writer
        .reembed_region("shared-provenance", model("model-b"), None)
        .unwrap();
    assert!(stale_ann.read().unwrap().is_none());
    assert!(!reader
        .regions
        .lock()
        .unwrap()
        .contains_key("shared-provenance"));

    reader
        .attach_existing_region("shared-provenance", model("model-b"))
        .unwrap();
    let second_stale_ann = reader.region_handle("shared-provenance").unwrap().ann;
    reader
        .recall(
            "shared-provenance",
            RecallQuery::by_text("cached secret", 1),
        )
        .unwrap();
    assert!(second_stale_ann.read().unwrap().is_some());

    writer
        .reclassify_region("shared-provenance", "model-c".to_owned())
        .unwrap();
    assert!(second_stale_ann.read().unwrap().is_none());
    assert!(!reader
        .regions
        .lock()
        .unwrap()
        .contains_key("shared-provenance"));
}

#[test]
fn direct_key_erasure_scrubs_dormant_region_and_atom_caches() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let writer = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let reader = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = writer
        .create_encrypted_region("direct-erasure", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    reader
        .attach_existing_region("direct-erasure", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom_id = writer
        .remember("direct-erasure", AtomInput::new("note", "cached secret"))
        .unwrap();
    reader
        .recall("direct-erasure", RecallQuery::by_text("cached secret", 1))
        .unwrap();
    let atom_cache = reader.region_handle("direct-erasure").unwrap().ann;
    assert!(atom_cache.read().unwrap().is_some());

    let conn = Connection::open(&db).unwrap();
    let h = writer.region_handle("direct-erasure").unwrap();
    let atom_binding = atom_key_slots(&conn, &h, &atom_id.to_string()).unwrap()[0];
    drop(conn);
    db.atom_store_tombstone(atom_binding.0, atom_id as u64, atom_binding.2)
        .unwrap();
    assert!(atom_cache.read().unwrap().is_none());
    assert!(reader
        .regions
        .lock()
        .unwrap()
        .contains_key("direct-erasure"));

    let region = writer
        .load_region_row(&Connection::open(&db).unwrap(), "direct-erasure")
        .unwrap()
        .unwrap();
    let region_slot = region.rsk_slot.unwrap();
    db.region_store_tombstone(region_slot, region_id as u64, region.rsk_gen.unwrap())
        .unwrap();
    assert!(!reader
        .regions
        .lock()
        .unwrap()
        .contains_key("direct-erasure"));
    assert!(!writer
        .regions
        .lock()
        .unwrap()
        .contains_key("direct-erasure"));
}

#[test]
fn encrypted_reranker_callbacks_reserve_the_region_and_atom_keys() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let engine = Arc::new(MemoryEngine::open(Arc::clone(&db)).unwrap());
    let region_id = engine
        .create_encrypted_region("callback-keys", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom_id = engine
        .remember(
            "callback-keys",
            AtomInput::new("note", "rerank candidate").with_embedding(unit(8, 0)),
        )
        .unwrap();
    engine.persist_ann_index("callback-keys").unwrap();
    assert!(seg_chunks_exist(&db, region_id, 8));
    assert!(read_annseg_meta(&Connection::open(&db).unwrap(), region_id)
        .unwrap()
        .is_some());
    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    engine.set_reranker(
        Arc::new(BlockingReranker {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            panic_after_release: false,
        }),
        RerankStrategy::Replace,
    );

    let recall = {
        let engine = Arc::clone(&engine);
        std::thread::spawn(move || {
            engine.recall(
                "callback-keys",
                RecallQuery::by_embedding(unit(8, 0), 1).with_text("rerank candidate"),
            )
        })
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !entered.load(Ordering::SeqCst) && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(entered.load(Ordering::SeqCst), "reranker was not reached");

    assert!(matches!(
        engine.drop_region("callback-keys"),
        Err(MemError::Core(citadel_core::Error::RegionInUse { .. }))
    ));
    assert!(matches!(
        engine.evict("callback-keys", EvictionPolicy::PurgeRegion),
        Err(MemError::Core(citadel_core::Error::AtomInUse { atom_id: held })) if held == atom_id as u64
    ));
    assert!(matches!(
        engine.forget_atoms_with_dependents("callback-keys", &[atom_id], false),
        Err(MemError::Core(citadel_core::Error::AtomInUse { atom_id: held })) if held == atom_id as u64
    ));
    assert!(matches!(
        engine.forget_atoms("callback-keys", &[atom_id], false),
        Err(MemError::Core(citadel_core::Error::AtomInUse { atom_id: held })) if held == atom_id as u64
    ));
    assert!(seg_chunks_exist(&db, region_id, 8));
    assert!(read_annseg_meta(&Connection::open(&db).unwrap(), region_id)
        .unwrap()
        .is_some());

    release.store(true, Ordering::SeqCst);
    recall.join().unwrap().unwrap();
    assert_eq!(
        engine
            .forget_atoms("callback-keys", &[atom_id], false)
            .unwrap()
            .rows_deleted,
        1
    );
    assert_segment_retired(&db, region_id, 8);
}

#[test]
fn a_panicking_batched_reranker_releases_its_key_reservations() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region("panic-callback", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom_id = engine
        .remember(
            "panic-callback",
            AtomInput::new("note", "rerank candidate").with_embedding(unit(8, 0)),
        )
        .unwrap();
    engine.set_reranker(
        Arc::new(BlockingReranker {
            entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            release: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            panic_after_release: true,
        }),
        RerankStrategy::Replace,
    );

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = engine.recall_many(
            "panic-callback",
            MultiRecallQuery::new(
                vec![RecallQuery::by_embedding(unit(8, 0), 1).with_text("rerank candidate")],
                1,
            )
            .with_rerank_query("rerank candidate"),
        );
    }));
    assert!(panic.is_err());
    engine.clear_reranker();
    assert_eq!(
        engine
            .forget_atoms("panic-callback", &[atom_id], false)
            .unwrap()
            .rows_deleted,
        1
    );
    engine.drop_region("panic-callback").unwrap();
}

#[test]
fn already_cancelled_database_token_skips_every_external_embedding_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let counter = Arc::new(SideCountingEmbedder {
        inner: MockEmbedder::new(8),
        passages: std::sync::atomic::AtomicUsize::new(0),
        queries: std::sync::atomic::AtomicUsize::new(0),
    });
    eng.create_region("cancelled-boundary", counter.clone())
        .unwrap();

    let token = citadel_core::CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));

    assert_mem_interrupted(eng.remember_batch("cancelled-boundary", Vec::new()));
    assert_mem_interrupted(eng.remember_replacing_keyed_batch("cancelled-boundary", Vec::new()));
    assert_mem_interrupted(eng.fetch_range("cancelled-boundary", &FetchQuery::new(0)));
    assert_mem_interrupted(eng.set_importance("cancelled-boundary", &[]));
    assert_mem_interrupted(eng.delete_atoms("cancelled-boundary", &[]));
    assert_mem_interrupted(eng.verify_atoms("cancelled-boundary", &[]));
    assert_mem_interrupted(eng.recall("cancelled-boundary", RecallQuery::by_text("query", 0)));
    assert_mem_interrupted(eng.preflight_mmr("cancelled-boundary", 0, 0, 0.5));
    assert_mem_interrupted(eng.recall_mmr(
        "cancelled-boundary",
        RecallQuery::by_text("query", 0),
        0,
        0.5,
    ));
    assert_mem_interrupted(
        eng.recall_many("cancelled-boundary", MultiRecallQuery::new(Vec::new(), 1)),
    );
    assert_mem_interrupted(eng.recall_many(
        "cancelled-boundary",
        MultiRecallQuery::new(vec![RecallQuery::by_text("query", 1)], 0),
    ));

    let atom = || AtomInput::new("note", "must not embed");
    assert_mem_interrupted(eng.remember("cancelled-boundary", atom()));
    assert_mem_interrupted(eng.remember_if_absent("cancelled-boundary", atom(), &[], None));
    assert_mem_interrupted(eng.remember_if_absent_keyed(
        "cancelled-boundary",
        atom(),
        &[],
        None,
        "key",
    ));
    assert_mem_interrupted(eng.remember_derived_checked(
        "cancelled-boundary",
        atom(),
        &[],
        &[],
        None,
    ));
    assert_mem_interrupted(eng.remember_batch("cancelled-boundary", vec![atom()]));
    assert_mem_interrupted(eng.remember_replacing_keyed_batch(
        "cancelled-boundary",
        vec![(atom(), "replacement-key".into())],
    ));
    assert_mem_interrupted(eng.recall("cancelled-boundary", RecallQuery::by_text("query", 1)));
    assert_mem_interrupted(eng.recall_many(
        "cancelled-boundary",
        MultiRecallQuery::new(vec![RecallQuery::by_text("query", 1)], 1),
    ));

    assert_eq!(counter.passages.load(Ordering::Relaxed), 0);
    assert_eq!(counter.queries.load(Ordering::Relaxed), 0);
    db.set_cancel(None);
    assert_eq!(eng.count_region("cancelled-boundary").unwrap(), 0);
}

#[test]
fn cancellation_landing_inside_an_embedder_is_observed_from_its_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();

    let passage_token = citadel_core::CancelToken::new();
    let passage = Arc::new(CancelAndClearEmbedder {
        inner: MockEmbedder::new(8),
        db: Arc::clone(&db),
        token: passage_token.clone(),
        passages: std::sync::atomic::AtomicUsize::new(0),
        queries: std::sync::atomic::AtomicUsize::new(0),
    });
    eng.create_region("cancelled-passage", passage.clone())
        .unwrap();
    db.set_cancel(Some(passage_token));
    assert_mem_interrupted(eng.remember(
        "cancelled-passage",
        AtomInput::new("note", "cancel in passage embedder"),
    ));
    assert_eq!(passage.passages.load(Ordering::Relaxed), 1);
    assert_eq!(eng.count_region("cancelled-passage").unwrap(), 0);

    let query_token = citadel_core::CancelToken::new();
    let query = Arc::new(CancelAndClearEmbedder {
        inner: MockEmbedder::new(8),
        db: Arc::clone(&db),
        token: query_token.clone(),
        passages: std::sync::atomic::AtomicUsize::new(0),
        queries: std::sync::atomic::AtomicUsize::new(0),
    });
    eng.create_region("cancelled-query", query.clone()).unwrap();
    db.set_cancel(Some(query_token));
    assert_mem_interrupted(eng.recall(
        "cancelled-query",
        RecallQuery::by_text("cancel in query embedder", 1),
    ));
    assert_eq!(query.queries.load(Ordering::Relaxed), 1);
}

#[test]
fn cancellation_landing_inside_a_reranker_is_observed_from_its_snapshot() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = if encrypted {
            create_enc_db(dir.path())
        } else {
            create_db(dir.path())
        };
        let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
        let embedder = Arc::new(MockEmbedder::new(8));
        if encrypted {
            eng.create_encrypted_region("cancelled-rerank", embedder)
                .unwrap();
        } else {
            eng.create_region("cancelled-rerank", embedder).unwrap();
        }
        eng.remember(
            "cancelled-rerank",
            AtomInput::new("note", "rerank candidate").with_embedding(unit(8, 0)),
        )
        .unwrap();

        let recall_token = citadel_core::CancelToken::new();
        let recall_reranker = Arc::new(CancelAndClearReranker {
            db: Arc::clone(&db),
            token: recall_token.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        eng.set_reranker(recall_reranker.clone(), RerankStrategy::Replace);
        db.set_cancel(Some(recall_token));
        assert_mem_interrupted(eng.recall(
            "cancelled-rerank",
            RecallQuery::by_embedding(unit(8, 0), 1).with_text("rerank candidate"),
        ));
        assert_eq!(recall_reranker.calls.load(Ordering::Relaxed), 1);

        let many_token = citadel_core::CancelToken::new();
        let many_reranker = Arc::new(CancelAndClearReranker {
            db: Arc::clone(&db),
            token: many_token.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        eng.set_reranker(many_reranker.clone(), RerankStrategy::Replace);
        db.set_cancel(Some(many_token));
        assert_mem_interrupted(
            eng.recall_many(
                "cancelled-rerank",
                MultiRecallQuery::new(
                    vec![RecallQuery::by_embedding(unit(8, 0), 1).with_text("rerank candidate")],
                    1,
                )
                .with_rerank_query("rerank candidate"),
            ),
        );
        assert_eq!(many_reranker.calls.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn model_callbacks_receive_the_operation_token_while_they_are_running() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = Arc::new(MemoryEngine::open(Arc::clone(&db)).unwrap());
    let embedder = Arc::new(CooperativeCancelEmbedder {
        inner: MockEmbedder::new(8),
        passage_calls: std::sync::atomic::AtomicUsize::new(0),
        query_calls: std::sync::atomic::AtomicUsize::new(0),
    });
    eng.create_region("cooperative-models", embedder.clone())
        .unwrap();

    let passage_token = citadel_core::CancelToken::new();
    db.set_cancel(Some(passage_token.clone()));
    let passage = {
        let eng = Arc::clone(&eng);
        std::thread::spawn(move || {
            eng.remember(
                "cooperative-models",
                AtomInput::new("note", "cancel passage inference"),
            )
        })
    };
    wait_for_model_call(&embedder.passage_calls, "passage embedding");
    passage_token.cancel();
    assert_mem_interrupted(passage.join().unwrap());
    db.set_cancel(None);
    assert_eq!(eng.count_region("cooperative-models").unwrap(), 0);

    let query_token = citadel_core::CancelToken::new();
    db.set_cancel(Some(query_token.clone()));
    let query = {
        let eng = Arc::clone(&eng);
        std::thread::spawn(move || {
            eng.recall(
                "cooperative-models",
                RecallQuery::by_text("cancel query inference", 1),
            )
        })
    };
    wait_for_model_call(&embedder.query_calls, "query embedding");
    query_token.cancel();
    assert_mem_interrupted(query.join().unwrap());
    db.set_cancel(None);

    eng.remember(
        "cooperative-models",
        AtomInput::new("note", "rerank candidate").with_embedding(unit(8, 0)),
    )
    .unwrap();
    let reranker = Arc::new(CooperativeCancelReranker {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    eng.set_reranker(reranker.clone(), RerankStrategy::Replace);

    let rerank_token = citadel_core::CancelToken::new();
    db.set_cancel(Some(rerank_token.clone()));
    let rerank = {
        let eng = Arc::clone(&eng);
        std::thread::spawn(move || {
            eng.recall(
                "cooperative-models",
                RecallQuery::by_embedding(unit(8, 0), 1).with_text("rerank candidate"),
            )
        })
    };
    wait_for_model_call(&reranker.calls, "reranking");
    rerank_token.cancel();
    assert_mem_interrupted(rerank.join().unwrap());
    db.set_cancel(None);
}

#[test]
fn recall_as_of_re_grades_recency_against_the_reference_clock() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("asof", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    // Identical text, so semantic and keyword signals tie; only recency can split
    // them. Both events sit far past the 30-day half-life relative to the wall
    // clock (recency underflows to 0.0 for both), so the default clock resolves
    // by the id tie-break, while an as-of near the newer event separates them.
    let day = 86_400i64 * 1_000_000;
    let old_event = micros_now() - 6_000 * day;
    let new_event = old_event + 30 * day;
    let a = eng
        .remember(
            "asof",
            AtomInput::new("note", "alpha").with_created_at(old_event),
        )
        .unwrap();
    let b = eng
        .remember(
            "asof",
            AtomInput::new("note", "alpha").with_created_at(new_event),
        )
        .unwrap();

    let recency_only = FusionWeights {
        semantic: 0.0,
        keyword: 0.0,
        recency: 1.0,
        importance: 0.0,
    };
    let ids = |hits: Vec<AtomHit>| hits.iter().map(|h| h.id).collect::<Vec<_>>();

    let wall = eng
        .recall(
            "asof",
            RecallQuery::by_text("alpha", 2).with_weights(recency_only),
        )
        .unwrap();
    assert_eq!(ids(wall), vec![a, b], "wall clock: ancient tie, id order");

    let asof = eng
        .recall(
            "asof",
            RecallQuery::by_text("alpha", 2)
                .with_weights(recency_only)
                .with_as_of(new_event + day),
        )
        .unwrap();
    assert_eq!(ids(asof), vec![b, a], "as-of ranks the newer event first");
}

#[test]
fn evolve_links_nearest_neighbor_with_inverse_distance_weight() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_region("evw", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    let a = eng
        .remember("evw", AtomInput::new("note", "red green blue"))
        .unwrap();
    let b = eng
        .remember("evw", AtomInput::new("note", "red green yellow"))
        .unwrap();

    let emb = embed_one(
        &*eng.region_handle("evw").unwrap().embedder,
        "red green blue",
        None,
    )
    .unwrap();
    let hits = eng
        .recall("evw", RecallQuery::by_embedding(emb, 2))
        .unwrap();
    let d = hits
        .iter()
        .find(|h| h.id == b)
        .expect("b is recalled")
        .distance
        .expect("semantic recall has a raw distance");
    assert!(
        d > 0.0 && d < 1.0,
        "distance must be a proper fraction: {d}"
    );

    let report = eng.evolve("evw", a, 1, 10.0).unwrap();
    assert_eq!(report.links_added, 1, "exactly one neighbor linked");

    let edges = eng.fetch_edges(Some(a), None, None).unwrap();
    assert_eq!(edges.len(), 1, "one outgoing edge");
    assert_eq!(edges[0].dst_id, b, "edge points at the neighbor");
    assert_eq!(edges[0].kind, EdgeKind::SimilarTo);

    let expected = 1.0f32 / (1.0 + d);
    assert!(
        (edges[0].weight - expected).abs() < 1e-5,
        "weight {} should be 1/(1+dist) = {}",
        edges[0].weight,
        expected
    );
    assert!(
        edges[0].weight < 1.0 && edges[0].weight > 0.5,
        "inverse-distance weight is a proper fraction above 0.5: {}",
        edges[0].weight
    );
}

#[test]
fn evolve_retain_requires_both_id_and_distance() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_region("evr", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    let a = eng
        .remember("evr", AtomInput::new("note", "red green blue"))
        .unwrap();
    let _b = eng
        .remember("evr", AtomInput::new("note", "alpha beta gamma"))
        .unwrap();

    let report = eng.evolve("evr", a, 5, 0.0).unwrap();
    assert_eq!(
        report.links_added, 0,
        "AND filter drops everything when the distance bound excludes all"
    );
    let edges = eng.fetch_edges(Some(a), None, None).unwrap();
    assert!(
        edges.is_empty(),
        "no edges created under the distance bound"
    );
}

#[test]
fn vec_distance_l2_uses_difference_not_sum() {
    let d = vec_distance(EmbeddingMetric::L2, &[1.0, 2.0], &[5.0, 10.0]).unwrap();
    assert!(
        (d - 80.0_f32.sqrt()).abs() < 1e-3,
        "L2 = sqrt(80) ~ 8.944, not sqrt(180)"
    );
}

#[test]
fn vec_distance_cosine_divides_by_denominator() {
    let d = vec_distance(EmbeddingMetric::Cosine, &[1.0, 1.0], &[1.0, 0.0]).unwrap();
    let expected = 1.0_f32 - 1.0 / 2.0_f32.sqrt();
    assert!(
        (d - expected).abs() < 1e-3,
        "cosine = 1 - 1/sqrt(2) ~ 0.293, not 1 - sqrt(2)"
    );
}

#[test]
fn dist_value_coerces_integer_to_f32() {
    assert_eq!(dist_value(&Value::Integer(-7)).unwrap(), Some(-7.0_f32));
    assert_eq!(dist_value(&Value::Integer(42)).unwrap(), Some(42.0_f32));
    assert_eq!(dist_value(&Value::Null).unwrap(), None);
}

#[test]
fn query_keyword_terms_tokenizes_lowercases_sorts_dedups() {
    assert_eq!(
        query_keyword_terms(Some("Beta alpha Beta gamma")),
        vec![
            String::from("alpha"),
            String::from("beta"),
            String::from("gamma")
        ]
    );
    assert_eq!(query_keyword_terms(None), Vec::<String>::new());
}

#[test]
fn bm25_idf_rewards_rare_terms_and_handles_tokenization() {
    use crate::fusion::Candidate;
    let mk = |id, text: &str| Candidate {
        id,
        kind: "fact".into(),
        text: text.into(),
        payload: serde_json::Value::Null,
        dist: Some(0.0),
        text_rank: 0.0,
        importance: 0.0,
        confidence: 1.0,
        created_micros: 0,
        expires_micros: None,
        immutable: false,
    };
    // 'common' is in all three (low IDF); 'zebra' is in one (high IDF). The query has
    // mixed case + punctuation to exercise UAX#29 tokenization.
    let mut cands = vec![
        mk(1, "common zebra here"),
        mk(2, "common word two"),
        mk(3, "common word three"),
    ];
    assign_bm25_ranks(
        &mut cands,
        &query_keyword_terms(Some("Common, ZEBRA!")),
        None,
    )
    .unwrap();
    assert!(
        cands[0].text_rank > cands[1].text_rank,
        "the rare-term match outscores common-only matches via IDF"
    );
    assert_eq!(
        cands[1].text_rank, cands[2].text_rank,
        "candidates matching only the pool-common term tie"
    );
    assert!(
        cands[1].text_rank > 0.0,
        "a pool-common term still carries a small positive IDF (Lucene +1 form)"
    );
    // No query terms or no candidates leaves ranks untouched.
    let mut none = vec![mk(9, "anything")];
    assign_bm25_ranks(&mut none, &query_keyword_terms(None), None).unwrap();
    assert_eq!(none[0].text_rank, 0.0);
}

#[test]
fn graph_expand_plaintext_exposes_depth_and_stored_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("g", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let future = micros_now() + 60_000_000;
    let a = eng
        .remember("g", AtomInput::new("fact", "alpha unique one"))
        .unwrap();
    let b = eng
        .remember(
            "g",
            AtomInput::new("fact", "beta unique two")
                .with_importance(0.7)
                .with_confidence(0.6)
                .with_expires_at(future),
        )
        .unwrap();
    let c = eng
        .remember(
            "g",
            AtomInput::new("fact", "gamma unique three")
                .with_importance(0.2)
                .with_confidence(0.3),
        )
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();
    eng.link(b, c, EdgeKind::DerivedFrom, 1.0).unwrap();

    let hits = eng
        .recall(
            "g",
            RecallQuery::by_text("alpha unique one", 1)
                .with_graph_expand(GraphExpand::new(2, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    let hit_b = hits.iter().find(|h| h.id == b).expect("1-hop atom reached");
    let hit_c = hits.iter().find(|h| h.id == c).expect("2-hop atom reached");
    assert_eq!(hit_b.graph_depth, Some(1));
    assert_eq!(hit_c.graph_depth, Some(2));
    assert_eq!(hit_b.importance, 0.7);
    assert_eq!(hit_c.importance, 0.2);
    assert_eq!((hit_b.confidence, hit_b.expires_at), (0.6, Some(future)));
    assert_eq!((hit_c.confidence, hit_c.expires_at), (0.3, None));
    for hit in [hit_b, hit_c] {
        assert_eq!(hit.relevance, None, "graph rows are not query-ranked");
        assert_eq!(hit.distance, None, "graph rows are not distance-ranked");
    }
}

#[test]
fn graph_expand_cannot_cross_a_foreign_region_as_an_intermediate_hop() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("local", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("foreign", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let seed = eng
        .remember(
            "local",
            AtomInput::new("fact", "local seed").with_embedding(unit(8, 0)),
        )
        .unwrap();
    let target = eng
        .remember(
            "local",
            AtomInput::new("fact", "local target").with_embedding(unit(8, 1)),
        )
        .unwrap();
    let bridge = eng
        .remember(
            "foreign",
            AtomInput::new("fact", "foreign bridge").with_embedding(unit(8, 2)),
        )
        .unwrap();
    eng.link(seed, bridge, EdgeKind::DerivedFrom, 1.0).unwrap();
    eng.link(bridge, target, EdgeKind::DerivedFrom, 1.0)
        .unwrap();

    assert_eq!(
        eng.fetch_edges(None, None, Some(EdgeKind::DerivedFrom))
            .unwrap()
            .len(),
        2,
        "the cross-region path exists in global edge storage"
    );
    let hits = eng
        .recall(
            "local",
            RecallQuery::by_embedding(unit(8, 0), 1)
                .with_graph_expand(GraphExpand::new(2, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    assert!(hits.iter().any(|hit| hit.id == seed));
    assert!(
        hits.iter().all(|hit| hit.id != target),
        "a foreign atom must not bridge two local atoms during graph expansion"
    );
}

#[test]
fn foreign_or_expired_superseders_do_not_hide_a_live_local_atom() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("local", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("foreign", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let target = eng
        .remember(
            "local",
            AtomInput::new("fact", "local target").with_embedding(unit(8, 0)),
        )
        .unwrap();
    let foreign = eng
        .remember(
            "foreign",
            AtomInput::new("fact", "foreign source").with_embedding(unit(8, 1)),
        )
        .unwrap();
    let expired = eng
        .remember(
            "local",
            AtomInput::new("fact", "expired source")
                .with_embedding(unit(8, 2))
                .with_expires_at(micros_now() - 1),
        )
        .unwrap();
    eng.link(foreign, target, EdgeKind::Supersedes, 1.0)
        .unwrap();
    eng.link(expired, target, EdgeKind::Supersedes, 1.0)
        .unwrap();

    let hits = eng
        .recall("local", RecallQuery::by_embedding(unit(8, 0), 1))
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, target);
}

#[test]
fn graph_expand_rejects_a_high_fanout_before_exceeding_its_node_budget() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("bounded", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let seed = eng
        .remember(
            "bounded",
            AtomInput::new("fact", "seed").with_embedding(unit(8, 0)),
        )
        .unwrap();
    let mut neighbours = Vec::new();
    for index in 1..=4 {
        let id = eng
            .remember(
                "bounded",
                AtomInput::new("fact", format!("neighbour {index}")).with_embedding(unit(8, index)),
            )
            .unwrap();
        eng.link(seed, id, EdgeKind::DerivedFrom, 1.0).unwrap();
        neighbours.push(id);
    }

    let allowed = eng
        .recall(
            "bounded",
            RecallQuery::by_embedding(unit(8, 0), 1).with_graph_expand(
                GraphExpand::new(1, vec![EdgeKind::DerivedFrom]).with_max_nodes(4),
            ),
        )
        .unwrap();
    assert!(neighbours
        .iter()
        .all(|id| allowed.iter().any(|hit| hit.id == *id)));

    let error = eng
        .recall(
            "bounded",
            RecallQuery::by_embedding(unit(8, 0), 1).with_graph_expand(
                GraphExpand::new(1, vec![EdgeKind::DerivedFrom]).with_max_nodes(3),
            ),
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            MemError::WorkLimitExceeded {
                operation: "graph expansion",
                limit: 3,
            }
        ),
        "{error:?}"
    );

    let error = eng
        .recall(
            "bounded",
            RecallQuery::by_embedding(unit(8, 0), 1).with_graph_expand(
                GraphExpand::new(1, vec![EdgeKind::DerivedFrom]).with_max_nodes(100_001),
            ),
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            MemError::WorkLimitExceeded {
                operation: "graph expansion",
                limit: 100_000,
            }
        ),
        "{error:?}"
    );
}

#[test]
fn graph_expand_plaintext_depth_zero_returns_no_reached_atoms() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("g0", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("g0", AtomInput::new("fact", "alpha unique one"))
        .unwrap();
    let b = eng
        .remember("g0", AtomInput::new("fact", "beta unique two"))
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();

    let hits = eng
        .recall(
            "g0",
            RecallQuery::by_text("alpha unique one", 1)
                .with_graph_expand(GraphExpand::new(0, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    let ids: Vec<i64> = hits.iter().map(|h| h.id).collect();
    assert!(ids.contains(&a), "seed survives recall");
    assert!(
        !ids.contains(&b),
        "depth 0 must short-circuit: no neighbor is expanded"
    );
}

#[test]
fn graph_expand_sealed_exposes_depth_and_stored_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("sg", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let future = micros_now() + 60_000_000;
    let a = eng
        .remember("sg", AtomInput::new("fact", "alpha unique one"))
        .unwrap();
    let b = eng
        .remember(
            "sg",
            AtomInput::new("fact", "beta unique two")
                .with_importance(0.7)
                .with_confidence(0.6)
                .with_expires_at(future),
        )
        .unwrap();
    let c = eng
        .remember(
            "sg",
            AtomInput::new("fact", "gamma unique three")
                .with_importance(0.2)
                .with_confidence(0.3),
        )
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();
    eng.link(b, c, EdgeKind::DerivedFrom, 1.0).unwrap();

    let hits = eng
        .recall(
            "sg",
            RecallQuery::by_text("alpha unique one", 1)
                .with_graph_expand(GraphExpand::new(2, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    let hit_b = hits
        .iter()
        .find(|h| h.id == b)
        .expect("1-hop sealed atom reached");
    let hit_c = hits
        .iter()
        .find(|h| h.id == c)
        .expect("2-hop sealed atom reached");
    assert_eq!(hit_b.graph_depth, Some(1));
    assert_eq!(hit_c.graph_depth, Some(2));
    assert_eq!(hit_b.importance, 0.7);
    assert_eq!(hit_c.importance, 0.2);
    assert_eq!((hit_b.confidence, hit_b.expires_at), (0.6, Some(future)));
    assert_eq!((hit_c.confidence, hit_c.expires_at), (0.3, None));
    for hit in [hit_b, hit_c] {
        assert_eq!(hit.relevance, None, "graph rows are not query-ranked");
        assert_eq!(hit.distance, None, "graph rows are not distance-ranked");
    }
    assert_eq!(
        hits.iter().find(|h| h.id == b).unwrap().text,
        "beta unique two"
    );
}

#[test]
fn graph_expand_sealed_rejects_a_stale_row_key_binding() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("sg", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let seed = eng
        .remember(
            "sg",
            AtomInput::new("fact", "seed").with_embedding(unit(8, 0)),
        )
        .unwrap();
    let neighbor = eng
        .remember(
            "sg",
            AtomInput::new("fact", "stale neighbor").with_embedding(unit(8, 1)),
        )
        .unwrap();
    eng.link(seed, neighbor, EdgeKind::DerivedFrom, 1.0)
        .unwrap();

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {table} SET key_gen = key_gen + 1 WHERE id = $1"),
            &[Value::Integer(neighbor)],
        )
        .unwrap();

    let hits = eng
        .recall(
            "sg",
            RecallQuery::by_embedding(unit(8, 0), 1)
                .with_graph_expand(GraphExpand::new(1, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    assert!(hits.iter().any(|hit| hit.id == seed));
    assert!(
        hits.iter().all(|hit| hit.id != neighbor),
        "graph expansion must not decrypt through an id-only key lookup"
    );
}

#[test]
fn stale_sealed_superseder_does_not_hide_a_live_atom() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("sealed", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let target = eng
        .remember(
            "sealed",
            AtomInput::new("fact", "live target").with_embedding(unit(8, 0)),
        )
        .unwrap();
    let stale = eng
        .remember(
            "sealed",
            AtomInput::new("fact", "stale superseder").with_embedding(unit(8, 1)),
        )
        .unwrap();
    eng.link(stale, target, EdgeKind::Supersedes, 1.0).unwrap();

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {table} SET key_gen = key_gen + 1 WHERE id = $1"),
            &[Value::Integer(stale)],
        )
        .unwrap();

    let hits = eng
        .recall("sealed", RecallQuery::by_embedding(unit(8, 0), 1))
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, target);
}

#[test]
fn graph_expand_sealed_does_not_traverse_a_stale_intermediary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("sealed", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let seed = eng
        .remember(
            "sealed",
            AtomInput::new("fact", "seed").with_embedding(unit(8, 0)),
        )
        .unwrap();
    let bridge = eng
        .remember(
            "sealed",
            AtomInput::new("fact", "stale bridge").with_embedding(unit(8, 1)),
        )
        .unwrap();
    let target = eng
        .remember(
            "sealed",
            AtomInput::new("fact", "live target").with_embedding(unit(8, 2)),
        )
        .unwrap();
    eng.link(seed, bridge, EdgeKind::DerivedFrom, 1.0).unwrap();
    eng.link(bridge, target, EdgeKind::DerivedFrom, 1.0)
        .unwrap();

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {table} SET key_gen = key_gen + 1 WHERE id = $1"),
            &[Value::Integer(bridge)],
        )
        .unwrap();

    let hits = eng
        .recall(
            "sealed",
            RecallQuery::by_embedding(unit(8, 0), 1)
                .with_graph_expand(GraphExpand::new(2, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    assert!(hits.iter().any(|hit| hit.id == seed));
    assert!(hits.iter().all(|hit| hit.id != bridge));
    assert!(
        hits.iter().all(|hit| hit.id != target),
        "a stale sealed atom must not be usable as a traversal bridge"
    );
}

#[test]
fn graph_expand_sealed_depth_zero_returns_no_reached_atoms() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("sg0", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("sg0", AtomInput::new("fact", "alpha unique one"))
        .unwrap();
    let b = eng
        .remember("sg0", AtomInput::new("fact", "beta unique two"))
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();

    let hits = eng
        .recall(
            "sg0",
            RecallQuery::by_text("alpha unique one", 1)
                .with_graph_expand(GraphExpand::new(0, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    let ids: Vec<i64> = hits.iter().map(|h| h.id).collect();
    assert!(ids.contains(&a), "seed survives sealed recall");
    assert!(
        !ids.contains(&b),
        "sealed depth 0 must short-circuit: no neighbor is expanded"
    );
}

#[test]
fn parse_candidate_rejects_short_row() {
    let row = vec![Value::Integer(1); 10];
    assert!(
        matches!(parse_candidate(&row), Err(MemError::Invalid(ref m)) if m.contains("recall row shape")),
        "row of len 10 (< 11) must be rejected by the length guard, not a later type error"
    );
    let ok = vec![
        Value::Integer(1),
        Value::Text("fact".into()),
        Value::Null,
        Value::Null,
        Value::Real(0.5),
        Value::Real(0.75),
        Value::Timestamp(0),
        Value::Null,
        Value::Real(0.1),
        Value::Real(0.0),
        Value::Integer(0),
    ];
    assert!(parse_candidate(&ok).is_ok(), "row of len 11 must parse");
}

#[test]
fn parse_fetched_rejects_short_row() {
    let row = vec![Value::Integer(1); 8];
    assert!(
        matches!(parse_fetched(&row), Err(MemError::Invalid(ref m)) if m.contains("fetch row shape")),
        "row of len 8 (< 9) must be rejected by the length guard, not a later type error"
    );
    let ok = vec![
        Value::Integer(1),
        Value::Text("fact".into()),
        Value::Null,
        Value::Null,
        Value::Real(0.5),
        Value::Real(0.75),
        Value::Integer(1),
        Value::Timestamp(0),
        Value::Null,
    ];
    assert!(parse_fetched(&ok).is_ok(), "row of len 9 must parse");
}

#[test]
fn parse_edge_rejects_short_row() {
    let row = vec![Value::Integer(1); 4];
    assert!(
        matches!(parse_edge(&row), Err(MemError::Invalid(_))),
        "row of len 4 (< 5) must be rejected"
    );
    let ok = vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Text("causes".into()),
        Value::Real(1.0),
        Value::Null,
    ];
    assert!(parse_edge(&ok).is_ok(), "row of len 5 must parse");
}

#[test]
fn as_f32_coerces_integer() {
    assert_eq!(as_f32(&Value::Integer(7)).unwrap(), 7.0f32);
    assert_eq!(as_f32(&Value::Real(2.5)).unwrap(), 2.5f32);
    assert_eq!(as_f32(&Value::Null).unwrap(), 0.0f32);
    assert!(as_f32(&Value::Text("7".into())).is_err());
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, f64::MAX] {
        assert!(as_f32(&Value::Real(value)).is_err(), "{value}");
        assert!(exact_f32_bits(&Value::Real(value)).is_err(), "{value}");
    }
}

#[test]
fn as_ts_coerces_integer() {
    assert_eq!(as_ts(&Value::Integer(123)).unwrap(), 123i64);
    assert_eq!(as_ts(&Value::Timestamp(456)).unwrap(), 456i64);
    assert_eq!(as_ts(&Value::Null).unwrap(), 0i64);
    assert!(as_ts(&Value::Text("123".into())).is_err());
}

#[test]
fn stored_payload_and_text_decoders_reject_malformed_non_null_values() {
    assert_eq!(
        parse_payload(&Value::Null).unwrap(),
        serde_json::Value::Null
    );
    assert!(parse_payload(&Value::Text("{".into())).is_err());
    assert!(parse_payload(&Value::Integer(7)).is_err());
    assert_eq!(opt_text(&Value::Null).unwrap(), "");
    assert!(opt_text(&Value::Integer(7)).is_err());
    assert!(!as_bool(&Value::Integer(0)).unwrap());
    assert!(as_bool(&Value::Integer(1)).unwrap());
    assert!(as_bool(&Value::Integer(2)).is_err());
    assert!(as_bool(&Value::Text("false".into())).is_err());
    assert_eq!(opt_ts(&Value::Null).unwrap(), None);
    assert_eq!(opt_ts(&Value::Timestamp(9)).unwrap(), Some(9));
    assert!(opt_ts(&Value::Text("never".into())).is_err());
}

#[test]
fn recall_fusion_arm_uses_reranker_replace_scores() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("fa", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("fa", AtomInput::new("turn", "alpha beta gamma"))
        .unwrap();
    eng.remember("fa", AtomInput::new("turn", "delta epsilon"))
        .unwrap();

    eng.set_reranker(
        Arc::new(crate::embed::MockReranker),
        RerankStrategy::Replace,
    );
    let hits = eng
        .recall("fa", RecallQuery::by_text("alpha beta gamma", 2))
        .unwrap();
    assert_eq!(hits[0].text, "alpha beta gamma");
    assert_eq!(
        hits[0].relevance,
        Some(3.0_f32),
        "Replace score is the raw overlap count"
    );
    assert_eq!(hits[1].text, "delta epsilon");
    assert_eq!(hits[1].relevance, Some(0.0_f32));
}

#[test]
fn recall_sealed_fusion_arm_uses_reranker_replace_scores() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("fas", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("fas", AtomInput::new("turn", "alpha beta gamma"))
        .unwrap();
    eng.remember("fas", AtomInput::new("turn", "delta epsilon"))
        .unwrap();

    eng.set_reranker(
        Arc::new(crate::embed::MockReranker),
        RerankStrategy::Replace,
    );
    let hits = eng
        .recall("fas", RecallQuery::by_text("alpha beta gamma", 2))
        .unwrap();
    assert_eq!(hits[0].text, "alpha beta gamma");
    assert_eq!(
        hits[0].relevance,
        Some(3.0_f32),
        "sealed Replace score is the raw overlap count"
    );
    assert_eq!(hits[1].text, "delta epsilon");
    assert_eq!(hits[1].relevance, Some(0.0_f32));
}

#[test]
fn set_reranker_changes_recall_score_from_fusion() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("sr", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("sr", AtomInput::new("turn", "alpha beta gamma"))
        .unwrap();

    let before = eng
        .recall("sr", RecallQuery::by_text("alpha beta gamma", 1))
        .unwrap();
    assert!(
        before[0].relevance.is_some_and(|score| score <= 1.0_f32),
        "fusion score is a normalized blend"
    );

    eng.set_reranker(
        Arc::new(crate::embed::MockReranker),
        RerankStrategy::Replace,
    );
    let after = eng
        .recall("sr", RecallQuery::by_text("alpha beta gamma", 1))
        .unwrap();
    assert_eq!(
        after[0].relevance,
        Some(3.0_f32),
        "reranker took effect: Replace overlap score"
    );
}

#[test]
fn remember_batch_returns_contiguous_ids_after_single() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("rb", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let first = eng.remember("rb", AtomInput::new("note", "seed")).unwrap();

    let ids = eng
        .remember_batch(
            "rb",
            vec![
                AtomInput::new("note", "a"),
                AtomInput::new("note", "b"),
                AtomInput::new("note", "c"),
            ],
        )
        .unwrap();
    assert_eq!(
        ids,
        vec![first + 1, first + 2, first + 3],
        "batch ids are start + offset, contiguous after the prior single insert"
    );
}

#[test]
fn attach_region_key_rejects_stale_generation_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    let conn = Connection::open(&db).unwrap();
    let row = eng.load_region_row(&conn, "s").unwrap().unwrap();
    drop(conn);

    let stale = RegionRow {
        id,
        dim: row.dim,
        metric: row.metric,
        model_id: row.model_id.clone(),
        encrypted: row.encrypted,
        rsk_slot: row.rsk_slot,
        rsk_gen: Some(row.rsk_gen.unwrap() + 1),
    };
    assert!(
        matches!(
            eng.attach_region_key("s", &stale),
            Err(MemError::RegionForgotten(_))
        ),
        "a generation mismatch alone (state Live, owner matching) must be RegionForgotten"
    );
}

#[test]
fn attach_region_key_rejects_wrong_owner_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    let conn = Connection::open(&db).unwrap();
    let row = eng.load_region_row(&conn, "s").unwrap().unwrap();
    drop(conn);

    let mut rck = [0x5au8; citadel_core::KEY_SIZE];
    let wrapped = db.wrap_region_key(&rck).unwrap();
    rck.zeroize();
    let other_owner = id as u64 + 1000;
    let (slot, gen) = db
        .region_store_allocate_write(other_owner, &wrapped)
        .unwrap();

    let wrong_owner = RegionRow {
        id,
        dim: row.dim,
        metric: row.metric,
        model_id: row.model_id.clone(),
        encrypted: row.encrypted,
        rsk_slot: Some(slot),
        rsk_gen: Some(gen),
    };
    assert!(
        matches!(
            eng.attach_region_key("s", &wrong_owner),
            Err(MemError::RegionForgotten(_))
        ),
        "an owner mismatch alone (state Live, generation matching) must be RegionForgotten"
    );
}

#[test]
fn attach_region_key_rejects_non_live_slot_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    let empty_slot = 1u32;
    let rec = db.region_store_slot(empty_slot).unwrap();
    assert_eq!(
        rec.state,
        SlotState::Empty,
        "an unallocated prealloc slot is Empty"
    );
    assert_eq!(rec.region_id, 0);
    assert_eq!(rec.gen, 0);

    let non_live = RegionRow {
        id: 0,
        dim: 8,
        metric: EmbeddingMetric::Cosine,
        model_id: "mock".to_string(),
        encrypted: true,
        rsk_slot: Some(empty_slot),
        rsk_gen: Some(0),
    };
    assert!(
        matches!(
            eng.attach_region_key("s", &non_live),
            Err(MemError::RegionForgotten(_))
        ),
        "a non-Live slot alone (owner 0 and gen 0 both matching) must be RegionForgotten"
    );
}

#[test]
fn verify_matches_rejects_each_field_mismatch() {
    let row = RegionRow {
        id: 42,
        dim: 8,
        metric: EmbeddingMetric::Cosine,
        model_id: "mock".to_string(),
        encrypted: true,
        rsk_slot: Some(0),
        rsk_gen: Some(1),
    };

    assert!(
        row.verify_matches("r", 8, EmbeddingMetric::Cosine, "mock", true)
            .is_ok(),
        "an exact match verifies"
    );
    assert!(
        matches!(
            row.verify_matches("r", 16, EmbeddingMetric::Cosine, "mock", true),
            Err(MemError::DimMismatch {
                expected: 8,
                got: 16,
                ..
            })
        ),
        "dim mismatch must be rejected"
    );
    assert!(
        matches!(
            row.verify_matches("r", 8, EmbeddingMetric::L2, "mock", true),
            Err(MemError::MetricMismatch { .. })
        ),
        "metric mismatch must be rejected"
    );
    assert!(
        matches!(
            row.verify_matches("r", 8, EmbeddingMetric::Cosine, "other", true),
            Err(MemError::ModelMismatch { .. })
        ),
        "model mismatch must be rejected"
    );
    assert!(
        matches!(
            row.verify_matches("r", 8, EmbeddingMetric::Cosine, "mock", false),
            Err(MemError::Invalid(_))
        ),
        "encrypted-flag mismatch must be rejected"
    );
}

#[test]
fn check_attached_rejects_mismatch_against_cached_region() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    let id = eng
        .create_region("notes", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    assert_eq!(
        eng.check_attached("notes", 8, EmbeddingMetric::Cosine, MOCK_MODEL_ID, false)
            .unwrap(),
        Some(id),
        "an exact match returns the cached id"
    );
    assert!(
        matches!(
            eng.check_attached("notes", 16, EmbeddingMetric::Cosine, MOCK_MODEL_ID, false),
            Err(MemError::DimMismatch {
                expected: 8,
                got: 16,
                ..
            })
        ),
        "cached dim mismatch must error, not fall through to Ok(None)"
    );
    assert!(
        matches!(
            eng.check_attached("notes", 8, EmbeddingMetric::L2, MOCK_MODEL_ID, false),
            Err(MemError::MetricMismatch { .. })
        ),
        "cached metric mismatch must error"
    );
    assert!(
        matches!(
            eng.check_attached("notes", 8, EmbeddingMetric::Cosine, "other", false),
            Err(MemError::ModelMismatch { .. })
        ),
        "cached model mismatch must error"
    );
    assert!(
        matches!(
            eng.check_attached("notes", 8, EmbeddingMetric::Cosine, MOCK_MODEL_ID, true),
            Err(MemError::Invalid(_))
        ),
        "cached encrypted-flag mismatch must error"
    );
}

#[test]
fn key_destruction_bumps_the_cache_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("v", AtomInput::new("fact", "x")).unwrap();

    let before = db.cache_epoch();
    eng.forget_atom("v", a).unwrap();
    let after_forget = db.cache_epoch();
    assert!(
        after_forget > before,
        "atom key tombstone must arm the epoch"
    );

    eng.drop_region("v").unwrap();
    assert!(
        db.cache_epoch() > after_forget,
        "region key tombstone must arm the epoch"
    );
}

#[test]
fn failed_erase_arms_the_epoch_retires_the_segment_and_reopen_converges() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let secret = eng
        .remember("v", AtomInput::new("fact", "doomed secret"))
        .unwrap();
    let keep = eng
        .remember("v", AtomInput::new("fact", "kept note"))
        .unwrap();
    eng.persist_ann_index("v").unwrap();

    // Capture the lifecycle bindings before the interruption.
    let conn = Connection::open(&db).unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let qr = conn
        .query_params(
            &format!("SELECT key_slot FROM {table} WHERE id = $1"),
            &[Value::Integer(secret)],
        )
        .unwrap();
    let secret_slot = as_int(&qr.rows[0][0]).unwrap() as u32;
    let (seg_slot, _, _) = read_annseg_meta(&conn, region_id).unwrap().unwrap();
    drop(conn);

    // Warm the decrypted cache before the interrupted erase.
    let warm: Vec<AtomId> = eng
        .recall("v", RecallQuery::by_text("doomed secret", 10))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert!(warm.contains(&secret));

    let before = db.cache_epoch();
    FAIL_ERASE_BEFORE_ROW_DELETE.with(|f| f.set(true));
    let err = eng.forget_atoms("v", &[secret], false).unwrap_err();
    assert!(err.to_string().contains("injected"), "{err}");
    assert!(db.cache_epoch() > before, "epoch armed before the failure");

    // Boundary: both keys Tombstone, tree and meta gone, the row alone survives.
    assert_eq!(
        db.atom_store_slot(secret_slot).unwrap().state,
        SlotState::Tombstone
    );
    assert_eq!(
        db.atom_store_slot(seg_slot).unwrap().state,
        SlotState::Tombstone
    );
    assert!(!seg_chunks_exist(&db, region_id, 8));
    let conn = Connection::open(&db).unwrap();
    assert!(
        read_annseg_meta(&conn, region_id).unwrap().is_none(),
        "segment must retire before the commit"
    );
    let row = conn
        .query_params(
            &format!("SELECT id FROM {table} WHERE id = $1"),
            &[Value::Integer(secret)],
        )
        .unwrap();
    assert!(
        !row.rows.is_empty(),
        "the row outlives its key, never the reverse"
    );
    drop(conn);

    // The row remains, but the warmed cache must refuse the stale plaintext.
    let hits: Vec<AtomId> = eng
        .recall("v", RecallQuery::by_text("doomed secret", 10))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert!(
        !hits.contains(&secret),
        "stale cache served an erased atom: {hits:?}"
    );
    assert!(hits.contains(&keep));

    // Reopen finishes the erase; only a direct table read observes the delete.
    drop(eng);
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let residue = conn
        .query_params(
            &format!("SELECT id FROM {table} WHERE id = $1"),
            &[Value::Integer(secret)],
        )
        .unwrap();
    assert!(
        residue.rows.is_empty(),
        "reconcile must delete the residue row"
    );
    assert!(eng.fetch_one("v", keep).unwrap().is_some());
}

#[test]
fn cascade_retires_the_segment_before_atom_key_erasure() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let turn = eng.remember("v", AtomInput::new("turn", "root")).unwrap();
    eng.remember_derived("v", AtomInput::new("derived", "cites"), &[turn], None)
        .unwrap();
    eng.persist_ann_index("v").unwrap();

    FAIL_CASCADE_AFTER_SEGMENT_RETIRE.with(|f| f.set(true));
    let err = eng
        .forget_atoms_with_dependents("v", &[turn], false)
        .unwrap_err();
    assert!(err.to_string().contains("injected"), "{err}");

    // The segment retired before the txn ever ran; the atoms are untouched.
    let conn = Connection::open(&db).unwrap();
    assert!(read_annseg_meta(&conn, region_id).unwrap().is_none());
    drop(conn);
    assert!(eng.fetch_one("v", turn).unwrap().is_some());

    // The retry completes the cascade in full.
    let receipt = eng
        .forget_atoms_with_dependents("v", &[turn], false)
        .unwrap();
    assert_eq!(receipt.rows_deleted, 2);
    assert_eq!(receipt.erased_count, 2);
}

#[test]
fn reconcile_deletes_rows_whose_key_gen_moved() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("v", AtomInput::new("fact", "gen drift"))
        .unwrap();
    drop(eng);

    // (slot, owner) still matches; only the gen bind catches the drifted row.
    let conn = Connection::open(&db).unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    conn.execute_params(
        &format!("UPDATE {table} SET key_gen = key_gen + 1 WHERE id = $1"),
        &[Value::Integer(a)],
    )
    .unwrap();
    drop(conn);

    let _eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let conn = Connection::open(&db).unwrap();
    let residue = conn
        .query_params(
            &format!("SELECT id FROM {table} WHERE id = $1"),
            &[Value::Integer(a)],
        )
        .unwrap();
    assert!(
        residue.rows.is_empty(),
        "gen-drifted row must reconcile away"
    );
    assert!(
        db.atom_store_live_owners()
            .unwrap()
            .iter()
            .all(|&(_, owner)| owner != a as u64),
        "the unreferenced slot must be reclaimed"
    );
}

#[test]
fn reconcile_tombstones_drift_keys_before_rows_and_converges() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("v", AtomInput::new("fact", "drift")).unwrap();
    drop(eng);

    let conn = Connection::open(&db).unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    conn.execute_params(
        &format!("UPDATE {table} SET key_gen = key_gen + 1 WHERE id = $1"),
        &[Value::Integer(a)],
    )
    .unwrap();
    drop(conn);

    // Interrupt between drift-key tombstones and row deletes: keys die first.
    FAIL_RECONCILE_AFTER_DRIFT_KEYS.with(|f| f.set(true));
    assert!(MemoryEngine::open(Arc::clone(&db)).is_err());
    assert!(db
        .atom_store_live_owners()
        .unwrap()
        .iter()
        .all(|&(_, owner)| owner != a as u64));
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .query_params(
            &format!("SELECT id FROM {table} WHERE id = $1"),
            &[Value::Integer(a)],
        )
        .unwrap();
    assert!(
        !rows.rows.is_empty(),
        "the row outlives its key, never the reverse"
    );
    drop(conn);

    // The retry converges: the key-dead row reconciles away.
    let _eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .query_params(
            &format!("SELECT id FROM {table} WHERE id = $1"),
            &[Value::Integer(a)],
        )
        .unwrap();
    assert!(rows.rows.is_empty());
}

#[test]
fn reconcile_drops_orphan_chunk_trees_without_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("v", AtomInput::new("fact", "x")).unwrap();
    eng.persist_ann_index("v").unwrap();
    drop(eng);

    // Crash after chunk commit, before metadata publication: chunks, no meta.
    let conn = Connection::open(&db).unwrap();
    let (seg_slot, _, _) = read_annseg_meta(&conn, region_id).unwrap().unwrap();
    for field in ["slot", "gen", "id"] {
        conn.execute_params(
            "DELETE FROM memory_meta WHERE key = $1",
            &[Value::Text(format!("annseg_{field}:{region_id}").into())],
        )
        .unwrap();
    }
    drop(conn);
    assert!(seg_chunks_exist(&db, region_id, 8), "probe sanity");
    assert_eq!(db.atom_store_slot(seg_slot).unwrap().state, SlotState::Live);

    let _eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert!(
        !seg_tree_exists(&db, region_id, 8),
        "meta-less chunk tree must be swept"
    );
    assert_eq!(
        db.atom_store_slot(seg_slot).unwrap().state,
        SlotState::Tombstone,
        "the unreferenced segment key must die"
    );
}

#[test]
fn content_changes_bump_the_cache_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("v", AtomInput::new("fact", "mutable"))
        .unwrap();

    let e0 = db.cache_epoch();
    eng.update_atom_payload("v", a, &serde_json::json!({"v": 2}))
        .unwrap();
    let e1 = db.cache_epoch();
    assert!(e1 > e0, "sealed payload rewrite must arm the epoch");

    assert_eq!(eng.set_importance("v", &[(a, 0.5)]).unwrap(), 1);
    let e2 = db.cache_epoch();
    assert!(e2 > e1, "an importance change must arm the epoch");
    assert_eq!(eng.set_importance("v", &[(a, 0.5)]).unwrap(), 0);
    assert_eq!(db.cache_epoch(), e2, "a converged pass writes nothing");

    eng.evolve("v", a, 1, f32::MAX).unwrap();
    assert!(db.cache_epoch() > e2, "evolve must arm the epoch");

    // Armed even on a failed write, and again after: no cache can stamp current between.
    let e3 = db.cache_epoch();
    assert!(db.atom_store_tombstone(u32::MAX, 1, 0).is_err());
    assert!(
        db.cache_epoch() >= e3 + 2,
        "tombstone must bump before AND after the attempt"
    );
}

#[test]
fn interrupted_erase_converges_after_a_genuine_disk_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let (secret, keep, region_id) = {
        let db = create_enc_db(dir.path());
        let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
        let region_id = eng
            .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
            .unwrap();
        let secret = eng.remember("v", AtomInput::new("fact", "doomed")).unwrap();
        let keep = eng.remember("v", AtomInput::new("fact", "kept")).unwrap();
        eng.persist_ann_index("v").unwrap();
        FAIL_ERASE_BEFORE_ROW_DELETE.with(|f| f.set(true));
        eng.forget_atoms("v", &[secret], false).unwrap_err();
        (secret, keep, region_id)
        // Everything drops here: the interrupted state is only on disk.
    };

    let db = open_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let residue = conn
        .query_params(
            &format!("SELECT id FROM {table} WHERE id = $1"),
            &[Value::Integer(secret)],
        )
        .unwrap();
    assert!(residue.rows.is_empty(), "disk reopen must finish the erase");
    assert!(read_annseg_meta(&conn, region_id).unwrap().is_none());
    drop(conn);
    assert!(eng.fetch_one("v", keep).unwrap().is_some());
    let hits: Vec<AtomId> = eng
        .recall("v", RecallQuery::by_text("doomed", 10))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert!(!hits.contains(&secret));
}

#[test]
fn reconcile_clears_incomplete_segment_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("v", AtomInput::new("fact", "x")).unwrap();
    eng.persist_ann_index("v").unwrap();
    drop(eng);

    // Break the record (older binary / direct SQL): one meta key disappears.
    let conn = Connection::open(&db).unwrap();
    let (seg_slot, _, _) = read_annseg_meta(&conn, region_id).unwrap().unwrap();
    conn.execute_params(
        "DELETE FROM memory_meta WHERE key = $1",
        &[Value::Text(format!("annseg_id:{region_id}").into())],
    )
    .unwrap();
    drop(conn);

    assert!(seg_chunks_exist(&db, region_id, 8), "probe sanity");
    let _eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert_eq!(
        db.atom_store_slot(seg_slot).unwrap().state,
        SlotState::Tombstone,
        "the unreferenced segment key must die"
    );
    let conn = Connection::open(&db).unwrap();
    let leftovers = conn
        .query_params("SELECT key FROM memory_meta WHERE key LIKE 'annseg_%'", &[])
        .unwrap();
    assert!(leftovers.rows.is_empty(), "incomplete metadata must clear");
    assert!(
        !seg_chunks_exist(&db, region_id, 8),
        "orphaned chunk tree must drop"
    );
}

#[test]
fn segment_of_a_removed_region_is_reclaimed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("v", AtomInput::new("fact", "x")).unwrap();
    eng.persist_ann_index("v").unwrap();
    drop(eng);

    // The parent region vanishes while the valid segment binding and tree survive.
    let conn = Connection::open(&db).unwrap();
    let (seg_slot, _, _) = read_annseg_meta(&conn, region_id).unwrap().unwrap();
    conn.execute_params(
        "DELETE FROM memory_regions WHERE id = $1",
        &[Value::Integer(region_id)],
    )
    .unwrap();
    drop(conn);
    assert!(seg_chunks_exist(&db, region_id, 8), "precondition: tree");

    let _eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert_eq!(
        db.atom_store_slot(seg_slot).unwrap().state,
        SlotState::Tombstone,
        "a region-less segment binding must not stay live"
    );
    let conn = Connection::open(&db).unwrap();
    let leftovers = conn
        .query_params("SELECT key FROM memory_meta WHERE key LIKE 'annseg_%'", &[])
        .unwrap();
    assert!(leftovers.rows.is_empty(), "region-less metadata must clear");
    drop(conn);
    // The physical catalog recovers the tree's name even with the parent gone.
    assert!(
        !seg_tree_exists(&db, region_id, 8),
        "a parentless chunk tree must be swept via the table inventory"
    );
}

#[test]
fn direct_tombstone_cannot_overlap_a_sealed_read_span() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("v", AtomInput::new("fact", "guarded"))
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let qr = conn
        .query_params(
            &format!("SELECT key_slot, key_gen FROM {table} WHERE id = $1"),
            &[Value::Integer(a)],
        )
        .unwrap();
    let slot = as_int(&qr.rows[0][0]).unwrap() as u32;
    let generation = as_int(&qr.rows[0][1]).unwrap() as u64;
    drop(conn);

    // The wrapper signals at its acquisition boundary; the barrier orders the span first.
    let (at_boundary, boundary) = std::sync::mpsc::channel::<()>();
    db.debug_set_destruction_acquire_hook(Some(Box::new(move || {
        let _ = at_boundary.send(());
    })));
    let order = Arc::new(Mutex::new(Vec::<&str>::new()));
    let start = Arc::new(std::sync::Barrier::new(2));
    let destroyer = {
        let db = Arc::clone(&db);
        let order = Arc::clone(&order);
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            // The acquiring wrapper must block until the read span ends.
            db.atom_store_tombstone(slot, a as u64, generation).unwrap();
            order.lock().unwrap().push("destroy");
        })
    };
    let h = eng.region_handle("v").unwrap();
    eng.with_live_sealed_read("v", &h, |_, _, _| {
        start.wait();
        // The held capability excludes the destroyer; the lock itself is the proof.
        boundary
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("destroyer must reach the acquisition boundary");
        assert!(
            order.lock().unwrap().is_empty(),
            "destruction completed inside the read span"
        );
        order.lock().unwrap().push("read-span-end");
        Ok(())
    })
    .unwrap();
    destroyer.join().unwrap();
    assert_eq!(*order.lock().unwrap(), ["read-span-end", "destroy"]);
}

#[test]
fn reconcile_retires_segments_whose_key_gen_moved() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("v", AtomInput::new("fact", "x")).unwrap();
    eng.persist_ann_index("v").unwrap();
    drop(eng);

    // The (slot, owner) pair still matches; only the gen bind catches it.
    let conn = Connection::open(&db).unwrap();
    let (seg_slot, _, _) = read_annseg_meta(&conn, region_id).unwrap().unwrap();
    conn.execute_params(
        "UPDATE memory_meta SET value = value + 1 WHERE key = $1",
        &[Value::Text(format!("annseg_gen:{region_id}").into())],
    )
    .unwrap();
    drop(conn);

    let _eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert_eq!(
        db.atom_store_slot(seg_slot).unwrap().state,
        SlotState::Tombstone
    );
    let conn = Connection::open(&db).unwrap();
    assert!(
        read_annseg_meta(&conn, region_id).unwrap().is_none(),
        "gen-drifted segment must retire"
    );
    assert!(!seg_chunks_exist(&db, region_id, 8));
}

#[test]
fn lifecycle_capabilities_are_bound_to_their_own_database() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let db_a = create_enc_db(dir_a.path());
    let db_b = create_enc_db(dir_b.path());
    let (slot_a, _) = db_a
        .atom_store_allocate_write(7, &[0x11; WRAPPED_KEY_SIZE])
        .unwrap();
    let (slot_b, generation_b) = db_b
        .atom_store_allocate_write(9, &[0x22; WRAPPED_KEY_SIZE])
        .unwrap();
    assert_eq!(slot_a, slot_b, "fresh stores hand out the same slot index");

    // Lifecycle spans are per-database: both capabilities held at once.
    let kl_a = db_a.key_lifecycle_lock();
    let kl_b = db_b.key_lifecycle_lock();
    // Destruction is a method OF the capability, so B's cannot be aimed at A.
    kl_b.atom_store_tombstone(slot_b, 9, generation_b).unwrap();
    drop(kl_b);
    drop(kl_a);

    assert_eq!(
        db_b.atom_store_slot(slot_b).unwrap().state,
        SlotState::Tombstone
    );
    let rec_a = db_a.atom_store_slot(slot_a).unwrap();
    assert_eq!(rec_a.state, SlotState::Live);
    assert_eq!(rec_a.region_id, 7);
}

#[test]
fn interrupted_batch_erase_normalizes_records_before_rows_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("v", AtomInput::new("fact", "torn one"))
        .unwrap();
    let b = eng
        .remember("v", AtomInput::new("fact", "torn two"))
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let qr = conn
        .query_params(&format!("SELECT id, key_slot FROM {table}"), &[])
        .unwrap();
    let slots: Vec<u32> = qr
        .rows
        .iter()
        .map(|r| as_int(&r[1]).unwrap() as u32)
        .collect();
    assert_eq!(slots.len(), 2);
    drop(conn);

    // Die after the live-copy update, before the sibling record update.
    db.debug_fail_next_atom_tombstone_batch_before_sibling();
    let err = eng.forget_atoms("v", &[a, b], false).unwrap_err();
    assert!(
        err.to_string().contains("before the sibling scrub"),
        "{err}"
    );
    drop(eng);

    // Torn boundary: duplicate records disagree and the rows still exist.
    for &slot in &slots {
        let states: Vec<Option<SlotState>> = db
            .debug_atom_slot_copies(slot)
            .unwrap()
            .iter()
            .map(|c| c.as_ref().map(|r| r.state))
            .collect();
        assert!(
            states.contains(&Some(SlotState::Tombstone))
                && states.iter().any(|s| *s != Some(SlotState::Tombstone)),
            "precondition: torn duplicate records, got {states:?}"
        );
    }

    // Same-process retry halted before row deletes: records normalized, rows intact.
    FAIL_RECONCILE_AFTER_DRIFT_KEYS.with(|f| f.set(true));
    assert!(MemoryEngine::open(Arc::clone(&db)).is_err());
    for &slot in &slots {
        let copies = db.debug_atom_slot_copies(slot).unwrap();
        assert!(
            copies
                .iter()
                .all(|c| c.as_ref().is_some_and(|r| r.state == SlotState::Tombstone)),
            "both duplicate records must be normalized, got {copies:?}"
        );
    }
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .query_params(&format!("SELECT id FROM {table}"), &[])
        .unwrap();
    assert_eq!(rows.rows.len(), 2, "normalization precedes row removal");
    drop(conn);

    // The clean retry converges: rows follow their dead keys.
    let _eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .query_params(&format!("SELECT id FROM {table}"), &[])
        .unwrap();
    assert!(rows.rows.is_empty());
}

#[test]
fn rows_of_a_removed_parent_region_reconcile_away() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let rid_a = eng
        .create_encrypted_region("a", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let rid_b = eng
        .create_encrypted_region("b", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert_ne!(rid_a, rid_b);
    let atom_a = eng
        .remember_if_absent_keyed(
            "a",
            AtomInput::new("fact", "doomed with region"),
            &[],
            None,
            "parent-bound",
        )
        .unwrap()
        .id;
    let atom_b = eng.remember("b", AtomInput::new("fact", "keeper")).unwrap();

    // Same dim and metric: both regions share ONE physical atoms table.
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT id, key_slot, key_gen FROM {table} ORDER BY id"),
            &[],
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 2, "precondition: a shared physical table");
    let binding = |aid: AtomId| {
        let r = qr
            .rows
            .iter()
            .find(|r| as_int(&r[0]).unwrap() == aid)
            .unwrap();
        (as_int(&r[1]).unwrap() as u32, as_int(&r[2]).unwrap() as u64)
    };
    let (slot_a, _) = binding(atom_a);
    let (slot_b, gen_b) = binding(atom_b);
    let rsk_a = {
        let qr = conn
            .query_params(
                "SELECT rsk_slot FROM memory_regions WHERE id = $1",
                &[Value::Integer(rid_a)],
            )
            .unwrap();
        as_int(&qr.rows[0][0]).unwrap() as u32
    };
    let idents = conn
        .query_params(
            "SELECT atom_id FROM memory_idempotency WHERE region_id = $1",
            &[Value::Integer(rid_a)],
        )
        .unwrap();
    assert_eq!(idents.rows.len(), 1, "precondition: identity record");
    // A's parent row vanishes; the key still matches, only the parent bind catches it.
    conn.execute_params(
        "DELETE FROM memory_regions WHERE id = $1",
        &[Value::Integer(rid_a)],
    )
    .unwrap();
    drop(conn);
    drop(eng);

    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .query_params(
            &format!("SELECT id FROM {table} WHERE id = $1"),
            &[Value::Integer(atom_a)],
        )
        .unwrap();
    assert!(
        rows.rows.is_empty(),
        "a parent-less row must reconcile away"
    );
    let idents = conn
        .query_params(
            "SELECT atom_id FROM memory_idempotency WHERE region_id = $1",
            &[Value::Integer(rid_a)],
        )
        .unwrap();
    assert!(idents.rows.is_empty(), "identity records die with the rows");
    drop(conn);
    assert_eq!(
        db.atom_store_slot(slot_a).unwrap().state,
        SlotState::Tombstone
    );
    assert_eq!(
        db.region_store_slot(rsk_a).unwrap().state,
        SlotState::Tombstone
    );

    // B is untouched: identical live binding and readable content.
    let rec_b = db.atom_store_slot(slot_b).unwrap();
    assert_eq!(rec_b.state, SlotState::Live);
    assert_eq!(rec_b.region_id, atom_b as u64);
    assert_eq!(rec_b.gen, gen_b);
    eng.create_encrypted_region("b", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert!(eng.fetch_one("b", atom_b).unwrap().is_some());
}

/// Break a persisted segment via `mutate`; reconcile reclaims it, the atoms survive.
fn assert_broken_segment_is_reclaimed(mutate: impl FnOnce(&Database, &str)) {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("v", AtomInput::new("fact", "x")).unwrap();
    eng.persist_ann_index("v").unwrap();
    drop(eng);

    let conn = Connection::open(&db).unwrap();
    let (seg_slot, _, _) = read_annseg_meta(&conn, region_id).unwrap().unwrap();
    drop(conn);
    assert!(seg_chunks_exist(&db, region_id, 8), "precondition: tree");
    assert_eq!(db.atom_store_slot(seg_slot).unwrap().state, SlotState::Live);
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    mutate(&db, &sealed_segment_table(&table, region_id));

    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert_eq!(
        db.atom_store_slot(seg_slot).unwrap().state,
        SlotState::Tombstone,
        "an invalid segment's key must die"
    );
    let conn = Connection::open(&db).unwrap();
    assert!(
        read_annseg_meta(&conn, region_id).unwrap().is_none(),
        "metadata clears only after the tree is handled"
    );
    drop(conn);
    assert!(
        !seg_tree_exists(&db, region_id, 8),
        "residue tree must drop"
    );
    // The atoms themselves are untouched by segment reclamation.
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert!(eng.fetch_one("v", a).unwrap().is_some());
}

#[test]
fn segment_metadata_without_a_tree_is_reclaimed() {
    assert_broken_segment_is_reclaimed(|db, seg| {
        let mut wtx = db.begin_write().unwrap();
        wtx.drop_table(seg.as_bytes()).unwrap();
        wtx.commit().unwrap();
    });
}

#[test]
fn segment_tree_without_a_count_record_is_reclaimed() {
    assert_broken_segment_is_reclaimed(|db, seg| {
        let mut wtx = db.begin_write().unwrap();
        assert!(wtx
            .table_delete(seg.as_bytes(), &0u32.to_be_bytes())
            .unwrap());
        wtx.commit().unwrap();
    });
}

#[test]
fn segment_tree_with_a_malformed_count_record_is_reclaimed() {
    assert_broken_segment_is_reclaimed(|db, seg| {
        let mut wtx = db.begin_write().unwrap();
        assert!(wtx
            .table_delete(seg.as_bytes(), &0u32.to_be_bytes())
            .unwrap());
        wtx.table_insert(seg.as_bytes(), &0u32.to_be_bytes(), &[0xAB, 0xCD])
            .unwrap();
        wtx.commit().unwrap();
    });
}

#[test]
fn segment_tree_missing_a_required_chunk_is_reclaimed() {
    assert_broken_segment_is_reclaimed(|db, seg| {
        let mut wtx = db.begin_write().unwrap();
        assert!(wtx
            .table_delete(seg.as_bytes(), &1u32.to_be_bytes())
            .unwrap());
        wtx.commit().unwrap();
    });
}

#[test]
fn unexpected_segment_probe_errors_propagate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("v", AtomInput::new("fact", "x")).unwrap();
    eng.persist_ann_index("v").unwrap();
    drop(eng);
    let conn = Connection::open(&db).unwrap();
    let (seg_slot, _, _) = read_annseg_meta(&conn, region_id).unwrap().unwrap();
    drop(conn);

    FAIL_SEGMENT_PROBE.with(|f| f.set(true));
    let err = match MemoryEngine::open(Arc::clone(&db)) {
        Ok(_) => panic!("open must fail on the injected probe error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("injected segment probe"), "{err}");

    // Nothing was classified or destroyed on the error path.
    assert_eq!(db.atom_store_slot(seg_slot).unwrap().state, SlotState::Live);
    assert!(seg_chunks_exist(&db, region_id, 8));
    let conn = Connection::open(&db).unwrap();
    assert!(read_annseg_meta(&conn, region_id).unwrap().is_some());
    drop(conn);

    // The clean retry validates the intact segment and serves the atom.
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert!(eng.fetch_one("v", a).unwrap().is_some());
}

#[test]
fn invalid_segment_id_or_generation_never_publishes_segment_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("v", AtomInput::new("fact", "retained"))
        .unwrap();
    let before = db.atom_store_live_bindings().unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute_params(
        "UPDATE memory_meta SET value = -1 WHERE key = 'next_atom_id'",
        &[],
    )
    .unwrap();
    drop(conn);

    let error = eng
        .persist_ann_index("v")
        .expect_err("a negative pseudo-id was truncated into a key owner");
    assert!(error.to_string().contains("negative next id"), "{error}");
    assert_eq!(db.atom_store_live_bindings().unwrap(), before);
    assert!(!seg_tree_exists(&db, region_id, 8));

    let conn = Connection::open(&db).unwrap();
    let error = write_annseg_meta(&conn, region_id, 0, u64::MAX, 1)
        .expect_err("an unrepresentable generation was written as a negative integer");
    assert!(
        error.to_string().contains("cannot be represented"),
        "{error}"
    );
    assert!(read_annseg_meta(&conn, region_id).unwrap().is_none());
}

#[test]
fn recall_and_evolve_accept_finite_extreme_event_times() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = if encrypted {
            create_enc_db(dir.path())
        } else {
            create_db(dir.path())
        };
        let eng = MemoryEngine::open(db).unwrap();
        let embedder = Arc::new(MockEmbedder::new(2));
        if encrypted {
            eng.create_encrypted_region("ages", embedder).unwrap();
        } else {
            eng.create_region("ages", embedder).unwrap();
        }
        let old = eng
            .remember(
                "ages",
                AtomInput::new("fact", "old")
                    .with_created_at(i64::MIN + 1)
                    .with_embedding(vec![1.0, 0.0]),
            )
            .unwrap();
        let future = eng
            .remember(
                "ages",
                AtomInput::new("fact", "future")
                    .with_created_at(i64::MAX - 1)
                    .with_embedding(vec![1.0, 0.0]),
            )
            .unwrap();
        let hits = eng
            .recall(
                "ages",
                RecallQuery::by_embedding(vec![1.0, 0.0], 2).with_weights(FusionWeights {
                    semantic: 0.0,
                    keyword: 0.0,
                    recency: 1.0,
                    importance: 0.0,
                }),
            )
            .unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.id).collect::<Vec<_>>(),
            [future, old]
        );
        assert_eq!(hits[0].relevance, Some(1.0));
        assert_eq!(hits[1].relevance, Some(0.0));
        assert_eq!(eng.evolve("ages", old, 0, 1.0).unwrap().importance, 0.0);
        assert!(eng
            .evolve("ages", future, 0, 1.0)
            .unwrap()
            .importance
            .is_finite());
    }
}

#[test]
fn semantic_recall_is_finite_with_extreme_stored_importance() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = if encrypted {
            create_enc_db(dir.path())
        } else {
            create_db(dir.path())
        };
        let eng = MemoryEngine::open(db).unwrap();
        let embedder = Arc::new(MockEmbedder::new(2));
        if encrypted {
            eng.create_encrypted_region("weights", embedder).unwrap();
        } else {
            eng.create_region("weights", embedder).unwrap();
        }
        eng.remember(
            "weights",
            AtomInput::new("fact", "far")
                .with_embedding(vec![0.0, 1.0])
                .with_importance(-f32::MAX),
        )
        .unwrap();
        let near = eng
            .remember(
                "weights",
                AtomInput::new("fact", "near")
                    .with_embedding(vec![1.0, 0.0])
                    .with_importance(f32::MAX),
            )
            .unwrap();
        let hits = eng
            .recall(
                "weights",
                RecallQuery::by_embedding(vec![1.0, 0.0], 2)
                    .with_weights(FusionWeights::semantic_only()),
            )
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, near);
        assert!(hits
            .iter()
            .all(|hit| hit.relevance.is_some_and(f32::is_finite)));
    }
}

#[test]
fn cancellation_after_batch_key_allocation_tombstones_every_new_slot() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("facts", Arc::new(MockEmbedder::new(2)))
        .unwrap();
    let retained = eng
        .remember("facts", AtomInput::new("fact", "retained"))
        .unwrap();
    let before = db.atom_store_live_bindings().unwrap();
    let token = citadel_core::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    CANCEL_AFTER_ATOM_KEY_ALLOCATION.with(|slot| {
        assert!(slot.borrow_mut().replace(token.clone()).is_none());
    });
    let atoms = (0..3)
        .map(|index| AtomInput::new("fact", format!("new fact {index}")))
        .collect();

    assert_mem_interrupted(eng.remember_batch("facts", atoms));
    assert!(token.is_cancelled());
    assert!(db.cancel_token().is_some_and(|token| token.is_cancelled()));
    assert!(CANCEL_AFTER_ATOM_KEY_ALLOCATION.with(|slot| slot.borrow().is_none()));
    db.set_cancel(None);
    assert_eq!(db.atom_store_live_bindings().unwrap(), before);
    assert_eq!(eng.count_region("facts").unwrap(), 1);
    assert!(eng.fetch_one("facts", retained).unwrap().is_some());

    let retried = eng
        .remember_batch(
            "facts",
            (0..3)
                .map(|index| AtomInput::new("fact", format!("new fact {index}")))
                .collect(),
        )
        .unwrap();
    assert_eq!(retried.len(), 3);
    assert_eq!(eng.count_region("facts").unwrap(), 4);
    assert_eq!(
        db.atom_store_live_bindings().unwrap().len(),
        before.len() + 3
    );
}

#[test]
fn a_negative_persisted_segment_owner_fails_closed_and_reconciles() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("v", AtomInput::new("fact", "retained"))
        .unwrap();
    eng.persist_ann_index("v").unwrap();
    let conn = Connection::open(&db).unwrap();
    let (segment_slot, _, _) = read_annseg_meta(&conn, region_id).unwrap().unwrap();
    conn.execute_params(
        "UPDATE memory_meta SET value = -1 WHERE key = $1",
        &[Value::Text(annseg_meta_key(region_id, "id").into())],
    )
    .unwrap();
    let error = read_annseg_meta(&conn, region_id)
        .expect_err("negative metadata was treated as an absent segment");
    assert!(error.to_string().contains("id is out of range"), "{error}");
    drop(conn);
    drop(eng);

    let reopened = MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert_eq!(
        db.atom_store_slot(segment_slot).unwrap().state,
        SlotState::Tombstone
    );
    assert!(!seg_tree_exists(&db, region_id, 8));
    assert!(read_annseg_meta(&Connection::open(&db).unwrap(), region_id)
        .unwrap()
        .is_none());
    reopened
        .attach_existing_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert_eq!(reopened.count_region("v").unwrap(), 1);
}

/// The atom's exact live `(slot, gen)` binding, read from its row.
fn atom_binding(db: &Database, table: &str, id: AtomId) -> (u32, u64) {
    let conn = Connection::open(db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT key_slot, key_gen FROM {table} WHERE id = $1"),
            &[Value::Integer(id)],
        )
        .unwrap();
    (
        as_int(&qr.rows[0][0]).unwrap() as u32,
        as_int(&qr.rows[0][1]).unwrap() as u64,
    )
}

#[test]
fn forged_segment_metadata_cannot_claim_an_atoms_binding() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("v", AtomInput::new("fact", "claimed"))
        .unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let (slot, gen) = atom_binding(&db, &table, a);
    drop(eng);

    // Forge metadata at the atom's exact binding; only the single-owner claim rejects it.
    let conn = Connection::open(&db).unwrap();
    write_annseg_meta(&conn, region_id, slot, gen, a).unwrap();
    drop(conn);
    let seg = sealed_segment_table(&table, region_id);
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(seg.as_bytes()).unwrap();
        wtx.table_insert(seg.as_bytes(), &0u32.to_be_bytes(), &1u32.to_le_bytes())
            .unwrap();
        wtx.table_insert(seg.as_bytes(), &1u32.to_be_bytes(), &[0xEE; 32])
            .unwrap();
        wtx.commit().unwrap();
    }
    assert!(seg_chunks_exist(&db, region_id, 8), "precondition: tree");

    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    // The atom's record is untouched; the forged claim lost everything.
    let rec = db.atom_store_slot(slot).unwrap();
    assert_eq!(rec.state, SlotState::Live);
    assert_eq!(rec.gen, gen, "the atom's binding must survive the forgery");
    let conn = Connection::open(&db).unwrap();
    assert!(read_annseg_meta(&conn, region_id).unwrap().is_none());
    drop(conn);
    assert!(!seg_tree_exists(&db, region_id, 8));
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert!(eng.fetch_one("v", a).unwrap().is_some());
}

#[test]
fn stale_segment_metadata_cannot_destroy_a_live_atom_on_retire() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("v", AtomInput::new("fact", "retire-safe"))
        .unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let (slot, gen) = atom_binding(&db, &table, a);

    // Forge metadata at the atom's triple; retire must refuse a binding with a row.
    let conn = Connection::open(&db).unwrap();
    write_annseg_meta(&conn, region_id, slot, gen, a).unwrap();
    drop(conn);

    // persist_ann_index retires the "previous segment" first.
    eng.persist_ann_index("v").unwrap();
    let rec = db.atom_store_slot(slot).unwrap();
    assert_eq!(rec.state, SlotState::Live);
    assert_eq!(rec.gen, gen, "retire must not act on forged metadata");
    // The real persist published a fresh segment under its own binding.
    let conn = Connection::open(&db).unwrap();
    let (seg_slot, _, pseudo_id) = read_annseg_meta(&conn, region_id).unwrap().unwrap();
    drop(conn);
    assert_ne!(seg_slot, slot);
    assert_ne!(pseudo_id, u64::try_from(a).unwrap());
    assert!(eng.fetch_one("v", a).unwrap().is_some());
}

#[test]
fn advanced_segment_tombstone_does_not_wedge_residue_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let atom = eng
        .remember("v", AtomInput::new("fact", "erase me"))
        .unwrap();
    eng.persist_ann_index("v").unwrap();
    let conn = Connection::open(&db).unwrap();
    let (slot, old_gen, owner) = read_annseg_meta(&conn, region_id).unwrap().unwrap();
    drop(conn);

    db.atom_store_tombstone(slot, owner, old_gen).unwrap();
    let (reused, successor_gen) = db
        .atom_store_allocate_write(owner, &[0x77; citadel_core::WRAPPED_KEY_SIZE])
        .unwrap();
    assert_eq!(reused, slot);
    db.atom_store_tombstone(reused, owner, successor_gen)
        .unwrap();

    let receipt = eng.forget_atoms("v", &[atom], false).unwrap();
    assert_eq!(receipt.rows_deleted, 1);
    assert_segment_retired(&db, region_id, 8);
    assert!(eng.fetch_one("v", atom).unwrap().is_none());
}

#[test]
fn forged_segment_metadata_cannot_claim_another_regions_atom() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let rid_a = eng
        .create_encrypted_region("a", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_encrypted_region("b", Arc::new(MockEmbedder::new(16)))
        .unwrap();
    eng.remember("a", AtomInput::new("fact", "local")).unwrap();
    let b1 = eng
        .remember("b", AtomInput::new("fact", "foreign"))
        .unwrap();
    let t16 = atoms_table(16, EmbeddingMetric::Cosine, true);
    let (slot_b, gen_b) = atom_binding(&db, &t16, b1);

    // Forge A's metadata at B's binding; the row-less check must span every table.
    let conn = Connection::open(&db).unwrap();
    write_annseg_meta(&conn, rid_a, slot_b, gen_b, b1).unwrap();
    drop(conn);

    // persist_ann_index("a") retires the "previous segment" first.
    eng.persist_ann_index("a").unwrap();
    let rec = db.atom_store_slot(slot_b).unwrap();
    assert_eq!(rec.state, SlotState::Live);
    assert_eq!(rec.gen, gen_b, "B's binding must survive A's retirement");
    assert_eq!(rec.region_id, b1 as u64);
    assert!(eng.fetch_one("b", b1).unwrap().is_some());
    // A still published its own fresh segment under its own binding.
    let conn = Connection::open(&db).unwrap();
    let (seg_slot, _, pseudo_id) = read_annseg_meta(&conn, rid_a).unwrap().unwrap();
    drop(conn);
    assert_ne!(seg_slot, slot_b);
    assert_ne!(pseudo_id, u64::try_from(b1).unwrap());
}

#[test]
fn regions_with_an_inexact_key_binding_are_not_valid_parents() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("v", AtomInput::new("fact", "gen-bound"))
        .unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let (atom_slot, _) = atom_binding(&db, &table, a);
    drop(eng);

    // rsk_gen drifts; only the gen bind catches it - the same bar as attachment.
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            "SELECT rsk_slot FROM memory_regions WHERE id = $1",
            &[Value::Integer(region_id)],
        )
        .unwrap();
    let rsk_slot = as_int(&qr.rows[0][0]).unwrap() as u32;
    conn.execute_params(
        "UPDATE memory_regions SET rsk_gen = rsk_gen + 1 WHERE id = $1",
        &[Value::Integer(region_id)],
    )
    .unwrap();
    drop(conn);

    let _eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert_eq!(
        db.region_store_slot(rsk_slot).unwrap().state,
        SlotState::Tombstone,
        "an inexact region binding must not stay live"
    );
    assert_eq!(
        db.atom_store_slot(atom_slot).unwrap().state,
        SlotState::Tombstone,
        "children of an unattachable region are reclaimed"
    );
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .query_params(
            &format!("SELECT id FROM {table} WHERE id = $1"),
            &[Value::Integer(a)],
        )
        .unwrap();
    assert!(rows.rows.is_empty());
}

#[test]
fn regions_without_a_recorded_gen_are_reclaimed_like_inexact_ones() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("v", AtomInput::new("fact", "no gen")).unwrap();
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let (atom_slot, _) = atom_binding(&db, &table, a);
    drop(eng);

    // A gen-less row can never attach (fails closed); reclaimed like any inexact binding.
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            "SELECT rsk_slot FROM memory_regions WHERE id = $1",
            &[Value::Integer(region_id)],
        )
        .unwrap();
    let rsk_slot = as_int(&qr.rows[0][0]).unwrap() as u32;
    conn.execute_params(
        "UPDATE memory_regions SET rsk_gen = NULL WHERE id = $1",
        &[Value::Integer(region_id)],
    )
    .unwrap();
    drop(conn);

    let _eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert_eq!(
        db.region_store_slot(rsk_slot).unwrap().state,
        SlotState::Tombstone
    );
    assert_eq!(
        db.atom_store_slot(atom_slot).unwrap().state,
        SlotState::Tombstone
    );
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .query_params(
            &format!("SELECT id FROM {table} WHERE id = $1"),
            &[Value::Integer(a)],
        )
        .unwrap();
    assert!(rows.rows.is_empty());
}

#[test]
fn forged_duplicate_ids_lose_their_row_but_not_the_graph() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_encrypted_region("w", Arc::new(MockEmbedder::new(16)))
        .unwrap();
    let a = eng.remember("v", AtomInput::new("fact", "source")).unwrap();
    eng.remember_derived("v", AtomInput::new("derived", "cites"), &[a], None)
        .unwrap();
    let w1 = eng.remember("w", AtomInput::new("fact", "other")).unwrap();
    drop(eng);

    let t16 = atoms_table(16, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let edges = conn
        .query_params(
            "SELECT src_id FROM memory_edges WHERE dst_id = $1",
            &[Value::Integer(a)],
        )
        .unwrap();
    assert_eq!(edges.rows.len(), 1, "precondition: a's provenance edge");
    // Forge a cross-table duplicate onto a's id; a's graph must not die with it.
    conn.execute_params(
        &format!("UPDATE {t16} SET id = $1 WHERE id = $2"),
        &[Value::Integer(a), Value::Integer(w1)],
    )
    .unwrap();
    drop(conn);

    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let conn = Connection::open(&db).unwrap();
    let dupes = conn
        .query_params(
            &format!("SELECT id FROM {t16} WHERE id = $1"),
            &[Value::Integer(a)],
        )
        .unwrap();
    assert!(dupes.rows.is_empty(), "the forged duplicate row must go");
    let edges = conn
        .query_params(
            "SELECT src_id FROM memory_edges WHERE dst_id = $1",
            &[Value::Integer(a)],
        )
        .unwrap();
    assert_eq!(edges.rows.len(), 1, "the surviving atom keeps its graph");
    drop(conn);
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert!(eng.fetch_one("v", a).unwrap().is_some());
}

#[test]
fn table_name_parsers_are_canonical() {
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    assert!(is_encrypted_atoms_table(&table));
    assert!(!is_encrypted_atoms_table("memory_atoms_d8_cosine"));
    assert!(!is_encrypted_atoms_table("memory_atoms_d08_cosine_enc"));
    assert!(!is_encrypted_atoms_table("memory_atoms_dx_cosine_enc"));
    assert!(!is_encrypted_atoms_table("memory_atoms_d8_flat_enc"));

    let seg = sealed_segment_table(&table, 7);
    assert_eq!(parse_sealed_segment_table(&seg), Some((7, table.as_str())));
    // SQL-layer plaintext index trees and prefix-sharing user tables are never claimed.
    assert_eq!(parse_sealed_segment_table("__annseg_rabbit"), None);
    assert_eq!(parse_sealed_segment_table("__annseg_r5__x"), None);
    assert_eq!(
        parse_sealed_segment_table("__annseg_r05__memory_atoms_d8_cosine_enc"),
        None
    );
}

#[test]
fn read_transaction_helper_enforces_read_only_mode_and_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE read_only_probe (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO read_only_probe VALUES (1, 7)")
        .unwrap();

    let error = with_read_txn(&conn, |c| {
        c.execute("UPDATE read_only_probe SET value = 8 WHERE id = 1")?;
        Ok(())
    })
    .expect_err("the read-only helper allowed a mutation");
    let message = error.to_string();
    assert!(
        message.contains("read-only") || message.contains("read only"),
        "expected a read-only error, got: {message}"
    );
    assert!(
        !conn.in_transaction(),
        "the failed read transaction was not rolled back"
    );

    let row = conn
        .prepare("SELECT value FROM read_only_probe WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(row.rows, vec![vec![Value::Integer(7)]]);
}

#[test]
fn identity_tag_persisted_format_is_frozen() {
    // Pins the persisted identity encoding: if it moves, bump the
    // IK_*_DOMAIN version tags instead of re-pinning.
    let atom = AtomInput::new("kind-a", "text-b")
        .with_payload(serde_json::json!({"p": 1}))
        .with_importance(0.5)
        .with_confidence(0.25)
        .with_created_at(123)
        .with_expires_at(456)
        .immutable()
        .with_embedding(vec![0.0, -0.0, 1.5]);
    let payload_json = serde_json::to_string(&atom.payload).unwrap();
    let evidence = serde_json::json!({"e": 2});

    let plain_key = identity_key_tag(None, &atom.kind, "key-1");
    let plain_req = identity_request_tag(
        None,
        &plain_key,
        &atom,
        &payload_json,
        &[7, 9],
        Some(&evidence),
    )
    .unwrap();
    let mac = IdentityMacKey { key: [0x42; 32] };
    let keyed_key = identity_key_tag(Some(&mac), &atom.kind, "key-1");
    let keyed_req = identity_request_tag(
        Some(&mac),
        &keyed_key,
        &atom,
        &payload_json,
        &[7, 9],
        Some(&evidence),
    )
    .unwrap();
    assert_eq!(
        plain_key,
        "c4b4b84a62057a3b3b5e6807cde597bb06f363aa15c7ae1201e2e7f68d3a9f0e"
    );
    assert_eq!(
        plain_req,
        "cc57721b05d378dc6c30856bcfe3a70632a1fa3cab1c69b945ff6a4373386572"
    );
    assert_eq!(
        keyed_key,
        "4b7e9ffa2b109dd90cbfe5c526930b3577fe09d2287e3b8e39d139d10bcebe9f"
    );
    assert_eq!(
        keyed_req,
        "61b07cf8d832b5dc0075a1f4023f4c0cd5ded74f66d865c4d4e26acc3e5b0c68"
    );
}

fn unit(dim: usize, axis: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; dim];
    v[axis] = 1.0;
    v
}

#[test]
fn supplied_embedding_is_stored_instead_of_embedding_the_text() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    // The texts are identical, so only the supplied vectors can tell them apart.
    eng.remember(
        "s",
        AtomInput::new("note", "same text").with_embedding(unit(8, 0)),
    )
    .unwrap();
    eng.remember(
        "s",
        AtomInput::new("note", "same text").with_embedding(unit(8, 7)),
    )
    .unwrap();

    let hits = eng
        .recall("s", RecallQuery::by_embedding(unit(8, 7), 1))
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert!(
        hits[0].distance.is_some_and(|distance| distance < 1e-3),
        "the text was embedded instead of the supplied vector (distance {})",
        hits[0].distance.unwrap()
    );
}

#[test]
fn supplied_embedding_of_the_wrong_dim_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    let err = eng
        .remember(
            "s",
            AtomInput::new("note", "x").with_embedding(vec![1.0, 0.0, 0.0]),
        )
        .unwrap_err();
    assert!(matches!(err, MemError::DimMismatch { .. }), "got {err:?}");
}

#[test]
fn batch_mixes_supplied_and_embedded_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    // The middle atom has no vector, so exactly one text reaches the embedder.
    let ids = eng
        .remember_batch(
            "s",
            vec![
                AtomInput::new("note", "supplied one").with_embedding(unit(8, 0)),
                AtomInput::new("note", "engine embeds this"),
                AtomInput::new("note", "supplied two").with_embedding(unit(8, 7)),
            ],
        )
        .unwrap();
    assert_eq!(ids.len(), 3);

    let hits = eng
        .recall("s", RecallQuery::by_embedding(unit(8, 7), 1))
        .unwrap();
    assert_eq!(hits[0].text, "supplied two", "wrong slot got the vector");
    assert!(
        hits[0].distance.is_some_and(|distance| distance < 1e-3),
        "distance {:?}",
        hits[0].distance
    );
}

#[test]
fn undefined_cosine_distance_is_none_in_plain_and_sealed_recall() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
        let embedder = Arc::new(MockEmbedder::new(8));
        if encrypted {
            eng.create_encrypted_region("r", embedder).unwrap();
        } else {
            eng.create_region("r", embedder).unwrap();
        }

        eng.remember(
            "r",
            AtomInput::new("note", "indexed seed").with_embedding(unit(8, 0)),
        )
        .unwrap();
        eng.recall("r", RecallQuery::by_embedding(unit(8, 0), 1))
            .unwrap();
        let zero = eng
            .remember(
                "r",
                AtomInput::new("note", "zero norm")
                    .with_importance(0.4)
                    .with_embedding(vec![0.0; 8]),
            )
            .unwrap();

        let hits = eng
            .recall("r", RecallQuery::by_embedding(unit(8, 0), 2))
            .unwrap();
        let hit = hits
            .iter()
            .find(|hit| hit.id == zero)
            .expect("zero-norm candidate remains visible");
        assert_eq!(hit.distance, None, "encrypted={encrypted}");
        assert!(hit.relevance.is_some(), "encrypted={encrypted}");
        assert_eq!(hit.importance, 0.4, "encrypted={encrypted}");
        assert_eq!(hit.graph_depth, None, "encrypted={encrypted}");
    }
}

#[test]
fn fetch_newest_takes_the_last_rows_still_ascending() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    for n in 0..5 {
        eng.remember("r", AtomInput::new("note", n.to_string()))
            .unwrap();
    }

    let q = FetchQuery::new(2).with_kind("note").newest();
    let texts: Vec<String> = eng
        .fetch_range("r", &q)
        .unwrap()
        .into_iter()
        .map(|h| h.text)
        .collect();
    assert_eq!(texts, vec!["3".to_string(), "4".to_string()]);
}

#[test]
fn fetch_page_only_returns_a_cursor_when_more_live_rows_exist() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
        if encrypted {
            eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(8)))
                .unwrap();
        } else {
            eng.create_region("r", Arc::new(MockEmbedder::new(8)))
                .unwrap();
        }
        let mut ids = Vec::new();
        for n in 0..4 {
            ids.push(
                eng.remember("r", AtomInput::new("note", n.to_string()))
                    .unwrap(),
            );
        }

        let first = eng.fetch_page("r", &FetchQuery::new(2)).unwrap();
        assert_eq!(first.atoms.len(), 2, "encrypted={encrypted}");
        assert_eq!(first.next_after_id, Some(ids[1]), "encrypted={encrypted}");
        let second = eng
            .fetch_page("r", &FetchQuery::new(2).with_after_id(ids[1]))
            .unwrap();
        assert_eq!(
            second.atoms.iter().map(|atom| atom.id).collect::<Vec<_>>(),
            ids[2..],
            "encrypted={encrypted}"
        );
        assert_eq!(second.next_after_id, None, "encrypted={encrypted}");

        let newest = eng.fetch_page("r", &FetchQuery::new(2).newest()).unwrap();
        assert_eq!(newest.next_after_id, None, "newest is a one-shot window");
    }
}

#[test]
fn fetch_newest_on_a_sealed_region_honours_the_payload_filter() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    // The filter runs after decryption, so the descending walk keeps paging.
    for n in 0..6 {
        let keep = n % 2 == 0;
        eng.remember(
            "s",
            AtomInput::new("note", n.to_string()).with_payload(serde_json::json!({ "keep": keep })),
        )
        .unwrap();
    }

    let mut q = FetchQuery::new(2).with_kind("note").newest();
    q.payload_filter = Some(serde_json::json!({ "keep": true }));
    let texts: Vec<String> = eng
        .fetch_range("s", &q)
        .unwrap()
        .into_iter()
        .map(|h| h.text)
        .collect();
    assert_eq!(texts, vec!["2".to_string(), "4".to_string()]);
}

fn keyed(kind: &str, text: &str, sid: &str) -> AtomInput {
    AtomInput::new(kind, text).with_payload(serde_json::json!({ "sid": sid }))
}

#[test]
fn a_keyed_replace_leaves_one_atom_per_key() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
        let embedder = Arc::new(MockEmbedder::new(8));
        if encrypted {
            eng.create_encrypted_region("r", embedder).unwrap();
        } else {
            eng.create_region("r", embedder).unwrap();
        }

        let first = eng
            .remember_replacing_keyed("r", keyed("note", "version one", "a"), "k")
            .unwrap();
        assert!(first.inserted, "encrypted={encrypted}");
        let second = eng
            .remember_replacing_keyed("r", keyed("note", "version two", "a"), "k")
            .unwrap();
        assert!(second.inserted, "encrypted={encrypted}");
        assert_ne!(first.id, second.id, "encrypted={encrypted}");

        let live = eng.fetch("r", "note", None, 100).unwrap();
        let texts: Vec<&str> = live.iter().map(|h| h.text.as_str()).collect();
        assert_eq!(texts, vec!["version two"], "encrypted={encrypted}");
        assert!(
            eng.fetch_one("r", first.id).unwrap().is_none(),
            "the superseded atom is still readable, encrypted={encrypted}"
        );
    }
}

#[test]
fn an_identical_keyed_replace_replays_instead_of_writing() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    let first = eng
        .remember_replacing_keyed("r", keyed("note", "same", "a"), "k")
        .unwrap();
    let retry = eng
        .remember_replacing_keyed("r", keyed("note", "same", "a"), "k")
        .unwrap();
    // A retry is not an edit: it converges on the stored atom rather than
    // superseding it, so the id a caller already holds stays valid.
    assert_eq!(retry.id, first.id);
    assert!(!retry.inserted);
    assert_eq!(eng.count("r", "note").unwrap(), 1);
}

#[test]
fn distinct_keys_replace_independently() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    for round in 0..3 {
        for slot in ["a", "b", "c"] {
            eng.remember_replacing_keyed(
                "r",
                keyed("note", &format!("{slot} round {round}"), slot),
                slot,
            )
            .unwrap();
        }
    }
    let mut texts: Vec<String> = eng
        .fetch("r", "note", None, 100)
        .unwrap()
        .into_iter()
        .map(|h| h.text)
        .collect();
    texts.sort();
    assert_eq!(texts, ["a round 2", "b round 2", "c round 2"]);
}

#[test]
fn concurrent_keyed_replaces_of_one_key_leave_one_row() {
    let dir = tempfile::tempdir().unwrap();
    let eng = Arc::new(MemoryEngine::open(create_enc_db(dir.path())).unwrap());
    eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    // Read-modify-write from outside lets every writer observe "absent" and
    // insert, leaving one key backed by as many rows as there were writers.
    let start = Arc::new(std::sync::Barrier::new(8));
    let writers: Vec<_> = (0..8)
        .map(|n| {
            let eng = Arc::clone(&eng);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                eng.remember_replacing_keyed("r", keyed("note", &format!("write {n}"), "a"), "k")
            })
        })
        .collect();
    let ids: Vec<AtomId> = writers
        .into_iter()
        .map(|w| w.join().unwrap().unwrap().id)
        .collect();

    let live = eng.fetch("r", "note", None, 100).unwrap();
    assert_eq!(live.len(), 1, "one key, {} rows: {live:?}", live.len());
    // The survivor is one of the writes, not a merge or a lost update.
    assert!(
        ids.contains(&live[0].id),
        "survivor {live:?} is not any write"
    );
}

#[test]
fn a_keyed_batch_replaces_and_inserts_in_one_pass() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember_replacing_keyed_batch(
        "r",
        vec![
            (keyed("note", "a first", "a"), "a".into()),
            (keyed("note", "b first", "b"), "b".into()),
        ],
    )
    .unwrap();

    // One batch spanning a replay, a replace and a fresh insert.
    let out = eng
        .remember_replacing_keyed_batch(
            "r",
            vec![
                (keyed("note", "a first", "a"), "a".into()),
                (keyed("note", "b second", "b"), "b".into()),
                (keyed("note", "c first", "c"), "c".into()),
            ],
        )
        .unwrap();
    assert_eq!(
        out.iter().map(|o| o.inserted).collect::<Vec<_>>(),
        vec![false, true, true]
    );

    let mut texts: Vec<String> = eng
        .fetch("r", "note", None, 100)
        .unwrap()
        .into_iter()
        .map(|h| h.text)
        .collect();
    texts.sort();
    assert_eq!(texts, ["a first", "b second", "c first"]);
}

#[test]
fn keyed_replace_preserves_the_binding_and_segment_when_the_old_atom_is_reserved() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let region_id = eng
        .create_encrypted_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let first = eng
        .remember_replacing_keyed("r", keyed("note", "first", "a"), "k")
        .unwrap();
    let lifecycle = db.key_lifecycle_lock();
    let reservation = lifecycle.reserve_memory_atom_callbacks(&[first.id as u64]);
    drop(lifecycle);
    let replay = eng
        .remember_replacing_keyed("r", keyed("note", "first", "a"), "k")
        .unwrap();
    assert_eq!(replay.id, first.id);
    assert!(!replay.inserted);
    drop(reservation);

    eng.persist_ann_index("r").unwrap();
    let lifecycle = db.key_lifecycle_lock();
    let reservation = lifecycle.reserve_memory_atom_callbacks(&[first.id as u64]);
    drop(lifecycle);

    let error = eng
        .remember_replacing_keyed("r", keyed("note", "second", "a"), "k")
        .unwrap_err();
    assert!(matches!(
        error,
        MemError::Core(citadel_core::Error::AtomInUse { atom_id }) if atom_id == first.id as u64
    ));
    assert!(seg_tree_exists(&db, region_id, 8));
    assert!(read_annseg_meta(&Connection::open(&db).unwrap(), region_id)
        .unwrap()
        .is_some());
    assert_eq!(eng.fetch_one("r", first.id).unwrap().unwrap().text, "first");

    drop(reservation);
    let replacement = eng
        .remember_replacing_keyed("r", keyed("note", "second", "a"), "k")
        .unwrap();
    assert_ne!(replacement.id, first.id);
    assert!(eng.fetch_one("r", first.id).unwrap().is_none());
    assert_segment_retired(&db, region_id, 8);
}

#[test]
fn one_key_twice_in_a_batch_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    // Two atoms for one key have no defensible answer, so it must not pick one.
    let err = eng
        .remember_replacing_keyed_batch(
            "r",
            vec![
                (keyed("note", "first", "a"), "dup".into()),
                (keyed("note", "second", "a"), "dup".into()),
            ],
        )
        .unwrap_err();
    assert!(format!("{err}").contains("twice in one batch"), "{err}");
    assert_eq!(eng.count("r", "note").unwrap(), 0, "the batch wrote anyway");
}

/// 300 nearer rows in another partition, against a first window of 64.
const BURIED_CHAFF: usize = 300;

/// A sealed region holding `BURIED_CHAFF` rows that outrank one filtered target.
fn buried_target_region(dir: &std::path::Path) -> MemoryEngine {
    let eng = MemoryEngine::open(create_enc_db(dir)).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    // Distinct but near-parallel to the query, so every one of them outranks the
    // target and no two share a vector.
    let chaff: Vec<AtomInput> = (0..BURIED_CHAFF)
        .map(|i| {
            let mut v = vec![0.0f32; 8];
            v[0] = 1.0;
            v[1] = i as f32 * 1e-4;
            AtomInput::new("note", format!("noise {i}"))
                .with_payload(serde_json::json!({ "sid": "noisy" }))
                .with_embedding(v)
        })
        .collect();
    eng.remember_batch("s", chaff).unwrap();
    let mut far = vec![0.0f32; 8];
    far[1] = 1.0;
    eng.remember_batch(
        "s",
        vec![AtomInput::new("note", "the one that matters")
            .with_payload(serde_json::json!({ "sid": "quiet" }))
            .with_embedding(far)],
    )
    .unwrap();
    eng
}

fn near_query() -> Vec<f32> {
    let mut v = vec![0.0f32; 8];
    v[0] = 1.0;
    v
}

#[test]
fn sealed_recall_widens_past_the_candidate_window_to_satisfy_a_payload_filter() {
    let dir = tempfile::tempdir().unwrap();
    let eng = buried_target_region(dir.path());

    // Control: the target really is out of reach of the first window, so this
    // test cannot pass by the target happening to rank well.
    let unfiltered = eng
        .recall("s", RecallQuery::by_embedding(near_query(), 1))
        .unwrap();
    assert_eq!(unfiltered.len(), 1);
    assert!(
        unfiltered[0].text.starts_with("noise "),
        "chaff must outrank the target, got {:?}",
        unfiltered[0].text
    );

    let hits = eng
        .recall(
            "s",
            RecallQuery::by_embedding(near_query(), 1)
                .with_payload_filter(serde_json::json!({ "sid": "quiet" })),
        )
        .unwrap();
    let texts: Vec<&str> = hits.iter().map(|h| h.text.as_str()).collect();
    assert_eq!(texts, vec!["the one that matters"]);
}

#[test]
fn sealed_recall_stops_widening_once_the_window_spans_the_region() {
    let dir = tempfile::tempdir().unwrap();
    let eng = buried_target_region(dir.path());
    // No atom carries this, so every widening step comes back empty. The loop
    // has to end on the region rather than on a satisfied filter.
    let hits = eng
        .recall(
            "s",
            RecallQuery::by_embedding(near_query(), 1)
                .with_payload_filter(serde_json::json!({ "sid": "absent" })),
        )
        .unwrap();
    assert!(hits.is_empty(), "got {hits:?}");
}

#[test]
fn sealed_recall_widens_past_a_window_of_expired_atoms() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    // Expiry is read off the decrypted atom, so the dead rows are ranked first
    // and dropped afterwards, exactly as a payload filter would be.
    let expired = micros_now() - 1;
    let dead: Vec<AtomInput> = (0..BURIED_CHAFF)
        .map(|i| {
            let mut v = vec![0.0f32; 8];
            v[0] = 1.0;
            v[1] = i as f32 * 1e-4;
            AtomInput::new("note", format!("expired {i}"))
                .with_expires_at(expired)
                .with_embedding(v)
        })
        .collect();
    eng.remember_batch("s", dead).unwrap();
    let mut far = vec![0.0f32; 8];
    far[1] = 1.0;
    eng.remember_batch(
        "s",
        vec![AtomInput::new("note", "still alive").with_embedding(far)],
    )
    .unwrap();

    let hits = eng
        .recall("s", RecallQuery::by_embedding(near_query(), 1))
        .unwrap();
    let texts: Vec<&str> = hits.iter().map(|h| h.text.as_str()).collect();
    assert_eq!(texts, vec!["still alive"]);
}

#[test]
fn sealed_recall_widens_past_a_window_of_superseded_atoms() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    // Supersession is a row relation resolved after ranking, so stale versions
    // occupy the window and are removed from it.
    let mut far = vec![0.0f32; 8];
    far[1] = 1.0;
    let current = eng
        .remember_batch(
            "s",
            vec![AtomInput::new("note", "the current version").with_embedding(far)],
        )
        .unwrap()[0];
    for i in 0..BURIED_CHAFF {
        let mut v = vec![0.0f32; 8];
        v[0] = 1.0;
        v[1] = i as f32 * 1e-4;
        let stale = eng
            .remember(
                "s",
                AtomInput::new("note", format!("old {i}")).with_embedding(v),
            )
            .unwrap();
        eng.link(current, stale, EdgeKind::Supersedes, 1.0).unwrap();
    }

    let hits = eng
        .recall("s", RecallQuery::by_embedding(near_query(), 1))
        .unwrap();
    let texts: Vec<&str> = hits.iter().map(|h| h.text.as_str()).collect();
    assert_eq!(texts, vec!["the current version"]);
}

#[test]
fn sealed_recall_returns_every_filtered_match_it_can_reach() {
    let dir = tempfile::tempdir().unwrap();
    let eng = buried_target_region(dir.path());
    // Fewer matches than asked for: widening must run to the end of the region
    // and still answer with the one that exists.
    let hits = eng
        .recall(
            "s",
            RecallQuery::by_embedding(near_query(), 5)
                .with_payload_filter(serde_json::json!({ "sid": "quiet" })),
        )
        .unwrap();
    let texts: Vec<&str> = hits.iter().map(|h| h.text.as_str()).collect();
    assert_eq!(texts, vec!["the one that matters"]);
}

#[test]
fn fetch_newest_respects_after_id_as_a_lower_bound() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let mut ids = Vec::new();
    for n in 0..4 {
        ids.push(
            eng.remember("s", AtomInput::new("note", n.to_string()))
                .unwrap(),
        );
    }

    // Walking down, `after_id` is a fixed bound, not the ascending watermark.
    let q = FetchQuery::new(10)
        .with_kind("note")
        .with_after_id(ids[1])
        .newest();
    let texts: Vec<String> = eng
        .fetch_range("s", &q)
        .unwrap()
        .into_iter()
        .map(|h| h.text)
        .collect();
    assert_eq!(texts, vec!["2".to_string(), "3".to_string()]);
}
