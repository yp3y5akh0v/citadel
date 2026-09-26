//! Engine primitives: keyed idempotent ingest over the identity table,
//! snapshot-validated derived writes, and atomic dependency-closure forgetting.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Barrier,
};

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{
    AtomInput, EdgeKind, EmbedError, Embedder, EmbeddingMetric, EvictionPolicy, MemError,
    MemoryEngine, MockEmbedder, SourceSnapshot,
};
use citadel_sql::{Connection, Value};
use serde_json::json;
use sha2::{Digest, Sha256};

const DIM: usize = 64;
const PAST: i64 = 1_000_000; // 1970: any wall clock is past this
const TABLE: &str = "memory_atoms_d64_cosine_enc";

struct CountingEmbedder {
    calls: Arc<AtomicUsize>,
}

impl Embedder for CountingEmbedder {
    fn dim(&self) -> usize {
        DIM
    }

    fn metric(&self) -> EmbeddingMetric {
        EmbeddingMetric::Cosine
    }

    fn model_id(&self) -> &str {
        "counting-keyed-batch-v1"
    }

    fn embed_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        if cancel.is_some_and(citadel_core::CancelToken::is_cancelled) {
            return Err(EmbedError::Interrupted);
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(texts
            .iter()
            .map(|text| {
                let mut vector = vec![0.0; DIM];
                vector[text.len() % DIM] = 1.0;
                vector
            })
            .collect())
    }
}

fn build_db(dir: &std::path::Path, encrypted: bool) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(dir.join("m.cdl"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(encrypted)
            .create()
            .unwrap(),
    )
}

fn engine(db: &Arc<Database>, encrypted: bool) -> MemoryEngine {
    let eng = MemoryEngine::open(Arc::clone(db)).unwrap();
    if encrypted {
        eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(DIM)))
            .unwrap();
    } else {
        eng.create_region("r", Arc::new(MockEmbedder::new(DIM)))
            .unwrap();
    }
    eng
}

fn snap(id: i64, text: &str) -> SourceSnapshot {
    SourceSnapshot {
        id,
        text_sha256: Sha256::digest(text.as_bytes()).into(),
    }
}

/// Identity records on disk, read beneath the engine API.
fn identity_rows(db: &Database) -> i64 {
    let conn = Connection::open(db).unwrap();
    let qr = conn
        .query_params("SELECT COUNT(*) FROM memory_idempotency", &[])
        .unwrap();
    match &qr.rows[0][0] {
        Value::Integer(n) => *n,
        other => panic!("count is not an integer: {other:?}"),
    }
}

/// Forget a keyed atom in region "r", then restore its row and identity
/// record verbatim: the exact state an interrupted keyed forget leaves
/// (key slot erased, committed rows survive).
fn plant_keyed_residue(db: &Arc<Database>, eng: &MemoryEngine, victim: i64) {
    let conn = Connection::open(db).unwrap();
    let row = conn
        .query_params(
            &format!(
                "SELECT region_id, kind, sealed, key_slot, key_gen FROM {TABLE} WHERE id = $1"
            ),
            &[Value::Integer(victim)],
        )
        .unwrap()
        .rows[0]
        .clone();
    let ident = conn
        .query_params(
            "SELECT region_id, kind, key_mac, request_mac FROM memory_idempotency \
             WHERE atom_id = $1",
            &[Value::Integer(victim)],
        )
        .unwrap()
        .rows[0]
        .clone();
    eng.forget_atoms("r", &[victim], false).unwrap();
    conn.execute_params(
        &format!(
            "INSERT INTO {TABLE} \
             (id, region_id, kind, sealed, key_slot, key_gen, score, confidence, \
              access_count, immutable, created_at, accessed_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, 0.0, 1.0, 0, 0, \
              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)"
        ),
        &[
            Value::Integer(victim),
            row[0].clone(),
            row[1].clone(),
            row[2].clone(),
            row[3].clone(),
            row[4].clone(),
        ],
    )
    .unwrap();
    conn.execute_params(
        "INSERT INTO memory_idempotency (region_id, kind, key_mac, request_mac, atom_id) \
         VALUES ($1, $2, $3, $4, $5)",
        &[
            ident[0].clone(),
            ident[1].clone(),
            ident[2].clone(),
            ident[3].clone(),
            Value::Integer(victim),
        ],
    )
    .unwrap();
}

#[test]
fn keyed_ingest_separates_identical_texts_and_converges_on_retry() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = build_db(dir.path(), encrypted);
        let eng = engine(&db, encrypted);
        let src = eng
            .remember("r", AtomInput::new("turn", "the source"))
            .unwrap();
        let atom = || AtomInput::new("fact", "yes").with_payload(json!({"p": 1}));
        let ev = Some(json!({"quote": "q"}));

        let a = eng
            .remember_if_absent_keyed("r", atom(), &[src], ev.clone(), "s1:t1")
            .unwrap();
        let b = eng
            .remember_if_absent_keyed("r", atom(), &[src], ev.clone(), "s1:t2")
            .unwrap();
        assert!(a.inserted && b.inserted);
        assert_ne!(a.id, b.id, "identical texts under different keys");

        let edges_before = eng
            .fetch_edges_in_region("r", Some(a.id), None, None, 10)
            .unwrap()
            .len();
        let retry = eng
            .remember_if_absent_keyed("r", atom(), &[src], ev.clone(), "s1:t1")
            .unwrap();
        assert!(!retry.inserted);
        assert_eq!(retry.id, a.id, "same key converges");
        assert_eq!(
            eng.fetch_edges_in_region("r", Some(a.id), None, None, 10)
                .unwrap()
                .len(),
            edges_before,
            "a retry never touches the original atom's provenance"
        );

        // The payload is exactly the caller's - no reserved marker injected.
        let stored = eng.fetch_one("r", a.id).unwrap().unwrap();
        assert_eq!(stored.payload, json!({"p": 1}));

        // The key is scoped per kind: another kind is another namespace.
        let other = eng
            .remember_if_absent_keyed("r", AtomInput::new("note", "yes"), &[], None, "s1:t1")
            .unwrap();
        assert!(other.inserted, "keys are scoped to (region, kind)");

        // No visible equality on disk: identical requests under different
        // keys and one key reused across kinds all store distinct MACs.
        let conn = Connection::open(&db).unwrap();
        let qr = conn
            .query_params("SELECT key_mac, request_mac FROM memory_idempotency", &[])
            .unwrap();
        let mut key_macs: Vec<String> = Vec::new();
        let mut request_macs: Vec<String> = Vec::new();
        for row in &qr.rows {
            match (&row[0], &row[1]) {
                (Value::Text(k), Value::Text(r)) => {
                    key_macs.push(k.to_string());
                    request_macs.push(r.to_string());
                }
                bad => panic!("unexpected identity row: {bad:?}"),
            }
        }
        assert_eq!(key_macs.len(), 3);
        key_macs.sort();
        key_macs.dedup();
        assert_eq!(key_macs.len(), 3, "kind-bound key MACs never collide");
        request_macs.sort();
        request_macs.dedup();
        assert_eq!(
            request_macs.len(),
            3,
            "identical requests under different keys leave no equality tag"
        );
    }
}

#[test]
fn keyed_retry_with_any_changed_input_fails() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = build_db(dir.path(), encrypted);
        let eng = engine(&db, encrypted);
        let s1 = eng.remember("r", AtomInput::new("turn", "one")).unwrap();
        let s2 = eng.remember("r", AtomInput::new("turn", "two")).unwrap();
        let base = AtomInput::new("fact", "v").with_payload(json!({"p": 1}));
        let ev = Some(json!({"e": 1}));
        let original = eng
            .remember_if_absent_keyed("r", base.clone(), &[s1], ev.clone(), "k")
            .unwrap();

        let edited: Vec<(AtomInput, Vec<i64>, Option<serde_json::Value>)> = vec![
            (
                AtomInput::new("fact", "v CHANGED").with_payload(json!({"p": 1})),
                vec![s1],
                ev.clone(),
            ),
            (
                AtomInput::new("fact", "v").with_payload(json!({"p": 2})),
                vec![s1],
                ev.clone(),
            ),
            (base.clone().with_importance(0.5), vec![s1], ev.clone()),
            (base.clone().with_confidence(0.25), vec![s1], ev.clone()),
            (base.clone().with_created_at(123), vec![s1], ev.clone()),
            (base.clone().with_expires_at(i64::MAX), vec![s1], ev.clone()),
            (base.clone().immutable(), vec![s1], ev.clone()),
            (
                base.clone().with_embedding(vec![0.0; DIM]),
                vec![s1],
                ev.clone(),
            ),
            (base.clone(), vec![s2], ev.clone()),
            (base.clone(), vec![s1], Some(json!({"e": 2}))),
            (base.clone(), vec![s1], None),
        ];
        for (atom, sources, evidence) in edited {
            let err = eng
                .remember_if_absent_keyed("r", atom, &sources, evidence, "k")
                .unwrap_err();
            assert!(err.to_string().contains("different request"), "{err}");
        }

        // The rejections aliased nothing: the identical request still
        // converges on the untouched original.
        let again = eng
            .remember_if_absent_keyed("r", base.clone(), &[s1], ev.clone(), "k")
            .unwrap();
        assert!(!again.inserted);
        assert_eq!(again.id, original.id);
        assert_eq!(
            eng.fetch_edges_in_region("r", Some(original.id), None, None, 10)
                .unwrap()
                .len(),
            1
        );
    }
}

#[test]
fn keyed_batch_replays_in_order_and_conflicts_before_any_insert() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = build_db(dir.path(), encrypted);
        let eng = engine(&db, encrypted);
        let entries = || {
            vec![
                (
                    AtomInput::new("fact", "first")
                        .with_payload(json!({"position": 1}))
                        .with_importance(0.25),
                    "batch/first".to_string(),
                ),
                (
                    AtomInput::new("fact", "second")
                        .with_payload(json!({"position": 2}))
                        .with_confidence(0.75),
                    "batch/second".to_string(),
                ),
            ]
        };

        let first = eng.remember_if_absent_keyed_batch("r", entries()).unwrap();
        assert_eq!(first.len(), 2);
        assert!(first.iter().all(|outcome| outcome.inserted));
        assert!(first[0].id < first[1].id, "outcomes preserve input order");

        let replay = eng.remember_if_absent_keyed_batch("r", entries()).unwrap();
        assert_eq!(
            replay.iter().map(|outcome| outcome.id).collect::<Vec<_>>(),
            first.iter().map(|outcome| outcome.id).collect::<Vec<_>>()
        );
        assert!(replay.iter().all(|outcome| !outcome.inserted));

        let mixed = eng
            .remember_if_absent_keyed_batch(
                "r",
                vec![
                    (
                        AtomInput::new("fact", "second")
                            .with_payload(json!({"position": 2}))
                            .with_confidence(0.75),
                        "batch/second".to_string(),
                    ),
                    (AtomInput::new("fact", "third"), "batch/third".to_string()),
                    (
                        AtomInput::new("fact", "first")
                            .with_payload(json!({"position": 1}))
                            .with_importance(0.25),
                        "batch/first".to_string(),
                    ),
                ],
            )
            .unwrap();
        assert_eq!(mixed[0], replay[1]);
        assert!(mixed[1].inserted);
        assert_eq!(mixed[2], replay[0]);

        let before = eng.count("r", "fact").unwrap();
        let error = eng
            .remember_if_absent_keyed_batch(
                "r",
                vec![
                    (
                        AtomInput::new("fact", "must roll back"),
                        "batch/fresh".to_string(),
                    ),
                    (
                        AtomInput::new("fact", "changed second"),
                        "batch/second".to_string(),
                    ),
                ],
            )
            .unwrap_err();
        assert!(matches!(
            error,
            MemError::IdempotencyConflict { atom_id } if atom_id == first[1].id
        ));
        assert_eq!(
            eng.count("r", "fact").unwrap(),
            before,
            "a later conflict must prevent an earlier fresh entry from committing"
        );

        let fresh = eng
            .remember_if_absent_keyed_batch(
                "r",
                vec![(
                    AtomInput::new("fact", "must roll back"),
                    "batch/fresh".to_string(),
                )],
            )
            .unwrap();
        assert!(fresh[0].inserted, "the rejected batch did not bind its key");
    }
}

#[test]
fn keyed_batch_rejects_missing_or_duplicate_keys_without_writing() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), false);
    let eng = engine(&db, false);

    for entries in [
        vec![(AtomInput::new("fact", "empty"), String::new())],
        vec![
            (AtomInput::new("fact", "one"), "duplicate".to_string()),
            (AtomInput::new("note", "two"), "duplicate".to_string()),
        ],
    ] {
        assert!(matches!(
            eng.remember_if_absent_keyed_batch("r", entries),
            Err(MemError::Invalid(_))
        ));
    }
    assert_eq!(eng.count("r", "fact").unwrap(), 0);
    assert_eq!(eng.count("r", "note").unwrap(), 0);
    assert_eq!(identity_rows(&db), 0);
}

#[test]
fn keyed_single_replay_and_conflict_do_not_run_the_embedder_again() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), false);
    let calls = Arc::new(AtomicUsize::new(0));
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_region(
        "r",
        Arc::new(CountingEmbedder {
            calls: Arc::clone(&calls),
        }),
    )
    .unwrap();

    let atom = || AtomInput::new("fact", "one");
    let first = eng
        .remember_if_absent_keyed("r", atom(), &[], None, "one")
        .unwrap();
    assert!(first.inserted);
    assert_eq!(calls.load(Ordering::Relaxed), 1);

    let replay = eng
        .remember_if_absent_keyed("r", atom(), &[], None, "one")
        .unwrap();
    assert_eq!(replay.id, first.id);
    assert!(!replay.inserted);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "an identical retry resolves before embedding"
    );

    let error = eng
        .remember_if_absent_keyed("r", AtomInput::new("fact", "changed"), &[], None, "one")
        .unwrap_err();
    assert!(matches!(error, MemError::IdempotencyConflict { .. }));
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "a conflicting retry fails before embedding"
    );
}

#[test]
fn keyed_batch_replay_and_conflict_do_not_run_the_embedder_again() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), false);
    let calls = Arc::new(AtomicUsize::new(0));
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_region(
        "r",
        Arc::new(CountingEmbedder {
            calls: Arc::clone(&calls),
        }),
    )
    .unwrap();
    let entries = || {
        vec![
            (AtomInput::new("fact", "one"), "one".to_string()),
            (AtomInput::new("fact", "two"), "two".to_string()),
        ]
    };

    eng.remember_if_absent_keyed_batch("r", entries()).unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    eng.remember_if_absent_keyed_batch("r", entries()).unwrap();
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "an all-replay batch resolves from its identity snapshot"
    );

    let error = eng
        .remember_if_absent_keyed_batch(
            "r",
            vec![
                (AtomInput::new("fact", "fresh"), "fresh".to_string()),
                (AtomInput::new("fact", "changed"), "two".to_string()),
            ],
        )
        .unwrap_err();
    assert!(matches!(error, MemError::IdempotencyConflict { .. }));
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "preflight finds a conflict even when an earlier entry is fresh"
    );
}

#[test]
fn keyed_identity_rebinds_after_removal() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = build_db(dir.path(), encrypted);
        let eng = engine(&db, encrypted);

        // Table-level proof: the lazy self-heal would mask a missing purge
        // on the rebind outcome alone.
        let a = eng
            .remember_if_absent_keyed("r", AtomInput::new("fact", "gone"), &[], None, "k")
            .unwrap();
        assert_eq!(identity_rows(&db), 1);
        eng.forget_atoms("r", &[a.id], false).unwrap();
        assert_eq!(identity_rows(&db), 0, "forget purged the identity record");
        let b = eng
            .remember_if_absent_keyed("r", AtomInput::new("fact", "gone"), &[], None, "k")
            .unwrap();
        assert!(b.inserted, "identity record died with the atom");
        assert_ne!(b.id, a.id);

        // Eviction purges identity records too, proven at the table level
        // (an expired atom already counts as absent for the rebind check).
        let brief = AtomInput::new("fact", "brief").with_expires_at(PAST);
        let c = eng
            .remember_if_absent_keyed("r", brief.clone(), &[], None, "k2")
            .unwrap();
        assert_eq!(identity_rows(&db), 2);
        assert_eq!(eng.evict("r", EvictionPolicy::Expired).unwrap().removed, 1);
        assert_eq!(identity_rows(&db), 1, "eviction purged the identity record");
        let d = eng
            .remember_if_absent_keyed("r", brief.clone(), &[], None, "k2")
            .unwrap();
        assert!(d.inserted);
        assert_ne!(d.id, c.id);

        // A lapsed-but-unevicted atom counts as absent: the identical retry
        // self-heals the record and stores fresh.
        let e = eng
            .remember_if_absent_keyed("r", brief.clone(), &[], None, "k3")
            .unwrap();
        let f = eng
            .remember_if_absent_keyed("r", brief, &[], None, "k3")
            .unwrap();
        assert!(f.inserted, "expired atom counts as absent");
        assert_ne!(f.id, e.id);
    }
}

#[test]
fn keyed_identity_dies_with_the_region() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), false);
    let eng = engine(&db, false);
    eng.remember_if_absent_keyed("r", AtomInput::new("fact", "x"), &[], None, "k")
        .unwrap();
    assert_eq!(identity_rows(&db), 1);

    eng.drop_region("r").unwrap();
    assert_eq!(
        identity_rows(&db),
        0,
        "region drop purges its identity rows"
    );

    eng.create_region("r", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let again = eng
        .remember_if_absent_keyed("r", AtomInput::new("fact", "x"), &[], None, "k")
        .unwrap();
    assert!(
        again.inserted,
        "recreated region starts with a clean namespace"
    );
}

#[test]
fn identity_table_rejects_duplicate_atom_mappings() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), false);
    let eng = engine(&db, false);
    let a = eng
        .remember_if_absent_keyed("r", AtomInput::new("fact", "x"), &[], None, "k")
        .unwrap();

    // The unique index refuses a second mapping to the same atom, so sweep
    // deletes can never over-match - even across regions.
    let conn = Connection::open(&db).unwrap();
    let dup = conn.execute_params(
        "INSERT INTO memory_idempotency (region_id, kind, key_mac, request_mac, atom_id) \
         VALUES (999, 'fact', 'other-key-mac', 'other-request-mac', $1)",
        &[Value::Integer(a.id)],
    );
    assert!(dup.is_err(), "duplicate atom_id mapping must be rejected");
    assert_eq!(identity_rows(&db), 1);
}

#[test]
fn keyed_rejects_an_empty_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), false);
    let eng = engine(&db, false);
    assert!(matches!(
        eng.remember_if_absent_keyed("r", AtomInput::new("fact", "x"), &[], None, ""),
        Err(MemError::Invalid(_))
    ));
}

#[test]
fn checked_write_commits_only_against_the_declared_snapshot() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = build_db(dir.path(), encrypted);
        let eng = engine(&db, encrypted);
        let t1 = eng.remember("r", AtomInput::new("turn", "alpha")).unwrap();
        let t2 = eng.remember("r", AtomInput::new("turn", "beta")).unwrap();

        // Edges follow the provenance list, not the snapshot.
        let fact = eng
            .remember_derived_checked(
                "r",
                AtomInput::new("derived", "a fact"),
                &[snap(t1, "alpha"), snap(t2, "beta")],
                &[t1],
                None,
            )
            .unwrap();
        let edges = eng
            .fetch_edges_in_region("r", Some(fact), None, Some(EdgeKind::DerivedFrom), 10)
            .unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].dst_id, t1);

        // A drifted digest on an UNCITED snapshot member still refuses.
        let err = eng
            .remember_derived_checked(
                "r",
                AtomInput::new("derived", "stale"),
                &[snap(t1, "alpha"), snap(t2, "beta but edited")],
                &[t1],
                None,
            )
            .unwrap_err();
        assert!(err.to_string().contains("changed since"), "{err}");

        // Provenance must be declared in the snapshot.
        let err = eng
            .remember_derived_checked(
                "r",
                AtomInput::new("derived", "uncited"),
                &[snap(t1, "alpha")],
                &[t2],
                None,
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("not in the declared snapshot"),
            "{err}"
        );

        // Duplicate snapshot ids are ambiguous.
        let err = eng
            .remember_derived_checked(
                "r",
                AtomInput::new("derived", "dup"),
                &[snap(t1, "alpha"), snap(t1, "alpha")],
                &[t1],
                None,
            )
            .unwrap_err();
        assert!(err.to_string().contains("duplicate snapshot id"), "{err}");

        // An absent snapshot member refuses.
        eng.forget_atoms("r", &[t2], false).unwrap();
        let err = eng
            .remember_derived_checked(
                "r",
                AtomInput::new("derived", "orphaned"),
                &[snap(t2, "beta")],
                &[t2],
                None,
            )
            .unwrap_err();
        assert!(err.to_string().contains("not in region"), "{err}");

        assert_eq!(
            eng.count("r", "derived").unwrap(),
            1,
            "no rejected write landed"
        );
    }
}

#[test]
fn checked_write_rejects_expired_snapshot_sources() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = build_db(dir.path(), encrypted);
        let eng = engine(&db, encrypted);
        let lapsed = eng
            .remember("r", AtomInput::new("turn", "lapsed").with_expires_at(PAST))
            .unwrap();
        let err = eng
            .remember_derived_checked(
                "r",
                AtomInput::new("derived", "from the past"),
                &[snap(lapsed, "lapsed")],
                &[lapsed],
                None,
            )
            .unwrap_err();
        assert!(err.to_string().contains("expired"), "{err}");
    }
}

#[test]
fn cascade_erases_derived_descendants_with_keys() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), true);
    let eng = engine(&db, true);
    let turn = eng
        .remember("r", AtomInput::new("turn", "the secret trip"))
        .unwrap();
    let keep = eng
        .remember("r", AtomInput::new("turn", "unrelated"))
        .unwrap();
    let fact = eng
        .remember_derived(
            "r",
            AtomInput::new("derived", "fact about trip"),
            &[turn],
            None,
        )
        .unwrap();
    let timeline = eng
        .remember_derived(
            "r",
            AtomInput::new("derived", "timeline via fact"),
            &[fact],
            None,
        )
        .unwrap();

    let receipt = eng
        .forget_atoms_with_dependents("r", &[turn], false)
        .unwrap();
    assert!(receipt.cryptographic_erasure);
    assert_eq!(receipt.rows_deleted, 3, "turn + fact + timeline");
    assert_eq!(receipt.erased_count, 3, "three keys destroyed");
    assert!(receipt.fsync && receipt.readback_confirmed);

    for id in [turn, fact, timeline] {
        assert!(eng.fetch_one("r", id).unwrap().is_none());
    }
    assert!(eng.fetch_one("r", keep).unwrap().is_some());
    assert!(
        eng.fetch_edges_in_region("r", None, None, Some(EdgeKind::DerivedFrom), 10)
            .unwrap()
            .is_empty(),
        "no dangling provenance edges"
    );
    let audit = citadel_mem::audit_provenance(&eng, "r", "derived").unwrap();
    assert!(audit.ok() && audit.derived_total == 0);

    let retry = eng
        .forget_atoms_with_dependents("r", &[turn], false)
        .unwrap();
    assert_eq!(retry.rows_deleted, 0, "retry converges");
    assert_eq!(retry.erased_count, 0);
    assert!(!retry.fsync, "a zero receipt attests nothing");
}

#[test]
fn cascade_diamond_spares_the_other_source() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), true);
    let eng = engine(&db, true);
    let t1 = eng.remember("r", AtomInput::new("turn", "one")).unwrap();
    let t2 = eng.remember("r", AtomInput::new("turn", "two")).unwrap();
    let fact = eng
        .remember_derived(
            "r",
            AtomInput::new("derived", "cites both"),
            &[t1, t2],
            None,
        )
        .unwrap();
    let timeline = eng
        .remember_derived("r", AtomInput::new("derived", "via fact"), &[fact], None)
        .unwrap();

    let receipt = eng.forget_atoms_with_dependents("r", &[t1], false).unwrap();
    assert_eq!(receipt.rows_deleted, 3, "t1 + fact + timeline");
    assert!(eng.fetch_one("r", t2).unwrap().is_some(), "t2 survives");
    assert!(eng.fetch_one("r", timeline).unwrap().is_none());
}

#[test]
fn cascade_refuses_immutable_dependents_without_force() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), true);
    let eng = engine(&db, true);
    let turn = eng.remember("r", AtomInput::new("turn", "src")).unwrap();
    let pinned = eng
        .remember_derived(
            "r",
            AtomInput::new("derived", "pinned note").immutable(),
            &[turn],
            None,
        )
        .unwrap();

    let err = eng
        .forget_atoms_with_dependents("r", &[turn], false)
        .unwrap_err();
    assert!(err.to_string().contains(&pinned.to_string()), "{err}");
    assert!(
        eng.fetch_one("r", turn).unwrap().is_some(),
        "nothing erased"
    );

    let receipt = eng
        .forget_atoms_with_dependents("r", &[turn], true)
        .unwrap();
    assert_eq!(receipt.rows_deleted, 2);
    assert!(eng.fetch_one("r", pinned).unwrap().is_none());
}

#[test]
fn cascade_rejects_foreign_roots_and_ignores_absent_ones() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), false);
    let eng = engine(&db, false);
    // Same embedder configuration: both regions share one atoms table.
    eng.create_region("r2", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let foreign = eng
        .remember("r2", AtomInput::new("turn", "not yours"))
        .unwrap();

    let err = eng
        .forget_atoms_with_dependents("r", &[foreign], false)
        .unwrap_err();
    assert!(err.to_string().contains("belongs to region 'r2'"), "{err}");
    assert!(
        eng.fetch_one("r2", foreign).unwrap().is_some(),
        "foreign root untouched"
    );

    let receipt = eng
        .forget_atoms_with_dependents("r", &[987_654], false)
        .unwrap();
    assert_eq!(receipt.rows_deleted, 0, "absent roots are a no-op");
    assert_eq!(receipt.erased_count, 0);
}

#[test]
fn cascade_terminates_on_provenance_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), false);
    let eng = engine(&db, false);
    let d1 = eng.remember("r", AtomInput::new("derived", "one")).unwrap();
    let d2 = eng.remember("r", AtomInput::new("derived", "two")).unwrap();
    // DerivedFrom is not acyclicity-checked at link time; the closure walk
    // must terminate on the cycle and erase both members.
    eng.link_in_region("r", d1, d2, EdgeKind::DerivedFrom, 1.0)
        .unwrap();
    eng.link_in_region("r", d2, d1, EdgeKind::DerivedFrom, 1.0)
        .unwrap();

    let receipt = eng.forget_atoms_with_dependents("r", &[d1], false).unwrap();
    assert_eq!(receipt.rows_deleted, 2, "cycle members erase together");
    assert!(eng.fetch_one("r", d2).unwrap().is_none());
}

#[test]
fn cascade_purges_identity_records_of_erased_atoms() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), false);
    let eng = engine(&db, false);
    let turn = eng
        .remember_if_absent_keyed("r", AtomInput::new("turn", "keyed root"), &[], None, "k")
        .unwrap();
    eng.remember_derived("r", AtomInput::new("derived", "cites it"), &[turn.id], None)
        .unwrap();
    assert_eq!(identity_rows(&db), 1);

    let receipt = eng
        .forget_atoms_with_dependents("r", &[turn.id], false)
        .unwrap();
    assert_eq!(receipt.rows_deleted, 2);
    assert_eq!(identity_rows(&db), 0, "cascade purged the identity record");

    let rebound = eng
        .remember_if_absent_keyed("r", AtomInput::new("turn", "keyed root"), &[], None, "k")
        .unwrap();
    assert!(rebound.inserted, "the key rebinds after the cascade");
    assert_ne!(rebound.id, turn.id);
}

#[test]
fn cascade_survives_concurrent_derived_writers() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), false);
    let eng = engine(&db, false);
    let root = eng.remember("r", AtomInput::new("turn", "root")).unwrap();

    // A SECOND engine over the same database: the interleaving crosses
    // engine handles, not just threads.
    let writer_eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    writer_eng
        .create_region("r", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();

    let start = Arc::new(Barrier::new(2));
    let writer = {
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            let mut written = 0u64;
            for i in 0.. {
                match writer_eng.remember_derived(
                    "r",
                    AtomInput::new("derived", format!("cites root {i}")),
                    &[root],
                    None,
                ) {
                    Ok(_) => written += 1,
                    // Single-writer design: a concurrent txn fails writes
                    // transiently; retry until the cascade's verdict lands.
                    Err(e)
                        if e.to_string()
                            .contains("write transaction is already active") =>
                    {
                        std::thread::yield_now();
                    }
                    // Root gone: the writer's in-txn source check refused.
                    Err(_) => break,
                }
            }
            written
        })
    };

    start.wait();
    let receipt = loop {
        match eng.forget_atoms_with_dependents("r", &[root], false) {
            Ok(r) => break r,
            Err(e)
                if e.to_string()
                    .contains("write transaction is already active") =>
            {
                std::thread::yield_now();
            }
            Err(e) => panic!("cascade failed: {e}"),
        }
    };
    let written = writer.join().unwrap();

    // Atomicity: every write either committed before the cascade (erased
    // with the root) or failed its source check - nothing in between.
    assert_eq!(receipt.rows_deleted, 1 + written, "root + every commit");
    assert!(eng.fetch_one("r", root).unwrap().is_none());
    assert_eq!(eng.count("r", "derived").unwrap(), 0);
    assert!(eng
        .fetch_edges_in_region("r", None, Some(root), Some(EdgeKind::DerivedFrom), 10)
        .unwrap()
        .is_empty());
    let audit = citadel_mem::audit_provenance(&eng, "r", "derived").unwrap();
    assert!(audit.ok() && audit.derived_total == 0);
}

#[test]
fn cascade_and_drop_region_interleave_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), true);
    let eng = engine(&db, true);
    let turn = eng.remember("r", AtomInput::new("turn", "secret")).unwrap();
    eng.remember_derived("r", AtomInput::new("derived", "fact"), &[turn], None)
        .unwrap();

    // The drop races from a SECOND engine handle over the same database.
    let dropper_eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    dropper_eng
        .create_encrypted_region("r", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();

    let start = Arc::new(Barrier::new(2));
    let dropper = {
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            dropper_eng.drop_region("r").unwrap();
        })
    };
    start.wait();
    let result = eng.forget_atoms_with_dependents("r", &[turn], false);
    dropper.join().unwrap();

    // The lifecycle lock serializes the erasure spans: full receipt or an unavailable
    // region, never a partial cascade. The drop invalidates cached handles before its
    // durable row cleanup, so the losing operation can observe either taxonomy state.
    match result {
        Ok(receipt) => assert_eq!(receipt.rows_deleted, 2, "complete cascade"),
        Err(MemError::RegionNotFound(_) | MemError::RegionNotAttached(_)) => {}
        Err(e) => panic!("unexpected cascade error: {e}"),
    }
}

#[test]
fn keyed_retry_converges_even_after_a_source_was_forgotten() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = build_db(dir.path(), encrypted);
        let eng = engine(&db, encrypted);
        let s = eng.remember("r", AtomInput::new("turn", "src")).unwrap();
        let a = eng
            .remember_if_absent_keyed("r", AtomInput::new("fact", "v"), &[s], None, "k")
            .unwrap();
        eng.forget_atoms("r", &[s], false).unwrap();

        // The replay writes nothing, so it must not require the sources to
        // still exist.
        let retry = eng
            .remember_if_absent_keyed("r", AtomInput::new("fact", "v"), &[s], None, "k")
            .unwrap();
        assert!(!retry.inserted, "identical retry converges");
        assert_eq!(retry.id, a.id);
    }
}

#[test]
fn keyed_stale_target_allows_rebinding_a_changed_request() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = build_db(dir.path(), encrypted);
        let eng = engine(&db, encrypted);
        // The original binding's atom lapses (row present but expired).
        let a = eng
            .remember_if_absent_keyed(
                "r",
                AtomInput::new("fact", "old").with_expires_at(PAST),
                &[],
                None,
                "k",
            )
            .unwrap();

        // A CHANGED request under the same key rebinds instead of erroring:
        // only a LIVE binding may refuse a different request.
        let b = eng
            .remember_if_absent_keyed("r", AtomInput::new("fact", "new"), &[], None, "k")
            .unwrap();
        assert!(b.inserted, "stale binding frees the key");
        assert_ne!(b.id, a.id);

        // The fresh binding is live, so a third variant still fails loudly.
        let err = eng
            .remember_if_absent_keyed("r", AtomInput::new("fact", "third"), &[], None, "k")
            .unwrap_err();
        assert!(err.to_string().contains("different request"), "{err}");
    }
}

#[test]
fn keyed_retry_treats_key_dead_residue_as_absent() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), true);
    let eng = engine(&db, true);
    let a = eng
        .remember_if_absent_keyed("r", AtomInput::new("fact", "interrupted"), &[], None, "k")
        .unwrap();
    plant_keyed_residue(&db, &eng, a.id);

    // The row survives with a destroyed key: the retry must not replay the
    // unreadable atom (the triple bind every sealed read applies).
    let retry = eng
        .remember_if_absent_keyed("r", AtomInput::new("fact", "interrupted"), &[], None, "k")
        .unwrap();
    assert!(retry.inserted, "key-dead residue counts as absent");
    assert_ne!(retry.id, a.id);
}

#[test]
fn open_sweep_purges_orphaned_identity_records() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), false);
    let eng = engine(&db, false);
    let a = eng
        .remember_if_absent_keyed("r", AtomInput::new("fact", "row removed"), &[], None, "k")
        .unwrap();
    eng.create_region("r2", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    eng.remember_if_absent_keyed(
        "r2",
        AtomInput::new("fact", "region removed"),
        &[],
        None,
        "k",
    )
    .unwrap();
    assert_eq!(identity_rows(&db), 2);
    drop(eng);

    // Simulate removals that bypass the engine's purges (an older binary or
    // direct SQL): one record loses its atom row, the other its region.
    let conn = Connection::open(&db).unwrap();
    conn.execute_params(
        "DELETE FROM memory_atoms_d64_cosine WHERE id = $1",
        &[Value::Integer(a.id)],
    )
    .unwrap();
    conn.execute_params("DELETE FROM memory_regions WHERE name = 'r2'", &[])
        .unwrap();
    drop(conn);

    let _eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert_eq!(
        identity_rows(&db),
        0,
        "open-time sweep purged both orphaned records"
    );
}

#[test]
fn reconcile_purges_identity_records_of_key_dead_residue() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path(), true);
    let eng = engine(&db, true);
    let a = eng
        .remember_if_absent_keyed("r", AtomInput::new("fact", "orphan"), &[], None, "k")
        .unwrap();
    plant_keyed_residue(&db, &eng, a.id);
    assert_eq!(identity_rows(&db), 1);
    drop(eng);

    // Reopen: reconcile finishes the interrupted erasure - the key-dead row
    // AND its identity record both go.
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert_eq!(
        identity_rows(&db),
        0,
        "reconcile purged the identity record"
    );
    eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    assert_eq!(eng.count("r", "fact").unwrap(), 0);
}
