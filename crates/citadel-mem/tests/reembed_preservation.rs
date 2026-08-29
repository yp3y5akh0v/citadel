//! Re-embedding replaces vectors while preserving atom identity and every other column.
//! Whole rows are read directly because `AtomHit` omits persisted scoring fields.

mod upgrade_fixtures;

use std::sync::Arc;

use citadel_mem::{AtomInput, EmbedError, Embedder, EmbeddingMetric, MemoryEngine};
use upgrade_fixtures::{fixture_vault, reopen, NamedEmbedder};

const DIM: u16 = 32;

struct NeighborSwapEmbedder {
    new_space: bool,
}

impl Embedder for NeighborSwapEmbedder {
    fn dim(&self) -> usize {
        2
    }

    fn metric(&self) -> EmbeddingMetric {
        EmbeddingMetric::Cosine
    }

    fn model_id(&self) -> &str {
        if self.new_space {
            "neighbor-swap-v2"
        } else {
            "neighbor-swap-v1"
        }
    }

    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        Ok(texts
            .iter()
            .map(|text| match (*text, self.new_space) {
                ("source", _) => vec![1.0, 0.0],
                ("old-neighbor", false) | ("new-neighbor", true) => vec![0.99, 0.1],
                ("new-neighbor", false) | ("old-neighbor", true) => vec![0.0, 1.0],
                _ => vec![0.0, 1.0],
            })
            .collect())
    }
}

/// Every column of an atom row, read straight from SQL so nothing is filtered
/// by a type that happens to carry fewer fields than the table has.
#[derive(Debug, PartialEq)]
struct AtomRow {
    id: i64,
    region_id: i64,
    kind: String,
    payload: String,
    text_content: String,
    score: String,
    confidence: String,
    access_count: i64,
    immutable: i64,
    created_at: i64,
    accessed_at: i64,
    expires_at: String,
}

fn read_rows(db: &Arc<citadel::Database>, table: &str) -> Vec<AtomRow> {
    let conn = citadel_sql::Connection::open(db).unwrap();
    let qr = conn
        .query(&format!(
            "SELECT id, region_id, kind, payload, text_content, score, confidence, \
             access_count, immutable, created_at, accessed_at, expires_at \
             FROM {table} ORDER BY id"
        ))
        .unwrap();
    qr.rows
        .iter()
        .map(|r| AtomRow {
            id: as_int(&r[0]),
            region_id: as_int(&r[1]),
            kind: format!("{:?}", r[2]),
            payload: format!("{:?}", r[3]),
            text_content: format!("{:?}", r[4]),
            score: format!("{:?}", r[5]),
            confidence: format!("{:?}", r[6]),
            access_count: as_int(&r[7]),
            immutable: as_int(&r[8]),
            created_at: as_int(&r[9]),
            accessed_at: as_int(&r[10]),
            expires_at: format!("{:?}", r[11]),
        })
        .collect()
}

fn as_int(v: &citadel_sql::Value) -> i64 {
    match v {
        citadel_sql::Value::Integer(n) => *n,
        citadel_sql::Value::Timestamp(n) => *n,
        other => panic!("expected an integer, got {other:?}"),
    }
}

fn read_edges(db: &Arc<citadel::Database>) -> Vec<String> {
    let conn = citadel_sql::Connection::open(db).unwrap();
    let qr = conn
        .query(
            "SELECT src_id, dst_id, kind, weight FROM memory_edges \
             ORDER BY src_id, dst_id, kind",
        )
        .unwrap();
    qr.rows.iter().map(|r| format!("{r:?}")).collect()
}

/// `SimilarTo` edges typed, because the weight is the thing being asserted.
fn read_similarity_edges(db: &Arc<citadel::Database>) -> Vec<(i64, i64, f32)> {
    let conn = citadel_sql::Connection::open(db).unwrap();
    let qr = conn
        .query(
            "SELECT src_id, dst_id, weight FROM memory_edges \
             WHERE kind = 'similar_to' ORDER BY src_id, dst_id",
        )
        .unwrap();
    qr.rows
        .iter()
        .map(|r| {
            let weight = match &r[2] {
                citadel_sql::Value::Real(w) => *w as f32,
                other => panic!("expected a weight, got {other:?}"),
            };
            (as_int(&r[0]), as_int(&r[1]), weight)
        })
        .collect()
}

fn read_managed_similarity_edges(db: &Arc<citadel::Database>) -> Vec<(i64, i64, f32)> {
    let conn = citadel_sql::Connection::open(db).unwrap();
    let qr = conn
        .query(
            "SELECT e.src_id, e.dst_id, e.weight FROM memory_edges e \
             JOIN memory_similarity_edges m ON m.src_id = e.src_id AND m.dst_id = e.dst_id \
             WHERE e.kind = 'similar_to' ORDER BY e.src_id, e.dst_id",
        )
        .unwrap();
    qr.rows
        .iter()
        .map(|row| {
            let citadel_sql::Value::Real(weight) = &row[2] else {
                panic!("expected a weight, got {:?}", row[2]);
            };
            (as_int(&row[0]), as_int(&row[1]), *weight as f32)
        })
        .collect()
}

fn read_vectors(db: &Arc<citadel::Database>, table: &str) -> Vec<String> {
    let conn = citadel_sql::Connection::open(db).unwrap();
    let qr = conn
        .query(&format!("SELECT id, embedding FROM {table} ORDER BY id"))
        .unwrap();
    qr.rows.iter().map(|r| format!("{r:?}")).collect()
}

/// A region carrying every kind of state a migration could drop.
fn seeded(path: &std::path::Path) -> (Arc<citadel::Database>, MemoryEngine) {
    let db = fixture_vault(path);
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
        )
        .unwrap();

    let a = engine
        .remember(
            "notes",
            AtomInput::new("note", "alpha beta")
                .with_created_at(1_700_000_000_000_000)
                .with_confidence(0.75)
                .with_score(0.5)
                .with_payload(serde_json::json!({"tag": "first"})),
        )
        .unwrap();
    let b = engine
        .remember(
            "notes",
            AtomInput::new("note", "gamma delta")
                .with_created_at(1_700_000_001_000_000)
                .with_expires_at(9_999_999_999_000_000)
                .immutable(),
        )
        .unwrap();
    engine
        .remember("notes", AtomInput::new("note", "epsilon zeta"))
        .unwrap();

    engine
        .link(a, b, citadel_mem::EdgeKind::Refines, 0.9)
        .unwrap();

    (db, engine)
}

/// Row count of an atoms table, zero when there is no such table.
///
/// An encrypted region keeps the whole atom inside `sealed`, so the
/// column-by-column reader above cannot be pointed at one.
fn count_rows(db: &Arc<citadel::Database>, table: &str) -> usize {
    let conn = citadel_sql::Connection::open(db).unwrap();
    if conn.table_schema(table).is_none() {
        return 0;
    }
    let qr = conn
        .query(&format!("SELECT COUNT(*) FROM {table}"))
        .unwrap();
    as_int(&qr.rows[0][0]) as usize
}

/// Give the first `n - 1` atoms one engine-managed neighbor each.
fn seed_similarity_web(db: &Arc<citadel::Database>, engine: &MemoryEngine, n: usize) {
    let ids: Vec<i64> = read_rows(db, &format!("memory_atoms_d{DIM}_cosine"))
        .iter()
        .map(|r| r.id)
        .take(n)
        .collect();
    for &src in ids.iter().take(n.saturating_sub(1)) {
        engine.evolve("notes", src, 1, f32::MAX).unwrap();
    }
}

/// A region with `atoms` plain notes, for the cases that need more atoms than
/// one migration batch holds.
fn seeded_with(path: &std::path::Path, atoms: usize) -> (Arc<citadel::Database>, MemoryEngine) {
    let db = fixture_vault(path);
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
        )
        .unwrap();
    for i in 0..atoms {
        engine
            .remember("notes", AtomInput::new("note", format!("atom number {i}")))
            .unwrap();
    }
    (db, engine)
}

/// The whole contract in one assertion set.
#[test]
fn reembedding_changes_only_the_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded(&dir.path().join("reembed.citadel"));
    let table = format!("memory_atoms_d{DIM}_cosine");

    let rows_before = read_rows(&db, &table);
    let edges_before = read_edges(&db);
    let vectors_before = read_vectors(&db, &table);
    assert_eq!(rows_before.len(), 3, "the fixture seeded three atoms");
    assert_eq!(edges_before.len(), 1, "the fixture seeded one edge");

    engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .expect("re-embed");

    let rows_after = read_rows(&db, &table);
    let edges_after = read_edges(&db);
    let vectors_after = read_vectors(&db, &table);

    assert_eq!(
        rows_after, rows_before,
        "every column except the vector must survive: ids, payload, text, score, \
         confidence, access_count, immutable, created_at, accessed_at, expires_at"
    );
    assert_eq!(
        edges_after, edges_before,
        "edges are keyed by atom id, so a migration that reallocates ids orphans them"
    );
    assert_ne!(
        vectors_after, vectors_before,
        "the vectors are the one thing that should have changed"
    );
}

/// The common case: models rarely share a width. Switching from a 32-dim model
/// to a 64-dim one moves every row to a differently-shaped atoms table, and all
/// the preservation guarantees have to survive that move too - ids above all,
/// since edges are keyed by them.
#[test]
fn changing_the_vector_width_moves_rows_and_keeps_everything_else() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded(&dir.path().join("widen.citadel"));
    let source = format!("memory_atoms_d{DIM}_cosine");
    let destination = format!("memory_atoms_d{}_cosine", DIM * 2);

    let rows_before = read_rows(&db, &source);
    let edges_before = read_edges(&db);
    assert_eq!(rows_before.len(), 3);

    let report = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new((DIM * 2) as usize, "wider-model")),
            None,
        )
        .expect("a width change is the ordinary reason to re-embed");

    assert_eq!(report.atoms_migrated, 3);

    // Every column survives the move, ids included.
    let rows_after = read_rows(&db, &destination);
    assert_eq!(
        rows_after, rows_before,
        "moving rows to a wider table must not reset any column"
    );
    assert_eq!(
        edges_after_ids(&db),
        edges_before,
        "ids are carried across, so edges keyed by them stay valid"
    );

    // The source is emptied of this region but never dropped: its name is
    // shared by every region of that shape.
    assert!(
        read_rows(&db, &source).is_empty(),
        "the region's rows moved out of the old table"
    );

    assert_eq!(engine.count_region("notes").unwrap(), 3);
}

fn edges_after_ids(db: &Arc<citadel::Database>) -> Vec<String> {
    read_edges(db)
}

/// The sibling check under a width change, which is where dropping the emptied
/// source table would have destroyed another region.
#[test]
fn widening_one_region_leaves_a_sibling_in_the_old_table_intact() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded(&dir.path().join("sibling-widen.citadel"));
    let source = format!("memory_atoms_d{DIM}_cosine");

    engine
        .create_region(
            "other",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
        )
        .unwrap();
    engine
        .remember("other", AtomInput::new("note", "sibling row"))
        .unwrap();

    engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new((DIM * 2) as usize, "wider-model")),
            None,
        )
        .unwrap();

    assert_eq!(
        engine.count_region("other").unwrap(),
        1,
        "the sibling shares the old atoms table and must be untouched"
    );
    assert_eq!(
        read_rows(&db, &source).len(),
        1,
        "the old table still holds the sibling's row, so it must not be dropped"
    );
}

/// Derived data that has become false is rebuilt, not merely deleted.
///
/// A `SimilarTo` edge asserts "these two atoms are near each other", measured in
/// a vector space the migration just replaced, so its weight is no longer true.
/// The exact rule that produced a managed edge survives in its persisted policy.
///
/// The assertion that matters is the last one. Comparing against a recall run
/// independently of the migration is what separates an edge recomputed over the
/// new vectors from one carried across unchanged - a count alone would pass for
/// both. Authored edges say what someone asserted and must survive untouched.
#[test]
fn similarity_edges_are_rebuilt_over_the_new_vectors_and_authored_edges_survive() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded(&dir.path().join("similar.citadel"));

    let ids: Vec<i64> = read_rows(&db, &format!("memory_atoms_d{DIM}_cosine"))
        .iter()
        .map(|r| r.id)
        .collect();
    engine.evolve("notes", ids[0], 1, 1.5).unwrap();
    engine
        .link(ids[2], ids[1], citadel_mem::EdgeKind::SimilarTo, 0.4)
        .unwrap();
    engine
        .link(ids[2], ids[1], citadel_mem::EdgeKind::DerivedFrom, 1.0)
        .unwrap();

    let report = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .unwrap();

    assert_eq!(
        report.similarity_edges_cleared, 0,
        "the persisted policy could be rebuilt, so nothing was discarded"
    );
    assert!(
        report.ann_rebuilt,
        "the index must be rebuilt over the new vectors, not left stale"
    );
    assert_eq!(report.atoms_migrated, 3);

    let edges = read_edges(&db);
    assert!(
        edges.iter().any(|e| e.contains("derived_from")),
        "an authored edge records what someone asserted and must survive: {edges:?}"
    );
    // The seeded Refines edge is authored too.
    assert!(
        edges.iter().any(|e| e.contains("refines")),
        "authored edges survive: {edges:?}"
    );

    // What the new vector space actually says, asked without going through the
    // migration. The region now holds model-b, and the atom's stored vector was
    // computed from this text by model-b, so this is the same query the reweave
    // ran.
    let expected: Vec<(i64, f32)> = engine
        .recall(
            "notes",
            citadel_mem::RecallQuery::by_text("alpha beta", 2)
                .with_kinds(vec!["note".to_string()])
                .with_weights(citadel_mem::FusionWeights::semantic_only()),
        )
        .unwrap()
        .into_iter()
        .filter(|h| h.id != ids[0] && h.distance <= 1.5)
        .take(1)
        .map(|h| (h.id, h.distance))
        .collect();
    assert!(
        !expected.is_empty(),
        "the fixture must put a neighbour under the ceiling, or this proves nothing"
    );

    let rebuilt = read_managed_similarity_edges(&db);
    assert_eq!(
        report.similarity_edges_rewoven as usize,
        rebuilt.len(),
        "the report must count the edges that actually exist: {rebuilt:?}"
    );
    let want: Vec<(i64, i64, f32)> = expected
        .iter()
        .map(|(dst, distance)| (ids[0], *dst, 1.0 / (1.0 + distance.max(0.0))))
        .collect();
    assert_eq!(
        rebuilt, want,
        "each edge must name the neighbour the NEW vectors pick, at the weight \
         those vectors imply"
    );
    assert!(
        read_similarity_edges(&db)
            .iter()
            .any(|&(src, dst, weight)| src == ids[2] && dst == ids[1] && weight == 0.4),
        "the authored SimilarTo assertion must survive the model change"
    );
}

#[test]
fn replaced_managed_neighbor_is_counted_as_cleared() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture_vault(&dir.path().join("neighbor-swap.citadel"));
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_region("notes", Arc::new(NeighborSwapEmbedder { new_space: false }))
        .unwrap();
    let source = engine
        .remember("notes", AtomInput::new("note", "source"))
        .unwrap();
    let old_neighbor = engine
        .remember("notes", AtomInput::new("note", "old-neighbor"))
        .unwrap();
    let new_neighbor = engine
        .remember("notes", AtomInput::new("note", "new-neighbor"))
        .unwrap();
    engine.evolve("notes", source, 1, 0.5).unwrap();
    let managed = read_managed_similarity_edges(&db);
    assert_eq!(managed.len(), 1);
    assert_eq!((managed[0].0, managed[0].1), (source, old_neighbor));

    let report = engine
        .reembed_region(
            "notes",
            Arc::new(NeighborSwapEmbedder { new_space: true }),
            None,
        )
        .unwrap();

    assert_eq!(report.similarity_edges_rewoven, 1);
    assert_eq!(report.similarity_edges_cleared, 1);
    let managed = read_managed_similarity_edges(&db);
    assert_eq!(managed.len(), 1);
    assert_eq!((managed[0].0, managed[0].1), (source, new_neighbor));
}

#[test]
fn an_authored_similarity_edge_is_not_reclassified_as_managed() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded(&dir.path().join("unrebuildable.citadel"));

    let ids: Vec<i64> = read_rows(&db, &format!("memory_atoms_d{DIM}_cosine"))
        .iter()
        .map(|r| r.id)
        .collect();
    engine
        .link(ids[0], ids[2], citadel_mem::EdgeKind::SimilarTo, 0.0)
        .unwrap();

    let report = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .unwrap();

    assert_eq!(report.similarity_edges_rewoven, 0);
    assert_eq!(report.similarity_edges_cleared, 0);
    assert_eq!(read_similarity_edges(&db), vec![(ids[0], ids[2], 0.0)]);
    assert!(
        read_edges(&db).iter().any(|e| e.contains("refines")),
        "the authored edge survives regardless"
    );
}

#[test]
fn the_regions_recorded_model_is_updated() {
    let dir = tempfile::tempdir().unwrap();
    let (_db, engine) = seeded(&dir.path().join("model.citadel"));

    engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .unwrap();

    let identities = engine.stored_region_identities().unwrap();
    let region = identities.iter().find(|r| r.name() == "notes").unwrap();
    assert_eq!(region.model_id(), "model-b");
}

/// The atoms table name is a function of
/// `(dim, metric, encrypted)`, so regions of the same shape SHARE it. Dropping
/// the source table after a migration would take another region's rows with it.
#[test]
fn a_region_sharing_the_atoms_table_is_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded(&dir.path().join("shared.citadel"));
    let table = format!("memory_atoms_d{DIM}_cosine");

    // Same dim, same metric, same encryption: the same physical table.
    engine
        .create_region(
            "other",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
        )
        .unwrap();
    for text in ["one two", "three four"] {
        engine
            .remember("other", AtomInput::new("note", text))
            .unwrap();
    }
    let other_before: Vec<_> = read_rows(&db, &table)
        .into_iter()
        .filter(|r| r.kind == "Text(\"note\")" && r.text_content.contains("one two"))
        .collect();
    assert_eq!(other_before.len(), 1, "the sibling region seeded rows");

    engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .unwrap();

    assert_eq!(
        engine.count_region("other").unwrap(),
        2,
        "migrating one region must not disturb another sharing its atoms table"
    );
}

#[test]
fn an_authored_external_similarity_edge_survives_reembedding_its_target() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded(&dir.path().join("incoming.citadel"));
    let table = format!("memory_atoms_d{DIM}_cosine");
    let note = read_rows(&db, &table)[0].id;

    engine
        .create_region(
            "other",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
        )
        .unwrap();
    let outside = engine
        .remember("other", AtomInput::new("note", "outside source"))
        .unwrap();
    engine
        .link(outside, note, citadel_mem::EdgeKind::SimilarTo, 0.1)
        .unwrap();

    let report = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .unwrap();

    assert_eq!(report.similarity_edges_cleared, 0);
    assert_eq!(read_similarity_edges(&db), vec![(outside, note, 0.1)]);
    assert_eq!(engine.count_region("other").unwrap(), 1);
}

/// Re-embedding to the model already in use is a no-op, not a rewrite.
#[test]
fn reembedding_to_the_same_model_does_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded(&dir.path().join("noop.citadel"));
    let table = format!("memory_atoms_d{DIM}_cosine");
    let vectors_before = read_vectors(&db, &table);

    let report = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
            None,
        )
        .unwrap();

    assert_eq!(report.atoms_migrated, 0, "nothing needed migrating");
    assert_eq!(read_vectors(&db, &table), vectors_before);
}

#[test]
fn a_vectors_mark_cannot_be_hidden_by_already_published_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded(&dir.path().join("impossible-vectors-mark.citadel"));
    let metadata = serde_json::json!({
        "reembed_to_model": "model-a",
        "reembed_to_dim": DIM,
        "reembed_to_metric": "cosine",
        "reembed_done_through": 1,
        "reembed_phase": "vectors"
    });
    citadel_sql::Connection::open(&db)
        .unwrap()
        .execute_params(
            "UPDATE memory_regions SET metadata = $1 WHERE name = 'notes'",
            &[citadel_sql::Value::Text(metadata.to_string().into())],
        )
        .unwrap();

    let error = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
            None,
        )
        .expect_err("an impossible vectors mark must not be reported as a no-op");
    assert!(error.to_string().contains("vectors-phase"), "got {error}");
}

#[test]
fn malformed_reembed_metadata_is_rejected_before_encrypted_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("malformed-mark.citadel");
    let db = fixture_vault(&path);
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
        )
        .unwrap();
    engine
        .remember("notes", AtomInput::new("note", "must survive"))
        .unwrap();
    let conn = citadel_sql::Connection::open(&db).unwrap();
    let atom = conn
        .query(&format!(
            "SELECT id, key_slot, key_gen FROM memory_atoms_d{DIM}_cosine_enc LIMIT 1"
        ))
        .unwrap();
    let atom_id = as_int(&atom.rows[0][0]) as u64;
    let slot = as_int(&atom.rows[0][1]) as u32;
    let generation = as_int(&atom.rows[0][2]) as u64;
    conn.execute_params(
        "UPDATE memory_regions SET metadata = $1 WHERE name = 'notes'",
        &[citadel_sql::Value::Text(
            serde_json::json!({"reembed_to_model": "model-b"})
                .to_string()
                .into(),
        )],
    )
    .unwrap();
    drop(conn);
    drop(engine);

    let error = MemoryEngine::open(Arc::clone(&db))
        .err()
        .expect("open must reject the partial reserved metadata");
    assert!(error.to_string().contains("incomplete"), "got {error}");
    assert_eq!(
        count_rows(&db, &format!("memory_atoms_d{DIM}_cosine_enc")),
        1
    );
    let record = db.atom_store_slot(slot).unwrap();
    assert_eq!(record.state, citadel::SlotState::Live);
    assert_eq!(record.region_id, atom_id);
    assert_eq!(record.gen, generation);
}

#[test]
fn non_object_region_metadata_is_not_overwritten_by_reembedding() {
    for (label, metadata) in [("array", "[]"), ("scalar", "42")] {
        let dir = tempfile::tempdir().unwrap();
        let (db, engine) = seeded(&dir.path().join(format!("{label}-metadata.citadel")));
        let conn = citadel_sql::Connection::open(&db).unwrap();
        conn.execute_params(
            "UPDATE memory_regions SET metadata = $1 WHERE name = 'notes'",
            &[citadel_sql::Value::Text(metadata.into())],
        )
        .unwrap();
        let error = engine
            .reembed_region(
                "notes",
                Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
                None,
            )
            .expect_err("non-object metadata must be refused, not replaced");
        assert!(error.to_string().contains("JSON object"), "got {error}");
        let stored = conn
            .query("SELECT CAST(metadata AS TEXT) FROM memory_regions WHERE name = 'notes'")
            .unwrap();
        assert_eq!(
            format!("{:?}", stored.rows[0][0]),
            format!("Text(\"{metadata}\")")
        );
    }
}

/// An atom without source text cannot be re-embedded.
#[test]
fn an_atom_with_no_text_is_reported_not_silently_lost() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture_vault(&dir.path().join("empty-text.citadel"));
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_region("byo", Arc::new(NamedEmbedder::new(DIM as usize, "model-a")))
        .unwrap();
    engine
        .remember(
            "byo",
            AtomInput::new("note", "").with_embedding(vec![0.25; DIM as usize]),
        )
        .unwrap();

    let refused = engine.reembed_region(
        "byo",
        Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
        None,
    );

    let Err(err) = refused else {
        panic!("an atom with no text cannot be re-embedded; this must refuse");
    };
    let message = err.to_string();
    assert!(
        message.contains("text"),
        "the refusal must say what is wrong: {message}"
    );

    assert_eq!(
        engine.count_region("byo").unwrap(),
        1,
        "a refused migration left the region marked"
    );
}

/// A failed graph repair remains marked and resumable after provenance publication.
#[test]
fn an_expired_atom_does_not_wedge_the_repair_stage() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded_with(&dir.path().join("expired.citadel"), 8);

    let ids: Vec<i64> = read_rows(&db, &format!("memory_atoms_d{DIM}_cosine"))
        .iter()
        .map(|r| r.id)
        .collect();
    let expired = engine
        .remember(
            "notes",
            AtomInput::new("note", "short lived").with_expires_at(9_999_999_999_000_000),
        )
        .unwrap();
    engine.evolve("notes", expired, 1, f32::MAX).unwrap();
    engine.evolve("notes", ids[0], 1, f32::MAX).unwrap();
    citadel_sql::Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE memory_atoms_d{DIM}_cosine SET expires_at = $1 WHERE id = $2"),
            &[
                citadel_sql::Value::Timestamp(1),
                citadel_sql::Value::Integer(expired),
            ],
        )
        .unwrap();

    let report = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .expect("an expired source must not fail the migration");

    // The live atom's web was still rebuilt, so the expired one was skipped
    // rather than the whole repair being abandoned.
    assert!(report.similarity_edges_rewoven > 0);
    assert!(report.similarity_edges_cleared > 0);

    // And the region is fully out of migration: it attaches under the new model.
    engine
        .attach_existing_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
        )
        .expect("a completed migration must leave no mark behind");
}

/// An embedder that parks inside `embed` once armed, so a caller can be held
/// between capturing its handle and using it.
struct BlockingEmbedder {
    inner: NamedEmbedder,
    armed: Arc<std::sync::atomic::AtomicBool>,
    entered: Arc<std::sync::atomic::AtomicBool>,
    release: Arc<std::sync::atomic::AtomicBool>,
}

impl citadel_mem::Embedder for BlockingEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }
    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }
    fn model_id(&self) -> &str {
        self.inner.model_id()
    }
    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, citadel_mem::EmbedError> {
        use std::sync::atomic::Ordering;
        if self.armed.load(Ordering::SeqCst) {
            self.entered.store(true, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        self.inner.embed(texts)
    }
}

#[test]
fn a_reembed_refuses_while_a_vector_write_is_in_flight() {
    use std::sync::atomic::{AtomicBool, Ordering};

    const NARROW: u16 = 16;
    const WIDE: u16 = 32;
    let dir = tempfile::tempdir().unwrap();
    let db = fixture_vault(&dir.path().join("stale-handle.citadel"));
    let engine = Arc::new(MemoryEngine::open(Arc::clone(&db)).unwrap());

    let armed = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    engine
        .create_region(
            "notes",
            Arc::new(BlockingEmbedder {
                inner: NamedEmbedder::new(NARROW as usize, "model-a"),
                armed: Arc::clone(&armed),
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
            }),
        )
        .unwrap();
    for i in 0..4 {
        engine
            .remember("notes", AtomInput::new("note", format!("atom {i}")))
            .unwrap();
    }

    armed.store(true, Ordering::SeqCst);
    let writer = {
        let engine = Arc::clone(&engine);
        std::thread::spawn(move || {
            engine.remember(
                "notes",
                AtomInput::new("note", "written across the migration"),
            )
        })
    };
    while !entered.load(Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    let refused = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(WIDE as usize, "model-b")),
            None,
        )
        .expect_err("the migration must not cross an in-flight vector write");
    assert!(format!("{refused:?}").contains("RegionInUse"));
    release.store(true, Ordering::SeqCst);

    writer
        .join()
        .expect("the writer thread panicked")
        .expect("the reserved write should finish on its original provenance");
    engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(WIDE as usize, "model-b")),
            None,
        )
        .expect("the migration should succeed after the write releases its reservation");

    assert_eq!(
        count_rows(&db, &format!("memory_atoms_d{NARROW}_cosine")),
        0,
    );
    assert_eq!(count_rows(&db, &format!("memory_atoms_d{WIDE}_cosine")), 5);
    assert_eq!(engine.count_region("notes").unwrap(), 5);
}

/// An embedder that trips a token once it has served `calls_before_stop` batches,
/// so the migration stops with earlier batches already durable.
struct StoppingEmbedder {
    inner: NamedEmbedder,
    token: citadel_core::CancelToken,
    calls: std::sync::atomic::AtomicUsize,
    calls_before_stop: usize,
}

impl citadel_mem::Embedder for StoppingEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }
    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }
    fn model_id(&self) -> &str {
        self.inner.model_id()
    }
    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, citadel_mem::EmbedError> {
        let n = self
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if n + 1 >= self.calls_before_stop {
            self.token.cancel();
        }
        self.inner.embed(texts)
    }
}

/// Trips the handle-wide token from inside the external callback, then clears
/// the handle. A re-embed must retain the token snapshot it began with rather
/// than depend on a later SQL statement to rediscover it.
struct CancelAndClearDatabaseEmbedder {
    inner: NamedEmbedder,
    database: Arc<citadel::Database>,
    token: citadel_core::CancelToken,
}

impl citadel_mem::Embedder for CancelAndClearDatabaseEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }

    fn model_id(&self) -> &str {
        self.inner.model_id()
    }

    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, citadel_mem::EmbedError> {
        let vectors = self.inner.embed(texts);
        self.token.cancel();
        self.database.set_cancel(None);
        vectors
    }
}

/// Records the largest slice a migration hands to its embedder.
struct BatchRecordingEmbedder {
    inner: NamedEmbedder,
    calls: std::sync::atomic::AtomicUsize,
    largest: std::sync::atomic::AtomicUsize,
}

impl citadel_mem::Embedder for BatchRecordingEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }
    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }
    fn model_id(&self) -> &str {
        self.inner.model_id()
    }
    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, citadel_mem::EmbedError> {
        use std::sync::atomic::Ordering;
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.largest.fetch_max(texts.len(), Ordering::Relaxed);
        self.inner.embed(texts)
    }
}

/// Re-enters an encrypted write on the same database from inside `embed`.
///
/// The worker + timeout keeps the regression bounded: the former implementation
/// held the global key-lifecycle guard across this callback, so the worker could
/// not acquire it. Returning an embedder error after the timeout releases the
/// old guard instead of leaving a permanently deadlocked test process.
struct ReentrantDatabaseEmbedder {
    inner: NamedEmbedder,
    engine: Arc<MemoryEngine>,
    db: Arc<citadel::Database>,
    source_region_id: u64,
    source_region_slot: u32,
    source_region_gen: u64,
    source_atom_id: u64,
    source_atom_slot: u32,
    source_atom_gen: u64,
    entered: std::sync::atomic::AtomicBool,
}

impl citadel_mem::Embedder for ReentrantDatabaseEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }

    fn model_id(&self) -> &str {
        self.inner.model_id()
    }

    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, citadel_mem::EmbedError> {
        use std::sync::atomic::Ordering;
        use std::sync::mpsc;
        use std::time::Duration;

        if !self.entered.swap(true, Ordering::SeqCst) {
            let engine = Arc::clone(&self.engine);
            let (done_tx, done_rx) = mpsc::channel();
            std::thread::spawn(move || {
                let result = engine.remember(
                    "callback-log",
                    AtomInput::new("event", "re-embed callback entered"),
                );
                let _ = done_tx.send(result);
            });
            match done_rx.recv_timeout(Duration::from_secs(5)) {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    return Err(citadel_mem::EmbedError::Backend(format!(
                        "same-database callback write failed: {error}"
                    )))
                }
                Err(_) => {
                    return Err(citadel_mem::EmbedError::Backend(
                        "same-database callback write was blocked by re-embed".into(),
                    ))
                }
            }
            match self.engine.drop_region("notes") {
                Err(citadel_mem::MemError::Core(citadel_core::Error::RegionInUse {
                    region_id,
                })) if region_id == self.source_region_id => {}
                Err(error) => {
                    return Err(citadel_mem::EmbedError::Backend(format!(
                        "source-region drop returned the wrong refusal: {error}"
                    )))
                }
                Ok(()) => {
                    return Err(citadel_mem::EmbedError::Backend(
                        "source-region drop bypassed the callback reservation".into(),
                    ))
                }
            }
            match self.db.region_store_tombstone(
                self.source_region_slot,
                self.source_region_id,
                self.source_region_gen,
            ) {
                Err(citadel::Error::RegionInUse { region_id })
                    if region_id == self.source_region_id => {}
                Err(error) => {
                    return Err(citadel_mem::EmbedError::Backend(format!(
                        "direct source-key tombstone returned the wrong refusal: {error}"
                    )))
                }
                Ok(()) => {
                    return Err(citadel_mem::EmbedError::Backend(
                        "direct source-key tombstone bypassed the callback reservation".into(),
                    ))
                }
            }
            match self.db.atom_store_tombstone(
                self.source_atom_slot,
                self.source_atom_id,
                self.source_atom_gen,
            ) {
                Err(citadel::Error::AtomInUse { atom_id }) if atom_id == self.source_atom_id => {}
                Err(error) => {
                    return Err(citadel_mem::EmbedError::Backend(format!(
                        "direct source-atom tombstone returned the wrong refusal: {error}"
                    )))
                }
                Ok(()) => {
                    return Err(citadel_mem::EmbedError::Backend(
                        "direct source-atom tombstone bypassed the callback reservation".into(),
                    ))
                }
            }
            match self.db.atom_store_tombstone_batch(&[(
                self.source_atom_slot,
                self.source_atom_id,
                self.source_atom_gen,
            )]) {
                Err(citadel::Error::AtomInUse { atom_id }) if atom_id == self.source_atom_id => {}
                Err(error) => {
                    return Err(citadel_mem::EmbedError::Backend(format!(
                        "direct source-atom batch tombstone returned the wrong refusal: {error}"
                    )))
                }
                Ok(_) => {
                    return Err(citadel_mem::EmbedError::Backend(
                        "direct source-atom batch tombstone bypassed the callback reservation"
                            .into(),
                    ))
                }
            }
        }
        self.inner.embed(texts)
    }
}

#[test]
fn a_reembedder_can_reenter_an_encrypted_write_on_the_same_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture_vault(&dir.path().join("reentrant.citadel"));
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_encrypted_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
        )
        .unwrap();
    for i in 0..12 {
        engine
            .remember("notes", AtomInput::new("note", format!("atom number {i}")))
            .unwrap();
    }
    let engine = Arc::new(engine);
    engine
        .create_encrypted_region(
            "callback-log",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
        )
        .unwrap();

    let conn = citadel_sql::Connection::open(&db).unwrap();
    let source = conn
        .query("SELECT id, rsk_slot, rsk_gen FROM memory_regions WHERE name = 'notes'")
        .unwrap();
    let source_region_id = as_int(&source.rows[0][0]) as u64;
    let source_region_slot = as_int(&source.rows[0][1]) as u32;
    let source_region_gen = as_int(&source.rows[0][2]) as u64;
    let source_atom = conn
        .query(&format!(
            "SELECT id, key_slot, key_gen FROM memory_atoms_d{DIM}_cosine_enc \
             WHERE region_id = {source_region_id} ORDER BY id LIMIT 1"
        ))
        .unwrap();
    let source_atom_id = as_int(&source_atom.rows[0][0]) as u64;
    let source_atom_slot = as_int(&source_atom.rows[0][1]) as u32;
    let source_atom_gen = as_int(&source_atom.rows[0][2]) as u64;
    drop(conn);

    let report = engine
        .reembed_region(
            "notes",
            Arc::new(ReentrantDatabaseEmbedder {
                inner: NamedEmbedder::new(DIM as usize, "model-b"),
                engine: Arc::clone(&engine),
                db: Arc::clone(&db),
                source_region_id,
                source_region_slot,
                source_region_gen,
                source_atom_id,
                source_atom_slot,
                source_atom_gen,
                entered: std::sync::atomic::AtomicBool::new(false),
            }),
            None,
        )
        .expect("the callback must run without the database key-lifecycle guard");

    assert_eq!(report.atoms_migrated, 12);
    assert_eq!(engine.count_region("callback-log").unwrap(), 1);
    let source_atom_record = db.atom_store_slot(source_atom_slot).unwrap();
    assert_eq!(source_atom_record.state, citadel::SlotState::Live);
    assert_eq!(source_atom_record.region_id, source_atom_id);
}

/// Attempts the same migration recursively while the outer callback is active.
struct CompletingReentrantEmbedder {
    inner: NamedEmbedder,
    engine: Arc<MemoryEngine>,
    entered: std::sync::atomic::AtomicBool,
}

impl citadel_mem::Embedder for CompletingReentrantEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }

    fn model_id(&self) -> &str {
        self.inner.model_id()
    }

    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, citadel_mem::EmbedError> {
        use std::sync::atomic::Ordering;

        if !self.entered.swap(true, Ordering::SeqCst) {
            self.engine
                .reembed_region(
                    "notes",
                    Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
                    None,
                )
                .map_err(|error| {
                    citadel_mem::EmbedError::Backend(format!("competing re-embed failed: {error}"))
                })?;
        }
        self.inner.embed(texts)
    }
}

#[test]
fn a_reentrant_reembed_is_refused_without_leaving_a_mark() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded_with(&dir.path().join("reentrant-winner.citadel"), 12);
    let engine = Arc::new(engine);

    let error = engine
        .reembed_region(
            "notes",
            Arc::new(CompletingReentrantEmbedder {
                inner: NamedEmbedder::new(DIM as usize, "model-b"),
                engine: Arc::clone(&engine),
                entered: std::sync::atomic::AtomicBool::new(false),
            }),
            None,
        )
        .expect_err("the nested migration must be refused while the region is reserved");
    let message = error.to_string();
    assert!(
        message.contains("RegionInUse") || message.contains("in use"),
        "the error must identify the active region operation, got {message}"
    );

    let conn = citadel_sql::Connection::open(&db).unwrap();
    let state = conn
        .query(
            "SELECT model_id, metadata ->> 'reembed_to_model' \
             FROM memory_regions WHERE name = 'notes'",
        )
        .unwrap();
    assert_eq!(format!("{:?}", state.rows[0][0]), "Text(\"model-a\")");
    assert!(matches!(state.rows[0][1], citadel_sql::Value::Null));
    assert_eq!(engine.count_region("notes").unwrap(), 12);
}

/// Embedding calls stay bounded by the migration batch size.
#[test]
fn reembedding_feeds_the_embedder_bounded_pages() {
    use std::sync::atomic::Ordering;

    const ATOMS: usize = 257;
    let dir = tempfile::tempdir().unwrap();
    let (_db, engine) = seeded_with(&dir.path().join("bounded.citadel"), ATOMS);
    let recording = Arc::new(BatchRecordingEmbedder {
        inner: NamedEmbedder::new(DIM as usize, "model-b"),
        calls: std::sync::atomic::AtomicUsize::new(0),
        largest: std::sync::atomic::AtomicUsize::new(0),
    });

    let report = engine
        .reembed_region("notes", recording.clone(), None)
        .unwrap();

    assert_eq!(report.atoms_migrated, ATOMS as u64);
    assert_eq!(recording.calls.load(Ordering::Relaxed), 2);
    assert_eq!(recording.largest.load(Ordering::Relaxed), 256);
}

/// A cancelled multi-batch migration remains closed and resumable.
#[test]
fn a_cancelled_reembed_is_refused_and_then_resumable() {
    const ATOMS: usize = 300;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("resume.citadel");
    let (db, engine) = seeded_with(&path, ATOMS);

    let token = citadel_core::CancelToken::new();
    let stopping = Arc::new(StoppingEmbedder {
        inner: NamedEmbedder::new(DIM as usize, "model-b"),
        token: token.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        calls_before_stop: 2,
    });

    let err = engine
        .reembed_region("notes", stopping, Some(&token))
        .expect_err("the migration should stop");
    assert!(format!("{err:?}").contains("Interrupted"), "got {err:?}");

    let reattach = engine.attach_existing_region(
        "notes",
        Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
    );
    let message = format!("{:?}", reattach.expect_err("attach must refuse"));
    assert!(
        message.contains("stopped") && message.contains("reembed_region"),
        "the refusal must name the way out, got {message}"
    );

    let wrong_target = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-c")),
            None,
        )
        .expect_err("a different pipeline must not resume committed model-b vectors");
    let message = wrong_target.to_string();
    assert!(
        message.contains("model-b") && message.contains("finish that one"),
        "the refusal must identify the durable target, got {message}"
    );

    engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .expect("resume should finish the migration");

    let identities = engine.stored_region_identities().unwrap();
    let region = identities.iter().find(|r| r.name() == "notes").unwrap();
    assert_eq!(region.model_id(), "model-b", "resume left the old model");

    // And once finished the region attaches normally again.
    drop(engine);
    drop(db);
}

#[test]
fn a_cancelled_repair_keeps_the_region_closed_until_ann_and_edges_retry() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded_with(&dir.path().join("repair-retry.citadel"), 12);
    seed_similarity_web(&db, &engine, 4);
    let token = citadel_core::CancelToken::new();
    db.debug_set_memory_edges_reweave_hook(Some(Box::new({
        let token = token.clone();
        move || token.cancel()
    })));

    let error = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            Some(&token),
        )
        .expect_err("cancellation in graph repair must keep the repair durable");
    assert!(format!("{error:?}").contains("Interrupted"));
    db.debug_set_memory_edges_reweave_hook(None);
    let refused = engine.count_region("notes").unwrap_err();
    assert!(refused.to_string().contains("reembed_region"));

    let report = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .expect("retry must rebuild the index and graph before opening the region");
    assert!(report.ann_rebuilt);
    assert!(report.similarity_edges_rewoven > 0);
    assert_eq!(engine.count_region("notes").unwrap(), 12);
}

/// Open-time reconciliation preserves both tables of an interrupted shape change.
#[test]
fn an_interrupted_shape_change_survives_a_restart() {
    const ATOMS: usize = 300;
    const NARROW: u16 = 8;
    const WIDE: u16 = 16;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("restart.citadel");

    {
        let db = fixture_vault(&path);
        let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
        engine
            .create_encrypted_region(
                "notes",
                Arc::new(NamedEmbedder::new(NARROW as usize, "model-a")),
            )
            .unwrap();
        for i in 0..ATOMS {
            engine
                .remember("notes", AtomInput::new("note", format!("atom number {i}")))
                .unwrap();
        }

        let token = citadel_core::CancelToken::new();
        let stopping = Arc::new(StoppingEmbedder {
            inner: NamedEmbedder::new(WIDE as usize, "model-b"),
            token: token.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            calls_before_stop: 2,
        });
        let err = engine
            .reembed_region("notes", stopping, Some(&token))
            .expect_err("the migration should stop");
        assert!(format!("{err:?}").contains("Interrupted"), "got {err:?}");
        drop(engine);
        drop(db);
    }

    let db = reopen(&path);
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();

    let source = format!("memory_atoms_d{NARROW}_cosine_enc");
    let destination = format!("memory_atoms_d{WIDE}_cosine_enc");
    let moved = count_rows(&db, &destination);
    assert_eq!(
        count_rows(&db, &source) + moved,
        ATOMS,
        "reconciliation erased rows the migration had already converted"
    );
    assert!(
        moved > 0,
        "the fixture never got as far as moving a row, so it proves nothing"
    );

    engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(WIDE as usize, "model-b")),
            None,
        )
        .expect("resume should finish the migration after a restart");
    assert_eq!(count_rows(&db, &destination), ATOMS);
    assert_eq!(engine.count_region("notes").unwrap(), ATOMS as u64);
}

/// A cancelled migration also closes handles attached before it began.
#[test]
fn a_cancelled_reembed_refuses_the_handle_that_was_already_attached() {
    const ATOMS: usize = 300;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("attached.citadel");
    let (_db, engine) = seeded_with(&path, ATOMS);

    assert_eq!(engine.count_region("notes").unwrap(), ATOMS as u64);

    let token = citadel_core::CancelToken::new();
    let stopping = Arc::new(StoppingEmbedder {
        inner: NamedEmbedder::new(DIM as usize, "model-b"),
        token: token.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        calls_before_stop: 2,
    });
    engine
        .reembed_region("notes", stopping, Some(&token))
        .expect_err("the migration should stop");

    for (what, message) in [
        (
            "count",
            format!("{:?}", engine.count_region("notes").unwrap_err()),
        ),
        (
            "recall",
            format!(
                "{:?}",
                engine
                    .recall(
                        "notes",
                        citadel_mem::RecallQuery::by_text("atom number 1", 5)
                    )
                    .unwrap_err()
            ),
        ),
        (
            "remember",
            format!(
                "{:?}",
                engine
                    .remember("notes", AtomInput::new("note", "written mid-migration"))
                    .unwrap_err()
            ),
        ),
    ] {
        assert!(
            message.contains("stopped") && message.contains("reembed_region"),
            "{what} through the attached handle must name the way out, got {message}"
        );
    }

    // Finishing it puts the region back in service.
    engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .unwrap();
    assert_eq!(engine.count_region("notes").unwrap(), ATOMS as u64);
}

/// A migration does not need the region attached, but the reweave that follows
/// it does. A process that opened the vault and went straight to `reembed_region`
/// would otherwise convert every vector, retire the mark, and only then fail -
/// leaving a retry to take the "already on this model" early return and skip the
/// web for good.
#[test]
fn a_reembed_completes_on_an_engine_that_never_attached_the_region() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unattached.citadel");
    {
        let (db, engine) = seeded_with(&path, 12);
        seed_similarity_web(&db, &engine, 4);
        drop(engine);
        drop(db);
    }

    let db = reopen(&path);
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    let report = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .expect("a re-embed must not require the caller to have attached first");

    assert!(
        report.similarity_edges_rewoven > 0,
        "the web was left asserting a nearness measured in the old vector space"
    );
}

/// A resumed migration must rebuild the whole web, not the part this run
/// happened to convert. The edges among the already-converted prefix were
/// measured in the old vector space and are just as stale as the rest.
///
/// Both target shapes, because they fail differently. Keeping the width means
/// the rows never leave their table, so the region is still readable in one
/// place; changing it MOVES converted rows, so on a resume the source table no
/// longer holds what an earlier run took, and a scope read from it is missing
/// exactly the atoms whose edges most need rebuilding.
#[test]
fn a_resumed_reembed_rebuilds_the_whole_similarity_web() {
    const ATOMS: usize = 300;
    let dir = tempfile::tempdir().unwrap();

    for (label, target_dim) in [("same shape", DIM), ("shape change", DIM * 2)] {
        let uninterrupted = {
            let (db, engine) = seeded_with(
                &dir.path().join(format!("whole-{target_dim}.citadel")),
                ATOMS,
            );
            seed_similarity_web(&db, &engine, 4);
            engine
                .reembed_region(
                    "notes",
                    Arc::new(NamedEmbedder::new(target_dim as usize, "model-b")),
                    None,
                )
                .unwrap()
                .similarity_edges_rewoven
        };

        let resumed_path = dir.path().join(format!("resumed-{target_dim}.citadel"));
        let (db, engine) = seeded_with(&resumed_path, ATOMS);
        seed_similarity_web(&db, &engine, 4);
        let token = citadel_core::CancelToken::new();
        let stopping = Arc::new(StoppingEmbedder {
            inner: NamedEmbedder::new(target_dim as usize, "model-b"),
            token: token.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            calls_before_stop: 2,
        });
        engine
            .reembed_region("notes", stopping, Some(&token))
            .expect_err("the migration should stop");
        drop(engine);
        drop(db);
        let db = reopen(&resumed_path);
        let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
        let resumed = engine
            .reembed_region(
                "notes",
                Arc::new(NamedEmbedder::new(target_dim as usize, "model-b")),
                None,
            )
            .unwrap()
            .similarity_edges_rewoven;

        assert!(uninterrupted > 0, "{label}: the fixture wove no edges");
        assert_eq!(
            resumed, uninterrupted,
            "{label}: the resume rebuilt only the atoms it converted itself"
        );
    }
}

/// Reweave pagination must not split or skip the source that crosses its page
/// boundary. A chain has one independently recoverable rule per source, so the
/// report and durable edge count are exact.
#[test]
fn reweaving_more_than_one_source_page_rebuilds_every_rule() {
    const ATOMS: usize = 258;
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded_with(&dir.path().join("paged-web.citadel"), ATOMS);
    seed_similarity_web(&db, &engine, ATOMS);
    assert_eq!(read_similarity_edges(&db).len(), ATOMS - 1);

    let report = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .unwrap();

    assert_eq!(report.similarity_edges_rewoven, (ATOMS - 1) as u64);
    assert_eq!(read_similarity_edges(&db).len(), ATOMS - 1);
}

/// One source may itself have more edges than a page. Keyset pagination must
/// carry that partially accumulated rule into the next page instead of treating
/// each page as a separate, smaller neighbourhood.
#[test]
fn one_high_degree_source_is_rebuilt_across_edge_pages() {
    const ATOMS: usize = 258;
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded_with(&dir.path().join("paged-fanout.citadel"), ATOMS);
    let ids: Vec<i64> = read_rows(&db, &format!("memory_atoms_d{DIM}_cosine"))
        .into_iter()
        .map(|row| row.id)
        .collect();
    engine.evolve("notes", ids[0], ATOMS - 1, f32::MAX).unwrap();

    let report = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .unwrap();

    assert_eq!(report.similarity_edges_rewoven, (ATOMS - 1) as u64);
    assert_eq!(read_similarity_edges(&db).len(), ATOMS - 1);
}

/// A token already tripped when the call arrives has converted nothing, so
/// there is nothing to resume. Marking the region anyway would take a healthy
/// one out of service for a migration that never began.
#[test]
fn an_already_cancelled_reembed_leaves_the_region_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded_with(&dir.path().join("nostart.citadel"), 12);
    let target_dim = DIM + 8;
    let target_table = format!("memory_atoms_d{target_dim}_cosine");
    let conn = citadel_sql::Connection::open(&db).unwrap();
    assert!(conn.table_schema(&target_table).is_none());
    drop(conn);

    let token = citadel_core::CancelToken::new();
    token.cancel();
    let err = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(target_dim as usize, "model-b")),
            Some(&token),
        )
        .expect_err("an already cancelled re-embed must refuse");
    assert!(format!("{err:?}").contains("Interrupted"), "got {err:?}");

    assert_eq!(engine.count_region("notes").unwrap(), 12);
    let conn = citadel_sql::Connection::open(&db).unwrap();
    assert!(
        conn.table_schema(&target_table).is_none(),
        "an already-cancelled migration must not create its destination table"
    );
    drop(conn);
    engine
        .attach_existing_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
        )
        .expect("a region that never started migrating must still attach");
}

/// The handle-wide token is an operation snapshot, not something each phase
/// may rediscover independently. Clearing it while an embedder callback is in
/// flight must not let the rest of the migration forget a cancellation that
/// already happened.
#[test]
fn reembed_retains_the_database_cancel_token_for_the_whole_operation() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded_with(&dir.path().join("database-token.citadel"), 12);
    let table = format!("memory_atoms_d{DIM}_cosine");
    let vectors_before = read_vectors(&db, &table);

    let token = citadel_core::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let stopping = Arc::new(CancelAndClearDatabaseEmbedder {
        inner: NamedEmbedder::new(DIM as usize, "model-b"),
        database: Arc::clone(&db),
        token,
    });

    let err = engine
        .reembed_region("notes", stopping, None)
        .expect_err("the retained Database token must stop the migration");
    assert!(format!("{err:?}").contains("Interrupted"), "got {err:?}");
    assert!(
        db.cancel_token().is_none(),
        "the callback did not clear the Database token, so the test proves nothing"
    );
    assert_eq!(
        read_vectors(&db, &table),
        vectors_before,
        "vectors computed after handle-wide cancellation became durable"
    );
    assert_eq!(
        engine.count_region("notes").unwrap(),
        12,
        "a zero-progress cancellation left the region out of service"
    );
}

/// Cancellation inside the embedder prevents that batch from becoming durable.
#[test]
fn a_cancel_inside_the_final_batch_is_not_reported_as_success() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lastbatch.citadel");
    let (db, engine) = seeded_with(&path, 12);
    let table = format!("memory_atoms_d{DIM}_cosine");
    let vectors_before = read_vectors(&db, &table);

    let token = citadel_core::CancelToken::new();
    let stopping = Arc::new(StoppingEmbedder {
        inner: NamedEmbedder::new(DIM as usize, "model-b"),
        token: token.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        calls_before_stop: 1,
    });

    let err = engine
        .reembed_region("notes", stopping, Some(&token))
        .expect_err("a cancel inside the only batch must not report success");
    assert!(format!("{err:?}").contains("Interrupted"), "got {err:?}");

    assert_eq!(
        read_vectors(&db, &table),
        vectors_before,
        "a batch computed after cancellation was made durable"
    );
    assert_eq!(
        engine.count_region("notes").unwrap(),
        12,
        "a first-batch cancellation left a repair mark despite writing nothing"
    );

    // With no durable checkpoint, a fresh call converts every atom.
    let report = engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
            None,
        )
        .expect("a fresh call should finish the migration");
    assert_eq!(report.atoms_migrated, 12);
    let identities = engine.stored_region_identities().unwrap();
    let region = identities.iter().find(|r| r.name() == "notes").unwrap();
    assert_eq!(region.model_id(), "model-b");
}

/// Zero-progress recovery ignores the operation's already-tripped token.
#[test]
fn zero_progress_cleanup_ignores_the_database_cancel_token() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded_with(&dir.path().join("cleanup-token.citadel"), 12);

    let token = citadel_core::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let stopping = Arc::new(StoppingEmbedder {
        inner: NamedEmbedder::new(DIM as usize, "model-b"),
        token: token.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        calls_before_stop: 1,
    });

    let err = engine
        .reembed_region("notes", stopping, Some(&token))
        .expect_err("the first embedder call trips the shared token");
    assert!(format!("{err:?}").contains("Interrupted"), "got {err:?}");

    db.set_cancel(None);
    assert_eq!(engine.count_region("notes").unwrap(), 12);
    engine
        .attach_existing_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
        )
        .expect("zero-progress cancellation must not strand a migration mark");
}

/// A failed new target restores the prior target's repair checkpoint.
#[test]
fn zero_progress_new_target_restores_the_prior_repair_mark() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded_with(&dir.path().join("restore-repair.citadel"), 12);
    let conn = citadel_sql::Connection::open(&db).unwrap();
    conn.execute_params(
        "UPDATE memory_regions SET metadata = $1 WHERE name = 'notes'",
        &[citadel_sql::Value::Text(
            serde_json::json!({
                "keep": "unrelated",
                "reembed_to_model": "model-a",
                "reembed_to_dim": DIM,
                "reembed_to_metric": "cosine",
                "reembed_done_through": 12,
                "reembed_phase": "repair"
            })
            .to_string()
            .into(),
        )],
    )
    .unwrap();

    let token = citadel_core::CancelToken::new();
    let stopping = Arc::new(StoppingEmbedder {
        inner: NamedEmbedder::new(DIM as usize, "model-b"),
        token: token.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        calls_before_stop: 1,
    });
    engine
        .reembed_region("notes", stopping, Some(&token))
        .expect_err("target B must stop before its first durable batch");

    let fields = conn
        .query(
            "SELECT metadata ->> 'reembed_to_model', metadata ->> 'reembed_phase', \
             metadata ->> 'keep' FROM memory_regions WHERE name = 'notes'",
        )
        .unwrap();
    assert_eq!(format!("{:?}", fields.rows[0][0]), "Text(\"model-a\")");
    assert_eq!(format!("{:?}", fields.rows[0][1]), "Text(\"repair\")");
    assert_eq!(format!("{:?}", fields.rows[0][2]), "Text(\"unrelated\")");

    // A retry of A must see and retire the restored repair mark.
    engine
        .reembed_region(
            "notes",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
            None,
        )
        .expect("the older repair remains resumable");
    let fields = conn
        .query(
            "SELECT metadata ->> 'reembed_to_model', metadata ->> 'keep' \
             FROM memory_regions WHERE name = 'notes'",
        )
        .unwrap();
    assert!(matches!(fields.rows[0][0], citadel_sql::Value::Null));
    assert_eq!(format!("{:?}", fields.rows[0][1]), "Text(\"unrelated\")");
}

/// Reweaving serializes edge writers across engines sharing one database.
#[test]
fn reweave_serializes_edge_writers_across_memory_engines() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = seeded_with(&dir.path().join("edge-race.citadel"), 4);
    let ids: Vec<i64> = read_rows(&db, &format!("memory_atoms_d{DIM}_cosine"))
        .into_iter()
        .map(|row| row.id)
        .collect();
    let src = ids[0];
    engine.evolve("notes", src, 1, f32::MAX).unwrap();
    engine
        .create_region(
            "foreign",
            Arc::new(NamedEmbedder::new(DIM as usize, "model-a")),
        )
        .unwrap();
    let foreign = engine
        .remember("foreign", AtomInput::new("note", "foreign target"))
        .unwrap();
    let writer_engine = MemoryEngine::open(Arc::clone(&db)).unwrap();

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let fired = Arc::new(AtomicBool::new(false));
    db.debug_set_memory_edges_reweave_hook(Some(Box::new({
        let fired = Arc::clone(&fired);
        let release_rx = Arc::clone(&release_rx);
        move || {
            if !fired.swap(true, Ordering::SeqCst) {
                let _ = entered_tx.send(());
                let _ = release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5));
            }
        }
    })));

    let migration = {
        let engine = Arc::new(engine);
        std::thread::spawn(move || {
            engine.reembed_region(
                "notes",
                Arc::new(NamedEmbedder::new(DIM as usize, "model-b")),
                None,
            )
        })
    };
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("reweave hook was not reached");

    let (attempt_tx, attempt_rx) = mpsc::channel();
    db.debug_set_memory_edges_acquire_hook(Some(Box::new(move || {
        let _ = attempt_tx.send(());
    })));
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        done_tx
            .send(writer_engine.link(src, foreign, citadel_mem::EdgeKind::SimilarTo, 0.7))
            .unwrap();
    });
    let writer_reached_shared_lock = attempt_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    let repair_holds_shared_lock = db.debug_memory_edges_is_locked();
    release_tx.send(()).unwrap();

    migration
        .join()
        .expect("re-embed thread panicked")
        .expect("re-embed failed");
    db.debug_set_memory_edges_reweave_hook(None);
    db.debug_set_memory_edges_acquire_hook(None);
    assert!(
        writer_reached_shared_lock,
        "the second MemoryEngine did not reach the shared edge guard"
    );
    assert!(
        repair_holds_shared_lock,
        "the complete repair did not hold the Database-scoped edge guard"
    );
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("edge writer stayed blocked after repair")
        .expect("edge writer failed");
    assert!(
        read_similarity_edges(&db)
            .iter()
            .any(|&(edge_src, edge_dst, _)| edge_src == src && edge_dst == foreign),
        "the edge committed after repair was lost"
    );
}
