use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{AtomInput, EdgeKind, MemoryEngine, MockEmbedder};
use citadel_sql::{Connection, Value};

const DIM: usize = 8;
const TABLE: &str = "memory_atoms_d8_cosine";

fn setup(path: &std::path::Path) -> (Arc<Database>, MemoryEngine) {
    let db = Arc::new(
        DatabaseBuilder::new(path)
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    engine
        .create_region("notes", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    (db, engine)
}

fn count(db: &Arc<Database>, table: &str) -> i64 {
    let rows = Connection::open(db)
        .unwrap()
        .query(&format!("SELECT COUNT(*) FROM {table}"))
        .unwrap();
    match &rows.rows[0][0] {
        Value::Integer(value) => *value,
        other => panic!("expected count, got {other:?}"),
    }
}

fn seed(engine: &MemoryEngine) -> Vec<i64> {
    ["alpha", "beta", "gamma"]
        .into_iter()
        .map(|text| {
            engine
                .remember("notes", AtomInput::new("note", text))
                .unwrap()
        })
        .collect()
}

#[test]
fn managed_similarity_state_follows_atom_and_region_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = setup(&dir.path().join("lifecycle.citadel"));
    let ids = seed(&engine);
    engine.evolve("notes", ids[0], 2, f32::MAX).unwrap();
    assert_eq!(count(&db, "memory_similarity_policies"), 1);
    assert_eq!(count(&db, "memory_similarity_edges"), 2);

    engine.delete_atoms("notes", &[ids[1]]).unwrap();
    assert_eq!(count(&db, "memory_similarity_policies"), 1);
    assert_eq!(count(&db, "memory_similarity_edges"), 1);
    assert_eq!(count(&db, "memory_edges"), 1);

    engine.delete_atoms("notes", &[ids[0]]).unwrap();
    assert_eq!(count(&db, "memory_similarity_policies"), 0);
    assert_eq!(count(&db, "memory_similarity_edges"), 0);
    assert_eq!(count(&db, "memory_edges"), 0);

    engine
        .create_region("other", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let a = engine
        .remember("other", AtomInput::new("note", "one"))
        .unwrap();
    engine
        .remember("other", AtomInput::new("note", "two"))
        .unwrap();
    engine.evolve("other", a, 1, f32::MAX).unwrap();
    engine.drop_region("other").unwrap();
    assert_eq!(count(&db, "memory_similarity_policies"), 0);
    assert_eq!(count(&db, "memory_similarity_edges"), 0);
    assert_eq!(count(&db, "memory_edges"), 0);
}

#[test]
fn open_reconciles_similarity_state_left_by_out_of_band_atom_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = setup(&dir.path().join("reconcile.citadel"));
    let ids = seed(&engine);
    engine.evolve("notes", ids[0], 2, f32::MAX).unwrap();

    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("DELETE FROM {TABLE} WHERE id = $1"),
            &[Value::Integer(ids[1])],
        )
        .unwrap();
    drop(engine);
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert_eq!(count(&db, "memory_similarity_policies"), 1);
    assert_eq!(count(&db, "memory_similarity_edges"), 1);
    assert_eq!(count(&db, "memory_edges"), 1);

    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("DELETE FROM {TABLE} WHERE id = $1"),
            &[Value::Integer(ids[0])],
        )
        .unwrap();
    drop(engine);
    MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert_eq!(count(&db, "memory_similarity_policies"), 0);
    assert_eq!(count(&db, "memory_similarity_edges"), 0);
    assert_eq!(count(&db, "memory_edges"), 0);
}

#[test]
fn reconciliation_deletes_only_exact_tracked_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = setup(&dir.path().join("exact-owned-pairs.citadel"));
    let ids = seed(&engine);
    let (authored_src, tracked_src, shared_dst) = (ids[0], ids[1], ids[2]);
    let wrong_region = engine
        .create_region("other", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    engine
        .link(authored_src, shared_dst, EdgeKind::SimilarTo, 0.25)
        .unwrap();
    engine
        .link(tracked_src, shared_dst, EdgeKind::SimilarTo, 0.5)
        .unwrap();

    let conn = Connection::open(&db).unwrap();
    for src in [authored_src, tracked_src] {
        conn.execute_params(
            "INSERT INTO memory_similarity_policies \
             (region_id, src_id, neighbors, max_distance, kinds) \
             VALUES ($1, $2, $3, $4, $5)",
            &[
                Value::Integer(wrong_region),
                Value::Integer(src),
                Value::Integer(1),
                Value::Real(1.0),
                Value::Text("[]".into()),
            ],
        )
        .unwrap();
    }
    conn.execute_params(
        "INSERT INTO memory_similarity_edges (src_id, dst_id) VALUES ($1, $2)",
        &[Value::Integer(tracked_src), Value::Integer(shared_dst)],
    )
    .unwrap();
    drop(conn);
    drop(engine);

    MemoryEngine::open(Arc::clone(&db)).unwrap();
    let edges = Connection::open(&db)
        .unwrap()
        .query("SELECT src_id, dst_id FROM memory_edges WHERE kind = 'similar_to'")
        .unwrap();
    assert_eq!(edges.rows.len(), 1);
    assert!(matches!(edges.rows[0][0], Value::Integer(value) if value == authored_src));
    assert!(matches!(edges.rows[0][1], Value::Integer(value) if value == shared_dst));
    assert_eq!(count(&db, "memory_similarity_edges"), 0);
    assert_eq!(count(&db, "memory_similarity_policies"), 0);
}

#[test]
fn a_tracker_without_a_policy_is_untracked_not_reclassified_or_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = setup(&dir.path().join("untracked.citadel"));
    let ids = seed(&engine);
    engine.evolve("notes", ids[0], 1, f32::MAX).unwrap();
    Connection::open(&db)
        .unwrap()
        .execute("DELETE FROM memory_similarity_policies")
        .unwrap();
    drop(engine);

    MemoryEngine::open(Arc::clone(&db)).unwrap();
    assert_eq!(count(&db, "memory_similarity_policies"), 0);
    assert_eq!(count(&db, "memory_similarity_edges"), 0);
    assert_eq!(count(&db, "memory_edges"), 1);
}

#[test]
fn explicitly_relinking_a_managed_pair_makes_it_authored() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = setup(&dir.path().join("authored.citadel"));
    let ids = seed(&engine);
    engine.evolve("notes", ids[0], 1, f32::MAX).unwrap();
    let conn = Connection::open(&db).unwrap();
    let tracked = conn
        .query("SELECT src_id, dst_id FROM memory_similarity_edges")
        .unwrap();
    let src = match &tracked.rows[0][0] {
        Value::Integer(value) => *value,
        other => panic!("expected source id, got {other:?}"),
    };
    let dst = match &tracked.rows[0][1] {
        Value::Integer(value) => *value,
        other => panic!("expected destination id, got {other:?}"),
    };
    drop(conn);

    engine.link(src, dst, EdgeKind::SimilarTo, 0.25).unwrap();
    assert_eq!(count(&db, "memory_similarity_policies"), 1);
    assert_eq!(count(&db, "memory_similarity_edges"), 0);
    let edge = Connection::open(&db)
        .unwrap()
        .query_params(
            "SELECT weight FROM memory_edges WHERE src_id = $1 AND dst_id = $2 \
             AND kind = 'similar_to'",
            &[Value::Integer(src), Value::Integer(dst)],
        )
        .unwrap();
    assert!(matches!(&edge.rows[0][0], Value::Real(value) if *value == 0.25));
}

#[test]
fn legacy_similarity_is_replaced_only_through_the_explicit_migration_path() {
    let dir = tempfile::tempdir().unwrap();
    let (db, engine) = setup(&dir.path().join("legacy-similarity.citadel"));
    let ids = seed(&engine);
    engine
        .link(ids[0], ids[1], EdgeKind::SimilarTo, 0.25)
        .unwrap();

    engine.evolve("notes", ids[0], 0, f32::MAX).unwrap();
    assert_eq!(count(&db, "memory_edges"), 1);
    assert_eq!(count(&db, "memory_similarity_edges"), 0);

    engine
        .evolve_replacing_similarity("notes", ids[0], 0, f32::MAX)
        .unwrap();
    assert_eq!(count(&db, "memory_edges"), 0);
    assert_eq!(count(&db, "memory_similarity_edges"), 0);
    assert_eq!(count(&db, "memory_similarity_policies"), 1);
}
