//! Repair incorrect legacy model provenance without rewriting atom data.
//! Fixtures preserve the already-written state that a fresh region cannot reproduce.

mod upgrade_fixtures;

use std::sync::Arc;

use citadel_mem::{MemError, MemoryEngine};
use upgrade_fixtures::{fixture_vault, seed_shim_region, NamedEmbedder};

const DIM: u16 = 32;
const REGION: &str = "shimmed";
/// What the integration's caller actually embedded with.
const REAL_MODEL: &str = "text-embedding-3-small";

/// A vault in the state a shim integration left behind.
fn shim_vault(path: &std::path::Path) -> Arc<citadel::Database> {
    let db = fixture_vault(path);
    seed_shim_region(&db, REGION, &["alpha beta", "gamma delta"]);
    db
}

fn recorded_model(db: &Arc<citadel::Database>) -> String {
    let conn = citadel_sql::Connection::open(db).unwrap();
    let qr = conn
        .query("SELECT model_id FROM memory_regions WHERE name = 'shimmed'")
        .unwrap();
    match &qr.rows.first().expect("the region row")[0] {
        citadel_sql::Value::Text(s) => s.to_string(),
        other => panic!("expected text, got {other:?}"),
    }
}

/// Every atom column including the vector, so "touched nothing" is checkable
/// rather than asserted about a chosen subset.
fn atom_rows(db: &Arc<citadel::Database>) -> Vec<String> {
    let conn = citadel_sql::Connection::open(db).unwrap();
    let qr = conn
        .query(&format!(
            "SELECT id, region_id, kind, embedding, payload, text_content, score, confidence, \
             access_count, immutable, created_at, accessed_at, expires_at \
             FROM memory_atoms_d{DIM}_cosine ORDER BY id"
        ))
        .unwrap();
    qr.rows.iter().map(|r| format!("{r:?}")).collect()
}

fn real_model() -> Arc<NamedEmbedder> {
    Arc::new(NamedEmbedder::new(DIM as usize, REAL_MODEL))
}

/// A model mismatch names both provenance repair choices.
#[test]
fn the_real_model_is_refused_and_the_message_names_the_repair() {
    let dir = tempfile::tempdir().unwrap();
    let db = shim_vault(&dir.path().join("shim.citadel"));
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();

    let err = engine
        .attach_existing_region(REGION, real_model())
        .expect_err("a region labelled 'mock' must not silently accept another model");

    assert!(
        matches!(err, MemError::ModelMismatch { .. }),
        "expected a model mismatch, got {err:?}"
    );
    let message = err.to_string();
    assert!(
        message.contains("reclassify_region"),
        "the refusal must name the call that repairs a wrong label: {message}"
    );
    assert!(
        message.contains("reembed_region"),
        "and the call that repairs wrong vectors, since only the caller knows \
         which fault this is: {message}"
    );
}

#[test]
fn reclassifying_lets_the_model_that_wrote_the_vectors_attach() {
    let dir = tempfile::tempdir().unwrap();
    let db = shim_vault(&dir.path().join("repair.citadel"));
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();

    engine
        .reclassify_region(REGION, REAL_MODEL.to_string())
        .expect("reclassify");

    assert_eq!(recorded_model(&db), REAL_MODEL);
    engine
        .attach_existing_region(REGION, real_model())
        .expect("the region now records the model that wrote it");
}

/// Reclassification changes provenance metadata only.
#[test]
fn reclassifying_changes_no_atom() {
    let dir = tempfile::tempdir().unwrap();
    let db = shim_vault(&dir.path().join("untouched.citadel"));
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();

    let before = atom_rows(&db);
    assert_eq!(before.len(), 2, "the fixture seeded two atoms");

    engine
        .reclassify_region(REGION, REAL_MODEL.to_string())
        .expect("reclassify");

    assert_eq!(
        atom_rows(&db),
        before,
        "every atom column, the vector included, must be identical"
    );
}

/// Reclassification detaches an embedder that contradicts the new provenance.
#[test]
fn reclassifying_ends_an_attachment_the_new_label_contradicts() {
    let dir = tempfile::tempdir().unwrap();
    let db = shim_vault(&dir.path().join("attached.citadel"));
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();

    engine
        .attach_existing_region(REGION, Arc::new(NamedEmbedder::new(DIM as usize, "mock")))
        .expect("the legacy mock label matches the legacy fixture embedder");

    engine
        .reclassify_region(REGION, REAL_MODEL.to_string())
        .expect("relabelling is the caller's assertion to make");
    assert_eq!(recorded_model(&db), REAL_MODEL);

    let err = engine
        .remember(REGION, citadel_mem::AtomInput::new("note", "epsilon zeta"))
        .expect_err("the contradicted attachment is gone");
    assert!(
        matches!(err, MemError::RegionNotAttached(ref name) if name == REGION),
        "expected the region to be detached, got {err:?}"
    );

    engine
        .attach_existing_region(REGION, real_model())
        .expect("the label and the model agree again");
    assert_eq!(engine.count_region(REGION).unwrap(), 2);
}

/// A model the new label agrees with is kept.
#[test]
fn reclassifying_to_the_attached_model_keeps_it_attached() {
    let dir = tempfile::tempdir().unwrap();
    let db = shim_vault(&dir.path().join("agrees.citadel"));
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();

    engine
        .attach_existing_region(REGION, Arc::new(NamedEmbedder::new(DIM as usize, "mock")))
        .unwrap();
    engine
        .reclassify_region(REGION, "mock".to_string())
        .expect("recording what is already true contradicts nothing");

    assert_eq!(recorded_model(&db), "mock");
    engine
        .remember(REGION, citadel_mem::AtomInput::new("note", "epsilon zeta"))
        .expect("the model that agrees with the label still embeds");
}

#[test]
fn reclassifying_an_unknown_region_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let db = shim_vault(&dir.path().join("absent.citadel"));
    let engine = MemoryEngine::open(Arc::clone(&db)).unwrap();

    let err = engine
        .reclassify_region("no-such-region", REAL_MODEL.to_string())
        .expect_err("a region that does not exist cannot be relabelled");
    assert!(
        matches!(err, MemError::RegionNotFound(_)),
        "expected a not-found, got {err:?}"
    );
}
