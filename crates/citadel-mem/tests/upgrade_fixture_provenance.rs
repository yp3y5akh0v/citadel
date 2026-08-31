mod upgrade_fixtures;

use citadel_mem::{
    AtomInput, EmbedError, Embedder, EmbeddingMetric, MemError, MemoryEngine, MockEmbedder,
    RecallQuery,
};
use upgrade_fixtures::{
    fixture_vault, seed_ordinary_region, seed_shim_region, NamedEmbedder, SHIM_VECTOR_MODEL,
};

struct LegacyMockEmbedder(MockEmbedder);

impl Embedder for LegacyMockEmbedder {
    fn dim(&self) -> usize {
        self.0.dim()
    }

    fn metric(&self) -> EmbeddingMetric {
        self.0.metric()
    }

    fn model_id(&self) -> &str {
        "mock"
    }

    fn embed_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        self.0.embed_with_cancel(texts, cancel)
    }
}

#[test]
fn an_ordinary_fixture_region_records_its_model() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture_vault(&dir.path().join("ordinary.citadel"));
    seed_ordinary_region(&db, "notes", &["alpha beta", "gamma delta"]);

    let engine = MemoryEngine::open(std::sync::Arc::clone(&db)).unwrap();
    let identities = engine.stored_region_identities().unwrap();
    let region = identities
        .iter()
        .find(|region| region.name() == "notes")
        .expect("seeded region");
    assert_eq!(region.model_id(), "fixture-model-v1");
}

#[test]
fn a_shim_fixture_records_mock_over_real_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture_vault(&dir.path().join("shim.citadel"));
    let text = "one two";
    seed_shim_region(&db, "documents", &[text]);

    let conn = citadel_sql::Connection::open(&db).unwrap();
    let row = conn
        .query(
            "SELECT r.model_id, a.embedding FROM memory_regions r \
             JOIN memory_atoms_d32_cosine a ON a.region_id = r.id \
             WHERE r.name = 'documents'",
        )
        .unwrap()
        .rows
        .into_iter()
        .next()
        .expect("seeded atom");
    let citadel_sql::Value::Text(model) = &row[0] else {
        panic!("expected model text, got {:?}", row[0]);
    };
    let citadel_sql::Value::Vector(stored) = &row[1] else {
        panic!("expected stored vector, got {:?}", row[1]);
    };

    let real = NamedEmbedder::new(32, SHIM_VECTOR_MODEL)
        .embed(&[text])
        .unwrap()
        .remove(0);
    let mock = MockEmbedder::new(32).embed(&[text]).unwrap().remove(0);

    assert_eq!(model.as_str(), "mock");
    assert_eq!(stored.as_ref(), real.as_slice());
    assert_ne!(stored.as_ref(), mock.as_slice());
}

#[test]
fn provenance_fixtures_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reopened.citadel");
    {
        let db = fixture_vault(&path);
        seed_ordinary_region(&db, "notes", &["alpha"]);
        seed_shim_region(&db, "documents", &["beta"]);
    }

    let db = upgrade_fixtures::reopen(&path);
    let engine = MemoryEngine::open(std::sync::Arc::clone(&db)).unwrap();
    let mut models: Vec<_> = engine
        .stored_region_identities()
        .unwrap()
        .iter()
        .map(|region| (region.name().to_string(), region.model_id().to_string()))
        .collect();
    models.sort();
    assert_eq!(
        models,
        vec![
            ("documents".to_string(), "mock".to_string()),
            ("notes".to_string(), "fixture-model-v1".to_string()),
        ]
    );
}

#[test]
fn a_genuine_legacy_mock_region_can_be_explicitly_reclassified() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture_vault(&dir.path().join("legacy-mock.citadel"));
    let legacy = MemoryEngine::open(std::sync::Arc::clone(&db)).unwrap();
    legacy
        .create_region(
            "notes",
            std::sync::Arc::new(LegacyMockEmbedder(MockEmbedder::new(32))),
        )
        .unwrap();
    legacy
        .remember("notes", AtomInput::new("fact", "alpha beta"))
        .unwrap();
    drop(legacy);

    let current = std::sync::Arc::new(MockEmbedder::new(32));
    let current_model = current.model_id().to_owned();
    let reopened = MemoryEngine::open(db).unwrap();
    assert!(matches!(
        reopened.attach_existing_region("notes", current.clone()),
        Err(MemError::ModelMismatch { .. })
    ));

    reopened
        .reclassify_region("notes", current_model.clone())
        .unwrap();
    reopened.attach_existing_region("notes", current).unwrap();
    let hits = reopened
        .recall("notes", RecallQuery::by_text("alpha beta", 1))
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].text, "alpha beta");
    assert_eq!(
        reopened
            .stored_region_identity("notes")
            .unwrap()
            .unwrap()
            .model_id(),
        current_model
    );
}
