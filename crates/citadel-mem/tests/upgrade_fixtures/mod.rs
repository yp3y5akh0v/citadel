// Each integration test binary uses a different subset of these shared fixtures.
#![allow(dead_code)]

//! Shared fixtures for re-embedding and repairing legacy provenance.

use std::path::Path;
use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{Embedder, EmbeddingMetric, MemoryEngine, MockEmbedder};

pub const PASSPHRASE: &[u8] = b"upgrade-fixture-passphrase";
pub const SHIM_VECTOR_MODEL: &str = "text-embedding-3-small";

/// An embedder standing in for a real model: it records a genuine `model_id`,
/// which is what separates an ordinary legacy region from a shim-written one.
pub struct NamedEmbedder {
    inner: MockEmbedder,
    model_id: &'static str,
}

impl NamedEmbedder {
    pub fn new(dim: usize, model_id: &'static str) -> Self {
        Self {
            inner: MockEmbedder::new(dim),
            model_id,
        }
    }
}

impl Embedder for NamedEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }

    fn model_id(&self) -> &str {
        self.model_id
    }

    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, citadel_mem::EmbedError> {
        let seed = self.model_id.bytes().fold(0u64, |acc, b| {
            acc.wrapping_mul(1_099_511_628_211).wrapping_add(b as u64)
        });
        Ok(self
            .inner
            .embed(texts)?
            .into_iter()
            .map(|mut vector| {
                for (index, value) in vector.iter_mut().enumerate() {
                    let lane = seed.rotate_left((index % 64) as u32);
                    let scale = 0.5 + (lane & 0xff) as f32 / 255.0;
                    let bias = (((lane >> 8) & 0xff) as f32 / 255.0 - 0.5) * 0.2;
                    *value = *value * scale + bias;
                }
                let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm > 0.0 {
                    for value in &mut vector {
                        *value /= norm;
                    }
                }
                vector
            })
            .collect())
    }
}

pub fn fixture_vault(path: &Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(path)
            .passphrase(PASSPHRASE)
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .create()
            .expect("fixture vault"),
    )
}

/// Reopen a fixture vault built earlier: a file this process did not create.
pub fn reopen(path: &Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(path)
            .passphrase(PASSPHRASE)
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .open()
            .expect("reopen the fixture"),
    )
}

/// A region written the way an earlier release would have written it: with a
/// real embedder, so `model_id` names an actual model.
pub fn seed_ordinary_region(db: &Arc<Database>, region: &str, atoms: &[&str]) {
    let engine = MemoryEngine::open(Arc::clone(db)).expect("engine");
    let embedder = Arc::new(NamedEmbedder::new(32, "fixture-model-v1"));
    engine
        .create_region(region, embedder)
        .expect("create region");
    seed_atoms(&engine, region, atoms);
}

/// A region as the shim integrations left it: `model_id = "mock"` recorded over
/// vectors the caller supplied.
pub fn seed_shim_region(db: &Arc<Database>, region: &str, atoms: &[&str]) {
    let engine = MemoryEngine::open(Arc::clone(db)).expect("engine");
    let embedder = Arc::new(NamedEmbedder::new(32, "mock"));
    engine
        .create_region(region, embedder)
        .expect("create region");
    let real = NamedEmbedder::new(32, SHIM_VECTOR_MODEL);
    for text in atoms {
        let vector = real
            .embed(&[*text])
            .expect("fixture embedding")
            .pop()
            .expect("one fixture embedding");
        engine
            .remember(
                region,
                citadel_mem::AtomInput::new("note", *text).with_embedding(vector),
            )
            .expect("remember");
    }
}

fn seed_atoms(engine: &MemoryEngine, region: &str, atoms: &[&str]) {
    for text in atoms {
        engine
            .remember(region, citadel_mem::AtomInput::new("note", *text))
            .expect("remember");
    }
}
