//! Benchmark database setup: open-or-create with optional persist + reuse.
//!
//! Without a path, each run uses a temporary database. Persisted regions are
//! validated against their ingestion inputs before reusing stored vectors.

use std::path::PathBuf;
use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{AtomInput, Embedder, FetchQuery, MemoryEngine};

use crate::core::error::{BenchError, Result};

/// A benchmark database plus whether it was reopened from a persisted file.
pub struct BenchDb {
    /// Open database, ready for `citadel_mem::MemoryEngine::open`.
    pub db: Arc<Database>,
    /// Set when a persisted DB was reopened. Validate each stored corpus before
    /// skipping ingestion with [`validate_ingested_atoms`].
    pub reuse: bool,
    /// Resolved data-file path (a temp path when not persisted), for logging.
    pub path: PathBuf,
    // RAII guard: deletes the temp dir on drop. Held, never read.
    #[allow(dead_code)]
    tmp: Option<tempfile::TempDir>,
}

/// Open the DB named by `path_env`, or a throwaway temp dir when unset:
/// - unset        -> fresh temp dir, deleted on exit.
/// - set, missing -> create + persist; caller ingests once.
/// - set, present -> open; caller validates the corpus before skipping ingest.
///
/// `encrypted` must match across runs so the region wrap keys and
/// `verify_matches` (dim/metric/model/encrypted) agree.
pub fn open_bench_db(path_env: &str, encrypted: bool) -> Result<BenchDb> {
    let persisted = std::env::var(path_env).ok().filter(|p| !p.is_empty());
    let (path, tmp) = match persisted {
        Some(p) => (PathBuf::from(p), None),
        None => {
            let dir = tempfile::tempdir()?;
            let file = dir.path().join("membench.cdl");
            (file, Some(dir))
        }
    };

    let reuse = path.exists();
    let mut builder = DatabaseBuilder::new(&path)
        .passphrase(b"membench")
        .argon2_profile(Argon2Profile::Iot);
    if encrypted {
        builder = builder.enable_region_keys(true);
    }
    let db = if reuse {
        builder.open()?
    } else {
        builder.create()?
    };
    Ok(BenchDb {
        db: Arc::new(db),
        reuse,
        path,
        tmp,
    })
}

/// Verify the live, unexpired corpus against ingestion inputs in atom-ID order.
/// Missing, extra or changed live atoms require a fresh corpus; nothing is overwritten.
pub fn validate_ingested_atoms(
    eng: &MemoryEngine,
    region: &str,
    expected: impl IntoIterator<Item = AtomInput>,
) -> Result<()> {
    let mut expected = expected.into_iter();
    let mut query = FetchQuery::new(256);
    let mismatch = || {
        BenchError::Dataset(format!(
            "stored corpus for region {region:?} is incomplete or differs from current ingestion; \
             use a new benchmark database path"
        ))
    };
    loop {
        let hits = eng.fetch_range(region, &query)?;
        if hits.is_empty() {
            return if expected.next().is_none() {
                Ok(())
            } else {
                Err(mismatch())
            };
        }
        for hit in &hits {
            let input = expected.next().ok_or_else(&mismatch)?;
            if hit.kind != input.kind
                || hit.text != input.text
                || hit.payload != input.payload
                || hit.importance != input.importance
                || hit.confidence != input.confidence
                || hit.expires_at != input.expires_at
                || hit.immutable != input.immutable
                || input
                    .created_at
                    .is_some_and(|created| created != hit.created_at)
            {
                return Err(mismatch());
            }
        }
        query.after_id = hits.last().map(|hit| hit.id);
    }
}

/// Attach an existing corpus without creating missing regions or changing its mode.
pub fn attach_reused_region(
    eng: &MemoryEngine,
    region: &str,
    embedder: Arc<dyn Embedder>,
    encrypted: bool,
) -> Result<()> {
    let identity = eng.stored_region_identity(region)?.ok_or_else(|| {
        BenchError::Dataset(format!("stored corpus region {region:?} is missing"))
    })?;
    if identity.encrypted() != encrypted {
        return Err(BenchError::Dataset(format!(
            "stored corpus region {region:?} encryption mode differs from the requested mode"
        )));
    }
    eng.attach_existing_region(region, embedder)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel_mem::MockEmbedder;

    fn inputs(count: usize) -> Vec<AtomInput> {
        (0..count)
            .map(|index| {
                AtomInput::new("turn", format!("turn {index}"))
                    .with_created_at(1_000_000)
                    .with_payload(serde_json::json!({"occurrence": index}))
            })
            .collect()
    }

    #[test]
    fn reused_corpus_checks_every_page_without_overwriting() {
        for encrypted in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = Arc::new(
                DatabaseBuilder::new(dir.path().join("corpus.cdl"))
                    .passphrase(b"test")
                    .argon2_profile(Argon2Profile::Iot)
                    .enable_region_keys(encrypted)
                    .create()
                    .unwrap(),
            );
            let eng = MemoryEngine::open(db).unwrap();
            let embedder = Arc::new(MockEmbedder::new(2));
            if encrypted {
                eng.create_encrypted_region("sample", embedder.clone())
                    .unwrap();
            } else {
                eng.create_region("sample", embedder.clone()).unwrap();
            }
            assert!(attach_reused_region(&eng, "missing", embedder.clone(), encrypted).is_err());
            assert!(eng.stored_region_identity("missing").unwrap().is_none());
            assert!(attach_reused_region(&eng, "sample", embedder.clone(), !encrypted).is_err());
            attach_reused_region(&eng, "sample", embedder, encrypted).unwrap();
            eng.remember_batch("sample", inputs(257)).unwrap();
            validate_ingested_atoms(&eng, "sample", inputs(257)).unwrap();
            assert!(validate_ingested_atoms(&eng, "sample", inputs(256)).is_err());
            assert!(validate_ingested_atoms(&eng, "sample", inputs(258)).is_err());
            let mut changed = inputs(257);
            changed[256].payload = serde_json::json!({"old_schema": true});
            assert!(validate_ingested_atoms(&eng, "sample", changed).is_err());
            let mut changed = inputs(257);
            changed[0].text = "different text".into();
            assert!(validate_ingested_atoms(&eng, "sample", changed).is_err());
            let mut changed = inputs(257);
            changed[0].created_at = Some(2_000_000);
            assert!(validate_ingested_atoms(&eng, "sample", changed).is_err());
            let mut changed = inputs(257);
            changed.swap(0, 1);
            assert!(validate_ingested_atoms(&eng, "sample", changed).is_err());
            validate_ingested_atoms(&eng, "sample", inputs(257)).unwrap();
            assert_eq!(eng.count_region("sample").unwrap(), 257);
        }
    }
}
