//! Benchmark database setup: open-or-create with optional persist + reuse.
//!
//! No path -> throwaway temp DB, re-ingested every run. With
//! `CITADEL_*_DB_PATH` set, later runs reopen and reuse: regions re-attach from
//! stored vectors and the ANN segment rebuilds on first recall, so a multi-hour
//! ingest drops to seconds next run.

use std::path::PathBuf;
use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder};

use crate::core::error::Result;

/// A benchmark database plus whether it was reopened from a persisted file.
pub struct BenchDb {
    /// Open database, ready for `citadel_mem::MemoryEngine::open`.
    pub db: Arc<Database>,
    /// Set when a persisted DB was reopened: the caller must skip ingestion
    /// (regions already hold the atoms) and recall from the stored vectors.
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
/// - set, present -> open + reuse; caller skips ingest ([`BenchDb::reuse`]).
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
