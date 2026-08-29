//! Serve a citadel memory region over MCP: open db, attach region, run stdio
//! loop.

use std::path::Path;
use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{Embedder, EmbeddingMetric, MemoryEngine, MockEmbedder};
use zeroize::Zeroizing;

/// Default mock dimension when the region has no persisted identity.
const DEFAULT_MOCK_DIM: usize = 256;

/// Resolved configuration for serving one region.
#[derive(Debug)]
pub struct ServeConfig {
    pub db: String,
    pub region: String,
    pub encrypted: bool,
    pub embedder: String,
    pub model_dir: Option<String>,
    pub models_dir: Option<String>,
    pub reranker: Option<String>,
    pub reranker_dir: Option<String>,
}

#[derive(Debug)]
struct ModelSelection<'a> {
    embedder: &'a str,
    #[cfg(feature = "candle-embed")]
    reranker: Option<&'a str>,
}

fn validate_serve_config(config: &ServeConfig) -> Result<ModelSelection<'_>, String> {
    if config.db.trim().is_empty() {
        return Err("--db needs a non-empty path".to_string());
    }
    if config.region.trim().is_empty() {
        return Err("--region needs a non-empty name".to_string());
    }
    let embedder = config.embedder.trim();
    if embedder.is_empty() {
        return Err("--embedder needs a non-empty name".to_string());
    }
    for (flag, value) in [
        ("--model-dir", config.model_dir.as_deref()),
        ("--models-dir", config.models_dir.as_deref()),
        ("--reranker-dir", config.reranker_dir.as_deref()),
    ] {
        if value.is_some_and(|value| value.trim().is_empty()) {
            return Err(format!("{flag} needs a non-empty path"));
        }
    }
    let reranker = config.reranker.as_deref().map(str::trim);
    if reranker == Some("") {
        return Err("--reranker needs a non-empty name".to_string());
    }

    #[cfg(not(feature = "candle-embed"))]
    if config.model_dir.is_some() || reranker.is_some() || config.reranker_dir.is_some() {
        return Err(
            "--model-dir, --reranker, and --reranker-dir need a build with --features \
             candle-embed (semantic embeddings + reranking)"
                .to_string(),
        );
    }
    #[cfg(not(feature = "hub"))]
    if config.models_dir.is_some() {
        return Err(
            "--models-dir needs a build with --features hub (model download support)".to_string(),
        );
    }

    validate_embedder_name(embedder)?;
    #[cfg(feature = "candle-embed")]
    if let Some(name) = reranker {
        reranker_spec(name).ok_or_else(|| unknown_reranker(name))?;
    }
    if config.reranker_dir.is_some() && reranker.is_none() {
        return Err("--reranker-dir requires --reranker".to_string());
    }
    if config.model_dir.is_some() && embedder == "mock" {
        return Err("--model-dir has no effect with --embedder mock".to_string());
    }
    #[cfg(all(feature = "candle-embed", not(feature = "hub")))]
    if embedder != "mock" && config.model_dir.is_none() {
        return Err(format!(
            "embedder '{embedder}' requires --model-dir (this build has no `hub` download \
             support)"
        ));
    }
    #[cfg(all(feature = "candle-embed", not(feature = "hub")))]
    if let Some(name) = reranker.filter(|_| config.reranker_dir.is_none()) {
        return Err(format!(
            "reranker '{name}' requires --reranker-dir (this build has no `hub` download \
             support)"
        ));
    }
    #[cfg(feature = "hub")]
    if config.models_dir.is_some()
        && !((embedder != "mock" && config.model_dir.is_none())
            || (reranker.is_some() && config.reranker_dir.is_none()))
    {
        return Err("--models-dir has no model or reranker to resolve".to_string());
    }

    Ok(ModelSelection {
        embedder,
        #[cfg(feature = "candle-embed")]
        reranker,
    })
}

/// Open (or create) the database, attach the region, and run the MCP stdio
/// loop. The passphrase is read from `CITADEL_KEY`. Blocks until the client
/// closes stdin.
pub fn serve_with_config(config: &ServeConfig) -> Result<(), String> {
    let selection = validate_serve_config(config)?;
    let key = Zeroizing::new(
        std::env::var("CITADEL_KEY")
            .map_err(|_| "set CITADEL_KEY to the database passphrase".to_string())?,
    );
    if key.is_empty() {
        return Err("CITADEL_KEY must not be empty".to_string());
    }
    let embedder_name = selection.embedder;

    // Authenticate an existing vault before loading potentially large models. For a
    // new path the order is reversed, so a bad model can never leave a vault artifact.
    let existing_database = if Path::new(&config.db).exists() {
        Some(
            database_builder(config, key.as_bytes())
                .open()
                .map_err(|e| format!("open database {}: {e}", config.db))?,
        )
    } else {
        None
    };

    let embedder = if embedder_name == "mock" {
        None
    } else {
        Some(build_real_embedder(embedder_name, config)?)
    };
    #[cfg(feature = "candle-embed")]
    let reranker = selection
        .reranker
        .map(|name| build_reranker(name, config).map(|reranker| (name, reranker)))
        .transpose()?;
    let db = match existing_database {
        Some(database) => database,
        None => database_builder(config, key.as_bytes())
            .create()
            .map_err(|e| format!("open database {}: {e}", config.db))?,
    };
    drop(key);

    let mem = MemoryEngine::open(Arc::new(db)).map_err(|e| format!("open memory engine: {e}"))?;
    let embedder = match embedder {
        Some(embedder) => embedder,
        None => build_mock_embedder(&mem, &config.region)?,
    };

    if config.encrypted {
        mem.create_encrypted_region(&config.region, embedder)
            .map_err(|e| format!("attach encrypted region '{}': {e}", config.region))?;
    } else {
        mem.create_region(&config.region, embedder)
            .map_err(|e| format!("attach region '{}': {e}", config.region))?;
    }

    eprintln!(
        "citadeldb-mcp: serving region '{}' ({}, embedder={}) from {} (MCP stdio)",
        config.region,
        if config.encrypted {
            "encrypted"
        } else {
            "plaintext"
        },
        embedder_name,
        config.db
    );
    #[cfg(feature = "candle-embed")]
    if let Some((name, reranker)) = reranker {
        use citadel_mem::RerankStrategy;
        mem.set_reranker(reranker, RerankStrategy::default());
        eprintln!("citadeldb-mcp: reranker={name} (rrf)");
    }
    crate::serve_stdio(Arc::new(mem), &config.region).map_err(|e| format!("serve: {e}"))
}

fn database_builder(config: &ServeConfig, key: &[u8]) -> DatabaseBuilder {
    let mut builder = DatabaseBuilder::new(&config.db)
        .passphrase(key)
        .argon2_profile(Argon2Profile::Iot);
    if config.encrypted {
        builder = builder.enable_region_keys(true);
    }
    builder
}

/// Dispatch the `pull` subcommand, otherwise serve a region over stdio.
pub fn run(argv: &[String]) -> Result<(), String> {
    match argv.first().map(String::as_str) {
        Some("pull") => run_pull(&argv[1..]),
        _ => serve_with_config(&parse_serve_config(argv)?),
    }
}

/// Parse serve argv into a [`ServeConfig`].
fn parse_serve_config(argv: &[String]) -> Result<ServeConfig, String> {
    let mut db = None;
    let mut region = String::from("default");
    let mut encrypted = true;
    let mut embedder = None;
    #[cfg(feature = "candle-embed")]
    let mut model_dir = None;
    #[cfg(not(feature = "candle-embed"))]
    let model_dir = None;
    #[cfg(feature = "hub")]
    let mut models_dir = None;
    #[cfg(not(feature = "hub"))]
    let models_dir = None;
    #[cfg(feature = "candle-embed")]
    let mut reranker = None;
    #[cfg(not(feature = "candle-embed"))]
    let reranker = None;
    #[cfg(feature = "candle-embed")]
    let mut reranker_dir = None;
    #[cfg(not(feature = "candle-embed"))]
    let reranker_dir = None;
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--db" => db = Some(it.next().ok_or("--db needs a path")?.clone()),
            "--region" => region = it.next().ok_or("--region needs a name")?.clone(),
            "--region-mode" => {
                encrypted = match it.next().map(String::as_str) {
                    Some("encrypted") => true,
                    Some("plaintext") => false,
                    other => {
                        return Err(format!(
                            "--region-mode must be encrypted|plaintext, got {}",
                            other.unwrap_or("(nothing)")
                        ))
                    }
                };
            }
            "--embedder" => embedder = Some(it.next().ok_or("--embedder needs a name")?.clone()),
            #[cfg(feature = "candle-embed")]
            "--model-dir" => {
                model_dir = Some(it.next().ok_or("--model-dir needs a path")?.clone())
            }
            #[cfg(feature = "hub")]
            "--models-dir" => {
                models_dir = Some(it.next().ok_or("--models-dir needs a path")?.clone())
            }
            #[cfg(feature = "candle-embed")]
            "--reranker" => {
                reranker = Some(it.next().ok_or("--reranker needs a name")?.clone())
            }
            #[cfg(feature = "candle-embed")]
            "--reranker-dir" => {
                reranker_dir = Some(it.next().ok_or("--reranker-dir needs a path")?.clone())
            }
            #[cfg(not(feature = "candle-embed"))]
            "--model-dir" | "--reranker" | "--reranker-dir" => {
                return Err(format!(
                    "{arg} needs a build with --features candle-embed (semantic embeddings + reranking)"
                ))
            }
            #[cfg(not(feature = "hub"))]
            "--models-dir" => {
                return Err(format!(
                    "{arg} needs a build with --features hub (model download support)"
                ))
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let db = db.ok_or("--db <path> is required")?;
    let embedder = embedder.ok_or(
        "--embedder <name> is required; choose a semantic model or pass \
         --embedder mock explicitly for keyword-only recall",
    )?;
    let embedder = embedder.trim();
    if embedder.is_empty() {
        return Err("--embedder needs a non-empty name".to_string());
    }
    Ok(ServeConfig {
        db,
        region,
        encrypted,
        embedder: embedder.to_string(),
        model_dir,
        models_dir,
        reranker,
        reranker_dir,
    })
}

/// `pull <model> [--models-dir <dir>]`: explicitly download a public model.
#[cfg(feature = "hub")]
fn run_pull(argv: &[String]) -> Result<(), String> {
    let mut model = None;
    let mut models_dir = None;
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--models-dir" => {
                models_dir = Some(it.next().ok_or("--models-dir needs a path")?.clone())
            }
            name if !name.starts_with("--") && model.is_none() => model = Some(name.to_string()),
            other => return Err(format!("unknown pull argument: {other}")),
        }
    }
    let name = model.ok_or(
        "usage: pull \
         <e5-large|e5-large-v2|bge-small|bge-base|bge-large|minilm|ms-marco-minilm> [--models-dir <dir>]",
    )?;
    pull_model(&name, models_dir.as_deref())
}

#[cfg(not(feature = "hub"))]
fn run_pull(_argv: &[String]) -> Result<(), String> {
    Err("`pull` needs a build with --features hub (model download support)".to_string())
}

fn build_mock_embedder(mem: &MemoryEngine, region: &str) -> Result<Arc<dyn Embedder>, String> {
    let identity = mem
        .stored_region_identity(region)
        .map_err(|e| format!("inspect region '{region}': {e}"))?;
    let (dim, metric) = identity.map_or((DEFAULT_MOCK_DIM, EmbeddingMetric::Cosine), |identity| {
        (usize::from(identity.dim()), identity.metric())
    });
    eprintln!(
        "citadeldb-mcp: WARNING mock embedder (dim={dim}, metric={metric:?}) - keyword-only \
         recall, not semantic. \
         For semantic recall run `citadeldb-mcp pull e5-large`, then restart with \
         --embedder e5-large (or pass --model-dir to a local model)."
    );
    Ok(Arc::new(MockEmbedder::with_metric(dim, metric)))
}

fn validate_embedder_name(name: &str) -> Result<(), String> {
    if name == "mock" {
        return Ok(());
    }
    #[cfg(feature = "candle-embed")]
    {
        model_spec(name)
            .map(|_| ())
            .ok_or_else(|| unknown_embedder(name))
    }
    #[cfg(not(feature = "candle-embed"))]
    {
        Err(format!(
            "embedder '{name}' needs a build with --features candle-embed (or `hub` for \
             downloads); this binary has only the mock embedder"
        ))
    }
}

/// Error for an `--embedder`/`pull` name that is not in the catalog.
#[cfg(feature = "candle-embed")]
fn unknown_embedder(name: &str) -> String {
    format!(
        "unknown embedder '{name}' (mock|e5-large|e5-large-v2|bge-small|bge-base|bge-large|minilm)"
    )
}

/// Built-in model catalog: CLI name -> inference config + pinned Hub snapshot.
#[cfg(feature = "candle-embed")]
fn model_spec(
    name: &str,
) -> Option<(
    citadel_mem::CandleConfig,
    &'static crate::model_cache::SnapshotSpec,
)> {
    use citadel_mem::CandleConfig;
    let config = match name {
        "e5-large" => CandleConfig::e5_large(),
        "e5-large-v2" => CandleConfig::e5_large_v2(),
        "bge-small" => CandleConfig::bge_small(),
        "bge-base" => CandleConfig::bge_base(),
        "bge-large" => CandleConfig::bge_large(),
        "minilm" => CandleConfig::minilm_l6(),
        _ => return None,
    };
    Some((config, crate::model_cache::embedder_snapshot(name)?))
}

/// Built-in cross-encoder reranker catalog.
#[cfg(feature = "candle-embed")]
fn reranker_spec(name: &str) -> Option<&'static crate::model_cache::SnapshotSpec> {
    crate::model_cache::reranker_snapshot(name)
}

/// Error for a `--reranker`/`pull` name that is not a known reranker.
#[cfg(feature = "candle-embed")]
fn unknown_reranker(name: &str) -> String {
    format!("unknown reranker '{name}' (ms-marco-minilm)")
}

/// Load a real model from `--model-dir` or the `pull` cache; never downloads.
#[cfg(feature = "hub")]
fn build_real_embedder(name: &str, config: &ServeConfig) -> Result<Arc<dyn Embedder>, String> {
    use citadel_mem::CandleEmbedder;
    let (cfg, snapshot) = model_spec(name).ok_or_else(|| unknown_embedder(name))?;
    let embedder = if let Some(dir) = &config.model_dir {
        CandleEmbedder::from_dir(dir, cfg)
            .map_err(|e| format!("load embedder '{name}' from {dir}: {e}"))?
    } else {
        let root = resolve_models_dir(config.models_dir.as_deref())?;
        let crate::model_cache::ModelArtifacts {
            config,
            tokenizer,
            weights,
            dir,
        } = crate::model_cache::load_snapshot(&root, snapshot)?;
        CandleEmbedder::from_bytes(&config, &tokenizer, weights, cfg)
            .map_err(|e| format!("load embedder '{name}' from {}: {e}", dir.display()))?
    };
    Ok(Arc::new(embedder))
}

/// Without `hub` there is no download cache, so a real model must come from
/// `--model-dir`.
#[cfg(all(feature = "candle-embed", not(feature = "hub")))]
fn build_real_embedder(name: &str, config: &ServeConfig) -> Result<Arc<dyn Embedder>, String> {
    use citadel_mem::CandleEmbedder;
    let (cfg, _snapshot) = model_spec(name).ok_or_else(|| unknown_embedder(name))?;
    let dir = config.model_dir.as_deref().ok_or_else(|| {
        format!("embedder '{name}' requires --model-dir (this build has no `hub` download support)")
    })?;
    let embedder = CandleEmbedder::from_dir(dir, cfg)
        .map_err(|e| format!("load embedder '{name}' from {dir}: {e}"))?;
    Ok(Arc::new(embedder))
}

/// No Candle backend compiled in: only the mock embedder exists.
#[cfg(not(feature = "candle-embed"))]
fn build_real_embedder(name: &str, _config: &ServeConfig) -> Result<Arc<dyn Embedder>, String> {
    Err(format!(
        "embedder '{name}' needs a build with --features candle-embed (or `hub` for downloads); \
         this binary has only the mock embedder"
    ))
}

/// Load reranker `name` from `--reranker-dir` or the `pull` cache; never
/// downloads.
#[cfg(feature = "hub")]
fn build_reranker(
    name: &str,
    config: &ServeConfig,
) -> Result<Arc<dyn citadel_mem::Reranker>, String> {
    use citadel_mem::CrossEncoder;
    let snapshot = reranker_spec(name).ok_or_else(|| unknown_reranker(name))?;
    let reranker = if let Some(dir) = &config.reranker_dir {
        CrossEncoder::ms_marco_minilm_l6(dir)
            .map_err(|e| format!("load reranker '{name}' from {dir}: {e}"))?
    } else {
        let root = resolve_models_dir(config.models_dir.as_deref())?;
        let crate::model_cache::ModelArtifacts {
            config,
            tokenizer,
            weights,
            dir,
        } = crate::model_cache::load_snapshot(&root, snapshot)?;
        CrossEncoder::from_bytes(&config, &tokenizer, weights, "ms-marco-MiniLM-L-6-v2", 512)
            .map_err(|e| format!("load reranker '{name}' from {}: {e}", dir.display()))?
    };
    Ok(Arc::new(reranker))
}

/// Without `hub` there is no download cache, so a reranker must come from
/// `--reranker-dir`.
#[cfg(all(feature = "candle-embed", not(feature = "hub")))]
fn build_reranker(
    name: &str,
    config: &ServeConfig,
) -> Result<Arc<dyn citadel_mem::Reranker>, String> {
    use citadel_mem::CrossEncoder;
    reranker_spec(name).ok_or_else(|| unknown_reranker(name))?;
    let dir = config.reranker_dir.as_deref().ok_or_else(|| {
        format!(
            "reranker '{name}' requires --reranker-dir (this build has no `hub` download support)"
        )
    })?;
    let reranker = CrossEncoder::ms_marco_minilm_l6(dir)
        .map_err(|e| format!("load reranker '{name}' from {dir}: {e}"))?;
    Ok(Arc::new(reranker))
}

/// Directory holding `pull`ed models: `--models-dir`, else
/// `$CITADEL_MODELS_DIR`, else `<home>/.citadel/models`.
#[cfg(feature = "hub")]
fn resolve_models_dir(override_dir: Option<&str>) -> Result<std::path::PathBuf, String> {
    use std::path::PathBuf;

    if let Some(dir) = override_dir {
        if dir.trim().is_empty() {
            return Err("--models-dir needs a non-empty path".to_string());
        }
        return Ok(PathBuf::from(dir));
    }
    if let Some(dir) = std::env::var_os("CITADEL_MODELS_DIR") {
        if os_string_is_blank(&dir) {
            return Err("CITADEL_MODELS_DIR must not be empty".to_string());
        }
        return Ok(PathBuf::from(dir));
    }
    let home = ["USERPROFILE", "HOME"]
        .into_iter()
        .filter_map(std::env::var_os)
        .find(|value| !os_string_is_blank(value))
        .ok_or_else(|| {
            "cannot locate home directory - set CITADEL_MODELS_DIR or pass --models-dir".to_string()
        })?;
    Ok(PathBuf::from(home).join(".citadel").join("models"))
}

#[cfg(feature = "hub")]
fn os_string_is_blank(value: &std::ffi::OsStr) -> bool {
    value.is_empty() || value.to_str().is_some_and(|value| value.trim().is_empty())
}

/// Error for a `pull` name that is neither a known embedder nor reranker.
#[cfg(feature = "hub")]
fn unknown_pullable(name: &str) -> String {
    format!(
        "unknown model '{name}' (embedders: e5-large|e5-large-v2|bge-small|bge-base|bge-large|minilm; \
         rerankers: ms-marco-minilm)"
    )
}

/// Download a public model (embedder or reranker) into the cache; never
/// implicit.
#[cfg(feature = "hub")]
pub fn pull_model(name: &str, models_dir: Option<&str>) -> Result<(), String> {
    let snapshot = model_spec(name)
        .map(|(_, snapshot)| snapshot)
        .or_else(|| reranker_spec(name))
        .ok_or_else(|| unknown_pullable(name))?;
    let root = resolve_models_dir(models_dir)?;
    let dest = crate::model_cache::pull_snapshot(&root, snapshot)?;
    eprintln!("citadeldb-mcp: pulled '{name}' to {}", dest.display());
    let flag = if reranker_spec(name).is_some() {
        "--reranker"
    } else {
        "--embedder"
    };
    eprintln!("citadeldb-mcp: serve it with `{flag} {name}`");
    Ok(())
}

#[cfg(test)]
mod tests {
    fn mock_config() -> super::ServeConfig {
        super::ServeConfig {
            db: "m.cdl".into(),
            region: "default".into(),
            encrypted: true,
            embedder: "mock".into(),
            model_dir: None,
            models_dir: None,
            reranker: None,
            reranker_dir: None,
        }
    }

    #[test]
    fn direct_config_rejects_options_that_would_be_ignored() {
        let mut config = mock_config();
        config.model_dir = Some("   ".into());
        let error = super::validate_serve_config(&config).unwrap_err();
        assert!(error.contains("non-empty path"), "{error}");

        let mut config = mock_config();
        config.reranker_dir = Some("reranker".into());
        let error = super::validate_serve_config(&config).unwrap_err();
        #[cfg(feature = "candle-embed")]
        assert!(error.contains("requires --reranker"), "{error}");
        #[cfg(not(feature = "candle-embed"))]
        assert!(error.contains("candle-embed"), "{error}");

        let mut config = mock_config();
        config.model_dir = Some("model".into());
        let error = super::validate_serve_config(&config).unwrap_err();
        #[cfg(feature = "candle-embed")]
        assert!(error.contains("no effect"), "{error}");
        #[cfg(not(feature = "candle-embed"))]
        assert!(error.contains("candle-embed"), "{error}");

        #[cfg(feature = "hub")]
        {
            let mut config = mock_config();
            config.models_dir = Some("models".into());
            let error = super::validate_serve_config(&config).unwrap_err();
            assert!(error.contains("no model or reranker"), "{error}");
        }
    }

    #[cfg(not(feature = "hub"))]
    #[test]
    fn direct_config_rejects_a_models_directory_without_hub() {
        let mut config = mock_config();
        config.models_dir = Some("models".into());
        let error = super::validate_serve_config(&config).unwrap_err();
        assert!(error.contains("--features hub"), "{error}");
    }

    #[cfg(feature = "candle-embed")]
    #[test]
    fn direct_config_validates_catalog_names_before_serving() {
        let mut config = mock_config();
        config.embedder = "not-a-model".into();
        let error = super::validate_serve_config(&config).unwrap_err();
        assert!(error.contains("unknown embedder"), "{error}");

        let mut config = mock_config();
        config.reranker = Some("not-a-reranker".into());
        let error = super::validate_serve_config(&config).unwrap_err();
        assert!(error.contains("unknown reranker"), "{error}");
    }

    #[cfg(feature = "candle-embed")]
    #[test]
    fn model_spec_maps_known_names_and_rejects_unknown() {
        use super::model_spec;
        let expected = [
            ("e5-large", "e5-large", "intfloat/e5-large"),
            ("e5-large-v2", "e5-large-v2", "intfloat/e5-large-v2"),
            ("bge-small", "bge-small-en-v1.5", "BAAI/bge-small-en-v1.5"),
            ("bge-base", "bge-base-en-v1.5", "BAAI/bge-base-en-v1.5"),
            ("bge-large", "bge-large-en-v1.5", "BAAI/bge-large-en-v1.5"),
            (
                "minilm",
                "all-MiniLM-L6-v2",
                "sentence-transformers/all-MiniLM-L6-v2",
            ),
        ];
        for (name, model_id, repo) in expected {
            let (config, snapshot) = model_spec(name).unwrap();
            assert_eq!(
                config.model_id, model_id,
                "wrong inference config for {name}"
            );
            assert_eq!(snapshot.name, name, "wrong cache snapshot for {name}");
            assert_eq!(snapshot.repo, repo, "wrong repository for {name}");
        }
        assert!(model_spec("does-not-exist").is_none());
    }

    #[cfg(feature = "candle-embed")]
    #[test]
    fn reranker_spec_maps_known_name_and_rejects_unknown() {
        use super::reranker_spec;
        assert_eq!(
            reranker_spec("ms-marco-minilm").map(|snapshot| snapshot.repo),
            Some("cross-encoder/ms-marco-MiniLM-L-6-v2")
        );
        assert!(reranker_spec("does-not-exist").is_none());
    }

    #[cfg(feature = "hub")]
    #[test]
    fn resolve_models_dir_prefers_explicit_override() {
        use super::resolve_models_dir;
        let dir = resolve_models_dir(Some("/tmp/custom-models")).unwrap();
        assert_eq!(dir, std::path::PathBuf::from("/tmp/custom-models"));

        let error = resolve_models_dir(Some("   ")).unwrap_err();
        assert!(error.contains("non-empty path"), "{error}");
    }

    #[test]
    fn parse_serve_config_requires_an_embedder_and_preserves_other_defaults() {
        use super::parse_serve_config;
        let err = parse_serve_config(&["--db".into(), "m.cdl".into()]).unwrap_err();
        assert!(err.contains("--embedder <name> is required"), "{err}");

        let a = parse_serve_config(&[
            "--db".into(),
            "m.cdl".into(),
            "--embedder".into(),
            "mock".into(),
        ])
        .unwrap();
        assert_eq!(a.db, "m.cdl");
        assert_eq!(a.region, "default");
        assert!(a.encrypted, "encrypted is the default");
        assert_eq!(a.embedder, "mock");

        let err = parse_serve_config(&[
            "--db".into(),
            "m.cdl".into(),
            "--embedder".into(),
            "   ".into(),
        ])
        .unwrap_err();
        assert!(err.contains("non-empty"), "{err}");

        let a = parse_serve_config(&[
            "--db".into(),
            "m.cdl".into(),
            "--region".into(),
            "notes".into(),
            "--region-mode".into(),
            "plaintext".into(),
            "--embedder".into(),
            "bge-small".into(),
        ])
        .unwrap();
        assert_eq!(a.region, "notes");
        assert!(!a.encrypted);
        assert_eq!(a.embedder, "bge-small");

        assert!(parse_serve_config(&[]).is_err(), "--db is required");
        assert!(
            parse_serve_config(&["--bogus".into()]).is_err(),
            "unknown flag"
        );
    }

    #[cfg(feature = "candle-embed")]
    #[test]
    fn parse_serve_config_accepts_reranker_flags() {
        use super::parse_serve_config;
        let a = parse_serve_config(&[
            "--db".into(),
            "m.cdl".into(),
            "--embedder".into(),
            "mock".into(),
            "--reranker".into(),
            "ms-marco-minilm".into(),
            "--reranker-dir".into(),
            "/models/ce".into(),
        ])
        .unwrap();
        assert_eq!(a.reranker.as_deref(), Some("ms-marco-minilm"));
        assert_eq!(a.reranker_dir.as_deref(), Some("/models/ce"));
    }

    #[cfg(not(feature = "candle-embed"))]
    #[test]
    fn parse_serve_config_rejects_candle_flags_with_feature_hint() {
        use super::parse_serve_config;
        let err = parse_serve_config(&[
            "--db".into(),
            "m.cdl".into(),
            "--embedder".into(),
            "mock".into(),
            "--model-dir".into(),
            "/m".into(),
        ])
        .unwrap_err();
        assert!(
            err.contains("candle-embed"),
            "feature-aware message, got: {err}"
        );
    }
}
