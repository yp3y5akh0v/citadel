//! Serve a citadel memory region over MCP: open db, attach region, run stdio loop.

use std::path::Path;
use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{Embedder, MemoryEngine, MockEmbedder};

/// MockEmbedder dim; real embedders take their dim from the model.
const EMBED_DIM: usize = 256;

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

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            db: String::new(),
            region: String::from("default"),
            encrypted: true,
            embedder: String::from("mock"),
            model_dir: None,
            models_dir: None,
            reranker: None,
            reranker_dir: None,
        }
    }
}

/// Open (or create) the database, attach the region, and run the MCP stdio loop.
/// The passphrase is read from `CITADEL_KEY`. Blocks until the client closes stdin.
pub fn serve_with_config(config: &ServeConfig) -> Result<(), String> {
    let key = std::env::var("CITADEL_KEY")
        .map_err(|_| "set CITADEL_KEY to the database passphrase".to_string())?;

    let mut builder = DatabaseBuilder::new(&config.db)
        .passphrase(key.as_bytes())
        .argon2_profile(Argon2Profile::Iot);
    // Encrypted regions seal each atom under its own key; that needs region wrap keys.
    if config.encrypted {
        builder = builder.enable_region_keys(true);
    }
    let db = if Path::new(&config.db).exists() {
        builder.open()
    } else {
        builder.create()
    }
    .map_err(|e| format!("open database {}: {e}", config.db))?;

    let mem = MemoryEngine::open(Arc::new(db)).map_err(|e| format!("open memory engine: {e}"))?;

    let embedder = build_embedder(config)?;
    if config.encrypted {
        mem.create_encrypted_region(&config.region, embedder)
            .map_err(|e| format!("attach encrypted region '{}': {e}", config.region))?;
    } else {
        mem.create_region(&config.region, embedder)
            .map_err(|e| format!("attach region '{}': {e}", config.region))?;
    }

    eprintln!(
        "citadel-mcp: serving region '{}' ({}, embedder={}) from {} (MCP stdio)",
        config.region,
        if config.encrypted {
            "encrypted"
        } else {
            "plaintext"
        },
        config.embedder,
        config.db
    );
    #[cfg(feature = "candle-embed")]
    if let Some(name) = &config.reranker {
        use citadel_mem::RerankStrategy;
        let reranker = build_reranker(name, config)?;
        mem.set_reranker(reranker, RerankStrategy::default());
        eprintln!("citadel-mcp: reranker={name} (rrf)");
    }
    crate::serve_stdio(Arc::new(mem), &config.region).map_err(|e| format!("serve: {e}"))
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
    let mut config = ServeConfig::default();
    let mut db = None;
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--db" => db = Some(it.next().ok_or("--db needs a path")?.clone()),
            "--region" => config.region = it.next().ok_or("--region needs a name")?.clone(),
            "--region-mode" => {
                config.encrypted = match it.next().map(String::as_str) {
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
            "--embedder" => config.embedder = it.next().ok_or("--embedder needs a name")?.clone(),
            #[cfg(feature = "candle-embed")]
            "--model-dir" => {
                config.model_dir = Some(it.next().ok_or("--model-dir needs a path")?.clone())
            }
            #[cfg(feature = "hub")]
            "--models-dir" => {
                config.models_dir = Some(it.next().ok_or("--models-dir needs a path")?.clone())
            }
            #[cfg(feature = "candle-embed")]
            "--reranker" => {
                config.reranker = Some(it.next().ok_or("--reranker needs a name")?.clone())
            }
            #[cfg(feature = "candle-embed")]
            "--reranker-dir" => {
                config.reranker_dir = Some(it.next().ok_or("--reranker-dir needs a path")?.clone())
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
    config.db = db.ok_or("--db <path> is required")?;
    Ok(config)
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
         <bge-small|bge-base|bge-large|minilm|e5-large|ms-marco-minilm> [--models-dir <dir>]",
    )?;
    pull_model(&name, models_dir.as_deref())
}

#[cfg(not(feature = "hub"))]
fn run_pull(_argv: &[String]) -> Result<(), String> {
    Err("`pull` needs a build with --features hub (model download support)".to_string())
}

/// Build the `--embedder`; never downloads and never silently falls back to mock.
fn build_embedder(config: &ServeConfig) -> Result<Arc<dyn Embedder>, String> {
    match config.embedder.as_str() {
        "mock" => {
            eprintln!(
                "citadel-mcp: WARNING mock embedder - keyword-only recall, not semantic. \
                 For semantic recall run `citadel-mcp pull bge-small`, then restart with \
                 --embedder bge-small (or pass --model-dir to a local model)."
            );
            Ok(Arc::new(MockEmbedder::new(EMBED_DIM)))
        }
        name => build_real_embedder(name, config),
    }
}

/// Error for an `--embedder`/`pull` name that is not in the catalog.
#[cfg(feature = "candle-embed")]
fn unknown_embedder(name: &str) -> String {
    format!("unknown embedder '{name}' (mock|bge-small|bge-base|bge-large|minilm|e5-large)")
}

/// Built-in model catalog: CLI name -> (preset config, HuggingFace repo id).
#[cfg(feature = "candle-embed")]
fn model_spec(name: &str) -> Option<(citadel_mem::CandleConfig, &'static str)> {
    use citadel_mem::CandleConfig;
    Some(match name {
        "bge-small" => (CandleConfig::bge_small(), "BAAI/bge-small-en-v1.5"),
        "bge-base" => (CandleConfig::bge_base(), "BAAI/bge-base-en-v1.5"),
        "bge-large" => (CandleConfig::bge_large(), "BAAI/bge-large-en-v1.5"),
        "minilm" => (
            CandleConfig::minilm_l6(),
            "sentence-transformers/all-MiniLM-L6-v2",
        ),
        "e5-large" => (CandleConfig::e5_large(), "intfloat/e5-large-v2"),
        _ => return None,
    })
}

/// Built-in cross-encoder reranker catalog: CLI name -> HuggingFace repo id.
#[cfg(feature = "candle-embed")]
fn reranker_spec(name: &str) -> Option<&'static str> {
    match name {
        "ms-marco-minilm" => Some("cross-encoder/ms-marco-MiniLM-L-6-v2"),
        _ => None,
    }
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
    let (cfg, _repo) = model_spec(name).ok_or_else(|| unknown_embedder(name))?;
    let dir = match &config.model_dir {
        Some(dir) => std::path::PathBuf::from(dir),
        None => {
            let cached = resolve_models_dir(config.models_dir.as_deref())?.join(name);
            if !cached.join("model.safetensors").exists() {
                return Err(format!(
                    "embedder '{name}' is not downloaded - run `citadel-mcp pull {name}` first, \
                     or pass --model-dir <dir> to a local model"
                ));
            }
            cached
        }
    };
    let embedder = CandleEmbedder::from_dir(&dir, cfg)
        .map_err(|e| format!("load embedder '{name}' from {}: {e}", dir.display()))?;
    Ok(Arc::new(embedder))
}

/// Without `hub` there is no download cache, so a real model must come from `--model-dir`.
#[cfg(all(feature = "candle-embed", not(feature = "hub")))]
fn build_real_embedder(name: &str, config: &ServeConfig) -> Result<Arc<dyn Embedder>, String> {
    use citadel_mem::CandleEmbedder;
    let (cfg, _repo) = model_spec(name).ok_or_else(|| unknown_embedder(name))?;
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

/// Load reranker `name` from `--reranker-dir` or the `pull` cache; never downloads.
#[cfg(feature = "hub")]
fn build_reranker(
    name: &str,
    config: &ServeConfig,
) -> Result<Arc<dyn citadel_mem::Reranker>, String> {
    use citadel_mem::CrossEncoder;
    reranker_spec(name).ok_or_else(|| unknown_reranker(name))?;
    let dir = match &config.reranker_dir {
        Some(dir) => std::path::PathBuf::from(dir),
        None => {
            let cached = resolve_models_dir(config.models_dir.as_deref())?.join(name);
            if !cached.join("model.safetensors").exists() {
                return Err(format!(
                    "reranker '{name}' is not downloaded - run `citadel-mcp pull {name}` first, \
                     or pass --reranker-dir <dir> to a local model"
                ));
            }
            cached
        }
    };
    let reranker = CrossEncoder::ms_marco_minilm_l6(&dir)
        .map_err(|e| format!("load reranker '{name}' from {}: {e}", dir.display()))?;
    Ok(Arc::new(reranker))
}

/// Without `hub` there is no download cache, so a reranker must come from `--reranker-dir`.
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

/// Directory holding `pull`ed models: `--models-dir`, else `$CITADEL_MODELS_DIR`, else
/// `<home>/.citadel/models`.
#[cfg(feature = "hub")]
fn resolve_models_dir(override_dir: Option<&str>) -> Result<std::path::PathBuf, String> {
    use std::path::PathBuf;
    if let Some(dir) = override_dir {
        return Ok(PathBuf::from(dir));
    }
    if let Ok(dir) = std::env::var("CITADEL_MODELS_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .map_err(|_| {
            "cannot locate home directory - set CITADEL_MODELS_DIR or pass --models-dir".to_string()
        })?;
    Ok(PathBuf::from(home).join(".citadel").join("models"))
}

/// Error for a `pull` name that is neither a known embedder nor reranker.
#[cfg(feature = "hub")]
fn unknown_pullable(name: &str) -> String {
    format!(
        "unknown model '{name}' (embedders: bge-small|bge-base|bge-large|minilm|e5-large; \
         rerankers: ms-marco-minilm)"
    )
}

/// Download a public model (embedder or reranker) into the cache; never implicit.
#[cfg(feature = "hub")]
pub fn pull_model(name: &str, models_dir: Option<&str>) -> Result<(), String> {
    let repo = model_spec(name)
        .map(|(_, r)| r)
        .or_else(|| reranker_spec(name))
        .ok_or_else(|| unknown_pullable(name))?;
    let dest = resolve_models_dir(models_dir)?.join(name);
    download_model(repo, &dest)?;
    eprintln!("citadel-mcp: pulled '{name}' to {}", dest.display());
    let flag = if reranker_spec(name).is_some() {
        "--reranker"
    } else {
        "--embedder"
    };
    eprintln!("citadel-mcp: serve it with `{flag} {name}`");
    Ok(())
}

/// The files that make up a Candle BERT model on the Hugging Face Hub.
#[cfg(feature = "hub")]
const MODEL_FILES: [&str; 3] = ["config.json", "tokenizer.json", "model.safetensors"];

/// Public Hub URL for one file of `repo` at the default revision.
#[cfg(feature = "hub")]
fn hub_url(repo: &str, file: &str) -> String {
    format!("https://huggingface.co/{repo}/resolve/main/{file}")
}

/// Download every model file of `repo` into `dest`, creating it if needed.
#[cfg(feature = "hub")]
fn download_model(repo: &str, dest: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dest).map_err(|e| format!("create {}: {e}", dest.display()))?;
    eprintln!("citadel-mcp: pulling '{repo}' from huggingface.co");
    for file in MODEL_FILES {
        download_file(&hub_url(repo, file), &dest.join(file))?;
    }
    Ok(())
}

/// Stream URL to a `.partial` sibling, rename on success; no half-written files.
#[cfg(feature = "hub")]
fn download_file(url: &str, target: &Path) -> Result<(), String> {
    use std::io::{Read, Write};

    let resp = ureq::get(url)
        .call()
        .map_err(|e| format!("GET {url}: {e}"))?;
    let total: u64 = resp
        .header("Content-Length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let mut partial = target.as_os_str().to_owned();
    partial.push(".partial");
    let partial = std::path::PathBuf::from(partial);
    let name = target.file_name().unwrap_or_default().to_string_lossy();

    let mut reader = resp.into_reader();
    let mut file = std::io::BufWriter::new(
        std::fs::File::create(&partial)
            .map_err(|e| format!("create {}: {e}", partial.display()))?,
    );
    let mut buf = [0u8; 64 * 1024];
    let mut done: u64 = 0;
    let mut last_pct = u64::MAX;
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| format!("read {url}: {e}"))?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])
            .map_err(|e| format!("write {}: {e}", partial.display()))?;
        done += n as u64;
        if let Some(pct) = (done * 100).checked_div(total) {
            if pct != last_pct {
                eprint!("\r  {name} {pct}%");
                let _ = std::io::stderr().flush();
                last_pct = pct;
            }
        }
    }
    file.flush()
        .map_err(|e| format!("flush {}: {e}", partial.display()))?;
    drop(file);
    if total > 0 {
        eprintln!();
    } else {
        eprintln!("  {name} ({done} bytes)");
    }
    std::fs::rename(&partial, target).map_err(|e| format!("finalize {}: {e}", target.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "candle-embed")]
    #[test]
    fn model_spec_maps_known_names_and_rejects_unknown() {
        use super::model_spec;
        let (cfg, repo) = model_spec("bge-small").expect("bge-small is known");
        assert_eq!(repo, "BAAI/bge-small-en-v1.5");
        assert_eq!(cfg.model_id, "bge-small-en-v1.5");
        assert_eq!(
            model_spec("minilm").unwrap().1,
            "sentence-transformers/all-MiniLM-L6-v2"
        );
        assert!(model_spec("does-not-exist").is_none());
    }

    #[cfg(feature = "candle-embed")]
    #[test]
    fn reranker_spec_maps_known_name_and_rejects_unknown() {
        use super::reranker_spec;
        assert_eq!(
            reranker_spec("ms-marco-minilm"),
            Some("cross-encoder/ms-marco-MiniLM-L-6-v2")
        );
        assert!(reranker_spec("does-not-exist").is_none());
    }

    #[cfg(feature = "hub")]
    #[test]
    fn hub_url_builds_resolve_url() {
        use super::hub_url;
        assert_eq!(
            hub_url("BAAI/bge-small-en-v1.5", "model.safetensors"),
            "https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main/model.safetensors"
        );
    }

    #[cfg(feature = "hub")]
    #[test]
    fn resolve_models_dir_prefers_explicit_override() {
        use super::resolve_models_dir;
        let dir = resolve_models_dir(Some("/tmp/custom-models")).unwrap();
        assert_eq!(dir, std::path::PathBuf::from("/tmp/custom-models"));
    }

    #[test]
    fn parse_serve_config_defaults_and_overrides() {
        use super::parse_serve_config;
        let a = parse_serve_config(&["--db".into(), "m.cdl".into()]).unwrap();
        assert_eq!(a.db, "m.cdl");
        assert_eq!(a.region, "default");
        assert!(a.encrypted, "encrypted is the default");
        assert_eq!(a.embedder, "mock", "mock is the default embedder");

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
