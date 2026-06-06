//! `citadel-mcp`: a stdio MCP server exposing a citadel memory region as tools.
//!
//! An MCP client (e.g. Claude Desktop) spawns this binary and talks JSON-RPC
//! over stdin/stdout. The database passphrase is read from `CITADEL_KEY`.
//! Everything except MCP protocol messages is written to stderr.
//!
//! The region is encrypted by default (per-atom sealed + cryptographic erasure);
//! pass `--region-mode plaintext` to opt out.
//!
//! Recall is keyword-only (the `mock` embedder) until you opt into a real semantic
//! model. The real (Candle) embedder is compiled into the default build, but models
//! are fetched only on explicit request - never automatically:
//!
//! ```text
//! citadel-mcp pull bge-small                 # one-time download to ~/.citadel/models
//! citadel-mcp --db memory.cdl --embedder bge-small
//! ```
//!
//! Or bring your own local model with `--model-dir <dir>` (fully offline); a
//! `cuda-embed` build runs the model on an NVIDIA GPU. Optionally improve recall ordering
//! with a cross-encoder reranker: `pull ms-marco-minilm` then `--reranker ms-marco-minilm`.
//!
//! Claude Desktop config (claude_desktop_config.json):
//! ```json
//! { "mcpServers": { "citadel": {
//!     "command": "citadel-mcp",
//!     "args": ["--db", "/path/memory.cdl", "--region", "default", "--embedder", "bge-small"],
//!     "env": { "CITADEL_KEY": "<passphrase>" } } } }
//! ```

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mcp::serve_stdio;
use citadel_mem::{Embedder, MemoryEngine, MockEmbedder};

/// Embedding dim for the MockEmbedder; real embedders take their dim from the model.
const EMBED_DIM: usize = 256;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("citadel-mcp: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Dispatch the `pull` subcommand, otherwise serve a region over stdio.
fn run() -> Result<(), String> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match argv.first().map(String::as_str) {
        Some("pull") => run_pull(&argv[1..]),
        _ => serve(Args::parse(&argv)?),
    }
}

/// Open (or create) the database, attach the region, and run the MCP stdio loop.
fn serve(args: Args) -> Result<(), String> {
    let key = std::env::var("CITADEL_KEY")
        .map_err(|_| "set CITADEL_KEY to the database passphrase".to_string())?;

    let mut builder = DatabaseBuilder::new(&args.db)
        .passphrase(key.as_bytes())
        .argon2_profile(Argon2Profile::Iot);
    // Encrypted regions seal each atom under its own key; that needs region wrap keys.
    if args.encrypted {
        builder = builder.enable_region_keys(true);
    }
    let db = if Path::new(&args.db).exists() {
        builder.open()
    } else {
        builder.create()
    }
    .map_err(|e| format!("open database {}: {e}", args.db))?;

    let mem = MemoryEngine::open(Arc::new(db)).map_err(|e| format!("open memory engine: {e}"))?;

    let embedder = build_embedder(&args)?;
    if args.encrypted {
        mem.create_encrypted_region(&args.region, embedder)
            .map_err(|e| format!("attach encrypted region '{}': {e}", args.region))?;
    } else {
        mem.create_region(&args.region, embedder)
            .map_err(|e| format!("attach region '{}': {e}", args.region))?;
    }

    eprintln!(
        "citadel-mcp: serving region '{}' ({}, embedder={}) from {} (MCP stdio)",
        args.region,
        if args.encrypted {
            "encrypted"
        } else {
            "plaintext"
        },
        args.embedder,
        args.db
    );
    // Attach the optional cross-encoder reranker (candle builds; from the pull cache or
    // --reranker-dir). The mutation lives only on this path, so `mem` stays immutable in a
    // mock-only build.
    #[cfg(feature = "candle-embed")]
    let mem = {
        let mut mem = mem;
        if let Some(name) = &args.reranker {
            use citadel_mem::RerankStrategy;
            let reranker = build_reranker(name, &args)?;
            mem.set_reranker(reranker, RerankStrategy::default());
            eprintln!("citadel-mcp: reranker={name} (rrf)");
        }
        mem
    };
    serve_stdio(Arc::new(mem), &args.region).map_err(|e| format!("serve: {e}"))
}

/// Build the `--embedder`. `mock` is always available (with a loud banner); a real model
/// loads from `--model-dir` or the `pull` cache - never downloaded here, never a silent fallback.
fn build_embedder(args: &Args) -> Result<Arc<dyn Embedder>, String> {
    match args.embedder.as_str() {
        "mock" => {
            eprintln!(
                "citadel-mcp: WARNING mock embedder - keyword-only recall, not semantic. \
                 For semantic recall run `citadel-mcp pull bge-small`, then restart with \
                 --embedder bge-small (or pass --model-dir to a local model)."
            );
            Ok(Arc::new(MockEmbedder::new(EMBED_DIM)))
        }
        name => build_real_embedder(name, args),
    }
}

/// Error for an `--embedder`/`pull` name that is not in the catalog.
#[cfg(feature = "candle-embed")]
fn unknown_embedder(name: &str) -> String {
    format!("unknown embedder '{name}' (mock|bge-small|bge-base|bge-large|minilm|e5-large)")
}

/// Built-in model catalog: CLI name -> (preset config, HuggingFace repo id). The single
/// source of truth for which `--embedder`/`pull` names are accepted.
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

/// Load a real model from `--model-dir` or the local `pull` cache. Never downloads (use
/// `citadel-mcp pull`) and never falls back to the mock.
#[cfg(feature = "hub")]
fn build_real_embedder(name: &str, args: &Args) -> Result<Arc<dyn Embedder>, String> {
    use citadel_mem::CandleEmbedder;
    let (cfg, _repo) = model_spec(name).ok_or_else(|| unknown_embedder(name))?;
    let dir = match &args.model_dir {
        Some(dir) => std::path::PathBuf::from(dir),
        None => {
            let cached = resolve_models_dir(args.models_dir.as_deref())?.join(name);
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
fn build_real_embedder(name: &str, args: &Args) -> Result<Arc<dyn Embedder>, String> {
    use citadel_mem::CandleEmbedder;
    let (cfg, _repo) = model_spec(name).ok_or_else(|| unknown_embedder(name))?;
    let dir = args.model_dir.as_deref().ok_or_else(|| {
        format!("embedder '{name}' requires --model-dir (this build has no `hub` download support)")
    })?;
    let embedder = CandleEmbedder::from_dir(dir, cfg)
        .map_err(|e| format!("load embedder '{name}' from {dir}: {e}"))?;
    Ok(Arc::new(embedder))
}

/// No Candle backend compiled in: only the mock embedder exists.
#[cfg(not(feature = "candle-embed"))]
fn build_real_embedder(name: &str, _args: &Args) -> Result<Arc<dyn Embedder>, String> {
    Err(format!(
        "embedder '{name}' needs a build with --features candle-embed (or `hub` for downloads); \
         this binary has only the mock embedder"
    ))
}

/// Load the cross-encoder reranker `name` from `--reranker-dir` or the local `pull` cache.
/// Never downloads (use `citadel-mcp pull`). Only called from the candle-gated serve hook.
#[cfg(feature = "hub")]
fn build_reranker(name: &str, args: &Args) -> Result<Arc<dyn citadel_mem::Reranker>, String> {
    use citadel_mem::CrossEncoder;
    reranker_spec(name).ok_or_else(|| unknown_reranker(name))?;
    let dir = match &args.reranker_dir {
        Some(dir) => std::path::PathBuf::from(dir),
        None => {
            let cached = resolve_models_dir(args.models_dir.as_deref())?.join(name);
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
fn build_reranker(name: &str, args: &Args) -> Result<Arc<dyn citadel_mem::Reranker>, String> {
    use citadel_mem::CrossEncoder;
    reranker_spec(name).ok_or_else(|| unknown_reranker(name))?;
    let dir = args.reranker_dir.as_deref().ok_or_else(|| {
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

/// `citadel-mcp pull <model> [--models-dir <dir>]`: explicitly download a public model.
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
        "usage: citadel-mcp pull \
         <bge-small|bge-base|bge-large|minilm|e5-large|ms-marco-minilm> [--models-dir <dir>]",
    )?;
    let repo = model_spec(&name)
        .map(|(_, r)| r)
        .or_else(|| reranker_spec(&name))
        .ok_or_else(|| unknown_pullable(&name))?;
    let dest = resolve_models_dir(models_dir.as_deref())?.join(&name);
    download_model(repo, &dest)?;
    eprintln!("citadel-mcp: pulled '{name}' to {}", dest.display());
    let flag = if reranker_spec(&name).is_some() {
        "--reranker"
    } else {
        "--embedder"
    };
    eprintln!("citadel-mcp: serve it with `{flag} {name}`");
    Ok(())
}

#[cfg(not(feature = "hub"))]
fn run_pull(_argv: &[String]) -> Result<(), String> {
    Err("`pull` needs a build with --features hub (model download support)".to_string())
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

/// Stream one URL to `target`, writing to a `.partial` sibling and renaming on success so
/// an interrupted download never leaves a half-written model file in place.
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

struct Args {
    db: String,
    region: String,
    encrypted: bool,
    embedder: String,
    #[cfg(feature = "candle-embed")]
    model_dir: Option<String>,
    #[cfg(feature = "hub")]
    models_dir: Option<String>,
    #[cfg(feature = "candle-embed")]
    reranker: Option<String>,
    #[cfg(feature = "candle-embed")]
    reranker_dir: Option<String>,
}

impl Args {
    fn parse(argv: &[String]) -> Result<Self, String> {
        let mut db = None;
        let mut region = String::from("default");
        let mut encrypted = true;
        let mut embedder = String::from("mock");
        #[cfg(feature = "candle-embed")]
        let mut model_dir = None;
        #[cfg(feature = "hub")]
        let mut models_dir = None;
        #[cfg(feature = "candle-embed")]
        let mut reranker = None;
        #[cfg(feature = "candle-embed")]
        let mut reranker_dir = None;
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
                "--embedder" => embedder = it.next().ok_or("--embedder needs a name")?.clone(),
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
                other => return Err(format!("unknown argument: {other}")),
            }
        }
        Ok(Self {
            db: db.ok_or("--db <path> is required")?,
            region,
            encrypted,
            embedder,
            #[cfg(feature = "candle-embed")]
            model_dir,
            #[cfg(feature = "hub")]
            models_dir,
            #[cfg(feature = "candle-embed")]
            reranker,
            #[cfg(feature = "candle-embed")]
            reranker_dir,
        })
    }
}

#[cfg(test)]
#[path = "mcp_tests.rs"]
mod tests;
