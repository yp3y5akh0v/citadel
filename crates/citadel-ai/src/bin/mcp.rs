//! `citadel-mcp`: a stdio MCP server exposing a citadel memory region as tools.
//!
//! An MCP client (e.g. Claude Desktop) spawns this binary and talks JSON-RPC
//! over stdin/stdout. The database passphrase is read from `CITADEL_KEY`.
//! Everything except MCP protocol messages is written to stderr.
//!
//! Claude Desktop config (claude_desktop_config.json):
//! ```json
//! { "mcpServers": { "citadel": {
//!     "command": "citadel-mcp",
//!     "args": ["--db", "/path/memory.cdl", "--region", "default"],
//!     "env": { "CITADEL_KEY": "<passphrase>" } } } }
//! ```

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_ai::mcp;
use citadel_mem::{MemoryEngine, MockEmbedder};

/// Embedding dim for the default MockEmbedder; a different dim needs a build with
/// a real embedder feature.
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

fn run() -> Result<(), String> {
    let args = Args::parse()?;
    let key = std::env::var("CITADEL_KEY")
        .map_err(|_| "set CITADEL_KEY to the database passphrase".to_string())?;

    let builder = DatabaseBuilder::new(&args.db)
        .passphrase(key.as_bytes())
        .argon2_profile(Argon2Profile::Iot);
    let db = if Path::new(&args.db).exists() {
        builder.open()
    } else {
        builder.create()
    }
    .map_err(|e| format!("open database {}: {e}", args.db))?;

    let mem = MemoryEngine::open(Arc::new(db)).map_err(|e| format!("open memory engine: {e}"))?;
    mem.create_region(&args.region, Arc::new(MockEmbedder::new(EMBED_DIM)))
        .map_err(|e| format!("attach region '{}': {e}", args.region))?;

    eprintln!(
        "citadel-mcp: serving region '{}' from {} (MCP stdio)",
        args.region, args.db
    );
    mcp::serve_stdio(Arc::new(mem), &args.region).map_err(|e| format!("serve: {e}"))
}

struct Args {
    db: String,
    region: String,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut db = None;
        let mut region = String::from("default");
        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--db" => db = Some(it.next().ok_or("--db needs a path")?),
                "--region" => region = it.next().ok_or("--region needs a name")?,
                other => return Err(format!("unknown argument: {other}")),
            }
        }
        Ok(Self {
            db: db.ok_or("--db <path> is required")?,
            region,
        })
    }
}
