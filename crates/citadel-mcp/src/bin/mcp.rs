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

use std::process::ExitCode;

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match citadel_mcp::run(&argv) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("citadel-mcp: {e}");
            ExitCode::FAILURE
        }
    }
}
