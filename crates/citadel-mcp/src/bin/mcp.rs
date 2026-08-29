//! `citadeldb-mcp`: a stdio MCP server exposing a memory region as tools.
//!
//! An MCP client (e.g. Claude Desktop) spawns this binary and talks JSON-RPC
//! over stdin/stdout; the passphrase is `CITADEL_KEY`, non-protocol output
//! goes to stderr. The region is encrypted by default (per-atom sealed + crypto
//! erasure); pass `--region-mode plaintext` to opt out.
//!
//! Every server invocation selects an embedder explicitly. The Candle embedder ships
//! in the default build; models are fetched only on request, never
//! automatically. Use `--embedder mock` only when keyword-only recall is
//! intentional:
//!
//! ```text
//! citadeldb-mcp pull e5-large              # download to ~/.citadel/models
//! citadeldb-mcp pull ms-marco-minilm       # cross-encoder reranker
//! citadeldb-mcp --db memory.cdl --embedder e5-large --reranker ms-marco-minilm
//! ```
//!
//! `e5-large` + `ms-marco-minilm` is the highest-recall config. Or
//! `--model-dir <dir>` for a compatible local checkpoint; a `cuda-embed`
//! build uses an NVIDIA GPU.
//!
//! Claude Desktop config (claude_desktop_config.json):
//! ```json
//! { "mcpServers": { "citadel": {
//!     "command": "citadeldb-mcp",
//!     "args": ["--db", "/path/memory.cdl", "--region", "default",
//!              "--embedder", "e5-large", "--reranker", "ms-marco-minilm"],
//!     "env": { "CITADEL_KEY": "<passphrase>" } } } }
//! ```

use std::process::ExitCode;

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match citadel_mcp::run(&argv) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("citadeldb-mcp: {e}");
            ExitCode::FAILURE
        }
    }
}
