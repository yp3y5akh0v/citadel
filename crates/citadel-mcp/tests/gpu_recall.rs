//! GPU semantic-recall integration test (local-only; not run in CI).
//!
//! Requires a real embedder build and a local bge-large model:
//!   set CITADEL_AI_BGE_LARGE_DIR to a bge-large-en-v1.5 directory, then
//!   cargo test -p citadeldb-mcp --features cuda-embed -- --ignored
//! (omit cuda-embed for a CPU candle build via `--features candle-embed`).
//!
//! The query is a paraphrase with near-zero word overlap with its target atom, so
//! only a real semantic embedder ranks the right atom first - the mock keyword
//! embedder cannot.

use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;

#[test]
#[ignore = "needs a candle/cuda build + CITADEL_AI_BGE_LARGE_DIR -> bge-large model dir"]
fn bge_large_semantic_recall() {
    let model_dir = std::env::var("CITADEL_AI_BGE_LARGE_DIR")
        .expect("set CITADEL_AI_BGE_LARGE_DIR to a local bge-large-en-v1.5 directory");
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("gpu.cdl");

    let requests = concat!(
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"mem_remember","arguments":{"text":"Citadel seals every page with AES-256 and verifies integrity using HMAC."}}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"mem_remember","arguments":{"text":"Plants turn sunlight, water, and carbon dioxide into sugar and oxygen."}}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"mem_recall","arguments":{"query":"How is the data kept confidential and protected from tampering?","k":2}}}"#,
        "\n",
    );

    let mut child = Command::new(env!("CARGO_BIN_EXE_citadel-mcp"))
        .args([
            "--db",
            db.to_str().unwrap(),
            "--region",
            "demo",
            "--embedder",
            "bge-large",
            "--model-dir",
            &model_dir,
        ])
        .env("CITADEL_KEY", "gpu-test")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn citadel-mcp");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(requests.as_bytes())
        .unwrap();
    let out = child.wait_with_output().expect("wait for citadel-mcp");
    assert!(
        out.status.success(),
        "citadel-mcp exited with {:?}",
        out.status
    );

    let lines: Vec<Value> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("valid JSON-RPC line"))
        .collect();

    // The final response is the paraphrase recall; the top hit must be the
    // security atom, which only semantic (not keyword) recall can surface.
    let recall = lines.last().unwrap();
    let hits = &recall["result"]["structuredContent"]["hits"];
    assert!(
        hits[0]["text"].as_str().unwrap().contains("AES-256"),
        "semantic recall should rank the security atom first; got: {hits}"
    );
}
