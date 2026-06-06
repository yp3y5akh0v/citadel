//! End-to-end integration test: spawn the real `citadel-mcp` binary and drive a
//! JSON-RPC session over stdio. Exercises the live `serve_stdio` IO loop, the CLI,
//! and the encrypted-by-default region with the mock embedder (so it runs in CI).

use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;

/// Spawn `citadel-mcp` on a throwaway encrypted DB, feed `requests` (newline-delimited
/// JSON-RPC), and return the parsed response lines. Dropping stdin sends EOF, which
/// ends the server's loop cleanly.
fn run_session(requests: &str) -> Vec<Value> {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("mcp.cdl");
    let mut child = Command::new(env!("CARGO_BIN_EXE_citadel-mcp"))
        // Pin the mock embedder explicitly: the test must never hit the network,
        // regardless of the binary's default embedder.
        .args([
            "--db",
            db.to_str().unwrap(),
            "--region",
            "demo",
            "--embedder",
            "mock",
        ])
        .env("CITADEL_KEY", "integration-test")
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
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("each stdout line is valid JSON-RPC"))
        .collect()
}

#[test]
fn stdio_round_trip_remember_recall_forget() {
    let requests = concat!(
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        "\n",
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
        "\n",
        r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"mem_remember","arguments":{"text":"the sky is blue today"}}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"mem_recall","arguments":{"query":"sky","k":5}}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"mem_evict","arguments":{"policy":"purge_region"}}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"mem_recall","arguments":{"query":"sky","k":5}}}"#,
        "\n",
    );
    let resps = run_session(requests);

    // 6 requests carry an id; the notification gets no reply.
    assert_eq!(resps.len(), 6);
    assert_eq!(resps[0]["result"]["serverInfo"]["name"], "citadel-mem");
    assert_eq!(resps[1]["result"]["tools"].as_array().unwrap().len(), 13);
    assert_eq!(resps[2]["result"]["isError"], false);
    assert!(resps[3]["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("the sky is blue today"));
    assert_eq!(resps[4]["result"]["isError"], false);
    // After purge_region the region is forgotten: recall returns no hits.
    assert!(resps[5]["result"]["structuredContent"]["hits"]
        .as_array()
        .unwrap()
        .is_empty());
}
