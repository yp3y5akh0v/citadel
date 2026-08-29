//! End-to-end integration test: spawn the real `citadeldb-mcp` binary and drive a
//! JSON-RPC session over stdio. Exercises the live `serve_stdio` IO loop, the CLI,
//! and the encrypted-by-default region with the mock embedder (so it runs in CI).

use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;

/// Spawn `citadeldb-mcp` on a throwaway encrypted DB, feed `requests` (newline-delimited
/// JSON-RPC), and return the parsed response lines. Dropping stdin sends EOF, which
/// ends the server's loop cleanly.
fn run_session(requests: &str) -> Vec<Value> {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("mcp.cdl");
    let mut child = Command::new(env!("CARGO_BIN_EXE_citadeldb-mcp"))
        // Select the mock embedder explicitly so the test never hits the network.
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
        .expect("spawn citadeldb-mcp");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(requests.as_bytes())
        .unwrap();
    let out = child.wait_with_output().expect("wait for citadeldb-mcp");
    assert!(
        out.status.success(),
        "citadeldb-mcp exited with {:?}",
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
fn serving_without_an_embedder_is_refused_before_the_database_is_opened() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("missing-embedder.cdl");
    let out = Command::new(env!("CARGO_BIN_EXE_citadeldb-mcp"))
        .args(["--db", db.to_str().unwrap()])
        .env("CITADEL_KEY", "integration-test")
        .output()
        .expect("run citadeldb-mcp");

    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("--embedder <name> is required"), "{stderr}");
    assert!(
        !db.exists(),
        "invalid configuration must not create a database"
    );
}

#[test]
fn serving_with_an_empty_passphrase_is_refused_before_the_database_is_opened() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("empty-passphrase.cdl");
    let out = Command::new(env!("CARGO_BIN_EXE_citadeldb-mcp"))
        .args(["--db", db.to_str().unwrap(), "--embedder", "mock"])
        .env("CITADEL_KEY", "")
        .output()
        .expect("run citadeldb-mcp");

    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("must not be empty"), "{stderr}");
    assert!(!db.exists(), "an empty key must not create a database");
}

#[test]
fn an_invalid_embedder_is_refused_before_the_database_is_opened() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("invalid-embedder.cdl");
    let out = Command::new(env!("CARGO_BIN_EXE_citadeldb-mcp"))
        .args([
            "--db",
            db.to_str().unwrap(),
            "--embedder",
            "not-a-real-embedder",
        ])
        .env("CITADEL_KEY", "integration-test")
        .output()
        .expect("run citadeldb-mcp");

    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("not-a-real-embedder"), "{stderr}");
    assert!(
        !db.exists(),
        "an invalid embedder must not leave a database behind"
    );
}

#[cfg(feature = "candle-embed")]
#[test]
fn a_missing_local_model_does_not_leave_a_new_vault_behind() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("missing-model.cdl");
    let model = dir.path().join("model-does-not-exist");
    let out = Command::new(env!("CARGO_BIN_EXE_citadeldb-mcp"))
        .args([
            "--db",
            db.to_str().unwrap(),
            "--embedder",
            "e5-large",
            "--model-dir",
            model.to_str().unwrap(),
        ])
        .env("CITADEL_KEY", "integration-test")
        .output()
        .expect("run citadeldb-mcp");

    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("load embedder"), "{stderr}");
    assert!(!db.exists(), "a model failure must not create a database");
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "a model failure must not leave key or database sidecars"
    );
}

#[cfg(feature = "candle-embed")]
#[test]
fn a_missing_local_reranker_does_not_leave_a_new_vault_behind() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("missing-reranker.cdl");
    let reranker = dir.path().join("reranker-does-not-exist");
    let out = Command::new(env!("CARGO_BIN_EXE_citadeldb-mcp"))
        .args([
            "--db",
            db.to_str().unwrap(),
            "--embedder",
            "mock",
            "--reranker",
            "ms-marco-minilm",
            "--reranker-dir",
            reranker.to_str().unwrap(),
        ])
        .env("CITADEL_KEY", "integration-test")
        .output()
        .expect("run citadeldb-mcp");

    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("load reranker"), "{stderr}");
    assert!(
        !db.exists(),
        "a reranker failure must not create a database"
    );
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "a reranker failure must not leave key or database sidecars"
    );
}

#[test]
fn mock_reuses_an_existing_regions_persisted_vector_shape() {
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("mock-dimension.cdl");
    let database = citadel::DatabaseBuilder::new(&db)
        .passphrase(b"integration-test")
        .argon2_profile(citadel::Argon2Profile::Iot)
        .create()
        .unwrap();
    let memory = citadel_mem::MemoryEngine::open(Arc::new(database)).unwrap();
    memory
        .create_region(
            "custom",
            Arc::new(citadel_mem::MockEmbedder::with_metric(
                64,
                citadel_mem::EmbeddingMetric::L2,
            )),
        )
        .unwrap();
    drop(memory);

    let out = Command::new(env!("CARGO_BIN_EXE_citadeldb-mcp"))
        .args([
            "--db",
            db.to_str().unwrap(),
            "--region",
            "custom",
            "--region-mode",
            "plaintext",
            "--embedder",
            "mock",
        ])
        .env("CITADEL_KEY", "integration-test")
        .output()
        .expect("run citadeldb-mcp");

    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(
        stderr.contains("mock embedder (dim=64, metric=L2)"),
        "{stderr}"
    );
}

#[test]
fn registry_manifest_requires_the_cli_embedder_argument() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../server.json");
    let Ok(source) = std::fs::read_to_string(path) else {
        return; // The workspace manifest is intentionally outside the published crate.
    };
    let manifest: Value = serde_json::from_str(&source).unwrap();
    let arguments = manifest["packages"][0]["packageArguments"]
        .as_array()
        .unwrap();
    let embedder = arguments
        .iter()
        .find(|argument| argument["name"] == "--embedder")
        .expect("server registry manifest declares --embedder");
    assert_eq!(embedder["isRequired"], true);
}

#[cfg(feature = "candle-embed")]
#[test]
fn an_existing_vault_is_authenticated_before_models_are_loaded() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("existing.cdl");
    let opened = citadel::DatabaseBuilder::new(&db)
        .passphrase(b"right-passphrase")
        .argon2_profile(citadel::Argon2Profile::Iot)
        .create()
        .unwrap();
    drop(opened);

    let missing_model = dir.path().join("model-does-not-exist");
    let out = Command::new(env!("CARGO_BIN_EXE_citadeldb-mcp"))
        .args([
            "--db",
            db.to_str().unwrap(),
            "--embedder",
            "e5-large",
            "--model-dir",
            missing_model.to_str().unwrap(),
        ])
        .env("CITADEL_KEY", "wrong-passphrase")
        .output()
        .expect("run citadeldb-mcp");

    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("open database"), "{stderr}");
    assert!(!stderr.contains("load embedder"), "{stderr}");
}

#[cfg(feature = "candle-embed")]
#[test]
fn an_unknown_model_is_rejected_before_an_existing_vault_is_opened() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("existing-invalid-model.cdl");
    let opened = citadel::DatabaseBuilder::new(&db)
        .passphrase(b"right-passphrase")
        .argon2_profile(citadel::Argon2Profile::Iot)
        .create()
        .unwrap();
    drop(opened);

    let out = Command::new(env!("CARGO_BIN_EXE_citadeldb-mcp"))
        .args([
            "--db",
            db.to_str().unwrap(),
            "--embedder",
            "not-a-real-embedder",
        ])
        .env("CITADEL_KEY", "wrong-passphrase")
        .output()
        .expect("run citadeldb-mcp");

    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("unknown embedder"), "{stderr}");
    assert!(!stderr.contains("open database"), "{stderr}");
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
