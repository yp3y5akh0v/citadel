use super::{dispatch, handle_line};
use crate::protocol::{INVALID_REQUEST, METHOD_NOT_FOUND, PARSE_ERROR};
use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, EdgeKind, MemoryEngine, MockEmbedder};
use serde_json::{json, Value};
use std::sync::Arc;

/// A throwaway engine with one region `r` bound to a 64-dim mock embedder.
/// `encrypted` chooses a sealed (per-atom crypto-erasure) region vs a plaintext one.
fn make_engine(encrypted: bool) -> (tempfile::TempDir, Arc<MemoryEngine>) {
    let dir = tempfile::tempdir().unwrap();
    let mut builder = DatabaseBuilder::new(dir.path().join("m.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot);
    if encrypted {
        builder = builder.enable_region_keys(true);
    }
    let db = builder.create().unwrap();
    let eng = Arc::new(MemoryEngine::open(Arc::new(db)).unwrap());
    let embedder = Arc::new(MockEmbedder::new(64));
    if encrypted {
        eng.create_encrypted_region("r", embedder).unwrap();
    } else {
        eng.create_region("r", embedder).unwrap();
    }
    (dir, eng)
}

fn engine() -> (tempfile::TempDir, Arc<MemoryEngine>) {
    make_engine(false)
}

fn call(eng: &MemoryEngine, name: &str, args: Value) -> Value {
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                     "params": {"name": name, "arguments": args}});
    dispatch(eng, "r", &req).unwrap()
}

#[test]
fn initialize_reports_protocol_and_server_info() {
    let (_d, eng) = engine();
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                     "params": {"protocolVersion": "2025-11-25"}});
    let resp = dispatch(&eng, "r", &req).unwrap();
    assert_eq!(resp["result"]["protocolVersion"], "2025-11-25");
    assert!(resp["result"]["capabilities"]["tools"].is_object());
    assert!(resp["result"]["capabilities"]["resources"].is_object());
    assert_eq!(resp["result"]["serverInfo"]["name"], json!("citadel-mem"));
}

#[test]
fn initialize_negotiates_protocol_version() {
    let (_d, eng) = engine();
    // A supported older version is echoed back.
    let older = dispatch(
        &eng,
        "r",
        &json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-06-18"}}),
    )
    .unwrap();
    assert_eq!(older["result"]["protocolVersion"], "2025-06-18");
    // An unsupported version makes the server offer its latest.
    let unknown = dispatch(
        &eng,
        "r",
        &json!({"jsonrpc": "2.0", "id": 2, "method": "initialize",
                "params": {"protocolVersion": "1999-01-01"}}),
    )
    .unwrap();
    assert_eq!(unknown["result"]["protocolVersion"], "2025-11-25");
}

#[test]
fn notifications_get_no_reply() {
    let (_d, eng) = engine();
    let note = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
    assert!(dispatch(&eng, "r", &note).is_none());
    // An id-less message with an unknown method is still a notification.
    let unknown = json!({"jsonrpc": "2.0", "method": "something/else"});
    assert!(dispatch(&eng, "r", &unknown).is_none());
}

#[test]
fn ping_returns_empty_result() {
    let (_d, eng) = engine();
    let req = json!({"jsonrpc": "2.0", "id": 7, "method": "ping"});
    let resp = dispatch(&eng, "r", &req).unwrap();
    assert_eq!(resp["result"], json!({}));
    assert_eq!(resp["id"], json!(7));
}

#[test]
fn tools_list_has_all_tools_with_expected_schemas() {
    let (_d, eng) = engine();
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"});
    let resp = dispatch(&eng, "r", &req).unwrap();
    let tools = resp["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 13);
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    for n in [
        "mem_recall",
        "mem_fetch",
        "mem_edges",
        "mem_profile",
        "mem_summarize",
        "mem_verify",
        "mem_remember",
        "mem_remember_batch",
        "mem_update",
        "mem_link",
        "mem_evolve",
        "mem_evict",
        "mem_forget",
    ] {
        assert!(names.contains(&n), "missing tool {n}");
    }
    let recall = tools.iter().find(|t| t["name"] == "mem_recall").unwrap();
    assert_eq!(recall["inputSchema"]["required"], json!(["query"]));
    // Read-only + structured output on recall; destructive annotation on evict.
    assert_eq!(recall["annotations"]["readOnlyHint"], true);
    assert!(recall["outputSchema"]["properties"]["hits"].is_object());
    let evict = tools.iter().find(|t| t["name"] == "mem_evict").unwrap();
    assert_eq!(evict["annotations"]["destructiveHint"], true);
    // mem_forget is destructive and advertises its erasure-receipt output schema.
    let forget = tools.iter().find(|t| t["name"] == "mem_forget").unwrap();
    assert_eq!(forget["annotations"]["destructiveHint"], true);
    assert!(forget["outputSchema"]["properties"]["cryptographicErasure"].is_object());
    // mem_verify is read-only and advertises its attestations output schema.
    let verify = tools.iter().find(|t| t["name"] == "mem_verify").unwrap();
    assert_eq!(verify["annotations"]["readOnlyHint"], true);
    assert!(verify["outputSchema"]["properties"]["attestations"].is_object());
}

#[test]
fn remember_then_recall_round_trip() {
    let (_d, eng) = engine();
    let stored = call(
        &eng,
        "mem_remember",
        json!({"text": "the sky is blue today"}),
    );
    assert_eq!(stored["result"]["isError"], json!(false));
    assert!(stored["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("\"status\":\"stored\""));

    let recalled = call(&eng, "mem_recall", json!({"query": "sky", "k": 5}));
    assert_eq!(recalled["result"]["isError"], json!(false));
    assert!(recalled["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("the sky is blue today"));
}

#[test]
fn link_evolve_summarize_evict_happy_paths() {
    let (_d, eng) = engine();
    let a = eng.remember("r", AtomInput::new("fact", "alpha")).unwrap();
    let b = eng.remember("r", AtomInput::new("fact", "beta")).unwrap();

    let linked = call(
        &eng,
        "mem_link",
        json!({"src": a, "dst": b, "kind": "derived_from"}),
    );
    assert_eq!(linked["result"]["isError"], json!(false));

    let evolved = call(
        &eng,
        "mem_evolve",
        json!({"atom_id": a, "max_distance": 10.0}),
    );
    assert_eq!(evolved["result"]["isError"], json!(false));

    let summary = call(&eng, "mem_summarize", json!({}));
    assert_eq!(summary["result"]["isError"], json!(false));
    assert!(summary["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("\"total\":"));

    let evicted = call(
        &eng,
        "mem_evict",
        json!({"policy": "lru", "keep_fraction": 1.0}),
    );
    assert_eq!(evicted["result"]["isError"], json!(false));
}

/// `mem_link` rejects an edge to a non-existent atom (no dangling edges); real atoms link fine.
#[test]
fn mem_link_rejects_nonexistent_atoms() {
    let (_d, eng) = engine();
    let a = eng
        .remember("r", AtomInput::new("fact", "real one"))
        .unwrap();

    let bad = call(
        &eng,
        "mem_link",
        json!({"src": a, "dst": 99999, "kind": "derived_from"}),
    );
    assert_eq!(
        bad["result"]["isError"],
        json!(true),
        "link to a missing atom is rejected"
    );
    assert!(
        eng.fetch_edges(Some(a), None, None).unwrap().is_empty(),
        "no dangling edge was created"
    );

    let b = eng
        .remember("r", AtomInput::new("fact", "real two"))
        .unwrap();
    let ok = call(
        &eng,
        "mem_link",
        json!({"src": a, "dst": b, "kind": "derived_from"}),
    );
    assert_eq!(
        ok["result"]["isError"],
        json!(false),
        "real atoms link fine"
    );
}

/// Encrypted (sealed) region: store, recall through decrypt-then-rank, then
/// `purge_region` (cryptographic erasure) and confirm recall returns nothing.
#[test]
fn encrypted_region_round_trip_and_crypto_erasure() {
    let (_d, eng) = make_engine(true);
    let stored = call(&eng, "mem_remember", json!({"text": "alpha beta gamma"}));
    assert_eq!(stored["result"]["isError"], json!(false));

    let recalled = call(
        &eng,
        "mem_recall",
        json!({"query": "alpha beta gamma", "k": 5}),
    );
    assert_eq!(recalled["result"]["isError"], json!(false));
    assert!(recalled["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("alpha beta gamma"));

    let evicted = call(&eng, "mem_evict", json!({"policy": "purge_region"}));
    assert_eq!(evicted["result"]["isError"], json!(false));

    let after = call(
        &eng,
        "mem_recall",
        json!({"query": "alpha beta gamma", "k": 5}),
    );
    assert!(after["result"]["structuredContent"]["hits"]
        .as_array()
        .unwrap()
        .is_empty());
}

/// `mem_forget` on an encrypted region: the receipt proves cryptographic erasure (key
/// destruction confirmed through the read-back gate) and a sibling atom survives.
#[test]
fn mem_forget_encrypted_returns_verifiable_receipt() {
    let (_d, eng) = make_engine(true);
    let a = eng
        .remember("r", AtomInput::new("fact", "secret alpha"))
        .unwrap();
    let b = eng
        .remember("r", AtomInput::new("fact", "sibling beta"))
        .unwrap();

    let resp = call(&eng, "mem_forget", json!({"ids": [a]}));
    assert_eq!(resp["result"]["isError"], json!(false));
    let receipt = &resp["result"]["structuredContent"];
    assert_eq!(receipt["cryptographicErasure"], true);
    assert_eq!(receipt["erasedCount"], 1);
    assert_eq!(receipt["rowsDeleted"], 1);
    assert_eq!(receipt["readbackConfirmed"], true);
    assert_eq!(receipt["algorithm"], "AES-256-KW(RFC3394)");
    assert!(receipt["scopeCaveat"].as_str().unwrap().contains("NIST"));
    assert!(
        eng.fetch_one("r", b).unwrap().is_some(),
        "sibling survives a targeted forget"
    );
}

/// `mem_forget` on a plaintext region: honest receipt - rows deleted but NOT cryptographically
/// erased (the second supported path).
#[test]
fn mem_forget_plaintext_reports_logical_delete() {
    let (_d, eng) = make_engine(false);
    let a = eng.remember("r", AtomInput::new("fact", "alpha")).unwrap();

    let resp = call(&eng, "mem_forget", json!({"ids": [a]}));
    assert_eq!(resp["result"]["isError"], json!(false));
    let receipt = &resp["result"]["structuredContent"];
    assert_eq!(receipt["cryptographicErasure"], false);
    assert_eq!(receipt["erasedCount"], 0);
    assert_eq!(receipt["rowsDeleted"], 1);
    assert!(eng.fetch_one("r", a).unwrap().is_none());
}

/// `mem_forget` refuses immutable atoms unless `force` (model-safety).
#[test]
fn mem_forget_skips_immutable_unless_forced() {
    let (_d, eng) = make_engine(true);
    let t = eng
        .remember("r", AtomInput::new("fact", "protected").immutable())
        .unwrap();

    let resp = call(&eng, "mem_forget", json!({"ids": [t]}));
    assert_eq!(
        resp["result"]["structuredContent"]["immutableSkipped"],
        json!([t])
    );
    assert!(eng.fetch_one("r", t).unwrap().is_some());

    let forced = call(&eng, "mem_forget", json!({"ids": [t], "force": true}));
    assert_eq!(forced["result"]["structuredContent"]["erasedCount"], 1);
    assert!(eng.fetch_one("r", t).unwrap().is_none());
}

/// `mem_verify`: an intact encrypted atom attests authentic (aad-bound); an unknown id is
/// missing. (Tamper/key-erased/replay verdicts are exercised in the engine tests.)
#[test]
fn mem_verify_attests_authentic_and_missing() {
    let (_d, eng) = make_engine(true);
    let a = eng.remember("r", AtomInput::new("fact", "alpha")).unwrap();

    let resp = call(&eng, "mem_verify", json!({"ids": [a, 9999]}));
    assert_eq!(resp["result"]["isError"], json!(false));
    let att = resp["result"]["structuredContent"]["attestations"]
        .as_array()
        .unwrap();
    assert_eq!(att.len(), 2);
    assert_eq!(att[0]["verdict"], "authentic");
    assert_eq!(att[0]["aadBound"], true);
    assert_eq!(att[1]["verdict"], "missing");
}

/// `mem_verify` on a plaintext region honestly reports plaintext_unattested (no per-atom MAC).
#[test]
fn mem_verify_plaintext_is_unattested() {
    let (_d, eng) = make_engine(false);
    let a = eng.remember("r", AtomInput::new("fact", "alpha")).unwrap();
    let resp = call(&eng, "mem_verify", json!({"ids": [a]}));
    assert_eq!(
        resp["result"]["structuredContent"]["attestations"][0]["verdict"],
        "plaintext_unattested"
    );
}

/// `mem_recall` with `attest: true` attaches an integrity verdict to each hit
/// (verify-as-you-recall: the agent can confirm a recalled memory was not tampered).
#[test]
fn mem_recall_attest_attaches_verdict_to_hits() {
    let (_d, eng) = make_engine(true);
    eng.remember("r", AtomInput::new("fact", "the sky is blue"))
        .unwrap();
    let resp = call(
        &eng,
        "mem_recall",
        json!({"query": "sky", "k": 5, "attest": true}),
    );
    assert_eq!(resp["result"]["isError"], json!(false));
    let hits = resp["result"]["structuredContent"]["hits"]
        .as_array()
        .unwrap();
    assert!(!hits.is_empty(), "the stored atom is recalled");
    assert_eq!(hits[0]["attestation"]["verdict"], "authentic");
    assert_eq!(hits[0]["attestation"]["aadBound"], true);
}

/// `mem_recall` attaches a `resource_link` content block per hit, each a dereferenceable
/// `memory://atom/{id}` URI; the links resolve via `resources/read`. structuredContent is
/// unchanged (the links are additive content blocks).
#[test]
fn mem_recall_emits_resolvable_resource_links() {
    let (_d, eng) = engine();
    let a = eng
        .remember("r", AtomInput::new("fact", "alpha gamma"))
        .unwrap();

    let resp = call(&eng, "mem_recall", json!({"query": "alpha gamma", "k": 5}));
    let content = resp["result"]["content"].as_array().unwrap();
    let links: Vec<&Value> = content
        .iter()
        .filter(|c| c["type"] == "resource_link")
        .collect();
    assert!(!links.is_empty(), "recall hits carry resource links");
    assert_eq!(links[0]["uri"], format!("memory://atom/{a}"));
    assert_eq!(links[0]["mimeType"], "application/json");
    assert!(resp["result"]["structuredContent"]["hits"].is_array());

    // The emitted link resolves: resources/read returns the atom's content.
    let read = dispatch(
        &eng,
        "r",
        &json!({"jsonrpc": "2.0", "id": 2, "method": "resources/read",
                "params": {"uri": links[0]["uri"].clone()}}),
    )
    .unwrap();
    assert!(read["result"]["contents"][0]["text"]
        .as_str()
        .unwrap()
        .contains("alpha gamma"));
}

/// `mem_update` replaces an atom's payload in place (encrypted path) and refuses immutable atoms.
#[test]
fn mem_update_replaces_payload_and_rejects_immutable() {
    let (_d, eng) = make_engine(true);
    let a = eng
        .remember(
            "r",
            AtomInput::new("fact", "alpha").with_payload(json!({"v": 1})),
        )
        .unwrap();

    let resp = call(&eng, "mem_update", json!({"id": a, "payload": {"v": 2}}));
    assert_eq!(resp["result"]["isError"], json!(false));
    assert_eq!(
        eng.fetch_one("r", a).unwrap().unwrap().payload,
        json!({"v": 2})
    );

    let imm = eng
        .remember("r", AtomInput::new("fact", "locked").immutable())
        .unwrap();
    let resp = call(&eng, "mem_update", json!({"id": imm, "payload": {"x": 1}}));
    assert_eq!(
        resp["result"]["isError"],
        json!(true),
        "immutable atom rejects update"
    );
}

/// `mem_update` also works on a plaintext region (both supported paths).
#[test]
fn mem_update_works_on_plaintext_region() {
    let (_d, eng) = make_engine(false);
    let a = eng
        .remember(
            "r",
            AtomInput::new("fact", "alpha").with_payload(json!({"v": 1})),
        )
        .unwrap();
    let resp = call(&eng, "mem_update", json!({"id": a, "payload": {"v": 9}}));
    assert_eq!(resp["result"]["isError"], json!(false));
    assert_eq!(
        eng.fetch_one("r", a).unwrap().unwrap().payload,
        json!({"v": 9})
    );
}

#[test]
fn unknown_method_is_protocol_error() {
    let (_d, eng) = engine();
    let req = json!({"jsonrpc": "2.0", "id": 2, "method": "prompts/list"});
    let resp = dispatch(&eng, "r", &req).unwrap();
    assert_eq!(resp["error"]["code"], json!(METHOD_NOT_FOUND));
}

#[test]
fn unknown_tool_and_bad_args_are_tool_errors_not_protocol_errors() {
    let (_d, eng) = engine();
    let ghost = call(&eng, "ghost_tool", json!({}));
    assert_eq!(ghost["result"]["isError"], json!(true));
    assert!(
        ghost.get("error").is_none(),
        "tool issues are not JSON-RPC errors"
    );

    let missing = call(&eng, "mem_recall", json!({}));
    assert_eq!(missing["result"]["isError"], json!(true));
    assert!(missing["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("query"));

    let bad_kind = call(
        &eng,
        "mem_link",
        json!({"src": 1, "dst": 2, "kind": "frobnicate"}),
    );
    assert_eq!(bad_kind["result"]["isError"], json!(true));
}

#[test]
fn parse_error_on_invalid_json() {
    let (_d, eng) = engine();
    let resp = handle_line(&eng, "r", "{not valid json").unwrap();
    assert_eq!(resp["error"]["code"], json!(PARSE_ERROR));
    assert_eq!(resp["id"], Value::Null);
}

#[test]
fn missing_method_is_invalid_request() {
    let (_d, eng) = engine();
    let req = json!({"jsonrpc": "2.0", "id": 3});
    let resp = dispatch(&eng, "r", &req).unwrap();
    assert_eq!(resp["error"]["code"], json!(INVALID_REQUEST));
}

#[test]
fn recall_filters_by_kind_and_exposes_provenance() {
    let (_d, eng) = engine();
    eng.remember("r", AtomInput::new("fact", "the alpha fact"))
        .unwrap();
    eng.remember("r", AtomInput::new("note", "a beta note"))
        .unwrap();

    let resp = call(
        &eng,
        "mem_recall",
        json!({"query": "alpha beta", "k": 5, "kinds": ["fact"]}),
    );
    assert_eq!(resp["result"]["isError"], json!(false));
    let rows = resp["result"]["structuredContent"]["hits"]
        .as_array()
        .unwrap();
    assert!(!rows.is_empty());
    for row in rows {
        assert_eq!(row["kind"], "fact");
        assert!(row.get("payload").is_some());
        assert!(row.get("distance").is_some());
        assert_eq!(row["immutable"], false);
    }
}

#[test]
fn fetch_lists_by_kind() {
    let (_d, eng) = engine();
    eng.remember("r", AtomInput::new("note", "first")).unwrap();
    eng.remember("r", AtomInput::new("note", "second")).unwrap();
    eng.remember("r", AtomInput::new("fact", "other")).unwrap();

    let resp = call(&eng, "mem_fetch", json!({"kind": "note", "limit": 10}));
    assert_eq!(resp["result"]["isError"], json!(false));
    let atoms = resp["result"]["structuredContent"]["atoms"]
        .as_array()
        .unwrap();
    assert_eq!(atoms.len(), 2);
    assert!(atoms.iter().all(|a| a["kind"] == "note"));
}

#[test]
fn remember_batch_stores_all_and_returns_ids() {
    let (_d, eng) = engine();
    let resp = call(
        &eng,
        "mem_remember_batch",
        json!({"atoms": [
            {"text": "one", "kind": "fact"},
            {"text": "two", "kind": "fact", "immutable": true},
            {"text": "three"}
        ]}),
    );
    assert_eq!(resp["result"]["isError"], json!(false));
    let ids = resp["result"]["structuredContent"]["ids"]
        .as_array()
        .unwrap();
    assert_eq!(ids.len(), 3);
}

#[test]
fn edges_introspects_the_graph() {
    let (_d, eng) = engine();
    let a = eng.remember("r", AtomInput::new("fact", "cause")).unwrap();
    let b = eng.remember("r", AtomInput::new("fact", "effect")).unwrap();
    let linked = call(
        &eng,
        "mem_link",
        json!({"src": a, "dst": b, "kind": "causes"}),
    );
    assert_eq!(linked["result"]["isError"], json!(false));

    let resp = call(&eng, "mem_edges", json!({"src": a}));
    assert_eq!(resp["result"]["isError"], json!(false));
    let edges = resp["result"]["structuredContent"]["edges"]
        .as_array()
        .unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0]["dst"], json!(b));
    assert_eq!(edges[0]["kind"], "causes");
}

#[test]
fn remember_with_payload_round_trips_through_fetch() {
    let (_d, eng) = engine();
    let stored = call(
        &eng,
        "mem_remember",
        json!({"text": "tagged", "kind": "note", "payload": {"tag": "x"}, "immutable": true}),
    );
    assert_eq!(stored["result"]["isError"], json!(false));

    let resp = call(&eng, "mem_fetch", json!({"kind": "note"}));
    let atoms = resp["result"]["structuredContent"]["atoms"]
        .as_array()
        .unwrap();
    assert_eq!(atoms.len(), 1);
    assert_eq!(atoms[0]["payload"]["tag"], "x");
    assert_eq!(atoms[0]["immutable"], true);
}

#[test]
fn recall_provenance_surfaces_derived_from() {
    let (_d, eng) = engine();
    let src = eng
        .remember("r", AtomInput::new("fact", "source observation"))
        .unwrap();
    let derived = eng
        .remember(
            "r",
            AtomInput::new("fact", "derived conclusion observation"),
        )
        .unwrap();
    eng.link(derived, src, EdgeKind::DerivedFrom, 1.0).unwrap();

    let resp = call(
        &eng,
        "mem_recall",
        json!({"query": "observation", "k": 5, "provenance": true}),
    );
    let rows = resp["result"]["structuredContent"]["hits"]
        .as_array()
        .unwrap();
    let hit = rows
        .iter()
        .find(|r| r["id"] == json!(derived))
        .expect("derived atom recalled");
    assert_eq!(hit["derived_from"], json!([src]));
}

#[test]
fn profile_returns_neighborhood_and_edges() {
    let (_d, eng) = engine();
    let a = eng
        .remember("r", AtomInput::new("fact", "alpha entity topic"))
        .unwrap();
    let b = eng
        .remember("r", AtomInput::new("fact", "beta related topic"))
        .unwrap();
    eng.link(a, b, EdgeKind::Causes, 1.0).unwrap();

    let resp = call(
        &eng,
        "mem_profile",
        json!({"query": "topic", "k": 5, "depth": 1}),
    );
    assert_eq!(resp["result"]["isError"], json!(false));
    let atoms = resp["result"]["structuredContent"]["atoms"]
        .as_array()
        .unwrap();
    let ids: Vec<i64> = atoms.iter().map(|x| x["id"].as_i64().unwrap()).collect();
    assert!(ids.contains(&a) && ids.contains(&b));
    let edges = resp["result"]["structuredContent"]["edges"]
        .as_array()
        .unwrap();
    assert!(edges
        .iter()
        .any(|e| e["src"] == json!(a) && e["dst"] == json!(b) && e["kind"] == "causes"));
}

#[test]
fn resources_templates_list_advertises_atom_template() {
    let (_d, eng) = engine();
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "resources/templates/list"});
    let resp = dispatch(&eng, "r", &req).unwrap();
    let templates = resp["result"]["resourceTemplates"].as_array().unwrap();
    assert!(templates
        .iter()
        .any(|t| t["uriTemplate"] == "memory://atom/{id}"));
}

#[test]
fn resources_read_returns_atom_contents() {
    let (_d, eng) = engine();
    let id = eng
        .remember("r", AtomInput::new("fact", "readable atom"))
        .unwrap();
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "resources/read",
                     "params": {"uri": format!("memory://atom/{id}")}});
    let resp = dispatch(&eng, "r", &req).unwrap();
    let contents = resp["result"]["contents"].as_array().unwrap();
    assert_eq!(contents.len(), 1);
    assert!(contents[0]["uri"]
        .as_str()
        .unwrap()
        .ends_with(&format!("/{id}")));
    assert!(contents[0]["text"]
        .as_str()
        .unwrap()
        .contains("readable atom"));
}

#[test]
fn resources_read_unknown_uri_is_invalid_params() {
    let (_d, eng) = engine();
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "resources/read",
                     "params": {"uri": "https://example.com/x"}});
    let resp = dispatch(&eng, "r", &req).unwrap();
    assert_eq!(resp["error"]["code"], json!(-32602));
}

#[test]
fn resources_read_missing_atom_is_not_found() {
    let (_d, eng) = engine();
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "resources/read",
                     "params": {"uri": "memory://atom/99999"}});
    let resp = dispatch(&eng, "r", &req).unwrap();
    assert_eq!(resp["error"]["code"], json!(-32002));
}
