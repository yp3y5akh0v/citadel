use super::memory::MAX_RECALL_RESULTS;
use super::{
    dispatch, dispatch_with_policy, handle_line, registry, CLIENT_CAPABILITIES_META,
    CLIENT_INFO_META, LOG_LEVEL_META, PROTOCOL_VERSION_META,
};
use crate::protocol::{
    INVALID_PARAMS, INVALID_REQUEST, METHOD_NOT_FOUND, MODERN_PROTOCOL_VERSIONS, PARSE_ERROR,
    RESOURCE_NOT_FOUND, SUPPORTED_PROTOCOL_VERSIONS, UNSUPPORTED_PROTOCOL_VERSION,
};
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
    call_with_policy(eng, name, args, false)
}

fn call_with_policy(
    eng: &MemoryEngine,
    name: &str,
    args: Value,
    allow_protected_memory_erasure: bool,
) -> Value {
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                     "params": {"name": name, "arguments": args.clone()}});
    let response = dispatch_with_policy(eng, "r", &req, allow_protected_memory_erasure).unwrap();
    assert_successful_call_matches_schema(name, &args, &response);
    response
}

fn assert_successful_call_matches_schema(name: &str, args: &Value, response: &Value) {
    if response["result"]["isError"] != false {
        return;
    }
    let definition = registry()
        .list()
        .into_iter()
        .find(|definition| definition.name == name)
        .expect("called tool is registered");
    let input = jsonschema::draft202012::new(&definition.input_schema).unwrap();
    assert!(
        input.is_valid(args),
        "{name} accepted arguments outside its input schema: {args}"
    );
    let output_schema = definition
        .output_schema
        .as_ref()
        .expect("every Citadel tool advertises structured output");
    let output = jsonschema::draft202012::new(output_schema).unwrap();
    let structured = &response["result"]["structuredContent"];
    assert!(
        output.is_valid(structured),
        "{name} returned structured content outside its output schema: {structured}"
    );
}

fn modern_request(method: &str, mut params: Value) -> Value {
    params.as_object_mut().unwrap().insert(
        "_meta".to_string(),
        json!({
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": {
                "name": "citadel-test",
                "version": "1.0.0"
            },
            "io.modelcontextprotocol/clientCapabilities": {}
        }),
    );
    json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
}

fn modern_call(eng: &MemoryEngine, name: &str, args: Value) -> Value {
    let response = dispatch(
        eng,
        "r",
        &modern_request(
            "tools/call",
            json!({"name": name, "arguments": args.clone()}),
        ),
    )
    .unwrap();
    assert_successful_call_matches_schema(name, &args, &response);
    response
}

fn assert_modern_result(response: &Value) {
    assert_eq!(response["result"]["resultType"], "complete");
    assert_eq!(
        response["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
        "citadel-mem"
    );
}

fn legacy_initialize(version: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": version,
            "capabilities": {},
            "clientInfo": {"name": "citadel-test", "version": "1.0.0"}
        }
    })
}

#[test]
fn initialize_reports_protocol_and_server_info() {
    let (_d, eng) = engine();
    let req = legacy_initialize("2025-11-25");
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
    let older = dispatch(&eng, "r", &legacy_initialize("2025-06-18")).unwrap();
    assert_eq!(older["result"]["protocolVersion"], "2025-06-18");
    // An unsupported version makes the server offer its latest.
    let unknown = dispatch(&eng, "r", &legacy_initialize("1999-01-01")).unwrap();
    assert_eq!(unknown["result"]["protocolVersion"], "2025-11-25");
}

#[test]
fn legacy_initialize_requires_its_wire_schema() {
    let (_d, eng) = engine();
    for params in [
        json!({}),
        json!({"protocolVersion": "2025-11-25", "capabilities": {}}),
        json!({
            "protocolVersion": "2025-11-25",
            "capabilities": [],
            "clientInfo": {"name": "test", "version": "1"}
        }),
        json!({
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "test"}
        }),
    ] {
        let req = json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": params});
        assert_eq!(
            dispatch(&eng, "r", &req).unwrap()["error"]["code"],
            INVALID_PARAMS,
            "{req}"
        );
    }
}

#[test]
fn modern_discovery_advertises_every_supported_version_and_only_implemented_capabilities() {
    let (_d, eng) = engine();
    let resp = dispatch(&eng, "r", &modern_request("server/discover", json!({}))).unwrap();
    let result = &resp["result"];

    assert_eq!(
        result["supportedVersions"],
        json!(SUPPORTED_PROTOCOL_VERSIONS)
    );
    assert_eq!(
        result["capabilities"],
        json!({"tools": {}, "resources": {}})
    );
    assert_eq!(result["resultType"], "complete");
    assert_eq!(result["ttlMs"], 3_600_000);
    assert_eq!(result["cacheScope"], "public");
    assert_eq!(
        result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
        "citadel-mem"
    );
    assert!(result["instructions"]
        .as_str()
        .unwrap()
        .contains("Encrypted memory"));
}

#[test]
fn modern_request_metadata_is_validated_per_request() {
    let (_d, eng) = engine();

    let no_meta = json!({"jsonrpc": "2.0", "id": 1, "method": "server/discover"});
    assert_eq!(
        dispatch(&eng, "r", &no_meta).unwrap()["error"]["code"],
        INVALID_PARAMS
    );

    let missing_capabilities = json!({
        "jsonrpc": "2.0", "id": 2, "method": "tools/list",
        "params": {"_meta": {PROTOCOL_VERSION_META: "2026-07-28"}}
    });
    assert_eq!(
        dispatch(&eng, "r", &missing_capabilities).unwrap()["error"]["code"],
        INVALID_PARAMS
    );

    let non_object_capabilities = json!({
        "jsonrpc": "2.0", "id": 3, "method": "tools/list",
        "params": {"_meta": {
            PROTOCOL_VERSION_META: "2026-07-28",
            CLIENT_CAPABILITIES_META: []
        }}
    });
    assert_eq!(
        dispatch(&eng, "r", &non_object_capabilities).unwrap()["error"]["code"],
        INVALID_PARAMS
    );

    let unsupported = json!({
        "jsonrpc": "2.0", "id": 4, "method": "tools/list",
        "params": {"_meta": {
            PROTOCOL_VERSION_META: "2025-11-25",
            CLIENT_CAPABILITIES_META: {}
        }}
    });
    let error = dispatch(&eng, "r", &unsupported).unwrap();
    assert_eq!(error["error"]["code"], UNSUPPORTED_PROTOCOL_VERSION);
    assert_eq!(error["error"]["data"]["requested"], "2025-11-25");
    assert_eq!(
        error["error"]["data"]["supported"],
        json!(MODERN_PROTOCOL_VERSIONS)
    );

    let malformed_info = json!({
        "jsonrpc": "2.0", "id": 5, "method": "tools/list",
        "params": {"_meta": {
            PROTOCOL_VERSION_META: "2026-07-28",
            CLIENT_CAPABILITIES_META: {},
            CLIENT_INFO_META: {"name": "missing-version"}
        }}
    });
    assert_eq!(
        dispatch(&eng, "r", &malformed_info).unwrap()["error"]["code"],
        INVALID_PARAMS
    );

    let malformed_meta = json!({
        "jsonrpc": "2.0", "id": 6, "method": "tools/list", "params": {"_meta": null}
    });
    assert_eq!(
        dispatch(&eng, "r", &malformed_meta).unwrap()["error"]["code"],
        INVALID_PARAMS
    );

    let legacy_meta = json!({
        "jsonrpc": "2.0", "id": 7, "method": "tools/list",
        "params": {"_meta": {"progressToken": "legacy-progress"}}
    });
    let legacy = dispatch(&eng, "r", &legacy_meta).unwrap();
    assert!(legacy["result"].get("resultType").is_none());

    let no_client_info = json!({
        "jsonrpc": "2.0", "id": 8, "method": "tools/list",
        "params": {"_meta": {
            PROTOCOL_VERSION_META: "2026-07-28",
            CLIENT_CAPABILITIES_META: {}
        }}
    });
    assert_modern_result(&dispatch(&eng, "r", &no_client_info).unwrap());

    for (field, value) in [
        (CLIENT_CAPABILITIES_META, json!({"sampling": false})),
        (
            CLIENT_INFO_META,
            json!({"name": "test", "version": "1", "title": 42}),
        ),
        ("progressToken", json!(true)),
        (LOG_LEVEL_META, json!([])),
    ] {
        let mut req = modern_request("tools/list", json!({}));
        req["params"]["_meta"][field] = value;
        assert_eq!(
            dispatch(&eng, "r", &req).unwrap()["error"]["code"],
            INVALID_PARAMS,
            "{field}"
        );
    }

    let mut extension = modern_request("tools/list", json!({}));
    extension["params"]["_meta"][CLIENT_CAPABILITIES_META] =
        json!({"extensions": {"com.example/custom": {"enabled": true}}});
    assert_modern_result(&dispatch(&eng, "r", &extension).unwrap());
}

#[test]
fn lifecycle_methods_are_selected_by_protocol_era() {
    let (_d, eng) = engine();
    for method in ["initialize", "ping", "subscriptions/listen"] {
        let params = if method == "initialize" {
            json!({"protocolVersion": "2025-11-25"})
        } else {
            json!({})
        };
        let resp = dispatch(&eng, "r", &modern_request(method, params)).unwrap();
        assert_eq!(resp["error"]["code"], METHOD_NOT_FOUND, "{method}");
    }

    let legacy_ping = dispatch(
        &eng,
        "r",
        &json!({"jsonrpc": "2.0", "id": 1, "method": "ping"}),
    )
    .unwrap();
    assert_eq!(legacy_ping["result"], json!({}));
    assert!(legacy_ping["result"].get("resultType").is_none());
}

#[test]
fn modern_extension_identifiers_follow_the_vendor_prefix_grammar() {
    let (_d, eng) = engine();
    for identifier in [
        "io.modelcontextprotocol/tasks",
        "com.example/foo_bar.baz-2",
        "a/b",
    ] {
        let mut request = modern_request("tools/list", json!({}));
        request["params"]["_meta"][CLIENT_CAPABILITIES_META] =
            json!({"extensions": {(identifier): {}}});
        assert_modern_result(&dispatch(&eng, "r", &request).unwrap());
    }

    for identifier in [
        "bad",
        "/name",
        "vendor/",
        "1vendor/name",
        "com..example/name",
        "com.-example/name",
        "com.example-/name",
        "com.example/_name",
        "com.example/name_",
        "com.example/a/b",
        "éxample/name",
    ] {
        let mut request = modern_request("tools/list", json!({}));
        request["params"]["_meta"][CLIENT_CAPABILITIES_META] =
            json!({"extensions": {(identifier): {}}});
        assert_eq!(
            dispatch(&eng, "r", &request).unwrap()["error"]["code"],
            INVALID_PARAMS,
            "{identifier}"
        );
    }

    let mut experimental = modern_request("tools/list", json!({}));
    experimental["params"]["_meta"][CLIENT_CAPABILITIES_META] =
        json!({"experimental": {"unrestricted key": {}}});
    assert_modern_result(&dispatch(&eng, "r", &experimental).unwrap());
}

#[test]
fn modern_results_have_common_metadata_and_scoped_cache_hints() {
    let (_d, eng) = engine();
    for method in ["tools/list", "resources/list", "resources/templates/list"] {
        let resp = dispatch(&eng, "r", &modern_request(method, json!({}))).unwrap();
        assert_modern_result(&resp);
        assert_eq!(resp["result"]["ttlMs"], 3_600_000, "{method}");
        assert_eq!(resp["result"]["cacheScope"], "public", "{method}");
    }

    let id = eng
        .remember("r", AtomInput::new("fact", "cache-sensitive atom"))
        .unwrap();
    let read = dispatch(
        &eng,
        "r",
        &modern_request(
            "resources/read",
            json!({"uri": format!("memory://atom/{id}")}),
        ),
    )
    .unwrap();
    assert_modern_result(&read);
    assert_eq!(read["result"]["ttlMs"], 0);
    assert_eq!(read["result"]["cacheScope"], "private");

    let call = modern_call(&eng, "mem_recall", json!({"query": "cache"}));
    assert_modern_result(&call);
    assert!(call["result"].get("ttlMs").is_none());
    assert!(call["result"].get("cacheScope").is_none());
}

#[test]
fn one_page_lists_reject_cursors_in_both_protocol_eras() {
    let (_d, eng) = engine();
    for method in ["tools/list", "resources/list", "resources/templates/list"] {
        for cursor in [json!(42), json!("never-issued")] {
            let modern = dispatch(
                &eng,
                "r",
                &modern_request(method, json!({"cursor": cursor.clone()})),
            )
            .unwrap();
            assert_eq!(modern["error"]["code"], INVALID_PARAMS, "{method}");

            let legacy = dispatch(
                &eng,
                "r",
                &json!({"jsonrpc": "2.0", "id": 1, "method": method,
                        "params": {"cursor": cursor}}),
            )
            .unwrap();
            assert_eq!(legacy["error"]["code"], INVALID_PARAMS, "{method}");
        }
    }
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
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        [
            "mem_recall",
            "mem_fetch",
            "mem_get",
            "mem_edges",
            "mem_profile",
            "mem_summarize",
            "mem_verify",
            "mem_remember",
            "mem_remember_batch",
            "mem_update",
            "mem_link",
            "mem_unlink",
            "mem_evolve",
            "mem_evict",
            "mem_forget",
        ]
    );
    let recall = tools.iter().find(|t| t["name"] == "mem_recall").unwrap();
    assert_eq!(recall["inputSchema"]["required"], json!(["query"]));
    // Recall updates in-process access accounting; eviction is directly destructive.
    assert_eq!(recall["annotations"]["readOnlyHint"], false);
    assert_eq!(recall["annotations"]["destructiveHint"], false);
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
    let remember_batch = tools
        .iter()
        .find(|tool| tool["name"] == "mem_remember_batch")
        .unwrap();
    assert_eq!(
        remember_batch["inputSchema"]["properties"]["atoms"]["items"]["required"],
        json!(["text", "idempotency_key"])
    );
    assert_eq!(
        remember_batch["inputSchema"]["properties"]["atoms"]["uniqueItems"],
        true
    );
    assert!(remember_batch["outputSchema"]["properties"]["results"].is_object());

    for tool in tools {
        assert_eq!(
            tool["inputSchema"]["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
        assert!(tool["outputSchema"].is_object(), "{}", tool["name"]);
        assert_eq!(
            tool["outputSchema"]["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
    }
}

#[test]
fn every_tool_schema_is_valid_json_schema_2020_12() {
    for definition in registry().list() {
        assert_eq!(
            definition.input_schema.get("type").and_then(Value::as_str),
            Some("object"),
            "{} input schema must have an object root",
            definition.name
        );
        jsonschema::draft202012::meta::validate(&definition.input_schema)
            .unwrap_or_else(|error| panic!("{} input schema: {error}", definition.name));
        jsonschema::draft202012::new(&definition.input_schema)
            .unwrap_or_else(|error| panic!("{} input schema: {error}", definition.name));
        let output = definition
            .output_schema
            .as_ref()
            .expect("every tool returns structured content");
        jsonschema::draft202012::meta::validate(output)
            .unwrap_or_else(|error| panic!("{} output schema: {error}", definition.name));
        jsonschema::draft202012::new(output)
            .unwrap_or_else(|error| panic!("{} output schema: {error}", definition.name));
    }
}

#[test]
fn every_tool_returns_structured_content_matching_its_output_schema() {
    let (_d, eng) = engine();
    let first = eng
        .remember("r", AtomInput::new("fact", "first schema atom"))
        .unwrap();
    let second = eng
        .remember("r", AtomInput::new("fact", "second schema atom"))
        .unwrap();
    let doomed = eng
        .remember("r", AtomInput::new("temporary", "doomed schema atom"))
        .unwrap();
    let cases = vec![
        ("mem_recall", json!({"query": "schema atom", "k": 2})),
        ("mem_fetch", json!({"limit": 2})),
        ("mem_get", json!({"ids": [second, second + 10_000, first]})),
        ("mem_edges", json!({"src": first})),
        ("mem_profile", json!({"query": "schema atom", "k": 2})),
        ("mem_summarize", json!({})),
        ("mem_verify", json!({"ids": [first]})),
        ("mem_remember", json!({"text": "single schema write"})),
        (
            "mem_remember_batch",
            json!({"atoms": [{
                "text": "batch schema write",
                "idempotency_key": "schema/batch/1"
            }]}),
        ),
        (
            "mem_update",
            json!({"id": first, "payload": {"checked": true}}),
        ),
        (
            "mem_link",
            json!({"src": first, "dst": second, "kind": "causes"}),
        ),
        (
            "mem_unlink",
            json!({"src": first, "dst": second, "kind": "causes"}),
        ),
        (
            "mem_evolve",
            json!({"atom_id": first, "neighbors": 1, "max_distance": 1.0}),
        ),
        ("mem_evict", json!({"policy": "expired"})),
        ("mem_forget", json!({"ids": [doomed]})),
    ];
    let registered = registry()
        .list()
        .into_iter()
        .map(|definition| definition.name)
        .collect::<Vec<_>>();
    let exercised = cases.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    assert_eq!(exercised, registered, "success table must cover every tool");

    for (name, arguments) in cases {
        let response = call(&eng, name, arguments);
        assert_eq!(response["result"]["isError"], false, "{name}: {response}");
    }
}

fn input_schema_accepts(name: &str, arguments: &Value) -> bool {
    let definition = registry()
        .list()
        .into_iter()
        .find(|definition| definition.name == name)
        .unwrap();
    jsonschema::draft202012::new(&definition.input_schema)
        .unwrap()
        .is_valid(arguments)
}

#[test]
fn schemas_and_handlers_reject_the_same_bounded_invalid_inputs() {
    let (_d, eng) = engine();
    for (name, arguments) in [
        ("mem_recall", json!({"query": "q", "k": 0})),
        ("mem_recall", json!({"query": "q", "graph_depth": 9})),
        (
            "mem_recall",
            json!({"query": "q", "graph_edge_kinds": ["not-an-edge"]}),
        ),
        ("mem_fetch", json!({"limit": 0})),
        (
            "mem_fetch",
            json!({"newest": true, "after_id": 1, "limit": 10}),
        ),
        ("mem_get", json!({"ids": []})),
        ("mem_get", json!({"ids": [1, 1]})),
        ("mem_edges", json!({"src": -1})),
        ("mem_profile", json!({"query": "q", "depth": 9})),
        ("mem_summarize", json!({"limit": 0})),
        ("mem_remember", json!({"text": "x", "confidence": 1.1})),
        ("mem_remember", json!({"text": "x", "expires_at": -1})),
        ("mem_remember", json!({"text": "x", "created_at": 1.5})),
        (
            "mem_remember",
            json!({"text": "x", "evidence": {"source": "missing"}}),
        ),
        ("mem_remember_batch", json!({"atoms": []})),
        (
            "mem_remember_batch",
            json!({"atoms": [{"text": "missing retry key"}]}),
        ),
        (
            "mem_remember_batch",
            json!({"atoms": [{"text": "empty retry key", "idempotency_key": ""}]}),
        ),
        (
            "mem_remember_batch",
            json!({"atoms": [{"text": "long retry key", "idempotency_key": "x".repeat(257)}]}),
        ),
        (
            "mem_remember_batch",
            json!({"atoms": [
                {"text": "duplicate entry", "idempotency_key": "same"},
                {"text": "duplicate entry", "idempotency_key": "same"}
            ]}),
        ),
        ("mem_link", json!({"src": 0, "dst": 1, "kind": "causes"})),
        ("mem_unlink", json!({"src": 1, "dst": 0, "kind": "causes"})),
        (
            "mem_evolve",
            json!({"atom_id": 1, "neighbors": 101, "max_distance": 1}),
        ),
        (
            "mem_evict",
            json!({"policy": "expired", "keep_fraction": 0.5}),
        ),
        (
            "mem_evict",
            json!({"policy": "predicate_match", "predicate": {}}),
        ),
        ("mem_forget", json!({"ids": []})),
        ("mem_verify", json!({"ids": [1, 1]})),
        ("mem_update", json!({"id": 0, "payload": {}})),
    ] {
        assert!(
            !input_schema_accepts(name, &arguments),
            "{name} schema accepted {arguments}"
        );
        let response = call(&eng, name, arguments.clone());
        assert_eq!(
            response["result"]["isError"], true,
            "{name} handler accepted {arguments}"
        );
    }

    let too_many_kinds = json!({
        "query": "q",
        "kinds": (0..101).map(|index| format!("kind-{index}")).collect::<Vec<_>>()
    });
    assert!(!input_schema_accepts("mem_recall", &too_many_kinds));
    assert_eq!(
        call(&eng, "mem_recall", too_many_kinds)["result"]["isError"],
        true
    );
    let duplicate_edge_kinds = json!({"query": "q", "graph_edge_kinds": ["causes", "causes"]});
    assert!(!input_schema_accepts("mem_recall", &duplicate_edge_kinds));
    assert_eq!(
        call(&eng, "mem_recall", duplicate_edge_kinds)["result"]["isError"],
        true
    );

    let duplicate_batch_keys = json!({"atoms": [
        {"text": "first", "idempotency_key": "same-key"},
        {"text": "second", "idempotency_key": "same-key"}
    ]});
    assert_eq!(
        call(&eng, "mem_remember_batch", duplicate_batch_keys)["result"]["isError"],
        true,
        "the handler enforces key-field uniqueness that JSON Schema cannot express"
    );

    let too_many_get_ids = json!({"ids": (1..=101).collect::<Vec<_>>()});
    assert!(!input_schema_accepts("mem_get", &too_many_get_ids));
    assert_eq!(
        call(&eng, "mem_get", too_many_get_ids)["result"]["isError"],
        true
    );
    let maximum_get_ids = json!({"ids": (1..=100).collect::<Vec<_>>()});
    assert!(input_schema_accepts("mem_get", &maximum_get_ids));
    let response = call(&eng, "mem_get", maximum_get_ids);
    assert_eq!(response["result"]["isError"], false);
    assert_eq!(
        response["result"]["structuredContent"]["results"]
            .as_array()
            .unwrap()
            .len(),
        100
    );

    let invalid_window = call(
        &eng,
        "mem_fetch",
        json!({"created_from": 20, "created_before": 20}),
    );
    assert_eq!(invalid_window["result"]["isError"], true);
}

#[test]
fn eviction_schema_pins_each_tagged_policy_shape() {
    for arguments in [
        json!({"policy": "stale", "older_than_micros": 1}),
        json!({"policy": "lru", "keep_fraction": 0.5}),
        json!({"policy": "expired"}),
        json!({"policy": "low_importance", "importance_threshold": 0.1,
               "confidence_threshold": 0.2}),
        json!({"policy": "purge_region", "confirm_region": "r"}),
        json!({"policy": "predicate_match", "predicate": {"kind": "temporary"}}),
    ] {
        assert!(input_schema_accepts("mem_evict", &arguments), "{arguments}");
    }
}

#[test]
fn explicit_null_matches_optional_argument_semantics() {
    let (_d, eng) = engine();
    eng.remember("r", AtomInput::new("fact", "nullable options"))
        .unwrap();
    for (name, arguments) in [
        (
            "mem_recall",
            json!({"query": "nullable", "payload_filter": null, "weights": null}),
        ),
        (
            "mem_fetch",
            json!({"kind": null, "payload_filter": null, "after_id": null,
                   "created_from": null, "created_before": null}),
        ),
        (
            "mem_edges",
            json!({"src": null, "dst": null, "kind": null, "after": null}),
        ),
        (
            "mem_remember",
            json!({"text": "null option", "importance": null, "confidence": null,
                   "expires_at": null, "created_at": null,
                   "idempotency_key": null, "evidence": null}),
        ),
    ] {
        assert!(
            input_schema_accepts(name, &arguments),
            "{name}: {arguments}"
        );
        assert_eq!(call(&eng, name, arguments)["result"]["isError"], false);
    }
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

#[test]
fn summarize_pages_kinds_without_hiding_the_region_total() {
    let (_d, eng) = engine();
    for kind in ["charlie", "alpha", "bravo"] {
        eng.remember("r", AtomInput::new(kind, format!("{kind} memory")))
            .unwrap();
    }

    let first = call(&eng, "mem_summarize", json!({"limit": 2}));
    assert_eq!(first["result"]["structuredContent"]["total"], 3);
    assert_eq!(
        first["result"]["structuredContent"]["kinds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|kind| kind["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["alpha", "bravo"]
    );
    assert_eq!(
        first["result"]["structuredContent"]["next_after_kind"],
        "bravo"
    );

    let second = call(
        &eng,
        "mem_summarize",
        json!({"limit": 2, "after_kind": "bravo"}),
    );
    assert_eq!(second["result"]["structuredContent"]["total"], 3);
    assert_eq!(
        second["result"]["structuredContent"]["kinds"][0]["kind"],
        "charlie"
    );
    assert!(second["result"]["structuredContent"]["next_after_kind"].is_null());
}

/// `expired` drops only atoms whose TTL has lapsed; unexpired ones survive.
#[test]
fn mem_evict_expired_removes_only_lapsed_ttl() {
    let (_d, eng) = engine();
    let lapsed = eng
        .remember("r", AtomInput::new("fact", "lapsed").with_expires_at(1))
        .unwrap();
    let live = eng.remember("r", AtomInput::new("fact", "kept")).unwrap();

    let evicted = call(&eng, "mem_evict", json!({"policy": "expired"}));
    assert_eq!(evicted["result"]["isError"], json!(false));

    assert!(eng.fetch_one("r", lapsed).unwrap().is_none());
    assert!(eng.fetch_one("r", live).unwrap().is_some());
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
        eng.fetch_edges_in_region("r", Some(a), None, None, 10)
            .unwrap()
            .is_empty(),
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

    let denied = call(
        &eng,
        "mem_evict",
        json!({"policy": "purge_region", "confirm_region": "r"}),
    );
    assert_eq!(denied["result"]["isError"], true);
    let evicted = call_with_policy(
        &eng,
        "mem_evict",
        json!({"policy": "purge_region", "confirm_region": "r"}),
        true,
    );
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

#[test]
fn mem_forget_cascade_is_opt_in_region_scoped_and_atomic_on_immutable_error() {
    let (_d, eng) = make_engine(false);

    let root = eng
        .remember("r", AtomInput::new("turn", "non-cascade root"))
        .unwrap();
    let dependent = eng
        .remember_derived(
            "r",
            AtomInput::new("fact", "non-cascade dependent"),
            &[root],
            None,
        )
        .unwrap();
    let targeted = call(&eng, "mem_forget", json!({"ids": [root]}));
    assert_eq!(targeted["result"]["isError"], false);
    assert!(eng.fetch_one("r", root).unwrap().is_none());
    assert!(
        eng.fetch_one("r", dependent).unwrap().is_some(),
        "the default targeted forget unexpectedly cascaded"
    );

    eng.create_region("other", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    let foreign_root = eng
        .remember("other", AtomInput::new("turn", "foreign root"))
        .unwrap();
    let foreign_dependent = eng
        .remember_derived(
            "other",
            AtomInput::new("fact", "foreign dependent"),
            &[foreign_root],
            None,
        )
        .unwrap();

    let cascade_root = eng
        .remember("r", AtomInput::new("turn", "cascade root"))
        .unwrap();
    let cascade_dependent = eng
        .remember_derived(
            "r",
            AtomInput::new("fact", "cascade dependent"),
            &[cascade_root],
            None,
        )
        .unwrap();
    let cascaded = call(
        &eng,
        "mem_forget",
        json!({"ids": [cascade_root], "cascade_dependents": true}),
    );
    assert_eq!(cascaded["result"]["isError"], false);
    assert_eq!(cascaded["result"]["structuredContent"]["rowsDeleted"], 2);
    assert!(eng.fetch_one("r", cascade_root).unwrap().is_none());
    assert!(eng.fetch_one("r", cascade_dependent).unwrap().is_none());
    assert!(eng.fetch_one("other", foreign_root).unwrap().is_some());
    assert!(eng.fetch_one("other", foreign_dependent).unwrap().is_some());

    let blocked_root = eng
        .remember("r", AtomInput::new("turn", "blocked root"))
        .unwrap();
    let blocked_dependent = eng
        .remember_derived(
            "r",
            AtomInput::new("fact", "protected dependent").immutable(),
            &[blocked_root],
            None,
        )
        .unwrap();
    let blocked = call(
        &eng,
        "mem_forget",
        json!({"ids": [blocked_root], "cascade_dependents": true}),
    );
    assert_eq!(blocked["result"]["isError"], true);
    assert!(eng.fetch_one("r", blocked_root).unwrap().is_some());
    assert!(eng.fetch_one("r", blocked_dependent).unwrap().is_some());
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

    let denied = call(
        &eng,
        "mem_forget",
        json!({"ids": [t], "force": true, "confirm_region": "r"}),
    );
    assert_eq!(denied["result"]["isError"], true);
    let wrong_region = call_with_policy(
        &eng,
        "mem_forget",
        json!({"ids": [t], "force": true, "confirm_region": "other"}),
        true,
    );
    assert_eq!(wrong_region["result"]["isError"], true);
    let forced = call_with_policy(
        &eng,
        "mem_forget",
        json!({"ids": [t], "force": true, "confirm_region": "r"}),
        true,
    );
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

/// The engine writes `similar_to` edges (mem_evolve, graph weave) and `mem_edges`
/// emits that string, so the tools must also accept it as a filter.
#[test]
fn similar_to_edges_round_trip_through_the_tools() {
    let (_d, eng) = engine();
    let a = eng.remember("r", AtomInput::new("fact", "alpha")).unwrap();
    let b = eng.remember("r", AtomInput::new("fact", "beta")).unwrap();

    let linked = call(
        &eng,
        "mem_link",
        json!({"src": a, "dst": b, "kind": "similar_to"}),
    );
    assert_eq!(
        linked["result"]["isError"],
        json!(false),
        "mem_link accepts it"
    );

    let listed = call(&eng, "mem_edges", json!({"src": a, "kind": "similar_to"}));
    assert_eq!(
        listed["result"]["isError"],
        json!(false),
        "mem_edges filters by the same string it emits"
    );
    let edges = listed["result"]["structuredContent"]["edges"]
        .as_array()
        .unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0]["kind"], "similar_to");
}

/// Recall hides atoms a newer atom supersedes; `include_superseded` is the way back.
/// Without the flag an MCP client cannot reach superseded history at all.
#[test]
fn mem_recall_excludes_superseded_unless_asked() {
    let (_d, eng) = engine();
    let old = eng
        .remember("r", AtomInput::new("fact", "alpha old value"))
        .unwrap();
    let new = eng
        .remember("r", AtomInput::new("fact", "alpha new value"))
        .unwrap();
    eng.link_in_region("r", new, old, citadel_mem::EdgeKind::Supersedes, 1.0)
        .unwrap();

    let ids = |resp: &serde_json::Value| -> Vec<i64> {
        resp["result"]["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["id"].as_i64().unwrap())
            .collect()
    };

    let default = call(&eng, "mem_recall", json!({"query": "alpha", "k": 10}));
    assert!(!ids(&default).contains(&old), "superseded atom is hidden");

    let opted = call(
        &eng,
        "mem_recall",
        json!({"query": "alpha", "k": 10, "include_superseded": true}),
    );
    assert!(
        ids(&opted).contains(&old),
        "include_superseded must bring it back"
    );
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
    assert_eq!(resp["result"]["structuredContent"]["changed"], true);
    assert_eq!(
        eng.fetch_one("r", a).unwrap().unwrap().payload,
        json!({"v": 2})
    );

    let unchanged = call(&eng, "mem_update", json!({"id": a, "payload": {"v": 2}}));
    assert_eq!(unchanged["result"]["structuredContent"]["changed"], false);

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
fn legacy_tool_request_errors_and_execution_errors_are_separated() {
    let (_d, eng) = engine();
    let ghost = call(&eng, "ghost_tool", json!({}));
    assert_eq!(ghost["error"]["code"], INVALID_PARAMS);

    let missing = call(&eng, "mem_recall", json!({}));
    assert_eq!(missing["result"]["isError"], true);

    let bad_kind = call(
        &eng,
        "mem_link",
        json!({"src": 1, "dst": 2, "kind": "frobnicate"}),
    );
    assert_eq!(bad_kind["result"]["isError"], json!(true));
}

#[test]
fn modern_tool_request_errors_and_execution_errors_are_separated() {
    let (_d, eng) = engine();

    let unknown = modern_call(&eng, "ghost_tool", json!({}));
    assert_eq!(unknown["error"]["code"], INVALID_PARAMS);
    assert!(unknown.get("result").is_none());

    let missing_name = dispatch(
        &eng,
        "r",
        &modern_request("tools/call", json!({"arguments": {}})),
    )
    .unwrap();
    assert_eq!(missing_name["error"]["code"], INVALID_PARAMS);

    let non_object_arguments = dispatch(
        &eng,
        "r",
        &modern_request("tools/call", json!({"name": "mem_recall", "arguments": []})),
    )
    .unwrap();
    assert_eq!(non_object_arguments["error"]["code"], INVALID_PARAMS);

    let invalid_args = modern_call(&eng, "mem_recall", json!({}));
    assert_modern_result(&invalid_args);
    assert_eq!(invalid_args["result"]["isError"], true);

    let semantic_error = dispatch(
        &eng,
        "not-attached",
        &modern_request(
            "tools/call",
            json!({"name": "mem_recall", "arguments": {"query": "memory"}}),
        ),
    )
    .unwrap();
    assert_modern_result(&semantic_error);
    assert_eq!(semantic_error["result"]["isError"], true);
}

#[test]
fn modern_stateful_request_params_are_validated() {
    let (_d, eng) = engine();
    let atom_id = eng
        .remember("r", AtomInput::new("fact", "stateful request"))
        .unwrap();

    for request in [
        modern_request(
            "tools/call",
            json!({
                "name": "mem_recall",
                "arguments": {"query": "stateful"},
                "requestState": 42
            }),
        ),
        modern_request(
            "resources/read",
            json!({"uri": format!("memory://atom/{atom_id}"), "requestState": 42}),
        ),
        modern_request(
            "tools/call",
            json!({
                "name": "mem_recall",
                "arguments": {"query": "stateful"},
                "inputResponses": 42
            }),
        ),
        modern_request(
            "resources/read",
            json!({"uri": format!("memory://atom/{atom_id}"), "inputResponses": 42}),
        ),
    ] {
        let response = dispatch(&eng, "r", &request).unwrap();
        assert_eq!(response["error"]["code"], INVALID_PARAMS);
    }

    let tool = dispatch(
        &eng,
        "r",
        &modern_request(
            "tools/call",
            json!({
                "name": "mem_recall",
                "arguments": {"query": "stateful"},
                "requestState": "opaque-client-state",
                "inputResponses": {}
            }),
        ),
    )
    .unwrap();
    assert_modern_result(&tool);
    assert_eq!(tool["result"]["isError"], false);

    let resource = dispatch(
        &eng,
        "r",
        &modern_request(
            "resources/read",
            json!({
                "uri": format!("memory://atom/{atom_id}"),
                "requestState": "opaque-client-state",
                "inputResponses": {}
            }),
        ),
    )
    .unwrap();
    assert_modern_result(&resource);
    assert_eq!(resource["result"]["contents"].as_array().unwrap().len(), 1);
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
fn malformed_json_rpc_envelopes_are_invalid_requests() {
    let (_d, eng) = engine();
    for req in [
        json!([]),
        json!({"jsonrpc": "2.0", "method": 42}),
        json!({"jsonrpc": "1.0", "method": "ping"}),
        json!({"jsonrpc": "2.0", "method": "ping", "params": 1}),
        json!({"jsonrpc": "1.0", "id": 1, "method": "ping"}),
        json!({"jsonrpc": "2.0", "id": true, "method": "ping"}),
        json!({"jsonrpc": "2.0", "id": 1, "method": 42}),
        json!({"jsonrpc": "2.0", "id": 1, "method": "ping", "params": []}),
    ] {
        let resp = dispatch(&eng, "r", &req).unwrap();
        assert_eq!(resp["error"]["code"], INVALID_REQUEST, "{req}");
    }

    for id in [
        Value::Null,
        json!([]),
        json!({}),
        json!(-2.5),
        json!(1.5),
        json!(9_007_199_254_740_992.0),
    ] {
        let request = json!({"jsonrpc": "2.0", "id": id, "method": "ping"});
        let response = dispatch(&eng, "r", &request).unwrap();
        assert_eq!(response["id"], Value::Null, "{request}");
        assert_eq!(response["error"]["code"], INVALID_REQUEST, "{request}");
    }

    for id in [
        json!(-1),
        json!(-2.0),
        json!(0),
        json!(1),
        json!(1.0),
        json!(u64::MAX),
        json!("request-1"),
    ] {
        let request = json!({"jsonrpc": "2.0", "id": id.clone(), "method": "ping"});
        let response = dispatch(&eng, "r", &request).unwrap();
        assert_eq!(response["id"], id);
        assert_eq!(response["result"], json!({}));
    }
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
        assert!(row["relevance"].is_number());
        assert!(row["distance"].is_number());
        assert!(row["graph_depth"].is_null());
        assert_eq!(row["immutable"], false);
    }
}

#[test]
fn recall_graph_expansion_has_a_server_work_budget() {
    let (_d, eng) = engine();
    let root = eng
        .remember("r", AtomInput::new("fact", "unique central memory"))
        .unwrap();
    for index in 0..=MAX_RECALL_RESULTS {
        let child = eng
            .remember(
                "r",
                AtomInput::new("fact", format!("peripheral memory {index}")),
            )
            .unwrap();
        eng.link_in_region("r", root, child, EdgeKind::Refines, 1.0)
            .unwrap();
    }

    let seeds = call(
        &eng,
        "mem_recall",
        json!({"query": "unique central memory", "k": 1}),
    );
    assert_eq!(seeds["result"]["structuredContent"]["hits"][0]["id"], root);

    let expanded = call(
        &eng,
        "mem_recall",
        json!({
            "query": "unique central memory",
            "k": 1,
            "graph_depth": 1,
            "graph_edge_kinds": ["refines"]
        }),
    );
    assert_eq!(
        expanded["result"]["isError"], true,
        "the graph must fail before materializing an unbounded response"
    );
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
fn fetch_pages_and_filters_without_embedding() {
    let (_d, eng) = engine();
    let first = eng
        .remember("r", AtomInput::new("note", "first").with_created_at(10))
        .unwrap();
    let second = eng
        .remember("r", AtomInput::new("fact", "second").with_created_at(20))
        .unwrap();
    let third = eng
        .remember("r", AtomInput::new("note", "third").with_created_at(30))
        .unwrap();

    let first_page = call(&eng, "mem_fetch", json!({"limit": 2}));
    assert_eq!(
        first_page["result"]["structuredContent"]["atoms"],
        json!([
            {"id": first, "kind": "note", "text": "first", "importance": 0.0,
             "confidence": 1.0, "relevance": null, "distance": null,
             "graph_depth": null, "created_at": 10, "expires_at": null,
             "immutable": false, "payload": null},
            {"id": second, "kind": "fact", "text": "second", "importance": 0.0,
             "confidence": 1.0, "relevance": null, "distance": null,
             "graph_depth": null, "created_at": 20, "expires_at": null,
             "immutable": false, "payload": null}
        ])
    );
    assert_eq!(
        first_page["result"]["structuredContent"]["next_after_id"],
        second
    );

    let resumed = call(&eng, "mem_fetch", json!({"after_id": second, "limit": 2}));
    assert_eq!(
        resumed["result"]["structuredContent"]["atoms"][0]["id"],
        third
    );
    assert!(resumed["result"]["structuredContent"]["next_after_id"].is_null());

    let time_window = call(
        &eng,
        "mem_fetch",
        json!({"created_from": 15, "created_before": 30, "limit": 10}),
    );
    assert_eq!(
        time_window["result"]["structuredContent"]["atoms"][0]["id"],
        second
    );
    assert_eq!(
        time_window["result"]["structuredContent"]["atoms"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(time_window["result"]["structuredContent"]["next_after_id"].is_null());

    let newest = call(&eng, "mem_fetch", json!({"newest": true, "limit": 2}));
    let ids: Vec<_> = newest["result"]["structuredContent"]["atoms"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, [second, third]);
    assert!(newest["result"]["structuredContent"]["next_after_id"].is_null());
}

#[test]
fn get_preserves_request_order_and_marks_missing_atoms() {
    let (_d, eng) = engine();
    let first = eng
        .remember("r", AtomInput::new("note", "first exact atom"))
        .unwrap();
    let expires_at = 4_000_000_000_000_000;
    let second = eng
        .remember(
            "r",
            AtomInput::new("fact", "second exact atom")
                .with_confidence(0.375)
                .with_created_at(-10)
                .with_expires_at(expires_at),
        )
        .unwrap();
    let missing = second + 10_000;

    let response = call(&eng, "mem_get", json!({"ids": [second, missing, first]}));
    let results = response["result"]["structuredContent"]["results"]
        .as_array()
        .unwrap();
    assert_eq!(
        results
            .iter()
            .map(|row| row["requested_id"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        [second, missing, first]
    );
    assert_eq!(results[0]["found"], true);
    assert_eq!(results[0]["atom"]["id"], second);
    assert_eq!(results[0]["atom"]["text"], "second exact atom");
    assert_eq!(results[0]["atom"]["confidence"], 0.375);
    assert_eq!(results[0]["atom"]["created_at"], -10);
    assert_eq!(results[0]["atom"]["expires_at"], expires_at);
    assert_eq!(results[1]["found"], false);
    assert!(results[1]["atom"].is_null());
    assert_eq!(results[2]["found"], true);
    assert_eq!(results[2]["atom"]["id"], first);

    let links = response["result"]["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|content| content["type"] == "resource_link")
        .map(|content| content["uri"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        links,
        [
            format!("memory://atom/{second}"),
            format!("memory://atom/{first}")
        ]
    );
}

#[test]
fn remember_retries_are_atomic_and_provenance_aware() {
    let (_d, eng) = engine();
    let source = eng.remember("r", AtomInput::new("fact", "source")).unwrap();
    let args = json!({
        "text": "derived",
        "idempotency_key": "turn-17/derived",
        "sources": [source],
        "evidence": {"span": "line 4"}
    });
    let first = call(&eng, "mem_remember", args.clone());
    let replay = call(&eng, "mem_remember", args);
    let id = first["result"]["structuredContent"]["id"].as_i64().unwrap();
    assert_eq!(first["result"]["structuredContent"]["inserted"], true);
    assert_eq!(replay["result"]["structuredContent"]["id"], id);
    assert_eq!(replay["result"]["structuredContent"]["inserted"], false);

    let edges = eng
        .fetch_edges_in_region("r", Some(id), None, Some(EdgeKind::DerivedFrom), 10)
        .unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].dst_id, source);
    assert_eq!(edges[0].evidence_ref, Some(json!({"span": "line 4"})));

    let changed = call(
        &eng,
        "mem_remember",
        json!({"text": "changed", "idempotency_key": "turn-17/derived"}),
    );
    assert_eq!(changed["result"]["isError"], true);
    let evidence_without_sources = call(
        &eng,
        "mem_remember",
        json!({"text": "invalid", "evidence": {"span": 1}}),
    );
    assert_eq!(evidence_without_sources["result"]["isError"], true);
}

#[test]
fn integral_wire_numbers_do_not_rewrite_payload_or_evidence_json() {
    let (_d, eng) = engine();
    let source = eng.remember("r", AtomInput::new("fact", "source")).unwrap();
    let response = call(
        &eng,
        "mem_remember",
        json!({
            "text": "derived",
            "expires_at": 4_000_000_000_000_000.0,
            "sources": [source as f64],
            "payload": {"numeric": 1.0},
            "evidence": {"numeric": 2.0}
        }),
    );
    assert_eq!(response["result"]["isError"], false, "{response}");
    let id = response["result"]["structuredContent"]["id"]
        .as_i64()
        .unwrap();

    let atom = eng.fetch_one("r", id).unwrap().unwrap();
    assert!(atom.payload["numeric"].as_i64().is_none());
    assert_eq!(atom.payload["numeric"].as_f64(), Some(1.0));
    let edge = eng
        .fetch_edges_in_region("r", Some(id), Some(source), Some(EdgeKind::DerivedFrom), 1)
        .unwrap()
        .pop()
        .unwrap();
    assert!(edge.evidence_ref.as_ref().unwrap()["numeric"]
        .as_i64()
        .is_none());
    assert_eq!(
        edge.evidence_ref.as_ref().unwrap()["numeric"].as_f64(),
        Some(2.0)
    );
}

#[test]
fn wire_integer_fields_reject_fractional_and_unsafe_json_numbers() {
    let (_d, eng) = engine();
    for after_id in [json!(1.5), json!(9_007_199_254_740_992.0), json!(u64::MAX)] {
        let arguments = json!({"after_id": after_id});
        assert!(
            !input_schema_accepts("mem_fetch", &arguments),
            "schema accepted {arguments}"
        );
        let response = call(&eng, "mem_fetch", arguments.clone());
        assert_eq!(
            response["result"]["isError"], true,
            "handler accepted {arguments}: {response}"
        );
    }

    let boundary = json!({"after_id": 9_007_199_254_740_991_i64});
    assert!(input_schema_accepts("mem_fetch", &boundary));
    assert_eq!(
        call(&eng, "mem_fetch", boundary)["result"]["isError"],
        false
    );
}

#[test]
fn mcp_graph_views_never_expose_foreign_region_edges() {
    let (_d, eng) = engine();
    eng.create_region("other", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    let local = eng
        .remember("r", AtomInput::new("fact", "local derived memory"))
        .unwrap();
    let foreign = eng
        .remember("other", AtomInput::new("fact", "foreign source"))
        .unwrap();
    let error = eng
        .link_with_evidence_in_region(
            "r",
            local,
            foreign,
            EdgeKind::DerivedFrom,
            1.0,
            Some(json!({"secret": "foreign"})),
        )
        .unwrap_err();
    assert!(matches!(error, citadel_mem::MemError::AtomNotLive { .. }));

    let edges = call(&eng, "mem_edges", json!({"src": local}));
    assert!(edges["result"]["structuredContent"]["edges"]
        .as_array()
        .unwrap()
        .is_empty());

    let recalled = call(
        &eng,
        "mem_recall",
        json!({"query": "local derived", "k": 10, "provenance": true}),
    );
    let local_hit = recalled["result"]["structuredContent"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .find(|hit| hit["id"] == local)
        .unwrap();
    assert_eq!(local_hit["derived_from"], json!([]));
}

#[test]
fn remember_batch_is_retry_safe_ordered_and_atomic_on_conflict() {
    let (_d, eng) = engine();
    let arguments = json!({"atoms": [
        {"text": "one", "kind": "fact", "idempotency_key": "batch/one"},
        {"text": "two", "kind": "fact", "immutable": true,
         "idempotency_key": "batch/two"},
        {"text": "three", "idempotency_key": "batch/three"}
    ]});
    let first = call(&eng, "mem_remember_batch", arguments);
    assert_eq!(first["result"]["isError"], json!(false));
    let first_results = first["result"]["structuredContent"]["results"]
        .as_array()
        .unwrap();
    assert_eq!(first_results.len(), 3);
    assert!(first_results
        .iter()
        .all(|result| result["inserted"] == true));
    let ids = first_results
        .iter()
        .map(|result| result["id"].as_i64().unwrap())
        .collect::<Vec<_>>();

    let replay = call(
        &eng,
        "mem_remember_batch",
        json!({"atoms": [
            {"text": "three", "idempotency_key": "batch/three"},
            {"text": "one", "kind": "fact", "idempotency_key": "batch/one"},
            {"text": "two", "kind": "fact", "immutable": true,
             "idempotency_key": "batch/two"}
        ]}),
    );
    let replay_results = replay["result"]["structuredContent"]["results"]
        .as_array()
        .unwrap();
    assert_eq!(
        replay_results
            .iter()
            .map(|result| result["id"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        [ids[2], ids[0], ids[1]]
    );
    assert!(replay_results
        .iter()
        .all(|result| result["inserted"] == false));

    let before = eng.count("r", "fact").unwrap();
    let conflict = call(
        &eng,
        "mem_remember_batch",
        json!({"atoms": [
            {"text": "must not commit", "kind": "fact", "idempotency_key": "batch/fresh"},
            {"text": "changed", "kind": "fact", "idempotency_key": "batch/two"}
        ]}),
    );
    assert_eq!(conflict["result"]["isError"], true);
    assert_eq!(
        eng.count("r", "fact").unwrap(),
        before,
        "a changed key in a later entry rejects the entire batch"
    );

    let fresh = call(
        &eng,
        "mem_remember_batch",
        json!({"atoms": [{
            "text": "must not commit", "kind": "fact", "idempotency_key": "batch/fresh"
        }]}),
    );
    assert_eq!(
        fresh["result"]["structuredContent"]["results"][0]["inserted"],
        true
    );
}

#[test]
fn edges_introspects_the_graph() {
    let (_d, eng) = engine();
    let a = eng.remember("r", AtomInput::new("fact", "cause")).unwrap();
    let b = eng.remember("r", AtomInput::new("fact", "effect")).unwrap();
    let linked = call(
        &eng,
        "mem_link",
        json!({
            "src": a,
            "dst": b,
            "kind": "causes",
            "evidence": {"source": "user statement", "confidence": 0.8}
        }),
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
    assert_eq!(
        edges[0]["evidence"],
        json!({"source": "user statement", "confidence": 0.8})
    );
    assert!(resp["result"]["structuredContent"]["next_after"].is_null());
}

#[test]
fn edges_paginate_and_unlink_exactly() {
    let (_d, eng) = engine();
    let a = eng.remember("r", AtomInput::new("fact", "a")).unwrap();
    let b = eng.remember("r", AtomInput::new("fact", "b")).unwrap();
    let c = eng.remember("r", AtomInput::new("fact", "c")).unwrap();
    for (dst, kind) in [(b, "causes"), (b, "refines"), (c, "precedes")] {
        let linked = call(
            &eng,
            "mem_link",
            json!({"src": a, "dst": dst, "kind": kind}),
        );
        assert_eq!(linked["result"]["isError"], false);
    }

    let first = call(&eng, "mem_edges", json!({"src": a, "limit": 2}));
    let first_content = &first["result"]["structuredContent"];
    assert_eq!(first_content["edges"].as_array().unwrap().len(), 2);
    let cursor = first_content["next_after"].clone();
    assert_eq!(cursor, json!({"src": a, "dst": b, "kind": "refines"}));

    let second = call(
        &eng,
        "mem_edges",
        json!({"src": a, "limit": 2, "after": cursor}),
    );
    let second_content = &second["result"]["structuredContent"];
    assert_eq!(second_content["edges"].as_array().unwrap().len(), 1);
    assert_eq!(second_content["edges"][0]["dst"], c);
    assert!(second_content["next_after"].is_null());

    let removed = call(
        &eng,
        "mem_unlink",
        json!({"src": a, "dst": b, "kind": "causes"}),
    );
    assert_eq!(removed["result"]["structuredContent"]["removed"], true);
    let replay = call(
        &eng,
        "mem_unlink",
        json!({"src": a, "dst": b, "kind": "causes"}),
    );
    assert_eq!(replay["result"]["structuredContent"]["removed"], false);
    let remaining = call(&eng, "mem_edges", json!({"src": a}));
    assert!(remaining["result"]["structuredContent"]["edges"]
        .as_array()
        .unwrap()
        .iter()
        .all(|edge| edge["kind"] != "causes"));
}

#[test]
fn remember_with_payload_and_event_time_round_trips_through_fetch() {
    let (_d, eng) = engine();
    let expires_at = 4_000_000_000_000_000_i64;
    let stored = call(
        &eng,
        "mem_remember",
        json!({"text": "tagged", "kind": "note", "payload": {"tag": "x"},
               "confidence": 0.625, "created_at": -123,
               "expires_at": expires_at, "immutable": true}),
    );
    assert_eq!(stored["result"]["isError"], json!(false));

    let resp = call(
        &eng,
        "mem_fetch",
        json!({"kind": "note", "created_from": -124, "created_before": -122}),
    );
    let atoms = resp["result"]["structuredContent"]["atoms"]
        .as_array()
        .unwrap();
    assert_eq!(atoms.len(), 1);
    assert_eq!(atoms[0]["payload"]["tag"], "x");
    assert_eq!(atoms[0]["confidence"], 0.625);
    assert_eq!(atoms[0]["created_at"], -123);
    assert_eq!(atoms[0]["expires_at"], expires_at);
    assert_eq!(atoms[0]["immutable"], true);

    let summary = call(&eng, "mem_summarize", json!({"since_micros": -124}));
    assert_eq!(summary["result"]["structuredContent"]["total"], 1);
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
    eng.link_in_region("r", derived, src, EdgeKind::DerivedFrom, 1.0)
        .unwrap();

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
    eng.link_in_region("r", a, b, EdgeKind::Causes, 1.0)
        .unwrap();

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
    assert_eq!(resp["error"]["code"], RESOURCE_NOT_FOUND);
}

#[test]
fn modern_resource_errors_use_invalid_params_and_echo_the_uri() {
    let (_d, eng) = engine();
    for uri in ["memory://atom/99999", "https://example.com/x"] {
        let resp = dispatch(
            &eng,
            "r",
            &modern_request("resources/read", json!({"uri": uri})),
        )
        .unwrap();
        assert_eq!(resp["error"]["code"], INVALID_PARAMS, "{uri}");
        assert_eq!(resp["error"]["data"]["uri"], uri);
    }
}
