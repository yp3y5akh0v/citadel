//! Hand-rolled synchronous MCP (Model Context Protocol) stdio server.
//!
//! Exposes citadel-mem ops as MCP tools so a client (Claude Desktop, an IDE) can use
//! citadel as agent memory. Transport is newline-delimited JSON-RPC 2.0 over
//! stdin/stdout - fully synchronous, no tokio/SDK; the pure [`dispatch`] fn holds all
//! protocol logic, [`serve_stdio`] is the thin IO loop. Only JSON-RPC goes to stdout
//! (diagnostics to stderr); one trusted local client at a time.

use std::io::{BufRead, Write};
use std::sync::Arc;

use serde_json::{json, Value};

use citadel_mem::{AtomInput, EdgeKind, EvictionPolicy, MemoryEngine, RecallQuery};

/// The MCP revision advertised; older clients still interoperate over the stable subset.
const PROTOCOL_VERSION: &str = "2025-11-25";
const SERVER_NAME: &str = "citadel-mem";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

// JSON-RPC 2.0 error codes used for protocol-level failures.
const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;

/// Serve MCP over stdin/stdout until EOF or the client goes away. A read EOF or
/// a stdout write/flush error (broken pipe) both end the loop cleanly.
pub fn serve_stdio(mem: Arc<MemoryEngine>, region: &str) -> std::io::Result<()> {
    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Ok(()); // client closed stdin
        }
        if line.trim().is_empty() {
            continue;
        }
        if let Some(resp) = handle_line(&mem, region, &line) {
            // `resp` is a serde_json::Value; its Display is compact JSON.
            if writeln!(out, "{resp}").is_err() || out.flush().is_err() {
                return Ok(()); // client gone
            }
        }
    }
}

/// Parse one line as JSON-RPC and dispatch it; an unparseable line yields a
/// parse-error response with null id.
fn handle_line(mem: &MemoryEngine, region: &str, line: &str) -> Option<Value> {
    match serde_json::from_str::<Value>(line) {
        Ok(req) => dispatch(mem, region, &req),
        Err(_) => Some(error_resp(
            Value::Null,
            PARSE_ERROR,
            "parse error: invalid JSON",
        )),
    }
}

/// The pure protocol core: map one parsed JSON-RPC message to its response, or
/// `None` for a notification (a message with no `id`, which is never answered).
fn dispatch(mem: &MemoryEngine, region: &str, req: &Value) -> Option<Value> {
    // No `id` member => notification => never reply (regardless of method).
    let id = req.get("id")?.clone();

    let method = match req.get("method").and_then(Value::as_str) {
        Some(m) => m,
        None => {
            return Some(error_resp(
                id,
                INVALID_REQUEST,
                "invalid request: missing method",
            ))
        }
    };

    let result = match method {
        "initialize" => json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": { "tools": {} },
            "serverInfo": { "name": SERVER_NAME, "version": SERVER_VERSION },
        }),
        // A liveness ping MUST get a prompt empty result or the client may drop us.
        "ping" => json!({}),
        "tools/list" => json!({ "tools": tools_list() }),
        "tools/call" => {
            let params = req.get("params");
            let name = params.and_then(|p| p.get("name")).and_then(Value::as_str);
            let args = params
                .and_then(|p| p.get("arguments"))
                .cloned()
                .unwrap_or_else(|| json!({}));
            let (text, is_error) = match name {
                Some(name) => call_tool(mem, region, name, &args),
                None => ("missing tool name".to_string(), true),
            };
            tool_result(&text, is_error)
        }
        other => {
            return Some(error_resp(
                id,
                METHOD_NOT_FOUND,
                &format!("method not found: {other}"),
            ))
        }
    };
    Some(result_resp(id, result))
}

fn result_resp(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn error_resp(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

/// Wrap tool output in an MCP `tools/call` result. A tool-level failure is a
/// successful response with `isError: true` (so the model sees and can correct
/// it), distinct from a JSON-RPC protocol error.
fn tool_result(text: &str, is_error: bool) -> Value {
    json!({ "content": [{ "type": "text", "text": text }], "isError": is_error })
}

fn tool(name: &str, description: &str, input_schema: Value) -> Value {
    json!({ "name": name, "description": description, "inputSchema": input_schema })
}

/// The six exposed memory tools. `delete_atoms` is deliberately NOT exposed: it
/// is a privileged force-delete that bypasses the immutable flag and must not be
/// reachable by a model.
fn tools_list() -> Vec<Value> {
    vec![
        // Kept byte-identical to MemRecallTool::spec (guarded by a test).
        tool(
            "mem_recall",
            "Recall the most relevant memories for a query.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "what to recall"},
                    "k": {"type": "integer", "description": "max results (default 5)"}
                },
                "required": ["query"]
            }),
        ),
        // Kept byte-identical to MemRememberTool::spec (guarded by a test).
        tool(
            "mem_remember",
            "Store a memory for later recall.",
            json!({
                "type": "object",
                "properties": {
                    "text": {"type": "string", "description": "the content to remember"},
                    "kind": {"type": "string", "description": "atom kind (default 'fact')"}
                },
                "required": ["text"]
            }),
        ),
        tool(
            "mem_link",
            "Create a directed edge between two memory atoms.",
            json!({
                "type": "object",
                "properties": {
                    "src": {"type": "integer"},
                    "dst": {"type": "integer"},
                    "kind": {"type": "string", "enum": [
                        "causes", "contradicts", "refines", "precedes",
                        "supersedes", "derived_from", "depends_on"
                    ]},
                    "weight": {"type": "number", "description": "edge weight (default 1.0)"}
                },
                "required": ["src", "dst", "kind"]
            }),
        ),
        tool(
            "mem_evolve",
            "Recompute neighbor links and score for an atom.",
            json!({
                "type": "object",
                "properties": {
                    "atom_id": {"type": "integer"},
                    "neighbors": {"type": "integer", "description": "max neighbor links (default 5)"},
                    "max_distance": {"type": "number", "description": "only link neighbors within this distance"}
                },
                "required": ["atom_id", "max_distance"]
            }),
        ),
        tool(
            "mem_summarize",
            "Per-kind structural digest of a region's atoms.",
            json!({
                "type": "object",
                "properties": {
                    "since_micros": {"type": "integer", "description": "epoch micros lower bound; 0 = all"}
                }
            }),
        ),
        tool(
            "mem_evict",
            "Selectively forget atoms by policy.",
            json!({
                "type": "object",
                "properties": {
                    "policy": {"type": "string", "enum": [
                        "stale", "lru", "low_score", "purge_region", "predicate_match"
                    ]},
                    "older_than_micros": {"type": "integer"},
                    "keep_fraction": {"type": "number"},
                    "score_threshold": {"type": "number"},
                    "confidence_threshold": {"type": "number"},
                    "predicate": {"type": "object"}
                },
                "required": ["policy"]
            }),
        ),
    ]
}

/// Run one tool; returns `(result_text, is_error)`. Tool-level failures (bad
/// args, unknown tool, engine error) are reported as `is_error = true`, never as
/// a JSON-RPC protocol error.
fn call_tool(mem: &MemoryEngine, region: &str, name: &str, args: &Value) -> (String, bool) {
    match name {
        "mem_recall" => run(|| {
            let query = req_str(args, "query")?;
            let k = args.get("k").and_then(Value::as_u64).unwrap_or(5) as usize;
            let hits = mem
                .recall(region, RecallQuery::by_text(query, k))
                .map_err(|e| e.to_string())?;
            let rows: Vec<Value> = hits
                .iter()
                .map(|h| json!({"id": h.id, "kind": h.kind, "text": h.text, "score": h.score}))
                .collect();
            Ok(Value::Array(rows).to_string())
        }),
        "mem_remember" => run(|| {
            let text = req_str(args, "text")?;
            let kind = args.get("kind").and_then(Value::as_str).unwrap_or("fact");
            let id = mem
                .remember(region, AtomInput::new(kind, text))
                .map_err(|e| e.to_string())?;
            Ok(json!({"id": id, "status": "stored"}).to_string())
        }),
        "mem_link" => run(|| {
            let src = req_i64(args, "src")?;
            let dst = req_i64(args, "dst")?;
            let kind = edge_kind(req_str(args, "kind")?)?;
            let weight = args.get("weight").and_then(Value::as_f64).unwrap_or(1.0) as f32;
            mem.link(src, dst, kind, weight)
                .map_err(|e| e.to_string())?;
            Ok(json!({"status": "linked"}).to_string())
        }),
        "mem_evolve" => run(|| {
            let atom_id = req_i64(args, "atom_id")?;
            let neighbors = args.get("neighbors").and_then(Value::as_u64).unwrap_or(5) as usize;
            let max_distance = req_f64(args, "max_distance")? as f32;
            let r = mem
                .evolve(region, atom_id, neighbors, max_distance)
                .map_err(|e| e.to_string())?;
            Ok(json!({"links_added": r.links_added, "score": r.score}).to_string())
        }),
        "mem_summarize" => run(|| {
            let since = args
                .get("since_micros")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let s = mem.summarize(region, since).map_err(|e| e.to_string())?;
            let kinds: Vec<Value> = s
                .kinds
                .iter()
                .map(|k| {
                    json!({
                        "kind": k.kind, "count": k.count,
                        "earliest": k.earliest, "latest": k.latest,
                        "avg_score": k.avg_score, "avg_confidence": k.avg_confidence
                    })
                })
                .collect();
            Ok(json!({"total": s.total, "kinds": kinds}).to_string())
        }),
        "mem_evict" => run(|| {
            let policy = eviction_policy(args)?;
            let r = mem.evict(region, policy).map_err(|e| e.to_string())?;
            Ok(json!({"removed": r.removed}).to_string())
        }),
        other => (format!("unknown tool: {other}"), true),
    }
}

fn run(f: impl FnOnce() -> Result<String, String>) -> (String, bool) {
    match f() {
        Ok(text) => (text, false),
        Err(reason) => (reason, true),
    }
}

fn req_str<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing string '{key}'"))
}

fn req_i64(args: &Value, key: &str) -> Result<i64, String> {
    args.get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("missing integer '{key}'"))
}

fn req_f64(args: &Value, key: &str) -> Result<f64, String> {
    args.get(key)
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("missing number '{key}'"))
}

fn edge_kind(s: &str) -> Result<EdgeKind, String> {
    Ok(match s {
        "causes" => EdgeKind::Causes,
        "contradicts" => EdgeKind::Contradicts,
        "refines" => EdgeKind::Refines,
        "precedes" => EdgeKind::Precedes,
        "supersedes" => EdgeKind::Supersedes,
        "derived_from" => EdgeKind::DerivedFrom,
        "depends_on" => EdgeKind::DependsOn,
        other => return Err(format!("unknown edge kind '{other}'")),
    })
}

fn eviction_policy(args: &Value) -> Result<EvictionPolicy, String> {
    Ok(match req_str(args, "policy")? {
        "stale" => EvictionPolicy::Stale {
            older_than_micros: req_i64(args, "older_than_micros")?,
        },
        "lru" => EvictionPolicy::Lru {
            keep_fraction: req_f64(args, "keep_fraction")? as f32,
        },
        "low_score" => EvictionPolicy::LowScore {
            score_threshold: req_f64(args, "score_threshold")? as f32,
            confidence_threshold: req_f64(args, "confidence_threshold")? as f32,
        },
        "purge_region" => EvictionPolicy::PurgeRegion,
        "predicate_match" => EvictionPolicy::PredicateMatch {
            predicate: args
                .get("predicate")
                .cloned()
                .ok_or_else(|| "missing object 'predicate'".to_string())?,
        },
        other => return Err(format!("unknown policy '{other}'")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::{MemRecallTool, MemRememberTool};
    use crate::Tool;
    use citadel::{Argon2Profile, DatabaseBuilder};
    use citadel_mem::MockEmbedder;

    fn engine() -> (tempfile::TempDir, Arc<MemoryEngine>) {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let eng = Arc::new(MemoryEngine::open(Arc::new(db)).unwrap());
        eng.create_region("r", Arc::new(MockEmbedder::new(64)))
            .unwrap();
        (dir, eng)
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
        assert_eq!(resp["result"]["protocolVersion"], json!(PROTOCOL_VERSION));
        assert!(resp["result"]["capabilities"]["tools"].is_object());
        assert_eq!(resp["result"]["serverInfo"]["name"], json!("citadel-mem"));
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
    fn tools_list_has_six_and_matches_agent_schemas() {
        let (_d, eng) = engine();
        let req = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"});
        let resp = dispatch(&eng, "r", &req).unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 6);
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"mem_recall") && names.contains(&"mem_evict"));
        // The reused schemas must stay identical to the agent's tool specs.
        let recall = tools.iter().find(|t| t["name"] == "mem_recall").unwrap();
        assert_eq!(
            recall["inputSchema"],
            MemRecallTool::new(eng.clone(), "r").spec().input_schema
        );
        let remember = tools.iter().find(|t| t["name"] == "mem_remember").unwrap();
        assert_eq!(
            remember["inputSchema"],
            MemRememberTool::new(eng.clone(), "r").spec().input_schema
        );
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
}
