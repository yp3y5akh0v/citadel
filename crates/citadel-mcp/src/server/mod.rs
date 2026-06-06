//! Hand-rolled synchronous MCP server: newline-delimited JSON-RPC 2.0 over stdio plus
//! the method router. Only protocol messages go to stdout (diagnostics to stderr); one
//! trusted local client at a time. [`dispatch`] is the pure router, [`serve_stdio`] the IO loop.

mod memory;
mod resource;
mod tool;

use std::io::{BufRead, Write};
use std::sync::{Arc, OnceLock};

use serde_json::{json, Value};

use citadel_mem::MemoryEngine;

use crate::protocol::{
    error_response, negotiate_protocol_version, parse_message, result_response, INVALID_PARAMS,
    INVALID_REQUEST, METHOD_NOT_FOUND,
};
use crate::types::{CallToolResult, InitializeResult, ServerInfo};

const SERVER_NAME: &str = "citadel-mem";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The memory tool set, built once on first use.
fn registry() -> &'static tool::Registry {
    static REGISTRY: OnceLock<tool::Registry> = OnceLock::new();
    REGISTRY.get_or_init(memory::registry)
}

/// The resource families, built once on first use.
fn resources() -> &'static resource::ResourceRegistry {
    static RESOURCES: OnceLock<resource::ResourceRegistry> = OnceLock::new();
    RESOURCES.get_or_init(memory::resource_registry)
}

/// Serve MCP over stdin/stdout until EOF or the client goes away. A read EOF or a
/// stdout write/flush error (broken pipe) both end the loop cleanly.
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
            if writeln!(out, "{resp}").is_err() || out.flush().is_err() {
                return Ok(()); // client gone
            }
        }
    }
}

/// Parse one line and dispatch it; an unparseable line yields a parse-error response.
fn handle_line(mem: &MemoryEngine, region: &str, line: &str) -> Option<Value> {
    match parse_message(line) {
        Ok(req) => dispatch(mem, region, &req),
        Err(parse_error) => Some(parse_error),
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
            return Some(error_response(
                id,
                INVALID_REQUEST,
                "invalid request: missing method",
            ))
        }
    };

    let result = match method {
        "initialize" => {
            let requested = req
                .get("params")
                .and_then(|p| p.get("protocolVersion"))
                .and_then(Value::as_str);
            to_result(InitializeResult {
                protocol_version: negotiate_protocol_version(requested),
                capabilities: json!({ "tools": {}, "resources": {} }),
                server_info: ServerInfo {
                    name: SERVER_NAME,
                    version: SERVER_VERSION,
                },
            })
        }
        // A liveness ping MUST get a prompt empty result or the client may drop us.
        "ping" => json!({}),
        "tools/list" => json!({ "tools": registry().list() }),
        "tools/call" => to_result(call_tool(mem, region, req)),
        "resources/list" => json!({ "resources": [] }),
        "resources/templates/list" => json!({ "resourceTemplates": resources().templates() }),
        "resources/read" => {
            let uri = req
                .get("params")
                .and_then(|p| p.get("uri"))
                .and_then(Value::as_str);
            match uri {
                Some(uri) => match resources().read(&tool::ToolCtx { mem, region }, uri) {
                    Ok(contents) => json!({ "contents": contents }),
                    Err(err) => {
                        let code = err.code();
                        return Some(error_response(id, code, &err.message()));
                    }
                },
                None => {
                    return Some(error_response(
                        id,
                        INVALID_PARAMS,
                        "resources/read requires a uri",
                    ))
                }
            }
        }
        other => {
            return Some(error_response(
                id,
                METHOD_NOT_FOUND,
                &format!("method not found: {other}"),
            ))
        }
    };
    Some(result_response(id, result))
}

/// Resolve and run a `tools/call`. A tool-level failure (bad args, unknown tool,
/// engine error) is a successful response with `isError: true`, distinct from a
/// JSON-RPC protocol error.
fn call_tool(mem: &MemoryEngine, region: &str, req: &Value) -> CallToolResult {
    let params = req.get("params");
    let name = params.and_then(|p| p.get("name")).and_then(Value::as_str);
    let args = params
        .and_then(|p| p.get("arguments"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    match name {
        Some(name) => match registry().get(name) {
            Some(handler) => {
                let ctx = tool::ToolCtx { mem, region };
                match handler.call(&ctx, args) {
                    Ok(value) => {
                        let links = handler.links(&ctx, &value);
                        if links.is_empty() {
                            CallToolResult::ok(value)
                        } else {
                            CallToolResult::ok_with_links(value, links)
                        }
                    }
                    Err(err) => CallToolResult::error(err.message()),
                }
            }
            None => CallToolResult::error(format!("unknown tool: {name}")),
        },
        None => CallToolResult::error("missing tool name".to_string()),
    }
}

fn to_result<T: serde::Serialize>(body: T) -> Value {
    serde_json::to_value(body).expect("result body serializes")
}

#[cfg(test)]
mod tests;
