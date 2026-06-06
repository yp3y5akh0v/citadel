//! JSON-RPC 2.0 mechanics (error codes, version negotiation, response envelope,
//! parsing). Transport-agnostic: knows nothing about stdio, MCP methods, or citadel.

use serde::Serialize;
use serde_json::Value;

/// JSON-RPC 2.0 error codes used for protocol-level failures.
pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;
/// MCP "resource not found" (server-defined error range).
pub const RESOURCE_NOT_FOUND: i64 = -32002;

/// MCP protocol revisions this server can speak, newest first.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18"];

/// Echo the requested version if supported, else our latest. Per MCP, a client that
/// can't accept the offer disconnects - it is not an error response.
pub fn negotiate_protocol_version(requested: Option<&str>) -> &'static str {
    if let Some(v) = requested {
        if let Some(&supported) = SUPPORTED_PROTOCOL_VERSIONS.iter().find(|&&s| s == v) {
            return supported;
        }
    }
    SUPPORTED_PROTOCOL_VERSIONS[0]
}

#[derive(Serialize)]
struct RpcError {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

/// Exactly one of `result`/`error` is set; `skip_serializing_if` yields the two wire shapes.
#[derive(Serialize)]
struct Response {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

pub fn result_response(id: Value, result: Value) -> Value {
    to_value(Response {
        jsonrpc: "2.0",
        id,
        result: Some(result),
        error: None,
    })
}

pub fn error_response(id: Value, code: i64, message: &str) -> Value {
    to_value(Response {
        jsonrpc: "2.0",
        id,
        result: None,
        error: Some(RpcError {
            code,
            message: message.to_string(),
            data: None,
        }),
    })
}

/// Parse one line; invalid JSON yields a parse-error response (null id) to send back.
pub fn parse_message(line: &str) -> Result<Value, Value> {
    serde_json::from_str::<Value>(line)
        .map_err(|_| error_response(Value::Null, PARSE_ERROR, "parse error: invalid JSON"))
}

/// These response structs are total and always serialize; surface a bug loudly.
fn to_value<T: Serialize>(response: T) -> Value {
    serde_json::to_value(response).expect("JSON-RPC response serializes")
}
