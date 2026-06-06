//! MCP wire types this server emits. Serialize-only: requests arrive as
//! `serde_json::Value` (untrusted, multi-revision) and are matched in `server`.

use serde::Serialize;
use serde_json::Value;

/// A `tools/list` definition; `input_schema`/`output_schema` are JSON Schema documents.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Tool {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
    pub annotations: ToolAnnotations,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
}

/// MCP tool behavior hints. Absent hints are omitted (no spec defaults baked in).
#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ToolAnnotations {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only_hint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destructive_hint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotent_hint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_world_hint: Option<bool>,
}

/// A content block in a tool result; the `type` tag drives the wire shape.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Content {
    Text {
        text: String,
    },
    /// An MCP `resource_link`: a dereferenceable URI a client can fetch or cite.
    ResourceLink {
        uri: String,
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
        mime_type: Option<&'static str>,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallToolResult {
    pub content: Vec<Content>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
}

impl CallToolResult {
    /// A successful call: the structured value, plus its JSON text for back-compat.
    pub fn ok(value: Value) -> Self {
        Self {
            content: vec![Content::Text {
                text: value.to_string(),
            }],
            structured_content: Some(value),
            is_error: Some(false),
        }
    }

    /// A successful call plus extra content blocks (e.g. `resource_link`s to the result's
    /// atoms). Identical to [`ok`](Self::ok) when `links` is empty.
    pub fn ok_with_links(value: Value, links: Vec<Content>) -> Self {
        let mut content = vec![Content::Text {
            text: value.to_string(),
        }];
        content.extend(links);
        Self {
            content,
            structured_content: Some(value),
            is_error: Some(false),
        }
    }

    /// A tool-level failure: the message as text, `isError: true`, no structured body.
    pub fn error(message: String) -> Self {
        Self {
            content: vec![Content::Text { text: message }],
            structured_content: None,
            is_error: Some(true),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerInfo {
    pub name: &'static str,
    pub version: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    pub protocol_version: &'static str,
    pub capabilities: Value,
    pub server_info: ServerInfo,
}

/// A `resources/templates/list` entry: a URI template a client can fill in to read
/// a memory as a resource.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceTemplate {
    pub uri_template: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub mime_type: &'static str,
}

/// One item in a `resources/read` result: the resource's text contents.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceContents {
    pub uri: String,
    pub mime_type: &'static str,
    pub text: String,
}
