//! The `LLMClient` trait and its request/response types.
//!
//! Sync and one-shot (no tokio) to match citadel; parallel tool calls fan out via
//! rayon at the loop, not here. Backends are feature-gated; `MockClient` is always built.

pub(crate) mod mock;

pub mod factory;

// HTTP backends are native-only (ureq is blocking std I/O); wasm builds mock only.
#[cfg(all(not(target_arch = "wasm32"), feature = "claude"))]
pub(crate) mod claude;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai")
))]
mod http;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai")
))]
pub use http::LlmTimeouts;
#[cfg(all(not(target_arch = "wasm32"), feature = "ollama"))]
pub(crate) mod ollama;
#[cfg(all(not(target_arch = "wasm32"), feature = "openai"))]
pub(crate) mod openai;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai")
))]
mod pricing;

use serde_json::{json, Value};

#[derive(Debug, thiserror::Error)]
pub enum LlmError {
    /// A backend failure that is not HTTP/transport: mock exhaustion, replay
    /// miss, or a malformed provider response body.
    #[error("llm backend error: {0}")]
    Backend(String),
    /// A non-2xx HTTP status from a provider. `retry_after` is the parsed
    /// `Retry-After` header (seconds), when the provider supplied one.
    #[error("llm http {status}: {message}")]
    Http {
        status: u16,
        retry_after: Option<u64>,
        message: String,
    },
    /// A connect/timeout/DNS failure that occurred before any status arrived.
    #[error("llm transport error: {0}")]
    Transport(String),
}

impl LlmError {
    /// Whether retrying might succeed: 429/5xx/transport are transient; other 4xx,
    /// malformed bodies, and mock/replay misses are terminal.
    pub fn is_retryable(&self) -> bool {
        match self {
            LlmError::Http { status, .. } => *status == 429 || (500..600).contains(status),
            LlmError::Transport(_) => true,
            LlmError::Backend(_) => false,
        }
    }

    /// The server-requested retry delay in seconds, if the error carried one.
    pub(crate) fn retry_after_secs(&self) -> Option<u64> {
        match self {
            LlmError::Http { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Message {
    System(String),
    User(String),
    Assistant(AssistantMessage),
    /// Output of a tool call, keyed by the originating [`ToolCall::id`].
    Tool {
        call_id: String,
        content: String,
        /// True if the tool call failed; lets a faithful backend mark the
        /// provider tool-result as an error (e.g. Anthropic `is_error`).
        is_error: bool,
    },
}

impl Message {
    pub fn system(text: impl Into<String>) -> Self {
        Message::System(text.into())
    }
    pub fn user(text: impl Into<String>) -> Self {
        Message::User(text.into())
    }
    pub fn tool(call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Message::Tool {
            call_id: call_id.into(),
            content: content.into(),
            is_error: false,
        }
    }
}

/// An assistant turn: text plus requested tool calls.
#[derive(Debug, Clone, Default)]
pub struct AssistantMessage {
    pub content: String,
    pub tool_calls: Vec<ToolCall>,
}

/// A function-schema tool the model may call. `input_schema` is raw JSON Schema
/// (the format every provider accepts).
#[derive(Debug, Clone)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

/// A model's request to invoke a tool. `arguments` is raw JSON, NOT schema-validated
/// before dispatch - each [`Tool::call`] validates and coerces its own.
#[derive(Debug, Clone)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

/// How the model may use the offered tools. Mapped per backend in `to_wire` and
/// folded into `canonical_json`, so the replay key stays deterministic.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ToolChoice {
    /// Model decides (the provider default when tools are present).
    #[default]
    Auto,
    /// Must call some tool; may emit several (parallel) tool calls.
    Any,
    /// Must call exactly this named tool.
    Tool(String),
}

/// Reasoning-spend cap for adaptive-thinking models (Anthropic
/// `output_config.effort`). Without it a thinking model may spend the whole
/// `max_tokens` budget reasoning and emit no text at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effort {
    Low,
    Medium,
    High,
    Max,
}

impl Effort {
    pub fn as_str(self) -> &'static str {
        match self {
            Effort::Low => "low",
            Effort::Medium => "medium",
            Effort::High => "high",
            Effort::Max => "max",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CompletionRequest {
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
    pub tool_choice: ToolChoice,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    /// Reasoning-spend cap; omitted from the wire when `None`. Only set it for
    /// models that accept `effort` (it 400s on e.g. Sonnet 4.5 / Haiku 4.5).
    pub effort: Option<Effort>,
    /// JSON Schema the reply must satisfy via provider structured outputs,
    /// guaranteeing the first content block is text with valid JSON.
    pub output_schema: Option<Value>,
    pub stop: Vec<String>,
}

impl CompletionRequest {
    pub fn new(messages: Vec<Message>) -> Self {
        Self {
            messages,
            ..Default::default()
        }
    }

    pub fn with_tools(mut self, tools: Vec<ToolSpec>) -> Self {
        self.tools = tools;
        self
    }

    /// Constrain tool use for this request (default [`ToolChoice::Auto`]).
    pub fn with_tool_choice(mut self, choice: ToolChoice) -> Self {
        self.tool_choice = choice;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishReason {
    Stop,
    Length,
    ToolUse,
    Error,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TokenUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    /// Set by backends with known pricing; `None` for local models.
    pub cost_usd: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct CompletionResponse {
    pub message: AssistantMessage,
    pub usage: TokenUsage,
    pub finish_reason: FinishReason,
}

impl CompletionResponse {
    /// A plain text reply with no tool calls.
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            message: AssistantMessage {
                content: content.into(),
                tool_calls: Vec::new(),
            },
            usage: TokenUsage::default(),
            finish_reason: FinishReason::Stop,
        }
    }

    /// A reply that requests one or more tool calls.
    pub fn tool_calls(calls: Vec<ToolCall>) -> Self {
        Self {
            message: AssistantMessage {
                content: String::new(),
                tool_calls: calls,
            },
            usage: TokenUsage::default(),
            finish_reason: FinishReason::ToolUse,
        }
    }
}

/// One-shot completion backend. Sync to match citadel; implement the three
/// methods and plug in via `Arc<dyn LLMClient>`.
pub trait LLMClient: Send + Sync {
    fn complete(&self, req: &CompletionRequest) -> Result<CompletionResponse, LlmError>;

    /// Identifies which model produced a response (for trace logs).
    fn model_id(&self) -> &str;

    /// Best-effort token count, used for pre-call budget checks. Local backends
    /// count exactly; HTTP backends may approximate.
    fn count_tokens(&self, messages: &[Message]) -> usize;
}

/// Deterministic JSON encoding of a request - the replay cache key. Messages keep
/// their order (semantic); tools are sorted by name; every field is present.
pub fn canonical_json(req: &CompletionRequest) -> String {
    let mut tools: Vec<&ToolSpec> = req.tools.iter().collect();
    tools.sort_by(|a, b| a.name.cmp(&b.name));
    let value = json!({
        "messages": req.messages.iter().map(message_to_value).collect::<Vec<_>>(),
        "tools": tools.iter().map(|t| tool_spec_to_value(t)).collect::<Vec<_>>(),
        "tool_choice": tool_choice_to_value(&req.tool_choice),
        "max_tokens": req.max_tokens,
        "temperature": req.temperature,
        "effort": req.effort.map(Effort::as_str),
        "output_schema": req.output_schema,
        "stop": req.stop,
    });
    serde_json::to_string(&value).unwrap_or_default()
}

/// BLAKE3 over `model_id` + the canonical request. The replay cache key.
pub fn request_hash(model_id: &str, req: &CompletionRequest) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(model_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(canonical_json(req).as_bytes());
    hasher.finalize().to_hex().to_string()
}

fn message_to_value(m: &Message) -> Value {
    match m {
        Message::System(s) => json!({ "role": "system", "content": s }),
        Message::User(s) => json!({ "role": "user", "content": s }),
        Message::Assistant(am) => json!({
            "role": "assistant",
            "content": am.content,
            "tool_calls": am.tool_calls.iter().map(tool_call_to_value).collect::<Vec<_>>(),
        }),
        Message::Tool {
            call_id,
            content,
            is_error,
        } => json!({
            "role": "tool",
            "call_id": call_id,
            "content": content,
            "is_error": is_error,
        }),
    }
}

fn tool_call_to_value(c: &ToolCall) -> Value {
    json!({ "id": c.id, "name": c.name, "arguments": c.arguments })
}

fn tool_spec_to_value(t: &ToolSpec) -> Value {
    json!({ "name": t.name, "description": t.description, "input_schema": t.input_schema })
}

fn tool_choice_to_value(tc: &ToolChoice) -> Value {
    match tc {
        ToolChoice::Auto => json!("auto"),
        ToolChoice::Any => json!("any"),
        ToolChoice::Tool(name) => json!({ "type": "tool", "name": name }),
    }
}

#[cfg(test)]
mod canonical_tests {
    use super::*;

    fn spec(name: &str) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: "d".into(),
            input_schema: json!({}),
        }
    }

    #[test]
    fn tool_order_does_not_change_hash() {
        let base = || CompletionRequest::new(vec![Message::system("s"), Message::user("u")]);
        let r1 = base().with_tools(vec![spec("alpha"), spec("beta")]);
        let r2 = base().with_tools(vec![spec("beta"), spec("alpha")]);
        assert_eq!(
            request_hash("m", &r1),
            request_hash("m", &r2),
            "tools are sorted by name before hashing"
        );
    }

    #[test]
    fn message_order_changes_hash() {
        let r1 = CompletionRequest::new(vec![Message::system("s"), Message::user("u")]);
        let r2 = CompletionRequest::new(vec![Message::user("u"), Message::system("s")]);
        assert_ne!(
            request_hash("m", &r1),
            request_hash("m", &r2),
            "message order is semantic"
        );
    }

    #[test]
    fn content_and_model_are_part_of_the_key() {
        let r1 = CompletionRequest::new(vec![Message::user("hello")]);
        let r2 = CompletionRequest::new(vec![Message::user("world")]);
        assert_ne!(request_hash("m", &r1), request_hash("m", &r2));
        assert_ne!(
            request_hash("m1", &r1),
            request_hash("m2", &r1),
            "model_id is part of the key"
        );
    }

    #[test]
    fn effort_and_output_schema_are_part_of_the_key() {
        let base = || CompletionRequest::new(vec![Message::user("u")]);
        let with_effort = CompletionRequest {
            effort: Some(Effort::Low),
            ..base()
        };
        let with_schema = CompletionRequest {
            output_schema: Some(json!({ "type": "array" })),
            ..base()
        };
        assert_ne!(request_hash("m", &base()), request_hash("m", &with_effort));
        assert_ne!(request_hash("m", &base()), request_hash("m", &with_schema));
    }

    #[test]
    fn tool_choice_is_part_of_the_key() {
        let base = || CompletionRequest::new(vec![Message::user("u")]).with_tools(vec![spec("t")]);
        assert_ne!(
            request_hash("m", &base()),
            request_hash("m", &base().with_tool_choice(ToolChoice::Any)),
            "tool_choice changes the replay key"
        );
        assert_eq!(
            request_hash("m", &base().with_tool_choice(ToolChoice::Any)),
            request_hash("m", &base().with_tool_choice(ToolChoice::Any)),
            "the same choice hashes identically (deterministic replay)"
        );
    }
}

#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn classifies_retryable_errors() {
        let http = |status| LlmError::Http {
            status,
            retry_after: None,
            message: String::new(),
        };
        assert!(http(429).is_retryable(), "rate limit");
        assert!(http(503).is_retryable(), "server error");
        assert!(http(500).is_retryable());
        assert!(!http(400).is_retryable(), "client error is terminal");
        assert!(!http(401).is_retryable());
        assert!(LlmError::Transport("dns".into()).is_retryable());
        assert!(!LlmError::Backend("mock drained".into()).is_retryable());
    }

    #[test]
    fn retry_after_is_read_only_from_http() {
        let with = LlmError::Http {
            status: 429,
            retry_after: Some(7),
            message: String::new(),
        };
        assert_eq!(with.retry_after_secs(), Some(7));
        assert_eq!(LlmError::Transport("x".into()).retry_after_secs(), None);
    }
}
