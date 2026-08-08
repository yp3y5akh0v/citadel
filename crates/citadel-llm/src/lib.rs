//! The `LLMClient` trait and its types; sync (no tokio) to match citadel.

pub(crate) mod mock;

pub mod factory;
#[cfg(any(test, feature = "test-util"))]
pub use factory::testing;

// HTTP backends are native-only (ureq is blocking I/O); wasm builds mock only.
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
mod limits;
#[cfg(all(not(target_arch = "wasm32"), feature = "ollama"))]
pub(crate) mod ollama;
#[cfg(all(not(target_arch = "wasm32"), feature = "openai"))]
pub(crate) mod openai;
mod openai_models;
mod pricing;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// USD per million (input, output) tokens; `None` when unknown, never guessed.
pub fn known_token_rates_usd_per_million(model_id: &str) -> Option<(f64, f64)> {
    pricing::pricing_for(model_id).map(|rate| (rate.input_per_mtok, rate.output_per_mtok))
}

/// Published output-token ceiling; a lower cap silently truncates output.
pub fn known_max_output_tokens(model_id: &str) -> Option<u32> {
    limits::max_output_tokens(model_id)
}

#[derive(Debug, thiserror::Error)]
pub enum LlmError {
    /// No faithful backend mapping; refused pre-dispatch, so no spend.
    #[error("llm request unsupported: {0}")]
    UnsupportedRequest(String),
    /// Not HTTP or transport: mock exhaustion, replay miss, malformed body.
    #[error("llm backend error: {0}")]
    Backend(String),
    /// Non-2xx provider status; `retry_after` is `Retry-After` in seconds.
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
    /// True only for 429, 5xx, and transport errors; all else is terminal.
    pub fn is_retryable(&self) -> bool {
        match self {
            LlmError::Http { status, .. } => *status == 429 || (500..600).contains(status),
            LlmError::Transport(_) => true,
            LlmError::UnsupportedRequest(_) | LlmError::Backend(_) => false,
        }
    }

    /// Whether the client refused locally before any provider dispatch.
    pub fn is_pre_dispatch(&self) -> bool {
        matches!(self, LlmError::UnsupportedRequest(_))
    }

    /// The server-requested retry delay in seconds, if the error carried one.
    pub fn retry_after_secs(&self) -> Option<u64> {
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
        /// True when the call failed; maps to Anthropic's `is_error` flag.
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

/// A tool the model may call; `input_schema` is raw JSON Schema.
#[derive(Debug, Clone)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

/// A model's tool invocation; `arguments` is raw, unvalidated JSON.
#[derive(Debug, Clone)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

/// How the model may use offered tools; folded into the replay key.
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

/// Provider-neutral reasoning cap; unsupported profiles refuse, never drop it.
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
    /// Reasoning cap; omitted from the wire when `None`, refused if unmappable.
    pub effort: Option<Effort>,
    /// Strict reply schema; guarantees JSON text or a pre-dispatch refusal.
    pub output_schema: Option<Value>,
    pub stop: Vec<String>,
    /// Best-effort seed. Temperature 0 alone does not make a reply reproducible.
    pub seed: Option<u64>,
}

impl CompletionRequest {
    pub fn new(messages: Vec<Message>) -> Self {
        Self {
            messages,
            ..Default::default()
        }
    }

    /// Pin the sampling seed (see [`CompletionRequest::seed`]).
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
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
    /// Provider declined but billed; refusal text is in `message.content`.
    Refusal,
    /// Safety filter stopped the response; content and usage still returned.
    ContentFilter,
    Error,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TokenUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    /// Set by backends with known pricing; `None` for local models.
    pub cost_usd: Option<f64>,
}

impl TokenUsage {
    /// Cost is a partial total when only one side is priced; track per call.
    pub fn add(&mut self, other: &TokenUsage) {
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        self.cost_usd = match (self.cost_usd, other.cost_usd) {
            (Some(x), Some(y)) => Some(x + y),
            (x, y) => x.or(y),
        };
    }
}

#[derive(Debug, Clone)]
pub struct CompletionResponse {
    pub message: AssistantMessage,
    pub usage: TokenUsage,
    pub finish_reason: FinishReason,
}

/// Non-secret provider/endpoint/wire identity; the model id is bound by callers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientRequestIdentity {
    provider: String,
    endpoint_sha256: String,
    wire_defaults_sha256: String,
}

impl ClientRequestIdentity {
    pub(crate) fn from_config(
        provider: &str,
        endpoint: &str,
        wire_defaults: &[(&str, &str)],
    ) -> Self {
        fn put_str(h: &mut Sha256, value: &str) {
            h.update((value.len() as u64).to_le_bytes());
            h.update(value.as_bytes());
        }
        fn hex(h: Sha256) -> String {
            h.finalize().iter().map(|b| format!("{b:02x}")).collect()
        }

        let mut endpoint_hash = Sha256::new();
        put_str(&mut endpoint_hash, "citadel-ai-client-endpoint-v1");
        put_str(&mut endpoint_hash, endpoint.trim_end_matches('/'));

        let mut defaults_hash = Sha256::new();
        put_str(&mut defaults_hash, "citadel-ai-client-wire-defaults-v1");
        defaults_hash.update((wire_defaults.len() as u64).to_le_bytes());
        for (name, value) in wire_defaults {
            put_str(&mut defaults_hash, name);
            put_str(&mut defaults_hash, value);
        }
        Self {
            provider: provider.to_string(),
            endpoint_sha256: hex(endpoint_hash),
            wire_defaults_sha256: hex(defaults_hash),
        }
    }

    /// Identity for in-process clients whose only wire is the canonical request.
    pub fn in_process() -> Self {
        Self::from_config(
            "in-process",
            "in-process://llm-client",
            &[("wire", "completion-request-v1")],
        )
    }

    pub fn provider(&self) -> &str {
        &self.provider
    }

    pub fn endpoint_sha256(&self) -> &str {
        &self.endpoint_sha256
    }

    pub fn wire_defaults_sha256(&self) -> &str {
        &self.wire_defaults_sha256
    }

    pub fn is_well_formed(&self) -> bool {
        let hex64 = |value: &str| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        };
        !self.provider.trim().is_empty()
            && hex64(&self.endpoint_sha256)
            && hex64(&self.wire_defaults_sha256)
    }
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

/// Native enforcement available for [`CompletionRequest::output_schema`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputSchemaSupport {
    Unsupported,
    StrictJsonSchema,
}

/// One-shot sync completion backend; plug in via `Arc<dyn LLMClient>`.
pub trait LLMClient: Send + Sync {
    fn complete(&self, req: &CompletionRequest) -> Result<CompletionResponse, LlmError>;

    /// Identifies which model produced a response (for trace logs).
    fn model_id(&self) -> &str;

    /// Non-secret wire identity; HTTP backends override the in-process default.
    fn request_identity(&self) -> ClientRequestIdentity {
        ClientRequestIdentity::in_process()
    }

    /// Whether this profile enforces strict JSON Schema; default unsupported.
    fn output_schema_support(&self) -> OutputSchemaSupport {
        OutputSchemaSupport::Unsupported
    }

    /// Token count for pre-call budgeting; HTTP backends may approximate.
    fn count_tokens(&self, messages: &[Message]) -> usize;
}

/// Deterministic replay-key encoding; message order semantic, tools name-sorted.
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
    fn strict_output_schema_request_hash_is_frozen() {
        let req = CompletionRequest {
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "decision": { "type": "string", "enum": ["facts", "nothing_durable"] },
                    "facts": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["decision", "facts"],
                "additionalProperties": false
            })),
            max_tokens: Some(1500),
            temperature: Some(0.0),
            ..CompletionRequest::new(vec![
                Message::system("extract durable facts"),
                Message::user("session text"),
            ])
        };
        assert_eq!(
            request_hash("gpt-4o-mini-2024-07-18", &req),
            "0c81c2092b7a67417f5c403f9c26fd1f7875746e68934bdfa5e14ebaa9c33f12"
        );
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
        let unsupported = LlmError::UnsupportedRequest("schema".into());
        assert!(!unsupported.is_retryable());
        assert!(unsupported.is_pre_dispatch());
        assert!(!LlmError::Backend("malformed reply".into()).is_pre_dispatch());
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
