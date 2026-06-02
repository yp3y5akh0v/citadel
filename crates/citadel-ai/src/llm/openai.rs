//! OpenAI Chat Completions backend (native-only, `openai` feature).
//!
//! Also serves any OpenAI-compatible endpoint via [`OpenAiClient::with_base_url`].
//! Tool-call `arguments` are a JSON string on the wire; stringified out, parsed back in.

use serde_json::{json, Value};
use ureq::Agent;

use super::http::{agent, estimate_tokens, post_json};
use super::pricing;
use super::{
    AssistantMessage, CompletionRequest, CompletionResponse, FinishReason, LLMClient, LlmError,
    Message, TokenUsage, ToolCall, ToolChoice,
};

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
/// OpenAI's modern output-token cap field; `max_tokens` is deprecated there.
const OPENAI_MAX_TOKENS_FIELD: &str = "max_completion_tokens";

/// Calls an OpenAI-compatible `/chat/completions` endpoint. The API key is held
/// only in memory and never logged or persisted.
pub struct OpenAiClient {
    model: String,
    base_url: String,
    api_key: String,
    /// Output-token-cap field: OpenAI wants `max_completion_tokens`, some
    /// compatible servers (Ollama) only honor `max_tokens`.
    max_tokens_field: &'static str,
    /// Whether to price usage from the pricing table (false for free/local).
    priced: bool,
    agent: Agent,
}

impl OpenAiClient {
    /// A client for the official OpenAI API.
    pub fn new(model: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self::with_base_url(model, DEFAULT_BASE_URL, api_key)
    }

    /// A client for any OpenAI-compatible endpoint (Together, OpenRouter, a
    /// local Ollama `/v1`, ...). `base_url` is the path up to but excluding
    /// `/chat/completions`.
    pub fn with_base_url(
        model: impl Into<String>,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            model: model.into(),
            base_url: base_url.into(),
            api_key: api_key.into(),
            max_tokens_field: OPENAI_MAX_TOKENS_FIELD,
            priced: true,
            agent: agent(),
        }
    }

    /// Override the output-token-cap field for a compatible server. Only the
    /// Ollama backend reuses this, so it is gated to its feature to stay free
    /// of dead code in an openai-only build.
    #[cfg(feature = "ollama")]
    pub(super) fn max_tokens_field(mut self, field: &'static str) -> Self {
        self.max_tokens_field = field;
        self
    }

    /// Report no cost (a free/local endpoint). Ollama-only, as above.
    #[cfg(feature = "ollama")]
    pub(super) fn unpriced(mut self) -> Self {
        self.priced = false;
        self
    }
}

impl LLMClient for OpenAiClient {
    fn complete(&self, req: &CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let body = to_wire(req, &self.model, self.max_tokens_field);
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let auth = format!("Bearer {}", self.api_key);
        let headers = [
            ("authorization", auth.as_str()),
            ("content-type", "application/json"),
        ];
        let resp = post_json(&self.agent, &url, &headers, &body)?;
        from_wire(&resp, &self.model, self.priced)
    }

    fn model_id(&self) -> &str {
        &self.model
    }

    fn count_tokens(&self, messages: &[Message]) -> usize {
        estimate_tokens(messages)
    }
}

fn to_wire(req: &CompletionRequest, model: &str, max_tokens_field: &str) -> Value {
    let messages: Vec<Value> = req.messages.iter().map(message_to_wire).collect();
    let mut body = json!({ "model": model, "messages": messages });
    let obj = body.as_object_mut().expect("json object literal");
    if !req.tools.is_empty() {
        let tools: Vec<Value> = req
            .tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.input_schema,
                    },
                })
            })
            .collect();
        obj.insert("tools".to_string(), Value::Array(tools));
        // Only meaningful alongside tools; Auto is the provider default (omit it).
        match &req.tool_choice {
            ToolChoice::Auto => {}
            ToolChoice::Any => {
                obj.insert("tool_choice".to_string(), json!("required"));
            }
            ToolChoice::Tool(name) => {
                obj.insert(
                    "tool_choice".to_string(),
                    json!({ "type": "function", "function": { "name": name } }),
                );
            }
        }
    }
    if let Some(mt) = req.max_tokens {
        obj.insert(max_tokens_field.to_string(), json!(mt));
    }
    if let Some(t) = req.temperature {
        obj.insert("temperature".to_string(), json!(t));
    }
    if !req.stop.is_empty() {
        obj.insert("stop".to_string(), json!(req.stop));
    }
    body
}

fn message_to_wire(m: &Message) -> Value {
    match m {
        Message::System(s) => json!({ "role": "system", "content": s }),
        Message::User(s) => json!({ "role": "user", "content": s }),
        Message::Assistant(am) => {
            let mut msg = serde_json::Map::new();
            msg.insert("role".to_string(), json!("assistant"));
            // content is null when the turn is purely tool calls.
            msg.insert(
                "content".to_string(),
                if am.content.is_empty() {
                    Value::Null
                } else {
                    json!(am.content)
                },
            );
            if !am.tool_calls.is_empty() {
                let calls: Vec<Value> = am
                    .tool_calls
                    .iter()
                    .map(|c| {
                        json!({
                            "id": c.id,
                            "type": "function",
                            "function": {
                                "name": c.name,
                                // OpenAI requires arguments as a JSON string.
                                "arguments": c.arguments.to_string(),
                            },
                        })
                    })
                    .collect();
                msg.insert("tool_calls".to_string(), Value::Array(calls));
            }
            Value::Object(msg)
        }
        Message::Tool {
            call_id, content, ..
        } => json!({
            "role": "tool",
            "tool_call_id": call_id,
            "content": content,
        }),
    }
}

fn from_wire(resp: &Value, model: &str, priced: bool) -> Result<CompletionResponse, LlmError> {
    let choice = resp
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .ok_or_else(|| LlmError::Backend("openai: missing choices".into()))?;
    let message = choice
        .get("message")
        .ok_or_else(|| LlmError::Backend("openai: missing message".into()))?;

    let content = message
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    let mut tool_calls: Vec<ToolCall> = Vec::new();
    if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
        for c in calls {
            let func = c.get("function");
            let args_str = func
                .and_then(|f| f.get("arguments"))
                .and_then(Value::as_str)
                .unwrap_or("{}");
            let arguments = serde_json::from_str(args_str).map_err(|e| {
                LlmError::Backend(format!("openai: tool arguments not valid JSON: {e}"))
            })?;
            tool_calls.push(ToolCall {
                id: c
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                name: func
                    .and_then(|f| f.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                arguments,
            });
        }
    }

    let finish_reason = match choice.get("finish_reason").and_then(Value::as_str) {
        Some("length") => FinishReason::Length,
        Some("tool_calls") => FinishReason::ToolUse,
        _ => FinishReason::Stop,
    };

    Ok(CompletionResponse {
        message: AssistantMessage {
            content,
            tool_calls,
        },
        usage: parse_usage(resp.get("usage"), model, priced),
        finish_reason,
    })
}

fn parse_usage(raw: Option<&Value>, model: &str, priced: bool) -> TokenUsage {
    let field = |name: &str| -> u32 {
        raw.and_then(|v| v.get(name))
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .min(u64::from(u32::MAX)) as u32
    };
    let mut usage = TokenUsage {
        input_tokens: field("prompt_tokens"),
        output_tokens: field("completion_tokens"),
        cost_usd: None,
    };
    if priced {
        usage.cost_usd = pricing::cost_for(model, &usage);
    }
    usage
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::ToolSpec;

    #[test]
    fn system_is_first_message_and_tools_are_wrapped() {
        let req = CompletionRequest::new(vec![Message::system("sys"), Message::user("u")])
            .with_tools(vec![ToolSpec {
                name: "search".into(),
                description: "find".into(),
                input_schema: json!({ "type": "object" }),
            }]);
        let w = to_wire(&req, "gpt", OPENAI_MAX_TOKENS_FIELD);
        assert_eq!(w["messages"][0]["role"], json!("system"));
        assert_eq!(w["tools"][0]["type"], json!("function"));
        assert_eq!(
            w["tools"][0]["function"]["parameters"],
            json!({ "type": "object" })
        );
    }

    #[test]
    fn tool_choice_maps_only_with_tools() {
        let spec = ToolSpec {
            name: "search".into(),
            description: "find".into(),
            input_schema: json!({ "type": "object" }),
        };
        let with_tools =
            CompletionRequest::new(vec![Message::user("u")]).with_tools(vec![spec.clone()]);
        let wire = |r: &CompletionRequest| to_wire(r, "gpt", OPENAI_MAX_TOKENS_FIELD);
        // Auto: omitted.
        assert!(wire(&with_tools).get("tool_choice").is_none());
        // Any -> "required".
        assert_eq!(
            wire(&with_tools.clone().with_tool_choice(ToolChoice::Any))["tool_choice"],
            json!("required")
        );
        // A forced tool -> {"type":"function","function":{"name":...}}.
        assert_eq!(
            wire(&with_tools.with_tool_choice(ToolChoice::Tool("search".into())))["tool_choice"],
            json!({ "type": "function", "function": { "name": "search" } })
        );
        // No tools -> omitted.
        let no_tools =
            CompletionRequest::new(vec![Message::user("u")]).with_tool_choice(ToolChoice::Any);
        assert!(wire(&no_tools).get("tool_choice").is_none());
    }

    #[test]
    fn assistant_tool_call_arguments_are_a_string() {
        let req = CompletionRequest::new(vec![Message::Assistant(AssistantMessage {
            content: String::new(),
            tool_calls: vec![ToolCall {
                id: "call_1".into(),
                name: "search".into(),
                arguments: json!({ "q": "rust" }),
            }],
        })]);
        let w = to_wire(&req, "gpt", OPENAI_MAX_TOKENS_FIELD);
        let msg = &w["messages"][0];
        assert_eq!(
            msg["content"],
            Value::Null,
            "pure tool-call turn has null content"
        );
        let args = &msg["tool_calls"][0]["function"]["arguments"];
        assert_eq!(
            args,
            &json!("{\"q\":\"rust\"}"),
            "arguments serialized to a string"
        );
    }

    #[test]
    fn tool_result_is_a_tool_role_with_call_id() {
        let req = CompletionRequest::new(vec![Message::tool("call_1", "result text")]);
        let w = to_wire(&req, "gpt", OPENAI_MAX_TOKENS_FIELD);
        assert_eq!(w["messages"][0]["role"], json!("tool"));
        assert_eq!(w["messages"][0]["tool_call_id"], json!("call_1"));
    }

    #[test]
    fn max_tokens_field_is_configurable() {
        let req = CompletionRequest {
            max_tokens: Some(256),
            ..CompletionRequest::new(vec![Message::user("u")])
        };
        let openai = to_wire(&req, "gpt", OPENAI_MAX_TOKENS_FIELD);
        assert_eq!(openai["max_completion_tokens"], json!(256));
        assert!(openai.get("max_tokens").is_none());
        let ollama = to_wire(&req, "qwen", "max_tokens");
        assert_eq!(ollama["max_tokens"], json!(256));
        assert!(ollama.get("max_completion_tokens").is_none());
    }

    #[test]
    fn from_wire_parses_tool_calls_and_usage() {
        let resp = json!({
            "choices": [{
                "message": {
                    "content": null,
                    "tool_calls": [{
                        "id": "call_9",
                        "type": "function",
                        "function": { "name": "search", "arguments": "{\"q\":\"x\"}" }
                    }]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": { "prompt_tokens": 12, "completion_tokens": 4 }
        });
        let r = from_wire(&resp, "gpt", true).unwrap();
        assert_eq!(r.finish_reason, FinishReason::ToolUse);
        assert_eq!(r.message.content, "");
        assert_eq!(
            r.message.tool_calls[0].arguments,
            json!({ "q": "x" }),
            "string parsed back to object"
        );
        assert_eq!(r.usage.input_tokens, 12);
        assert_eq!(r.usage.output_tokens, 4);
        assert_eq!(r.usage.cost_usd, None, "gpt is not in the pricing table");
    }

    #[test]
    fn from_wire_plain_text_reply() {
        let resp = json!({
            "choices": [{ "message": { "content": "hello" }, "finish_reason": "stop" }],
            "usage": { "prompt_tokens": 3, "completion_tokens": 1 }
        });
        let r = from_wire(&resp, "gpt", true).unwrap();
        assert_eq!(r.message.content, "hello");
        assert!(r.message.tool_calls.is_empty());
        assert_eq!(r.finish_reason, FinishReason::Stop);
    }
}
