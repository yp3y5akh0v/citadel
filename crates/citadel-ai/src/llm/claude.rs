//! Anthropic Messages API backend (native-only, `claude` feature).
//!
//! The wire shape differs from OpenAI: the system prompt is a top-level field (not
//! a role), tool results are `tool_result` blocks in a user turn, and tool-call
//! `input` is a JSON object both ways (no string round-trip).

use serde_json::{json, Value};
use ureq::Agent;

use super::http::{agent, estimate_tokens, post_json};
use super::pricing;
use super::{
    AssistantMessage, CompletionRequest, CompletionResponse, FinishReason, LLMClient, LlmError,
    Message, TokenUsage, ToolCall, ToolChoice,
};

const API_URL: &str = "https://api.anthropic.com/v1/messages";
const API_VERSION: &str = "2023-06-01";
/// Anthropic requires `max_tokens`; used when the request leaves it unset.
const DEFAULT_MAX_TOKENS: u32 = 4096;

/// Calls api.anthropic.com. The API key is held only in memory and never
/// logged or persisted.
pub(crate) struct ClaudeClient {
    model: String,
    api_key: String,
    agent: Agent,
}

impl ClaudeClient {
    pub(crate) fn new(model: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            api_key: api_key.into(),
            agent: agent(),
        }
    }
}

impl LLMClient for ClaudeClient {
    fn complete(&self, req: &CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let body = to_wire(req, &self.model);
        let headers = [
            ("x-api-key", self.api_key.as_str()),
            ("anthropic-version", API_VERSION),
            ("content-type", "application/json"),
        ];
        let resp = post_json(&self.agent, API_URL, &headers, &body)?;
        from_wire(&resp, &self.model)
    }

    fn model_id(&self) -> &str {
        &self.model
    }

    fn count_tokens(&self, messages: &[Message]) -> usize {
        estimate_tokens(messages)
    }
}

/// Opus 4.7+ reject any non-default `temperature`/`top_p`/`top_k` with a 400 (per the
/// migration guide); omit them for those models. Sonnet/Haiku/Opus<=4.6 accept temperature.
fn rejects_sampling_params(model: &str) -> bool {
    model
        .strip_prefix("claude-opus-4-")
        .and_then(|rest| rest.split('-').next())
        .and_then(|minor| minor.parse::<u32>().ok())
        .is_some_and(|minor| minor >= 7)
}

fn to_wire(req: &CompletionRequest, model: &str) -> Value {
    let mut system = String::new();
    let mut messages: Vec<Value> = Vec::new();
    for m in &req.messages {
        match m {
            Message::System(s) => {
                if !system.is_empty() {
                    system.push('\n');
                }
                system.push_str(s);
            }
            Message::User(s) => messages.push(json!({ "role": "user", "content": s })),
            Message::Assistant(am) => {
                let mut content: Vec<Value> = Vec::new();
                if !am.content.is_empty() {
                    content.push(json!({ "type": "text", "text": am.content }));
                }
                for c in &am.tool_calls {
                    content.push(json!({
                        "type": "tool_use",
                        "id": c.id,
                        "name": c.name,
                        "input": c.arguments,
                    }));
                }
                messages.push(json!({ "role": "assistant", "content": content }));
            }
            Message::Tool {
                call_id,
                content,
                is_error,
            } => messages.push(json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "content": content,
                    "is_error": is_error,
                }],
            })),
        }
    }

    let mut body = json!({
        "model": model,
        "max_tokens": req.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
        "messages": messages,
    });
    let obj = body.as_object_mut().expect("json object literal");
    if !system.is_empty() {
        obj.insert("system".to_string(), Value::String(system));
    }
    if !req.tools.is_empty() {
        let tools: Vec<Value> = req
            .tools
            .iter()
            .map(|t| {
                json!({
                    "name": t.name,
                    "description": t.description,
                    "input_schema": t.input_schema,
                })
            })
            .collect();
        obj.insert("tools".to_string(), Value::Array(tools));
        // Only meaningful alongside tools; Auto is the provider default (omit it).
        match &req.tool_choice {
            ToolChoice::Auto => {}
            ToolChoice::Any => {
                obj.insert("tool_choice".to_string(), json!({ "type": "any" }));
            }
            ToolChoice::Tool(name) => {
                obj.insert(
                    "tool_choice".to_string(),
                    json!({ "type": "tool", "name": name }),
                );
            }
        }
    }
    // Omit temperature for models that 400 on it (see rejects_sampling_params).
    if let Some(t) = req.temperature {
        if !rejects_sampling_params(model) {
            obj.insert("temperature".to_string(), json!(t));
        }
    }
    if !req.stop.is_empty() {
        obj.insert("stop_sequences".to_string(), json!(req.stop));
    }
    body
}

fn from_wire(resp: &Value, model: &str) -> Result<CompletionResponse, LlmError> {
    let content = resp
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| LlmError::Backend("anthropic: missing content array".into()))?;

    let mut text = String::new();
    let mut tool_calls: Vec<ToolCall> = Vec::new();
    for block in content {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(t) = block.get("text").and_then(Value::as_str) {
                    text.push_str(t);
                }
            }
            Some("tool_use") => tool_calls.push(ToolCall {
                id: str_field(block, "id"),
                name: str_field(block, "name"),
                arguments: block.get("input").cloned().unwrap_or(Value::Null),
            }),
            _ => {}
        }
    }

    let finish_reason = match resp.get("stop_reason").and_then(Value::as_str) {
        Some("max_tokens") => FinishReason::Length,
        Some("tool_use") => FinishReason::ToolUse,
        _ => FinishReason::Stop,
    };

    Ok(CompletionResponse {
        message: AssistantMessage {
            content: text,
            tool_calls,
        },
        usage: parse_usage(resp.get("usage"), model),
        finish_reason,
    })
}

fn str_field(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn parse_usage(raw: Option<&Value>, model: &str) -> TokenUsage {
    let field = |name: &str| -> u32 {
        raw.and_then(|v| v.get(name))
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .min(u64::from(u32::MAX)) as u32
    };
    // input_tokens excludes cached tokens; sum them so budget accounting
    // reflects the true input cost.
    let input_tokens = field("input_tokens")
        .saturating_add(field("cache_read_input_tokens"))
        .saturating_add(field("cache_creation_input_tokens"));
    let mut usage = TokenUsage {
        input_tokens,
        output_tokens: field("output_tokens"),
        cost_usd: None,
    };
    usage.cost_usd = pricing::cost_for(model, &usage);
    usage
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::ToolSpec;

    fn spec() -> ToolSpec {
        ToolSpec {
            name: "search".into(),
            description: "find".into(),
            input_schema: json!({ "type": "object" }),
        }
    }

    #[test]
    fn system_is_hoisted_and_max_tokens_defaulted() {
        let req = CompletionRequest::new(vec![Message::system("be terse"), Message::user("hi")]);
        let w = to_wire(&req, "claude-opus-4-8");
        assert_eq!(w["system"], json!("be terse"));
        assert_eq!(w["max_tokens"], json!(DEFAULT_MAX_TOKENS));
        let msgs = w["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1, "system is not a message");
        assert_eq!(msgs[0]["role"], json!("user"));
    }

    #[test]
    fn temperature_omitted_only_for_opus_4_7_plus() {
        assert!(rejects_sampling_params("claude-opus-4-7"));
        assert!(rejects_sampling_params("claude-opus-4-8"));
        assert!(rejects_sampling_params("claude-opus-4-8-20260101"));
        assert!(!rejects_sampling_params("claude-opus-4-6"));
        assert!(!rejects_sampling_params("claude-sonnet-4-6"));
        assert!(!rejects_sampling_params("claude-haiku-4-5"));

        let req = CompletionRequest {
            temperature: Some(0.9),
            ..CompletionRequest::new(vec![Message::user("x")])
        };
        assert!(
            to_wire(&req, "claude-opus-4-8")
                .get("temperature")
                .is_none(),
            "Opus 4.8 would 400 on a non-default temperature"
        );
        assert_eq!(
            to_wire(&req, "claude-sonnet-4-6")["temperature"],
            json!(0.9)
        );
    }

    #[test]
    fn tools_pass_through_as_input_schema() {
        let req = CompletionRequest::new(vec![Message::user("x")]).with_tools(vec![spec()]);
        let w = to_wire(&req, "m");
        let tools = w["tools"].as_array().unwrap();
        assert_eq!(tools[0]["name"], json!("search"));
        assert_eq!(tools[0]["input_schema"], json!({ "type": "object" }));
    }

    #[test]
    fn tool_choice_maps_only_with_tools() {
        let with_tools = CompletionRequest::new(vec![Message::user("x")]).with_tools(vec![spec()]);
        assert!(to_wire(&with_tools, "m").get("tool_choice").is_none());
        assert_eq!(
            to_wire(&with_tools.clone().with_tool_choice(ToolChoice::Any), "m")["tool_choice"],
            json!({ "type": "any" })
        );
        assert_eq!(
            to_wire(
                &with_tools.with_tool_choice(ToolChoice::Tool("search".into())),
                "m"
            )["tool_choice"],
            json!({ "type": "tool", "name": "search" })
        );
        let no_tools =
            CompletionRequest::new(vec![Message::user("x")]).with_tool_choice(ToolChoice::Any);
        assert!(to_wire(&no_tools, "m").get("tool_choice").is_none());
    }

    #[test]
    fn tool_result_is_a_user_block_with_is_error() {
        let req = CompletionRequest::new(vec![Message::Tool {
            call_id: "toolu_1".into(),
            content: "boom".into(),
            is_error: true,
        }]);
        let w = to_wire(&req, "m");
        let block = &w["messages"][0]["content"][0];
        assert_eq!(w["messages"][0]["role"], json!("user"));
        assert_eq!(block["type"], json!("tool_result"));
        assert_eq!(block["tool_use_id"], json!("toolu_1"));
        assert_eq!(block["is_error"], json!(true));
    }

    #[test]
    fn assistant_tool_use_uses_input_object_no_string() {
        let req = CompletionRequest::new(vec![Message::Assistant(AssistantMessage {
            content: String::new(),
            tool_calls: vec![ToolCall {
                id: "toolu_2".into(),
                name: "search".into(),
                arguments: json!({ "q": "rust" }),
            }],
        })]);
        let w = to_wire(&req, "m");
        let block = &w["messages"][0]["content"][0];
        assert_eq!(block["type"], json!("tool_use"));
        assert_eq!(
            block["input"],
            json!({ "q": "rust" }),
            "object, not a string"
        );
        assert_eq!(w["messages"][0]["content"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn from_wire_parses_text_tooluse_and_sums_cached_input() {
        let resp = json!({
            "content": [
                { "type": "text", "text": "thinking" },
                { "type": "tool_use", "id": "toolu_9", "name": "search", "input": { "q": "x" } }
            ],
            "stop_reason": "tool_use",
            "usage": {
                "input_tokens": 10,
                "cache_read_input_tokens": 5,
                "cache_creation_input_tokens": 2,
                "output_tokens": 7
            }
        });
        let r = from_wire(&resp, "claude-opus-4-8").unwrap();
        assert_eq!(r.message.content, "thinking");
        assert_eq!(r.finish_reason, FinishReason::ToolUse);
        assert_eq!(r.message.tool_calls[0].name, "search");
        assert_eq!(r.message.tool_calls[0].arguments, json!({ "q": "x" }));
        assert_eq!(r.usage.input_tokens, 17, "10 + 5 + 2 cached");
        assert_eq!(r.usage.output_tokens, 7);
        assert!(r.usage.cost_usd.is_some(), "known model is priced");
    }

    #[test]
    fn from_wire_maps_stop_reasons() {
        let base = |reason: &str| json!({ "content": [], "stop_reason": reason });
        assert_eq!(
            from_wire(&base("end_turn"), "m").unwrap().finish_reason,
            FinishReason::Stop
        );
        assert_eq!(
            from_wire(&base("max_tokens"), "m").unwrap().finish_reason,
            FinishReason::Length
        );
    }
}
