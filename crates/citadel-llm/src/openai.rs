//! OpenAI Chat Completions and compatible endpoints; tool args are a JSON string.

use serde_json::{json, Value};
use ureq::Agent;

use super::http::{agent, estimate_tokens, post_json, LlmTimeouts};
use super::openai_models;
use super::pricing;
use super::{
    AssistantMessage, ClientRequestIdentity, CompletionRequest, CompletionResponse, Effort,
    FinishReason, LLMClient, LlmError, Message, OutputSchemaSupport, TokenUsage, ToolCall,
    ToolChoice,
};

pub(super) const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
/// OpenAI's modern output-token cap field; `max_tokens` is deprecated there.
pub(super) const OPENAI_MAX_TOKENS_FIELD: &str = "max_completion_tokens";
/// Historical profile; preserving it keeps `effort: None` cache identities valid.
pub(super) const LEGACY_CHAT_COMPLETIONS_WIRE_REVISION: &str = "openai-chat-completions-v2";
/// Profile that additionally admits request-level GPT-5 reasoning effort.
pub(super) const REQUEST_EFFORT_CHAT_COMPLETIONS_WIRE_REVISION: &str = "openai-chat-completions-v3";
pub(super) const STRUCTURED_OUTPUTS_REVISION: &str = "strict-json-schema-v1";
pub(super) const UNSUPPORTED_STRUCTURED_OUTPUTS_REVISION: &str = "unsupported";
const STRUCTURED_OUTPUT_NAME: &str = "citadel_structured_output";

/// Effort capability: compat and non-reasoning models must not get reasoning_effort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RequestEffortSupport {
    Unsupported,
    /// GPT-5 values; minimal unrepresentable, max unsupported by this generation.
    LowMediumHigh,
}

impl RequestEffortSupport {
    pub(super) fn identity(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::LowMediumHigh => "reasoning-effort-low-medium-high-v1",
        }
    }
}

/// Closed model capability lookup for native OpenAI clients.
pub(super) fn request_effort_support_for_model(model: &str) -> RequestEffortSupport {
    if openai_models::gpt5_model(model).is_some() {
        RequestEffortSupport::LowMediumHigh
    } else {
        RequestEffortSupport::Unsupported
    }
}

/// Non-secret client identity: only request-effort profiles use v3, others v2.
pub(super) fn client_request_identity(
    identity_provider: &str,
    base_url: &str,
    max_tokens_field: &str,
    fixed_reasoning_effort: Option<&str>,
    request_effort_support: RequestEffortSupport,
    output_schema_support: OutputSchemaSupport,
) -> ClientRequestIdentity {
    let structured_outputs = match output_schema_support {
        OutputSchemaSupport::Unsupported => UNSUPPORTED_STRUCTURED_OUTPUTS_REVISION,
        OutputSchemaSupport::StrictJsonSchema => STRUCTURED_OUTPUTS_REVISION,
    };
    let fixed_reasoning_effort = fixed_reasoning_effort.unwrap_or("<none>");
    match request_effort_support {
        RequestEffortSupport::Unsupported => ClientRequestIdentity::from_config(
            identity_provider,
            base_url,
            &[
                ("wire", LEGACY_CHAT_COMPLETIONS_WIRE_REVISION),
                ("max_tokens_field", max_tokens_field),
                ("reasoning_effort", fixed_reasoning_effort),
                ("structured_outputs", structured_outputs),
            ],
        ),
        RequestEffortSupport::LowMediumHigh => ClientRequestIdentity::from_config(
            identity_provider,
            base_url,
            &[
                ("wire", REQUEST_EFFORT_CHAT_COMPLETIONS_WIRE_REVISION),
                ("max_tokens_field", max_tokens_field),
                ("reasoning_effort", fixed_reasoning_effort),
                ("request_effort", request_effort_support.identity()),
                ("structured_outputs", structured_outputs),
            ],
        ),
    }
}

/// Calls an OpenAI-compatible endpoint; the API key is never logged or persisted.
pub(crate) struct OpenAiClient {
    model: String,
    base_url: String,
    api_key: String,
    /// Output-cap field: some compatible servers (Ollama) honor only `max_tokens`.
    max_tokens_field: &'static str,
    /// Client-fixed `reasoning_effort`, distinct from request-level effort.
    fixed_reasoning_effort: Option<String>,
    request_effort_support: RequestEffortSupport,
    identity_provider: &'static str,
    output_schema_support: OutputSchemaSupport,
    /// Whether to price usage from the pricing table (false for free/local).
    priced: bool,
    agent: Agent,
}

impl OpenAiClient {
    /// Any OpenAI-compatible endpoint; `base_url` excludes `/chat/completions`.
    pub(crate) fn with_base_url(
        model: impl Into<String>,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        output_schema_support: OutputSchemaSupport,
        request_effort_support: RequestEffortSupport,
    ) -> Self {
        Self {
            model: model.into(),
            base_url: base_url.into(),
            api_key: api_key.into(),
            max_tokens_field: OPENAI_MAX_TOKENS_FIELD,
            fixed_reasoning_effort: None,
            request_effort_support,
            identity_provider: "openai",
            output_schema_support,
            priced: true,
            agent: agent(&LlmTimeouts::default()),
        }
    }

    /// Replace the default HTTP deadlines.
    pub(crate) fn with_timeouts(mut self, timeouts: LlmTimeouts) -> Self {
        self.agent = agent(&timeouts);
        self
    }

    /// Override the cap field: Ollama and Gemini compat use `max_tokens`.
    #[cfg(any(feature = "ollama", feature = "gemini"))]
    pub(super) fn max_tokens_field(mut self, field: &'static str) -> Self {
        self.max_tokens_field = field;
        self
    }

    /// Set `reasoning_effort` (low|medium|high) for backends that support it.
    #[cfg(feature = "gemini")]
    pub(super) fn reasoning_effort(mut self, effort: impl Into<String>) -> Self {
        self.fixed_reasoning_effort = Some(effort.into());
        self
    }

    pub(super) fn identity_provider(mut self, provider: &'static str) -> Self {
        self.identity_provider = provider;
        self
    }

    /// Report no cost (a free/local endpoint).
    #[cfg(feature = "ollama")]
    pub(super) fn unpriced(mut self) -> Self {
        self.priced = false;
        self
    }
}

impl LLMClient for OpenAiClient {
    fn complete(&self, req: &CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let body = to_wire(
            req,
            &self.model,
            self.max_tokens_field,
            self.fixed_reasoning_effort.as_deref(),
            self.request_effort_support,
            self.output_schema_support,
            self.identity_provider,
        )?;
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let auth = format!("Bearer {}", self.api_key);
        let headers = [
            ("authorization", auth.as_str()),
            ("content-type", "application/json"),
        ];
        let resp = post_json(&self.agent, &url, &headers, &body)?;
        // Forced choice + offered names let from_wire recover a leaked call.
        let forced_tool = !matches!(req.tool_choice, ToolChoice::Auto);
        let tool_names: Vec<&str> = req.tools.iter().map(|t| t.name.as_str()).collect();
        from_wire(&resp, &self.model, self.priced, forced_tool, &tool_names)
    }

    fn model_id(&self) -> &str {
        &self.model
    }

    fn request_identity(&self) -> ClientRequestIdentity {
        client_request_identity(
            self.identity_provider,
            &self.base_url,
            self.max_tokens_field,
            self.fixed_reasoning_effort.as_deref(),
            self.request_effort_support,
            self.output_schema_support,
        )
    }

    fn output_schema_support(&self) -> OutputSchemaSupport {
        self.output_schema_support
    }

    fn count_tokens(&self, messages: &[Message]) -> usize {
        estimate_tokens(messages)
    }
}

fn to_wire(
    req: &CompletionRequest,
    model: &str,
    max_tokens_field: &str,
    fixed_reasoning_effort: Option<&str>,
    request_effort_support: RequestEffortSupport,
    output_schema_support: OutputSchemaSupport,
    identity_provider: &str,
) -> Result<Value, LlmError> {
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
        // Meaningful only with tools; Auto is the provider default (omit it).
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
    // OpenAI honors a seed only within one backend build (system_fingerprint).
    if let Some(s) = req.seed {
        obj.insert("seed".to_string(), json!(s));
    }
    if !req.stop.is_empty() {
        obj.insert("stop".to_string(), json!(req.stop));
    }
    let request_reasoning_effort = match (req.effort, request_effort_support) {
        (None, _) => None,
        (
            Some(Effort::Low | Effort::Medium | Effort::High),
            RequestEffortSupport::LowMediumHigh,
        ) => req.effort.map(Effort::as_str),
        (Some(effort), support) => {
            return Err(LlmError::UnsupportedRequest(format!(
                "{identity_provider}: request effort={} is not supported by model {model} \
                 under profile {}",
                effort.as_str(),
                support.identity()
            )));
        }
    };
    if request_reasoning_effort.is_some() && fixed_reasoning_effort.is_some() {
        return Err(LlmError::UnsupportedRequest(format!(
            "{identity_provider}: request effort cannot override the client's fixed reasoning_effort"
        )));
    }
    if let Some(effort) = request_reasoning_effort.or(fixed_reasoning_effort) {
        obj.insert("reasoning_effort".to_string(), json!(effort));
    }
    if let Some(schema) = &req.output_schema {
        match output_schema_support {
            OutputSchemaSupport::Unsupported => {
                return Err(LlmError::UnsupportedRequest(format!(
                    "{identity_provider}: strict output_schema is not supported by this client"
                )));
            }
            OutputSchemaSupport::StrictJsonSchema => {}
        }
        obj.insert(
            "response_format".to_string(),
            json!({
                "type": "json_schema",
                "json_schema": {
                    "name": STRUCTURED_OUTPUT_NAME,
                    "strict": true,
                    "schema": schema,
                }
            }),
        );
    }
    Ok(body)
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

fn from_wire(
    resp: &Value,
    model: &str,
    priced: bool,
    forced_tool: bool,
    tool_names: &[&str],
) -> Result<CompletionResponse, LlmError> {
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
    let refusal = message
        .get("refusal")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty());

    let provider_finish = choice.get("finish_reason").and_then(Value::as_str);
    let safety_finish = if provider_finish == Some("content_filter") {
        Some(FinishReason::ContentFilter)
    } else if refusal.is_some() {
        Some(FinishReason::Refusal)
    } else {
        None
    };
    let mut tool_calls: Vec<ToolCall> = Vec::new();
    let mut wire_tool_evidence = false;
    if safety_finish.is_none() {
        if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
            wire_tool_evidence = !calls.is_empty();
            for c in calls {
                let func = c.get("function");
                let arguments = func
                    .and_then(|f| f.get("arguments"))
                    .and_then(Value::as_str)
                    .and_then(|raw| serde_json::from_str(raw).ok());
                let Some(arguments) = arguments else {
                    // One malformed call makes the whole batch unsafe to dispatch.
                    tool_calls.clear();
                    break;
                };
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

        // Local models emit a forced call as JSON text; safety replies excluded.
        if forced_tool && tool_calls.is_empty() && !wire_tool_evidence {
            if let Some(call) = recover_tool_call(&content, tool_names) {
                tool_calls.push(call);
            }
        }
    }

    let finish_reason = safety_finish.unwrap_or_else(|| {
        if wire_tool_evidence || !tool_calls.is_empty() {
            return FinishReason::ToolUse;
        }
        match provider_finish {
            Some("stop") => FinishReason::Stop,
            Some("length") => FinishReason::Length,
            Some("tool_calls" | "function_call") => FinishReason::ToolUse,
            Some("content_filter") => FinishReason::ContentFilter,
            _ => FinishReason::Error,
        }
    });
    let content = match finish_reason {
        FinishReason::Refusal => refusal.unwrap_or_default().to_string(),
        _ if content.is_empty() => refusal.unwrap_or_default().to_string(),
        _ => content,
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

fn parse_usage(raw: Option<&Value>, model: &str, priced: bool) -> Option<TokenUsage> {
    let raw = raw?.as_object()?;
    let field = |name: &str| -> Option<u32> { u32::try_from(raw.get(name)?.as_u64()?).ok() };
    let mut usage = TokenUsage {
        input_tokens: field("prompt_tokens")?,
        output_tokens: field("completion_tokens")?,
        cost_usd: None,
    };
    if let Some(total) = raw.get("total_tokens") {
        let expected = u64::from(usage.input_tokens) + u64::from(usage.output_tokens);
        if total.as_u64()? != expected {
            return None;
        }
    }
    if priced {
        usage.cost_usd = pricing::cost_for(model, &usage);
    }
    Some(usage)
}

/// Recovers a tool call leaked into `content`: known name, or bare args if one tool.
fn recover_tool_call(content: &str, tool_names: &[&str]) -> Option<ToolCall> {
    let obj = extract_json_object(content)?;
    let recovered = |name: &str, arguments: Value| ToolCall {
        id: format!("recovered_{name}"),
        name: name.to_string(),
        arguments,
    };
    if let Some(name) = obj.get("name").and_then(Value::as_str) {
        return tool_names.contains(&name).then(|| {
            let args = obj
                .get("parameters")
                .or_else(|| obj.get("arguments"))
                .cloned()
                .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
            recovered(name, args)
        });
    }
    // No name wrapper: with exactly one tool offered, the object is its args.
    match tool_names {
        &[only] if !obj.contains_key("tool_calls") => Some(recovered(only, Value::Object(obj))),
        _ => None,
    }
}

/// First balanced top-level JSON object in `s`; escape-aware, fences/prose ok.
fn extract_json_object(s: &str) -> Option<serde_json::Map<String, Value>> {
    let start = s.find('{')?;
    let mut depth = 0u32;
    let mut in_str = false;
    let mut escaped = false;
    for (i, ch) in s.char_indices().skip_while(|&(i, _)| i < start) {
        if in_str {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_str = false;
            }
            continue;
        }
        match ch {
            '"' => in_str = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return serde_json::from_str(&s[start..=i]).ok();
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ToolSpec;

    #[test]
    fn unavailable_usage_preserves_completed_response() {
        let invalid = [
            Value::Null,
            json!([]),
            json!({}),
            json!({"prompt_tokens": 3}),
            json!({"completion_tokens": 2}),
            json!({"prompt_tokens": -1, "completion_tokens": 2}),
            json!({"prompt_tokens": "3", "completion_tokens": 2}),
            json!({"prompt_tokens": 3, "completion_tokens": 2.0}),
            json!({"prompt_tokens": true, "completion_tokens": 2}),
            json!({"prompt_tokens": 4294967296_u64, "completion_tokens": 2}),
            json!({"prompt_tokens": 3, "completion_tokens": null}),
        ];
        for usage in std::iter::once(None).chain(invalid.into_iter().map(Some)) {
            let mut wire =
                json!({"choices": [{"message": {"content": "retained"}, "finish_reason": "stop"}]});
            if let Some(usage) = usage {
                wire["usage"] = usage;
            }
            let response = from_wire(&wire, "gpt-4o-mini", true, false, &[]).unwrap();
            assert_eq!(response.message.content, "retained");
            assert_eq!(response.finish_reason, FinishReason::Stop);
            assert_eq!(response.usage, None, "{wire}");
        }
    }

    #[test]
    fn reported_usage_distinguishes_zero_and_unknown_pricing() {
        let zero = json!({"prompt_tokens": 0, "completion_tokens": 0});
        assert_eq!(
            parse_usage(Some(&zero), "gpt-4o-mini", true),
            Some(TokenUsage {
                input_tokens: 0,
                output_tokens: 0,
                cost_usd: Some(0.0),
            })
        );
        let counts = json!({"prompt_tokens": 12, "completion_tokens": 4});
        for (model, priced) in [("unlisted-model", true), ("gpt-4o-mini", false)] {
            assert_eq!(
                parse_usage(Some(&counts), model, priced),
                Some(TokenUsage {
                    input_tokens: 12,
                    output_tokens: 4,
                    cost_usd: None,
                })
            );
        }
        let max = json!({"prompt_tokens": u32::MAX, "completion_tokens": u32::MAX});
        assert!(parse_usage(Some(&max), "gpt-4o-mini", true).is_some());
    }

    #[test]
    fn explicitly_reported_total_must_match_without_narrowing() {
        for total in [
            json!(null),
            json!(-1),
            json!("7"),
            json!(7.0),
            json!(true),
            json!(6),
        ] {
            let usage = json!({"prompt_tokens": 3, "completion_tokens": 4, "total_tokens": total});
            assert_eq!(
                parse_usage(Some(&usage), "gpt-4o-mini", true),
                None,
                "{usage}"
            );
        }
        for (input, output) in [(0, 0), (3, 4), (u32::MAX, u32::MAX)] {
            let usage = json!({"prompt_tokens": input, "completion_tokens": output,
                "total_tokens": u64::from(input) + u64::from(output)});
            let report = parse_usage(Some(&usage), "gpt-4o-mini", true).unwrap();
            assert_eq!(report.input_tokens, input);
            assert_eq!(report.output_tokens, output);
        }
    }

    #[test]
    fn system_is_first_message_and_tools_are_wrapped() {
        let req = CompletionRequest::new(vec![Message::system("sys"), Message::user("u")])
            .with_tools(vec![ToolSpec {
                name: "search".into(),
                description: "find".into(),
                input_schema: json!({ "type": "object" }),
            }]);
        let w = to_wire(
            &req,
            "gpt",
            OPENAI_MAX_TOKENS_FIELD,
            None,
            RequestEffortSupport::Unsupported,
            OutputSchemaSupport::StrictJsonSchema,
            "openai",
        )
        .unwrap();
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
        let wire = |r: &CompletionRequest| {
            to_wire(
                r,
                "gpt",
                OPENAI_MAX_TOKENS_FIELD,
                None,
                RequestEffortSupport::Unsupported,
                OutputSchemaSupport::StrictJsonSchema,
                "openai",
            )
            .unwrap()
        };
        assert!(wire(&with_tools).get("tool_choice").is_none());
        assert_eq!(
            wire(&with_tools.clone().with_tool_choice(ToolChoice::Any))["tool_choice"],
            json!("required")
        );
        assert_eq!(
            wire(&with_tools.with_tool_choice(ToolChoice::Tool("search".into())))["tool_choice"],
            json!({ "type": "function", "function": { "name": "search" } })
        );
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
        let w = to_wire(
            &req,
            "gpt",
            OPENAI_MAX_TOKENS_FIELD,
            None,
            RequestEffortSupport::Unsupported,
            OutputSchemaSupport::StrictJsonSchema,
            "openai",
        )
        .unwrap();
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
        let w = to_wire(
            &req,
            "gpt",
            OPENAI_MAX_TOKENS_FIELD,
            None,
            RequestEffortSupport::Unsupported,
            OutputSchemaSupport::StrictJsonSchema,
            "openai",
        )
        .unwrap();
        assert_eq!(w["messages"][0]["role"], json!("tool"));
        assert_eq!(w["messages"][0]["tool_call_id"], json!("call_1"));
    }

    #[test]
    fn max_tokens_field_is_configurable() {
        let req = CompletionRequest {
            max_tokens: Some(256),
            ..CompletionRequest::new(vec![Message::user("u")])
        };
        let openai = to_wire(
            &req,
            "gpt",
            OPENAI_MAX_TOKENS_FIELD,
            None,
            RequestEffortSupport::Unsupported,
            OutputSchemaSupport::StrictJsonSchema,
            "openai",
        )
        .unwrap();
        assert_eq!(openai["max_completion_tokens"], json!(256));
        assert!(openai.get("max_tokens").is_none());
        let ollama = to_wire(
            &req,
            "llama",
            "max_tokens",
            None,
            RequestEffortSupport::Unsupported,
            OutputSchemaSupport::Unsupported,
            "ollama",
        )
        .unwrap();
        assert_eq!(ollama["max_tokens"], json!(256));
        assert!(ollama.get("max_completion_tokens").is_none());
    }

    #[test]
    fn fixed_compatible_reasoning_effort_is_emitted_only_when_set() {
        let req = CompletionRequest {
            max_tokens: Some(512),
            ..CompletionRequest::new(vec![Message::user("q")])
        };
        let gemini = to_wire(
            &req,
            "gemini-3.5-flash",
            "max_tokens",
            Some("low"),
            RequestEffortSupport::Unsupported,
            OutputSchemaSupport::Unsupported,
            "gemini",
        )
        .unwrap();
        assert_eq!(gemini["reasoning_effort"], json!("low"));
        assert_eq!(gemini["max_tokens"], json!(512));
        assert!(gemini.get("max_completion_tokens").is_none());
        let plain = to_wire(
            &req,
            "gpt",
            OPENAI_MAX_TOKENS_FIELD,
            None,
            RequestEffortSupport::Unsupported,
            OutputSchemaSupport::StrictJsonSchema,
            "openai",
        )
        .unwrap();
        assert!(plain.get("reasoning_effort").is_none());
    }

    #[test]
    fn supported_request_effort_is_emitted_and_changes_the_wire_bytes() {
        let request = |effort| CompletionRequest {
            effort: Some(effort),
            ..CompletionRequest::new(vec![Message::user("reason")])
        };
        let wire = |req: &CompletionRequest| {
            to_wire(
                req,
                "gpt-5-mini",
                OPENAI_MAX_TOKENS_FIELD,
                None,
                request_effort_support_for_model("gpt-5-mini"),
                OutputSchemaSupport::StrictJsonSchema,
                "openai",
            )
            .unwrap()
        };

        let low = wire(&request(Effort::Low));
        let high = wire(&request(Effort::High));
        assert_eq!(low["reasoning_effort"], json!("low"));
        assert_eq!(high["reasoning_effort"], json!("high"));
        assert_ne!(
            serde_json::to_vec(&low).unwrap(),
            serde_json::to_vec(&high).unwrap(),
            "changing request effort must change the dispatched bytes"
        );
    }

    #[test]
    fn unsupported_request_effort_refuses_before_dispatch_and_none_is_omitted() {
        let bare = CompletionRequest::new(vec![Message::user("plain")]);
        let unsupported = request_effort_support_for_model("gpt-4o-mini");
        assert_eq!(unsupported, RequestEffortSupport::Unsupported);
        let wire = to_wire(
            &bare,
            "gpt-4o-mini",
            OPENAI_MAX_TOKENS_FIELD,
            None,
            unsupported,
            OutputSchemaSupport::StrictJsonSchema,
            "openai",
        )
        .unwrap();
        assert!(wire.get("reasoning_effort").is_none());

        for (model, effort, support) in [
            ("gpt-4o-mini", Effort::Low, unsupported),
            (
                "gpt-5-mini",
                Effort::Max,
                request_effort_support_for_model("gpt-5-mini"),
            ),
        ] {
            let req = CompletionRequest {
                effort: Some(effort),
                ..bare.clone()
            };
            let error = to_wire(
                &req,
                model,
                OPENAI_MAX_TOKENS_FIELD,
                None,
                support,
                OutputSchemaSupport::StrictJsonSchema,
                "openai",
            )
            .unwrap_err();
            assert!(
                matches!(&error, LlmError::UnsupportedRequest(message)
                    if message.contains(model) && message.contains(effort.as_str())),
                "{error}"
            );
            assert!(error.is_pre_dispatch());
        }
    }

    #[test]
    fn request_effort_capability_is_exact_and_binds_client_identity() {
        for model in [
            "gpt-5",
            "gpt-5-2025-08-07",
            "gpt-5-mini",
            "gpt-5-mini-2025-08-07",
        ] {
            assert_eq!(
                request_effort_support_for_model(model),
                RequestEffortSupport::LowMediumHigh,
                "{model}"
            );
        }
        for model in [
            "gpt-4o-mini",
            "gpt-5-chat-latest",
            "gpt-5.4-mini",
            "gpt-5-mini-future-snapshot",
        ] {
            assert_eq!(
                request_effort_support_for_model(model),
                RequestEffortSupport::Unsupported,
                "{model}"
            );
        }

        let client = |model| {
            let support = request_effort_support_for_model(model);
            OpenAiClient::with_base_url(
                model,
                "https://example.test/v1",
                "secret",
                OutputSchemaSupport::StrictJsonSchema,
                support,
            )
        };
        let gpt4o = client("gpt-4o-mini");
        let gpt5 = client("gpt-5-mini");
        assert_ne!(gpt4o.request_identity(), gpt5.request_identity());
        let request = CompletionRequest {
            effort: Some(Effort::Low),
            ..CompletionRequest::new(vec![Message::user("reason")])
        };
        assert_eq!(
            to_wire(
                &request,
                &gpt5.model,
                gpt5.max_tokens_field,
                gpt5.fixed_reasoning_effort.as_deref(),
                gpt5.request_effort_support,
                gpt5.output_schema_support,
                gpt5.identity_provider,
            )
            .unwrap()["reasoning_effort"],
            json!("low"),
            "the capability bound into identity is the one used by the wire"
        );
        assert!(
            to_wire(
                &request,
                &gpt4o.model,
                gpt4o.max_tokens_field,
                gpt4o.fixed_reasoning_effort.as_deref(),
                gpt4o.request_effort_support,
                gpt4o.output_schema_support,
                gpt4o.identity_provider,
            )
            .unwrap_err()
            .is_pre_dispatch(),
            "an unsupported identity profile cannot emit request effort"
        );
        assert_eq!(
            gpt5.request_identity(),
            ClientRequestIdentity::from_config(
                "openai",
                "https://example.test/v1",
                &[
                    ("wire", REQUEST_EFFORT_CHAT_COMPLETIONS_WIRE_REVISION),
                    ("max_tokens_field", OPENAI_MAX_TOKENS_FIELD),
                    ("reasoning_effort", "<none>"),
                    (
                        "request_effort",
                        RequestEffortSupport::LowMediumHigh.identity(),
                    ),
                    ("structured_outputs", STRUCTURED_OUTPUTS_REVISION),
                ],
            )
        );
    }

    #[test]
    fn strict_schema_has_the_exact_openai_wire_and_compatible_dialects_refuse() {
        let schema = json!({
            "type": "object",
            "properties": {
                "decision": { "type": "string", "enum": ["facts", "nothing_durable"] },
                "facts": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["decision", "facts"],
            "additionalProperties": false
        });
        let req = CompletionRequest {
            output_schema: Some(schema.clone()),
            ..CompletionRequest::new(vec![Message::user("extract")])
        };
        let wire = to_wire(
            &req,
            "gpt-4o-mini-2024-07-18",
            OPENAI_MAX_TOKENS_FIELD,
            None,
            RequestEffortSupport::Unsupported,
            OutputSchemaSupport::StrictJsonSchema,
            "openai",
        )
        .unwrap();
        assert_eq!(
            wire["response_format"],
            json!({
                "type": "json_schema",
                "json_schema": {
                    "name": "citadel_structured_output",
                    "strict": true,
                    "schema": schema
                }
            })
        );

        for provider in ["ollama", "gemini"] {
            let err = to_wire(
                &req,
                "compatible",
                "max_tokens",
                None,
                RequestEffortSupport::Unsupported,
                OutputSchemaSupport::Unsupported,
                provider,
            )
            .unwrap_err();
            assert!(
                matches!(&err, LlmError::UnsupportedRequest(message) if
                    message.contains(provider) && message.contains("output_schema")),
                "{err}"
            );
            assert!(err.is_pre_dispatch());
        }
        let bare = CompletionRequest::new(vec![Message::user("plain")]);
        let compatible = to_wire(
            &bare,
            "compatible",
            "max_tokens",
            None,
            RequestEffortSupport::Unsupported,
            OutputSchemaSupport::Unsupported,
            "ollama",
        )
        .unwrap();
        assert!(
            compatible.get("response_format").is_none(),
            "an unstructured compatible request keeps its historical wire"
        );
    }

    #[test]
    fn schema_capability_has_a_versioned_request_identity() {
        let openai = OpenAiClient::with_base_url(
            "gpt",
            "https://example.test/v1",
            "secret",
            OutputSchemaSupport::StrictJsonSchema,
            RequestEffortSupport::Unsupported,
        );
        let compatible = OpenAiClient::with_base_url(
            "gpt",
            "https://example.test/v1",
            "secret",
            OutputSchemaSupport::Unsupported,
            RequestEffortSupport::Unsupported,
        );
        assert_eq!(
            openai.output_schema_support(),
            OutputSchemaSupport::StrictJsonSchema
        );
        assert_eq!(
            compatible.output_schema_support(),
            OutputSchemaSupport::Unsupported
        );
        assert_eq!(
            openai.request_identity(),
            ClientRequestIdentity::from_config(
                "openai",
                "https://example.test/v1",
                &[
                    ("wire", LEGACY_CHAT_COMPLETIONS_WIRE_REVISION),
                    ("max_tokens_field", OPENAI_MAX_TOKENS_FIELD),
                    ("reasoning_effort", "<none>"),
                    ("structured_outputs", STRUCTURED_OUTPUTS_REVISION),
                ],
            )
        );
        assert_ne!(
            openai.request_identity(),
            compatible.request_identity(),
            "support for a new canonical request shape is transport identity"
        );
        assert_eq!(
            compatible.request_identity(),
            ClientRequestIdentity::from_config(
                "openai",
                "https://example.test/v1",
                &[
                    ("wire", LEGACY_CHAT_COMPLETIONS_WIRE_REVISION),
                    ("max_tokens_field", OPENAI_MAX_TOKENS_FIELD),
                    ("reasoning_effort", "<none>"),
                    (
                        "structured_outputs",
                        UNSUPPORTED_STRUCTURED_OUTPUTS_REVISION,
                    ),
                ],
            )
        );
        assert_eq!(
            OpenAiClient::with_base_url(
                "gpt",
                DEFAULT_BASE_URL,
                "secret",
                OutputSchemaSupport::StrictJsonSchema,
                RequestEffortSupport::Unsupported,
            )
            .request_identity()
            .wire_defaults_sha256(),
            "4a3cbeceff10687575d0fef65e9d6299236c6f86c4eae17cff9eeeebd18fe3be"
        );
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
        let r = from_wire(&resp, "gpt", true, false, &[]).unwrap();
        assert_eq!(r.finish_reason, FinishReason::ToolUse);
        assert_eq!(r.message.content, "");
        assert_eq!(
            r.message.tool_calls[0].arguments,
            json!({ "q": "x" }),
            "string parsed back to object"
        );
        assert_eq!(r.usage.unwrap().input_tokens, 12);
        assert_eq!(r.usage.unwrap().output_tokens, 4);
        assert_eq!(
            r.usage.unwrap().cost_usd,
            None,
            "bare 'gpt' has no confident rate"
        );
    }

    #[test]
    fn malformed_tool_arguments_preserve_completed_tool_use_and_usage() {
        for finish_reason in ["tool_calls", "stop"] {
            let resp = json!({
                "choices": [{
                    "message": {
                        "content": "partial",
                        "tool_calls": [{
                            "id": "call_good",
                            "type": "function",
                            "function": { "name": "valid", "arguments": "{\"ok\":true}" }
                        }, {
                            "id": "call_bad",
                            "type": "function",
                            "function": { "name": "unexpected", "arguments": "{not json" }
                        }]
                    },
                    "finish_reason": finish_reason
                }],
                "usage": { "prompt_tokens": 8, "completion_tokens": 3 }
            });
            let r = from_wire(&resp, "gpt", true, false, &[]).unwrap();
            assert_eq!(r.finish_reason, FinishReason::ToolUse);
            assert_eq!(r.message.content, "partial");
            assert!(
                r.message.tool_calls.is_empty(),
                "one malformed call makes the whole batch inert"
            );
            assert_eq!(r.usage.unwrap().input_tokens, 8);
            assert_eq!(r.usage.unwrap().output_tokens, 3);
        }
    }

    #[test]
    fn from_wire_plain_text_reply() {
        let resp = json!({
            "choices": [{ "message": { "content": "hello" }, "finish_reason": "stop" }],
            "usage": { "prompt_tokens": 3, "completion_tokens": 1 }
        });
        let r = from_wire(&resp, "gpt", true, false, &[]).unwrap();
        assert_eq!(r.message.content, "hello");
        assert!(r.message.tool_calls.is_empty());
        assert_eq!(r.finish_reason, FinishReason::Stop);
    }

    #[test]
    fn refusal_is_a_completed_disposition_with_text_and_usage() {
        let resp = json!({
            "choices": [{
                "message": {
                    "content": "partial answer that must not hide the refusal",
                    "refusal": "I cannot help with that.",
                    "tool_calls": [{
                        "id": "blocked",
                        "type": "function",
                        "function": { "name": "act", "arguments": "{not json" }
                    }]
                },
                "finish_reason": "stop"
            }],
            "usage": { "prompt_tokens": 17, "completion_tokens": 6 }
        });
        let r = from_wire(&resp, "gpt", false, false, &[]).unwrap();
        assert_eq!(r.finish_reason, FinishReason::Refusal);
        assert_eq!(r.message.content, "I cannot help with that.");
        assert!(r.message.tool_calls.is_empty());
        assert_eq!(r.usage.unwrap().input_tokens, 17);
        assert_eq!(r.usage.unwrap().output_tokens, 6);
    }

    #[test]
    fn content_filter_is_distinct_and_preserves_partial_content_and_usage() {
        let resp = json!({
            "choices": [{
                "message": {
                    "content": "partial",
                    "refusal": "policy",
                    "tool_calls": [{
                        "id": "blocked",
                        "type": "function",
                        "function": { "name": "act", "arguments": "{not json" }
                    }]
                },
                "finish_reason": "content_filter"
            }],
            "usage": { "prompt_tokens": 9, "completion_tokens": 2 }
        });
        let r = from_wire(&resp, "gpt", false, false, &[]).unwrap();
        assert_eq!(r.finish_reason, FinishReason::ContentFilter);
        assert_eq!(r.message.content, "partial");
        assert!(r.message.tool_calls.is_empty(), "filtered calls are inert");
        assert_eq!(r.usage.unwrap().input_tokens, 9);
        assert_eq!(r.usage.unwrap().output_tokens, 2);
    }

    #[test]
    fn recovers_forced_tool_call_leaked_into_fenced_content() {
        // Ollama/small models fence the call in content with finish_reason "stop".
        let resp = json!({
            "choices": [{
                "message": { "content": "```json\n{\"name\": \"submit_plan\", \"parameters\": {\"goal\": {\"prompt\": \"fix\"}}}\n```" },
                "finish_reason": "stop"
            }],
            "usage": { "prompt_tokens": 5, "completion_tokens": 3 }
        });
        let r = from_wire(&resp, "llama", false, true, &["submit_plan"]).unwrap();
        assert_eq!(
            r.finish_reason,
            FinishReason::ToolUse,
            "recovered call -> ToolUse"
        );
        assert_eq!(r.message.tool_calls.len(), 1);
        let call = &r.message.tool_calls[0];
        assert_eq!(call.name, "submit_plan");
        assert_eq!(
            call.id, "recovered_submit_plan",
            "marked recovered in the trace"
        );
        assert_eq!(call.arguments, json!({ "goal": { "prompt": "fix" } }));
    }

    #[test]
    fn recovery_is_inert_on_the_auto_path() {
        // Auto path: a JSON-naming-a-tool text reply stays the final answer.
        let resp = json!({
            "choices": [{
                "message": { "content": "{\"name\": \"submit_plan\", \"parameters\": {}}" },
                "finish_reason": "stop"
            }],
            "usage": {}
        });
        let r = from_wire(&resp, "gpt", true, false, &["submit_plan"]).unwrap();
        assert!(
            r.message.tool_calls.is_empty(),
            "no phantom tool call on the auto path"
        );
        assert_eq!(r.finish_reason, FinishReason::Stop);
    }

    #[test]
    fn recovery_ignores_an_unknown_tool_name() {
        let resp = json!({
            "choices": [{
                "message": { "content": "{\"name\": \"other\", \"parameters\": {}}" },
                "finish_reason": "stop"
            }],
            "usage": {}
        });
        let r = from_wire(&resp, "llama", false, true, &["submit_plan"]).unwrap();
        assert!(
            r.message.tool_calls.is_empty(),
            "unknown tool name is not recovered"
        );
    }

    #[test]
    fn recovers_bare_arguments_for_a_single_forced_tool() {
        // Some models emit the arguments object with no {name, ...} wrapper.
        let resp = json!({
            "choices": [{
                "message": { "content": "Here is my verdict: {\"satisfied\": true, \"reason\": \"ok\"}" },
                "finish_reason": "stop"
            }],
            "usage": {}
        });
        let r = from_wire(&resp, "llama", false, true, &["verdict"]).unwrap();
        assert_eq!(r.message.tool_calls.len(), 1);
        assert_eq!(r.message.tool_calls[0].name, "verdict");
        assert_eq!(
            r.message.tool_calls[0].arguments,
            json!({ "satisfied": true, "reason": "ok" }),
            "prose-prefixed bare args recovered for the single offered tool"
        );
    }
}
