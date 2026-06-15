//! Pluggable candidate generator for discovery search.
//!
//! A [`ProposalOperator`] proposes candidate artifacts for an external checker; the
//! default agent loop never uses it, the opt-in discovery controller does. The
//! operator does NOT own an LLM client - the controller injects a [`Completer`]
//! (its traced, budgeted, replayable channel) so every call is recorded for replay.

use serde_json::{json, Value};

use citadel_mem::AtomId;

use crate::graph::Goal;
use crate::llm::{
    AssistantMessage, CompletionRequest, CompletionResponse, Effort, LlmError, Message, ToolChoice,
    ToolSpec,
};
use crate::prompts::ResolvedPrompt;

/// A proposed-but-unchecked artifact + its mutation parent + the operator's rationale.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub artifact: Value,
    pub parent: Option<AtomId>,
    pub rationale: String,
}

/// An elite-archive entry offered to the operator as a mutation parent.
#[derive(Debug, Clone)]
pub struct Elite {
    pub atom: AtomId,
    pub artifact: Value,
    pub score: f64,
}

/// Inputs to one proposal round: the goal, the current elites, and the resolved
/// proposer prompt (so the request body and its recorded provenance share a source).
pub struct ProposalContext<'a> {
    pub goal: &'a Goal,
    pub elites: &'a [Elite],
    pub system: &'a ResolvedPrompt,
}

#[derive(Debug, thiserror::Error)]
pub enum ProposeError {
    #[error(transparent)]
    Llm(#[from] LlmError),
    #[error("proposal operator failed: {0}")]
    Failed(String),
}

/// The controller's traced, budgeted, replayable LLM channel, injected into an
/// operator so its calls are recorded without the operator owning a client.
pub trait Completer {
    fn complete(&mut self, req: &CompletionRequest) -> Result<CompletionResponse, ProposeError>;
}

/// Generates candidate artifacts for an external checker to score. Implementors
/// may be LLM-backed, local search, SAT-model extraction, etc.
pub trait ProposalOperator: Send + Sync {
    /// Propose zero or more candidates this round (empty = a barren round, valid).
    /// LLM operators drive the model through `llm`; non-LLM operators ignore it.
    /// `llm` is an OWNED one-shot channel (not a borrow) so it can be handed across
    /// a language boundary; the controller still traces and budgets every call.
    fn propose(
        &self,
        ctx: &ProposalContext<'_>,
        llm: Box<dyn Completer>,
    ) -> Result<Vec<Candidate>, ProposeError>;
}

/// A [`ProposalOperator`] that asks a model (via the injected [`Completer`]) for
/// candidate artifacts. Stateless apart from the sampling temperature.
pub struct LlmProposer {
    temperature: f32,
    max_tokens: Option<u32>,
    artifact_schema: Value,
    plain_json: bool,
}

impl Default for LlmProposer {
    fn default() -> Self {
        Self {
            temperature: 0.9,
            max_tokens: None,
            // Permissive: goal + checker define the shape; tighten via with_artifact_schema.
            artifact_schema: json!({ "type": "object" }),
            plain_json: false,
        }
    }
}

impl LlmProposer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sampling temperature for proposal diversity (default 0.9).
    pub fn with_temperature(mut self, temperature: f32) -> Self {
        self.temperature = temperature;
        self
    }

    /// Per-reply token budget (default: the backend's). Adaptive-thinking
    /// models spend reply tokens reasoning BEFORE any text begins, so a
    /// proposal call needs far more headroom than the JSON alone.
    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = Some(max_tokens);
        self
    }

    /// Constrain the propose tool's input to a JSON Schema (default: any object).
    pub fn with_artifact_schema(mut self, schema: Value) -> Self {
        self.artifact_schema = schema;
        self
    }

    /// For models that reject forced tool use (`tool_choice` incompatibility):
    /// the request carries NO tools and asks for bare JSON, which
    /// [`parse_artifacts`]' free-text path already handles. The reply is
    /// constrained via structured outputs (an array of `artifact_schema`) and
    /// reasoning spend is effort-capped, so a text block actually begins.
    pub fn with_plain_json(mut self) -> Self {
        self.plain_json = true;
        self
    }

    /// Structured-output tool. Forcing a tool call (ToolChoice::Any) returns
    /// candidates as tool inputs, side-stepping markdown-fence/prose JSON breakage.
    fn propose_tool(&self) -> ToolSpec {
        ToolSpec {
            name: "propose".into(),
            description: "Propose one candidate solution artifact for the external checker to \
                          score. Call once per candidate."
                .into(),
            input_schema: self.artifact_schema.clone(),
        }
    }

    fn build_request(&self, ctx: &ProposalContext<'_>) -> CompletionRequest {
        let best: Vec<&Value> = ctx.elites.iter().map(|e| &e.artifact).collect();
        let best = serde_json::to_string(&best).unwrap_or_else(|_| "[]".to_string());
        let mut req = if self.plain_json {
            // The verification delegation is load-bearing for adaptive-thinking
            // models: without it the model re-implements the external checker
            // inside its billed, invisible reasoning and can emit no text at all.
            let user = format!(
                "Goal: {}\nBest-known artifacts (mutate or improve on these): {best}\n\
                 Immediately output candidate objects in the required JSON schema (an \
                 object with a \"candidates\" array). Use minimal reasoning: propose \
                 quick heuristic mutations. Do NOT attempt to verify or optimize your \
                 candidates - an external checker validates every one of them.",
                ctx.goal.prompt
            );
            CompletionRequest::new(vec![ctx.system.as_system(), Message::user(user)])
        } else {
            let user = format!(
                "Goal: {}\nBest-known artifacts (mutate or improve on these): {best}\n\
                 Call the propose tool once per candidate artifact.",
                ctx.goal.prompt
            );
            CompletionRequest::new(vec![ctx.system.as_system(), Message::user(user)])
                .with_tools(vec![self.propose_tool()])
                .with_tool_choice(ToolChoice::Any)
        };
        if self.plain_json {
            // Cap reasoning + force a schema-valid block; object root is the
            // canonical structured-output shape (the prompt names "candidates").
            req.effort = Some(Effort::Low);
            req.output_schema = Some(json!({
                "type": "object",
                "properties": {
                    "candidates": {
                        "type": "array",
                        "items": self.artifact_schema.clone(),
                    }
                },
                "required": ["candidates"],
                "additionalProperties": false,
            }));
        }
        req.max_tokens = self.max_tokens;
        req.temperature = Some(self.temperature);
        req
    }
}

/// Extract artifact values from a model reply: each tool call's arguments is one
/// artifact; otherwise the first balanced JSON value in the text (tolerating
/// markdown fences and surrounding prose) - an array yields one artifact per
/// element, an object whose sole shape is the structured-output envelope
/// `{"candidates": [...]}` yields its elements, any other value yields one.
/// Text with no parseable JSON yields none (a barren round, not an error).
/// Shared by every `ProposalOperator` so the reply-parsing has a single source
/// (e.g. `citadel-lean`'s conjecture generator).
pub fn parse_artifacts(msg: &AssistantMessage) -> Vec<Value> {
    if !msg.tool_calls.is_empty() {
        return msg.tool_calls.iter().map(|c| c.arguments.clone()).collect();
    }
    match extract_first_json(msg.content.trim())
        .and_then(|span| serde_json::from_str::<Value>(span).ok())
    {
        Some(Value::Array(items)) => items,
        Some(Value::Object(mut obj)) if obj.len() == 1 && obj.contains_key("candidates") => {
            match obj.remove("candidates") {
                Some(Value::Array(items)) => items,
                Some(other) => vec![other],
                None => Vec::new(),
            }
        }
        Some(value) => vec![value],
        None => Vec::new(),
    }
}

/// Wrap [`parse_artifacts`] as fresh proposer candidates (no mutation parent).
fn parse_candidates(msg: &AssistantMessage) -> Vec<Candidate> {
    parse_artifacts(msg)
        .into_iter()
        .map(|artifact| Candidate {
            artifact,
            parent: None,
            rationale: "proposed".to_string(),
        })
        .collect()
}

/// First balanced top-level `[...]`/`{...}` span, ignoring fences/prose. String-aware
/// and depth-tracked, so nested arrays only close the outer bracket at depth 0.
fn extract_first_json(s: &str) -> Option<&str> {
    let mut depth = 0i32;
    let mut start = None;
    let mut in_str = false;
    let mut escaped = false;
    for (i, &c) in s.as_bytes().iter().enumerate() {
        if in_str {
            match c {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_str = false,
                _ => {}
            }
            continue;
        }
        match c {
            b'"' => in_str = true,
            b'[' | b'{' => {
                if depth == 0 {
                    start = Some(i);
                }
                depth += 1;
            }
            b']' | b'}' if depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    return start.map(|st| &s[st..=i]);
                }
            }
            _ => {}
        }
    }
    None
}

impl ProposalOperator for LlmProposer {
    fn propose(
        &self,
        ctx: &ProposalContext<'_>,
        mut llm: Box<dyn Completer>,
    ) -> Result<Vec<Candidate>, ProposeError> {
        let resp = llm.complete(&self.build_request(ctx))?;
        Ok(parse_candidates(&resp.message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::{CompletionResponse, ToolCall};
    use crate::prompts::{PromptId, PromptLibrary};

    /// Build a `{"values":[[a,b],...]}` artifact string (local: no problem-specific dep).
    fn artifact_with(rows: &[(i64, i64)]) -> String {
        let vals: Vec<[i64; 2]> = rows.iter().map(|&(a, b)| [a, b]).collect();
        serde_json::json!({ "values": vals }).to_string()
    }

    /// A [`Completer`] that returns one fixed reply (stands in for the traced channel).
    struct OneShot(CompletionResponse);
    impl Completer for OneShot {
        fn complete(
            &mut self,
            _req: &CompletionRequest,
        ) -> Result<CompletionResponse, ProposeError> {
            Ok(self.0.clone())
        }
    }

    fn ctx_for<'a>(goal: &'a Goal, system: &'a ResolvedPrompt) -> ProposalContext<'a> {
        ProposalContext {
            goal,
            elites: &[],
            system,
        }
    }

    fn propose_with(reply: CompletionResponse) -> Vec<Candidate> {
        let goal = Goal::new("produce a candidate satisfying the constraints");
        let lib = PromptLibrary::default();
        let system = lib.resolve(PromptId::Proposer);
        LlmProposer::new()
            .propose(&ctx_for(&goal, &system), Box::new(OneShot(reply)))
            .unwrap()
    }

    #[test]
    fn parses_json_array_of_artifacts() {
        let a = artifact_with(&[(1, 1), (2, 1)]);
        let b = artifact_with(&[(1, 1), (1, 2), (2, 1)]);
        let cands = propose_with(CompletionResponse::text(format!("[{a},{b}]")));
        assert_eq!(cands.len(), 2);
        assert!(cands.iter().all(|c| c.artifact.get("values").is_some()));
    }

    #[test]
    fn parses_tool_call_arguments_as_artifacts() {
        let cands = propose_with(CompletionResponse::tool_calls(vec![ToolCall {
            id: "p".into(),
            name: "propose".into(),
            arguments: serde_json::json!({ "values": [[1, 1], [2, 1]] }),
        }]));
        assert_eq!(cands.len(), 1);
        assert!(cands[0].artifact.get("values").is_some());
    }

    #[test]
    fn prose_yields_no_candidates_not_an_error() {
        let cands = propose_with(CompletionResponse::text("Sorry, I cannot help."));
        assert!(cands.is_empty());
    }

    #[test]
    fn parses_fenced_json_array() {
        let a = artifact_with(&[(1, 1), (2, 1)]);
        let cands = propose_with(CompletionResponse::text(format!("```json\n[{a}]\n```")));
        assert_eq!(cands.len(), 1);
        assert!(cands[0].artifact.get("values").is_some());
    }

    #[test]
    fn parses_prose_wrapped_json() {
        let a = artifact_with(&[(1, 1), (1, 2), (2, 1)]);
        let cands = propose_with(CompletionResponse::text(format!(
            "Sure, here is a candidate: [{a}] - hope it helps!"
        )));
        assert_eq!(cands.len(), 1);
        assert!(cands[0].artifact.get("values").is_some());
    }

    #[test]
    fn single_object_is_one_candidate() {
        let a = artifact_with(&[(1, 1), (2, 1), (3, 1)]);
        let cands = propose_with(CompletionResponse::text(a));
        assert_eq!(cands.len(), 1);
        assert!(cands[0].artifact.get("values").is_some());
    }

    #[test]
    fn nested_arrays_are_not_mis_split() {
        let a = artifact_with(&[(1, 1), (2, 1), (3, 2), (1, 4)]);
        let cands = propose_with(CompletionResponse::text(format!("[{a}]")));
        assert_eq!(cands.len(), 1);
        let vals = cands[0].artifact["values"].as_array().unwrap();
        assert_eq!(
            vals.len(),
            4,
            "outer array closed at depth 0, not at an inner array"
        );
    }

    #[test]
    fn first_of_multiple_text_blocks_wins() {
        let a = artifact_with(&[(1, 1), (2, 1)]);
        let b = artifact_with(&[(3, 3), (4, 4)]);
        let cands = propose_with(CompletionResponse::text(format!("[{a}] and also [{b}]")));
        assert_eq!(cands.len(), 1, "only the first balanced span is taken");
    }

    #[test]
    fn invalid_json_yields_no_candidates() {
        // Balanced span found but serde rejects the trailing comma: barren, not a panic.
        let cands = propose_with(CompletionResponse::text("[ {\"values\": [[1,1]],} ]"));
        assert!(cands.is_empty());
    }

    #[test]
    fn build_request_forces_the_propose_tool() {
        let goal = Goal::new("g");
        let lib = PromptLibrary::default();
        let system = lib.resolve(PromptId::Proposer);
        let req = LlmProposer::new().build_request(&ctx_for(&goal, &system));
        assert_eq!(req.tools.len(), 1);
        assert_eq!(req.tools[0].name, "propose");
        assert_eq!(req.tool_choice, ToolChoice::Any);
        assert_eq!(req.temperature, Some(0.9));
        assert_eq!(req.effort, None, "tool mode leaves reasoning uncapped");
        assert_eq!(req.output_schema, None, "tool inputs are the structure");
    }

    #[test]
    fn with_max_tokens_sets_the_budget_in_both_modes() {
        let goal = Goal::new("g");
        let lib = PromptLibrary::default();
        let system = lib.resolve(PromptId::Proposer);
        let tool_req = LlmProposer::new()
            .with_max_tokens(16_384)
            .build_request(&ctx_for(&goal, &system));
        assert_eq!(tool_req.max_tokens, Some(16_384));
        let plain_req = LlmProposer::new()
            .with_plain_json()
            .with_max_tokens(16_384)
            .build_request(&ctx_for(&goal, &system));
        assert_eq!(plain_req.max_tokens, Some(16_384));
        let default_req = LlmProposer::new().build_request(&ctx_for(&goal, &system));
        assert_eq!(
            default_req.max_tokens, None,
            "unset keeps the backend default"
        );
    }

    #[test]
    fn plain_json_caps_effort_and_constrains_the_reply_schema() {
        let goal = Goal::new("g");
        let lib = PromptLibrary::default();
        let system = lib.resolve(PromptId::Proposer);
        let schema = serde_json::json!({
            "type": "object",
            "properties": { "points": { "type": "array" } },
            "required": ["points"],
            "additionalProperties": false
        });
        let req = LlmProposer::new()
            .with_artifact_schema(schema.clone())
            .with_plain_json()
            .build_request(&ctx_for(&goal, &system));
        assert_eq!(req.effort, Some(crate::llm::Effort::Low));
        assert_eq!(
            req.output_schema,
            Some(serde_json::json!({
                "type": "object",
                "properties": { "candidates": { "type": "array", "items": schema } },
                "required": ["candidates"],
                "additionalProperties": false,
            })),
            "object-rooted envelope holding the artifact array"
        );
        let Message::User(user) = &req.messages[1] else {
            panic!("second message is the user prompt");
        };
        assert!(user.contains("Do NOT attempt to verify"));
        assert!(user.contains("\"candidates\""));
        assert!(
            !user.contains("ONLY a JSON array"),
            "schema-contradicting phrasing removed"
        );
    }

    #[test]
    fn parse_artifacts_unwraps_the_candidates_envelope() {
        let cands = propose_with(CompletionResponse::text(
            "{\"candidates\": [{\"points\": [[1,2]]}, {\"points\": [[3,4]]}]}",
        ));
        assert_eq!(cands.len(), 2, "envelope elements become candidates");
        // An object that is NOT the envelope still yields itself (e.g. lean
        // conjecture artifacts carry statement/proof keys).
        let single = propose_with(CompletionResponse::text(
            "{\"statement\": \"theorem t : 1 = 1\", \"proof\": \"rfl\"}",
        ));
        assert_eq!(single.len(), 1);
    }

    #[test]
    fn plain_json_request_carries_no_tools_and_parses_text_candidates() {
        // For models that reject forced tool use: NO tools in the request,
        // and the free-text parser still produces the candidates.
        let goal = Goal::new("g");
        let lib = PromptLibrary::default();
        let system = lib.resolve(PromptId::Proposer);
        let req = LlmProposer::new()
            .with_plain_json()
            .build_request(&ctx_for(&goal, &system));
        assert!(req.tools.is_empty(), "no tools for plain-json models");
        assert_eq!(req.tool_choice, ToolChoice::Auto);

        let cands = propose_with(CompletionResponse::text(
            "[{\"points\": [[1,2]]}, {\"points\": [[3,4]]}]",
        ));
        assert_eq!(cands.len(), 2, "bare-array text replies still parse");
    }
}
