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
    AssistantMessage, CompletionRequest, CompletionResponse, LlmError, Message, ToolChoice,
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
    fn propose(
        &self,
        ctx: &ProposalContext<'_>,
        llm: &mut dyn Completer,
    ) -> Result<Vec<Candidate>, ProposeError>;
}

/// A [`ProposalOperator`] that asks a model (via the injected [`Completer`]) for
/// candidate artifacts. Stateless apart from the sampling temperature.
pub struct LlmProposer {
    temperature: f32,
    artifact_schema: Value,
}

impl Default for LlmProposer {
    fn default() -> Self {
        Self {
            temperature: 0.9,
            // Permissive: goal + checker define the shape; tighten via with_artifact_schema.
            artifact_schema: json!({ "type": "object" }),
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

    /// Constrain the propose tool's input to a JSON Schema (default: any object).
    pub fn with_artifact_schema(mut self, schema: Value) -> Self {
        self.artifact_schema = schema;
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
        let user = format!(
            "Goal: {}\nBest-known artifacts (mutate or improve on these): {best}\n\
             Call the propose tool once per candidate artifact.",
            ctx.goal.prompt
        );
        let mut req = CompletionRequest::new(vec![ctx.system.as_system(), Message::user(user)])
            .with_tools(vec![self.propose_tool()])
            .with_tool_choice(ToolChoice::Any);
        req.temperature = Some(self.temperature);
        req
    }
}

/// Extract candidate artifacts from a model reply: each tool call's arguments is
/// one artifact; otherwise the first balanced JSON value in the text (tolerating
/// markdown fences and surrounding prose) is parsed - an array yields one
/// candidate per element, any other value yields a single candidate. Text with no
/// parseable JSON yields no candidates rather than an error - a barren round.
fn parse_candidates(msg: &AssistantMessage) -> Vec<Candidate> {
    if !msg.tool_calls.is_empty() {
        return msg
            .tool_calls
            .iter()
            .map(|c| Candidate {
                artifact: c.arguments.clone(),
                parent: None,
                rationale: format!("tool_call {}", c.name),
            })
            .collect();
    }
    // Free-text fallback: pull the first balanced JSON value out of any markdown
    // fence or prose. Unparseable = a barren round, not an error.
    let parsed = extract_first_json(msg.content.trim())
        .and_then(|span| serde_json::from_str::<Value>(span).ok());
    match parsed {
        Some(Value::Array(items)) => items
            .into_iter()
            .map(|artifact| Candidate {
                artifact,
                parent: None,
                rationale: "proposed".to_string(),
            })
            .collect(),
        Some(artifact) => vec![Candidate {
            artifact,
            parent: None,
            rationale: "proposed".to_string(),
        }],
        None => Vec::new(),
    }
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
        llm: &mut dyn Completer,
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
        let mut llm = OneShot(reply);
        LlmProposer::new()
            .propose(&ctx_for(&goal, &system), &mut llm)
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
    }
}
