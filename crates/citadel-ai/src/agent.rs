//! The cognition loop: a sequential state machine over the Belief-and-Goal graph.
//!
//! `run` drives `Plan -> Execute -> Tool -> Observe -> Reflect/Converge -> Done`,
//! checking the [`AgentBudget`] before every transition and recording each LLM call
//! as an immutable `llm_trace`. `Observe` enforces co-instantiation: each step is
//! gated on structural provenance to the immutable goal, recorded RECORD-BEFORE-ABORT
//! into the BLAKE3 chain. Sync, single-agent, no tokio.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use rustc_hash::FxHashMap;
use serde_json::{json, Value};

use citadel_mem::{AtomId, MemError, RecallProfile};

use crate::budget::{
    valid_cost, AgentBudget, BudgetExceeded, BudgetInvalid, BudgetStop, BudgetUnavailable,
    BudgetUsage,
};
use crate::graph::{
    BeliefGraph, CoInstantiationCheck, Evidence, Goal, GoalStatus, GraphError, Reflection,
    SelfModel, Task, TaskStatus, Verdict, VerifiedKind, CANDIDATE_KIND,
};
use crate::prompts::{PromptId, PromptLibrary, ResolvedPrompt};
use crate::propose::{
    Candidate, Completer, Elite, ProposalContext, ProposalOperator, ProposeError, RejectedCandidate,
};
use crate::tools::{
    is_known_memory_mutation, structural_constraints_ok, ExecPolicy, FsPolicy, Tool, ToolError,
    ToolPermissions, ToolRegistry,
};
use crate::verify::{CheckerAttestation, Verifier, VerifyKind, VerifyRequest};
use citadel_llm::{
    request_hash, AssistantMessage, CompletionRequest, CompletionResponse, FinishReason, LLMClient,
    LlmError, Message, TokenUsage, ToolCall, ToolChoice, ToolSpec,
};

#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error(transparent)]
    Graph(#[from] GraphError),
    #[error(transparent)]
    Llm(#[from] citadel_llm::LlmError),
    #[error(transparent)]
    Budget(#[from] BudgetStop),
    #[error("agent: {0}")]
    Other(String),
}

pub type AgentResult<T> = Result<T, AgentError>;

/// One node of the cognition loop. Data travels in the state so the driver is a
/// pure `fn(state) -> state`.
#[derive(Debug)]
pub enum CognitionState {
    Plan,
    Execute,
    /// ReAct re-entry: continue the in-progress task, carrying the tool transcript.
    Reason {
        task: AtomId,
        round: u32,
        transcript: Vec<Message>,
    },
    Tool {
        task: AtomId,
        round: u32,
        /// Replayed next round so tool results stay paired with the call_id.
        assistant: AssistantMessage,
        transcript: Vec<Message>,
    },
    Observe {
        task: AtomId,
        round: u32,
        answer: Option<String>,
        results: Vec<(ToolCall, Result<String, ToolError>)>,
        /// `Some` on a tool round (replayable), `None` on a text-answer round.
        assistant: Option<AssistantMessage>,
        transcript: Vec<Message>,
    },
    Reflect {
        reason: ReflectReason,
    },
    Converge,
    Done {
        terminated_by: TerminatedBy,
    },
}

/// Why the loop entered `Reflect` (also recorded in the reflection's context).
#[derive(Debug, Clone, Copy)]
pub enum ReflectReason {
    TaskFailed(AtomId),
    CoInstViolation(AtomId),
    ExplicitReplan,
    BudgetPressure,
}

/// How a run ended. `Success` = the search converged (verified work exists), not
/// "problem solved".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminatedBy {
    Success,
    Incomplete,
    DriftExceeded,
    BudgetExceeded(BudgetExceeded),
    BudgetUnavailable(BudgetUnavailable),
    InvalidBudget(BudgetInvalid),
}

impl From<BudgetStop> for TerminatedBy {
    fn from(stop: BudgetStop) -> Self {
        match stop {
            BudgetStop::Exceeded(cap) => Self::BudgetExceeded(cap),
            BudgetStop::UsageUnavailable(reason) => Self::BudgetUnavailable(reason),
            BudgetStop::InvalidConfiguration(reason) => Self::InvalidBudget(reason),
        }
    }
}

/// Tunables for a run. `verifier` None = fall back to a bounded audited critic.
///
/// Build from [`Default`] and override the fields you need, keeping `..Default::default()`.
/// Tunables are added in minor releases, so an exhaustive literal will not keep compiling.
pub struct AgentConfig {
    pub drift_bound: u32,
    pub max_replans: u32,
    pub max_tool_attempts: u32,
    /// Max ReAct rounds per task before failing it (each round is one step()).
    pub max_react_steps: u32,
    /// Prior atoms recall injects per subtask; 0 disables. Recency-free = replay-stable.
    pub recall_context_k: usize,
    /// Recall recipe for the always-on context injected per subtask. Defaults to the
    /// agent-context profile (narrative kinds, recency-disabled for replay stability).
    pub recall_context: RecallProfile,
    pub verifier: Option<Arc<dyn Verifier>>,
    /// Versioned, overridable prompts for the loop's LLM call sites.
    pub prompt_library: Arc<PromptLibrary>,
    /// Discovery candidate generator; required only by run_discovery.
    pub proposal_operator: Option<Arc<dyn ProposalOperator>>,
    /// Max error-feedback repair re-prompts per rejected discovery candidate (0 disables).
    pub max_repairs: u32,
    /// Sampling temperature for control calls.
    pub temperature: f32,
    /// Sampling seed for control calls. Temperature 0 alone does not make a reply
    /// reproducible; `None` leaves the backend free to vary.
    pub seed: Option<u64>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            drift_bound: 5,
            max_replans: 3,
            max_tool_attempts: 3,
            max_react_steps: 6,
            recall_context_k: 5,
            recall_context: RecallProfile::agent_context(),
            verifier: None,
            prompt_library: Arc::new(PromptLibrary::default()),
            proposal_operator: None,
            max_repairs: 2,
            temperature: 0.0,
            seed: Some(1),
        }
    }
}

/// The outcome of a run.
#[derive(Debug, Clone)]
pub struct AgentReport {
    pub goal_id: Option<AtomId>,
    pub final_answer: Option<String>,
    pub tasks_done: u32,
    pub terminated_by: TerminatedBy,
    pub chain_valid: bool,
}

/// Configures a discovery search ([`Agent::run_discovery`]).
pub struct DiscoveryGoal {
    /// `prompt` seeds the proposer; admission is the verifier's, not the prompt's.
    pub goal: Goal,
    /// The atom kind minted when a candidate strictly beats the baseline.
    pub kind: VerifiedKind,
    /// Published baseline a candidate must STRICTLY beat to mint.
    pub baseline_score: f64,
    /// How many top candidates seed each proposal round.
    pub archive_width: usize,
    /// Stop after this many consecutive rounds with no new best (convergence).
    pub max_idle_rounds: u32,
    /// Hard cap on verified mints (overflow skips, never fails the run).
    pub max_mints: u32,
}

/// The outcome of a discovery run.
#[derive(Debug, Clone)]
pub struct DiscoveryReport {
    /// Best score among candidates that cleared the mint bar; NEG_INFINITY if none.
    pub best_score: f64,
    /// The best artifact that cleared the bar, if any.
    pub best_artifact: Option<Value>,
    /// The MAX-SCORE mint's atom; full set is `minted`.
    pub verified: Option<AtomId>,
    /// Every verified atom this run minted, in mint order.
    pub minted: Vec<AtomId>,
    pub proposals: u32,
    pub checker_calls: u32,
    /// Diagnostic: largest valid set seen below the mint bar (0 if none).
    pub best_valid_score: f64,
    /// Diagnostic: valid candidate count (any size).
    pub valid_candidates: u32,
    /// Diagnostic: one example reject reason, or None if all valid.
    pub sample_reject_reason: Option<String>,
    /// Diagnostic: rounds the proposer yielded zero candidates (also logged to stderr).
    pub barren_rounds: u32,
    /// Would-be mints the independent oracle rejected; should be 0 in a healthy run.
    pub cross_check_failures: u32,
    pub terminated_by: TerminatedBy,
    pub chain_valid: bool,
}

/// A single-agent cognition runtime over one memory region.
pub struct Agent {
    llm: Arc<dyn LLMClient>,
    graph: BeliefGraph,
    tools: ToolRegistry,
    budget: AgentBudget,
    config: AgentConfig,
}

impl Agent {
    /// Build an agent. The built-in `request_replan` tool is registered if the
    /// caller's registry does not already provide it.
    pub fn new(
        llm: Arc<dyn LLMClient>,
        graph: BeliefGraph,
        mut tools: ToolRegistry,
        budget: AgentBudget,
        config: AgentConfig,
    ) -> Self {
        if !tools.contains("request_replan") {
            tools.register(Box::new(RequestReplan));
        }
        Self {
            llm,
            graph,
            tools,
            budget,
            config,
        }
    }

    /// Read-only access to the underlying graph (e.g. to `verify_chain` after a run).
    pub fn graph(&self) -> &BeliefGraph {
        &self.graph
    }

    /// Drive the loop to a terminal state. Infra and proven local LLM errors
    /// are `Err`; unavailable provider usage produces an explicit budget stop.
    pub fn run(&self, prompt: impl Into<String>) -> AgentResult<AgentReport> {
        let mut ctx = self.new_ctx(prompt.into());

        let mut state = CognitionState::Plan;
        loop {
            if let CognitionState::Done { terminated_by } = &state {
                return ctx.finish(*terminated_by);
            }
            ctx.usage.wall_secs = ctx.started.elapsed().as_secs();
            // Converge is exempt: already justified, makes no LLM call under an
            // attested verifier; a replan re-trips the guard next iteration.
            if !matches!(state, CognitionState::Converge) {
                if let Err(cap) = ctx.budget.check(&ctx.usage) {
                    state = CognitionState::Done {
                        terminated_by: TerminatedBy::from(cap),
                    };
                    continue;
                }
            }
            state = match ctx.step(state) {
                Ok(state) => state,
                Err(AgentError::Budget(stop)) => CognitionState::Done {
                    terminated_by: stop.into(),
                },
                Err(error) => return Err(error),
            };
            ctx.usage.steps += 1;
        }
    }

    /// Build a fresh per-run context borrowing the agent's components.
    fn new_ctx(&self, prompt: String) -> Ctx<'_> {
        Ctx {
            llm: Arc::clone(&self.llm),
            graph: &self.graph,
            tools: &self.tools,
            budget: &self.budget,
            config: &self.config,
            usage: BudgetUsage::default(),
            started: Instant::now(),
            goal_id: None,
            self_model_id: None,
            drift_count: 0,
            replans_used: 0,
            replan_flag: false,
            prompt,
        }
    }

    /// Run a discovery search (recall -> propose -> check -> mint). Requires a
    /// proposal_operator and a DETERMINISTIC verifier (a critic cannot mint).
    pub fn run_discovery(&self, goal: DiscoveryGoal) -> AgentResult<DiscoveryReport> {
        let mut ctx = self.new_ctx(goal.goal.prompt.clone());
        ctx.discover(goal)
    }
}

/// Record the provider attempt before propagating a response or error. The
/// mandatory token budget cannot authorize another attempt after unknown spend.
fn complete_observed(
    llm: &dyn LLMClient,
    req: &CompletionRequest,
    observe: impl FnOnce(&Result<CompletionResponse, LlmError>) -> AgentResult<()>,
) -> AgentResult<CompletionResponse> {
    let outcome = llm.complete(req);
    observe(&outcome)?;
    outcome.map_err(Into::into)
}

/// Per-run state plus borrowed handles to the agent's components.
struct Ctx<'a> {
    llm: Arc<dyn LLMClient>,
    graph: &'a BeliefGraph,
    tools: &'a ToolRegistry,
    budget: &'a AgentBudget,
    config: &'a AgentConfig,
    usage: BudgetUsage,
    started: Instant,
    goal_id: Option<AtomId>,
    self_model_id: Option<AtomId>,
    drift_count: u32,
    replans_used: u32,
    replan_flag: bool,
    prompt: String,
}

impl Ctx<'_> {
    fn step(&mut self, state: CognitionState) -> AgentResult<CognitionState> {
        match state {
            CognitionState::Plan => self.plan(),
            CognitionState::Execute => self.execute(),
            CognitionState::Reason {
                task,
                round,
                transcript,
            } => self.reason(task, round, transcript),
            CognitionState::Tool {
                task,
                round,
                assistant,
                transcript,
            } => self.tool(task, round, assistant, transcript),
            CognitionState::Observe {
                task,
                round,
                answer,
                results,
                assistant,
                transcript,
            } => self.observe(task, round, answer, results, assistant, transcript),
            CognitionState::Reflect { reason } => self.reflect(reason),
            CognitionState::Converge => self.converge(),
            done @ CognitionState::Done { .. } => Ok(done),
        }
    }

    /// The single LLM chokepoint: calls the backend, accrues budget usage, and
    /// records an immutable `llm_trace` atom for replay/audit.
    fn complete(
        &mut self,
        mut req: CompletionRequest,
        prompt: &ResolvedPrompt,
    ) -> AgentResult<CompletionResponse> {
        // Control calls carry the configured temperature and seed so tool output is
        // reproducible and schema-adherent across backends.
        req.temperature = Some(self.config.temperature);
        req.seed = self.config.seed;
        self.usage.wall_secs = self.started.elapsed().as_secs();
        self.budget.check_llm_call(&self.usage)?;
        let llm = Arc::clone(&self.llm);
        let resp = complete_observed(&*llm, &req, |outcome| {
            let call = RecordedCall::new(&req, 1, outcome);
            self.accrue_and_record(&call, prompt)?;
            // Preserve the attempt before stopping; never dispatch tools or
            // spend again when a configured budget cannot be checked.
            self.usage.wall_secs = self.started.elapsed().as_secs();
            self.budget.check_llm_call(&self.usage)?;
            Ok(())
        })?;
        // A terminal or malformed reply still incurred spend, so trace it.
        let refusal = match resp.finish_reason {
            FinishReason::Stop | FinishReason::Length => None,
            FinishReason::ToolUse if !resp.message.tool_calls.is_empty() => None,
            FinishReason::ToolUse => {
                Some("provider reported tool use without a dispatchable tool call")
            }
            FinishReason::Refusal => Some("provider refused the request"),
            FinishReason::ContentFilter => Some("provider filtered the response content"),
            FinishReason::Error => Some("provider reported an error disposition"),
        };
        if let Some(message) = refusal {
            return Err(LlmError::Backend(message.into()).into());
        }
        Ok(resp)
    }

    /// Record every provider attempt before enforcing its budget consequences.
    fn accrue_and_record(
        &mut self,
        call: &RecordedCall,
        prompt: &ResolvedPrompt,
    ) -> AgentResult<()> {
        call.accrue(&mut self.usage);
        let hash = request_hash(self.llm.model_id(), &call.req);
        let provenance = json!({
            "node": prompt.id.as_str(),
            "version": prompt.version,
            "hash": prompt.hash,
            "source": prompt.source.as_str(),
        });
        self.graph.record_llm_call(
            &hash,
            self.llm.model_id(),
            &call.trace(),
            call.cost(),
            Some(&provenance),
        )?;
        Ok(())
    }

    /// Run repair through the same observed, budgeted channel as proposals.
    /// Preserve every attempt before returning an operator error or budget stop.
    /// Operators without repair support no-op.
    fn repair_candidate(
        &mut self,
        op: &Arc<dyn ProposalOperator>,
        failed: &RejectedCandidate,
        system: &ResolvedPrompt,
        elites: &[Elite],
        dgoal: &DiscoveryGoal,
    ) -> AgentResult<Vec<Candidate>> {
        let log: Rc<RefCell<Vec<RecordedCall>>> = Rc::new(RefCell::new(Vec::new()));
        let fixes = {
            let pctx = ProposalContext {
                goal: &dgoal.goal,
                elites,
                system,
            };
            let channel = OwnedChannel {
                llm: Arc::clone(&self.llm),
                started: self.started,
                budget: *self.budget,
                usage: self.usage,
                log: Rc::clone(&log),
            };
            op.repair(&pctx, failed, Box::new(channel))
        };
        let calls: Vec<RecordedCall> = log.borrow_mut().drain(..).collect();
        for call in &calls {
            self.accrue_and_record(call, system)?;
        }
        self.usage.wall_secs = self.started.elapsed().as_secs();
        self.budget.check_llm_call(&self.usage)?;
        match fixes {
            Ok(fixes) => Ok(fixes),
            Err(ProposeError::Llm(e)) if e.is_retryable() => Ok(Vec::new()),
            Err(e) => Err(AgentError::Other(format!("repair: {e}"))),
        }
    }

    fn plan(&mut self) -> AgentResult<CognitionState> {
        // A replan re-attempts the EXISTING task DAG (reflect() reset unfinished tasks
        // to Pending); re-materializing the planner's tasks would duplicate them. So
        // re-attempt unless no tasks exist yet (a first plan that never materialized).
        if !self.graph.tasks()?.is_empty() {
            return Ok(CognitionState::Execute);
        }
        let sys = self.config.prompt_library.resolve(PromptId::Planner);
        let req = CompletionRequest::new(vec![sys.as_system(), Message::user(self.prompt.clone())])
            .with_tools(vec![submit_plan_spec()])
            .with_tool_choice(ToolChoice::Tool("submit_plan".into()));
        let resp = self.complete(req, &sys)?;

        let plan_args = match resp
            .message
            .tool_calls
            .iter()
            .find(|c| c.name == "submit_plan")
        {
            Some(call) => call.arguments.clone(),
            None => return self.no_plan_outcome(),
        };

        if self.goal_id.is_none() {
            let goal = parse_goal(&plan_args)?;
            let goal_id = self.graph.add_goal(&goal)?;
            let mut sm = SelfModel::new("citadel-agent");
            sm.goal_ref = Some(goal_id);
            let self_model_id = self.graph.set_self_model(&sm)?;
            self.goal_id = Some(goal_id);
            self.self_model_id = Some(self_model_id);
        }
        let goal_id = self.goal_id.expect("goal_id set above");

        let specs = parse_tasks(&plan_args);
        if specs.is_empty() {
            return self.no_plan_outcome();
        }
        let mut ids: Vec<AtomId> = Vec::with_capacity(specs.len());
        for (desc, dep_idx) in &specs {
            let deps: Vec<AtomId> = dep_idx
                .iter()
                .filter_map(|&i| ids.get(i).copied())
                .collect();
            match self
                .graph
                .add_task(&Task::new(desc.clone()), &deps, goal_id)
            {
                Ok(id) => ids.push(id),
                // A planner-produced dependency cycle is a planning failure -> replan.
                Err(GraphError::Mem(MemError::Cycle { .. })) => {
                    return Ok(CognitionState::Reflect {
                        reason: ReflectReason::ExplicitReplan,
                    });
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(CognitionState::Execute)
    }

    /// No tasks produced: converge if anything is already done, else give up.
    fn no_plan_outcome(&self) -> AgentResult<CognitionState> {
        let any_done = self
            .graph
            .tasks()?
            .iter()
            .any(|(_, t)| t.status == TaskStatus::Done);
        Ok(if any_done {
            CognitionState::Converge
        } else {
            CognitionState::Done {
                terminated_by: TerminatedBy::Incomplete,
            }
        })
    }

    fn execute(&mut self) -> AgentResult<CognitionState> {
        if self.budget_pressure()? {
            return Ok(CognitionState::Reflect {
                reason: ReflectReason::BudgetPressure,
            });
        }
        if self.replan_flag {
            self.replan_flag = false;
            return Ok(CognitionState::Reflect {
                reason: ReflectReason::ExplicitReplan,
            });
        }

        let (task_id, task) = match self.graph.next_unblocked_tasks()?.into_iter().next() {
            Some(t) => t,
            None => return self.no_runnable_outcome(),
        };

        let sys = self.config.prompt_library.resolve(PromptId::Execute);
        let context = self.assemble_context(&task, &sys, &[])?;
        let req = CompletionRequest::new(context).with_tools(self.tools.specs());
        let resp = self.complete(req, &sys)?;
        self.graph
            .set_task_status(task_id, TaskStatus::InProgress)?;

        Ok(self.turn_outcome(task_id, 0, Vec::new(), resp))
    }

    /// Continue a single in-progress task for another ReAct round: re-prompt with
    /// the running tool transcript so the model can reason over what it observed.
    /// No task re-selection (the id is carried); each call is one `step()`, so the
    /// global budget guard fires between rounds.
    fn reason(
        &mut self,
        task: AtomId,
        round: u32,
        transcript: Vec<Message>,
    ) -> AgentResult<CognitionState> {
        if self.budget_pressure()? {
            return Ok(CognitionState::Reflect {
                reason: ReflectReason::BudgetPressure,
            });
        }
        let task_atom = self
            .graph
            .get_task(task)?
            .ok_or_else(|| AgentError::Other("reason: task vanished".into()))?;
        let sys = self.config.prompt_library.resolve(PromptId::Execute);
        let context = self.assemble_context(&task_atom, &sys, &transcript)?;
        let req = CompletionRequest::new(context).with_tools(self.tools.specs());
        let resp = self.complete(req, &sys)?;
        Ok(self.turn_outcome(task, round, transcript, resp))
    }

    /// Route one Execute/Reason turn: a tool-call turn goes to `Tool` (carrying the
    /// assistant + transcript), a text-only turn to `Observe` as the final answer.
    fn turn_outcome(
        &self,
        task: AtomId,
        round: u32,
        transcript: Vec<Message>,
        resp: CompletionResponse,
    ) -> CognitionState {
        if resp.message.tool_calls.is_empty() {
            CognitionState::Observe {
                task,
                round,
                answer: Some(resp.message.content),
                results: Vec::new(),
                assistant: None,
                transcript,
            }
        } else {
            CognitionState::Tool {
                task,
                round,
                assistant: resp.message,
                transcript,
            }
        }
    }

    /// Nothing runnable: a failed task means the plan stalled (replan), otherwise
    /// every task is done (converge).
    fn no_runnable_outcome(&self) -> AgentResult<CognitionState> {
        let failed = self
            .graph
            .tasks()?
            .into_iter()
            .find(|(_, t)| t.status == TaskStatus::Failed)
            .map(|(id, _)| id);
        Ok(match failed {
            Some(id) => CognitionState::Reflect {
                reason: ReflectReason::TaskFailed(id),
            },
            None => CognitionState::Converge,
        })
    }

    fn assemble_context(
        &self,
        task: &Task,
        sys: &ResolvedPrompt,
        transcript: &[Message],
    ) -> AgentResult<Vec<Message>> {
        let mut user = String::new();
        if let Some(goal) = self
            .goal_id
            .and_then(|g| self.graph.get_goal(g).ok().flatten())
        {
            user.push_str(&format!("Goal: {}\n", goal.prompt));
        }
        // Semantic recall of the most relevant prior evidence (config-tunable, 0
        // disables). Recency-free, so it does not perturb replay.
        let k = self.config.recall_context_k;
        if k > 0 {
            let recalled = self.graph.recall_relevant_with(
                &task.description,
                k,
                &self.config.recall_context,
            )?;
            if !recalled.is_empty() {
                user.push_str("Relevant context:\n");
                for hit in &recalled {
                    user.push_str(&format!("- {}\n", hit.text));
                }
            }
        }
        user.push_str(&format!("Current subtask: {}", task.description));
        let mut messages = vec![sys.as_system(), Message::user(user)];
        messages.extend(transcript.iter().cloned());
        Ok(messages)
    }

    /// Soft replan signal (distinct from the hard cap): not enough steps left for
    /// the tasks still pending.
    fn budget_pressure(&self) -> AgentResult<bool> {
        let pending = self
            .graph
            .tasks()?
            .iter()
            .filter(|(_, t)| t.status == TaskStatus::Pending)
            .count() as u32;
        let remaining_steps = self.budget.max_steps.saturating_sub(self.usage.steps);
        Ok(pending > 0 && remaining_steps < pending)
    }

    fn tool(
        &mut self,
        task: AtomId,
        round: u32,
        assistant: AssistantMessage,
        transcript: Vec<Message>,
    ) -> AgentResult<CognitionState> {
        let mut results = Vec::with_capacity(assistant.tool_calls.len());
        for call in &assistant.tool_calls {
            let res = self.dispatch_with_retry(call);
            results.push((call.clone(), res));
        }
        Ok(CognitionState::Observe {
            task,
            round,
            answer: None,
            results,
            assistant: Some(assistant),
            transcript,
        })
    }

    fn dispatch_with_retry(&self, call: &ToolCall) -> Result<String, ToolError> {
        // Only read-only tools retry; a side-effecting tool (write file, run process,
        // write memory) dispatches once so a transient error can't double-apply.
        let attempts = if self.is_mutating_tool(&call.name) {
            1
        } else {
            self.config.max_tool_attempts.max(1)
        };
        let mut last = self.tools.dispatch(call);
        let mut n = 1;
        while last.is_err() && n < attempts {
            last = self.tools.dispatch(call);
            n += 1;
        }
        last
    }

    /// Whether a tool may cause a non-idempotent side effect (so it must not retry):
    /// known memory mutations, or a declared filesystem write path / exec policy.
    /// Reads the tool's own [`ToolPermissions`], so any write/exec tool is covered.
    fn is_mutating_tool(&self, name: &str) -> bool {
        if is_known_memory_mutation(name) {
            return true;
        }
        match self.tools.permissions(name) {
            Some(perms) => {
                matches!(&perms.filesystem, FsPolicy::AllowPaths { write, .. } if !write.is_empty())
                    || matches!(perms.exec, ExecPolicy::AllowPrograms { .. })
            }
            None => false,
        }
    }

    fn observe(
        &mut self,
        task: AtomId,
        round: u32,
        answer: Option<String>,
        results: Vec<(ToolCall, Result<String, ToolError>)>,
        assistant: Option<AssistantMessage>,
        transcript: Vec<Message>,
    ) -> AgentResult<CognitionState> {
        let goal_id = self
            .goal_id
            .ok_or_else(|| AgentError::Other("observe before goal".into()))?;
        let self_model_id = self
            .self_model_id
            .ok_or_else(|| AgentError::Other("observe before self-model".into()))?;

        let mut dispatched: Vec<ToolCall> = Vec::with_capacity(results.len());
        let mut any_failure = false;
        let mut replan_requested = false;
        for (call, res) in &results {
            if call.name == "request_replan" {
                replan_requested = true;
            }
            dispatched.push(call.clone());
            match res {
                Ok(out) => {
                    self.graph.add_evidence(
                        &Evidence {
                            source: call.name.clone(),
                            content: out.clone(),
                        },
                        task,
                    )?;
                }
                Err(e) => {
                    any_failure = true;
                    self.graph.add_evidence(
                        &Evidence {
                            source: call.name.clone(),
                            content: format!("tool error: {e}"),
                        },
                        task,
                    )?;
                }
            }
        }
        if let Some(text) = &answer {
            self.graph.add_evidence(
                &Evidence {
                    source: "answer".into(),
                    content: text.clone(),
                },
                task,
            )?;
        }

        let goal = self
            .graph
            .get_goal(goal_id)?
            .ok_or_else(|| AgentError::Other("goal vanished".into()))?;
        let has_provenance = self.graph.has_provenance(task, goal_id)?;
        let constraints_ok = self.constraints_satisfied(&goal, &dispatched)?;
        let check = CoInstantiationCheck::new(
            format!("observe_task_{task}"),
            goal_id,
            self_model_id,
            has_provenance,
            constraints_ok,
            self.drift_count,
            self.config.drift_bound,
        );
        let verdict = check.verdict;
        self.graph.record_check(check, task)?; // RECORD-BEFORE-ABORT

        // Verdict wins over continue: a Drift/Violation this round preempts feeding results back.
        match verdict {
            Verdict::Drift => {
                return Ok(CognitionState::Done {
                    terminated_by: TerminatedBy::DriftExceeded,
                })
            }
            Verdict::Violation => {
                self.drift_count += 1;
                return Ok(CognitionState::Reflect {
                    reason: ReflectReason::CoInstViolation(task),
                });
            }
            Verdict::Pass => {}
        }

        // A deterministically verified goal converges NOW, even with Pending siblings
        // (verified acceptance wins). converge() stays the sole minter of Success.
        if self.acceptance_verified(&goal)? {
            self.graph.set_task_status(task, TaskStatus::Done)?;
            return Ok(CognitionState::Converge);
        }

        // An explicit replan request defers to Execute, which consumes the flag.
        if replan_requested {
            self.replan_flag = true;
            return Ok(CognitionState::Execute);
        }

        // A text answer (no tool calls) is the done-signal: close the task.
        if answer.is_some() {
            self.graph.set_task_status(task, TaskStatus::Done)?;
            return Ok(CognitionState::Execute);
        }

        // Otherwise a tool round: feed results back and reason again (a tool error
        // gets a bounded read-then-fix), or fail the task at the ReAct cap.
        let assistant = match assistant {
            Some(a) => a,
            // turn_outcome guarantees answer XOR assistant, so this is unreachable.
            // Loud in tests; close defensively in release (no InProgress limbo).
            None => {
                debug_assert!(
                    false,
                    "observe: Pass round with neither answer nor assistant"
                );
                self.graph.set_task_status(task, TaskStatus::Done)?;
                return Ok(CognitionState::Execute);
            }
        };
        if round + 1 < self.config.max_react_steps {
            let transcript = extend_transcript(transcript, assistant, results);
            Ok(CognitionState::Reason {
                task,
                round: round + 1,
                transcript,
            })
        } else {
            let reason = if any_failure {
                "tool call failed after retries; react budget exhausted"
            } else {
                "react inner loop exceeded max_react_steps"
            };
            self.graph.record_task_failure(task, reason)?;
            Ok(CognitionState::Reflect {
                reason: ReflectReason::TaskFailed(task),
            })
        }
    }

    /// `structural AND (verifier | critic)`. The critic fires only when free-text
    /// constraints exist; a constraint `VerifyError` fails OPEN (drift still catches a breach).
    fn constraints_satisfied(&mut self, goal: &Goal, dispatched: &[ToolCall]) -> AgentResult<bool> {
        if !structural_constraints_ok(self.tools, &goal.constraints, dispatched) {
            return Ok(false);
        }
        if goal.constraints.is_empty() {
            return Ok(true);
        }
        if let Some(verifier) = self.config.verifier.clone() {
            return Ok(
                match verifier.verify(&VerifyRequest {
                    kind: VerifyKind::Constraint,
                    goal,
                    tool_calls: dispatched,
                    evidence: &[],
                }) {
                    Ok(outcome) => outcome.satisfied,
                    Err(_) => true, // fail-OPEN
                },
            );
        }
        self.constraint_critic(goal, dispatched)
    }

    fn constraint_critic(&mut self, goal: &Goal, calls: &[ToolCall]) -> AgentResult<bool> {
        let names: Vec<&str> = calls.iter().map(|c| c.name.as_str()).collect();
        let prompt = format!(
            "Constraints: {:?}\nDispatched tools: {:?}\nDo the tools comply with the constraints?",
            goal.constraints, names
        );
        let sys = self
            .config
            .prompt_library
            .resolve(PromptId::ConstraintCritic);
        let req = CompletionRequest::new(vec![sys.as_system(), Message::user(prompt)])
            .with_tools(vec![verdict_spec()])
            .with_tool_choice(ToolChoice::Tool("verdict".into()));
        let resp = self.complete(req, &sys)?;
        // No verdict -> lenient (the structural pass already gate-kept the call).
        Ok(match parse_verdict(&resp) {
            Some((satisfied, _)) => satisfied,
            None => true,
        })
    }

    fn reflect(&mut self, reason: ReflectReason) -> AgentResult<CognitionState> {
        let insight = self.reflect_insight(reason)?;
        if let Some(goal_id) = self.goal_id {
            self.graph.add_reflection(
                &Reflection {
                    insight,
                    confidence: 0.5,
                },
                goal_id,
            )?;
        }
        if self.replans_used < self.config.max_replans {
            self.replans_used += 1;
            for (id, task) in self.graph.tasks()? {
                if matches!(task.status, TaskStatus::InProgress | TaskStatus::Failed) {
                    self.graph.set_task_status(id, TaskStatus::Pending)?;
                }
            }
            Ok(CognitionState::Plan)
        } else {
            Ok(CognitionState::Done {
                terminated_by: TerminatedBy::Incomplete,
            })
        }
    }

    fn reflect_insight(&mut self, reason: ReflectReason) -> AgentResult<String> {
        let sys = self.config.prompt_library.resolve(PromptId::Reflect);
        let req = CompletionRequest::new(vec![
            sys.as_system(),
            Message::user(format!("Situation: {reason:?}. How should the plan adapt?")),
        ]);
        let resp = self.complete(req, &sys)?;
        Ok(resp.message.content)
    }

    /// Has the goal's acceptance been deterministically verified THIS round? Lets
    /// `observe` route to Converge the instant an attested checker certifies it, even
    /// with Pending siblings. Fail-CLOSED; a critic (no attestation) may not self-close.
    fn acceptance_verified(&self, goal: &Goal) -> AgentResult<bool> {
        if goal.acceptance_criteria.is_empty() {
            return Ok(false);
        }
        let Some(verifier) = self.config.verifier.clone() else {
            return Ok(false);
        };
        if verifier.attestation().is_none() {
            return Ok(false); // a critic LLM may not self-close the goal
        }
        let goal_id = self
            .goal_id
            .ok_or_else(|| AgentError::Other("acceptance before goal".into()))?;
        let evidence = self.graph.evidence_for_goal(goal_id)?;
        Ok(matches!(
            verifier.verify(&VerifyRequest {
                kind: VerifyKind::Acceptance,
                goal,
                tool_calls: &[],
                evidence: &evidence,
            }),
            Ok(o) if o.satisfied
        ))
    }

    fn converge(&mut self) -> AgentResult<CognitionState> {
        let goal_id = match self.goal_id {
            Some(g) => g,
            None => {
                return Ok(CognitionState::Done {
                    terminated_by: TerminatedBy::Incomplete,
                })
            }
        };
        let self_model_id = self
            .self_model_id
            .ok_or_else(|| AgentError::Other("converge before self-model".into()))?;
        let goal = self
            .graph
            .get_goal(goal_id)?
            .ok_or_else(|| AgentError::Other("goal vanished".into()))?;

        // Attested verifier is sole authority (runs even with no criteria,
        // fail-closed); else empty-criteria finishes or the critic judges.
        let evidence = self.graph.evidence_for_goal(goal_id)?;
        let met = match self.config.verifier.clone() {
            Some(v) if v.attestation().is_some() => verifier_accepts(v.as_ref(), &goal, &evidence),
            _ if goal.acceptance_criteria.is_empty() => true,
            Some(v) => verifier_accepts(v.as_ref(), &goal, &evidence),
            None => self.acceptance_critic(&goal, &evidence)?,
        };

        // Record the acceptance decision in the same audit chain (anchor = goal).
        let check = CoInstantiationCheck::new(
            format!("converge_goal_{goal_id}"),
            goal_id,
            self_model_id,
            true,
            met,
            0,
            self.config.drift_bound,
        );
        self.graph.record_check(check, goal_id)?;

        if met {
            self.graph.set_goal_status(goal_id, GoalStatus::Achieved)?;
            Ok(CognitionState::Done {
                terminated_by: TerminatedBy::Success,
            })
        } else if self.replans_used < self.config.max_replans {
            self.replans_used += 1;
            Ok(CognitionState::Plan)
        } else {
            Ok(CognitionState::Done {
                terminated_by: TerminatedBy::Incomplete,
            })
        }
    }

    fn acceptance_critic(
        &mut self,
        goal: &Goal,
        evidence: &[(String, String)],
    ) -> AgentResult<bool> {
        // Readable (not Debug-escaped) so the critic can read test-runner output.
        let criteria = goal.acceptance_criteria.join("; ");
        let evidence_text = if evidence.is_empty() {
            "(no evidence gathered)".to_string()
        } else {
            evidence
                .iter()
                .map(|(source, content)| format!("--- {source} ---\n{content}"))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let prompt = format!(
            "Goal: {}\nAcceptance criteria: {}\nEvidence from the agent's actions:\n{}",
            goal.prompt, criteria, evidence_text
        );
        let sys = self
            .config
            .prompt_library
            .resolve(PromptId::AcceptanceCritic);
        let req = CompletionRequest::new(vec![sys.as_system(), Message::user(prompt)])
            .with_tools(vec![verdict_spec()])
            .with_tool_choice(ToolChoice::Tool("verdict".into()));
        let resp = self.complete(req, &sys)?;
        // No verdict -> conservative: acceptance is not met.
        Ok(match parse_verdict(&resp) {
            Some((satisfied, _)) => satisfied,
            None => false,
        })
    }

    fn finish(&self, terminated_by: TerminatedBy) -> AgentResult<AgentReport> {
        if let Some(goal_id) = self.goal_id {
            // Converge already set Achieved on Success; record the others.
            let status = match terminated_by {
                TerminatedBy::Success => None,
                TerminatedBy::DriftExceeded => Some(GoalStatus::Abandoned),
                TerminatedBy::Incomplete
                | TerminatedBy::BudgetExceeded(_)
                | TerminatedBy::BudgetUnavailable(_)
                | TerminatedBy::InvalidBudget(_) => Some(GoalStatus::Active),
            };
            if let Some(status) = status {
                self.graph.set_goal_status(goal_id, status)?;
            }
        }

        let tasks = self.graph.tasks()?;
        let tasks_done = tasks
            .iter()
            .filter(|(_, t)| t.status == TaskStatus::Done)
            .count() as u32;
        let final_answer = match self.goal_id {
            Some(g) => self
                .graph
                .evidence_for_goal(g)?
                .into_iter()
                .rev()
                .find(|(source, _)| source == "answer")
                .map(|(_, content)| content),
            None => None,
        };
        let chain_valid = self.graph.verify_chain()?.valid;

        Ok(AgentReport {
            goal_id: self.goal_id,
            final_answer,
            tasks_done,
            terminated_by,
            chain_valid,
        })
    }
}

/// Whether `v` accepts the goal given the gathered evidence (fail-closed: an error is
/// not acceptance). The shared acceptance check for `converge`'s verifier arms.
fn verifier_accepts(v: &dyn Verifier, goal: &Goal, evidence: &[(String, String)]) -> bool {
    matches!(
        v.verify(&VerifyRequest {
            kind: VerifyKind::Acceptance,
            goal,
            tool_calls: &[],
            evidence,
        }),
        Ok(o) if o.satisfied
    )
}

/// Append one ReAct round to the transcript: the assistant turn, then each tool
/// result keyed by `call_id` (errors marked `is_error`).
fn extend_transcript(
    mut transcript: Vec<Message>,
    assistant: AssistantMessage,
    results: Vec<(ToolCall, Result<String, ToolError>)>,
) -> Vec<Message> {
    transcript.push(Message::Assistant(assistant));
    for (call, res) in results {
        let (content, is_error) = match res {
            Ok(out) => (out, false),
            Err(e) => (format!("tool error: {e}"), true),
        };
        transcript.push(Message::Tool {
            call_id: call.id,
            content,
            is_error,
        });
    }
    transcript
}

/// Built-in no-op tool: the model calls it to ask the planner to revise the plan.
struct RequestReplan;

impl Tool for RequestReplan {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "request_replan".into(),
            description: "Ask the planner to revise the current plan.".into(),
            input_schema: json!({ "type": "object" }),
        }
    }
    fn permissions(&self) -> ToolPermissions {
        ToolPermissions::default()
    }
    fn call(&self, _args: &Value) -> Result<String, ToolError> {
        Ok(json!({ "replan": true }).to_string())
    }
}

fn submit_plan_spec() -> ToolSpec {
    ToolSpec {
        name: "submit_plan".into(),
        description: "Submit the goal and the ordered subtasks.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "goal": {
                    "type": "object",
                    "properties": {
                        "prompt": { "type": "string" },
                        "acceptance_criteria": { "type": "array", "items": { "type": "string" } },
                        "constraints": { "type": "array", "items": { "type": "string" } }
                    }
                },
                "tasks": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "description": { "type": "string" },
                            "deps": { "type": "array", "items": { "type": "integer" } }
                        }
                    }
                }
            },
            "required": ["goal", "tasks"]
        }),
    }
}

fn verdict_spec() -> ToolSpec {
    ToolSpec {
        name: "verdict".into(),
        description: "Return a structured verdict.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "satisfied": { "type": "boolean" },
                "reason": { "type": "string" }
            },
            "required": ["satisfied"]
        }),
    }
}

fn parse_goal(args: &Value) -> AgentResult<Goal> {
    let raw = args
        .get("goal")
        .ok_or_else(|| AgentError::Other("plan missing 'goal'".into()))?;
    // Tolerate a model that STRINGIFIED the whole goal object (the same quirk
    // run_command handles): parse it back if `goal` arrived as a JSON string.
    let unstringified = raw
        .as_str()
        .and_then(|s| serde_json::from_str::<Value>(s).ok());
    let goal = unstringified.as_ref().unwrap_or(raw);
    let prompt = goal
        .get("prompt")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let mut out = Goal::new(prompt);
    out.acceptance_criteria = str_array(goal, "acceptance_criteria");
    out.constraints = str_array(goal, "constraints");
    Ok(out)
}

/// `(description, dependency indices)` for each task, in submitted order.
fn parse_tasks(args: &Value) -> Vec<(String, Vec<usize>)> {
    array_field(args, "tasks")
        .map(|tasks| {
            tasks
                .iter()
                .filter_map(|t| {
                    let desc = t.get("description").and_then(Value::as_str)?.to_string();
                    let deps = array_field(t, "deps")
                        .map(|d| {
                            d.iter()
                                .filter_map(|x| x.as_u64().map(|n| n as usize))
                                .collect()
                        })
                        .unwrap_or_default();
                    Some((desc, deps))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn str_array(v: &Value, key: &str) -> Vec<String> {
    array_field(v, key)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// `parent[key]` as a JSON array, tolerating a model that STRINGIFIED it ("[\"x\"]"
/// instead of `["x"]`). `None` if absent or neither an array nor a string parsing to one.
fn array_field(parent: &Value, key: &str) -> Option<Vec<Value>> {
    match parent.get(key)? {
        Value::Array(a) => Some(a.clone()),
        Value::String(s) => match serde_json::from_str::<Value>(s) {
            Ok(Value::Array(a)) => Some(a),
            _ => None,
        },
        _ => None,
    }
}

fn parse_verdict(resp: &CompletionResponse) -> Option<(bool, String)> {
    let call = resp
        .message
        .tool_calls
        .iter()
        .find(|c| c.name == "verdict")?;
    let satisfied = call.arguments.get("satisfied").and_then(Value::as_bool)?;
    let reason = call
        .arguments
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Some((satisfied, reason))
}

/// What a barren-round diagnostic needs from the round's last model reply
/// (raw replies are otherwise consumed inside the proposal operator).
struct ReplyDigest {
    head: String,
    tool_calls: usize,
    finish_reason: FinishReason,
}

/// Enough of the reply text to show WHAT failed to parse, without dumping it.
const REPLY_DIGEST_HEAD_CHARS: usize = 200;

impl ReplyDigest {
    fn of(resp: &CompletionResponse) -> Self {
        Self {
            head: resp
                .message
                .content
                .chars()
                .take(REPLY_DIGEST_HEAD_CHARS)
                .collect(),
            tool_calls: resp.message.tool_calls.len(),
            finish_reason: resp.finish_reason,
        }
    }
}

/// Every provider attempt is buffered, including failures without known usage.
struct RecordedCall {
    req: CompletionRequest,
    attempt: u32,
    outcome: RecordedOutcome,
}

enum RecordedOutcome {
    Response(CompletionResponse),
    Failure { error: Value, pre_dispatch: bool },
}

impl RecordedCall {
    fn new(
        req: &CompletionRequest,
        attempt: u32,
        outcome: &Result<CompletionResponse, LlmError>,
    ) -> Self {
        let outcome = match outcome {
            Ok(response) => RecordedOutcome::Response(response.clone()),
            Err(error) => RecordedOutcome::Failure {
                error: error_to_value(error),
                pre_dispatch: error.is_pre_dispatch(),
            },
        };
        Self {
            req: req.clone(),
            attempt,
            outcome,
        }
    }

    fn response(&self) -> Option<&CompletionResponse> {
        match &self.outcome {
            RecordedOutcome::Response(response) => Some(response),
            RecordedOutcome::Failure { .. } => None,
        }
    }

    fn accrue(&self, usage: &mut BudgetUsage) {
        match &self.outcome {
            RecordedOutcome::Response(response) => usage.accrue(response.usage),
            RecordedOutcome::Failure {
                pre_dispatch: false,
                ..
            } => usage.accrue(None),
            RecordedOutcome::Failure {
                pre_dispatch: true, ..
            } => {}
        }
    }

    fn cost(&self) -> Option<f64> {
        match &self.outcome {
            RecordedOutcome::Response(response) => {
                response.usage.and_then(|u| valid_cost(u.cost_usd))
            }
            RecordedOutcome::Failure {
                pre_dispatch: true, ..
            } => Some(0.0),
            RecordedOutcome::Failure {
                pre_dispatch: false,
                ..
            } => None,
        }
    }

    fn trace(&self) -> Value {
        let mut trace = match &self.outcome {
            RecordedOutcome::Response(response) => response_to_value(response),
            RecordedOutcome::Failure {
                error,
                pre_dispatch,
            } => json!({
                "error": error, "pre_dispatch": pre_dispatch, "usage": null,
            }),
        };
        trace["attempt"] = json!(self.attempt);
        trace
    }
}

fn error_to_value(error: &LlmError) -> Value {
    match error {
        LlmError::UnsupportedRequest(message) => {
            json!({"kind":"unsupported_request", "message":message})
        }
        LlmError::Backend(message) => json!({"kind":"backend", "message":message}),
        LlmError::Transport(message) => json!({"kind":"transport", "message":message}),
        LlmError::Http {
            status,
            retry_after,
            message,
        } => json!({"kind":"http", "status":status, "retry_after":retry_after, "message":message}),
    }
}

fn value_to_error(value: &Value) -> LlmError {
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("recorded provider failure")
        .to_owned();
    match value.get("kind").and_then(Value::as_str) {
        Some("unsupported_request") => LlmError::UnsupportedRequest(message),
        Some("transport") => LlmError::Transport(message),
        Some("http") => LlmError::Http {
            status: value
                .get("status")
                .and_then(Value::as_u64)
                .and_then(|s| u16::try_from(s).ok())
                .unwrap_or(0),
            retry_after: value.get("retry_after").and_then(Value::as_u64),
            message,
        },
        _ => LlmError::Backend(message),
    }
}

/// The OWNED one-shot LLM channel handed to a proposal operator. It runs each call
/// through the same observed attempt path as the cognition loop and BUFFERS every
/// (request, response) into a shared log; the controller drains the log after
/// `propose` to accrue budget and record the trace. Decoupled from `Ctx` (the
/// controller no longer completes inline) so the channel is `'static` and can be
/// wrapped for another language runtime. Single-threaded by
/// construction (`Rc`/`RefCell`), matching the sequential discovery loop.
struct OwnedChannel {
    llm: Arc<dyn LLMClient>,
    started: Instant,
    budget: AgentBudget,
    usage: BudgetUsage,
    log: Rc<RefCell<Vec<RecordedCall>>>,
}

impl Completer for OwnedChannel {
    fn complete(&mut self, req: &CompletionRequest) -> Result<CompletionResponse, ProposeError> {
        self.usage.wall_secs = self.started.elapsed().as_secs();
        self.budget
            .check_llm_call(&self.usage)
            .map_err(|stop| ProposeError::Failed(stop.to_string()))?;
        let llm = Arc::clone(&self.llm);
        complete_observed(&*llm, req, |outcome| {
            let call = RecordedCall::new(req, 1, outcome);
            call.accrue(&mut self.usage);
            self.log.borrow_mut().push(call);
            self.usage.wall_secs = self.started.elapsed().as_secs();
            self.budget.check_llm_call(&self.usage)?;
            Ok(())
        })
        .map_err(|error| match error {
            AgentError::Llm(error) => ProposeError::Llm(error),
            error => ProposeError::Failed(error.to_string()),
        })
    }
}

impl Ctx<'_> {
    /// The discovery search loop (see [`Agent::run_discovery`]). Each round seeds the
    /// proposer from the elite archive, scores candidates with the deterministic
    /// verifier, archives valid ones, mints a `verified_*` above baseline. Budget-bounded.
    fn discover(&mut self, dgoal: DiscoveryGoal) -> AgentResult<DiscoveryReport> {
        // Elites span the never-regress verified records and working candidates, so a
        // checked best always seeds future rounds. Kinds derive from their sources.
        const ELITE_KINDS: [&str; 3] = [
            VerifiedKind::Construction.as_str(),
            VerifiedKind::Lemma.as_str(),
            CANDIDATE_KIND,
        ];

        let op = self.config.proposal_operator.clone().ok_or_else(|| {
            AgentError::Other("run_discovery requires a proposal_operator".into())
        })?;
        let verifier = self
            .config
            .verifier
            .clone()
            .ok_or_else(|| AgentError::Other("run_discovery requires a verifier".into()))?;
        let attestation = verifier.attestation().ok_or_else(|| {
            AgentError::Other(
                "run_discovery verifier must be a deterministic checker (attestation is None)"
                    .into(),
            )
        })?;
        let system = self.config.prompt_library.resolve(PromptId::Proposer);

        let mut best_score = f64::NEG_INFINITY;
        let mut best_artifact: Option<Value> = None;
        // Per-cell mint bars so diverse candidates stop blocking each other;
        // max_mints caps the run.
        let mut bars: FxHashMap<String, f64> = FxHashMap::default();
        let mut minted: Vec<AtomId> = Vec::new();
        let mut best_verified: Option<(f64, AtomId)> = None;
        let mut cross_check_failures = 0u32;
        let mut idle = 0u32;
        // Above-bar candidates queue and flush in ONE batched cross-check;
        // flushed before any budget break so no earned mint is dropped.
        let mut mint_queue: Vec<PendingMint> = Vec::new();
        // Diagnostics only (do not affect the search).
        let mut best_valid_score = 0.0f64;
        let mut valid_candidates = 0u32;
        let mut sample_reject_reason: Option<String> = None;
        let mut barren_rounds = 0u32;

        let terminated_by = 'search: loop {
            self.usage.wall_secs = self.started.elapsed().as_secs();
            if let Err(cap) = self.budget.check(&self.usage) {
                break 'search TerminatedBy::from(cap);
            }

            let elites: Vec<Elite> = self
                .graph
                .top_scored(&ELITE_KINDS, dgoal.archive_width)?
                .into_iter()
                .map(|(atom, text, score)| Elite {
                    atom,
                    artifact: serde_json::from_str(&text).unwrap_or(Value::Null),
                    score,
                })
                .collect();

            self.usage.proposals += 1;
            let log: Rc<RefCell<Vec<RecordedCall>>> = Rc::new(RefCell::new(Vec::new()));
            // Every channel attempt is recorded. A retryable error returned by the
            // operator itself can enter the idle path; unknown provider spend stops
            // the budget check before any candidates or another round are accepted.
            let mut unreachable: Option<String> = None;
            let candidates = {
                let pctx = ProposalContext {
                    goal: &dgoal.goal,
                    elites: &elites,
                    system: &system,
                };
                let channel = OwnedChannel {
                    llm: Arc::clone(&self.llm),
                    started: self.started,
                    budget: *self.budget,
                    usage: self.usage,
                    log: Rc::clone(&log),
                };
                op.propose(&pctx, Box::new(channel))
            };
            // Drain the buffered calls: accrue budget + record the replay trace (the
            // accounting the controller used to do inline), then summarize the last reply.
            let calls: Vec<RecordedCall> = log.borrow_mut().drain(..).collect();
            let mut last_reply: Option<ReplyDigest> = None;
            for call in &calls {
                self.accrue_and_record(call, &system)?;
                last_reply = call.response().map(ReplyDigest::of);
            }
            self.usage.wall_secs = self.started.elapsed().as_secs();
            if let Err(stop) = self.budget.check_llm_call(&self.usage) {
                break 'search stop.into();
            }
            let candidates = match candidates {
                Ok(candidates) => candidates,
                Err(ProposeError::Llm(e)) if e.is_retryable() => {
                    unreachable = Some(e.to_string());
                    Vec::new()
                }
                Err(e) => return Err(AgentError::Other(format!("proposer: {e}"))),
            };

            // Barren rounds are LOUD: a $-burning structural failure (every reply
            // unparseable) must be visible per round, not after the budget dies.
            if candidates.is_empty() {
                barren_rounds += 1;
                let round = self.usage.proposals;
                match (&unreachable, &last_reply) {
                    (Some(err), _) => eprintln!(
                        "[discovery] round {round}: proposer LLM unreachable \
                         (transient: {err}); idle round, continuing"
                    ),
                    (None, Some(r)) => eprintln!(
                        "[discovery] barren round {round}: 0 candidates \
                         (finish={:?}, tool_calls={}, text head: {:?})",
                        r.finish_reason, r.tool_calls, r.head
                    ),
                    (None, None) => eprintln!(
                        "[discovery] barren round {round}: operator yielded 0 \
                         candidates without an LLM reply"
                    ),
                }
            }
            let mut improved = false;
            let mut round_stop = None;
            // FIFO worklist: fresh candidates keep proposed order (mint bars are
            // arrival-ordered); a rejected candidate is re-proposed with the kernel error
            // and its fix re-enters with one less repair. No-op for operators without
            // repair support.
            let mut work: VecDeque<(Candidate, u32)> = candidates
                .into_iter()
                .map(|c| (c, self.config.max_repairs))
                .collect();
            while let Some((cand, repairs_left)) = work.pop_front() {
                // Check before increment so max_checker_calls = N permits exactly
                // N calls; flush first so no earned mint is dropped.
                self.usage.wall_secs = self.started.elapsed().as_secs();
                if let Err(cap) = self.budget.check(&self.usage) {
                    flush_mints(
                        &mut mint_queue,
                        verifier.as_ref(),
                        &dgoal,
                        self.graph,
                        &attestation,
                        MintLedger {
                            bars: &mut bars,
                            minted: &mut minted,
                            best_verified: &mut best_verified,
                            cross_check_failures: &mut cross_check_failures,
                        },
                    )?;
                    break 'search TerminatedBy::from(cap);
                }
                self.usage.checker_calls += 1;
                let artifact = serde_json::to_string(&cand.artifact).unwrap_or_default();
                let evidence = [("candidate".to_string(), artifact.clone())];
                let scored = verifier
                    .score(&VerifyRequest {
                        kind: VerifyKind::Rank,
                        goal: &dgoal.goal,
                        tool_calls: &[],
                        evidence: &evidence,
                    })
                    .map_err(|e| AgentError::Other(format!("verifier: {e}")))?;
                // Diagnostic: a valid set scores |A|>0 (invalid = 0); capture the best
                // valid set + one reject reason even when nothing clears the mint bar.
                if scored.score.is_finite() && scored.score > 0.0 {
                    valid_candidates += 1;
                    best_valid_score = best_valid_score.max(scored.score);
                } else if sample_reject_reason.is_none() {
                    sample_reject_reason = Some(scored.reason.clone());
                }
                // Skip invalid, below-floor, or (per ScoredOutcome's contract)
                // non-finite scores - never archive or rank them. Error-feedback: hand
                // the rejection back to the operator for a bounded fix.
                if !scored.satisfied || !scored.score.is_finite() {
                    if repairs_left > 0 {
                        let rejected = RejectedCandidate {
                            artifact: cand.artifact.clone(),
                            reason: scored.reason.clone(),
                        };
                        let fixes =
                            match self.repair_candidate(&op, &rejected, &system, &elites, &dgoal) {
                                Ok(fixes) => fixes,
                                Err(AgentError::Budget(stop)) => {
                                    round_stop = Some(TerminatedBy::from(stop));
                                    break;
                                }
                                Err(error) => return Err(error),
                            };
                        for fix in fixes {
                            work.push_back((fix, repairs_left - 1));
                        }
                    }
                    continue;
                }
                let atom = self.graph.add_candidate(&artifact, scored.score)?;
                if scored.score > best_score {
                    best_score = scored.score;
                    best_artifact = Some(cand.artifact.clone());
                    improved = true;
                }
                let bar = bars
                    .get(&scored.cell)
                    .copied()
                    .unwrap_or(dgoal.baseline_score);
                if scored.score > bar {
                    mint_queue.push(PendingMint {
                        atom,
                        artifact,
                        score: scored.score,
                        cell: scored.cell,
                        terminal: scored.terminal,
                    });
                }
            }

            // End-of-round flush before idle bookkeeping, so a converging final round still mints.
            self.usage.wall_secs = self.started.elapsed().as_secs();
            let terminal_minted = flush_mints(
                &mut mint_queue,
                verifier.as_ref(),
                &dgoal,
                self.graph,
                &attestation,
                MintLedger {
                    bars: &mut bars,
                    minted: &mut minted,
                    best_verified: &mut best_verified,
                    cross_check_failures: &mut cross_check_failures,
                },
            )?;
            if let Some(stop) = round_stop {
                break 'search stop;
            }
            // A minted TERMINAL candidate ends a directed search (its proof is stamped).
            if terminal_minted {
                break 'search TerminatedBy::Success;
            }

            // Barren rounds count as idle, so a broken proposer dies after max_idle_rounds.
            idle = if improved { 0 } else { idle + 1 };
            // Per-round heartbeat: the LLM call and kernel checks are otherwise silent.
            eprintln!(
                "[discovery] round {}: valid {}, minted {}, best {:.3}, idle {}/{}",
                self.usage.proposals,
                valid_candidates,
                minted.len(),
                best_score,
                idle,
                dgoal.max_idle_rounds
            );
            // Converge after max_idle_rounds, but only with a real best (else Incomplete).
            if !improved && idle >= dgoal.max_idle_rounds {
                break 'search if best_score.is_finite() {
                    TerminatedBy::Success
                } else {
                    TerminatedBy::Incomplete
                };
            }
        };

        let chain_valid = self.graph.verify_chain()?.valid;
        Ok(DiscoveryReport {
            best_score,
            best_artifact,
            verified: best_verified.map(|(_, atom)| atom),
            minted,
            proposals: self.usage.proposals,
            checker_calls: self.usage.checker_calls,
            best_valid_score,
            valid_candidates,
            sample_reject_reason,
            barren_rounds,
            cross_check_failures,
            terminated_by,
            chain_valid,
        })
    }
}

/// An above-bar discovery candidate awaiting the end-of-round mint flush.
struct PendingMint {
    atom: AtomId,
    artifact: String,
    score: f64,
    cell: String,
    terminal: bool,
}

/// The mutable mint state a flush advances: the per-cell score bars, every
/// minted atom (`minted[0]` need not be the best; see `best_verified`), and the
/// failed-cross-check counter.
struct MintLedger<'a> {
    bars: &'a mut FxHashMap<String, f64>,
    minted: &'a mut Vec<AtomId>,
    best_verified: &'a mut Option<(f64, AtomId)>,
    cross_check_failures: &'a mut u32,
}

impl MintLedger<'_> {
    /// The score a candidate in `cell` must strictly beat: the cell's best mint,
    /// or the published baseline while the cell is unminted. Every cell starts
    /// at the SAME version-pinned baseline - diversity earns extra mints, never
    /// a lower bar.
    fn bar(&self, cell: &str, baseline: f64) -> f64 {
        self.bars.get(cell).copied().unwrap_or(baseline)
    }
}

/// Flush the mint queue through ONE batched cross-check. Consume order is
/// arrival order with each member's PER-CELL bar re-applied, which reproduces
/// the immediate-mint minted set exactly (same-cell arrivals 5, 3, 7 over bar 0
/// mint {5, 7}); a member skipped by its bar or by the `max_mints` cap
/// contributes to neither counter (cell diversity can never inflate the mint
/// count past the cap). An `Err` from the batch aborts the run loudly - no
/// verdicts, nothing stamped (fail closed). Returns whether a TERMINAL member
/// minted: the directed-run stop signal (the search converged on its target;
/// the claim itself lives in the minted evidence records, never in this bool).
fn flush_mints(
    queue: &mut Vec<PendingMint>,
    verifier: &dyn Verifier,
    dgoal: &DiscoveryGoal,
    graph: &BeliefGraph,
    attestation: &CheckerAttestation,
    ledger: MintLedger<'_>,
) -> AgentResult<bool> {
    if queue.is_empty() {
        return Ok(false);
    }
    let evidence: Vec<[(String, String); 1]> = queue
        .iter()
        .map(|p| [("candidate".to_string(), p.artifact.clone())])
        .collect();
    let reqs: Vec<VerifyRequest<'_>> = evidence
        .iter()
        .map(|ev| VerifyRequest {
            kind: VerifyKind::Rank,
            goal: &dgoal.goal,
            tool_calls: &[],
            evidence: ev,
        })
        .collect();
    let agrees = verifier
        .cross_check_batch(&reqs)
        .map_err(|e| AgentError::Other(format!("cross-check: {e}")))?;
    if agrees.len() != queue.len() {
        return Err(AgentError::Other(
            "cross-check batch verdict count mismatch".into(),
        ));
    }
    let mut terminal_minted = false;
    for (p, agree) in queue.drain(..).zip(agrees) {
        if p.score <= ledger.bar(&p.cell, dgoal.baseline_score)
            || ledger.minted.len() as u32 >= dgoal.max_mints
        {
            continue;
        }
        if agree {
            ledger.bars.insert(p.cell, p.score);
            let atom =
                graph.add_verified_artifact(p.atom, dgoal.kind, attestation.clone(), p.score)?;
            ledger.minted.push(atom);
            // Log the certified statement so mints are inspectable from the run log.
            let stmt = serde_json::from_str::<Value>(&p.artifact)
                .ok()
                .and_then(|v| {
                    v.get("statement")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| p.artifact.clone());
            eprintln!(
                "[discovery] minted {atom:?} (score {score:.3}): {stmt}",
                score = p.score
            );
            terminal_minted |= p.terminal;
            if ledger.best_verified.is_none_or(|(best, _)| p.score > best) {
                *ledger.best_verified = Some((p.score, atom));
            }
        } else {
            *ledger.cross_check_failures += 1;
        }
    }
    Ok(terminal_minted)
}

fn response_to_value(resp: &CompletionResponse) -> Value {
    json!({
        "content": resp.message.content,
        "tool_calls": resp.message.tool_calls.iter().map(|c| json!({
            "id": c.id, "name": c.name, "arguments": c.arguments,
        })).collect::<Vec<_>>(),
        "finish_reason": format!("{:?}", resp.finish_reason),
        "usage": resp.usage.map(|usage| json!({
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "cost_usd": valid_cost(usage.cost_usd),
        })),
    })
}

/// Inverse of [`response_to_value`]: reconstruct a response from a trace payload.
fn value_to_response(v: &Value) -> CompletionResponse {
    let content = v
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let tool_calls = v
        .get("tool_calls")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|c| {
                    Some(ToolCall {
                        id: c.get("id").and_then(Value::as_str)?.to_string(),
                        name: c.get("name").and_then(Value::as_str)?.to_string(),
                        arguments: c.get("arguments").cloned().unwrap_or(Value::Null),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let finish_reason = match v.get("finish_reason").and_then(Value::as_str) {
        Some("ToolUse") => FinishReason::ToolUse,
        Some("Length") => FinishReason::Length,
        Some("Refusal") => FinishReason::Refusal,
        Some("ContentFilter") => FinishReason::ContentFilter,
        Some("Error") => FinishReason::Error,
        _ => FinishReason::Stop,
    };
    CompletionResponse {
        message: AssistantMessage {
            content,
            tool_calls,
        },
        usage: v.get("usage").and_then(|usage| {
            Some(TokenUsage {
                input_tokens: u32::try_from(usage.get("input_tokens")?.as_u64()?).ok()?,
                output_tokens: u32::try_from(usage.get("output_tokens")?.as_u64()?).ok()?,
                cost_usd: valid_cost(usage.get("cost_usd").and_then(Value::as_f64)),
            })
        }),
        finish_reason,
    }
}

/// An [`LLMClient`] that replays recorded responses by `request_hash` (zero live
/// calls). Seed from [`BeliefGraph::load_llm_traces`]; an unrecorded request bumps
/// [`ReplayClient::misses`] and errors, so a faithful replay has `misses() == 0`.
pub(crate) struct ReplayClient {
    responses: FxHashMap<String, Value>,
    model_id: String,
    misses: AtomicU32,
}

impl ReplayClient {
    pub fn from_traces(model_id: impl Into<String>, traces: Vec<(String, Value)>) -> Self {
        Self {
            responses: traces.into_iter().collect(),
            model_id: model_id.into(),
            misses: AtomicU32::new(0),
        }
    }

    /// Build a replay client from a graph's traces, reusing the original model id so
    /// hashes match. Errors if no trace. For a repeated `request_hash`, the newest
    /// response wins (replay reproduces the final answer, not the sequence).
    pub fn from_graph(graph: &BeliefGraph) -> Result<Self, GraphError> {
        let model_id = graph.llm_model_id()?.ok_or(GraphError::NoTraces)?;
        Ok(Self::from_traces(model_id, graph.load_llm_traces()?))
    }

    /// Requests with no recorded response (0 on a clean replay).
    pub fn misses(&self) -> u32 {
        self.misses.load(Ordering::Relaxed)
    }
}

impl LLMClient for ReplayClient {
    fn complete(&self, req: &CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let hash = request_hash(&self.model_id, req);
        match self.responses.get(&hash) {
            Some(value) => match value.get("error") {
                Some(error) => Err(value_to_error(error)),
                None => Ok(value_to_response(value)),
            },
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                Err(LlmError::Backend(format!(
                    "replay: no recorded response for {hash}"
                )))
            }
        }
    }

    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn count_tokens(&self, _messages: &[Message]) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verify::{CheckerAttestation, VerifyError, VerifyOutcome};
    use citadel::{Argon2Profile, DatabaseBuilder};
    use citadel_llm::factory::testing;
    use citadel_mem::{MemoryEngine, MockEmbedder};

    fn region() -> (tempfile::TempDir, Arc<MemoryEngine>) {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let eng = Arc::new(MemoryEngine::open(Arc::new(db)).unwrap());
        eng.create_region("agent", Arc::new(MockEmbedder::new(64)))
            .unwrap();
        (dir, eng)
    }

    // These fixtures model local, explicitly measured zero-cost completions.
    // Tests of unavailable accounting use the raw testing clients instead.
    fn measured(mut response: CompletionResponse) -> CompletionResponse {
        response.usage.get_or_insert(TokenUsage {
            input_tokens: 0,
            output_tokens: 0,
            cost_usd: Some(0.0),
        });
        response
    }

    fn scripted_measured(responses: Vec<CompletionResponse>) -> Arc<dyn LLMClient> {
        testing::scripted(responses.into_iter().map(measured).collect())
    }

    fn capturing_measured(responses: Vec<CompletionResponse>) -> testing::Capture {
        testing::capturing(responses.into_iter().map(measured).collect())
    }

    fn agent_with(
        responses: Vec<CompletionResponse>,
        budget: AgentBudget,
    ) -> (tempfile::TempDir, Agent) {
        let (dir, eng) = region();
        let graph = BeliefGraph::new(eng, "agent");
        let llm = scripted_measured(responses);
        let agent = Agent::new(
            llm,
            graph,
            ToolRegistry::new(),
            budget,
            AgentConfig::default(),
        );
        (dir, agent)
    }

    /// Temperature 0 alone does not make a backend reply reproducible, so a control call
    /// that omits the seed is not deterministic however the doc reads.
    #[test]
    fn control_calls_carry_the_configured_seed() {
        let (_dir, eng) = region();
        let cap = capturing_measured(vec![CompletionResponse::text("done")]);
        let agent = Agent::new(
            cap.client(),
            BeliefGraph::new(eng, "agent"),
            ToolRegistry::new(),
            AgentBudget::default(),
            AgentConfig {
                seed: Some(7),
                ..AgentConfig::default()
            },
        );
        let _ = agent.run("say hello");
        let reqs = cap.requests();
        assert!(!reqs.is_empty(), "no control call was made");
        for r in &reqs {
            assert_eq!(r.seed, Some(7), "control call sent without the seed");
            assert_eq!(r.temperature, Some(0.0));
        }
    }

    fn agent_with_config(
        responses: Vec<CompletionResponse>,
        config: AgentConfig,
    ) -> (tempfile::TempDir, Agent) {
        let (dir, eng) = region();
        let graph = BeliefGraph::new(eng, "agent");
        let llm = scripted_measured(responses);
        let agent = Agent::new(
            llm,
            graph,
            ToolRegistry::new(),
            AgentBudget::default(),
            config,
        );
        (dir, agent)
    }

    fn agent_with_llm(llm: Arc<dyn LLMClient>, config: AgentConfig) -> (tempfile::TempDir, Agent) {
        let (dir, eng) = region();
        let graph = BeliefGraph::new(eng, "agent");
        let agent = Agent::new(
            llm,
            graph,
            ToolRegistry::new(),
            AgentBudget::default(),
            config,
        );
        (dir, agent)
    }

    /// A storm/flaky client: fails its first `fail` calls with `status`, then
    /// replies with plain text. Surfaces a call counter for attempt assertions.
    fn flaky(fail: u32, status: u16) -> testing::Probe {
        testing::http_storm(
            fail,
            status,
            "flaky",
            measured(CompletionResponse::text("plain reply, no plan")),
        )
    }

    #[test]
    fn cost_cap_with_an_unpriced_model_stops_with_a_distinct_reason() {
        let mut response = CompletionResponse::text("hi");
        response.usage = Some(TokenUsage {
            input_tokens: 1,
            output_tokens: 1,
            cost_usd: None,
        });
        let (_dir, agent) = agent_with(
            vec![response],
            AgentBudget {
                max_cost_usd: Some(1.0),
                ..Default::default()
            },
        );
        let report = agent.run("goal").unwrap();
        assert_eq!(
            report.terminated_by,
            TerminatedBy::BudgetUnavailable(BudgetUnavailable::Cost)
        );
        assert_eq!(agent.graph().load_llm_traces().unwrap().len(), 1);
    }

    #[test]
    fn potentially_dispatched_failure_stops_without_retry_and_keeps_attempt_context() {
        for status in [400, 429, 503] {
            let probe = flaky(2, status);
            let (_dir, agent) = agent_with_llm(probe.client(), AgentConfig::default());
            let report = agent.run("do it").unwrap();
            assert_eq!(
                report.terminated_by,
                TerminatedBy::BudgetUnavailable(BudgetUnavailable::Tokens)
            );
            assert_eq!(
                probe.calls(),
                1,
                "unknown spend cannot be retried under a token cap"
            );
            let traces = agent.graph().load_llm_traces().unwrap();
            assert_eq!(traces.len(), 1);
            assert_eq!(traces[0].1["attempt"], 1);
            assert_eq!(traces[0].1["pre_dispatch"], false);
            assert_eq!(traces[0].1["error"]["kind"], "http");
            assert_eq!(traces[0].1["error"]["status"], status);
            assert!(!traces[0].1["error"]["message"].as_str().unwrap().is_empty());
            let replay = Arc::new(ReplayClient::from_graph(agent.graph()).unwrap());
            let (_other_dir, replayed) = agent_with_llm(replay.clone(), AgentConfig::default());
            assert_eq!(
                replayed.run("do it").unwrap().terminated_by,
                report.terminated_by
            );
            assert_eq!(replay.misses(), 0);
        }
    }

    #[test]
    fn proven_pre_dispatch_failure_preserves_zero_spend_and_original_error() {
        struct Unsupported;
        impl LLMClient for Unsupported {
            fn complete(&self, _req: &CompletionRequest) -> Result<CompletionResponse, LlmError> {
                Err(LlmError::UnsupportedRequest("local refusal".into()))
            }
            fn model_id(&self) -> &str {
                "unsupported-test"
            }
            fn count_tokens(&self, _messages: &[Message]) -> usize {
                0
            }
        }
        let (_dir, agent) = agent_with_llm(Arc::new(Unsupported), AgentConfig::default());
        let mut ctx = agent.new_ctx("goal".into());
        let prompt = ctx.config.prompt_library.resolve(PromptId::Execute);
        let error = ctx
            .complete(CompletionRequest::new(vec![Message::user("x")]), &prompt)
            .unwrap_err();
        assert!(matches!(
            error,
            AgentError::Llm(LlmError::UnsupportedRequest(_))
        ));
        assert_eq!(ctx.usage.tokens, Some(0));
        assert_eq!(ctx.usage.cost_usd, Some(0.0));
        let traces = ctx.graph.load_llm_traces().unwrap();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].1["pre_dispatch"], true);
        assert_eq!(traces[0].1["error"]["message"], "local refusal");
    }

    /// A verifier with a fixed verdict for every request.
    struct FixedVerifier(bool);
    impl Verifier for FixedVerifier {
        fn verify(&self, _req: &VerifyRequest<'_>) -> Result<VerifyOutcome, VerifyError> {
            Ok(VerifyOutcome {
                satisfied: self.0,
                reason: "fixed".into(),
            })
        }
    }

    /// Like [`FixedVerifier`] but ATTESTED, so `acceptance_verified` accepts it and
    /// `observe` can route a met goal to Converge.
    struct AttestedVerifier(bool);
    impl Verifier for AttestedVerifier {
        fn verify(&self, _req: &VerifyRequest<'_>) -> Result<VerifyOutcome, VerifyError> {
            Ok(VerifyOutcome {
                satisfied: self.0,
                reason: "attested".into(),
            })
        }
        fn attestation(&self) -> Option<CheckerAttestation> {
            Some(CheckerAttestation::new("test-attested", "1"))
        }
    }

    /// A verifier that errors for one kind of check (fail-open / fail-closed).
    struct ErrVerifier(VerifyKind);
    impl Verifier for ErrVerifier {
        fn verify(&self, req: &VerifyRequest<'_>) -> Result<VerifyOutcome, VerifyError> {
            if req.kind == self.0 {
                Err(VerifyError::Failed("boom".into()))
            } else {
                Ok(VerifyOutcome {
                    satisfied: true,
                    reason: "ok".into(),
                })
            }
        }
    }

    fn plan_response(criteria: &[&str], tasks: &[&str]) -> CompletionResponse {
        plan_full(criteria, &[], tasks)
    }

    fn plan_full(criteria: &[&str], constraints: &[&str], tasks: &[&str]) -> CompletionResponse {
        CompletionResponse::tool_calls(vec![ToolCall {
            id: "plan".into(),
            name: "submit_plan".into(),
            arguments: json!({
                "goal": {
                    "prompt": "do the thing",
                    "acceptance_criteria": criteria,
                    "constraints": constraints,
                },
                "tasks": tasks.iter().map(|d| json!({"description": d, "deps": []})).collect::<Vec<_>>(),
            }),
        }])
    }

    #[test]
    fn parse_tasks_and_goal_tolerate_stringified_json() {
        // Models sometimes STRINGIFY structured args; a stringified tasks/criteria
        // array or goal object must still parse, never a silent empty plan.
        let args = json!({
            "goal": { "prompt": "g", "acceptance_criteria": "[\"crit one\"]", "constraints": "[]" },
            "tasks": "[{\"description\": \"do it\", \"deps\": []}]",
        });
        let goal = parse_goal(&args).unwrap();
        assert_eq!(goal.acceptance_criteria, vec!["crit one".to_string()]);
        let tasks = parse_tasks(&args);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].0, "do it");

        let args2 = json!({
            "goal": "{\"prompt\": \"g2\", \"acceptance_criteria\": [\"c\"]}",
            "tasks": "[]",
        });
        assert_eq!(
            parse_goal(&args2).unwrap().acceptance_criteria,
            vec!["c".to_string()]
        );
    }

    #[test]
    fn runs_plan_execute_converge_to_success() {
        // Empty acceptance criteria -> Converge takes the deterministic fast path.
        let plan = plan_response(&[], &["step one"]);
        let exec = CompletionResponse::text("completed step one");
        let (_d, agent) = agent_with(vec![plan, exec], AgentBudget::default());

        let report = agent.run("do the thing").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Success);
        assert_eq!(report.tasks_done, 1);
        assert!(report.chain_valid, "audit chain verifies");
        assert_eq!(report.final_answer.as_deref(), Some("completed step one"));
    }

    #[test]
    fn loop_uses_overridden_prompt_library() {
        // An operator prompt override threads through AgentConfig without breaking
        // the loop (the scripted mock ignores content, so this guards wiring).
        let plan = plan_response(&[], &["step one"]);
        let exec = CompletionResponse::text("completed step one");
        let config = AgentConfig {
            prompt_library: Arc::new(PromptLibrary::new().with_override(
                PromptId::Planner,
                2,
                "a custom planner prompt",
            )),
            ..Default::default()
        };
        let (_d, agent) = agent_with_config(vec![plan, exec], config);
        let report = agent.run("do the thing").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Success);
    }

    #[test]
    fn converge_with_criteria_uses_critic_verdict() {
        let plan = plan_response(&["did the thing"], &["step"]);
        let exec = CompletionResponse::text("did the thing");
        let verdict = CompletionResponse::tool_calls(vec![ToolCall {
            id: "v".into(),
            name: "verdict".into(),
            arguments: json!({ "satisfied": true, "reason": "met" }),
        }]);
        let (_d, agent) = agent_with(vec![plan, exec, verdict], AgentBudget::default());

        let report = agent.run("do the thing").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Success);
        assert!(report.chain_valid);
    }

    #[test]
    fn budget_steps_cap_terminates_gracefully() {
        let plan = plan_response(&["never met"], &["t"]);
        let mut responses = vec![plan];
        responses.extend((0..10).map(|_| CompletionResponse::text("working")));
        let (_d, agent) = agent_with(
            responses,
            AgentBudget {
                max_steps: 2,
                ..Default::default()
            },
        );

        let report = agent.run("do the thing").unwrap();
        assert!(matches!(
            report.terminated_by,
            TerminatedBy::BudgetExceeded(BudgetExceeded::Steps)
        ));
        assert!(report.chain_valid);
    }

    #[test]
    fn converge_at_step_cap_mints_success() {
        // A goal verified on the very step that exhausts the budget still reports
        // Success: the caps bound WORK, not the terminal mint. Plan->Execute->Observe
        // routes to Converge at max_steps; Converge is exempt so it runs once.
        let config = AgentConfig {
            verifier: Some(Arc::new(AttestedVerifier(true))),
            ..Default::default()
        };
        let (dir, eng) = region();
        let graph = BeliefGraph::new(eng, "agent");
        let llm = scripted_measured(vec![
            plan_response(&["the criterion"], &["t"]),
            CompletionResponse::text("done"),
        ]);
        let agent = Agent::new(
            llm,
            graph,
            ToolRegistry::new(),
            AgentBudget {
                max_steps: 3,
                ..Default::default()
            },
            config,
        );

        let report = agent.run("do the thing").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Success);
        assert_eq!(report.tasks_done, 1);
        assert!(report.chain_valid);
        // converge() made no LLM call under the attested verifier, so only the
        // plan + execute calls were traced - the mint ran free, past the cap.
        assert_eq!(agent.graph().load_llm_traces().unwrap().len(), 2);
        drop(dir);
    }

    #[test]
    fn attested_verifier_gates_empty_criteria() {
        // A plan with NO acceptance criteria must not self-close when an attested
        // verifier is configured: the verifier (here rejecting) is the sole authority.
        let config = AgentConfig {
            max_replans: 0,
            verifier: Some(Arc::new(AttestedVerifier(false))),
            ..Default::default()
        };
        let (_d, agent) = agent_with_config(
            vec![
                plan_response(&[], &["step"]),
                CompletionResponse::text("done"),
            ],
            config,
        );
        let report = agent.run("do the thing").unwrap();
        assert_ne!(
            report.terminated_by,
            TerminatedBy::Success,
            "empty criteria must not bypass an attested verifier that rejects acceptance"
        );
    }

    #[test]
    fn attested_verifier_accepts_empty_criteria() {
        // Empty criteria + an attested verifier that ACCEPTS is consulted and
        // converges to Success.
        let config = AgentConfig {
            verifier: Some(Arc::new(AttestedVerifier(true))),
            ..Default::default()
        };
        let (_d, agent) = agent_with_config(
            vec![
                plan_response(&[], &["step"]),
                CompletionResponse::text("done"),
            ],
            config,
        );
        let report = agent.run("do the thing").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Success);
        assert!(report.chain_valid);
    }

    #[test]
    fn replay_reproduces_run_with_zero_misses() {
        let (_d1, agent1) = agent_with(
            vec![
                plan_response(&[], &["step one"]),
                CompletionResponse::text("completed step one"),
            ],
            AgentBudget::default(),
        );
        let report1 = agent1.run("do the thing").unwrap();
        assert_eq!(report1.terminated_by, TerminatedBy::Success);
        assert_eq!(
            agent1.graph().load_llm_traces().unwrap().len(),
            2,
            "plan + execute recorded"
        );

        // Replay into a fresh region, re-feeding responses keyed by request_hash.
        // from_graph recovers the original model id from the traces (no magic string).
        let (_d2, eng2) = region();
        let graph2 = BeliefGraph::new(eng2, "agent");
        let replay = crate::replay::replay_from_graph(agent1.graph()).unwrap();
        let agent2 = Agent::new(
            replay.client(),
            graph2,
            ToolRegistry::new(),
            AgentBudget::default(),
            AgentConfig::default(),
        );
        let report2 = agent2.run("do the thing").unwrap();

        assert_eq!(report2.terminated_by, TerminatedBy::Success);
        assert_eq!(report2.tasks_done, report1.tasks_done);
        assert!(report2.chain_valid);
        assert_eq!(replay.misses(), 0, "every request hit a recorded trace");
    }

    #[test]
    fn constraint_verifier_error_fails_open() {
        let config = AgentConfig {
            verifier: Some(Arc::new(ErrVerifier(VerifyKind::Constraint))),
            ..Default::default()
        };
        let (_d, agent) = agent_with_config(
            vec![
                plan_full(&[], &["respect privacy"], &["step"]),
                CompletionResponse::text("did it"),
            ],
            config,
        );
        let report = agent.run("do the thing").unwrap();
        assert_eq!(
            report.terminated_by,
            TerminatedBy::Success,
            "a constraint verifier error must not abort a valid run (fail-open)"
        );
    }

    #[test]
    fn acceptance_verifier_error_fails_closed() {
        let config = AgentConfig {
            max_replans: 0,
            verifier: Some(Arc::new(ErrVerifier(VerifyKind::Acceptance))),
            ..Default::default()
        };
        let (_d, agent) = agent_with_config(
            vec![
                plan_response(&["the criterion"], &["step"]),
                CompletionResponse::text("did it"),
            ],
            config,
        );
        let report = agent.run("do the thing").unwrap();
        assert_eq!(
            report.terminated_by,
            TerminatedBy::Incomplete,
            "an acceptance verifier error must not declare success (fail-closed)"
        );
    }

    #[test]
    fn constraint_violation_is_recorded_and_drives_reflect() {
        let config = AgentConfig {
            max_replans: 0,
            verifier: Some(Arc::new(FixedVerifier(false))),
            ..Default::default()
        };
        let (_d, agent) = agent_with_config(
            vec![
                plan_full(&[], &["must comply"], &["step"]),
                CompletionResponse::text("did it"),
                CompletionResponse::text("the action broke the constraint"),
            ],
            config,
        );
        let report = agent.run("do the thing").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Incomplete);

        let trail = agent.graph().export_audit_trail().unwrap();
        assert!(
            trail.iter().any(|c| c.verdict == Verdict::Violation),
            "the constraint violation is recorded in the audit chain"
        );
    }

    fn agent_full(
        llm: Arc<dyn LLMClient>,
        budget: AgentBudget,
        config: AgentConfig,
        tools: ToolRegistry,
    ) -> (tempfile::TempDir, Agent) {
        let (dir, eng) = region();
        let graph = BeliefGraph::new(eng, "agent");
        (dir, Agent::new(llm, graph, tools, budget, config))
    }

    /// A tool that returns fixed content (a stand-in for file_read).
    struct StubTool {
        name: String,
        output: String,
    }
    impl Tool for StubTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: self.name.clone(),
                description: "stub".into(),
                input_schema: json!({ "type": "object" }),
            }
        }
        fn call(&self, _args: &Value) -> Result<String, ToolError> {
            Ok(self.output.clone())
        }
    }

    /// A tool whose dispatch always errors (exhausts retries -> any_failure).
    struct FailingTool {
        name: String,
    }
    impl Tool for FailingTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: self.name.clone(),
                description: "always fails".into(),
                input_schema: json!({ "type": "object" }),
            }
        }
        fn call(&self, _args: &Value) -> Result<String, ToolError> {
            Err(ToolError::Failed {
                tool: self.name.clone(),
                reason: "always fails".into(),
            })
        }
    }

    fn one_tool_call(name: &str) -> CompletionResponse {
        CompletionResponse::tool_calls(vec![ToolCall {
            id: format!("{name}-call"),
            name: name.into(),
            arguments: json!({}),
        }])
    }

    #[test]
    fn react_reads_then_acts_and_feeds_results_forward() {
        let marker = "the source defines a buggy frob()";
        let mut tools = ToolRegistry::new();
        tools.register(Box::new(StubTool {
            name: "read_src".into(),
            output: marker.into(),
        }));
        let cap = capturing_measured(vec![
            plan_response(&[], &["diagnose then fix"]),
            one_tool_call("read_src"),
            CompletionResponse::text("fixed it using what the source showed"),
        ]);
        let (_d, agent) = agent_full(
            cap.client(),
            AgentBudget::default(),
            AgentConfig::default(),
            tools,
        );

        let report = agent.run("fix the bug").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Success);
        assert_eq!(report.tasks_done, 1);
        assert!(report.chain_valid);
        assert_eq!(
            report.final_answer.as_deref(),
            Some("fixed it using what the source showed")
        );

        // Round 1 (plan=0, round0=1, round1=2) must carry the tool result forward.
        let reqs = cap.requests();
        assert_eq!(reqs.len(), 3, "plan + 2 react rounds");
        let fed_back = reqs[2]
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content, .. } if content == marker));
        assert!(fed_back, "round-1 prompt must include the tool observation");
        let round0_has_tool = reqs[1]
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        assert!(!round0_has_tool, "round 0 has no transcript");
    }

    #[test]
    fn text_only_exec_completes_in_one_round() {
        let (_d, agent) = agent_with(
            vec![
                plan_response(&[], &["step"]),
                CompletionResponse::text("done"),
            ],
            AgentBudget::default(),
        );
        let report = agent.run("x").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Success);
        assert_eq!(
            agent.graph().load_llm_traces().unwrap().len(),
            2,
            "plan + exactly one execute round"
        );
    }

    #[test]
    fn react_inner_cap_bounds_nonterminating_model() {
        // A model that always calls a tool and never answers is bounded by
        // max_react_steps; the task fails and (no replans) the run ends Incomplete.
        let mut tools = ToolRegistry::new();
        tools.register(Box::new(StubTool {
            name: "noop".into(),
            output: "ok".into(),
        }));
        let mut responses = vec![plan_response(&[], &["loop"])];
        responses.extend((0..10).map(|_| one_tool_call("noop")));
        let config = AgentConfig {
            max_react_steps: 3,
            max_replans: 0,
            ..Default::default()
        };
        let (_d, agent) = agent_full(
            scripted_measured(responses),
            AgentBudget {
                max_steps: 100,
                ..Default::default()
            },
            config,
            tools,
        );
        let report = agent.run("x").unwrap();
        assert_eq!(
            report.terminated_by,
            TerminatedBy::Incomplete,
            "inner cap fails the task; no replans -> Incomplete"
        );
        assert!(report.chain_valid);
    }

    #[test]
    fn global_budget_bounds_inner_loop() {
        // Each react round is one step(), so the global step cap stops a runaway
        // inner loop even with max_react_steps set high.
        let mut tools = ToolRegistry::new();
        tools.register(Box::new(StubTool {
            name: "noop".into(),
            output: "ok".into(),
        }));
        let mut responses = vec![plan_response(&[], &["loop"])];
        responses.extend((0..10).map(|_| one_tool_call("noop")));
        let config = AgentConfig {
            max_react_steps: 50,
            ..Default::default()
        };
        let (_d, agent) = agent_full(
            scripted_measured(responses),
            AgentBudget {
                max_steps: 4,
                ..Default::default()
            },
            config,
            tools,
        );
        let report = agent.run("x").unwrap();
        assert!(matches!(
            report.terminated_by,
            TerminatedBy::BudgetExceeded(BudgetExceeded::Steps)
        ));
        assert!(report.chain_valid);
    }

    #[test]
    fn react_midloop_co_inst_violation_routes_to_reflect() {
        // A constraint violation on a tool round is recorded BEFORE the abort and
        // routes to Reflect; the inner loop does not continue past it.
        let mut tools = ToolRegistry::new();
        tools.register(Box::new(StubTool {
            name: "noop".into(),
            output: "ok".into(),
        }));
        let config = AgentConfig {
            max_replans: 0,
            verifier: Some(Arc::new(FixedVerifier(false))),
            ..Default::default()
        };
        let (_d, agent) = agent_full(
            scripted_measured(vec![
                plan_full(&[], &["must be polite"], &["t"]),
                one_tool_call("noop"),
                CompletionResponse::text("reflecting"),
            ]),
            AgentBudget::default(),
            config,
            tools,
        );
        let report = agent.run("x").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Incomplete);
        let trail = agent.graph().export_audit_trail().unwrap();
        let violations = trail
            .iter()
            .filter(|c| c.verdict == Verdict::Violation)
            .count();
        assert_eq!(violations, 1, "recorded once; the loop did not continue");
    }

    #[test]
    fn react_recovers_from_tool_error_within_cap() {
        // A tool error is fed back (is_error) for a bounded read-then-fix attempt
        // rather than failing the task outright on the first error.
        let mut tools = ToolRegistry::new();
        tools.register(Box::new(FailingTool {
            name: "always_fails".into(),
        }));
        let cap = capturing_measured(vec![
            plan_response(&[], &["use the tool"]),
            one_tool_call("always_fails"),
            CompletionResponse::text("recovered: proceeding without it"),
        ]);
        let (_d, agent) = agent_full(
            cap.client(),
            AgentBudget::default(),
            AgentConfig::default(),
            tools,
        );

        let report = agent.run("x").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Success);
        assert_eq!(report.tasks_done, 1);
        assert!(report.chain_valid);
        let reqs = cap.requests();
        let err_fed_back = reqs[2].messages.iter().any(|m| {
            matches!(m, Message::Tool { is_error, content, .. }
                if *is_error && content.contains("tool error"))
        });
        assert!(err_fed_back, "the tool error must be fed back for recovery");
    }

    /// A mutating tool (declares a filesystem write path) that always errors,
    /// counting dispatches so a test can prove it is not retried.
    struct CountingFailTool {
        name: String,
        calls: Arc<AtomicU32>,
    }
    impl Tool for CountingFailTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: self.name.clone(),
                description: "mutating; always fails".into(),
                input_schema: json!({ "type": "object" }),
            }
        }
        fn permissions(&self) -> ToolPermissions {
            ToolPermissions {
                filesystem: FsPolicy::AllowPaths {
                    read: Vec::new(),
                    write: vec![std::path::PathBuf::from("/sandbox")],
                },
                ..Default::default()
            }
        }
        fn call(&self, _args: &Value) -> Result<String, ToolError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ToolError::Failed {
                tool: self.name.clone(),
                reason: "transient".into(),
            })
        }
    }

    #[test]
    fn replan_does_not_duplicate_tasks() {
        // A request_replan round drives Reflect -> Plan; plan() must re-attempt the
        // single existing task, not add a second copy (which inflated tasks_done).
        let plan = plan_response(&[], &["only task"]);
        let replan = CompletionResponse::tool_calls(vec![ToolCall {
            id: "rp".into(),
            name: "request_replan".into(),
            arguments: json!({}),
        }]);
        let (_d, agent) = agent_with(
            vec![
                plan,
                replan,
                CompletionResponse::text("reflecting"),
                CompletionResponse::text("done"),
            ],
            AgentBudget::default(),
        );
        let report = agent.run("x").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Success);
        assert_eq!(
            report.tasks_done, 1,
            "the task is not duplicated by the replan"
        );
        assert_eq!(
            agent.graph().tasks().unwrap().len(),
            1,
            "exactly one task atom survives the replan"
        );
    }

    #[test]
    fn mutating_tool_is_not_retried() {
        // A tool that declares a write path is dispatched exactly once on a
        // transient error (no double-apply), unlike read-only tools which retry.
        let calls = Arc::new(AtomicU32::new(0));
        let mut tools = ToolRegistry::new();
        tools.register(Box::new(CountingFailTool {
            name: "write_thing".into(),
            calls: Arc::clone(&calls),
        }));
        let (_d, agent) = agent_full(
            scripted_measured(vec![
                plan_response(&[], &["w"]),
                one_tool_call("write_thing"),
                CompletionResponse::text("done without it"),
            ]),
            AgentBudget::default(),
            AgentConfig::default(),
            tools,
        );
        let report = agent.run("x").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Success);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a write tool is dispatched once, not retried"
        );
    }

    #[test]
    fn tool_use_without_dispatchable_calls_is_accounted_then_refused() {
        let mut malformed = CompletionResponse::text("partial provider text");
        malformed.finish_reason = FinishReason::ToolUse;
        malformed.usage = Some(TokenUsage {
            input_tokens: 11,
            output_tokens: 7,
            cost_usd: Some(0.125),
        });
        let capture = capturing_measured(vec![malformed]);
        let (_dir, agent) = agent_with_llm(capture.client(), AgentConfig::default());
        let mut ctx = agent.new_ctx("goal".into());
        let prompt = ctx.config.prompt_library.resolve(PromptId::Execute);
        let request =
            CompletionRequest::new(vec![prompt.as_system(), Message::user("continue the task")]);

        let err = ctx
            .complete(request, &prompt)
            .expect_err("empty tool-use batch must fail closed");
        assert!(matches!(err, AgentError::Llm(LlmError::Backend(_))));
        assert_eq!(capture.requests().len(), 1);
        assert_eq!(ctx.usage.tokens, Some(18));
        assert_eq!(ctx.usage.cost_usd, Some(0.125));

        let traces = ctx.graph.load_llm_traces().unwrap();
        assert_eq!(traces.len(), 1, "the completed provider call is audited");
        let response = &traces[0].1;
        assert_eq!(response["finish_reason"], json!("ToolUse"));
        assert_eq!(response["content"], json!("partial provider text"));
        assert_eq!(response["tool_calls"], json!([]));
    }

    #[test]
    fn hard_finish_dispositions_are_accounted_then_refused() {
        let cases = [
            (FinishReason::Refusal, "refused"),
            (FinishReason::ContentFilter, "filtered"),
            (FinishReason::Error, "error disposition"),
        ];
        let responses = cases
            .iter()
            .enumerate()
            .map(|(index, (reason, _))| {
                let mut response = CompletionResponse::tool_calls(vec![ToolCall {
                    id: format!("call-{index}"),
                    name: "tempting_tool".into(),
                    arguments: json!({"unsafe": true}),
                }]);
                response.message.content = "tempting partial answer".into();
                response.finish_reason = *reason;
                response.usage = Some(TokenUsage {
                    input_tokens: 3,
                    output_tokens: 2,
                    cost_usd: Some(0.025),
                });
                response
            })
            .collect();
        let capture = capturing_measured(responses);
        let (_dir, agent) = agent_with_llm(capture.client(), AgentConfig::default());
        let mut ctx = agent.new_ctx("goal".into());
        let prompt = ctx.config.prompt_library.resolve(PromptId::Execute);

        for (index, (_, message)) in cases.iter().enumerate() {
            let request = CompletionRequest::new(vec![
                prompt.as_system(),
                Message::user(format!("attempt {index}")),
            ]);
            let err = ctx
                .complete(request, &prompt)
                .expect_err("hard provider disposition must fail closed");
            assert!(matches!(err, AgentError::Llm(LlmError::Backend(_))));
            assert!(err.to_string().contains(message));
        }

        assert_eq!(capture.requests().len(), cases.len());
        assert_eq!(ctx.usage.tokens, Some(15));
        assert!((ctx.usage.cost_usd.unwrap() - 0.075).abs() < 1e-12);
        let traces = ctx.graph.load_llm_traces().unwrap();
        assert_eq!(traces.len(), cases.len());
        for (trace, (reason, _)) in traces.iter().zip(cases) {
            assert_eq!(trace.1["finish_reason"], json!(format!("{reason:?}")));
            assert_eq!(trace.1["content"], json!("tempting partial answer"));
            assert_eq!(trace.1["tool_calls"].as_array().unwrap().len(), 1);
        }
    }

    #[test]
    fn replay_trace_preserves_every_finish_reason() {
        for reason in [
            FinishReason::Stop,
            FinishReason::Length,
            FinishReason::ToolUse,
            FinishReason::Refusal,
            FinishReason::ContentFilter,
            FinishReason::Error,
        ] {
            let mut response = CompletionResponse::text("recorded");
            response.finish_reason = reason;
            let replayed = value_to_response(&response_to_value(&response));
            assert_eq!(replayed.finish_reason, reason);
        }
    }

    #[test]
    fn missing_usage_stops_before_tools_and_preserves_the_completion() {
        let calls = Arc::new(AtomicU32::new(0));
        struct CountTool(Arc<AtomicU32>);
        impl Tool for CountTool {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "count".into(),
                    description: "count".into(),
                    input_schema: json!({"type":"object"}),
                }
            }
            fn call(&self, _args: &Value) -> Result<String, ToolError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok("done".into())
            }
        }
        let mut tools = ToolRegistry::new();
        tools.register(Box::new(CountTool(Arc::clone(&calls))));
        let mut unknown = one_tool_call("count");
        unknown.message.content = "useful partial result".into();
        assert!(unknown.usage.is_none());
        let capture =
            testing::capturing(vec![measured(plan_response(&[], &["use count"])), unknown]);
        let (_dir, agent) = agent_full(
            capture.client(),
            AgentBudget::default(),
            AgentConfig::default(),
            tools,
        );
        let report = agent.run("x").unwrap();
        assert_eq!(
            report.terminated_by,
            TerminatedBy::BudgetUnavailable(BudgetUnavailable::Tokens)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(capture.requests().len(), 2);
        let traces = agent.graph().load_llm_traces().unwrap();
        assert_eq!(traces.len(), 2);
        let received = traces
            .iter()
            .find(|(_, value)| value["content"] == "useful partial result")
            .unwrap();
        assert!(received.1["usage"].is_null());
        assert_eq!(received.1["tool_calls"].as_array().unwrap().len(), 1);
        assert!(report.chain_valid);
    }

    #[test]
    fn unusable_cost_stops_after_trace_and_before_another_call() {
        for cost in [None, Some(-1.0), Some(f64::NAN), Some(f64::INFINITY)] {
            let mut response = CompletionResponse::text("retained answer");
            response.usage = Some(TokenUsage {
                input_tokens: 4,
                output_tokens: 2,
                cost_usd: cost,
            });
            let capture = testing::capturing(vec![response]);
            let (_dir, agent) = agent_full(
                capture.client(),
                AgentBudget {
                    max_cost_usd: Some(1.0),
                    ..Default::default()
                },
                AgentConfig::default(),
                ToolRegistry::new(),
            );
            let mut ctx = agent.new_ctx("x".into());
            let prompt = ctx.config.prompt_library.resolve(PromptId::Execute);
            let request = CompletionRequest::new(vec![Message::user("x")]);
            for _ in 0..2 {
                assert!(matches!(
                    ctx.complete(request.clone(), &prompt),
                    Err(AgentError::Budget(BudgetStop::UsageUnavailable(
                        BudgetUnavailable::Cost
                    )))
                ));
            }
            assert_eq!(capture.requests().len(), 1);
            assert_eq!(ctx.usage.tokens, Some(6));
            assert_eq!(ctx.usage.cost_usd, None);
            let traces = agent.graph().load_llm_traces().unwrap();
            assert_eq!(traces.len(), 1);
            assert_eq!(traces[0].1["content"], "retained answer");
            assert_eq!(traces[0].1["usage"]["input_tokens"], 4);
            assert!(traces[0].1["usage"]["cost_usd"].is_null());
        }
    }

    #[test]
    fn replay_preserves_usage_and_never_invents_old_trace_counters() {
        for usage in [
            None,
            Some(TokenUsage {
                input_tokens: 0,
                output_tokens: 0,
                cost_usd: Some(0.0),
            }),
            Some(TokenUsage {
                input_tokens: 5,
                output_tokens: 9,
                cost_usd: None,
            }),
        ] {
            let mut response = CompletionResponse::text("recorded answer");
            response.usage = usage;
            assert_eq!(
                value_to_response(&response_to_value(&response)).usage,
                usage
            );
        }
        let old = json!({"content":"old", "finish_reason":"Stop", "tool_calls":[]});
        assert!(value_to_response(&old).usage.is_none());
    }

    #[test]
    fn proposal_channel_keeps_unknown_response_and_blocks_repeated_attempts() {
        let capture = testing::capturing(vec![CompletionResponse::text("unknown usage")]);
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut channel = OwnedChannel {
            llm: capture.client(),
            started: Instant::now(),
            budget: AgentBudget::default(),
            usage: BudgetUsage::default(),
            log: Rc::clone(&log),
        };
        let request = CompletionRequest::new(vec![Message::user("x")]);
        // An operator that catches the first failure cannot spend again.
        assert!(channel.complete(&request).is_err());
        assert!(channel.complete(&request).is_err());
        assert_eq!(capture.requests().len(), 1);
        assert_eq!(log.borrow().len(), 1);
        assert_eq!(
            log.borrow()[0].response().unwrap().message.content,
            "unknown usage"
        );
        assert!(channel.usage.tokens.is_none());
    }

    #[test]
    fn repair_traces_the_last_response_before_accounting_or_operator_failure() {
        struct FailingRepair;
        impl ProposalOperator for FailingRepair {
            fn propose(
                &self,
                _ctx: &ProposalContext<'_>,
                _llm: Box<dyn Completer>,
            ) -> Result<Vec<Candidate>, ProposeError> {
                Ok(Vec::new())
            }
            fn repair(
                &self,
                _ctx: &ProposalContext<'_>,
                _failed: &RejectedCandidate,
                mut llm: Box<dyn Completer>,
            ) -> Result<Vec<Candidate>, ProposeError> {
                llm.complete(&CompletionRequest::new(vec![Message::user("repair")]))?;
                Err(ProposeError::Failed(
                    "operator failed after completion".into(),
                ))
            }
        }
        for known in [false, true] {
            let response = CompletionResponse::text("repair evidence");
            let response = if known { measured(response) } else { response };
            let capture = testing::capturing(vec![response]);
            let (_dir, agent) = agent_with_llm(capture.client(), AgentConfig::default());
            let mut ctx = agent.new_ctx("repair".into());
            let prompt = ctx.config.prompt_library.resolve(PromptId::Proposer);
            let op: Arc<dyn ProposalOperator> = Arc::new(FailingRepair);
            let goal = DiscoveryGoal {
                goal: Goal::new("repair"),
                kind: VerifiedKind::Construction,
                baseline_score: 0.0,
                archive_width: 8,
                max_idle_rounds: 1,
                max_mints: 1,
            };
            let error = ctx
                .repair_candidate(
                    &op,
                    &RejectedCandidate {
                        artifact: json!({}),
                        reason: "rejected".into(),
                    },
                    &prompt,
                    &[],
                    &goal,
                )
                .unwrap_err();
            if known {
                assert!(error
                    .to_string()
                    .contains("operator failed after completion"));
            } else {
                assert!(matches!(
                    error,
                    AgentError::Budget(BudgetStop::UsageUnavailable(BudgetUnavailable::Tokens))
                ));
            }
            let traces = ctx.graph.load_llm_traces().unwrap();
            assert_eq!(traces.len(), 1);
            assert_eq!(traces[0].1["content"], "repair evidence");
        }
    }

    #[test]
    fn valid_tokens_allow_an_unpriced_answer_without_a_cost_cap() {
        let responses = [
            plan_response(&[], &["answer"]),
            CompletionResponse::text("useful answer"),
        ]
        .into_iter()
        .map(|mut response| {
            response.usage = Some(TokenUsage {
                input_tokens: 4,
                output_tokens: 2,
                cost_usd: None,
            });
            response
        })
        .collect();
        let capture = testing::capturing(responses);
        let (_dir, agent) = agent_with_llm(capture.client(), AgentConfig::default());
        let report = agent.run("answer").unwrap();
        assert_eq!(report.terminated_by, TerminatedBy::Success);
        assert_eq!(report.final_answer.as_deref(), Some("useful answer"));
        let traces = agent.graph().load_llm_traces().unwrap();
        assert_eq!(traces.len(), 2);
        assert!(traces.iter().all(|(_, v)| v["usage"]["cost_usd"].is_null()));
        assert!(traces.iter().all(|(_, v)| v["usage"]["input_tokens"] == 4));
    }

    #[test]
    fn transport_failure_invalidates_prior_totals_and_prevents_further_spend() {
        struct TransportAfterResponse(Arc<AtomicU32>);
        impl LLMClient for TransportAfterResponse {
            fn complete(&self, _req: &CompletionRequest) -> Result<CompletionResponse, LlmError> {
                if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                    let mut response = CompletionResponse::text("first answer");
                    response.usage = Some(TokenUsage {
                        input_tokens: 4,
                        output_tokens: 2,
                        cost_usd: Some(0.1),
                    });
                    Ok(response)
                } else {
                    Err(LlmError::Transport("connection lost after send".into()))
                }
            }
            fn model_id(&self) -> &str {
                "transport-test"
            }
            fn count_tokens(&self, _messages: &[Message]) -> usize {
                0
            }
        }
        let calls = Arc::new(AtomicU32::new(0));
        let (_dir, agent) = agent_with_llm(
            Arc::new(TransportAfterResponse(Arc::clone(&calls))),
            AgentConfig::default(),
        );
        let mut ctx = agent.new_ctx("goal".into());
        let prompt = ctx.config.prompt_library.resolve(PromptId::Execute);
        let request = CompletionRequest::new(vec![Message::user("x")]);
        assert_eq!(
            ctx.complete(request.clone(), &prompt)
                .unwrap()
                .message
                .content,
            "first answer"
        );
        assert_eq!(ctx.usage.tokens, Some(6));
        for _ in 0..2 {
            assert!(matches!(
                ctx.complete(request.clone(), &prompt),
                Err(AgentError::Budget(BudgetStop::UsageUnavailable(
                    BudgetUnavailable::Tokens
                )))
            ));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(ctx.usage.tokens, None);
        assert_eq!(ctx.usage.cost_usd, None);
        let traces = ctx.graph.load_llm_traces().unwrap();
        assert_eq!(traces.len(), 2);
        let failed = traces
            .iter()
            .find(|(_, value)| value.get("error").is_some())
            .unwrap();
        assert_eq!(failed.1["error"]["kind"], "transport");
        assert_eq!(failed.1["error"]["message"], "connection lost after send");
        assert_eq!(failed.1["attempt"], 1);
    }

    #[test]
    fn invalid_cost_configuration_and_zero_cap_never_dispatch() {
        for limit in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.1, 0.0] {
            let capture = testing::capturing(Vec::new());
            let (_dir, agent) = agent_full(
                capture.client(),
                AgentBudget {
                    max_cost_usd: Some(limit),
                    ..Default::default()
                },
                AgentConfig::default(),
                ToolRegistry::new(),
            );
            let report = agent.run("x").unwrap();
            let expected = if limit == 0.0 {
                TerminatedBy::BudgetExceeded(BudgetExceeded::Cost)
            } else {
                TerminatedBy::InvalidBudget(BudgetInvalid::Cost)
            };
            assert_eq!(report.terminated_by, expected);
            assert!(capture.requests().is_empty());
            assert!(agent.graph().load_llm_traces().unwrap().is_empty());
        }
    }
}
