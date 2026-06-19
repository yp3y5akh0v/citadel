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

use citadel_mem::{AtomId, MemError};

use crate::budget::{AgentBudget, BudgetExceeded, BudgetUsage};
use crate::graph::{
    BeliefGraph, CoInstantiationCheck, Evidence, Goal, GoalStatus, GraphError, RecallContextConfig,
    Reflection, SelfModel, Task, TaskStatus, Verdict, VerifiedKind, CANDIDATE_KIND,
};
use crate::llm::{
    request_hash, AssistantMessage, CompletionRequest, CompletionResponse, FinishReason, LLMClient,
    LlmError, Message, TokenUsage, ToolCall, ToolChoice, ToolSpec,
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

#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error(transparent)]
    Graph(#[from] GraphError),
    #[error(transparent)]
    Llm(#[from] crate::llm::LlmError),
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
}

/// Capped, jittered backoff for a transient LLM error. No attempt cap: a transient
/// retries until the run's wall-clock budget (see `retry_complete`); a permanent
/// error never retries.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Base backoff, doubled each subsequent attempt.
    pub base_ms: u64,
    /// Ceiling on any single backoff (a server `Retry-After` may still exceed it).
    pub max_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base_ms: 200,
            max_ms: 2_000,
        }
    }
}

impl RetryPolicy {
    /// Backoff before retry `attempt` (1-based): capped exponential from `base_ms`,
    /// then equal jitter (half fixed + half random) so the delay grows without
    /// synchronizing or busy-looping at zero. A server `Retry-After` is a hard floor
    /// and may exceed `max_ms`. Equal jitter, not full jitter: a single sequential
    /// client needs only the non-zero floor, not fleet load-spreading.
    fn delay_ms(&self, attempt: u32, retry_after_secs: Option<u64>) -> u64 {
        let shift = (attempt.saturating_sub(1)).min(16);
        let exp = self.base_ms.saturating_mul(1u64 << shift).min(self.max_ms);
        let jittered = exp / 2 + jitter(exp / 2 + 1);
        let server = retry_after_secs.unwrap_or(0).saturating_mul(1_000);
        jittered.max(server)
    }
}

/// A uniform sample in `[0, bound)` from the wall clock's sub-second nanos. Timing
/// only, so it decorrelates backoff without perturbing the seed-pinned search
/// (replay determinism is over candidates, not sleep length).
fn jitter(bound: u64) -> u64 {
    if bound <= 1 {
        return 0;
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 % bound)
        .unwrap_or(0)
}

/// Sleep `ms`. No-op on wasm (no blocking sleep; retryable HTTP backends absent).
#[cfg(not(target_arch = "wasm32"))]
fn backoff_sleep(ms: u64) {
    std::thread::sleep(std::time::Duration::from_millis(ms));
}
#[cfg(target_arch = "wasm32")]
fn backoff_sleep(_ms: u64) {}

/// Tunables for a run. `verifier` None = fall back to a bounded audited critic.
pub struct AgentConfig {
    pub drift_bound: u32,
    pub max_replans: u32,
    pub max_tool_attempts: u32,
    /// Max ReAct rounds per task before failing it (each round is one step()).
    pub max_react_steps: u32,
    /// Prior atoms recall injects per subtask; 0 disables. Recency-free = replay-stable.
    pub recall_context_k: usize,
    /// Configured retrieval controls for the always-on recall injected per subtask.
    pub recall_context: RecallContextConfig,
    pub retry: RetryPolicy,
    pub verifier: Option<Arc<dyn Verifier>>,
    /// Versioned, overridable prompts for the loop's LLM call sites.
    pub prompt_library: Arc<PromptLibrary>,
    /// Discovery candidate generator; required only by run_discovery.
    pub proposal_operator: Option<Arc<dyn ProposalOperator>>,
    /// Max error-feedback repair re-prompts per rejected discovery candidate (0 disables).
    pub max_repairs: u32,
    /// Sampling temperature for control calls; 0.0 keeps them deterministic.
    pub temperature: f32,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            drift_bound: 5,
            max_replans: 3,
            max_tool_attempts: 3,
            max_react_steps: 6,
            recall_context_k: 5,
            recall_context: RecallContextConfig::default(),
            retry: RetryPolicy::default(),
            verifier: None,
            prompt_library: Arc::new(PromptLibrary::default()),
            proposal_operator: None,
            max_repairs: 2,
            temperature: 0.0,
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

    /// Drive the loop to a terminal state. Infra/LLM errors are `Err`; every
    /// graceful end is `Ok`.
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
                        terminated_by: TerminatedBy::BudgetExceeded(cap),
                    };
                    continue;
                }
            }
            state = ctx.step(state)?;
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

/// Call the backend, retrying a transient error with capped, jittered backoff until
/// the wall-clock deadline, so a network outage does not abort a long run. A
/// permanent error (auth/malformed) fails fast. The wall-clock budget is the only
/// bound; this single layer serves the cognition loop and the discovery proposal
/// channel.
fn retry_complete(
    llm: &dyn LLMClient,
    policy: RetryPolicy,
    started: Instant,
    max_wall_secs: u64,
    req: &CompletionRequest,
) -> Result<CompletionResponse, LlmError> {
    let mut attempt = 1u32;
    loop {
        match llm.complete(req) {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                // Permanent errors never succeed; only transient ones are worth waiting on.
                if !e.is_retryable() {
                    return Err(e);
                }
                let delay = policy.delay_ms(attempt, e.retry_after_secs());
                // The wall-clock budget, not an attempt count, ends a retry.
                if would_exceed_wall(started, max_wall_secs, delay) {
                    return Err(e);
                }
                backoff_sleep(delay);
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

/// Whether sleeping `delay_ms` would reach the wall-clock cap. Rounds up to whole
/// seconds so a sub-second backoff cannot slip past the second-granular guard.
fn would_exceed_wall(started: Instant, max_wall_secs: u64, delay_ms: u64) -> bool {
    let delay_secs = delay_ms.saturating_add(999) / 1_000;
    started.elapsed().as_secs().saturating_add(delay_secs) >= max_wall_secs
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
        // Every control call runs at the configured temperature (0 by default) so tool
        // output is deterministic and schema-adherent across backends.
        req.temperature = Some(self.config.temperature);
        let resp = self.call_with_retry(&req)?;
        self.accrue_and_record(&req, &resp, prompt)?;
        Ok(resp)
    }

    /// Accrue token/cost usage and record the immutable `llm_trace` for `resp`. Shared
    /// by the cognition loop and discovery so every call is budgeted/replayed one way.
    fn accrue_and_record(
        &mut self,
        req: &CompletionRequest,
        resp: &CompletionResponse,
        prompt: &ResolvedPrompt,
    ) -> AgentResult<()> {
        self.usage.tokens +=
            u64::from(resp.usage.input_tokens) + u64::from(resp.usage.output_tokens);
        match resp.usage.cost_usd {
            Some(cost) => self.usage.cost_usd += cost,
            // Unpriced response accrues $0, so a cost cap could never engage - fail closed.
            None if self.budget.max_cost_usd.is_some() => {
                return Err(AgentError::Other(format!(
                    "max_cost_usd is set but model '{}' returned an unpriced response; \
                     add the model to llm pricing or unset the cost cap",
                    self.llm.model_id()
                )));
            }
            None => {}
        }
        let hash = request_hash(self.llm.model_id(), req);
        let provenance = json!({
            "node": prompt.id.as_str(),
            "version": prompt.version,
            "hash": prompt.hash,
            "source": prompt.source.as_str(),
        });
        self.graph.record_llm_call(
            &hash,
            self.llm.model_id(),
            &response_to_value(resp),
            resp.usage.cost_usd,
            Some(&provenance),
        )?;
        Ok(())
    }

    /// Call the backend, retrying transient errors with bounded backoff (only the
    /// successful response is traced). Backoff is skipped if it would cross the deadline.
    fn call_with_retry(&self, req: &CompletionRequest) -> Result<CompletionResponse, LlmError> {
        retry_complete(
            &*self.llm,
            self.config.retry,
            self.started,
            self.budget.max_wall_secs,
            req,
        )
    }

    /// Run one repair call through a fresh budgeted/traced channel (same accrual as
    /// the proposal path), returning the operator's fixes. A transient LLM failure
    /// yields no fix; a hard error aborts. Operators without repair support no-op.
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
                retry: self.config.retry,
                started: self.started,
                max_wall_secs: self.budget.max_wall_secs,
                cost_capped: self.budget.max_cost_usd.is_some(),
                log: Rc::clone(&log),
            };
            match op.repair(&pctx, failed, Box::new(channel)) {
                Ok(fixes) => fixes,
                Err(ProposeError::Llm(e)) if e.is_retryable() => Vec::new(),
                Err(e) => return Err(AgentError::Other(format!("repair: {e}"))),
            }
        };
        let calls: Vec<RecordedCall> = log.borrow_mut().drain(..).collect();
        for call in &calls {
            self.accrue_and_record(&call.req, &call.resp, system)?;
        }
        Ok(fixes)
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
                TerminatedBy::Incomplete | TerminatedBy::BudgetExceeded(_) => {
                    Some(GoalStatus::Active)
                }
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

/// One buffered proposal LLM call; the controller drains these after `propose` to
/// accrue budget and record the replay trace.
struct RecordedCall {
    req: CompletionRequest,
    resp: CompletionResponse,
}

/// The OWNED one-shot LLM channel handed to a proposal operator. It runs each call
/// through the same retry path as the cognition loop and BUFFERS every
/// (request, response) into a shared log; the controller drains the log after
/// `propose` to accrue budget and record the trace. Decoupled from `Ctx` (the
/// controller no longer completes inline) so the channel is `'static` and can be
/// wrapped for another language runtime. Single-threaded by
/// construction (`Rc`/`RefCell`), matching the sequential discovery loop.
struct OwnedChannel {
    llm: Arc<dyn LLMClient>,
    retry: RetryPolicy,
    started: Instant,
    max_wall_secs: u64,
    cost_capped: bool,
    log: Rc<RefCell<Vec<RecordedCall>>>,
}

impl Completer for OwnedChannel {
    fn complete(&mut self, req: &CompletionRequest) -> Result<CompletionResponse, ProposeError> {
        // The proposal operator OWNS its request's sampling (its explore/exploit
        // temperature schedule, or its deliberate omission) - pass it untouched.
        let resp = retry_complete(
            &*self.llm,
            self.retry,
            self.started,
            self.max_wall_secs,
            req,
        )
        .map_err(ProposeError::Llm)?;
        // Fail closed PER CALL (as accrue_and_record does): an unpriced response
        // cannot be cost-capped, so a multi-call operator stops on the first one.
        if self.cost_capped && resp.usage.cost_usd.is_none() {
            return Err(ProposeError::Failed(format!(
                "max_cost_usd is set but model '{}' returned an unpriced response; \
                 add the model to llm pricing or unset the cost cap",
                self.llm.model_id()
            )));
        }
        self.log.borrow_mut().push(RecordedCall {
            req: req.clone(),
            resp: resp.clone(),
        });
        Ok(resp)
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
                break 'search TerminatedBy::BudgetExceeded(cap);
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
            // retry_complete waits out a transient proposer error until the wall-clock
            // budget, so a blip never reaches here. A retryable error that does reach
            // here (an outage past the wall budget, or one returned directly) folds into
            // the idle path and the next budget check ends the run. Non-retryable is fatal.
            let mut unreachable: Option<String> = None;
            let candidates = {
                let pctx = ProposalContext {
                    goal: &dgoal.goal,
                    elites: &elites,
                    system: &system,
                };
                let channel = OwnedChannel {
                    llm: Arc::clone(&self.llm),
                    retry: self.config.retry,
                    started: self.started,
                    max_wall_secs: self.budget.max_wall_secs,
                    cost_capped: self.budget.max_cost_usd.is_some(),
                    log: Rc::clone(&log),
                };
                match op.propose(&pctx, Box::new(channel)) {
                    Ok(candidates) => candidates,
                    Err(ProposeError::Llm(e)) if e.is_retryable() => {
                        unreachable = Some(e.to_string());
                        Vec::new()
                    }
                    Err(e) => return Err(AgentError::Other(format!("proposer: {e}"))),
                }
            };
            // Drain the buffered calls: accrue budget + record the replay trace (the
            // accounting the controller used to do inline), then summarize the last reply.
            let calls: Vec<RecordedCall> = log.borrow_mut().drain(..).collect();
            let mut last_reply: Option<ReplyDigest> = None;
            for call in &calls {
                self.accrue_and_record(&call.req, &call.resp, &system)?;
                last_reply = Some(ReplyDigest::of(&call.resp));
            }

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
                    break 'search TerminatedBy::BudgetExceeded(cap);
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
                            self.repair_candidate(&op, &rejected, &system, &elites, &dgoal)?;
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
            if ledger
                .best_verified
                .map_or(true, |(best, _)| p.score > best)
            {
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
        Some("Error") => FinishReason::Error,
        _ => FinishReason::Stop,
    };
    CompletionResponse {
        message: AssistantMessage {
            content,
            tool_calls,
        },
        usage: TokenUsage::default(),
        finish_reason,
    }
}

/// An [`LLMClient`] that replays recorded responses by `request_hash` (zero live
/// calls). Seed from [`BeliefGraph::load_llm_traces`]; an unrecorded request bumps
/// [`ReplayClient::misses`] and errors, so a faithful replay has `misses() == 0`.
pub(crate) struct ReplayClient {
    responses: FxHashMap<String, CompletionResponse>,
    model_id: String,
    misses: AtomicU32,
}

impl ReplayClient {
    pub fn from_traces(model_id: impl Into<String>, traces: Vec<(String, Value)>) -> Self {
        Self {
            responses: traces
                .into_iter()
                .map(|(hash, value)| (hash, value_to_response(&value)))
                .collect(),
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
            Some(resp) => Ok(resp.clone()),
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
    use crate::llm::factory::testing;
    use crate::verify::{CheckerAttestation, VerifyError, VerifyOutcome};
    use citadel::{Argon2Profile, DatabaseBuilder};
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

    fn agent_with(
        responses: Vec<CompletionResponse>,
        budget: AgentBudget,
    ) -> (tempfile::TempDir, Agent) {
        let (dir, eng) = region();
        let graph = BeliefGraph::new(eng, "agent");
        let llm = testing::scripted(responses);
        let agent = Agent::new(
            llm,
            graph,
            ToolRegistry::new(),
            budget,
            AgentConfig::default(),
        );
        (dir, agent)
    }

    fn agent_with_config(
        responses: Vec<CompletionResponse>,
        config: AgentConfig,
    ) -> (tempfile::TempDir, Agent) {
        let (dir, eng) = region();
        let graph = BeliefGraph::new(eng, "agent");
        let llm = testing::scripted(responses);
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
            CompletionResponse::text("plain reply, no plan"),
        )
    }

    fn fast_retry() -> AgentConfig {
        AgentConfig {
            retry: RetryPolicy {
                base_ms: 0,
                max_ms: 0,
            },
            ..Default::default()
        }
    }

    #[test]
    fn cost_cap_with_an_unpriced_model_fails_closed() {
        // An unpriced response accrues $0, so a configured max_cost_usd could
        // never engage - the run must refuse instead of silently not capping.
        let budget = AgentBudget {
            max_cost_usd: Some(1.0),
            ..Default::default()
        };
        let (_dir, agent) = agent_with(vec![CompletionResponse::text("hi")], budget);
        let err = agent.run("goal").expect_err("unpriced + cost cap refuses");
        assert!(
            err.to_string().contains("unpriced"),
            "the refusal names the cause: {err}"
        );
    }

    #[test]
    fn retry_backoff_is_capped_jittered_and_honors_retry_after() {
        let p = RetryPolicy {
            base_ms: 100,
            max_ms: 1_000,
        };
        // Equal jitter: every delay lands in [cap/2, cap] of the capped exponential.
        for (attempt, lo, hi) in [(1u32, 50u64, 100u64), (2, 100, 200), (3, 200, 400)] {
            for _ in 0..64 {
                let d = p.delay_ms(attempt, None);
                assert!(
                    (lo..=hi).contains(&d),
                    "attempt {attempt}: {d} not in [{lo},{hi}]"
                );
            }
        }
        // The exponential is capped at max_ms before jitter, so attempt 10 -> [500, 1000].
        for _ in 0..64 {
            let d = p.delay_ms(10, None);
            assert!((500..=1_000).contains(&d), "capped: {d}");
        }
        // A 2s Retry-After is a hard floor - honored even above max_ms.
        assert_eq!(
            p.delay_ms(1, Some(2)),
            2_000,
            "Retry-After is respected even when it exceeds max_ms"
        );
    }

    #[test]
    fn retryable_error_is_retried_until_success() {
        let (_d, agent) = agent_with_llm(flaky(2, 503).client(), fast_retry());
        assert!(
            agent.run("do it").is_ok(),
            "a transient error must not abort the run when retries cover it"
        );
    }

    #[test]
    fn non_retryable_error_is_not_retried() {
        let flaky = flaky(1, 400);
        let (_d, agent) = agent_with_llm(flaky.client(), fast_retry());
        let err = agent.run("do it").unwrap_err();
        assert!(matches!(err, AgentError::Llm(_)), "a 4xx propagates");
        assert_eq!(flaky.calls(), 1, "a terminal error is not retried");
    }

    #[test]
    fn a_transient_retries_past_the_old_attempt_cap_until_success() {
        // No attempt cap: a transient retries until it succeeds (here after 5
        // failures, past the old cap of 3), bounded only by wall-clock.
        let flaky = flaky(5, 503);
        let (_d, agent) = agent_with_llm(flaky.client(), fast_retry());
        assert!(
            agent.run("do it").is_ok(),
            "a persistent-but-recovering transient retries until success"
        );
        assert_eq!(
            flaky.calls(),
            6,
            "5 transient failures, then success on the 6th"
        );
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
        let llm = testing::scripted(vec![
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
        let replay = crate::llm::factory::replay_from_graph(agent1.graph()).unwrap();
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
        let cap = testing::capturing(vec![
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
            testing::scripted(responses),
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
            testing::scripted(responses),
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
            testing::scripted(vec![
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
        let cap = testing::capturing(vec![
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
            testing::scripted(vec![
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
}
