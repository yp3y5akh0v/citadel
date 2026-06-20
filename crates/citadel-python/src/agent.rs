//! The agent runtime: a cognition loop over a memory region's [`PyBeliefGraph`].

use std::sync::Arc;

use citadel_ai::{
    Agent, AgentBudget, AgentConfig, AgentReport, BeliefGraph, BudgetExceeded, Candidate,
    Completer, DiscoveryGoal, DiscoveryReport, Elite, Goal, LlmProposer, PromptId, PromptLibrary,
    ProposalContext, ProposalOperator, ProposeError, RetryPolicy, TerminatedBy, ToolRegistry,
    VerifiedKind, Verifier,
};
use citadel_mem::{EdgeKind, FusionWeights, GraphExpand, MemoryEngine, RecallProfile};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;
use serde_json::Value as Json;

use crate::errors::programming_err;
use crate::graph::{goal_to_py, parse_verified_kind, verified_kind_name, PyBeliefGraph, PyGoal};
use crate::llm::{build_llm, request_from_py, response_to_py};
use crate::mem::PyMemory;
use crate::tools::PyToolRegistry;
use crate::verify::build_verifier;
use crate::{json_to_py, py_to_json, to_pyerr};

// ---- termination -----------------------------------------------------------

fn terminated_by_category(t: TerminatedBy) -> &'static str {
    match t {
        TerminatedBy::Success => "success",
        TerminatedBy::Incomplete => "incomplete",
        TerminatedBy::DriftExceeded => "drift_exceeded",
        TerminatedBy::BudgetExceeded(_) => "budget_exceeded",
    }
}

/// The specific cap that fired, when a run ended on `budget_exceeded`.
fn budget_exceeded_cap(t: TerminatedBy) -> Option<&'static str> {
    match t {
        TerminatedBy::BudgetExceeded(b) => Some(match b {
            BudgetExceeded::Steps => "steps",
            BudgetExceeded::Tokens => "tokens",
            BudgetExceeded::Wall => "wall",
            BudgetExceeded::Cost => "cost",
            BudgetExceeded::Proposals => "proposals",
            BudgetExceeded::CheckerCalls => "checker_calls",
        }),
        _ => None,
    }
}

// ---- budget ----------------------------------------------------------------

/// Hard caps that abort a run (checked at every transition). Unset fields keep
/// their defaults (50 steps / 1M tokens / 600s; cost uncapped).
#[pyclass(name = "AgentBudget")]
pub(crate) struct PyAgentBudget {
    inner: AgentBudget,
}

impl PyAgentBudget {
    pub(crate) fn budget(&self) -> AgentBudget {
        self.inner
    }
}

#[pymethods]
impl PyAgentBudget {
    #[new]
    #[pyo3(signature = (*, max_steps=None, max_tokens=None, max_wall_secs=None, max_cost_usd=None, max_proposals=None, max_checker_calls=None))]
    fn new(
        max_steps: Option<u32>,
        max_tokens: Option<u64>,
        max_wall_secs: Option<u64>,
        max_cost_usd: Option<f64>,
        max_proposals: Option<u32>,
        max_checker_calls: Option<u32>,
    ) -> Self {
        let d = AgentBudget::default();
        Self {
            inner: AgentBudget {
                max_steps: max_steps.unwrap_or(d.max_steps),
                max_tokens: max_tokens.unwrap_or(d.max_tokens),
                max_wall_secs: max_wall_secs.unwrap_or(d.max_wall_secs),
                max_cost_usd: max_cost_usd.or(d.max_cost_usd),
                max_proposals: max_proposals.unwrap_or(d.max_proposals),
                max_checker_calls: max_checker_calls.unwrap_or(d.max_checker_calls),
            },
        }
    }

    #[getter]
    fn max_steps(&self) -> u32 {
        self.inner.max_steps
    }

    #[getter]
    fn max_tokens(&self) -> u64 {
        self.inner.max_tokens
    }

    #[getter]
    fn max_wall_secs(&self) -> u64 {
        self.inner.max_wall_secs
    }

    #[getter]
    fn max_cost_usd(&self) -> Option<f64> {
        self.inner.max_cost_usd
    }

    #[getter]
    fn max_proposals(&self) -> u32 {
        self.inner.max_proposals
    }

    #[getter]
    fn max_checker_calls(&self) -> u32 {
        self.inner.max_checker_calls
    }
}

// ---- discovery proposal operator -------------------------------------------

/// The built-in LLM proposal operator for discovery: it asks the agent's LLM for
/// candidate artifacts. Configure sampling and the artifact JSON schema.
#[pyclass(name = "LlmProposer")]
pub(crate) struct PyLlmProposer {
    temperature: f32,
    max_tokens: Option<u32>,
    artifact_schema: Option<Json>,
    plain_json: bool,
}

impl PyLlmProposer {
    fn build(&self) -> Arc<dyn ProposalOperator> {
        let mut p = LlmProposer::new().with_temperature(self.temperature);
        if let Some(max_tokens) = self.max_tokens {
            p = p.with_max_tokens(max_tokens);
        }
        if let Some(schema) = &self.artifact_schema {
            p = p.with_artifact_schema(schema.clone());
        }
        if self.plain_json {
            p = p.with_plain_json();
        }
        Arc::new(p)
    }
}

#[pymethods]
impl PyLlmProposer {
    #[new]
    #[pyo3(signature = (*, temperature=0.9, max_tokens=None, artifact_schema=None, plain_json=false))]
    fn new(
        py: Python<'_>,
        temperature: f32,
        max_tokens: Option<u32>,
        artifact_schema: Option<&Bound<'_, PyAny>>,
        plain_json: bool,
    ) -> PyResult<Self> {
        Ok(Self {
            temperature,
            max_tokens,
            artifact_schema: artifact_schema.map(|s| py_to_json(py, s)).transpose()?,
            plain_json,
        })
    }
}

// ---- the Python proposal-operator bridge -----------------------------------

fn pyerr_to_propose(e: PyErr) -> ProposeError {
    ProposeError::Failed(e.to_string())
}

/// LLM channel for a Python proposal operator; valid only during one `propose`
/// call. The bridge clears it after, so a stashed handle raises (no untracked calls).
#[pyclass(name = "Completer", unsendable)]
pub(crate) struct PyCompleter {
    inner: Option<Box<dyn Completer>>,
}

#[pymethods]
impl PyCompleter {
    /// Run one completion. `request` is a dict (messages/tools/...); returns a dict
    /// (content/tool_calls/usage/finish_reason).
    fn complete(&mut self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let completer = self.inner.as_mut().ok_or_else(|| {
            programming_err("completer is valid only during propose(); it cannot be used afterward")
        })?;
        let req = request_from_py(request)?;
        let resp = completer.complete(&req).map_err(to_pyerr)?;
        response_to_py(py, &resp)
    }
}

/// Serialize a proposal round's context (goal + elite archive + proposer prompt).
fn proposal_context_to_py(py: Python<'_>, ctx: &ProposalContext<'_>) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("goal", goal_to_py(py, ctx.goal)?)?;
    let elites = ctx
        .elites
        .iter()
        .map(|e| elite_to_py(py, e))
        .collect::<PyResult<Vec<_>>>()?;
    d.set_item("elites", elites)?;
    d.set_item("system", ctx.system.text.clone())?;
    d.into_py_any(py)
}

fn elite_to_py(py: Python<'_>, elite: &Elite) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("atom", elite.atom)?;
    d.set_item("artifact", json_to_py(py, &elite.artifact)?)?;
    d.set_item("score", elite.score)?;
    d.into_py_any(py)
}

/// A `propose` result is a list of candidate artifacts (each any JSON value);
/// lineage/rationale default, matching the built-in proposer's fresh candidates.
fn candidates_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Vec<Candidate>> {
    obj.extract::<Vec<Bound<'_, PyAny>>>()
        .map_err(|_| PyValueError::new_err("propose() must return a list of candidate artifacts"))?
        .iter()
        .map(|artifact| {
            Ok(Candidate {
                artifact: py_to_json(artifact.py(), artifact)?,
                parent: None,
                rationale: "python-proposed".to_string(),
            })
        })
        .collect()
}

/// Adapts a Python proposal operator to [`ProposalOperator`]. Its
/// `propose(ctx, llm) -> list` receives the context dict and an owned `Completer`.
struct PyProposalOperator {
    callable: Py<PyAny>,
}

impl ProposalOperator for PyProposalOperator {
    fn propose(
        &self,
        ctx: &ProposalContext<'_>,
        llm: Box<dyn Completer>,
    ) -> Result<Vec<Candidate>, ProposeError> {
        Python::attach(|py| {
            let ctx_dict = proposal_context_to_py(py, ctx).map_err(pyerr_to_propose)?;
            let completer =
                Py::new(py, PyCompleter { inner: Some(llm) }).map_err(pyerr_to_propose)?;
            let result = self
                .callable
                .bind(py)
                .call_method1("propose", (ctx_dict, completer.clone_ref(py)));
            // Poison the channel no matter how propose ended (success or raise).
            completer.borrow_mut(py).inner = None;
            candidates_from_py(&result.map_err(pyerr_to_propose)?).map_err(pyerr_to_propose)
        })
    }
}

/// Resolve `set_proposal_operator`: a built-in `LlmProposer`, else a Python operator.
pub(crate) fn build_proposal_operator(
    obj: &Bound<'_, PyAny>,
) -> PyResult<Arc<dyn ProposalOperator>> {
    if let Ok(builtin) = obj.extract::<PyRef<'_, PyLlmProposer>>() {
        return Ok(builtin.build());
    }
    Ok(Arc::new(PyProposalOperator {
        callable: obj.clone().unbind(),
    }))
}

// ---- prompt overrides ------------------------------------------------------

fn prompt_id(id: &str) -> PyResult<PromptId> {
    PromptId::from_name(id).ok_or_else(|| {
        PyValueError::new_err(format!(
            "unknown prompt id '{id}' \
             (planner|execute|reflect|constraint_critic|acceptance_critic|proposer)"
        ))
    })
}

fn parse_recall_edge_kind(s: &str) -> PyResult<EdgeKind> {
    match s.to_ascii_lowercase().as_str() {
        "causes" => Ok(EdgeKind::Causes),
        "contradicts" => Ok(EdgeKind::Contradicts),
        "refines" => Ok(EdgeKind::Refines),
        "precedes" => Ok(EdgeKind::Precedes),
        "supersedes" => Ok(EdgeKind::Supersedes),
        "derived_from" => Ok(EdgeKind::DerivedFrom),
        "depends_on" => Ok(EdgeKind::DependsOn),
        other => Err(PyValueError::new_err(format!(
            "unknown edge kind '{other}' \
             (causes|contradicts|refines|precedes|supersedes|derived_from|depends_on)"
        ))),
    }
}

/// Versioned overrides for the loop's LLM prompts; unset ids use the shipped defaults.
#[pyclass(name = "PromptLibrary")]
#[derive(Default)]
pub(crate) struct PyPromptLibrary {
    inner: PromptLibrary,
}

#[pymethods]
impl PyPromptLibrary {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    /// Override a prompt. `id`: planner|execute|reflect|constraint_critic|
    /// acceptance_critic|proposer. `version` should not regress the shipped default.
    fn set(&mut self, id: &str, version: u32, text: String) -> PyResult<()> {
        self.inner.set(prompt_id(id)?, version, text);
        Ok(())
    }

    /// The resolved prompt text for an id (override or shipped default).
    fn resolve(&self, id: &str) -> PyResult<String> {
        Ok(self.inner.resolve(prompt_id(id)?).text)
    }

    /// Load prompt overrides from `prompt`-kind atoms in `region` of `memory`
    /// (payload `{name, version}`); unknown names ignored, highest version wins.
    #[staticmethod]
    fn from_region(memory: &PyMemory, region: &str) -> PyResult<Self> {
        let inner = PromptLibrary::from_region(&memory.engine(), region).map_err(to_pyerr)?;
        Ok(Self { inner })
    }
}

// ---- config ----------------------------------------------------------------

/// Agent tuning plus the optional verifier, discovery proposal operator, and
/// prompt overrides. Defaults mirror the engine's `AgentConfig::default`.
#[pyclass(name = "AgentConfig")]
pub(crate) struct PyAgentConfig {
    drift_bound: u32,
    max_replans: u32,
    max_tool_attempts: u32,
    max_react_steps: u32,
    recall_context_k: usize,
    recall_context: RecallProfile,
    temperature: f32,
    retry: RetryPolicy,
    verifier: Option<Arc<dyn Verifier>>,
    proposal_operator: Option<Arc<dyn ProposalOperator>>,
    max_repairs: u32,
    prompt_library: PromptLibrary,
}

impl Default for PyAgentConfig {
    fn default() -> Self {
        let c = AgentConfig::default();
        Self {
            drift_bound: c.drift_bound,
            max_replans: c.max_replans,
            max_tool_attempts: c.max_tool_attempts,
            max_react_steps: c.max_react_steps,
            recall_context_k: c.recall_context_k,
            recall_context: c.recall_context,
            temperature: c.temperature,
            retry: c.retry,
            verifier: c.verifier,
            proposal_operator: c.proposal_operator,
            max_repairs: c.max_repairs,
            prompt_library: PromptLibrary::default(),
        }
    }
}

impl PyAgentConfig {
    fn to_config(&self) -> AgentConfig {
        AgentConfig {
            drift_bound: self.drift_bound,
            max_replans: self.max_replans,
            max_tool_attempts: self.max_tool_attempts,
            max_react_steps: self.max_react_steps,
            recall_context_k: self.recall_context_k,
            recall_context: self.recall_context.clone(),
            retry: self.retry,
            verifier: self.verifier.clone(),
            prompt_library: Arc::new(self.prompt_library.clone()),
            proposal_operator: self.proposal_operator.clone(),
            max_repairs: self.max_repairs,
            temperature: self.temperature,
        }
    }
}

#[pymethods]
impl PyAgentConfig {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    #[getter]
    fn drift_bound(&self) -> u32 {
        self.drift_bound
    }

    #[setter]
    fn set_drift_bound(&mut self, value: u32) {
        self.drift_bound = value;
    }

    #[getter]
    fn max_replans(&self) -> u32 {
        self.max_replans
    }

    #[setter]
    fn set_max_replans(&mut self, value: u32) {
        self.max_replans = value;
    }

    #[getter]
    fn max_tool_attempts(&self) -> u32 {
        self.max_tool_attempts
    }

    #[setter]
    fn set_max_tool_attempts(&mut self, value: u32) {
        self.max_tool_attempts = value;
    }

    #[getter]
    fn max_react_steps(&self) -> u32 {
        self.max_react_steps
    }

    #[setter]
    fn set_max_react_steps(&mut self, value: u32) {
        self.max_react_steps = value;
    }

    #[getter]
    fn max_repairs(&self) -> u32 {
        self.max_repairs
    }

    #[setter]
    fn set_max_repairs(&mut self, value: u32) {
        self.max_repairs = value;
    }

    #[getter]
    fn recall_context_k(&self) -> usize {
        self.recall_context_k
    }

    #[setter]
    fn set_recall_context_k(&mut self, value: usize) {
        self.recall_context_k = value;
    }

    /// Configure the always-on recall fusion weights used to inject context per subtask.
    fn set_recall_context_weights(
        &mut self,
        semantic: f32,
        keyword: f32,
        recency: f32,
        importance: f32,
    ) {
        self.recall_context.weights = FusionWeights {
            semantic,
            keyword,
            recency,
            importance,
        };
    }

    /// Configure graph expansion for always-on context recall.
    fn set_recall_context_graph_expand(
        &mut self,
        depth: usize,
        edge_kinds: Vec<String>,
    ) -> PyResult<()> {
        let kinds = edge_kinds
            .iter()
            .map(|s| parse_recall_edge_kind(s))
            .collect::<PyResult<Vec<_>>>()?;
        self.recall_context.graph_expand = Some(GraphExpand::new(depth, kinds));
        Ok(())
    }

    /// Disable graph expansion for always-on context recall.
    fn clear_recall_context_graph_expand(&mut self) {
        self.recall_context.graph_expand = None;
    }

    #[getter]
    fn temperature(&self) -> f32 {
        self.temperature
    }

    #[setter]
    fn set_temperature(&mut self, value: f32) {
        self.temperature = value;
    }

    /// Capped, jittered backoff for transient tool/LLM failures. Transient errors
    /// retry until the wall-clock budget (no attempt cap); base/ceiling only tune it.
    fn set_retry(&mut self, base_ms: u64, max_ms: u64) {
        self.retry = RetryPolicy { base_ms, max_ms };
    }

    /// Set the deterministic verifier (a Python object implementing the verifier
    /// protocol). A verifier with `checker_id`/`checker_version` may mint.
    fn set_verifier(&mut self, verifier: &Bound<'_, PyAny>) -> PyResult<()> {
        self.verifier = Some(build_verifier(verifier)?);
        Ok(())
    }

    /// Set the discovery proposal operator: a built-in `LlmProposer`, or a Python
    /// object with `propose(ctx, llm) -> list` (required by `run_discovery`).
    fn set_proposal_operator(&mut self, proposer: &Bound<'_, PyAny>) -> PyResult<()> {
        self.proposal_operator = Some(build_proposal_operator(proposer)?);
        Ok(())
    }

    /// Override the loop's prompts.
    fn set_prompt_library(&mut self, library: &PyPromptLibrary) {
        self.prompt_library = library.inner.clone();
    }
}

// ---- discovery goal --------------------------------------------------------

/// Configures a discovery search: a goal seed, the kind to mint, and the
/// baseline/convergence bounds.
#[pyclass(name = "DiscoveryGoal")]
pub(crate) struct PyDiscoveryGoal {
    goal: Goal,
    kind: VerifiedKind,
    baseline_score: f64,
    archive_width: usize,
    max_idle_rounds: u32,
    max_mints: u32,
}

impl PyDiscoveryGoal {
    fn to_goal(&self) -> DiscoveryGoal {
        DiscoveryGoal {
            goal: self.goal.clone(),
            kind: self.kind,
            baseline_score: self.baseline_score,
            archive_width: self.archive_width,
            max_idle_rounds: self.max_idle_rounds,
            max_mints: self.max_mints,
        }
    }
}

#[pymethods]
impl PyDiscoveryGoal {
    #[new]
    #[pyo3(signature = (goal, *, kind="construction", baseline_score=0.0, archive_width=16, max_idle_rounds=5, max_mints=100))]
    fn new(
        goal: &PyGoal,
        kind: &str,
        baseline_score: f64,
        archive_width: usize,
        max_idle_rounds: u32,
        max_mints: u32,
    ) -> PyResult<Self> {
        Ok(Self {
            goal: goal.inner.clone(),
            kind: parse_verified_kind(kind)?,
            baseline_score,
            archive_width,
            max_idle_rounds,
            max_mints,
        })
    }

    #[getter]
    fn goal(&self) -> PyGoal {
        PyGoal {
            inner: self.goal.clone(),
        }
    }

    #[getter]
    fn kind(&self) -> &'static str {
        verified_kind_name(self.kind)
    }

    #[getter]
    fn baseline_score(&self) -> f64 {
        self.baseline_score
    }

    #[getter]
    fn archive_width(&self) -> usize {
        self.archive_width
    }

    #[getter]
    fn max_idle_rounds(&self) -> u32 {
        self.max_idle_rounds
    }

    #[getter]
    fn max_mints(&self) -> u32 {
        self.max_mints
    }
}

// ---- reports ---------------------------------------------------------------

/// The outcome of `Agent.run`.
#[pyclass(name = "AgentReport")]
pub(crate) struct PyAgentReport {
    inner: AgentReport,
}

#[pymethods]
impl PyAgentReport {
    #[getter]
    fn goal_id(&self) -> Option<i64> {
        self.inner.goal_id
    }

    #[getter]
    fn final_answer(&self) -> Option<String> {
        self.inner.final_answer.clone()
    }

    #[getter]
    fn tasks_done(&self) -> u32 {
        self.inner.tasks_done
    }

    /// `success` | `incomplete` | `drift_exceeded` | `budget_exceeded`.
    #[getter]
    fn terminated_by(&self) -> &'static str {
        terminated_by_category(self.inner.terminated_by)
    }

    /// The cap that fired if `terminated_by == budget_exceeded`, else `None`.
    #[getter]
    fn budget_exceeded(&self) -> Option<&'static str> {
        budget_exceeded_cap(self.inner.terminated_by)
    }

    #[getter]
    fn chain_valid(&self) -> bool {
        self.inner.chain_valid
    }

    fn __repr__(&self) -> String {
        format!(
            "AgentReport(terminated_by={}, tasks_done={}, chain_valid={})",
            terminated_by_category(self.inner.terminated_by),
            self.inner.tasks_done,
            self.inner.chain_valid
        )
    }
}

/// The outcome of `Agent.run_discovery`.
#[pyclass(name = "DiscoveryReport")]
pub(crate) struct PyDiscoveryReport {
    inner: DiscoveryReport,
}

#[pymethods]
impl PyDiscoveryReport {
    #[getter]
    fn best_score(&self) -> f64 {
        self.inner.best_score
    }

    #[getter]
    fn best_artifact(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match &self.inner.best_artifact {
            Some(v) => Ok(Some(json_to_py(py, v)?)),
            None => Ok(None),
        }
    }

    #[getter]
    fn verified(&self) -> Option<i64> {
        self.inner.verified
    }

    #[getter]
    fn minted(&self) -> Vec<i64> {
        self.inner.minted.clone()
    }

    #[getter]
    fn proposals(&self) -> u32 {
        self.inner.proposals
    }

    #[getter]
    fn checker_calls(&self) -> u32 {
        self.inner.checker_calls
    }

    #[getter]
    fn best_valid_score(&self) -> f64 {
        self.inner.best_valid_score
    }

    #[getter]
    fn valid_candidates(&self) -> u32 {
        self.inner.valid_candidates
    }

    #[getter]
    fn sample_reject_reason(&self) -> Option<String> {
        self.inner.sample_reject_reason.clone()
    }

    #[getter]
    fn barren_rounds(&self) -> u32 {
        self.inner.barren_rounds
    }

    #[getter]
    fn cross_check_failures(&self) -> u32 {
        self.inner.cross_check_failures
    }

    #[getter]
    fn terminated_by(&self) -> &'static str {
        terminated_by_category(self.inner.terminated_by)
    }

    #[getter]
    fn budget_exceeded(&self) -> Option<&'static str> {
        budget_exceeded_cap(self.inner.terminated_by)
    }

    #[getter]
    fn chain_valid(&self) -> bool {
        self.inner.chain_valid
    }
}

// ---- the agent -------------------------------------------------------------

/// A single-agent cognition runtime over one memory region.
#[pyclass(name = "Agent")]
pub(crate) struct PyAgent {
    inner: Agent,
    engine: Arc<MemoryEngine>,
    region: String,
}

#[pymethods]
impl PyAgent {
    /// Build an agent over `region` (must already exist with an embedder). `llm`
    /// is an `LLMClient` handle or any object with `model_id` + `complete`.
    #[new]
    #[pyo3(signature = (memory, region, llm, *, tools=None, budget=None, config=None))]
    fn new(
        memory: &PyMemory,
        region: &str,
        llm: &Bound<'_, PyAny>,
        tools: Option<&Bound<'_, PyToolRegistry>>,
        budget: Option<&PyAgentBudget>,
        config: Option<&PyAgentConfig>,
    ) -> PyResult<Self> {
        let engine = memory.engine();
        let llm = build_llm(llm)?;
        let graph = BeliefGraph::new(Arc::clone(&engine), region.to_string());
        let tools = match tools {
            Some(t) => PyToolRegistry::take(t),
            None => ToolRegistry::new(),
        };
        let budget = budget.map(PyAgentBudget::budget).unwrap_or_default();
        let config = config.map(PyAgentConfig::to_config).unwrap_or_default();
        Ok(Self {
            inner: Agent::new(llm, graph, tools, budget, config),
            engine,
            region: region.to_string(),
        })
    }

    /// Run the cognition loop on `prompt` to a terminal state. Releases the GIL
    /// during the run (a Python LLM callback re-acquires it when invoked).
    fn run(&self, py: Python<'_>, prompt: String) -> PyResult<PyAgentReport> {
        let report = py.detach(|| self.inner.run(prompt)).map_err(to_pyerr)?;
        Ok(PyAgentReport { inner: report })
    }

    /// Run a discovery search. Requires `config.proposal_operator` and an attested
    /// `config.verifier` (the sole authority that gates minting).
    fn run_discovery(&self, py: Python<'_>, goal: &PyDiscoveryGoal) -> PyResult<PyDiscoveryReport> {
        let goal = goal.to_goal();
        let report = py
            .detach(|| self.inner.run_discovery(goal))
            .map_err(to_pyerr)?;
        Ok(PyDiscoveryReport { inner: report })
    }

    /// A read/write view of the agent's belief graph (inspect goals/tasks/audit
    /// chain after a run).
    fn graph(&self) -> PyBeliefGraph {
        PyBeliefGraph::open(Arc::clone(&self.engine), &self.region)
    }
}
