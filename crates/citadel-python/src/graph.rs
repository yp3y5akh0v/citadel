//! The Belief-and-Goal graph: an agent's verifiable reasoning state over a region.

use std::sync::Arc;

use citadel_ai::{
    BeliefGraph, ChainReport, CheckerAttestation, CoInstantiationCheck, Evidence, Goal, GoalStatus,
    Hypothesis, Reflection, SelfModel, Task, TaskStatus, TraceEvictionPolicy, VerifiedExport,
    VerifiedKind,
};
use citadel_mem::MemoryEngine;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;

use crate::mem::{PyAtomHit, PyEvictionPolicy, PyMemory};
use crate::{json_to_py, py_to_json, to_pyerr};

// ---- string <-> enum -------------------------------------------------------

fn parse_goal_status(s: &str) -> PyResult<GoalStatus> {
    match s.to_ascii_lowercase().as_str() {
        "active" => Ok(GoalStatus::Active),
        "achieved" => Ok(GoalStatus::Achieved),
        "abandoned" => Ok(GoalStatus::Abandoned),
        other => Err(PyValueError::new_err(format!(
            "unknown goal status '{other}' (active|achieved|abandoned)"
        ))),
    }
}

fn parse_task_status(s: &str) -> PyResult<TaskStatus> {
    match s.to_ascii_lowercase().as_str() {
        "pending" => Ok(TaskStatus::Pending),
        "in_progress" | "inprogress" => Ok(TaskStatus::InProgress),
        "done" => Ok(TaskStatus::Done),
        "failed" => Ok(TaskStatus::Failed),
        other => Err(PyValueError::new_err(format!(
            "unknown task status '{other}' (pending|in_progress|done|failed)"
        ))),
    }
}

/// Python-facing artifact kind. `VerifiedKind::as_str` is the atom-kind string
/// (`verified_construction`); the binding speaks the shorter `construction`/`lemma`.
pub(crate) fn parse_verified_kind(s: &str) -> PyResult<VerifiedKind> {
    match s.to_ascii_lowercase().as_str() {
        "construction" => Ok(VerifiedKind::Construction),
        "lemma" => Ok(VerifiedKind::Lemma),
        other => Err(PyValueError::new_err(format!(
            "unknown verified kind '{other}' (construction|lemma)"
        ))),
    }
}

pub(crate) fn verified_kind_name(kind: VerifiedKind) -> &'static str {
    match kind {
        VerifiedKind::Construction => "construction",
        VerifiedKind::Lemma => "lemma",
    }
}

/// Serialize a goal to the dict a Python verifier receives.
pub(crate) fn goal_to_py(py: Python<'_>, goal: &Goal) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("prompt", &goal.prompt)?;
    d.set_item("acceptance_criteria", goal.acceptance_criteria.clone())?;
    d.set_item("constraints", goal.constraints.clone())?;
    d.set_item("target", goal.target.clone())?;
    d.into_py_any(py)
}

// ---- node types ------------------------------------------------------------

/// A crystallized goal: what "done" means plus the bounds to respect.
#[pyclass(name = "Goal")]
pub(crate) struct PyGoal {
    pub(crate) inner: Goal,
}

#[pymethods]
impl PyGoal {
    #[new]
    #[pyo3(signature = (prompt, *, acceptance_criteria=None, constraints=None, target=None))]
    fn new(
        prompt: String,
        acceptance_criteria: Option<Vec<String>>,
        constraints: Option<Vec<String>>,
        target: Option<String>,
    ) -> Self {
        Self {
            inner: Goal {
                prompt,
                acceptance_criteria: acceptance_criteria.unwrap_or_default(),
                constraints: constraints.unwrap_or_default(),
                target,
            },
        }
    }

    #[getter]
    fn prompt(&self) -> &str {
        &self.inner.prompt
    }

    #[getter]
    fn acceptance_criteria(&self) -> Vec<String> {
        self.inner.acceptance_criteria.clone()
    }

    #[getter]
    fn constraints(&self) -> Vec<String> {
        self.inner.constraints.clone()
    }

    #[getter]
    fn target(&self) -> Option<String> {
        self.inner.target.clone()
    }

    fn __repr__(&self) -> String {
        format!("Goal(prompt={:?})", self.inner.prompt)
    }
}

/// A subtask in the plan DAG.
#[pyclass(name = "Task")]
pub(crate) struct PyTask {
    pub(crate) inner: Task,
}

#[pymethods]
impl PyTask {
    #[new]
    #[pyo3(signature = (description, *, status="pending", attempts=0, last_error=None))]
    fn new(
        description: String,
        status: &str,
        attempts: u32,
        last_error: Option<String>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: Task {
                description,
                status: parse_task_status(status)?,
                attempts,
                last_error,
            },
        })
    }

    #[getter]
    fn description(&self) -> &str {
        &self.inner.description
    }

    #[getter]
    fn status(&self) -> &'static str {
        self.inner.status.as_str()
    }

    #[getter]
    fn attempts(&self) -> u32 {
        self.inner.attempts
    }

    #[getter]
    fn last_error(&self) -> Option<String> {
        self.inner.last_error.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "Task(description={:?}, status={})",
            self.inner.description,
            self.inner.status.as_str()
        )
    }
}

/// A candidate approach, not yet committed; `refines` a goal.
#[pyclass(name = "Hypothesis")]
pub(crate) struct PyHypothesis {
    pub(crate) inner: Hypothesis,
}

#[pymethods]
impl PyHypothesis {
    #[new]
    #[pyo3(signature = (summary, confidence=1.0))]
    fn new(summary: String, confidence: f32) -> Self {
        Self {
            inner: Hypothesis {
                summary,
                confidence,
            },
        }
    }

    #[getter]
    fn summary(&self) -> &str {
        &self.inner.summary
    }

    #[getter]
    fn confidence(&self) -> f32 {
        self.inner.confidence
    }
}

/// An observation or tool result, linked to what it was `derived_from`.
#[pyclass(name = "Evidence")]
pub(crate) struct PyEvidence {
    pub(crate) inner: Evidence,
}

#[pymethods]
impl PyEvidence {
    #[new]
    fn new(source: String, content: String) -> Self {
        Self {
            inner: Evidence { source, content },
        }
    }

    #[getter]
    fn source(&self) -> &str {
        &self.inner.source
    }

    #[getter]
    fn content(&self) -> &str {
        &self.inner.content
    }
}

/// A critique/insight note, linked to what it was `derived_from`.
#[pyclass(name = "Reflection")]
pub(crate) struct PyReflection {
    pub(crate) inner: Reflection,
}

#[pymethods]
impl PyReflection {
    #[new]
    #[pyo3(signature = (insight, confidence=1.0))]
    fn new(insight: String, confidence: f32) -> Self {
        Self {
            inner: Reflection {
                insight,
                confidence,
            },
        }
    }

    #[getter]
    fn insight(&self) -> &str {
        &self.inner.insight
    }

    #[getter]
    fn confidence(&self) -> f32 {
        self.inner.confidence
    }
}

/// The agent's functional self-model: who it is and the goal it serves.
#[pyclass(name = "SelfModel")]
pub(crate) struct PySelfModel {
    pub(crate) inner: SelfModel,
}

#[pymethods]
impl PySelfModel {
    #[new]
    #[pyo3(signature = (identity, *, goal_ref=None))]
    fn new(identity: String, goal_ref: Option<i64>) -> Self {
        Self {
            inner: SelfModel { identity, goal_ref },
        }
    }

    #[getter]
    fn identity(&self) -> &str {
        &self.inner.identity
    }

    #[getter]
    fn goal_ref(&self) -> Option<i64> {
        self.inner.goal_ref
    }

    fn __repr__(&self) -> String {
        format!("SelfModel(identity={:?})", self.inner.identity)
    }
}

/// One co-instantiation check: an action's structural provenance to the goal and
/// its constraint compliance. `verdict`/hashes are derived; the hashes are stamped
/// when `record_check` writes it into the chain.
#[pyclass(name = "CoInstantiationCheck")]
pub(crate) struct PyCoInstantiationCheck {
    pub(crate) inner: CoInstantiationCheck,
}

#[pymethods]
impl PyCoInstantiationCheck {
    #[new]
    fn new(
        action_id: String,
        goal_ref: i64,
        self_model_ref: i64,
        has_provenance: bool,
        constraints_satisfied: bool,
        drift_count: u32,
        drift_bound: u32,
    ) -> Self {
        Self {
            inner: CoInstantiationCheck::new(
                action_id,
                goal_ref,
                self_model_ref,
                has_provenance,
                constraints_satisfied,
                drift_count,
                drift_bound,
            ),
        }
    }

    #[getter]
    fn version(&self) -> u32 {
        self.inner.version
    }

    #[getter]
    fn action_id(&self) -> &str {
        &self.inner.action_id
    }

    #[getter]
    fn goal_ref(&self) -> i64 {
        self.inner.goal_ref
    }

    #[getter]
    fn self_model_ref(&self) -> i64 {
        self.inner.self_model_ref
    }

    #[getter]
    fn has_provenance(&self) -> bool {
        self.inner.has_provenance
    }

    #[getter]
    fn constraints_satisfied(&self) -> bool {
        self.inner.constraints_satisfied
    }

    #[getter]
    fn verdict(&self) -> &'static str {
        self.inner.verdict.as_str()
    }

    #[getter]
    fn drift_count(&self) -> u32 {
        self.inner.drift_count
    }

    #[getter]
    fn drift_bound(&self) -> u32 {
        self.inner.drift_bound
    }

    #[getter]
    fn timestamp_micros(&self) -> i64 {
        self.inner.timestamp_micros
    }

    #[getter]
    fn prev_hash(&self) -> &str {
        &self.inner.prev_hash
    }

    #[getter]
    fn this_hash(&self) -> &str {
        &self.inner.this_hash
    }

    fn __repr__(&self) -> String {
        format!(
            "CoInstantiationCheck(action_id={:?}, verdict={})",
            self.inner.action_id,
            self.inner.verdict.as_str()
        )
    }
}

/// Result of replaying the audit chain; `breaches` are `(atom_id, reason)`.
#[pyclass(name = "ChainReport")]
pub(crate) struct PyChainReport {
    inner: ChainReport,
}

#[pymethods]
impl PyChainReport {
    #[getter]
    fn valid(&self) -> bool {
        self.inner.valid
    }

    #[getter]
    fn total_checks(&self) -> usize {
        self.inner.total_checks
    }

    #[getter]
    fn breaches(&self) -> Vec<(i64, String)> {
        self.inner.breaches.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "ChainReport(valid={}, total_checks={}, breaches={})",
            self.inner.valid,
            self.inner.total_checks,
            self.inner.breaches.len()
        )
    }
}

/// A checker-verified artifact exported for independent re-checking.
#[pyclass(name = "VerifiedExport")]
pub(crate) struct PyVerifiedExport {
    inner: VerifiedExport,
}

#[pymethods]
impl PyVerifiedExport {
    #[getter]
    fn atom(&self) -> i64 {
        self.inner.atom
    }

    #[getter]
    fn kind(&self) -> &'static str {
        // Return the short construction/lemma vocab, not the long atom-kind string.
        if self.inner.kind == VerifiedKind::Lemma.as_str() {
            verified_kind_name(VerifiedKind::Lemma)
        } else {
            verified_kind_name(VerifiedKind::Construction)
        }
    }

    #[getter]
    fn artifact(&self) -> &str {
        &self.inner.artifact
    }

    #[getter]
    fn score(&self) -> f64 {
        self.inner.score
    }

    #[getter]
    fn checker_id(&self) -> &str {
        &self.inner.checker_id
    }

    #[getter]
    fn checker_version(&self) -> &str {
        &self.inner.checker_version
    }

    #[getter]
    fn checked_at_micros(&self) -> i64 {
        self.inner.checked_at_micros
    }
}

/// How `BeliefGraph.evict_traces` selects `llm_trace` atoms to forget.
#[pyclass(name = "TraceEvictionPolicy")]
pub(crate) struct PyTraceEvictionPolicy {
    inner: TraceEvictionPolicy,
}

#[pymethods]
impl PyTraceEvictionPolicy {
    /// Keep the `n` newest traces; delete the rest.
    #[staticmethod]
    fn keep_last_n(n: usize) -> Self {
        Self {
            inner: TraceEvictionPolicy::KeepLastN { n },
        }
    }

    /// Delete traces older than `older_than_micros`.
    #[staticmethod]
    fn by_age(older_than_micros: i64) -> Self {
        Self {
            inner: TraceEvictionPolicy::ByAge { older_than_micros },
        }
    }

    /// Keep newest traces within cumulative `max_cost_usd`; delete the rest.
    #[staticmethod]
    fn by_total_cost(max_cost_usd: f64) -> Self {
        Self {
            inner: TraceEvictionPolicy::ByTotalCost { max_cost_usd },
        }
    }
}

// ---- the graph -------------------------------------------------------------

/// Typed Belief-and-Goal graph over one memory region.
#[pyclass(name = "BeliefGraph")]
pub(crate) struct PyBeliefGraph {
    inner: BeliefGraph,
}

impl PyBeliefGraph {
    /// Open a graph over a region of an engine shared with the agent binding.
    pub(crate) fn open(engine: Arc<MemoryEngine>, region: &str) -> Self {
        Self {
            inner: BeliefGraph::new(engine, region.to_string()),
        }
    }

    /// Borrow the underlying graph (for the replay LLM client).
    pub(crate) fn belief_graph(&self) -> &BeliefGraph {
        &self.inner
    }
}

#[pymethods]
impl PyBeliefGraph {
    /// Attach to `region` (must already exist with an embedder) in `memory`.
    #[new]
    fn new(memory: &PyMemory, region: &str) -> Self {
        Self::open(memory.engine(), region)
    }

    // -- write --------------------------------------------------------------

    fn add_goal(&self, goal: &PyGoal) -> PyResult<i64> {
        self.inner.add_goal(&goal.inner).map_err(to_pyerr)
    }

    fn add_task(&self, task: &PyTask, deps: Vec<i64>, goal_id: i64) -> PyResult<i64> {
        self.inner
            .add_task(&task.inner, &deps, goal_id)
            .map_err(to_pyerr)
    }

    fn add_hypothesis(&self, hypothesis: &PyHypothesis, refines_goal: i64) -> PyResult<i64> {
        self.inner
            .add_hypothesis(&hypothesis.inner, refines_goal)
            .map_err(to_pyerr)
    }

    fn add_evidence(&self, evidence: &PyEvidence, supports: i64) -> PyResult<i64> {
        self.inner
            .add_evidence(&evidence.inner, supports)
            .map_err(to_pyerr)
    }

    fn add_reflection(&self, reflection: &PyReflection, about: i64) -> PyResult<i64> {
        self.inner
            .add_reflection(&reflection.inner, about)
            .map_err(to_pyerr)
    }

    fn add_candidate(&self, artifact: &str, score: f64) -> PyResult<i64> {
        self.inner.add_candidate(artifact, score).map_err(to_pyerr)
    }

    /// Mint a checker-verified artifact. `kind` is `construction`|`lemma`; the
    /// `checker_id`/`checker_version` form the attestation stamped onto the atom.
    fn add_verified_artifact(
        &self,
        candidate_atom: i64,
        kind: &str,
        checker_id: &str,
        checker_version: &str,
        score: f64,
    ) -> PyResult<i64> {
        self.inner
            .add_verified_artifact(
                candidate_atom,
                parse_verified_kind(kind)?,
                CheckerAttestation::new(checker_id, checker_version),
                score,
            )
            .map_err(to_pyerr)
    }

    fn set_goal_status(&self, goal_id: i64, status: &str) -> PyResult<()> {
        self.inner
            .set_goal_status(goal_id, parse_goal_status(status)?)
            .map_err(to_pyerr)
    }

    fn update_task(&self, id: i64, task: &PyTask) -> PyResult<()> {
        self.inner.update_task(id, &task.inner).map_err(to_pyerr)
    }

    fn set_task_status(&self, id: i64, status: &str) -> PyResult<()> {
        self.inner
            .set_task_status(id, parse_task_status(status)?)
            .map_err(to_pyerr)
    }

    fn record_task_failure(&self, id: i64, error: &str) -> PyResult<()> {
        self.inner.record_task_failure(id, error).map_err(to_pyerr)
    }

    /// Write the initial self-model (write-once; raises if one exists).
    fn set_self_model(&self, self_model: &PySelfModel) -> PyResult<i64> {
        self.inner
            .set_self_model(&self_model.inner)
            .map_err(to_pyerr)
    }

    /// Append a new self-model version superseding the current head.
    fn supersede_self_model(&self, self_model: &PySelfModel) -> PyResult<i64> {
        self.inner
            .supersede_self_model(&self_model.inner)
            .map_err(to_pyerr)
    }

    /// Record a co-instantiation check as an immutable, hash-linked audit atom.
    fn record_check(&self, check: &PyCoInstantiationCheck, action_atom: i64) -> PyResult<i64> {
        self.inner
            .record_check(check.inner.clone(), action_atom)
            .map_err(to_pyerr)
    }

    /// Record an LLM call as an immutable `llm_trace` atom (for deterministic replay).
    #[pyo3(signature = (request_hash, model_id, response, cost_usd=None, prompt=None))]
    fn record_llm_call(
        &self,
        py: Python<'_>,
        request_hash: &str,
        model_id: &str,
        response: &Bound<'_, PyAny>,
        cost_usd: Option<f64>,
        prompt: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<i64> {
        let response = py_to_json(py, response)?;
        let prompt = prompt.map(|p| py_to_json(py, p)).transpose()?;
        self.inner
            .record_llm_call(request_hash, model_id, &response, cost_usd, prompt.as_ref())
            .map_err(to_pyerr)
    }

    // -- read ---------------------------------------------------------------

    fn get_goal(&self, id: i64) -> PyResult<Option<PyGoal>> {
        Ok(self
            .inner
            .get_goal(id)
            .map_err(to_pyerr)?
            .map(|inner| PyGoal { inner }))
    }

    fn get_goal_status(&self, goal_id: i64) -> PyResult<Option<String>> {
        Ok(self
            .inner
            .get_goal_status(goal_id)
            .map_err(to_pyerr)?
            .map(|s| s.as_str().to_string()))
    }

    fn get_task(&self, id: i64) -> PyResult<Option<PyTask>> {
        Ok(self
            .inner
            .get_task(id)
            .map_err(to_pyerr)?
            .map(|inner| PyTask { inner }))
    }

    fn tasks(&self) -> PyResult<Vec<(i64, PyTask)>> {
        Ok(self
            .inner
            .tasks()
            .map_err(to_pyerr)?
            .into_iter()
            .map(|(id, inner)| (id, PyTask { inner }))
            .collect())
    }

    /// Pending tasks whose every dependency is `done` (a pure DAG walk).
    fn next_unblocked_tasks(&self) -> PyResult<Vec<(i64, PyTask)>> {
        Ok(self
            .inner
            .next_unblocked_tasks()
            .map_err(to_pyerr)?
            .into_iter()
            .map(|(id, inner)| (id, PyTask { inner }))
            .collect())
    }

    fn current_self_model_id(&self) -> PyResult<Option<i64>> {
        self.inner.current_self_model_id().map_err(to_pyerr)
    }

    fn current_self_model(&self) -> PyResult<Option<PySelfModel>> {
        Ok(self
            .inner
            .current_self_model()
            .map_err(to_pyerr)?
            .map(|inner| PySelfModel { inner }))
    }

    /// Top `k` atoms across `kinds` by payload score, as `(id, text, score)`.
    fn top_scored(&self, kinds: Vec<String>, k: usize) -> PyResult<Vec<(i64, String, f64)>> {
        let refs: Vec<&str> = kinds.iter().map(String::as_str).collect();
        self.inner.top_scored(&refs, k).map_err(to_pyerr)
    }

    fn export_verified_artifact(&self, atom: i64) -> PyResult<Option<PyVerifiedExport>> {
        Ok(self
            .inner
            .export_verified_artifact(atom)
            .map_err(to_pyerr)?
            .map(|inner| PyVerifiedExport { inner }))
    }

    /// `(source, content)` evidence for tasks refining `goal_id` (excludes audit atoms).
    fn evidence_for_goal(&self, goal_id: i64) -> PyResult<Vec<(String, String)>> {
        self.inner.evidence_for_goal(goal_id).map_err(to_pyerr)
    }

    /// Semantically recall the `k` most relevant evidence/fact/reflection atoms.
    fn recall_relevant(&self, query: &str, k: usize) -> PyResult<Vec<PyAtomHit>> {
        Ok(self
            .inner
            .recall_relevant(query, k)
            .map_err(to_pyerr)?
            .into_iter()
            .map(PyAtomHit::from_hit)
            .collect())
    }

    /// Recorded LLM calls as `(request_hash, response)` pairs, in id order.
    fn load_llm_traces(&self, py: Python<'_>) -> PyResult<Vec<(String, Py<PyAny>)>> {
        self.inner
            .load_llm_traces()
            .map_err(to_pyerr)?
            .iter()
            .map(|(hash, response)| Ok((hash.clone(), json_to_py(py, response)?)))
            .collect()
    }

    fn llm_model_id(&self) -> PyResult<Option<String>> {
        self.inner.llm_model_id().map_err(to_pyerr)
    }

    // -- audit / verify -----------------------------------------------------

    /// True if `action_atom` is structurally anchored to the immutable `goal_ref`.
    fn has_provenance(&self, action_atom: i64, goal_ref: i64) -> PyResult<bool> {
        self.inner
            .has_provenance(action_atom, goal_ref)
            .map_err(to_pyerr)
    }

    fn verify_self_model_chain(&self) -> PyResult<()> {
        self.inner.verify_self_model_chain().map_err(to_pyerr)
    }

    /// Replay the audit chain, recomputing every hash; breaches are returned as data.
    fn verify_chain(&self) -> PyResult<PyChainReport> {
        Ok(PyChainReport {
            inner: self.inner.verify_chain().map_err(to_pyerr)?,
        })
    }

    fn export_audit_trail(&self) -> PyResult<Vec<PyCoInstantiationCheck>> {
        Ok(self
            .inner
            .export_audit_trail()
            .map_err(to_pyerr)?
            .into_iter()
            .map(|inner| PyCoInstantiationCheck { inner })
            .collect())
    }

    // -- eviction -----------------------------------------------------------

    /// Evict by policy; refuses `purge_region` unless `force=True` (it would
    /// destroy the audit/self-model chain). Returns the number removed.
    #[pyo3(signature = (policy, force=false))]
    fn evict_guarded(&self, policy: &PyEvictionPolicy, force: bool) -> PyResult<u64> {
        Ok(self
            .inner
            .evict_guarded(policy.policy(), force)
            .map_err(to_pyerr)?
            .removed)
    }

    /// Force-delete `llm_trace` atoms by policy (never the audit/self-model).
    fn evict_traces(&self, policy: &PyTraceEvictionPolicy) -> PyResult<u64> {
        Ok(self
            .inner
            .evict_traces(policy.inner)
            .map_err(to_pyerr)?
            .removed)
    }
}
