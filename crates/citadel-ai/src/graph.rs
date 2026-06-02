//! Typed Belief-and-Goal graph + co-instantiation substrate over citadel-mem.
//!
//! The agent's reasoning state lives as memory atoms (goals, tasks, evidence,
//! reflections, a self-model) joined by `memory_edges`; this is the typed view over
//! their JSONB. [`BeliefGraph::next_unblocked_tasks`] walks the `depends_on` DAG
//! (deterministic, no embeddings); the co-instantiation audit chain records each
//! action as an immutable BLAKE3-linked [`CoInstantiationCheck`] that
//! [`BeliefGraph::verify_chain`] replays - sequence EVIDENCE, not tamper-resistance.
//! The self-model + its goal are write-once; evolution is append-only via
//! [`BeliefGraph::supersede_self_model`].

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::{json, Value};

use citadel_mem::{
    AtomHit, AtomId, AtomInput, EdgeKind, EvictionPolicy, EvictionReport, FusionWeights,
    MemoryEngine, RecallQuery,
};

use crate::verify::CheckerAttestation;

/// Upper bound on tasks in one belief graph; crossing it signals a runaway, so we
/// error rather than truncate silently.
const MAX_TASKS: usize = 10_000;
/// Verify-time backstop on audit-chain length (the real bound is the loop's
/// drift-abort + budget caps; this only guards `verify_chain`/`export`).
const MAX_AUDIT_CHECKS: usize = 1_000_000;
/// Self-model version chains are tiny in practice; this is a generous ceiling.
const MAX_SELF_MODEL_VERSIONS: usize = 4_096;
/// Backstop on the discovery archive scan in `top_scored` (the real bound is the
/// discovery budget caps; a run cannot accumulate anywhere near this).
const MAX_DISCOVERY_ATOMS: usize = 1_000_000;
/// Canonical-encoding version stamped into every check (lets the format evolve).
const CHECK_VERSION: u32 = 1;
/// `prev_hash` of the first check in a chain.
const GENESIS_PREV_HASH: &str = "";
/// Atom kind for a discovery candidate; shared by `add_candidate` and the
/// elite-archive reader so the two cannot drift to a typo.
pub(crate) const CANDIDATE_KIND: &str = "candidate";
/// Atom kinds that protect a region's history (audit chain, self-model, verified
/// artifacts): `PurgeRegion` is refused while any exists, unless forced.
const PROTECTED_HISTORY_KINDS: [&str; 4] = [
    "audit",
    "self_model",
    VerifiedKind::Construction.as_str(),
    VerifiedKind::Lemma.as_str(),
];

#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error(transparent)]
    Mem(#[from] citadel_mem::MemError),
    #[error("malformed {kind} payload: {reason}")]
    Payload { kind: &'static str, reason: String },
    #[error("task {0} not found")]
    TaskNotFound(AtomId),
    #[error("belief graph exceeds {max} {kind} atoms")]
    TooLarge { kind: &'static str, max: usize },
    #[error("a self-model already exists; it is write-once (use supersede_self_model)")]
    SelfModelExists,
    #[error("no self-model exists to supersede; call set_self_model first")]
    NoSelfModel,
    #[error("self-model {0} not found")]
    SelfModelNotFound(AtomId),
    #[error("goal {0} not found")]
    GoalNotFound(AtomId),
    #[error("goal {0} is mutable; co-instantiation anchors must be immutable")]
    GoalMutable(AtomId),
    #[error("self-model {0} is mutable; co-instantiation anchors must be immutable")]
    SelfModelMutable(AtomId),
    #[error("self-model version chain is not single-headed (corruption)")]
    SelfModelBranch,
    #[error("self-model {0} is already superseded; cannot branch")]
    SupersededBranch(AtomId),
    #[error(
        "eviction refused: PurgeRegion would destroy the audit/self-model chain (pass force=true)"
    )]
    EvictionRefused,
    #[error("no llm_trace atoms recorded to replay")]
    NoTraces,
    #[error("candidate atom {0} not found")]
    CandidateNotFound(AtomId),
}

/// A checker-verified artifact kind. Maps to the immutable atom kind that a
/// deterministic checker may mint via [`BeliefGraph::add_verified_artifact`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifiedKind {
    Construction,
    Lemma,
}

impl VerifiedKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            VerifiedKind::Construction => "verified_construction",
            VerifiedKind::Lemma => "verified_lemma",
        }
    }
}

/// A checker-verified artifact exported for INDEPENDENT third-party re-checking
/// ([`BeliefGraph::export_verified_artifact`]): a third party re-runs the named
/// checker on `artifact` and confirms `score`. Provenance proves lineage, never correctness.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedExport {
    pub atom: AtomId,
    pub kind: String,
    /// The raw artifact text, fed verbatim back into the named checker.
    pub artifact: String,
    pub score: f64,
    pub checker_id: String,
    pub checker_version: String,
    pub checked_at_micros: i64,
}

pub type GraphResult<T> = Result<T, GraphError>;

/// How [`BeliefGraph::evict_traces`] selects `llm_trace` atoms to forget. Traces
/// are immutable, so this is a deliberate force-delete (never the audit/self-model).
#[derive(Debug, Clone, Copy)]
pub enum TraceEvictionPolicy {
    /// Keep the `n` newest traces; delete the rest.
    KeepLastN { n: usize },
    /// Delete traces recorded before `now - older_than_micros`.
    ByAge { older_than_micros: i64 },
    /// Keep newest traces within cumulative `max_cost_usd`; delete the boundary
    /// trace that would exceed it and everything older.
    ByTotalCost { max_cost_usd: f64 },
}

/// Lifecycle of a goal: being pursued, met its acceptance criteria, or dropped.
/// Tracked OUT of the immutable goal, as a mutable `goal_status` record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalStatus {
    Active,
    Achieved,
    Abandoned,
}

impl GoalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            GoalStatus::Active => "active",
            GoalStatus::Achieved => "achieved",
            GoalStatus::Abandoned => "abandoned",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "active" => GoalStatus::Active,
            "achieved" => GoalStatus::Achieved,
            "abandoned" => GoalStatus::Abandoned,
            _ => return None,
        })
    }
}

/// Execution state of a task in the plan DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    InProgress,
    Done,
    Failed,
}

impl TaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::InProgress => "in_progress",
            TaskStatus::Done => "done",
            TaskStatus::Failed => "failed",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => TaskStatus::Pending,
            "in_progress" => TaskStatus::InProgress,
            "done" => TaskStatus::Done,
            "failed" => TaskStatus::Failed,
            _ => return None,
        })
    }
}

/// Verdict of one co-instantiation check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Violation,
    Drift,
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Pass => "pass",
            Verdict::Violation => "violation",
            Verdict::Drift => "drift",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pass" => Verdict::Pass,
            "violation" => Verdict::Violation,
            "drift" => Verdict::Drift,
            _ => return None,
        })
    }

    /// Stable byte tag for the canonical hash body.
    fn code(self) -> u8 {
        match self {
            Verdict::Pass => 0,
            Verdict::Violation => 1,
            Verdict::Drift => 2,
        }
    }

    /// Decide a verdict: failed provenance/constraint = `Violation`; else drift over
    /// its bound = `Drift`; else `Pass`.
    pub fn evaluate(
        has_provenance: bool,
        constraints_satisfied: bool,
        drift_count: u32,
        drift_bound: u32,
    ) -> Self {
        if !has_provenance || !constraints_satisfied {
            Verdict::Violation
        } else if drift_count > drift_bound {
            Verdict::Drift
        } else {
            Verdict::Pass
        }
    }
}

/// A crystallized goal: what "done" means plus the bounds to respect. Write-once
/// (the co-instantiation charter); lifecycle is a separate [`GoalStatusRecord`].
#[derive(Debug, Clone, PartialEq)]
pub struct Goal {
    pub prompt: String,
    pub acceptance_criteria: Vec<String>,
    pub constraints: Vec<String>,
}

impl Goal {
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            acceptance_criteria: Vec::new(),
            constraints: Vec::new(),
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "prompt": self.prompt,
            "acceptance_criteria": self.acceptance_criteria,
            "constraints": self.constraints,
        })
    }

    fn from_json(v: &Value) -> GraphResult<Self> {
        Ok(Self {
            prompt: req_str(v, "prompt", "goal")?,
            acceptance_criteria: str_vec(v, "acceptance_criteria"),
            constraints: str_vec(v, "constraints"),
        })
    }
}

/// Mutable lifecycle record for a goal (one per goal, last-write-wins). Kept
/// separate from the immutable [`Goal`] so achievement can be recorded at Converge.
#[derive(Debug, Clone, PartialEq)]
pub struct GoalStatusRecord {
    pub goal_ref: AtomId,
    pub status: GoalStatus,
    pub timestamp_micros: i64,
}

impl GoalStatusRecord {
    fn to_json(&self) -> Value {
        json!({
            "goal_ref": self.goal_ref,
            "status": self.status.as_str(),
            "timestamp_micros": self.timestamp_micros,
        })
    }

    fn from_json(v: &Value) -> GraphResult<Self> {
        Ok(Self {
            goal_ref: req_i64(v, "goal_ref", "goal_status")?,
            status: enum_field(v, "status", "goal_status", GoalStatus::parse)?,
            timestamp_micros: v
                .get("timestamp_micros")
                .and_then(Value::as_i64)
                .unwrap_or(0),
        })
    }
}

/// A subtask in the plan DAG. `attempts`/`last_error` track failure recovery.
#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    pub description: String,
    pub status: TaskStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
}

impl Task {
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
            status: TaskStatus::Pending,
            attempts: 0,
            last_error: None,
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "description": self.description,
            "status": self.status.as_str(),
            "attempts": self.attempts,
            "last_error": self.last_error,
        })
    }

    fn from_json(v: &Value) -> GraphResult<Self> {
        Ok(Self {
            description: req_str(v, "description", "task")?,
            status: enum_field(v, "status", "task", TaskStatus::parse)?,
            attempts: v.get("attempts").and_then(Value::as_u64).unwrap_or(0) as u32,
            last_error: opt_str(v, "last_error"),
        })
    }
}

/// A candidate approach, not yet committed. `refines` a goal.
#[derive(Debug, Clone, PartialEq)]
pub struct Hypothesis {
    pub summary: String,
    pub confidence: f32,
}

impl Hypothesis {
    fn to_json(&self) -> Value {
        json!({ "summary": self.summary, "confidence": self.confidence })
    }
}

/// An observation or tool result, linked to what it was `derived_from`.
#[derive(Debug, Clone, PartialEq)]
pub struct Evidence {
    pub source: String,
    pub content: String,
}

impl Evidence {
    fn to_json(&self) -> Value {
        json!({ "source": self.source, "content": self.content })
    }
}

/// A critique/reflection note, linked to what it was `derived_from`.
#[derive(Debug, Clone, PartialEq)]
pub struct Reflection {
    pub insight: String,
    pub confidence: f32,
}

impl Reflection {
    fn to_json(&self) -> Value {
        json!({ "insight": self.insight, "confidence": self.confidence })
    }
}

/// The agent's functional self-model: who it is and the goal it serves. Write-once;
/// change is a new version via [`BeliefGraph::supersede_self_model`].
#[derive(Debug, Clone, PartialEq)]
pub struct SelfModel {
    pub identity: String,
    pub goal_ref: Option<AtomId>,
}

impl SelfModel {
    pub fn new(identity: impl Into<String>) -> Self {
        Self {
            identity: identity.into(),
            goal_ref: None,
        }
    }

    fn to_json(&self) -> Value {
        json!({ "identity": self.identity, "goal_ref": self.goal_ref })
    }

    fn from_json(v: &Value) -> GraphResult<Self> {
        Ok(Self {
            identity: req_str(v, "identity", "self_model")?,
            goal_ref: v.get("goal_ref").and_then(Value::as_i64),
        })
    }
}

/// One co-instantiation check: an action's structural provenance to the goal and
/// its constraint compliance, recorded as an immutable, hash-linked audit atom.
#[derive(Debug, Clone, PartialEq)]
pub struct CoInstantiationCheck {
    pub version: u32,
    pub action_id: String,
    pub goal_ref: AtomId,
    pub self_model_ref: AtomId,
    pub has_provenance: bool,
    pub constraints_satisfied: bool,
    pub verdict: Verdict,
    pub drift_count: u32,
    pub drift_bound: u32,
    pub timestamp_micros: i64,
    pub prev_hash: String,
    pub this_hash: String,
}

impl CoInstantiationCheck {
    /// Build a check from the loop's evaluation. `version`/`verdict` are derived;
    /// `timestamp_micros`/`prev_hash`/`this_hash` are stamped by `record_check`.
    pub fn new(
        action_id: impl Into<String>,
        goal_ref: AtomId,
        self_model_ref: AtomId,
        has_provenance: bool,
        constraints_satisfied: bool,
        drift_count: u32,
        drift_bound: u32,
    ) -> Self {
        Self {
            version: CHECK_VERSION,
            action_id: action_id.into(),
            goal_ref,
            self_model_ref,
            has_provenance,
            constraints_satisfied,
            verdict: Verdict::evaluate(
                has_provenance,
                constraints_satisfied,
                drift_count,
                drift_bound,
            ),
            drift_count,
            drift_bound,
            timestamp_micros: 0,
            prev_hash: String::new(),
            this_hash: String::new(),
        }
    }

    /// Length-prefixed binary encoding, fixed field order, INCLUDING `prev_hash`
    /// (this is what links the chain) and EXCLUDING `this_hash`. The field order
    /// is a stability contract: changing it invalidates replay of older chains -
    /// bump `CHECK_VERSION` if it must change.
    fn canonical_body(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&self.version.to_le_bytes());
        push_bytes(&mut b, self.action_id.as_bytes());
        b.extend_from_slice(&self.goal_ref.to_le_bytes());
        b.extend_from_slice(&self.self_model_ref.to_le_bytes());
        b.push(self.has_provenance as u8);
        b.push(self.constraints_satisfied as u8);
        b.push(self.verdict.code());
        b.extend_from_slice(&self.drift_count.to_le_bytes());
        b.extend_from_slice(&self.drift_bound.to_le_bytes());
        b.extend_from_slice(&self.timestamp_micros.to_le_bytes());
        push_bytes(&mut b, self.prev_hash.as_bytes());
        b
    }

    fn compute_hash(&self) -> String {
        blake3::hash(&self.canonical_body()).to_hex().to_string()
    }

    fn to_json(&self) -> Value {
        json!({
            "version": self.version,
            "action_id": self.action_id,
            "goal_ref": self.goal_ref,
            "self_model_ref": self.self_model_ref,
            "has_provenance": self.has_provenance,
            "constraints_satisfied": self.constraints_satisfied,
            "verdict": self.verdict.as_str(),
            "drift_count": self.drift_count,
            "drift_bound": self.drift_bound,
            "timestamp_micros": self.timestamp_micros,
            "prev_hash": self.prev_hash,
            "this_hash": self.this_hash,
        })
    }

    fn from_json(v: &Value) -> GraphResult<Self> {
        Ok(Self {
            version: v.get("version").and_then(Value::as_u64).unwrap_or(0) as u32,
            action_id: req_str(v, "action_id", "audit")?,
            goal_ref: req_i64(v, "goal_ref", "audit")?,
            self_model_ref: req_i64(v, "self_model_ref", "audit")?,
            has_provenance: v
                .get("has_provenance")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            constraints_satisfied: v
                .get("constraints_satisfied")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            verdict: enum_field(v, "verdict", "audit", Verdict::parse)?,
            drift_count: v.get("drift_count").and_then(Value::as_u64).unwrap_or(0) as u32,
            drift_bound: v.get("drift_bound").and_then(Value::as_u64).unwrap_or(0) as u32,
            timestamp_micros: v
                .get("timestamp_micros")
                .and_then(Value::as_i64)
                .unwrap_or(0),
            prev_hash: opt_str(v, "prev_hash").unwrap_or_default(),
            this_hash: opt_str(v, "this_hash").unwrap_or_default(),
        })
    }
}

/// Result of replaying the audit chain. `breaches` are returned as data (not
/// errors) so verification is total even on a tampered chain.
#[derive(Debug, Clone, PartialEq)]
pub struct ChainReport {
    pub valid: bool,
    pub total_checks: usize,
    pub breaches: Vec<(AtomId, String)>,
}

/// Typed Belief-and-Goal graph over a single memory region. The region must
/// already exist (the caller attaches its embedder via `create_region`).
pub struct BeliefGraph {
    mem: Arc<MemoryEngine>,
    region: String,
    /// (id, this_hash) of the last audit atom; lazily seeded, then kept O(1).
    audit_tail: Mutex<Option<(AtomId, String)>>,
    /// Current (head) self-model version id; lazily seeded.
    self_model_head: Mutex<Option<AtomId>>,
}

impl BeliefGraph {
    pub fn new(mem: Arc<MemoryEngine>, region: impl Into<String>) -> Self {
        Self {
            mem,
            region: region.into(),
            audit_tail: Mutex::new(None),
            self_model_head: Mutex::new(None),
        }
    }

    // --- goals, tasks, hypotheses, evidence, reflections ---

    /// Store an immutable goal atom; returns its id.
    pub fn add_goal(&self, goal: &Goal) -> GraphResult<AtomId> {
        let id = self.mem.remember(
            &self.region,
            AtomInput::new("goal", &goal.prompt)
                .with_payload(goal.to_json())
                .immutable(),
        )?;
        Ok(id)
    }

    /// Store a task atom with a `depends_on` edge to each predecessor and a
    /// `refines` edge to the goal (the provenance link; invisible to
    /// `next_unblocked_tasks`, which follows `depends_on` only).
    pub fn add_task(&self, task: &Task, deps: &[AtomId], goal_id: AtomId) -> GraphResult<AtomId> {
        let id = self.mem.remember(
            &self.region,
            AtomInput::new("task", &task.description).with_payload(task.to_json()),
        )?;
        for &dep in deps {
            self.mem.link(id, dep, EdgeKind::DependsOn, 1.0)?;
        }
        self.mem.link(id, goal_id, EdgeKind::Refines, 1.0)?;
        Ok(id)
    }

    /// Store a hypothesis and a `refines` edge to the goal it addresses.
    pub fn add_hypothesis(&self, hyp: &Hypothesis, refines_goal: AtomId) -> GraphResult<AtomId> {
        let id = self.mem.remember(
            &self.region,
            AtomInput::new("hypothesis", &hyp.summary).with_payload(hyp.to_json()),
        )?;
        self.mem.link(id, refines_goal, EdgeKind::Refines, 1.0)?;
        Ok(id)
    }

    /// Store evidence and a `derived_from` edge to the atom it supports.
    pub fn add_evidence(&self, ev: &Evidence, supports: AtomId) -> GraphResult<AtomId> {
        let id = self.mem.remember(
            &self.region,
            AtomInput::new("evidence", &ev.content).with_payload(ev.to_json()),
        )?;
        self.mem.link(id, supports, EdgeKind::DerivedFrom, 1.0)?;
        Ok(id)
    }

    /// Store a reflection and a `derived_from` edge to the atom it is about.
    pub fn add_reflection(&self, refl: &Reflection, about: AtomId) -> GraphResult<AtomId> {
        let id = self.mem.remember(
            &self.region,
            AtomInput::new("reflection", &refl.insight).with_payload(refl.to_json()),
        )?;
        self.mem.link(id, about, EdgeKind::DerivedFrom, 1.0)?;
        Ok(id)
    }

    // --- discovery: candidates + checker-verified artifacts ---

    /// Store a discovery candidate: a MUTABLE `candidate` atom with the raw artifact
    /// and a provisional score. Only a checker-stamped `verified_*` atom is durable
    /// proof (see [`BeliefGraph::add_verified_artifact`]).
    pub fn add_candidate(&self, artifact: &str, score: f64) -> GraphResult<AtomId> {
        let id = self.mem.remember(
            &self.region,
            AtomInput::new(CANDIDATE_KIND, artifact).with_payload(json!({ "score": score })),
        )?;
        Ok(id)
    }

    /// Mint a checker-stamped, IMMUTABLE verified artifact from a candidate. The
    /// discovery write-path GATE: it requires a [`CheckerAttestation`], which only a
    /// deterministic [`Verifier`] produces (a critic returns `None`), so a critic
    /// cannot mint. The atom carries the raw candidate text, score, checker, and a
    /// `derived_from` edge to the candidate.
    ///
    /// [`Verifier`]: crate::verify::Verifier
    pub fn add_verified_artifact(
        &self,
        candidate_atom: AtomId,
        kind: VerifiedKind,
        attestation: CheckerAttestation,
        score: f64,
    ) -> GraphResult<AtomId> {
        let candidate = self
            .mem
            .fetch_one(&self.region, candidate_atom)?
            .ok_or(GraphError::CandidateNotFound(candidate_atom))?;
        let payload = json!({
            "score": score,
            "checker_id": attestation.checker_id,
            "checker_version": attestation.checker_version,
            "checked_at_micros": now_micros(),
            "candidate": candidate_atom,
        });
        let id = self.mem.remember(
            &self.region,
            AtomInput::new(kind.as_str(), &candidate.text)
                .with_payload(payload)
                .with_confidence(1.0)
                .immutable(),
        )?;
        self.mem
            .link(id, candidate_atom, EdgeKind::DerivedFrom, 1.0)?;
        Ok(id)
    }

    /// Top-`k` atoms across `kinds` by payload `score` desc (ties keep insertion
    /// order). Seeds discovery rounds from the elite frontier. Returns `(id, text, score)`.
    ///
    /// REPLAY CONTRACT: the tie-break MUST stay a STABLE sort, so a run and its
    /// replay build byte-identical proposer requests (zero replay misses).
    pub fn top_scored(&self, kinds: &[&str], k: usize) -> GraphResult<Vec<(AtomId, String, f64)>> {
        let mut out: Vec<(AtomId, String, f64)> = Vec::new();
        for kind in kinds {
            for h in self
                .mem
                .fetch(&self.region, kind, None, MAX_DISCOVERY_ATOMS)?
            {
                let score = h
                    .payload
                    .get("score")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                out.push((h.id, h.text, score));
            }
        }
        out.sort_by(|a, b| b.2.total_cmp(&a.2));
        out.truncate(k);
        Ok(out)
    }

    /// Read a verified artifact by id for INDEPENDENT re-checking (`None` if absent
    /// or not `verified_*`): artifact + checker id/version to re-run outside.
    pub fn export_verified_artifact(&self, atom: AtomId) -> GraphResult<Option<VerifiedExport>> {
        let Some(hit) = self.mem.fetch_one(&self.region, atom)? else {
            return Ok(None);
        };
        if hit.kind != VerifiedKind::Construction.as_str()
            && hit.kind != VerifiedKind::Lemma.as_str()
        {
            return Ok(None);
        }
        let str_field = |key: &str| {
            hit.payload
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        Ok(Some(VerifiedExport {
            atom: hit.id,
            kind: hit.kind.clone(),
            artifact: hit.text.clone(),
            score: hit
                .payload
                .get("score")
                .and_then(Value::as_f64)
                .unwrap_or(0.0),
            checker_id: str_field("checker_id"),
            checker_version: str_field("checker_version"),
            checked_at_micros: hit
                .payload
                .get("checked_at_micros")
                .and_then(Value::as_i64)
                .unwrap_or(0),
        }))
    }

    /// Read a goal by id, or `None` if absent.
    pub fn get_goal(&self, id: AtomId) -> GraphResult<Option<Goal>> {
        match self.mem.fetch_one(&self.region, id)? {
            Some(hit) => Ok(Some(Goal::from_json(&hit.payload)?)),
            None => Ok(None),
        }
    }

    /// Upsert the lifecycle status of a goal (mutable; one record per goal).
    pub fn set_goal_status(&self, goal_id: AtomId, status: GoalStatus) -> GraphResult<()> {
        let rec = GoalStatusRecord {
            goal_ref: goal_id,
            status,
            timestamp_micros: now_micros(),
        };
        let filter = json!({ "goal_ref": goal_id });
        match self
            .mem
            .fetch(&self.region, "goal_status", Some(&filter), 1)?
            .first()
        {
            Some(hit) => {
                self.mem
                    .update_atom_payload(&self.region, hit.id, &rec.to_json())?;
            }
            None => {
                let id = self.mem.remember(
                    &self.region,
                    AtomInput::new("goal_status", status.as_str()).with_payload(rec.to_json()),
                )?;
                self.mem.link(id, goal_id, EdgeKind::DerivedFrom, 1.0)?;
            }
        }
        Ok(())
    }

    /// Current lifecycle status of a goal, or `None` if never set.
    pub fn get_goal_status(&self, goal_id: AtomId) -> GraphResult<Option<GoalStatus>> {
        let filter = json!({ "goal_ref": goal_id });
        match self
            .mem
            .fetch(&self.region, "goal_status", Some(&filter), 1)?
            .first()
        {
            Some(hit) => Ok(Some(GoalStatusRecord::from_json(&hit.payload)?.status)),
            None => Ok(None),
        }
    }

    /// Read a task by id, or `None` if absent.
    pub fn get_task(&self, id: AtomId) -> GraphResult<Option<Task>> {
        match self.mem.fetch_one(&self.region, id)? {
            Some(hit) => Ok(Some(Task::from_json(&hit.payload)?)),
            None => Ok(None),
        }
    }

    /// Overwrite a task's payload (full read-modify-write happens in the caller).
    pub fn update_task(&self, id: AtomId, task: &Task) -> GraphResult<()> {
        self.mem
            .update_atom_payload(&self.region, id, &task.to_json())?;
        Ok(())
    }

    /// Transition a task to `status`, preserving its other fields.
    pub fn set_task_status(&self, id: AtomId, status: TaskStatus) -> GraphResult<()> {
        let mut task = self.get_task(id)?.ok_or(GraphError::TaskNotFound(id))?;
        task.status = status;
        self.update_task(id, &task)
    }

    /// Mark a task failed: bump `attempts`, record `error`, set status `Failed`.
    pub fn record_task_failure(&self, id: AtomId, error: impl Into<String>) -> GraphResult<()> {
        let mut task = self.get_task(id)?.ok_or(GraphError::TaskNotFound(id))?;
        task.status = TaskStatus::Failed;
        task.attempts = task.attempts.saturating_add(1);
        task.last_error = Some(error.into());
        self.update_task(id, &task)
    }

    /// Every `(id, Task)` in the region, ordered by id. Errors if the graph is
    /// implausibly large (see [`MAX_TASKS`]) rather than truncating.
    pub fn tasks(&self) -> GraphResult<Vec<(AtomId, Task)>> {
        let hits = self.mem.fetch(&self.region, "task", None, MAX_TASKS + 1)?;
        if hits.len() > MAX_TASKS {
            return Err(GraphError::TooLarge {
                kind: "task",
                max: MAX_TASKS,
            });
        }
        hits.iter()
            .map(|h| Ok((h.id, Task::from_json(&h.payload)?)))
            .collect()
    }

    /// Pending tasks whose every `depends_on` predecessor is `Done` (no deps = ready;
    /// a missing dep leaves the dependent blocked). Purely structural, no embeddings.
    pub fn next_unblocked_tasks(&self) -> GraphResult<Vec<(AtomId, Task)>> {
        let all = self.tasks()?;
        let status: FxHashMap<AtomId, TaskStatus> =
            all.iter().map(|(id, t)| (*id, t.status)).collect();

        let mut ready = Vec::new();
        for (id, task) in &all {
            if task.status != TaskStatus::Pending {
                continue;
            }
            let deps = self
                .mem
                .fetch_edges(Some(*id), None, Some(EdgeKind::DependsOn))?;
            let unblocked = deps
                .iter()
                .all(|e| status.get(&e.dst_id).copied() == Some(TaskStatus::Done));
            if unblocked {
                ready.push((*id, task.clone()));
            }
        }
        Ok(ready)
    }

    // --- self-model (write-once + supersession) ---

    /// Set the region's initial self-model (write-once). Errors with
    /// [`GraphError::SelfModelExists`] if present - evolve via `supersede_self_model`.
    pub fn set_self_model(&self, sm: &SelfModel) -> GraphResult<AtomId> {
        if self.mem.fetch_last(&self.region, "self_model")?.is_some() {
            return Err(GraphError::SelfModelExists);
        }
        let id = self.mem.remember(
            &self.region,
            AtomInput::new("self_model", &sm.identity)
                .with_payload(sm.to_json())
                .immutable(),
        )?;
        *self.self_model_head.lock().unwrap() = Some(id);
        Ok(id)
    }

    /// Evolve the charter: append a new immutable self-model version that
    /// `Supersedes` the current head. The old version stays immutable for audit.
    pub fn supersede_self_model(&self, new_sm: &SelfModel) -> GraphResult<AtomId> {
        let head = self
            .current_self_model_id()?
            .ok_or(GraphError::NoSelfModel)?;
        // Defence in depth: a correctly-resolved head has no incoming Supersedes.
        if !self
            .mem
            .fetch_edges(None, Some(head), Some(EdgeKind::Supersedes))?
            .is_empty()
        {
            return Err(GraphError::SupersededBranch(head));
        }
        let new_id = self.mem.remember(
            &self.region,
            AtomInput::new("self_model", &new_sm.identity)
                .with_payload(new_sm.to_json())
                .immutable(),
        )?;
        self.mem.link(new_id, head, EdgeKind::Supersedes, 1.0)?;
        *self.self_model_head.lock().unwrap() = Some(new_id);
        Ok(new_id)
    }

    /// The id of the current (head) self-model version, or `None` if unset.
    pub fn current_self_model_id(&self) -> GraphResult<Option<AtomId>> {
        if let Some(id) = *self.self_model_head.lock().unwrap() {
            return Ok(Some(id));
        }
        let head = self.seed_self_model_head()?;
        if let Some(id) = head {
            *self.self_model_head.lock().unwrap() = Some(id);
        }
        Ok(head)
    }

    /// The current (head) self-model, or `None` if unset.
    pub fn current_self_model(&self) -> GraphResult<Option<SelfModel>> {
        match self.current_self_model_id()? {
            Some(id) => match self.mem.fetch_one(&self.region, id)? {
                Some(hit) => Ok(Some(SelfModel::from_json(&hit.payload)?)),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    /// Assert the self-model version chain is single-headed (acyclicity is already
    /// guaranteed by `link` on the acyclic `Supersedes` kind).
    pub fn verify_self_model_chain(&self) -> GraphResult<()> {
        self.seed_self_model_head()?;
        Ok(())
    }

    /// Resolve the head from storage: the lone self-model not superseded by any
    /// `Supersedes` edge. Errors [`GraphError::SelfModelBranch`] if not exactly one.
    fn seed_self_model_head(&self) -> GraphResult<Option<AtomId>> {
        let atoms = self
            .mem
            .fetch(&self.region, "self_model", None, MAX_SELF_MODEL_VERSIONS)?;
        if atoms.is_empty() {
            return Ok(None);
        }
        let superseded: FxHashSet<AtomId> = self
            .mem
            .fetch_edges(None, None, Some(EdgeKind::Supersedes))?
            .iter()
            .map(|e| e.dst_id)
            .collect();
        let heads: Vec<AtomId> = atoms
            .iter()
            .map(|a| a.id)
            .filter(|id| !superseded.contains(id))
            .collect();
        match heads.as_slice() {
            [h] => Ok(Some(*h)),
            _ => Err(GraphError::SelfModelBranch),
        }
    }

    // --- co-instantiation audit chain ---

    /// Record a co-instantiation check as an immutable, hash-linked audit atom.
    /// Gates on present+immutable goal/self-model anchors, stamps the chain hashes,
    /// links DerivedFrom(action)+Refines(goal), advances the tail. O(1) amortized.
    pub fn record_check(
        &self,
        mut check: CoInstantiationCheck,
        action_atom: AtomId,
    ) -> GraphResult<AtomId> {
        let goal_hit = self
            .mem
            .fetch_one(&self.region, check.goal_ref)?
            .ok_or(GraphError::GoalNotFound(check.goal_ref))?;
        if !goal_hit.immutable {
            return Err(GraphError::GoalMutable(check.goal_ref));
        }
        let sm_hit = self
            .mem
            .fetch_one(&self.region, check.self_model_ref)?
            .ok_or(GraphError::SelfModelNotFound(check.self_model_ref))?;
        if !sm_hit.immutable {
            return Err(GraphError::SelfModelMutable(check.self_model_ref));
        }

        // Hold the tail lock across read -> insert -> update so the chain can't
        // fork (single-writer-per-region is the invariant; see module docs).
        let mut tail = self.audit_tail.lock().unwrap();
        if tail.is_none() {
            if let Some(last) = self.mem.fetch_last(&self.region, "audit")? {
                let prev = CoInstantiationCheck::from_json(&last.payload)?;
                *tail = Some((last.id, prev.this_hash));
            }
        }
        let prev_hash = tail
            .as_ref()
            .map(|(_, h)| h.clone())
            .unwrap_or_else(|| GENESIS_PREV_HASH.to_string());

        check.version = CHECK_VERSION;
        check.timestamp_micros = now_micros();
        check.prev_hash = prev_hash;
        check.this_hash = check.compute_hash();

        let confidence = if check.verdict == Verdict::Pass {
            1.0
        } else {
            0.0
        };
        let id = self.mem.remember(
            &self.region,
            AtomInput::new("audit", &check.action_id)
                .with_payload(check.to_json())
                .with_confidence(confidence)
                .immutable(),
        )?;
        self.mem.link(id, action_atom, EdgeKind::DerivedFrom, 1.0)?;
        self.mem.link(id, check.goal_ref, EdgeKind::Refines, 1.0)?;
        *tail = Some((id, check.this_hash.clone()));
        Ok(id)
    }

    /// Replay the audit chain: recompute every hash + linkage and assert each audit
    /// atom and its anchors are still immutable. Breaches are returned as data (never
    /// errors on tamper). Any immutable self-model is a valid anchor (not just the head).
    pub fn verify_chain(&self) -> GraphResult<ChainReport> {
        let hits = self
            .mem
            .fetch(&self.region, "audit", None, MAX_AUDIT_CHECKS + 1)?;
        if hits.len() > MAX_AUDIT_CHECKS {
            return Err(GraphError::TooLarge {
                kind: "audit",
                max: MAX_AUDIT_CHECKS,
            });
        }

        let mut ids = Vec::with_capacity(hits.len());
        let mut immut = Vec::with_capacity(hits.len());
        let mut checks = Vec::with_capacity(hits.len());
        for h in &hits {
            ids.push(h.id);
            immut.push(h.immutable);
            checks.push(CoInstantiationCheck::from_json(&h.payload)?);
        }

        // One fetch per distinct anchor (kills the per-check N+1).
        let mut anchor_immut: FxHashMap<AtomId, bool> = FxHashMap::default();
        for c in &checks {
            for aid in [c.goal_ref, c.self_model_ref] {
                if let std::collections::hash_map::Entry::Vacant(e) = anchor_immut.entry(aid) {
                    let immutable = self
                        .mem
                        .fetch_one(&self.region, aid)?
                        .map(|h| h.immutable)
                        .unwrap_or(false);
                    e.insert(immutable);
                }
            }
        }

        let mut breaches = Vec::new();
        for (i, c) in checks.iter().enumerate() {
            if !immut[i] {
                breaches.push((ids[i], "audit atom is not immutable".to_string()));
            }
            check_anchor(&mut breaches, ids[i], "goal", c.goal_ref, &anchor_immut);
            check_anchor(
                &mut breaches,
                ids[i],
                "self-model",
                c.self_model_ref,
                &anchor_immut,
            );
        }
        for (i, reason) in verify_link_chain(&checks) {
            breaches.push((ids[i], reason));
        }

        Ok(ChainReport {
            valid: breaches.is_empty(),
            total_checks: checks.len(),
            breaches,
        })
    }

    /// All recorded checks in id order (advisory backup, e.g. before a forced purge).
    pub fn export_audit_trail(&self) -> GraphResult<Vec<CoInstantiationCheck>> {
        let hits = self
            .mem
            .fetch(&self.region, "audit", None, MAX_AUDIT_CHECKS + 1)?;
        if hits.len() > MAX_AUDIT_CHECKS {
            return Err(GraphError::TooLarge {
                kind: "audit",
                max: MAX_AUDIT_CHECKS,
            });
        }
        hits.iter()
            .map(|h| CoInstantiationCheck::from_json(&h.payload))
            .collect()
    }

    /// All recorded LLM calls as `(request_hash, response)` pairs in id order - the
    /// basis for deterministic replay (hash = atom text, response = payload field).
    pub fn load_llm_traces(&self) -> GraphResult<Vec<(String, Value)>> {
        let hits = self
            .mem
            .fetch(&self.region, "llm_trace", None, MAX_AUDIT_CHECKS + 1)?;
        if hits.len() > MAX_AUDIT_CHECKS {
            return Err(GraphError::TooLarge {
                kind: "llm_trace",
                max: MAX_AUDIT_CHECKS,
            });
        }
        Ok(hits
            .iter()
            .filter_map(|h| {
                h.payload
                    .get("response")
                    .map(|r| (h.text.clone(), r.clone()))
            })
            .collect())
    }

    /// Whether `action_atom` is structurally anchored to `goal_ref`: a direct
    /// `refines` edge AND the goal still immutable. O(1). Proves anchoring to the
    /// charter, NOT that the action advances the goal (undecidable).
    pub fn has_provenance(&self, action_atom: AtomId, goal_ref: AtomId) -> GraphResult<bool> {
        let edges =
            self.mem
                .fetch_edges(Some(action_atom), Some(goal_ref), Some(EdgeKind::Refines))?;
        if edges.is_empty() {
            return Ok(false);
        }
        Ok(self
            .mem
            .fetch_one(&self.region, goal_ref)?
            .map(|h| h.immutable)
            .unwrap_or(false))
    }

    /// Record an LLM call as an immutable `llm_trace` atom (text = `request_hash`,
    /// payload = `{model_id, response, recorded_at_micros, cost_usd}` + optional
    /// `prompt` provenance). The basis for replay; the loop routes every call here.
    pub fn record_llm_call(
        &self,
        request_hash: &str,
        model_id: &str,
        response: &Value,
        cost_usd: Option<f64>,
        prompt: Option<&Value>,
    ) -> GraphResult<AtomId> {
        let mut payload = json!({
            "model_id": model_id,
            "response": response,
            "recorded_at_micros": now_micros(),
            "cost_usd": cost_usd,
        });
        // Prompt provenance (node/version/hash/source) for human-auditable
        // attribution; omitted when the caller has no prompt context.
        if let Some(p) = prompt {
            payload["prompt"] = p.clone();
        }
        let id = self.mem.remember(
            &self.region,
            AtomInput::new("llm_trace", request_hash)
                .with_payload(payload)
                .immutable(),
        )?;
        Ok(id)
    }

    /// Gather `(source, content)` evidence for the tasks that `refine` `goal_id`.
    /// The `kind == "evidence"` filter is required: audit atoms also `derived_from`
    /// the task and must be excluded.
    pub fn evidence_for_goal(&self, goal_id: AtomId) -> GraphResult<Vec<(String, String)>> {
        let mut out = Vec::new();
        for (task_id, _task) in self.tasks()? {
            let refines =
                self.mem
                    .fetch_edges(Some(task_id), Some(goal_id), Some(EdgeKind::Refines))?;
            if refines.is_empty() {
                continue;
            }
            for edge in self
                .mem
                .fetch_edges(None, Some(task_id), Some(EdgeKind::DerivedFrom))?
            {
                if let Some(hit) = self.mem.fetch_one(&self.region, edge.src_id)? {
                    if hit.kind == "evidence" {
                        let source = hit
                            .payload
                            .get("source")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        out.push((source, hit.text));
                    }
                }
            }
        }
        Ok(out)
    }

    /// Semantically recall the `k` most relevant atoms, restricted to narrative kinds
    /// (evidence/fact/reflection) so trace/audit never leak into a prompt. Recency
    /// weight is 0, so results depend only on (query, region state) - replay-stable.
    pub fn recall_relevant(&self, query: &str, k: usize) -> GraphResult<Vec<AtomHit>> {
        let weights = FusionWeights {
            recency: 0.0,
            ..FusionWeights::default()
        };
        let q = RecallQuery::by_text(query, k)
            .with_kinds(vec![
                "evidence".to_string(),
                "fact".to_string(),
                "reflection".to_string(),
            ])
            .with_weights(weights);
        Ok(self.mem.recall(&self.region, q)?)
    }

    /// Evict, refusing a `PurgeRegion` that would destroy protected history unless
    /// `force` (irreversible, for key rotation - export first).
    pub fn evict_guarded(
        &self,
        policy: EvictionPolicy,
        force: bool,
    ) -> GraphResult<EvictionReport> {
        if matches!(policy, EvictionPolicy::PurgeRegion) && !force {
            for kind in PROTECTED_HISTORY_KINDS {
                if self.mem.fetch_last(&self.region, kind)?.is_some() {
                    return Err(GraphError::EvictionRefused);
                }
            }
        }
        let report = self.mem.evict(&self.region, policy)?;
        Ok(report)
    }

    /// The model id on the newest `llm_trace`, or `None` if no trace exists.
    /// Lets a replay reuse the original model id without a magic string.
    pub fn llm_model_id(&self) -> GraphResult<Option<String>> {
        Ok(self
            .mem
            .fetch_last(&self.region, "llm_trace")?
            .and_then(|h| {
                h.payload
                    .get("model_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            }))
    }

    /// Evict `llm_trace` atoms per `policy` (immutable, so a scoped force-delete via
    /// [`MemoryEngine::delete_atoms`]; victims are built ONLY from `llm_trace`, so
    /// audit/self-model are unreachable). Missing timestamp/cost count as 0.
    pub fn evict_traces(&self, policy: TraceEvictionPolicy) -> GraphResult<EvictionReport> {
        // Ids are monotonic, so fetch order (oldest first) is chronological.
        let traces = self
            .mem
            .fetch(&self.region, "llm_trace", None, MAX_AUDIT_CHECKS + 1)?;
        if traces.len() > MAX_AUDIT_CHECKS {
            return Err(GraphError::TooLarge {
                kind: "llm_trace",
                max: MAX_AUDIT_CHECKS,
            });
        }

        let victims: Vec<AtomId> = match policy {
            TraceEvictionPolicy::KeepLastN { n } => {
                if traces.len() > n {
                    traces[..traces.len() - n].iter().map(|h| h.id).collect()
                } else {
                    Vec::new()
                }
            }
            TraceEvictionPolicy::ByAge { older_than_micros } => {
                let cutoff = now_micros().saturating_sub(older_than_micros);
                traces
                    .iter()
                    .filter(|h| trace_recorded_at(h) < cutoff)
                    .map(|h| h.id)
                    .collect()
            }
            TraceEvictionPolicy::ByTotalCost { max_cost_usd } => {
                // Newest -> oldest: keep while cumulative cost stays within the
                // cap; the first trace to exceed it and all older ones go.
                let mut kept = 0.0_f64;
                let mut over = false;
                let mut victims = Vec::new();
                for h in traces.iter().rev() {
                    if over {
                        victims.push(h.id);
                        continue;
                    }
                    let next = kept + trace_cost(h);
                    if next <= max_cost_usd {
                        kept = next;
                    } else {
                        over = true;
                        victims.push(h.id);
                    }
                }
                victims
            }
        };

        if victims.is_empty() {
            return Ok(EvictionReport { removed: 0 });
        }
        Ok(self.mem.delete_atoms(&self.region, &victims)?)
    }
}

/// Recorded time of an `llm_trace` atom; missing => 0 (treated as oldest).
fn trace_recorded_at(h: &AtomHit) -> i64 {
    h.payload
        .get("recorded_at_micros")
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

/// Recorded cost of an `llm_trace` atom; missing/null => 0 (no cost pressure).
fn trace_cost(h: &AtomHit) -> f64 {
    h.payload
        .get("cost_usd")
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
}

/// Push a `u64` length prefix (LE) followed by the bytes - unambiguous framing.
fn push_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Pure hash-linkage replay: `(index, reason)` for every check whose stored hash or
/// predecessor link is wrong. Storage-independent, so unit-testable on hand sequences.
fn verify_link_chain(checks: &[CoInstantiationCheck]) -> Vec<(usize, String)> {
    let mut breaches = Vec::new();
    let mut prev_hash = GENESIS_PREV_HASH.to_string();
    for (i, c) in checks.iter().enumerate() {
        if c.prev_hash != prev_hash {
            breaches.push((i, "prev_hash does not link to predecessor".to_string()));
        }
        if c.compute_hash() != c.this_hash {
            breaches.push((i, "this_hash does not match recomputed hash".to_string()));
        }
        prev_hash = c.this_hash.clone();
    }
    breaches
}

fn check_anchor(
    breaches: &mut Vec<(AtomId, String)>,
    check_id: AtomId,
    label: &str,
    anchor: AtomId,
    anchor_immut: &FxHashMap<AtomId, bool>,
) {
    match anchor_immut.get(&anchor) {
        Some(true) => {}
        Some(false) => breaches.push((check_id, format!("{label} anchor {anchor} is mutable"))),
        None => breaches.push((check_id, format!("{label} anchor {anchor} is missing"))),
    }
}

fn req_str(v: &Value, key: &str, kind: &'static str) -> GraphResult<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| GraphError::Payload {
            kind,
            reason: format!("missing string '{key}'"),
        })
}

fn req_i64(v: &Value, key: &str, kind: &'static str) -> GraphResult<i64> {
    v.get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| GraphError::Payload {
            kind,
            reason: format!("missing integer '{key}'"),
        })
}

fn opt_str(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn str_vec(v: &Value, key: &str) -> Vec<String> {
    v.get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn enum_field<T>(
    v: &Value,
    key: &str,
    kind: &'static str,
    parse: impl Fn(&str) -> Option<T>,
) -> GraphResult<T> {
    let s = req_str(v, key, kind)?;
    parse(&s).ok_or_else(|| GraphError::Payload {
        kind,
        reason: format!("unknown {key} '{s}'"),
    })
}

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel::{Argon2Profile, DatabaseBuilder};
    use citadel_mem::MockEmbedder;

    fn graph() -> (tempfile::TempDir, BeliefGraph) {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let eng = Arc::new(MemoryEngine::open(Arc::new(db)).unwrap());
        eng.create_region("agent", Arc::new(MockEmbedder::new(64)))
            .unwrap();
        (dir, BeliefGraph::new(eng, "agent"))
    }

    #[test]
    fn goal_payload_round_trips() {
        let mut g = Goal::new("ship v1.0");
        g.acceptance_criteria = vec!["tests pass".into(), "docs done".into()];
        g.constraints = vec!["no new deps".into()];
        assert_eq!(g, Goal::from_json(&g.to_json()).unwrap());
    }

    #[test]
    fn task_payload_round_trips() {
        let mut t = Task::new("write parser");
        t.status = TaskStatus::InProgress;
        t.attempts = 3;
        t.last_error = Some("boom".into());
        assert_eq!(t, Task::from_json(&t.to_json()).unwrap());

        let default = Task::new("fresh");
        assert_eq!(default, Task::from_json(&default.to_json()).unwrap());
    }

    #[test]
    fn unblocked_respects_depends_on_chain() {
        let (_d, g) = graph();
        let goal = g.add_goal(&Goal::new("g")).unwrap();
        let root = g.add_task(&Task::new("root"), &[], goal).unwrap();
        let mid = g.add_task(&Task::new("mid"), &[root], goal).unwrap();
        let leaf = g.add_task(&Task::new("leaf"), &[mid], goal).unwrap();

        let ready: Vec<AtomId> = ids(g.next_unblocked_tasks().unwrap());
        assert_eq!(ready, vec![root], "only the root has no pending deps");

        g.set_task_status(root, TaskStatus::Done).unwrap();
        assert_eq!(ids(g.next_unblocked_tasks().unwrap()), vec![mid]);

        g.set_task_status(mid, TaskStatus::Done).unwrap();
        assert_eq!(ids(g.next_unblocked_tasks().unwrap()), vec![leaf]);
    }

    #[test]
    fn task_with_multiple_deps_waits_for_all() {
        let (_d, g) = graph();
        let goal = g.add_goal(&Goal::new("g")).unwrap();
        let a = g.add_task(&Task::new("a"), &[], goal).unwrap();
        let b = g.add_task(&Task::new("b"), &[], goal).unwrap();
        let c = g.add_task(&Task::new("c"), &[a, b], goal).unwrap();

        g.set_task_status(a, TaskStatus::Done).unwrap();
        assert_eq!(
            ids(g.next_unblocked_tasks().unwrap()),
            vec![b],
            "c waits for b"
        );

        g.set_task_status(b, TaskStatus::Done).unwrap();
        assert_eq!(ids(g.next_unblocked_tasks().unwrap()), vec![c]);
    }

    #[test]
    fn record_task_failure_tracks_attempts() {
        let (_d, g) = graph();
        let goal = g.add_goal(&Goal::new("g")).unwrap();
        let t = g.add_task(&Task::new("flaky"), &[], goal).unwrap();

        g.record_task_failure(t, "first error").unwrap();
        let task = g.get_task(t).unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(task.attempts, 1);
        assert_eq!(task.last_error.as_deref(), Some("first error"));

        g.record_task_failure(t, "second error").unwrap();
        assert_eq!(g.get_task(t).unwrap().unwrap().attempts, 2);

        assert!(matches!(
            g.set_task_status(999_999, TaskStatus::Done),
            Err(GraphError::TaskNotFound(_))
        ));
        assert!(g.get_task(999_999).unwrap().is_none());
    }

    #[test]
    fn provenance_edges_are_created() {
        let (_d, g) = graph();
        let goal = g.add_goal(&Goal::new("solve X")).unwrap();
        let task = g.add_task(&Task::new("step 1"), &[], goal).unwrap();
        let hyp = g
            .add_hypothesis(
                &Hypothesis {
                    summary: "try Y".into(),
                    confidence: 0.6,
                },
                goal,
            )
            .unwrap();
        let ev = g
            .add_evidence(
                &Evidence {
                    source: "tool".into(),
                    content: "Y works".into(),
                },
                task,
            )
            .unwrap();
        let refl = g
            .add_reflection(
                &Reflection {
                    insight: "Y is promising".into(),
                    confidence: 0.8,
                },
                goal,
            )
            .unwrap();

        let refines = g
            .mem
            .fetch_edges(Some(hyp), None, Some(EdgeKind::Refines))
            .unwrap();
        assert_eq!(refines.len(), 1);
        assert_eq!(refines[0].dst_id, goal);

        let ev_edges = g
            .mem
            .fetch_edges(Some(ev), None, Some(EdgeKind::DerivedFrom))
            .unwrap();
        assert_eq!(ev_edges[0].dst_id, task);

        let refl_edges = g
            .mem
            .fetch_edges(Some(refl), None, Some(EdgeKind::DerivedFrom))
            .unwrap();
        assert_eq!(refl_edges[0].dst_id, goal);

        assert_eq!(g.get_goal(goal).unwrap().unwrap().prompt, "solve X");
    }

    #[test]
    fn goal_status_upserts() {
        let (_d, g) = graph();
        let goal = g.add_goal(&Goal::new("g")).unwrap();
        assert!(g.get_goal_status(goal).unwrap().is_none());

        g.set_goal_status(goal, GoalStatus::Achieved).unwrap();
        assert_eq!(g.get_goal_status(goal).unwrap(), Some(GoalStatus::Achieved));

        g.set_goal_status(goal, GoalStatus::Abandoned).unwrap();
        assert_eq!(
            g.get_goal_status(goal).unwrap(),
            Some(GoalStatus::Abandoned)
        );
    }

    #[test]
    fn self_model_write_once_then_supersede() {
        let (_d, g) = graph();
        assert!(g.current_self_model().unwrap().is_none());

        let v1 = g.set_self_model(&SelfModel::new("v1")).unwrap();
        assert!(matches!(
            g.set_self_model(&SelfModel::new("dup")),
            Err(GraphError::SelfModelExists)
        ));
        assert_eq!(g.current_self_model_id().unwrap(), Some(v1));
        assert_eq!(g.current_self_model().unwrap().unwrap().identity, "v1");

        let v2 = g.supersede_self_model(&SelfModel::new("v2")).unwrap();
        assert_eq!(g.current_self_model_id().unwrap(), Some(v2));
        assert_eq!(g.current_self_model().unwrap().unwrap().identity, "v2");

        let v3 = g.supersede_self_model(&SelfModel::new("v3")).unwrap();
        assert_eq!(g.current_self_model_id().unwrap(), Some(v3));
        g.verify_self_model_chain().unwrap();

        // Older versions remain immutable and auditable.
        assert!(g.mem.fetch_one("agent", v1).unwrap().unwrap().immutable);
        assert!(g.mem.fetch_one("agent", v2).unwrap().unwrap().immutable);
    }

    #[test]
    fn self_model_branch_is_detected() {
        let (_d, g) = graph();
        let v1 = g.set_self_model(&SelfModel::new("v1")).unwrap();
        let _v2 = g.supersede_self_model(&SelfModel::new("v2")).unwrap();

        // Forge a second self-model that also supersedes v1 -> two heads.
        let x = g
            .mem
            .remember(
                "agent",
                AtomInput::new("self_model", "x")
                    .with_payload(SelfModel::new("x").to_json())
                    .immutable(),
            )
            .unwrap();
        g.mem.link(x, v1, EdgeKind::Supersedes, 1.0).unwrap();
        *g.self_model_head.lock().unwrap() = None; // force a re-seed

        assert!(matches!(
            g.current_self_model_id(),
            Err(GraphError::SelfModelBranch)
        ));
        assert!(matches!(
            g.verify_self_model_chain(),
            Err(GraphError::SelfModelBranch)
        ));
    }

    #[test]
    fn record_check_chains_and_verifies() {
        let (_d, g) = graph();
        let goal = g.add_goal(&Goal::new("solve X")).unwrap();
        let mut sm = SelfModel::new("agent");
        sm.goal_ref = Some(goal);
        let sm_id = g.set_self_model(&sm).unwrap();
        let action = g.add_task(&Task::new("step"), &[], goal).unwrap();

        let id1 = g
            .record_check(
                CoInstantiationCheck::new("a1", goal, sm_id, true, true, 0, 5),
                action,
            )
            .unwrap();
        let id2 = g
            .record_check(
                CoInstantiationCheck::new("a2", goal, sm_id, true, true, 0, 5),
                action,
            )
            .unwrap();
        assert!(id2 > id1);

        // Audit atoms are immutable: no in-place edit.
        assert!(g
            .mem
            .update_atom_payload("agent", id1, &json!({"x": 1}))
            .is_err());

        let report = g.verify_chain().unwrap();
        assert!(report.valid, "breaches: {:?}", report.breaches);
        assert_eq!(report.total_checks, 2);

        let trail = g.export_audit_trail().unwrap();
        assert_eq!(trail.len(), 2);
        assert_eq!(trail[0].prev_hash, "", "genesis");
        assert_eq!(trail[1].prev_hash, trail[0].this_hash, "links forward");
        assert_eq!(trail[0].verdict, Verdict::Pass);
    }

    #[test]
    fn record_check_requires_immutable_anchors() {
        let (_d, g) = graph();
        let goal = g.add_goal(&Goal::new("g")).unwrap();
        let sm = g.set_self_model(&SelfModel::new("agent")).unwrap();
        let action = g.add_task(&Task::new("act"), &[], goal).unwrap();

        // A mutable "goal" atom (not via add_goal, which is immutable).
        let mutable_goal = g
            .mem
            .remember(
                "agent",
                AtomInput::new("goal", "mut").with_payload(json!({})),
            )
            .unwrap();
        assert!(matches!(
            g.record_check(
                CoInstantiationCheck::new("a", mutable_goal, sm, true, true, 0, 5),
                action
            ),
            Err(GraphError::GoalMutable(_))
        ));

        assert!(matches!(
            g.record_check(
                CoInstantiationCheck::new("a", 999_999, sm, true, true, 0, 5),
                action
            ),
            Err(GraphError::GoalNotFound(_))
        ));
    }

    #[test]
    fn verify_link_chain_detects_tamper_and_reorder() {
        let mut a = CoInstantiationCheck::new("a", 1, 2, true, true, 0, 5);
        a.timestamp_micros = 100;
        a.prev_hash = String::new();
        a.this_hash = a.compute_hash();
        let mut b = CoInstantiationCheck::new("b", 1, 2, true, true, 0, 5);
        b.timestamp_micros = 200;
        b.prev_hash = a.this_hash.clone();
        b.this_hash = b.compute_hash();

        assert!(
            verify_link_chain(&[a.clone(), b.clone()]).is_empty(),
            "a valid chain has no breaches"
        );

        let mut tampered = b.clone();
        tampered.this_hash = "deadbeef".into();
        assert!(
            !verify_link_chain(&[a.clone(), tampered]).is_empty(),
            "a forged this_hash is caught"
        );

        assert!(
            !verify_link_chain(&[b, a]).is_empty(),
            "reordering breaks the prev_hash linkage"
        );
    }

    #[test]
    fn evict_guarded_protects_audit_chain() {
        let (_d, g) = graph();
        let goal = g.add_goal(&Goal::new("g")).unwrap();
        let sm = g.set_self_model(&SelfModel::new("agent")).unwrap();
        let action = g.add_task(&Task::new("act"), &[], goal).unwrap();
        g.record_check(
            CoInstantiationCheck::new("a1", goal, sm, true, true, 0, 5),
            action,
        )
        .unwrap();

        assert!(matches!(
            g.evict_guarded(EvictionPolicy::PurgeRegion, false),
            Err(GraphError::EvictionRefused)
        ));
        let report = g.evict_guarded(EvictionPolicy::PurgeRegion, true).unwrap();
        assert!(report.removed > 0, "force purges the chain");
    }

    #[test]
    fn has_provenance_requires_refines_edge_and_immutable_goal() {
        let (_d, g) = graph();
        let goal = g.add_goal(&Goal::new("anchor")).unwrap();
        let task = g.add_task(&Task::new("t"), &[], goal).unwrap();
        assert!(
            g.has_provenance(task, goal).unwrap(),
            "task refines the goal"
        );

        // An atom with no Refines edge to the goal has no provenance.
        let ev = g
            .add_evidence(
                &Evidence {
                    source: "s".into(),
                    content: "c".into(),
                },
                task,
            )
            .unwrap();
        assert!(
            !g.has_provenance(ev, goal).unwrap(),
            "evidence has no Refines(goal) edge"
        );

        // A mutable atom used as the goal_ref fails (the anchor must be immutable).
        let mutable = g
            .mem
            .remember("agent", AtomInput::new("goal", "m").with_payload(json!({})))
            .unwrap();
        let t2 = g.add_task(&Task::new("t2"), &[], mutable).unwrap();
        assert!(
            !g.has_provenance(t2, mutable).unwrap(),
            "goal_ref is mutable"
        );
    }

    #[test]
    fn evidence_for_goal_excludes_audit_atoms() {
        let (_d, g) = graph();
        let goal = g.add_goal(&Goal::new("solve")).unwrap();
        let mut sm = SelfModel::new("agent");
        sm.goal_ref = Some(goal);
        let sm_id = g.set_self_model(&sm).unwrap();
        let task = g.add_task(&Task::new("t"), &[], goal).unwrap();
        g.add_evidence(
            &Evidence {
                source: "tool".into(),
                content: "found it".into(),
            },
            task,
        )
        .unwrap();
        // record_check links an immutable audit atom DerivedFrom the task too.
        g.record_check(
            CoInstantiationCheck::new("a", goal, sm_id, true, true, 0, 5),
            task,
        )
        .unwrap();

        let evidence = g.evidence_for_goal(goal).unwrap();
        assert_eq!(
            evidence.len(),
            1,
            "only the evidence atom, not the audit atom"
        );
        assert_eq!(evidence[0], ("tool".to_string(), "found it".to_string()));
    }

    #[test]
    fn record_llm_call_is_immutable() {
        let (_d, g) = graph();
        let id = g
            .record_llm_call(
                "hash123",
                "mock-1",
                &json!({"text": "hi"}),
                Some(0.25),
                None,
            )
            .unwrap();
        let hit = g.mem.fetch_one("agent", id).unwrap().unwrap();
        assert!(hit.immutable);
        assert_eq!(hit.kind, "llm_trace");
        assert_eq!(hit.payload["cost_usd"], json!(0.25));
        assert!(hit.payload["recorded_at_micros"].as_i64().unwrap() > 0);
        assert!(
            g.mem
                .update_atom_payload("agent", id, &json!({"x": 1}))
                .is_err(),
            "llm_trace atoms are immutable"
        );
    }

    #[test]
    fn record_llm_call_folds_prompt_provenance() {
        let (_d, g) = graph();
        let prov = json!({
            "node": "planner", "version": 1, "hash": "abc", "source": "shipped_default"
        });
        let id = g
            .record_llm_call("h", "mock", &json!({"text": "x"}), None, Some(&prov))
            .unwrap();
        let hit = g.mem.fetch_one("agent", id).unwrap().unwrap();
        assert_eq!(hit.payload["prompt"]["node"], json!("planner"));
        assert_eq!(hit.payload["prompt"]["source"], json!("shipped_default"));
        // With no prompt context the key is absent (not null).
        let id2 = g
            .record_llm_call("h2", "mock", &json!({}), None, None)
            .unwrap();
        let hit2 = g.mem.fetch_one("agent", id2).unwrap().unwrap();
        assert!(hit2.payload.get("prompt").is_none());
    }

    #[test]
    fn evict_traces_keep_last_n() {
        let (_d, g) = graph();
        for i in 0..5 {
            g.record_llm_call(&format!("h{i}"), "mock", &json!({ "i": i }), None, None)
                .unwrap();
        }
        let report = g
            .evict_traces(TraceEvictionPolicy::KeepLastN { n: 2 })
            .unwrap();
        assert_eq!(report.removed, 3);
        let kept = g.load_llm_traces().unwrap();
        let hashes: Vec<&str> = kept.iter().map(|(h, _)| h.as_str()).collect();
        assert_eq!(hashes, vec!["h3", "h4"], "the two newest survive");
    }

    #[test]
    fn evict_traces_by_age_uses_recorded_time() {
        let (_d, g) = graph();
        let seed = |hash: &str, payload: Value| {
            g.mem
                .remember(
                    "agent",
                    AtomInput::new("llm_trace", hash)
                        .with_payload(payload)
                        .immutable(),
                )
                .unwrap()
        };
        let now = now_micros();
        seed(
            "old",
            json!({ "model_id": "mock", "response": {}, "recorded_at_micros": now - 10_000_000 }),
        );
        // A legacy trace with no timestamp counts as oldest (evictable).
        seed("legacy", json!({ "model_id": "mock", "response": {} }));
        seed(
            "recent",
            json!({ "model_id": "mock", "response": {}, "recorded_at_micros": now - 1_000 }),
        );

        let report = g
            .evict_traces(TraceEvictionPolicy::ByAge {
                older_than_micros: 5_000_000,
            })
            .unwrap();
        assert_eq!(report.removed, 2, "old + legacy evicted, recent kept");
        let kept = g.load_llm_traces().unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].0, "recent");
    }

    #[test]
    fn evict_traces_by_total_cost_keeps_newest_within_cap() {
        let (_d, g) = graph();
        for i in 0..5 {
            g.record_llm_call(&format!("h{i}"), "mock", &json!({}), Some(2.0), None)
                .unwrap();
        }
        // Newest-first cumulative: 2, 4 (<=5 keep), 6 (>5 -> evict it + older).
        let report = g
            .evict_traces(TraceEvictionPolicy::ByTotalCost { max_cost_usd: 5.0 })
            .unwrap();
        assert_eq!(report.removed, 3);
        assert_eq!(g.load_llm_traces().unwrap().len(), 2);
    }

    #[test]
    fn evict_traces_by_total_cost_keeps_all_cost_free_traces() {
        let (_d, g) = graph();
        for i in 0..3 {
            g.record_llm_call(&format!("h{i}"), "mock", &json!({}), None, None)
                .unwrap();
        }
        // None -> 0 cost: cumulative stays 0, never exceeds the cap.
        let report = g
            .evict_traces(TraceEvictionPolicy::ByTotalCost { max_cost_usd: 1.0 })
            .unwrap();
        assert_eq!(report.removed, 0);
    }

    #[test]
    fn evict_traces_never_touches_audit_or_self_model() {
        let (_d, g) = graph();
        let sm = g
            .mem
            .remember(
                "agent",
                AtomInput::new("self_model", "identity").immutable(),
            )
            .unwrap();
        let au = g
            .mem
            .remember("agent", AtomInput::new("audit", "check").immutable())
            .unwrap();
        g.record_llm_call("h0", "mock", &json!({}), None, None)
            .unwrap();
        g.record_llm_call("h1", "mock", &json!({}), None, None)
            .unwrap();

        // Evict every trace; the kind filter keeps the protected chains safe.
        let report = g
            .evict_traces(TraceEvictionPolicy::KeepLastN { n: 0 })
            .unwrap();
        assert_eq!(report.removed, 2);
        assert!(g.load_llm_traces().unwrap().is_empty(), "traces gone");
        assert!(
            g.mem.fetch_one("agent", sm).unwrap().is_some(),
            "self_model survives"
        );
        assert!(
            g.mem.fetch_one("agent", au).unwrap().is_some(),
            "audit survives"
        );
    }

    fn ids(tasks: Vec<(AtomId, Task)>) -> Vec<AtomId> {
        tasks.into_iter().map(|(id, _)| id).collect()
    }
}
