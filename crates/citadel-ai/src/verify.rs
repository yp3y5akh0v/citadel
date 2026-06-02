//! Pluggable deterministic verifier for constraint and acceptance decisions.
//!
//! A [`Verifier`] is a pure oracle (goal + context -> verdict); it must NOT call an
//! LLM, the network, or memory. When none is wired in, the agent loop falls back to
//! a bounded, audited critic. A subprocess/SMT/proof-checker impl is the caller's choice.

use crate::graph::Goal;
use crate::llm::ToolCall;

/// Which decision a [`Verifier`] is asked to make.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyKind {
    /// Does the action (its dispatched tool calls) comply with the goal's constraints?
    Constraint,
    /// Does the gathered evidence satisfy the goal's acceptance criteria?
    Acceptance,
    /// Rank a candidate artifact for an elite archive (read only by the
    /// discovery controller via [`Verifier::score`]; never gates the core loop).
    Rank,
}

/// The context handed to a [`Verifier`]. Borrows everything; nothing is owned.
pub struct VerifyRequest<'a> {
    pub kind: VerifyKind,
    /// The immutable charter: prompt, acceptance criteria, constraints.
    pub goal: &'a Goal,
    /// Constraint checks: the tool calls dispatched this step. Acceptance: `&[]`.
    pub tool_calls: &'a [ToolCall],
    /// Acceptance checks: gathered `(source, content)` evidence. Constraint: `&[]`.
    pub evidence: &'a [(String, String)],
}

/// A verifier's verdict with a human-readable reason (recorded for audit).
#[derive(Debug, Clone)]
pub struct VerifyOutcome {
    pub satisfied: bool,
    pub reason: String,
}

/// A verdict plus a scalar `score` for ranking discovery candidates. Admission
/// stays gated on `satisfied` (a buggy score can mis-rank, never admit a false
/// positive); higher is better. Callers must reject non-finite scores (NaN
/// corrupts a heap).
#[derive(Debug, Clone)]
pub struct ScoredOutcome {
    pub satisfied: bool,
    pub score: f64,
    pub reason: String,
}

/// Self-identification of a deterministic checker. Only a real checker returns
/// `Some` (stamped onto every `verified_*` atom it mints); a critic-LLM returns
/// `None` and is barred from minting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckerAttestation {
    pub checker_id: String,
    pub checker_version: String,
}

impl CheckerAttestation {
    pub fn new(checker_id: impl Into<String>, checker_version: impl Into<String>) -> Self {
        Self {
            checker_id: checker_id.into(),
            checker_version: checker_version.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("verifier failed: {0}")]
    Failed(String),
}

/// Deterministic external arbiter for a constraint or acceptance decision; when
/// none is configured the agent falls back to a bounded, audited critic LLM call.
pub trait Verifier: Send + Sync {
    fn verify(&self, req: &VerifyRequest<'_>) -> Result<VerifyOutcome, VerifyError>;

    /// Rank a candidate for a discovery elite archive. Default derives 1.0/0.0 from
    /// [`Verifier::verify`]; a discovery checker overrides with its real metric (e.g.
    /// `|A|`). Read ONLY by the discovery controller; the core loop reads `satisfied`.
    fn score(&self, req: &VerifyRequest<'_>) -> Result<ScoredOutcome, VerifyError> {
        let o = self.verify(req)?;
        Ok(ScoredOutcome {
            score: if o.satisfied { 1.0 } else { 0.0 },
            satisfied: o.satisfied,
            reason: o.reason,
        })
    }

    /// Identify this verifier as a deterministic checker eligible to mint `verified_*`
    /// artifacts ([`BeliefGraph::add_verified_artifact`]). Default `None` bars a critic.
    ///
    /// [`BeliefGraph::add_verified_artifact`]: crate::graph::BeliefGraph::add_verified_artifact
    fn attestation(&self) -> Option<CheckerAttestation> {
        None
    }

    /// Independent re-validation for the high-stakes novel-mint path: a second oracle
    /// must AGREE with [`Verifier::verify`] before a `verified_*` record is stamped
    /// (fail-closed against a latent checker bug). Default `Ok(true)` = no second
    /// oracle; the controller refuses to mint on `Ok(false)`. Run only at mint time.
    fn cross_check(&self, _req: &VerifyRequest<'_>) -> Result<bool, VerifyError> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Goal;

    struct Fixed(bool);
    impl Verifier for Fixed {
        fn verify(&self, _req: &VerifyRequest<'_>) -> Result<VerifyOutcome, VerifyError> {
            Ok(VerifyOutcome {
                satisfied: self.0,
                reason: "fixed".into(),
            })
        }
    }

    /// The default `score` derives a 1.0/0.0 scale from `verify`.
    #[test]
    fn default_score_maps_satisfied_to_unit_scale() {
        let goal = Goal::new("g");
        let req = VerifyRequest {
            kind: VerifyKind::Rank,
            goal: &goal,
            tool_calls: &[],
            evidence: &[],
        };
        let yes = Fixed(true).score(&req).unwrap();
        assert!(yes.satisfied);
        assert_eq!(yes.score, 1.0);
        let no = Fixed(false).score(&req).unwrap();
        assert!(!no.satisfied);
        assert_eq!(no.score, 0.0);
    }
}
