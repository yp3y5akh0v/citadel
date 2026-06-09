//! In-crate coverage of the GENERAL discovery controller (`Agent::run_discovery`)
//! with a problem-agnostic `MockVerifier`. Proves the controller archives, scores,
//! mints, and converges with ANY deterministic checker - independent of any concrete
//! problem plugin (tested in its own crate).

use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_ai::testing;
use citadel_ai::{
    Agent, AgentBudget, AgentConfig, BeliefGraph, Candidate, CheckerAttestation, Completer,
    DiscoveryGoal, Goal, ProposalContext, ProposalOperator, ProposeError, ScoredOutcome,
    TerminatedBy, ToolRegistry, VerifiedKind, Verifier, VerifyError, VerifyOutcome, VerifyRequest,
};
use citadel_mem::{MemoryEngine, MockEmbedder};
use serde_json::json;

/// Accepts every artifact at a fixed score and attests, so it may mint. Carries
/// no problem semantics - the controller never sees a concrete domain.
struct MockVerifier {
    score: f64,
}

impl Verifier for MockVerifier {
    fn verify(&self, _req: &VerifyRequest<'_>) -> Result<VerifyOutcome, VerifyError> {
        Ok(VerifyOutcome {
            satisfied: true,
            reason: "mock accepts".into(),
        })
    }

    fn score(&self, _req: &VerifyRequest<'_>) -> Result<ScoredOutcome, VerifyError> {
        Ok(ScoredOutcome {
            satisfied: true,
            score: self.score,
            reason: "mock score".into(),
        })
    }

    fn attestation(&self) -> Option<CheckerAttestation> {
        Some(CheckerAttestation::new("mock-checker", "1"))
    }

    fn cross_check(&self, _req: &VerifyRequest<'_>) -> Result<bool, VerifyError> {
        Ok(true)
    }
}

/// Emits one fixed candidate per round, ignoring the LLM, so the test isolates the
/// controller's archive/score/mint/converge logic from any proposer behavior.
struct FixedProposer;

impl ProposalOperator for FixedProposer {
    fn propose(
        &self,
        _ctx: &ProposalContext<'_>,
        _llm: &mut dyn Completer,
    ) -> Result<Vec<Candidate>, ProposeError> {
        Ok(vec![Candidate {
            artifact: json!({ "value": 42 }),
            parent: None,
            rationale: "fixed candidate".into(),
        }])
    }
}

#[test]
fn run_discovery_climbs_and_mints_with_a_mock_verifier() {
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("m.db"))
        .passphrase(b"disc-controller")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let eng = Arc::new(MemoryEngine::open(Arc::new(db)).unwrap());
    eng.create_region("agent", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    let graph = BeliefGraph::new(eng, "agent");

    let config = AgentConfig {
        verifier: Some(Arc::new(MockVerifier { score: 5.0 })),
        proposal_operator: Some(Arc::new(FixedProposer)),
        ..Default::default()
    };
    let llm = testing::scripted(vec![]);
    let agent = Agent::new(
        llm,
        graph,
        ToolRegistry::new(),
        AgentBudget::default(),
        config,
    );

    let report = agent
        .run_discovery(DiscoveryGoal {
            goal: Goal::new("a problem-agnostic discovery target"),
            kind: VerifiedKind::Construction,
            baseline_score: 0.0,
            archive_width: 8,
            max_idle_rounds: 1,
        })
        .unwrap();

    assert_eq!(report.terminated_by, TerminatedBy::Success);
    assert!(
        report.verified.is_some(),
        "a score above baseline, cross-checked, mints a verified record"
    );
    assert_eq!(report.cross_check_failures, 0);
    assert!(report.chain_valid, "the audit chain verifies after a mint");
}
