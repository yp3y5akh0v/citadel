//! In-crate coverage of the GENERAL discovery controller (`Agent::run_discovery`)
//! with a problem-agnostic `MockVerifier`. Proves the controller archives, scores,
//! mints, and converges with ANY deterministic checker - independent of any concrete
//! problem plugin (tested in its own crate).

use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_ai::testing;
use citadel_ai::{
    Agent, AgentBudget, AgentConfig, BeliefGraph, Candidate, CheckerAttestation, Completer,
    CompletionRequest, CompletionResponse, DiscoveryGoal, Goal, Message, ProposalContext,
    ProposalOperator, ProposeError, ScoredOutcome, TerminatedBy, ToolRegistry, VerifiedKind,
    Verifier, VerifyError, VerifyOutcome, VerifyRequest,
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
            cell: String::new(),
            terminal: false,
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
        _llm: Box<dyn Completer>,
    ) -> Result<Vec<Candidate>, ProposeError> {
        Ok(vec![Candidate {
            artifact: json!({ "value": 42 }),
            parent: None,
            rationale: "fixed candidate".into(),
        }])
    }
}

/// Scores each candidate from its own JSON (`score`, `cell`), so the test
/// controls exactly which mint-bar cell every candidate competes in.
struct CellVerifier;

impl Verifier for CellVerifier {
    fn verify(&self, _req: &VerifyRequest<'_>) -> Result<VerifyOutcome, VerifyError> {
        Ok(VerifyOutcome {
            satisfied: true,
            reason: "cell mock accepts".into(),
        })
    }

    fn score(&self, req: &VerifyRequest<'_>) -> Result<ScoredOutcome, VerifyError> {
        let v: serde_json::Value = serde_json::from_str(&req.evidence[0].1)
            .map_err(|e| VerifyError::Failed(e.to_string()))?;
        Ok(ScoredOutcome {
            satisfied: true,
            score: v["score"].as_f64().unwrap_or(0.0),
            reason: "cell mock score".into(),
            cell: v["cell"].as_str().unwrap_or_default().to_string(),
            terminal: v["terminal"].as_bool().unwrap_or(false),
        })
    }

    fn attestation(&self) -> Option<CheckerAttestation> {
        Some(CheckerAttestation::new("cell-mock-checker", "1"))
    }

    fn cross_check(&self, _req: &VerifyRequest<'_>) -> Result<bool, VerifyError> {
        Ok(true)
    }
}

/// Emits the same fixed multi-cell batch every round: round one mints, later
/// rounds tie their cells' bars (equal, not greater) so the run converges.
struct MultiCellProposer;

impl ProposalOperator for MultiCellProposer {
    fn propose(
        &self,
        _ctx: &ProposalContext<'_>,
        _llm: Box<dyn Completer>,
    ) -> Result<Vec<Candidate>, ProposeError> {
        Ok([(5.0, "a"), (3.0, "a"), (7.0, "a"), (4.0, "b")]
            .iter()
            .map(|(score, cell)| Candidate {
                artifact: json!({ "score": score, "cell": cell }),
                parent: None,
                rationale: "scripted".into(),
            })
            .collect())
    }
}

fn run_multi_cell(max_mints: u32) -> citadel_ai::DiscoveryReport {
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("m.db"))
        .passphrase(b"disc-cells")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let eng = Arc::new(MemoryEngine::open(Arc::new(db)).unwrap());
    eng.create_region("agent", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    let graph = BeliefGraph::new(eng, "agent");
    let config = AgentConfig {
        verifier: Some(Arc::new(CellVerifier)),
        proposal_operator: Some(Arc::new(MultiCellProposer)),
        ..Default::default()
    };
    let agent = Agent::new(
        testing::scripted(vec![]),
        graph,
        ToolRegistry::new(),
        AgentBudget::default(),
        config,
    );
    agent
        .run_discovery(DiscoveryGoal {
            goal: Goal::new("multi-cell minting"),
            kind: VerifiedKind::Construction,
            baseline_score: 0.0,
            archive_width: 8,
            max_idle_rounds: 1,
            max_mints,
        })
        .unwrap()
}

#[test]
fn per_cell_bars_mint_independently_and_ratchet_within_a_cell() {
    let report = run_multi_cell(16);
    // Cell "a" arrivals 5, 3, 7 over baseline 0 mint {5, 7} (the in-cell
    // ratchet skips 3); cell "b"'s 4 mints against ITS OWN baseline instead of
    // being blocked by cell "a"'s 7. The representative `verified` is the
    // max-score mint, not the last.
    assert_eq!(report.minted.len(), 3, "two cells mint independently");
    assert_eq!(report.best_score, 7.0);
    assert_eq!(report.terminated_by, TerminatedBy::Success);
    assert!(report.verified.is_some());
    assert_eq!(report.cross_check_failures, 0);
}

#[test]
fn max_mints_caps_the_run_without_failing_it() {
    let report = run_multi_cell(2);
    // Arrival order mints 5 then 7; the cap skips everything after (cell
    // diversity can never inflate the count) and the run still converges.
    assert_eq!(report.minted.len(), 2);
    assert_eq!(report.terminated_by, TerminatedBy::Success);
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
            max_mints: 16,
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

#[test]
fn proposer_multi_call_is_fully_traced_and_still_mints() {
    // A proposal operator that drives the owned channel TWICE per round - the
    // multi-call path no built-in operator exercises. The channel buffers both
    // calls and the controller drains them, so EVERY call is traced and the
    // returned candidate still mints.
    struct TwoCallProposer;
    impl ProposalOperator for TwoCallProposer {
        fn propose(
            &self,
            _ctx: &ProposalContext<'_>,
            mut llm: Box<dyn Completer>,
        ) -> Result<Vec<Candidate>, ProposeError> {
            llm.complete(&CompletionRequest::new(vec![Message::user("first")]))?;
            llm.complete(&CompletionRequest::new(vec![Message::user("second")]))?;
            Ok(vec![Candidate {
                artifact: json!({ "v": 1 }),
                parent: None,
                rationale: "two-call".into(),
            }])
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("m.db"))
        .passphrase(b"two-call")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let eng = Arc::new(MemoryEngine::open(Arc::new(db)).unwrap());
    eng.create_region("agent", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    let graph = BeliefGraph::new(eng, "agent");
    let config = AgentConfig {
        verifier: Some(Arc::new(MockVerifier { score: 5.0 })),
        proposal_operator: Some(Arc::new(TwoCallProposer)),
        ..Default::default()
    };
    let agent = Agent::new(
        testing::scripted(vec![CompletionResponse::text("{}"); 20]),
        graph,
        ToolRegistry::new(),
        AgentBudget::default(),
        config,
    );
    let report = agent
        .run_discovery(DiscoveryGoal {
            goal: Goal::new("multi-call channel"),
            kind: VerifiedKind::Construction,
            baseline_score: 0.0,
            archive_width: 8,
            max_idle_rounds: 1,
            max_mints: 1,
        })
        .unwrap();
    // Every proposer call (two per round) was buffered then traced - no call lost.
    let traces = agent.graph().load_llm_traces().unwrap();
    assert_eq!(
        traces.len(),
        2 * report.proposals as usize,
        "two completer calls per proposal round, all traced"
    );
    assert!(report.proposals >= 1);
    assert!(
        report.verified.is_some(),
        "the multi-call operator still mints"
    );
    assert!(report.chain_valid);
}

#[test]
fn discovery_cost_cap_fails_closed_on_the_first_unpriced_call() {
    use std::sync::atomic::{AtomicU32, Ordering};

    // The owned channel checks the cost cap PER CALL, so an unpriced response stops
    // the operator on its FIRST call - it never makes a second, untracked call.
    struct CountingProposer(Arc<AtomicU32>);
    impl ProposalOperator for CountingProposer {
        fn propose(
            &self,
            _ctx: &ProposalContext<'_>,
            mut llm: Box<dyn Completer>,
        ) -> Result<Vec<Candidate>, ProposeError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            llm.complete(&CompletionRequest::new(vec![Message::user("first")]))?;
            self.0.fetch_add(1, Ordering::Relaxed);
            llm.complete(&CompletionRequest::new(vec![Message::user("second")]))?;
            Ok(Vec::new())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("m.db"))
        .passphrase(b"cost-cap")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let eng = Arc::new(MemoryEngine::open(Arc::new(db)).unwrap());
    eng.create_region("agent", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    let graph = BeliefGraph::new(eng, "agent");
    let calls = Arc::new(AtomicU32::new(0));
    let config = AgentConfig {
        verifier: Some(Arc::new(MockVerifier { score: 5.0 })),
        proposal_operator: Some(Arc::new(CountingProposer(Arc::clone(&calls)))),
        ..Default::default()
    };
    let budget = AgentBudget {
        max_cost_usd: Some(1.0),
        ..Default::default()
    };
    let agent = Agent::new(
        testing::scripted(vec![CompletionResponse::text("{}"); 4]),
        graph,
        ToolRegistry::new(),
        budget,
        config,
    );
    let result = agent.run_discovery(DiscoveryGoal {
        goal: Goal::new("cost-capped unpriced run"),
        kind: VerifiedKind::Construction,
        baseline_score: 0.0,
        archive_width: 8,
        max_idle_rounds: 1,
        max_mints: 1,
    });
    assert!(
        result.is_err(),
        "an unpriced response under a cost cap fails closed"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "the operator stopped on the first call; the second never ran"
    );
}
