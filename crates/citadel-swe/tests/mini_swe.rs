//! mini-SWE benchmark: the token-free mock smoke + gold self-test run on every CI
//! build; the live real-LLM run is triple-gated (`#[ignore]` + `CITADEL_SWE_LIVE`
//! + the `claude` feature + a key), so a normal `cargo test` never burns tokens.
// file-tools + command-tool are always on for this crate, so only wasm is gated.
#![cfg(not(target_arch = "wasm32"))]

mod swe_harness;

use std::fs;
use std::sync::Arc;

use serde_json::json;

use citadel_ai::testing;
use citadel_ai::{AgentBudget, CompletionResponse, FileWriteTool, LLMClient, Tool};
use citadel_ai::{AgentReport, TerminatedBy};

use swe_harness::*;

/// Every task is solvable (gold solves) and genuinely buggy (pristine does not).
#[test]
fn gold_self_test_all_tasks() {
    for id in all_task_ids() {
        gold_patch_selftest(&id);
    }
}

/// Real agent + real tools, only the LLM mocked: the agent writes the fix and the
/// independent scorer confirms it solved the task.
#[test]
fn mock_smoke_agent_solves_a_task() {
    let id = "off_by_one_window";
    let gold = fs::read_to_string(tasks_dir().join(id).join("gold").join("lib.rs")).unwrap();
    let scratch = clone_fixture(id);
    let src_path = scratch.root.join("src").join("lib.rs");
    let m = load_manifest(id);

    let llm: Arc<dyn LLMClient> = testing::scripted(vec![
        plan_response(&["fix the off-by-one in window_maxes"]),
        tool_call(
            "file_write",
            json!({ "path": src_path.to_str().unwrap(), "contents": gold }),
        ),
        CompletionResponse::text("applied the fix"),
    ]);
    let (_memdir, agent) = build_agent(&scratch, &m, llm, AgentBudget::default());
    let report: AgentReport = agent.run(build_prompt(&scratch, &m)).unwrap();
    assert_eq!(report.terminated_by, TerminatedBy::Success);
    assert!(report.chain_valid);

    restore_and_inject_tests(&scratch, id);
    assert!(
        score_task(&scratch, &m),
        "the fix the agent wrote scores solved"
    );
}

/// A gutted fix scores not-solved even when the agent's loop reports success.
#[test]
fn mock_smoke_bad_fix_scores_zero() {
    let id = "off_by_one_window";
    let scratch = clone_fixture(id);
    let src_path = scratch.root.join("src").join("lib.rs");
    let m = load_manifest(id);

    let bad = "pub fn window_maxes(_xs: &[i32], _k: usize) -> Vec<i32> { Vec::new() }\n";
    let llm: Arc<dyn LLMClient> = testing::scripted(vec![
        plan_response(&["fix it"]),
        tool_call(
            "file_write",
            json!({ "path": src_path.to_str().unwrap(), "contents": bad }),
        ),
        CompletionResponse::text("done"),
    ]);
    let (_memdir, agent) = build_agent(&scratch, &m, llm, AgentBudget::default());
    let report = agent.run(build_prompt(&scratch, &m)).unwrap();
    assert_eq!(
        report.terminated_by,
        TerminatedBy::Success,
        "agent claims success"
    );

    restore_and_inject_tests(&scratch, id);
    assert!(
        !score_task(&scratch, &m),
        "a gutted fix must score not-solved despite the agent's claim"
    );
}

/// The agent cannot edit a test file (only src/ is writable).
#[test]
fn write_sandbox_blocks_editing_the_test() {
    let scratch = clone_fixture("off_by_one_window");
    let writer = FileWriteTool::new([scratch.root.join("src")]).unwrap();
    let err = writer
        .call(&json!({
            "path": scratch.root.join("tests").join("visible.rs").to_str().unwrap(),
            "contents": "fn main() {}"
        }))
        .unwrap_err();
    assert!(format!("{err}").contains("outside allowed roots"));
}

/// With non-empty acceptance criteria and a clean solved trajectory (gold fix,
/// green `cargo test`, answer), the agent converges via SweTestVerifier.
#[test]
fn verifier_converges_on_green_cargo_test() {
    let id = "off_by_one_window";
    let gold = fs::read_to_string(tasks_dir().join(id).join("gold").join("lib.rs")).unwrap();
    let scratch = clone_fixture(id);
    let src_path = scratch.root.join("src").join("lib.rs");
    let m = load_manifest(id);

    let llm: Arc<dyn LLMClient> = testing::scripted(vec![
        plan_response_with_criteria(&["the visible test passes"], &["fix the off-by-one"]),
        tool_call(
            "file_write",
            json!({ "path": src_path.to_str().unwrap(), "contents": gold }),
        ),
        tool_call(
            "run_command",
            json!({ "program": "cargo", "args": ["test"] }),
        ),
        CompletionResponse::text("fixed the off-by-one; cargo test is green"),
    ]);
    let (_memdir, agent) = build_agent(&scratch, &m, llm, AgentBudget::default());
    let report = agent.run(build_prompt(&scratch, &m)).unwrap();
    assert_eq!(
        report.terminated_by,
        TerminatedBy::Success,
        "a clean green cargo test under non-empty criteria must converge via the verifier"
    );
    assert!(report.chain_valid);
}

/// A clean trajectory plus a few exploratory rounds under the live caps
/// (max_steps=30) must still converge to Success.
#[test]
fn live_budget_converges_on_realistic_trajectory() {
    let id = "off_by_one_window";
    let gold = fs::read_to_string(tasks_dir().join(id).join("gold").join("lib.rs")).unwrap();
    let scratch = clone_fixture(id);
    let src_path = scratch.root.join("src").join("lib.rs");
    let p = src_path.to_str().unwrap();
    let m = load_manifest(id);

    let llm: Arc<dyn LLMClient> = testing::scripted(vec![
        plan_response_with_criteria(&["the visible test passes"], &["fix the off-by-one"]),
        tool_call("file_read", json!({ "path": p })),
        tool_call("file_read", json!({ "path": p })),
        tool_call("file_write", json!({ "path": p, "contents": gold })),
        tool_call(
            "run_command",
            json!({ "program": "cargo", "args": ["test"] }),
        ),
        CompletionResponse::text("fixed; cargo test is green"),
    ]);
    let (_memdir, agent) = build_agent(
        &scratch,
        &m,
        llm,
        AgentBudget {
            max_steps: 30,
            ..Default::default()
        },
    );
    let report = agent.run(build_prompt(&scratch, &m)).unwrap();
    assert_eq!(
        report.terminated_by,
        TerminatedBy::Success,
        "30 steps must suffice for a clean multi-round solve ending in a green test"
    );
}

/// A goal verified (green cargo test) on the FIRST of several planned tasks must
/// converge to Success with the rest still Pending, not grind to the step cap.
/// The tight budget proves the early-converge route fires.
#[test]
fn verified_goal_with_pending_siblings_converges() {
    let id = "off_by_one_window";
    let gold = fs::read_to_string(tasks_dir().join(id).join("gold").join("lib.rs")).unwrap();
    let scratch = clone_fixture(id);
    let src_path = scratch.root.join("src").join("lib.rs");
    let m = load_manifest(id);

    let llm: Arc<dyn LLMClient> = testing::scripted(vec![
        plan_response_with_criteria(
            &["the visible test passes"],
            &["fix the off-by-one", "add docs", "clean up"],
        ),
        tool_call(
            "file_write",
            json!({ "path": src_path.to_str().unwrap(), "contents": gold }),
        ),
        tool_call(
            "run_command",
            json!({ "program": "cargo", "args": ["test"] }),
        ),
        CompletionResponse::text("fixed the off-by-one; cargo test is green"),
    ]);
    let (_memdir, agent) = build_agent(
        &scratch,
        &m,
        llm,
        AgentBudget {
            max_steps: 12,
            ..Default::default()
        },
    );
    let report = agent.run(build_prompt(&scratch, &m)).unwrap();
    assert_eq!(
        report.terminated_by,
        TerminatedBy::Success,
        "a goal verified on an early task must converge even with Pending sibling tasks"
    );
    assert!(report.chain_valid);
}

/// The real-LLM run (opt-in: `CITADEL_SWE_LIVE` + `#[ignore]` + a backend
/// feature). The backend is selected by the factory (default claude); scores
/// solved/total and fails only if the rate drops below a floor.
#[cfg(feature = "live")]
#[test]
#[ignore]
fn live_swe_bench() {
    if std::env::var("CITADEL_SWE_LIVE").is_err() {
        return;
    }
    let llm = citadel_ai::factory::from_env("CITADEL_SWE", "claude", "claude-sonnet-4-6")
        .unwrap_or_else(|e| panic!("{e}"));
    let model = llm.model_id().to_string();
    let min_rate: f64 = std::env::var("CITADEL_SWE_MIN_RATE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.4);

    let mut ids = all_task_ids();
    // Optional single-task capture (cheap), e.g. for a trajectory dump run.
    if let Ok(only) = std::env::var("CITADEL_SWE_ONLY") {
        ids.retain(|id| *id == only);
    }
    let mut solved = 0usize;
    let mut rows: Vec<(String, bool)> = Vec::new();
    for id in &ids {
        let r = run_one_task(
            id,
            Arc::clone(&llm),
            AgentBudget {
                max_steps: 30,
                max_tokens: 250_000,
                max_wall_secs: 300,
                max_cost_usd: Some(0.50),
                ..Default::default()
            },
        );
        if r.solved {
            solved += 1;
        }
        eprintln!(
            "[swe] {id}: solved={} terminated_by={:?}",
            r.solved, r.terminated_by
        );
        rows.push((id.clone(), r.solved));
    }
    let total = ids.len();
    let rate = solved as f64 / total as f64;
    eprintln!("[swe] completion_rate={rate:.3} ({solved}/{total}) model={model} target=0.6");
    emit_artifact(&model, rate, solved, total, &rows);
    assert!(
        rate >= min_rate,
        "completion_rate {rate:.3} below floor {min_rate:.3}"
    );
}
