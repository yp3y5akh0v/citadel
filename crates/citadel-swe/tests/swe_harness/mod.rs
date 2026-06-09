//! Shared rig for the mini-SWE benchmark: clone a task fixture, run a real agent
//! over file/command tools sandboxed to the clone, then INDEPENDENTLY score by
//! restoring pristine tests + injecting the hidden test + re-running `cargo test`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use citadel::{Argon2Profile, DatabaseBuilder};
#[cfg(feature = "live")]
use citadel_ai::AgentReport;
use citadel_ai::{
    Agent, AgentBudget, AgentConfig, BeliefGraph, CheckerAttestation, CompletionResponse,
    FileReadTool, FileWriteTool, LLMClient, ListDirTool, RunCommandTool, ToolCall, ToolRegistry,
    Verifier, VerifyError, VerifyKind, VerifyOutcome, VerifyRequest,
};
use citadel_mem::{Embedder, MemoryEngine, MockEmbedder};

/// The checked-in task fixtures (stable regardless of the current directory).
pub fn tasks_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("swe-tasks")
}

/// All task ids (one subdirectory per task), sorted for a stable run order.
pub fn all_task_ids() -> Vec<String> {
    let mut ids: Vec<String> = fs::read_dir(tasks_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    ids.sort();
    ids
}

pub struct TaskManifest {
    pub symptom: String,
    pub fail_to_pass: Vec<String>,
    pub pass_to_pass: Vec<String>,
}

pub fn load_manifest(id: &str) -> TaskManifest {
    let raw = fs::read_to_string(tasks_dir().join(id).join("task.json")).unwrap();
    let v: Value = serde_json::from_str(&raw).unwrap();
    let list = |k: &str| {
        v[k].as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    };
    TaskManifest {
        symptom: v["symptom"].as_str().unwrap().to_string(),
        fail_to_pass: list("fail_to_pass"),
        pass_to_pass: list("pass_to_pass"),
    }
}

/// A cloned task crate in a tempdir; `root` is its canonical path.
pub struct Scratch {
    /// Owns the tempdir for the clone's lifetime (RAII cleanup on drop).
    #[allow(dead_code)]
    dir: tempfile::TempDir,
    pub root: PathBuf,
}

/// Clone the agent-visible parts of a task into a fresh tempdir. The hidden test
/// and the gold fix are NOT copied, so the agent never sees them.
pub fn clone_fixture(id: &str) -> Scratch {
    let src = tasks_dir().join(id);
    let dir = tempfile::tempdir().unwrap();
    let dst = dir.path();
    fs::copy(src.join("Cargo.toml"), dst.join("Cargo.toml")).unwrap();
    copy_tree(&src.join("src"), &dst.join("src"));
    fs::create_dir_all(dst.join("tests")).unwrap();
    for t in ["visible.rs", "pass_to_pass.rs"] {
        fs::copy(src.join("tests").join(t), dst.join("tests").join(t)).unwrap();
    }
    let root = fs::canonicalize(dst).unwrap();
    Scratch { dir, root }
}

fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = to.join(entry.file_name());
        if path.is_dir() {
            copy_tree(&path, &target);
        } else {
            fs::copy(&path, &target).unwrap();
        }
    }
}

/// The task prompt: the symptom (never the fix) + the tool contract.
pub fn build_prompt(scratch: &Scratch, m: &TaskManifest) -> String {
    // Forward-slash path (strip the `\\?\` prefix): the canonicalized Windows form is
    // backslash-hostile - the model echoes the backslashes unescaped and breaks its JSON.
    let raw = scratch.root.to_string_lossy();
    let root = raw.strip_prefix(r"\\?\").unwrap_or(&raw).replace('\\', "/");
    format!(
        "The Rust crate at {root} has a failing test. Run cargo test to see it. {symptom} \
         Find and fix the bug in the source under src/. Use list_dir to see what files exist \
         (omit the path or pass \".\" for the crate root), file_read to inspect files (absolute \
         paths under {root}, or paths relative to it), file_write to edit files under {root}/src, \
         and run_command with program \"cargo\" and args [\"test\"] to check your fix. Do not \
         modify any test file or Cargo.toml. When the bug is fixed, reply with a short summary.",
        root = root,
        symptom = m.symptom,
    )
}

/// Test binaries the agent must see green: the visible FAIL_TO_PASS test(s) plus
/// the PASS_TO_PASS guard (the hidden test is never in the clone).
fn required_visible_tests(scratch: &Scratch, m: &TaskManifest) -> Vec<String> {
    let mut names: Vec<String> = m
        .fail_to_pass
        .iter()
        .filter(|t| scratch.root.join("tests").join(format!("{t}.rs")).exists())
        .cloned()
        .collect();
    for t in &m.pass_to_pass {
        if !names.contains(t) {
            names.push(t.clone());
        }
    }
    names
}

/// Recall embedder for an A/B arm: Mock when `CITADEL_SWE_SEMANTIC_RECALL` is
/// unset (recall off), else a REAL bge-small (needs `--features candle-embed` +
/// `CITADEL_BGE_SMALL_DIR`). No silent fallback - a missing one is a hard error.
fn make_embedder(semantic: bool) -> Arc<dyn Embedder> {
    if !semantic {
        return Arc::new(MockEmbedder::new(64));
    }
    #[cfg(feature = "candle-embed")]
    {
        let dir = std::env::var("CITADEL_BGE_SMALL_DIR").expect(
            "CITADEL_SWE_SEMANTIC_RECALL=1 requires CITADEL_BGE_SMALL_DIR \
             pointing at a local bge-small-en-v1.5 model directory",
        );
        let emb = citadel_mem::CandleEmbedder::bge_small(&dir)
            .unwrap_or_else(|e| panic!("failed to load bge-small from {dir}: {e}"));
        Arc::new(emb)
    }
    #[cfg(not(feature = "candle-embed"))]
    {
        panic!(
            "CITADEL_SWE_SEMANTIC_RECALL is set but this binary was built without \
             --features candle-embed; rebuild with it to enable semantic recall"
        );
    }
}

/// Wire a real agent over the tools, sandboxed to `scratch`: read the clone, write
/// only src/, run only `cargo`. Memory is its own tempdir.
pub fn build_agent(
    scratch: &Scratch,
    m: &TaskManifest,
    llm: Arc<dyn LLMClient>,
    budget: AgentBudget,
) -> (tempfile::TempDir, Agent) {
    let memdir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(memdir.path().join("mem.db"))
        .passphrase(b"swe-bench")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    // A/B knob: semantic recall ON (real embedder) vs OFF (deterministic baseline).
    let semantic = std::env::var("CITADEL_SWE_SEMANTIC_RECALL").is_ok();
    let eng = Arc::new(MemoryEngine::open(Arc::new(db)).unwrap());
    eng.create_region("agent", make_embedder(semantic)).unwrap();
    let graph = BeliefGraph::new(eng, "agent");

    let mut tools = ToolRegistry::new();
    tools.register(Box::new(FileReadTool::new([scratch.root.clone()]).unwrap()));
    tools.register(Box::new(ListDirTool::new([scratch.root.clone()]).unwrap()));
    tools.register(Box::new(
        FileWriteTool::new([scratch.root.join("src")]).unwrap(),
    ));
    tools.register(Box::new(
        RunCommandTool::new(
            ["cargo".to_string()],
            scratch.root.clone(),
            Duration::from_secs(90),
        )
        .unwrap(),
    ));

    let mut config = AgentConfig {
        verifier: Some(Arc::new(SweTestVerifier::new(required_visible_tests(
            scratch, m,
        )))),
        ..AgentConfig::default()
    };
    // OFF arm disables recall entirely (the baseline); ON keeps the default depth.
    if !semantic {
        config.recall_context_k = 0;
    }
    let agent = Agent::new(llm, graph, tools, budget, config);
    (memdir, agent)
}

/// Acceptance checker: certifies the agent's honest belief from its own green `cargo
/// test`, never the hidden test, so the independent scorer stays the truth for `solved`.
/// Fail-closed; only Acceptance is judged.
pub struct SweTestVerifier {
    required: Vec<String>,
}

impl SweTestVerifier {
    pub fn new(required: Vec<String>) -> Self {
        Self { required }
    }

    /// `true` iff `content` is a `run_command` JSON for a `cargo test` that exited 0,
    /// did not time out, and ran every required test binary.
    fn run_is_green(&self, content: &str) -> bool {
        let Ok(v) = serde_json::from_str::<Value>(content) else {
            return false; // a "tool error: ..." string, not the command JSON
        };
        if v.get("exit_code").and_then(Value::as_i64) != Some(0) {
            return false;
        }
        if v.get("timed_out").and_then(Value::as_bool) == Some(true) {
            return false;
        }
        let combined = format!(
            "{}\n{}",
            v.get("stdout").and_then(Value::as_str).unwrap_or(""),
            v.get("stderr").and_then(Value::as_str).unwrap_or(""),
        );
        self.required
            .iter()
            .all(|name| combined.contains(&format!("{name}.rs")))
    }
}

impl Verifier for SweTestVerifier {
    fn verify(&self, req: &VerifyRequest<'_>) -> Result<VerifyOutcome, VerifyError> {
        if req.kind != VerifyKind::Acceptance {
            return Ok(VerifyOutcome {
                satisfied: true,
                reason: "non-acceptance check is not this checker's job".into(),
            });
        }
        let last_cmd = req
            .evidence
            .iter()
            .rev()
            .find(|(source, _)| source == "run_command");
        let satisfied = matches!(last_cmd, Some((_, content)) if self.run_is_green(content));
        Ok(VerifyOutcome {
            satisfied,
            reason: if satisfied {
                format!("agent's last cargo test ran {:?} green", self.required)
            } else {
                "agent's last command was not a green cargo test over all required tests".into()
            },
        })
    }

    fn attestation(&self) -> Option<CheckerAttestation> {
        Some(CheckerAttestation::new("swe-cargo-test", "1"))
    }
}

/// Overwrite the (possibly tampered) visible/pass_to_pass tests with pristine
/// bytes and inject the hidden test, so scoring uses the original harness.
pub fn restore_and_inject_tests(scratch: &Scratch, id: &str) {
    let src = tasks_dir().join(id);
    let dst = scratch.root.join("tests");
    for t in ["visible.rs", "pass_to_pass.rs"] {
        fs::copy(src.join("tests").join(t), dst.join(t)).unwrap();
    }
    fs::copy(src.join("hidden").join("hidden.rs"), dst.join("hidden.rs")).unwrap();
}

/// Independently grade: `true` iff every FAIL_TO_PASS and PASS_TO_PASS binary is green.
pub fn score_task(scratch: &Scratch, m: &TaskManifest) -> bool {
    m.fail_to_pass
        .iter()
        .all(|t| cargo_test_binary(&scratch.root, t))
        && m.pass_to_pass
            .iter()
            .all(|t| cargo_test_binary(&scratch.root, t))
}

fn cargo_test_binary(root: &Path, name: &str) -> bool {
    Command::new("cargo")
        .args(["test", "--test", name])
        .current_dir(root)
        // Isolated target dir (not the outer run's), reusing the agent's build.
        .env_remove("CARGO_TARGET_DIR")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Oracle self-test: the gold fix scores SOLVED and the pristine buggy crate does
/// NOT, proving the task is solvable and the tests genuinely catch the bug.
pub fn gold_patch_selftest(id: &str) {
    let m = load_manifest(id);

    let gold = clone_fixture(id);
    fs::copy(
        tasks_dir().join(id).join("gold").join("lib.rs"),
        gold.root.join("src").join("lib.rs"),
    )
    .unwrap();
    restore_and_inject_tests(&gold, id);
    assert!(score_task(&gold, &m), "gold must solve {id}");

    // Pristine buggy: a FAIL_TO_PASS must fail (bug is real) and every PASS_TO_PASS
    // stays green (a valid regression guard).
    let buggy = clone_fixture(id);
    restore_and_inject_tests(&buggy, id);
    let f2p = m
        .fail_to_pass
        .iter()
        .all(|t| cargo_test_binary(&buggy.root, t));
    let p2p = m
        .pass_to_pass
        .iter()
        .all(|t| cargo_test_binary(&buggy.root, t));
    assert!(!f2p, "pristine buggy {id} must fail a fail_to_pass test");
    assert!(
        p2p,
        "pristine buggy {id} must keep pass_to_pass green (valid regression guard)"
    );
}

pub fn plan_response(tasks: &[&str]) -> CompletionResponse {
    plan_response_with_criteria(&[], tasks)
}

/// Like [`plan_response`] but with acceptance criteria, so converge exercises the verifier.
pub fn plan_response_with_criteria(criteria: &[&str], tasks: &[&str]) -> CompletionResponse {
    CompletionResponse::tool_calls(vec![ToolCall {
        id: "plan".into(),
        name: "submit_plan".into(),
        arguments: json!({
            "goal": {
                "prompt": "fix the bug",
                "acceptance_criteria": criteria,
                "constraints": [],
            },
            "tasks": tasks.iter().map(|d| json!({ "description": d, "deps": [] })).collect::<Vec<_>>(),
        }),
    }])
}

pub fn tool_call(name: &str, args: Value) -> CompletionResponse {
    CompletionResponse::tool_calls(vec![ToolCall {
        id: format!("{name}-1"),
        name: name.into(),
        arguments: args,
    }])
}

#[cfg(feature = "live")]
pub struct TaskResult {
    pub solved: bool,
    pub terminated_by: citadel_ai::TerminatedBy,
}

/// Post-run snapshot of the agent's citadel-mem state for diagnosing stalls.
/// Written to `<dir>/<id>.trace.txt` when CITADEL_SWE_TRACE_DIR is set.
#[cfg(feature = "live")]
fn dump_trajectory(
    agent: &Agent,
    report: Option<&AgentReport>,
    required: &[String],
    id: &str,
    dir: &Path,
) {
    use std::fmt::Write as _;
    let g = agent.graph();
    let goal_id = report.and_then(|r| r.goal_id);
    let mut s = String::new();
    let _ = writeln!(s, "=== {id} ===");
    match report {
        Some(r) => {
            let _ = writeln!(
                s,
                "TERMINATED_BY={:?}  CHAIN_VALID={}  TASKS_DONE={}",
                r.terminated_by, r.chain_valid, r.tasks_done
            );
        }
        None => {
            let _ = writeln!(s, "TERMINATED_BY=<run returned Err>");
        }
    }
    if let Some(gid) = goal_id {
        if let Ok(Some(goal)) = g.get_goal(gid) {
            let _ = writeln!(s, "ACCEPTANCE_CRITERIA={:?}", goal.acceptance_criteria);
        }
        let _ = writeln!(s, "GOAL_STATUS={:?}", g.get_goal_status(gid).ok().flatten());
    }
    if let Ok(tasks) = g.tasks() {
        for (tid, t) in tasks {
            let _ = writeln!(
                s,
                "TASK {tid} status={:?} attempts={} last_error={:?}",
                t.status, t.attempts, t.last_error
            );
        }
    }
    let mut converge = String::from("absent");
    if let Ok(trail) = g.export_audit_trail() {
        for c in &trail {
            let _ = writeln!(
                s,
                "AUDIT {} prov={} met={} verdict={:?}",
                c.action_id, c.has_provenance, c.constraints_satisfied, c.verdict
            );
            if c.action_id.starts_with("converge_goal_") {
                converge = format!("present met={}", c.constraints_satisfied);
            }
        }
    }
    let _ = writeln!(s, "CONVERGE_ATOM={converge}");
    if let Some(gid) = goal_id {
        if let Ok(ev) = g.evidence_for_goal(gid) {
            for (i, (src, content)) in ev.iter().enumerate() {
                let snip = content
                    .chars()
                    .take(160)
                    .collect::<String>()
                    .replace('\n', " ");
                let _ = writeln!(s, "EVIDENCE[{i}] {src}: {snip}");
            }
            let checker = SweTestVerifier::new(required.to_vec());
            let last_green = ev
                .iter()
                .rev()
                .find(|(src, _)| src == "run_command")
                .map(|(_, c)| checker.run_is_green(c))
                .unwrap_or(false);
            let _ = writeln!(s, "LAST_RUN_COMMAND green={last_green}");
        }
    }
    let _ = writeln!(
        s,
        "LLM_TRACES={}",
        g.load_llm_traces().map(|t| t.len()).unwrap_or(0)
    );
    let _ = fs::create_dir_all(dir);
    let _ = fs::write(dir.join(format!("{id}.trace.txt")), &s);
    eprint!("{s}");
}

/// Run one task end to end with a real backend, then independently score it.
#[cfg(feature = "live")]
pub fn run_one_task(id: &str, llm: Arc<dyn LLMClient>, budget: AgentBudget) -> TaskResult {
    let m = load_manifest(id);
    let scratch = clone_fixture(id);
    let (_memdir, agent) = build_agent(&scratch, &m, llm, budget);
    let run = agent.run(build_prompt(&scratch, &m));
    if let Ok(dir) = std::env::var("CITADEL_SWE_TRACE_DIR") {
        let required = required_visible_tests(&scratch, &m);
        dump_trajectory(&agent, run.as_ref().ok(), &required, id, Path::new(&dir));
    }
    restore_and_inject_tests(&scratch, id);
    let scored = score_task(&scratch, &m);
    let (terminated_by, chain_valid, ok) = match &run {
        Ok(r) => (r.terminated_by, r.chain_valid, true),
        Err(_) => (citadel_ai::TerminatedBy::Incomplete, false, false),
    };
    TaskResult {
        solved: scored && chain_valid && ok,
        terminated_by,
    }
}

#[cfg(feature = "live")]
pub fn emit_artifact(model: &str, rate: f64, solved: usize, total: usize, rows: &[(String, bool)]) {
    let tasks: Vec<Value> = rows
        .iter()
        .map(|(id, s)| json!({ "id": id, "solved": s }))
        .collect();
    let doc = json!({
        "model": model,
        "completion_rate": rate,
        "solved": solved,
        "total": total,
        "target": 0.6,
        "tasks": tasks,
    });
    let path =
        std::env::var("CITADEL_SWE_OUT").unwrap_or_else(|_| "swe-bench-result.json".to_string());
    let _ = fs::write(path, serde_json::to_string_pretty(&doc).unwrap());
}
