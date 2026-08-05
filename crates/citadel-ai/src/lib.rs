//! Autonomous agent runtime on top of citadel-mem.

pub mod agent;
pub mod budget;
pub mod graph;
pub mod prompts;
pub mod propose;
pub mod replay;
pub mod tools;
pub mod verify;

pub use agent::{
    Agent, AgentConfig, AgentError, AgentReport, AgentResult, CognitionState, DiscoveryGoal,
    DiscoveryReport, ReflectReason, RetryPolicy, TerminatedBy,
};
pub use budget::{AgentBudget, BudgetExceeded, BudgetUsage};
pub use graph::{
    BeliefGraph, ChainReport, CoInstantiationCheck, Evidence, Goal, GoalStatus, GoalStatusRecord,
    GraphError, GraphResult, Hypothesis, Reflection, SelfModel, Task, TaskStatus,
    TraceEvictionPolicy, Verdict, VerifiedExport, VerifiedKind,
};
pub use prompts::{Prompt, PromptId, PromptLibrary, PromptSource, ResolvedPrompt};
pub use propose::{
    parse_artifacts, Candidate, Completer, Elite, LlmProposer, ProposalContext, ProposalOperator,
    ProposeError, RejectedCandidate,
};
pub use replay::{replay_from_graph, Replay};
#[cfg(all(feature = "command-tool", not(target_arch = "wasm32")))]
pub use tools::RunCommandTool;
pub use tools::{
    ExecPolicy, FsPolicy, MemRecallTool, MemRememberTool, NetworkPolicy, Tool, ToolError,
    ToolPermissions, ToolRegistry,
};
#[cfg(all(feature = "file-tools", not(target_arch = "wasm32")))]
pub use tools::{FileReadTool, FileWriteTool, ListDirTool};
pub use verify::{
    CheckerAttestation, ScoredOutcome, Verifier, VerifyError, VerifyKind, VerifyOutcome,
    VerifyRequest,
};
