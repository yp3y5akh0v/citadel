//! Tool abstraction: each [`Tool`] bundles its name, wire definition, and handler;
//! [`Registry`] resolves tools by name.

use rustc_hash::FxHashMap;
use serde_json::Value;

use citadel_mem::MemoryEngine;

use crate::types::{Content, Tool as ToolDef};

/// A tool-level failure, surfaced as `isError: true` (not a JSON-RPC protocol error).
pub(super) enum ToolError {
    InvalidParams(String),
    Failed(String),
}

impl ToolError {
    pub(super) fn message(self) -> String {
        match self {
            ToolError::InvalidParams(m) | ToolError::Failed(m) => m,
        }
    }
}

/// What a tool needs to run: the engine plus the single region this server serves.
pub(super) struct ToolCtx<'a> {
    pub(super) mem: &'a MemoryEngine,
    pub(super) region: &'a str,
}

pub(super) trait Tool: Send + Sync {
    fn name(&self) -> &'static str;
    fn definition(&self) -> ToolDef;
    fn call(&self, ctx: &ToolCtx, args: Value) -> Result<Value, ToolError>;

    /// Optional `resource_link` blocks for a successful result; default none.
    fn links(&self, _ctx: &ToolCtx, _result: &Value) -> Vec<Content> {
        Vec::new()
    }
}

/// Name-indexed tools driving `tools/list` (registration order) and `tools/call`.
pub(super) struct Registry {
    by_name: FxHashMap<&'static str, Box<dyn Tool>>,
    order: Vec<&'static str>,
}

impl Registry {
    pub(super) fn new(tools: Vec<Box<dyn Tool>>) -> Self {
        let order = tools.iter().map(|t| t.name()).collect();
        let by_name = tools.into_iter().map(|t| (t.name(), t)).collect();
        Self { by_name, order }
    }

    pub(super) fn list(&self) -> Vec<ToolDef> {
        self.order
            .iter()
            .map(|n| self.by_name[n].definition())
            .collect()
    }

    pub(super) fn get(&self, name: &str) -> Option<&dyn Tool> {
        self.by_name.get(name).map(|tool| &**tool)
    }
}
