//! Versioned, overridable prompt library for the cognition loop.
//!
//! Each call site resolves its system prompt through a [`PromptLibrary`] rather
//! than a hardcoded const. Shipped defaults are human-curated and exhaustive over
//! [`PromptId`]; an operator overrides them programmatically or at runtime via
//! `prompt` atoms in a region - no recompile, never agent-authored.

use rustc_hash::FxHashMap;

use citadel_mem::{MemError, MemoryEngine};
use serde_json::Value;

use crate::llm::Message;

// Curated default system prompts for the cognition-loop call sites.
const PLANNER_DEFAULT: &str =
    "You are a planner. Call submit_plan with the goal (prompt, acceptance criteria, \
     constraints) and the FEWEST subtasks that, once done, satisfy the acceptance \
     criteria. Decompose backward from the desired end state: a single well-scoped \
     change is one subtask, and you add another only when it changes the end state or \
     a later subtask depends on it. Give each subtask its dependency indices. Do not \
     add bookkeeping subtasks (documentation, cleanup, or re-verification) that do not \
     change the outcome - acceptance is verified separately.";
const EXECUTE_DEFAULT: &str =
    "You are executing one subtask. Call tools as needed, or reply with the final result text.";
const REFLECT_DEFAULT: &str =
    "You are a critic. Reflect briefly on what is blocking progress and how the plan should adapt.";
const CONSTRAINT_CRITIC_DEFAULT: &str =
    "Judge whether the dispatched tools comply with the goal's constraints. Call verdict.";
const ACCEPTANCE_CRITIC_DEFAULT: &str =
    "You are a strict acceptance critic. Decide whether the gathered evidence shows the \
     goal's acceptance criteria are met, then call verdict with satisfied set accordingly. \
     When the goal is verified by running tests, treat the criteria as met only if the \
     agent's most recent test run reports success (for cargo: exit code 0 and a \
     \"test result: ok\" line with 0 failed). If the evidence is missing, stale, or shows \
     any failure, set satisfied false.";
const PROPOSER_DEFAULT: &str =
    "You are a discovery proposal operator. Given a goal and the best-known artifacts, propose new \
     candidate artifacts that may score higher. Call the propose tool once per candidate. If you \
     cannot call a tool, reply with ONLY a JSON array of artifacts and no prose or code fences; \
     each artifact is a complete solution object an external checker can validate.";

/// Atom kind used for operator prompt overrides in a region.
const PROMPT_KIND: &str = "prompt";
/// Upper bound on prompt atoms scanned by [`PromptLibrary::from_region`].
const MAX_PROMPT_ATOMS: usize = 1024;

/// The closed set of cognition-loop prompts. Adding a variant forces a curated
/// default at compile time (the `shipped_default` match is wildcard-free).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PromptId {
    Planner,
    Execute,
    Reflect,
    ConstraintCritic,
    AcceptanceCritic,
    /// System prompt for the discovery proposal operator.
    Proposer,
}

impl PromptId {
    pub const ALL: [PromptId; 6] = [
        PromptId::Planner,
        PromptId::Execute,
        PromptId::Reflect,
        PromptId::ConstraintCritic,
        PromptId::AcceptanceCritic,
        PromptId::Proposer,
    ];

    /// Stable slug: the override-lookup key and audit name (not the Debug form).
    pub fn as_str(self) -> &'static str {
        match self {
            PromptId::Planner => "planner",
            PromptId::Execute => "execute",
            PromptId::Reflect => "reflect",
            PromptId::ConstraintCritic => "constraint_critic",
            PromptId::AcceptanceCritic => "acceptance_critic",
            PromptId::Proposer => "proposer",
        }
    }

    /// Parse a slug back to a `PromptId` (inverse of [`PromptId::as_str`]).
    pub fn from_name(s: &str) -> Option<Self> {
        Some(match s {
            "planner" => PromptId::Planner,
            "execute" => PromptId::Execute,
            "reflect" => PromptId::Reflect,
            "constraint_critic" => PromptId::ConstraintCritic,
            "acceptance_critic" => PromptId::AcceptanceCritic,
            "proposer" => PromptId::Proposer,
            _ => return None,
        })
    }
}

/// A versioned prompt body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prompt {
    pub version: u32,
    pub text: String,
}

/// Whether a resolved prompt came from the shipped default or an override.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptSource {
    ShippedDefault,
    Override,
}

impl PromptSource {
    pub fn as_str(self) -> &'static str {
        match self {
            PromptSource::ShippedDefault => "shipped_default",
            PromptSource::Override => "override",
        }
    }
}

/// A prompt resolved for one call site, with provenance for the audit trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPrompt {
    pub id: PromptId,
    pub version: u32,
    pub text: String,
    /// BLAKE3 of `text` (hex): proves which exact prompt body produced an output.
    pub hash: String,
    pub source: PromptSource,
}

impl ResolvedPrompt {
    /// The system message to prepend to a request.
    pub fn as_system(&self) -> Message {
        Message::system(self.text.clone())
    }
}

/// Curated default for an id; exhaustive (no wildcard) so a new variant can't ship undocumented.
fn shipped_default(id: PromptId) -> Prompt {
    let text = match id {
        PromptId::Planner => PLANNER_DEFAULT,
        PromptId::Execute => EXECUTE_DEFAULT,
        PromptId::Reflect => REFLECT_DEFAULT,
        PromptId::ConstraintCritic => CONSTRAINT_CRITIC_DEFAULT,
        PromptId::AcceptanceCritic => ACCEPTANCE_CRITIC_DEFAULT,
        PromptId::Proposer => PROPOSER_DEFAULT,
    };
    Prompt {
        version: 1,
        text: text.to_string(),
    }
}

/// Resolves each call site's prompt: an override if present, else the curated
/// default (the deliberate baseline). Holds only overrides; defaults stay compiled-in.
#[derive(Debug, Clone, Default)]
pub struct PromptLibrary {
    overrides: FxHashMap<PromptId, Prompt>,
}

impl PromptLibrary {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set an override (builder form). `version` is operator-owned; bump it past
    /// the default's so audit history is monotonic.
    #[must_use]
    pub fn with_override(mut self, id: PromptId, version: u32, text: impl Into<String>) -> Self {
        self.set(id, version, text);
        self
    }

    /// Set an override in place.
    pub fn set(&mut self, id: PromptId, version: u32, text: impl Into<String>) {
        debug_assert!(
            version >= shipped_default(id).version,
            "override version should not regress below the shipped default"
        );
        self.overrides.insert(
            id,
            Prompt {
                version,
                text: text.into(),
            },
        );
    }

    /// Load overrides from `prompt`-kind atoms in `region` (text = body, payload =
    /// `{name, version}`). Unknown names are ignored; for a repeated name the
    /// highest version wins (newest on a tie).
    pub fn from_region(mem: &MemoryEngine, region: &str) -> Result<Self, MemError> {
        let mut lib = Self::new();
        for hit in mem.fetch(region, PROMPT_KIND, None, MAX_PROMPT_ATOMS)? {
            let Some(id) = hit
                .payload
                .get("name")
                .and_then(Value::as_str)
                .and_then(PromptId::from_name)
            else {
                continue;
            };
            let version = hit
                .payload
                .get("version")
                .and_then(Value::as_u64)
                .unwrap_or(0) as u32;
            let replace = match lib.overrides.get(&id) {
                Some(existing) => version >= existing.version,
                None => true,
            };
            if replace {
                lib.overrides.insert(
                    id,
                    Prompt {
                        version,
                        text: hit.text,
                    },
                );
            }
        }
        Ok(lib)
    }

    /// Resolve a prompt: override wins, else the curated default. Total over
    /// `PromptId` - never errors.
    pub fn resolve(&self, id: PromptId) -> ResolvedPrompt {
        let (prompt, source) = match self.overrides.get(&id) {
            Some(p) => (p.clone(), PromptSource::Override),
            None => (shipped_default(id), PromptSource::ShippedDefault),
        };
        let hash = blake3::hash(prompt.text.as_bytes()).to_hex().to_string();
        ResolvedPrompt {
            id,
            version: prompt.version,
            text: prompt.text,
            hash,
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel::{Argon2Profile, DatabaseBuilder};
    use citadel_mem::{AtomInput, MockEmbedder};
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn defaults_are_total_present_and_nonempty() {
        for id in PromptId::ALL {
            let r = PromptLibrary::default().resolve(id);
            assert_eq!(r.source, PromptSource::ShippedDefault);
            assert_eq!(r.version, 1);
            assert!(!r.text.is_empty());
            assert_eq!(r.hash.len(), 64, "blake3 hex");
            assert_eq!(r.id, id);
        }
    }

    #[test]
    fn default_texts_match_the_loops_former_consts() {
        // Change-detector: these literals must equal the shipped defaults exactly.
        let lib = PromptLibrary::default();
        assert_eq!(
            lib.resolve(PromptId::Planner).text,
            "You are a planner. Call submit_plan with the goal (prompt, acceptance criteria, constraints) and the FEWEST subtasks that, once done, satisfy the acceptance criteria. Decompose backward from the desired end state: a single well-scoped change is one subtask, and you add another only when it changes the end state or a later subtask depends on it. Give each subtask its dependency indices. Do not add bookkeeping subtasks (documentation, cleanup, or re-verification) that do not change the outcome - acceptance is verified separately."
        );
        assert_eq!(
            lib.resolve(PromptId::Execute).text,
            "You are executing one subtask. Call tools as needed, or reply with the final result text."
        );
        assert_eq!(
            lib.resolve(PromptId::Reflect).text,
            "You are a critic. Reflect briefly on what is blocking progress and how the plan should adapt."
        );
        assert_eq!(
            lib.resolve(PromptId::ConstraintCritic).text,
            "Judge whether the dispatched tools comply with the goal's constraints. Call verdict."
        );
        assert_eq!(
            lib.resolve(PromptId::AcceptanceCritic).text,
            "You are a strict acceptance critic. Decide whether the gathered evidence shows the goal's acceptance criteria are met, then call verdict with satisfied set accordingly. When the goal is verified by running tests, treat the criteria as met only if the agent's most recent test run reports success (for cargo: exit code 0 and a \"test result: ok\" line with 0 failed). If the evidence is missing, stale, or shows any failure, set satisfied false."
        );
    }

    #[test]
    fn override_replaces_only_its_own_id() {
        let lib = PromptLibrary::new().with_override(PromptId::Planner, 7, "custom planner");
        let p = lib.resolve(PromptId::Planner);
        assert_eq!(p.text, "custom planner");
        assert_eq!(p.version, 7);
        assert_eq!(p.source, PromptSource::Override);
        // A non-overridden id still resolves the curated default.
        assert_eq!(
            lib.resolve(PromptId::Execute).source,
            PromptSource::ShippedDefault
        );
    }

    #[test]
    fn hash_is_deterministic_and_text_sensitive() {
        let lib = PromptLibrary::new().with_override(PromptId::Reflect, 2, "abc");
        let h1 = lib.resolve(PromptId::Reflect).hash;
        let h2 = lib.resolve(PromptId::Reflect).hash;
        assert_eq!(h1, h2);
        assert_eq!(h1, blake3::hash(b"abc").to_hex().to_string());
        let other = PromptLibrary::new()
            .with_override(PromptId::Reflect, 2, "abd")
            .resolve(PromptId::Reflect)
            .hash;
        assert_ne!(h1, other);
    }

    fn region() -> (tempfile::TempDir, Arc<MemoryEngine>) {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let eng = Arc::new(MemoryEngine::open(Arc::new(db)).unwrap());
        eng.create_region("prompts", Arc::new(MockEmbedder::new(32)))
            .unwrap();
        (dir, eng)
    }

    #[test]
    fn from_region_loads_overrides_and_takes_highest_version() {
        let (_d, eng) = region();
        eng.remember(
            "prompts",
            AtomInput::new("prompt", "old planner")
                .with_payload(json!({"name": "planner", "version": 2})),
        )
        .unwrap();
        eng.remember(
            "prompts",
            AtomInput::new("prompt", "new planner")
                .with_payload(json!({"name": "planner", "version": 5})),
        )
        .unwrap();
        // Unknown name is ignored, not an error.
        eng.remember(
            "prompts",
            AtomInput::new("prompt", "ignored")
                .with_payload(json!({"name": "not_a_node", "version": 9})),
        )
        .unwrap();

        let lib = PromptLibrary::from_region(&eng, "prompts").unwrap();
        let p = lib.resolve(PromptId::Planner);
        assert_eq!(p.text, "new planner", "highest version wins");
        assert_eq!(p.version, 5);
        assert_eq!(p.source, PromptSource::Override);
        // An id with no stored atom still resolves the default.
        assert_eq!(
            lib.resolve(PromptId::Execute).source,
            PromptSource::ShippedDefault
        );
    }

    #[test]
    fn from_region_empty_yields_all_defaults() {
        let (_d, eng) = region();
        let lib = PromptLibrary::from_region(&eng, "prompts").unwrap();
        for id in PromptId::ALL {
            assert_eq!(lib.resolve(id).source, PromptSource::ShippedDefault);
        }
    }
}
