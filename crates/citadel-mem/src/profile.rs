//! One source of truth for a recall recipe. A [`RecallProfile`] bundles the fusion
//! weights, kind guard, graph expansion, recency anchor, and payload filter that shape
//! a recall; consumers pick a preset and [`apply`](RecallProfile::apply) it to a
//! [`RecallQuery`]. `k` stays per-call; reranking stays region-level.

use serde_json::Value as Json;

use crate::types::{FusionWeights, GraphExpand, RecallQuery};

/// Atom kinds that may seed an agent's prompt context. Heterogeneous agent memory
/// also holds trace/audit/self-model atoms, which must never seed a prompt.
pub const NARRATIVE_KINDS: [&str; 3] = ["evidence", "fact", "reflection"];

/// A reusable recall recipe. [`default`](Default::default) is membench's scored
/// LoCoMo recipe, the only quality-measured configuration; presets adapt it.
#[derive(Debug, Clone)]
pub struct RecallProfile {
    pub weights: FusionWeights,
    /// Kind allowlist; empty = all kinds.
    pub kinds: Vec<String>,
    /// Seed graph expansion; `None` = off.
    pub graph_expand: Option<GraphExpand>,
    /// Recency reference clock (micros); `None` = wall clock.
    pub as_of_micros: Option<i64>,
    /// JSONB containment filter; `None` = none.
    pub payload_filter: Option<Json>,
}

impl Default for RecallProfile {
    /// The scored recipe: default fusion weights, no kind guard, no graph, wall-clock
    /// recency. Byte-identical to [`RecallQuery::by_text`] defaults.
    fn default() -> Self {
        Self {
            weights: FusionWeights::default(),
            kinds: Vec::new(),
            graph_expand: None,
            as_of_micros: None,
            payload_filter: None,
        }
    }
}

impl RecallProfile {
    /// The scored recipe adapted for an agent: recency disabled (replay stability) and
    /// restricted to narrative kinds (so trace/audit atoms never seed a prompt).
    pub fn agent_context() -> Self {
        Self {
            weights: FusionWeights {
                recency: 0.0,
                ..FusionWeights::default()
            },
            kinds: NARRATIVE_KINDS.iter().map(|k| k.to_string()).collect(),
            ..Self::default()
        }
    }

    /// Pure vector similarity for immutable reference corpora.
    pub fn semantic_only() -> Self {
        Self {
            weights: FusionWeights::semantic_only(),
            ..Self::default()
        }
    }

    /// Write the recipe onto a query: weights always, the optional fields only when the
    /// profile sets them (so it augments a query without clearing caller-set fields).
    pub fn apply(&self, mut q: RecallQuery) -> RecallQuery {
        q.weights = self.weights;
        if !self.kinds.is_empty() {
            q.kinds = self.kinds.clone();
        }
        if let Some(expand) = &self.graph_expand {
            q.graph_expand = Some(expand.clone());
        }
        if let Some(as_of) = self.as_of_micros {
            q.as_of_micros = Some(as_of);
        }
        if let Some(filter) = &self.payload_filter {
            q.payload_filter = Some(filter.clone());
        }
        q
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matches_by_text_recipe() {
        let p = RecallProfile::default();
        let q = RecallQuery::by_text("x", 7);
        assert_eq!(p.weights.semantic, q.weights.semantic);
        assert_eq!(p.weights.keyword, q.weights.keyword);
        assert_eq!(p.weights.recency, q.weights.recency);
        assert_eq!(p.weights.importance, q.weights.importance);
        assert!(p.kinds.is_empty());
        assert!(p.graph_expand.is_none());
        assert!(p.as_of_micros.is_none());
        assert!(p.payload_filter.is_none());
    }

    #[test]
    fn agent_context_zeroes_recency_and_guards_kinds() {
        let p = RecallProfile::agent_context();
        assert_eq!(p.weights.recency, 0.0);
        assert_eq!(p.weights.semantic, FusionWeights::default().semantic);
        assert_eq!(p.weights.keyword, FusionWeights::default().keyword);
        assert_eq!(p.weights.importance, FusionWeights::default().importance);
        assert_eq!(p.kinds, NARRATIVE_KINDS.map(String::from).to_vec());
    }

    #[test]
    fn apply_preserves_k_and_sets_recipe() {
        let q = RecallProfile::agent_context().apply(RecallQuery::by_text("x", 11));
        assert_eq!(q.k, 11);
        assert_eq!(q.weights.recency, 0.0);
        assert_eq!(q.kinds, NARRATIVE_KINDS.map(String::from).to_vec());
    }

    #[test]
    fn apply_is_additive_on_optional_fields() {
        // default() sets no optionals, so apply must not clear a caller-set graph_expand.
        let q = RecallQuery::by_text("x", 3).with_graph_expand(GraphExpand::new(2, Vec::new()));
        let out = RecallProfile::default().apply(q);
        assert!(
            out.graph_expand.is_some(),
            "must not clear caller graph_expand"
        );
        assert!(out.kinds.is_empty());
    }
}
