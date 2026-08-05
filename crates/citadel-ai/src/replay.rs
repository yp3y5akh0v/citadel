//! Record-replay [`LLMClient`] seeded from a graph's recorded traces.

use std::sync::Arc;

use citadel_llm::LLMClient;

use crate::graph::{BeliefGraph, GraphError};

/// A record-replay client seeded from a graph's recorded traces (real model id, so
/// request hashes match). [`Replay::misses`] is 0 on a faithful replay.
pub fn replay_from_graph(graph: &BeliefGraph) -> Result<Replay, GraphError> {
    let inner = Arc::new(crate::agent::ReplayClient::from_graph(graph)?);
    let client: Arc<dyn LLMClient> = inner.clone();
    Ok(Replay { client, inner })
}

/// A replay client plus its miss counter (a request with no recorded response
/// bumps the count and errors).
pub struct Replay {
    client: Arc<dyn LLMClient>,
    inner: Arc<crate::agent::ReplayClient>,
}

impl Replay {
    pub fn client(&self) -> Arc<dyn LLMClient> {
        Arc::clone(&self.client)
    }

    /// Requests with no recorded response (0 on a clean replay).
    pub fn misses(&self) -> u32 {
        self.inner.misses()
    }
}
