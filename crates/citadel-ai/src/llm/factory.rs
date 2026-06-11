//! Backend-agnostic [`LLMClient`] construction by provider name: the single door
//! to a client - production backends via [`from_env`] / [`build`], closures via
//! [`from_fn`], the replay double via [`replay_from_graph`], test doubles in
//! [`testing`]. No silent fallback: an unknown, not-compiled, or key-less provider
//! is a hard error, never a mock.

use std::sync::Arc;

use crate::graph::{BeliefGraph, GraphError};
use crate::llm::mock::MockClient;
use crate::llm::LLMClient;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai", feature = "ollama")
))]
use crate::llm::LlmTimeouts;
#[cfg(any(test, feature = "test-util"))]
use crate::llm::{CompletionRequest, CompletionResponse, LlmError, Message};

#[cfg(any(test, feature = "test-util"))]
pub mod testing;

/// Provider names the factory recognizes (whether or not compiled in this build).
const KNOWN_PROVIDERS: &[&str] = &["mock", "claude", "openai", "ollama"];

/// Select an [`LLMClient`] from the environment: `{prefix}_PROVIDER` and
/// `{prefix}_MODEL`, each falling back to the given default. API keys are read
/// per provider (`ANTHROPIC_API_KEY` / `OPENAI_API_KEY`); ollama needs none. An
/// endpoint override is honored per provider (`OPENAI_BASE_URL` / `OLLAMA_BASE_URL`).
/// The HTTP receive deadline comes from `CITADEL_AI_LLM_TIMEOUT_SECS` (seconds,
/// default 120; the send and global deadlines derive from it); HTTP-backend
/// builds can set it programmatically via `from_env_with_timeouts` instead.
pub fn from_env(
    prefix: &str,
    default_provider: &str,
    default_model: &str,
) -> Result<Arc<dyn LLMClient>, String> {
    let (provider, model) = provider_model(prefix, default_provider, default_model);
    build(&provider, &model)
}

/// [`from_env`] with explicit HTTP deadlines instead of
/// `CITADEL_AI_LLM_TIMEOUT_SECS`.
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai", feature = "ollama")
))]
pub fn from_env_with_timeouts(
    prefix: &str,
    default_provider: &str,
    default_model: &str,
    timeouts: LlmTimeouts,
) -> Result<Arc<dyn LLMClient>, String> {
    let (provider, model) = provider_model(prefix, default_provider, default_model);
    build_with_timeouts(&provider, &model, timeouts)
}

/// `{prefix}_PROVIDER` / `{prefix}_MODEL`, each falling back to its default.
fn provider_model(prefix: &str, default_provider: &str, default_model: &str) -> (String, String) {
    let provider = std::env::var(format!("{prefix}_PROVIDER"))
        .unwrap_or_else(|_| default_provider.to_string());
    let model =
        std::env::var(format!("{prefix}_MODEL")).unwrap_or_else(|_| default_model.to_string());
    (provider, model)
}

/// Build an HTTP-backed client for `provider` + `model`, with HTTP deadlines
/// from `CITADEL_AI_LLM_TIMEOUT_SECS`; any other name defers to [`fallback`].
/// Compiled only when at least one HTTP backend is enabled.
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai", feature = "ollama")
))]
pub fn build(provider: &str, model: &str) -> Result<Arc<dyn LLMClient>, String> {
    build_with_timeouts(provider, model, timeouts_from_env())
}

/// [`build`] with explicit HTTP deadlines.
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai", feature = "ollama")
))]
pub fn build_with_timeouts(
    provider: &str,
    model: &str,
    timeouts: LlmTimeouts,
) -> Result<Arc<dyn LLMClient>, String> {
    match provider {
        #[cfg(feature = "claude")]
        "claude" => {
            let key = require_key("ANTHROPIC_API_KEY", "claude")?;
            let client = crate::llm::claude::ClaudeClient::new(model, key);
            Ok(Arc::new(client.with_timeouts(timeouts)))
        }
        #[cfg(feature = "openai")]
        "openai" => {
            let key = require_key("OPENAI_API_KEY", "openai")?;
            let client = match std::env::var("OPENAI_BASE_URL") {
                Ok(base) => crate::llm::openai::OpenAiClient::with_base_url(model, base, key),
                Err(_) => crate::llm::openai::OpenAiClient::new(model, key),
            };
            Ok(Arc::new(client.with_timeouts(timeouts)))
        }
        #[cfg(feature = "ollama")]
        "ollama" => {
            let client = match std::env::var("OLLAMA_BASE_URL") {
                Ok(base) => crate::llm::ollama::OllamaClient::with_base_url(model, base),
                Err(_) => crate::llm::ollama::OllamaClient::new(model),
            };
            Ok(Arc::new(client.with_timeouts(timeouts)))
        }
        other => fallback(other),
    }
}

/// `CITADEL_AI_LLM_TIMEOUT_SECS` parsed into deadlines; env config belongs to
/// the factory, the explicit `*_with_timeouts` paths bypass it.
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai", feature = "ollama")
))]
fn timeouts_from_env() -> LlmTimeouts {
    parse_timeouts(std::env::var("CITADEL_AI_LLM_TIMEOUT_SECS").ok().as_deref())
}

/// Pure half of [`timeouts_from_env`]: a numeric value sets the receive
/// budget; unset or malformed keeps the default.
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai", feature = "ollama")
))]
fn parse_timeouts(value: Option<&str>) -> LlmTimeouts {
    value
        .and_then(|v| v.parse().ok())
        .map(|recv_secs| LlmTimeouts { recv_secs })
        .unwrap_or_default()
}

/// Mock-only build (wasm, or no HTTP backend enabled): every non-mock provider is
/// a hard error.
#[cfg(not(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai", feature = "ollama")
)))]
pub fn build(provider: &str, _: &str) -> Result<Arc<dyn LLMClient>, String> {
    fallback(provider)
}

/// The non-HTTP outcomes: the always-available mock, a hard error for a
/// recognized-but-not-compiled provider, or an unknown name.
fn fallback(provider: &str) -> Result<Arc<dyn LLMClient>, String> {
    match provider {
        "mock" => Ok(Arc::new(MockClient::replying("mock"))),
        p if KNOWN_PROVIDERS.contains(&p) => Err(not_compiled(p)),
        p => Err(unknown_provider(p)),
    }
}

#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai")
))]
fn require_key(env_name: &str, provider: &str) -> Result<String, String> {
    std::env::var(env_name)
        .map_err(|_| format!("llm provider '{provider}' requires {env_name} to be set"))
}

fn not_compiled(provider: &str) -> String {
    format!(
        "llm provider '{provider}' needs a build with --features {provider}; \
         this build has only the compiled-in providers"
    )
}

fn unknown_provider(provider: &str) -> String {
    format!("unknown llm provider '{provider}' (valid: mock, claude, openai, ollama)")
}

/// How a closure-built client counts tokens.
#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Copy)]
pub enum TokenCount {
    /// ~`n` chars per token, never below the message count (the mock estimate).
    CharsPerToken(usize),
    /// A flat `n` per message.
    PerMessage(usize),
    /// A fixed total regardless of input.
    Constant(usize),
    /// Always 0.
    Zero,
}

#[cfg(any(test, feature = "test-util"))]
impl TokenCount {
    fn count(&self, messages: &[Message]) -> usize {
        match *self {
            TokenCount::CharsPerToken(n) => {
                let chars: usize = messages.iter().map(crate::llm::mock::message_chars).sum();
                (chars / n.max(1)).max(messages.len())
            }
            TokenCount::PerMessage(n) => messages.len() * n,
            TokenCount::Constant(n) => n,
            TokenCount::Zero => 0,
        }
    }
}

/// An [`LLMClient`] whose behavior is a closure.
#[cfg(any(test, feature = "test-util"))]
struct FnClient<F> {
    model_id: String,
    tokens: TokenCount,
    complete: F,
}

#[cfg(any(test, feature = "test-util"))]
impl<F> LLMClient for FnClient<F>
where
    F: Fn(&CompletionRequest) -> Result<CompletionResponse, LlmError> + Send + Sync + 'static,
{
    fn complete(&self, req: &CompletionRequest) -> Result<CompletionResponse, LlmError> {
        (self.complete)(req)
    }

    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn count_tokens(&self, messages: &[Message]) -> usize {
        self.tokens.count(messages)
    }
}

/// Build a client from a closure, with a default ~4-chars/token estimate.
#[cfg(any(test, feature = "test-util"))]
pub fn from_fn<F>(model_id: impl Into<String>, complete: F) -> Arc<dyn LLMClient>
where
    F: Fn(&CompletionRequest) -> Result<CompletionResponse, LlmError> + Send + Sync + 'static,
{
    from_fn_with(model_id, TokenCount::CharsPerToken(4), complete)
}

/// Build a client from a closure with an explicit token-count strategy.
#[cfg(any(test, feature = "test-util"))]
pub fn from_fn_with<F>(
    model_id: impl Into<String>,
    tokens: TokenCount,
    complete: F,
) -> Arc<dyn LLMClient>
where
    F: Fn(&CompletionRequest) -> Result<CompletionResponse, LlmError> + Send + Sync + 'static,
{
    Arc::new(FnClient {
        model_id: model_id.into(),
        tokens,
        complete,
    })
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_is_always_available() {
        let llm = from_env("CITADEL_FACTORY_TEST_UNSET", "mock", "ignored").unwrap();
        assert_eq!(llm.model_id(), "mock");
    }

    #[test]
    fn unknown_provider_is_an_error() {
        let Err(err) = build("definitely-not-a-provider", "m") else {
            panic!("expected an error");
        };
        assert!(err.contains("unknown llm provider"), "{err}");
    }

    #[cfg(not(all(not(target_arch = "wasm32"), feature = "claude")))]
    #[test]
    fn uncompiled_provider_errors_not_mock() {
        let Err(err) = build("claude", "m") else {
            panic!("expected an error");
        };
        assert!(err.contains("--features claude"), "{err}");
    }

    // The pure half of the env read; mutating CITADEL_AI_LLM_TIMEOUT_SECS in
    // a parallel test run would race other tests.
    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "claude", feature = "openai", feature = "ollama")
    ))]
    #[test]
    fn timeout_parses_seconds_and_keeps_default_otherwise() {
        assert_eq!(parse_timeouts(Some("300")), LlmTimeouts { recv_secs: 300 });
        assert_eq!(parse_timeouts(Some("not-a-number")), LlmTimeouts::default());
        assert_eq!(parse_timeouts(None), LlmTimeouts::default());
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "ollama"))]
    #[test]
    fn from_env_reads_prefix_provider_and_model() {
        std::env::set_var("CITADEL_ENVTEST_PROVIDER", "ollama");
        std::env::set_var("CITADEL_ENVTEST_MODEL", "llama-envtest");
        let llm = from_env("CITADEL_ENVTEST", "mock", "default-model").unwrap();
        std::env::remove_var("CITADEL_ENVTEST_PROVIDER");
        std::env::remove_var("CITADEL_ENVTEST_MODEL");
        assert_eq!(
            llm.model_id(),
            "llama-envtest",
            "PROVIDER+MODEL env honored"
        );
    }
}
