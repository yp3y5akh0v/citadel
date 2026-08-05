//! [`LLMClient`] construction by provider name; no silent fallback to a mock.

use std::sync::Arc;

use crate::mock::MockClient;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai", feature = "ollama")
))]
use crate::LlmTimeouts;
use crate::{ClientRequestIdentity, LLMClient};
#[cfg(any(test, feature = "test-util"))]
use crate::{CompletionRequest, CompletionResponse, LlmError, Message};

#[cfg(any(test, feature = "test-util"))]
pub mod testing;

/// Provider names the factory recognizes (whether or not compiled in this build).
const KNOWN_PROVIDERS: &[&str] = &["mock", "claude", "openai", "ollama", "gemini"];
#[cfg(feature = "gemini")]
const GEMINI_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta/openai";

/// Client from `{prefix}_PROVIDER` / `{prefix}_MODEL` plus any API key it requires.
pub fn from_env(
    prefix: &str,
    default_provider: &str,
    default_model: &str,
) -> Result<Arc<dyn LLMClient>, String> {
    let (provider, model) = provider_model(prefix, default_provider, default_model);
    build(&provider, &model)
}

/// Key-free identity resolution for preflight; the live client re-verifies it.
pub fn request_identity_from_env(
    prefix: &str,
    default_provider: &str,
    default_model: &str,
) -> Result<(String, ClientRequestIdentity), String> {
    let (provider, model) = provider_model(prefix, default_provider, default_model);
    Ok((
        model.clone(),
        request_identity_for_provider(&provider, &model)?,
    ))
}

/// [`from_env`] with explicit deadlines instead of `CITADEL_AI_LLM_TIMEOUT_SECS`.
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

fn provider_model(prefix: &str, default_provider: &str, default_model: &str) -> (String, String) {
    let provider = std::env::var(format!("{prefix}_PROVIDER"))
        .unwrap_or_else(|_| default_provider.to_string());
    let model =
        std::env::var(format!("{prefix}_MODEL")).unwrap_or_else(|_| default_model.to_string());
    (provider, model)
}

/// HTTP client for `provider`/`model`; deadlines from `CITADEL_AI_LLM_TIMEOUT_SECS`.
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
            let client = crate::claude::ClaudeClient::new(model, key);
            Ok(Arc::new(client.with_timeouts(timeouts)))
        }
        #[cfg(feature = "openai")]
        "openai" => {
            let key = require_key("OPENAI_API_KEY", "openai")?;
            let client = crate::openai::OpenAiClient::with_base_url(
                model,
                openai_base_url(),
                key,
                crate::OutputSchemaSupport::StrictJsonSchema,
                crate::openai::request_effort_support_for_model(model),
            )
            .identity_provider("openai");
            Ok(Arc::new(client.with_timeouts(timeouts)))
        }
        #[cfg(feature = "ollama")]
        "ollama" => {
            let client = crate::ollama::OllamaClient::with_base_url(model, ollama_base_url());
            Ok(Arc::new(client.with_timeouts(timeouts)))
        }
        #[cfg(feature = "gemini")]
        "gemini" => {
            // Compat layer wants `max_tokens`; own key so an openai judge coexists.
            let key = require_key("GEMINI_API_KEY", "gemini")?;
            let mut client = crate::openai::OpenAiClient::with_base_url(
                model,
                GEMINI_BASE_URL,
                key,
                crate::OutputSchemaSupport::Unsupported,
                crate::openai::RequestEffortSupport::Unsupported,
            )
            .max_tokens_field("max_tokens")
            .identity_provider("gemini");
            if let Some(effort) = gemini_reasoning_effort() {
                client = client.reasoning_effort(effort);
            }
            Ok(Arc::new(client.with_timeouts(timeouts)))
        }
        other => fallback(other),
    }
}

#[cfg(feature = "openai")]
fn openai_base_url() -> String {
    std::env::var("OPENAI_BASE_URL").unwrap_or_else(|_| crate::openai::DEFAULT_BASE_URL.to_string())
}

#[cfg(feature = "ollama")]
fn ollama_base_url() -> String {
    std::env::var("OLLAMA_BASE_URL").unwrap_or_else(|_| crate::ollama::OLLAMA_BASE_URL.to_string())
}

#[cfg(feature = "gemini")]
fn gemini_reasoning_effort() -> Option<String> {
    std::env::var("CITADEL_GEMINI_REASONING_EFFORT")
        .ok()
        .map(|effort| effort.trim().to_string())
        .filter(|effort| !effort.is_empty())
}

fn request_identity_for_provider(
    provider: &str,
    model: &str,
) -> Result<ClientRequestIdentity, String> {
    identity_from_parts(
        provider,
        model,
        resolved_base_url(provider).as_deref(),
        resolved_gemini_effort(provider).as_deref(),
    )
}

fn resolved_base_url(provider: &str) -> Option<String> {
    match provider {
        #[cfg(feature = "openai")]
        "openai" => Some(openai_base_url()),
        #[cfg(feature = "ollama")]
        "ollama" => Some(ollama_base_url()),
        _ => None,
    }
}

fn resolved_gemini_effort(provider: &str) -> Option<String> {
    match provider {
        #[cfg(feature = "gemini")]
        "gemini" => gemini_reasoning_effort(),
        _ => None,
    }
}

fn identity_from_parts(
    provider: &str,
    model: &str,
    base_url: Option<&str>,
    gemini_effort: Option<&str>,
) -> Result<ClientRequestIdentity, String> {
    let _ = (model, base_url, gemini_effort);
    match provider {
        "mock" => Ok(ClientRequestIdentity::in_process()),
        #[cfg(feature = "claude")]
        "claude" => {
            let default_max_tokens = crate::claude::DEFAULT_MAX_TOKENS.to_string();
            Ok(ClientRequestIdentity::from_config(
                "claude",
                crate::claude::API_URL,
                &[
                    ("wire", crate::claude::MESSAGES_WIRE_REVISION),
                    ("anthropic-version", crate::claude::API_VERSION),
                    ("default_max_tokens", &default_max_tokens),
                    (
                        "structured_outputs",
                        crate::claude::STRUCTURED_OUTPUTS_REVISION,
                    ),
                ],
            ))
        }
        #[cfg(feature = "openai")]
        "openai" => {
            let request_effort = crate::openai::request_effort_support_for_model(model);
            Ok(crate::openai::client_request_identity(
                "openai",
                base_url.ok_or("openai identity requires a resolved base URL")?,
                crate::openai::OPENAI_MAX_TOKENS_FIELD,
                None,
                request_effort,
                crate::OutputSchemaSupport::StrictJsonSchema,
            ))
        }
        #[cfg(feature = "ollama")]
        "ollama" => Ok(crate::openai::client_request_identity(
            "ollama",
            base_url.ok_or("ollama identity requires a resolved base URL")?,
            "max_tokens",
            None,
            crate::openai::RequestEffortSupport::Unsupported,
            crate::OutputSchemaSupport::Unsupported,
        )),
        #[cfg(feature = "gemini")]
        "gemini" => Ok(crate::openai::client_request_identity(
            "gemini",
            GEMINI_BASE_URL,
            "max_tokens",
            gemini_effort,
            crate::openai::RequestEffortSupport::Unsupported,
            crate::OutputSchemaSupport::Unsupported,
        )),
        p if KNOWN_PROVIDERS.contains(&p) => Err(not_compiled(p)),
        p => Err(unknown_provider(p)),
    }
}

/// Deadlines from `CITADEL_AI_LLM_TIMEOUT_SECS`; `*_with_timeouts` bypasses it.
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai", feature = "ollama")
))]
fn timeouts_from_env() -> LlmTimeouts {
    parse_timeouts(std::env::var("CITADEL_AI_LLM_TIMEOUT_SECS").ok().as_deref())
}

/// Pure half of [`timeouts_from_env`]; unset or malformed keeps the default.
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

/// Mock-only build (wasm or no HTTP backend); every non-mock name is an error.
#[cfg(not(all(
    not(target_arch = "wasm32"),
    any(feature = "claude", feature = "openai", feature = "ollama")
)))]
pub fn build(provider: &str, _: &str) -> Result<Arc<dyn LLMClient>, String> {
    fallback(provider)
}

/// Non-HTTP outcomes: mock always builds, every other name is a hard error.
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
    format!("unknown llm provider '{provider}' (valid: mock, claude, openai, ollama, gemini)")
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
                let chars: usize = messages.iter().map(crate::mock::message_chars).sum();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "claude", feature = "openai", feature = "ollama")
    ))]
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "claude", feature = "openai", feature = "ollama")
    ))]
    struct EnvGuard(Vec<(&'static str, Option<std::ffi::OsString>)>);

    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "claude", feature = "openai", feature = "ollama")
    ))]
    impl EnvGuard {
        fn set(values: &[(&'static str, Option<&str>)]) -> Self {
            let old = values
                .iter()
                .map(|(name, _)| (*name, std::env::var_os(name)))
                .collect();
            for (name, value) in values {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
            Self(old)
        }
    }

    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "claude", feature = "openai", feature = "ollama")
    ))]
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (name, value) in self.0.drain(..) {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    #[test]
    fn mock_is_always_available() {
        let llm = from_env("CITADEL_FACTORY_TEST_UNSET", "mock", "ignored").unwrap();
        assert_eq!(llm.model_id(), "mock");
        let (_, resolved) =
            request_identity_from_env("CITADEL_FACTORY_IDENTITY_UNSET", "mock", "ignored").unwrap();
        assert_eq!(resolved, llm.request_identity());
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

    // Pure half only: mutating CITADEL_AI_LLM_TIMEOUT_SECS would race other tests.
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
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::set(&[
            ("CITADEL_ENVTEST_PROVIDER", Some("ollama")),
            ("CITADEL_ENVTEST_MODEL", Some("llama-envtest")),
        ]);
        let llm = from_env("CITADEL_ENVTEST", "mock", "default-model").unwrap();
        assert_eq!(
            llm.model_id(),
            "llama-envtest",
            "PROVIDER+MODEL env honored"
        );
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "openai"))]
    #[test]
    fn openai_pure_identity_matches_built_custom_endpoint() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::set(&[
            ("OPENAI_API_KEY", Some("not-a-real-key")),
            (
                "OPENAI_BASE_URL",
                Some("https://gateway.invalid/custom/v1/"),
            ),
        ]);
        let expected = request_identity_for_provider("openai", "same-model").unwrap();
        let client = build("openai", "same-model").unwrap();
        assert_eq!(expected, client.request_identity());
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "ollama"))]
    #[test]
    fn ollama_pure_identity_matches_built_custom_endpoint() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::set(&[("OLLAMA_BASE_URL", Some("http://localhost:19999/custom/v1/"))]);
        let expected = request_identity_for_provider("ollama", "same-model").unwrap();
        let client = build("ollama", "same-model").unwrap();
        assert_eq!(expected, client.request_identity());
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "gemini"))]
    #[test]
    fn gemini_pure_identity_matches_built_reasoning_default() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::set(&[
            ("GEMINI_API_KEY", Some("not-a-real-key")),
            ("CITADEL_GEMINI_REASONING_EFFORT", Some("low")),
        ]);
        let expected = request_identity_for_provider("gemini", "same-model").unwrap();
        let client = build("gemini", "same-model").unwrap();
        assert_eq!(expected, client.request_identity());
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "claude"))]
    #[test]
    fn claude_pure_identity_matches_built_client() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::set(&[("ANTHROPIC_API_KEY", Some("not-a-real-key"))]);
        let expected = request_identity_for_provider("claude", "same-model").unwrap();
        let client = build("claude", "same-model").unwrap();
        assert_eq!(expected, client.request_identity());
    }
}
