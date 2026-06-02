//! Ollama backend (native-only, `ollama` feature).
//!
//! Ollama's OpenAI-compatible `/v1` is wire-identical to OpenAI, so this is the
//! OpenAI adapter pointed at the local daemon with `max_tokens` and no cost (the
//! native `/api/chat` path is deliberately not used).

use super::openai::OpenAiClient;
use super::{CompletionRequest, CompletionResponse, LLMClient, LlmError, Message};

const OLLAMA_BASE_URL: &str = "http://localhost:11434/v1";

/// Talks to a local Ollama server. `model` is an Ollama model tag, e.g.
/// `qwen2.5` or `llama3.2`.
pub struct OllamaClient {
    inner: OpenAiClient,
}

impl OllamaClient {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            inner: OpenAiClient::with_base_url(model, OLLAMA_BASE_URL, "ollama")
                .max_tokens_field("max_tokens")
                .unpriced(),
        }
    }
}

impl LLMClient for OllamaClient {
    fn complete(&self, req: &CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.inner.complete(req)
    }

    fn model_id(&self) -> &str {
        self.inner.model_id()
    }

    fn count_tokens(&self, messages: &[Message]) -> usize {
        self.inner.count_tokens(messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn carries_model_tag() {
        let c = OllamaClient::new("qwen2.5");
        assert_eq!(c.model_id(), "qwen2.5");
    }
}
