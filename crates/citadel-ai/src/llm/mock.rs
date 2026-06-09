//! Deterministic LLM for tests: returns scripted responses in order.

use std::collections::VecDeque;
use std::sync::Mutex;

use super::{CompletionRequest, CompletionResponse, LLMClient, LlmError, Message};

pub(crate) struct MockClient {
    scripted: Mutex<VecDeque<CompletionResponse>>,
    model_id: String,
}

impl MockClient {
    /// Hands back `responses` one per `complete` call, then errors once drained.
    pub(crate) fn scripted(responses: Vec<CompletionResponse>) -> Self {
        Self {
            scripted: Mutex::new(responses.into()),
            model_id: "mock".into(),
        }
    }

    /// A client that answers a single call with one plain-text reply.
    pub(crate) fn replying(text: impl Into<String>) -> Self {
        Self::scripted(vec![CompletionResponse::text(text)])
    }
}

impl LLMClient for MockClient {
    fn complete(&self, _req: &CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.scripted
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| LlmError::Backend("mock: no scripted responses left".into()))
    }

    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn count_tokens(&self, messages: &[Message]) -> usize {
        let chars: usize = messages.iter().map(message_chars).sum();
        (chars / 4).max(messages.len())
    }
}

pub(crate) fn message_chars(m: &Message) -> usize {
    match m {
        Message::System(s) | Message::User(s) => s.len(),
        Message::Tool { content, .. } => content.len(),
        Message::Assistant(a) => {
            a.content.len()
                + a.tool_calls
                    .iter()
                    .map(|c| c.name.len() + c.arguments.to_string().len())
                    .sum::<usize>()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::llm::{CompletionResponse, FinishReason, ToolCall};

    #[test]
    fn tool_call_response_carries_calls() {
        let resp = CompletionResponse::tool_calls(vec![ToolCall {
            id: "c1".into(),
            name: "recall".into(),
            arguments: serde_json::json!({"q": "x"}),
        }]);
        assert_eq!(resp.finish_reason, FinishReason::ToolUse);
        assert_eq!(resp.message.tool_calls.len(), 1);
        assert_eq!(resp.message.tool_calls[0].name, "recall");
    }
}
