//! Deterministic LLM for tests: returns scripted responses in order.

use std::collections::VecDeque;
use std::sync::Mutex;

use super::{CompletionRequest, CompletionResponse, LLMClient, LlmError, Message};

pub struct MockClient {
    scripted: Mutex<VecDeque<CompletionResponse>>,
    model_id: String,
}

impl MockClient {
    /// Hands back `responses` one per `complete` call, then errors once drained.
    pub fn scripted(responses: Vec<CompletionResponse>) -> Self {
        Self {
            scripted: Mutex::new(responses.into()),
            model_id: "mock".into(),
        }
    }

    /// A client that answers a single call with one plain-text reply.
    pub fn replying(text: impl Into<String>) -> Self {
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
        // Rough ~4 chars/token estimate; deterministic, never zero for input.
        let chars: usize = messages.iter().map(message_chars).sum();
        (chars / 4).max(messages.len())
    }
}

fn message_chars(m: &Message) -> usize {
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
    use super::*;
    use crate::llm::{FinishReason, ToolCall};
    use std::sync::Arc;

    #[test]
    fn scripted_responses_are_returned_in_order_then_exhaust() {
        let client = MockClient::scripted(vec![
            CompletionResponse::text("first"),
            CompletionResponse::text("second"),
        ]);
        let req = CompletionRequest::default();
        assert_eq!(client.complete(&req).unwrap().message.content, "first");
        assert_eq!(client.complete(&req).unwrap().message.content, "second");
        assert!(client.complete(&req).is_err(), "exhausted -> error");
    }

    #[test]
    fn replying_answers_once() {
        let client = MockClient::replying("hello");
        assert_eq!(client.model_id(), "mock");
        let resp = client.complete(&CompletionRequest::default()).unwrap();
        assert_eq!(resp.message.content, "hello");
        assert_eq!(resp.finish_reason, FinishReason::Stop);
    }

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

    #[test]
    fn count_tokens_is_deterministic_and_scales() {
        let client = MockClient::replying("x");
        let short = client.count_tokens(&[Message::user("hi")]);
        let long = client.count_tokens(&[Message::user("a".repeat(400).as_str())]);
        assert!(long > short);
        assert_eq!(short, client.count_tokens(&[Message::user("hi")]));
    }

    #[test]
    fn usable_as_trait_object() {
        let client: Arc<dyn LLMClient> = Arc::new(MockClient::replying("ok"));
        assert_eq!(
            client
                .complete(&CompletionRequest::default())
                .unwrap()
                .message
                .content,
            "ok"
        );
    }
}
