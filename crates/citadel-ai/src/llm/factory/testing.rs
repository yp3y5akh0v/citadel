//! Test-only [`LLMClient`] builders: the single sanctioned way to get a fake
//! client, so no test constructs a concrete client type directly. Always built
//! (no feature gate) - the mock + closure adapter are pure.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use super::{from_fn_with, TokenCount};
use crate::llm::mock::MockClient;
use crate::llm::{CompletionRequest, CompletionResponse, LLMClient};

/// Pops `responses` in order, then errors when drained (empty errors on first call).
pub fn scripted(responses: Vec<CompletionResponse>) -> Arc<dyn LLMClient> {
    Arc::new(MockClient::scripted(responses))
}

/// One plain-text reply, then exhausts.
pub fn reply_once(text: impl Into<String>) -> Arc<dyn LLMClient> {
    Arc::new(MockClient::replying(text))
}

/// The SAME text on every call, forever - never exhausts.
pub fn constant(text: impl Into<String>) -> Arc<dyn LLMClient> {
    let text = text.into();
    from_fn_with("const", TokenCount::PerMessage(1), move |_req| {
        Ok(CompletionResponse::text(text.clone()))
    })
}

/// Every call returns a fresh error from `mk` (caller picks the flavor).
pub fn error<E>(mk: E) -> Arc<dyn LLMClient>
where
    E: Fn() -> crate::llm::LlmError + Send + Sync + 'static,
{
    from_fn_with("error", TokenCount::Constant(1), move |_req| Err(mk()))
}

/// Returns `Http { status, message }` for the first `fail` calls, then `then`.
/// The returned [`Probe`] surfaces an atomic call counter for attempt assertions.
pub fn http_storm(
    fail: u32,
    status: u16,
    message: impl Into<String>,
    then: CompletionResponse,
) -> Probe {
    let calls = Arc::new(AtomicU32::new(0));
    let remaining = Arc::new(AtomicU32::new(fail));
    let message = message.into();
    let counter = Arc::clone(&calls);
    let client = from_fn_with("storm", TokenCount::Constant(10), move |_req| {
        calls.fetch_add(1, Ordering::SeqCst);
        let was_failing = remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if was_failing {
            Err(crate::llm::LlmError::Http {
                status,
                retry_after: None,
                message: message.clone(),
            })
        } else {
            Ok(then.clone())
        }
    });
    Probe {
        client,
        calls: counter,
    }
}

/// A storm/flaky client plus its atomic invocation counter.
pub struct Probe {
    client: Arc<dyn LLMClient>,
    calls: Arc<AtomicU32>,
}

impl Probe {
    pub fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn client(&self) -> Arc<dyn LLMClient> {
        Arc::clone(&self.client)
    }
}

/// Scripted (pops in order, errors when drained) AND records every request, so a
/// test can assert what context each round was handed via [`Capture::requests`].
pub fn capturing(responses: Vec<CompletionResponse>) -> Capture {
    let inner = MockClient::scripted(responses);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&requests);
    let client = from_fn_with("capturing", TokenCount::Constant(1), move |req| {
        sink.lock().unwrap().push(req.clone());
        inner.complete(req)
    });
    Capture { client, requests }
}

/// A capturing client plus the recorded request log (a cloned snapshot).
pub struct Capture {
    client: Arc<dyn LLMClient>,
    requests: Arc<Mutex<Vec<CompletionRequest>>>,
}

impl Capture {
    pub fn requests(&self) -> Vec<CompletionRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub fn client(&self) -> Arc<dyn LLMClient> {
        Arc::clone(&self.client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::{FinishReason, Message};

    #[test]
    fn scripted_responses_are_returned_in_order_then_exhaust() {
        let client = scripted(vec![
            CompletionResponse::text("first"),
            CompletionResponse::text("second"),
        ]);
        let req = CompletionRequest::default();
        assert_eq!(client.complete(&req).unwrap().message.content, "first");
        assert_eq!(client.complete(&req).unwrap().message.content, "second");
        assert!(client.complete(&req).is_err(), "exhausted -> error");
    }

    #[test]
    fn reply_once_answers_once() {
        let client = reply_once("hello");
        assert_eq!(client.model_id(), "mock");
        let resp = client.complete(&CompletionRequest::default()).unwrap();
        assert_eq!(resp.message.content, "hello");
        assert_eq!(resp.finish_reason, FinishReason::Stop);
    }

    #[test]
    fn count_tokens_is_deterministic_and_scales() {
        let client = reply_once("x");
        let short = client.count_tokens(&[Message::user("hi")]);
        let long = client.count_tokens(&[Message::user("a".repeat(400).as_str())]);
        assert!(long > short);
        assert_eq!(short, client.count_tokens(&[Message::user("hi")]));
    }

    #[test]
    fn constant_answers_every_call() {
        let client = constant("same");
        let req = CompletionRequest::default();
        for _ in 0..3 {
            assert_eq!(client.complete(&req).unwrap().message.content, "same");
        }
    }

    #[test]
    fn http_storm_fails_then_succeeds_and_counts_calls() {
        let storm = http_storm(2, 503, "flaky", CompletionResponse::text("ok"));
        let client = storm.client();
        let req = CompletionRequest::default();
        assert!(client.complete(&req).is_err());
        assert!(client.complete(&req).is_err());
        assert_eq!(client.complete(&req).unwrap().message.content, "ok");
        assert_eq!(storm.calls(), 3);
    }

    #[test]
    fn capturing_records_every_request() {
        let cap = capturing(vec![CompletionResponse::text("a")]);
        let client = cap.client();
        client
            .complete(&CompletionRequest::new(vec![Message::user("ctx")]))
            .unwrap();
        let reqs = cap.requests();
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].messages[0], Message::User(_)));
    }

    #[test]
    fn error_always_fails() {
        let client = error(|| crate::llm::LlmError::Backend("boom".into()));
        assert!(client.complete(&CompletionRequest::default()).is_err());
    }
}
