//! Blocking-HTTP for native backends; non-2xx returns so error bodies are readable.

use std::time::Duration;

use serde_json::Value;
use ureq::Agent;

use super::{LlmError, Message};

const TIMEOUT_CONNECT_SECS: u64 = 15;
/// Floor for the send deadline; see [`LlmTimeouts::send_secs`].
const TIMEOUT_SEND_FLOOR_SECS: u64 = 30;
const TIMEOUT_RECV_SECS: u64 = 120;
/// Headroom the global deadline keeps above the receive budget.
const TIMEOUT_GLOBAL_MARGIN_SECS: u64 = 60;
/// Keep provider error bodies bounded in the error message / trace.
const MAX_ERROR_BODY: usize = 500;

/// HTTP deadlines derived from `recv_secs`; a global one can't cut a stalled read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LlmTimeouts {
    /// Seconds for response start plus body; deep-reasoning models take minutes.
    pub recv_secs: u64,
}

impl Default for LlmTimeouts {
    fn default() -> Self {
        Self {
            recv_secs: TIMEOUT_RECV_SECS,
        }
    }
}

impl LlmTimeouts {
    /// Scales with recv: queued API edges stall the socket and bill it to send.
    fn send_secs(&self) -> u64 {
        TIMEOUT_SEND_FLOOR_SECS.max(self.recv_secs / 2)
    }

    /// The global deadline bounds the whole exchange above send + recv.
    fn global_secs(&self) -> u64 {
        self.recv_secs + TIMEOUT_GLOBAL_MARGIN_SECS
    }
}

/// Shared agent; pooling off: a peer-dropped idle socket blocks past recv deadline.
pub(super) fn agent(timeouts: &LlmTimeouts) -> Agent {
    Agent::config_builder()
        .timeout_connect(Some(Duration::from_secs(TIMEOUT_CONNECT_SECS)))
        .timeout_send_request(Some(Duration::from_secs(timeouts.send_secs())))
        .timeout_recv_response(Some(Duration::from_secs(timeouts.recv_secs)))
        .timeout_recv_body(Some(Duration::from_secs(timeouts.recv_secs)))
        .timeout_global(Some(Duration::from_secs(timeouts.global_secs())))
        .max_idle_connections(0)
        .max_idle_connections_per_host(0)
        .http_status_as_error(false)
        .build()
        .into()
}

/// POST JSON; non-2xx becomes [`LlmError::Http`] with `Retry-After` when present.
pub(super) fn post_json(
    agent: &Agent,
    url: &str,
    headers: &[(&str, &str)],
    body: &Value,
) -> Result<Value, LlmError> {
    let mut req = agent.post(url);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let mut resp = req
        .send_json(body)
        .map_err(|e| LlmError::Transport(e.to_string()))?;
    let status = resp.status().as_u16();
    if (200..300).contains(&status) {
        return resp
            .body_mut()
            .read_json::<Value>()
            .map_err(|e| LlmError::Backend(format!("malformed response body: {e}")));
    }
    let retry_after = resp
        .headers()
        .get("retry-after")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok());
    let message = resp
        .body_mut()
        .read_to_string()
        .unwrap_or_else(|_| "<no body>".to_string());
    Err(LlmError::Http {
        status,
        retry_after,
        message: truncate(&message, MAX_ERROR_BODY),
    })
}

fn truncate(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((idx, _)) => format!("{}...", &s[..idx]),
        None => s.to_string(),
    }
}

/// Pre-call estimate at ~4 chars/token; exact counts only arrive in response usage.
pub(super) fn estimate_tokens(messages: &[Message]) -> usize {
    let chars: usize = messages.iter().map(message_chars).sum();
    (chars / 4).max(messages.len())
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

    #[test]
    fn default_timeouts_derive_send_and_global() {
        let t = LlmTimeouts::default();
        assert_eq!(t.recv_secs, 120);
        assert_eq!(t.send_secs(), 60);
        assert_eq!(t.global_secs(), 180);
    }

    #[test]
    fn send_scales_with_recv_above_a_floor() {
        assert_eq!(LlmTimeouts { recv_secs: 10 }.send_secs(), 30, "floor");
        assert_eq!(LlmTimeouts { recv_secs: 600 }.send_secs(), 300, "recv / 2");
    }

    #[test]
    fn global_keeps_headroom_above_recv() {
        assert_eq!(LlmTimeouts { recv_secs: 600 }.global_secs(), 660);
    }

    #[test]
    fn agent_applies_the_derived_deadlines() {
        let t = agent(&LlmTimeouts { recv_secs: 300 }).config().timeouts();
        assert_eq!(t.connect, Some(Duration::from_secs(15)));
        assert_eq!(t.send_request, Some(Duration::from_secs(150)));
        assert_eq!(t.recv_response, Some(Duration::from_secs(300)));
        assert_eq!(t.recv_body, Some(Duration::from_secs(300)));
        assert_eq!(t.global, Some(Duration::from_secs(360)));
    }

    #[test]
    fn estimate_scales_and_is_never_zero() {
        let short = estimate_tokens(&[Message::user("hi")]);
        let long = estimate_tokens(&[Message::user("a".repeat(400))]);
        assert!(long > short);
        assert!(estimate_tokens(&[Message::user("")]) >= 1, "never zero");
    }

    #[test]
    fn truncate_keeps_short_strings() {
        assert_eq!(truncate("abc", 10), "abc");
        assert_eq!(truncate("abcdef", 3), "abc...");
    }
}
