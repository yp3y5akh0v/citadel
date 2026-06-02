//! Shared blocking-HTTP plumbing for the provider backends (native-only).
//!
//! One ureq agent per client; non-2xx is returned as a normal response so the
//! provider's error body can be read and classified into [`LlmError`].

use std::time::Duration;

use serde_json::Value;
use ureq::Agent;

use super::{LlmError, Message};

/// Per-phase deadlines. A global timeout alone does not reliably interrupt a read
/// stalled on a half-closed (peer-dropped) socket; the recv deadlines do, so a dead
/// connection errors promptly instead of blocking forever.
const TIMEOUT_CONNECT_SECS: u64 = 15;
const TIMEOUT_SEND_SECS: u64 = 30;
const TIMEOUT_RECV_SECS: u64 = 120;
const TIMEOUT_GLOBAL_SECS: u64 = 180;
/// Keep provider error bodies bounded in the error message / trace.
const MAX_ERROR_BODY: usize = 500;

/// The shared ureq agent: per-phase + global deadlines, non-2xx surfaced as a normal
/// response so the provider error body is readable.
///
/// Connection pooling is disabled (`max_idle_connections* = 0`): on a long, rate-paced
/// run the peer drops idle keep-alive sockets, and reusing a half-closed one could block
/// a read past even the recv deadline. A fresh connection per request avoids it.
pub(super) fn agent() -> Agent {
    Agent::config_builder()
        .timeout_connect(Some(Duration::from_secs(TIMEOUT_CONNECT_SECS)))
        .timeout_send_request(Some(Duration::from_secs(TIMEOUT_SEND_SECS)))
        .timeout_recv_response(Some(Duration::from_secs(TIMEOUT_RECV_SECS)))
        .timeout_recv_body(Some(Duration::from_secs(TIMEOUT_RECV_SECS)))
        .timeout_global(Some(Duration::from_secs(TIMEOUT_GLOBAL_SECS)))
        .max_idle_connections(0)
        .max_idle_connections_per_host(0)
        .http_status_as_error(false)
        .build()
        .into()
}

/// POST `body` as JSON to `url` with `headers`. Returns the parsed JSON on 2xx,
/// a classified [`LlmError::Http`] for a non-2xx status (with `Retry-After`
/// parsed when present), or [`LlmError::Transport`] for a pre-status failure.
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

/// Best-effort pre-call token estimate shared by the HTTP backends (~4 chars
/// per token, never zero for a non-empty conversation). HTTP backends cannot
/// count exactly without a network round-trip; the real counts arrive in the
/// response usage.
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
