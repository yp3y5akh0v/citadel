//! Ollama backend (native-only, `ollama` feature): the OpenAI adapter pointed at
//! the local daemon, since Ollama's `/v1` is wire-identical to OpenAI.

use super::http::LlmTimeouts;
use super::openai::OpenAiClient;
use super::{CompletionRequest, CompletionResponse, LLMClient, LlmError, Message};

const OLLAMA_BASE_URL: &str = "http://localhost:11434/v1";

pub(crate) struct OllamaClient {
    inner: OpenAiClient,
}

impl OllamaClient {
    pub(crate) fn new(model: impl Into<String>) -> Self {
        Self::with_base_url(model, OLLAMA_BASE_URL)
    }

    /// A specific Ollama `/v1` base: remote host, custom port, or test server.
    pub(crate) fn with_base_url(model: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            inner: OpenAiClient::with_base_url(model, base_url, "ollama")
                .max_tokens_field("max_tokens")
                .unpriced(),
        }
    }

    /// Replace the default HTTP deadlines.
    pub(crate) fn with_timeouts(mut self, timeouts: LlmTimeouts) -> Self {
        self.inner = self.inner.with_timeouts(timeouts);
        self
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
    use super::OllamaClient;
    use crate::llm::{CompletionRequest, FinishReason, LLMClient, Message};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    #[test]
    fn carries_model_tag() {
        let c = crate::llm::factory::build("ollama", "llama3.2").unwrap();
        assert_eq!(c.model_id(), "llama3.2");
    }

    #[test]
    fn http_round_trip_against_mock_server() {
        const RESPONSE: &str = concat!(
            r#"{"choices":[{"message":{"content":"pong from llama"},"#,
            r#""finish_reason":"stop"}],"#,
            r#""usage":{"prompt_tokens":5,"completion_tokens":3}}"#
        );
        let (base_url, server) = one_shot_server(RESPONSE);

        let client = OllamaClient::with_base_url("llama3.2", base_url);
        let req = CompletionRequest {
            max_tokens: Some(64),
            ..CompletionRequest::new(vec![Message::user("ping")])
        };
        let resp = client.complete(&req).unwrap();

        assert_eq!(resp.message.content, "pong from llama");
        assert!(resp.message.tool_calls.is_empty());
        assert_eq!(resp.finish_reason, FinishReason::Stop);
        assert_eq!(resp.usage.input_tokens, 5);
        assert_eq!(resp.usage.output_tokens, 3);
        assert_eq!(resp.usage.cost_usd, None, "local ollama reports no cost");

        let request = server.join().unwrap();
        let (head, body) = request
            .split_once("\r\n\r\n")
            .expect("request separates headers from body");
        assert!(
            head.starts_with("POST /v1/chat/completions "),
            "unexpected request line: {head}"
        );
        assert!(
            head.to_lowercase().contains("authorization: bearer ollama"),
            "missing ollama auth header: {head}"
        );
        let sent: serde_json::Value =
            serde_json::from_str(body.trim()).expect("request body is JSON");
        assert_eq!(sent["model"], "llama3.2");
        assert_eq!(sent["max_tokens"], 64, "ollama must send max_tokens");
        assert!(
            sent.get("max_completion_tokens").is_none(),
            "must not send the OpenAI-only field"
        );
    }

    fn one_shot_server(body: &'static str) -> (String, thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&mut stream);
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 content-type: application/json\r\n\
                 content-length: {}\r\n\
                 connection: close\r\n\
                 \r\n\
                 {}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();
            request
        });
        (format!("http://{addr}/v1"), handle)
    }

    fn read_http_request(stream: &mut TcpStream) -> String {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        let header_end = loop {
            if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                break pos + 4;
            }
            let n = stream.read(&mut chunk).unwrap();
            if n == 0 {
                break buf.len();
            }
            buf.extend_from_slice(&chunk[..n]);
        };
        let want = header_end + content_length_of(&buf[..header_end.min(buf.len())]);
        while buf.len() < want {
            let n = stream.read(&mut chunk).unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    fn content_length_of(header_bytes: &[u8]) -> usize {
        let headers = String::from_utf8_lossy(header_bytes);
        for line in headers.lines() {
            if let Some((k, v)) = line.split_once(':') {
                if k.trim().eq_ignore_ascii_case("content-length") {
                    return v.trim().parse().unwrap_or(0);
                }
            }
        }
        0
    }
}
