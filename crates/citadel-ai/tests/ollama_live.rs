//! Live Ollama smoke test (opt-in, ignored by default).
//!
//! Ollama is the local model runner; the model is Meta's Llama (no Chinese-origin
//! models). This test needs a running daemon with a model pulled.
//!
//! Setup: install Ollama from <https://ollama.com>, then `ollama pull llama3.2:3b`
//! (the daemon serves on <http://localhost:11434>).
//!
//! Run: `cargo test -p citadeldb-ai --features ollama --test ollama_live -- --ignored --nocapture`
//!
//! Override the model (default `llama3.2`) with CITADEL_OLLAMA_MODEL.

#![cfg(feature = "ollama")]

use citadel_ai::factory;
use citadel_ai::{CompletionRequest, FinishReason, Message};

#[test]
#[ignore = "requires a running Ollama daemon with a model pulled (see file header)"]
fn live_ollama_completion() {
    // The factory door: CITADEL_OLLAMA_PROVIDER (default ollama) + CITADEL_OLLAMA_MODEL.
    let client =
        factory::from_env("CITADEL_OLLAMA", "ollama", "llama3.2").expect("build ollama client");
    println!("model: {}", client.model_id());

    let req = CompletionRequest {
        max_tokens: Some(64),
        temperature: Some(0.0),
        ..CompletionRequest::new(vec![
            Message::system("You are concise. Answer in one short sentence."),
            Message::user("Say hello and name yourself."),
        ])
    };

    let resp = client.complete(&req).unwrap_or_else(|e| {
        panic!("ollama call failed (is the daemon running with the model pulled?): {e}")
    });

    println!("reply: {}", resp.message.content);
    println!(
        "usage: in={} out={}",
        resp.usage.input_tokens, resp.usage.output_tokens
    );
    assert!(
        !resp.message.content.trim().is_empty(),
        "expected a non-empty reply from the local model"
    );
    assert!(
        matches!(
            resp.finish_reason,
            FinishReason::Stop | FinishReason::Length
        ),
        "unexpected finish reason: {:?}",
        resp.finish_reason
    );
}
