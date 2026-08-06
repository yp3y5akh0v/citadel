# citadeldb-ai

Autonomous agent runtime for [Citadel](https://github.com/yp3y5akh0v/citadel), using
[`citadeldb-mem`](https://crates.io/crates/citadeldb-mem) for encrypted, persistent memory.
Implements a ReAct + Reflexion agent loop with a tool registry, hard budget caps (steps,
tokens, wall-time, cost), and memory-backed plan caching.

The `LLMClient` trait and its Claude / OpenAI / Ollama / Gemini backends live in
[`citadeldb-llm`](https://crates.io/crates/citadeldb-llm); depend on that crate directly to
build one. To serve Citadel memory over MCP, use
[`citadeldb-mcp`](https://crates.io/crates/citadeldb-mcp).

This crate is part of the Citadel workspace.

## License

Apache-2.0
