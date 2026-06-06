# citadeldb-ai

Autonomous agent runtime for [Citadel](https://github.com/yp3y5akh0v/citadel), using
[`citadeldb-mem`](https://crates.io/crates/citadeldb-mem) for encrypted, persistent memory.
Implements a ReAct + Reflexion agent loop with a tool registry, hard budget caps (steps,
tokens, wall-time, cost), memory-backed plan caching, and pluggable `LLMClient` backends
(Claude, OpenAI, Ollama, a mock for tests, or your own). To serve Citadel memory over MCP, use
[`citadeldb-mcp`](https://crates.io/crates/citadeldb-mcp).

This crate is part of the Citadel workspace.

## License

MIT OR Apache-2.0
