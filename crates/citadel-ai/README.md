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

`BudgetUsage.tokens` and `cost_usd` are optional cumulative totals. Unknown token
usage stops the run with `TerminatedBy::BudgetUnavailable`; unknown cost does so
when a cost cap is configured. A configured cost cap must be finite and nonnegative.
The agent does not automatically retry LLM requests.

`AgentBudget::check` returns `BudgetStop`, distinguishing exceeded caps,
unavailable usage, and invalid configuration.

If recording an LLM trace fails, `AgentError::TracePersistence` retains the
affected calls' requests, identities, responses or provider errors, cumulative
usage, and the storage error. Its `confirmed_persisted` field counts the prefix of
acknowledged writes. The failed write may also have persisted, so reconcile the
retained calls with stored traces before retrying persistence or making new calls.

## License

Apache-2.0
