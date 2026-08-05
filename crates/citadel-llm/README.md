# citadeldb-llm

Provider-neutral LLM client layer for [Citadel](https://github.com/yp3y5akh0v/citadel):
the `LLMClient` trait, request/response types, canonical request hashing
(`canonical_json` / `request_hash`), non-secret `ClientRequestIdentity`, and
feature-gated Claude / OpenAI / Ollama / Gemini backends behind a single
factory (`factory::from_env` / `factory::build`).

## Features

- `claude`, `openai`, `ollama`, `gemini` - HTTP backends (native only)
- `test-util` - the `testing` doubles toolkit (scripted, capturing, storm)

## Environment

`factory::from_env(prefix, default_provider, default_model)` reads:

| Variable | Applies to | Effect |
|---|---|---|
| `{prefix}_PROVIDER` | all | Provider name; falls back to `default_provider` |
| `{prefix}_MODEL` | all | Model id; falls back to `default_model` |
| `ANTHROPIC_API_KEY` | `claude` | Required; absent is a hard error |
| `OPENAI_API_KEY` | `openai` | Required; absent is a hard error |
| `GEMINI_API_KEY` | `gemini` | Required; absent is a hard error |
| `OPENAI_BASE_URL` | `openai` | Endpoint override; default `https://api.openai.com/v1` |
| `OLLAMA_BASE_URL` | `ollama` | Endpoint override; default `http://localhost:11434/v1` |
| `CITADEL_GEMINI_REASONING_EFFORT` | `gemini` | Reasoning effort; omitted from the wire when unset |
| `CITADEL_AI_LLM_TIMEOUT_SECS` | HTTP backends | Receive budget in seconds, default 120; send and global deadlines derive from it |

`ollama` needs no key. `factory::from_env_with_timeouts` and
`factory::build_with_timeouts` take deadlines directly and ignore
`CITADEL_AI_LLM_TIMEOUT_SECS`.

## License

Apache-2.0
