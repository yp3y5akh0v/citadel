# Packages

Distribution packages built from this workspace. Each adapter is versioned from the release
tag and `citadeldb-mcp` from the workspace crate version; each is built and published by
its own workflow under [`.github/workflows/`](../.github/workflows).

Every adapter requires `citadeldb>=2.2,<3`. CrewAI, Haystack, LangChain, and LlamaIndex
consume expanded hit-scoring or core MMR APIs; the other five use the cancellation-aware
embedder protocol added in 2.2.
Their test jobs build the core wheel in-run and constrain adapter dependency resolution
to that exact version, so PyPI cannot silently replace the wheel under test.

## Agent framework adapters

Each implements the framework's own storage interface, so a project swaps one constructor
and keeps every other feature. Deletes destroy the record's key, not just its row, and
the database is encrypted with no unencrypted mode. Where the framework hands over a query
string, search is hybrid vector + keyword recall; the vector-store adapters rank on the
embedding the framework supplies.

| Framework | Package | Implements | Source |
|---|---|---|---|
| [LangGraph](https://github.com/langchain-ai/langgraph) | [`citadeldb-langgraph`](https://pypi.org/project/citadeldb-langgraph/) | `langgraph.store.base.BaseStore` | [`citadeldb-langgraph/`](citadeldb-langgraph) |
| [CrewAI](https://github.com/crewAIInc/crewAI) | [`citadeldb-crewai`](https://pypi.org/project/citadeldb-crewai/) | `crewai.memory.storage.backend.StorageBackend` | [`citadeldb-crewai/`](citadeldb-crewai) |
| [OpenAI Agents SDK](https://github.com/openai/openai-agents-python) | [`citadeldb-openai-agents`](https://pypi.org/project/citadeldb-openai-agents/) | `agents.memory.session.Session` | [`citadeldb-openai-agents/`](citadeldb-openai-agents) |
| [Google ADK](https://github.com/google/adk-python) | [`citadeldb-google-adk`](https://pypi.org/project/citadeldb-google-adk/) | `google.adk.memory.BaseMemoryService` | [`citadeldb-google-adk/`](citadeldb-google-adk) |
| [LlamaIndex](https://github.com/run-llama/llama_index) | [`citadeldb-llamaindex`](https://pypi.org/project/citadeldb-llamaindex/) | `llama_index.core.vector_stores.BasePydanticVectorStore` | [`citadeldb-llamaindex/`](citadeldb-llamaindex) |
| [LangChain](https://github.com/langchain-ai/langchain) | [`citadeldb-langchain`](https://pypi.org/project/citadeldb-langchain/) | `langchain_core.vectorstores.VectorStore`, `langchain_core.chat_history.BaseChatMessageHistory` | [`citadeldb-langchain/`](citadeldb-langchain) |
| [Haystack](https://github.com/deepset-ai/haystack) | [`citadeldb-haystack`](https://pypi.org/project/citadeldb-haystack/) | `haystack.document_stores.types.DocumentStore` | [`citadeldb-haystack/`](citadeldb-haystack) |
| [Microsoft Agent Framework](https://github.com/microsoft/agent-framework) | [`citadeldb-ms-agent-framework`](https://pypi.org/project/citadeldb-ms-agent-framework/) | `agent_framework.HistoryProvider`, `agent_framework.ContextProvider` | [`citadeldb-ms-agent-framework/`](citadeldb-ms-agent-framework) |
| [Strands Agents](https://github.com/strands-agents/harness-sdk) | [`citadeldb-strands-agents`](https://pypi.org/project/citadeldb-strands-agents/) | `strands.session.SessionRepository` | [`citadeldb-strands-agents/`](citadeldb-strands-agents) |

Each package's README carries its own usage, and each ships its own test suite run against
the built wheel in CI.

## Server

| Package | What it is | Source |
|---|---|---|
| [`citadeldb-mcp`](https://pypi.org/project/citadeldb-mcp/) | MCP server, registered as `dev.citadeldb/mcp` | [`../crates/citadel-mcp`](../crates/citadel-mcp) |

## Sharing one database

Citadel is embedded, and a database file is held under a whole-file exclusive lock, so a
second open of the same path fails even inside one process. `citadeldb.connect` returns a
handle onto the database this process already holds instead, so two adapters can back onto
one encrypted file rather than needing a file each:

```python
import citadeldb
from citadeldb_langgraph import CitadelStore
from citadeldb_openai_agents import CitadelSession

PATH, KEY = "agent.cdl", "your-passphrase"
# No adapter defaults an embedder: silent substitution would change ranking
# semantics and persist different provenance.
# This example performs keyed state and transcript reads only, so the mock avoids
# model work that neither adapter invokes here. Use a real embedder before semantic search.
EMB = citadeldb.MockEmbedder(dim=64)

store = CitadelStore(PATH, key=KEY, embedder=EMB)            # LangGraph state
session = CitadelSession(                                    # Agents SDK transcripts
    "user-123", db_path=PATH, key=KEY, embedder=EMB
)
```

There is one database here, not two. Each adapter writes to its own region inside it
(`store` and `sessions` by default), so the state and the transcripts stay separate without
a second database to open, back up or erase.

Treat a region as owned by one adapter family. Multiple adapter types may reuse the same
database, but they must use distinct region names: their record kinds, keyed identities,
and payload schemas are not an interchange format and can overlap.

Every adapter keeps the constructor shape of the framework it plugs into, which is why
`CitadelSession` takes the session id first: the SDK's own `SQLiteSession(session_id,
db_path=...)` does the same.

Each holder gets its own handle over one shared connection. Closing is per holder, so an
adapter's `with` block cannot disable the application's handle, and the file is released
once the last handle and any engine built from it have dropped.

The exclusive lock itself is unchanged. Sharing removes only the second open inside the
process that already owns the file; everything else is still refused, and each refusal
names its cause:

| Second open | Result |
|---|---|
| Same thread, same terms | another handle onto it |
| Wrong passphrase | `EncryptionError`, as on a first open |
| Different `region_keys`, any `options`, or `create=True` | `ProgrammingError` |
| Another thread, while a handle is still open | `ProgrammingError` |
| Another thread, once every handle has closed | it takes the file over |
| Another OS process | `OperationalError` |

The passphrase is checked against the key file, and the key file is hashed, so anything
that rewrites it takes effect at once: `change_passphrase` and `restore_key_from_backup`
both admit the new passphrase and refuse the old. What is retained is a digest under a
per-process random key, never a passphrase.

A connection belongs to the thread that opened it, so dispatch work to that thread or pass
the `Memory` engine, which is safe to use from any thread and is what these adapters hand
to their workers. One engine serves the whole database, so a region one adapter creates is
visible to the next.
