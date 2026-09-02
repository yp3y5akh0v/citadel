# Packages

Python framework adapters, the MCP server, and desktop installers for Citadel.

Every adapter requires `citadeldb>=2.2,<3`. Framework-specific version
requirements are listed in each package's README and manifest.

## Agent framework adapters

Each implements a framework storage interface and stores records in encrypted regions.
Deletes destroy the selected records' keys; pre-erasure backups, snapshots, and exported
plaintext remain outside that erasure. Text-query adapters use hybrid recall; vector-store
adapters rank on the embeddings supplied by their framework.

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

Each package's README contains setup, supported operations, and limitations.

## Server

| Package | What it is | Source |
|---|---|---|
| [`citadeldb-mcp`](https://pypi.org/project/citadeldb-mcp/) | MCP server, registered as `dev.citadeldb/mcp` | [`../crates/citadel-mcp`](../crates/citadel-mcp) |

## Desktop installers

Citadel Studio is packaged for Linux, macOS, and Windows.
See [downloads](https://citadeldb.dev/download/) for release availability.

| Platform | Installer | Architecture |
|---|---|---|
| Linux | `.AppImage` | x86_64 |
| macOS | `.dmg` | Apple Silicon or Intel |
| Windows | `.msi` | x86_64 |

The macOS app is ad-hoc signed, not Developer ID signed or notarized; the DMG is unsigned.
Gatekeeper may require **Open Anyway** in **System Settings > Privacy & Security**.
The Windows installer and executable are unsigned; SmartScreen may warn or block
installation. Organization security policies may also prevent installation.

The release workflow produces `.sha256` checksum files and GitHub build-provenance attestations.
These do not replace OS publisher signing or suppress its warnings. Verify provenance with:

```console
gh attestation verify <installer> --repo yp3y5akh0v/citadel
```

Installers include the project licence and applicable third-party notices.
See [third-party licences](licenses/THIRD_PARTY_LICENSES.html).

## Sharing one database

Adapters constructed on the same thread can share one encrypted database through
`citadeldb.connect`. Use the same passphrase and distinct region names:

```python
import citadeldb
from citadeldb_langgraph import CitadelStore
from citadeldb_openai_agents import CitadelSession

PATH, KEY = "agent.cdl", "your-passphrase"
EMB = citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large")

store = CitadelStore(PATH, key=KEY, embedder=EMB)
session = CitadelSession(
    "user-123", db_path=PATH, key=KEY, embedder=EMB
)
```

This example requires the [Candle source build and model setup](../python/README.md#local-candle-models).
The default Python wheel instead accepts a [bring-your-own semantic embedder](../python/README.md#semantic-embeddings).
Use the same model identity and encoding settings each time a region is reopened.

The default regions above are `store` and `sessions`. Regions belong to one adapter
family; record schemas are not interchangeable. Closing one handle leaves other
holders usable. The file lock is released after the last handle and engine are dropped.

Connection-sharing rules:

| Second open | Result |
|---|---|
| Same thread, same terms | another handle onto it |
| Wrong passphrase | `EncryptionError`, as on a first open |
| Different `region_keys`, any `options`, or `create=True` | `ProgrammingError` |
| Another thread, while a handle is still open | `ProgrammingError` |
| Another thread, once every handle has closed | it takes the file over |
| Another OS process | `OperationalError` |

Connections belong to their opening thread. The shared `Memory` engine can be passed
to worker threads.
