# citadeldb-langchain

A [LangChain](https://github.com/langchain-ai/langchain) `VectorStore` and
`BaseChatMessageHistory` backed by [Citadel](https://citadeldb.dev). Encrypted at rest,
embedded in your process, and deletes that destroy the key, not just the row.

```
pip install citadeldb-langchain
```

Requires `citadeldb>=2.2,<3` and `langchain-core>=0.3.22,<2`.

## Vector store

The example uses `langchain-openai` (`pip install langchain-openai`) and requires
`OPENAI_API_KEY`. Text is sent to the configured embedding provider; use a local
LangChain `Embeddings` implementation to keep embedding inference local.

```python
from langchain_openai import OpenAIEmbeddings
from citadeldb_langchain import CitadelVectorStore

store = CitadelVectorStore(OpenAIEmbeddings(), "corpus.cdl", key="your-passphrase")

store.add_texts(["the deploy failed because the disk was full"], ids=["note-1"])
store.similarity_search("why did the release break?", k=1)

retriever = store.as_retriever(search_kwargs={"k": 4})
```

The width is read from your embedding model on construction, so nothing has to be
configured to match it. Pass `dim=` to skip that probe.
Models exposing `model_id`, `model`, or `model_name` record that identity automatically,
in that order. For a custom embedding without any of those attributes, pass a stable
`model_id=` explicitly; Citadel refuses to guess from the Python class name.

Adding an id that is already stored replaces it, so re-indexing a document does not
duplicate it.

### Deletes destroy the key

Every document is sealed under its own key. Deleting destroys that key and removes the
row. Pre-erasure backups or snapshots containing keys, and exported plaintext, are outside
that erasure.

```python
store.delete(["note-1"])  # named ids
store.clear()  # the whole corpus
```

`delete()` with no ids is a no-op. Use `clear()` to empty the store.

### Filters

```python
store.similarity_search("...", k=4, filter={"source": "handbook.pdf"})
```

Filters narrow candidates before final top-k selection.

MMR selection runs inside Citadel over the exact vectors stored for the recalled candidates.
Stored vectors do not cross the Python boundary, and the document embedding model is not run
again during search.

```python
retriever = store.as_retriever(
    search_type="mmr",
    search_kwargs={"k": 4, "fetch_k": 20, "lambda_mult": 0.5},
)
```

## Chat history

Chat history reads complete sessions by id. This example uses local e5-large and
requires the [Candle source build and model setup](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#local-candle-models).
The default wheel accepts a [bring-your-own semantic embedder](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#semantic-embeddings).

```python
import citadeldb
from citadeldb_langchain import CitadelChatMessageHistory

embedder = citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large")
history = CitadelChatMessageHistory(
    "user-123",
    "chats.cdl",
    key="your-passphrase",
    embedder=embedder,
)
history.add_user_message("remember my dog is called Mochi")
history.messages
```

Messages round-trip through LangChain's own serialization, so tool calls, block content
and `additional_kwargs` all survive. `clear()` destroys each message's key, so a cleared
conversation is unreadable.

Use it with `RunnableWithMessageHistory` the same way as any other history:

```python
import citadeldb
from langchain_core.runnables.history import RunnableWithMessageHistory

chain = RunnableWithMessageHistory(
    runnable,  # your chain
    lambda session_id: CitadelChatMessageHistory(
        session_id,
        "chats.cdl",
        key="your-passphrase",
        embedder=embedder,
    ),
    input_messages_key="input",
    history_messages_key="history",
)
```

## Notes

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so the vector store and the chat history can sit on
one encrypted database; construct them on the same thread.

A custom chat-history embedder must expose `dim`, `metric`, `model_id`, and
`embed_with_cancel(texts, cancel_token)`. Accept `None` as the token; otherwise check
it between bounded batches. An asymmetric model can also provide
`embed_queries_with_cancel`.

## License

Apache-2.0
