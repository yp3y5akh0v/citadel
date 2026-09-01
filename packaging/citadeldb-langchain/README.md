# citadeldb-langchain

A [LangChain](https://github.com/langchain-ai/langchain) `VectorStore` and
`BaseChatMessageHistory` backed by [Citadel](https://citadeldb.dev). Encrypted at rest,
embedded in your process, and deletes that destroy the key, not just the row.

```
pip install citadeldb-langchain
```

## Vector store

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

Every document is sealed under its own key. Deleting destroys that key and then removes the
row, so any ciphertext surviving elsewhere stays unreadable.

```python
store.delete(["note-1"])  # named ids
store.clear()  # the whole corpus, deliberately
```

`delete()` with no ids is a no-op, matching `InMemoryVectorStore`. Emptying the store is
`clear()`, because erasure cannot be undone.

### Filters

```python
store.similarity_search("...", k=4, filter={"source": "handbook.pdf"})
```

The filter is evaluated inside the scan, so it narrows candidates before top-k rather than
trimming results after it, and `k` is `k`: a filter matching only distant documents still
returns them, however many others outrank them.

MMR selection runs inside Citadel over the exact vectors stored for the recalled candidates.
Stored vectors do not cross the Python boundary, and the document embedding model is not run
again during search.

## Chat history

```python
import citadeldb
from citadeldb_langchain import CitadelChatMessageHistory

history = CitadelChatMessageHistory(
    "user-123",
    "chats.cdl",
    key="your-passphrase",
    # Required, and no default. This history reads by session id rather than by
    # vector, so the mock is the honest choice unless you want semantic recall
    # over turns; either way the region records which model wrote it.
    embedder=citadeldb.MockEmbedder(dim=64),
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
        key="...",
        embedder=citadeldb.MockEmbedder(dim=64),
    ),
    input_messages_key="input",
    history_messages_key="history",
)
```

## Notes

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so the vector store and the chat history can sit on
one encrypted database; construct them on the same thread.

## License

Apache-2.0
