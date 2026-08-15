"""LangChain storage backed by Citadel, encrypted at rest."""

from importlib.metadata import PackageNotFoundError, version

from .chat_history import CitadelChatMessageHistory
from .vector_store import CitadelVectorStore

__all__ = ["CitadelVectorStore", "CitadelChatMessageHistory", "__version__"]


try:
    __version__ = version("citadeldb-langchain")
except PackageNotFoundError:  # running from a source tree, never installed
    __version__ = "0+unknown"
