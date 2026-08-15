"""LlamaIndex vector storage backed by Citadel, encrypted at rest."""

from importlib.metadata import PackageNotFoundError, version

from .vector_store import CitadelVectorStore

__all__ = ["CitadelVectorStore", "__version__"]


try:
    __version__ = version("citadeldb-llamaindex")
except PackageNotFoundError:  # running from a source tree, never installed
    __version__ = "0+unknown"
