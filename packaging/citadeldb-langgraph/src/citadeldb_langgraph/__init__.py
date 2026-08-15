"""LangGraph store backed by Citadel: encrypted, semantic, real deletes."""

from importlib.metadata import PackageNotFoundError, version

from .store import CitadelStore

__all__ = ["CitadelStore", "__version__"]

try:
    __version__ = version("citadeldb-langgraph")
except PackageNotFoundError:  # running from a source tree, never installed
    __version__ = "0+unknown"
