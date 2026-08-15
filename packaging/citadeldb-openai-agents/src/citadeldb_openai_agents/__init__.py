"""OpenAI Agents SDK sessions backed by Citadel, encrypted at rest."""

from importlib.metadata import PackageNotFoundError, version

from .session import CitadelSession, CitadelSessionStore

__all__ = ["CitadelSession", "CitadelSessionStore", "__version__"]


try:
    __version__ = version("citadeldb-openai-agents")
except PackageNotFoundError:  # running from a source tree, never installed
    __version__ = "0+unknown"
