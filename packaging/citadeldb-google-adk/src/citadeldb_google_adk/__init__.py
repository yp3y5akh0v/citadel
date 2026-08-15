"""Google ADK memory backed by Citadel, encrypted at rest."""

from importlib.metadata import PackageNotFoundError, version

from .memory import CitadelMemoryService

__all__ = ["CitadelMemoryService", "__version__"]


try:
    __version__ = version("citadeldb-google-adk")
except PackageNotFoundError:  # running from a source tree, never installed
    __version__ = "0+unknown"
