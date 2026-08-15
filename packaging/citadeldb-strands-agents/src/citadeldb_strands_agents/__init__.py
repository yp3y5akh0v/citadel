"""Strands Agents session storage backed by Citadel, encrypted at rest."""

from importlib.metadata import PackageNotFoundError, version

from .session import CitadelSessionManager

__all__ = ["CitadelSessionManager", "__version__"]


try:
    __version__ = version("citadeldb-strands-agents")
except PackageNotFoundError:  # running from a source tree, never installed
    __version__ = "0+unknown"
