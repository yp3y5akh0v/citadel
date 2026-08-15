"""Haystack document storage backed by Citadel, encrypted at rest."""

from importlib.metadata import PackageNotFoundError, version

from .document_store import CitadelDocumentStore

__all__ = ["CitadelDocumentStore", "__version__"]

# Haystack wires stores by construction, so there is nothing to register.

try:
    __version__ = version("citadeldb-haystack")
except PackageNotFoundError:  # running from a source tree, never installed
    __version__ = "0+unknown"
