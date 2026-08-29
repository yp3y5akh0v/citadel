"""CrewAI memory backed by Citadel, encrypted at rest."""

from importlib.metadata import PackageNotFoundError, version
from typing import Any

from .backend import CitadelBackend

__all__ = ["CitadelBackend", "__version__", "use_citadel"]

try:
    __version__ = version("citadeldb-crewai")
except PackageNotFoundError:  # running from a source tree, never installed
    __version__ = "0+unknown"


# Claiming CrewAI's default spec is what routes an unconfigured crew here.
_DEFAULT_SPEC = "lancedb"
_EXPLICIT_SPEC = "citadel"


def use_citadel(
    path: str = "crew_memory.cdl", key: str = "", *, embedder: Any, **kwargs: Any
) -> CitadelBackend:
    """Route CrewAI memory through Citadel, process-wide."""
    from crewai.memory.storage.factory import set_memory_storage_factory

    backend = CitadelBackend(path, key, embedder=embedder, **kwargs)
    set_memory_storage_factory(
        lambda spec: backend if spec in (_DEFAULT_SPEC, _EXPLICIT_SPEC) else None
    )
    return backend
