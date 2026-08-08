"""CrewAI memory backed by Citadel: encrypted at rest, with deletes that destroy the key."""

from importlib.metadata import PackageNotFoundError, version

from .backend import CitadelBackend

__all__ = ["CitadelBackend", "use_citadel", "__version__"]

try:
    __version__ = version("citadeldb-crewai")
except PackageNotFoundError:  # running from a source tree, never installed
    __version__ = "0+unknown"


# CrewAI's default storage spec. Claiming it is what routes an ordinary crew to Citadel;
# a crew naming any other backend picked it deliberately and is left alone.
_DEFAULT_SPEC = "lancedb"
_EXPLICIT_SPEC = "citadel"


def use_citadel(path: str = "crew_memory.cdl", key: str = "crewai", **kwargs) -> CitadelBackend:
    """Route CrewAI memory through Citadel, process-wide.

    One backend is built and reused: Citadel is embedded and one handle owns the file.

    Crews that name a different backend keep it, so this cannot silently displace a
    deliberate `storage="qdrant-edge"` or a LanceDB path. Pass `storage="citadel"` to opt a
    single crew in without calling this at all.
    """
    from crewai.memory.storage.factory import set_memory_storage_factory

    backend = CitadelBackend(path, key, **kwargs)
    set_memory_storage_factory(
        lambda spec: backend if spec in (_DEFAULT_SPEC, _EXPLICIT_SPEC) else None
    )
    return backend
