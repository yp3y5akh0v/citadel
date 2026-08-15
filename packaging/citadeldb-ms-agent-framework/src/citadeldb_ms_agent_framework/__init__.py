"""Microsoft Agent Framework storage backed by Citadel, encrypted at rest."""

from importlib.metadata import PackageNotFoundError, version

from .history import CitadelHistoryProvider
from .memory import CitadelContextProvider

__all__ = ["CitadelContextProvider", "CitadelHistoryProvider", "__version__"]


try:
    __version__ = version("citadeldb-ms-agent-framework")
except PackageNotFoundError:  # running from a source tree, never installed
    __version__ = "0+unknown"
