"""Memory engine: regions, recall/remember, forgetting, attestation."""

from typing import TYPE_CHECKING

from citadeldb._core import (
    AtomAttestation,
    AtomHit,
    ErasureReceipt,
    EvictionPolicy,
    Memory,
    MockEmbedder,
    MockReranker,
    RecallOptions,
    SlotErasure,
)

__all__ = [
    "Memory",
    "MockEmbedder",
    "MockReranker",
    "EvictionPolicy",
    "RecallOptions",
    "AtomHit",
    "ErasureReceipt",
    "SlotErasure",
    "AtomAttestation",
]

# CandleEmbedder + CrossEncoder are only in builds with the `candle-embed` feature.
if TYPE_CHECKING:
    from citadeldb._core import CandleEmbedder as CandleEmbedder
    from citadeldb._core import CrossEncoder as CrossEncoder
else:
    try:
        from citadeldb._core import CandleEmbedder, CrossEncoder
    except ImportError:

        class CandleEmbedder:
            """Available only in builds with the ``candle-embed`` feature."""

            def __init__(self, *args, **kwargs):
                raise RuntimeError(
                    "CandleEmbedder needs a build with the `candle-embed` feature "
                    "(not in the default wheel). Bring your own embedder, or build "
                    "from source: maturin build --features candle-embed."
                )

        class CrossEncoder:
            """Available only in builds with the ``candle-embed`` feature."""

            def __init__(self, *args, **kwargs):
                raise RuntimeError(
                    "CrossEncoder needs a build with the `candle-embed` feature "
                    "(not in the default wheel). Bring your own reranker, or build "
                    "from source: maturin build --features candle-embed."
                )

    __all__.append("CandleEmbedder")
    __all__.append("CrossEncoder")
