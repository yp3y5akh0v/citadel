"""Default-wheel fallback: candle-only classes raise a helpful RuntimeError."""

import pytest

import citadeldb.memory as memory
from citadeldb import _core

pytestmark = pytest.mark.skipif(
    hasattr(_core, "CrossEncoder"),
    reason="candle-embed build: the real CrossEncoder/CandleEmbedder are present",
)


@pytest.mark.parametrize("name", ["CrossEncoder", "CandleEmbedder"])
def test_candle_only_class_raises_build_hint(name):
    cls = getattr(memory, name)
    with pytest.raises(RuntimeError, match="candle-embed"):
        cls()
