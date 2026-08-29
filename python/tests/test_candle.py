"""In-process Candle embedder (opt-in: needs a candle-embed build + local model)."""

import os
import re

import pytest

import citadeldb
from citadeldb import _core

MODEL = os.environ.get("CITADEL_EMBEDDER_DIR", "")

pytestmark = pytest.mark.skipif(
    not hasattr(_core, "CandleEmbedder") or not MODEL or not os.path.isdir(MODEL),
    reason="set CITADEL_EMBEDDER_DIR to a local e5-large dir (needs a candle-embed build)",
)


def test_candle_embedder_loads_and_embeds():
    emb = citadeldb.CandleEmbedder(MODEL, preset="e5-large")
    assert emb.dim == 1024 and emb.metric == "cosine"
    assert re.fullmatch(
        r"e5-large@citadel-candle-v1-p1:[0-9a-f]{64}", emb.model_id
    )
    v = emb.embed(["a", "b c d"])
    assert len(v) == 2 and len(v[0]) == 1024


def test_candle_semantic_recall():
    emb = citadeldb.CandleEmbedder(MODEL, preset="e5-large")
    mem = citadeldb.connect(key="k").memory()
    mem.create_region("kb", emb)
    for t in ["The Eiffel Tower is in Paris.", "Cats are small mammals.", "The sun is a star."]:
        mem.remember("kb", {"kind": "fact", "text": t})
    hits = mem.recall("kb", text="Where is the Eiffel Tower located?", k=1)
    assert "Eiffel" in hits[0].text


def test_unknown_preset_raises():
    with pytest.raises(ValueError):
        citadeldb.CandleEmbedder(MODEL, preset="not-a-model")
