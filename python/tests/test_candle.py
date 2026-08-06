"""In-process Candle embedder (opt-in: needs a candle-embed build + local model)."""

import os

import pytest

import citadeldb
from citadeldb import _core

MODEL = os.environ.get("CITADEL_BGE_SMALL_DIR", "")

pytestmark = pytest.mark.skipif(
    not hasattr(_core, "CandleEmbedder") or not MODEL or not os.path.isdir(MODEL),
    reason="set CITADEL_BGE_SMALL_DIR to a local bge-small model (needs a candle-embed build)",
)


def test_candle_embedder_loads_and_embeds():
    emb = citadeldb.CandleEmbedder(MODEL, preset="bge-small")
    assert emb.dim == 384 and emb.metric == "cosine"
    assert emb.model_id == "bge-small-en-v1.5"
    v = emb.embed(["a", "b c d"])
    assert len(v) == 2 and len(v[0]) == 384


def test_candle_semantic_recall():
    emb = citadeldb.CandleEmbedder(MODEL, preset="bge-small")
    mem = citadeldb.connect(key="k").memory()
    mem.create_region("kb", emb)
    for t in ["The Eiffel Tower is in Paris.", "Cats are small mammals.", "The sun is a star."]:
        mem.remember("kb", {"kind": "fact", "text": t})
    hits = mem.recall("kb", text="Where is the Eiffel Tower located?", k=1)
    assert "Eiffel" in hits[0].text


def test_unknown_preset_raises():
    with pytest.raises(ValueError):
        citadeldb.CandleEmbedder(MODEL, preset="not-a-model")
