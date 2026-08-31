"""Python regressions for repairing incorrect persisted model provenance."""

import hashlib
import os
import tempfile

import pytest

import citadeldb

DIM = 32
REAL_MODEL = "text-embedding-3-small"
MOCK_MODEL = citadeldb.MockEmbedder(DIM).model_id


class CallerSideEmbedder:
    """Deterministic stand-in for caller-side embeddings."""

    dim = DIM
    metric = "cosine"
    model_id = REAL_MODEL

    def embed_with_cancel(self, texts, cancel_token):
        if cancel_token is not None:
            cancel_token.check()
        out = []
        for t in texts:
            h = hashlib.sha256(t.encode()).digest()
            out.append([h[i % len(h)] / 255.0 for i in range(DIM)])
        return out

    def embed(self, texts):
        return self.embed_with_cancel(texts, None)


class LegacyMockEmbedder:
    """The label-only identity written by releases before the versioned mock."""

    dim = DIM
    metric = "cosine"
    model_id = "mock"

    def __init__(self):
        self._inner = citadeldb.MockEmbedder(DIM)

    def embed_with_cancel(self, texts, cancel_token):
        return self._inner.embed_with_cancel(texts, cancel_token)


def shim_store():
    """Build a region whose caller-provided vectors are mislabeled as mock."""
    path = os.path.join(tempfile.mkdtemp(), "shim.cdl")
    db = citadeldb.connect(path, key="k", create=True, region_keys=True)
    mem = db.memory()
    mem.create_region("shimmed", LegacyMockEmbedder())
    embedder = CallerSideEmbedder()
    for text, embedding in zip(
        ["alpha beta", "gamma delta"],
        embedder.embed(["alpha beta", "gamma delta"]),
        strict=True,
    ):
        mem.remember(
            "shimmed", {"kind": "note", "text": text, "embedding": embedding}
        )
    return db, mem


def mock_store():
    path = os.path.join(tempfile.mkdtemp(), "mock.cdl")
    db = citadeldb.connect(path, key="k", create=True, region_keys=True)
    mem = db.memory()
    mem.create_region("shimmed", citadeldb.MockEmbedder(DIM))
    mem.remember("shimmed", {"kind": "note", "text": "alpha beta"})
    mem.remember("shimmed", {"kind": "note", "text": "gamma delta"})
    return db, mem


def recorded_model(db):
    return db.query("SELECT model_id FROM memory_regions WHERE name = 'shimmed'").rows[0][0]


def test_operational_region_identities_report_the_model_needed_for_attachment():
    _db, mem = shim_store()
    identity = mem.region("shimmed")
    assert identity is not None
    assert (
        identity.name,
        identity.dim,
        identity.metric,
        identity.encrypted,
        identity.model_id,
    ) == ("shimmed", DIM, "cosine", False, "mock")
    assert [region.name for region in mem.regions()] == ["shimmed"]


def test_the_real_model_is_refused_and_the_message_names_the_repair():
    db, mem = shim_store()
    with pytest.raises(citadeldb.DataError) as e:
        mem.create_region("shimmed", CallerSideEmbedder())
    message = str(e.value)
    assert "reclassify_region" in message, message
    assert "reembed_region" in message, message
    assert recorded_model(db) == "mock", "a refused attach must write nothing"


def test_reclassifying_lets_the_model_that_wrote_the_vectors_attach():
    db, mem = shim_store()
    mem.reclassify_region("shimmed", REAL_MODEL)
    assert recorded_model(db) == REAL_MODEL
    mem.attach_existing_region("shimmed", CallerSideEmbedder())
    assert mem.count("shimmed", "note") == 2


def test_reclassifying_ends_an_attachment_the_new_label_contradicts():
    db, mem = shim_store()
    mem.reclassify_region("shimmed", REAL_MODEL)

    with pytest.raises(citadeldb.ProgrammingError):
        mem.remember("shimmed", {"kind": "note", "text": "epsilon zeta"})

    mem.attach_existing_region("shimmed", CallerSideEmbedder())
    assert mem.count("shimmed", "note") == 2
    assert recorded_model(db) == REAL_MODEL


def test_reclassifying_changes_no_atom():
    db, mem = shim_store()
    before = db.query(
        f"SELECT id, kind, embedding, text_content, score, confidence, access_count, "
        f"created_at, accessed_at FROM memory_atoms_d{DIM}_cosine ORDER BY id"
    ).rows

    mem.reclassify_region("shimmed", REAL_MODEL)

    after = db.query(
        f"SELECT id, kind, embedding, text_content, score, confidence, access_count, "
        f"created_at, accessed_at FROM memory_atoms_d{DIM}_cosine ORDER BY id"
    ).rows
    assert after == before


def test_reclassifying_to_the_attached_model_keeps_it_attached():
    db, mem = mock_store()
    mem.reclassify_region("shimmed", MOCK_MODEL)
    assert recorded_model(db) == MOCK_MODEL
    mem.remember("shimmed", {"kind": "note", "text": "epsilon zeta"})
    assert mem.count("shimmed", "note") == 3


def test_reclassifying_an_unknown_region_says_so():
    _db, mem = shim_store()
    with pytest.raises(citadeldb.ProgrammingError):
        mem.reclassify_region("no-such-region", REAL_MODEL)


def test_attach_existing_region_never_creates_a_missing_region():
    _db, mem = shim_store()
    with pytest.raises(citadeldb.ProgrammingError):
        mem.attach_existing_region("missing", CallerSideEmbedder())
    assert mem.region("missing") is None


def test_reembed_region_runs_the_python_model_and_reports_migrated_atoms():
    db, mem = mock_store()

    report = mem.reembed_region("shimmed", CallerSideEmbedder())
    assert report.model_id == REAL_MODEL
    assert report.atoms_migrated == 2
    assert isinstance(report.ann_rebuilt, bool)
    assert isinstance(report.similarity_edges_rewoven, int)
    assert isinstance(report.similarity_edges_cleared, int)
    assert recorded_model(db) == REAL_MODEL
