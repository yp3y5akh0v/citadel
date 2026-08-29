"""Model-free maintenance stays separate from operational memory."""

import gc

import citadeldb


def test_an_existing_encrypted_region_can_be_maintained_without_its_embedder(tmp_path):
    path = str(tmp_path / "maintenance.cdl")
    db = citadeldb.connect(path, key="pw", region_keys=True)
    memory = db.memory()
    memory.create_encrypted_region("notes", citadeldb.MockEmbedder(8))
    atom_id = memory.remember(
        "notes", {"kind": "fact", "text": "secret", "payload": {"owner": "alice"}}
    )
    del memory, db
    gc.collect()

    reopened = citadeldb.connect(path, key="pw", region_keys=True)
    maintenance = reopened.memory_maintenance()
    assert not hasattr(maintenance, "remember")
    assert not hasattr(maintenance, "recall")

    regions = maintenance.regions()
    assert [(r.name, r.dim, r.metric, r.encrypted, r.model_id) for r in regions] == [
        ("notes", 8, "cosine", True, "mock-fnv1a-bow-v1")
    ]
    inventory = maintenance.inventory()
    assert [(item.region.name, item.live_atoms, item.unavailable) for item in inventory] == [
        ("notes", 1, None)
    ]
    assert maintenance.count("notes") == 1
    hits = maintenance.fetch("notes", "fact", payload_filter={"owner": "alice"})
    assert [hit.id for hit in hits] == [atom_id]
    assert maintenance.verify("notes", [atom_id])[0].verdict == "authentic"

    receipt = maintenance.forget("notes", [atom_id])
    assert receipt.cryptographic_erasure and receipt.erased_count == 1
    assert maintenance.count("notes") == 0


def test_maintenance_fetch_can_page_atoms_without_knowing_their_kinds(tmp_path):
    path = str(tmp_path / "mixed-kinds.cdl")
    db = citadeldb.connect(path, key="pw", region_keys=True)
    memory = db.memory()
    memory.create_region("mixed", citadeldb.MockEmbedder(8))
    fact_id = memory.remember("mixed", {"kind": "fact", "text": "first"})
    event_id = memory.remember("mixed", {"kind": "event", "text": "second"})

    maintenance = db.memory_maintenance()
    assert [hit.id for hit in maintenance.fetch("mixed")] == [fact_id, event_id]
    assert [hit.id for hit in maintenance.fetch("mixed", "event")] == [event_id]


def test_maintenance_capability_survives_the_originating_database_handle(tmp_path):
    db = citadeldb.connect(str(tmp_path / "lifetime.cdl"), key="pw", region_keys=True)
    memory = db.memory()
    memory.create_region("notes", citadeldb.MockEmbedder(8))
    maintenance = db.memory_maintenance()

    del memory
    db.close()

    assert [region.name for region in maintenance.regions()] == ["notes"]
