"""One process, one open file: the contract every adapter is built on."""

import concurrent.futures as cf
import gc
import os
import tempfile

import pytest

import citadeldb


def path(name="s.cdl"):
    return os.path.join(tempfile.mkdtemp(), name)


def opened(p, key="pw", **kw):
    return citadeldb.connect(p, key=key, region_keys=True, **kw)


# ---- what may be shared ---------------------------------------------------


def test_the_same_terms_reopen_onto_one_database():
    """A second real open would fail the file lock, so it reuses the live one."""
    p = path()
    first = opened(p)
    first.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    assert opened(p).tables() == ["t"]


def test_each_holder_gets_its_own_handle():
    """Handles are independent, so one holder cannot disable another."""
    p = path()
    assert opened(p) is not opened(p)


def test_holders_share_one_connection_so_none_sees_a_stale_schema():
    """Two connections would each cache the schema they loaded."""
    p = path()
    first, second = opened(p), opened(p)
    second.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    assert first.tables() == ["t"]


def test_closing_one_holder_leaves_the_others_working():
    p = path()
    first, second = opened(p), opened(p)
    first.close()
    assert first.is_closed is True
    assert second.is_closed is False
    assert second.tables() is not None


def test_a_with_block_does_not_close_the_file_for_everyone():
    """The idiom an adapter reaches for must not disable the application."""
    p = path()
    held = opened(p)
    with opened(p) as scoped:
        scoped.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    assert held.tables() == ["t"]


def test_in_memory_databases_are_never_pooled():
    """They have no file to contend over and no identity to key on."""
    first = citadeldb.connect(":memory:", key="pw")
    first.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    assert citadeldb.connect(":memory:", key="pw").tables() == []


def test_the_lock_is_released_once_the_last_holder_drops():
    p = path()
    db = opened(p)
    del db
    gc.collect()
    assert opened(p) is not None


def test_a_dropped_holder_does_not_release_the_file_under_a_live_one():
    p = path()
    first, second = opened(p), opened(p)
    del first
    gc.collect()
    assert second.tables() is not None


# ---- what may not ---------------------------------------------------------


def test_a_different_passphrase_is_refused_not_served():
    """Serving the first caller's passphrase would be a security surprise."""
    p = path()
    held = opened(p, key="one")
    with pytest.raises(citadeldb.EncryptionError):
        opened(p, key="two")
    assert held is not None


def test_a_different_region_keys_setting_is_refused():
    p = path()
    held = opened(p)
    with pytest.raises(citadeldb.ProgrammingError, match="region_keys"):
        citadeldb.connect(p, key="pw", region_keys=False)
    assert held is not None


def test_create_time_options_cannot_be_applied_to_an_open_file():
    p = path()
    held = opened(p)
    with pytest.raises(citadeldb.ProgrammingError, match="options"):
        citadeldb.connect(
            p, key="pw", region_keys=True, options=citadeldb.DatabaseOptions()
        )
    assert held is not None


def test_create_true_is_refused_on_an_already_open_path():
    """`create=True` asks for a new database; the open one is not that."""
    p = path()
    held = opened(p)
    with pytest.raises(citadeldb.ProgrammingError, match="create=True"):
        citadeldb.connect(p, key="pw", region_keys=True, create=True)
    assert held is not None


def test_verify_passphrase_refuses_an_in_memory_database():
    """There is no key file to answer from, so it must not guess."""
    db = citadeldb.connect(":memory:", key="pw")
    with pytest.raises(Exception, match="in-memory"):
        db.verify_passphrase("pw")


def test_a_wrong_passphrase_still_cannot_reopen_after_sharing():
    p = path()
    a, b = opened(p, key="right"), opened(p, key="right")
    del a, b
    gc.collect()
    with pytest.raises(citadeldb.EncryptionError):
        opened(p, key="wrong")


# ---- across threads -------------------------------------------------------


def test_another_thread_is_refused_while_the_opener_holds_it():
    """Touching a handle off-thread aborts, so refuse before handing one over."""
    p = path()
    held = opened(p)
    with cf.ThreadPoolExecutor(max_workers=1) as ex:
        with pytest.raises(citadeldb.ProgrammingError, match="another thread"):
            ex.submit(lambda: opened(p).tables()).result()
    assert held is not None


def test_another_thread_takes_over_once_the_opener_lets_go():
    """A closed handle must not hold the file against the rest of the process."""
    p = path()
    first = opened(p)
    first.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    first.close()

    def takeover():
        db = opened(p)  # dropped here, on the thread that opened it
        names = db.tables()
        db.close()
        return names

    with cf.ThreadPoolExecutor(max_workers=1) as ex:
        assert ex.submit(takeover).result() == ["t"]


def test_a_dead_thread_does_not_hold_the_file_forever():
    """The opening thread is gone, so nothing is left to dispatch work to."""
    p = path()

    def open_and_exit():
        db = opened(p)
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        db.close()

    with cf.ThreadPoolExecutor(max_workers=1) as ex:
        ex.submit(open_and_exit).result()
    assert opened(p).tables() == ["t"]


# ---- isolation across one shared file -------------------------------------


def two_regions(p):
    mem = opened(p).memory()
    for name in ("alpha", "beta"):
        mem.create_encrypted_region(name, citadeldb.MockEmbedder(dim=8))
        mem.remember(name, {"kind": "k", "text": f"{name} content"})
    return mem


def test_regions_on_one_file_do_not_contaminate_each_other():
    mem = two_regions(path())
    assert mem.count("alpha", "k") == 1
    assert mem.count("beta", "k") == 1
    assert [h.text for h in mem.fetch("alpha", "k")] == ["alpha content"]


def test_erasing_one_region_spares_the_other():
    """One adapter erasing its own data must not touch another's."""
    mem = two_regions(path())
    doomed = [h.id for h in mem.fetch("alpha", "k")]
    receipt = mem.forget("alpha", doomed)
    assert receipt.cryptographic_erasure is True
    assert mem.count("alpha", "k") == 0
    assert mem.count("beta", "k") == 1


def test_a_region_cannot_be_reopened_at_a_different_width():
    """Two adapters defaulting to one region name must not silently disagree."""
    mem = opened(path()).memory()
    mem.create_encrypted_region("r", citadeldb.MockEmbedder(dim=8))
    mem.create_encrypted_region("r", citadeldb.MockEmbedder(dim=8))  # idempotent
    with pytest.raises(citadeldb.DataError, match="dim"):
        mem.create_encrypted_region("r", citadeldb.MockEmbedder(dim=16))


def test_integrity_and_audit_survive_two_writers_on_one_file():
    p = path()
    db = opened(p)
    mem = db.memory()
    for name in ("alpha", "beta"):
        mem.create_encrypted_region(name, citadeldb.MockEmbedder(dim=8))
    for i in range(20):
        mem.remember("alpha", {"kind": "k", "text": f"a{i}"})
        mem.remember("beta", {"kind": "k", "text": f"b{i}"})
    assert db.integrity_check()["ok"] is True
    assert db.verify_audit_log()["chain_valid"] is True


# ---- one engine per database ----------------------------------------------


def test_a_second_handle_sees_a_region_the_first_created():
    """Engines cache regions, so two would disagree about what exists."""
    p = path()
    first = opened(p).memory()
    first.create_encrypted_region("r", citadeldb.MockEmbedder(dim=8))
    first.remember("r", {"kind": "k", "text": "written by the first"})
    assert opened(p).memory().count("r", "k") == 1


def test_an_engine_outlives_the_handle_that_built_it():
    p = path()
    db = opened(p)
    mem = db.memory()
    mem.create_encrypted_region("r", citadeldb.MockEmbedder(dim=8))
    db.close()
    mem.remember("r", {"kind": "k", "text": "after the handle closed"})
    assert opened(p).memory().count("r", "k") == 1


# ---- the engine is the thread-safe half -----------------------------------


def test_the_engine_is_usable_from_many_threads():
    """Adapters hand workers the engine, never the handle."""
    mem = opened(path()).memory()
    mem.create_encrypted_region("r", citadeldb.MockEmbedder(dim=8))
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(lambda i: mem.remember("r", {"kind": "k", "text": str(i)}), range(64)))
    assert mem.count("r", "k") == 64
