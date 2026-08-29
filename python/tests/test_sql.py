"""Encrypted SQL surface: connect, execute/query (+ params), scripts, admin."""

import datetime as dt
import gc
import os
import subprocess
import sys
import tempfile
import threading
import time

import pytest

import citadeldb


def fresh(**kw):
    return citadeldb.connect(key="testkey", **kw)


def test_roundtrip_and_queryresult():
    db = fresh()
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)")
    assert db.execute("INSERT INTO t VALUES (1, 'alice')") == 1
    r = db.query("SELECT id, name FROM t")
    assert r.columns == ["id", "name"]
    assert r.rows == [(1, "alice")]
    assert len(r) == 1
    assert r.to_dicts() == [{"id": 1, "name": "alice"}]


def test_value_types_via_params():
    db = fresh()
    db.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, i INTEGER, r REAL, t TEXT, b BLOB, n TEXT)")
    db.execute("INSERT INTO v VALUES (1, $1, $2, $3, $4, $5)", [42, 3.5, "hi", b"\x00\xff", None])
    row = db.query("SELECT i, r, t, b, n FROM v").rows[0]
    assert row == (42, 3.5, "hi", b"\x00\xff", None)


def test_bool_param_distinct_from_int():
    db = fresh()
    db.execute("CREATE TABLE q(id INTEGER PRIMARY KEY, f BOOLEAN)")
    db.execute("INSERT INTO q VALUES (1, $1)", [True])
    assert db.query("SELECT f FROM q").rows[0][0] is True


def test_numpy_bool_param_binds_as_boolean():
    import numpy as np

    db = fresh()
    db.execute("CREATE TABLE nb(id INTEGER PRIMARY KEY, f BOOLEAN)")
    db.execute("INSERT INTO nb VALUES (1, $1)", [np.bool_(True)])  # not a Python bool
    db.execute("INSERT INTO nb VALUES (2, $1)", [np.bool_(False)])
    rows = db.query("SELECT f FROM nb ORDER BY id").rows
    assert rows[0][0] is True and rows[1][0] is False


def test_parameterized_filter():
    db = fresh()
    db.execute("CREATE TABLE p(id INTEGER PRIMARY KEY, name TEXT)")
    for i, n in enumerate(["a", "b", "c"], start=1):
        db.execute("INSERT INTO p VALUES ($1, $2)", [i, n])
    assert db.query("SELECT name FROM p WHERE id = $1", [2]).rows == [("b",)]


def test_execute_script():
    db = fresh()
    out = db.execute_script(
        "CREATE TABLE s(id INTEGER PRIMARY KEY); INSERT INTO s VALUES (1); INSERT INTO s VALUES (2);"
    )
    assert len(out) == 3
    assert db.query("SELECT COUNT(*) FROM s").rows[0][0] == 2


def test_tables_and_transaction_state():
    db = fresh()
    db.execute("CREATE TABLE a(id INTEGER PRIMARY KEY)")
    db.execute("CREATE TABLE b(id INTEGER PRIMARY KEY)")
    assert {"a", "b"} <= set(db.tables())
    assert db.in_transaction() is False
    db.execute("BEGIN")
    assert db.in_transaction() is True
    db.execute("INSERT INTO a VALUES (1)")
    db.execute("ROLLBACK")
    assert db.in_transaction() is False
    assert db.query("SELECT COUNT(*) FROM a").rows[0][0] == 0


def test_file_db_create_open_wrong_passphrase():
    path = os.path.join(tempfile.mkdtemp(), "f.cdl")
    db = citadeldb.connect(path, key="right", create=True)
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    db.execute("INSERT INTO t VALUES (5)")
    del db
    db2 = citadeldb.connect(path, key="right")
    assert db2.query("SELECT id FROM t").rows[0][0] == 5
    del db2
    with pytest.raises(citadeldb.EncryptionError):
        citadeldb.connect(path, key="wrong")


def test_admin_stats_integrity():
    db = fresh()
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    st = db.stats()
    assert "entry_count" in st and isinstance(st["merkle_root"], bytes)
    ic = db.integrity_check(quiet=True)
    assert ic["ok"] is True and ic["error_count"] == 0
    assert ic["tampered_count"] == 0 and ic["errors"] == []


def test_cancel_token_is_one_shot_and_can_interrupt_a_running_query():
    db = fresh()
    db.execute("CREATE TABLE cancel_rows(id INTEGER PRIMARY KEY)")
    values = ", ".join(f"({i})" for i in range(5000))
    db.execute(f"INSERT INTO cancel_rows VALUES {values}")
    token = citadeldb.CancelToken()
    db.set_cancel(token)

    def cancel_soon():
        time.sleep(0.02)
        token.cancel()

    stopper = threading.Thread(target=cancel_soon)
    stopper.start()
    with pytest.raises(citadeldb.OperationalError, match="cancel"):
        db.query("SELECT SUM(a.id * b.id) FROM cancel_rows a CROSS JOIN cancel_rows b")
    stopper.join()
    assert token.is_cancelled is True
    with pytest.raises(citadeldb.OperationalError, match="cancel"):
        db.query("SELECT 1")

    db.set_cancel(None)
    assert db.query("SELECT 1").rows == [(1,)]


def test_backup_and_change_passphrase():
    d = tempfile.mkdtemp()
    path = os.path.join(d, "m.cdl")
    db = citadeldb.connect(path, key="pw1", create=True)
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    db.execute("INSERT INTO t VALUES (1)")
    backup = os.path.join(d, "backup.cdl")
    db.backup(backup)
    assert os.path.exists(backup)
    db.change_passphrase("pw1", "pw2")
    del db
    db2 = citadeldb.connect(path, key="pw2")
    assert db2.query("SELECT COUNT(*) FROM t").rows[0][0] == 1


def test_a_rekey_is_honoured_by_the_open_file_table():
    """A cached digest would admit the revoked key and refuse the current."""
    d = tempfile.mkdtemp()
    path = os.path.join(d, "rekey.cdl")
    db = citadeldb.connect(path, key="old", create=True)
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    db.change_passphrase("old", "new")

    assert citadeldb.connect(path, key="new").tables() == ["t"]
    with pytest.raises(citadeldb.EncryptionError):
        citadeldb.connect(path, key="old")


def test_restore_refuses_to_race_an_open_database():
    d = tempfile.mkdtemp()
    path, backup = os.path.join(d, "restore.cdl"), os.path.join(d, "escrow.bin")
    db = citadeldb.connect(path, key="old", create=True)
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    db.export_key_backup("old", "escrow-pass", backup)
    with pytest.raises(citadeldb.OperationalError, match="locked"):
        citadeldb.Database.restore_key_from_backup(backup, "escrow-pass", "new", path)

    assert db.tables() == ["t"]
    with pytest.raises(citadeldb.EncryptionError):
        citadeldb.connect(path, key="new")


def test_verify_passphrase_reads_the_key_file():
    db = citadeldb.connect(os.path.join(tempfile.mkdtemp(), "v.cdl"), key="pw",
                           create=True)
    assert db.verify_passphrase("pw") is True
    assert db.verify_passphrase("nope") is False
    db.change_passphrase("pw", "pw2")
    assert db.verify_passphrase("pw") is False
    assert db.verify_passphrase("pw2") is True


def test_a_wrong_passphrase_fails_alike_open_or_not():
    """Otherwise the exception itself reports whether this process holds the file."""
    d = tempfile.mkdtemp()
    closed, held = os.path.join(d, "closed.cdl"), os.path.join(d, "held.cdl")
    citadeldb.connect(closed, key="pw", create=True).close()
    keep_open = citadeldb.connect(held, key="pw", create=True)

    with pytest.raises(citadeldb.EncryptionError) as fresh:
        citadeldb.connect(closed, key="wrong")
    with pytest.raises(citadeldb.EncryptionError) as reopen:
        citadeldb.connect(held, key="wrong")
    assert type(fresh.value) is type(reopen.value)
    assert keep_open.tables() == []


def test_a_file_this_process_still_holds_reopens_onto_the_live_database():
    """An engine outlives its handle and still holds the file."""
    path = os.path.join(tempfile.mkdtemp(), "held.cdl")
    db = citadeldb.connect(path, key="pw", create=True, region_keys=True)
    engine = db.memory()
    engine.create_encrypted_region("r", citadeldb.MockEmbedder(dim=8))
    db.close()

    again = citadeldb.connect(path, key="pw", region_keys=True)
    # One engine per database, so the region the closed handle made is still there.
    assert again.memory().count("r", "k") == 0
    assert again.is_closed is False


def test_database_rejects_foreign_thread_use_but_can_be_destroyed_there(tmp_path):
    path = str(tmp_path / "foreign-drop.cdl")
    db = citadeldb.connect(path, key="pw", create=True, region_keys=True)
    memory = db.memory()
    memory.create_region("r", citadeldb.MockEmbedder(8))
    maintenance = db.memory_maintenance()
    holder = [db, memory, maintenance]
    del db, memory, maintenance
    outcome = []

    def use_then_drop_final_reference():
        handle, memory, maintenance = holder
        for operation in (handle.tables, handle.memory):
            try:
                operation()
            except BaseException as error:
                outcome.append((type(error), str(error)))
        outcome.append(memory.count("r", "fact"))
        outcome.append(maintenance.count("r"))
        holder.clear()
        del handle, memory, maintenance
        gc.collect()

    worker = threading.Thread(target=use_then_drop_final_reference)
    worker.start()
    worker.join()

    assert len(outcome) == 4
    assert all(item[0] is citadeldb.ProgrammingError for item in outcome[:2])
    assert all("thread that opened" in item[1] for item in outcome[:2])
    assert outcome[2:] == [0, 0]
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            "import citadeldb, sys; db = citadeldb.connect(sys.argv[1], key='pw'); db.close()",
            path,
        ],
        capture_output=True,
        text=True,
    )
    assert probe.returncode == 0, probe.stderr


def test_errors():
    db = fresh()
    with pytest.raises(citadeldb.ProgrammingError):
        db.query("SELECT * FROM does_not_exist")
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    with pytest.raises(ValueError):
        db.execute("INSERT INTO t VALUES ($1)", [object()])


def test_datetime_roundtrip():
    db = fresh()
    db.execute("CREATE TABLE e(id INTEGER PRIMARY KEY, ts TIMESTAMP, d DATE, t TIME)")
    ts = dt.datetime(2024, 6, 14, 13, 30, 45, 123456)
    d = dt.date(2024, 6, 14)
    t = dt.time(13, 30, 45, 123456)
    db.execute("INSERT INTO e VALUES (1, $1, $2, $3)", [ts, d, t])
    row = db.query("SELECT ts, d, t FROM e").rows[0]
    assert row == (ts, d, t)
    assert isinstance(row[0], dt.datetime)
    assert isinstance(row[1], dt.date) and not isinstance(row[1], dt.datetime)
    assert isinstance(row[2], dt.time)


def test_datetime_tz_aware_normalized_to_utc():
    db = fresh()
    db.execute("CREATE TABLE z(id INTEGER PRIMARY KEY, ts TIMESTAMP)")
    aware = dt.datetime(2024, 1, 1, 12, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=5)))
    db.execute("INSERT INTO z VALUES (1, $1)", [aware])
    # 12:00 at +05:00 is 07:00 UTC, returned as a naive datetime.
    assert db.query("SELECT ts FROM z").rows[0][0] == dt.datetime(2024, 1, 1, 7, 0, 0)


def test_json_and_jsonb_dict_roundtrip():
    db = fresh()
    db.execute("CREATE TABLE j(id INTEGER PRIMARY KEY, a JSON, b JSONB)")
    payload = {"k": 1, "list": [1, 2, 3], "nested": {"x": True}}
    db.execute("INSERT INTO j VALUES (1, $1, $2)", [payload, payload])
    row = db.query("SELECT a, b FROM j").rows[0]
    assert row[0] == payload
    assert row[1] == payload
    assert isinstance(row[0], dict) and isinstance(row[1], dict)


def test_timestamp_infinity_falls_back_to_string():
    db = fresh()
    db.execute("CREATE TABLE inf(id INTEGER PRIMARY KEY, ts TIMESTAMP)")
    db.execute("INSERT INTO inf VALUES (1, 'infinity')")  # TEXT coerced to TIMESTAMP
    assert db.query("SELECT ts FROM inf").rows[0][0] == "infinity"


def test_int_overflow_raises_not_lossy_float():
    import numpy as np

    db = fresh()
    db.execute("CREATE TABLE big(id INTEGER PRIMARY KEY, n INTEGER)")
    db.execute("INSERT INTO big VALUES (1, $1)", [2**62])  # fits i64
    assert db.query("SELECT n FROM big").rows[0][0] == 2**62
    with pytest.raises(OverflowError):  # 2**63 > i64::MAX, must not become a float
        db.execute("INSERT INTO big VALUES (2, $1)", [2**63])
    with pytest.raises(OverflowError):  # numpy uint64 >= 2^63 must raise, not saturate
        db.execute("INSERT INTO big VALUES (3, $1)", [np.uint64(2**63)])
    db.execute("INSERT INTO big VALUES (4, $1)", [np.uint64(123)])  # in-range still binds
    assert db.query("SELECT n FROM big WHERE id = 4").rows[0][0] == 123


def test_numpy_vector_param_binds_as_vector():
    import numpy as np

    db = fresh()
    db.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, emb VECTOR(3))")
    db.execute("INSERT INTO v VALUES (1, $1)", [np.array([1.0, 2.0, 3.0], dtype="float32")])
    assert list(db.query("SELECT emb FROM v WHERE id = 1").rows[0][0]) == [1.0, 2.0, 3.0]
    # a length-1 float32 array must bind as a 1-D VECTOR, not a scalar REAL
    db.execute("CREATE TABLE v1(id INTEGER PRIMARY KEY, emb VECTOR(1))")
    db.execute("INSERT INTO v1 VALUES (1, $1)", [np.array([0.5], dtype="float32")])
    assert list(db.query("SELECT emb FROM v1 WHERE id = 1").rows[0][0]) == [0.5]


def test_non_finite_vector_param_rejected():
    import numpy as np

    db = fresh()
    db.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, emb VECTOR(3))")
    with pytest.raises(ValueError):  # NaN must raise at the boundary, not panic later
        db.execute("INSERT INTO v VALUES (1, $1)", [np.array([1.0, np.nan, 3.0], dtype="float32")])


def test_db_ann_persist_and_status():
    import numpy as np

    db = fresh()
    db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, emb VECTOR(3))")
    db.execute("CREATE INDEX items_ann ON items USING ann (emb) WITH (metric = 'l2')")
    for i in range(40):
        v = np.array([i * 0.1, (40 - i) * 0.1, (i % 7) * 0.1], dtype="float32")
        db.execute("INSERT INTO items VALUES ($1, $2)", [i, v])
    db.query(  # build the ANN index before freezing it
        "SELECT id FROM items ORDER BY emb <-> $1 LIMIT 3",
        [np.array([1.0, 0.0, 0.0], dtype="float32")],
    )
    info = db.persist_ann_index("items", "emb")
    assert isinstance(info["segment_b3"], bytes) and info["n"] == 40
    status = db.ann_cache_status("items", "emb")
    assert status is not None and status["source"] in ("loaded", "built")


def test_db_ann_persist_refuses_the_handle_owning_an_explicit_transaction():
    db = fresh()
    db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, emb VECTOR(3))")
    db.execute("BEGIN")
    with pytest.raises(citadeldb.DataError, match="explicit transaction"):
        db.persist_ann_index("items", "emb")
    db.execute("ROLLBACK")


def test_connect_options_secure_delete_and_fips_kdf():
    path = os.path.join(tempfile.mkdtemp(), "opt.cdl")
    opts = citadeldb.DatabaseOptions(secure_delete=True, kdf="pbkdf2", pbkdf2_iterations=600000)
    db = citadeldb.connect(path, key="pw", create=True, options=opts)
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    db.execute("INSERT INTO t VALUES (1)")
    del db
    db2 = citadeldb.connect(path, key="pw")  # reopen (KDF is recorded in the header)
    assert db2.query("SELECT COUNT(*) FROM t").rows[0][0] == 1


def test_audit_log_verify_and_key_backup_restore():
    d = tempfile.mkdtemp()
    path = os.path.join(d, "a.cdl")
    db = citadeldb.connect(path, key="pw1", create=True)
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    db.execute("INSERT INTO t VALUES (1)")
    audit = db.verify_audit_log()
    assert audit["chain_valid"] is True
    assert db.audit_log_path() is not None
    backup = os.path.join(d, "key.bak")
    db.export_key_backup("pw1", "backuppw", backup)
    assert os.path.exists(backup)
    del db
    citadeldb.Database.restore_key_from_backup(backup, "backuppw", "pw2", path)
    db2 = citadeldb.connect(path, key="pw2")  # opens under the recovered passphrase
    assert db2.query("SELECT COUNT(*) FROM t").rows[0][0] == 1


def test_close_and_context_manager():
    with citadeldb.connect(key="k") as db:
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        assert db.query("SELECT COUNT(*) FROM t").rows[0][0] == 0
    with pytest.raises(citadeldb.ProgrammingError):  # __exit__ closed it
        db.tables()
    db2 = fresh()
    db2.close()
    with pytest.raises(citadeldb.ProgrammingError):
        db2.execute("SELECT 1")
