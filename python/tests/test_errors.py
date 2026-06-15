"""Typed exception hierarchy: structure, identity, and engine-failure mapping."""

import os
import tempfile

import pytest

import citadeldb
from citadeldb import exceptions as exc

SUBCLASSES = [
    exc.EncryptionError,
    exc.IntegrityError,
    exc.OperationalError,
    exc.ProgrammingError,
    exc.DataError,
    exc.NotSupportedError,
    exc.LlmError,
    exc.AgentError,
]


def test_hierarchy():
    assert issubclass(exc.CitadelError, Exception)
    for cls in SUBCLASSES:
        assert issubclass(cls, exc.CitadelError)
        assert issubclass(cls, Exception)


def test_reexport_identity():
    assert citadeldb.CitadelError is exc.CitadelError
    assert citadeldb.ProgrammingError is exc.ProgrammingError
    assert citadeldb.IntegrityError is exc.IntegrityError


def test_programming_error_missing_table():
    db = citadeldb.connect(key="k")
    with pytest.raises(citadeldb.ProgrammingError) as e:
        db.query("SELECT * FROM nope")
    assert isinstance(e.value, citadeldb.CitadelError)


def test_integrity_error_unique_violation():
    db = citadeldb.connect(key="k")
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    db.execute("INSERT INTO t VALUES (1)")
    with pytest.raises(citadeldb.IntegrityError):
        db.execute("INSERT INTO t VALUES (1)")


def test_data_error_division_by_zero():
    db = citadeldb.connect(key="k")
    with pytest.raises(citadeldb.DataError):
        db.query("SELECT 1 / 0")


def test_encryption_error_wrong_passphrase():
    path = os.path.join(tempfile.mkdtemp(), "e.cdl")
    citadeldb.connect(path, key="right", create=True).execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY)"
    )
    with pytest.raises(citadeldb.EncryptionError):
        citadeldb.connect(path, key="wrong")


def test_llm_error_unknown_provider():
    with pytest.raises(citadeldb.LlmError):
        citadeldb.LLMClient.provider("not-a-provider", "x")


def test_base_catches_typed():
    db = citadeldb.connect(key="k")
    try:
        db.query("SELECT * FROM nope")
    except citadeldb.CitadelError as e:
        assert type(e) is citadeldb.ProgrammingError
    else:
        pytest.fail("expected a CitadelError")


def test_input_validation_stays_value_error():
    db = citadeldb.connect(key="k")
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    with pytest.raises(ValueError) as e:
        db.execute("INSERT INTO t VALUES ($1)", [object()])
    assert not isinstance(e.value, citadeldb.CitadelError)
