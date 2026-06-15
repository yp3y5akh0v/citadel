"""Guard that _core.pyi stays in sync with the compiled module."""

import re
from pathlib import Path

import citadeldb
from citadeldb import _core

# Only compiled into candle/cuda wheels.
OPTIONAL = {"CandleEmbedder", "CrossEncoder"}


def stub_text() -> str:
    return (Path(citadeldb.__file__).parent / "_core.pyi").read_text(encoding="utf-8")


def test_stub_and_py_typed_are_shipped():
    pkg = Path(citadeldb.__file__).parent
    assert (pkg / "_core.pyi").is_file()
    assert (pkg / "py.typed").is_file()


def test_stub_symbols_exist_at_runtime():
    text = stub_text()
    declared = set(re.findall(r"^class (\w+)", text, re.M))
    declared |= set(re.findall(r"^def (\w+)", text, re.M))
    missing = {n for n in declared if n not in OPTIONAL and not hasattr(_core, n)}
    assert not missing, f"stub declares symbols absent from _core: {sorted(missing)}"


def test_public_types_are_documented():
    declared = set(re.findall(r"^class (\w+)", stub_text(), re.M))
    runtime = {
        n
        for n in dir(_core)
        if not n.startswith("_") and isinstance(getattr(_core, n), type)
    }
    undocumented = runtime - declared
    assert not undocumented, f"_core types missing from the stub: {sorted(undocumented)}"


def test_stub_methods_cover_runtime():
    """Every public method/property of a runtime class must be in its stub block."""
    stub_methods: dict[str, set[str]] = {}
    current = None
    for line in stub_text().splitlines():
        header = re.match(r"^class (\w+)", line)
        if header:
            current = header.group(1)
            stub_methods.setdefault(current, set())
        elif current is not None:
            defn = re.match(r"^    def (\w+)", line)
            attr = re.match(r"^    (\w+)\s*:", line)  # bare #[pyo3(get)] field annotation
            if defn:
                stub_methods[current].add(defn.group(1))
            elif attr:
                stub_methods[current].add(attr.group(1))
            elif line and not line[0].isspace():
                current = None  # left the class body
    object_attrs = set(dir(object))
    keep = {"__enter__", "__exit__", "__len__"}
    for name, declared in stub_methods.items():
        if name in OPTIONAL or not hasattr(_core, name):
            continue
        cls = getattr(_core, name)
        if not isinstance(cls, type) or issubclass(cls, BaseException):
            continue  # exceptions inherit BaseException's methods; the stub declares the base only
        runtime = {
            m
            for m in dir(cls)
            if (not m.startswith("_") or m in keep) and m not in object_attrs
        }
        # Filter the stub side the same way so __init__/__repr__ aren't read as phantoms.
        declared_public = {
            d for d in declared if (not d.startswith("_") or d in keep) and d not in object_attrs
        }
        missing = runtime - declared_public
        assert not missing, f"{name}: stub missing {sorted(missing)}"
        phantom = declared_public - runtime
        assert not phantom, f"{name}: stub declares methods absent at runtime: {sorted(phantom)}"
