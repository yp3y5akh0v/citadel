//! PyO3 bindings: the compiled `citadeldb._core` extension module.

mod agent;
mod errors;
mod graph;
mod llm;
mod mcp;
mod mem;
mod sql;
mod tools;
mod vector;
mod verify;

use citadel_sql::executor::{AnnIndexSource, AnnSegmentInfo};
use citadel_sql::Value;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDate, PyDateTime, PyDict, PyTime};
use pyo3::IntoPyObjectExt;
use serde_json::Value as Json;

pub(crate) use errors::to_pyerr;

/// Convert a SQL `Value` to a Python object (temporal/JSON as native objects).
pub(crate) fn value_to_py(py: Python<'_>, v: &Value) -> PyResult<Py<PyAny>> {
    use citadel_sql::Value as V;
    match v {
        V::Null => Ok(py.None()),
        V::Integer(i) => (*i).into_py_any(py),
        V::Real(r) => (*r).into_py_any(py),
        V::Boolean(b) => (*b).into_py_any(py),
        V::Text(s) => s.as_str().into_py_any(py),
        V::Json(s) => json_text_to_py(py, s),
        V::Blob(b) => PyBytes::new(py, b).into_py_any(py),
        V::Jsonb(b) => jsonb_to_py(py, b),
        V::TsVector(b) | V::TsQuery(b) => PyBytes::new(py, b).into_py_any(py),
        V::Vector(vec) => vec.to_vec().into_py_any(py),
        V::Array(items) => {
            let elems = items
                .iter()
                .map(|x| value_to_py(py, x))
                .collect::<PyResult<Vec<_>>>()?;
            elems.into_py_any(py)
        }
        V::Time(t) => time_to_py(py, *t),
        V::Timestamp(t) => timestamp_to_py(py, *t),
        V::Date(d) => date_to_py(py, *d),
        V::Interval {
            months,
            days,
            micros,
        } => {
            let d = PyDict::new(py);
            d.set_item("months", *months)?;
            d.set_item("days", *days)?;
            d.set_item("micros", *micros)?;
            d.into_py_any(py)
        }
    }
}

/// Timestamp micros -> naive `datetime`; infinity/out-of-range -> engine string.
fn timestamp_to_py(py: Python<'_>, micros: i64) -> PyResult<Py<PyAny>> {
    use citadel_sql::datetime as dt;
    if dt::is_infinity_ts(micros) {
        return dt::format_timestamp(micros).into_py_any(py);
    }
    let (days, time_micros) = dt::ts_split(micros);
    let (y, mo, d) = dt::days_to_ymd(days);
    if !(1..=9999).contains(&y) {
        return dt::format_timestamp(micros).into_py_any(py);
    }
    let (h, mi, s, us) = dt::micros_to_hmsn(time_micros);
    PyDateTime::new(py, y, mo, d, h, mi, s, us, None)?.into_py_any(py)
}

/// Date days -> `date`; infinity/out-of-range -> engine string.
fn date_to_py(py: Python<'_>, days: i32) -> PyResult<Py<PyAny>> {
    use citadel_sql::datetime as dt;
    if dt::is_infinity_date(days) {
        return dt::format_date(days).into_py_any(py);
    }
    let (y, mo, d) = dt::days_to_ymd(days);
    if !(1..=9999).contains(&y) {
        return dt::format_date(days).into_py_any(py);
    }
    PyDate::new(py, y, mo, d)?.into_py_any(py)
}

/// Time micros -> `time`; 24:00:00 (not representable) -> engine string.
fn time_to_py(py: Python<'_>, micros: i64) -> PyResult<Py<PyAny>> {
    use citadel_sql::datetime as dt;
    if micros >= dt::MICROS_PER_DAY {
        return dt::format_time(micros).into_py_any(py);
    }
    let (h, mi, s, us) = dt::micros_to_hmsn(micros);
    PyTime::new(py, h, mi, s, us, None)?.into_py_any(py)
}

/// JSON text -> Python object; raw text if not valid JSON.
fn json_text_to_py(py: Python<'_>, s: &str) -> PyResult<Py<PyAny>> {
    match serde_json::from_str::<Json>(s) {
        Ok(v) => json_to_py(py, &v),
        Err(_) => s.into_py_any(py),
    }
}

/// JSONB -> Python object; raw bytes if undecodable.
fn jsonb_to_py(py: Python<'_>, b: &[u8]) -> PyResult<Py<PyAny>> {
    match citadel_sql::json::decode_to_serde(b) {
        Ok(v) => json_to_py(py, &v),
        Err(_) => PyBytes::new(py, b).into_py_any(py),
    }
}

/// Python object (dict/list/scalar) -> JSON, via the `json` module.
pub(crate) fn py_to_json(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Json> {
    if obj.is_none() {
        return Ok(Json::Null);
    }
    let json = py.import("json")?;
    let s: String = json.call_method1("dumps", (obj,))?.extract()?;
    serde_json::from_str(&s).map_err(to_pyerr)
}

/// JSON -> Python object, via `json.loads`.
pub(crate) fn json_to_py(py: Python<'_>, v: &Json) -> PyResult<Py<PyAny>> {
    if v.is_null() {
        return Ok(py.None());
    }
    let s = serde_json::to_string(v).map_err(to_pyerr)?;
    let json = py.import("json")?;
    Ok(json.call_method1("loads", (s,))?.unbind())
}

/// A dict value that is present and not `None` (the caller extracts the type).
pub(crate) fn dict_item<'py>(
    d: &Bound<'py, PyDict>,
    key: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(match d.get_item(key)? {
        Some(v) if !v.is_none() => Some(v),
        _ => None,
    })
}

/// `AnnSegmentInfo` -> a Python dict (BLAKE3 digests as `bytes`).
pub(crate) fn ann_segment_info_dict<'py>(
    py: Python<'py>,
    info: &AnnSegmentInfo,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("segment_b3", PyBytes::new(py, &info.segment_b3))?;
    d.set_item(
        "content_fingerprint",
        PyBytes::new(py, &info.content_fingerprint),
    )?;
    d.set_item("n", info.n)?;
    d.set_item("dim", info.dim)?;
    d.set_item("metric_tag", info.metric_tag)?;
    d.set_item("chunk_count", info.chunk_count)?;
    Ok(d)
}

/// `AnnIndexSource` -> a Python dict: `{source: "loaded", segment_b3: bytes}` or
/// `{source: "built", refusal: str|None}`. Callers may add more keys.
pub(crate) fn ann_index_source_dict<'py>(
    py: Python<'py>,
    src: &AnnIndexSource,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    match src {
        AnnIndexSource::Loaded { segment_b3 } => {
            d.set_item("source", "loaded")?;
            d.set_item("segment_b3", PyBytes::new(py, segment_b3))?;
        }
        AnnIndexSource::Built { refusal } => {
            d.set_item("source", "built")?;
            d.set_item("refusal", refusal.clone())?;
        }
    }
    Ok(d)
}

/// Run the Citadel CLI with the given argv; returns the process exit code. Backs
/// the `citadeldb` console-script.
#[pyfunction]
fn cli_main(py: Python<'_>, argv: Vec<String>) -> i32 {
    py.detach(|| citadel_cli::run(argv))
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    errors::register(m)?;
    m.add_function(wrap_pyfunction!(cli_main, m)?)?;
    m.add_function(wrap_pyfunction!(mcp::mcp_main, m)?)?;
    m.add_function(wrap_pyfunction!(sql::connect, m)?)?;
    m.add_class::<sql::PyDatabase>()?;
    m.add_class::<sql::PyQueryResult>()?;
    m.add_class::<sql::PyDatabaseOptions>()?;
    m.add_class::<vector::PyVectorIndex>()?;
    m.add_class::<vector::PyFilter>()?;
    m.add_function(wrap_pyfunction!(vector::vector_l2_squared, m)?)?;
    m.add_function(wrap_pyfunction!(vector::vector_inner_product, m)?)?;
    m.add_function(wrap_pyfunction!(vector::vector_cosine, m)?)?;
    m.add_function(wrap_pyfunction!(vector::vector_distance, m)?)?;
    m.add_function(wrap_pyfunction!(vector::vector_normalize, m)?)?;
    m.add_class::<mem::PyMemory>()?;
    m.add_class::<mem::PyRecallOptions>()?;
    m.add_class::<mem::PyMockEmbedder>()?;
    m.add_class::<mem::PyAtomHit>()?;
    m.add_class::<mem::PyEvictionPolicy>()?;
    m.add_class::<mem::PyErasureReceipt>()?;
    m.add_class::<mem::PySlotErasure>()?;
    m.add_class::<mem::PyAtomAttestation>()?;
    m.add_class::<mem::PyMockReranker>()?;
    #[cfg(feature = "candle-embed")]
    m.add_class::<mem::PyCandleEmbedder>()?;
    #[cfg(feature = "candle-embed")]
    m.add_class::<mem::PyCrossEncoder>()?;
    m.add_class::<llm::PyLlmHandle>()?;
    m.add_class::<graph::PyBeliefGraph>()?;
    m.add_class::<graph::PyGoal>()?;
    m.add_class::<graph::PyTask>()?;
    m.add_class::<graph::PyHypothesis>()?;
    m.add_class::<graph::PyEvidence>()?;
    m.add_class::<graph::PyReflection>()?;
    m.add_class::<graph::PySelfModel>()?;
    m.add_class::<graph::PyCoInstantiationCheck>()?;
    m.add_class::<graph::PyChainReport>()?;
    m.add_class::<graph::PyVerifiedExport>()?;
    m.add_class::<graph::PyTraceEvictionPolicy>()?;
    m.add_class::<tools::PyToolRegistry>()?;
    m.add_class::<agent::PyAgent>()?;
    m.add_class::<agent::PyAgentConfig>()?;
    m.add_class::<agent::PyAgentBudget>()?;
    m.add_class::<agent::PyLlmProposer>()?;
    m.add_class::<agent::PyCompleter>()?;
    m.add_class::<agent::PyPromptLibrary>()?;
    m.add_class::<agent::PyDiscoveryGoal>()?;
    m.add_class::<agent::PyAgentReport>()?;
    m.add_class::<agent::PyDiscoveryReport>()?;
    Ok(())
}
