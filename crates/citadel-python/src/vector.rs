//! Vector search: filtered ANN index, filters, and distance metrics.

use citadel_vector::prism::distance;
use citadel_vector::{AnnIndex, Filter, Metric};
use numpy::{PyArray1, PyReadonlyArray1, PyReadonlyArray2, PyUntypedArrayMethods};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::to_pyerr;

/// `(ids, distances)` numpy arrays returned by a search.
type SearchHits<'py> = (Bound<'py, PyArray1<u64>>, Bound<'py, PyArray1<f32>>);

fn metric_from_str(s: &str) -> PyResult<Metric> {
    match s.to_ascii_lowercase().as_str() {
        "l2" | "euclidean" => Ok(Metric::L2),
        "ip" | "inner" | "inner_product" | "dot" => Ok(Metric::InnerProduct),
        "cosine" | "cos" => Ok(Metric::Cosine),
        other => Err(PyValueError::new_err(format!(
            "unknown metric '{other}' (l2|inner|cosine)"
        ))),
    }
}

fn metric_name(m: Metric) -> &'static str {
    match m {
        Metric::L2 => "l2",
        Metric::InnerProduct => "inner",
        Metric::Cosine => "cosine",
    }
}

/// Reject NaN/inf at the FFI boundary: the PRISM distance sort unwraps
/// `partial_cmp`, which panics on NaN.
pub(crate) fn require_finite(name: &str, v: &[f32]) -> PyResult<()> {
    if v.iter().any(|x| !x.is_finite()) {
        return Err(PyValueError::new_err(format!(
            "{name} contains non-finite values (NaN or inf)"
        )));
    }
    Ok(())
}

/// Copy a 1-D float32 numpy array into an owned `Vec`, requiring C-contiguity and
/// finite components.
fn vec_arg(name: &str, a: &PyReadonlyArray1<f32>) -> PyResult<Vec<f32>> {
    let v = a
        .as_array()
        .as_slice()
        .ok_or_else(|| PyValueError::new_err(format!("{name} must be C-contiguous")))?
        .to_vec();
    require_finite(name, &v)?;
    Ok(v)
}

/// A conjunctive attribute filter for `VectorIndex.search`.
#[pyclass(name = "Filter")]
pub(crate) struct PyFilter {
    pub(crate) inner: Filter,
}

#[pymethods]
impl PyFilter {
    /// `Filter()` matches everything; `Filter([(attr, [vals...]), ...])` is a
    /// conjunction of per-attribute allow-sets.
    #[new]
    #[pyo3(signature = (constraints=None))]
    fn new(constraints: Option<Vec<(usize, Vec<u32>)>>) -> Self {
        match constraints {
            Some(c) => Self {
                inner: Filter::new(c),
            },
            None => Self {
                inner: Filter::none(),
            },
        }
    }

    /// A filter requiring attribute `attr` to equal `val`.
    #[staticmethod]
    fn eq(attr: usize, val: u32) -> Self {
        Self {
            inner: Filter::eq(attr, val),
        }
    }

    fn strength(&self) -> usize {
        self.inner.strength()
    }
}

/// In-memory filtered ANN index over a fixed `(id, vector)` snapshot. Immutable:
/// rebuild to change its contents.
#[pyclass(name = "VectorIndex")]
pub(crate) struct PyVectorIndex {
    inner: AnnIndex,
}

#[pymethods]
impl PyVectorIndex {
    /// Build from `vectors` (shape `(n, dim)`, float32) and `ids` (shape `(n,)`,
    /// int64). Optional `attrs` (shape `(n, num_attrs)`, uint32) enables filtered
    /// search over those attribute columns.
    #[new]
    #[pyo3(signature = (vectors, ids, metric="l2", *, attrs=None))]
    fn new(
        py: Python<'_>,
        vectors: PyReadonlyArray2<f32>,
        ids: PyReadonlyArray1<i64>,
        metric: &str,
        attrs: Option<PyReadonlyArray2<u32>>,
    ) -> PyResult<Self> {
        let metric = metric_from_str(metric)?;
        let vshape = vectors.shape();
        let n = vshape[0];
        let dim = vshape[1];
        if n == 0 {
            return Err(PyValueError::new_err("vectors must have at least one row"));
        }
        if dim == 0 || dim > u16::MAX as usize {
            return Err(PyValueError::new_err(format!(
                "dim {dim} out of range (1..=65535)"
            )));
        }
        let varr = vectors.as_array();
        let ids_arr = ids.as_array();
        if ids_arr.len() != n {
            return Err(PyValueError::new_err(format!(
                "ids length {} != {n} vector rows",
                ids_arr.len()
            )));
        }
        if varr.iter().any(|x| !x.is_finite()) {
            return Err(PyValueError::new_err(
                "vectors contains non-finite values (NaN or inf)",
            ));
        }
        let inner = match attrs {
            None => {
                let rows: Vec<(u64, Vec<f32>)> = (0..n)
                    .map(|i| (ids_arr[i] as u64, varr.row(i).to_vec()))
                    .collect();
                py.detach(move || AnnIndex::build(rows, metric, dim as u16))
                    .map_err(to_pyerr)?
            }
            Some(attrs) => {
                let ashape = attrs.shape();
                if ashape[0] != n {
                    return Err(PyValueError::new_err("attrs rows must match vectors rows"));
                }
                let num_attrs = ashape[1];
                let aarr = attrs.as_array();
                let rows: Vec<(u64, Vec<f32>, Vec<u32>)> = (0..n)
                    .map(|i| {
                        (
                            ids_arr[i] as u64,
                            varr.row(i).to_vec(),
                            aarr.row(i).to_vec(),
                        )
                    })
                    .collect();
                py.detach(move || AnnIndex::build_with_attrs(rows, num_attrs, metric, dim as u16))
                    .map_err(to_pyerr)?
            }
        };
        Ok(Self { inner })
    }

    /// Top-k search. Returns `(ids, distances)` as numpy arrays (uint64, float32),
    /// ascending by distance. `filter` restricts by the build-time attributes.
    #[pyo3(signature = (query, k=10, *, ef=None, filter=None))]
    fn search<'py>(
        &self,
        py: Python<'py>,
        query: PyReadonlyArray1<f32>,
        k: usize,
        ef: Option<usize>,
        filter: Option<&PyFilter>,
    ) -> PyResult<SearchHits<'py>> {
        let q = vec_arg("query", &query)?;
        if q.len() != self.inner.dim as usize {
            return Err(PyValueError::new_err(format!(
                "query dim {} != index dim {}",
                q.len(),
                self.inner.dim
            )));
        }
        let none = Filter::none();
        let f = filter.map(|pf| &pf.inner).unwrap_or(&none);
        let results = match ef {
            Some(ef) => self.inner.search_filtered(&q, k, ef, f),
            None => self.inner.search_filtered_default_ef(&q, k, f),
        };
        let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        let dists: Vec<f32> = results.iter().map(|(_, d)| *d).collect();
        Ok((PyArray1::from_vec(py, ids), PyArray1::from_vec(py, dists)))
    }

    #[getter]
    fn dim(&self) -> u16 {
        self.inner.dim
    }

    #[getter]
    fn metric(&self) -> &'static str {
        metric_name(self.inner.metric)
    }

    fn __len__(&self) -> usize {
        self.inner.indexed_len()
    }

    fn __repr__(&self) -> String {
        format!(
            "VectorIndex(len={}, dim={}, metric={})",
            self.inner.indexed_len(),
            self.inner.dim,
            metric_name(self.inner.metric)
        )
    }
}

/// Squared L2 distance between two equal-length float32 vectors.
#[pyfunction]
pub(crate) fn vector_l2_squared(
    a: PyReadonlyArray1<f32>,
    b: PyReadonlyArray1<f32>,
) -> PyResult<f32> {
    let (a, b) = (vec_arg("a", &a)?, vec_arg("b", &b)?);
    check_same_len(&a, &b)?;
    Ok(distance::l2_squared(&a, &b))
}

/// Inner product (dot) between two equal-length float32 vectors.
#[pyfunction]
pub(crate) fn vector_inner_product(
    a: PyReadonlyArray1<f32>,
    b: PyReadonlyArray1<f32>,
) -> PyResult<f32> {
    let (a, b) = (vec_arg("a", &a)?, vec_arg("b", &b)?);
    check_same_len(&a, &b)?;
    Ok(distance::inner_product(&a, &b))
}

/// Cosine distance `1 - cos(a, b)` between two equal-length float32 vectors.
#[pyfunction]
pub(crate) fn vector_cosine(a: PyReadonlyArray1<f32>, b: PyReadonlyArray1<f32>) -> PyResult<f32> {
    let (a, b) = (vec_arg("a", &a)?, vec_arg("b", &b)?);
    check_same_len(&a, &b)?;
    Ok(distance::cosine(&a, &b))
}

/// Distance under the named metric (`l2` = squared L2, `inner` = -dot, `cosine` = 1-cos).
#[pyfunction]
#[pyo3(signature = (a, b, metric="l2"))]
pub(crate) fn vector_distance(
    a: PyReadonlyArray1<f32>,
    b: PyReadonlyArray1<f32>,
    metric: &str,
) -> PyResult<f32> {
    let m = metric_from_str(metric)?;
    let (a, b) = (vec_arg("a", &a)?, vec_arg("b", &b)?);
    check_same_len(&a, &b)?;
    Ok(distance::distance(&a, &b, m))
}

/// L2-normalized copy of `v` as a numpy float32 array.
#[pyfunction]
pub(crate) fn vector_normalize<'py>(
    py: Python<'py>,
    v: PyReadonlyArray1<f32>,
) -> PyResult<Bound<'py, PyArray1<f32>>> {
    let v = vec_arg("v", &v)?;
    Ok(PyArray1::from_vec(py, distance::normalized(&v)))
}

fn check_same_len(a: &[f32], b: &[f32]) -> PyResult<()> {
    if a.len() != b.len() {
        return Err(PyValueError::new_err(format!(
            "length mismatch: {} vs {}",
            a.len(),
            b.len()
        )));
    }
    Ok(())
}
