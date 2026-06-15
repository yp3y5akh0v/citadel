//! Encrypted SQL surface: `Database`, `QueryResult`, and DB administration.

use std::sync::Arc;

use citadel::{Argon2Profile, CipherId, Database, DatabaseBuilder, KdfAlgorithm, SyncMode};
use citadel_mem::MemoryEngine;
use citadel_sql::{datetime, Connection, ExecutionResult, QueryResult, Value};
use numpy::PyReadonlyArray1;
use pyo3::exceptions::{PyOverflowError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyBytes, PyDate, PyDateAccess, PyDateTime, PyDelta, PyDeltaAccess, PyDict, PyList,
    PyTime, PyTimeAccess, PyTuple,
};
use pyo3::IntoPyObjectExt;
use self_cell::self_cell;

use crate::errors::programming_err;
use crate::mem::PyMemory;
use crate::vector::require_finite;
use crate::{ann_index_source_dict, ann_segment_info_dict, to_pyerr, value_to_py};

self_cell!(
    struct DbCell {
        owner: Arc<Database>,
        #[not_covariant]
        dependent: Connection,
    }
);

/// Convert a Python value to a SQL bind [`Value`] (positional `$1..$N` params).
fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    // bool before int: Python bool is an int subclass.
    if obj.is_instance_of::<PyBool>() {
        return Ok(Value::Boolean(obj.extract::<bool>()?));
    }
    // numpy bool isn't a bool subclass and lacks __index__: would bind as Real.
    if let Ok(ty) = obj.get_type().fully_qualified_name() {
        if matches!(ty.to_str(), Ok("numpy.bool" | "numpy.bool_")) {
            return Ok(Value::Boolean(obj.is_truthy()?));
        }
    }
    // datetime before date: datetime.datetime is a subclass of datetime.date.
    if let Ok(dt) = obj.cast::<PyDateTime>() {
        return Ok(Value::Timestamp(datetime_to_micros(dt)?));
    }
    if let Ok(d) = obj.cast::<PyDate>() {
        return Ok(Value::Date(date_to_days(d)?));
    }
    if let Ok(t) = obj.cast::<PyTime>() {
        return Ok(Value::Time(time_to_micros(t)?));
    }
    // Before scalar extracts so a length-1 float32 array stays a Vector, not Real.
    if let Ok(arr) = obj.extract::<PyReadonlyArray1<f32>>() {
        let v = arr
            .as_array()
            .as_slice()
            .ok_or_else(|| PyValueError::new_err("vector parameter must be C-contiguous"))?
            .to_vec();
        require_finite("vector parameter", &v)?;
        return Ok(Value::Vector(v.into()));
    }
    // Via __index__ + range-check: extract::<i64>() saturates numpy uint64 >= 2^63;
    // an out-of-range id must raise, not bind lossily.
    if let Ok(idx) = obj.call_method0("__index__") {
        return idx.extract::<i64>().map(Value::Integer).map_err(|_| {
            PyOverflowError::new_err("integer parameter out of range for a 64-bit INTEGER column")
        });
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(Value::Real(f));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::Text(s.into()));
    }
    if obj.is_instance_of::<PyBytes>() {
        return Ok(Value::Blob(obj.extract::<Vec<u8>>()?));
    }
    if obj.is_instance_of::<PyList>() {
        let items: Vec<Bound<'_, PyAny>> = obj.extract()?;
        let mut out = Vec::with_capacity(items.len());
        for it in &items {
            out.push(py_to_value(it)?);
        }
        return Ok(Value::Array(Arc::new(out)));
    }
    // dict -> JSON; the engine coerces to a JSON/JSONB column.
    if obj.is_instance_of::<PyDict>() {
        let json = obj.py().import("json")?;
        let s: String = json.call_method1("dumps", (obj,))?.extract()?;
        return Ok(Value::Json(s.into()));
    }
    Err(PyValueError::new_err(
        "unsupported SQL parameter type \
         (use None/bool/int/float/str/bytes/datetime/date/time/list/dict)",
    ))
}

/// Python `datetime` -> Timestamp micros (UTC; tz-aware values normalized).
fn datetime_to_micros(dt: &Bound<'_, PyDateTime>) -> PyResult<i64> {
    let days = datetime::ymd_to_days(dt.get_year(), dt.get_month(), dt.get_day())
        .ok_or_else(|| PyValueError::new_err("datetime out of range"))?;
    let time = datetime::hmsn_to_micros(
        dt.get_hour(),
        dt.get_minute(),
        dt.get_second(),
        dt.get_microsecond(),
    )
    .ok_or_else(|| PyValueError::new_err("datetime out of range"))?;
    let mut micros = datetime::ts_combine(days, time);
    let offset = dt.call_method0("utcoffset")?;
    if !offset.is_none() {
        let td = offset
            .cast::<PyDelta>()
            .map_err(|_| PyValueError::new_err("datetime utcoffset must be a timedelta"))?;
        micros -= (td.get_days() as i64) * datetime::MICROS_PER_DAY
            + (td.get_seconds() as i64) * datetime::MICROS_PER_SEC
            + td.get_microseconds() as i64;
    }
    Ok(micros)
}

/// Python `datetime.date` -> `Date` days since the Unix epoch.
fn date_to_days(d: &Bound<'_, PyDate>) -> PyResult<i32> {
    datetime::ymd_to_days(d.get_year(), d.get_month(), d.get_day())
        .ok_or_else(|| PyValueError::new_err("date out of range"))
}

/// Python `datetime.time` -> `Time` micros since midnight (tzinfo ignored).
fn time_to_micros(t: &Bound<'_, PyTime>) -> PyResult<i64> {
    datetime::hmsn_to_micros(
        t.get_hour(),
        t.get_minute(),
        t.get_second(),
        t.get_microsecond(),
    )
    .ok_or_else(|| PyValueError::new_err("time out of range"))
}

fn to_values(py: Python<'_>, params: &Option<Vec<Py<PyAny>>>) -> PyResult<Option<Vec<Value>>> {
    match params {
        None => Ok(None),
        Some(ps) => {
            let mut out = Vec::with_capacity(ps.len());
            for p in ps {
                out.push(py_to_value(p.bind(py))?);
            }
            Ok(Some(out))
        }
    }
}

/// An open encrypted database with one long-lived connection, so transaction
/// state persists across calls. `unsendable`: the connection is `!Sync`, so the
/// handle is pinned to its creating thread (like sqlite3's default).
#[pyclass(unsendable, name = "Database")]
pub(crate) struct PyDatabase {
    cell: Option<DbCell>,
}

impl PyDatabase {
    /// Borrow the live cell, or raise if the database has been closed.
    fn cell(&self) -> PyResult<&DbCell> {
        self.cell
            .as_ref()
            .ok_or_else(|| programming_err("operation on a closed Database"))
    }
}

#[pymethods]
impl PyDatabase {
    /// Execute one statement (optionally with positional `$1..$N` params). Returns
    /// rows-affected (int), a `QueryResult`, or `None`.
    #[pyo3(signature = (sql, params=None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<Vec<Py<PyAny>>>,
    ) -> PyResult<Py<PyAny>> {
        let values = to_values(py, &params)?;
        self.cell()?.with_dependent(|_owner, conn| {
            let res = match &values {
                Some(v) => conn.execute_params(sql, v),
                None => conn.execute(sql),
            };
            match res.map_err(to_pyerr)? {
                ExecutionResult::RowsAffected(n) => (n as i64).into_py_any(py),
                ExecutionResult::Query(qr) => PyQueryResult::from(qr).into_py_any(py),
                ExecutionResult::Ok => Ok(py.None()),
            }
        })
    }

    /// Run a query (optionally with positional params) and return all rows.
    #[pyo3(signature = (sql, params=None))]
    fn query(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<Vec<Py<PyAny>>>,
    ) -> PyResult<PyQueryResult> {
        let values = to_values(py, &params)?;
        self.cell()?.with_dependent(|_owner, conn| {
            let qr = match &values {
                Some(v) => conn.query_params(sql, v),
                None => conn.query(sql),
            };
            qr.map(PyQueryResult::from).map_err(to_pyerr)
        })
    }

    /// Execute `;`-separated statements; returns one result per completed statement.
    /// Stops and raises at the first error (completed statements persist).
    fn execute_script(&self, py: Python<'_>, sql: &str) -> PyResult<Vec<Py<PyAny>>> {
        self.cell()?.with_dependent(|_owner, conn| {
            let exec = conn.execute_script(sql);
            let mut out = Vec::with_capacity(exec.completed.len());
            for r in exec.completed {
                out.push(match r {
                    ExecutionResult::RowsAffected(n) => (n as i64).into_py_any(py)?,
                    ExecutionResult::Query(qr) => PyQueryResult::from(qr).into_py_any(py)?,
                    ExecutionResult::Ok => py.None(),
                });
            }
            match exec.error {
                Some(e) => Err(to_pyerr(e)),
                None => Ok(out),
            }
        })
    }

    /// Names of the user tables.
    fn tables(&self) -> PyResult<Vec<String>> {
        Ok(self.cell()?.with_dependent(|_owner, conn| conn.tables()))
    }

    /// Whether an explicit transaction is open.
    fn in_transaction(&self) -> PyResult<bool> {
        Ok(self
            .cell()?
            .with_dependent(|_owner, conn| conn.in_transaction()))
    }

    /// Open the memory engine over this database (shares the underlying storage).
    fn memory(&self) -> PyResult<PyMemory> {
        let db = self.cell()?.borrow_owner().clone();
        let engine = MemoryEngine::open(db).map_err(to_pyerr)?;
        Ok(PyMemory::from_engine(Arc::new(engine)))
    }

    /// Storage statistics: `{tree_depth, entry_count, total_pages, high_water_mark, merkle_root}`.
    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let s = self.cell()?.borrow_owner().stats();
        let d = PyDict::new(py);
        d.set_item("tree_depth", s.tree_depth)?;
        d.set_item("entry_count", s.entry_count)?;
        d.set_item("total_pages", s.total_pages)?;
        d.set_item("high_water_mark", s.high_water_mark)?;
        d.set_item("merkle_root", PyBytes::new(py, &s.merkle_root))?;
        d.into_py_any(py)
    }

    /// Verify page integrity: `{ok, pages_checked, error_count}`.
    fn integrity_check(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let r = self
            .cell()?
            .borrow_owner()
            .integrity_check()
            .map_err(to_pyerr)?;
        let d = PyDict::new(py);
        d.set_item("ok", r.is_ok())?;
        d.set_item("pages_checked", r.pages_checked)?;
        d.set_item("error_count", r.errors.len())?;
        d.into_py_any(py)
    }

    /// Write a consistent encrypted copy of the database to `dest`.
    fn backup(&self, dest: &str) -> PyResult<()> {
        self.cell()?
            .borrow_owner()
            .backup(std::path::Path::new(dest))
            .map_err(to_pyerr)
    }

    /// Write a compacted (free-space-reclaimed) copy of the database to `dest`.
    fn compact(&self, dest: &str) -> PyResult<()> {
        self.cell()?
            .borrow_owner()
            .compact(std::path::Path::new(dest))
            .map_err(to_pyerr)
    }

    /// Re-wrap the root key under a new passphrase (data is not re-encrypted).
    fn change_passphrase(&self, old: &str, new: &str) -> PyResult<()> {
        self.cell()?
            .borrow_owner()
            .change_passphrase(old.as_bytes(), new.as_bytes())
            .map_err(to_pyerr)
    }

    /// Freeze the ANN index for a VECTOR `column` of `table` into a persisted
    /// segment so a later cold open LOADs it instead of rebuilding by full scan.
    /// Returns the segment manifest dict.
    fn persist_ann_index(&self, py: Python<'_>, table: &str, column: &str) -> PyResult<Py<PyAny>> {
        // Build off a fresh connection on a detached thread: the scan/PRISM build
        // can take minutes, so it must not hold the GIL (persist is refused inside
        // an open transaction anyway, so a throwaway connection is equivalent).
        let db = self.cell()?.borrow_owner().clone();
        let (table, column) = (table.to_string(), column.to_string());
        let info = py
            .detach(move || Connection::open(&db)?.persist_ann_index(&table, &column))
            .map_err(to_pyerr)?;
        ann_segment_info_dict(py, &info)?.into_py_any(py)
    }

    /// How a VECTOR `column`'s ANN queries are served: `None` if nothing is cached,
    /// else the source dict plus `{"generation": int}`.
    fn ann_cache_status(
        &self,
        py: Python<'_>,
        table: &str,
        column: &str,
    ) -> PyResult<Option<Py<PyAny>>> {
        self.cell()?.with_dependent(|_owner, conn| {
            match conn.ann_cache_status(table, column).map_err(to_pyerr)? {
                None => Ok(None),
                Some((src, generation)) => {
                    let d = ann_index_source_dict(py, &src)?;
                    d.set_item("generation", generation)?;
                    Ok(Some(d.into_py_any(py)?))
                }
            }
        })
    }

    /// Export an encrypted key escrow (under its own `backup_pass`) for disaster
    /// recovery; restore later with `restore_key_from_backup` if the DB passphrase
    /// is lost. Requires the current DB passphrase.
    fn export_key_backup(&self, db_pass: &str, backup_pass: &str, dest: &str) -> PyResult<()> {
        self.cell()?
            .borrow_owner()
            .export_key_backup(
                db_pass.as_bytes(),
                backup_pass.as_bytes(),
                std::path::Path::new(dest),
            )
            .map_err(to_pyerr)
    }

    /// Recreate a key file from an escrow `backup` under `new_db_pass`, for the
    /// database at `db_path`. Static: no open handle needed.
    #[staticmethod]
    fn restore_key_from_backup(
        backup: &str,
        backup_pass: &str,
        new_db_pass: &str,
        db_path: &str,
    ) -> PyResult<()> {
        Database::restore_key_from_backup(
            std::path::Path::new(backup),
            backup_pass.as_bytes(),
            new_db_pass.as_bytes(),
            std::path::Path::new(db_path),
        )
        .map_err(to_pyerr)
    }

    /// Verify the tamper-evident audit log (on by default for file databases):
    /// `{entries_verified, chain_valid, chain_break_at}`.
    fn verify_audit_log(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let r = self
            .cell()?
            .borrow_owner()
            .verify_audit_log()
            .map_err(to_pyerr)?;
        let d = PyDict::new(py);
        d.set_item("entries_verified", r.entries_verified)?;
        d.set_item("chain_valid", r.chain_valid)?;
        d.set_item("chain_break_at", r.chain_break_at)?;
        d.into_py_any(py)
    }

    /// Path of the tamper-evident audit log, or `None` if disabled (in-memory DBs).
    fn audit_log_path(&self) -> PyResult<Option<String>> {
        Ok(self
            .cell()?
            .borrow_owner()
            .audit_log_path()
            .map(|p| p.to_string_lossy().into_owned()))
    }

    /// Release this handle's connection and database reference. Later calls raise;
    /// a still-open `memory()` keeps the database alive until it drops too.
    fn close(&mut self) {
        self.cell = None;
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        self.close();
        false
    }

    fn __repr__(&self) -> &'static str {
        if self.cell.is_some() {
            "Database(open)"
        } else {
            "Database(closed)"
        }
    }
}

/// Column names + rows from a query.
#[pyclass(name = "QueryResult")]
pub(crate) struct PyQueryResult {
    #[pyo3(get)]
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

impl From<QueryResult> for PyQueryResult {
    fn from(qr: QueryResult) -> Self {
        Self {
            columns: qr.columns,
            rows: qr.rows,
        }
    }
}

#[pymethods]
impl PyQueryResult {
    /// Rows as a list of tuples.
    #[getter]
    fn rows(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.rows
            .iter()
            .map(|row| {
                let vals = row
                    .iter()
                    .map(|v| value_to_py(py, v))
                    .collect::<PyResult<Vec<_>>>()?;
                PyTuple::new(py, vals)?.into_py_any(py)
            })
            .collect()
    }

    /// Rows as a list of `{column: value}` dicts.
    fn to_dicts(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.rows
            .iter()
            .map(|row| {
                let d = PyDict::new(py);
                for (col, v) in self.columns.iter().zip(row.iter()) {
                    d.set_item(col, value_to_py(py, v)?)?;
                }
                d.into_py_any(py)
            })
            .collect()
    }

    fn __len__(&self) -> usize {
        self.rows.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "QueryResult(columns={:?}, rows={})",
            self.columns,
            self.rows.len()
        )
    }
}

fn parse_sync_mode(s: &str) -> PyResult<SyncMode> {
    match s.to_ascii_lowercase().as_str() {
        "full" => Ok(SyncMode::Full),
        "normal" => Ok(SyncMode::Normal),
        "off" => Ok(SyncMode::Off),
        other => Err(PyValueError::new_err(format!(
            "unknown sync_mode '{other}' (full|normal|off)"
        ))),
    }
}

fn parse_cipher(s: &str) -> PyResult<CipherId> {
    match s.to_ascii_lowercase().as_str() {
        "aes256ctr" | "aes-256-ctr" => Ok(CipherId::Aes256Ctr),
        "chacha20" => Ok(CipherId::ChaCha20),
        other => Err(PyValueError::new_err(format!(
            "unknown cipher '{other}' (aes256ctr|chacha20)"
        ))),
    }
}

fn parse_kdf(s: &str) -> PyResult<KdfAlgorithm> {
    match s.to_ascii_lowercase().as_str() {
        "argon2id" => Ok(KdfAlgorithm::Argon2id),
        "pbkdf2" | "pbkdf2hmacsha256" => Ok(KdfAlgorithm::Pbkdf2HmacSha256),
        other => Err(PyValueError::new_err(format!(
            "unknown kdf '{other}' (argon2id|pbkdf2)"
        ))),
    }
}

fn parse_argon2_profile(s: &str) -> PyResult<Argon2Profile> {
    match s.to_ascii_lowercase().as_str() {
        "iot" => Ok(Argon2Profile::Iot),
        "desktop" => Ok(Argon2Profile::Desktop),
        "server" => Ok(Argon2Profile::Server),
        other => Err(PyValueError::new_err(format!(
            "unknown argon2_profile '{other}' (iot|desktop|server)"
        ))),
    }
}

/// Create-time security/durability knobs for [`connect`] (all optional).
#[pyclass(name = "DatabaseOptions")]
pub(crate) struct PyDatabaseOptions {
    secure_delete: bool,
    cache_size: Option<usize>,
    sync_mode: Option<SyncMode>,
    cipher: Option<CipherId>,
    kdf: Option<KdfAlgorithm>,
    pbkdf2_iterations: Option<u32>,
    argon2_profile: Option<Argon2Profile>,
}

#[pymethods]
impl PyDatabaseOptions {
    /// `secure_delete` zero-fills freed pages; `cipher`="aes256ctr"|"chacha20";
    /// `kdf`="argon2id"|"pbkdf2" (the FIPS path) with `pbkdf2_iterations`;
    /// `argon2_profile`="iot"|"desktop"|"server"; `cache_size`=buffer-pool pages;
    /// `sync_mode`="full"|"normal"|"off".
    #[new]
    #[pyo3(signature = (*, secure_delete=false, cache_size=None, sync_mode=None, cipher=None, kdf=None, pbkdf2_iterations=None, argon2_profile=None))]
    fn new(
        secure_delete: bool,
        cache_size: Option<usize>,
        sync_mode: Option<&str>,
        cipher: Option<&str>,
        kdf: Option<&str>,
        pbkdf2_iterations: Option<u32>,
        argon2_profile: Option<&str>,
    ) -> PyResult<Self> {
        Ok(Self {
            secure_delete,
            cache_size,
            sync_mode: sync_mode.map(parse_sync_mode).transpose()?,
            cipher: cipher.map(parse_cipher).transpose()?,
            kdf: kdf.map(parse_kdf).transpose()?,
            pbkdf2_iterations,
            argon2_profile: argon2_profile.map(parse_argon2_profile).transpose()?,
        })
    }
}

/// Open or create an encrypted database. `path=None` (or `":memory:"`) is in-memory;
/// `create=None` opens an existing file else creates a new one. `region_keys=True`
/// enables per-region wrap keys (required for encrypted memory regions). `options`
/// is a `DatabaseOptions` of create-time security/durability knobs.
#[pyfunction]
#[pyo3(signature = (path=None, *, key, create=None, region_keys=false, options=None))]
pub(crate) fn connect(
    path: Option<String>,
    key: &str,
    create: Option<bool>,
    region_keys: bool,
    options: Option<&PyDatabaseOptions>,
) -> PyResult<PyDatabase> {
    let configure = |mut b: DatabaseBuilder| {
        b = b.passphrase(key.as_bytes()).enable_region_keys(region_keys);
        if let Some(o) = options {
            b = b.enable_secure_delete(o.secure_delete);
            if let Some(c) = o.cache_size {
                b = b.cache_size(c);
            }
            if let Some(s) = o.sync_mode {
                b = b.sync_mode(s);
            }
            if let Some(c) = o.cipher {
                b = b.cipher(c);
            }
            if let Some(k) = o.kdf {
                b = b.kdf_algorithm(k);
            }
            if let Some(it) = o.pbkdf2_iterations {
                b = b.pbkdf2_iterations(it);
            }
            if let Some(p) = o.argon2_profile {
                b = b.argon2_profile(p);
            }
        }
        b
    };
    let in_memory = matches!(path.as_deref(), None | Some("") | Some(":memory:"));
    let db = if in_memory {
        configure(DatabaseBuilder::new(""))
            .create_in_memory()
            .map_err(to_pyerr)?
    } else {
        let p = path.as_deref().unwrap();
        let builder = configure(DatabaseBuilder::new(p));
        let exists = std::path::Path::new(p).exists();
        match create {
            Some(true) => builder.create(),
            Some(false) => builder.open(),
            None if exists => builder.open(),
            None => builder.create(),
        }
        .map_err(to_pyerr)?
    };
    let cell = DbCell::try_new(Arc::new(db), |owner| Connection::open(owner)).map_err(to_pyerr)?;
    Ok(PyDatabase { cell: Some(cell) })
}
