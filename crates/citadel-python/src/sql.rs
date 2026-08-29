//! Encrypted SQL surface: `Database`, `QueryResult`, and DB administration.

use std::sync::Arc;

use citadel::{
    Argon2Profile, CancelToken, CipherId, Database, DatabaseBuilder, IntegrityError, KdfAlgorithm,
    SyncMode,
};
use citadel_mem::MemoryEngine;
use citadel_sql::{datetime, Connection, ExecutionResult, QueryResult, Value};
use numpy::PyReadonlyArray1;
use parking_lot::Mutex;
use pyo3::exceptions::{PyOverflowError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyBytes, PyDate, PyDateAccess, PyDateTime, PyDelta, PyDeltaAccess, PyDict, PyList,
    PyTime, PyTimeAccess, PyTuple,
};
use pyo3::IntoPyObjectExt;
use self_cell::self_cell;
use zeroize::Zeroizing;

use crate::errors::{encryption_err, programming_err};
use crate::mem::{PyMemory, PyMemoryMaintenance};
use crate::vector::require_finite;
use crate::{ann_index_source_dict, ann_segment_info_dict, to_pyerr, value_to_py};

self_cell!(
    struct DbCell {
        owner: Arc<Database>,
        #[not_covariant]
        dependent: Connection,
    }
);

/// The one connection over a file, shared by every handle to it.
///
/// One connection, not one per handle: each caches the schema it loaded, so a second
/// would not see a table the first created.
struct SharedConn {
    db: Arc<Database>,
    cell: Mutex<DbCell>,
    /// Watched from other threads, which must not touch `cell` at all: alive for
    /// exactly as long as some handle still holds this connection. Atomic where the
    /// connection itself is not, since only this crosses a thread boundary.
    token: Arc<()>,
}

impl SharedConn {
    fn open(owner: Arc<Database>) -> PyResult<Self> {
        Ok(Self {
            db: Arc::clone(&owner),
            cell: Mutex::new(
                DbCell::try_new(owner, |owner| Connection::open(owner)).map_err(to_pyerr)?,
            ),
            token: Arc::new(()),
        })
    }
}

thread_local! {
    /// Connections this thread owns. Weak, so a closed handle releases its own.
    static OPEN_CONNS: std::cell::RefCell<
        std::collections::HashMap<std::path::PathBuf, std::sync::Weak<SharedConn>>,
    > = Default::default();
}

/// One open file: the database its handles share, its memory engine, and the terms
/// last accepted for it.
///
/// Weak throughout, so the file is released once the last handle and engine drop.
struct OpenFile {
    db: std::sync::Weak<Database>,
    engine: std::sync::Weak<MemoryEngine>,
    /// Live while a handle still holds the connection. A connection is pinned to its
    /// thread, so this is how another one tells "in use" from "opened here and gone".
    conn: std::sync::Weak<()>,
    owner_thread: std::thread::ThreadId,
    /// The key file's hash when `passphrase` was accepted. A rekey or a restore from
    /// backup rewrites that file, so the digest below cannot outlive the key it names.
    key_file: [u8; 32],
    /// Keyed digest of the accepted passphrase; the passphrase itself is not kept.
    passphrase: [u8; 32],
}

/// Files this process holds open, by canonical path. A second real open would fail
/// on the whole-file lock, so a reopen builds another connection over this instead.
static OPEN_FILES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, OpenFile>>,
> = std::sync::LazyLock::new(Default::default);

/// Random per process, so a digest read out of memory is worthless anywhere else.
static DIGEST_KEY: std::sync::LazyLock<[u8; 32]> = std::sync::LazyLock::new(|| {
    let mut k = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut k);
    k
});

fn passphrase_digest(key: &[u8]) -> [u8; 32] {
    *blake3::keyed_hash(&DIGEST_KEY, key).as_bytes()
}

/// Constant time, so a rejected passphrase leaks nothing through how long it took.
fn same_digest(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// One key per file, through symlinks and Windows case-insensitivity. `None` until
/// the file exists, which can only ever be a lookup miss.
fn identity(path: &std::path::Path) -> Option<std::path::PathBuf> {
    std::fs::canonicalize(path).ok()
}

fn file_hash(path: &std::path::Path) -> PyResult<[u8; 32]> {
    let bytes = std::fs::read(path).map_err(|e| to_pyerr(citadel::Error::Io(e)))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

/// The database this process already holds at `ident`, or `None` to open the file.
///
/// The caller holds the registry lock, so a racing open cannot land between this
/// lookup and the insert that follows it.
fn reuse(
    open: &mut std::collections::HashMap<std::path::PathBuf, OpenFile>,
    ident: &std::path::Path,
    path: &str,
    key: &[u8],
    region_keys: bool,
    options: bool,
    create: Option<bool>,
) -> PyResult<Option<Arc<Database>>> {
    let Some(entry) = open.get_mut(ident) else {
        return Ok(None);
    };
    let Some(db) = entry.db.upgrade() else {
        open.remove(ident);
        return Ok(None);
    };

    // Passphrase first, and as the same error a fresh open raises: a caller without
    // it must not learn from the reply that the file is open, nor on what terms.
    let key_file = file_hash(db.key_path())?;
    let digest = passphrase_digest(key);
    let accepted = if key_file == entry.key_file {
        same_digest(&digest, &entry.passphrase)
    } else {
        db.verify_passphrase(key).map_err(to_pyerr)?
    };
    if !accepted {
        return Err(encryption_err(format!(
            "wrong passphrase for {path}, which this process already holds open"
        )));
    }
    entry.key_file = key_file;
    entry.passphrase = digest;

    // A connection belongs to the thread that opened it, so a live one elsewhere
    // cannot be shared. Once its last handle drops, this thread may take it over.
    if entry.conn.strong_count() > 0 && entry.owner_thread != std::thread::current().id() {
        return Err(programming_err(format!(
            "{path} is open on another thread of this process. A connection belongs to \
             the thread that opened it, so open it once and dispatch work to that \
             thread, or pass a Memory or MemoryMaintenance capability, which any thread may use."
        )));
    }

    if create == Some(true) {
        return Err(programming_err(format!(
            "{path} is already open in this process, so `create=True` cannot be \
             honoured. Use a different path, or connect without `create`."
        )));
    }
    if db.region_keys_enabled() != region_keys {
        return Err(programming_err(format!(
            "{path} is already open in this process with region_keys={}",
            db.region_keys_enabled()
        )));
    }
    if options {
        return Err(programming_err(format!(
            "{path} is already open in this process, so `options` cannot be applied. \
             Pass them on the first connect."
        )));
    }
    Ok(Some(db))
}

/// A handle over `owner`, joining the connection this thread already has for it.
///
/// Each handle keeps its own reference, so one holder closing leaves the rest working
/// and the file is released only when the last of them drops.
fn attach(
    open: &mut std::collections::HashMap<std::path::PathBuf, OpenFile>,
    ident: Option<&std::path::Path>,
    owner: Arc<Database>,
) -> PyResult<PyDatabase> {
    let Some(ident) = ident else {
        // In-memory: no file to contend over, so nothing to share it with.
        return Ok(PyDatabase::new(Arc::new(SharedConn::open(owner)?)));
    };
    let live = OPEN_CONNS
        .with_borrow(|c| c.get(ident).and_then(std::sync::Weak::upgrade))
        // Only when it wraps this database: a connection cached over a predecessor at
        // the same path would silently drop the one just opened.
        .filter(|c| Arc::ptr_eq(&c.db, &owner));
    let conn = match live {
        Some(conn) => conn,
        None => {
            let conn = Arc::new(SharedConn::open(owner)?);
            OPEN_CONNS.with_borrow_mut(|c| c.insert(ident.to_path_buf(), Arc::downgrade(&conn)));
            conn
        }
    };
    if let Some(entry) = open.get_mut(ident) {
        entry.conn = Arc::downgrade(&conn.token);
        entry.owner_thread = std::thread::current().id();
    }
    Ok(PyDatabase::new(conn))
}

/// The one engine over this database, built on first use.
///
/// Engines cache regions separately, so a second would not see a region the first
/// created.
fn shared_engine(db: &Arc<Database>) -> PyResult<Arc<MemoryEngine>> {
    let ident = identity(db.data_path());
    if let Some(i) = ident.as_ref() {
        if let Some(engine) = OPEN_FILES
            .lock()
            .unwrap()
            .get(i)
            .and_then(|e| e.engine.upgrade())
        {
            return Ok(engine);
        }
    }

    // Opening the memory schema can perform file I/O. Do it outside the global
    // registry lock, then resolve a race in favor of the engine registered first.
    let engine = Arc::new(MemoryEngine::open(Arc::clone(db)).map_err(to_pyerr)?);
    if let Some(i) = ident {
        let mut open = OPEN_FILES.lock().unwrap();
        if let Some(existing) = open.get(&i).and_then(|e| e.engine.upgrade()) {
            return Ok(existing);
        }
        if let Some(entry) = open.get_mut(&i) {
            let same_database = entry
                .db
                .upgrade()
                .is_some_and(|registered| Arc::ptr_eq(&registered, db));
            if same_database {
                entry.engine = Arc::downgrade(&engine);
            }
        }
    }
    Ok(engine)
}

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

/// A one-shot cancellation flag that can be tripped from another Python thread.
#[pyclass(name = "CancelToken")]
pub(crate) struct PyCancelToken {
    inner: CancelToken,
}

#[pymethods]
impl PyCancelToken {
    #[new]
    fn new() -> Self {
        Self {
            inner: CancelToken::new(),
        }
    }

    fn cancel(&self) {
        self.inner.cancel();
    }

    #[getter]
    fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    fn __repr__(&self) -> String {
        format!("CancelToken(cancelled={})", self.inner.is_cancelled())
    }
}

/// An open encrypted database with one long-lived connection, so transaction
/// state persists across calls. Operations are pinned to the creating thread
/// (like sqlite3's default), while final destruction is safe on any thread.
#[pyclass(name = "Database")]
pub(crate) struct PyDatabase {
    owner_thread: std::thread::ThreadId,
    /// Shared with every other handle to this file, so closing is per handle.
    conn: Option<Arc<SharedConn>>,
    /// Cached from the open-file table, which keeps one engine per database.
    memory: std::sync::OnceLock<Arc<MemoryEngine>>,
}

impl PyDatabase {
    fn new(conn: Arc<SharedConn>) -> Self {
        Self {
            owner_thread: std::thread::current().id(),
            conn: Some(conn),
            memory: std::sync::OnceLock::new(),
        }
    }

    fn ensure_owner_thread(&self) -> PyResult<()> {
        if self.owner_thread == std::thread::current().id() {
            return Ok(());
        }
        Err(programming_err(
            "Database operations must run on the thread that opened the handle; pass its Memory or MemoryMaintenance capability across threads instead",
        ))
    }

    /// Borrow the live cell, or raise if this handle has been closed.
    fn shared_conn(&self) -> PyResult<&Arc<SharedConn>> {
        self.ensure_owner_thread()?;
        self.conn
            .as_ref()
            .ok_or_else(|| programming_err("operation on a closed Database"))
    }

    fn database(&self) -> PyResult<Arc<Database>> {
        Ok(Arc::clone(&self.shared_conn()?.db))
    }

    fn with_connection<T>(&self, f: impl FnOnce(&Connection<'_>) -> T) -> PyResult<T> {
        let cell = self.shared_conn()?.cell.lock();
        Ok(cell.with_dependent(|_owner, conn| f(conn)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PyIntegrityErrorKind {
    CommitSlotChecksumMismatch,
    CommitSlotMacMismatch,
    CommitSlotDowngrade,
    CommitSlotUnknownFormat,
    CommitSlotUnknownMerkleScheme,
    PageReadFailed,
    PageTampered,
    ChecksumMismatch,
    PageIdMismatch,
    PageTransactionOutOfBounds,
    ReachablePageOutOfBounds,
    TreeDepthMismatch,
    PageMerkleMismatch,
    SlotMerkleRootMismatch,
    PageCountMetadataMismatch,
    KeyOrderViolation,
    KeyRangeViolation,
    MalformedPage,
    MalformedOverflowReference,
    OverflowLengthOutOfBounds,
    OverflowPageDataLengthOutOfBounds,
    OverflowChainLengthMismatch,
    OverflowChainPageCountOutOfBounds,
    OverflowDigestMismatch,
    DuplicatePageRef,
    EntryCountMismatch,
    NamedTableEntryCountMismatch,
    MalformedTableDescriptor,
    InvalidTableDescriptor,
    NamedTableHashCollision,
    DuplicateNamedTableSlotHash,
    InvalidPageType,
    PendingFreeEntryCountOutOfBounds,
    PendingFreePageOutOfBounds,
    PendingFreeEntryOutOfBounds,
    PendingFreeTransactionOutOfBounds,
    DuplicatePendingFreeEntry,
    PendingFreeEntryStillReachable,
}

impl PyIntegrityErrorKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::CommitSlotChecksumMismatch => "commit_slot_checksum_mismatch",
            Self::CommitSlotMacMismatch => "commit_slot_mac_mismatch",
            Self::CommitSlotDowngrade => "commit_slot_downgrade",
            Self::CommitSlotUnknownFormat => "commit_slot_unknown_format",
            Self::CommitSlotUnknownMerkleScheme => "commit_slot_unknown_merkle_scheme",
            Self::PageReadFailed => "page_read_failed",
            Self::PageTampered => "page_tampered",
            Self::ChecksumMismatch => "checksum_mismatch",
            Self::PageIdMismatch => "page_id_mismatch",
            Self::PageTransactionOutOfBounds => "page_transaction_out_of_bounds",
            Self::ReachablePageOutOfBounds => "reachable_page_out_of_bounds",
            Self::TreeDepthMismatch => "tree_depth_mismatch",
            Self::PageMerkleMismatch => "page_merkle_mismatch",
            Self::SlotMerkleRootMismatch => "slot_merkle_root_mismatch",
            Self::PageCountMetadataMismatch => "page_count_metadata_mismatch",
            Self::KeyOrderViolation => "key_order_violation",
            Self::KeyRangeViolation => "key_range_violation",
            Self::MalformedPage => "malformed_page",
            Self::MalformedOverflowReference => "malformed_overflow_reference",
            Self::OverflowLengthOutOfBounds => "overflow_length_out_of_bounds",
            Self::OverflowPageDataLengthOutOfBounds => "overflow_page_data_length_out_of_bounds",
            Self::OverflowChainLengthMismatch => "overflow_chain_length_mismatch",
            Self::OverflowChainPageCountOutOfBounds => "overflow_chain_page_count_out_of_bounds",
            Self::OverflowDigestMismatch => "overflow_digest_mismatch",
            Self::DuplicatePageRef => "duplicate_page_ref",
            Self::EntryCountMismatch => "entry_count_mismatch",
            Self::NamedTableEntryCountMismatch => "named_table_entry_count_mismatch",
            Self::MalformedTableDescriptor => "malformed_table_descriptor",
            Self::InvalidTableDescriptor => "invalid_table_descriptor",
            Self::NamedTableHashCollision => "named_table_hash_collision",
            Self::DuplicateNamedTableSlotHash => "duplicate_named_table_slot_hash",
            Self::InvalidPageType => "invalid_page_type",
            Self::PendingFreeEntryCountOutOfBounds => "pending_free_entry_count_out_of_bounds",
            Self::PendingFreePageOutOfBounds => "pending_free_page_out_of_bounds",
            Self::PendingFreeEntryOutOfBounds => "pending_free_entry_out_of_bounds",
            Self::PendingFreeTransactionOutOfBounds => "pending_free_transaction_out_of_bounds",
            Self::DuplicatePendingFreeEntry => "duplicate_pending_free_entry",
            Self::PendingFreeEntryStillReachable => "pending_free_entry_still_reachable",
        }
    }
}

fn integrity_error_kind(error: &IntegrityError) -> &'static str {
    let kind = match error {
        IntegrityError::CommitSlotChecksumMismatch { .. } => {
            PyIntegrityErrorKind::CommitSlotChecksumMismatch
        }
        IntegrityError::CommitSlotMacMismatch { .. } => PyIntegrityErrorKind::CommitSlotMacMismatch,
        IntegrityError::CommitSlotDowngrade { .. } => PyIntegrityErrorKind::CommitSlotDowngrade,
        IntegrityError::CommitSlotUnknownFormat { .. } => {
            PyIntegrityErrorKind::CommitSlotUnknownFormat
        }
        IntegrityError::CommitSlotUnknownMerkleScheme { .. } => {
            PyIntegrityErrorKind::CommitSlotUnknownMerkleScheme
        }
        IntegrityError::PageReadFailed { .. } => PyIntegrityErrorKind::PageReadFailed,
        IntegrityError::PageTampered(_) => PyIntegrityErrorKind::PageTampered,
        IntegrityError::ChecksumMismatch(_) => PyIntegrityErrorKind::ChecksumMismatch,
        IntegrityError::PageIdMismatch { .. } => PyIntegrityErrorKind::PageIdMismatch,
        IntegrityError::PageTransactionOutOfBounds { .. } => {
            PyIntegrityErrorKind::PageTransactionOutOfBounds
        }
        IntegrityError::ReachablePageOutOfBounds { .. } => {
            PyIntegrityErrorKind::ReachablePageOutOfBounds
        }
        IntegrityError::TreeDepthMismatch { .. } => PyIntegrityErrorKind::TreeDepthMismatch,
        IntegrityError::PageMerkleMismatch { .. } => PyIntegrityErrorKind::PageMerkleMismatch,
        IntegrityError::SlotMerkleRootMismatch { .. } => {
            PyIntegrityErrorKind::SlotMerkleRootMismatch
        }
        IntegrityError::PageCountMetadataMismatch { .. } => {
            PyIntegrityErrorKind::PageCountMetadataMismatch
        }
        IntegrityError::KeyOrderViolation { .. } => PyIntegrityErrorKind::KeyOrderViolation,
        IntegrityError::KeyRangeViolation { .. } => PyIntegrityErrorKind::KeyRangeViolation,
        IntegrityError::MalformedPage { .. } => PyIntegrityErrorKind::MalformedPage,
        IntegrityError::MalformedOverflowReference { .. } => {
            PyIntegrityErrorKind::MalformedOverflowReference
        }
        IntegrityError::OverflowLengthOutOfBounds { .. } => {
            PyIntegrityErrorKind::OverflowLengthOutOfBounds
        }
        IntegrityError::OverflowPageDataLengthOutOfBounds { .. } => {
            PyIntegrityErrorKind::OverflowPageDataLengthOutOfBounds
        }
        IntegrityError::OverflowChainLengthMismatch { .. } => {
            PyIntegrityErrorKind::OverflowChainLengthMismatch
        }
        IntegrityError::OverflowChainPageCountOutOfBounds { .. } => {
            PyIntegrityErrorKind::OverflowChainPageCountOutOfBounds
        }
        IntegrityError::OverflowDigestMismatch { .. } => {
            PyIntegrityErrorKind::OverflowDigestMismatch
        }
        IntegrityError::DuplicatePageRef(_) => PyIntegrityErrorKind::DuplicatePageRef,
        IntegrityError::EntryCountMismatch { .. } => PyIntegrityErrorKind::EntryCountMismatch,
        IntegrityError::NamedTableEntryCountMismatch { .. } => {
            PyIntegrityErrorKind::NamedTableEntryCountMismatch
        }
        IntegrityError::MalformedTableDescriptor { .. } => {
            PyIntegrityErrorKind::MalformedTableDescriptor
        }
        IntegrityError::InvalidTableDescriptor { .. } => {
            PyIntegrityErrorKind::InvalidTableDescriptor
        }
        IntegrityError::NamedTableHashCollision { .. } => {
            PyIntegrityErrorKind::NamedTableHashCollision
        }
        IntegrityError::DuplicateNamedTableSlotHash { .. } => {
            PyIntegrityErrorKind::DuplicateNamedTableSlotHash
        }
        IntegrityError::InvalidPageType { .. } => PyIntegrityErrorKind::InvalidPageType,
        IntegrityError::PendingFreeEntryCountOutOfBounds { .. } => {
            PyIntegrityErrorKind::PendingFreeEntryCountOutOfBounds
        }
        IntegrityError::PendingFreePageOutOfBounds { .. } => {
            PyIntegrityErrorKind::PendingFreePageOutOfBounds
        }
        IntegrityError::PendingFreeEntryOutOfBounds { .. } => {
            PyIntegrityErrorKind::PendingFreeEntryOutOfBounds
        }
        IntegrityError::PendingFreeTransactionOutOfBounds { .. } => {
            PyIntegrityErrorKind::PendingFreeTransactionOutOfBounds
        }
        IntegrityError::DuplicatePendingFreeEntry { .. } => {
            PyIntegrityErrorKind::DuplicatePendingFreeEntry
        }
        IntegrityError::PendingFreeEntryStillReachable { .. } => {
            PyIntegrityErrorKind::PendingFreeEntryStillReachable
        }
    };
    kind.as_str()
}

#[cfg(test)]
mod tests {
    use super::PyIntegrityErrorKind;

    #[test]
    fn every_public_integrity_kind_name_is_pinned() {
        use PyIntegrityErrorKind as K;
        let cases = [
            (
                K::CommitSlotChecksumMismatch,
                "commit_slot_checksum_mismatch",
            ),
            (K::CommitSlotMacMismatch, "commit_slot_mac_mismatch"),
            (K::CommitSlotDowngrade, "commit_slot_downgrade"),
            (K::CommitSlotUnknownFormat, "commit_slot_unknown_format"),
            (
                K::CommitSlotUnknownMerkleScheme,
                "commit_slot_unknown_merkle_scheme",
            ),
            (K::PageReadFailed, "page_read_failed"),
            (K::PageTampered, "page_tampered"),
            (K::ChecksumMismatch, "checksum_mismatch"),
            (K::PageIdMismatch, "page_id_mismatch"),
            (
                K::PageTransactionOutOfBounds,
                "page_transaction_out_of_bounds",
            ),
            (K::ReachablePageOutOfBounds, "reachable_page_out_of_bounds"),
            (K::TreeDepthMismatch, "tree_depth_mismatch"),
            (K::PageMerkleMismatch, "page_merkle_mismatch"),
            (K::SlotMerkleRootMismatch, "slot_merkle_root_mismatch"),
            (K::PageCountMetadataMismatch, "page_count_metadata_mismatch"),
            (K::KeyOrderViolation, "key_order_violation"),
            (K::KeyRangeViolation, "key_range_violation"),
            (K::MalformedPage, "malformed_page"),
            (
                K::MalformedOverflowReference,
                "malformed_overflow_reference",
            ),
            (
                K::OverflowLengthOutOfBounds,
                "overflow_length_out_of_bounds",
            ),
            (
                K::OverflowPageDataLengthOutOfBounds,
                "overflow_page_data_length_out_of_bounds",
            ),
            (
                K::OverflowChainLengthMismatch,
                "overflow_chain_length_mismatch",
            ),
            (
                K::OverflowChainPageCountOutOfBounds,
                "overflow_chain_page_count_out_of_bounds",
            ),
            (K::OverflowDigestMismatch, "overflow_digest_mismatch"),
            (K::DuplicatePageRef, "duplicate_page_ref"),
            (K::EntryCountMismatch, "entry_count_mismatch"),
            (
                K::NamedTableEntryCountMismatch,
                "named_table_entry_count_mismatch",
            ),
            (K::MalformedTableDescriptor, "malformed_table_descriptor"),
            (K::InvalidTableDescriptor, "invalid_table_descriptor"),
            (K::NamedTableHashCollision, "named_table_hash_collision"),
            (
                K::DuplicateNamedTableSlotHash,
                "duplicate_named_table_slot_hash",
            ),
            (K::InvalidPageType, "invalid_page_type"),
            (
                K::PendingFreeEntryCountOutOfBounds,
                "pending_free_entry_count_out_of_bounds",
            ),
            (
                K::PendingFreePageOutOfBounds,
                "pending_free_page_out_of_bounds",
            ),
            (
                K::PendingFreeEntryOutOfBounds,
                "pending_free_entry_out_of_bounds",
            ),
            (
                K::PendingFreeTransactionOutOfBounds,
                "pending_free_transaction_out_of_bounds",
            ),
            (K::DuplicatePendingFreeEntry, "duplicate_pending_free_entry"),
            (
                K::PendingFreeEntryStillReachable,
                "pending_free_entry_still_reachable",
            ),
        ];
        for (kind, expected) in cases {
            assert_eq!(kind.as_str(), expected);
        }
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
        let sql = sql.to_owned();
        let shared = Arc::clone(self.shared_conn()?);
        let result = py
            .detach(move || {
                let cell = shared.cell.lock();
                cell.with_dependent(|_owner, conn| match &values {
                    Some(v) => conn.execute_params(&sql, v),
                    None => conn.execute(&sql),
                })
            })
            .map_err(to_pyerr)?;
        match result {
            ExecutionResult::RowsAffected(n) => (n as i64).into_py_any(py),
            ExecutionResult::Query(qr) => PyQueryResult::from(qr).into_py_any(py),
            ExecutionResult::Ok => Ok(py.None()),
        }
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
        let sql = sql.to_owned();
        let shared = Arc::clone(self.shared_conn()?);
        py.detach(move || {
            let cell = shared.cell.lock();
            cell.with_dependent(|_owner, conn| match &values {
                Some(v) => conn.query_params(&sql, v),
                None => conn.query(&sql),
            })
        })
        .map(PyQueryResult::from)
        .map_err(to_pyerr)
    }

    /// Execute `;`-separated statements; returns one result per completed statement.
    /// Stops and raises at the first error (completed statements persist).
    fn execute_script(&self, py: Python<'_>, sql: &str) -> PyResult<Vec<Py<PyAny>>> {
        let sql = sql.to_owned();
        let shared = Arc::clone(self.shared_conn()?);
        let exec = py.detach(move || {
            let cell = shared.cell.lock();
            cell.with_dependent(|_owner, conn| conn.execute_script(&sql))
        });
        let mut out = Vec::with_capacity(exec.completed.len());
        for result in exec.completed {
            out.push(match result {
                ExecutionResult::RowsAffected(n) => (n as i64).into_py_any(py)?,
                ExecutionResult::Query(qr) => PyQueryResult::from(qr).into_py_any(py)?,
                ExecutionResult::Ok => py.None(),
            });
        }
        match exec.error {
            Some(error) => Err(to_pyerr(error)),
            None => Ok(out),
        }
    }

    /// Install a token for subsequent SQL, memory, and integrity work on every
    /// handle sharing this database, or clear it. A tripped token stays tripped.
    #[pyo3(signature = (token=None))]
    fn set_cancel(&self, token: Option<PyRef<'_, PyCancelToken>>) -> PyResult<()> {
        self.database()?
            .set_cancel(token.map(|token| token.inner.clone()));
        Ok(())
    }

    /// Names of the user tables.
    fn tables(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let shared = Arc::clone(self.shared_conn()?);
        Ok(py.detach(move || {
            let cell = shared.cell.lock();
            cell.with_dependent(|_owner, conn| conn.tables())
        }))
    }

    /// Whether an explicit transaction is open.
    fn in_transaction(&self) -> PyResult<bool> {
        self.with_connection(|conn| conn.in_transaction())
    }

    /// Open the memory engine over this database (shares the underlying storage).
    fn memory(&self, py: Python<'_>) -> PyResult<PyMemory> {
        let db = self.database()?;
        if let Some(engine) = self.memory.get() {
            return Ok(PyMemory::from_engine(Arc::clone(engine)));
        }
        let engine = py.detach(move || shared_engine(&db))?;
        let _ = self.memory.set(Arc::clone(&engine));
        Ok(PyMemory::from_engine(engine))
    }

    /// Open model-free inventory, verification, and erasure for existing memory data.
    fn memory_maintenance(&self, py: Python<'_>) -> PyResult<PyMemoryMaintenance> {
        PyMemoryMaintenance::open(py, self.database()?)
    }

    /// Storage statistics: `{tree_depth, entry_count, total_pages, high_water_mark, merkle_root}`.
    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let s = self.database()?.stats();
        let d = PyDict::new(py);
        d.set_item("tree_depth", s.tree_depth)?;
        d.set_item("entry_count", s.entry_count)?;
        d.set_item("total_pages", s.total_pages)?;
        d.set_item("high_water_mark", s.high_water_mark)?;
        d.set_item("merkle_root", PyBytes::new(py, &s.merkle_root))?;
        d.into_py_any(py)
    }

    /// Verify page integrity. Returns counts plus every finding's stable kind,
    /// message, and tamper classification. `quiet=True` writes no audit entry.
    #[pyo3(signature = (quiet=false))]
    fn integrity_check(&self, py: Python<'_>, quiet: bool) -> PyResult<Py<PyAny>> {
        let db = self.database()?;
        let r = py
            .detach(move || {
                if quiet {
                    db.integrity_check_quiet()
                } else {
                    db.integrity_check()
                }
            })
            .map_err(to_pyerr)?;
        let d = PyDict::new(py);
        d.set_item("ok", r.is_ok())?;
        d.set_item("pages_checked", r.pages_checked)?;
        d.set_item("error_count", r.errors.len())?;
        d.set_item("tampered_count", r.tampered().count())?;
        let errors = PyList::empty(py);
        for error in &r.errors {
            let item = PyDict::new(py);
            item.set_item("kind", integrity_error_kind(error))?;
            item.set_item("message", error.to_string())?;
            item.set_item("tampered", error.is_tamper())?;
            errors.append(item)?;
        }
        d.set_item("errors", errors)?;
        d.into_py_any(py)
    }

    /// Write a consistent encrypted copy of the database to `dest`.
    fn backup(&self, py: Python<'_>, dest: &str) -> PyResult<()> {
        let db = self.database()?;
        let dest = dest.to_owned();
        py.detach(move || db.backup(std::path::Path::new(&dest)))
            .map_err(to_pyerr)
    }

    /// Write a compacted (free-space-reclaimed) copy of the database to `dest`.
    fn compact(&self, py: Python<'_>, dest: &str) -> PyResult<()> {
        let db = self.database()?;
        let dest = dest.to_owned();
        py.detach(move || db.compact(std::path::Path::new(&dest)))
            .map_err(to_pyerr)
    }

    /// Whether `passphrase` unwraps this database, read from the key file.
    fn verify_passphrase(&self, py: Python<'_>, passphrase: &str) -> PyResult<bool> {
        let db = self.database()?;
        let passphrase = Zeroizing::new(passphrase.as_bytes().to_owned());
        py.detach(move || db.verify_passphrase(&passphrase))
            .map_err(to_pyerr)
    }

    /// Re-wrap the root key under a new passphrase (data is not re-encrypted).
    fn change_passphrase(&self, py: Python<'_>, old: &str, new: &str) -> PyResult<()> {
        let db = self.database()?;
        let old = Zeroizing::new(old.as_bytes().to_owned());
        let new = Zeroizing::new(new.as_bytes().to_owned());
        py.detach(move || db.change_passphrase(&old, &new))
            .map_err(to_pyerr)
    }

    /// Freeze the ANN index for a VECTOR `column` of `table` into a persisted
    /// segment so a later cold open LOADs it instead of rebuilding by full scan.
    /// Returns the segment manifest dict.
    fn persist_ann_index(&self, py: Python<'_>, table: &str, column: &str) -> PyResult<Py<PyAny>> {
        // Keep this handle's transaction state and schema while releasing the GIL;
        // a fresh connection would bypass the explicit-transaction refusal.
        let shared = Arc::clone(self.shared_conn()?);
        let (table, column) = (table.to_string(), column.to_string());
        let info = py
            .detach(move || {
                let cell = shared.cell.lock();
                cell.with_dependent(|_owner, conn| conn.persist_ann_index(&table, &column))
            })
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
        let shared = Arc::clone(self.shared_conn()?);
        let (table, column) = (table.to_owned(), column.to_owned());
        let status = py
            .detach(move || {
                let cell = shared.cell.lock();
                cell.with_dependent(|_owner, conn| conn.ann_cache_status(&table, &column))
            })
            .map_err(to_pyerr)?;
        match status {
            None => Ok(None),
            Some((src, generation)) => {
                let d = ann_index_source_dict(py, &src)?;
                d.set_item("generation", generation)?;
                Ok(Some(d.into_py_any(py)?))
            }
        }
    }

    /// Export an encrypted key escrow (under its own `backup_pass`) for disaster
    /// recovery; restore later with `restore_key_from_backup` if the DB passphrase
    /// is lost. Requires the current DB passphrase.
    fn export_key_backup(
        &self,
        py: Python<'_>,
        db_pass: &str,
        backup_pass: &str,
        dest: &str,
    ) -> PyResult<()> {
        let db = self.database()?;
        let db_pass = Zeroizing::new(db_pass.as_bytes().to_owned());
        let backup_pass = Zeroizing::new(backup_pass.as_bytes().to_owned());
        let dest = dest.to_owned();
        py.detach(move || db.export_key_backup(&db_pass, &backup_pass, std::path::Path::new(&dest)))
            .map_err(to_pyerr)
    }

    /// Recreate a key file from an escrow `backup` under `new_db_pass`, for the
    /// database at `db_path`. Static: no open handle needed.
    #[staticmethod]
    fn restore_key_from_backup(
        py: Python<'_>,
        backup: &str,
        backup_pass: &str,
        new_db_pass: &str,
        db_path: &str,
    ) -> PyResult<()> {
        let backup = backup.to_owned();
        let backup_pass = Zeroizing::new(backup_pass.as_bytes().to_owned());
        let new_db_pass = Zeroizing::new(new_db_pass.as_bytes().to_owned());
        let db_path = db_path.to_owned();
        py.detach(move || {
            Database::restore_key_from_backup(
                std::path::Path::new(&backup),
                &backup_pass,
                &new_db_pass,
                std::path::Path::new(&db_path),
            )
        })
        .map_err(to_pyerr)
    }

    /// Verify the live audit segment's HMAC chain (on by default for file
    /// databases): `{entries_verified, chain_valid, chain_break_at}`. This
    /// single-segment check does not authenticate the mutable header count or
    /// provide an anti-rollback guarantee.
    fn verify_audit_log(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let db = self.database()?;
        let r = py.detach(move || db.verify_audit_log()).map_err(to_pyerr)?;
        let d = PyDict::new(py);
        d.set_item("entries_verified", r.entries_verified)?;
        d.set_item("chain_valid", r.chain_valid)?;
        d.set_item("chain_break_at", r.chain_break_at)?;
        d.into_py_any(py)
    }

    /// Path of the live HMAC-chained audit segment, or `None` if disabled
    /// (in-memory DBs).
    fn audit_log_path(&self) -> PyResult<Option<String>> {
        Ok(self
            .database()?
            .audit_log_path()
            .map(|p| p.to_string_lossy().into_owned()))
    }

    /// Release this handle's connection and database reference. Later calls raise.
    /// Other handles over the same file are unaffected; the file is released once
    /// the last of them, and any memory or maintenance capability they built, has dropped.
    fn close(&mut self) {
        self.conn = None;
        // Drop this handle's cached engine; separately returned capabilities retain theirs.
        self.memory.take();
    }

    /// True once `close` has run on this handle; other handles are unaffected.
    #[getter]
    fn is_closed(&self) -> bool {
        self.conn.is_none()
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        slf.ensure_owner_thread()?;
        Ok(slf)
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
        if self.conn.is_some() {
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
#[derive(Clone, Copy)]
#[pyclass(name = "DatabaseOptions", skip_from_py_object)]
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
    py: Python<'_>,
    path: Option<String>,
    key: &str,
    create: Option<bool>,
    region_keys: bool,
    options: Option<&PyDatabaseOptions>,
) -> PyResult<PyDatabase> {
    let key = Zeroizing::new(key.as_bytes().to_owned());
    let options = options.copied();
    py.detach(move || connect_detached(path, key, create, region_keys, options))
}

fn connect_detached(
    path: Option<String>,
    key: Zeroizing<Vec<u8>>,
    create: Option<bool>,
    region_keys: bool,
    options: Option<PyDatabaseOptions>,
) -> PyResult<PyDatabase> {
    let configure = |mut b: DatabaseBuilder| {
        b = b.passphrase(&key).enable_region_keys(region_keys);
        if let Some(o) = options.as_ref() {
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
    // Held across the lookup and the insert: two threads racing to open one new file
    // would otherwise both open it, and the loser would fail on the file lock.
    let mut open = OPEN_FILES.lock().unwrap();
    if in_memory {
        let db = configure(DatabaseBuilder::new(""))
            .create_in_memory()
            .map_err(to_pyerr)?;
        return attach(&mut open, None, Arc::new(db));
    }

    let p = path.as_deref().unwrap();
    if let Some(ident) = identity(std::path::Path::new(p)) {
        let shared = reuse(
            &mut open,
            &ident,
            p,
            &key,
            region_keys,
            options.is_some(),
            create,
        )?;
        if let Some(db) = shared {
            return attach(&mut open, Some(&ident), db);
        }
    }

    let builder = configure(DatabaseBuilder::new(p));
    let exists = std::path::Path::new(p).exists();
    let owner = Arc::new(
        match create {
            Some(true) => builder.create(),
            Some(false) => builder.open(),
            None if exists => builder.open(),
            None => builder.create(),
        }
        .map_err(to_pyerr)?,
    );
    let ident = identity(owner.data_path());
    if let Some(ident) = ident.clone() {
        // Dropped files are only ever noticed on their own path, so a long-lived
        // process would otherwise keep an entry per file it had ever opened.
        open.retain(|_, e| e.db.strong_count() > 0);
        OPEN_CONNS.with_borrow_mut(|c| c.retain(|_, w| w.strong_count() > 0));
        open.insert(
            ident,
            OpenFile {
                db: Arc::downgrade(&owner),
                engine: std::sync::Weak::new(),
                conn: std::sync::Weak::new(),
                owner_thread: std::thread::current().id(),
                key_file: file_hash(owner.key_path())?,
                passphrase: passphrase_digest(&key),
            },
        );
    }
    attach(&mut open, ident.as_deref(), owner)
}
