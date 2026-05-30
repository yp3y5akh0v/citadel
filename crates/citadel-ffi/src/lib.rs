//! C FFI bindings for Citadel encrypted database.
//!
//! Provides opaque handle types and C-compatible functions. All
//! functions are panic-safe via catch_unwind.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;
use std::ptr;
use std::slice;

use citadel::{Database, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

/// Error codes returned by all citadel_* functions.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CitadelError {
    Ok = 0,
    InvalidArgument = -1,
    IoError = -2,
    BadPassphrase = -3,
    DatabaseLocked = -4,
    DatabaseCorrupted = -5,
    PageTampered = -6,
    TransactionTooLarge = -7,
    KeyTooLarge = -8,
    ValueTooLarge = -9,
    TableNotFound = -10,
    TableAlreadyExists = -11,
    KeyFileMismatch = -12,
    PassphraseRequired = -13,
    NoWriteTransaction = -14,
    WriteTransactionActive = -15,
    SqlError = -16,
    InternalPanic = -99,
}

/// Opaque database configuration.
#[repr(C)]
pub struct CitadelConfig {
    pub cache_size: u32,
    pub argon2_profile: u8,
    pub cipher_id: u8,
    _reserved: [u8; 26],
}

impl Default for CitadelConfig {
    fn default() -> Self {
        Self {
            cache_size: 256,
            argon2_profile: 1,
            cipher_id: 0,
            _reserved: [0; 26],
        }
    }
}

/// Opaque database handle.
pub struct CitadelDb {
    db: Database,
}

/// Opaque read transaction handle.
pub struct CitadelReadTxn {
    txn: citadel::txn::read_txn::ReadTxn<'static>,
}

/// Opaque write transaction handle.
pub struct CitadelWriteTxn {
    txn: Option<citadel::txn::write_txn::WriteTxn<'static>>,
}

/// Opaque SQL connection handle.
pub struct CitadelSqlConn {
    conn: Connection<'static>,
}

/// Opaque SQL result handle.
pub struct CitadelSqlResult {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    rows_affected: u64,
    is_query: bool,
}

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(msg: &str) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg).ok();
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = None;
    });
}

fn map_error(err: &citadel_core::Error) -> CitadelError {
    match err {
        citadel_core::Error::Io(_) => CitadelError::IoError,
        citadel_core::Error::BadPassphrase => CitadelError::BadPassphrase,
        citadel_core::Error::DatabaseLocked => CitadelError::DatabaseLocked,
        citadel_core::Error::DatabaseCorrupted => CitadelError::DatabaseCorrupted,
        citadel_core::Error::PageTampered(_) => CitadelError::PageTampered,
        citadel_core::Error::TransactionTooLarge { .. } => CitadelError::TransactionTooLarge,
        citadel_core::Error::KeyTooLarge { .. } => CitadelError::KeyTooLarge,
        citadel_core::Error::ValueTooLarge { .. } => CitadelError::ValueTooLarge,
        citadel_core::Error::TableNotFound(_) => CitadelError::TableNotFound,
        citadel_core::Error::TableAlreadyExists(_) => CitadelError::TableAlreadyExists,
        citadel_core::Error::KeyFileMismatch => CitadelError::KeyFileMismatch,
        citadel_core::Error::KeyFileIntegrity => CitadelError::BadPassphrase,
        citadel_core::Error::KeyUnwrapFailed => CitadelError::BadPassphrase,
        citadel_core::Error::PassphraseRequired => CitadelError::PassphraseRequired,
        citadel_core::Error::NoWriteTransaction => CitadelError::NoWriteTransaction,
        citadel_core::Error::WriteTransactionActive => CitadelError::WriteTransactionActive,
        _ => CitadelError::IoError,
    }
}

fn map_core_error(err: citadel_core::Error) -> CitadelError {
    let code = map_error(&err);
    set_last_error(&err.to_string());
    code
}

fn map_sql_error(err: citadel_sql::SqlError) -> CitadelError {
    set_last_error(&err.to_string());
    match err {
        citadel_sql::SqlError::Storage(e) => map_error(&e),
        _ => CitadelError::SqlError,
    }
}

/// Wraps an FFI function body with panic catching.
macro_rules! ffi_guard {
    ($body:expr) => {{
        clear_last_error();
        match panic::catch_unwind(AssertUnwindSafe(|| $body)) {
            Ok(result) => result,
            Err(_) => {
                set_last_error("internal panic in citadel FFI");
                CitadelError::InternalPanic
            }
        }
    }};
}

/// Get the last error message for the current thread.
///
/// Returns a pointer to a null-terminated UTF-8 string. The pointer is
/// valid until the next citadel_* call on this thread. Returns NULL if
/// no error occurred.
#[no_mangle]
pub extern "C" fn citadel_last_error_message() -> *const c_char {
    LAST_ERROR.with(|e| match e.borrow().as_ref() {
        Some(cstr) => cstr.as_ptr(),
        None => ptr::null(),
    })
}

/// Get the library version string.
///
/// Returns a pointer to a static null-terminated string.
#[no_mangle]
pub extern "C" fn citadel_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const c_char
}

/// Create a new encrypted database.
///
/// # Parameters
/// - `path`: null-terminated UTF-8 path to the data file
/// - `passphrase`: passphrase bytes (not null-terminated)
/// - `passphrase_len`: length of the passphrase
/// - `config`: optional configuration (NULL for defaults)
/// - `out`: receives the database handle on success
///
/// # Returns
/// `CITADEL_OK` on success, error code on failure.
#[no_mangle]
pub extern "C" fn citadel_create(
    path: *const c_char,
    passphrase: *const u8,
    passphrase_len: usize,
    config: *const CitadelConfig,
    out: *mut *mut CitadelDb,
) -> CitadelError {
    ffi_guard!({
        if path.is_null() || passphrase.is_null() || out.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("path is not valid UTF-8");
                return CitadelError::InvalidArgument;
            }
        };
        let pass = unsafe { slice::from_raw_parts(passphrase, passphrase_len) };

        let mut builder = DatabaseBuilder::new(PathBuf::from(path_str)).passphrase(pass);

        if !config.is_null() {
            let cfg = unsafe { &*config };
            if cfg.cache_size > 0 {
                builder = builder.cache_size(cfg.cache_size as usize);
            }
            builder = match cfg.argon2_profile {
                0 => builder.argon2_profile(citadel::Argon2Profile::Iot),
                2 => builder.argon2_profile(citadel::Argon2Profile::Server),
                _ => builder.argon2_profile(citadel::Argon2Profile::Desktop),
            };
            builder = match cfg.cipher_id {
                1 => builder.cipher(citadel::CipherId::ChaCha20),
                _ => builder.cipher(citadel::CipherId::Aes256Ctr),
            };
        }

        match builder.create() {
            Ok(db) => {
                let handle = Box::new(CitadelDb { db });
                unsafe { *out = Box::into_raw(handle) };
                CitadelError::Ok
            }
            Err(e) => map_core_error(e),
        }
    })
}

/// Open an existing encrypted database.
///
/// # Parameters
/// - `path`: null-terminated UTF-8 path to the data file
/// - `passphrase`: passphrase bytes (not null-terminated)
/// - `passphrase_len`: length of the passphrase
/// - `config`: optional configuration (NULL for defaults)
/// - `out`: receives the database handle on success
///
/// # Returns
/// `CITADEL_OK` on success, error code on failure.
#[no_mangle]
pub extern "C" fn citadel_open(
    path: *const c_char,
    passphrase: *const u8,
    passphrase_len: usize,
    config: *const CitadelConfig,
    out: *mut *mut CitadelDb,
) -> CitadelError {
    ffi_guard!({
        if path.is_null() || passphrase.is_null() || out.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("path is not valid UTF-8");
                return CitadelError::InvalidArgument;
            }
        };
        let pass = unsafe { slice::from_raw_parts(passphrase, passphrase_len) };

        let mut builder = DatabaseBuilder::new(PathBuf::from(path_str)).passphrase(pass);

        if !config.is_null() {
            let cfg = unsafe { &*config };
            if cfg.cache_size > 0 {
                builder = builder.cache_size(cfg.cache_size as usize);
            }
        }

        match builder.open() {
            Ok(db) => {
                let handle = Box::new(CitadelDb { db });
                unsafe { *out = Box::into_raw(handle) };
                CitadelError::Ok
            }
            Err(e) => map_core_error(e),
        }
    })
}

/// Close a database and free its resources.
///
/// Accepts NULL (no-op). After this call the handle is invalid.
#[no_mangle]
pub extern "C" fn citadel_close(db: *mut CitadelDb) {
    if !db.is_null() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            unsafe { drop(Box::from_raw(db)) };
        }));
    }
}

/// Begin a read-only transaction.
///
/// Multiple read transactions can be active simultaneously.
#[no_mangle]
pub extern "C" fn citadel_read_begin(
    db: *mut CitadelDb,
    out: *mut *mut CitadelReadTxn,
) -> CitadelError {
    ffi_guard!({
        if db.is_null() || out.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let db_ref = unsafe { &*db };
        // Safety: transmute to 'static - caller must ensure db outlives txn.
        let txn = db_ref.db.begin_read();
        let txn: citadel::txn::read_txn::ReadTxn<'static> = unsafe { std::mem::transmute(txn) };

        let handle = Box::new(CitadelReadTxn { txn });
        unsafe { *out = Box::into_raw(handle) };
        CitadelError::Ok
    })
}

/// End a read transaction and free its resources.
///
/// Accepts NULL (no-op).
#[no_mangle]
pub extern "C" fn citadel_read_end(txn: *mut CitadelReadTxn) {
    if !txn.is_null() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            unsafe { drop(Box::from_raw(txn)) };
        }));
    }
}

/// Get a value by key in a read transaction.
///
/// On success, `*out_val` and `*out_val_len` are set. The memory is
/// allocated by Citadel and must be freed with `citadel_free_bytes`.
/// If the key is not found, `*out_val` is set to NULL and
/// `*out_val_len` to 0, and the function returns `CITADEL_OK`.
#[no_mangle]
pub extern "C" fn citadel_read_get(
    txn: *mut CitadelReadTxn,
    key: *const u8,
    key_len: usize,
    out_val: *mut *mut u8,
    out_val_len: *mut usize,
) -> CitadelError {
    ffi_guard!({
        if txn.is_null() || key.is_null() || out_val.is_null() || out_val_len.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let txn_ref = unsafe { &mut *txn };
        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };

        match txn_ref.txn.get(key_slice) {
            Ok(Some(val)) => {
                let mut boxed = val.into_boxed_slice();
                let len = boxed.len();
                let ptr = boxed.as_mut_ptr();
                std::mem::forget(boxed);
                unsafe {
                    *out_val = ptr;
                    *out_val_len = len;
                }
                CitadelError::Ok
            }
            Ok(None) => {
                unsafe {
                    *out_val = ptr::null_mut();
                    *out_val_len = 0;
                }
                CitadelError::Ok
            }
            Err(e) => map_core_error(e),
        }
    })
}

/// Get a value by key from a named table in a read transaction.
#[no_mangle]
pub extern "C" fn citadel_read_table_get(
    txn: *mut CitadelReadTxn,
    table: *const u8,
    table_len: usize,
    key: *const u8,
    key_len: usize,
    out_val: *mut *mut u8,
    out_val_len: *mut usize,
) -> CitadelError {
    ffi_guard!({
        if txn.is_null()
            || table.is_null()
            || key.is_null()
            || out_val.is_null()
            || out_val_len.is_null()
        {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let txn_ref = unsafe { &mut *txn };
        let table_slice = unsafe { slice::from_raw_parts(table, table_len) };
        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };

        match txn_ref.txn.table_get(table_slice, key_slice) {
            Ok(Some(val)) => {
                let mut boxed = val.into_boxed_slice();
                let len = boxed.len();
                let ptr = boxed.as_mut_ptr();
                std::mem::forget(boxed);
                unsafe {
                    *out_val = ptr;
                    *out_val_len = len;
                }
                CitadelError::Ok
            }
            Ok(None) => {
                unsafe {
                    *out_val = ptr::null_mut();
                    *out_val_len = 0;
                }
                CitadelError::Ok
            }
            Err(e) => map_core_error(e),
        }
    })
}

/// Begin a read-write transaction.
///
/// Only one write transaction can be active at a time.
#[no_mangle]
pub extern "C" fn citadel_write_begin(
    db: *mut CitadelDb,
    out: *mut *mut CitadelWriteTxn,
) -> CitadelError {
    ffi_guard!({
        if db.is_null() || out.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let db_ref = unsafe { &*db };
        let txn = match db_ref.db.begin_write() {
            Ok(t) => t,
            Err(e) => return map_core_error(e),
        };
        let txn: citadel::txn::write_txn::WriteTxn<'static> = unsafe { std::mem::transmute(txn) };

        let handle = Box::new(CitadelWriteTxn { txn: Some(txn) });
        unsafe { *out = Box::into_raw(handle) };
        CitadelError::Ok
    })
}

/// Commit a write transaction.
///
/// On success the handle is consumed and freed. On failure the
/// transaction is still valid and can be retried or aborted.
#[no_mangle]
pub extern "C" fn citadel_write_commit(txn: *mut CitadelWriteTxn) -> CitadelError {
    ffi_guard!({
        if txn.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let txn_ref = unsafe { &mut *txn };
        let inner = match txn_ref.txn.take() {
            Some(t) => t,
            None => {
                set_last_error("transaction already consumed");
                return CitadelError::InvalidArgument;
            }
        };

        match inner.commit() {
            Ok(()) => {
                unsafe { drop(Box::from_raw(txn)) };
                CitadelError::Ok
            }
            Err(e) => {
                // Commit failed - transaction was consumed by commit()
                // (it takes self), so we free the handle.
                unsafe { drop(Box::from_raw(txn)) };
                map_core_error(e)
            }
        }
    })
}

/// Abort a write transaction and discard all changes.
///
/// Accepts NULL (no-op). The handle is freed.
#[no_mangle]
pub extern "C" fn citadel_write_abort(txn: *mut CitadelWriteTxn) {
    if !txn.is_null() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            let txn_ref = unsafe { Box::from_raw(txn) };
            if let Some(inner) = txn_ref.txn {
                inner.abort();
            }
        }));
    }
}

/// Insert or update a key-value pair in the default table.
///
/// `*was_new` is set to 1 if the key was new, 0 if it was updated.
/// `was_new` can be NULL if the caller doesn't care.
#[no_mangle]
pub extern "C" fn citadel_write_put(
    txn: *mut CitadelWriteTxn,
    key: *const u8,
    key_len: usize,
    val: *const u8,
    val_len: usize,
    was_new: *mut i32,
) -> CitadelError {
    ffi_guard!({
        if txn.is_null() || key.is_null() || val.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let txn_ref = unsafe { &mut *txn };
        let inner = match txn_ref.txn.as_mut() {
            Some(t) => t,
            None => {
                set_last_error("transaction already consumed");
                return CitadelError::InvalidArgument;
            }
        };

        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };
        let val_slice = unsafe { slice::from_raw_parts(val, val_len) };

        match inner.insert(key_slice, val_slice) {
            Ok(is_new) => {
                if !was_new.is_null() {
                    unsafe { *was_new = if is_new { 1 } else { 0 } };
                }
                CitadelError::Ok
            }
            Err(e) => map_core_error(e),
        }
    })
}

/// Delete a key from the default table.
///
/// `*existed` is set to 1 if the key existed, 0 otherwise.
/// `existed` can be NULL if the caller doesn't care.
#[no_mangle]
pub extern "C" fn citadel_write_delete(
    txn: *mut CitadelWriteTxn,
    key: *const u8,
    key_len: usize,
    existed: *mut i32,
) -> CitadelError {
    ffi_guard!({
        if txn.is_null() || key.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let txn_ref = unsafe { &mut *txn };
        let inner = match txn_ref.txn.as_mut() {
            Some(t) => t,
            None => {
                set_last_error("transaction already consumed");
                return CitadelError::InvalidArgument;
            }
        };

        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };

        match inner.delete(key_slice) {
            Ok(was_present) => {
                if !existed.is_null() {
                    unsafe { *existed = if was_present { 1 } else { 0 } };
                }
                CitadelError::Ok
            }
            Err(e) => map_core_error(e),
        }
    })
}

/// Get a value by key within a write transaction.
///
/// Same semantics as `citadel_read_get` but within an active write txn.
#[no_mangle]
pub extern "C" fn citadel_write_get(
    txn: *mut CitadelWriteTxn,
    key: *const u8,
    key_len: usize,
    out_val: *mut *mut u8,
    out_val_len: *mut usize,
) -> CitadelError {
    ffi_guard!({
        if txn.is_null() || key.is_null() || out_val.is_null() || out_val_len.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let txn_ref = unsafe { &mut *txn };
        let inner = match txn_ref.txn.as_mut() {
            Some(t) => t,
            None => {
                set_last_error("transaction already consumed");
                return CitadelError::InvalidArgument;
            }
        };

        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };

        match inner.get(key_slice) {
            Ok(Some(val)) => {
                let mut boxed = val.into_boxed_slice();
                let len = boxed.len();
                let ptr = boxed.as_mut_ptr();
                std::mem::forget(boxed);
                unsafe {
                    *out_val = ptr;
                    *out_val_len = len;
                }
                CitadelError::Ok
            }
            Ok(None) => {
                unsafe {
                    *out_val = ptr::null_mut();
                    *out_val_len = 0;
                }
                CitadelError::Ok
            }
            Err(e) => map_core_error(e),
        }
    })
}

/// Create a named table within a write transaction.
#[no_mangle]
pub extern "C" fn citadel_write_create_table(
    txn: *mut CitadelWriteTxn,
    name: *const u8,
    name_len: usize,
) -> CitadelError {
    ffi_guard!({
        if txn.is_null() || name.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let txn_ref = unsafe { &mut *txn };
        let inner = match txn_ref.txn.as_mut() {
            Some(t) => t,
            None => {
                set_last_error("transaction already consumed");
                return CitadelError::InvalidArgument;
            }
        };

        let name_slice = unsafe { slice::from_raw_parts(name, name_len) };

        match inner.create_table(name_slice) {
            Ok(()) => CitadelError::Ok,
            Err(e) => map_core_error(e),
        }
    })
}

/// Drop a named table within a write transaction.
#[no_mangle]
pub extern "C" fn citadel_write_drop_table(
    txn: *mut CitadelWriteTxn,
    name: *const u8,
    name_len: usize,
) -> CitadelError {
    ffi_guard!({
        if txn.is_null() || name.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let txn_ref = unsafe { &mut *txn };
        let inner = match txn_ref.txn.as_mut() {
            Some(t) => t,
            None => {
                set_last_error("transaction already consumed");
                return CitadelError::InvalidArgument;
            }
        };

        let name_slice = unsafe { slice::from_raw_parts(name, name_len) };

        match inner.drop_table(name_slice) {
            Ok(()) => CitadelError::Ok,
            Err(e) => map_core_error(e),
        }
    })
}

/// Insert or update a key-value pair in a named table.
#[no_mangle]
pub extern "C" fn citadel_write_table_put(
    txn: *mut CitadelWriteTxn,
    table: *const u8,
    table_len: usize,
    key: *const u8,
    key_len: usize,
    val: *const u8,
    val_len: usize,
    was_new: *mut i32,
) -> CitadelError {
    ffi_guard!({
        if txn.is_null() || table.is_null() || key.is_null() || val.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let txn_ref = unsafe { &mut *txn };
        let inner = match txn_ref.txn.as_mut() {
            Some(t) => t,
            None => {
                set_last_error("transaction already consumed");
                return CitadelError::InvalidArgument;
            }
        };

        let table_slice = unsafe { slice::from_raw_parts(table, table_len) };
        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };
        let val_slice = unsafe { slice::from_raw_parts(val, val_len) };

        match inner.table_insert(table_slice, key_slice, val_slice) {
            Ok(is_new) => {
                if !was_new.is_null() {
                    unsafe { *was_new = if is_new { 1 } else { 0 } };
                }
                CitadelError::Ok
            }
            Err(e) => map_core_error(e),
        }
    })
}

/// Delete a key from a named table.
#[no_mangle]
pub extern "C" fn citadel_write_table_delete(
    txn: *mut CitadelWriteTxn,
    table: *const u8,
    table_len: usize,
    key: *const u8,
    key_len: usize,
    existed: *mut i32,
) -> CitadelError {
    ffi_guard!({
        if txn.is_null() || table.is_null() || key.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let txn_ref = unsafe { &mut *txn };
        let inner = match txn_ref.txn.as_mut() {
            Some(t) => t,
            None => {
                set_last_error("transaction already consumed");
                return CitadelError::InvalidArgument;
            }
        };

        let table_slice = unsafe { slice::from_raw_parts(table, table_len) };
        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };

        match inner.table_delete(table_slice, key_slice) {
            Ok(was_present) => {
                if !existed.is_null() {
                    unsafe { *existed = if was_present { 1 } else { 0 } };
                }
                CitadelError::Ok
            }
            Err(e) => map_core_error(e),
        }
    })
}

/// Get a value by key from a named table within a write transaction.
#[no_mangle]
pub extern "C" fn citadel_write_table_get(
    txn: *mut CitadelWriteTxn,
    table: *const u8,
    table_len: usize,
    key: *const u8,
    key_len: usize,
    out_val: *mut *mut u8,
    out_val_len: *mut usize,
) -> CitadelError {
    ffi_guard!({
        if txn.is_null()
            || table.is_null()
            || key.is_null()
            || out_val.is_null()
            || out_val_len.is_null()
        {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let txn_ref = unsafe { &mut *txn };
        let inner = match txn_ref.txn.as_mut() {
            Some(t) => t,
            None => {
                set_last_error("transaction already consumed");
                return CitadelError::InvalidArgument;
            }
        };

        let table_slice = unsafe { slice::from_raw_parts(table, table_len) };
        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };

        match inner.table_get(table_slice, key_slice) {
            Ok(Some(val)) => {
                let mut boxed = val.into_boxed_slice();
                let len = boxed.len();
                let ptr = boxed.as_mut_ptr();
                std::mem::forget(boxed);
                unsafe {
                    *out_val = ptr;
                    *out_val_len = len;
                }
                CitadelError::Ok
            }
            Ok(None) => {
                unsafe {
                    *out_val = ptr::null_mut();
                    *out_val_len = 0;
                }
                CitadelError::Ok
            }
            Err(e) => map_core_error(e),
        }
    })
}

/// Open a SQL connection on a database.
///
/// The connection borrows the database - the database must outlive the
/// connection.
#[no_mangle]
pub extern "C" fn citadel_sql_open(
    db: *mut CitadelDb,
    out: *mut *mut CitadelSqlConn,
) -> CitadelError {
    ffi_guard!({
        if db.is_null() || out.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let db_ref = unsafe { &*db };
        let conn = match Connection::open(&db_ref.db) {
            Ok(c) => c,
            Err(e) => return map_sql_error(e),
        };
        let conn: Connection<'static> = unsafe { std::mem::transmute(conn) };

        let handle = Box::new(CitadelSqlConn { conn });
        unsafe { *out = Box::into_raw(handle) };
        CitadelError::Ok
    })
}

/// Close a SQL connection and free its resources.
///
/// Accepts NULL (no-op).
#[no_mangle]
pub extern "C" fn citadel_sql_close(conn: *mut CitadelSqlConn) {
    if !conn.is_null() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            unsafe { drop(Box::from_raw(conn)) };
        }));
    }
}

/// Execute a SQL statement.
///
/// For DDL/DML statements, `*out` receives a result handle that can be
/// queried with `citadel_sql_rows_affected`. For SELECT queries, the
/// result handle provides column/row access. The result must be freed
/// with `citadel_sql_result_free`.
///
/// `out` can be NULL if the caller doesn't need the result.
#[no_mangle]
pub extern "C" fn citadel_sql_execute(
    conn: *mut CitadelSqlConn,
    sql: *const c_char,
    out: *mut *mut CitadelSqlResult,
) -> CitadelError {
    ffi_guard!({
        if conn.is_null() || sql.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let conn_ref = unsafe { &mut *conn };
        let sql_str = match unsafe { CStr::from_ptr(sql) }.to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("SQL is not valid UTF-8");
                return CitadelError::InvalidArgument;
            }
        };

        match conn_ref.conn.execute(sql_str) {
            Ok(result) => {
                if !out.is_null() {
                    let handle = Box::new(match result {
                        ExecutionResult::RowsAffected(n) => CitadelSqlResult {
                            columns: vec![],
                            rows: vec![],
                            rows_affected: n,
                            is_query: false,
                        },
                        ExecutionResult::Query(qr) => CitadelSqlResult {
                            columns: qr.columns,
                            rows: qr.rows,
                            rows_affected: 0,
                            is_query: true,
                        },
                        ExecutionResult::Ok => CitadelSqlResult {
                            columns: vec![],
                            rows: vec![],
                            rows_affected: 0,
                            is_query: false,
                        },
                    });
                    unsafe { *out = Box::into_raw(handle) };
                }
                CitadelError::Ok
            }
            Err(e) => map_sql_error(e),
        }
    })
}

/// Free a SQL result.
///
/// Accepts NULL (no-op).
#[no_mangle]
pub extern "C" fn citadel_sql_result_free(result: *mut CitadelSqlResult) {
    if !result.is_null() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            unsafe { drop(Box::from_raw(result)) };
        }));
    }
}

/// Get the number of rows affected by a DML statement.
#[no_mangle]
pub extern "C" fn citadel_sql_rows_affected(result: *const CitadelSqlResult) -> u64 {
    if result.is_null() {
        return 0;
    }
    let r = unsafe { &*result };
    r.rows_affected
}

/// Check if a result is a query result (SELECT).
#[no_mangle]
pub extern "C" fn citadel_sql_is_query(result: *const CitadelSqlResult) -> i32 {
    if result.is_null() {
        return 0;
    }
    let r = unsafe { &*result };
    if r.is_query {
        1
    } else {
        0
    }
}

/// Get the number of columns in a query result.
#[no_mangle]
pub extern "C" fn citadel_sql_column_count(result: *const CitadelSqlResult) -> u32 {
    if result.is_null() {
        return 0;
    }
    let r = unsafe { &*result };
    r.columns.len() as u32
}

/// Get a column name by index.
///
/// Returns a pointer to a null-terminated UTF-8 string. The pointer is
/// valid for the lifetime of the result. Returns NULL on invalid index.
#[no_mangle]
pub extern "C" fn citadel_sql_column_name(
    result: *const CitadelSqlResult,
    col: u32,
) -> *const c_char {
    if result.is_null() {
        return ptr::null();
    }
    let r = unsafe { &*result };
    match r.columns.get(col as usize) {
        Some(name) => COLUMN_NAME_BUF.with(|buf| {
            let cstr = CString::new(name.as_str()).unwrap_or_default();
            let ptr = cstr.as_ptr();
            *buf.borrow_mut() = Some(cstr);
            ptr
        }),
        None => ptr::null(),
    }
}

thread_local! {
    static COLUMN_NAME_BUF: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Get the number of rows in a query result.
#[no_mangle]
pub extern "C" fn citadel_sql_row_count(result: *const CitadelSqlResult) -> u64 {
    if result.is_null() {
        return 0;
    }
    let r = unsafe { &*result };
    r.rows.len() as u64
}

/// Value type tag for SQL result cells.
#[repr(i32)]
#[derive(Debug, Clone, Copy)]
pub enum CitadelValueType {
    Null = 0,
    Integer = 1,
    Real = 2,
    Text = 3,
    Blob = 4,
    Boolean = 5,
    Date = 6,
    Time = 7,
    Timestamp = 8,
    Interval = 9,
    Json = 10,
    Jsonb = 11,
    TsVector = 12,
    TsQuery = 13,
    Array = 14,
    Vector = 15,
}

/// Get the type of a value in a query result cell.
///
/// Returns `CITADEL_VALUE_NULL` for out-of-bounds access.
#[no_mangle]
pub extern "C" fn citadel_sql_value_type(
    result: *const CitadelSqlResult,
    row: u64,
    col: u32,
) -> CitadelValueType {
    if result.is_null() {
        return CitadelValueType::Null;
    }
    let r = unsafe { &*result };
    match r.rows.get(row as usize).and_then(|r| r.get(col as usize)) {
        Some(Value::Null) | None => CitadelValueType::Null,
        Some(Value::Integer(_)) => CitadelValueType::Integer,
        Some(Value::Real(_)) => CitadelValueType::Real,
        Some(Value::Text(_)) => CitadelValueType::Text,
        Some(Value::Blob(_)) => CitadelValueType::Blob,
        Some(Value::Boolean(_)) => CitadelValueType::Boolean,
        Some(Value::Date(_)) => CitadelValueType::Date,
        Some(Value::Time(_)) => CitadelValueType::Time,
        Some(Value::Timestamp(_)) => CitadelValueType::Timestamp,
        Some(Value::Interval { .. }) => CitadelValueType::Interval,
        Some(Value::Json(_)) => CitadelValueType::Json,
        Some(Value::Jsonb(_)) => CitadelValueType::Jsonb,
        Some(Value::TsVector(_)) => CitadelValueType::TsVector,
        Some(Value::TsQuery(_)) => CitadelValueType::TsQuery,
        Some(Value::Array(_)) => CitadelValueType::Array,
        Some(Value::Vector(_)) => CitadelValueType::Vector,
    }
}

/// Get an integer value from a query result cell.
///
/// Returns 0 for NULL or type mismatch.
#[no_mangle]
pub extern "C" fn citadel_sql_value_int(
    result: *const CitadelSqlResult,
    row: u64,
    col: u32,
) -> i64 {
    if result.is_null() {
        return 0;
    }
    let r = unsafe { &*result };
    match r.rows.get(row as usize).and_then(|r| r.get(col as usize)) {
        Some(Value::Integer(v)) => *v,
        Some(Value::Boolean(true)) => 1,
        Some(Value::Boolean(false)) => 0,
        _ => 0,
    }
}

/// Get a real (double) value from a query result cell.
///
/// Returns 0.0 for NULL or type mismatch.
#[no_mangle]
pub extern "C" fn citadel_sql_value_real(
    result: *const CitadelSqlResult,
    row: u64,
    col: u32,
) -> f64 {
    if result.is_null() {
        return 0.0;
    }
    let r = unsafe { &*result };
    match r.rows.get(row as usize).and_then(|r| r.get(col as usize)) {
        Some(Value::Real(v)) => *v,
        Some(Value::Integer(v)) => *v as f64,
        _ => 0.0,
    }
}

/// Get a text value from a query result cell.
///
/// Returns a pointer to a null-terminated UTF-8 string. The pointer is
/// valid for the lifetime of the result. Returns NULL for NULL values
/// or type mismatch. `*out_len` is set to the string length (excluding
/// null terminator). `out_len` can be NULL.
#[no_mangle]
pub extern "C" fn citadel_sql_value_text(
    result: *const CitadelSqlResult,
    row: u64,
    col: u32,
    out_len: *mut usize,
) -> *const c_char {
    if result.is_null() {
        return ptr::null();
    }
    let r = unsafe { &*result };
    match r.rows.get(row as usize).and_then(|r| r.get(col as usize)) {
        Some(Value::Text(s)) => {
            if !out_len.is_null() {
                unsafe { *out_len = s.len() };
            }
            TEXT_VALUE_BUF.with(|buf| {
                let cstr = CString::new(s.as_str()).unwrap_or_default();
                let ptr = cstr.as_ptr();
                *buf.borrow_mut() = Some(cstr);
                ptr
            })
        }
        _ => {
            if !out_len.is_null() {
                unsafe { *out_len = 0 };
            }
            ptr::null()
        }
    }
}

thread_local! {
    static TEXT_VALUE_BUF: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Get a blob value from a query result cell.
///
/// Returns a pointer to the blob data. The pointer is valid for the
/// lifetime of the result. Returns NULL for NULL values or type
/// mismatch. `*out_len` is set to the blob length. `out_len` must
/// not be NULL.
#[no_mangle]
pub extern "C" fn citadel_sql_value_blob(
    result: *const CitadelSqlResult,
    row: u64,
    col: u32,
    out_len: *mut usize,
) -> *const u8 {
    if result.is_null() || out_len.is_null() {
        return ptr::null();
    }
    let r = unsafe { &*result };
    match r.rows.get(row as usize).and_then(|r| r.get(col as usize)) {
        Some(Value::Blob(b)) => {
            unsafe { *out_len = b.len() };
            b.as_ptr()
        }
        _ => {
            unsafe { *out_len = 0 };
            ptr::null()
        }
    }
}

/// Free bytes allocated by Citadel (e.g., from citadel_read_get).
///
/// Accepts NULL (no-op). `len` must be the exact length returned by
/// the allocating function.
#[no_mangle]
pub extern "C" fn citadel_free_bytes(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            unsafe {
                let _ = Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len));
            };
        }));
    }
}

/// Get database statistics.
///
/// On success, the out-parameters are filled. Any out-parameter can be
/// NULL if the caller doesn't want that value.
#[no_mangle]
pub extern "C" fn citadel_stats(
    db: *const CitadelDb,
    out_entry_count: *mut u64,
    out_total_pages: *mut u32,
    out_tree_depth: *mut u16,
) -> CitadelError {
    ffi_guard!({
        if db.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let db_ref = unsafe { &*db };
        let stats = db_ref.db.stats();

        if !out_entry_count.is_null() {
            unsafe { *out_entry_count = stats.entry_count };
        }
        if !out_total_pages.is_null() {
            unsafe { *out_total_pages = stats.total_pages };
        }
        if !out_tree_depth.is_null() {
            unsafe { *out_tree_depth = stats.tree_depth };
        }

        CitadelError::Ok
    })
}

/// Change the database passphrase (fast key rotation).
///
/// Re-wraps the Root Encryption Key with a new Master Key derived from
/// the new passphrase. No page re-encryption needed.
#[no_mangle]
pub extern "C" fn citadel_change_passphrase(
    db: *const CitadelDb,
    old_passphrase: *const u8,
    old_len: usize,
    new_passphrase: *const u8,
    new_len: usize,
) -> CitadelError {
    ffi_guard!({
        if db.is_null() || old_passphrase.is_null() || new_passphrase.is_null() {
            set_last_error("null pointer argument");
            return CitadelError::InvalidArgument;
        }

        let db_ref = unsafe { &*db };
        let old = unsafe { slice::from_raw_parts(old_passphrase, old_len) };
        let new = unsafe { slice::from_raw_parts(new_passphrase, new_len) };

        match db_ref.db.change_passphrase(old, new) {
            Ok(()) => CitadelError::Ok,
            Err(e) => map_core_error(e),
        }
    })
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
