use std::any::Any;
use std::fs;
#[cfg(not(target_arch = "wasm32"))]
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use citadel_core::{
    CancelToken, Error, Result, KEY_FILE_SIZE, KEY_SIZE, MERKLE_HASH_SIZE, WRAPPED_KEY_SIZE,
};
use citadel_crypto::hkdf_utils::RegionWrapKeys;
use citadel_crypto::key_manager::{KeyFile, KeyFileAuthKey};
use citadel_io::durable;
#[cfg(not(target_arch = "wasm32"))]
use citadel_io::mmap_io::MmapPageIO;
use citadel_txn::integrity::IntegrityReport;
use citadel_txn::manager::{ScanMeasurement, TxnManager};
use citadel_txn::read_txn::ReadTxn;
use citadel_txn::write_txn::WriteTxn;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
#[cfg(feature = "audit-log")]
use zeroize::Zeroizing;

use crate::atom_store::AtomKeyStore;
#[cfg(feature = "audit-log")]
use crate::audit::{AuditEventType, AuditLog};
use crate::key_codec::SlotRecord;
use crate::region_store::RegionKeyStore;

/// Exclusive key-lifecycle capability over ONE database; always the outermost lock.
#[must_use = "the capability releases the lifecycle span when dropped"]
pub struct KeyLifecycleGuard<'a> {
    db: &'a Database,
    _span: parking_lot::MutexGuard<'a, ()>,
}

impl KeyLifecycleGuard<'_> {
    /// Cryptographically erase region key `slot` (no-op if already erased).
    pub fn region_store_tombstone(&self, slot: u32, region_id: u64) -> Result<()> {
        self.db.with_region_store(|s| {
            // Bump before and after: a cache built between them is never stamped current.
            self.db.bump_cache_epoch();
            let result = s.tombstone(slot, region_id);
            self.db.bump_cache_epoch();
            result
        })
    }

    /// Cryptographically erase atom key `slot` (no-op if already erased).
    pub fn atom_store_tombstone(&self, slot: u32, atom_id: u64) -> Result<()> {
        self.db.with_atom_store(|s| {
            // Armed before and after the attempt (see region_store_tombstone).
            self.db.bump_cache_epoch();
            let result = s.tombstone(slot, atom_id);
            self.db.bump_cache_epoch();
            result
        })
    }

    /// Batch erase, two fsyncs; recycled slots skip so retries converge; returns receipts.
    pub fn atom_store_tombstone_batch(
        &self,
        items: &[(u32, u64, u64)],
    ) -> Result<Vec<(u32, u64, u64, u64)>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        self.db.with_atom_store(|s| {
            // Armed before and after the attempt (see region_store_tombstone).
            self.db.bump_cache_epoch();
            let result = s.tombstone_batch(items);
            self.db.bump_cache_epoch();
            result
        })
    }

    /// Finish torn batch erases; no epoch bump - only already-armed keys are touched.
    pub fn normalize_atom_store_torn_erases(&self) -> Result<usize> {
        self.db.with_atom_store(|s| s.normalize_torn_tombstones())
    }
}

/// Type-erased cache of `Arc<T>` entries shared across connections to one DB.
pub type SharedCache = Mutex<FxHashMap<String, Arc<dyn Any + Send + Sync>>>;

/// Cloneable handle to the per-Database shared cache.
pub type SqlCacheHandle = Arc<SharedCache>;

/// Database statistics read from the current commit slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbStats {
    pub tree_depth: u16,
    pub entry_count: u64,
    pub total_pages: u32,
    pub high_water_mark: u32,
    pub merkle_root: [u8; MERKLE_HASH_SIZE],
}

/// Slot counts for one sidecar key store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct SlotCounts {
    /// Allocated slots, including empty and tombstoned capacity.
    pub total_slots: u32,
    /// Tombstones not yet reused: a lower bound, not lifetime erasures.
    pub tombstoned: u32,
}

/// Key-store counts for a vault inspection.
/// `None` means the sidecar has not been created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct KeyStoreFacts {
    pub region: Option<SlotCounts>,
    pub atom: Option<SlotCounts>,
}

/// Outcome of [`Database::upgrade_format`].
#[derive(Debug, Clone, Copy)]
pub struct UpgradeReport {
    /// Named tables whose catalog descriptors were rewritten (staleness
    /// cleared).
    pub tables_refreshed: usize,
    /// Whether HEADER_FLAG_SLOTS_V1 is set (both commit slots sealed V1).
    pub slots_flagged: bool,
    /// Whether the audit log header was converted from v1 to v2 by this call.
    pub audit_upgraded: bool,
}

/// Last authenticated key-file image plus the narrow key that can reseal its
/// metadata. Serializing rewrites prevents passphrase change and format
/// upgrade from racing or authenticating bytes replaced on disk after open.
pub(crate) struct KeyFileState {
    trusted: KeyFile,
    auth_key: KeyFileAuthKey,
}

impl KeyFileState {
    pub(crate) fn new(trusted: KeyFile, auth_key: KeyFileAuthKey) -> Self {
        Self { trusted, auth_key }
    }

    pub(crate) fn slots_v1_required(&self) -> bool {
        self.trusted.slots_v1_required()
    }

    #[cfg(feature = "audit-log")]
    pub(crate) fn audit_v2_required(&self) -> bool {
        self.trusted.audit_v2_required()
    }

    pub(crate) fn require_v1_slots(&mut self) {
        self.trusted.require_v1_slots(&self.auth_key);
    }

    #[cfg(feature = "audit-log")]
    pub(crate) fn require_audit_v2(&mut self) {
        self.trusted.require_audit_v2(&self.auth_key);
    }

    pub(crate) fn serialize(&self) -> [u8; citadel_core::KEY_FILE_SIZE] {
        self.trusted.serialize()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn replace_trusted(
        &mut self,
        path: &Path,
        replacement: KeyFile,
        operation: &'static str,
    ) -> Result<()> {
        self.replace_trusted_outcome(path, replacement)?
            .into_result(operation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn replace_trusted_outcome(
        &mut self,
        path: &Path,
        replacement: KeyFile,
    ) -> Result<PublishedWriteOutcome> {
        self.replace_trusted_with(path, replacement, durable::atomic_write_with_status)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn replace_trusted_with(
        &mut self,
        path: &Path,
        replacement: KeyFile,
        write: impl FnOnce(
            &Path,
            &[u8],
        ) -> std::result::Result<(), citadel_io::durable::AtomicWriteError>,
    ) -> Result<PublishedWriteOutcome> {
        match write(path, &replacement.serialize()) {
            Ok(()) => {
                self.trusted = replacement;
                Ok(PublishedWriteOutcome::Durable)
            }
            Err(error) if error.was_published() => {
                self.trusted = replacement;
                Ok(PublishedWriteOutcome::DurabilityUnconfirmed(
                    error.into_inner(),
                ))
            }
            Err(error) => Err(Error::Io(error.into_inner())),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
enum PublishedWriteOutcome {
    Durable,
    DurabilityUnconfirmed(std::io::Error),
}

#[cfg(not(target_arch = "wasm32"))]
impl PublishedWriteOutcome {
    fn into_result(self, operation: &'static str) -> Result<()> {
        match self {
            Self::Durable => Ok(()),
            Self::DurabilityUnconfirmed(source) => {
                Err(Error::DurabilityFailureAfterOperation { operation, source })
            }
        }
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "audit-log"))]
fn finish_audited_operation(
    operation: &'static str,
    publication: PublishedWriteOutcome,
    audit: Result<()>,
) -> Result<()> {
    match (publication, audit) {
        (PublishedWriteOutcome::Durable, Ok(())) => Ok(()),
        (PublishedWriteOutcome::Durable, Err(source)) => Err(Error::AuditFailureAfterOperation {
            operation,
            source: Box::new(source),
        }),
        (PublishedWriteOutcome::DurabilityUnconfirmed(source), Ok(())) => {
            Err(Error::DurabilityFailureAfterOperation { operation, source })
        }
        (PublishedWriteOutcome::DurabilityUnconfirmed(durability), Err(audit)) => {
            Err(Error::DurabilityAndAuditFailureAfterOperation {
                operation,
                durability,
                audit: Box::new(audit),
            })
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn atomic_write_outcome(path: &Path, bytes: &[u8]) -> Result<PublishedWriteOutcome> {
    match durable::atomic_write_with_status(path, bytes) {
        Ok(()) => Ok(PublishedWriteOutcome::Durable),
        Err(error) if error.was_published() => Ok(PublishedWriteOutcome::DurabilityUnconfirmed(
            error.into_inner(),
        )),
        Err(error) => Err(Error::Io(error.into_inner())),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn atomic_write_for_operation(
    path: &Path,
    bytes: &[u8],
    operation: &'static str,
) -> Result<()> {
    atomic_write_outcome(path, bytes)?.into_result(operation)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct CreatedFileGuard {
    paths: Vec<PathBuf>,
    armed: bool,
}

#[cfg(not(target_arch = "wasm32"))]
impl CreatedFileGuard {
    pub(crate) fn new() -> Self {
        Self {
            paths: Vec::new(),
            armed: true,
        }
    }

    pub(crate) fn track(&mut self, path: impl Into<PathBuf>) {
        self.paths.push(path.into());
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for CreatedFileGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        for path in self.paths.iter().rev() {
            if fs::remove_file(path).is_ok() {
                let _ = durable::fsync_directory(path);
            }
        }
    }
}

/// An open Citadel database (`Send + Sync`).
///
/// Exclusively locks the database file for its lifetime.
pub struct Database {
    manager: TxnManager,
    data_path: PathBuf,
    key_path: PathBuf,
    /// Database file_id (from the file header), binding the region key store.
    file_id: u64,
    key_file_state: Mutex<KeyFileState>,
    #[cfg(feature = "audit-log")]
    audit_log: Option<Mutex<AuditLog>>,
    /// Shared cache for higher-level crates (e.g. citadel-sql ANN indexes).
    /// Held here so it spans all connections without a dependency cycle.
    sql_caches: Arc<SharedCache>,
    /// Region wrap keys for per-region cryptographic erasure (citadel-mem).
    /// `Some` only when the builder enabled region keys; derived from the REK
    /// and zeroized on drop. The raw REK is never retained here.
    region_keys: Option<RegionWrapKeys>,
    /// Sidecar region key store (lazy); shared by every `MemoryEngine` over
    /// this db.
    region_store: Mutex<Option<RegionKeyStore>>,
    /// Sidecar per-atom key store (lazy); holds each atom's wrapped ACK.
    atom_store: Mutex<Option<AtomKeyStore>>,
    /// Serializes multi-step key-lifecycle spans (allocate->commit, reconcile,
    /// erase, persist) across handles, so reconcile can't tombstone a key an
    /// in-flight write just allocated. Store calls are already internally
    /// locked; this guards the spans between them.
    key_lifecycle: Mutex<()>,
    /// Bumped before key destruction and rewrites; caches refuse older-epoch plaintext.
    cache_epoch: AtomicU64,
    /// Token cloned into transactions and consulted by non-transactional fast paths.
    cancel: Mutex<Option<CancelToken>>,
    /// Test-only hook fired as a destruction wrapper reaches the acquisition boundary.
    #[cfg(any(test, feature = "test-util"))]
    destruction_acquire_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("data_path", &self.data_path)
            .field("key_path", &self.key_path)
            .finish()
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Database>();
};

impl Database {
    #[cfg(feature = "audit-log")]
    pub(crate) fn new(
        manager: TxnManager,
        data_path: PathBuf,
        key_path: PathBuf,
        file_id: u64,
        key_file_state: KeyFileState,
        region_keys: Option<RegionWrapKeys>,
        audit_log: Option<AuditLog>,
    ) -> Self {
        Self {
            manager,
            data_path,
            key_path,
            file_id,
            key_file_state: Mutex::new(key_file_state),
            audit_log: audit_log.map(Mutex::new),
            sql_caches: Arc::new(Mutex::new(FxHashMap::default())),
            region_keys,
            region_store: Mutex::new(None),
            atom_store: Mutex::new(None),
            key_lifecycle: Mutex::new(()),
            cache_epoch: AtomicU64::new(0),
            cancel: Mutex::new(None),
            #[cfg(any(test, feature = "test-util"))]
            destruction_acquire_hook: Mutex::new(None),
        }
    }

    #[cfg(not(feature = "audit-log"))]
    pub(crate) fn new(
        manager: TxnManager,
        data_path: PathBuf,
        key_path: PathBuf,
        file_id: u64,
        key_file_state: KeyFileState,
        region_keys: Option<RegionWrapKeys>,
    ) -> Self {
        Self {
            manager,
            data_path,
            key_path,
            file_id,
            key_file_state: Mutex::new(key_file_state),
            sql_caches: Arc::new(Mutex::new(FxHashMap::default())),
            region_keys,
            region_store: Mutex::new(None),
            atom_store: Mutex::new(None),
            key_lifecycle: Mutex::new(()),
            cache_epoch: AtomicU64::new(0),
            cancel: Mutex::new(None),
            #[cfg(any(test, feature = "test-util"))]
            destruction_acquire_hook: Mutex::new(None),
        }
    }

    /// Capability for a key-lifecycle span (see the field doc); never held in callbacks.
    pub fn key_lifecycle_lock(&self) -> KeyLifecycleGuard<'_> {
        KeyLifecycleGuard {
            db: self,
            _span: self.key_lifecycle.lock(),
        }
    }

    /// Fires the test-only hook at the acquisition boundary; no-op unless armed.
    fn fire_destruction_acquire_hook(&self) {
        #[cfg(any(test, feature = "test-util"))]
        if let Some(hook) = self.destruction_acquire_hook.lock().as_ref() {
            hook();
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    #[doc(hidden)]
    pub fn debug_set_destruction_acquire_hook(&self, hook: Option<Box<dyn Fn() + Send + Sync>>) {
        *self.destruction_acquire_hook.lock() = hook;
    }

    /// Current invalidation epoch (see the field doc); caches refuse reads once it moves.
    pub fn cache_epoch(&self) -> u64 {
        self.cache_epoch.load(Ordering::Acquire)
    }

    /// Advance the epoch; key destruction bumps automatically, sealed writes explicitly.
    pub fn bump_cache_epoch(&self) -> u64 {
        self.cache_epoch.fetch_add(1, Ordering::Release) + 1
    }

    /// Fetch a typed entry from the shared SQL cache.
    /// Returns `None` if the key is missing or stored under a different type.
    pub fn sql_cache_get<T: Any + Send + Sync>(&self, key: &str) -> Option<Arc<T>> {
        let guard = self.sql_caches.lock();
        let entry = guard.get(key)?;
        Arc::clone(entry).downcast::<T>().ok()
    }

    /// Insert (or overwrite) a typed entry in the shared SQL cache.
    pub fn sql_cache_insert<T: Any + Send + Sync>(&self, key: String, value: Arc<T>) {
        self.sql_caches.lock().insert(key, value);
    }

    /// Remove every entry whose key starts with `prefix`.
    /// Returns the number of entries removed.
    pub fn sql_cache_invalidate_prefix(&self, prefix: &str) -> usize {
        let mut guard = self.sql_caches.lock();
        let before = guard.len();
        guard.retain(|k, _| !k.starts_with(prefix));
        before - guard.len()
    }

    /// Total number of cache entries (test/diagnostics helper).
    pub fn sql_cache_len(&self) -> usize {
        self.sql_caches.lock().len()
    }

    /// Cloneable handle to the shared cache.
    pub fn sql_cache_handle(&self) -> SqlCacheHandle {
        Arc::clone(&self.sql_caches)
    }

    /// Begin a read-only transaction with snapshot isolation.
    ///
    /// The transaction clones the currently installed cancellation token.
    /// Replacing the handle token later does not retarget an open transaction;
    /// install the desired token before this call or use [`ReadTxn::set_cancel`].
    pub fn begin_read(&self) -> ReadTxn<'_> {
        let mut txn = self.manager.begin_read();
        txn.set_cancel(self.cancel.lock().clone());
        txn
    }

    /// Begin a read-write transaction. Only one can be active at a time.
    ///
    /// The transaction clones the currently installed cancellation token.
    /// Replacing the handle token later does not retarget an open transaction;
    /// install the desired token before this call or use [`WriteTxn::set_cancel`].
    pub fn begin_write(&self) -> Result<WriteTxn<'_>> {
        let mut txn = self.manager.begin_write()?;
        txn.set_cancel(self.cancel.lock().clone());
        Ok(txn)
    }

    /// Install the token cloned by future transactions and checked by work
    /// that can complete without opening a transaction. `None` clears it.
    /// Existing raw transactions retain the token they already cloned.
    pub fn set_cancel(&self, token: Option<CancelToken>) {
        *self.cancel.lock() = token;
    }

    /// Clone the currently installed cancellation token.
    pub fn cancel_token(&self) -> Option<CancelToken> {
        self.cancel.lock().clone()
    }

    /// Database-wide storage entries examined since this database opened.
    /// This is monotonic telemetry across all connections and threads.
    pub fn rows_scanned(&self) -> u64 {
        self.manager.rows_scanned()
    }

    /// Begin an operation-local scan measurement on the current thread.
    /// The returned RAII guard reports the work done until it is dropped.
    pub fn measure_scans(&self) -> ScanMeasurement {
        self.manager.measure_scans()
    }

    /// Get database statistics from the current commit slot.
    pub fn stats(&self) -> DbStats {
        let slot = self.manager.current_slot();
        DbStats {
            tree_depth: slot.tree_depth,
            entry_count: slot.tree_entries,
            total_pages: slot.total_pages,
            high_water_mark: slot.high_water_mark,
            merkle_root: slot.merkle_root,
        }
    }

    pub fn data_path(&self) -> &Path {
        &self.data_path
    }

    /// Authenticated, non-secret facts from the current key-file image.
    pub fn key_file(&self) -> crate::inspect::KeyFileInfo {
        crate::inspect::KeyFileInfo::from_key_file(&self.key_file_state.lock().trusted)
    }

    pub fn key_path(&self) -> &Path {
        &self.key_path
    }

    /// Re-read the sidecar and require the exact image authenticated at open (or
    /// written by the last serialized rewrite). The data-file lock does not cover
    /// this path, so an update must not bless an offline replacement with a MAC.
    fn read_unchanged_key_file(&self, state: &KeyFileState) -> Result<KeyFile> {
        Ok(self.read_unchanged_key_file_with_permissions(state)?.0)
    }

    /// Read bytes and permissions through one no-follow handle so a backup
    /// cannot authenticate one key image and copy metadata from another.
    fn read_unchanged_key_file_with_permissions(
        &self,
        state: &KeyFileState,
    ) -> Result<(KeyFile, fs::Permissions)> {
        let (image, metadata) = durable::read_regular_file_exact::<KEY_FILE_SIZE>(&self.key_path)?;
        let current = KeyFile::deserialize(&image)?;
        if current.serialize() != state.trusted.serialize() {
            return Err(Error::KeyFileIntegrity);
        }
        Ok((current, metadata.permissions()))
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "audit-log"))]
    fn refuse_copy_audit_sidecars(dest_path: &Path) -> Result<()> {
        let audit_path = crate::audit::resolve_audit_path(dest_path);
        crate::builder::DatabaseBuilder::refuse_stale_sidecar(&audit_path, "audit log")?;
        crate::builder::DatabaseBuilder::refuse_stale_sidecar(
            &crate::audit::rotation_work_path(&audit_path),
            "audit rotation journal",
        )?;
        crate::builder::DatabaseBuilder::refuse_stale_sidecar(
            &crate::audit::audit_upgrade_path(&audit_path),
            "audit upgrade image",
        )?;
        crate::audit::ensure_no_retained_audit_history(&audit_path)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn refuse_copy_sidecars(dest_path: &Path) -> Result<PathBuf> {
        let dest_key_path = resolve_key_path_for(dest_path);
        crate::builder::DatabaseBuilder::refuse_stale_sidecar(&dest_key_path, "key file")?;
        crate::builder::DatabaseBuilder::refuse_stale_sidecar(
            &region_store_path_for(&dest_key_path),
            "region key store",
        )?;
        crate::builder::DatabaseBuilder::refuse_stale_sidecar(
            &atom_store_path_for(&dest_key_path),
            "atom key store",
        )?;
        #[cfg(feature = "audit-log")]
        Self::refuse_copy_audit_sidecars(dest_path)?;
        Ok(dest_key_path)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn write_trusted_key_copy(
        dest: &Path,
        key_file: &KeyFile,
        permissions: fs::Permissions,
    ) -> Result<()> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(dest)?;
        let write = (|| -> std::io::Result<()> {
            std::io::Write::write_all(&mut file, &key_file.serialize())?;
            file.set_permissions(permissions)?;
            file.sync_all()
        })();
        if let Err(error) = write {
            drop(file);
            let _ = fs::remove_file(dest);
            return Err(error.into());
        }
        drop(file);
        Ok(())
    }

    /// Database file identifier from the file header. citadel-mem binds the
    /// region key store to this value so a mismatched sidecar is rejected.
    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    /// Whether per-region cryptographic erasure keys are available. `true` only
    /// when the database was opened with `enable_region_keys(true)`.
    pub fn region_keys_enabled(&self) -> bool {
        self.region_keys.is_some()
    }

    /// Wrap a region's random content key (RCK) under the region KEK
    /// (AES-256-KW). The 40-byte result is the sole copy of the RCK;
    /// citadel-mem stores it in the sidecar key store and overwrites it in
    /// place to erase the region.
    pub fn wrap_region_key(&self, rck: &[u8; KEY_SIZE]) -> Result<[u8; WRAPPED_KEY_SIZE]> {
        self.region_keys
            .as_ref()
            .map(|rk| rk.wrap_region_key(rck))
            .ok_or(Error::RegionKeysDisabled)
    }

    /// Unwrap a region content key. Fails if the slot was erased (zeroed wrap).
    pub fn unwrap_region_key(&self, wrapped: &[u8; WRAPPED_KEY_SIZE]) -> Result<[u8; KEY_SIZE]> {
        self.region_keys
            .as_ref()
            .ok_or(Error::RegionKeysDisabled)?
            .unwrap_region_key(wrapped)
    }

    /// HMAC key authenticating the region key store's header and slots
    /// (torn-write detection only; RCK secrecy is protected by AES-KW).
    pub fn region_store_mac_key(&self) -> Result<[u8; KEY_SIZE]> {
        self.region_keys
            .as_ref()
            .map(|rk| rk.store_mac_key)
            .ok_or(Error::RegionKeysDisabled)
    }

    /// Path to the sidecar region key store, `{key_path}` with the
    /// `citadel-regions` extension. Pure path math; valid even when region keys
    /// are disabled (the file only exists once an encrypted region is created).
    pub fn region_store_path(&self) -> PathBuf {
        region_store_path_for(&self.key_path)
    }

    /// Run `f` against the lazily-opened sidecar store under its lock.
    fn with_region_store<T>(&self, f: impl FnOnce(&mut RegionKeyStore) -> Result<T>) -> Result<T> {
        let mut guard = self.region_store.lock();
        if guard.is_none() {
            let mac_key = self.region_store_mac_key()?;
            *guard = Some(RegionKeyStore::create_or_open(
                &self.region_store_path(),
                self.file_id,
                mac_key,
            )?);
        }
        f(guard.as_mut().expect("region store initialized above"))
    }

    /// Allocate a slot and store the wrapped RCK (fsync'd); returns `(slot,
    /// gen)`.
    pub fn region_store_allocate_write(
        &self,
        region_id: u64,
        wrapped: &[u8; WRAPPED_KEY_SIZE],
    ) -> Result<(u32, u64)> {
        self.with_region_store(|s| s.allocate_write(region_id, wrapped))
    }

    /// The authoritative record of region key `slot`.
    pub fn region_store_slot(&self, slot: u32) -> Result<SlotRecord> {
        self.with_region_store(|s| s.read_slot(slot))
    }

    /// Cryptographically erase region key `slot` (no-op if erased); acquires the span.
    pub fn region_store_tombstone(&self, slot: u32, region_id: u64) -> Result<()> {
        self.fire_destruction_acquire_hook();
        self.key_lifecycle_lock()
            .region_store_tombstone(slot, region_id)
    }

    /// `(slot, region_id)` for every LIVE region key slot.
    pub fn region_store_live_owners(&self) -> Result<Vec<(u32, u64)>> {
        Ok(self
            .region_store_live_bindings()?
            .into_iter()
            .map(|(slot, owner, _)| (slot, owner))
            .collect())
    }

    /// `(slot, region_id, gen)` per LIVE slot - the binding a reconciler matches.
    pub fn region_store_live_bindings(&self) -> Result<Vec<(u32, u64, u64)>> {
        self.with_region_store(|s| s.live_bindings())
    }

    /// Path to the sidecar per-atom key store, `{key_path}` with the
    /// `citadel-atomkeys` extension. Pure path math; the file only exists once
    /// an encrypted atom is written.
    pub fn atom_store_path(&self) -> PathBuf {
        atom_store_path_for(&self.key_path)
    }

    /// Run `f` against the lazily-opened atom key store under its lock.
    fn with_atom_store<T>(&self, f: impl FnOnce(&mut AtomKeyStore) -> Result<T>) -> Result<T> {
        let mut guard = self.atom_store.lock();
        if guard.is_none() {
            let mac_key = self.region_store_mac_key()?;
            *guard = Some(AtomKeyStore::create_or_open(
                &self.atom_store_path(),
                self.file_id,
                mac_key,
            )?);
        }
        f(guard.as_mut().expect("atom store initialized above"))
    }

    /// Allocate a slot and store one atom's wrapped ACK (fsync'd); returns
    /// `(slot, gen)`.
    pub fn atom_store_allocate_write(
        &self,
        atom_id: u64,
        wrapped: &[u8; WRAPPED_KEY_SIZE],
    ) -> Result<(u32, u64)> {
        self.with_atom_store(|s| s.allocate_write(atom_id, wrapped))
    }

    /// Allocate and durably write a batch of `(atom_id, wrapped)` ACKs with one
    /// fsync; returns `(slot, gen)` per item in order.
    pub fn atom_store_allocate_batch(
        &self,
        items: &[(u64, [u8; WRAPPED_KEY_SIZE])],
    ) -> Result<Vec<(u32, u64)>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        self.with_atom_store(|s| s.allocate_write_batch(items))
    }

    /// The authoritative record of atom key `slot` (its wrapped ACK and state).
    pub fn atom_store_slot(&self, slot: u32) -> Result<SlotRecord> {
        self.with_atom_store(|s| s.read_slot(slot))
    }

    /// Cryptographically erase atom key `slot` (no-op if erased); acquires the span.
    pub fn atom_store_tombstone(&self, slot: u32, atom_id: u64) -> Result<()> {
        self.fire_destruction_acquire_hook();
        self.key_lifecycle_lock()
            .atom_store_tombstone(slot, atom_id)
    }

    /// Batch erase, two fsyncs; recycled skips, returns receipts; acquires the span.
    pub fn atom_store_tombstone_batch(
        &self,
        items: &[(u32, u64, u64)],
    ) -> Result<Vec<(u32, u64, u64, u64)>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        self.fire_destruction_acquire_hook();
        self.key_lifecycle_lock().atom_store_tombstone_batch(items)
    }

    /// Every LIVE atom key's `atom_id -> wrapped ACK`, in one whole-file pass.
    pub fn atom_store_live_wrapped(&self) -> Result<FxHashMap<u64, [u8; WRAPPED_KEY_SIZE]>> {
        self.with_atom_store(|s| s.live_wrapped())
    }

    /// `(slot, atom_id)` for every LIVE atom key slot.
    pub fn atom_store_live_owners(&self) -> Result<Vec<(u32, u64)>> {
        Ok(self
            .atom_store_live_bindings()?
            .into_iter()
            .map(|(slot, owner, _)| (slot, owner))
            .collect())
    }

    /// `(slot, atom_id, gen)` per LIVE slot - the binding a reconciler matches.
    pub fn atom_store_live_bindings(&self) -> Result<Vec<(u32, u64, u64)>> {
        self.with_atom_store(|s| s.live_bindings())
    }

    /// One-shot `tombstone_batch` failure before the sibling scrub - the torn window.
    #[cfg(any(test, feature = "test-util"))]
    #[doc(hidden)]
    pub fn debug_fail_next_atom_tombstone_batch_before_sibling(&self) {
        crate::atom_store::fail_next_batch_before_sibling();
    }

    /// Both raw copies of atom key slot `slot` (A then B); `None` per MAC-invalid copy.
    #[cfg(any(test, feature = "test-util"))]
    #[doc(hidden)]
    pub fn debug_atom_slot_copies(&self, slot: u32) -> Result<[Option<SlotRecord>; 2]> {
        self.with_atom_store(|s| s.slot_copies(slot))
    }

    /// Number of currently active readers.
    pub fn reader_count(&self) -> usize {
        self.manager.reader_count()
    }

    /// Change the database passphrase (re-wraps REK, no page re-encryption).
    pub fn change_passphrase(&self, old_passphrase: &[u8], new_passphrase: &[u8]) -> Result<()> {
        use citadel_crypto::kdf::{derive_mk, generate_salt};
        use citadel_crypto::key_manager::{unwrap_rek, wrap_rek};

        let mut state = self.key_file_state.lock();
        let kf = self.read_unchanged_key_file(&state)?;

        let old_mk = derive_mk(
            kf.kdf_algorithm,
            old_passphrase,
            &kf.argon2_salt,
            kf.argon2_m_cost,
            kf.argon2_t_cost,
            kf.argon2_p_cost,
        )?;
        kf.verify_mac(&old_mk)?;

        let rek = unwrap_rek(&old_mk, &kf.wrapped_rek).map_err(|_| Error::BadPassphrase)?;

        let new_salt = generate_salt();
        let new_mk = derive_mk(
            kf.kdf_algorithm,
            new_passphrase,
            &new_salt,
            kf.argon2_m_cost,
            kf.argon2_t_cost,
            kf.argon2_p_cost,
        )?;

        let new_wrapped = wrap_rek(&new_mk, &rek);

        let mut new_kf = kf.clone();
        new_kf.argon2_salt = new_salt;
        new_kf.wrapped_rek = new_wrapped;
        if new_kf.slots_v1_required() {
            new_kf.update_mac_with_auth_key(&state.auth_key);
        } else {
            new_kf.update_mac(&new_mk)?;
        }

        let publication = state.replace_trusted_outcome(&self.key_path, new_kf)?;
        drop(state);

        #[cfg(feature = "audit-log")]
        {
            let audit = self.log_audit(AuditEventType::PassphraseChanged, &[]);
            finish_audited_operation("passphrase change", publication, audit)
        }

        #[cfg(not(feature = "audit-log"))]
        publication.into_result("passphrase change")
    }

    /// Whether `passphrase` unwraps this database, read from the key file so a
    /// rekey cannot leave the answer stale.
    ///
    /// Checks the key file's `file_id` as the open path does, so a key file belonging
    /// to a different database cannot pass on its MAC alone.
    pub fn verify_passphrase(&self, passphrase: &[u8]) -> Result<bool> {
        use citadel_crypto::kdf::derive_mk;

        if self.key_path.as_os_str().is_empty() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "an in-memory database has no key file to verify against",
            )));
        }
        let kf = {
            let state = self.key_file_state.lock();
            self.read_unchanged_key_file(&state)?
        };
        if kf.file_id != self.file_id {
            return Ok(false); // a key file for some other database
        }
        let mk = derive_mk(
            kf.kdf_algorithm,
            passphrase,
            &kf.argon2_salt,
            kf.argon2_m_cost,
            kf.argon2_t_cost,
            kf.argon2_p_cost,
        )?;
        Ok(kf.verify_mac(&mk).is_ok())
    }

    pub fn integrity_check(&self) -> Result<IntegrityReport> {
        let report = self.integrity_check_quiet()?;

        #[cfg(feature = "audit-log")]
        {
            let error_count = report.errors.len() as u32;
            self.log_audit_after_operation(
                "integrity check",
                AuditEventType::IntegrityCheckPerformed,
                &error_count.to_le_bytes(),
            )?;
        }

        Ok(report)
    }

    /// Run [`Database::integrity_check`] without writing an audit entry.
    pub fn integrity_check_quiet(&self) -> Result<IntegrityReport> {
        let cancel = self.cancel_token();
        self.manager.integrity_check_with_cancel(cancel.as_ref())
    }

    /// Slot counts for both sidecar key stores.
    ///
    /// Missing stores report `None`; this method never creates or repairs them.
    /// If either store exists, the database must have been opened with region
    /// keys enabled or this returns [`Error::RegionKeysDisabled`].
    pub fn key_store_facts(&self) -> Result<KeyStoreFacts> {
        if self.key_path.as_os_str().is_empty() {
            return Ok(KeyStoreFacts {
                region: None,
                atom: None,
            });
        }

        let region_path = self.region_store_path();
        let atom_path = self.atom_store_path();
        let region_exists = durable::path_entry_exists(&region_path)?;
        let atom_exists = durable::path_entry_exists(&atom_path)?;
        if !region_exists && !atom_exists {
            return Ok(KeyStoreFacts {
                region: None,
                atom: None,
            });
        }

        let mac_key = self.region_store_mac_key()?;
        let region = if region_exists {
            let _store = self.region_store.lock();
            match RegionKeyStore::inspect_counts(&region_path, self.file_id, &mac_key) {
                Ok((total_slots, tombstoned)) => Some(SlotCounts {
                    total_slots,
                    tombstoned,
                }),
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        let atom = if atom_exists {
            let _store = self.atom_store.lock();
            match AtomKeyStore::inspect_counts(&atom_path, self.file_id, &mac_key) {
                Ok((total_slots, tombstoned)) => Some(SlotCounts {
                    total_slots,
                    tombstoned,
                }),
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error),
            }
        } else {
            None
        };

        Ok(KeyStoreFacts { region, atom })
    }

    /// Create a hot backup via MVCC snapshot. Also copies the key file.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn backup(&self, dest_path: &Path) -> Result<()> {
        let dest_key_path = Self::refuse_copy_sidecars(dest_path)?;
        let mut created = CreatedFileGuard::new();
        {
            let _lifecycle = self.key_lifecycle.lock();
            let (trusted_key_file, key_permissions) = {
                let key_state = self.key_file_state.lock();
                self.read_unchanged_key_file_with_permissions(&key_state)?
            };
            let dest_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(dest_path)?;
            created.track(dest_path);
            citadel_io::file_lock::try_lock_exclusive(&dest_file)?;
            let dest_io = MmapPageIO::try_new(dest_file)?;
            self.manager.backup_to(&dest_io)?;

            Self::write_trusted_key_copy(&dest_key_path, &trusted_key_file, key_permissions)?;
            created.track(&dest_key_path);
            self.copy_region_store_to(&dest_key_path, &mut created)?;

            // File fsyncs do not persist the new directory entries.
            durable::fsync_directory(dest_path)?;
            #[cfg(feature = "audit-log")]
            Self::refuse_copy_audit_sidecars(dest_path)?;
        }
        created.disarm();

        #[cfg(feature = "audit-log")]
        self.log_audit_with_path("database backup", AuditEventType::BackupCreated, dest_path)?;

        Ok(())
    }

    /// Export an encrypted key backup for disaster recovery.
    ///
    /// Requires the current database passphrase. The backup can later restore
    /// access via `restore_key_from_backup` if the database passphrase is lost.
    pub fn export_key_backup(
        &self,
        db_passphrase: &[u8],
        backup_passphrase: &[u8],
        dest_path: &Path,
    ) -> Result<()> {
        use citadel_crypto::kdf::derive_mk;
        use citadel_crypto::key_backup::create_key_backup;
        use citadel_crypto::key_manager::unwrap_rek;

        let kf = {
            let state = self.key_file_state.lock();
            self.read_unchanged_key_file(&state)?
        };

        let mk = derive_mk(
            kf.kdf_algorithm,
            db_passphrase,
            &kf.argon2_salt,
            kf.argon2_m_cost,
            kf.argon2_t_cost,
            kf.argon2_p_cost,
        )?;
        kf.verify_mac(&mk)?;

        let rek = unwrap_rek(&mk, &kf.wrapped_rek).map_err(|_| Error::BadPassphrase)?;

        let backup_data = create_key_backup(
            &rek,
            backup_passphrase,
            kf.file_id,
            kf.cipher_id,
            kf.kdf_algorithm,
            kf.argon2_m_cost,
            kf.argon2_t_cost,
            kf.argon2_p_cost,
            kf.current_epoch,
            kf.flags,
        )?;

        let publication = atomic_write_outcome(dest_path, &backup_data)?;

        #[cfg(feature = "audit-log")]
        {
            let audit = self.log_audit_path(AuditEventType::KeyBackupExported, dest_path);
            finish_audited_operation("key backup export", publication, audit)
        }

        #[cfg(not(feature = "audit-log"))]
        publication.into_result("key backup export")
    }

    /// Restore a key file from an encrypted backup (static; no `Database`).
    ///
    /// Unwraps the REK using `backup_passphrase`, validates it against the
    /// destination database, then creates a new key file protected by
    /// `new_db_passphrase`. `backup_path` must directly name a regular file;
    /// symlinks, reparse points, and special files are rejected.
    ///
    /// An older backup may predate the authenticated slot-policy bit. When the
    /// current data file proves both slots are authenticated V1 (or its one-way
    /// header bit is set), restore carries that earned policy forward rather than
    /// reopening a downgrade window. With audit logging, restore also upgrades the
    /// current sidecar before establishing its separate v2 policy; without that
    /// feature it can only preserve the witness carried by the backup.
    pub fn restore_key_from_backup(
        backup_path: &Path,
        backup_passphrase: &[u8],
        new_db_passphrase: &[u8],
        db_path: &Path,
    ) -> Result<()> {
        use citadel_core::{
            FILE_HEADER_SIZE, HEADER_FLAG_SLOTS_V1, KEY_BACKUP_SIZE, KEY_FILE_MAGIC,
            KEY_FILE_VERSION, MAC_SIZE, WRAPPED_KEY_SIZE,
        };
        use citadel_crypto::kdf::{derive_mk, generate_salt};
        use citadel_crypto::key_backup::restore_rek_from_backup;
        use citadel_crypto::key_manager::{wrap_rek, KeyFile, KEY_FILE_FLAG_SLOTS_V1_REQUIRED};
        use citadel_crypto::page_cipher::compute_dek_id;
        use citadel_io::file_manager::{FileHeader, SlotFormat};

        let (backup_buf, _) = durable::read_regular_file_exact::<KEY_BACKUP_SIZE>(backup_path)?;

        let restored = restore_rek_from_backup(&backup_buf, backup_passphrase)?;
        // Fail a restore against an open database before spending another KDF.
        // Only a preflight: the lock is reacquired below and held across
        // validation and durable replacement.
        {
            let db_file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(db_path)?;
            citadel_io::file_lock::try_lock_exclusive(&db_file)?;
        }
        // Run the expensive KDF before holding the database lock;
        // only validation and durable replacement need serialization.
        let new_salt = generate_salt();
        let new_mk = derive_mk(
            restored.kdf_algorithm,
            new_db_passphrase,
            &new_salt,
            restored.kdf_param1,
            restored.kdf_param2,
            restored.kdf_param3,
        )?;
        let new_wrapped = wrap_rek(&new_mk, &restored.rek);

        // Match normal open's lifetime lock: restoring a sidecar while another
        // handle commits or rekeys could publish a key image for an unvalidated
        // database generation.
        let mut db_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_path)?;
        citadel_io::file_lock::try_lock_exclusive(&db_file)?;
        let mut header_buf = [0u8; FILE_HEADER_SIZE];
        std::io::Read::read_exact(&mut db_file, &mut header_buf)?;
        let header = FileHeader::deserialize(&header_buf)?;
        if header.file_id != restored.file_id {
            return Err(Error::KeyFileMismatch);
        }

        let mut restored_flags = restored.key_file_flags;
        if header.flags & HEADER_FLAG_SLOTS_V1 != 0 {
            restored_flags |= KEY_FILE_FLAG_SLOTS_V1_REQUIRED;
        }
        let v1_required = restored_flags & KEY_FILE_FLAG_SLOTS_V1_REQUIRED != 0;
        if v1_required
            && header
                .slots
                .iter()
                .any(|slot| slot.slot_format == SlotFormat::Legacy && slot.verify_checksum())
        {
            return Err(Error::SlotDowngradeDetected);
        }

        let expected_dek_id = compute_dek_id(&restored.keys.mac_key, &restored.keys.dek);
        let backup_matches_data = header.slots.iter().any(|slot| {
            slot.verify_checksum()
                && slot.dek_id == expected_dek_id
                && if v1_required {
                    slot.slot_format == SlotFormat::V1 && slot.verify_mac(&restored.keys.mac_key)
                } else {
                    slot.slot_format == SlotFormat::Legacy
                        || slot.verify_mac(&restored.keys.mac_key)
                }
        });
        if !backup_matches_data {
            return Err(Error::KeyFileMismatch);
        }

        let both_slots_authenticated_v1 = header.slots.iter().all(|slot| {
            slot.slot_format == SlotFormat::V1
                && slot.verify_checksum()
                && slot.verify_mac(&restored.keys.mac_key)
        });
        if both_slots_authenticated_v1 {
            restored_flags |= KEY_FILE_FLAG_SLOTS_V1_REQUIRED;
        }

        #[cfg(feature = "audit-log")]
        if restored_flags & KEY_FILE_FLAG_SLOTS_V1_REQUIRED != 0 {
            let audit_path = crate::audit::resolve_audit_path(db_path);
            crate::audit::recover_rotation(&audit_path, &restored.keys.audit_key)?;
            crate::audit::recover_abandoned_audit_upgrade(&audit_path)?;
            if durable::path_entry_exists(&audit_path)? {
                let mut audit = AuditLog::open_existing(
                    &audit_path,
                    restored.file_id,
                    restored.keys.audit_key,
                    crate::audit::AuditConfig::default(),
                    restored_flags & citadel_crypto::key_manager::KEY_FILE_FLAG_AUDIT_V2_REQUIRED
                        != 0,
                )?;
                audit.upgrade_to_v2()?;
            } else {
                crate::audit::ensure_no_retained_audit_history(&audit_path)?;
            }
            restored_flags |= citadel_crypto::key_manager::KEY_FILE_FLAG_AUDIT_V2_REQUIRED;
        }

        let mut new_kf = KeyFile {
            magic: KEY_FILE_MAGIC,
            version: KEY_FILE_VERSION,
            file_id: restored.file_id,
            argon2_salt: new_salt,
            argon2_m_cost: restored.kdf_param1,
            argon2_t_cost: restored.kdf_param2,
            argon2_p_cost: restored.kdf_param3,
            cipher_id: restored.cipher_id,
            kdf_algorithm: restored.kdf_algorithm,
            flags: restored_flags,
            wrapped_rek: new_wrapped,
            current_epoch: restored.epoch,
            prev_wrapped_rek: [0u8; WRAPPED_KEY_SIZE],
            prev_epoch: 0,
            rotation_active: false,
            file_mac: [0u8; MAC_SIZE],
        };
        if new_kf.slots_v1_required() {
            let auth_key = KeyFileAuthKey::from_database_mac_key(&restored.keys.mac_key);
            new_kf.update_mac_with_auth_key(&auth_key);
        } else {
            new_kf.update_mac(&new_mk)?;
        }

        let key_path = resolve_key_path_for(db_path);
        if durable::path_entry_exists(&key_path)? {
            drop(durable::open_regular_read(&key_path)?);
        }
        atomic_write_for_operation(&key_path, &new_kf.serialize(), "key-file restore")?;

        Ok(())
    }

    /// Compact the database into a new file. Also copies the key file.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn compact(&self, dest_path: &Path) -> Result<()> {
        let dest_key_path = Self::refuse_copy_sidecars(dest_path)?;
        let mut created = CreatedFileGuard::new();
        {
            let _lifecycle = self.key_lifecycle.lock();
            let (trusted_key_file, key_permissions) = {
                let key_state = self.key_file_state.lock();
                self.read_unchanged_key_file_with_permissions(&key_state)?
            };
            let dest_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(dest_path)?;
            created.track(dest_path);
            citadel_io::file_lock::try_lock_exclusive(&dest_file)?;
            let dest_io = MmapPageIO::try_new(dest_file)?;
            self.manager.compact_to(&dest_io)?;

            Self::write_trusted_key_copy(&dest_key_path, &trusted_key_file, key_permissions)?;
            created.track(&dest_key_path);
            self.copy_region_store_to(&dest_key_path, &mut created)?;

            // File fsyncs do not persist the new directory entries.
            durable::fsync_directory(dest_path)?;
            #[cfg(feature = "audit-log")]
            Self::refuse_copy_audit_sidecars(dest_path)?;
        }
        created.disarm();

        #[cfg(feature = "audit-log")]
        self.log_audit_with_path(
            "database compaction",
            AuditEventType::CompactionPerformed,
            dest_path,
        )?;

        Ok(())
    }

    /// Copy the sidecar region key store next to `dest_key_path`, if it exists.
    ///
    /// A backup/compaction must carry the wrapped region keys so encrypted
    /// regions remain openable from the copy. A backup taken while a region is
    /// live retains a recoverable key that `forget` cannot reach, so backup
    /// retention is the operator's job (see `region_store_path`).
    #[cfg(not(target_arch = "wasm32"))]
    fn copy_region_store_to(
        &self,
        dest_key_path: &Path,
        created: &mut CreatedFileGuard,
    ) -> Result<()> {
        let _region_store = self.region_store.lock();
        let _atom_store = self.atom_store.lock();
        let src = self.region_store_path();
        if durable::path_entry_exists(&src)? {
            let dest = region_store_path_for(dest_key_path);
            durable::copy_new_and_sync(&src, &dest)?;
            created.track(dest);
        }
        let atom_src = self.atom_store_path();
        if durable::path_entry_exists(&atom_src)? {
            let dest = atom_store_path_for(dest_key_path);
            durable::copy_new_and_sync(&atom_src, &dest)?;
            created.track(dest);
        }
        Ok(())
    }
}

impl Database {
    #[doc(hidden)]
    pub fn manager(&self) -> &TxnManager {
        &self.manager
    }

    /// Every named tree in the physical catalog (the SQL catalog lists only DDL tables).
    pub fn table_names(&self) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .manager
            .list_tables()?
            .into_iter()
            .map(|(name, _)| name)
            .collect())
    }

    /// Convert a pre-v1 file to the protected format: reseal both slots V1
    /// (for any table count), persist an authenticated key-file requirement,
    /// stamp the compatibility HEADER_FLAG_SLOTS_V1, and upgrade the audit
    /// header to v2. One-way (pre-v1 binaries can no longer open it) and
    /// idempotent.
    ///
    /// The marker detects a data-header/slot downgrade while the current key file
    /// remains trusted. Restoring an older authentic unflagged key file removes
    /// that witness; a database also rolled back or rewritten to a valid legacy-slot
    /// shape is then indistinguishable without an external freshness anchor.
    pub fn upgrade_format(&self) -> Result<UpgradeReport> {
        let _lifecycle = self.key_lifecycle.lock();
        // Refuse an externally replaced key sidecar before touching either
        // commit slot. Keep internal key-file rewrites serialized through the
        // full upgrade, then recheck immediately before publishing policy.
        let mut key_file_state = if self.key_path.as_os_str().is_empty() {
            None
        } else {
            let state = self.key_file_state.lock();
            self.read_unchanged_key_file(&state)?;
            Some(state)
        };

        let names: Vec<Vec<u8>> = self
            .manager
            .list_tables()?
            .into_iter()
            .map(|(name, _)| name)
            .collect();

        // Each commit rewrites one physical slot. The first refresh clears
        // all staleness; the second pass only needs a forced empty commit to
        // reseal the other physical slot with the (now fresh) entries.
        let mut txn = self.manager.begin_write()?;
        txn.refresh_all_catalog_descriptors(&names)?;
        txn.commit()?;
        let mut txn = self.manager.begin_write()?;
        txn.refresh_all_catalog_descriptors(&[])?;
        txn.commit()?;
        // Audit must reach v2 before authenticated metadata makes that
        // requirement permanent. The inverse order could strand the vault beside
        // a v1 audit file it is required to reject.
        #[cfg(feature = "audit-log")]
        let (audit_upgraded, audit_v2_ready) = match self.audit_log {
            Some(ref mutex) => (mutex.lock().upgrade_to_v2()?, true),
            None => (false, false),
        };
        #[cfg(not(feature = "audit-log"))]
        let audit_upgraded = false;
        #[cfg(not(feature = "audit-log"))]
        let audit_v2_ready = false;

        // Exclude writers from the final both-slot check through both durable
        // markers, or a concurrent commit could replace one V1 slot between the
        // check and the key-file requirement landing.
        let upgrade_exclusion = self.manager.exclude_writers()?;
        // Arm the in-process backstop before any durable policy write, so a
        // later key/header failure leaves this handle conservatively V1-only
        // and a retry can finish the idempotent work.
        upgrade_exclusion.require_authenticated_v1()?;

        if let Some(state) = key_file_state.as_mut() {
            let current = self.read_unchanged_key_file(state)?;
            if !current.slots_v1_required() || (audit_v2_ready && !current.audit_v2_required()) {
                let mut protected = current;
                protected.require_v1_slots(&state.auth_key);
                if audit_v2_ready {
                    protected.require_audit_v2(&state.auth_key);
                }
                state.replace_trusted(
                    &self.key_path,
                    protected,
                    "authenticated format policy upgrade",
                )?;
            }
        }

        // The authenticated marker lands before the mutable compatibility header.
        // The backstop armed above means a failed header write cannot let a
        // legacy slot through this handle.
        let slots_flagged = upgrade_exclusion.mark_slots_v1()?;

        Ok(UpgradeReport {
            tables_refreshed: names.len(),
            slots_flagged,
            audit_upgraded,
        })
    }

    /// Path to the audit log file, if audit logging is enabled.
    #[cfg(feature = "audit-log")]
    pub fn audit_log_path(&self) -> Option<PathBuf> {
        if self.audit_log.is_some() && !self.data_path.as_os_str().is_empty() {
            Some(crate::audit::resolve_audit_path(&self.data_path))
        } else {
            None
        }
    }

    #[cfg(feature = "audit-log")]
    fn discover_audit_log_paths(&self) -> Result<Vec<PathBuf>> {
        let paths = crate::audit::audit_log_paths_while_locked(&self.data_path)?;
        if paths.is_empty() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "audit logging is enabled but its history is missing",
            )));
        }
        Ok(paths)
    }

    /// A moment-in-time list of audit log names, newest first, or empty when
    /// logging is off. Rotation is excluded while names are captured but can
    /// rename them after return; use [`Database::visit_verified_audit_history`]
    /// to read a stable authenticated snapshot.
    #[cfg(feature = "audit-log")]
    pub fn audit_log_paths(&self) -> Result<Vec<PathBuf>> {
        let Some(audit) = &self.audit_log else {
            return Ok(Vec::new());
        };
        if self.data_path.as_os_str().is_empty() {
            return Ok(Vec::new());
        }
        let _guard = audit.lock();
        self.discover_audit_log_paths()
    }

    /// Visit authenticated audit entries oldest first without loading the full
    /// history into memory. Open segment handles are snapshotted while rotation
    /// is excluded; verification and callbacks run after releasing the writer.
    /// Authentication, generation, identity, and header-shortfall failures are
    /// returned before the first callback. `None` means logging is disabled;
    /// `Some(n)` is the number of entries visited.
    #[cfg(feature = "audit-log")]
    pub fn visit_verified_audit_history<F>(&self, visitor: F) -> Result<Option<u64>>
    where
        F: FnMut(&Path, &crate::audit::AuditEntry) -> Result<()>,
    {
        let Some(audit) = &self.audit_log else {
            return Ok(None);
        };
        if self.data_path.as_os_str().is_empty() {
            return Ok(None);
        }
        let guard = audit.lock();
        let audit_key = Zeroizing::new(*guard.audit_key());
        let missing_at_open = guard.missing_at_open();
        let mut snapshot = crate::audit::AuditHistorySnapshot::open_while_locked(&self.data_path)?;
        drop(guard);
        if missing_at_open > 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("audit history was short by {missing_at_open} entries when opened"),
            )));
        }
        snapshot
            .verify_and_visit(&audit_key, self.file_id, visitor)
            .map(Some)
    }

    /// Entries in the live audit log file, or `None` when logging is off.
    ///
    /// Rotated predecessors are not counted; use
    /// [`Database::visit_verified_audit_history`] to total the whole chain.
    #[cfg(feature = "audit-log")]
    pub fn live_audit_entry_count(&self) -> Option<u64> {
        self.audit_log.as_ref().map(|m| m.lock().entry_count())
    }

    /// Entries the live log's header claimed at open that its records no longer
    /// held, or `None` when logging is off.
    ///
    /// An unauthenticated consistency signal, not an anti-rollback guarantee: it
    /// catches uncoordinated truncation when the mutable count was left behind,
    /// but an offline writer can lower the count too. Opening recounts and then
    /// overwrites the field, so a mismatch is retained here once, at open.
    #[cfg(feature = "audit-log")]
    pub fn audit_entries_missing(&self) -> Option<u64> {
        self.audit_log.as_ref().map(|m| m.lock().missing_at_open())
    }

    /// Verify every audit log file, newest first, pairing each with its result.
    ///
    /// Beyond each segment's local HMACs, this checks generation continuity,
    /// database identity, sequence continuity, and v2 seed handoff.
    /// [`Database::verify_audit_log`] covers only the newest segment and trusts
    /// its header seed. Identity and count fields stay mutable consistency
    /// checks; the oldest retained seed has no anti-rollback anchor.
    #[cfg(feature = "audit-log")]
    pub fn verify_audit_chain(&self) -> Result<Vec<(PathBuf, crate::audit::AuditVerifyResult)>> {
        let audit = self
            .audit_log
            .as_ref()
            .ok_or_else(|| Error::Io(std::io::Error::other("audit logging is not enabled")))?;
        let guard = audit.lock();
        let audit_key = Zeroizing::new(*guard.audit_key());
        let mut snapshot = crate::audit::AuditHistorySnapshot::open_while_locked(&self.data_path)?;
        drop(guard);
        snapshot.verify(&audit_key, self.file_id)
    }

    /// Verify the audit log's HMAC chain integrity.
    #[cfg(feature = "audit-log")]
    pub fn verify_audit_log(&self) -> Result<crate::audit::AuditVerifyResult> {
        let audit = self
            .audit_log
            .as_ref()
            .ok_or_else(|| Error::Io(std::io::Error::other("audit logging is not enabled")))?;
        let guard = audit.lock();
        let path = crate::audit::resolve_audit_path(&self.data_path);
        crate::audit::verify_audit_log(&path, guard.audit_key())
    }

    #[cfg(feature = "audit-log")]
    pub(crate) fn log_audit(&self, event_type: AuditEventType, detail: &[u8]) -> Result<()> {
        if let Some(ref mutex) = self.audit_log {
            mutex.lock().log(event_type, detail)?;
        }
        Ok(())
    }

    #[cfg(feature = "audit-log")]
    pub(crate) fn log_audit_after_operation(
        &self,
        operation: &'static str,
        event_type: AuditEventType,
        detail: &[u8],
    ) -> Result<()> {
        self.log_audit(event_type, detail)
            .map_err(|source| Error::AuditFailureAfterOperation {
                operation,
                source: Box::new(source),
            })
    }

    #[cfg(feature = "audit-log")]
    pub(crate) fn retain_created_audit_history(&self) {
        if let Some(ref mutex) = self.audit_log {
            mutex.lock().retain_created_history();
        }
    }

    #[cfg(feature = "audit-log")]
    fn log_audit_with_path(
        &self,
        operation: &'static str,
        event_type: AuditEventType,
        path: &Path,
    ) -> Result<()> {
        self.log_audit_path(event_type, path)
            .map_err(|source| Error::AuditFailureAfterOperation {
                operation,
                source: Box::new(source),
            })
    }

    #[cfg(feature = "audit-log")]
    fn log_audit_path(&self, event_type: AuditEventType, path: &Path) -> Result<()> {
        let path_str = path.to_string_lossy();
        let path_bytes = path_str.as_bytes();
        let len = u16::try_from(path_bytes.len())
            .map_err(|_| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "audit path detail exceeds 65535 bytes",
                ))
            })?
            .to_le_bytes();
        let mut detail = Vec::with_capacity(2 + path_bytes.len());
        detail.extend_from_slice(&len);
        detail.extend_from_slice(path_bytes);
        self.log_audit(event_type, &detail)
    }
}

use citadel_sync::transport::SyncTransport;

/// Outcome of a sync operation.
#[derive(Debug, Clone)]
pub struct SyncOutcome {
    /// Per-table results: `(table_name, entries_applied)`.
    pub tables_synced: Vec<(Vec<u8>, u64)>,
    /// Default tree sync result (if performed).
    pub default_tree: Option<citadel_sync::SyncOutcome>,
}

const NODE_ID_KEY: &[u8] = b"__citadel_node_id";

fn decode_node_id(data: &[u8]) -> Option<citadel_sync::NodeId> {
    Some(citadel_sync::NodeId::from_bytes(data.try_into().ok()?))
}

impl Database {
    /// Get or create a persistent NodeId for this database.
    pub fn node_id(&self) -> Result<citadel_sync::NodeId> {
        let mut rtx = self.begin_read();
        if let Some(data) = rtx.get(NODE_ID_KEY)? {
            return decode_node_id(&data).ok_or(Error::DatabaseCorrupted);
        }
        drop(rtx);

        let mut wtx = self.begin_write()?;
        // Another caller may have initialized the ID after the optimistic
        // read. Recheck under the single-writer lock before creating one.
        if let Some(data) = wtx.get(NODE_ID_KEY)? {
            return decode_node_id(&data).ok_or(Error::DatabaseCorrupted);
        }
        let node_id = citadel_sync::NodeId::random();
        wtx.insert(NODE_ID_KEY, &node_id.to_bytes())?;
        wtx.commit()?;
        Ok(node_id)
    }

    /// Push local named tables to a remote peer.
    ///
    /// Tables commit independently, so an error can follow earlier tables being
    /// applied; retrying resumes from the peers' durable state.
    pub fn sync_to(&self, addr: &str, sync_key: &citadel_sync::SyncKey) -> Result<SyncOutcome> {
        let node_id = self.node_id()?;
        let transport =
            citadel_sync::NoiseTransport::connect(addr, sync_key).map_err(sync_err_to_core)?;
        let session = citadel_sync::SyncSession::new(citadel_sync::SyncConfig {
            node_id,
            direction: citadel_sync::SyncDirection::Push,
            crdt_aware: false,
        });

        let results = session
            .sync_tables_as_initiator(&self.manager, &transport)
            .map_err(sync_err_to_core)?;

        transport.close().map_err(sync_err_to_core)?;

        Ok(SyncOutcome {
            tables_synced: results
                .into_iter()
                .map(|(name, r)| (name, r.entries_applied))
                .collect(),
            default_tree: None,
        })
    }

    /// Handle an incoming sync session from a remote peer.
    ///
    /// Tables commit independently, so an error can follow earlier tables being
    /// applied; retrying resumes from the peers' durable state.
    pub fn handle_sync(
        &self,
        stream: std::net::TcpStream,
        sync_key: &citadel_sync::SyncKey,
    ) -> Result<SyncOutcome> {
        let node_id = self.node_id()?;
        let transport =
            citadel_sync::NoiseTransport::accept(stream, sync_key).map_err(sync_err_to_core)?;
        let session = citadel_sync::SyncSession::new(citadel_sync::SyncConfig {
            node_id,
            direction: citadel_sync::SyncDirection::Push,
            crdt_aware: false,
        });

        let results = session
            .handle_table_sync_as_responder(&self.manager, &transport)
            .map_err(sync_err_to_core)?;

        transport.close().map_err(sync_err_to_core)?;

        Ok(SyncOutcome {
            tables_synced: results
                .into_iter()
                .map(|(name, r)| (name, r.entries_applied))
                .collect(),
            default_tree: None,
        })
    }
}

fn sync_err_to_core(e: citadel_sync::transport::SyncError) -> Error {
    match e {
        citadel_sync::transport::SyncError::Io(io) => Error::Io(io),
        other => Error::Sync(other.to_string()),
    }
}

#[cfg(feature = "audit-log")]
impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.log_audit(AuditEventType::DatabaseClosed, &[]);
    }
}

/// `{data_path}.citadel-keys`
fn resolve_key_path_for(data_path: &Path) -> PathBuf {
    let mut name = data_path.as_os_str().to_os_string();
    name.push(".citadel-keys");
    PathBuf::from(name)
}

/// Sidecar region key store path: `key_path` with the `citadel-regions`
/// extension, e.g. `mydb.citadel.citadel-keys` ->
/// `mydb.citadel.citadel-regions`.
fn region_store_path_for(key_path: &Path) -> PathBuf {
    key_path.with_extension("citadel-regions")
}

/// Sidecar atom key store path: `key_path` with the `citadel-atomkeys`
/// extension.
fn atom_store_path_for(key_path: &Path) -> PathBuf {
    key_path.with_extension("citadel-atomkeys")
}

#[cfg(test)]
mod sql_cache_tests {
    use super::*;
    use crate::builder::DatabaseBuilder;
    use citadel_core::types::Argon2Profile;

    fn open_db(dir: &Path) -> Database {
        DatabaseBuilder::new(dir.join("test.db"))
            .passphrase(b"x")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap()
    }

    #[derive(Debug, PartialEq)]
    struct Marker(u32);

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn created_file_guard_removes_only_armed_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let removed = dir.path().join("removed");
        let retained = dir.path().join("retained");
        fs::write(&removed, b"owned").unwrap();
        fs::write(&retained, b"complete").unwrap();

        {
            let mut guard = CreatedFileGuard::new();
            guard.track(&removed);
        }
        {
            let mut guard = CreatedFileGuard::new();
            guard.track(&retained);
            guard.disarm();
        }

        assert!(!removed.exists());
        assert_eq!(fs::read(retained).unwrap(), b"complete");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn published_key_replacement_updates_the_live_trusted_image() {
        let (current, keys) = citadel_crypto::key_manager::create_key_file(
            b"password",
            7,
            citadel_core::types::CipherId::Aes256Ctr,
            citadel_core::types::KdfAlgorithm::Argon2id,
            64,
            1,
            1,
        )
        .unwrap();
        let auth_key = KeyFileAuthKey::from_database_mac_key(&keys.mac_key);
        let mut state = KeyFileState::new(current.clone(), auth_key);
        let mut replacement = current;
        replacement.current_epoch += 1;
        let expected = replacement.serialize();

        let publication = state
            .replace_trusted_with(Path::new("unused"), replacement, |_, _| {
                Err(citadel_io::durable::AtomicWriteError::Published(
                    std::io::Error::other("injected directory sync failure"),
                ))
            })
            .unwrap();

        assert!(matches!(
            publication,
            PublishedWriteOutcome::DurabilityUnconfirmed(_)
        ));
        assert_eq!(state.serialize(), expected);
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "audit-log"))]
    #[test]
    fn audited_publication_preserves_durability_and_audit_failures() {
        let error = finish_audited_operation(
            "test operation",
            PublishedWriteOutcome::DurabilityUnconfirmed(std::io::Error::other(
                "directory sync failed",
            )),
            Err(Error::Io(std::io::Error::other("audit disk full"))),
        )
        .unwrap_err();

        match error {
            Error::DurabilityAndAuditFailureAfterOperation {
                operation,
                durability,
                audit,
            } => {
                assert_eq!(operation, "test operation");
                assert_eq!(durability.to_string(), "directory sync failed");
                assert_eq!(audit.to_string(), "I/O error: audit disk full");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn insert_then_get_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path());
        db.sql_cache_insert("k".to_string(), Arc::new(Marker(42)));
        let got = db.sql_cache_get::<Marker>("k").unwrap();
        assert_eq!(*got, Marker(42));
    }

    #[test]
    fn get_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path());
        assert!(db.sql_cache_get::<Marker>("missing").is_none());
    }

    #[test]
    fn get_wrong_type_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path());
        db.sql_cache_insert("k".to_string(), Arc::new(Marker(1)));
        assert!(db.sql_cache_get::<String>("k").is_none());
    }

    #[test]
    fn insert_overwrites_existing_entry() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path());
        db.sql_cache_insert("k".to_string(), Arc::new(Marker(1)));
        db.sql_cache_insert("k".to_string(), Arc::new(Marker(2)));
        assert_eq!(*db.sql_cache_get::<Marker>("k").unwrap(), Marker(2));
    }

    #[test]
    fn invalidate_prefix_removes_matching_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path());
        db.sql_cache_insert("ann:t1:ix_v".to_string(), Arc::new(Marker(1)));
        db.sql_cache_insert("ann:t1:ix_w".to_string(), Arc::new(Marker(2)));
        db.sql_cache_insert("ann:t2:ix_v".to_string(), Arc::new(Marker(3)));
        db.sql_cache_insert("other:x".to_string(), Arc::new(Marker(4)));

        let removed = db.sql_cache_invalidate_prefix("ann:t1:");
        assert_eq!(removed, 2);
        assert!(db.sql_cache_get::<Marker>("ann:t1:ix_v").is_none());
        assert!(db.sql_cache_get::<Marker>("ann:t1:ix_w").is_none());
        assert!(db.sql_cache_get::<Marker>("ann:t2:ix_v").is_some());
        assert!(db.sql_cache_get::<Marker>("other:x").is_some());
    }

    #[test]
    fn invalidate_prefix_no_match_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path());
        db.sql_cache_insert("a:1".to_string(), Arc::new(Marker(1)));
        assert_eq!(db.sql_cache_invalidate_prefix("z:"), 0);
        assert_eq!(db.sql_cache_len(), 1);
    }

    #[test]
    fn shared_arc_observed_by_two_borrows() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path());
        let value = Arc::new(Marker(7));
        db.sql_cache_insert("k".to_string(), Arc::clone(&value));
        let a = db.sql_cache_get::<Marker>("k").unwrap();
        let b = db.sql_cache_get::<Marker>("k").unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert!(Arc::ptr_eq(&a, &value));
    }
}
