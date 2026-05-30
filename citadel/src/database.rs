use std::any::Any;
use std::fs;
#[cfg(not(target_arch = "wasm32"))]
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use citadel_core::{Error, Result, KEY_FILE_SIZE, KEY_SIZE, MERKLE_HASH_SIZE, WRAPPED_KEY_SIZE};
use citadel_crypto::hkdf_utils::RegionWrapKeys;
use citadel_io::durable;
#[cfg(not(target_arch = "wasm32"))]
use citadel_io::mmap_io::MmapPageIO;
use citadel_txn::integrity::IntegrityReport;
use citadel_txn::manager::TxnManager;
use citadel_txn::read_txn::ReadTxn;
use citadel_txn::write_txn::WriteTxn;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;

use crate::atom_store::AtomKeyStore;
#[cfg(feature = "audit-log")]
use crate::audit::{AuditEventType, AuditLog};
use crate::key_codec::SlotRecord;
use crate::region_store::RegionKeyStore;

/// Type-erased cache of `Arc<T>` entries shared across connections to one DB.
pub type SharedCache = Mutex<FxHashMap<String, Arc<dyn Any + Send + Sync>>>;

/// Cloneable handle to the per-Database shared cache.
pub type SqlCacheHandle = Arc<SharedCache>;

/// Database statistics read from the current commit slot.
#[derive(Debug, Clone)]
pub struct DbStats {
    pub tree_depth: u16,
    pub entry_count: u64,
    pub total_pages: u32,
    pub high_water_mark: u32,
    pub merkle_root: [u8; MERKLE_HASH_SIZE],
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
    #[cfg(feature = "audit-log")]
    audit_log: Option<Mutex<AuditLog>>,
    /// Shared cache for higher-level crates (e.g. citadel-sql ANN indexes).
    /// Held here so it spans all connections without a dependency cycle.
    sql_caches: Arc<SharedCache>,
    /// Region wrap keys for per-region cryptographic erasure (citadel-mem).
    /// `Some` only when the builder enabled region keys; derived from the REK
    /// and zeroized on drop. The raw REK is never retained here.
    region_keys: Option<RegionWrapKeys>,
    /// Sidecar region key store (lazy); shared by every `MemoryEngine` over this db.
    region_store: Mutex<Option<RegionKeyStore>>,
    /// Sidecar per-atom key store (lazy); holds each atom's wrapped ACK.
    atom_store: Mutex<Option<AtomKeyStore>>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("data_path", &self.data_path)
            .field("key_path", &self.key_path)
            .finish()
    }
}

// TxnManager is internally synchronized (Mutex + Atomic)
unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Database {
    #[cfg(feature = "audit-log")]
    pub(crate) fn new(
        manager: TxnManager,
        data_path: PathBuf,
        key_path: PathBuf,
        file_id: u64,
        region_keys: Option<RegionWrapKeys>,
        audit_log: Option<AuditLog>,
    ) -> Self {
        Self {
            manager,
            data_path,
            key_path,
            file_id,
            audit_log: audit_log.map(Mutex::new),
            sql_caches: Arc::new(Mutex::new(FxHashMap::default())),
            region_keys,
            region_store: Mutex::new(None),
            atom_store: Mutex::new(None),
        }
    }

    #[cfg(not(feature = "audit-log"))]
    pub(crate) fn new(
        manager: TxnManager,
        data_path: PathBuf,
        key_path: PathBuf,
        file_id: u64,
        region_keys: Option<RegionWrapKeys>,
    ) -> Self {
        Self {
            manager,
            data_path,
            key_path,
            file_id,
            sql_caches: Arc::new(Mutex::new(FxHashMap::default())),
            region_keys,
            region_store: Mutex::new(None),
            atom_store: Mutex::new(None),
        }
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
    pub fn begin_read(&self) -> ReadTxn<'_> {
        self.manager.begin_read()
    }

    /// Begin a read-write transaction. Only one can be active at a time.
    pub fn begin_write(&self) -> Result<WriteTxn<'_>> {
        self.manager.begin_write()
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

    pub fn key_path(&self) -> &Path {
        &self.key_path
    }

    /// Database file identifier from the file header. citadel-mem binds the
    /// region key store to this value so a mismatched sidecar is rejected.
    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    /// Whether per-region cryptographic erasure keys are available.
    /// `true` only when the database was opened with `enable_region_keys(true)`.
    pub fn region_keys_enabled(&self) -> bool {
        self.region_keys.is_some()
    }

    /// Wrap a region's random content key (RCK) under the region KEK (AES-256-KW).
    /// The 40-byte result is the sole copy of the RCK; citadel-mem stores it in the
    /// sidecar key store and overwrites it in place to erase the region.
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

    /// Allocate a slot and store the wrapped RCK (fsync'd); returns `(slot, gen)`.
    pub fn region_store_allocate_write(
        &self,
        region_id: u64,
        wrapped: &[u8; WRAPPED_KEY_SIZE],
    ) -> Result<(u32, u64)> {
        self.with_region_store(|s| {
            let slot = s.allocate_slot()?;
            let gen = s.write_live(slot, region_id, wrapped)?;
            Ok((slot, gen))
        })
    }

    /// The authoritative record of region key `slot`.
    pub fn region_store_slot(&self, slot: u32) -> Result<SlotRecord> {
        self.with_region_store(|s| s.read_slot(slot))
    }

    /// Cryptographically erase region key `slot` (no-op if already erased).
    pub fn region_store_tombstone(&self, slot: u32, region_id: u64) -> Result<()> {
        self.with_region_store(|s| s.tombstone(slot, region_id))
    }

    /// `(slot, region_id)` for every LIVE region key slot.
    pub fn region_store_live_owners(&self) -> Result<Vec<(u32, u64)>> {
        self.with_region_store(|s| s.live_owners())
    }

    /// Path to the sidecar per-atom key store, `{key_path}` with the `citadel-atomkeys`
    /// extension. Pure path math; the file only exists once an encrypted atom is written.
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

    /// Allocate a slot and store one atom's wrapped ACK (fsync'd); returns `(slot, gen)`.
    pub fn atom_store_allocate_write(
        &self,
        atom_id: u64,
        wrapped: &[u8; WRAPPED_KEY_SIZE],
    ) -> Result<(u32, u64)> {
        self.with_atom_store(|s| {
            let slot = s.allocate_slot()?;
            let gen = s.write_live(slot, atom_id, wrapped)?;
            Ok((slot, gen))
        })
    }

    /// Allocate and durably write a batch of `(atom_id, wrapped)` ACKs with ONE fsync;
    /// returns `(slot, gen)` per item in order.
    pub fn atom_store_allocate_batch(
        &self,
        items: &[(u64, [u8; WRAPPED_KEY_SIZE])],
    ) -> Result<Vec<(u32, u64)>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        self.with_atom_store(|s| {
            let slots = s.allocate_batch(items.len())?;
            let writes: Vec<(u32, u64, [u8; WRAPPED_KEY_SIZE])> = slots
                .iter()
                .zip(items)
                .map(|(&slot, (atom_id, wrapped))| (slot, *atom_id, *wrapped))
                .collect();
            let gens = s.write_live_batch(&writes)?;
            Ok(slots.into_iter().zip(gens).collect())
        })
    }

    /// The authoritative record of atom key `slot` (its wrapped ACK and state).
    pub fn atom_store_slot(&self, slot: u32) -> Result<SlotRecord> {
        self.with_atom_store(|s| s.read_slot(slot))
    }

    /// Cryptographically erase atom key `slot` (no-op if already erased).
    pub fn atom_store_tombstone(&self, slot: u32, atom_id: u64) -> Result<()> {
        self.with_atom_store(|s| s.tombstone(slot, atom_id))
    }

    /// Erase a batch of atom key slots with two fsyncs total (not 2N). Items are `(slot, atom_id)`.
    pub fn atom_store_tombstone_batch(&self, items: &[(u32, u64)]) -> Result<()> {
        self.with_atom_store(|s| s.tombstone_batch(items))
    }

    /// Every LIVE atom key's `atom_id -> wrapped ACK`, in one whole-file pass.
    pub fn atom_store_live_wrapped(&self) -> Result<FxHashMap<u64, [u8; WRAPPED_KEY_SIZE]>> {
        self.with_atom_store(|s| s.live_wrapped())
    }

    /// `(slot, atom_id)` for every LIVE atom key slot.
    pub fn atom_store_live_owners(&self) -> Result<Vec<(u32, u64)>> {
        self.with_atom_store(|s| s.live_owners())
    }

    /// Number of currently active readers.
    pub fn reader_count(&self) -> usize {
        self.manager.reader_count()
    }

    /// Change the database passphrase (re-wraps REK, no page re-encryption).
    pub fn change_passphrase(&self, old_passphrase: &[u8], new_passphrase: &[u8]) -> Result<()> {
        use citadel_crypto::kdf::{derive_mk, generate_salt};
        use citadel_crypto::key_manager::{unwrap_rek, wrap_rek, KeyFile};

        let key_data = fs::read(&self.key_path)?;
        if key_data.len() != KEY_FILE_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "key file has incorrect size",
            )));
        }
        let key_buf: [u8; KEY_FILE_SIZE] = key_data.try_into().unwrap();
        let kf = KeyFile::deserialize(&key_buf)?;

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
        new_kf.update_mac(&new_mk);

        durable::atomic_write(&self.key_path, &new_kf.serialize())?;

        #[cfg(feature = "audit-log")]
        self.log_audit(AuditEventType::PassphraseChanged, &[]);

        Ok(())
    }

    pub fn integrity_check(&self) -> Result<IntegrityReport> {
        let report = self.manager.integrity_check()?;

        #[cfg(feature = "audit-log")]
        {
            let error_count = report.errors.len() as u32;
            self.log_audit(
                AuditEventType::IntegrityCheckPerformed,
                &error_count.to_le_bytes(),
            );
        }

        Ok(report)
    }

    /// Create a hot backup via MVCC snapshot. Also copies the key file.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn backup(&self, dest_path: &Path) -> Result<()> {
        let dest_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dest_path)?;
        let dest_io = MmapPageIO::try_new(dest_file)?;
        self.manager.backup_to(&dest_io)?;

        let dest_key_path = resolve_key_path_for(dest_path);
        fs::copy(&self.key_path, &dest_key_path)?;
        self.copy_region_store_to(&dest_key_path)?;

        #[cfg(feature = "audit-log")]
        self.log_audit_with_path(AuditEventType::BackupCreated, dest_path);

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
        use citadel_crypto::key_manager::{unwrap_rek, KeyFile};

        let key_data = fs::read(&self.key_path)?;
        if key_data.len() != KEY_FILE_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "key file has incorrect size",
            )));
        }
        let key_buf: [u8; KEY_FILE_SIZE] = key_data.try_into().unwrap();
        let kf = KeyFile::deserialize(&key_buf)?;

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
        )?;

        durable::write_and_sync(dest_path, &backup_data)?;

        #[cfg(feature = "audit-log")]
        self.log_audit_with_path(AuditEventType::KeyBackupExported, dest_path);

        Ok(())
    }

    /// Restore a key file from an encrypted backup (static - no `Database` needed).
    ///
    /// Unwraps the REK using `backup_passphrase`, then creates a new key file
    /// protected by `new_db_passphrase`.
    pub fn restore_key_from_backup(
        backup_path: &Path,
        backup_passphrase: &[u8],
        new_db_passphrase: &[u8],
        db_path: &Path,
    ) -> Result<()> {
        use citadel_core::{
            KEY_BACKUP_SIZE, KEY_FILE_MAGIC, KEY_FILE_VERSION, MAC_SIZE, WRAPPED_KEY_SIZE,
        };
        use citadel_crypto::kdf::{derive_mk, generate_salt};
        use citadel_crypto::key_backup::restore_rek_from_backup;
        use citadel_crypto::key_manager::wrap_rek;
        use citadel_crypto::key_manager::KeyFile;

        let backup_data = fs::read(backup_path)?;
        if backup_data.len() != KEY_BACKUP_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "backup file has incorrect size",
            )));
        }
        let backup_buf: [u8; KEY_BACKUP_SIZE] = backup_data.try_into().unwrap();

        let restored = restore_rek_from_backup(&backup_buf, backup_passphrase)?;

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
            wrapped_rek: new_wrapped,
            current_epoch: restored.epoch,
            prev_wrapped_rek: [0u8; WRAPPED_KEY_SIZE],
            prev_epoch: 0,
            rotation_active: false,
            file_mac: [0u8; MAC_SIZE],
        };
        new_kf.update_mac(&new_mk);

        let key_path = resolve_key_path_for(db_path);
        durable::atomic_write(&key_path, &new_kf.serialize())?;

        Ok(())
    }

    /// Compact the database into a new file. Also copies the key file.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn compact(&self, dest_path: &Path) -> Result<()> {
        let dest_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dest_path)?;
        let dest_io = MmapPageIO::try_new(dest_file)?;
        self.manager.compact_to(&dest_io)?;

        let dest_key_path = resolve_key_path_for(dest_path);
        fs::copy(&self.key_path, &dest_key_path)?;
        self.copy_region_store_to(&dest_key_path)?;

        #[cfg(feature = "audit-log")]
        self.log_audit_with_path(AuditEventType::CompactionPerformed, dest_path);

        Ok(())
    }

    /// Copy the sidecar region key store next to `dest_key_path`, if it exists.
    ///
    /// A backup/compaction must carry the wrapped region keys so encrypted
    /// regions remain openable from the copy. Note: a backup taken while a
    /// region is live retains a recoverable key; `forget` cannot reach it, so
    /// backup retention is the operator's responsibility (see `region_store_path`).
    #[cfg(not(target_arch = "wasm32"))]
    fn copy_region_store_to(&self, dest_key_path: &Path) -> Result<()> {
        let src = self.region_store_path();
        if src.exists() {
            let dest = region_store_path_for(dest_key_path);
            fs::copy(&src, &dest)?;
        }
        let atom_src = self.atom_store_path();
        if atom_src.exists() {
            fs::copy(&atom_src, atom_store_path_for(dest_key_path))?;
        }
        Ok(())
    }
}

impl Database {
    #[doc(hidden)]
    pub fn manager(&self) -> &TxnManager {
        &self.manager
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
    pub(crate) fn log_audit(&self, event_type: AuditEventType, detail: &[u8]) {
        if let Some(ref mutex) = self.audit_log {
            let _ = mutex.lock().log(event_type, detail);
        }
    }

    #[cfg(feature = "audit-log")]
    fn log_audit_with_path(&self, event_type: AuditEventType, path: &Path) {
        let path_str = path.to_string_lossy();
        let path_bytes = path_str.as_bytes();
        let len = (path_bytes.len() as u16).to_le_bytes();
        let mut detail = Vec::with_capacity(2 + path_bytes.len());
        detail.extend_from_slice(&len);
        detail.extend_from_slice(path_bytes);
        self.log_audit(event_type, &detail);
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

impl Database {
    /// Get or create a persistent NodeId for this database.
    pub fn node_id(&self) -> Result<citadel_sync::NodeId> {
        let mut rtx = self.manager.begin_read();
        if let Some(data) = rtx.get(NODE_ID_KEY)? {
            if data.len() == 8 {
                return Ok(citadel_sync::NodeId::from_bytes(
                    data[..8].try_into().unwrap(),
                ));
            }
        }
        drop(rtx);

        let node_id = citadel_sync::NodeId::random();
        let mut wtx = self.manager.begin_write()?;
        wtx.insert(NODE_ID_KEY, &node_id.to_bytes())?;
        wtx.commit()?;
        Ok(node_id)
    }

    /// Push local named tables to a remote peer.
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
        self.log_audit(AuditEventType::DatabaseClosed, &[]);
    }
}

/// `{data_path}.citadel-keys`
fn resolve_key_path_for(data_path: &Path) -> PathBuf {
    let mut name = data_path.as_os_str().to_os_string();
    name.push(".citadel-keys");
    PathBuf::from(name)
}

/// Sidecar region key store path: `key_path` with the `citadel-regions` extension,
/// e.g. `mydb.citadel.citadel-keys` -> `mydb.citadel.citadel-regions`.
fn region_store_path_for(key_path: &Path) -> PathBuf {
    key_path.with_extension("citadel-regions")
}

/// Sidecar atom key store path: `key_path` with the `citadel-atomkeys` extension.
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
