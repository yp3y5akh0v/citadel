use std::fs;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use citadel_core::{Error, Result, KEY_FILE_SIZE, MERKLE_HASH_SIZE};
use citadel_io::durable;
use citadel_io::sync_io::SyncPageIO;
use citadel_txn::integrity::IntegrityReport;
use citadel_txn::manager::TxnManager;
use citadel_txn::read_txn::ReadTxn;
use citadel_txn::write_txn::WriteTxn;

#[cfg(feature = "audit-log")]
use crate::audit::{AuditEventType, AuditLog};

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
    #[cfg(feature = "audit-log")]
    audit_log: Option<parking_lot::Mutex<AuditLog>>,
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
        audit_log: Option<AuditLog>,
    ) -> Self {
        Self {
            manager,
            data_path,
            key_path,
            audit_log: audit_log.map(parking_lot::Mutex::new),
        }
    }

    #[cfg(not(feature = "audit-log"))]
    pub(crate) fn new(manager: TxnManager, data_path: PathBuf, key_path: PathBuf) -> Self {
        Self {
            manager,
            data_path,
            key_path,
        }
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

    /// Path to the data file.
    pub fn data_path(&self) -> &Path {
        &self.data_path
    }

    /// Path to the key file.
    pub fn key_path(&self) -> &Path {
        &self.key_path
    }

    /// Number of currently active readers.
    pub fn reader_count(&self) -> usize {
        self.manager.reader_count()
    }

    /// Change the database passphrase (re-wraps REK, no page re-encryption).
    pub fn change_passphrase(&self, old_passphrase: &[u8], new_passphrase: &[u8]) -> Result<()> {
        use citadel_crypto::key_manager::{KeyFile, wrap_rek, unwrap_rek};
        use citadel_crypto::kdf::{derive_mk, generate_salt};

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

        let rek = unwrap_rek(&old_mk, &kf.wrapped_rek)
            .map_err(|_| Error::BadPassphrase)?;

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

    /// Run an integrity check on the database.
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
    pub fn backup(&self, dest_path: &Path) -> Result<()> {
        let dest_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dest_path)?;
        let dest_io = SyncPageIO::new(dest_file);
        self.manager.backup_to(&dest_io)?;

        let dest_key_path = resolve_key_path_for(dest_path);
        fs::copy(&self.key_path, &dest_key_path)?;

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
        use citadel_crypto::key_manager::{KeyFile, unwrap_rek};
        use citadel_crypto::key_backup::create_key_backup;
        use citadel_crypto::kdf::derive_mk;

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

        let rek = unwrap_rek(&mk, &kf.wrapped_rek)
            .map_err(|_| Error::BadPassphrase)?;

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

    /// Restore a key file from an encrypted backup (static — no `Database` needed).
    ///
    /// Unwraps the REK using `backup_passphrase`, then creates a new key file
    /// protected by `new_db_passphrase`.
    pub fn restore_key_from_backup(
        backup_path: &Path,
        backup_passphrase: &[u8],
        new_db_passphrase: &[u8],
        db_path: &Path,
    ) -> Result<()> {
        use citadel_crypto::key_backup::restore_rek_from_backup;
        use citadel_crypto::key_manager::wrap_rek;
        use citadel_crypto::key_manager::KeyFile;
        use citadel_crypto::kdf::{derive_mk, generate_salt};
        use citadel_core::{KEY_BACKUP_SIZE, KEY_FILE_MAGIC, KEY_FILE_VERSION, MAC_SIZE, WRAPPED_KEY_SIZE};

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
    pub fn compact(&self, dest_path: &Path) -> Result<()> {
        let dest_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dest_path)?;
        let dest_io = SyncPageIO::new(dest_file);
        self.manager.compact_to(&dest_io)?;

        let dest_key_path = resolve_key_path_for(dest_path);
        fs::copy(&self.key_path, &dest_key_path)?;

        #[cfg(feature = "audit-log")]
        self.log_audit_with_path(AuditEventType::CompactionPerformed, dest_path);

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
        let audit = self.audit_log.as_ref().ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "audit logging is not enabled",
            ))
        })?;
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

// --- Peer-to-peer sync ---

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
    pub fn sync_to(&self, addr: &str) -> Result<SyncOutcome> {
        let node_id = self.node_id()?;
        let transport = citadel_sync::TcpTransport::connect(addr)
            .map_err(sync_err_to_core)?;
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
    pub fn handle_sync(&self, stream: std::net::TcpStream) -> Result<SyncOutcome> {
        let node_id = self.node_id()?;
        let transport = citadel_sync::TcpTransport::from_stream(stream)
            .map_err(sync_err_to_core)?;
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
    Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
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
