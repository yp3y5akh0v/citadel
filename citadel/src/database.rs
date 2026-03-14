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

        Ok(())
    }

    /// Run an integrity check on the database.
    pub fn integrity_check(&self) -> Result<IntegrityReport> {
        self.manager.integrity_check()
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

        Ok(())
    }
}

impl Database {
    #[doc(hidden)]
    pub fn manager(&self) -> &TxnManager {
        &self.manager
    }
}

/// `{data_path}.citadel-keys`
fn resolve_key_path_for(data_path: &Path) -> PathBuf {
    let mut name = data_path.as_os_str().to_os_string();
    name.push(".citadel-keys");
    PathBuf::from(name)
}
