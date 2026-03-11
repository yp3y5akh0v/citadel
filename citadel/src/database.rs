use std::fs;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use citadel_core::{Error, Result, KEY_FILE_SIZE};
use citadel_txn::integrity::IntegrityReport;
use citadel_txn::manager::TxnManager;
use citadel_txn::read_txn::ReadTxn;
use citadel_txn::write_txn::WriteTxn;
use citadel_io::sync_io::SyncPageIO;

/// Database statistics read from the current commit slot.
#[derive(Debug, Clone)]
pub struct DbStats {
    pub tree_depth: u16,
    pub entry_count: u64,
    pub total_pages: u32,
    pub high_water_mark: u32,
}

/// An open Citadel database.
///
/// Thread-safe: `Database` is `Send + Sync`. Multiple threads can hold
/// concurrent read transactions. Only one write transaction at a time.
///
/// The database file is exclusively locked for the lifetime of this struct.
/// Dropping `Database` releases the lock.
pub struct Database {
    manager: TxnManager,
    data_path: PathBuf,
    key_path: PathBuf,
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

    /// Change the database passphrase (fast key rotation).
    ///
    /// Re-wraps the Root Encryption Key with a new Master Key derived from
    /// the new passphrase. No page re-encryption needed — the DEK and MAC_KEY
    /// remain unchanged. Atomic via temp file + rename.
    pub fn change_passphrase(&self, old_passphrase: &[u8], new_passphrase: &[u8]) -> Result<()> {
        use citadel_crypto::key_manager::{KeyFile, wrap_rek, unwrap_rek};
        use citadel_crypto::kdf::{derive_mk_argon2id, generate_salt};

        // Read key file
        let key_data = fs::read(&self.key_path)?;
        if key_data.len() != KEY_FILE_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "key file has incorrect size",
            )));
        }
        let key_buf: [u8; KEY_FILE_SIZE] = key_data.try_into().unwrap();
        let kf = KeyFile::deserialize(&key_buf)?;

        // Derive old MK and verify
        let old_mk = derive_mk_argon2id(
            old_passphrase,
            &kf.argon2_salt,
            kf.argon2_m_cost,
            kf.argon2_t_cost,
            kf.argon2_p_cost,
        )?;
        kf.verify_mac(&old_mk)?;

        // Unwrap REK
        let rek = unwrap_rek(&old_mk, &kf.wrapped_rek)
            .map_err(|_| Error::BadPassphrase)?;

        // Generate new salt and derive new MK
        let new_salt = generate_salt();
        let new_mk = derive_mk_argon2id(
            new_passphrase,
            &new_salt,
            kf.argon2_m_cost,
            kf.argon2_t_cost,
            kf.argon2_p_cost,
        )?;

        // Re-wrap REK with new MK
        let new_wrapped = wrap_rek(&new_mk, &rek);

        // Build new key file
        let mut new_kf = kf.clone();
        new_kf.argon2_salt = new_salt;
        new_kf.wrapped_rek = new_wrapped;
        new_kf.update_mac(&new_mk);

        // Write atomically: temp file + rename
        let temp_path = self.key_path.with_extension("tmp");
        fs::write(&temp_path, new_kf.serialize())?;
        fs::rename(&temp_path, &self.key_path)?;

        Ok(())
    }

    /// Run an integrity check on the database.
    ///
    /// Walks all B+ trees, verifying HMAC, checksums, key ordering, and
    /// page accounting. Returns a report of any issues found.
    pub fn integrity_check(&self) -> Result<IntegrityReport> {
        self.manager.integrity_check()
    }

    /// Create a hot backup of the database.
    ///
    /// Uses an MVCC snapshot to copy consistent encrypted pages to the
    /// destination file. The source database can continue accepting writes
    /// during the backup. Also copies the key file.
    pub fn backup(&self, dest_path: &Path) -> Result<()> {
        let dest_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dest_path)?;
        let dest_io = SyncPageIO::new(dest_file);
        self.manager.backup_to(&dest_io)?;

        // Copy key file
        let dest_key_path = resolve_key_path_for(dest_path);
        fs::copy(&self.key_path, &dest_key_path)?;

        Ok(())
    }

    /// Compact the database into a new file.
    ///
    /// Copies only live pages with sequential page IDs, eliminating
    /// free space gaps. Re-encrypts each page with a fresh IV.
    /// Also copies the key file.
    pub fn compact(&self, dest_path: &Path) -> Result<()> {
        let dest_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dest_path)?;
        let dest_io = SyncPageIO::new(dest_file);
        self.manager.compact_to(&dest_io)?;

        // Copy key file
        let dest_key_path = resolve_key_path_for(dest_path);
        fs::copy(&self.key_path, &dest_key_path)?;

        Ok(())
    }
}

/// Resolve the default key file path for a given data path.
fn resolve_key_path_for(data_path: &Path) -> PathBuf {
    let mut name = data_path.as_os_str().to_os_string();
    name.push(".citadel-keys");
    PathBuf::from(name)
}
