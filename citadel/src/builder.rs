#[cfg(not(target_arch = "wasm32"))]
use std::fs::OpenOptions;
#[cfg(not(target_arch = "wasm32"))]
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use citadel_core::types::{Argon2Profile, CipherId, KdfAlgorithm, SyncMode};
use citadel_core::{Error, Result, DEFAULT_BUFFER_POOL_SIZE, PBKDF2_MIN_ITERATIONS};
#[cfg(not(target_arch = "wasm32"))]
use citadel_core::{FILE_HEADER_SIZE, KEY_FILE_SIZE};
use citadel_crypto::hkdf_utils::RegionWrapKeys;
use citadel_crypto::key_manager::{
    create_key_file, create_key_file_with_region_keys, KeyFileAuthKey,
};
#[cfg(not(target_arch = "wasm32"))]
use citadel_crypto::key_manager::{open_key_file, open_key_file_with_region_keys};
use citadel_crypto::page_cipher::compute_dek_id;
#[cfg(not(target_arch = "wasm32"))]
use citadel_io::durable;
#[cfg(not(target_arch = "wasm32"))]
use citadel_io::file_lock;
#[cfg(not(target_arch = "wasm32"))]
use citadel_io::file_manager::FileHeader;
#[cfg(not(target_arch = "wasm32"))]
use citadel_io::mmap_io::MmapPageIO;
use citadel_io::traits::PageIO;
use citadel_txn::manager::TxnManager;
use zeroize::Zeroizing;

#[cfg(not(target_arch = "wasm32"))]
use crate::database::{atomic_write_for_operation, CreatedFileGuard};
use crate::database::{Database, KeyFileState};

/// Builder for creating or opening a Citadel database.
///
/// # Examples
///
/// ```no_run
/// use citadel::{DatabaseBuilder, Argon2Profile};
///
/// let db = DatabaseBuilder::new("mydb.citadel")
///     .passphrase(b"secret")
///     .cache_size(512)
///     .create()
///     .unwrap();
/// ```
/// Key-file identity and authenticated state shared by both `finish` variants.
struct FileIdentity {
    key_path: PathBuf,
    file_id: u64,
    key_file: KeyFileState,
}

pub struct DatabaseBuilder {
    path: PathBuf,
    key_path: Option<PathBuf>,
    passphrase: Option<Zeroizing<Vec<u8>>>,
    argon2_profile: Argon2Profile,
    cache_size: usize,
    cipher: CipherId,
    kdf_algorithm: KdfAlgorithm,
    pbkdf2_iterations: u32,
    sync_mode: SyncMode,
    enable_region_keys: bool,
    secure_delete: bool,
    #[cfg(feature = "audit-log")]
    audit_config: crate::audit::AuditConfig,
}

impl DatabaseBuilder {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            key_path: None,
            passphrase: None,
            argon2_profile: Argon2Profile::Desktop,
            cache_size: DEFAULT_BUFFER_POOL_SIZE,
            cipher: CipherId::Aes256Ctr,
            kdf_algorithm: KdfAlgorithm::Argon2id,
            pbkdf2_iterations: PBKDF2_MIN_ITERATIONS,
            sync_mode: SyncMode::Full,
            enable_region_keys: false,
            secure_delete: false,
            #[cfg(feature = "audit-log")]
            audit_config: crate::audit::AuditConfig::default(),
        }
    }

    pub fn passphrase(mut self, passphrase: &[u8]) -> Self {
        self.passphrase = Some(Zeroizing::new(passphrase.to_vec()));
        self
    }

    pub fn key_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.key_path = Some(path.into());
        self
    }

    pub fn argon2_profile(mut self, profile: Argon2Profile) -> Self {
        self.argon2_profile = profile;
        self
    }

    /// Buffer pool capacity in pages. Must be at least 1;
    /// `create()`/`open()`/`create_in_memory()` reject 0 with an error.
    pub fn cache_size(mut self, pages: usize) -> Self {
        self.cache_size = pages;
        self
    }

    pub fn cipher(mut self, cipher: CipherId) -> Self {
        self.cipher = cipher;
        self
    }

    /// Set the key derivation function algorithm.
    ///
    /// Default: `Argon2id`. Use `Pbkdf2HmacSha256` for FIPS 140-3 compliance.
    /// When using PBKDF2, the Argon2 profile is ignored and iterations are
    /// controlled by `pbkdf2_iterations()`.
    pub fn kdf_algorithm(mut self, algorithm: KdfAlgorithm) -> Self {
        self.kdf_algorithm = algorithm;
        self
    }

    /// Set the number of PBKDF2 iterations (only used when KDF is PBKDF2).
    ///
    /// Default: 600,000 (OWASP 2024 minimum for PBKDF2-HMAC-SHA256).
    pub fn pbkdf2_iterations(mut self, iterations: u32) -> Self {
        self.pbkdf2_iterations = iterations;
        self
    }

    pub fn sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    /// Enable per-region cryptographic erasure (used by citadel-mem).
    ///
    /// When set, a region wrap key is derived from the REK at create/open and
    /// retained for the database lifetime so encrypted memory regions can be
    /// sealed under random per-region keys and erased on `forget`. Off by
    /// default; the plaintext storage path is unaffected either way.
    pub fn enable_region_keys(mut self, enable: bool) -> Self {
        self.enable_region_keys = enable;
        self
    }

    /// Zero-fill freed B+ tree pages once they are past all readers, so a
    /// passphrase holder with disk access cannot recover deleted-row residue
    /// from stale pages. Off by default (a small write cost on delete-heavy
    /// workloads).
    pub fn enable_secure_delete(mut self, enable: bool) -> Self {
        self.secure_delete = enable;
        self
    }

    /// Configure the audit log.
    ///
    /// Default: enabled with 10 MB max file size and 3 rotated files.
    #[cfg(feature = "audit-log")]
    pub fn audit_config(mut self, config: crate::audit::AuditConfig) -> Self {
        self.audit_config = config;
        self
    }

    /// Default key file path: `{data_path}.citadel-keys`
    #[cfg(not(target_arch = "wasm32"))]
    fn resolve_key_path(&self) -> PathBuf {
        self.key_path.clone().unwrap_or_else(|| {
            let mut name = self.path.as_os_str().to_os_string();
            name.push(".citadel-keys");
            PathBuf::from(name)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn refuse_stale_sidecar(path: &std::path::Path, what: &str) -> Result<()> {
        if durable::path_entry_exists(path)? {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "{what} already exists at {}, but its database does not. It belongs to a \
                     database that was deleted without it; move it aside to create a new one here.",
                    path.display()
                ),
            )));
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_page_io(file: std::fs::File) -> Box<dyn PageIO> {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        {
            if let Some(uring) = citadel_io::uring_io::UringPageIO::try_new(
                file.try_clone().expect("failed to clone file handle"),
            ) {
                return Box::new(uring);
            }
        }
        Box::new(MmapPageIO::try_new(file).expect("mmap init failed"))
    }

    /// Reject a zero cache size up front: the buffer pool requires capacity
    /// >= 1 and would otherwise panic deep inside the transaction manager.
    fn validate_cache_size(&self) -> Result<()> {
        if self.cache_size == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cache_size must be at least 1 page",
            )));
        }
        Ok(())
    }

    /// Resolve KDF parameters: (m_cost, t_cost, p_cost) for Argon2id,
    /// or (iterations, 0, 0) for PBKDF2.
    fn resolve_kdf_params(&self) -> (u32, u32, u32) {
        match self.kdf_algorithm {
            KdfAlgorithm::Argon2id => {
                let profile = self.argon2_profile;
                (profile.m_cost(), profile.t_cost(), profile.p_cost())
            }
            KdfAlgorithm::Pbkdf2HmacSha256 => (self.pbkdf2_iterations, 0, 0),
        }
    }

    /// Validate configuration against FIPS constraints (when fips feature
    /// enabled).
    #[cfg(feature = "fips")]
    fn validate_fips(&self) -> Result<()> {
        if self.kdf_algorithm != KdfAlgorithm::Pbkdf2HmacSha256 {
            return Err(Error::FipsViolation(
                "FIPS mode requires PBKDF2-HMAC-SHA256 (Argon2id is not NIST approved)".into(),
            ));
        }
        if self.cipher == CipherId::ChaCha20 {
            return Err(Error::FipsViolation(
                "FIPS mode requires AES-256-CTR (ChaCha20 is not NIST approved)".into(),
            ));
        }
        Ok(())
    }

    /// Build a `Database` from a `TxnManager`, optionally creating or opening
    /// an audit log. Centralizes the audit-log feature gating.
    #[cfg(feature = "audit-log")]
    fn finish(
        self,
        manager: TxnManager,
        file: FileIdentity,
        audit_key: [u8; citadel_core::KEY_SIZE],
        region_keys: Option<RegionWrapKeys>,
        initial_event: Option<(crate::audit::AuditEventType, Vec<u8>)>,
    ) -> Result<Database> {
        use crate::audit;
        let FileIdentity {
            key_path,
            file_id,
            mut key_file,
        } = file;

        // Audit enforcement has its own authenticated marker. Slots may have
        // become V1 while audit logging was disabled; using the slot marker
        // would reject the still-valid legacy audit before the upgrade.
        let audit_v2_required = key_file.audit_v2_required();
        let mut audit_log = if self.audit_config.enabled && !self.path.as_os_str().is_empty() {
            let audit_path = audit::resolve_audit_path(&self.path);
            audit::recover_rotation(&audit_path, &audit_key)?;
            let log = if durable::path_entry_exists(&audit_path)? {
                audit::AuditLog::open_existing(
                    &audit_path,
                    file_id,
                    audit_key,
                    self.audit_config,
                    audit_v2_required,
                )?
            } else {
                let mut log = audit::AuditLog::create(
                    &audit_path,
                    file_id,
                    audit_key,
                    self.audit_config,
                    // A missing sidecar has no legacy history to preserve.
                    // Once the data slots are protected, create v2 directly
                    // instead of durably writing v1 only to rewrite it below.
                    audit_v2_required || manager.slots_flagged(),
                )?;
                log.discard_if_initialization_fails();
                log
            };
            Some(log)
        } else {
            None
        };

        // Heal each authenticated policy independently: a legacy audit upgrades
        // before its marker becomes durable, and a disabled audit leaves the
        // marker unset so enabling it later repairs the same way. All idempotent,
        // so an interrupted open resumes safely.
        if manager.slots_flagged() && !key_path.as_os_str().is_empty() {
            let upgrade_audit = audit_log.is_some() && !key_file.audit_v2_required();
            if upgrade_audit {
                audit_log.as_mut().expect("checked above").upgrade_to_v2()?;
            }

            let mut key_file_changed = false;
            if !key_file.slots_v1_required() {
                key_file.require_v1_slots();
                key_file_changed = true;
            }
            if upgrade_audit {
                key_file.require_audit_v2();
                key_file_changed = true;
            }
            if key_file_changed {
                atomic_write_for_operation(
                    &key_path,
                    &key_file.serialize(),
                    "authenticated format policy repair",
                )?;
            }
        }

        manager.set_secure_delete(self.secure_delete);
        let db = Database::new(
            manager,
            self.path,
            key_path,
            file_id,
            key_file,
            region_keys,
            audit_log,
        );

        if let Some((event, detail)) = initial_event {
            // Neither a failed create (rolled back by CreatedFileGuard) nor a
            // failed open completed from the caller's view, so this stays an
            // ordinary audit error rather than AuditFailureAfterOperation.
            db.log_audit(event, &detail)?;
            db.retain_created_audit_history();
        }

        Ok(db)
    }

    #[cfg(not(feature = "audit-log"))]
    fn finish(
        self,
        manager: TxnManager,
        file: FileIdentity,
        _audit_key: [u8; citadel_core::KEY_SIZE],
        region_keys: Option<RegionWrapKeys>,
        _initial_event: Option<((), Vec<u8>)>,
    ) -> Result<Database> {
        let FileIdentity {
            key_path,
            file_id,
            mut key_file,
        } = file;
        if manager.slots_flagged()
            && !key_file.slots_v1_required()
            && !key_path.as_os_str().is_empty()
        {
            key_file.require_v1_slots();
            #[cfg(not(target_arch = "wasm32"))]
            atomic_write_for_operation(
                &key_path,
                &key_file.serialize(),
                "authenticated format policy repair",
            )?;
        }
        manager.set_secure_delete(self.secure_delete);
        Ok(Database::new(
            manager,
            self.path,
            key_path,
            file_id,
            key_file,
            region_keys,
        ))
    }

    /// Create a new database. Fails if the data file already exists.
    ///
    /// The data file is created (`create_new`) before the key file, so a
    /// failed `create()` never clobbers an existing database's key file; a
    /// crash between the two just leaves an empty data file to remove.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create(self) -> Result<Database> {
        #[cfg(feature = "fips")]
        self.validate_fips()?;
        self.validate_cache_size()?;
        #[cfg(feature = "audit-log")]
        if self.audit_config.enabled {
            crate::audit::validate_audit_config(&self.audit_config)?;
        }

        let passphrase = self
            .passphrase
            .as_deref()
            .ok_or(Error::PassphraseRequired)?;

        let key_path = self.resolve_key_path();
        // Fail before creating the data file rather than leave a partial vault
        // when an orphaned sidecar is present.
        Self::refuse_stale_sidecar(&key_path, "key file")?;
        #[cfg(feature = "audit-log")]
        if !self.path.as_os_str().is_empty() {
            let audit_path = crate::audit::resolve_audit_path(&self.path);
            Self::refuse_stale_sidecar(&audit_path, "audit log")?;
            Self::refuse_stale_sidecar(
                &crate::audit::rotation_work_path(&audit_path),
                "audit rotation journal",
            )?;
            Self::refuse_stale_sidecar(
                &crate::audit::audit_upgrade_path(&audit_path),
                "audit upgrade image",
            )?;
            crate::audit::ensure_no_retained_audit_history(&audit_path)?;
        }

        let file_id: u64 = rand::random();

        let (kf, keys, region_keys) = self.create_keys(passphrase, file_id)?;
        let key_file_auth = KeyFileAuthKey::from_database_mac_key(&keys.mac_key);

        let mut created = CreatedFileGuard::new();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&self.path)?;
        created.track(&self.path);

        file_lock::try_lock_exclusive(&file)?;
        durable::fsync_directory(&self.path)?;
        durable::write_new_and_sync(&key_path, &kf.serialize())?;
        created.track(&key_path);

        let dek_id = compute_dek_id(&keys.mac_key, &keys.dek);
        let io = Self::create_page_io(file);

        let manager = TxnManager::create_with_sync(
            io,
            keys.dek,
            keys.mac_key,
            kf.current_epoch,
            file_id,
            dek_id,
            self.cache_size,
            self.sync_mode,
        )?;

        #[cfg(feature = "audit-log")]
        let event = {
            let detail = vec![self.cipher as u8, self.kdf_algorithm as u8];
            Some((crate::audit::AuditEventType::DatabaseCreated, detail))
        };
        #[cfg(not(feature = "audit-log"))]
        let event: Option<((), Vec<u8>)> = None;

        let result = self.finish(
            manager,
            FileIdentity {
                key_path,
                file_id,
                key_file: KeyFileState::new(kf, key_file_auth),
            },
            keys.audit_key,
            region_keys,
            event,
        );
        if result.is_ok() {
            created.disarm();
        }
        result
    }

    /// Create a new in-memory database (volatile, no file I/O).
    ///
    /// Data exists only for the lifetime of the returned `Database`.
    /// Useful for testing, caching, and WASM environments.
    pub fn create_in_memory(mut self) -> Result<Database> {
        #[cfg(feature = "fips")]
        self.validate_fips()?;
        self.validate_cache_size()?;

        // Per-region cryptographic erasure needs a durable overwrite-in-place
        // sidecar, which an in-memory database cannot provide; reject the
        // combination up front.
        if self.enable_region_keys {
            return Err(Error::RegionKeysRequireFile);
        }

        let passphrase = self
            .passphrase
            .as_deref()
            .ok_or(Error::PassphraseRequired)?;

        let file_id: u64 = rand::random();

        let (kf, keys, region_keys) = self.create_keys(passphrase, file_id)?;
        let key_file_auth = KeyFileAuthKey::from_database_mac_key(&keys.mac_key);

        let dek_id = compute_dek_id(&keys.mac_key, &keys.dek);
        let io: Box<dyn PageIO> = Box::new(citadel_io::memory_io::MemoryPageIO::new());

        let manager = TxnManager::create_with_sync(
            io,
            keys.dek,
            keys.mac_key,
            1,
            file_id,
            dek_id,
            self.cache_size,
            self.sync_mode,
        )?;

        // Clear path so finish() won't create an audit log file on disk
        self.path = PathBuf::new();
        self.finish(
            manager,
            FileIdentity {
                key_path: PathBuf::new(),
                file_id,
                key_file: KeyFileState::new(kf, key_file_auth),
            },
            keys.audit_key,
            region_keys,
            None,
        )
    }

    /// Open an existing database. Fails if the data file does not exist.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(self) -> Result<Database> {
        self.validate_cache_size()?;
        #[cfg(feature = "audit-log")]
        if self.audit_config.enabled {
            crate::audit::validate_audit_config(&self.audit_config)?;
        }

        let passphrase = self
            .passphrase
            .as_deref()
            .ok_or(Error::PassphraseRequired)?;

        let key_path = self.resolve_key_path();

        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;

        file_lock::try_lock_exclusive(&file)?;

        let mut header_buf = [0u8; FILE_HEADER_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header_buf)?;
        let header = FileHeader::deserialize(&header_buf)?;

        let (key_buf, _) = durable::read_regular_file_exact::<KEY_FILE_SIZE>(&key_path)?;
        let (kf, keys, region_keys) = self.open_keys(&key_buf, passphrase, header.file_id)?;
        let key_file_auth = KeyFileAuthKey::from_database_mac_key(&keys.mac_key);

        let dek_id = compute_dek_id(&keys.mac_key, &keys.dek);

        let io = Self::create_page_io(file);

        let manager = TxnManager::open_with_sync_and_v1_requirement(
            io,
            keys.dek,
            keys.mac_key,
            kf.current_epoch,
            self.cache_size,
            self.sync_mode,
            kf.slots_v1_required(),
        )?;

        let slot = manager.current_slot();
        if slot.dek_id != dek_id {
            return Err(Error::BadPassphrase);
        }

        #[cfg(feature = "audit-log")]
        let event = Some((crate::audit::AuditEventType::DatabaseOpened, vec![]));
        #[cfg(not(feature = "audit-log"))]
        let event: Option<((), Vec<u8>)> = None;

        self.finish(
            manager,
            FileIdentity {
                key_path,
                file_id: header.file_id,
                key_file: KeyFileState::new(kf, key_file_auth),
            },
            keys.audit_key,
            region_keys,
            event,
        )
    }

    /// Create a key file, deriving region wrap keys only when
    /// `enable_region_keys` is set. Returns the wrap keys to retain (`Some`) or
    /// `None` so the plaintext path holds no region key material.
    #[allow(clippy::type_complexity)]
    fn create_keys(
        &self,
        passphrase: &[u8],
        file_id: u64,
    ) -> Result<(
        citadel_crypto::key_manager::KeyFile,
        citadel_crypto::hkdf_utils::DerivedKeys,
        Option<RegionWrapKeys>,
    )> {
        let (m_cost, t_cost, p_cost) = self.resolve_kdf_params();
        if self.enable_region_keys {
            let (kf, keys, region) = create_key_file_with_region_keys(
                passphrase,
                file_id,
                self.cipher,
                self.kdf_algorithm,
                m_cost,
                t_cost,
                p_cost,
            )?;
            Ok((kf, keys, Some(region)))
        } else {
            let (kf, keys) = create_key_file(
                passphrase,
                file_id,
                self.cipher,
                self.kdf_algorithm,
                m_cost,
                t_cost,
                p_cost,
            )?;
            Ok((kf, keys, None))
        }
    }

    /// Open a key file, deriving region wrap keys only when
    /// `enable_region_keys`.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::type_complexity)]
    fn open_keys(
        &self,
        key_buf: &[u8; KEY_FILE_SIZE],
        passphrase: &[u8],
        expected_file_id: u64,
    ) -> Result<(
        citadel_crypto::key_manager::KeyFile,
        citadel_crypto::hkdf_utils::DerivedKeys,
        Option<RegionWrapKeys>,
    )> {
        if self.enable_region_keys {
            let (kf, keys, region) =
                open_key_file_with_region_keys(key_buf, passphrase, expected_file_id)?;
            Ok((kf, keys, Some(region)))
        } else {
            let (kf, keys) = open_key_file(key_buf, passphrase, expected_file_id)?;
            Ok((kf, keys, None))
        }
    }
}

#[cfg(test)]
#[path = "builder_tests.rs"]
mod tests;
