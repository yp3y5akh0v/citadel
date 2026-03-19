use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use citadel_core::types::{Argon2Profile, CipherId, KdfAlgorithm};
use citadel_core::{
    Error, Result, DEFAULT_BUFFER_POOL_SIZE, FILE_HEADER_SIZE, KEY_FILE_SIZE, PBKDF2_MIN_ITERATIONS,
};
use citadel_crypto::key_manager::{create_key_file, open_key_file};
use citadel_crypto::page_cipher::compute_dek_id;
use citadel_io::durable;
use citadel_io::file_lock;
use citadel_io::file_manager::FileHeader;
use citadel_io::sync_io::SyncPageIO;
use citadel_io::traits::PageIO;
use citadel_txn::manager::TxnManager;

use crate::database::Database;

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
pub struct DatabaseBuilder {
    path: PathBuf,
    key_path: Option<PathBuf>,
    passphrase: Option<Vec<u8>>,
    argon2_profile: Argon2Profile,
    cache_size: usize,
    cipher: CipherId,
    kdf_algorithm: KdfAlgorithm,
    pbkdf2_iterations: u32,
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
            #[cfg(feature = "audit-log")]
            audit_config: crate::audit::AuditConfig::default(),
        }
    }

    pub fn passphrase(mut self, passphrase: &[u8]) -> Self {
        self.passphrase = Some(passphrase.to_vec());
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

    /// Configure the audit log.
    ///
    /// Default: enabled with 10 MB max file size and 3 rotated files.
    #[cfg(feature = "audit-log")]
    pub fn audit_config(mut self, config: crate::audit::AuditConfig) -> Self {
        self.audit_config = config;
        self
    }

    /// Default key file path: `{data_path}.citadel-keys`
    fn resolve_key_path(&self) -> PathBuf {
        self.key_path.clone().unwrap_or_else(|| {
            let mut name = self.path.as_os_str().to_os_string();
            name.push(".citadel-keys");
            PathBuf::from(name)
        })
    }

    fn create_page_io(file: std::fs::File) -> Box<dyn PageIO> {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        {
            if let Some(uring) = citadel_io::uring_io::UringPageIO::try_new(
                file.try_clone().expect("failed to clone file handle"),
            ) {
                return Box::new(uring);
            }
        }
        Box::new(SyncPageIO::new(file))
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

    /// Validate configuration against FIPS constraints (when fips feature enabled).
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
        key_path: PathBuf,
        file_id: u64,
        audit_key: [u8; citadel_core::KEY_SIZE],
        initial_event: Option<(crate::audit::AuditEventType, Vec<u8>)>,
    ) -> Result<Database> {
        use crate::audit;

        let audit_log = if self.audit_config.enabled && !self.path.as_os_str().is_empty() {
            let audit_path = audit::resolve_audit_path(&self.path);
            let log = if audit_path.exists() {
                audit::AuditLog::open_existing(&audit_path, file_id, audit_key, self.audit_config)?
            } else {
                audit::AuditLog::create(&audit_path, file_id, audit_key, self.audit_config)?
            };
            Some(log)
        } else {
            None
        };

        let db = Database::new(manager, self.path, key_path, audit_log);

        if let Some((event, detail)) = initial_event {
            db.log_audit(event, &detail);
        }

        Ok(db)
    }

    #[cfg(not(feature = "audit-log"))]
    fn finish(
        self,
        manager: TxnManager,
        key_path: PathBuf,
        _file_id: u64,
        _audit_key: [u8; citadel_core::KEY_SIZE],
        _initial_event: Option<((), Vec<u8>)>,
    ) -> Result<Database> {
        Ok(Database::new(manager, self.path, key_path))
    }

    /// Create a new database. Fails if the data file already exists.
    pub fn create(self) -> Result<Database> {
        #[cfg(feature = "fips")]
        self.validate_fips()?;

        let passphrase = self
            .passphrase
            .as_deref()
            .ok_or(Error::PassphraseRequired)?;

        let key_path = self.resolve_key_path();
        let file_id: u64 = rand::random();
        let (m_cost, t_cost, p_cost) = self.resolve_kdf_params();

        let (kf, keys) = create_key_file(
            passphrase,
            file_id,
            self.cipher,
            self.kdf_algorithm,
            m_cost,
            t_cost,
            p_cost,
        )?;

        // Write key file to disk with fsync + directory sync
        durable::write_and_sync(&key_path, &kf.serialize())?;

        // Create data file (fail if exists)
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&self.path)?;

        file_lock::try_lock_exclusive(&file)?;

        let dek_id = compute_dek_id(&keys.mac_key, &keys.dek);
        let io = Self::create_page_io(file);

        let manager = TxnManager::create(
            io,
            keys.dek,
            keys.mac_key,
            kf.current_epoch,
            file_id,
            dek_id,
            self.cache_size,
        )?;

        #[cfg(feature = "audit-log")]
        let event = {
            let detail = vec![self.cipher as u8, self.kdf_algorithm as u8];
            Some((crate::audit::AuditEventType::DatabaseCreated, detail))
        };
        #[cfg(not(feature = "audit-log"))]
        let event: Option<((), Vec<u8>)> = None;

        self.finish(manager, key_path, file_id, keys.audit_key, event)
    }

    /// Create a new in-memory database (volatile, no file I/O).
    ///
    /// Data exists only for the lifetime of the returned `Database`.
    /// Useful for testing, caching, and WASM environments.
    pub fn create_in_memory(mut self) -> Result<Database> {
        #[cfg(feature = "fips")]
        self.validate_fips()?;

        let passphrase = self
            .passphrase
            .as_deref()
            .ok_or(Error::PassphraseRequired)?;

        let file_id: u64 = rand::random();
        let (m_cost, t_cost, p_cost) = self.resolve_kdf_params();

        let (_kf, keys) = create_key_file(
            passphrase,
            file_id,
            self.cipher,
            self.kdf_algorithm,
            m_cost,
            t_cost,
            p_cost,
        )?;

        let dek_id = compute_dek_id(&keys.mac_key, &keys.dek);
        let io: Box<dyn PageIO> = Box::new(citadel_io::memory_io::MemoryPageIO::new());

        let manager = TxnManager::create(
            io,
            keys.dek,
            keys.mac_key,
            1,
            file_id,
            dek_id,
            self.cache_size,
        )?;

        // Clear path so finish() won't create an audit log file on disk
        self.path = PathBuf::new();
        self.finish(manager, PathBuf::new(), file_id, keys.audit_key, None)
    }

    /// Open an existing database. Fails if the data file does not exist.
    pub fn open(self) -> Result<Database> {
        let passphrase = self
            .passphrase
            .as_deref()
            .ok_or(Error::PassphraseRequired)?;

        let key_path = self.resolve_key_path();

        // Open data file
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;

        file_lock::try_lock_exclusive(&file)?;

        // Read file header to get file_id
        let mut header_buf = [0u8; FILE_HEADER_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header_buf)?;
        let header = FileHeader::deserialize(&header_buf)?;

        // Read and validate key file
        let key_data = fs::read(&key_path)?;
        if key_data.len() != KEY_FILE_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "key file has incorrect size",
            )));
        }
        let key_buf: [u8; KEY_FILE_SIZE] = key_data.try_into().unwrap();
        let (kf, keys) = open_key_file(&key_buf, passphrase, header.file_id)?;

        let dek_id = compute_dek_id(&keys.mac_key, &keys.dek);

        let io = Self::create_page_io(file);

        let manager = TxnManager::open(
            io,
            keys.dek,
            keys.mac_key,
            kf.current_epoch,
            self.cache_size,
        )?;

        // Verify dek_id against the recovered commit slot
        let slot = manager.current_slot();
        if slot.dek_id != dek_id {
            return Err(Error::BadPassphrase);
        }

        #[cfg(feature = "audit-log")]
        let event = Some((crate::audit::AuditEventType::DatabaseOpened, vec![]));
        #[cfg(not(feature = "audit-log"))]
        let event: Option<((), Vec<u8>)> = None;

        self.finish(manager, key_path, header.file_id, keys.audit_key, event)
    }
}
