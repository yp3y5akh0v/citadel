//! Unauthenticated vault metadata that can be read before asking for a
//! passphrase.
//!
//! These values are useful diagnostics, not proof: opening the vault verifies
//! keyed commit-slot and key-file MACs and may recover a different slot.

use std::io::Read;
use std::path::{Path, PathBuf};

use citadel_core::types::{CipherId, KdfAlgorithm};
use citadel_core::{Error, Result, FILE_HEADER_SIZE, KEY_FILE_SIZE};
use citadel_crypto::key_manager::KeyFile;
use citadel_io::durable;
use citadel_io::file_manager::FileHeader;

use crate::database::DbStats;

/// `{data_path}.citadel-keys`, the key file a vault uses unless told otherwise.
pub fn default_key_path(data_path: &Path) -> PathBuf {
    let mut name = data_path.as_os_str().to_os_string();
    name.push(".citadel-keys");
    PathBuf::from(name)
}

/// Parsed, non-secret key-file metadata.
///
/// [`Database::key_file`](crate::Database::key_file) returns an authenticated
/// instance of the same type.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct KeyFileInfo {
    pub cipher: CipherId,
    /// The serialized file carries ID 1 from the removed selector; its pages
    /// were nevertheless written with AES-256-CTR. Inspection does not verify
    /// the key-file MAC; [`Database::key_file`](crate::Database::key_file) does.
    pub legacy_cipher_encoding: bool,
    pub kdf: KdfAlgorithm,
    /// Argon2 memory cost in KiB, or the PBKDF2 iteration count.
    pub kdf_m_cost: u32,
    pub kdf_t_cost: u32,
    pub kdf_p_cost: u32,
    pub epoch: u32,
    /// A key rotation was interrupted and is still in progress.
    pub rotation_active: bool,
    pub slots_v1_required: bool,
    pub audit_v2_required: bool,
}

impl KeyFileInfo {
    pub(crate) fn from_key_file(kf: &citadel_crypto::key_manager::KeyFile) -> Self {
        Self {
            cipher: kf.cipher_id(),
            legacy_cipher_encoding: kf.has_legacy_cipher_encoding(),
            kdf: kf.kdf_algorithm,
            kdf_m_cost: kf.argon2_m_cost,
            kdf_t_cost: kf.argon2_t_cost,
            kdf_p_cost: kf.argon2_p_cost,
            epoch: kf.current_epoch,
            rotation_active: kf.rotation_active,
            slots_v1_required: kf.slots_v1_required(),
            audit_v2_required: kf.audit_v2_required(),
        }
    }
}

/// Result of inspecting the key-file path without verifying its MAC.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeyFileStatus {
    Missing,
    /// The entry could not be read as a fixed-size regular file.
    Unreadable(String),
    /// The bytes were readable but are not a supported Citadel key file.
    Invalid(String),
    Present {
        info: KeyFileInfo,
        file_id_matches: bool,
    },
}

impl KeyFileStatus {
    pub fn info(&self) -> Option<&KeyFileInfo> {
        match self {
            Self::Present { info, .. } => Some(info),
            _ => None,
        }
    }

    pub fn file_id_matches(&self) -> Option<bool> {
        match self {
            Self::Present {
                file_id_matches, ..
            } => Some(*file_id_matches),
            _ => None,
        }
    }
}

/// Header claims read before the vault is authenticated.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct VaultInfo {
    pub data_path: PathBuf,
    pub key_path: PathBuf,
    pub format_version: u32,
    pub page_size: u32,
    /// Which commit slot the unauthenticated god byte points at.
    ///
    /// A successful open can select the other slot if this one fails its MAC.
    pub active_slot: usize,
    /// Whether the selected slot passes its keyless checksum.
    ///
    /// This detects common corruption but is not an authenticity check.
    pub active_slot_checksum_valid: bool,
    pub recovery_required: bool,
    /// Values claimed by the god-byte-selected slot.
    ///
    /// [`crate::Database::stats`] can differ after MAC verification and
    /// recovery.
    pub stats: DbStats,
    pub key_file: KeyFileStatus,
}

/// Read a vault's headers using the key file at its default path.
pub fn inspect_vault(data_path: &Path) -> Result<VaultInfo> {
    inspect_vault_with_key(data_path, &default_key_path(data_path))
}

/// Read a vault's headers, taking the key file from `key_path`.
///
/// Data-file failures are returned. Key-file absence, I/O failure, and invalid
/// structure remain distinct in [`VaultInfo::key_file`] so the data header can
/// still be reported without hiding why the key file was unavailable.
pub fn inspect_vault_with_key(data_path: &Path, key_path: &Path) -> Result<VaultInfo> {
    inspect_vault_with_key_reader(data_path, key_path, inspect_key_file)
}

fn inspect_vault_with_key_reader(
    data_path: &Path,
    key_path: &Path,
    read_key_file: impl FnOnce(&Path, u64) -> KeyFileStatus,
) -> Result<VaultInfo> {
    // Keep the data-file lock until the key-file metadata has been captured.
    // A cooperating vault process therefore cannot rotate the key between the
    // two reads and make one report combine state from different moments.
    let (_data_file, header) = read_locked_header(data_path)?;
    let slot = &header.slots[header.active_slot()];
    let key_file = read_key_file(key_path, header.file_id);

    Ok(VaultInfo {
        data_path: data_path.to_path_buf(),
        key_path: key_path.to_path_buf(),
        format_version: header.format_version,
        page_size: header.page_size,
        active_slot: header.active_slot(),
        active_slot_checksum_valid: slot.verify_checksum(),
        recovery_required: header.recovery_required(),
        stats: DbStats {
            tree_depth: slot.tree_depth,
            entry_count: slot.tree_entries,
            total_pages: slot.total_pages,
            high_water_mark: slot.high_water_mark,
            merkle_root: slot.merkle_root,
        },
        key_file,
    })
}

fn read_locked_header(data_path: &Path) -> Result<(std::fs::File, FileHeader)> {
    let mut file = durable::open_regular_read(data_path)?;
    citadel_io::file_lock::try_lock_exclusive(&file)?;
    if file.metadata()?.len() < FILE_HEADER_SIZE as u64 {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "file is too short to hold a Citadel header",
        )));
    }
    let mut buf = [0u8; FILE_HEADER_SIZE];
    file.read_exact(&mut buf)?;
    let header = FileHeader::deserialize(&buf)?;
    Ok((file, header))
}

fn inspect_key_file(key_path: &Path, data_file_id: u64) -> KeyFileStatus {
    let buf = match durable::read_regular_file_exact::<KEY_FILE_SIZE>(key_path) {
        Ok((buf, _)) => buf,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return KeyFileStatus::Missing;
        }
        Err(error) => return KeyFileStatus::Unreadable(error.to_string()),
    };
    match KeyFile::deserialize(&buf) {
        Ok(key_file) => KeyFileStatus::Present {
            file_id_matches: key_file.file_id == data_file_id,
            info: KeyFileInfo::from_key_file(&key_file),
        },
        Err(error) => KeyFileStatus::Invalid(error.to_string()),
    }
}

#[cfg(test)]
#[path = "inspect_tests.rs"]
mod tests;
