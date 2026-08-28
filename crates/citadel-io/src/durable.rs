//! Durable file write utilities.
//!
//! Provides helpers for crash-safe file operations: atomic writes via
//! temp-file-then-rename with proper fsync ordering.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ATOMIC_TEMP_ID: AtomicU64 = AtomicU64::new(0);

/// Whether any directory entry occupies `path`, without following a symlink in
/// the final path component.
///
/// `Path::try_exists` follows them and reports a dangling symlink as absent, so
/// a caller reserving a sidecar name would let a later `create_new` fail after
/// the main operation already succeeded.
pub fn path_entry_exists(path: &Path) -> std::io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn open_regular(path: &Path, write: bool) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(write);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }

    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{} is not a regular file", path.display()),
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{} is a reparse point, not a regular file", path.display()),
            ));
        }
    }
    Ok(file)
}

/// Open an existing regular file for reading without following a symlink or
/// reparse point in the final path component.
pub fn open_regular_read(path: &Path) -> std::io::Result<File> {
    open_regular(path, false)
}

/// Open an existing regular file for reading and writing without following a
/// symlink or reparse point in the final path component.
pub fn open_regular_read_write(path: &Path) -> std::io::Result<File> {
    open_regular(path, true)
}

/// Read an existing regular file without following a symlink or reparse point
/// in the final path component or blocking on a FIFO. The returned metadata
/// belongs to the same handle that supplied the bytes.
pub fn read_regular_file(path: &Path) -> std::io::Result<(Vec<u8>, fs::Metadata)> {
    let mut file = open_regular_read(path)?;
    let metadata = file.metadata()?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok((bytes, metadata))
}

/// Read a fixed-size regular file without following a symlink or reparse point
/// in the final path component or allocating from an untrusted on-disk length.
/// The returned metadata belongs to the same handle that supplied the bytes.
pub fn read_regular_file_exact<const N: usize>(
    path: &Path,
) -> std::io::Result<([u8; N], fs::Metadata)> {
    let mut file = open_regular_read(path)?;
    let metadata = file.metadata()?;
    if metadata.len() != N as u64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "{} has incorrect size: expected {N} bytes, found {}",
                path.display(),
                metadata.len()
            ),
        ));
    }

    let mut bytes = [0u8; N];
    file.read_exact(&mut bytes)?;
    let mut extra = [0u8; 1];
    if file.read(&mut extra)? != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{} changed size while it was being read", path.display()),
        ));
    }
    Ok((bytes, metadata))
}

/// Failure from an atomic replacement, including whether the new name was
/// already published before durability confirmation failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum AtomicWriteError {
    /// The destination name still refers to its previous image.
    NotPublished(std::io::Error),
    /// The destination name refers to the replacement, but syncing its parent
    /// directory failed, so crash durability is uncertain.
    Published(std::io::Error),
}

impl AtomicWriteError {
    /// Whether the replacement name was published before the failure.
    pub fn was_published(&self) -> bool {
        matches!(self, Self::Published(_))
    }

    /// Consume the publication context and return the underlying I/O error.
    pub fn into_inner(self) -> std::io::Error {
        match self {
            Self::NotPublished(source) | Self::Published(source) => source,
        }
    }
}

impl std::fmt::Display for AtomicWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPublished(source) | Self::Published(source) => source.fmt(f),
        }
    }
}

impl std::error::Error for AtomicWriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotPublished(source) | Self::Published(source) => Some(source),
        }
    }
}

/// Write data durably: temp file + atomic rename + directory fsync.
/// Crash-safe: always leaves either old or new file, never partial.
pub fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    atomic_write_with_status(path, data).map_err(AtomicWriteError::into_inner)
}

/// Atomic replacement with a machine-readable post-publish failure state.
pub fn atomic_write_with_status(path: &Path, data: &[u8]) -> Result<(), AtomicWriteError> {
    atomic_write_with_directory_sync(path, data, fsync_directory)
}

fn atomic_write_with_directory_sync(
    path: &Path,
    data: &[u8],
    sync_directory: impl FnOnce(&Path) -> std::io::Result<()>,
) -> Result<(), AtomicWriteError> {
    atomic_write_with_callbacks(path, data, || Ok(()), sync_directory)
}

fn atomic_write_with_callbacks(
    path: &Path,
    data: &[u8],
    before_publish: impl FnOnce() -> std::io::Result<()>,
    sync_directory: impl FnOnce(&Path) -> std::io::Result<()>,
) -> Result<(), AtomicWriteError> {
    let (temp_path, mut temp) = create_atomic_temp(path).map_err(AtomicWriteError::NotPublished)?;
    let permissions = match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            Some(metadata.permissions())
        }
        Ok(_) => {
            let error = std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{} is not a regular file", path.display()),
            );
            drop(temp);
            return Err(atomic_write_before_publish_failure(&temp_path, error));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            drop(temp);
            return Err(atomic_write_before_publish_failure(&temp_path, error));
        }
    };
    let write_result = (|| -> std::io::Result<()> {
        temp.write_all(data)?;
        if let Some(permissions) = permissions {
            temp.set_permissions(permissions)?;
        }
        temp.sync_all()
    })();
    drop(temp);
    if let Err(source) = write_result {
        return Err(atomic_write_before_publish_failure(&temp_path, source));
    }
    if let Err(source) = before_publish() {
        return Err(atomic_write_before_publish_failure(&temp_path, source));
    }
    if let Err(source) = fs::rename(&temp_path, path) {
        return Err(atomic_write_before_publish_failure(&temp_path, source));
    }
    sync_directory(path).map_err(AtomicWriteError::Published)
}

fn create_atomic_temp(path: &Path) -> std::io::Result<(PathBuf, std::fs::File)> {
    for _ in 0..1024 {
        let id = NEXT_ATOMIC_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let mut name = path.as_os_str().to_os_string();
        name.push(format!(".tmp.{}.{id}", std::process::id()));
        let candidate = PathBuf::from(name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not reserve a unique atomic-write temporary path",
    ))
}

fn atomic_write_before_publish_failure(path: &Path, primary: std::io::Error) -> AtomicWriteError {
    let source = match fs::remove_file(path) {
        Ok(()) => primary,
        Err(cleanup) if cleanup.kind() == std::io::ErrorKind::NotFound => primary,
        Err(cleanup) => std::io::Error::other(format!(
            "atomic replacement failed ({primary}); temporary-file cleanup also failed ({cleanup})"
        )),
    };
    AtomicWriteError::NotPublished(source)
}

/// Write data and fsync; for new files (no rename protection needed).
pub fn write_and_sync(path: &Path, data: &[u8]) -> std::io::Result<()> {
    write_synced(path, data)?;
    fsync_directory(path)?;
    Ok(())
}

/// Create a new file and write it durably without ever truncating an existing
/// path. The existence check and creation are one filesystem operation.
pub fn write_new_and_sync(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let result = file.write_all(data).and_then(|()| file.sync_data());
    drop(file);
    if let Err(error) = result {
        let _ = fs::remove_file(path);
        return Err(error);
    }
    if let Err(error) = fsync_directory(path) {
        let _ = fs::remove_file(path);
        let _ = fsync_directory(path);
        return Err(error);
    }
    Ok(())
}

/// Write `data`, fsync, and close, so callers can rename or sync the dir after.
fn write_synced(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.write_all(data)?;
    file.sync_data()
}

/// Fsync `file_path`'s parent directory to durably record a file
/// creation/rename (no-op on non-Unix, where directory handles can't be
/// fsynced and the platforms persist directory metadata themselves).
pub fn fsync_directory(file_path: &Path) -> std::io::Result<()> {
    let dir = file_path.parent().unwrap_or(Path::new("."));

    #[cfg(unix)]
    {
        let dir_file = File::open(dir)?;
        dir_file.sync_data()?;
    }

    #[cfg(not(unix))]
    {
        let _ = dir;
    }

    Ok(())
}

/// Copy `src` to `dest`, fsync the content, then apply the source's
/// permissions. Streams into a fresh writable file rather than `fs::copy`,
/// which clones the source mode up front - that both fails the fsync reopen
/// on a read-only key file and would leave a 0600 file world-readable under
/// the umask. Callers still fsync the destination directory per batch.
pub fn copy_and_sync(src: &Path, dest: &Path) -> std::io::Result<()> {
    let mut from = open_regular_read(src)?;
    let perms = from.metadata()?.permissions();
    let mut to = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dest)?;
    std::io::copy(&mut from, &mut to)?;
    to.sync_data()?;
    fs::set_permissions(dest, perms)
}

/// Copy into a newly created path without truncating an existing file.
pub fn copy_new_and_sync(src: &Path, dest: &Path) -> std::io::Result<()> {
    let mut from = open_regular_read(src)?;
    let perms = from.metadata()?.permissions();
    let mut to = OpenOptions::new().write(true).create_new(true).open(dest)?;
    let result = std::io::copy(&mut from, &mut to)
        .and_then(|_| to.set_permissions(perms))
        .and_then(|()| to.sync_all());
    drop(to);
    if let Err(error) = result {
        let _ = fs::remove_file(dest);
        return Err(error);
    }
    Ok(())
}

/// Overwrite `bytes` at `offset` in place in an existing file, then fsync.
///
/// Does NOT create, truncate, or rename; reuses the same byte range. Key-slot
/// crypto-erasure relies on this: no temp file or rename that would orphan a
/// prior copy of the destroyed bytes.
pub fn overwrite_in_place(path: &Path, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(bytes)?;
    file.sync_data()
}

/// Overwrite several fixed byte ranges of an existing file through one open
/// handle, then fsync once: one durability barrier for the batch, not one per
/// block. Durable on `Ok`.
pub fn write_blocks_synced<const N: usize>(
    path: &Path,
    blocks: &[(u64, [u8; N])],
) -> std::io::Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }
    let mut file = OpenOptions::new().write(true).open(path)?;
    for (offset, bytes) in blocks {
        file.seek(SeekFrom::Start(*offset))?;
        file.write_all(bytes)?;
    }
    file.sync_data()
}

/// Append bytes to an existing file and fsync (no truncate/rename).
pub fn append_and_sync(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new().append(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_data()
}

/// Truncate to `len` bytes and fsync (removes torn tail from crash recovery).
pub fn truncate_and_sync(path: &Path, len: u64) -> std::io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    file.sync_all()
}

#[cfg(test)]
#[path = "durable_tests.rs"]
mod tests;
