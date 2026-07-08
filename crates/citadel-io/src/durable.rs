//! Durable file write utilities.
//!
//! Provides helpers for crash-safe file operations: atomic writes via
//! temp-file-then-rename with proper fsync ordering.

#[cfg(unix)]
use std::fs::File;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

/// Write data durably: temp file + atomic rename + directory fsync.
/// Crash-safe: always leaves either old or new file, never partial.
pub fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let temp_path = path.with_extension("tmp");
    // `write_synced` closes before rename; Windows refuses to rename an open
    // file.
    write_synced(&temp_path, data)?;
    fs::rename(&temp_path, path)?;
    fsync_directory(path)?;
    Ok(())
}

/// Write data and fsync; for new files (no rename protection needed).
pub fn write_and_sync(path: &Path, data: &[u8]) -> std::io::Result<()> {
    write_synced(path, data)?;
    fsync_directory(path)?;
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
    let mut from = std::fs::File::open(src)?;
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
