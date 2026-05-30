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
    // `write_synced` closes before rename; Windows refuses to rename an open file.
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

/// Fsync parent directory to durably record file creation/rename (Linux only).
fn fsync_directory(file_path: &Path) -> std::io::Result<()> {
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

/// Overwrite `bytes` at `offset` in an EXISTING file, in place, then fsync the data.
///
/// Does NOT create, truncate, or rename - it reuses the same physical byte range.
/// This is what cryptographic erasure of a key slot relies on: there is no temp file
/// or rename that would orphan a prior copy of the bytes being destroyed.
pub fn overwrite_in_place(path: &Path, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(bytes)?;
    file.sync_data()
}

/// Overwrite several fixed byte ranges of an EXISTING file through one open handle, then
/// fsync once: one durability barrier for the batch, not one per block. Durable on `Ok`.
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
