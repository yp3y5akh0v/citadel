//! Durable file write utilities.
//!
//! Provides helpers for crash-safe file operations: atomic writes via
//! temp-file-then-rename with proper fsync ordering.

#[cfg(unix)]
use std::fs::File;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

/// Write data to a file durably using temp-file + rename.
///
/// Sequence:
/// 1. Write data to `{path}.tmp`
/// 2. fsync the temp file (data is on disk)
/// 3. Rename temp file to final path (atomic on POSIX)
/// 4. fsync the parent directory (rename is durable)
///
/// On crash at any point, either the old file or the new file is present,
/// never a partial write.
pub fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let temp_path = path.with_extension("tmp");

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)?;

    file.write_all(data)?;
    file.sync_data()?;
    drop(file);

    fs::rename(&temp_path, path)?;

    fsync_directory(path)?;

    Ok(())
}

/// Write data to a file and fsync it (no atomic rename).
///
/// Use this for initial file creation where there is no previous
/// version to protect. Fsyncs both the file and the parent directory.
pub fn write_and_sync(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;

    file.write_all(data)?;
    file.sync_data()?;
    drop(file);

    fsync_directory(path)?;

    Ok(())
}

/// Fsync a file's parent directory to make directory entries durable.
///
/// On Linux, directory entries (file creation, rename) are not durable until
/// the directory inode is fsynced. On macOS (F_FULLFSYNC) and Windows
/// (FlushFileBuffers), this is a no-op because the OS flushes directory
/// metadata as part of file sync.
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

#[cfg(test)]
#[path = "durable_tests.rs"]
mod tests;
