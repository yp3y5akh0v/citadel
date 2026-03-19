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
mod tests {
    use super::*;

    #[test]
    fn atomic_write_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        atomic_write(&path, b"hello world").unwrap();

        let data = fs::read(&path).unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn atomic_write_replaces_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        fs::write(&path, b"old data").unwrap();
        atomic_write(&path, b"new data").unwrap();

        let data = fs::read(&path).unwrap();
        assert_eq!(data, b"new data");
    }

    #[test]
    fn atomic_write_no_temp_file_left() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let temp_path = path.with_extension("tmp");

        atomic_write(&path, b"data").unwrap();

        assert!(!temp_path.exists());
    }

    #[test]
    fn write_and_sync_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        write_and_sync(&path, b"hello").unwrap();

        let data = fs::read(&path).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn atomic_write_empty_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        atomic_write(&path, b"").unwrap();

        let data = fs::read(&path).unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn atomic_write_large_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let large = vec![0xABu8; 1024 * 1024];
        atomic_write(&path, &large).unwrap();

        let data = fs::read(&path).unwrap();
        assert_eq!(data, large);
    }
}
