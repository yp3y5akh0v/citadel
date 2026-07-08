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

// Callers (database create/backup/compact) rely on this to persist new
// directory entries; pin that it stays public and succeeds on a real path.
#[test]
fn fsync_directory_succeeds_for_new_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.dat");

    fs::write(&path, b"data").unwrap();
    fsync_directory(&path).unwrap();
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

#[test]
fn copy_and_sync_replicates_content() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.dat");
    let dest = dir.path().join("dest.dat");
    fs::write(&src, b"key material").unwrap();

    copy_and_sync(&src, &dest).unwrap();

    assert_eq!(fs::read(&dest).unwrap(), b"key material");
}

#[test]
fn copy_and_sync_missing_source_errors() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("missing.dat");
    let dest = dir.path().join("dest.dat");

    assert!(copy_and_sync(&src, &dest).is_err());
    assert!(!dest.exists());
}

/// A chmod-protected (read-only) source must still back up - fs::copy would
/// clone the read-only bit onto the destination and fail the fsync reopen -
/// and the destination must end up with the source's restrictive
/// permissions, not umask defaults.
#[test]
fn copy_and_sync_copies_readonly_source_and_preserves_permissions() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.dat");
    let dest = dir.path().join("dest.dat");
    fs::write(&src, b"protected key material").unwrap();
    let orig = fs::metadata(&src).unwrap().permissions();
    let mut readonly = orig.clone();
    readonly.set_readonly(true);
    fs::set_permissions(&src, readonly).unwrap();

    copy_and_sync(&src, &dest).unwrap();

    assert_eq!(fs::read(&dest).unwrap(), b"protected key material");
    assert!(
        fs::metadata(&dest).unwrap().permissions().readonly(),
        "destination must inherit the source's restrictive permissions"
    );

    fs::set_permissions(&src, orig.clone()).unwrap();
    fs::set_permissions(&dest, orig).unwrap();
}

// The region-erasure guarantee rests on these primitives never truncating or
// growing the file and never disturbing surrounding bytes.

#[test]
fn overwrite_in_place_preserves_surrounding_bytes_and_length() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.dat");
    fs::write(&path, vec![0xAAu8; 100]).unwrap();

    overwrite_in_place(&path, 40, &[0xBB; 10]).unwrap();

    let data = fs::read(&path).unwrap();
    assert_eq!(data.len(), 100, "no truncation or growth");
    assert!(
        data[0..40].iter().all(|&b| b == 0xAA),
        "bytes before untouched"
    );
    assert_eq!(&data[40..50], &[0xBB; 10], "target range overwritten");
    assert!(
        data[50..100].iter().all(|&b| b == 0xAA),
        "bytes after untouched"
    );
}

#[test]
fn overwrite_in_place_on_missing_file_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing.dat");
    // Uses .open() not .create(), so a missing file must error (never silently
    // create).
    assert!(overwrite_in_place(&path, 0, &[1, 2, 3]).is_err());
}

#[test]
fn append_and_sync_only_extends() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.dat");
    fs::write(&path, [1u8, 2, 3, 4, 5, 6, 7, 8]).unwrap();

    append_and_sync(&path, &[9, 10, 11, 12, 13, 14, 15, 16]).unwrap();

    let data = fs::read(&path).unwrap();
    assert_eq!(data.len(), 16);
    assert_eq!(
        &data[0..8],
        &[1, 2, 3, 4, 5, 6, 7, 8],
        "existing bytes unchanged"
    );
    assert_eq!(
        &data[8..16],
        &[9, 10, 11, 12, 13, 14, 15, 16],
        "appended at end"
    );
}

#[test]
fn truncate_and_sync_removes_partial_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.dat");
    fs::write(&path, vec![0xCDu8; 100]).unwrap();

    truncate_and_sync(&path, 64).unwrap();

    let data = fs::read(&path).unwrap();
    assert_eq!(data.len(), 64);
    assert!(data.iter().all(|&b| b == 0xCD), "kept bytes unchanged");
}
