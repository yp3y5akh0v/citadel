use super::*;

#[cfg(unix)]
#[test]
fn path_entry_exists_counts_a_dangling_symlink() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let link = dir.path().join("reserved-sidecar");
    symlink(dir.path().join("missing-target"), &link).unwrap();

    assert!(path_entry_exists(&link).unwrap());
    assert!(!link.try_exists().unwrap());
}

#[cfg(unix)]
#[test]
fn regular_file_open_refuses_symlinks_and_fifos() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    let link = dir.path().join("link");
    let fifo = dir.path().join("fifo");
    fs::write(&target, b"data").unwrap();
    symlink(&target, &link).unwrap();
    let fifo_name = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo_name.as_ptr(), 0o600) }, 0);

    assert!(open_regular_read(&link).is_err());
    assert!(open_regular_read(&fifo).is_err());
    assert_eq!(read_regular_file(&target).unwrap().0, b"data");
    assert!(read_regular_file_exact::<4>(&link).is_err());
    assert!(read_regular_file_exact::<4>(&fifo).is_err());
}

#[test]
fn exact_regular_file_read_rejects_the_wrong_size_before_reading() {
    let dir = tempfile::tempdir().unwrap();
    let exact = dir.path().join("exact");
    let short = dir.path().join("short");
    let long = dir.path().join("long");
    fs::write(&exact, b"data").unwrap();
    fs::write(&short, b"dat").unwrap();
    fs::write(&long, b"data!").unwrap();

    assert_eq!(read_regular_file_exact::<4>(&exact).unwrap().0, *b"data");
    for path in [&short, &long] {
        let error = read_regular_file_exact::<4>(path).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}

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
fn atomic_write_does_not_touch_the_legacy_temp_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.dat");
    let temp_path = path.with_extension("tmp");
    fs::write(&temp_path, b"unrelated").unwrap();

    atomic_write(&path, b"data").unwrap();

    assert_eq!(fs::read(&temp_path).unwrap(), b"unrelated");
    let prefix = format!("{}.tmp.", path.file_name().unwrap().to_string_lossy());
    assert!(fs::read_dir(dir.path()).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(&prefix)));
}

#[test]
fn atomic_write_reports_a_failure_after_publication() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.dat");
    fs::write(&path, b"old").unwrap();

    let error = atomic_write_with_directory_sync(&path, b"new", |_| {
        Err(std::io::Error::other("injected directory sync failure"))
    })
    .unwrap_err();

    assert!(error.was_published());
    assert_eq!(fs::read(&path).unwrap(), b"new");
}

#[test]
fn atomic_write_preserves_the_destination_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.dat");
    fs::write(&path, b"old").unwrap();

    let error = atomic_write_with_callbacks(
        &path,
        b"new",
        || Err(std::io::Error::other("injected pre-publish failure")),
        |_| panic!("directory sync must not run before publication"),
    )
    .unwrap_err();

    assert!(!error.was_published());
    assert_eq!(fs::read(&path).unwrap(), b"old");
}

#[cfg(unix)]
#[test]
fn atomic_write_refuses_to_replace_a_symlink() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    let link = dir.path().join("link");
    fs::write(&target, b"keep me").unwrap();
    symlink(&target, &link).unwrap();

    let error = atomic_write_with_status(&link, b"replacement").unwrap_err();
    assert!(!error.was_published());
    assert_eq!(fs::read(&target).unwrap(), b"keep me");
    assert!(fs::symlink_metadata(&link)
        .unwrap()
        .file_type()
        .is_symlink());
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
fn write_new_and_sync_never_truncates_an_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing");
    std::fs::write(&path, b"keep me").unwrap();

    let error = write_new_and_sync(&path, b"replacement").unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&path).unwrap(), b"keep me");
}

#[test]
fn copy_new_and_sync_never_truncates_an_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let destination = dir.path().join("destination");
    std::fs::write(&source, b"new bytes").unwrap();
    std::fs::write(&destination, b"keep me").unwrap();

    let error = copy_new_and_sync(&source, &destination).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&destination).unwrap(), b"keep me");
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
