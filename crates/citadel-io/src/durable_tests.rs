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
