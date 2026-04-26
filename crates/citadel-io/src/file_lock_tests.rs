use super::*;
use std::fs::File;

#[test]
fn lock_and_unlock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock_test.db");
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .unwrap();

    try_lock_exclusive(&file).unwrap();
    unlock(&file).unwrap();
}

#[test]
fn double_lock_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock_test2.db");
    let file1 = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .unwrap();
    let file2 = File::options().read(true).write(true).open(&path).unwrap();

    try_lock_exclusive(&file1).unwrap();

    let result = try_lock_exclusive(&file2);
    assert!(matches!(result, Err(citadel_core::Error::DatabaseLocked)));

    unlock(&file1).unwrap();
}
