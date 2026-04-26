use super::*;

fn create_test_file() -> (tempfile::TempDir, File) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    (dir, file)
}

#[test]
fn try_new_succeeds() {
    let (_dir, file) = create_test_file();
    let io = UringPageIO::try_new(file);
    assert!(io.is_some(), "io_uring should be available on this kernel");
}

#[test]
fn read_write_page_roundtrip() {
    let (_dir, file) = create_test_file();
    let io = UringPageIO::try_new(file).unwrap();

    let mut page = [0u8; PAGE_SIZE];
    page[0] = 0xAA;
    page[PAGE_SIZE - 1] = 0xBB;

    io.truncate(PAGE_SIZE as u64).unwrap();
    io.write_page(0, &page).unwrap();

    let mut read_buf = [0u8; PAGE_SIZE];
    io.read_page(0, &mut read_buf).unwrap();
    assert_eq!(read_buf, page);
}

#[test]
fn read_write_at() {
    let (_dir, file) = create_test_file();
    let io = UringPageIO::try_new(file).unwrap();

    let header = [0x42u8; 512];
    io.truncate(512).unwrap();
    io.write_at(0, &header).unwrap();

    let mut read_buf = [0u8; 512];
    io.read_at(0, &mut read_buf).unwrap();
    assert_eq!(read_buf, header);
}

#[test]
fn file_size_and_truncate() {
    let (_dir, file) = create_test_file();
    let io = UringPageIO::try_new(file).unwrap();

    assert_eq!(io.file_size().unwrap(), 0);
    io.truncate(PAGE_SIZE as u64).unwrap();
    assert_eq!(io.file_size().unwrap(), PAGE_SIZE as u64);
}

#[test]
fn flush_pages_batch() {
    let (_dir, file) = create_test_file();
    let io = UringPageIO::try_new(file).unwrap();

    let mut pages = Vec::new();
    for i in 0..10u8 {
        let offset = i as u64 * PAGE_SIZE as u64;
        let mut page = [0u8; PAGE_SIZE];
        page[0] = i;
        page[PAGE_SIZE - 1] = 0xFF - i;
        pages.push((offset, page));
    }

    io.flush_pages(&pages).unwrap();

    for (offset, expected) in &pages {
        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(*offset, &mut buf).unwrap();
        assert_eq!(&buf[..], &expected[..]);
    }
}

#[test]
fn flush_pages_empty() {
    let (_dir, file) = create_test_file();
    let io = UringPageIO::try_new(file).unwrap();
    io.flush_pages(&[]).unwrap();
}

#[test]
fn fsync_works() {
    let (_dir, file) = create_test_file();
    let io = UringPageIO::try_new(file).unwrap();
    io.truncate(PAGE_SIZE as u64).unwrap();

    let page = [0xAB; PAGE_SIZE];
    io.write_page(0, &page).unwrap();
    io.fsync().unwrap();
}
