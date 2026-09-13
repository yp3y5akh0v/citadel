use super::*;

fn create_test_file() -> (tempfile::TempDir, File) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
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

fn expect_invalid_range(result: Result<()>) {
    assert!(matches!(result, Err(Error::Io(error))
        if error.kind() == io::ErrorKind::InvalidInput));
}

#[test]
fn kernel_range_conversion_checks_boundaries_without_large_allocations() {
    assert_eq!(checked_io_length(0, PAGE_SIZE).unwrap(), PAGE_SIZE as u32);
    assert_eq!(
        checked_offset_end(i64::MAX as u64, 0).unwrap(),
        i64::MAX as u64
    );
    assert!(checked_offset_end(i64::MAX as u64, 1).is_err());
    assert!(checked_offset_end(u64::MAX, 0).is_err());
    assert!(checked_offset_end(u64::MAX, PAGE_SIZE).is_err());
    assert_eq!(
        checked_page_batch_end([0, 7].into_iter()).unwrap(),
        PAGE_SIZE as u64 + 7
    );
    assert_eq!(checked_file_length(0).unwrap(), 0);
    assert!(checked_file_length(u64::MAX).is_err());
    #[cfg(target_pointer_width = "64")]
    assert!(checked_io_length(0, u32::MAX as usize + 1).is_err());
}

#[test]
fn invalid_kernel_ranges_do_not_use_the_shared_file_cursor_or_touch_buffers() {
    let (_dir, file) = create_test_file();
    let io = UringPageIO::try_new(file).unwrap();
    let original = [0x37; PAGE_SIZE];
    io.write_page(0, &original).unwrap();
    let size = io.file_size().unwrap();
    for offset in [u64::MAX, i64::MAX as u64] {
        let mut page = [0xa5; PAGE_SIZE];
        expect_invalid_range(io.read_page(offset, &mut page));
        assert_eq!(page, [0xa5; PAGE_SIZE]);
        expect_invalid_range(io.write_page(offset, &[0x99; PAGE_SIZE]));
        let mut bytes = [0x6a; 2];
        expect_invalid_range(io.read_at(offset, &mut bytes));
        assert_eq!(bytes, [0x6a; 2]);
        expect_invalid_range(io.write_at(offset, &[0xcc; 2]));
    }
    expect_invalid_range(io.read_at(u64::MAX, &mut []));
    expect_invalid_range(io.write_at(u64::MAX, &[]));
    expect_invalid_range(io.truncate(u64::MAX));
    assert_eq!(io.file_size().unwrap(), size);
    let mut got = [0; PAGE_SIZE];
    io.read_page(0, &mut got).unwrap();
    assert_eq!(got, original);
}

#[test]
fn invalid_later_batch_offset_is_rejected_before_any_page_changes() {
    let (_dir, file) = create_test_file();
    let io = UringPageIO::try_new(file).unwrap();
    let original = [0x37; PAGE_SIZE];
    let replacement = [0x99; PAGE_SIZE];
    io.write_page(0, &original).unwrap();
    let size = io.file_size().unwrap();
    expect_invalid_range(io.write_pages(&[(0, replacement), (u64::MAX, replacement)]));
    expect_invalid_range(io.write_pages_ref(&[(0, &replacement), (u64::MAX, &replacement)]));
    expect_invalid_range(io.flush_pages(&[(0, replacement), (u64::MAX, replacement)]));
    assert_eq!(io.file_size().unwrap(), size);
    let mut got = [0; PAGE_SIZE];
    io.read_page(0, &mut got).unwrap();
    assert_eq!(got, original);
}

#[test]
fn invalid_metadata_range_is_rejected_before_slot_publication() {
    let (_dir, file) = create_test_file();
    let io = UringPageIO::try_new(file).unwrap();
    let original = [0x37; PAGE_SIZE];
    io.write_page(0, &original).unwrap();
    let size = io.file_size().unwrap();
    expect_invalid_range(io.write_commit_meta(u64::MAX, 1, 8, &[0x99; 8]));
    expect_invalid_range(io.write_commit_meta(0, 1, u64::MAX, &[0x99; 8]));
    expect_invalid_range(io.write_commit_meta(0, 1, u64::MAX, &[]));
    assert_eq!(io.file_size().unwrap(), size);
    let mut got = [0; PAGE_SIZE];
    io.read_page(0, &mut got).unwrap();
    assert_eq!(got, original);
}
