use super::*;

fn open_new_file(dir: &tempfile::TempDir, name: &str) -> File {
    File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.path().join(name))
        .unwrap()
}

#[test]
fn read_write_page_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let file = open_new_file(&dir, "test.db");
    let io = MmapPageIO::try_new(file).unwrap();

    let mut page = [0u8; PAGE_SIZE];
    page[0] = 0xAA;
    page[PAGE_SIZE - 1] = 0xBB;

    io.write_page(0, &page).unwrap();

    let mut read_buf = [0u8; PAGE_SIZE];
    io.read_page(0, &mut read_buf).unwrap();
    assert_eq!(read_buf, page);
}

#[test]
fn read_write_at() {
    let dir = tempfile::tempdir().unwrap();
    let file = open_new_file(&dir, "test.db");
    let io = MmapPageIO::try_new(file).unwrap();

    let header = [0x42u8; 512];
    io.write_at(0, &header).unwrap();

    let mut read_buf = [0u8; 512];
    io.read_at(0, &mut read_buf).unwrap();
    assert_eq!(read_buf, header);
}

#[test]
fn file_size_and_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let file = open_new_file(&dir, "test.db");
    let io = MmapPageIO::try_new(file).unwrap();

    assert_eq!(io.file_size().unwrap(), INITIAL_MAPPING_SIZE);

    let grow_to = 2 * INITIAL_MAPPING_SIZE;
    io.truncate(grow_to).unwrap();
    assert_eq!(io.file_size().unwrap(), grow_to);
}

#[test]
fn multiple_pages() {
    let dir = tempfile::tempdir().unwrap();
    let file = open_new_file(&dir, "test.db");
    let io = MmapPageIO::try_new(file).unwrap();

    let mut p0 = [0u8; PAGE_SIZE];
    let mut p1 = [0u8; PAGE_SIZE];
    p0[0] = 0x01;
    p1[0] = 0x02;

    io.write_page(0, &p0).unwrap();
    io.write_page(PAGE_SIZE as u64, &p1).unwrap();

    let mut r0 = [0u8; PAGE_SIZE];
    let mut r1 = [0u8; PAGE_SIZE];
    io.read_page(0, &mut r0).unwrap();
    io.read_page(PAGE_SIZE as u64, &mut r1).unwrap();

    assert_eq!(r0[0], 0x01);
    assert_eq!(r1[0], 0x02);
}

#[test]
fn write_auto_extends() {
    let dir = tempfile::tempdir().unwrap();
    let file = open_new_file(&dir, "test.db");
    let io = MmapPageIO::try_new(file).unwrap();

    let far_offset = 3 * INITIAL_MAPPING_SIZE;
    let page = [0xCCu8; PAGE_SIZE];
    io.write_page(far_offset, &page).unwrap();

    let mut read_buf = [0u8; PAGE_SIZE];
    io.read_page(far_offset, &mut read_buf).unwrap();
    assert_eq!(read_buf[0], 0xCC);
    assert!(io.file_size().unwrap() >= far_offset + PAGE_SIZE as u64);
}

#[test]
fn fsync_does_not_error() {
    let dir = tempfile::tempdir().unwrap();
    let file = open_new_file(&dir, "test.db");
    let io = MmapPageIO::try_new(file).unwrap();

    let page = [0xFFu8; PAGE_SIZE];
    io.write_page(0, &page).unwrap();
    io.fsync().unwrap();
}

#[test]
fn empty_file_init() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.db");
    let file = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    assert_eq!(file.metadata().unwrap().len(), 0);
    let io = MmapPageIO::try_new(file).unwrap();
    assert!(io.file_size().unwrap() >= INITIAL_MAPPING_SIZE);
}

#[test]
fn write_commit_meta_works() {
    let dir = tempfile::tempdir().unwrap();
    let file = open_new_file(&dir, "test.db");
    let io = MmapPageIO::try_new(file).unwrap();

    io.write_commit_meta(20, 0x01, 100, &[0xAB; 64]).unwrap();

    let mut god = [0u8; 1];
    io.read_at(20, &mut god).unwrap();
    assert_eq!(god[0], 0x01);

    let mut slot = [0u8; 64];
    io.read_at(100, &mut slot).unwrap();
    assert_eq!(slot, [0xAB; 64]);
}

#[test]
fn failed_remap_does_not_poison_backend() {
    let dir = tempfile::tempdir().unwrap();
    let file = open_new_file(&dir, "test.db");
    let io = MmapPageIO::try_new(file).unwrap();

    let page = [0x5Au8; PAGE_SIZE];
    io.write_page(0, &page).unwrap();

    // Absurd grow: set_len or map_mut must fail. Before the fix this left a
    // 1-byte dummy mapping behind a stale size, so any later access panicked.
    assert!(io.truncate(1 << 60).is_err());

    // Accesses after the failed remap must return Err at worst, never panic,
    // and file_size() must keep reporting the real on-disk length so no
    // caller sizes a destructive set_len() from a lying zero.
    assert!(io.file_size().unwrap() >= INITIAL_MAPPING_SIZE);
    let mut read_buf = [0u8; PAGE_SIZE];
    if io.read_page(0, &mut read_buf).is_ok() {
        assert_eq!(read_buf, page);
        io.write_page(PAGE_SIZE as u64, &page).unwrap();
        io.read_page(PAGE_SIZE as u64, &mut read_buf).unwrap();
        assert_eq!(read_buf, page);
    }
}

#[test]
fn degraded_mapping_reports_real_file_size() {
    let dir = tempfile::tempdir().unwrap();
    let file = open_new_file(&dir, "test.db");
    let io = MmapPageIO::try_new(file).unwrap();

    let page = [0x3Cu8; PAGE_SIZE];
    io.write_page(0, &page).unwrap();
    io.fsync().unwrap();

    // Simulate the worst remap_locked failure arm: both the grow and the
    // recovery map failed, leaving the 1-byte dummy behind size = 0.
    {
        let mut inner = io.inner.write();
        inner.mmap = MmapOptions::new().len(1).map_anon().unwrap();
        inner.size = 0;
    }

    // The file still holds live data; file_size() must say so, or the next
    // ensure_file_size would shrink the file over committed pages.
    assert!(io.file_size().unwrap() >= INITIAL_MAPPING_SIZE);

    // Reads fail their bounds check instead of panicking on the dummy map.
    let mut read_buf = [0u8; PAGE_SIZE];
    assert!(io.read_page(0, &mut read_buf).is_err());

    // A later write remaps at the real file length and recovers the backend.
    io.write_page(PAGE_SIZE as u64, &page).unwrap();
    io.read_page(0, &mut read_buf).unwrap();
    assert_eq!(read_buf, page);
}

#[test]
fn shrink_then_read_past_end_errs() {
    let dir = tempfile::tempdir().unwrap();
    let file = open_new_file(&dir, "test.db");
    let io = MmapPageIO::try_new(file).unwrap();

    let page = [0x77u8; PAGE_SIZE];
    io.write_page(0, &page).unwrap();

    io.truncate(PAGE_SIZE as u64).unwrap();
    assert_eq!(io.file_size().unwrap(), PAGE_SIZE as u64);

    let mut read_buf = [0u8; PAGE_SIZE];
    io.read_page(0, &mut read_buf).unwrap();
    assert_eq!(read_buf, page);
    assert!(io.read_page(PAGE_SIZE as u64, &mut read_buf).is_err());
}

#[test]
fn write_pages_ref_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let file = open_new_file(&dir, "test.db");
    let io = MmapPageIO::try_new(file).unwrap();

    let a = [0x11u8; PAGE_SIZE];
    let b = [0x22u8; PAGE_SIZE];
    io.write_pages_ref(&[(0, &a), (PAGE_SIZE as u64 * 3, &b)])
        .unwrap();

    let mut got = [0u8; PAGE_SIZE];
    io.read_page(0, &mut got).unwrap();
    assert_eq!(got, a);
    io.read_page(PAGE_SIZE as u64 * 3, &mut got).unwrap();
    assert_eq!(got, b);
}

#[test]
fn truncate_to_zero_keeps_empty_io_and_later_growth_usable() {
    let dir = tempfile::tempdir().unwrap();
    let io = MmapPageIO::try_new(open_new_file(&dir, "zero.db")).unwrap();
    io.write_at(0, &[0x77; 32]).unwrap();
    io.truncate(0).unwrap();
    assert_eq!(io.file_size().unwrap(), 0);
    assert_eq!(io.inner.read().size, 0);
    io.read_at(0, &mut []).unwrap();
    let mut sentinel = [0xa5];
    assert!(matches!(io.read_at(0, &mut sentinel), Err(Error::Io(error))
        if error.kind() == io::ErrorKind::UnexpectedEof));
    assert_eq!(sentinel, [0xa5]);
    assert!(io.read_at(1, &mut []).is_err());
    io.write_at(0, &[]).unwrap();
    io.write_pages(&[]).unwrap();
    io.write_pages_ref(&[]).unwrap();
    io.fsync().unwrap();
    io.truncate(0).unwrap();
    assert_eq!(io.file_size().unwrap(), 0);

    io.write_at(7, &[]).unwrap();
    assert_eq!(io.file_size().unwrap(), 7);
    let mut gap = [0xff; 7];
    io.read_at(0, &mut gap).unwrap();
    assert_eq!(gap, [0; 7]);
    io.truncate(0).unwrap();
    io.write_commit_meta(0, 1, 8, &[0x42; 4]).unwrap();
    let mut metadata = [0xff; 12];
    io.read_at(0, &mut metadata).unwrap();
    assert_eq!(metadata, [1, 0, 0, 0, 0, 0, 0, 0, 0x42, 0x42, 0x42, 0x42]);

    io.truncate(0).unwrap();
    let page = [0x39; PAGE_SIZE];
    io.write_pages_ref(&[(PAGE_SIZE as u64, &page)]).unwrap();
    io.fsync().unwrap();
    drop(io);
    let reopened = MmapPageIO::try_new(open_new_file(&dir, "zero.db")).unwrap();
    let mut got = [0xff; PAGE_SIZE];
    reopened.read_page(0, &mut got).unwrap();
    assert_eq!(got, [0; PAGE_SIZE]);
    reopened.read_page(PAGE_SIZE as u64, &mut got).unwrap();
    assert_eq!(got, page);
}

#[test]
fn truncate_zero_from_degraded_mapping_truncates_the_real_file() {
    let dir = tempfile::tempdir().unwrap();
    let io = MmapPageIO::try_new(open_new_file(&dir, "degraded-zero.db")).unwrap();
    io.write_at(0, &[0x61; 32]).unwrap();
    io.fsync().unwrap();
    {
        let mut inner = io.inner.write();
        inner.mmap = MmapOptions::new().len(1).map_anon().unwrap();
        inner.size = 0;
    }
    assert!(io.file_size().unwrap() > 0);
    io.truncate(0).unwrap();
    assert_eq!(io.file_size().unwrap(), 0);
    io.fsync().unwrap();
    io.write_at(0, b"recovered").unwrap();
    let mut got = [0; 9];
    io.read_at(0, &mut got).unwrap();
    assert_eq!(&got, b"recovered");
}
