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
