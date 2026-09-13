use citadel_core::{Error, Result, PAGE_SIZE};
use citadel_io::memory_io::MemoryPageIO;
use citadel_io::traits::PageIO;
use std::io::ErrorKind;

fn backends(test: impl Fn(&dyn PageIO)) {
    test(&MemoryPageIO::new());
    #[cfg(not(target_arch = "wasm32"))]
    {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.path().join("ranges.db"))
            .unwrap();
        test(&citadel_io::mmap_io::MmapPageIO::try_new(file).unwrap());
    }
}

fn invalid(result: Result<()>) {
    assert!(
        matches!(result, Err(Error::Io(ref error)) if error.kind() == ErrorKind::InvalidInput),
        "expected an invalid range, got {result:?}"
    );
}

fn unchanged(io: &dyn PageIO, size: u64) {
    assert_eq!(io.file_size().unwrap(), size);
    let mut bytes = [0; PAGE_SIZE];
    io.read_page(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0x37; PAGE_SIZE]);
}

#[test]
fn invalid_reads_leave_output_untouched() {
    backends(|io| {
        io.write_page(0, &[0x37; PAGE_SIZE]).unwrap();
        for offset in [u64::MAX, isize::MAX as u64 + 1] {
            let mut page = [0x91; PAGE_SIZE];
            invalid(io.read_page(offset, &mut page));
            assert_eq!(page, [0x91; PAGE_SIZE]);
            let mut bytes = [0x73; 2];
            invalid(io.read_at(offset, &mut bytes));
            assert_eq!(bytes, [0x73; 2]);
        }
        let mut bytes = [0x45; 2];
        assert!(matches!(
            io.read_at(io.file_size().unwrap(), &mut bytes),
            Err(Error::Io(ref error)) if error.kind() == ErrorKind::UnexpectedEof
        ));
        assert_eq!(bytes, [0x45; 2]);
    });
}

#[test]
fn invalid_writes_and_truncate_leave_size_and_content_unchanged() {
    backends(|io| {
        io.write_page(0, &[0x37; PAGE_SIZE]).unwrap();
        let size = io.file_size().unwrap();
        for offset in [u64::MAX, isize::MAX as u64 + 1] {
            invalid(io.write_page(offset, &[0xe1; PAGE_SIZE]));
            unchanged(io, size);
            invalid(io.write_at(offset, b"invalid"));
            unchanged(io, size);
            invalid(io.truncate(offset));
            unchanged(io, size);
        }
    });
}

#[test]
fn invalid_later_batch_ranges_are_rejected_before_the_first_write() {
    backends(|io| {
        io.write_page(0, &[0x37; PAGE_SIZE]).unwrap();
        let size = io.file_size().unwrap();
        let replacement = [0xe1; PAGE_SIZE];
        invalid(io.write_pages(&[(0, replacement), (u64::MAX, replacement)]));
        unchanged(io, size);
        invalid(io.write_pages_ref(&[(0, &replacement), (u64::MAX, &replacement)]));
        unchanged(io, size);
        invalid(io.flush_pages(&[(0, replacement), (u64::MAX, replacement)]));
        unchanged(io, size);
    });
}

#[test]
fn both_metadata_ranges_are_admitted_before_either_is_written() {
    backends(|io| {
        io.write_page(0, &[0x37; PAGE_SIZE]).unwrap();
        let size = io.file_size().unwrap();
        invalid(io.write_commit_meta(u64::MAX, 0xaa, 20, b"new slot"));
        unchanged(io, size);
        invalid(io.write_commit_meta(10, 0xaa, u64::MAX, b"new slot"));
        unchanged(io, size);
        invalid(io.write_commit_meta(10, 0xaa, u64::MAX, &[]));
        unchanged(io, size);
    });
}

#[test]
fn valid_empty_operations_and_overlapping_metadata_keep_their_behavior() {
    backends(|io| {
        io.write_page(0, &[0x37; PAGE_SIZE]).unwrap();
        let size = io.file_size().unwrap();
        io.write_pages(&[]).unwrap();
        io.write_pages_ref(&[]).unwrap();
        unchanged(io, size);
        io.read_at(size, &mut []).unwrap();
        assert!(matches!(
            io.read_at(size + 1, &mut []),
            Err(Error::Io(ref error)) if error.kind() == ErrorKind::UnexpectedEof
        ));
        io.write_at(size + 3, &[]).unwrap();
        assert_eq!(io.file_size().unwrap(), size + 3);
        let mut gap = [0xff; 3];
        io.read_at(size, &mut gap).unwrap();
        assert_eq!(gap, [0; 3]);
        io.write_commit_meta(3, 0xa7, size + 5, &[]).unwrap();
        assert_eq!(io.file_size().unwrap(), size + 5);
        io.write_commit_meta(12, 0xa7, 10, &[1, 2, 3, 4]).unwrap();
        let mut overlap = [0; 4];
        io.read_at(10, &mut overlap).unwrap();
        assert_eq!(overlap, [1, 2, 0xa7, 4]);
        io.write_pages_ref(&[(0, &[0x13; PAGE_SIZE]), (0, &[0x29; PAGE_SIZE])])
            .unwrap();
        let mut page = [0; PAGE_SIZE];
        io.read_page(0, &mut page).unwrap();
        assert_eq!(page, [0x29; PAGE_SIZE]);
    });
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn concurrent_shrink_and_writes_keep_mapping_bounds_atomic() {
    use citadel_io::mmap_io::MmapPageIO;
    use std::sync::{Arc, Barrier};

    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(dir.path().join("concurrent.db"))
        .unwrap();
    let io = Arc::new(MmapPageIO::try_new(file).unwrap());
    let start = Arc::new(Barrier::new(2));
    let writer = Arc::clone(&io);
    let writer_start = Arc::clone(&start);
    let thread = std::thread::spawn(move || {
        writer_start.wait();
        for _ in 0..64 {
            writer
                .write_page((PAGE_SIZE * 2) as u64, &[0x29; PAGE_SIZE])
                .unwrap();
        }
    });
    start.wait();
    for _ in 0..64 {
        io.truncate(PAGE_SIZE as u64).unwrap();
    }
    thread.join().unwrap();
    // The last concurrent operation may be the truncate. Confirm recovery
    // with a final complete write and read, without assuming thread order.
    io.write_page((PAGE_SIZE * 2) as u64, &[0x17; PAGE_SIZE])
        .unwrap();
    let mut page = [0; PAGE_SIZE];
    io.read_page((PAGE_SIZE * 2) as u64, &mut page).unwrap();
    assert_eq!(page, [0x17; PAGE_SIZE]);
}
