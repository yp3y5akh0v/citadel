use citadel_core::PAGE_SIZE;
use citadel_io::memory_io::MemoryPageIO;
use citadel_io::traits::PageIO;

#[test]
fn sequential_page_writes_1000() {
    let io = MemoryPageIO::new();

    for i in 0u32..1000 {
        let offset = i as u64 * PAGE_SIZE as u64;
        let mut page = [0u8; PAGE_SIZE];
        page[0..4].copy_from_slice(&i.to_le_bytes());
        io.write_page(offset, &page).unwrap();
    }

    assert_eq!(io.file_size().unwrap(), 1000 * PAGE_SIZE as u64);

    for i in 0u32..1000 {
        let offset = i as u64 * PAGE_SIZE as u64;
        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(offset, &mut buf).unwrap();
        let stored = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        assert_eq!(stored, i);
    }
}

#[test]
fn random_access_pattern() {
    let io = MemoryPageIO::new();
    io.truncate(100 * PAGE_SIZE as u64).unwrap();

    let indices: Vec<u32> = (0..100).collect();

    for &i in indices.iter().rev() {
        let offset = i as u64 * PAGE_SIZE as u64;
        let mut page = [0u8; PAGE_SIZE];
        page[0..4].copy_from_slice(&i.to_le_bytes());
        page[PAGE_SIZE - 4..].copy_from_slice(&i.to_le_bytes());
        io.write_page(offset, &page).unwrap();
    }

    for &i in &indices {
        let offset = i as u64 * PAGE_SIZE as u64;
        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(offset, &mut buf).unwrap();
        let head = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let tail = u32::from_le_bytes(buf[PAGE_SIZE - 4..].try_into().unwrap());
        assert_eq!(head, i);
        assert_eq!(tail, i);
    }
}

#[test]
fn overwrite_all_pages() {
    let io = MemoryPageIO::new();

    for round in 0u8..5 {
        for i in 0..50u64 {
            let offset = i * PAGE_SIZE as u64;
            let page = [round; PAGE_SIZE];
            io.write_page(offset, &page).unwrap();
        }
    }

    for i in 0..50u64 {
        let offset = i * PAGE_SIZE as u64;
        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(offset, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 4), "page {i} should have round 4 value");
    }
}

#[test]
fn truncate_shrink_then_grow() {
    let io = MemoryPageIO::new();

    let page = [0xAAu8; PAGE_SIZE];
    io.write_page(0, &page).unwrap();
    io.write_page(PAGE_SIZE as u64, &page).unwrap();
    assert_eq!(io.file_size().unwrap(), 2 * PAGE_SIZE as u64);

    io.truncate(PAGE_SIZE as u64).unwrap();
    assert_eq!(io.file_size().unwrap(), PAGE_SIZE as u64);

    let mut buf = [0u8; PAGE_SIZE];
    let err = io.read_page(PAGE_SIZE as u64, &mut buf);
    assert!(err.is_err());

    io.truncate(3 * PAGE_SIZE as u64).unwrap();
    io.read_page(PAGE_SIZE as u64, &mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0), "grown pages should be zeroed");
}

#[test]
fn write_at_small_chunks() {
    let io = MemoryPageIO::new();

    for i in 0u64..1000 {
        let data = (i as u32).to_le_bytes();
        io.write_at(i * 4, &data).unwrap();
    }

    for i in 0u64..1000 {
        let mut buf = [0u8; 4];
        io.read_at(i * 4, &mut buf).unwrap();
        let val = u32::from_le_bytes(buf);
        assert_eq!(val, i as u32);
    }
}

#[test]
fn flush_pages_large_batch() {
    let io = MemoryPageIO::new();

    let batch: Vec<(u64, [u8; PAGE_SIZE])> = (0..100u64)
        .map(|i| {
            let offset = i * PAGE_SIZE as u64;
            let mut page = [0u8; PAGE_SIZE];
            page[0..8].copy_from_slice(&i.to_le_bytes());
            (offset, page)
        })
        .collect();

    io.truncate(100 * PAGE_SIZE as u64).unwrap();
    io.flush_pages(&batch).unwrap();

    for i in 0u64..100 {
        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(i * PAGE_SIZE as u64, &mut buf).unwrap();
        let val = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(val, i);
    }
}

#[test]
fn concurrent_read_write_threads() {
    use std::sync::Arc;
    use std::thread;

    let io = Arc::new(MemoryPageIO::new());
    io.truncate(10 * PAGE_SIZE as u64).unwrap();

    let writer = {
        let io = Arc::clone(&io);
        thread::spawn(move || {
            for round in 0u8..50 {
                for i in 0..10u64 {
                    let mut page = [round; PAGE_SIZE];
                    page[0..8].copy_from_slice(&i.to_le_bytes());
                    io.write_page(i * PAGE_SIZE as u64, &page).unwrap();
                }
            }
        })
    };

    let reader = {
        let io = Arc::clone(&io);
        thread::spawn(move || {
            for _ in 0..100 {
                for i in 0..10u64 {
                    let mut buf = [0u8; PAGE_SIZE];
                    let _ = io.read_page(i * PAGE_SIZE as u64, &mut buf);
                }
            }
        })
    };

    writer.join().unwrap();
    reader.join().unwrap();

    for i in 0..10u64 {
        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(i * PAGE_SIZE as u64, &mut buf).unwrap();
        assert!(buf[8..PAGE_SIZE].iter().all(|&b| b == 49),
            "page {i} should have final round value");
    }
}

#[test]
fn file_header_plus_pages() {
    let io = MemoryPageIO::new();

    let header = [0x42u8; 512];
    io.write_at(0, &header).unwrap();

    for i in 0..20u64 {
        let offset = 512 + i * PAGE_SIZE as u64;
        let mut page = [0u8; PAGE_SIZE];
        page[0..8].copy_from_slice(&i.to_le_bytes());
        io.write_page(offset, &page).unwrap();
    }

    let mut hdr_buf = [0u8; 512];
    io.read_at(0, &mut hdr_buf).unwrap();
    assert_eq!(hdr_buf, header);

    for i in 0..20u64 {
        let offset = 512 + i * PAGE_SIZE as u64;
        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(offset, &mut buf).unwrap();
        let val = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(val, i);
    }
}

#[test]
fn empty_operations() {
    let io = MemoryPageIO::new();

    io.fsync().unwrap();
    assert_eq!(io.file_size().unwrap(), 0);
    io.truncate(0).unwrap();
    assert_eq!(io.file_size().unwrap(), 0);

    io.flush_pages(&[]).unwrap();
    assert_eq!(io.file_size().unwrap(), 0);

    io.write_at(0, &[]).unwrap();
    assert_eq!(io.file_size().unwrap(), 0);
}
