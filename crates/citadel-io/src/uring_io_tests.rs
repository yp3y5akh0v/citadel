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

mod retirement {
    use super::*;
    use std::collections::VecDeque;

    enum Step {
        Wait(std::result::Result<usize, i32>),
        Complete(u64, i32),
    }
    struct ScriptedDriver(VecDeque<Step>);
    impl CompletionDriver for ScriptedDriver {
        fn next_completion(&mut self) -> Option<(u64, i32)> {
            if matches!(self.0.front(), Some(Step::Complete(..))) {
                let Some(Step::Complete(id, result)) = self.0.pop_front() else {
                    unreachable!()
                };
                Some((id, result))
            } else {
                None
            }
        }
        fn submit_and_wait_one(&mut self) -> io::Result<usize> {
            let Some(Step::Wait(result)) = self.0.pop_front() else {
                panic!("retirement must consume the expected wait/completion sequence")
            };
            result.map_err(io::Error::from_raw_os_error)
        }
    }
    fn published(count: usize, length: Option<usize>) -> PendingRequests {
        let mut pending = PendingRequests::new(length);
        pending.published = count;
        pending
    }

    #[test]
    fn first_error_waits_for_delayed_out_of_order_completions_and_partial_submissions() {
        let mut driver = ScriptedDriver(VecDeque::from([
            Step::Wait(Ok(1)),
            Step::Complete(0, -libc::EBADF),
            Step::Wait(Err(libc::EAGAIN)),
            Step::Wait(Ok(1)),
            Step::Complete(2, PAGE_SIZE as i32),
            Step::Wait(Err(libc::EINTR)),
            Step::Wait(Err(libc::EBUSY)),
            Step::Wait(Ok(1)),
            Step::Complete(1, 0),
        ]));
        let error = complete_pending(&mut driver, published(3, Some(PAGE_SIZE))).unwrap_err();
        assert!(matches!(error, Error::Io(error) if error.raw_os_error() == Some(libc::EBADF)));
        assert!(driver.0.is_empty(), "return must follow every terminal CQE");
    }

    #[test]
    fn short_write_is_returned_only_after_the_other_request_completes() {
        let mut driver = ScriptedDriver(VecDeque::from([
            Step::Wait(Ok(2)),
            Step::Complete(1, PAGE_SIZE as i32 - 1),
            Step::Wait(Ok(0)),
            Step::Complete(0, PAGE_SIZE as i32),
        ]));
        let error = complete_pending(&mut driver, published(2, Some(PAGE_SIZE))).unwrap_err();
        assert!(matches!(error, Error::Io(error) if error.kind() == io::ErrorKind::WriteZero));
        assert!(driver.0.is_empty());
    }

    #[test]
    fn enter_error_can_return_when_all_published_requests_have_retired() {
        let mut driver = ScriptedDriver(VecDeque::from([
            Step::Wait(Err(libc::EBADF)),
            Step::Complete(0, 17),
        ]));
        let error = complete_pending(&mut driver, published(1, None)).unwrap_err();
        assert!(matches!(error, Error::Io(error) if error.raw_os_error() == Some(libc::EBADF)));
        assert!(driver.0.is_empty());
        let mut driver = ScriptedDriver(VecDeque::from([Step::Wait(Ok(1)), Step::Complete(0, 7)]));
        assert_eq!(
            complete_pending(&mut driver, published(1, None)).unwrap(),
            7
        );
    }

    #[test]
    fn unretired_buffers_never_escape_through_unwind_or_invalid_driver_state() {
        const CHILD: &str = "CITADEL_TEST_PENDING_IO_CHILD";
        if let Ok(scenario) = std::env::var(CHILD) {
            // Deliberately aborting children must not emit a core dump.
            let limit = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            unsafe {
                libc::setrlimit(libc::RLIMIT_CORE, &limit);
            }
            if scenario == "unwind" {
                let _guard = published(1, None);
                panic!("simulated unwind after SQ publication");
            }
            let steps = match scenario.as_str() {
                "duplicate" => vec![Step::Complete(0, 0), Step::Complete(0, 0)],
                "foreign" => vec![Step::Complete(7, 0)],
                "driver" => vec![Step::Wait(Err(libc::EBADR))],
                _ => panic!("unknown child scenario"),
            };
            let mut driver = ScriptedDriver(steps.into());
            let _ = complete_pending(&mut driver, published(2, None));
            panic!("an unsafe return was allowed");
        }
        use std::os::unix::process::ExitStatusExt;
        for scenario in ["unwind", "duplicate", "foreign", "driver"] {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "uring_io::tests::retirement::unretired_buffers_never_escape_through_unwind_or_invalid_driver_state", "--nocapture"])
                .env(CHILD, scenario)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status().unwrap();
            assert_eq!(status.signal(), Some(libc::SIGABRT), "{scenario}");
        }
    }
}

#[test]
fn readonly_multioperation_failures_drain_the_ring_before_it_is_reused() {
    let (dir, mut file) = create_test_file();
    use std::io::Write;
    let original = [0x37; PAGE_SIZE];
    for _ in 0..8 {
        file.write_all(&original).unwrap();
    }
    drop(file);
    let readonly = File::open(dir.path().join("test.db")).unwrap();
    let io = UringPageIO::try_new(readonly).unwrap();
    let replacement = [0x99; PAGE_SIZE];
    let pages: Vec<_> = (0..8)
        .map(|index| (index as u64 * PAGE_SIZE as u64, replacement))
        .collect();
    let refs: Vec<_> = pages.iter().map(|(offset, page)| (*offset, page)).collect();
    for use_refs in [false, true] {
        let result = if use_refs {
            io.write_pages_ref(&refs)
        } else {
            io.write_pages(&pages)
        };
        assert!(
            matches!(result, Err(Error::Io(error)) if error.raw_os_error() == Some(libc::EBADF))
        );
        let mut ring = io.ring.lock();
        assert!(ring.submission().is_empty());
        assert!(ring.completion().next().is_none());
    }
    let mut got = [0; PAGE_SIZE];
    io.read_page(0, &mut got).unwrap();
    assert_eq!(got, original);
    io.read_page(7 * PAGE_SIZE as u64, &mut got).unwrap();
    assert_eq!(got, original);
}

#[test]
fn multiple_write_chunks_retire_before_buffers_are_reused() {
    let (_dir, file) = create_test_file();
    let io = UringPageIO::try_new(file).unwrap();
    let mut pages: Vec<_> = (0..300)
        .map(|index| (index as u64 * PAGE_SIZE as u64, [index as u8; PAGE_SIZE]))
        .collect();
    io.write_pages(&pages).unwrap();
    for index in [0, 254, 255, 256, 299] {
        let mut got = [0; PAGE_SIZE];
        io.read_page(pages[index].0, &mut got).unwrap();
        assert_eq!(got, pages[index].1);
    }
    for (_, page) in &mut pages {
        page.fill(0x69);
    }
    let refs: Vec<_> = pages.iter().map(|(offset, page)| (*offset, page)).collect();
    io.write_pages_ref(&refs).unwrap();
    for index in [0, 254, 255, 256, 299] {
        let mut got = [0; PAGE_SIZE];
        io.read_page(pages[index].0, &mut got).unwrap();
        assert_eq!(got, pages[index].1);
    }
    let mut ring = io.ring.lock();
    assert!(ring.submission().is_empty());
    assert!(ring.completion().next().is_none());
}

#[test]
fn explicit_truncate_waits_for_the_ring_owner() {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    let (_dir, file) = create_test_file();
    let io = Arc::new(UringPageIO::try_new(file).unwrap());
    io.truncate(PAGE_SIZE as u64).unwrap();
    let guard = io.ring.lock();
    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (done_tx, done_rx) = mpsc::channel();
    let worker_io = Arc::clone(&io);
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx.send(worker_io.truncate(0)).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = done_rx.recv_timeout(Duration::from_millis(50));
    // Always release before assertions/join, including on a pre-fix failure.
    drop(guard);
    let completed_early = early.is_ok();
    match early {
        Ok(result) => result.unwrap(),
        Err(mpsc::RecvTimeoutError::Timeout) => done_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        Err(error) => panic!("truncate worker disconnected: {error}"),
    }
    worker.join().unwrap();
    assert!(!completed_early, "truncate bypassed the active ring owner");
    assert_eq!(io.file_size().unwrap(), 0);
    io.write_page(0, &[0x63; PAGE_SIZE]).unwrap();
    let mut got = [0; PAGE_SIZE];
    io.read_page(0, &mut got).unwrap();
    assert_eq!(got, [0x63; PAGE_SIZE]);
}

#[test]
fn concurrent_unequal_batches_preserve_both_ranges_and_allow_reuse() {
    use std::sync::{Arc, Barrier};
    let (_dir, file) = create_test_file();
    let io = Arc::new(UringPageIO::try_new(file).unwrap());
    let far_offset = 3 * PAGE_SIZE as u64;
    for _ in 0..32 {
        io.truncate(0).unwrap();
        let start = Arc::new(Barrier::new(2));
        let near_io = Arc::clone(&io);
        let near_start = Arc::clone(&start);
        let near = std::thread::spawn(move || {
            near_start.wait();
            near_io.write_pages(&[(0, [0x37; PAGE_SIZE])])
        });
        let far_io = Arc::clone(&io);
        let far = std::thread::spawn(move || {
            start.wait();
            far_io.write_pages_ref(&[(far_offset, &[0x69; PAGE_SIZE])])
        });
        // Join both before propagating either operation error.
        let near_result = near.join();
        let far_result = far.join();
        near_result.unwrap().unwrap();
        far_result.unwrap().unwrap();
        assert!(io.file_size().unwrap() >= far_offset + PAGE_SIZE as u64);
        let mut got = [0; PAGE_SIZE];
        io.read_page(0, &mut got).unwrap();
        assert_eq!(got, [0x37; PAGE_SIZE]);
        io.read_page(far_offset, &mut got).unwrap();
        assert_eq!(got, [0x69; PAGE_SIZE]);
    }
}
