use std::sync::{Arc, Mutex, Weak};

use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{CancelToken, Error, Result, PAGE_SIZE};
use citadel_crypto::page_cipher;
use citadel_io::file_manager::page_offset;
use citadel_io::traits::PageIO;
use citadel_page::leaf_node;
use citadel_page::page::Page;
use rustc_hash::FxHashMap;

use crate::manager::tests::{test_keys, MemIO};
use crate::manager::{TxnManager, SCAN_CACHE_BATCH_SIZE};
use crate::ReadBudget;

const TABLE: &[u8] = b"streamed";
const CACHE_PAGES: usize = 2;
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

#[test]
fn visited_pages_pack_dense_ids_without_losing_duplicates() {
    let mut visited = super::VisitedPages::default();
    for id in 0..8192 {
        assert!(visited.insert(PageId(id)));
    }
    assert_eq!(visited.words.len(), 128);
    for id in (0..8192).rev() {
        assert!(!visited.insert(PageId(id)));
    }
}

#[test]
fn visited_pages_handle_sparse_ids_and_word_boundaries() {
    let mut visited = super::VisitedPages::default();
    let mut expected = std::collections::BTreeSet::new();
    let ids = [0, 63, 64, u32::MAX, u32::MAX - 1, 1, 65, 128];
    for id in ids.into_iter().chain(ids.into_iter().rev()) {
        assert_eq!(visited.insert(PageId(id)), expected.insert(id));
    }
    assert_eq!(visited.words.len(), 4);
    for id in (0..4096u32).map(|id| id << 20) {
        assert_eq!(visited.insert(PageId(id)), expected.insert(id));
    }
    assert!(visited.words.len() <= expected.len());
}

#[test]
fn visited_pages_match_a_reference_set_in_shuffled_order() {
    let mut visited = super::VisitedPages::default();
    let mut expected = std::collections::BTreeSet::new();
    let mut state = 0x243f_6a88u32;
    for _ in 0..20_000 {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        for id in [state, state & 0x3fff, state, state & 0x3fff] {
            assert_eq!(visited.insert(PageId(id)), expected.insert(id));
        }
    }
}

#[derive(Default)]
struct ReadTrace {
    offsets: Vec<u64>,
    cancel_at: Option<(u64, CancelToken)>,
}

struct RecordingIO {
    inner: MemIO,
    trace: Arc<Mutex<ReadTrace>>,
}

impl RecordingIO {
    fn share(&self) -> Self {
        Self {
            inner: self.inner.share(),
            trace: Arc::clone(&self.trace),
        }
    }

    fn clear_reads(&self) {
        self.trace.lock().unwrap().offsets.clear();
    }

    fn reads(&self) -> Vec<u64> {
        self.trace.lock().unwrap().offsets.clone()
    }

    fn open(&self) -> TxnManager {
        self.open_with_cache(CACHE_PAGES)
    }

    fn open_with_cache(&self, cache_pages: usize) -> TxnManager {
        let (dek, mac, _) = test_keys();
        TxnManager::open(Box::new(self.share()), dek, mac, 1, cache_pages).unwrap()
    }

    fn read_plain(&self, id: PageId) -> Page {
        let (dek, mac, _) = test_keys();
        citadel_buffer::pool::read_and_decrypt(&self.inner, id, page_offset(id), &dek, &mac, 1)
            .unwrap()
    }

    fn rewrite(&self, page: Page) {
        self.rewrite_at(page.page_id(), page);
    }

    fn rewrite_at(&self, id: PageId, mut page: Page) {
        let (dek, mac, _) = test_keys();
        page.update_checksum();
        let mut encrypted = [0; PAGE_SIZE];
        page_cipher::encrypt_page(&dek, &mac, id, 1, page.as_bytes(), &mut encrypted);
        self.inner.write_page(page_offset(id), &encrypted).unwrap();
    }
}

impl PageIO for RecordingIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let token = {
            let mut trace = self.trace.lock().unwrap();
            trace.offsets.push(offset);
            trace
                .cancel_at
                .as_ref()
                .filter(|(target, _)| *target == offset)
                .map(|(_, token)| token.clone())
        };
        self.inner.read_page(offset, buf)?;
        if let Some(token) = token {
            token.cancel();
        }
        Ok(())
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.inner.write_page(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.inner.read_at(offset, buf)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        self.inner.write_at(offset, buf)
    }

    fn fsync(&self) -> Result<()> {
        self.inner.fsync()
    }

    fn file_size(&self) -> Result<u64> {
        self.inner.file_size()
    }

    fn truncate(&self, size: u64) -> Result<()> {
        self.inner.truncate(size)
    }
}

fn seeded(count: u32) -> (TxnManager, RecordingIO, Rows) {
    let io = RecordingIO {
        inner: MemIO::new(1024 * 1024),
        trace: Arc::default(),
    };
    let (dek, mac, dek_id) = test_keys();
    let manager = TxnManager::create(
        Box::new(io.share()),
        dek,
        mac,
        1,
        0x1234,
        dek_id,
        CACHE_PAGES,
    )
    .unwrap();
    let mut writer = manager.begin_write().unwrap();
    writer.create_table(TABLE).unwrap();
    let mut expected = Vec::new();
    for id in 0..count {
        let key = id.to_be_bytes().to_vec();
        let size = if id % 13 == 0 {
            citadel_page::overflow::OVERFLOW_DATA_CAPACITY * 3 + 17
        } else {
            1000
        };
        let value = vec![id as u8; size];
        writer.table_insert(TABLE, &key, &value).unwrap();
        expected.push((key, value));
    }
    writer.commit().unwrap();
    (manager, io, expected)
}

fn scan(reader: &mut super::ReadTxn<'_>) -> Rows {
    let mut rows = Vec::new();
    reader
        .table_scan_raw(TABLE, |key, value| {
            rows.push((key.to_vec(), value.to_vec()));
            true
        })
        .unwrap();
    rows
}

fn scan_page_fixture(
    types: &[PageType],
    cache_pages: usize,
) -> (TxnManager, RecordingIO, Vec<PageId>) {
    let (manager, io, _) = seeded(0);
    drop(manager);
    let ids: Vec<_> = (0..types.len()).map(|i| PageId(32 + i as u32)).collect();
    for (&id, &page_type) in ids.iter().zip(types) {
        io.rewrite(Page::new(id, page_type, TxnId(1)));
    }
    let manager = io.open_with_cache(cache_pages);
    (manager, io, ids)
}

fn scan_view<'a>(
    manager: &'a TxnManager,
    cache: &'a FxHashMap<PageId, Arc<Page>>,
    high_water_mark: u32,
) -> super::StreamingReadPages<'a> {
    super::StreamingReadPages {
        cache,
        manager,
        high_water_mark,
        current: None,
        cached_leaves: Vec::with_capacity(SCAN_CACHE_BATCH_SIZE - 1),
    }
}

#[test]
fn cached_scan_batch_pins_only_a_bounded_run_in_traversal_order() {
    let count = SCAN_CACHE_BATCH_SIZE * 2 + 3;
    let (manager, io, ids) = scan_page_fixture(&vec![PageType::Leaf; count], count + 4);
    let weak: Vec<_> = ids
        .iter()
        .map(|&id| Arc::downgrade(&manager.fetch_page(id).unwrap()))
        .collect();
    assert!(weak.iter().all(|page| page.strong_count() == 1));
    let cache = FxHashMap::default();
    let mut view = scan_view(&manager, &cache, ids.last().unwrap().0 + 1);
    let mut pending: Vec<_> = ids.iter().rev().copied().collect();
    io.clear_reads();
    for (index, &expected) in ids.iter().enumerate() {
        let page = view
            .load_scan_page(pending.pop().unwrap(), &pending)
            .unwrap();
        assert_eq!(page.page_id(), expected);
        assert!(view.cached_leaves.len() < SCAN_CACHE_BATCH_SIZE);
        let pinned: usize = weak.iter().map(|page| page.strong_count() - 1).sum();
        assert!(pinned <= SCAN_CACHE_BATCH_SIZE);
        if index == 0 {
            assert_eq!(pinned, SCAN_CACHE_BATCH_SIZE);
            assert_eq!(view.cached_leaves.len(), SCAN_CACHE_BATCH_SIZE - 1);
        }
    }
    assert!(view.cached_leaves.is_empty());
    assert!(weak.iter().all(|page| page.strong_count() == 1));
    assert!(cache.is_empty());
    assert!(io.reads().is_empty());
}

#[test]
fn cached_scan_batch_stops_before_misses_nonleaves_and_snapshot_bounds() {
    for barrier in ["miss", "branch", "bounds"] {
        let barrier_type = if barrier == "branch" {
            PageType::Branch
        } else {
            PageType::Leaf
        };
        let (manager, io, ids) = scan_page_fixture(
            &[PageType::Leaf, PageType::Leaf, barrier_type, PageType::Leaf],
            8,
        );
        let high_water_mark = if barrier == "bounds" {
            ids[2].0
        } else {
            ids[3].0 + 1
        };
        for (index, &id) in ids.iter().enumerate() {
            if barrier != "miss" || index != 2 {
                manager.fetch_page(id).unwrap();
            }
        }
        let cache = FxHashMap::default();
        let mut view = scan_view(&manager, &cache, high_water_mark);
        io.clear_reads();
        assert_eq!(
            view.load_scan_page(ids[0], &[ids[3], ids[2], ids[1]])
                .unwrap()
                .page_id(),
            ids[0]
        );
        assert_eq!(
            view.cached_leaves
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            [ids[1]]
        );
        assert!(io.reads().is_empty(), "{barrier} barrier caused ahead I/O");
        view.load_scan_page(ids[1], &[ids[3], ids[2]]).unwrap();
        let result = view.load_scan_page(ids[2], &[ids[3]]);
        if barrier == "bounds" {
            assert!(matches!(result, Err(Error::PageOutOfBounds(id)) if id == ids[2]));
        } else {
            assert_eq!(result.unwrap().page_type(), Some(barrier_type));
        }
        assert!(view.cached_leaves.is_empty());
        assert_eq!(
            io.reads(),
            if barrier == "miss" {
                vec![page_offset(ids[2])]
            } else {
                vec![]
            }
        );
    }
}

#[test]
fn cached_scan_batch_defers_bad_headers_and_observes_cancellation_first() {
    let (manager, io, ids) = scan_page_fixture(&[PageType::Leaf; 3], 8);
    let mut bad = io.read_plain(ids[1]);
    bad.set_page_id(ids[2]);
    io.rewrite_at(ids[1], bad);
    for &id in &ids {
        manager.fetch_page(id).unwrap();
    }
    let cache = FxHashMap::default();
    let mut view = scan_view(&manager, &cache, ids[2].0 + 1);
    let mut traversal = super::LeafTraversal {
        pending: ids.iter().rev().copied().collect(),
        visited: super::VisitedPages::default(),
    };
    io.clear_reads();
    assert_eq!(
        traversal
            .next_leaf(None, |id, pending| view.load_scan_page(id, pending))
            .unwrap()
            .unwrap()
            .page_id(),
        ids[0]
    );
    assert_eq!(view.cached_leaves.len(), 2);
    let token = CancelToken::new();
    token.cancel();
    assert!(matches!(
        traversal.next_leaf(Some(&token), |id, pending| view.load_scan_page(id, pending)),
        Err(Error::Interrupted)
    ));
    assert_eq!(view.cached_leaves.len(), 2);
    assert!(matches!(
        traversal.next_leaf(None, |id, pending| view.load_scan_page(id, pending)),
        Err(Error::DatabaseCorrupted)
    ));
    assert!(io.reads().is_empty());
}

#[test]
fn cached_scan_batch_rejects_repeated_pages_only_when_traversal_reaches_them() {
    let (manager, io, ids) = scan_page_fixture(&[PageType::Leaf; 2], 8);
    for &id in &ids {
        manager.fetch_page(id).unwrap();
    }
    let cache = FxHashMap::default();
    let mut view = scan_view(&manager, &cache, ids[1].0 + 1);
    let mut traversal = super::LeafTraversal {
        pending: vec![ids[0], ids[1], ids[0]],
        visited: super::VisitedPages::default(),
    };
    io.clear_reads();
    for &expected in &ids {
        assert_eq!(
            traversal
                .next_leaf(None, |id, pending| view.load_scan_page(id, pending))
                .unwrap()
                .unwrap()
                .page_id(),
            expected
        );
    }
    assert_eq!(view.cached_leaves.len(), 1);
    assert!(matches!(
        traversal.next_leaf(None, |_, _| panic!("repeated page was loaded")),
        Err(Error::DatabaseCorrupted)
    ));
    assert!(io.reads().is_empty());
}

#[test]
fn cached_scan_batch_preserves_snapshot_cache_priority_and_discards_stale_queue() {
    let (manager, io, ids) = scan_page_fixture(&[PageType::Leaf; 4], 8);
    for &id in &ids {
        manager.fetch_page(id).unwrap();
    }
    let snapshot_page = Arc::new(io.read_plain(ids[1]));
    let cache = FxHashMap::from_iter([(ids[1], Arc::clone(&snapshot_page))]);
    let mut view = scan_view(&manager, &cache, ids[3].0 + 1);
    view.load_scan_page(ids[0], &[ids[2], ids[1]]).unwrap();
    assert_eq!(view.cached_leaves.len(), 2);
    assert!(Arc::ptr_eq(
        &snapshot_page,
        &view.load_scan_page(ids[1], &[ids[2]]).unwrap()
    ));
    assert_eq!(view.cached_leaves.len(), 1);
    view.load_scan_page(ids[3], &[]).unwrap();
    assert!(view.cached_leaves.is_empty());
    assert_eq!(cache.len(), 1);
}

#[test]
fn cached_scan_batch_survives_small_pool_eviction_between_leaves() {
    let (manager, io, ids) = scan_page_fixture(&[PageType::Leaf; 6], CACHE_PAGES);
    let first = manager.fetch_page(ids[0]).unwrap();
    let second = manager.fetch_page(ids[1]).unwrap();
    let cache = FxHashMap::default();
    let mut view = scan_view(&manager, &cache, ids[5].0 + 1);
    assert!(Arc::ptr_eq(
        &first,
        &view.load_scan_page(ids[0], &[ids[1]]).unwrap()
    ));
    assert_eq!(view.cached_leaves.len(), 1);
    for &id in &ids[2..] {
        manager.fetch_page(id).unwrap();
    }
    io.clear_reads();
    assert!(Arc::ptr_eq(
        &second,
        &view.load_scan_page(ids[1], &[]).unwrap()
    ));
    assert!(io.reads().is_empty());
    assert!(cache.is_empty());
}

#[test]
fn streaming_scan_preserves_inline_overflow_tombstone_order_and_counts() {
    let (manager, io, mut expected) = seeded(64);
    let leaves = manager.begin_read().collect_table_leaves(TABLE).unwrap();
    assert!(leaves.len() > CACHE_PAGES);
    let deleted_key = 1u32.to_be_bytes();
    let leaf = leaves
        .iter()
        .find(|page| {
            (0..page.num_cells()).any(|i| leaf_node::read_cell(page, i).key == deleted_key)
        })
        .unwrap();
    let mut page = leaf.as_ref().clone();
    let cells: Vec<_> = (0..page.num_cells())
        .map(|i| {
            let cell = leaf_node::read_cell(&page, i);
            if cell.key == deleted_key {
                leaf_node::build_cell(cell.key, ValueType::Tombstone, &[])
            } else {
                leaf_node::build_cell(cell.key, cell.val_type, cell.value)
            }
        })
        .collect();
    page.rebuild_cells(&cells.iter().map(Vec::as_slice).collect::<Vec<_>>());
    io.rewrite(page);
    drop(leaves);
    drop(manager);
    let manager = io.open();
    expected.retain(|(key, _)| key.as_slice() != deleted_key);

    let measurement = manager.measure_scans();
    let mut reader = manager.begin_read();
    assert_eq!(scan(&mut reader), expected);
    assert_eq!(
        measurement.rows_scanned(),
        64,
        "tombstones are examined rows"
    );
    assert!(reader.page_cache.is_empty());
    let leaves = reader.collect_table_leaves(TABLE).unwrap();
    assert!(reader.page_cache.len() >= leaves.len());
    let mut collected = Vec::new();
    reader
        .scan_leaves(&leaves, |key, value| {
            collected.push((key.to_vec(), value.to_vec()));
            true
        })
        .unwrap();
    assert_eq!(collected, expected);
    assert_eq!(measurement.rows_scanned(), 128);
}

#[test]
fn streaming_scan_does_not_grow_existing_snapshot_page_cache() {
    let (manager, _, expected) = seeded(96);
    let mut reader = manager.begin_read();
    assert_eq!(
        reader.table_get(TABLE, &expected[0].0).unwrap(),
        Some(expected[0].1.clone())
    );
    let before = reader.page_cache.clone();
    assert!(!before.is_empty());
    for _ in 0..2 {
        assert_eq!(scan(&mut reader), expected);
        assert_eq!(reader.page_cache.len(), before.len());
        for (id, page) in &before {
            assert!(Arc::ptr_eq(page, &reader.page_cache[id]));
        }
    }
}

#[test]
fn early_stop_does_not_fetch_a_later_corrupt_leaf() {
    let (manager, io, expected) = seeded(96);
    let leaves = manager.begin_read().collect_table_leaves(TABLE).unwrap();
    assert!(leaves.len() > 4);
    let last = leaves.last().unwrap().page_id();
    let mut encrypted = [0; PAGE_SIZE];
    io.inner
        .read_page(page_offset(last), &mut encrypted)
        .unwrap();
    encrypted[0] ^= 1;
    io.inner.write_page(page_offset(last), &encrypted).unwrap();
    drop(leaves);
    drop(manager);
    let manager = io.open();
    let mut reader = manager.begin_read();
    reader.lookup_table(TABLE).unwrap();
    io.clear_reads();
    let measurement = manager.measure_scans();
    let mut emitted = 0;
    reader
        .table_scan_raw(TABLE, |key, value| {
            assert_eq!(
                (key, value),
                (expected[0].0.as_slice(), expected[0].1.as_slice())
            );
            emitted += 1;
            false
        })
        .unwrap();
    assert_eq!(emitted, 1);
    assert_eq!(measurement.rows_scanned(), 1);
    assert!(!io.reads().contains(&page_offset(last)));
    assert!(reader.page_cache.is_empty());
    assert!(matches!(
        reader.table_scan_raw(TABLE, |_, _| true),
        Err(Error::PageTampered(id)) if id == last
    ));
    assert!(io.reads().contains(&page_offset(last)));
}

#[test]
fn cached_first_leaf_does_not_read_an_adjacent_corrupt_miss_before_early_stop() {
    let (manager, io, expected) = seeded(96);
    let leaves = manager.begin_read().collect_table_leaves(TABLE).unwrap();
    let first = leaves[0].page_id();
    let second = leaves[1].page_id();
    let mut encrypted = [0; PAGE_SIZE];
    io.inner
        .read_page(page_offset(second), &mut encrypted)
        .unwrap();
    encrypted[0] ^= 1;
    io.inner
        .write_page(page_offset(second), &encrypted)
        .unwrap();
    drop(leaves);
    drop(manager);
    let manager = io.open_with_cache(16);
    let first = Arc::downgrade(&manager.fetch_page(first).unwrap());
    let mut reader = manager.begin_read();
    reader.lookup_table(TABLE).unwrap();
    io.clear_reads();
    let mut emitted = 0;
    reader
        .table_scan_raw(TABLE, |key, value| {
            assert_eq!(
                (key, value),
                (expected[0].0.as_slice(), expected[0].1.as_slice())
            );
            assert_eq!(first.strong_count(), 2);
            emitted += 1;
            false
        })
        .unwrap();
    assert_eq!(emitted, 1);
    assert!(!io.reads().contains(&page_offset(second)));
    assert!(reader.page_cache.is_empty());
    assert!(
        matches!(reader.table_scan_raw(TABLE, |_, _| true), Err(Error::PageTampered(id)) if id == second)
    );
}

#[test]
fn cached_batch_callback_can_reenter_manager_and_preserve_its_snapshot() {
    const CHILD: &str = "CITADEL_STREAM_SCAN_REENTRANT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        run_stream_child(
            concat!(
                module_path!(),
                "::cached_batch_callback_can_reenter_manager_and_preserve_its_snapshot"
            ),
            CHILD,
            "reentrant",
        );
        return;
    }
    // Each 13-row group has twelve 1,000-byte inline values, requiring more
    // than one 8 KiB leaf even when sequential inserts pack leaves densely.
    let (manager, io, expected) = seeded((13 * SCAN_CACHE_BATCH_SIZE) as u32);
    let mut reader = manager.begin_read();
    let root = reader.lookup_table(TABLE).unwrap().root_page;
    let leaves = reader.collect_table_leaves(TABLE).unwrap();
    let ids: Vec<_> = leaves.iter().map(|page| page.page_id()).collect();
    assert!(ids.len() > SCAN_CACHE_BATCH_SIZE);
    drop(leaves);
    drop(reader);
    drop(manager);
    let manager = io.open_with_cache(512);
    manager.fetch_page(root).unwrap();
    let weak: Vec<Weak<Page>> = ids
        .iter()
        .map(|&id| Arc::downgrade(&manager.fetch_page(id).unwrap()))
        .collect();
    assert!(weak.iter().all(|page| page.strong_count() == 1));
    let mut reader = manager.begin_read();
    let mut actual = Vec::new();
    reader
        .table_scan_raw(TABLE, |key, value| {
            actual.push((key.to_vec(), value.to_vec()));
            if actual.len() == 1 {
                let pinned: usize = weak.iter().map(|page| page.strong_count() - 1).sum();
                assert!(
                    pinned > 1,
                    "callback must run with future cached leaves queued"
                );
                assert!(pinned <= SCAN_CACHE_BATCH_SIZE);
                assert_eq!(manager.fetch_page(root).unwrap().page_id(), root);
                let mut nested = manager.begin_read();
                assert_eq!(
                    nested.table_get(TABLE, &expected[1].0).unwrap(),
                    Some(expected[1].1.clone())
                );
                drop(nested);
                let mut writer = manager.begin_write().unwrap();
                writer
                    .table_insert(TABLE, &expected[0].0, b"changed")
                    .unwrap();
                writer.table_delete(TABLE, &expected[3].0).unwrap();
                writer.commit().unwrap();
            }
            true
        })
        .unwrap();
    assert_eq!(actual, expected);
    assert!(reader.page_cache.is_empty());
    assert!(weak.iter().all(|page| page.strong_count() <= 1));
    assert_eq!(scan(&mut reader), expected);
    let mut current = manager.begin_read();
    assert_eq!(
        current.table_get(TABLE, &expected[0].0).unwrap(),
        Some(b"changed".to_vec())
    );
    assert_eq!(current.table_get(TABLE, &expected[3].0).unwrap(), None);
}

fn run_stream_child(full_name: &str, env: &str, mode: &str) {
    let test_name = full_name.split_once("::").unwrap().1;
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture"])
        .env(env, mode)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut expired = false;
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            expired = true;
            child.kill().unwrap();
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(!expired, "{mode} exceeded deadline");
    assert!(
        output.status.success(),
        "{mode}: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed"),
        "child test did not execute"
    );
}

#[test]
fn streaming_scan_retains_registered_snapshot_across_commits() {
    let (manager, _, expected) = seeded(64);
    let mut reader = manager.begin_read();
    let mut actual = Vec::new();
    let mut committed = false;
    reader
        .table_scan_raw(TABLE, |key, value| {
            actual.push((key.to_vec(), value.to_vec()));
            if !committed {
                std::thread::scope(|scope| {
                    scope
                        .spawn(|| {
                            let mut writer = manager.begin_write().unwrap();
                            writer
                                .table_insert(TABLE, &0u32.to_be_bytes(), b"changed")
                                .unwrap();
                            writer.table_delete(TABLE, &3u32.to_be_bytes()).unwrap();
                            writer
                                .table_insert(TABLE, &99u32.to_be_bytes(), b"new")
                                .unwrap();
                            writer.commit().unwrap();
                        })
                        .join()
                        .unwrap();
                });
                committed = true;
            }
            true
        })
        .unwrap();
    assert_eq!(actual, expected);
    assert!(reader.page_cache.is_empty());
    for generation in 0..3 {
        let mut writer = manager.begin_write().unwrap();
        writer
            .table_insert(TABLE, &99u32.to_be_bytes(), &[generation])
            .unwrap();
        writer.commit().unwrap();
        assert_eq!(scan(&mut reader), expected);
        assert!(reader.page_cache.is_empty());
    }
    let mut current = manager.begin_read();
    assert_eq!(
        current.table_get(TABLE, &0u32.to_be_bytes()).unwrap(),
        Some(b"changed".to_vec())
    );
    assert_eq!(current.table_get(TABLE, &3u32.to_be_bytes()).unwrap(), None);
    assert_eq!(
        current.table_get(TABLE, &99u32.to_be_bytes()).unwrap(),
        Some(vec![2])
    );
}

#[test]
fn streaming_scan_preserves_shared_inline_and_overflow_budgets() {
    let (manager, _, expected) = seeded(3);
    let large = expected[0].1.len();
    for (max_value, total, expected_emitted, expected_examined) in
        [(large - 1, large * 2, 0, 1), (large, large + 500, 1, 2)]
    {
        let mut reader = manager.begin_read();
        let budget = ReadBudget::new(max_value, total);
        reader.set_read_budget(Some(budget.clone()));
        let measurement = reader.measure_scans();
        let mut emitted = 0;
        let error = reader
            .table_scan_raw(TABLE, |_, _| {
                emitted += 1;
                true
            })
            .unwrap_err();
        assert!(matches!(error, Error::ReadBudgetExceeded { .. }));
        assert_eq!(emitted, expected_emitted);
        assert_eq!(measurement.rows_scanned(), expected_examined);
        assert_eq!(budget.remaining(), total - expected_emitted * large);
        assert!(reader.page_cache.is_empty());
    }
}

#[test]
fn streaming_scan_observes_cancel_after_branch_and_overflow_io() {
    let (manager, io, _) = seeded(64);
    let mut reader = manager.begin_read();
    let root = reader.lookup_table(TABLE).unwrap().root_page;
    let leaves = reader.collect_table_leaves(TABLE).unwrap();
    let first = leaf_node::read_cell(&leaves[0], 0);
    assert_eq!(first.val_type, ValueType::Overflow);
    let overflow = leaf_node::OverflowRef::from_bytes(first.value).first_page;
    drop(leaves);
    drop(reader);
    drop(manager);
    for (target, examined) in [(root, 0), (overflow, 1)] {
        let manager = io.open();
        let mut reader = manager.begin_read();
        reader.lookup_table(TABLE).unwrap();
        let token = CancelToken::new();
        reader.set_cancel(Some(token.clone()));
        io.trace.lock().unwrap().cancel_at = Some((page_offset(target), token));
        let measurement = reader.measure_scans();
        let result = reader.table_scan_raw(TABLE, |_, _| panic!("cancelled scan emitted"));
        assert!(matches!(result, Err(Error::Interrupted)));
        assert_eq!(measurement.rows_scanned(), examined);
        assert!(reader.page_cache.is_empty());
        io.trace.lock().unwrap().cancel_at = None;
    }
}

#[test]
fn empty_and_pre_cancelled_streaming_scans_do_not_emit_rows() {
    let (manager, io, _) = seeded(0);
    let mut reader = manager.begin_read();
    assert!(scan(&mut reader).is_empty());
    assert!(reader.page_cache.is_empty());
    let token = CancelToken::new();
    token.cancel();
    reader.set_cancel(Some(token));
    io.clear_reads();
    assert!(matches!(
        reader.table_scan_raw(TABLE, |_, _| panic!("cancelled scan emitted")),
        Err(Error::Interrupted)
    ));
    assert!(io.reads().is_empty());
}

#[test]
fn traversal_rejects_invalid_and_out_of_snapshot_children() {
    for invalid in [true, false] {
        let (manager, io, _) = seeded(64);
        let root = manager
            .begin_read()
            .table_root_page(TABLE)
            .unwrap()
            .unwrap();
        let outside = PageId(manager.current_slot().high_water_mark);
        let mut page = io.read_plain(root);
        assert_eq!(page.page_type(), Some(PageType::Branch));
        page.set_right_child(if invalid { PageId::INVALID } else { outside });
        io.rewrite(page);
        drop(manager);
        for collect in [false, true] {
            let manager = io.open();
            let mut reader = manager.begin_read();
            let result = if collect {
                reader.collect_table_leaves(TABLE).map(|_| ())
            } else {
                reader.table_scan_raw(TABLE, |_, _| true)
            };
            if invalid {
                assert!(matches!(result, Err(Error::DatabaseCorrupted)));
            } else {
                assert!(matches!(result, Err(Error::PageOutOfBounds(id)) if id == outside));
            }
        }
    }
}

#[test]
fn authenticated_cross_page_cycles_are_rejected_without_recursion() {
    const CHILD: &str = "CITADEL_STREAM_SCAN_CYCLE_CHILD";
    if let Ok(mode) = std::env::var(CHILD) {
        let (manager, io, _) = seeded(1);
        let root = manager
            .begin_read()
            .table_root_page(TABLE)
            .unwrap()
            .unwrap();
        let other = manager.current_slot().tree_root;
        assert_ne!(root, other);
        for (id, child) in [(root, other), (other, root)] {
            let mut page = io.read_plain(id);
            page.set_page_type(PageType::Branch);
            page.rebuild_cells(&[]);
            page.set_right_child(child);
            io.rewrite(page);
        }
        drop(manager);
        let manager = io.open();
        let mut reader = manager.begin_read();
        let result = if mode == "collect" {
            reader.collect_table_leaves(TABLE).map(|_| ())
        } else {
            reader.table_scan_raw(TABLE, |_, _| panic!("cycle contains no leaves"))
        };
        assert!(matches!(result, Err(Error::DatabaseCorrupted)));
        return;
    }

    // Bound malformed-page traversal so a stack overflow cannot abort the suite.
    let full_name = concat!(
        module_path!(),
        "::authenticated_cross_page_cycles_are_rejected_without_recursion"
    );
    for mode in ["raw", "collect"] {
        run_stream_child(full_name, CHILD, mode);
    }
}
