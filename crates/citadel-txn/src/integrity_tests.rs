use super::*;
use citadel_core::types::{PageType, SyncMode, ValueType};
use citadel_core::{
    Error, COMMIT_SLOT_OFFSET, COMMIT_SLOT_SIZE, PAGE_SIZE, SLOT_CHECKSUM, SLOT_ENTRY_STALE,
    SLOT_FORMAT_MARKER, SLOT_MAC,
};
use citadel_crypto::page_cipher;
use citadel_io::file_manager::{
    page_offset, table_name_hash, write_commit_slot, CommitSlot, MerkleScheme, SlotFormat,
};
use citadel_io::traits::PageIO;
use citadel_page::leaf_node::OverflowRef;
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node, overflow};

use crate::manager::tests::{test_keys, MemIO};

fn create_manager_with_raw_io() -> (TxnManager, MemIO) {
    create_manager_with_raw_io_and_sync(SyncMode::Full)
}

fn create_manager_with_raw_io_and_sync(sync_mode: SyncMode) -> (TxnManager, MemIO) {
    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let mgr = TxnManager::create_with_sync(
        Box::new(io.share()),
        dek,
        mac_key,
        1,
        0x1234,
        dek_id,
        256,
        sync_mode,
    )
    .unwrap();
    (mgr, io)
}

fn commit_value(mgr: &TxnManager, key: &[u8]) {
    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(key, b"value").unwrap();
    wtx.commit().unwrap();
}

fn raw_slots(mgr: &TxnManager) -> (usize, [CommitSlot; 2]) {
    let snapshot = mgr.integrity_snapshot().unwrap();
    (snapshot.active_slot(), snapshot.slots().clone())
}

fn flip_raw_byte(io: &MemIO, offset: usize) {
    let mut byte = [0u8; 1];
    io.read_at(offset as u64, &mut byte).unwrap();
    byte[0] ^= 0x80;
    io.write_at(offset as u64, &byte).unwrap();
}

fn write_encrypted_page(io: &MemIO, page: &Page, epoch: u32) {
    write_encrypted_page_at(io, page.page_id(), page, epoch);
}

fn write_encrypted_page_at(io: &MemIO, physical: PageId, page: &Page, epoch: u32) {
    let (dek, mac_key, _) = test_keys();
    let mut encrypted = [0u8; PAGE_SIZE];
    page_cipher::encrypt_page(
        &dek,
        &mac_key,
        physical,
        epoch,
        page.as_bytes(),
        &mut encrypted,
    );
    io.write_page(page_offset(physical), &encrypted).unwrap();
}

fn publish_pages_and_slot(io: &MemIO, active: usize, mut slot: CommitSlot, pages: &[Page]) {
    for page in pages {
        write_encrypted_page(io, page, slot.encryption_epoch);
        let next = page.page_id().as_u32() + 1;
        slot.total_pages = slot.total_pages.max(next);
        slot.high_water_mark = slot.high_water_mark.max(next);
    }
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(io, active, &slot).unwrap();
}

fn one_cell_leaf(
    page_id: PageId,
    txn_id: citadel_core::types::TxnId,
    key: &[u8],
    value: &[u8],
) -> Page {
    let mut page = Page::new(page_id, PageType::Leaf, txn_id);
    assert!(leaf_node::insert_append_direct(
        &mut page,
        key,
        ValueType::Inline,
        value,
    ));
    page.update_checksum();
    page
}

fn overflow_leaf(
    page_id: PageId,
    txn_id: citadel_core::types::TxnId,
    cells: &[(&[u8], OverflowRef)],
) -> Page {
    let mut page = Page::new(page_id, PageType::Leaf, txn_id);
    for (key, reference) in cells {
        assert!(leaf_node::insert_append_direct(
            &mut page,
            key,
            ValueType::Overflow,
            &reference.to_bytes(),
        ));
    }
    page.update_checksum();
    page
}

fn overflow_page(
    page_id: PageId,
    txn_id: citadel_core::types::TxnId,
    data: &[u8],
    next: PageId,
) -> Page {
    let mut page = Page::new(page_id, PageType::Overflow, txn_id);
    overflow::write_data(&mut page, data);
    overflow::set_next_page(&mut page, next);
    page.update_checksum();
    page
}

fn pending_free_page(
    page_id: PageId,
    txn_id: citadel_core::types::TxnId,
    entries: &[(PageId, citadel_core::types::TxnId)],
    next: PageId,
) -> Page {
    assert!(entries.len() <= PENDING_FREE_ENTRY_CAPACITY);
    let mut page = Page::new(page_id, PageType::PendingFree, txn_id);
    page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4]
        .copy_from_slice(&(entries.len() as u32).to_le_bytes());
    for (index, &(entry, freed_at)) in entries.iter().enumerate() {
        let offset = PAGE_HEADER_SIZE + 4 + index * citadel_core::PENDING_FREE_ENTRY_SIZE;
        page.data[offset..offset + 4].copy_from_slice(&entry.as_u32().to_le_bytes());
        page.data[offset + 4..offset + 12].copy_from_slice(&freed_at.as_u64().to_le_bytes());
    }
    page.set_right_child(next);
    page.update_checksum();
    page
}

fn replace_catalog_descriptor(page: &mut Page, table: &[u8], descriptor: &TableDescriptor) {
    let index = leaf_node::search(page, table).expect("table must exist in catalog");
    let offset = page.cell_offset(index) as usize;
    let key_len = u16::from_le_bytes(page.data[offset..offset + 2].try_into().unwrap()) as usize;
    let value_len = u32::from_le_bytes(page.data[offset + 2..offset + 6].try_into().unwrap());
    assert_eq!(value_len as usize, TABLE_DESCRIPTOR_SIZE);
    let value_start = offset + 6 + key_len + 1;
    page.data[value_start..value_start + TABLE_DESCRIPTOR_SIZE]
        .copy_from_slice(&descriptor.serialize());
}

fn one_separator_branch(
    page_id: PageId,
    txn_id: citadel_core::types::TxnId,
    left: PageId,
    separator: &[u8],
    right: PageId,
) -> Page {
    let mut page = Page::new(page_id, PageType::Branch, txn_id);
    assert!(page
        .write_cell(&branch_node::build_cell(left, separator))
        .is_some());
    page.set_right_child(right);
    page.update_checksum();
    page
}

fn committed_leaf_depths(mgr: &TxnManager, root: PageId) -> std::collections::BTreeSet<u32> {
    let mut depths = std::collections::BTreeSet::new();
    let mut stack = vec![(root, 1u32)];
    while let Some((page_id, depth)) = stack.pop() {
        let page = mgr.read_page_from_disk(page_id).unwrap();
        match page.page_type() {
            Some(PageType::Leaf) => {
                depths.insert(depth);
            }
            Some(PageType::Branch) => {
                for index in 0..page.num_cells() {
                    stack.push((branch_node::read_cell(&page, index).child, depth + 1));
                }
                if page.right_child().is_valid() {
                    stack.push((page.right_child(), depth + 1));
                }
            }
            other => panic!("tree walk reached unexpected page type {other:?}"),
        }
    }
    depths
}

/// Cancels on the first storage-page read, after integrity has acquired its
/// writer exclusion and passed the pre-walk cancellation checks.
struct CancelOnPageReadIO {
    inner: MemIO,
    token: citadel_core::CancelToken,
}

impl PageIO for CancelOnPageReadIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.token.cancel();
        self.inner.read_page(offset, buf)
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

struct CountingPageReadsIO {
    inner: MemIO,
    reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl PageIO for CountingPageReadsIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.read_page(offset, buf)
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

/// The precondition the both-slot walk rests on. Without this, the higher-level
/// "no invented duplicate pages" test would pass by never walking a second slot
/// at all.
///
/// `create` seals BOTH slots, so the spare is checksum-valid from birth while
/// rooting nothing. Sitting at txn 0 is what marks it as never committed; a
/// checksum test alone would hand the walk an empty slot to chase.
#[test]
fn a_committed_slot_is_what_gives_the_walk_a_second_slot() {
    let mgr = crate::manager::tests::create_test_manager();

    let (active, slots) = raw_slots(&mgr);
    assert_eq!(slots[1 - active].txn_id, citadel_core::types::TxnId(0));

    commit_value(&mgr, b"one");

    let (active, after_first) = raw_slots(&mgr);
    let first_active_txn = after_first[active].txn_id;
    assert_ne!(
        after_first[1 - active].txn_id,
        citadel_core::types::TxnId(0),
        "the first application commit retires the database's initial commit"
    );

    commit_value(&mgr, b"two");

    let (active, after_second) = raw_slots(&mgr);
    let inactive = 1 - active;

    assert_ne!(
        after_second[inactive].txn_id,
        citadel_core::types::TxnId(0),
        "two commits must leave a committed inactive slot to walk"
    );
    assert_ne!(
        after_second[inactive], after_second[active],
        "the inactive slot must differ from the active one, or the walk is skipped"
    );
    assert_eq!(
        first_active_txn, after_second[inactive].txn_id,
        "each commit must retire the previously active slot"
    );
}

#[test]
fn a_writer_and_an_integrity_snapshot_cannot_overlap() {
    let mgr = crate::manager::tests::create_test_manager();

    let snapshot = mgr.integrity_snapshot().unwrap();
    assert!(matches!(
        mgr.begin_write(),
        Err(Error::WriteTransactionActive)
    ));
    drop(snapshot);

    let writer = mgr.begin_write().unwrap();
    assert!(matches!(
        run_integrity_check(&mgr),
        Err(Error::WriteTransactionActive)
    ));
    drop(writer);

    assert!(run_integrity_check(&mgr).unwrap().is_ok());
}

#[test]
fn an_installed_token_cancels_the_integrity_walk_without_stranding_the_writer() {
    let token = citadel_core::CancelToken::new();
    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let mgr = TxnManager::create(
        Box::new(CancelOnPageReadIO {
            inner: io,
            token: token.clone(),
        }),
        dek,
        mac_key,
        1,
        0x1234,
        dek_id,
        256,
    )
    .unwrap();

    assert!(matches!(
        mgr.integrity_check_with_cancel(Some(&token)),
        Err(Error::Interrupted)
    ));

    // A cancelled inspection must not affect writer availability.
    mgr.begin_write().unwrap().abort();
}

#[test]
fn merkle_recomputation_polls_cancellation_before_hashing_pages() {
    let mgr = crate::manager::tests::create_test_manager();
    let page = Page::new(PageId(0), PageType::Leaf, citadel_core::types::TxnId(1));
    let cells = checked_leaf_cells(&page).unwrap();
    let mut merkle = MerkleWalk::new(PageId(0));
    let logical_hash = hash_checked_leaf_cells(&cells, &[]).unwrap();
    merkle.record_leaf(PageId(0), &page, logical_hash);
    let token = CancelToken::new();
    token.cancel();
    let mut errors = Vec::new();
    let mut pages_checked = 0;
    let mut ctx = WalkContext {
        mgr: &mgr,
        slot_txn: citadel_core::types::TxnId(1),
        high_water_mark: 1,
        verify_merkle: true,
        accept_missing_overflow_digest: false,
        errors: &mut errors,
        pages_checked: &mut pages_checked,
        cancel: Some(&token),
    };

    assert!(matches!(
        merkle.finish(&mut ctx, false),
        Err(Error::Interrupted)
    ));
}

#[test]
fn catalog_hash_collision_is_reported_without_a_slot_entry() {
    const FIRST: &[u8] = b"collision_table_51661";
    const SECOND: &[u8] = b"collision_table_134778";

    let (mgr, io) = create_manager_with_raw_io();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(FIRST).unwrap();
    writer
        .create_table_without_hash_guard_for_test(SECOND)
        .unwrap();
    writer.commit().unwrap();

    let hash = table_name_hash(FIRST);
    assert_eq!(hash, 0xab88_afb6);
    assert_eq!(table_name_hash(SECOND), hash);

    // Exercise the gap this regression guards: the full catalog names collide,
    // but no hash-only slot cache entry exists to make merge notice them.
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    slot.named_table_entries.retain(|entry| entry.0 != hash);
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.iter().any(|error| {
        matches!(
            error,
            IntegrityError::NamedTableHashCollision {
                table_hash,
                first_table,
                conflicting_table,
            } if *table_hash == hash
                && ((first_table == FIRST && conflicting_table == SECOND)
                    || (first_table == SECOND && conflicting_table == FIRST))
        )
    }));
}

#[test]
fn catalog_hash_collision_walks_catalog_roots_and_the_slot_root_once() {
    const FIRST: &[u8] = b"collision_table_51661";
    const SECOND: &[u8] = b"collision_table_134778";

    let (mgr, io) = create_manager_with_raw_io();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(FIRST).unwrap();
    writer
        .create_table_without_hash_guard_for_test(SECOND)
        .unwrap();
    writer
        .table_insert(FIRST, b"first-key", b"first-value")
        .unwrap();
    writer
        .table_insert(SECOND, b"second-key", b"second-value")
        .unwrap();
    writer.commit().unwrap();

    let hash = table_name_hash(FIRST);
    assert_eq!(table_name_hash(SECOND), hash);
    let tables = mgr.list_tables().unwrap();
    let first = &tables.iter().find(|(name, _)| name == FIRST).unwrap().1;
    let second = &tables.iter().find(|(name, _)| name == SECOND).unwrap().1;
    assert_ne!(first.root_page, second.root_page);

    // Give the colliding hash one authoritative runtime root. Integrity must
    // still walk both full-name catalog roots, but must not walk `first` twice
    // merely because the slot aliases that same root.
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    slot.named_table_entries.retain(|entry| entry.0 != hash);
    slot.named_table_entries.push((
        hash,
        first.entry_count,
        first.root_page.as_u32(),
        first.depth,
    ));
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    flip_raw_byte(&io, page_offset(second.root_page) as usize + 32);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::PageTampered(second.root_page)));
    assert!(!report
        .errors
        .contains(&IntegrityError::DuplicatePageRef(first.root_page)));
}

#[test]
fn a_reopened_legacy_collision_fails_closed_in_read_and_write_fast_paths() {
    const FIRST: &[u8] = b"collision_table_51661";
    const SECOND: &[u8] = b"collision_table_134778";

    let (mgr, io) = create_manager_with_raw_io();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(FIRST).unwrap();
    writer
        .create_table_without_hash_guard_for_test(SECOND)
        .unwrap();
    writer.table_insert(FIRST, b"key", b"first").unwrap();
    writer.table_insert(SECOND, b"key", b"second").unwrap();
    writer.commit().unwrap();
    drop(mgr);

    let (dek, mac_key, _) = test_keys();
    let reopened = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256).unwrap();

    for (requested, existing) in [(FIRST, SECOND), (SECOND, FIRST), (FIRST, SECOND)] {
        let mut reader = reopened.begin_read();
        assert!(matches!(
            reader.table_get(requested, b"key"),
            Err(Error::NamedTableHashCollision {
                requested: error_requested,
                existing: error_existing,
                hash: 0xab88_afb6,
            }) if error_requested.as_bytes() == requested && error_existing.as_bytes() == existing
        ));
    }
    let mut writer = reopened.begin_write().unwrap();
    assert!(matches!(
        writer.table_get(FIRST, b"key"),
        Err(Error::NamedTableHashCollision {
            requested,
            existing,
            hash: 0xab88_afb6,
        }) if requested.as_bytes() == FIRST && existing.as_bytes() == SECOND
    ));
    assert!(matches!(
        writer.drop_table(FIRST),
        Err(Error::NamedTableHashCollision { .. })
    ));
    assert!(matches!(
        writer.rename_table(FIRST, b"repaired_name"),
        Err(Error::NamedTableHashCollision { .. })
    ));
    writer.abort();
}

#[test]
fn opening_is_lazy_and_schema_listing_populates_the_collision_index() {
    let (mgr, io) = create_manager_with_raw_io();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"table").unwrap();
    writer.table_insert(b"table", b"key", b"value").unwrap();
    writer.commit().unwrap();
    drop(mgr);

    let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (dek, mac_key, _) = test_keys();
    let reopened = TxnManager::open(
        Box::new(CountingPageReadsIO {
            inner: io.share(),
            reads: std::sync::Arc::clone(&reads),
        }),
        dek,
        mac_key,
        1,
        1,
    )
    .unwrap();
    assert_eq!(reads.load(std::sync::atomic::Ordering::Relaxed), 0);

    assert_eq!(reopened.list_tables().unwrap().len(), 1);
    assert!(reads.load(std::sync::atomic::Ordering::Relaxed) > 0);
    reads.store(0, std::sync::atomic::Ordering::Relaxed);

    let mut reader = reopened.begin_read();
    assert_eq!(reader.table_entry_count(b"table").unwrap(), 1);
    assert!(reads.load(std::sync::atomic::Ordering::Relaxed) > 0);
    reads.store(0, std::sync::atomic::Ordering::Relaxed);

    // The transaction proves the exact catalog name once before trusting the
    // hash-only slot entry, then reuses that resolved descriptor.
    assert_eq!(reader.table_entry_count(b"table").unwrap(), 1);
    assert_eq!(reads.load(std::sync::atomic::Ordering::Relaxed), 0);
    drop(reader);

    let slot = reopened.current_slot();
    let root = slot.named_entry_root(b"table").unwrap().0;
    drop(
        reopened
            .fetch_reachable_page(root, slot.high_water_mark)
            .unwrap(),
    );
    assert_eq!(reads.load(std::sync::atomic::Ordering::Relaxed), 1);
    reads.store(0, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        reopened.begin_read().table_entry_count(b"table").unwrap(),
        1
    );
    assert_eq!(
        reads.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "a fresh reader must reuse the descriptor after the catalog page is evicted"
    );
}

#[test]
fn cold_catalog_resolutions_reuse_authenticated_buffer_pages() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let (mgr, io) = create_manager_with_raw_io();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"alpha").unwrap();
    writer.create_table(b"beta").unwrap();
    writer.commit().unwrap();
    drop(mgr);

    let reads = Arc::new(AtomicUsize::new(0));
    let (dek, mac_key, _) = test_keys();
    let reopened = TxnManager::open(
        Box::new(CountingPageReadsIO {
            inner: io.share(),
            reads: Arc::clone(&reads),
        }),
        dek,
        mac_key,
        1,
        8,
    )
    .unwrap();
    assert_eq!(reopened.list_tables().unwrap().len(), 2);
    let slot = reopened.current_slot();
    let catalog = reopened
        .fetch_reachable_page(slot.catalog_root, slot.high_water_mark)
        .unwrap();
    assert_eq!(catalog.page_type(), Some(PageType::Leaf));
    drop(catalog);
    reads.store(0, Ordering::Relaxed);

    for name in [b"alpha".as_slice(), b"beta".as_slice()] {
        assert_eq!(reopened.begin_read().table_entry_count(name).unwrap(), 0);
    }
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "first-time name resolutions must not re-read authenticated cached pages"
    );
}

#[test]
fn warm_catalog_cache_does_not_hide_damage_from_diagnostics() {
    let (mgr, io) = create_manager_with_raw_io();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"items").unwrap();
    writer.commit().unwrap();
    assert_eq!(mgr.begin_read().table_entry_count(b"items").unwrap(), 0);

    let root = mgr.current_slot().catalog_root;
    flip_raw_byte(&io, page_offset(root) as usize + PAGE_SIZE - 1);
    assert_eq!(mgr.begin_read().table_entry_count(b"items").unwrap(), 0);
    assert!(matches!(
        mgr.begin_read().list_tables(),
        Err(Error::PageTampered(_))
    ));
    assert!(!mgr.integrity_check().unwrap().is_ok());
}

#[test]
fn cold_missing_catalog_lookup_observes_cancellation_after_io() {
    let (mgr, io) = create_manager_with_raw_io();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"present").unwrap();
    writer.commit().unwrap();
    drop(mgr);

    let token = citadel_core::CancelToken::new();
    let (dek, mac_key, _) = test_keys();
    let reopened = TxnManager::open(
        Box::new(CancelOnPageReadIO {
            inner: io.share(),
            token: token.clone(),
        }),
        dek,
        mac_key,
        1,
        8,
    )
    .unwrap();
    assert!(!token.is_cancelled());
    let mut reader = reopened.begin_read();
    reader.set_cancel(Some(token.clone()));
    assert!(matches!(
        reader.table_entry_count(b"missing"),
        Err(Error::Interrupted)
    ));
    assert!(token.is_cancelled(), "catalog page read was not reached");
    assert!(matches!(
        reopened.begin_read().table_entry_count(b"missing"),
        Err(Error::TableNotFound(_))
    ));
}

#[test]
fn cancelling_lazy_collision_index_initialization_leaves_it_retryable() {
    let (mgr, io) = create_manager_with_raw_io();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"table").unwrap();
    writer.commit().unwrap();
    drop(mgr);

    let token = citadel_core::CancelToken::new();
    let (dek, mac_key, _) = test_keys();
    let reopened = TxnManager::open(
        Box::new(CancelOnPageReadIO {
            inner: io.share(),
            token: token.clone(),
        }),
        dek,
        mac_key,
        1,
        256,
    )
    .unwrap();
    let mut reader = reopened.begin_read();
    reader.set_cancel(Some(token));
    assert!(matches!(
        reader.table_entry_count(b"table"),
        Err(Error::Interrupted)
    ));
    drop(reader);

    // Interrupted initialization did not publish a partial/empty index.
    let mut retry = reopened.begin_read();
    assert_eq!(retry.table_entry_count(b"table").unwrap(), 0);
}

#[test]
fn both_committed_slots_can_share_pages_without_inventing_duplicates() {
    let mgr = crate::manager::tests::create_test_manager();

    for generation in 0..2u32 {
        let mut wtx = mgr.begin_write().unwrap();
        for key in 0..64u32 {
            let key = format!("k{generation}-{key}");
            wtx.insert(key.as_bytes(), b"value").unwrap();
        }
        wtx.commit().unwrap();
    }

    let report = run_integrity_check(&mgr).unwrap();
    assert!(
        report.is_ok(),
        "healthy shared pages were reported as corrupt: {:?}",
        report.errors
    );
    assert!(report.pages_checked > 0);
}

#[test]
fn an_off_mode_slot_override_exposes_corruption_in_the_current_named_root() {
    let (mgr, io) = create_manager_with_raw_io_and_sync(SyncMode::Off);

    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"t").unwrap();
    writer.table_insert(b"t", b"a", b"row-a").unwrap();
    writer.commit().unwrap();

    let mut writer = mgr.begin_write().unwrap();
    writer.table_insert(b"t", b"b", b"row-b").unwrap();
    writer.commit().unwrap();

    let catalog_desc = mgr
        .list_tables()
        .unwrap()
        .into_iter()
        .find(|(name, _)| name == b"t")
        .unwrap()
        .1;
    let (active, slots) = raw_slots(&mgr);
    let table_hash = table_name_hash(b"t");
    let &(hash, flagged_count, current_root, _) = slots[active]
        .named_table_entries
        .iter()
        .find(|&&(hash, ..)| hash == table_hash)
        .expect("Off-mode commit must carry the current table root");
    assert_eq!(hash, table_hash);
    assert_ne!(current_root, catalog_desc.root_page.as_u32());
    assert_ne!(flagged_count & SLOT_ENTRY_STALE, 0);

    let current_root = PageId(current_root);
    flip_raw_byte(&io, page_offset(current_root) as usize + 32);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(
        report
            .errors
            .contains(&IntegrityError::PageTampered(current_root)),
        "integrity followed the stale catalog descriptor instead of the authoritative slot root: {:?}",
        report.errors
    );
}

#[test]
fn a_slot_only_named_root_is_walked_and_its_count_is_checked() {
    let (mgr, io) = create_manager_with_raw_io();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"orphaned-from-catalog").unwrap();
    writer
        .table_insert(b"orphaned-from-catalog", b"one", b"row")
        .unwrap();
    writer.commit().unwrap();

    let (active, slots) = raw_slots(&mgr);
    let table_hash = table_name_hash(b"orphaned-from-catalog");
    let mut slot = slots[active].clone();
    slot.catalog_root = PageId::INVALID;
    let entry = slot
        .named_table_entries
        .iter_mut()
        .find(|entry| entry.0 == table_hash)
        .expect("committed table must have a slot entry");
    entry.1 = 7;
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::NamedTableEntryCountMismatch {
            table: None,
            table_hash,
            expected: 7,
            actual: 1,
        }));
}

#[test]
fn a_named_slot_entry_can_reuse_physical_page_zero() {
    let (mgr, io) = create_manager_with_raw_io();
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let table_hash = table_name_hash(b"page-zero");
    let default_root = PageId(slot.high_water_mark + 8);
    let named_root = one_cell_leaf(PageId(0), slot.txn_id, b"row", b"value");
    let mut default = Page::new(default_root, PageType::Leaf, slot.txn_id);
    default.update_checksum();

    slot.tree_root = default_root;
    slot.tree_depth = 1;
    slot.tree_entries = 0;
    slot.catalog_root = PageId::INVALID;
    slot.pending_free_root = PageId::INVALID;
    slot.named_table_entries = vec![(table_hash, 0, 0, 1)];
    slot.merkle_root = [0u8; citadel_core::MERKLE_HASH_SIZE];
    publish_pages_and_slot(&io, active, slot, &[named_root, default]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(
        report
            .errors
            .contains(&IntegrityError::NamedTableEntryCountMismatch {
                table: None,
                table_hash,
                expected: 0,
                actual: 1,
            }),
        "(root=0, depth=1) was mistaken for the absent cache sentinel: {:?}",
        report.errors
    );
}

#[test]
fn a_short_catalog_descriptor_is_reported_without_panicking() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let fake_catalog = PageId(slot.high_water_mark + 8);
    let mut page = Page::new(fake_catalog, PageType::Leaf, slot.txn_id);
    assert!(leaf_node::insert_append_direct(
        &mut page,
        b"short",
        ValueType::Inline,
        &[1, 2, 3, 4],
    ));
    page.update_checksum();
    write_encrypted_page(&io, &page, slot.encryption_epoch);

    slot.catalog_root = fake_catalog;
    slot.total_pages = slot.total_pages.max(fake_catalog.as_u32() + 1);
    slot.high_water_mark = slot.high_water_mark.max(fake_catalog.as_u32() + 1);
    slot.named_table_entries.clear();
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::MalformedTableDescriptor {
            page: fake_catalog,
            table: b"short".to_vec(),
            value_type: ValueType::Inline,
            actual_size: 4,
        }));

    // Runtime catalog consumers fail closed too: schema load, exact root
    // lookup, compaction's reachability walk, and both transaction lookups.
    drop(mgr);
    let (dek, mac_key, _) = test_keys();
    let reopened = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256).unwrap();
    assert!(matches!(
        reopened.list_tables(),
        Err(Error::DatabaseCorrupted)
    ));
    assert!(matches!(
        reopened.table_root(b"short"),
        Err(Error::DatabaseCorrupted)
    ));
    let compacted = MemIO::new(1024 * 1024);
    assert!(matches!(
        reopened.compact_to(&compacted),
        Err(Error::DatabaseCorrupted)
    ));

    let mut reader = reopened.begin_read();
    assert!(matches!(
        reader.table_entry_count(b"short"),
        Err(Error::DatabaseCorrupted)
    ));
    drop(reader);
    let mut writer = reopened.begin_write().unwrap();
    assert!(matches!(
        writer.table_get(b"short", b"key"),
        Err(Error::DatabaseCorrupted)
    ));
    writer.abort();
}

#[test]
fn an_overflow_catalog_cell_is_not_interpreted_as_a_descriptor() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let fake_catalog = PageId(slot.high_water_mark + 8);
    let mut page = Page::new(fake_catalog, PageType::Leaf, slot.txn_id);
    assert!(leaf_node::insert_append_direct(
        &mut page,
        b"overflow",
        ValueType::Overflow,
        &[0; TABLE_DESCRIPTOR_SIZE],
    ));
    page.update_checksum();
    write_encrypted_page(&io, &page, slot.encryption_epoch);

    slot.catalog_root = fake_catalog;
    slot.total_pages = slot.total_pages.max(fake_catalog.as_u32() + 1);
    slot.high_water_mark = slot.high_water_mark.max(fake_catalog.as_u32() + 1);
    slot.named_table_entries.clear();
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::MalformedTableDescriptor {
            page: fake_catalog,
            table: b"overflow".to_vec(),
            value_type: ValueType::Overflow,
            actual_size: TABLE_DESCRIPTOR_SIZE,
        }));
}

#[test]
fn uncached_catalog_descriptors_with_impossible_tree_metadata_are_reported() {
    let (mgr, io) = create_manager_with_raw_io();
    let names: Vec<Vec<u8>> = (0..8)
        .map(|index| format!("descriptor-{index}").into_bytes())
        .collect();
    let mut writer = mgr.begin_write().unwrap();
    for name in &names {
        writer.create_table(name).unwrap();
    }
    writer.commit().unwrap();

    let slot = mgr.current_slot();
    assert_eq!(
        slot.named_table_entries.len(),
        citadel_core::SLOT_NAMED_MAX_ENTRIES_V1,
        "fixture must exceed the authenticated slot cache"
    );
    let uncached: Vec<&[u8]> = names
        .iter()
        .map(Vec::as_slice)
        .filter(|name| {
            let hash = table_name_hash(name);
            slot.named_table_entries.iter().all(|entry| entry.0 != hash)
        })
        .collect();
    assert!(
        uncached.len() >= 2,
        "fixture needs two uncached descriptors"
    );

    let mut catalog = mgr.read_page_from_disk(slot.catalog_root).unwrap();
    assert_eq!(catalog.page_type(), Some(PageType::Leaf));
    let mut invalid_root = {
        let index = leaf_node::search(&catalog, uncached[0]).unwrap();
        TableDescriptor::deserialize(leaf_node::read_cell(&catalog, index).value)
    };
    invalid_root.root_page = PageId::INVALID;
    replace_catalog_descriptor(&mut catalog, uncached[0], &invalid_root);

    let mut zero_depth = {
        let index = leaf_node::search(&catalog, uncached[1]).unwrap();
        TableDescriptor::deserialize(leaf_node::read_cell(&catalog, index).value)
    };
    zero_depth.depth = 0;
    replace_catalog_descriptor(&mut catalog, uncached[1], &zero_depth);
    catalog.update_checksum();
    write_encrypted_page(&io, &catalog, slot.encryption_epoch);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::InvalidTableDescriptor {
            page: slot.catalog_root,
            table: uncached[0].to_vec(),
            detail: "root page is invalid",
        }));
    assert!(report
        .errors
        .contains(&IntegrityError::InvalidTableDescriptor {
            page: slot.catalog_root,
            table: uncached[1].to_vec(),
            detail: "tree depth is zero",
        }));
}

#[test]
fn catalog_key_ordering_is_checked() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let fake_catalog = PageId(slot.high_water_mark + 8);
    let mut page = Page::new(fake_catalog, PageType::Leaf, slot.txn_id);
    let descriptor = TableDescriptor {
        root_page: PageId::INVALID,
        entry_count: 0,
        depth: 0,
        flags: 0,
    }
    .serialize();
    assert!(leaf_node::insert_append_direct(
        &mut page,
        b"z-last",
        ValueType::Inline,
        &descriptor,
    ));
    assert!(leaf_node::insert_append_direct(
        &mut page,
        b"a-first",
        ValueType::Inline,
        &descriptor,
    ));
    page.update_checksum();
    write_encrypted_page(&io, &page, slot.encryption_epoch);

    slot.catalog_root = fake_catalog;
    slot.total_pages = slot.total_pages.max(fake_catalog.as_u32() + 1);
    slot.high_water_mark = slot.high_water_mark.max(fake_catalog.as_u32() + 1);
    slot.named_table_entries.clear();
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(
        report.errors.contains(&IntegrityError::KeyOrderViolation {
            page: fake_catalog,
            index: 1,
        }),
        "catalog leaf ordering was not checked: {:?}",
        report.errors
    );
}

#[test]
fn branch_separator_ordering_is_checked() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let base = slot.high_water_mark + 8;
    let left = Page::new(PageId(base), PageType::Leaf, slot.txn_id);
    let middle = Page::new(PageId(base + 1), PageType::Leaf, slot.txn_id);
    let right = Page::new(PageId(base + 2), PageType::Leaf, slot.txn_id);
    let root_id = PageId(base + 3);
    let mut root = Page::new(root_id, PageType::Branch, slot.txn_id);
    assert!(root
        .write_cell(&branch_node::build_cell(left.page_id(), b"m"))
        .is_some());
    assert!(root
        .write_cell(&branch_node::build_cell(middle.page_id(), b"g"))
        .is_some());
    root.set_right_child(right.page_id());
    root.update_checksum();

    slot.tree_root = root_id;
    slot.tree_entries = 0;
    slot.tree_depth = 2;
    publish_pages_and_slot(&io, active, slot, &[left, middle, right, root]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.contains(&IntegrityError::KeyOrderViolation {
        page: root_id,
        index: 1,
    }));
}

#[test]
fn healthy_multi_leaf_default_and_named_trees_satisfy_parent_ranges() {
    let mgr = crate::manager::tests::create_test_manager();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"named").unwrap();
    let value = [b'v'; 1800];
    for index in 0..64 {
        let key = format!("key-{index:04}");
        writer.insert(key.as_bytes(), &value).unwrap();
        writer
            .table_insert(b"named", key.as_bytes(), &value)
            .unwrap();
    }
    writer.commit().unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(
        report.is_ok(),
        "healthy multi-leaf trees violated parent bounds: {:?}",
        report.errors
    );
}

#[test]
fn default_and_named_walk_capacity_must_reach_the_deepest_leaf() {
    let (mgr, io) = create_manager_with_raw_io();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"named-depth").unwrap();
    writer.commit().unwrap();

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let actual_default_depth = slot.tree_depth;
    assert!(actual_default_depth > 0);
    let too_small_default_depth = actual_default_depth - 1;
    slot.tree_depth = too_small_default_depth;
    let named_hash = table_name_hash(b"named-depth");
    let named_entry = slot
        .named_table_entries
        .iter_mut()
        .find(|entry| entry.0 == named_hash)
        .unwrap();
    let named_root = PageId(named_entry.2);
    named_entry.3 = 0;
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.iter().any(|error| matches!(
        error,
        IntegrityError::TreeDepthMismatch {
            root,
            expected,
            actual,
            ..
        } if *root == slot.tree_root
            && *expected == too_small_default_depth
            && *actual == u32::from(actual_default_depth)
    )));
    assert!(report.errors.contains(&IntegrityError::TreeDepthMismatch {
        root: named_root,
        leaf: named_root,
        expected: 0,
        actual: 1,
    }));
}

#[test]
fn a_tree_deeper_than_its_walk_capacity_reports_a_deepest_leaf() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let base = slot.high_water_mark + 8;
    let shallow = Page::new(PageId(base), PageType::Leaf, slot.txn_id);
    let deep_left = Page::new(PageId(base + 1), PageType::Leaf, slot.txn_id);
    let deep_right = Page::new(PageId(base + 2), PageType::Leaf, slot.txn_id);
    let inner = one_separator_branch(
        PageId(base + 3),
        slot.txn_id,
        deep_left.page_id(),
        b"t",
        deep_right.page_id(),
    );
    let root = one_separator_branch(
        PageId(base + 4),
        slot.txn_id,
        shallow.page_id(),
        b"m",
        inner.page_id(),
    );
    slot.tree_root = root.page_id();
    slot.tree_depth = 2;
    slot.tree_entries = 0;
    slot.merkle_root = [0u8; citadel_core::MERKLE_HASH_SIZE];
    publish_pages_and_slot(
        &io,
        active,
        slot,
        &[shallow, deep_left.clone(), deep_right.clone(), inner, root],
    );

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.iter().any(|error| matches!(
        error,
        IntegrityError::TreeDepthMismatch {
            root,
            leaf,
            expected: 2,
            actual: 3,
        } if *root == PageId(base + 4)
            && [deep_left.page_id(), deep_right.page_id()].contains(leaf)
    )));
}

#[test]
fn a_writer_spliced_short_path_does_not_violate_depth_capacity() {
    let mgr = crate::manager::tests::create_test_manager();
    let make_key = |index: u32| {
        let mut key = format!("{index:04}").into_bytes();
        key.resize(2000, b'k');
        key
    };
    let keys: Vec<Vec<u8>> = (0..40).map(make_key).collect();

    let mut writer = mgr.begin_write().unwrap();
    for key in &keys {
        writer.insert(key, b"v").unwrap();
    }
    writer.commit().unwrap();
    assert!(mgr.current_slot().tree_depth >= 3);

    for key in keys.iter().take(keys.len() - 1) {
        let mut writer = mgr.begin_write().unwrap();
        assert!(writer.delete(key).unwrap());
        writer.commit().unwrap();

        let slot = mgr.current_slot();
        let depths = committed_leaf_depths(&mgr, slot.tree_root);
        if depths.len() < 2 {
            continue;
        }
        let deepest = depths.iter().next_back().copied().unwrap();
        assert!(
            deepest <= u32::from(slot.tree_depth),
            "writer depth {} does not cover leaf depths {depths:?}",
            slot.tree_depth
        );
        let report = run_integrity_check(&mgr).unwrap();
        assert!(
            report.is_ok(),
            "a healthy writer-produced unbalanced tree was rejected: {:?}",
            report.errors
        );
        return;
    }

    panic!("wide-key deletion fixture never produced unequal leaf depths");
}

#[test]
fn swapped_main_tree_children_are_outside_their_separator_ranges() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let base = slot.high_water_mark + 8;
    let low = one_cell_leaf(PageId(base), slot.txn_id, b"a", b"low");
    let high = one_cell_leaf(PageId(base + 1), slot.txn_id, b"z", b"high");
    let root_id = PageId(base + 2);
    let root = one_separator_branch(root_id, slot.txn_id, high.page_id(), b"m", low.page_id());

    slot.tree_root = root_id;
    slot.tree_entries = 2;
    slot.tree_depth = 2;
    publish_pages_and_slot(&io, active, slot, &[low.clone(), high.clone(), root]);

    let report = run_integrity_check(&mgr).unwrap();
    for page in [low.page_id(), high.page_id()] {
        assert!(
            report
                .errors
                .contains(&IntegrityError::KeyRangeViolation { page, index: 0 }),
            "swapped main-tree child {page} escaped range validation: {:?}",
            report.errors
        );
    }
}

#[test]
fn swapped_named_tree_children_are_outside_their_separator_ranges() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let base = slot.high_water_mark + 8;
    let low = one_cell_leaf(PageId(base), slot.txn_id, b"a", b"low");
    let high = one_cell_leaf(PageId(base + 1), slot.txn_id, b"z", b"high");
    let root_id = PageId(base + 2);
    let root = one_separator_branch(root_id, slot.txn_id, high.page_id(), b"m", low.page_id());
    let table_hash = table_name_hash(b"swapped");
    slot.named_table_entries = vec![(table_hash, 2, root_id.as_u32(), 2)];
    publish_pages_and_slot(&io, active, slot, &[low.clone(), high.clone(), root]);

    let report = run_integrity_check(&mgr).unwrap();
    for page in [low.page_id(), high.page_id()] {
        assert!(
            report
                .errors
                .contains(&IntegrityError::KeyRangeViolation { page, index: 0 }),
            "swapped named-tree child {page} escaped range validation: {:?}",
            report.errors
        );
    }
}

#[test]
fn swapped_catalog_children_are_outside_their_separator_ranges() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let descriptor = TableDescriptor {
        root_page: PageId::INVALID,
        entry_count: 0,
        depth: 0,
        flags: 0,
    }
    .serialize();
    let base = slot.high_water_mark + 8;
    let low = one_cell_leaf(PageId(base), slot.txn_id, b"a", &descriptor);
    let high = one_cell_leaf(PageId(base + 1), slot.txn_id, b"z", &descriptor);
    let root_id = PageId(base + 2);
    let root = one_separator_branch(root_id, slot.txn_id, high.page_id(), b"m", low.page_id());

    slot.catalog_root = root_id;
    slot.named_table_entries.clear();
    publish_pages_and_slot(&io, active, slot, &[low.clone(), high.clone(), root]);

    let report = run_integrity_check(&mgr).unwrap();
    for page in [low.page_id(), high.page_id()] {
        assert!(
            report
                .errors
                .contains(&IntegrityError::KeyRangeViolation { page, index: 0 }),
            "swapped catalog child {page} escaped range validation: {:?}",
            report.errors
        );
    }
}

#[test]
fn an_impossible_branch_cell_count_is_reported_without_panicking() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let page_id = PageId(slot.high_water_mark + 8);
    let mut page = Page::new(page_id, PageType::Branch, slot.txn_id);
    page.set_num_cells(u16::MAX);
    page.update_checksum();
    slot.tree_root = page_id;
    slot.tree_entries = 0;
    publish_pages_and_slot(&io, active, slot, &[page]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.contains(&IntegrityError::MalformedPage {
        page: page_id,
        detail: "cell pointer array exceeds the page body",
    }));
}

#[test]
fn a_leaf_cell_offset_outside_the_cell_area_is_reported_without_panicking() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let page_id = PageId(slot.high_water_mark + 8);
    let mut page = Page::new(page_id, PageType::Leaf, slot.txn_id);
    page.set_num_cells(1);
    page.set_cell_area_start((BODY_SIZE - 16) as u16);
    page.set_cell_offset(0, PAGE_HEADER_SIZE as u16);
    page.update_checksum();
    slot.tree_root = page_id;
    slot.tree_entries = 0;
    publish_pages_and_slot(&io, active, slot, &[page]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.contains(&IntegrityError::MalformedPage {
        page: page_id,
        detail: "cell offset lies outside the cell area",
    }));
}

#[test]
fn a_truncated_leaf_cell_is_reported_without_panicking() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let page_id = PageId(slot.high_water_mark + 8);
    let mut page = Page::new(page_id, PageType::Leaf, slot.txn_id);
    page.set_num_cells(1);
    page.set_cell_area_start((BODY_SIZE - 2) as u16);
    page.set_cell_offset(0, (BODY_SIZE - 2) as u16);
    page.update_checksum();
    slot.tree_root = page_id;
    slot.tree_entries = 0;
    publish_pages_and_slot(&io, active, slot, &[page]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.contains(&IntegrityError::MalformedPage {
        page: page_id,
        detail: "leaf cell header exceeds the page body",
    }));
}

fn assert_forged_free_space_is_reported(delta: i32) {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let root = PageId(slot.high_water_mark + 8);
    let mut leaf = one_cell_leaf(root, slot.txn_id, b"key", b"value");
    let forged = i32::from(leaf.free_space()) + delta;
    leaf.set_free_space(u16::try_from(forged).unwrap());
    leaf.update_checksum();
    slot.tree_root = root;
    slot.tree_entries = 1;
    publish_pages_and_slot(&io, active, slot, &[leaf]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.contains(&IntegrityError::MalformedPage {
        page: root,
        detail: "free-space accounting does not match live cells",
    }));
}

#[test]
fn forged_too_small_free_space_is_reported() {
    assert_forged_free_space_is_reported(-1);
}

#[test]
fn forged_too_large_free_space_is_reported() {
    assert_forged_free_space_is_reported(1);
}

#[test]
fn an_embedded_page_id_must_match_its_physical_location() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let physical = PageId(slot.high_water_mark + 8);
    let embedded = PageId(physical.as_u32() + 1);
    let page = Page::new(embedded, PageType::Leaf, slot.txn_id);
    write_encrypted_page_at(&io, physical, &page, slot.encryption_epoch);
    slot.tree_root = physical;
    slot.tree_entries = 0;
    slot.total_pages = physical.as_u32() + 1;
    slot.high_water_mark = physical.as_u32() + 1;
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.contains(&IntegrityError::PageIdMismatch {
        expected: physical,
        actual: embedded,
    }));
}

#[test]
fn a_page_from_a_future_transaction_cannot_bypass_cow_validation() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let root = PageId(slot.high_water_mark + 8);
    let future = citadel_core::types::TxnId(slot.txn_id.as_u64() + 1);
    let page = Page::new(root, PageType::Leaf, future);
    slot.tree_root = root;
    slot.tree_entries = 0;
    publish_pages_and_slot(&io, active, slot.clone(), &[page]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::PageTransactionOutOfBounds {
            page: root,
            page_txn: future,
            slot_txn: slot.txn_id,
        }));
}

#[test]
fn a_physically_present_branch_child_beyond_high_water_is_not_trusted() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let root = PageId(slot.high_water_mark + 8);
    let right = PageId(root.as_u32() + 1);
    let outside = PageId(root.as_u32() + 20);
    let root_page = one_separator_branch(root, slot.txn_id, outside, b"m", right);
    let right_page = Page::new(right, PageType::Leaf, slot.txn_id);
    let outside_page = Page::new(outside, PageType::Leaf, slot.txn_id);
    for page in [&root_page, &right_page, &outside_page] {
        write_encrypted_page(&io, page, slot.encryption_epoch);
    }
    slot.tree_root = root;
    slot.tree_entries = 0;
    slot.total_pages = right.as_u32() + 1;
    slot.high_water_mark = right.as_u32() + 1;
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::ReachablePageOutOfBounds {
            page: outside,
            high_water_mark: slot.high_water_mark,
        }));
}

#[test]
fn a_physically_present_overflow_tail_beyond_high_water_is_not_trusted() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let root = PageId(slot.high_water_mark + 8);
    let first = PageId(root.as_u32() + 1);
    let outside = PageId(root.as_u32() + 20);
    let leaf = overflow_leaf(
        root,
        slot.txn_id,
        &[(
            b"key",
            OverflowRef {
                first_page: first,
                total_len: 2,
            },
        )],
    );
    let first_page = overflow_page(first, slot.txn_id, b"a", outside);
    let outside_page = overflow_page(outside, slot.txn_id, b"b", PageId(0));
    for page in [&leaf, &first_page, &outside_page] {
        write_encrypted_page(&io, page, slot.encryption_epoch);
    }
    slot.tree_root = root;
    slot.tree_entries = 1;
    slot.total_pages = first.as_u32() + 1;
    slot.high_water_mark = first.as_u32() + 1;
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::ReachablePageOutOfBounds {
            page: outside,
            high_water_mark: slot.high_water_mark,
        }));
}

#[test]
fn a_page_merkle_header_is_recomputed_from_checked_cells() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let slot = mgr.current_slot();
    let mut root = mgr.read_page_from_disk(slot.tree_root).unwrap();
    let expected = root.merkle_hash();
    let forged = [0xabu8; citadel_core::MERKLE_HASH_SIZE];
    assert_ne!(expected, forged);
    root.set_merkle_hash(&forged);
    root.update_checksum();
    write_encrypted_page(&io, &root, slot.encryption_epoch);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.contains(&IntegrityError::PageMerkleMismatch {
        page: slot.tree_root,
        expected,
        actual: forged,
    }));
    assert!(
        !report
            .errors
            .iter()
            .any(|error| matches!(error, IntegrityError::SlotMerkleRootMismatch { .. })),
        "changing only the page header must not change the logical root"
    );
}

#[test]
fn a_nonzero_slot_root_does_not_exempt_a_zero_root_page_hash() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let slot = mgr.current_slot();
    assert_ne!(slot.merkle_root, [0u8; citadel_core::MERKLE_HASH_SIZE]);

    let mut root = mgr.read_page_from_disk(slot.tree_root).unwrap();
    let expected = root.merkle_hash();
    assert_eq!(expected, slot.merkle_root);
    root.set_merkle_hash(&[0u8; citadel_core::MERKLE_HASH_SIZE]);
    root.update_checksum();
    write_encrypted_page(&io, &root, slot.encryption_epoch);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.contains(&IntegrityError::PageMerkleMismatch {
        page: slot.tree_root,
        expected,
        actual: [0u8; citadel_core::MERKLE_HASH_SIZE],
    }));
}

#[test]
fn a_nonzero_tree_root_does_not_exempt_a_zero_descendant_hash() {
    let (mgr, io) = create_manager_with_raw_io();
    let value = [b'v'; 1800];
    let mut writer = mgr.begin_write().unwrap();
    for index in 0..64 {
        let key = format!("k-{index:04}");
        writer.insert(key.as_bytes(), &value).unwrap();
    }
    writer.commit().unwrap();

    let slot = mgr.current_slot();
    assert!(slot.tree_depth > 1);
    assert_ne!(slot.merkle_root, [0u8; citadel_core::MERKLE_HASH_SIZE]);
    let root = mgr.read_page_from_disk(slot.tree_root).unwrap();
    let child_id = branch_node::get_child(&root, 0);
    let mut child = mgr.read_page_from_disk(child_id).unwrap();
    let expected = child.merkle_hash();
    assert_ne!(expected, [0u8; citadel_core::MERKLE_HASH_SIZE]);
    child.set_merkle_hash(&[0u8; citadel_core::MERKLE_HASH_SIZE]);
    child.update_checksum();
    write_encrypted_page(&io, &child, slot.encryption_epoch);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.contains(&IntegrityError::PageMerkleMismatch {
        page: child_id,
        expected,
        actual: [0u8; citadel_core::MERKLE_HASH_SIZE],
    }));
}

#[test]
fn the_slot_merkle_root_is_compared_to_recomputed_contents() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let expected = slot.merkle_root;
    let forged = [0xcdu8; citadel_core::MERKLE_HASH_SIZE];
    assert_ne!(expected, forged);
    slot.merkle_root = forged;
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::SlotMerkleRootMismatch {
            expected,
            actual: forged,
        }));
}

#[test]
fn named_and_catalog_page_merkle_headers_are_recomputed_too() {
    let (mgr, io) = create_manager_with_raw_io();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"merkle-named").unwrap();
    writer.commit().unwrap();
    let slot = mgr.current_slot();
    let named_hash = table_name_hash(b"merkle-named");
    let named_root = PageId(
        slot.named_table_entries
            .iter()
            .find(|entry| entry.0 == named_hash)
            .unwrap()
            .2,
    );
    let mut catalog = mgr.read_page_from_disk(slot.catalog_root).unwrap();
    let mut named = mgr.read_page_from_disk(named_root).unwrap();
    let expected_catalog = catalog.merkle_hash();
    let expected_named = named.merkle_hash();
    let forged_catalog = [0x31u8; citadel_core::MERKLE_HASH_SIZE];
    let forged_named = [0x32u8; citadel_core::MERKLE_HASH_SIZE];
    catalog.set_merkle_hash(&forged_catalog);
    catalog.update_checksum();
    named.set_merkle_hash(&forged_named);
    named.update_checksum();
    write_encrypted_page(&io, &catalog, slot.encryption_epoch);
    write_encrypted_page(&io, &named, slot.encryption_epoch);

    let report = run_integrity_check(&mgr).unwrap();
    for finding in [
        IntegrityError::PageMerkleMismatch {
            page: slot.catalog_root,
            expected: expected_catalog,
            actual: forged_catalog,
        },
        IntegrityError::PageMerkleMismatch {
            page: named_root,
            expected: expected_named,
            actual: forged_named,
        },
    ] {
        assert!(report.errors.contains(&finding), "missing {finding:?}");
    }
}

#[test]
fn an_all_zero_off_mode_merkle_root_explicitly_exempts_page_hashes() {
    let (mgr, io) = create_manager_with_raw_io_and_sync(SyncMode::Off);
    commit_value(&mgr, b"default-row");
    let slot = mgr.current_slot();
    assert_eq!(slot.merkle_root, [0u8; citadel_core::MERKLE_HASH_SIZE]);
    let mut root = mgr.read_page_from_disk(slot.tree_root).unwrap();
    root.set_merkle_hash(&[0x55u8; citadel_core::MERKLE_HASH_SIZE]);
    root.update_checksum();
    write_encrypted_page(&io, &root, slot.encryption_epoch);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(!report.errors.iter().any(|error| matches!(
        error,
        IntegrityError::PageMerkleMismatch { page, .. } if *page == slot.tree_root
    )));
    assert!(!report
        .errors
        .iter()
        .any(|error| matches!(error, IntegrityError::SlotMerkleRootMismatch { .. })));
}

#[test]
fn full_mode_after_off_exempts_unknown_named_and_catalog_pages() {
    let (off, io) = create_manager_with_raw_io_and_sync(SyncMode::Off);
    let value = [b'v'; 1800];
    let mut writer = off.begin_write().unwrap();
    writer.create_table(b"unknown-named").unwrap();
    writer
        .table_insert(b"unknown-named", b"row", b"value")
        .unwrap();
    writer.insert(b"off-default", &value).unwrap();
    writer.commit().unwrap();
    assert_eq!(
        off.current_slot().merkle_root,
        [0u8; citadel_core::MERKLE_HASH_SIZE]
    );
    drop(off);

    let (dek, mac_key, _) = test_keys();
    let full =
        TxnManager::open_with_sync(Box::new(io.share()), dek, mac_key, 1, 256, SyncMode::Full)
            .unwrap();
    let mut writer = full.begin_write().unwrap();
    writer.insert(b"zzzz", &value).unwrap();
    writer.commit().unwrap();

    let slot = full.current_slot();
    assert_ne!(
        slot.merkle_root,
        [0u8; citadel_core::MERKLE_HASH_SIZE],
        "the Full commit must establish a nonzero root"
    );
    let root = full.read_page_from_disk(slot.tree_root).unwrap();
    assert_ne!(root.merkle_hash(), [0u8; citadel_core::MERKLE_HASH_SIZE]);
    assert_eq!(
        full.read_page_from_disk(slot.catalog_root)
            .unwrap()
            .merkle_hash(),
        [0u8; citadel_core::MERKLE_HASH_SIZE]
    );
    let named_hash = table_name_hash(b"unknown-named");
    let named_root = PageId(
        slot.named_table_entries
            .iter()
            .find(|entry| entry.0 == named_hash)
            .unwrap()
            .2,
    );
    assert_eq!(
        full.read_page_from_disk(named_root).unwrap().merkle_hash(),
        [0u8; citadel_core::MERKLE_HASH_SIZE]
    );

    let report = run_integrity_check(&full).unwrap();
    assert!(
        report.is_ok(),
        "UNKNOWN named/catalog pages are not proof of a Merkle mismatch: {:?}",
        report.errors
    );
}

#[test]
fn an_unknown_default_child_keeps_the_slot_merkle_root_unknown() {
    let (off, io) = create_manager_with_raw_io_and_sync(SyncMode::Off);
    let value = [b'v'; 1800];
    let mut writer = off.begin_write().unwrap();
    for index in 0..64 {
        let key = format!("k-{index:04}");
        writer.insert(key.as_bytes(), &value).unwrap();
    }
    writer.commit().unwrap();
    assert!(off.current_slot().tree_depth > 1);
    drop(off);

    let (dek, mac_key, _) = test_keys();
    let full =
        TxnManager::open_with_sync(Box::new(io.share()), dek, mac_key, 1, 256, SyncMode::Full)
            .unwrap();
    let mut writer = full.begin_write().unwrap();
    writer.insert(b"zzzz", &value).unwrap();
    writer.commit().unwrap();

    let slot = full.current_slot();
    assert_eq!(
        slot.merkle_root,
        [0u8; citadel_core::MERKLE_HASH_SIZE],
        "a partially known tree must not publish a certifying root"
    );
    assert_eq!(
        full.read_page_from_disk(slot.tree_root)
            .unwrap()
            .merkle_hash(),
        [0u8; citadel_core::MERKLE_HASH_SIZE]
    );
    let report = run_integrity_check(&full).unwrap();
    assert!(
        report.is_ok(),
        "UNKNOWN root metadata is exempt rather than a mismatch: {:?}",
        report.errors
    );
}

#[test]
fn total_pages_must_equal_high_water_but_slack_need_not_be_reachable() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let original = slots[active].clone();
    let mut mismatched = original.clone();
    mismatched.total_pages -= 1;
    let (_, mac_key, _) = test_keys();
    mismatched.seal(&mac_key);
    write_commit_slot(&io, active, &mismatched).unwrap();
    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::PageCountMetadataMismatch {
            total_pages: mismatched.total_pages,
            high_water_mark: mismatched.high_water_mark,
        }));

    let mut with_slack = original;
    with_slack.total_pages += 10;
    with_slack.high_water_mark += 10;
    with_slack.seal(&mac_key);
    write_commit_slot(&io, active, &with_slack).unwrap();
    let report = run_integrity_check(&mgr).unwrap();
    assert!(!report
        .errors
        .iter()
        .any(|error| matches!(error, IntegrityError::PageCountMetadataMismatch { .. })));
}

#[test]
fn healthy_overflow_and_pending_free_pages_are_walked_per_slot() {
    let mgr = crate::manager::tests::create_test_manager();
    let large = vec![b'x'; citadel_core::MAX_INLINE_VALUE_SIZE + 17];

    let mut writer = mgr.begin_write().unwrap();
    writer.insert(b"overflow", &large).unwrap();
    writer.commit().unwrap();

    // This commit copies the overflow reference into a new root. The inactive
    // slot still reaches the same chain, and the active pending-free list may
    // contain pages that remain reachable from that inactive slot.
    let mut writer = mgr.begin_write().unwrap();
    writer.insert(b"second", b"inline").unwrap();
    writer.commit().unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(
        report.is_ok(),
        "cross-slot sharing was mistaken for same-slot corruption: {:?}",
        report.errors
    );
}

#[test]
fn authenticated_overflow_payload_mutation_breaks_logical_merkle() {
    let (mgr, io) = create_manager_with_raw_io();
    let payload = vec![b'a'; citadel_core::MAX_INLINE_VALUE_SIZE + 37];
    let mut writer = mgr.begin_write().unwrap();
    writer.insert(b"overflow", &payload).unwrap();
    writer.commit().unwrap();

    let slot = mgr.current_slot();
    let root = mgr.read_page_from_disk(slot.tree_root).unwrap();
    assert_eq!(root.page_type(), Some(PageType::Leaf));
    assert_eq!(root.num_cells(), 1);
    let cell = leaf_node::read_cell(&root, 0);
    let reference = OverflowRef::from_bytes(cell.value);
    let mut head = mgr.read_page_from_disk(reference.first_page).unwrap();
    let stored_digest = head.merkle_hash();
    assert_ne!(stored_digest, [0u8; citadel_core::MERKLE_HASH_SIZE]);

    let mut mutated = payload;
    mutated[0] ^= 0x5a;
    head.data[PAGE_HEADER_SIZE + 4] ^= 0x5a;
    head.update_checksum();
    write_encrypted_page(&io, &head, slot.encryption_epoch);

    let expected_digest = crate::merkle::overflow_payload_hash(&mutated);
    let expected_leaf =
        crate::merkle::hash_logical_leaf_cells([(cell.key, cell.val_type, cell.value)], |_| {
            Ok(expected_digest)
        })
        .unwrap();
    let report = run_integrity_check(&mgr).unwrap();
    for finding in [
        IntegrityError::OverflowDigestMismatch {
            first_page: reference.first_page,
            expected: expected_digest,
            actual: stored_digest,
        },
        IntegrityError::PageMerkleMismatch {
            page: slot.tree_root,
            expected: expected_leaf,
            actual: root.merkle_hash(),
        },
        IntegrityError::SlotMerkleRootMismatch {
            expected: expected_leaf,
            actual: slot.merkle_root,
        },
    ] {
        assert!(report.errors.contains(&finding), "missing {finding:?}");
    }
}

#[test]
fn a_zero_logical_overflow_v1_head_digest_is_reported_as_missing() {
    let (mgr, io) = create_manager_with_raw_io();
    let payload = vec![b'z'; citadel_core::MAX_INLINE_VALUE_SIZE + 19];
    let mut writer = mgr.begin_write().unwrap();
    writer.insert(b"overflow", &payload).unwrap();
    writer.commit().unwrap();

    let slot = mgr.current_slot();
    assert_eq!(slot.merkle_scheme, MerkleScheme::LogicalOverflowV1);
    let root = mgr.read_page_from_disk(slot.tree_root).unwrap();
    let cell = leaf_node::read_cell(&root, 0);
    let reference = OverflowRef::from_bytes(cell.value);
    let mut head = mgr.read_page_from_disk(reference.first_page).unwrap();
    head.set_merkle_hash(&[0u8; citadel_core::MERKLE_HASH_SIZE]);
    head.update_checksum();
    write_encrypted_page(&io, &head, slot.encryption_epoch);

    let expected = crate::merkle::overflow_payload_hash(&payload);
    let report = run_integrity_check(&mgr).unwrap();
    assert!(
        report
            .errors
            .contains(&IntegrityError::OverflowDigestMismatch {
                first_page: reference.first_page,
                expected,
                actual: [0u8; citadel_core::MERKLE_HASH_SIZE],
            }),
        "zero overflow digest was not reported: {:?}",
        report.errors
    );
    assert!(
        !report.errors.iter().any(|error| matches!(
            error,
            IntegrityError::PageMerkleMismatch { .. }
                | IntegrityError::SlotMerkleRootMismatch { .. }
        )),
        "recomputed payload still agrees with the logical tree: {:?}",
        report.errors
    );
}

#[test]
fn a_legacy_scheme_accepts_a_missing_overflow_digest_and_ignores_merkle_caches() {
    let (mgr, io) = create_manager_with_raw_io();
    let payload = vec![b'l'; citadel_core::MAX_INLINE_VALUE_SIZE + 23];
    let mut writer = mgr.begin_write().unwrap();
    writer.insert(b"legacy-overflow", &payload).unwrap();
    writer.commit().unwrap();

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let mut root = mgr.read_page_from_disk(slot.tree_root).unwrap();
    let cell = leaf_node::read_cell(&root, 0);
    let reference = OverflowRef::from_bytes(cell.value);
    let mut head = mgr.read_page_from_disk(reference.first_page).unwrap();

    head.set_merkle_hash(&[0u8; citadel_core::MERKLE_HASH_SIZE]);
    head.update_checksum();
    write_encrypted_page(&io, &head, slot.encryption_epoch);

    root.set_merkle_hash(&[0x61; citadel_core::MERKLE_HASH_SIZE]);
    root.update_checksum();
    write_encrypted_page(&io, &root, slot.encryption_epoch);

    slot.merkle_scheme = MerkleScheme::Legacy;
    slot.merkle_root = [0x62; citadel_core::MERKLE_HASH_SIZE];
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(
        report.is_ok(),
        "legacy Merkle metadata was treated as certifying: {:?}",
        report.errors
    );
    assert!(
        report.pages_checked >= 2,
        "the overflow chain was not walked"
    );
}

#[test]
fn a_legacy_scheme_still_reports_a_nonzero_overflow_digest_mismatch() {
    let (mgr, io) = create_manager_with_raw_io();
    let payload = vec![b'm'; citadel_core::MAX_INLINE_VALUE_SIZE + 29];
    let mut writer = mgr.begin_write().unwrap();
    writer.insert(b"legacy-overflow", &payload).unwrap();
    writer.commit().unwrap();

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let root = mgr.read_page_from_disk(slot.tree_root).unwrap();
    let cell = leaf_node::read_cell(&root, 0);
    let reference = OverflowRef::from_bytes(cell.value);
    let mut head = mgr.read_page_from_disk(reference.first_page).unwrap();
    let expected = crate::merkle::overflow_payload_hash(&payload);
    let forged = [0x63; citadel_core::MERKLE_HASH_SIZE];
    assert_ne!(forged, expected);
    head.set_merkle_hash(&forged);
    head.update_checksum();
    write_encrypted_page(&io, &head, slot.encryption_epoch);

    slot.merkle_scheme = MerkleScheme::Legacy;
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(
        report
            .errors
            .contains(&IntegrityError::OverflowDigestMismatch {
                first_page: reference.first_page,
                expected,
                actual: forged,
            }),
        "legacy nonzero digest mismatch was not reported: {:?}",
        report.errors
    );
    assert!(!report.errors.iter().any(|error| matches!(
        error,
        IntegrityError::PageMerkleMismatch { .. } | IntegrityError::SlotMerkleRootMismatch { .. }
    )));
}

#[test]
fn overflow_physical_remap_does_not_change_logical_leaf_hash() {
    let payload = b"same logical overflow payload";
    let digest = crate::merkle::overflow_payload_hash(payload);
    let first = overflow_leaf(
        PageId(10),
        citadel_core::types::TxnId(1),
        &[(
            b"key",
            OverflowRef {
                first_page: PageId(11),
                total_len: payload.len() as u32,
            },
        )],
    );
    let remapped = overflow_leaf(
        PageId(20),
        citadel_core::types::TxnId(1),
        &[(
            b"key",
            OverflowRef {
                first_page: PageId(99),
                total_len: payload.len() as u32,
            },
        )],
    );
    let first_cells = checked_leaf_cells(&first).unwrap();
    let remapped_cells = checked_leaf_cells(&remapped).unwrap();
    assert_eq!(
        hash_checked_leaf_cells(&first_cells, &[Some(digest)]),
        hash_checked_leaf_cells(&remapped_cells, &[Some(digest)])
    );
}

#[test]
fn tampering_in_an_overflow_page_is_reported_at_that_page() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let root = PageId(slot.high_water_mark + 8);
    let overflow_id = PageId(root.as_u32() + 1);
    let data = b"overflow payload";
    let leaf = overflow_leaf(
        root,
        slot.txn_id,
        &[(
            b"key",
            OverflowRef {
                first_page: overflow_id,
                total_len: data.len() as u32,
            },
        )],
    );
    let overflow = overflow_page(overflow_id, slot.txn_id, data, PageId(0));
    slot.tree_root = root;
    slot.tree_entries = 1;
    publish_pages_and_slot(&io, active, slot, &[leaf, overflow]);

    flip_raw_byte(&io, page_offset(overflow_id) as usize + 100);
    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::PageTampered(overflow_id)));
}

#[test]
fn malformed_and_oversized_overflow_references_are_typed() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let root = PageId(slot.high_water_mark + 8);
    let mut leaf = Page::new(root, PageType::Leaf, slot.txn_id);
    assert!(leaf_node::insert_append_direct(
        &mut leaf,
        b"short",
        ValueType::Overflow,
        &[1; 7],
    ));
    let too_large = OverflowRef {
        first_page: PageId(0),
        total_len: (citadel_core::MAX_VALUE_SIZE + 1) as u32,
    };
    assert!(leaf_node::insert_append_direct(
        &mut leaf,
        b"too-large",
        ValueType::Overflow,
        &too_large.to_bytes(),
    ));
    leaf.update_checksum();
    slot.tree_root = root;
    slot.tree_entries = 2;
    publish_pages_and_slot(&io, active, slot, &[leaf]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::MalformedOverflowReference {
            page: root,
            index: 0,
            actual_size: 7,
        }));
    assert!(report
        .errors
        .contains(&IntegrityError::OverflowLengthOutOfBounds {
            page: root,
            index: 1,
            declared: (citadel_core::MAX_VALUE_SIZE + 1) as u32,
            max: citadel_core::MAX_VALUE_SIZE,
        }));
}

#[test]
fn a_truncated_overflow_chain_reports_its_actual_length() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let root = PageId(slot.high_water_mark + 8);
    let overflow_id = PageId(root.as_u32() + 1);
    let leaf = overflow_leaf(
        root,
        slot.txn_id,
        &[(
            b"key",
            OverflowRef {
                first_page: overflow_id,
                total_len: 10,
            },
        )],
    );
    let overflow = overflow_page(overflow_id, slot.txn_id, b"short", PageId(0));
    slot.tree_root = root;
    slot.tree_entries = 1;
    publish_pages_and_slot(&io, active, slot, &[leaf, overflow]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::OverflowChainLengthMismatch {
            first_page: overflow_id,
            expected: 10,
            actual: 5,
        }));
}

#[test]
fn overflow_page_lengths_and_extra_pages_are_bounded() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let root = PageId(slot.high_water_mark + 8);
    let first = PageId(root.as_u32() + 1);
    let second = PageId(root.as_u32() + 2);
    let leaf = overflow_leaf(
        root,
        slot.txn_id,
        &[(
            b"key",
            OverflowRef {
                first_page: first,
                total_len: 1,
            },
        )],
    );
    let mut oversized = overflow_page(first, slot.txn_id, &[], second);
    let impossible = (overflow::OVERFLOW_DATA_CAPACITY + 1) as u32;
    overflow::set_data_len(&mut oversized, impossible);
    oversized.update_checksum();
    let extra = overflow_page(second, slot.txn_id, &[], PageId(0));
    slot.tree_root = root;
    slot.tree_entries = 1;
    publish_pages_and_slot(&io, active, slot, &[leaf, oversized, extra]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::OverflowPageDataLengthOutOfBounds {
            page: first,
            declared: impossible,
            max: overflow::OVERFLOW_DATA_CAPACITY,
        }));
    assert!(report
        .errors
        .contains(&IntegrityError::OverflowChainPageCountOutOfBounds {
            first_page: first,
            expected_max: 1,
            actual: 2,
        }));
}

#[test]
fn an_overflow_cycle_is_a_duplicate_page_reference() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let root = PageId(slot.high_water_mark + 8);
    let overflow_id = PageId(root.as_u32() + 1);
    let leaf = overflow_leaf(
        root,
        slot.txn_id,
        &[(
            b"key",
            OverflowRef {
                first_page: overflow_id,
                total_len: 1,
            },
        )],
    );
    let overflow = overflow_page(overflow_id, slot.txn_id, b"x", overflow_id);
    slot.tree_root = root;
    slot.tree_entries = 1;
    publish_pages_and_slot(&io, active, slot, &[leaf, overflow]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::DuplicatePageRef(overflow_id)));
}

#[test]
fn two_values_cannot_share_one_overflow_chain_in_the_same_slot() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let root = PageId(slot.high_water_mark + 8);
    let overflow_id = PageId(root.as_u32() + 1);
    let reference = || OverflowRef {
        first_page: overflow_id,
        total_len: 1,
    };
    let leaf = overflow_leaf(
        root,
        slot.txn_id,
        &[(b"a", reference()), (b"b", reference())],
    );
    let overflow = overflow_page(overflow_id, slot.txn_id, b"x", PageId(0));
    slot.tree_root = root;
    slot.tree_entries = 2;
    publish_pages_and_slot(&io, active, slot, &[leaf, overflow]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::DuplicatePageRef(overflow_id)));
}

#[test]
fn a_pending_free_root_must_point_to_a_pending_free_page() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let fake_pending_free = PageId(slot.high_water_mark + 8);
    let mut page = Page::new(fake_pending_free, PageType::Leaf, slot.txn_id);
    page.update_checksum();
    write_encrypted_page(&io, &page, slot.encryption_epoch);

    slot.pending_free_root = fake_pending_free;
    slot.total_pages = slot.total_pages.max(fake_pending_free.as_u32() + 1);
    slot.high_water_mark = slot.high_water_mark.max(fake_pending_free.as_u32() + 1);
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report.errors.contains(&IntegrityError::InvalidPageType {
        page: fake_pending_free,
        expected: "PendingFree",
    }));
}

#[test]
fn a_pending_free_page_cannot_claim_entries_beyond_its_body() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let fake_pending_free = PageId(slot.high_water_mark + 8);
    let mut page = Page::new(fake_pending_free, PageType::PendingFree, slot.txn_id);
    let impossible = u32::try_from(PENDING_FREE_ENTRY_CAPACITY + 1).unwrap();
    page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4].copy_from_slice(&impossible.to_le_bytes());
    page.update_checksum();
    write_encrypted_page(&io, &page, slot.encryption_epoch);

    slot.pending_free_root = fake_pending_free;
    slot.total_pages = slot.total_pages.max(fake_pending_free.as_u32() + 1);
    slot.high_water_mark = slot.high_water_mark.max(fake_pending_free.as_u32() + 1);
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::PendingFreeEntryCountOutOfBounds {
            page: fake_pending_free,
            count: impossible,
            max: PENDING_FREE_ENTRY_CAPACITY,
        }));
}

#[test]
fn a_pending_free_chain_page_must_be_below_the_slot_high_water_mark() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let out_of_bounds = PageId(slot.high_water_mark);
    slot.pending_free_root = out_of_bounds;
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::PendingFreePageOutOfBounds {
            page: out_of_bounds,
            high_water_mark: slot.high_water_mark,
        }));
}

#[test]
fn pending_free_entries_are_bounded_unique_old_and_unreachable() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let pending = PageId(slot.high_water_mark + 8);
    let live = slot.tree_root;
    let outside = PageId(pending.as_u32() + 50);
    let future = citadel_core::types::TxnId(slot.txn_id.as_u64() + 1);
    let page = pending_free_page(
        pending,
        slot.txn_id,
        &[
            (live, slot.txn_id),
            (live, slot.txn_id),
            (outside, slot.txn_id),
            (PageId(1), future),
            (PageId(2), citadel_core::types::TxnId::ZERO),
        ],
        PageId::INVALID,
    );
    slot.pending_free_root = pending;
    publish_pages_and_slot(&io, active, slot.clone(), &[page]);
    let high_water_mark = pending.as_u32() + 1;

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::PendingFreeEntryStillReachable { page: live }));
    assert!(report
        .errors
        .contains(&IntegrityError::DuplicatePendingFreeEntry {
            page: live,
            first_chain_page: pending,
            duplicate_chain_page: pending,
        }));
    assert!(report
        .errors
        .contains(&IntegrityError::PendingFreeEntryOutOfBounds {
            chain_page: pending,
            index: 2,
            entry: outside,
            high_water_mark,
        }));
    assert!(report
        .errors
        .contains(&IntegrityError::PendingFreeTransactionOutOfBounds {
            chain_page: pending,
            index: 3,
            freed_at: future,
            slot_txn: slot.txn_id,
        }));
    assert!(report
        .errors
        .contains(&IntegrityError::PendingFreeTransactionOutOfBounds {
            chain_page: pending,
            index: 4,
            freed_at: citadel_core::types::TxnId::ZERO,
            slot_txn: slot.txn_id,
        }));
}

#[test]
fn a_pending_free_cycle_is_a_duplicate_page_reference() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");
    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let pending = PageId(slot.high_water_mark + 8);
    let page = pending_free_page(pending, slot.txn_id, &[], pending);
    slot.pending_free_root = pending;
    publish_pages_and_slot(&io, active, slot, &[page]);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::DuplicatePageRef(pending)));
}

#[test]
fn slot_hash_ambiguity_preserves_every_catalog_root_and_the_runtime_root() {
    let hash = 0x1234_5678;
    let mut tables = vec![
        NamedTable {
            name: Some(b"first".to_vec()),
            hash,
            root: PageId(10),
            entry_count: 1,
            depth: 1,
        },
        NamedTable {
            name: Some(b"second".to_vec()),
            hash,
            root: PageId(20),
            entry_count: 2,
            depth: 1,
        },
    ];
    let slot = CommitSlot {
        named_table_entries: vec![(hash, 3, 30, 0), (hash, 4, 40, 0)],
        ..Default::default()
    };
    let mut errors = Vec::new();

    let ambiguous = merge_slot_named_tables(&slot, &mut tables, &mut errors, None).unwrap();

    assert!(ambiguous.contains(&hash));
    assert_eq!(tables.len(), 4);
    assert_eq!(tables[0].name.as_deref(), Some(b"first".as_slice()));
    assert_eq!(tables[0].root, PageId(10));
    assert_eq!(tables[0].entry_count, 1);
    assert_eq!(tables[1].name.as_deref(), Some(b"second".as_slice()));
    assert_eq!(tables[1].root, PageId(20));
    assert_eq!(tables[1].entry_count, 2);
    assert_eq!(tables[2].name, None);
    assert_eq!(tables[2].root, PageId(30));
    assert_eq!(tables[2].entry_count, 3);
    assert_eq!(tables[3].name, None);
    assert_eq!(tables[3].root, PageId(40));
    assert_eq!(tables[3].entry_count, 4);
    assert!(errors.contains(&IntegrityError::NamedTableHashCollision {
        table_hash: hash,
        first_table: b"first".to_vec(),
        conflicting_table: b"second".to_vec(),
    }));
    assert!(
        errors.contains(&IntegrityError::DuplicateNamedTableSlotHash {
            table_hash: hash,
            first_index: 0,
            duplicate_index: 1,
        })
    );
}

#[test]
fn named_table_postprocessing_observes_cancellation() {
    let token = CancelToken::new();
    token.cancel();
    let mut tables = vec![NamedTable {
        name: Some(b"table".to_vec()),
        hash: 7,
        root: PageId(10),
        entry_count: 1,
        depth: 1,
    }];
    let mut errors = Vec::new();

    let result = merge_slot_named_tables(
        &CommitSlot::default(),
        &mut tables,
        &mut errors,
        Some(&token),
    );

    assert!(matches!(result, Err(citadel_core::Error::Interrupted)));
}

#[test]
fn duplicate_slot_hash_preserves_the_catalog_root_and_every_runtime_root() {
    let hash = 0x1234_5678;
    let mut tables = vec![NamedTable {
        name: Some(b"only-catalog-name".to_vec()),
        hash,
        root: PageId(10),
        entry_count: 1,
        depth: 1,
    }];
    let slot = CommitSlot {
        named_table_entries: vec![(hash, 2, 20, 0), (hash, 3, 30, 0)],
        ..Default::default()
    };
    let mut errors = Vec::new();

    let ambiguous = merge_slot_named_tables(&slot, &mut tables, &mut errors, None).unwrap();

    assert!(ambiguous.contains(&hash));
    assert_eq!(tables.len(), 3);
    assert_eq!(
        tables[0].name.as_deref(),
        Some(b"only-catalog-name".as_slice())
    );
    assert_eq!(tables[0].root, PageId(10));
    assert_eq!(tables[0].entry_count, 1);
    assert_eq!(tables[1].name, None);
    assert_eq!(tables[1].root, PageId(20));
    assert_eq!(tables[1].entry_count, 2);
    assert_eq!(tables[2].name, None);
    assert_eq!(tables[2].root, PageId(30));
    assert_eq!(tables[2].entry_count, 3);
    assert!(
        errors.contains(&IntegrityError::DuplicateNamedTableSlotHash {
            table_hash: hash,
            first_index: 0,
            duplicate_index: 1,
        })
    );
}

#[test]
fn a_duplicate_slot_hash_does_not_hide_tampering_under_its_second_root() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"default-row");

    let (active, slots) = raw_slots(&mgr);
    let mut slot = slots[active].clone();
    let first_root = PageId(slot.high_water_mark + 8);
    let second_root = PageId(slot.high_water_mark + 9);
    for root in [first_root, second_root] {
        let mut page = Page::new(root, PageType::Leaf, slot.txn_id);
        page.update_checksum();
        write_encrypted_page(&io, &page, slot.encryption_epoch);
    }

    let hash = 0x1234_5678;
    slot.catalog_root = PageId::INVALID;
    slot.named_table_entries = vec![
        (hash, 0, first_root.as_u32(), 0),
        (hash, 0, second_root.as_u32(), 0),
    ];
    slot.total_pages = slot.total_pages.max(second_root.as_u32() + 1);
    slot.high_water_mark = slot.high_water_mark.max(second_root.as_u32() + 1);
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();

    flip_raw_byte(&io, page_offset(second_root) as usize + 32);

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::DuplicateNamedTableSlotHash {
            table_hash: hash,
            first_index: 0,
            duplicate_index: 1,
        }));
    assert!(
        report
            .errors
            .contains(&IntegrityError::PageTampered(second_root)),
        "the second ambiguous root escaped the walk: {:?}",
        report.errors
    );
}

#[test]
fn an_inactive_slot_checksum_failure_is_reported() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"one");
    commit_value(&mgr, b"two");

    let (active, slots) = raw_slots(&mgr);
    let inactive = 1 - active;
    assert_ne!(slots[inactive].txn_id, citadel_core::types::TxnId(0));
    flip_raw_byte(
        &io,
        COMMIT_SLOT_OFFSET + inactive * COMMIT_SLOT_SIZE + SLOT_CHECKSUM,
    );

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::CommitSlotChecksumMismatch { slot: inactive }));
    assert!(report
        .tampered()
        .any(|error| { error == &IntegrityError::CommitSlotChecksumMismatch { slot: inactive } }));
}

#[test]
fn an_inactive_v1_slot_mac_failure_is_reported() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"one");
    commit_value(&mgr, b"two");

    let (active, slots) = raw_slots(&mgr);
    let inactive = 1 - active;
    assert_eq!(slots[inactive].slot_format, SlotFormat::V1);
    flip_raw_byte(
        &io,
        COMMIT_SLOT_OFFSET + inactive * COMMIT_SLOT_SIZE + SLOT_MAC,
    );

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::CommitSlotMacMismatch { slot: inactive }));
    assert!(
        !report
            .errors
            .contains(&IntegrityError::CommitSlotChecksumMismatch { slot: inactive }),
        "the MAC tail is outside the keyless checksum"
    );
}

#[test]
fn a_legacy_slot_in_a_v1_required_file_is_reported_as_a_downgrade() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"one");
    commit_value(&mgr, b"two");

    let (active, slots) = raw_slots(&mgr);
    let inactive = 1 - active;
    assert_eq!(slots[inactive].slot_format, SlotFormat::V1);
    io.write_at(
        (COMMIT_SLOT_OFFSET + inactive * COMMIT_SLOT_SIZE + SLOT_FORMAT_MARKER) as u64,
        &[0, 0],
    )
    .unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::CommitSlotDowngrade { slot: inactive }));
    assert!(
        !report
            .errors
            .contains(&IntegrityError::CommitSlotChecksumMismatch { slot: inactive }),
        "the legacy marker is outside the keyless checksum"
    );
}

#[test]
fn an_unknown_slot_format_is_reported_as_its_own_error() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"one");
    commit_value(&mgr, b"two");

    let (active, slots) = raw_slots(&mgr);
    let inactive = 1 - active;
    assert_eq!(slots[inactive].slot_format, SlotFormat::V1);
    io.write_at(
        (COMMIT_SLOT_OFFSET + inactive * COMMIT_SLOT_SIZE + SLOT_FORMAT_MARKER) as u64,
        &[0xff, 0xff],
    )
    .unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::CommitSlotUnknownFormat { slot: inactive }));
    assert!(
        !report
            .errors
            .contains(&IntegrityError::CommitSlotChecksumMismatch { slot: inactive }),
        "an unknown format is not evidence that the keyless checksum bytes differ"
    );
}

#[test]
fn an_unknown_merkle_scheme_is_reported_and_its_roots_are_not_walked() {
    let (mgr, io) = create_manager_with_raw_io();
    commit_value(&mgr, b"one");
    commit_value(&mgr, b"two");

    let (active, slots) = raw_slots(&mgr);
    let inactive = 1 - active;
    let mut slot = slots[inactive].clone();
    assert_ne!(slot.txn_id, citadel_core::types::TxnId(0));
    slot.merkle_scheme = MerkleScheme::Unknown;
    slot.tree_root = PageId(slot.high_water_mark.saturating_add(10));
    let forged_root = slot.tree_root;
    let (_, mac_key, _) = test_keys();
    slot.seal(&mac_key);
    write_commit_slot(&io, inactive, &slot).unwrap();

    let report = run_integrity_check(&mgr).unwrap();
    assert!(report
        .errors
        .contains(&IntegrityError::CommitSlotUnknownMerkleScheme { slot: inactive }));
    assert!(
        !report.errors.iter().any(|error| matches!(
            error,
            IntegrityError::ReachablePageOutOfBounds { page, .. } if *page == forged_root
        )),
        "an unknown scheme's roots must not be trusted: {:?}",
        report.errors
    );
}

#[test]
fn a_tampered_page_keeps_its_type_instead_of_becoming_a_string() {
    let e = IntegrityError::from_page_read(PageId(7), Error::PageTampered(PageId(7)));
    assert_eq!(e, IntegrityError::PageTampered(PageId(7)));
    assert!(e.is_tamper());
}

#[test]
fn a_checksum_mismatch_keeps_its_type_too() {
    let e = IntegrityError::from_page_read(PageId(7), Error::ChecksumMismatch(PageId(7)));
    assert_eq!(e, IntegrityError::ChecksumMismatch(PageId(7)));
    assert!(e.is_tamper());
}

/// The detecting layer names the page. A leaf read that trips on an overflow
/// page must report the overflow page, not the leaf we asked for.
#[test]
fn the_reported_page_comes_from_the_layer_that_detected_it() {
    let e = IntegrityError::from_page_read(PageId(7), Error::PageTampered(PageId(99)));
    assert_eq!(e, IntegrityError::PageTampered(PageId(99)));
}

#[test]
fn an_ordinary_read_failure_is_still_a_string_and_is_not_a_tamper() {
    let e = IntegrityError::from_page_read(PageId(7), Error::PageOutOfBounds(PageId(7)));
    let IntegrityError::PageReadFailed { page, error } = &e else {
        panic!("expected PageReadFailed, got {e:?}");
    };
    assert_eq!(*page, PageId(7));
    assert!(!error.is_empty());
    assert!(!e.is_tamper());
}

#[test]
fn every_variant_renders_without_debug_formatting() {
    let variants = [
        IntegrityError::CommitSlotChecksumMismatch { slot: 0 },
        IntegrityError::CommitSlotMacMismatch { slot: 1 },
        IntegrityError::CommitSlotDowngrade { slot: 0 },
        IntegrityError::CommitSlotUnknownFormat { slot: 1 },
        IntegrityError::CommitSlotUnknownMerkleScheme { slot: 0 },
        IntegrityError::PageReadFailed {
            page: PageId(1),
            error: "disk gone".into(),
        },
        IntegrityError::PageTampered(PageId(2)),
        IntegrityError::ChecksumMismatch(PageId(3)),
        IntegrityError::PageIdMismatch {
            expected: PageId(30),
            actual: PageId(31),
        },
        IntegrityError::PageTransactionOutOfBounds {
            page: PageId(32),
            page_txn: citadel_core::types::TxnId(9),
            slot_txn: citadel_core::types::TxnId(8),
        },
        IntegrityError::ReachablePageOutOfBounds {
            page: PageId(47),
            high_water_mark: 40,
        },
        IntegrityError::TreeDepthMismatch {
            root: PageId(48),
            leaf: PageId(49),
            expected: 2,
            actual: 3,
        },
        IntegrityError::PageMerkleMismatch {
            page: PageId(50),
            expected: [0x11; citadel_core::MERKLE_HASH_SIZE],
            actual: [0x22; citadel_core::MERKLE_HASH_SIZE],
        },
        IntegrityError::SlotMerkleRootMismatch {
            expected: [0x33; citadel_core::MERKLE_HASH_SIZE],
            actual: [0x44; citadel_core::MERKLE_HASH_SIZE],
        },
        IntegrityError::PageCountMetadataMismatch {
            total_pages: 9,
            high_water_mark: 10,
        },
        IntegrityError::KeyOrderViolation {
            page: PageId(4),
            index: 5,
        },
        IntegrityError::KeyRangeViolation {
            page: PageId(5),
            index: 6,
        },
        IntegrityError::MalformedPage {
            page: PageId(6),
            detail: "cell offset lies outside the cell area",
        },
        IntegrityError::MalformedOverflowReference {
            page: PageId(33),
            index: 2,
            actual_size: 7,
        },
        IntegrityError::OverflowLengthOutOfBounds {
            page: PageId(34),
            index: 3,
            declared: u32::MAX,
            max: citadel_core::MAX_VALUE_SIZE,
        },
        IntegrityError::OverflowPageDataLengthOutOfBounds {
            page: PageId(35),
            declared: 9000,
            max: overflow::OVERFLOW_DATA_CAPACITY,
        },
        IntegrityError::OverflowChainLengthMismatch {
            first_page: PageId(36),
            expected: 100,
            actual: 90,
        },
        IntegrityError::OverflowChainPageCountOutOfBounds {
            first_page: PageId(37),
            expected_max: 2,
            actual: 3,
        },
        IntegrityError::OverflowDigestMismatch {
            first_page: PageId(38),
            expected: [0x11; citadel_core::MERKLE_HASH_SIZE],
            actual: [0x22; citadel_core::MERKLE_HASH_SIZE],
        },
        IntegrityError::DuplicatePageRef(PageId(6)),
        IntegrityError::EntryCountMismatch {
            expected: 10,
            actual: 9,
        },
        IntegrityError::NamedTableEntryCountMismatch {
            table: Some(b"na`me\\x".to_vec()),
            table_hash: 0x1234_abcd,
            expected: 8,
            actual: 7,
        },
        IntegrityError::MalformedTableDescriptor {
            page: PageId(8),
            table: b"broken".to_vec(),
            value_type: ValueType::Inline,
            actual_size: 4,
        },
        IntegrityError::InvalidTableDescriptor {
            page: PageId(38),
            table: b"invalid".to_vec(),
            detail: "tree depth is zero",
        },
        IntegrityError::NamedTableHashCollision {
            table_hash: 0xdead_beef,
            first_table: b"first".to_vec(),
            conflicting_table: b"second".to_vec(),
        },
        IntegrityError::DuplicateNamedTableSlotHash {
            table_hash: 0x1234_5678,
            first_index: 2,
            duplicate_index: 5,
        },
        IntegrityError::InvalidPageType {
            page: PageId(7),
            expected: "leaf",
        },
        IntegrityError::PendingFreeEntryCountOutOfBounds {
            page: PageId(8),
            count: 900,
            max: PENDING_FREE_ENTRY_CAPACITY,
        },
        IntegrityError::PendingFreePageOutOfBounds {
            page: PageId(39),
            high_water_mark: 20,
        },
        IntegrityError::PendingFreeEntryOutOfBounds {
            chain_page: PageId(40),
            index: 4,
            entry: PageId(41),
            high_water_mark: 20,
        },
        IntegrityError::PendingFreeTransactionOutOfBounds {
            chain_page: PageId(42),
            index: 5,
            freed_at: citadel_core::types::TxnId(12),
            slot_txn: citadel_core::types::TxnId(11),
        },
        IntegrityError::DuplicatePendingFreeEntry {
            page: PageId(43),
            first_chain_page: PageId(44),
            duplicate_chain_page: PageId(45),
        },
        IntegrityError::PendingFreeEntryStillReachable { page: PageId(46) },
    ];
    for v in &variants {
        let shown = v.to_string();
        assert!(!shown.is_empty(), "{v:?} renders empty");
        assert!(!shown.contains('{'), "{v:?} leaked a Debug struct: {shown}");
    }
}

#[test]
fn named_table_errors_keep_their_identity_and_counts_in_display() {
    let mismatch = IntegrityError::NamedTableEntryCountMismatch {
        table: Some(b"ta\xffble".to_vec()),
        table_hash: 0x1234_abcd,
        expected: 17,
        actual: 16,
    }
    .to_string();
    for field in ["ta\\xffble", "0x1234abcd", "17", "16"] {
        assert!(
            mismatch.contains(field),
            "display dropped {field}: {mismatch}"
        );
    }

    let malformed = IntegrityError::MalformedTableDescriptor {
        page: PageId(23),
        table: b"short".to_vec(),
        value_type: ValueType::Inline,
        actual_size: 4,
    }
    .to_string();
    for field in ["page:23", "short", "Inline", "4", "20"] {
        assert!(
            malformed.contains(field),
            "display dropped {field}: {malformed}"
        );
    }

    let invalid = IntegrityError::InvalidTableDescriptor {
        page: PageId(24),
        table: b"bad-root".to_vec(),
        detail: "root page is invalid",
    }
    .to_string();
    for field in ["page:24", "bad-root", "root page is invalid"] {
        assert!(
            invalid.contains(field),
            "display dropped {field}: {invalid}"
        );
    }

    let collision = IntegrityError::NamedTableHashCollision {
        table_hash: 0xdead_beef,
        first_table: b"first".to_vec(),
        conflicting_table: b"second".to_vec(),
    }
    .to_string();
    for field in ["first", "second", "0xdeadbeef"] {
        assert!(
            collision.contains(field),
            "display dropped {field}: {collision}"
        );
    }

    let duplicate = IntegrityError::DuplicateNamedTableSlotHash {
        table_hash: 0x1234_5678,
        first_index: 2,
        duplicate_index: 5,
    }
    .to_string();
    for field in ["0x12345678", "2", "5"] {
        assert!(
            duplicate.contains(field),
            "display dropped {field}: {duplicate}"
        );
    }

    let pending_free = IntegrityError::PendingFreeEntryCountOutOfBounds {
        page: PageId(29),
        count: 900,
        max: PENDING_FREE_ENTRY_CAPACITY,
    }
    .to_string();
    let pending_free_capacity = PENDING_FREE_ENTRY_CAPACITY.to_string();
    for field in ["page:29", "900", pending_free_capacity.as_str()] {
        assert!(
            pending_free.contains(field),
            "display dropped {field}: {pending_free}"
        );
    }
}

#[test]
fn tree_shape_errors_keep_their_page_index_and_detail_in_display() {
    let range = IntegrityError::KeyRangeViolation {
        page: PageId(31),
        index: 7,
    }
    .to_string();
    for field in ["page:31", "7", "parent range"] {
        assert!(range.contains(field), "display dropped {field}: {range}");
    }

    let malformed = IntegrityError::MalformedPage {
        page: PageId(32),
        detail: "leaf cell value exceeds the page body",
    }
    .to_string();
    for field in ["page:32", "leaf cell value exceeds the page body"] {
        assert!(
            malformed.contains(field),
            "display dropped {field}: {malformed}"
        );
    }
}

#[test]
fn page_header_errors_keep_both_sides_in_display() {
    let id = IntegrityError::PageIdMismatch {
        expected: PageId(51),
        actual: PageId(52),
    }
    .to_string();
    for field in ["page:51", "page:52"] {
        assert!(id.contains(field), "display dropped {field}: {id}");
    }

    let txn = IntegrityError::PageTransactionOutOfBounds {
        page: PageId(53),
        page_txn: citadel_core::types::TxnId(18),
        slot_txn: citadel_core::types::TxnId(17),
    }
    .to_string();
    for field in ["page:53", "txn:18", "txn:17"] {
        assert!(txn.contains(field), "display dropped {field}: {txn}");
    }

    let bounds = IntegrityError::ReachablePageOutOfBounds {
        page: PageId(54),
        high_water_mark: 40,
    }
    .to_string();
    for field in ["page:54", "40"] {
        assert!(bounds.contains(field), "display dropped {field}: {bounds}");
    }
}

#[test]
fn overflow_errors_keep_reference_page_and_bounds_in_display() {
    let cases = [
        (
            IntegrityError::MalformedOverflowReference {
                page: PageId(54),
                index: 3,
                actual_size: 7,
            },
            vec!["page:54", "3", "7", "8"],
        ),
        (
            IntegrityError::OverflowLengthOutOfBounds {
                page: PageId(55),
                index: 4,
                declared: 123,
                max: 99,
            },
            vec!["page:55", "4", "123", "99"],
        ),
        (
            IntegrityError::OverflowPageDataLengthOutOfBounds {
                page: PageId(56),
                declared: 9000,
                max: 8092,
            },
            vec!["page:56", "9000", "8092"],
        ),
        (
            IntegrityError::OverflowChainLengthMismatch {
                first_page: PageId(57),
                expected: 100,
                actual: 90,
            },
            vec!["page:57", "100", "90"],
        ),
        (
            IntegrityError::OverflowChainPageCountOutOfBounds {
                first_page: PageId(58),
                expected_max: 2,
                actual: 3,
            },
            vec!["page:58", "2", "3"],
        ),
        (
            IntegrityError::OverflowDigestMismatch {
                first_page: PageId(59),
                expected: [0x11; citadel_core::MERKLE_HASH_SIZE],
                actual: [0x22; citadel_core::MERKLE_HASH_SIZE],
            },
            vec!["page:59"],
        ),
    ];
    for (error, fields) in cases {
        let shown = error.to_string();
        for field in fields {
            assert!(shown.contains(field), "display dropped {field}: {shown}");
        }
    }
}

#[test]
fn pending_free_errors_keep_entry_identity_and_bounds_in_display() {
    let cases = [
        (
            IntegrityError::PendingFreePageOutOfBounds {
                page: PageId(60),
                high_water_mark: 59,
            },
            vec!["page:60", "59"],
        ),
        (
            IntegrityError::PendingFreeEntryOutOfBounds {
                chain_page: PageId(61),
                index: 7,
                entry: PageId(62),
                high_water_mark: 50,
            },
            vec!["page:61", "7", "page:62", "50"],
        ),
        (
            IntegrityError::PendingFreeTransactionOutOfBounds {
                chain_page: PageId(63),
                index: 8,
                freed_at: citadel_core::types::TxnId(20),
                slot_txn: citadel_core::types::TxnId(19),
            },
            vec!["page:63", "8", "txn:20", "txn:19"],
        ),
        (
            IntegrityError::DuplicatePendingFreeEntry {
                page: PageId(64),
                first_chain_page: PageId(65),
                duplicate_chain_page: PageId(66),
            },
            vec!["page:64", "page:65", "page:66"],
        ),
        (
            IntegrityError::PendingFreeEntryStillReachable { page: PageId(67) },
            vec!["page:67"],
        ),
    ];
    for (error, fields) in cases {
        let shown = error.to_string();
        for field in fields {
            assert!(shown.contains(field), "display dropped {field}: {shown}");
        }
    }
}

#[test]
fn every_commit_slot_display_keeps_the_physical_slot_index() {
    let variants = [
        IntegrityError::CommitSlotChecksumMismatch { slot: 7 },
        IntegrityError::CommitSlotMacMismatch { slot: 7 },
        IntegrityError::CommitSlotDowngrade { slot: 7 },
        IntegrityError::CommitSlotUnknownFormat { slot: 7 },
        IntegrityError::CommitSlotUnknownMerkleScheme { slot: 7 },
    ];

    for variant in variants {
        assert!(
            variant.to_string().contains("slot 7"),
            "{variant:?} dropped its slot index"
        );
    }

    let depth = IntegrityError::TreeDepthMismatch {
        root: PageId(55),
        leaf: PageId(56),
        expected: 2,
        actual: 3,
    }
    .to_string();
    for field in ["page:55", "page:56", "2", "3"] {
        assert!(depth.contains(field), "display dropped {field}: {depth}");
    }

    let page_count = IntegrityError::PageCountMetadataMismatch {
        total_pages: 70,
        high_water_mark: 71,
    }
    .to_string();
    for field in ["70", "71"] {
        assert!(
            page_count.contains(field),
            "display dropped {field}: {page_count}"
        );
    }
}

#[test]
fn merkle_errors_keep_full_expected_and_actual_hashes_in_display() {
    let expected = [0x11; citadel_core::MERKLE_HASH_SIZE];
    let actual = [0x22; citadel_core::MERKLE_HASH_SIZE];
    let expected_hex = "11".repeat(citadel_core::MERKLE_HASH_SIZE);
    let actual_hex = "22".repeat(citadel_core::MERKLE_HASH_SIZE);
    for shown in [
        IntegrityError::PageMerkleMismatch {
            page: PageId(57),
            expected,
            actual,
        }
        .to_string(),
        IntegrityError::SlotMerkleRootMismatch { expected, actual }.to_string(),
        IntegrityError::OverflowDigestMismatch {
            first_page: PageId(58),
            expected,
            actual,
        }
        .to_string(),
    ] {
        assert!(
            shown.contains(&expected_hex),
            "expected hash missing: {shown}"
        );
        assert!(shown.contains(&actual_hex), "actual hash missing: {shown}");
    }

    let missing = IntegrityError::OverflowDigestMismatch {
        first_page: PageId(59),
        expected,
        actual: [0u8; citadel_core::MERKLE_HASH_SIZE],
    }
    .to_string();
    for field in ["page:59", "no payload digest", expected_hex.as_str()] {
        assert!(
            missing.contains(field),
            "display dropped {field}: {missing}"
        );
    }
}

#[test]
fn a_report_separates_tampering_from_everything_else() {
    let report = IntegrityReport {
        pages_checked: 3,
        errors: vec![
            IntegrityError::DuplicatePageRef(PageId(1)),
            IntegrityError::PendingFreeEntryCountOutOfBounds {
                page: PageId(4),
                count: 900,
                max: PENDING_FREE_ENTRY_CAPACITY,
            },
            IntegrityError::PageTampered(PageId(2)),
            IntegrityError::ChecksumMismatch(PageId(3)),
            IntegrityError::CommitSlotChecksumMismatch { slot: 0 },
            IntegrityError::CommitSlotMacMismatch { slot: 1 },
            IntegrityError::CommitSlotDowngrade { slot: 0 },
            IntegrityError::CommitSlotUnknownFormat { slot: 1 },
            IntegrityError::CommitSlotUnknownMerkleScheme { slot: 0 },
        ],
    };
    assert!(!report.is_ok());
    let tampered: Vec<_> = report.tampered().collect();
    assert_eq!(
        tampered,
        vec![
            &IntegrityError::PageTampered(PageId(2)),
            &IntegrityError::ChecksumMismatch(PageId(3)),
            &IntegrityError::CommitSlotChecksumMismatch { slot: 0 },
            &IntegrityError::CommitSlotMacMismatch { slot: 1 },
            &IntegrityError::CommitSlotDowngrade { slot: 0 },
            &IntegrityError::CommitSlotUnknownFormat { slot: 1 },
            &IntegrityError::CommitSlotUnknownMerkleScheme { slot: 0 },
        ]
    );
}
