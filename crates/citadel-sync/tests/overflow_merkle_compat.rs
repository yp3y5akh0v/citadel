use std::fs::OpenOptions;
use std::path::Path;

use citadel_core::types::{PageType, ValueType};
use citadel_core::{HEADER_FLAG_SLOTS_V1, MAC_KEY_SIZE, MERKLE_HASH_SIZE, SLOT_MAC_SIZE};
use citadel_io::file_manager::{read_file_header, write_file_header, MerkleScheme, SlotFormat};
use citadel_io::mmap_io::MmapPageIO;
use citadel_io::traits::PageIO;
use citadel_sync::{apply_patch, merkle_diff, LocalTreeReader, NodeId, SyncPatch, TreeReader};
use citadel_txn::manager::TxnManager;

const DEK: [u8; 32] = [0x42; 32];
const MAC_KEY: [u8; MAC_KEY_SIZE] = [0x43; MAC_KEY_SIZE];
const DEK_ID: [u8; 32] = [0x44; 32];

fn create_manager(path: &Path) -> TxnManager {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .unwrap();
    TxnManager::create(
        Box::new(MmapPageIO::try_new(file).unwrap()),
        DEK,
        MAC_KEY,
        1,
        0x1234,
        DEK_ID,
        256,
    )
    .unwrap()
}

fn open_manager(path: &Path) -> TxnManager {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    TxnManager::open(
        Box::new(MmapPageIO::try_new(file).unwrap()),
        DEK,
        MAC_KEY,
        1,
        256,
    )
    .unwrap()
}

#[test]
fn sync_reader_materializes_overflow_payloads_instead_of_physical_refs() {
    let dir = tempfile::tempdir().unwrap();
    let manager = create_manager(&dir.path().join("logical.db"));
    let value = vec![b'v'; 20_000];
    let mut write = manager.begin_write().unwrap();
    write.insert(b"large", &value).unwrap();
    write.create_table(b"named").unwrap();
    write.table_insert(b"named", b"large", &value).unwrap();
    write.commit().unwrap();

    let reader = LocalTreeReader::new(&manager);
    let (root, root_hash) = reader.root_info().unwrap();
    assert_ne!(root_hash, [0u8; MERKLE_HASH_SIZE]);
    reader.page_digest(root).unwrap();
    let entries = reader.leaf_entries(root).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].val_type, ValueType::Overflow as u8);
    assert_eq!(entries[0].value, value);

    let named_reader = LocalTreeReader::for_table(&manager, b"named").unwrap();
    let (named_root, named_hash) = named_reader.root_info().unwrap();
    assert_ne!(named_hash, [0u8; MERKLE_HASH_SIZE]);
    named_reader.page_digest(named_root).unwrap();
    let named_entries = named_reader.leaf_entries(named_root).unwrap();
    assert_eq!(named_entries.len(), 1);
    assert_eq!(named_entries[0].val_type, ValueType::Overflow as u8);
    assert_eq!(named_entries[0].value, value);
}

#[test]
fn legacy_scheme_cannot_prune_or_send_physical_overflow_refs() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("legacy.db");
    let target_path = dir.path().join("logical.db");
    let source_value = vec![b'x'; 20_000];
    let target_value = vec![b'y'; source_value.len()];

    let source = create_manager(&source_path);
    let target = create_manager(&target_path);
    for (manager, overflow) in [(&source, &source_value), (&target, &target_value)] {
        let mut write = manager.begin_write().unwrap();
        write.insert(b"large", overflow).unwrap();
        write.create_table(b"named").unwrap();
        write.table_insert(b"named", b"large", overflow).unwrap();
        for index in 0..1_200u32 {
            let key = format!("key-{index:04}");
            write.insert(key.as_bytes(), b"same-value").unwrap();
        }
        write.commit().unwrap();
    }
    let target_root_hash = target.current_slot().merkle_root;
    drop(source);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&source_path)
        .unwrap();
    let io = MmapPageIO::try_new(file).unwrap();
    let mut header = read_file_header(&io).unwrap();
    let mut legacy = header.slots[header.active_slot()].clone();
    legacy.merkle_scheme = MerkleScheme::Legacy;
    // Model the dangerous case: legacy physical-reference hashing says the
    // roots match even though same-length overflow payloads differ.
    legacy.merkle_root = target_root_hash;
    legacy.slot_format = SlotFormat::Legacy;
    legacy.slot_mac = [0u8; SLOT_MAC_SIZE];
    header.flags &= !HEADER_FLAG_SLOTS_V1;
    header.god_byte = 0;
    header.slots = [legacy.clone(), legacy];
    write_file_header(&io, &header).unwrap();
    io.fsync().unwrap();
    drop(io);

    let source = open_manager(&source_path);
    let reader = LocalTreeReader::new(&source);
    let (root, root_hash) = reader.root_info().unwrap();
    assert_eq!(root_hash, [0u8; MERKLE_HASH_SIZE]);

    let named_reader = LocalTreeReader::for_table(&source, b"named").unwrap();
    let (named_root, named_hash) = named_reader.root_info().unwrap();
    assert_eq!(named_hash, [0u8; MERKLE_HASH_SIZE]);
    let named_digest = named_reader.page_digest(named_root).unwrap();
    assert_eq!(named_digest.merkle_hash, [0u8; MERKLE_HASH_SIZE]);
    let named_entries = named_reader.leaf_entries(named_root).unwrap();
    assert_eq!(named_entries.len(), 1);
    assert_eq!(named_entries[0].value, source_value);

    let mut stack = vec![root];
    let mut branches = 0;
    let mut leaves = 0;
    let mut materialized = None;
    while let Some(page_id) = stack.pop() {
        let digest = reader.page_digest(page_id).unwrap();
        assert_eq!(digest.merkle_hash, [0u8; MERKLE_HASH_SIZE]);
        match digest.page_type {
            PageType::Branch => {
                branches += 1;
                stack.extend(digest.children);
            }
            PageType::Leaf => {
                leaves += 1;
                for entry in reader.leaf_entries(page_id).unwrap() {
                    if entry.key == b"large" {
                        assert_eq!(entry.val_type, ValueType::Overflow as u8);
                        materialized = Some(entry.value);
                    }
                }
            }
            other => panic!("unexpected tree page type: {other:?}"),
        }
    }
    assert!(branches > 0, "fixture did not force a branch page");
    assert!(leaves > 1, "fixture did not force multiple leaf pages");
    assert_eq!(materialized, Some(source_value.clone()));

    let target_reader = LocalTreeReader::new(&target);
    let diff = merkle_diff(&reader, &target_reader).unwrap();
    assert!(diff.pages_compared > 1);
    assert!(diff
        .entries
        .iter()
        .any(|entry| entry.key == b"large" && entry.value == source_value));
    drop(reader);
    drop(target_reader);

    let patch = SyncPatch::from_diff(NodeId::from_u64(1), &diff, false);
    apply_patch(&target, &patch).unwrap();
    assert_eq!(
        target.begin_read().get(b"large").unwrap(),
        Some(source_value)
    );
}
