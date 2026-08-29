use std::collections::{HashMap, HashSet};

use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{
    CancelToken, Result, BODY_SIZE, MAX_VALUE_SIZE, MERKLE_HASH_SIZE, PAGE_HEADER_SIZE, USABLE_SIZE,
};
use citadel_io::file_manager::MerkleScheme;
use citadel_page::page::Page;
use citadel_page::{leaf_node::OverflowRef, overflow};

use crate::catalog::{TableDescriptor, TABLE_DESCRIPTOR_SIZE};
use crate::manager::TxnManager;
use crate::merkle::OverflowPayloadDigest;

const PENDING_FREE_ENTRY_CAPACITY: usize = citadel_core::PENDING_FREE_ENTRIES_PER_PAGE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityReport {
    pub pages_checked: u64,
    pub errors: Vec<IntegrityError>,
}

impl IntegrityReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    /// Errors that mean the bytes on disk were altered, as opposed to a page
    /// that could not be read or a structure that does not add up.
    pub fn tampered(&self) -> impl Iterator<Item = &IntegrityError> {
        self.errors.iter().filter(|e| e.is_tamper())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegrityError {
    /// The commit slot's keyless checksum did not verify.
    CommitSlotChecksumMismatch {
        slot: usize,
    },
    /// A V1 commit slot's keyed authentication tag did not verify.
    CommitSlotMacMismatch {
        slot: usize,
    },
    /// A file that permanently requires V1 slots contains a legacy slot.
    CommitSlotDowngrade {
        slot: usize,
    },
    /// The slot-format marker is neither legacy nor V1.
    CommitSlotUnknownFormat {
        slot: usize,
    },
    /// The commit slot's Merkle-scheme marker is not supported.
    CommitSlotUnknownMerkleScheme {
        slot: usize,
    },
    PageReadFailed {
        page: PageId,
        error: String,
    },
    /// The page's authentication tag did not verify.
    PageTampered(PageId),
    /// The page's checksum did not match.
    ChecksumMismatch(PageId),
    PageIdMismatch {
        expected: PageId,
        actual: PageId,
    },
    PageTransactionOutOfBounds {
        page: PageId,
        page_txn: TxnId,
        slot_txn: TxnId,
    },
    ReachablePageOutOfBounds {
        page: PageId,
        high_water_mark: u32,
    },
    TreeDepthMismatch {
        root: PageId,
        leaf: PageId,
        expected: u16,
        actual: u32,
    },
    PageMerkleMismatch {
        page: PageId,
        expected: [u8; MERKLE_HASH_SIZE],
        actual: [u8; MERKLE_HASH_SIZE],
    },
    SlotMerkleRootMismatch {
        expected: [u8; MERKLE_HASH_SIZE],
        actual: [u8; MERKLE_HASH_SIZE],
    },
    PageCountMetadataMismatch {
        total_pages: u32,
        high_water_mark: u32,
    },
    KeyOrderViolation {
        page: PageId,
        index: usize,
    },
    KeyRangeViolation {
        page: PageId,
        index: usize,
    },
    MalformedPage {
        page: PageId,
        detail: &'static str,
    },
    MalformedOverflowReference {
        page: PageId,
        index: usize,
        actual_size: usize,
    },
    OverflowLengthOutOfBounds {
        page: PageId,
        index: usize,
        declared: u32,
        max: usize,
    },
    OverflowPageDataLengthOutOfBounds {
        page: PageId,
        declared: u32,
        max: usize,
    },
    OverflowChainLengthMismatch {
        first_page: PageId,
        expected: u32,
        actual: u64,
    },
    OverflowChainPageCountOutOfBounds {
        first_page: PageId,
        expected_max: usize,
        actual: usize,
    },
    OverflowDigestMismatch {
        first_page: PageId,
        expected: [u8; MERKLE_HASH_SIZE],
        actual: [u8; MERKLE_HASH_SIZE],
    },
    DuplicatePageRef(PageId),
    EntryCountMismatch {
        expected: u64,
        actual: u64,
    },
    NamedTableEntryCountMismatch {
        table: Option<Vec<u8>>,
        table_hash: u32,
        expected: u64,
        actual: u64,
    },
    MalformedTableDescriptor {
        page: PageId,
        table: Vec<u8>,
        value_type: ValueType,
        actual_size: usize,
    },
    InvalidTableDescriptor {
        page: PageId,
        table: Vec<u8>,
        detail: &'static str,
    },
    NamedTableHashCollision {
        table_hash: u32,
        first_table: Vec<u8>,
        conflicting_table: Vec<u8>,
    },
    DuplicateNamedTableSlotHash {
        table_hash: u32,
        first_index: usize,
        duplicate_index: usize,
    },
    InvalidPageType {
        page: PageId,
        expected: &'static str,
    },
    /// A pending-free page claims more entries than fit in its body.
    PendingFreeEntryCountOutOfBounds {
        page: PageId,
        count: u32,
        max: usize,
    },
    PendingFreePageOutOfBounds {
        page: PageId,
        high_water_mark: u32,
    },
    PendingFreeEntryOutOfBounds {
        chain_page: PageId,
        index: usize,
        entry: PageId,
        high_water_mark: u32,
    },
    PendingFreeTransactionOutOfBounds {
        chain_page: PageId,
        index: usize,
        freed_at: TxnId,
        slot_txn: TxnId,
    },
    DuplicatePendingFreeEntry {
        page: PageId,
        first_chain_page: PageId,
        duplicate_chain_page: PageId,
    },
    PendingFreeEntryStillReachable {
        page: PageId,
    },
}

impl IntegrityError {
    /// Classify a failed page read. Every walk routes through here, so a
    /// tamper is never flattened into a string at one site and typed at another.
    fn from_page_read(page: PageId, error: citadel_core::Error) -> Self {
        match error {
            citadel_core::Error::PageTampered(p) => Self::PageTampered(p),
            citadel_core::Error::ChecksumMismatch(p) => Self::ChecksumMismatch(p),
            other => Self::PageReadFailed {
                page,
                error: other.to_string(),
            },
        }
    }

    /// True when the bytes on disk were altered rather than merely unreadable.
    pub fn is_tamper(&self) -> bool {
        matches!(
            self,
            Self::CommitSlotChecksumMismatch { .. }
                | Self::CommitSlotMacMismatch { .. }
                | Self::CommitSlotDowngrade { .. }
                | Self::CommitSlotUnknownFormat { .. }
                | Self::CommitSlotUnknownMerkleScheme { .. }
                | Self::PageTampered(_)
                | Self::ChecksumMismatch(_)
        )
    }
}

impl std::fmt::Display for IntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommitSlotChecksumMismatch { slot } => {
                write!(
                    f,
                    "commit slot {slot} failed its checksum; its write was torn or altered"
                )
            }
            Self::CommitSlotMacMismatch { slot } => {
                write!(
                    f,
                    "commit slot {slot} failed its authentication tag; its contents were altered"
                )
            }
            Self::CommitSlotDowngrade { slot } => {
                write!(
                    f,
                    "commit slot {slot} uses the legacy format after V1 became mandatory"
                )
            }
            Self::CommitSlotUnknownFormat { slot } => {
                write!(f, "commit slot {slot} has an unrecognized format marker")
            }
            Self::CommitSlotUnknownMerkleScheme { slot } => {
                write!(f, "commit slot {slot} has an unrecognized Merkle-scheme marker")
            }
            Self::PageReadFailed { page, error } => {
                write!(f, "{page} could not be read: {error}")
            }
            Self::PageTampered(page) => {
                write!(
                    f,
                    "{page} failed its authentication tag; contents were altered"
                )
            }
            Self::ChecksumMismatch(page) => {
                write!(
                    f,
                    "{page} failed its checksum; contents were altered or a write was torn"
                )
            }
            Self::PageIdMismatch { expected, actual } => {
                write!(f, "physical {expected} embeds the different id {actual}")
            }
            Self::PageTransactionOutOfBounds {
                page,
                page_txn,
                slot_txn,
            } => write!(
                f,
                "{page} belongs to future {page_txn}, beyond commit {slot_txn}"
            ),
            Self::ReachablePageOutOfBounds {
                page,
                high_water_mark,
            } => write!(
                f,
                "reachable {page} lies beyond commit high-water mark {high_water_mark}"
            ),
            Self::TreeDepthMismatch {
                root,
                leaf,
                expected,
                actual,
            } => write!(
                f,
                "tree at {root} records walk capacity {expected}, but {leaf} requires depth {actual}"
            ),
            Self::PageMerkleMismatch {
                page,
                expected,
                actual,
            } => {
                write!(f, "{page} stores Merkle hash ")?;
                write_hash(f, actual)?;
                write!(f, ", but its contents recompute to ")?;
                write_hash(f, expected)
            }
            Self::SlotMerkleRootMismatch { expected, actual } => {
                write!(f, "commit slot stores Merkle root ")?;
                write_hash(f, actual)?;
                write!(f, ", but the default tree recomputes to ")?;
                write_hash(f, expected)
            }
            Self::PageCountMetadataMismatch {
                total_pages,
                high_water_mark,
            } => write!(
                f,
                "commit slot records total_pages {total_pages}, but high_water_mark is {high_water_mark}"
            ),
            Self::KeyOrderViolation { page, index } => {
                write!(f, "{page} has keys out of order at index {index}")
            }
            Self::KeyRangeViolation { page, index } => {
                write!(
                    f,
                    "{page} has a key outside its parent range at index {index}"
                )
            }
            Self::MalformedPage { page, detail } => {
                write!(f, "{page} has malformed cell metadata: {detail}")
            }
            Self::MalformedOverflowReference {
                page,
                index,
                actual_size,
            } => write!(
                f,
                "{page} cell {index} stores {actual_size} overflow-reference bytes; exactly 8 are required"
            ),
            Self::OverflowLengthOutOfBounds {
                page,
                index,
                declared,
                max,
            } => write!(
                f,
                "{page} cell {index} declares an overflow value of {declared} bytes; the maximum is {max}"
            ),
            Self::OverflowPageDataLengthOutOfBounds {
                page,
                declared,
                max,
            } => write!(
                f,
                "{page} declares {declared} overflow-data bytes; at most {max} fit in one page"
            ),
            Self::OverflowChainLengthMismatch {
                first_page,
                expected,
                actual,
            } => write!(
                f,
                "overflow chain at {first_page} declares {expected} bytes but contains {actual}"
            ),
            Self::OverflowChainPageCountOutOfBounds {
                first_page,
                expected_max,
                actual,
            } => write!(
                f,
                "overflow chain at {first_page} uses {actual} pages; at most {expected_max} are valid for its declared length"
            ),
            Self::OverflowDigestMismatch {
                first_page,
                expected,
                actual,
            } => {
                if actual == &[0u8; MERKLE_HASH_SIZE] {
                    write!(f, "overflow chain at {first_page} has no payload digest; its payload recomputes to ")?;
                    write_hash(f, expected)
                } else {
                    write!(f, "overflow chain at {first_page} stores payload digest ")?;
                    write_hash(f, actual)?;
                    write!(f, ", but its payload recomputes to ")?;
                    write_hash(f, expected)
                }
            }
            Self::DuplicatePageRef(page) => {
                write!(f, "{page} is reachable by more than one path")
            }
            Self::EntryCountMismatch { expected, actual } => {
                write!(
                    f,
                    "the commit slot records {expected} entries but the tree holds {actual}"
                )
            }
            Self::NamedTableEntryCountMismatch {
                table,
                table_hash,
                expected,
                actual,
            } => {
                write!(f, "named table ")?;
                match table {
                    Some(table) => write_escaped_table_name(f, table)?,
                    None => write!(f, "with no catalog name")?,
                }
                write!(
                    f,
                    " (hash {table_hash:#010x}) records {expected} entries but its tree holds {actual}"
                )
            }
            Self::MalformedTableDescriptor {
                page,
                table,
                value_type,
                actual_size,
            } => {
                let value_type = match value_type {
                    ValueType::Inline => "Inline",
                    ValueType::Overflow => "Overflow",
                    ValueType::Tombstone => "Tombstone",
                };
                write!(
                    f,
                    "{page} uses {value_type} with {actual_size} bytes for table descriptor "
                )?;
                write_escaped_table_name(f, table)?;
                write!(
                    f,
                    "; an Inline value of exactly {TABLE_DESCRIPTOR_SIZE} bytes is required"
                )
            }
            Self::InvalidTableDescriptor {
                page,
                table,
                detail,
            } => {
                write!(f, "{page} has an invalid descriptor for table ")?;
                write_escaped_table_name(f, table)?;
                write!(f, ": {detail}")
            }
            Self::NamedTableHashCollision {
                table_hash,
                first_table,
                conflicting_table,
            } => {
                write!(f, "named tables ")?;
                write_escaped_table_name(f, first_table)?;
                write!(f, " and ")?;
                write_escaped_table_name(f, conflicting_table)?;
                write!(f, " share slot hash {table_hash:#010x}")
            }
            Self::DuplicateNamedTableSlotHash {
                table_hash,
                first_index,
                duplicate_index,
            } => write!(
                f,
                "named-table slot entries {first_index} and {duplicate_index} repeat hash {table_hash:#010x}"
            ),
            Self::InvalidPageType { page, expected } => {
                write!(f, "{page} is not a {expected} page")
            }
            Self::PendingFreeEntryCountOutOfBounds { page, count, max } => {
                write!(
                    f,
                    "{page} records {count} pending-free entries, but at most {max} fit in one page"
                )
            }
            Self::PendingFreePageOutOfBounds {
                page,
                high_water_mark,
            } => write!(
                f,
                "pending-free chain page {page} lies beyond high-water mark {high_water_mark}"
            ),
            Self::PendingFreeEntryOutOfBounds {
                chain_page,
                index,
                entry,
                high_water_mark,
            } => write!(
                f,
                "{chain_page} pending-free entry {index} names {entry}, beyond high-water mark {high_water_mark}"
            ),
            Self::PendingFreeTransactionOutOfBounds {
                chain_page,
                index,
                freed_at,
                slot_txn,
            } => write!(
                f,
                "{chain_page} pending-free entry {index} was freed at {freed_at}, outside committed range txn:1..={slot_txn}"
            ),
            Self::DuplicatePendingFreeEntry {
                page,
                first_chain_page,
                duplicate_chain_page,
            } => write!(
                f,
                "pending-free page {page} is listed by both {first_chain_page} and {duplicate_chain_page}"
            ),
            Self::PendingFreeEntryStillReachable { page } => {
                write!(
                    f,
                    "pending-free page {page} is still reachable in the same commit"
                )
            }
        }
    }
}

fn write_escaped_table_name(f: &mut std::fmt::Formatter<'_>, table: &[u8]) -> std::fmt::Result {
    f.write_str("`")?;
    for &byte in table {
        match byte {
            b'`' => f.write_str("\\`")?,
            b'\\' => f.write_str("\\\\")?,
            0x20..=0x7e => write!(f, "{}", char::from(byte))?,
            _ => write!(f, "\\x{byte:02x}")?,
        }
    }
    f.write_str("`")
}

fn write_hash(f: &mut std::fmt::Formatter<'_>, hash: &[u8; MERKLE_HASH_SIZE]) -> std::fmt::Result {
    for byte in hash {
        write!(f, "{byte:02x}")?;
    }
    Ok(())
}

pub(crate) fn run_integrity_check(mgr: &TxnManager) -> Result<IntegrityReport> {
    run_integrity_check_with_cancel(mgr, None)
}

pub(crate) fn run_integrity_check_with_cancel(
    mgr: &TxnManager,
    cancel: Option<&CancelToken>,
) -> Result<IntegrityReport> {
    check_cancel(cancel)?;
    let snapshot = mgr.integrity_snapshot()?;
    check_cancel(cancel)?;
    let active_slot = snapshot.active_slot();
    let mut errors = Vec::new();
    let mut pages_checked: u64 = 0;

    // Validate both raw slots even when one is the txn-0 spare or duplicates
    // the other. Only a committed, authenticated slot is safe to dereference.
    // Active first keeps the report's walk order stable.
    for slot_index in [active_slot, 1 - active_slot] {
        check_cancel(cancel)?;
        let slot = &snapshot.slots()[slot_index];
        let unknown_format = slot.slot_format == citadel_io::file_manager::SlotFormat::Unknown;
        let unknown_merkle_scheme = slot.merkle_scheme == MerkleScheme::Unknown;
        let checksum_valid = !unknown_format && !unknown_merkle_scheme && slot.verify_checksum();
        let mac_valid = unknown_merkle_scheme
            || slot.slot_format != citadel_io::file_manager::SlotFormat::V1
            || snapshot.slot_mac_valid(slot_index);
        let downgrade = snapshot.v1_required()
            && slot.slot_format == citadel_io::file_manager::SlotFormat::Legacy
            && checksum_valid;

        if unknown_format {
            errors.push(IntegrityError::CommitSlotUnknownFormat { slot: slot_index });
        } else if unknown_merkle_scheme {
            errors.push(IntegrityError::CommitSlotUnknownMerkleScheme { slot: slot_index });
        } else if !checksum_valid {
            errors.push(IntegrityError::CommitSlotChecksumMismatch { slot: slot_index });
        }
        if !mac_valid {
            errors.push(IntegrityError::CommitSlotMacMismatch { slot: slot_index });
        }
        if downgrade {
            errors.push(IntegrityError::CommitSlotDowngrade { slot: slot_index });
        }
        if unknown_format
            || unknown_merkle_scheme
            || !checksum_valid
            || !mac_valid
            || downgrade
            || slot.txn_id == citadel_core::types::TxnId(0)
        {
            continue;
        }
        if slot.total_pages != slot.high_water_mark {
            errors.push(IntegrityError::PageCountMetadataMismatch {
                total_pages: slot.total_pages,
                high_water_mark: slot.high_water_mark,
            });
        }

        // Copy-on-write can leave two identical committed slots, both already
        // authenticated; walking identical roots twice adds no coverage.
        if slot_index != active_slot && slot == &snapshot.slots()[active_slot] {
            continue;
        }
        walk_slot(mgr, slot, &mut errors, &mut pages_checked, cancel)?;
    }

    check_cancel(cancel)?;

    Ok(IntegrityReport {
        pages_checked,
        errors,
    })
}

#[inline]
fn check_cancel(cancel: Option<&CancelToken>) -> Result<()> {
    match cancel {
        Some(token) => token.check(),
        None => Ok(()),
    }
}

fn check_page_header(
    page: &Page,
    expected: PageId,
    slot_txn: TxnId,
    errors: &mut Vec<IntegrityError>,
) {
    let actual = page.page_id();
    if actual != expected {
        errors.push(IntegrityError::PageIdMismatch { expected, actual });
    }
    let page_txn = page.txn_id();
    if page_txn > slot_txn {
        errors.push(IntegrityError::PageTransactionOutOfBounds {
            page: expected,
            page_txn,
            slot_txn,
        });
    }
}

struct WalkContext<'a> {
    mgr: &'a TxnManager,
    slot_txn: TxnId,
    high_water_mark: u32,
    verify_merkle: bool,
    accept_missing_overflow_digest: bool,
    errors: &'a mut Vec<IntegrityError>,
    pages_checked: &'a mut u64,
    cancel: Option<&'a CancelToken>,
}

impl WalkContext<'_> {
    fn check_cancel(&self) -> Result<()> {
        check_cancel(self.cancel)
    }

    fn reachable_page_is_out_of_bounds(&mut self, page: PageId) -> bool {
        if page.as_u32() < self.high_water_mark {
            return false;
        }
        self.errors.push(IntegrityError::ReachablePageOutOfBounds {
            page,
            high_water_mark: self.high_water_mark,
        });
        true
    }

    fn read_page(&mut self, page_id: PageId) -> Option<Page> {
        let page = match self.mgr.read_page_from_disk(page_id) {
            Ok(page) => page,
            Err(error) => {
                self.errors
                    .push(IntegrityError::from_page_read(page_id, error));
                return None;
            }
        };
        *self.pages_checked += 1;
        check_page_header(&page, page_id, self.slot_txn, self.errors);
        Some(page)
    }
}

#[derive(Clone)]
struct TreeSummary {
    entry_count: u64,
    deepest_leaf: Option<(PageId, u32)>,
    root_hash: Option<[u8; MERKLE_HASH_SIZE]>,
}

struct MerkleWalk {
    root: PageId,
    nodes: HashMap<PageId, MerkleNode>,
    visit_order: Vec<PageId>,
}

struct MerkleNode {
    stored: [u8; MERKLE_HASH_SIZE],
    kind: MerkleNodeKind,
}

enum MerkleNodeKind {
    Leaf([u8; MERKLE_HASH_SIZE]),
    Branch(Vec<PageId>),
}

impl MerkleWalk {
    fn new(root: PageId) -> Self {
        Self {
            root,
            nodes: HashMap::new(),
            visit_order: Vec::new(),
        }
    }

    fn record_leaf(&mut self, page_id: PageId, page: &Page, logical_hash: [u8; MERKLE_HASH_SIZE]) {
        self.visit_order.push(page_id);
        self.nodes.insert(
            page_id,
            MerkleNode {
                stored: page.merkle_hash(),
                kind: MerkleNodeKind::Leaf(logical_hash),
            },
        );
    }

    fn record_branch(&mut self, page_id: PageId, page: &Page, cells: &[CheckedBranchCell<'_>]) {
        let mut children: Vec<PageId> = cells.iter().map(|cell| cell.child).collect();
        if page.right_child().is_valid() {
            children.push(page.right_child());
        }
        self.visit_order.push(page_id);
        self.nodes.insert(
            page_id,
            MerkleNode {
                stored: page.merkle_hash(),
                kind: MerkleNodeKind::Branch(children),
            },
        );
    }

    fn finish(
        mut self,
        ctx: &mut WalkContext<'_>,
        root_must_be_known: bool,
    ) -> Result<Option<[u8; MERKLE_HASH_SIZE]>> {
        // A zero root makes this individual tree UNKNOWN. That remains valid
        // for an untouched named/catalog tree after an Off -> Full transition,
        // even when the default tree already has a certifying slot root. Once
        // this tree's root (or the slot metadata for the default tree) claims a
        // nonzero digest, however, every descendant must be known: the writer
        // propagates UNKNOWN all the way to the root.
        let require_complete = root_must_be_known
            || self
                .nodes
                .get(&self.root)
                .is_some_and(|node| node.stored != [0u8; MERKLE_HASH_SIZE]);
        let mut expected_by_page = HashMap::<PageId, Option<[u8; MERKLE_HASH_SIZE]>>::new();
        for page_id in self.visit_order.into_iter().rev() {
            ctx.check_cancel()?;
            let Some(node) = self.nodes.remove(&page_id) else {
                continue;
            };
            // Zero is the on-disk UNKNOWN sentinel written by Off mode. A
            // freshly recomputed content hash cannot retroactively certify
            // this page or any stored ancestor that depends on it, so UNKNOWN
            // propagates rather than manufacturing a mismatch.
            if node.stored == [0u8; MERKLE_HASH_SIZE] && !require_complete {
                expected_by_page.insert(page_id, None);
                continue;
            }
            let expected = match node.kind {
                MerkleNodeKind::Leaf(hash) => hash,
                MerkleNodeKind::Branch(children) => {
                    let mut hasher = blake3::Hasher::new();
                    let mut complete = true;
                    for child in children {
                        ctx.check_cancel()?;
                        match expected_by_page.get(&child) {
                            Some(Some(hash)) => {
                                hasher.update(hash);
                            }
                            Some(None) | None => {
                                complete = false;
                                break;
                            }
                        }
                    }
                    if !complete {
                        expected_by_page.insert(page_id, None);
                        continue;
                    }
                    truncate_hash(&hasher.finalize())
                }
            };
            if expected != node.stored {
                ctx.errors.push(IntegrityError::PageMerkleMismatch {
                    page: page_id,
                    expected,
                    actual: node.stored,
                });
            }
            expected_by_page.insert(page_id, Some(expected));
        }
        Ok(expected_by_page.get(&self.root).copied().flatten())
    }
}

fn hash_checked_leaf_cells(
    cells: &[CheckedLeafCell<'_>],
    overflow_digests: &[Option<[u8; MERKLE_HASH_SIZE]>],
) -> Option<[u8; MERKLE_HASH_SIZE]> {
    let mut overflow_digests = overflow_digests.iter();
    crate::merkle::hash_logical_leaf_cells(
        cells
            .iter()
            .map(|cell| (cell.key, cell.val_type, cell.value)),
        |_| {
            overflow_digests
                .next()
                .copied()
                .flatten()
                .ok_or(citadel_core::Error::DatabaseCorrupted)
        },
    )
    .ok()
}

fn truncate_hash(hash: &blake3::Hash) -> [u8; MERKLE_HASH_SIZE] {
    let mut out = [0u8; MERKLE_HASH_SIZE];
    out.copy_from_slice(&hash.as_bytes()[..MERKLE_HASH_SIZE]);
    out
}

/// Walk everything one commit slot roots.
///
/// `visited` is per slot: copy-on-write leaves both slots sharing almost every
/// page, so a shared set would report the second slot's pages as
/// `DuplicatePageRef` and call a healthy database corrupt. Within one slot,
/// reaching a page twice really is corruption.
fn walk_slot(
    mgr: &TxnManager,
    slot: &citadel_io::file_manager::CommitSlot,
    errors: &mut Vec<IntegrityError>,
    pages_checked: &mut u64,
    cancel: Option<&CancelToken>,
) -> Result<()> {
    let mut visited = HashSet::new();
    let mut ctx = WalkContext {
        mgr,
        slot_txn: slot.txn_id,
        high_water_mark: slot.high_water_mark,
        verify_merkle: slot.merkle_scheme == MerkleScheme::LogicalOverflowV1
            && slot.merkle_root != [0u8; MERKLE_HASH_SIZE],
        accept_missing_overflow_digest: slot.merkle_scheme == MerkleScheme::Legacy,
        errors,
        pages_checked,
        cancel,
    };

    let default_root_must_be_known = ctx.verify_merkle;
    let default_tree = walk_tree(
        &mut ctx,
        slot.tree_root,
        &mut visited,
        default_root_must_be_known,
    )?;
    report_tree_depth(&mut ctx, slot.tree_root, slot.tree_depth, &default_tree)?;

    if default_tree.entry_count != slot.tree_entries {
        ctx.errors.push(IntegrityError::EntryCountMismatch {
            expected: slot.tree_entries,
            actual: default_tree.entry_count,
        });
    }
    if let Some(expected) = default_tree.root_hash {
        if expected != slot.merkle_root {
            ctx.errors.push(IntegrityError::SlotMerkleRootMismatch {
                expected,
                actual: slot.merkle_root,
            });
        }
    }

    let mut named_tables = if slot.catalog_root.is_valid() {
        collect_named_tables(&mut ctx, slot.catalog_root, &mut visited)?
    } else {
        Vec::new()
    };

    let ambiguous_hashes =
        merge_slot_named_tables(slot, &mut named_tables, ctx.errors, ctx.cancel)?;
    let mut walked_ambiguous_roots: HashMap<(u32, PageId), TreeSummary> = HashMap::new();
    for table in named_tables {
        ctx.check_cancel()?;
        let tree = if ambiguous_hashes.contains(&table.hash) {
            let key = (table.hash, table.root);
            if let Some(summary) = walked_ambiguous_roots.get(&key) {
                summary.clone()
            } else {
                let summary = walk_tree(&mut ctx, table.root, &mut visited, false)?;
                walked_ambiguous_roots.insert(key, summary.clone());
                summary
            }
        } else {
            walk_tree(&mut ctx, table.root, &mut visited, false)?
        };
        report_tree_depth(&mut ctx, table.root, table.depth, &tree)?;
        if tree.entry_count != table.entry_count {
            ctx.errors
                .push(IntegrityError::NamedTableEntryCountMismatch {
                    table: table.name,
                    table_hash: table.hash,
                    expected: table.entry_count,
                    actual: tree.entry_count,
                });
        }
    }

    if slot.pending_free_root.is_valid() {
        walk_chain(&mut ctx, slot.pending_free_root, &mut visited)?;
    }
    ctx.check_cancel()
}

fn report_tree_depth(
    ctx: &mut WalkContext<'_>,
    root: PageId,
    expected: u16,
    summary: &TreeSummary,
) -> Result<()> {
    ctx.check_cancel()?;
    let Some((leaf, actual)) = summary.deepest_leaf else {
        return Ok(());
    };
    // Deletion can splice a drained non-root branch's only child upward,
    // making some paths shorter without reducing BTree::depth. The stored
    // value is therefore a walk-capacity bound, not a balance assertion.
    if actual > u32::from(expected) {
        ctx.errors.push(IntegrityError::TreeDepthMismatch {
            root,
            leaf,
            expected,
            actual,
        });
    }
    Ok(())
}

fn walk_tree(
    ctx: &mut WalkContext<'_>,
    root: PageId,
    visited: &mut HashSet<PageId>,
    root_must_be_known: bool,
) -> Result<TreeSummary> {
    let mut entry_count: u64 = 0;
    let mut deepest_leaf = None;
    let mut merkle = ctx.verify_merkle.then(|| MerkleWalk::new(root));
    let mut stack = vec![TreeFrame::root(root)];

    while let Some(frame) = stack.pop() {
        ctx.check_cancel()?;
        let page_id = frame.page;
        if ctx.reachable_page_is_out_of_bounds(page_id) {
            continue;
        }
        if !visited.insert(page_id) {
            ctx.errors.push(IntegrityError::DuplicatePageRef(page_id));
            continue;
        }

        let Some(page) = ctx.read_page(page_id) else {
            continue;
        };

        match page.page_type() {
            Some(PageType::Leaf) => {
                let cells = match checked_leaf_cells(&page) {
                    Ok(cells) => cells,
                    Err(detail) => {
                        ctx.errors.push(IntegrityError::MalformedPage {
                            page: page_id,
                            detail,
                        });
                        continue;
                    }
                };
                entry_count += cells.len() as u64;
                if deepest_leaf.is_none_or(|(_, depth)| frame.depth > depth) {
                    deepest_leaf = Some((page_id, frame.depth));
                }
                check_leaf_keys(&cells, &frame, ctx.errors);
                let overflow_digests = walk_overflow_cells(ctx, page_id, &cells, visited)?;
                if let (Some(merkle), Some(logical_hash)) = (
                    &mut merkle,
                    hash_checked_leaf_cells(&cells, &overflow_digests),
                ) {
                    merkle.record_leaf(page_id, &page, logical_hash);
                }
            }
            Some(PageType::Branch) => {
                let cells = match checked_branch_cells(&page) {
                    Ok(cells) => cells,
                    Err(detail) => {
                        ctx.errors.push(IntegrityError::MalformedPage {
                            page: page_id,
                            detail,
                        });
                        continue;
                    }
                };
                if let Some(merkle) = &mut merkle {
                    merkle.record_branch(page_id, &page, &cells);
                }
                let keys_are_usable = check_branch_keys(&cells, &frame, ctx.errors);
                push_branch_children(
                    &cells,
                    page.right_child(),
                    &frame,
                    keys_are_usable,
                    &mut stack,
                    ctx.errors,
                );
                if !page.right_child().is_valid() {
                    ctx.errors.push(IntegrityError::MalformedPage {
                        page: page_id,
                        detail: "branch right-child reference is invalid",
                    });
                }
            }
            _ => {
                ctx.errors.push(IntegrityError::InvalidPageType {
                    page: page_id,
                    expected: "Leaf or Branch",
                });
            }
        }
    }

    let root_hash = match merkle {
        Some(merkle) => merkle.finish(ctx, root_must_be_known)?,
        None => None,
    };
    Ok(TreeSummary {
        entry_count,
        deepest_leaf,
        root_hash,
    })
}

#[derive(Clone)]
struct TreeFrame {
    page: PageId,
    depth: u32,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
}

impl TreeFrame {
    fn root(page: PageId) -> Self {
        Self {
            page,
            depth: 1,
            lower: None,
            upper: None,
        }
    }
}

struct CheckedBranchCell<'a> {
    child: PageId,
    key: &'a [u8],
}

struct CheckedLeafCell<'a> {
    key: &'a [u8],
    val_type: ValueType,
    value: &'a [u8],
}

fn checked_cell_offsets(page: &Page) -> std::result::Result<Vec<usize>, &'static str> {
    let count = page.num_cells() as usize;
    let pointer_end = PAGE_HEADER_SIZE
        .checked_add(
            count
                .checked_mul(2)
                .ok_or("cell pointer array size overflow")?,
        )
        .ok_or("cell pointer array size overflow")?;
    if pointer_end > BODY_SIZE {
        return Err("cell pointer array exceeds the page body");
    }

    let cell_area_start = page.cell_area_start() as usize;
    if cell_area_start < pointer_end || cell_area_start > BODY_SIZE {
        return Err("cell area start overlaps the cell pointer array");
    }

    let mut offsets = Vec::with_capacity(count);
    for index in 0..count {
        let pointer = PAGE_HEADER_SIZE + index * 2;
        let offset = u16::from_le_bytes([page.data[pointer], page.data[pointer + 1]]) as usize;
        if offset < cell_area_start || offset >= BODY_SIZE {
            return Err("cell offset lies outside the cell area");
        }
        offsets.push(offset);
    }
    Ok(offsets)
}

fn checked_branch_cells(
    page: &Page,
) -> std::result::Result<Vec<CheckedBranchCell<'_>>, &'static str> {
    let offsets = checked_cell_offsets(page)?;
    let mut cells = Vec::with_capacity(offsets.len());
    let mut spans = Vec::with_capacity(offsets.len());
    for offset in offsets {
        let fixed_end = offset
            .checked_add(6)
            .filter(|&end| end <= BODY_SIZE)
            .ok_or("branch cell header exceeds the page body")?;
        let child = PageId(u32::from_le_bytes([
            page.data[offset],
            page.data[offset + 1],
            page.data[offset + 2],
            page.data[offset + 3],
        ]));
        let key_len = u16::from_le_bytes([page.data[offset + 4], page.data[offset + 5]]) as usize;
        let end = fixed_end
            .checked_add(key_len)
            .filter(|&end| end <= BODY_SIZE)
            .ok_or("branch cell key exceeds the page body")?;
        spans.push((offset, end));
        cells.push(CheckedBranchCell {
            child,
            key: &page.data[fixed_end..end],
        });
    }
    ensure_non_overlapping(&mut spans, "branch cells overlap")?;
    validate_free_space(page, &spans)?;
    Ok(cells)
}

fn checked_leaf_cells(page: &Page) -> std::result::Result<Vec<CheckedLeafCell<'_>>, &'static str> {
    let offsets = checked_cell_offsets(page)?;
    let mut cells = Vec::with_capacity(offsets.len());
    let mut spans = Vec::with_capacity(offsets.len());
    for offset in offsets {
        let fixed_end = offset
            .checked_add(6)
            .filter(|&end| end <= BODY_SIZE)
            .ok_or("leaf cell header exceeds the page body")?;
        let key_len = u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
        let value_len = u32::from_le_bytes([
            page.data[offset + 2],
            page.data[offset + 3],
            page.data[offset + 4],
            page.data[offset + 5],
        ]) as usize;
        let value_type_offset = fixed_end
            .checked_add(key_len)
            .filter(|&end| end < BODY_SIZE)
            .ok_or("leaf cell key exceeds the page body")?;
        let end = value_type_offset
            .checked_add(1)
            .and_then(|start| start.checked_add(value_len))
            .filter(|&end| end <= BODY_SIZE)
            .ok_or("leaf cell value exceeds the page body")?;
        let val_type = ValueType::from_u8(page.data[value_type_offset])
            .ok_or("leaf cell value type is invalid")?;
        spans.push((offset, end));
        cells.push(CheckedLeafCell {
            key: &page.data[fixed_end..value_type_offset],
            val_type,
            value: &page.data[value_type_offset + 1..end],
        });
    }
    ensure_non_overlapping(&mut spans, "leaf cells overlap")?;
    validate_free_space(page, &spans)?;
    Ok(cells)
}

fn ensure_non_overlapping(
    spans: &mut [(usize, usize)],
    detail: &'static str,
) -> std::result::Result<(), &'static str> {
    spans.sort_unstable_by_key(|&(start, _)| start);
    if spans.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        Err(detail)
    } else {
        Ok(())
    }
}

fn validate_free_space(
    page: &Page,
    spans: &[(usize, usize)],
) -> std::result::Result<(), &'static str> {
    let pointer_bytes = spans
        .len()
        .checked_mul(2)
        .ok_or("free-space accounting overflow")?;
    let cell_bytes = spans.iter().try_fold(0usize, |total, &(start, end)| {
        total
            .checked_add(end - start)
            .ok_or("free-space accounting overflow")
    })?;
    let expected = USABLE_SIZE
        .checked_sub(pointer_bytes)
        .and_then(|space| space.checked_sub(cell_bytes))
        .ok_or("live cells exceed usable page space")?;
    if page.free_space() as usize != expected {
        return Err("free-space accounting does not match live cells");
    }
    Ok(())
}

fn walk_overflow_cells(
    ctx: &mut WalkContext<'_>,
    leaf_page: PageId,
    cells: &[CheckedLeafCell<'_>],
    visited: &mut HashSet<PageId>,
) -> Result<Vec<Option<[u8; MERKLE_HASH_SIZE]>>> {
    let mut digests = Vec::new();
    for (index, cell) in cells.iter().enumerate() {
        ctx.check_cancel()?;
        if cell.val_type != ValueType::Overflow {
            continue;
        }
        if cell.value.len() != 8 {
            ctx.errors.push(IntegrityError::MalformedOverflowReference {
                page: leaf_page,
                index,
                actual_size: cell.value.len(),
            });
            digests.push(None);
            continue;
        }

        let reference = OverflowRef::from_bytes(cell.value);
        if reference.total_len as usize > MAX_VALUE_SIZE {
            ctx.errors.push(IntegrityError::OverflowLengthOutOfBounds {
                page: leaf_page,
                index,
                declared: reference.total_len,
                max: MAX_VALUE_SIZE,
            });
        }
        digests.push(walk_overflow_chain(ctx, &reference, visited)?);
    }
    Ok(digests)
}

fn walk_overflow_chain(
    ctx: &mut WalkContext<'_>,
    reference: &OverflowRef,
    visited: &mut HashSet<PageId>,
) -> Result<Option<[u8; MERKLE_HASH_SIZE]>> {
    let first_page = reference.first_page;
    let expected_max = overflow::pages_needed(reference.total_len as usize);
    let mut current = first_page;
    let mut actual_len = Some(0u64);
    let mut payload_digest = Some(OverflowPayloadDigest::new(reference.total_len));
    let mut stored_digest = None;
    let mut page_count = 0usize;
    let mut page_count_reported = false;

    // Overflow chains use page zero, not PageId::INVALID, as their terminator.
    while current.as_u32() != 0 {
        ctx.check_cancel()?;
        if ctx.reachable_page_is_out_of_bounds(current) {
            return Ok(None);
        }
        if !visited.insert(current) {
            ctx.errors.push(IntegrityError::DuplicatePageRef(current));
            return Ok(None);
        }

        let Some(page) = ctx.read_page(current) else {
            return Ok(None);
        };

        if page.page_type() != Some(PageType::Overflow) {
            ctx.errors.push(IntegrityError::InvalidPageType {
                page: current,
                expected: "Overflow",
            });
            return Ok(None);
        }

        page_count += 1;
        if page_count == 1 {
            stored_digest = Some(page.merkle_hash());
        }
        if page_count > expected_max && !page_count_reported {
            ctx.errors
                .push(IntegrityError::OverflowChainPageCountOutOfBounds {
                    first_page,
                    expected_max,
                    actual: page_count,
                });
            page_count_reported = true;
        }

        let declared = overflow::data_len(&page);
        if declared as usize > overflow::OVERFLOW_DATA_CAPACITY {
            ctx.errors
                .push(IntegrityError::OverflowPageDataLengthOutOfBounds {
                    page: current,
                    declared,
                    max: overflow::OVERFLOW_DATA_CAPACITY,
                });
            actual_len = None;
            payload_digest = None;
        } else if let Some(actual) = &mut actual_len {
            *actual += u64::from(declared);
            if let Some(digest) = &mut payload_digest {
                digest.update(overflow::read_data(&page));
            }
        }
        current = overflow::next_page(&page);
    }

    if let Some(actual) = actual_len {
        if actual != u64::from(reference.total_len) {
            ctx.errors
                .push(IntegrityError::OverflowChainLengthMismatch {
                    first_page,
                    expected: reference.total_len,
                    actual,
                });
        }
    }
    ctx.check_cancel()?;
    let Some(payload_digest) = payload_digest else {
        return Ok(None);
    };
    let expected = payload_digest.finalize();
    let actual = stored_digest.unwrap_or([0u8; MERKLE_HASH_SIZE]);
    if actual == [0u8; MERKLE_HASH_SIZE] && ctx.accept_missing_overflow_digest {
        return Ok(None);
    }
    if expected != actual {
        ctx.errors.push(IntegrityError::OverflowDigestMismatch {
            first_page,
            expected,
            actual,
        });
    }
    Ok(Some(expected))
}

fn key_in_range(key: &[u8], frame: &TreeFrame) -> bool {
    frame.lower.as_deref().is_none_or(|lower| key >= lower)
        && frame.upper.as_deref().is_none_or(|upper| key < upper)
}

fn check_leaf_keys(
    cells: &[CheckedLeafCell<'_>],
    frame: &TreeFrame,
    errors: &mut Vec<IntegrityError>,
) {
    for (index, pair) in cells.windows(2).enumerate() {
        if pair[0].key >= pair[1].key {
            errors.push(IntegrityError::KeyOrderViolation {
                page: frame.page,
                index: index + 1,
            });
        }
    }
    for (index, cell) in cells.iter().enumerate() {
        if !key_in_range(cell.key, frame) {
            errors.push(IntegrityError::KeyRangeViolation {
                page: frame.page,
                index,
            });
        }
    }
}

/// Returns whether the separator keys are safe to use as child bounds. When
/// they are not, callers still walk every valid child using the inherited
/// bounds so one corrupt branch cannot hide the rest of the tree.
fn check_branch_keys(
    cells: &[CheckedBranchCell<'_>],
    frame: &TreeFrame,
    errors: &mut Vec<IntegrityError>,
) -> bool {
    let mut usable = true;
    for (index, pair) in cells.windows(2).enumerate() {
        if pair[0].key >= pair[1].key {
            errors.push(IntegrityError::KeyOrderViolation {
                page: frame.page,
                index: index + 1,
            });
            usable = false;
        }
    }
    for (index, cell) in cells.iter().enumerate() {
        if !key_in_range(cell.key, frame) {
            errors.push(IntegrityError::KeyRangeViolation {
                page: frame.page,
                index,
            });
            usable = false;
        }
    }
    usable
}

fn push_branch_children(
    cells: &[CheckedBranchCell<'_>],
    right_child: PageId,
    frame: &TreeFrame,
    keys_are_usable: bool,
    stack: &mut Vec<TreeFrame>,
    errors: &mut Vec<IntegrityError>,
) {
    for (index, cell) in cells.iter().enumerate() {
        if !cell.child.is_valid() {
            errors.push(IntegrityError::MalformedPage {
                page: frame.page,
                detail: "branch child reference is invalid",
            });
            continue;
        }
        let (lower, upper) = if keys_are_usable {
            (
                if index == 0 {
                    frame.lower.clone()
                } else {
                    Some(cells[index - 1].key.to_vec())
                },
                Some(cell.key.to_vec()),
            )
        } else {
            (frame.lower.clone(), frame.upper.clone())
        };
        stack.push(TreeFrame {
            page: cell.child,
            depth: frame.depth.saturating_add(1),
            lower,
            upper,
        });
    }

    if right_child.is_valid() {
        let lower = if keys_are_usable {
            cells
                .last()
                .map(|cell| cell.key.to_vec())
                .or_else(|| frame.lower.clone())
        } else {
            frame.lower.clone()
        };
        stack.push(TreeFrame {
            page: right_child,
            depth: frame.depth.saturating_add(1),
            lower,
            upper: frame.upper.clone(),
        });
    }
}

struct NamedTable {
    name: Option<Vec<u8>>,
    hash: u32,
    root: PageId,
    entry_count: u64,
    depth: u16,
}

/// Merge the commit slot's fast-path metadata with the catalog descriptors.
///
/// A slot entry with a root wins: after a `SyncMode::Off` catalog skip the
/// descriptor stays stale and the slot is the sole record of the current tree,
/// and an entry with no catalog match is itself a durable root. Once names or
/// entries collide in the 32-bit slot namespace, keep every catalog and runtime
/// root; the caller deduplicates identical ones inside that ambiguity group.
fn merge_slot_named_tables(
    slot: &citadel_io::file_manager::CommitSlot,
    tables: &mut Vec<NamedTable>,
    errors: &mut Vec<IntegrityError>,
    cancel: Option<&CancelToken>,
) -> Result<HashSet<u32>> {
    let mut ambiguous_hashes = report_named_table_hash_collisions(tables, errors, cancel)?;

    // Once a hash is ambiguous no hash-only record can replace a full catalog
    // descriptor, so find the duplicates before merging any entry.
    let mut first_slot_index_by_hash = HashMap::new();
    for (slot_index, &(hash, _, _, _)) in slot.named_table_entries.iter().enumerate() {
        check_cancel(cancel)?;
        if let Some(&first_index) = first_slot_index_by_hash.get(&hash) {
            errors.push(IntegrityError::DuplicateNamedTableSlotHash {
                table_hash: hash,
                first_index,
                duplicate_index: slot_index,
            });
            ambiguous_hashes.insert(hash);
        } else {
            first_slot_index_by_hash.insert(hash, slot_index);
        }
    }

    for &(hash, flagged_count, root, depth) in &slot.named_table_entries {
        check_cancel(cancel)?;
        let entry_count = flagged_count & !citadel_core::SLOT_ENTRY_STALE;
        // Page zero is a valid reusable page id. The cache's absent sentinel
        // is the pair (root=0, depth=0), while (root=0, depth>=1) names a real
        // tree rooted at physical page zero.
        let root_is_present = root != 0 || depth != 0;
        let mut matching = Vec::new();
        for (index, table) in tables.iter().enumerate() {
            check_cancel(cancel)?;
            if table.hash == hash {
                matching.push(index);
            }
        }
        if ambiguous_hashes.contains(&hash) {
            if root_is_present {
                // A hash-only entry belongs to neither full name, so keep the
                // descriptors and add every runtime root.
                tables.push(NamedTable {
                    name: None,
                    hash,
                    root: PageId(root),
                    entry_count,
                    depth,
                });
            }
            continue;
        }
        if let Some(&first) = matching.first() {
            tables[first].entry_count = entry_count;
            if root_is_present {
                tables[first].root = PageId(root);
                tables[first].depth = depth;
            } else {
                for &index in &matching {
                    tables[index].entry_count = entry_count;
                }
            }
        } else if root_is_present {
            tables.push(NamedTable {
                name: None,
                hash,
                root: PageId(root),
                entry_count,
                depth,
            });
        }
    }

    Ok(ambiguous_hashes)
}

/// A slot entry is not required for a 32-bit catalog hash collision to be
/// dangerous: either name can acquire a hash-only fast-path entry on a later
/// commit, after which runtime lookup aliases both names to the first match.
/// Report the ambiguity while the catalog still holds both full names.
fn report_named_table_hash_collisions(
    tables: &[NamedTable],
    errors: &mut Vec<IntegrityError>,
    cancel: Option<&CancelToken>,
) -> Result<HashSet<u32>> {
    let mut first_by_hash: HashMap<u32, Vec<u8>> = HashMap::new();
    let mut colliding_hashes = HashSet::new();
    for table in tables {
        check_cancel(cancel)?;
        let Some(name) = table.name.as_ref() else {
            continue;
        };
        if let Some(first) = first_by_hash.get(&table.hash) {
            if first != name {
                colliding_hashes.insert(table.hash);
                errors.push(IntegrityError::NamedTableHashCollision {
                    table_hash: table.hash,
                    first_table: first.clone(),
                    conflicting_table: name.clone(),
                });
            }
        } else {
            first_by_hash.insert(table.hash, name.clone());
        }
    }

    Ok(colliding_hashes)
}

fn collect_named_tables(
    ctx: &mut WalkContext<'_>,
    catalog_root: PageId,
    visited: &mut HashSet<PageId>,
) -> Result<Vec<NamedTable>> {
    let mut tables = Vec::new();
    let mut merkle = ctx.verify_merkle.then(|| MerkleWalk::new(catalog_root));
    let mut stack = vec![TreeFrame::root(catalog_root)];

    while let Some(frame) = stack.pop() {
        ctx.check_cancel()?;
        let page_id = frame.page;
        if ctx.reachable_page_is_out_of_bounds(page_id) {
            continue;
        }
        if !visited.insert(page_id) {
            ctx.errors.push(IntegrityError::DuplicatePageRef(page_id));
            continue;
        }

        let Some(page) = ctx.read_page(page_id) else {
            continue;
        };

        match page.page_type() {
            Some(PageType::Leaf) => {
                let cells = match checked_leaf_cells(&page) {
                    Ok(cells) => cells,
                    Err(detail) => {
                        ctx.errors.push(IntegrityError::MalformedPage {
                            page: page_id,
                            detail,
                        });
                        continue;
                    }
                };
                check_leaf_keys(&cells, &frame, ctx.errors);
                let overflow_digests = walk_overflow_cells(ctx, page_id, &cells, visited)?;
                if let (Some(merkle), Some(logical_hash)) = (
                    &mut merkle,
                    hash_checked_leaf_cells(&cells, &overflow_digests),
                ) {
                    merkle.record_leaf(page_id, &page, logical_hash);
                }
                for cell in cells {
                    if cell.val_type == ValueType::Tombstone {
                        continue;
                    }
                    if cell.val_type != ValueType::Inline
                        || cell.value.len() != TABLE_DESCRIPTOR_SIZE
                    {
                        ctx.errors.push(IntegrityError::MalformedTableDescriptor {
                            page: page_id,
                            table: cell.key.to_vec(),
                            value_type: cell.val_type,
                            actual_size: cell.value.len(),
                        });
                        continue;
                    }
                    let desc = TableDescriptor::deserialize(cell.value);
                    let detail = if !desc.root_page.is_valid() {
                        Some("root page is invalid")
                    } else if desc.root_page.as_u32() >= ctx.high_water_mark {
                        Some("root page lies beyond the commit high-water mark")
                    } else if desc.depth == 0 {
                        Some("tree depth is zero")
                    } else {
                        None
                    };
                    if let Some(detail) = detail {
                        ctx.errors.push(IntegrityError::InvalidTableDescriptor {
                            page: page_id,
                            table: cell.key.to_vec(),
                            detail,
                        });
                        continue;
                    }
                    tables.push(NamedTable {
                        name: Some(cell.key.to_vec()),
                        hash: citadel_io::file_manager::table_name_hash(cell.key),
                        root: desc.root_page,
                        entry_count: desc.entry_count,
                        depth: desc.depth,
                    });
                }
            }
            Some(PageType::Branch) => {
                let cells = match checked_branch_cells(&page) {
                    Ok(cells) => cells,
                    Err(detail) => {
                        ctx.errors.push(IntegrityError::MalformedPage {
                            page: page_id,
                            detail,
                        });
                        continue;
                    }
                };
                if let Some(merkle) = &mut merkle {
                    merkle.record_branch(page_id, &page, &cells);
                }
                let keys_are_usable = check_branch_keys(&cells, &frame, ctx.errors);
                push_branch_children(
                    &cells,
                    page.right_child(),
                    &frame,
                    keys_are_usable,
                    &mut stack,
                    ctx.errors,
                );
                if !page.right_child().is_valid() {
                    ctx.errors.push(IntegrityError::MalformedPage {
                        page: page_id,
                        detail: "branch right-child reference is invalid",
                    });
                }
            }
            _ => {
                ctx.errors.push(IntegrityError::InvalidPageType {
                    page: page_id,
                    expected: "Leaf or Branch (catalog)",
                });
            }
        }
    }

    if let Some(merkle) = merkle {
        let _ = merkle.finish(ctx, false)?;
    }

    Ok(tables)
}

fn walk_chain(
    ctx: &mut WalkContext<'_>,
    root: PageId,
    visited: &mut HashSet<PageId>,
) -> Result<()> {
    let mut current = root;
    let mut entries = HashMap::<PageId, PageId>::new();
    while current.is_valid() {
        ctx.check_cancel()?;
        if current.as_u32() >= ctx.high_water_mark {
            ctx.errors.push(IntegrityError::PendingFreePageOutOfBounds {
                page: current,
                high_water_mark: ctx.high_water_mark,
            });
            break;
        }
        if !visited.insert(current) {
            ctx.errors.push(IntegrityError::DuplicatePageRef(current));
            break;
        }

        let Some(page) = ctx.read_page(current) else {
            return Ok(());
        };

        if page.page_type() != Some(PageType::PendingFree) {
            ctx.errors.push(IntegrityError::InvalidPageType {
                page: current,
                expected: "PendingFree",
            });
            break;
        }

        let count = u32::from_le_bytes(
            page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4]
                .try_into()
                .expect("pending-free entry count is four bytes"),
        );
        if count as usize > PENDING_FREE_ENTRY_CAPACITY {
            ctx.errors
                .push(IntegrityError::PendingFreeEntryCountOutOfBounds {
                    page: current,
                    count,
                    max: PENDING_FREE_ENTRY_CAPACITY,
                });
            break;
        }

        let data_start = PAGE_HEADER_SIZE + 4;
        for index in 0..count as usize {
            ctx.check_cancel()?;
            let offset = data_start + index * citadel_core::PENDING_FREE_ENTRY_SIZE;
            let entry = PageId(u32::from_le_bytes(
                page.data[offset..offset + 4]
                    .try_into()
                    .expect("validated pending-free entry fits in the page"),
            ));
            let freed_at = TxnId(u64::from_le_bytes(
                page.data[offset + 4..offset + 12]
                    .try_into()
                    .expect("validated pending-free entry fits in the page"),
            ));

            if !entry.is_valid() || entry.as_u32() >= ctx.high_water_mark {
                ctx.errors
                    .push(IntegrityError::PendingFreeEntryOutOfBounds {
                        chain_page: current,
                        index,
                        entry,
                        high_water_mark: ctx.high_water_mark,
                    });
            }
            if freed_at == TxnId::ZERO || freed_at > ctx.slot_txn {
                ctx.errors
                    .push(IntegrityError::PendingFreeTransactionOutOfBounds {
                        chain_page: current,
                        index,
                        freed_at,
                        slot_txn: ctx.slot_txn,
                    });
            }
            if let Some(&first_chain_page) = entries.get(&entry) {
                ctx.errors.push(IntegrityError::DuplicatePendingFreeEntry {
                    page: entry,
                    first_chain_page,
                    duplicate_chain_page: current,
                });
            } else {
                entries.insert(entry, current);
            }
        }

        current = page.right_child();
    }

    // Entries name unallocated pages; unlike the chain pages themselves they
    // must not be reachable from any root in this same commit. A different
    // commit slot may still reach them while an older MVCC snapshot survives.
    for page in entries.keys().copied() {
        if visited.contains(&page) {
            ctx.errors
                .push(IntegrityError::PendingFreeEntryStillReachable { page });
        }
    }
    ctx.check_cancel()
}

#[cfg(test)]
#[path = "integrity_tests.rs"]
mod tests;
