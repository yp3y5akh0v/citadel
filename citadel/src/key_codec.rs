//! Shared crash-safe slot/header byte codec for the per-region and per-atom key stores.
//!
//! Layout (units = [`BLOCK`] = 512 bytes): header copy A @ 0, header copy B @ BLOCK,
//! slot `i` copy A @ `2*BLOCK + i*2*BLOCK`, slot `i` copy B @ `... + BLOCK`. Header and
//! every slot are double-buffered: a write lands in the inactive copy with `gen+1` and
//! readers pick the MAC-valid copy with the highest `gen`. A torn 512-byte write fails
//! its HMAC and is ignored in favour of the intact sibling, so a crash never surfaces a
//! partial slot as a key. The slot/header HMAC (keyed by the store MAC key) provides
//! integrity and torn-write detection only; wrapped-key secrecy rests on AES-256-KW.
//!
//! The two stores differ only in the header `magic`/`version` and their I/O strategy
//! (the region store reads the whole small file; the atom store does random-access
//! single-slot reads), so they share this codec but not their store types.

use std::io::{BufReader, Read};
use std::path::Path;

use citadel_core::{Error, Result, KEY_SIZE, REGION_STORE_BLOCK, WRAPPED_KEY_SIZE};
use citadel_crypto::mac::{hmac_sha256, verify_hmac_sha256};

pub(crate) const BLOCK: usize = REGION_STORE_BLOCK;
/// Header bytes authenticated by its HMAC: magic, version, file_id, slot_count, gen.
pub(crate) const HEADER_MAC_INPUT: usize = 28;
/// Slot bytes authenticated by its HMAC: state, owner_id, gen, wrapped_key.
pub(crate) const SLOT_MAC_INPUT: usize = 60;

/// Lifecycle state of a logical slot.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SlotState {
    Empty = 0,
    Live = 1,
    Tombstone = 2,
}

impl SlotState {
    pub(crate) fn from_u32(v: u32) -> Option<Self> {
        match v {
            0 => Some(SlotState::Empty),
            1 => Some(SlotState::Live),
            2 => Some(SlotState::Tombstone),
            _ => None,
        }
    }
}

/// The authoritative (highest-`gen`, MAC-valid) view of a logical slot. `region_id` is
/// the generic owner id: a region id in the region store, an atom id in the atom store.
#[derive(Clone, Copy, Debug)]
pub struct SlotRecord {
    pub state: SlotState,
    pub region_id: u64,
    pub gen: u64,
    pub wrapped: [u8; WRAPPED_KEY_SIZE],
}

/// Offset of header copy `b` (false = A, true = B).
pub(crate) fn header_offset(copy_b: bool) -> u64 {
    if copy_b {
        BLOCK as u64
    } else {
        0
    }
}

/// Offset of slot `i`'s copy `b` (false = A, true = B).
pub(crate) fn slot_offset(i: u32, copy_b: bool) -> u64 {
    let base = 2 * BLOCK as u64 + i as u64 * 2 * BLOCK as u64;
    base + if copy_b { BLOCK as u64 } else { 0 }
}

pub(crate) fn build_header_block(
    mac_key: &[u8; KEY_SIZE],
    magic: u32,
    version: u32,
    file_id: u64,
    slot_count: u32,
    gen: u64,
) -> [u8; BLOCK] {
    let mut b = [0u8; BLOCK];
    b[0..4].copy_from_slice(&magic.to_le_bytes());
    b[4..8].copy_from_slice(&version.to_le_bytes());
    b[8..16].copy_from_slice(&file_id.to_le_bytes());
    b[16..20].copy_from_slice(&slot_count.to_le_bytes());
    b[20..28].copy_from_slice(&gen.to_le_bytes());
    let mac = hmac_sha256(mac_key, &b[0..HEADER_MAC_INPUT]);
    b[HEADER_MAC_INPUT..HEADER_MAC_INPUT + 32].copy_from_slice(&mac);
    b
}

/// Parse a header copy, returning `(slot_count, gen)` if magic/version/file_id match and
/// the HMAC verifies; `None` for an absent, mismatched, or torn copy.
pub(crate) fn parse_header_block(
    mac_key: &[u8; KEY_SIZE],
    magic: u32,
    version: u32,
    file_id: u64,
    b: &[u8],
) -> Option<(u32, u64)> {
    if b.len() < HEADER_MAC_INPUT + 32 {
        return None;
    }
    if u32::from_le_bytes(b[0..4].try_into().ok()?) != magic {
        return None;
    }
    if u32::from_le_bytes(b[4..8].try_into().ok()?) != version {
        return None;
    }
    let tag: [u8; 32] = b[HEADER_MAC_INPUT..HEADER_MAC_INPUT + 32].try_into().ok()?;
    if !verify_hmac_sha256(mac_key, &b[0..HEADER_MAC_INPUT], &tag) {
        return None;
    }
    if u64::from_le_bytes(b[8..16].try_into().ok()?) != file_id {
        return None;
    }
    let slot_count = u32::from_le_bytes(b[16..20].try_into().ok()?);
    let gen = u64::from_le_bytes(b[20..28].try_into().ok()?);
    Some((slot_count, gen))
}

pub(crate) fn build_slot_block(
    mac_key: &[u8; KEY_SIZE],
    state: SlotState,
    region_id: u64,
    gen: u64,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
) -> [u8; BLOCK] {
    let mut b = [0u8; BLOCK];
    b[0..4].copy_from_slice(&(state as u32).to_le_bytes());
    b[4..12].copy_from_slice(&region_id.to_le_bytes());
    b[12..20].copy_from_slice(&gen.to_le_bytes());
    b[20..20 + WRAPPED_KEY_SIZE].copy_from_slice(wrapped);
    let mac = hmac_sha256(mac_key, &b[0..SLOT_MAC_INPUT]);
    b[SLOT_MAC_INPUT..SLOT_MAC_INPUT + 32].copy_from_slice(&mac);
    b
}

/// The empty-slot block (`state=EMPTY`, zeroed key) with a valid HMAC, so pre-allocated
/// and recycled slots are MAC-valid and a torn write reads as invalid, not empty.
pub(crate) fn empty_slot_block(mac_key: &[u8; KEY_SIZE]) -> [u8; BLOCK] {
    build_slot_block(mac_key, SlotState::Empty, 0, 0, &[0u8; WRAPPED_KEY_SIZE])
}

pub(crate) fn parse_slot_block(mac_key: &[u8; KEY_SIZE], b: &[u8]) -> Option<SlotRecord> {
    if b.len() < SLOT_MAC_INPUT + 32 {
        return None;
    }
    let tag: [u8; 32] = b[SLOT_MAC_INPUT..SLOT_MAC_INPUT + 32].try_into().ok()?;
    if !verify_hmac_sha256(mac_key, &b[0..SLOT_MAC_INPUT], &tag) {
        return None;
    }
    let state = SlotState::from_u32(u32::from_le_bytes(b[0..4].try_into().ok()?))?;
    let region_id = u64::from_le_bytes(b[4..12].try_into().ok()?);
    let gen = u64::from_le_bytes(b[12..20].try_into().ok()?);
    let wrapped: [u8; WRAPPED_KEY_SIZE] = b[20..20 + WRAPPED_KEY_SIZE].try_into().ok()?;
    Some(SlotRecord {
        state,
        region_id,
        gen,
        wrapped,
    })
}

/// Read authenticated slot counts without creating, repairing, or caching a
/// key store. The file is streamed through one no-follow handle.
pub(crate) fn inspect_store_counts(
    path: &Path,
    mac_key: &[u8; KEY_SIZE],
    magic: u32,
    version: u32,
    file_id: u64,
    label: &str,
) -> Result<(u32, u32)> {
    let file = citadel_io::durable::open_regular_read(path)?;
    let length = file.metadata()?.len();
    if length < (2 * BLOCK) as u64 {
        return Err(Error::RegionStoreCorrupt(format!(
            "{label} store is smaller than its header"
        )));
    }

    let mut file = BufReader::with_capacity(64 * 1024, file);
    let mut header_a = [0u8; BLOCK];
    let mut header_b = [0u8; BLOCK];
    file.read_exact(&mut header_a)?;
    file.read_exact(&mut header_b)?;
    let a = parse_header_block(mac_key, magic, version, file_id, &header_a);
    let b = parse_header_block(mac_key, magic, version, file_id, &header_b);
    let declared = match (a, b) {
        (Some((count_a, gen_a)), Some((count_b, gen_b))) => {
            if gen_a >= gen_b {
                count_a
            } else {
                count_b
            }
        }
        (Some((count, _)), None) | (None, Some((count, _))) => count,
        (None, None) => {
            return Err(Error::RegionStoreCorrupt(format!(
                "{label} store has no valid header copy"
            )));
        }
    };

    let complete_slots =
        ((length - (2 * BLOCK) as u64) / (2 * BLOCK) as u64).min(u64::from(u32::MAX)) as u32;
    if declared > complete_slots {
        return Err(Error::RegionStoreCorrupt(format!(
            "{label} store header declares {declared} slots, but only {complete_slots} are complete"
        )));
    }
    let total = declared;
    let mut tombstoned = 0u32;
    for slot in 0..total {
        let mut block_a = [0u8; BLOCK];
        let mut block_b = [0u8; BLOCK];
        file.read_exact(&mut block_a)?;
        file.read_exact(&mut block_b)?;
        let a = parse_slot_block(mac_key, &block_a);
        let b = parse_slot_block(mac_key, &block_b);
        let record = match (a, b) {
            (Some(a), Some(b)) => {
                if b.gen > a.gen {
                    b
                } else {
                    a
                }
            }
            (Some(record), None) | (None, Some(record)) => record,
            (None, None) => {
                return Err(Error::RegionStoreCorrupt(format!(
                    "{label} slot {slot} has no valid copy"
                )));
            }
        };
        if record.state == SlotState::Tombstone {
            tombstoned += 1;
        }
    }
    Ok((total, tombstoned))
}
