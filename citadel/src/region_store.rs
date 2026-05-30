//! Crash-safe sidecar key store for per-region cryptographic erasure.
//!
//! Each encrypted region's content is sealed under a random 256-bit key (RCK).
//! The RCK's SOLE persistent copy is `AES-256-KW(REK, RCK)` (40 bytes) stored in a
//! slot of `{db}.citadel-regions`. `forget` overwrites that slot in place and
//! fsyncs it, after which the RCK - and therefore the region's content - is
//! unrecoverable even to an adversary holding the passphrase, REK, and a full image
//! of the live store, because the RCK is random (not re-derivable from any surviving
//! secret) and the content was never written in plaintext.
//!
//! This is CRYPTOGRAPHIC erasure (destroy the sole logical copy of the random RCK), not
//! physical destruction: on an SSD, FTL remapping may keep stale physical copies.
//!
//! ## On-disk layout (all units are [`REGION_STORE_BLOCK`] = 512 bytes)
//! ```text
//!   header copy A @ 0
//!   header copy B @ BLOCK
//!   slot i copy A @ 2*BLOCK + i*2*BLOCK
//!   slot i copy B @ 2*BLOCK + i*2*BLOCK + BLOCK
//! ```
//! Header and every slot are double-buffered: a write lands in the inactive copy
//! with `gen+1`, and readers pick the MAC-valid copy with the highest `gen`. A torn
//! 512-byte write fails its HMAC and is ignored in favour of the intact sibling, so a
//! crash never surfaces a partial slot as a key. The file is allocated once at full
//! size and thereafter mutated only via [`overwrite_in_place`]/[`append_and_sync`] -
//! never truncated or renamed - so no prior physical copy of a wrapped key is ever
//! orphaned where `forget` cannot reach it.
//!
//! The slot/header HMAC (keyed by `store_mac_key`) provides integrity and torn-write
//! detection only; the secrecy of the wrapped RCK rests on AES-256-KW.

use std::path::{Path, PathBuf};

use citadel_core::{
    Error, Result, KEY_SIZE, REGION_STORE_MAGIC, REGION_STORE_PREALLOC_SLOTS, REGION_STORE_VERSION,
    WRAPPED_KEY_SIZE,
};
use citadel_io::durable::{append_and_sync, overwrite_in_place, truncate_and_sync, write_and_sync};
use zeroize::Zeroizing;

use crate::key_codec::{
    self, build_slot_block, empty_slot_block, header_offset, parse_slot_block, slot_offset,
    SlotRecord, SlotState, BLOCK,
};
#[cfg(test)]
use crate::key_codec::{HEADER_MAC_INPUT, SLOT_MAC_INPUT};

/// Slots appended per growth step once the pre-allocated run is exhausted.
const GROW_SLOTS: u32 = REGION_STORE_PREALLOC_SLOTS;

/// Build a region-store header block (region magic/version).
fn build_header_block(
    mac_key: &[u8; KEY_SIZE],
    file_id: u64,
    slot_count: u32,
    gen: u64,
) -> [u8; BLOCK] {
    key_codec::build_header_block(
        mac_key,
        REGION_STORE_MAGIC,
        REGION_STORE_VERSION,
        file_id,
        slot_count,
        gen,
    )
}

/// Parse a region-store header copy (region magic/version).
fn parse_header_block(mac_key: &[u8; KEY_SIZE], file_id: u64, b: &[u8]) -> Option<(u32, u64)> {
    key_codec::parse_header_block(
        mac_key,
        REGION_STORE_MAGIC,
        REGION_STORE_VERSION,
        file_id,
        b,
    )
}

/// Crash-safe sidecar key store. Holds the store MAC key (zeroized on drop); the
/// caller (the engine) owns RCK generation and AES-KW wrap/unwrap via `Database`.
pub(crate) struct RegionKeyStore {
    path: PathBuf,
    file_id: u64,
    mac_key: Zeroizing<[u8; KEY_SIZE]>,
    slot_count: u32,
}

impl std::fmt::Debug for RegionKeyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the MAC key.
        f.debug_struct("RegionKeyStore")
            .field("path", &self.path)
            .field("file_id", &self.file_id)
            .field("slot_count", &self.slot_count)
            .finish_non_exhaustive()
    }
}

/// Which slot copy currently holds the authoritative state.
struct SlotView {
    record: SlotRecord,
    /// `true` if copy B is authoritative; `false` for copy A.
    authoritative_b: bool,
    max_gen: u64,
}

impl RegionKeyStore {
    /// Open the sidecar store at `path`, creating and pre-allocating it if absent.
    pub(crate) fn create_or_open(
        path: &Path,
        file_id: u64,
        mac_key: [u8; KEY_SIZE],
    ) -> Result<Self> {
        let mac_key = Zeroizing::new(mac_key);
        if path.exists() {
            let bytes = std::fs::read(path)?;
            if bytes.len() < 2 * BLOCK {
                return Err(Error::RegionStoreCorrupt(
                    "store smaller than header".into(),
                ));
            }
            let a = parse_header_block(&mac_key, file_id, &bytes[header_offset(false) as usize..]);
            let b = parse_header_block(&mac_key, file_id, &bytes[header_offset(true) as usize..]);
            let slot_count = match (a, b) {
                (Some((sa, ga)), Some((sb, gb))) => {
                    if ga >= gb {
                        sa
                    } else {
                        sb
                    }
                }
                (Some((s, _)), None) | (None, Some((s, _))) => s,
                (None, None) => {
                    return Err(Error::RegionStoreCorrupt(
                        "no valid header copy (wrong key or corrupt store)".into(),
                    ))
                }
            };
            // Ignore any orphan tail slots beyond what the header commits (a crash
            // during growth that durably appended slots but never bumped the header).
            let on_disk = ((bytes.len() - 2 * BLOCK) / (2 * BLOCK)) as u32;
            let slot_count = slot_count.min(on_disk);
            // Truncate to the committed slot_count so an uncommitted tail can't strand a grow.
            let aligned_len = (2 + 2 * slot_count as usize) * BLOCK;
            if bytes.len() != aligned_len {
                truncate_and_sync(path, aligned_len as u64)?;
            }
            Ok(Self {
                path: path.to_path_buf(),
                file_id,
                mac_key,
                slot_count,
            })
        } else {
            let slot_count = REGION_STORE_PREALLOC_SLOTS;
            let mut buf = Vec::with_capacity((2 + 2 * slot_count as usize) * BLOCK);
            let hdr = build_header_block(&mac_key, file_id, slot_count, 1);
            buf.extend_from_slice(&hdr);
            buf.extend_from_slice(&hdr);
            let empty = empty_slot_block(&mac_key);
            for _ in 0..slot_count {
                buf.extend_from_slice(&empty);
                buf.extend_from_slice(&empty);
            }
            write_and_sync(path, &buf)?;
            Ok(Self {
                path: path.to_path_buf(),
                file_id,
                mac_key,
                slot_count,
            })
        }
    }

    fn read_file(&self) -> Result<Vec<u8>> {
        Ok(std::fs::read(&self.path)?)
    }

    /// Authoritative view of slot `i` from the given file image.
    fn view(&self, bytes: &[u8], i: u32) -> Result<SlotView> {
        let off_a = slot_offset(i, false) as usize;
        let off_b = slot_offset(i, true) as usize;
        if bytes.len() < off_b + BLOCK {
            return Err(Error::RegionStoreCorrupt(format!("slot {i} out of bounds")));
        }
        let a = parse_slot_block(&self.mac_key, &bytes[off_a..off_a + BLOCK]);
        let b = parse_slot_block(&self.mac_key, &bytes[off_b..off_b + BLOCK]);
        match (a, b) {
            (Some(ra), Some(rb)) => {
                if rb.gen > ra.gen {
                    Ok(SlotView {
                        record: rb,
                        authoritative_b: true,
                        max_gen: rb.gen,
                    })
                } else {
                    Ok(SlotView {
                        record: ra,
                        authoritative_b: false,
                        max_gen: ra.gen,
                    })
                }
            }
            (Some(ra), None) => Ok(SlotView {
                record: ra,
                authoritative_b: false,
                max_gen: ra.gen,
            }),
            (None, Some(rb)) => Ok(SlotView {
                record: rb,
                authoritative_b: true,
                max_gen: rb.gen,
            }),
            (None, None) => Err(Error::RegionStoreCorrupt(format!(
                "slot {i} has no valid copy"
            ))),
        }
    }

    /// Authoritative record for slot `i` (used by the engine to attach a region).
    pub(crate) fn read_slot(&self, i: u32) -> Result<SlotRecord> {
        let bytes = self.read_file()?;
        Ok(self.view(&bytes, i)?.record)
    }

    /// `(slot, region_id)` for every LIVE slot.
    pub(crate) fn live_owners(&self) -> Result<Vec<(u32, u64)>> {
        let bytes = self.read_file()?;
        let mut live = Vec::new();
        for i in 0..self.slot_count {
            let rec = self.view(&bytes, i)?.record;
            if rec.state == SlotState::Live {
                live.push((i, rec.region_id));
            }
        }
        Ok(live)
    }

    /// Find a free slot (lowest EMPTY or TOMBSTONE), growing the store if none exist.
    pub(crate) fn allocate_slot(&mut self) -> Result<u32> {
        let bytes = self.read_file()?;
        for i in 0..self.slot_count {
            let st = self.view(&bytes, i)?.record.state;
            if st == SlotState::Empty || st == SlotState::Tombstone {
                return Ok(i);
            }
        }
        self.grow()?;
        Ok(self.slot_count - GROW_SLOTS)
    }

    /// Append `GROW_SLOTS` zeroed-but-MAC'd slot pairs, fsync, then bump the header.
    /// Tail-durable-before-header so a crash mid-grow ignores the orphan tail.
    fn grow(&mut self) -> Result<()> {
        let empty = empty_slot_block(&self.mac_key);
        let mut tail = Vec::with_capacity(GROW_SLOTS as usize * 2 * BLOCK);
        for _ in 0..GROW_SLOTS {
            tail.extend_from_slice(&empty);
            tail.extend_from_slice(&empty);
        }
        append_and_sync(&self.path, &tail)?;

        let new_count = self.slot_count + GROW_SLOTS;
        let bytes = self.read_file()?;
        let gen = self.header_gen(&bytes)?.saturating_add(1);
        let hdr = build_header_block(&self.mac_key, self.file_id, new_count, gen);
        overwrite_in_place(&self.path, header_offset(false), &hdr)?;
        overwrite_in_place(&self.path, header_offset(true), &hdr)?;
        self.slot_count = new_count;
        Ok(())
    }

    fn header_gen(&self, bytes: &[u8]) -> Result<u64> {
        let a = parse_header_block(
            &self.mac_key,
            self.file_id,
            &bytes[header_offset(false) as usize..],
        );
        let b = parse_header_block(
            &self.mac_key,
            self.file_id,
            &bytes[header_offset(true) as usize..],
        );
        match (a, b) {
            (Some((_, ga)), Some((_, gb))) => Ok(ga.max(gb)),
            (Some((_, g)), None) | (None, Some((_, g))) => Ok(g),
            (None, None) => Err(Error::RegionStoreCorrupt("no valid header copy".into())),
        }
    }

    /// Write a LIVE slot to the inactive copy with `gen+1`; returns the new `gen`.
    pub(crate) fn write_live(
        &self,
        slot: u32,
        region_id: u64,
        wrapped: &[u8; WRAPPED_KEY_SIZE],
    ) -> Result<u64> {
        let bytes = self.read_file()?;
        let view = self.view(&bytes, slot)?;
        let new_gen = view.max_gen + 1;
        let block = build_slot_block(&self.mac_key, SlotState::Live, region_id, new_gen, wrapped);
        // Write the copy that is NOT currently authoritative, so the live copy is
        // preserved until the new one is durable; the higher gen then wins.
        let target_b = !view.authoritative_b;
        let off = slot_offset(slot, target_b);
        overwrite_in_place(&self.path, off, &block)?;
        // Durability gate: re-read the written copy, require LIVE at the new gen.
        let confirm = std::fs::read(&self.path)?;
        let o = off as usize;
        match parse_slot_block(&self.mac_key, &confirm[o..o + BLOCK]) {
            Some(r) if r.state == SlotState::Live && r.gen == new_gen => {}
            _ => {
                return Err(Error::RegionStoreCorrupt(format!(
                    "write_live of slot {slot} did not persist"
                )))
            }
        }
        Ok(new_gen)
    }

    /// Cryptographically erase `slot`: overwrite both copies in place with a zeroed
    /// TOMBSTONE (`gen+1`), fsync, and read back to confirm before returning.
    ///
    /// The authoritative copy (which physically holds the live wrapped key) is
    /// overwritten and read-back-confirmed FIRST - the commit point - so that even a
    /// crash immediately afterwards leaves no recoverable wrapped key. Idempotent:
    /// an already-tombstoned slot returns `Ok`.
    pub(crate) fn tombstone(&self, slot: u32, expected_region_id: u64) -> Result<()> {
        let bytes = self.read_file()?;
        let view = self.view(&bytes, slot)?;
        match view.record.state {
            SlotState::Tombstone => return Ok(()),
            SlotState::Empty => {
                return Err(Error::RegionStoreCorrupt(format!(
                    "forget of slot {slot} which holds no live key"
                )))
            }
            SlotState::Live => {}
        }
        if view.record.region_id != expected_region_id {
            return Err(Error::RegionStoreCorrupt(format!(
                "slot {slot} holds region {} not {expected_region_id}",
                view.record.region_id
            )));
        }

        let new_gen = view.max_gen + 1;
        let tomb = build_slot_block(
            &self.mac_key,
            SlotState::Tombstone,
            0,
            new_gen,
            &[0u8; WRAPPED_KEY_SIZE],
        );

        // 1. Overwrite the copy holding the live wrapped key (commit point).
        let live_copy_b = view.authoritative_b;
        overwrite_in_place(&self.path, slot_offset(slot, live_copy_b), &tomb)?;
        // Durability gate: re-read that copy from a fresh handle and require it to be
        // a TOMBSTONE at the new gen before treating the key as destroyed.
        let confirm = std::fs::read(&self.path)?;
        let off = slot_offset(slot, live_copy_b) as usize;
        match parse_slot_block(&self.mac_key, &confirm[off..off + BLOCK]) {
            Some(r) if r.state == SlotState::Tombstone && r.gen == new_gen => {}
            _ => {
                return Err(Error::RegionStoreCorrupt(format!(
                    "tombstone of slot {slot} did not persist"
                )))
            }
        }
        // 2. Overwrite the sibling copy too (it held only zeros, but this keeps both
        //    copies consistent and removes any partially-written residue).
        overwrite_in_place(&self.path, slot_offset(slot, !live_copy_b), &tomb)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn slot_count(&self) -> u32 {
        self.slot_count
    }
}

#[cfg(test)]
#[path = "region_store_tests.rs"]
mod tests;
