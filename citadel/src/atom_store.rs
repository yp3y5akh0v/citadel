//! Crash-safe per-atom key store with random-access slot I/O and an O(1) free-list
//! allocator, for per-atom cryptographic erasure.
//!
//! Same double-buffered slot/header codec as the region store (see [`crate::key_codec`])
//! and the same overwrite-in-place + fsync + read-back tombstone, but scaled to atom
//! cardinality: a slot is read by seeking to its offset (never the whole file), and
//! tombstoned/empty slots are reused via an in-memory free list so allocation is O(1)
//! amortized. `forget_atom` destroys one atom's wrapped ACK; destroying the region's RCK
//! makes every ACK (wrapped under a key derived from RCK) unrecoverable at once.
//!
//! "Erase" is CRYPTOGRAPHIC erasure (destroy the sole wrapped copy of a random key), not
//! physical NAND destruction: SSD FTL remapping may keep stale copies.

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use citadel_core::{
    Error, Result, ATOM_STORE_MAGIC, ATOM_STORE_PREALLOC_SLOTS, KEY_SIZE, REGION_STORE_VERSION,
    WRAPPED_KEY_SIZE,
};
use citadel_io::durable::{
    append_and_sync, overwrite_in_place, truncate_and_sync, write_and_sync, write_blocks_synced,
};
use rustc_hash::FxHashMap;
use zeroize::Zeroizing;

use crate::key_codec::{
    self, build_slot_block, empty_slot_block, parse_slot_block, slot_offset, SlotRecord, SlotState,
    BLOCK,
};

/// Atom store version (shares the region store's `1`; the magic distinguishes the files).
const VERSION: u32 = REGION_STORE_VERSION;
/// Slots appended per growth step once the free list and pre-allocated run are exhausted.
const GROW_SLOTS: u32 = ATOM_STORE_PREALLOC_SLOTS;

fn build_header(mac_key: &[u8; KEY_SIZE], file_id: u64, slot_count: u32, gen: u64) -> [u8; BLOCK] {
    key_codec::build_header_block(mac_key, ATOM_STORE_MAGIC, VERSION, file_id, slot_count, gen)
}

fn parse_header(mac_key: &[u8; KEY_SIZE], file_id: u64, b: &[u8]) -> Option<(u32, u64)> {
    key_codec::parse_header_block(mac_key, ATOM_STORE_MAGIC, VERSION, file_id, b)
}

/// Random-access per-atom key store. Holds the store MAC key (zeroized on drop); the
/// engine owns ACK generation and AES-KW wrap/unwrap. Allocation reuses tombstoned slots
/// via the in-memory free list before bumping the high-water mark / growing the file.
pub(crate) struct AtomKeyStore {
    path: PathBuf,
    file_id: u64,
    mac_key: Zeroizing<[u8; KEY_SIZE]>,
    slot_count: u32,
    /// Slots known free (EMPTY or TOMBSTONE), reused before growing; rebuilt on open.
    free: Vec<u32>,
}

impl std::fmt::Debug for AtomKeyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomKeyStore")
            .field("path", &self.path)
            .field("file_id", &self.file_id)
            .field("slot_count", &self.slot_count)
            .field("free", &self.free.len())
            .finish_non_exhaustive()
    }
}

struct SlotView {
    record: SlotRecord,
    authoritative_b: bool,
    max_gen: u64,
}

impl AtomKeyStore {
    /// Open the atom key store at `path`, creating and pre-allocating it if absent.
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
                    "atom store smaller than header".into(),
                ));
            }
            let a = parse_header(&mac_key, file_id, &bytes[0..]);
            let b = parse_header(&mac_key, file_id, &bytes[BLOCK..]);
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
                        "no valid atom-store header copy (wrong key or corrupt store)".into(),
                    ))
                }
            };
            let on_disk = ((bytes.len() - 2 * BLOCK) / (2 * BLOCK)) as u32;
            let slot_count = slot_count.min(on_disk);
            let aligned_len = (2 + 2 * slot_count as usize) * BLOCK;
            if bytes.len() != aligned_len {
                truncate_and_sync(path, aligned_len as u64)?;
            }
            let mut free = Vec::new();
            for i in 0..slot_count {
                if view_from(&mac_key, &bytes, i)?.record.state != SlotState::Live {
                    free.push(i);
                }
            }
            free.reverse();
            Ok(Self {
                path: path.to_path_buf(),
                file_id,
                mac_key,
                slot_count,
                free,
            })
        } else {
            let slot_count = ATOM_STORE_PREALLOC_SLOTS;
            let mut buf = Vec::with_capacity((2 + 2 * slot_count as usize) * BLOCK);
            let hdr = build_header(&mac_key, file_id, slot_count, 1);
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
                free: (0..slot_count).rev().collect(),
            })
        }
    }

    fn read_block_at(&self, offset: u64) -> Result<[u8; BLOCK]> {
        let mut f = OpenOptions::new().read(true).open(&self.path)?;
        f.seek(SeekFrom::Start(offset))?;
        let mut buf = [0u8; BLOCK];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Authoritative view of slot `i` via two single-block reads (no whole-file read).
    fn view(&self, i: u32) -> Result<SlotView> {
        if i >= self.slot_count {
            return Err(Error::RegionStoreCorrupt(format!(
                "atom slot {i} out of bounds"
            )));
        }
        let ba = self.read_block_at(slot_offset(i, false))?;
        let bb = self.read_block_at(slot_offset(i, true))?;
        pick_view(&self.mac_key, i, &ba, &bb)
    }

    /// The authoritative record of slot `slot`.
    pub(crate) fn read_slot(&self, slot: u32) -> Result<SlotRecord> {
        Ok(self.view(slot)?.record)
    }

    /// Every LIVE slot's `atom_id -> wrapped key`, read in a single whole-file pass.
    /// Lets the engine (re)build the in-RAM ANN index without one `open()` per atom.
    pub(crate) fn live_wrapped(&self) -> Result<FxHashMap<u64, [u8; WRAPPED_KEY_SIZE]>> {
        let bytes = std::fs::read(&self.path)?;
        let mut out = FxHashMap::default();
        for i in 0..self.slot_count {
            let rec = view_from(&self.mac_key, &bytes, i)?.record;
            if rec.state == SlotState::Live {
                out.insert(rec.region_id, rec.wrapped);
            }
        }
        Ok(out)
    }

    /// `(slot, atom_id)` for every LIVE atom key slot (one whole-file pass).
    pub(crate) fn live_owners(&self) -> Result<Vec<(u32, u64)>> {
        let bytes = std::fs::read(&self.path)?;
        let mut live = Vec::new();
        for i in 0..self.slot_count {
            let rec = view_from(&self.mac_key, &bytes, i)?.record;
            if rec.state == SlotState::Live {
                live.push((i, rec.region_id));
            }
        }
        Ok(live)
    }

    /// Allocate one free slot, growing the store if the free list is empty.
    pub(crate) fn allocate_slot(&mut self) -> Result<u32> {
        if let Some(s) = self.free.pop() {
            return Ok(s);
        }
        self.grow()?;
        Ok(self.free.pop().expect("grow pushes free slots"))
    }

    /// Allocate `n` free slots (for batch inserts).
    pub(crate) fn allocate_batch(&mut self, n: usize) -> Result<Vec<u32>> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(self.allocate_slot()?);
        }
        Ok(out)
    }

    fn grow(&mut self) -> Result<()> {
        let empty = empty_slot_block(&self.mac_key);
        let mut tail = Vec::with_capacity(GROW_SLOTS as usize * 2 * BLOCK);
        for _ in 0..GROW_SLOTS {
            tail.extend_from_slice(&empty);
            tail.extend_from_slice(&empty);
        }
        append_and_sync(&self.path, &tail)?;

        let new_count = self.slot_count + GROW_SLOTS;
        let gen = self.header_gen()?.saturating_add(1);
        let hdr = build_header(&self.mac_key, self.file_id, new_count, gen);
        overwrite_in_place(&self.path, key_codec::header_offset(false), &hdr)?;
        overwrite_in_place(&self.path, key_codec::header_offset(true), &hdr)?;
        for i in (self.slot_count..new_count).rev() {
            self.free.push(i);
        }
        self.slot_count = new_count;
        Ok(())
    }

    fn header_gen(&self) -> Result<u64> {
        let a = parse_header(
            &self.mac_key,
            self.file_id,
            &self.read_block_at(key_codec::header_offset(false))?,
        );
        let b = parse_header(
            &self.mac_key,
            self.file_id,
            &self.read_block_at(key_codec::header_offset(true))?,
        );
        match (a, b) {
            (Some((_, ga)), Some((_, gb))) => Ok(ga.max(gb)),
            (Some((_, g)), None) | (None, Some((_, g))) => Ok(g),
            (None, None) => Err(Error::RegionStoreCorrupt(
                "no valid atom-store header copy".into(),
            )),
        }
    }

    /// Write a LIVE slot to the inactive copy with `gen+1`; returns the new `gen`.
    pub(crate) fn write_live(
        &self,
        slot: u32,
        atom_id: u64,
        wrapped: &[u8; WRAPPED_KEY_SIZE],
    ) -> Result<u64> {
        let view = self.view(slot)?;
        let new_gen = view.max_gen + 1;
        let block = build_slot_block(&self.mac_key, SlotState::Live, atom_id, new_gen, wrapped);
        let target_b = !view.authoritative_b;
        let off = slot_offset(slot, target_b);
        overwrite_in_place(&self.path, off, &block)?;
        // Re-read to confirm persistence before returning.
        match parse_slot_block(&self.mac_key, &self.read_block_at(off)?) {
            Some(r) if r.state == SlotState::Live && r.gen == new_gen => {}
            _ => {
                return Err(Error::RegionStoreCorrupt(
                    "write_live did not persist".into(),
                ))
            }
        }
        Ok(new_gen)
    }

    /// Write many LIVE slots (each `gen+1`) with ONE fsync for the batch, then read back a
    /// marker to confirm. Returns the new `gen` per slot, in input order.
    pub(crate) fn write_live_batch(
        &self,
        items: &[(u32, u64, [u8; WRAPPED_KEY_SIZE])],
    ) -> Result<Vec<u64>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let image = std::fs::read(&self.path)?;
        let mut gens = Vec::with_capacity(items.len());
        let mut writes: Vec<(u64, [u8; BLOCK])> = Vec::with_capacity(items.len());
        let mut marker: Option<(u64, u64)> = None;
        for &(slot, atom_id, wrapped) in items {
            let view = view_from(&self.mac_key, &image, slot)?;
            let new_gen = view.max_gen + 1;
            let block =
                build_slot_block(&self.mac_key, SlotState::Live, atom_id, new_gen, &wrapped);
            let off = slot_offset(slot, !view.authoritative_b);
            writes.push((off, block));
            marker = Some((off, new_gen));
            gens.push(new_gen);
        }
        write_blocks_synced(&self.path, &writes)?;
        // Marker read-back confirms batch persisted.
        if let Some((off, new_gen)) = marker {
            match parse_slot_block(&self.mac_key, &self.read_block_at(off)?) {
                Some(r) if r.state == SlotState::Live && r.gen == new_gen => {}
                _ => {
                    return Err(Error::RegionStoreCorrupt(
                        "write_live_batch marker slot did not persist".into(),
                    ))
                }
            }
        }
        Ok(gens)
    }

    /// Cryptographically erase `slot`: overwrite both copies in place with a zeroed
    /// TOMBSTONE (`gen+1`), fsync, and read back the authoritative copy to confirm
    /// before returning. Idempotent; frees the slot for reuse on a real transition.
    pub(crate) fn tombstone(&mut self, slot: u32, expected_atom_id: u64) -> Result<()> {
        let view = self.view(slot)?;
        match view.record.state {
            SlotState::Tombstone => return Ok(()), // already erased + already free
            SlotState::Empty => {
                return Err(Error::RegionStoreCorrupt(format!(
                    "forget of atom slot {slot} which holds no live key"
                )))
            }
            SlotState::Live => {}
        }
        if view.record.region_id != expected_atom_id {
            return Err(Error::RegionStoreCorrupt(format!(
                "atom slot {slot} holds atom {} not {expected_atom_id}",
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
        // Durability gate: re-read that copy and require TOMBSTONE at the new gen.
        match parse_slot_block(
            &self.mac_key,
            &self.read_block_at(slot_offset(slot, live_copy_b))?,
        ) {
            Some(r) if r.state == SlotState::Tombstone && r.gen == new_gen => {}
            _ => {
                return Err(Error::RegionStoreCorrupt(format!(
                    "tombstone of atom slot {slot} did not persist"
                )))
            }
        }
        // 2. Overwrite the sibling copy too, then free the slot for reuse.
        overwrite_in_place(&self.path, slot_offset(slot, !live_copy_b), &tomb)?;
        self.free.push(slot);
        Ok(())
    }

    /// Erase many slots with TWO fsyncs for the batch (not 2N): overwrite all live copies
    /// (commit point) + fsync + marker read-back, then all sibling copies + fsync. Skips
    /// already-tombstoned slots; EMPTY or owner mismatch aborts before any write.
    pub(crate) fn tombstone_batch(&mut self, items: &[(u32, u64)]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let image = std::fs::read(&self.path)?;
        let tomb_block = |gen: u64| {
            build_slot_block(
                &self.mac_key,
                SlotState::Tombstone,
                0,
                gen,
                &[0u8; WRAPPED_KEY_SIZE],
            )
        };
        let mut live_writes: Vec<(u64, [u8; BLOCK])> = Vec::with_capacity(items.len());
        let mut sibling_writes: Vec<(u64, [u8; BLOCK])> = Vec::with_capacity(items.len());
        let mut to_free: Vec<u32> = Vec::with_capacity(items.len());
        let mut marker: Option<(u64, u64)> = None;
        for &(slot, atom_id) in items {
            let view = view_from(&self.mac_key, &image, slot)?;
            match view.record.state {
                SlotState::Tombstone => continue, // already erased + already free
                SlotState::Empty => {
                    return Err(Error::RegionStoreCorrupt(format!(
                        "forget of atom slot {slot} which holds no live key"
                    )))
                }
                SlotState::Live => {}
            }
            if view.record.region_id != atom_id {
                return Err(Error::RegionStoreCorrupt(format!(
                    "atom slot {slot} holds atom {} not {atom_id}",
                    view.record.region_id
                )));
            }
            let new_gen = view.max_gen + 1;
            let tomb = tomb_block(new_gen);
            let live_off = slot_offset(slot, view.authoritative_b);
            live_writes.push((live_off, tomb));
            sibling_writes.push((slot_offset(slot, !view.authoritative_b), tomb));
            marker = Some((live_off, new_gen));
            to_free.push(slot);
        }
        if live_writes.is_empty() {
            return Ok(()); // every slot was already tombstoned
        }
        // Overwrite all live copies, one fsync: the batch commit point.
        write_blocks_synced(&self.path, &live_writes)?;
        if let Some((off, new_gen)) = marker {
            match parse_slot_block(&self.mac_key, &self.read_block_at(off)?) {
                Some(r) if r.state == SlotState::Tombstone && r.gen == new_gen => {}
                _ => {
                    return Err(Error::RegionStoreCorrupt(
                        "tombstone_batch marker slot did not persist".into(),
                    ))
                }
            }
        }
        // Overwrite all sibling copies, one fsync; free the slots.
        write_blocks_synced(&self.path, &sibling_writes)?;
        for slot in to_free {
            self.free.push(slot);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn slot_count(&self) -> u32 {
        self.slot_count
    }
}

/// View of slot `i` from a full file image (used on open and batch reads).
fn view_from(mac_key: &[u8; KEY_SIZE], bytes: &[u8], i: u32) -> Result<SlotView> {
    let off_a = slot_offset(i, false) as usize;
    let off_b = slot_offset(i, true) as usize;
    if bytes.len() < off_b + BLOCK {
        return Err(Error::RegionStoreCorrupt(format!(
            "atom slot {i} out of bounds"
        )));
    }
    pick_view(
        mac_key,
        i,
        &bytes[off_a..off_a + BLOCK],
        &bytes[off_b..off_b + BLOCK],
    )
}

/// Pick the authoritative (higher-`gen`, MAC-valid) copy of a slot's two blocks.
fn pick_view(mac_key: &[u8; KEY_SIZE], i: u32, block_a: &[u8], block_b: &[u8]) -> Result<SlotView> {
    match (
        parse_slot_block(mac_key, block_a),
        parse_slot_block(mac_key, block_b),
    ) {
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
            "atom slot {i}: no valid copy"
        ))),
    }
}

#[cfg(test)]
#[path = "atom_store_tests.rs"]
mod tests;
