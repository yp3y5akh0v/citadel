//! Crash-safe per-atom key store with random-access slot I/O and an O(1) free-list
//! allocator, for per-atom cryptographic erasure.
//!
//! Same double-buffered codec and overwrite-in-place + fsync + read-back tombstone as the
//! region store ([`crate::key_codec`]), scaled to atom cardinality: slots are read by seek,
//! and tombstoned/empty slots are reused via an in-memory free list. Destroying an atom's
//! wrapped ACK forgets that atom; destroying the region RCK forgets every ACK at once.
//!
//! "Erase" is cryptographic (destroy the sole wrapped copy of a random key), not physical
//! NAND destruction: SSD FTL remapping may keep stale copies.

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use citadel_core::{
    Error, Result, ATOM_STORE_MAGIC, ATOM_STORE_PREALLOC_SLOTS, ATOM_STORE_VERSION, KEY_SIZE,
    WRAPPED_KEY_SIZE,
};
use citadel_io::durable::{
    append_and_sync, overwrite_in_place, truncate_and_sync, write_and_sync, write_blocks_synced,
};
use rustc_hash::{FxHashMap, FxHashSet};
use zeroize::Zeroizing;

use crate::key_codec::{
    self, build_slot_block, empty_slot_block, parse_slot_block, slot_offset, SlotRecord, SlotState,
    BLOCK,
};

/// Slots appended per growth step once the free list and pre-allocated run are exhausted.
const GROW_SLOTS: u32 = ATOM_STORE_PREALLOC_SLOTS;
const MAX_BINDING_GENERATION: u64 = i64::MAX as u64;

fn generation_is_reusable(max_gen: u64) -> bool {
    max_gen < MAX_BINDING_GENERATION
}

#[cfg(test)]
std::thread_local! {
    /// Fault-inject failure after durable LIVE bytes, before the slot returns.
    static FAIL_LIVE_READBACK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_BATCH_READBACK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(any(test, feature = "test-util"))]
std::thread_local! {
    /// Die between the two durable updates - the window the normalization sweep heals.
    static FAIL_BATCH_BEFORE_SIBLING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Arm [`FAIL_BATCH_BEFORE_SIBLING`] for this thread's next batch erase.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn fail_next_batch_before_sibling() {
    FAIL_BATCH_BEFORE_SIBLING.with(|f| f.set(true));
}

fn build_header(mac_key: &[u8; KEY_SIZE], file_id: u64, slot_count: u32, gen: u64) -> [u8; BLOCK] {
    key_codec::build_header_block(
        mac_key,
        ATOM_STORE_MAGIC,
        ATOM_STORE_VERSION,
        file_id,
        slot_count,
        gen,
    )
}

fn parse_header(mac_key: &[u8; KEY_SIZE], file_id: u64, b: &[u8]) -> Option<(u32, u64)> {
    key_codec::parse_header_block(mac_key, ATOM_STORE_MAGIC, ATOM_STORE_VERSION, file_id, b)
}

/// Random-access per-atom key store. Holds the store MAC key (zeroized on drop); the engine
/// owns ACK generation and AES-KW wrap/unwrap.
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
        Self::open(path, file_id, mac_key, true)
    }

    /// Open an existing store without manufacturing one for a read-only caller.
    pub(crate) fn open_existing(
        path: &Path,
        file_id: u64,
        mac_key: [u8; KEY_SIZE],
    ) -> Result<Self> {
        Self::open(path, file_id, mac_key, false)
    }

    fn open(
        path: &Path,
        file_id: u64,
        mac_key: [u8; KEY_SIZE],
        create_missing: bool,
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
            // A torn erase can leave the sibling copy holding the key; scrub before reuse.
            let mut stale: Vec<(u32, u64, u64)> = Vec::new();
            for i in 0..slot_count {
                let view = view_from(&mac_key, &bytes, i)?;
                if view.record.state != SlotState::Live && generation_is_reusable(view.max_gen) {
                    free.push(i);
                }
                if view.record.state == SlotState::Tombstone {
                    let off = slot_offset(i, !view.authoritative_b);
                    let o = off as usize;
                    let sib_clean = parse_slot_block(&mac_key, &bytes[o..o + BLOCK])
                        .is_some_and(|r| r.state == SlotState::Tombstone);
                    if !sib_clean {
                        stale.push((i, view.max_gen, off));
                    }
                }
            }
            if !stale.is_empty() {
                let writes: Vec<(u64, [u8; BLOCK])> = stale
                    .iter()
                    .map(|&(_, gen, off)| {
                        (
                            off,
                            build_slot_block(
                                &mac_key,
                                SlotState::Tombstone,
                                0,
                                gen,
                                &[0u8; WRAPPED_KEY_SIZE],
                            ),
                        )
                    })
                    .collect();
                write_blocks_synced(path, &writes)?;
                let confirm = std::fs::read(path)?;
                for &(slot, _, off) in &stale {
                    let o = off as usize;
                    match parse_slot_block(&mac_key, &confirm[o..o + BLOCK]) {
                        Some(r) if r.state == SlotState::Tombstone => {}
                        _ => {
                            return Err(Error::RegionStoreCorrupt(format!(
                                "open scrub of atom slot {slot} did not persist"
                            )))
                        }
                    }
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
        } else if create_missing {
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
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("atom key store '{}' is missing", path.display()),
            )
            .into())
        }
    }

    fn read_block_at(&self, offset: u64) -> Result<[u8; BLOCK]> {
        let mut f = OpenOptions::new().read(true).open(&self.path)?;
        Self::read_block_from(&mut f, offset)
    }

    fn read_block_from(file: &mut std::fs::File, offset: u64) -> Result<[u8; BLOCK]> {
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = [0u8; BLOCK];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn existing_slot_count(
        file: &mut std::fs::File,
        file_id: u64,
        mac_key: &[u8; KEY_SIZE],
    ) -> Result<u32> {
        let a = Self::read_block_from(file, 0)?;
        let b = Self::read_block_from(file, BLOCK as u64)?;
        let declared = match (
            parse_header(mac_key, file_id, &a),
            parse_header(mac_key, file_id, &b),
        ) {
            (Some((sa, ga)), Some((sb, gb))) => {
                if ga >= gb {
                    sa
                } else {
                    sb
                }
            }
            (Some((slots, _)), None) | (None, Some((slots, _))) => slots,
            (None, None) => {
                return Err(Error::RegionStoreCorrupt(
                    "no valid atom-store header copy (wrong key or corrupt store)".into(),
                ))
            }
        };
        let len = file.metadata()?.len() as usize;
        let on_disk = len
            .saturating_sub(2 * BLOCK)
            .checked_div(2 * BLOCK)
            .unwrap_or(0) as u32;
        Ok(declared.min(on_disk))
    }

    /// Read selected records without installing an allocator or scanning unrelated slots.
    pub(crate) fn read_existing_slots(
        path: &Path,
        file_id: u64,
        mac_key: [u8; KEY_SIZE],
        slots: &[u32],
    ) -> Result<Vec<SlotRecord>> {
        Self::read_existing_slot_results(path, file_id, mac_key, slots)?
            .into_iter()
            .collect()
    }

    /// Read selected records through one file open while isolating a malformed slot.
    pub(crate) fn read_existing_slot_results(
        path: &Path,
        file_id: u64,
        mac_key: [u8; KEY_SIZE],
        slots: &[u32],
    ) -> Result<Vec<Result<SlotRecord>>> {
        let mac_key = Zeroizing::new(mac_key);
        let mut file = OpenOptions::new().read(true).open(path)?;
        let slot_count = Self::existing_slot_count(&mut file, file_id, &mac_key)?;
        Ok(slots
            .iter()
            .map(|&slot| {
                if slot >= slot_count {
                    return Err(Error::RegionStoreCorrupt(format!(
                        "atom slot {slot} out of bounds"
                    )));
                }
                let a = Self::read_block_from(&mut file, slot_offset(slot, false))?;
                let b = Self::read_block_from(&mut file, slot_offset(slot, true))?;
                pick_view(&mac_key, slot, &a, &b).map(|view| view.record)
            })
            .collect())
    }

    /// Compare selected exact bindings with one file open and no wrapped-key result copies.
    pub(crate) fn read_existing_bindings_live(
        path: &Path,
        file_id: u64,
        mac_key: [u8; KEY_SIZE],
        bindings: &[(u32, u64, u64)],
    ) -> Result<Vec<bool>> {
        let mac_key = Zeroizing::new(mac_key);
        let mut file = OpenOptions::new().read(true).open(path)?;
        let slot_count = Self::existing_slot_count(&mut file, file_id, &mac_key)?;
        bindings
            .iter()
            .map(|&(slot, owner, generation)| {
                if slot >= slot_count {
                    return Err(Error::RegionStoreCorrupt(format!(
                        "atom slot {slot} out of bounds"
                    )));
                }
                let a = Self::read_block_from(&mut file, slot_offset(slot, false))?;
                let b = Self::read_block_from(&mut file, slot_offset(slot, true))?;
                let record = pick_view(&mac_key, slot, &a, &b)?.record;
                Ok(record.state == SlotState::Live
                    && record.region_id == owner
                    && record.gen == generation)
            })
            .collect()
    }

    fn read_existing_image(
        path: &Path,
        file_id: u64,
        mac_key: [u8; KEY_SIZE],
    ) -> Result<(Zeroizing<[u8; KEY_SIZE]>, Vec<u8>, u32)> {
        let mac_key = Zeroizing::new(mac_key);
        let bytes = std::fs::read(path)?;
        if bytes.len() < 2 * BLOCK {
            return Err(Error::RegionStoreCorrupt(
                "atom store smaller than header".into(),
            ));
        }
        let declared = match (
            parse_header(&mac_key, file_id, &bytes[..BLOCK]),
            parse_header(&mac_key, file_id, &bytes[BLOCK..2 * BLOCK]),
        ) {
            (Some((sa, ga)), Some((sb, gb))) => {
                if ga >= gb {
                    sa
                } else {
                    sb
                }
            }
            (Some((slots, _)), None) | (None, Some((slots, _))) => slots,
            (None, None) => {
                return Err(Error::RegionStoreCorrupt(
                    "no valid atom-store header copy (wrong key or corrupt store)".into(),
                ))
            }
        };
        let on_disk = ((bytes.len() - 2 * BLOCK) / (2 * BLOCK)) as u32;
        Ok((mac_key, bytes, declared.min(on_disk)))
    }

    /// Read the complete live inventory in one file read without building a free list.
    pub(crate) fn read_existing_live_bindings(
        path: &Path,
        file_id: u64,
        mac_key: [u8; KEY_SIZE],
    ) -> Result<Vec<(u32, u64, u64)>> {
        let (mac_key, bytes, slot_count) = Self::read_existing_image(path, file_id, mac_key)?;
        let mut live = Vec::new();
        for slot in 0..slot_count {
            let record = view_from(&mac_key, &bytes, slot)?.record;
            if record.state == SlotState::Live {
                live.push((slot, record.region_id, record.gen));
            }
        }
        Ok(live)
    }

    /// Read all live wrapped keys in one file read without building a free list.
    pub(crate) fn read_existing_live_wrapped(
        path: &Path,
        file_id: u64,
        mac_key: [u8; KEY_SIZE],
    ) -> Result<FxHashMap<u64, [u8; WRAPPED_KEY_SIZE]>> {
        let (mac_key, bytes, slot_count) = Self::read_existing_image(path, file_id, mac_key)?;
        let mut live = FxHashMap::default();
        for slot in 0..slot_count {
            let record = view_from(&mac_key, &bytes, slot)?.record;
            if record.state == SlotState::Live {
                live.insert(record.region_id, record.wrapped);
            }
        }
        Ok(live)
    }

    fn view_from_file(&self, file: &mut std::fs::File, i: u32) -> Result<SlotView> {
        if i >= self.slot_count {
            return Err(Error::RegionStoreCorrupt(format!(
                "atom slot {i} out of bounds"
            )));
        }
        let ba = Self::read_block_from(file, slot_offset(i, false))?;
        let bb = Self::read_block_from(file, slot_offset(i, true))?;
        pick_view(&self.mac_key, i, &ba, &bb)
    }

    /// Authoritative views for selected slots with one file open.
    pub(crate) fn read_slots(&self, slots: &[u32]) -> Result<Vec<SlotRecord>> {
        self.read_slot_results(slots)?.into_iter().collect()
    }

    /// Read selected records with one file open while retaining per-slot errors.
    pub(crate) fn read_slot_results(&self, slots: &[u32]) -> Result<Vec<Result<SlotRecord>>> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        Ok(slots
            .iter()
            .map(|&slot| self.view_from_file(&mut file, slot).map(|view| view.record))
            .collect())
    }

    /// Compare exact bindings with one file open and no wrapped-key result copies.
    pub(crate) fn bindings_live(&self, bindings: &[(u32, u64, u64)]) -> Result<Vec<bool>> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        bindings
            .iter()
            .map(|&(slot, owner, generation)| {
                let record = self.view_from_file(&mut file, slot)?.record;
                Ok(record.state == SlotState::Live
                    && record.region_id == owner
                    && record.gen == generation)
            })
            .collect()
    }

    /// Authoritative view of slot `i` via two single-block reads (no whole-file read).
    fn view(&self, i: u32) -> Result<SlotView> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        self.view_from_file(&mut file, i)
    }

    #[cfg(test)]
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

    /// `(slot, atom_id, gen)` for every LIVE atom key slot (one whole-file pass).
    pub(crate) fn live_bindings(&self) -> Result<Vec<(u32, u64, u64)>> {
        let bytes = std::fs::read(&self.path)?;
        let mut live = Vec::new();
        for i in 0..self.slot_count {
            let rec = view_from(&self.mac_key, &bytes, i)?.record;
            if rec.state == SlotState::Live {
                live.push((i, rec.region_id, rec.gen));
            }
        }
        Ok(live)
    }

    /// The create_or_open torn-erase heal for handles that never reopen from disk.
    pub(crate) fn normalize_torn_tombstones(&mut self) -> Result<usize> {
        let bytes = std::fs::read(&self.path)?;
        let free: FxHashSet<u32> = self.free.iter().copied().collect();
        let mut repaired = 0;
        for i in 0..self.slot_count {
            let view = view_from(&self.mac_key, &bytes, i)?;
            if view.record.state != SlotState::Tombstone {
                continue;
            }
            let off = slot_offset(i, !view.authoritative_b) as usize;
            let sib_clean = parse_slot_block(&self.mac_key, &bytes[off..off + BLOCK])
                .is_some_and(|r| r.state == SlotState::Tombstone);
            let reusable = generation_is_reusable(view.max_gen);
            let listed_free = free.contains(&i);
            if sib_clean && reusable == listed_free {
                continue;
            }
            if !sib_clean {
                self.scrub_stale_sibling(i, &view)?;
            }
            if reusable && !listed_free {
                self.free.push(i);
            } else if !reusable && listed_free {
                self.free.retain(|&slot| slot != i);
            }
            repaired += 1;
        }
        Ok(repaired)
    }

    /// Push `slot` exactly once: a retried erase must never hand it to two allocations.
    fn restore_free(&mut self, slot: u32, max_gen: u64) {
        if generation_is_reusable(max_gen) && !self.free.contains(&slot) {
            self.free.push(slot);
        }
    }

    /// Both raw copies of `slot` (A then B); `None` per MAC-invalid copy.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn slot_copies(&self, slot: u32) -> Result<[Option<SlotRecord>; 2]> {
        if slot >= self.slot_count {
            return Err(Error::RegionStoreCorrupt(format!(
                "atom slot {slot} out of bounds"
            )));
        }
        let a = parse_slot_block(
            &self.mac_key,
            &self.read_block_at(slot_offset(slot, false))?,
        );
        let b = parse_slot_block(&self.mac_key, &self.read_block_at(slot_offset(slot, true))?);
        Ok([a, b])
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
            match self.allocate_slot() {
                Ok(slot) => out.push(slot),
                Err(source) => {
                    // Restore every reservation already popped if a later grow fails.
                    self.free.extend(out.into_iter().rev());
                    return Err(source);
                }
            }
        }
        Ok(out)
    }

    /// Allocates and binds one atom key; no unguarded post-write error window.
    pub(crate) fn allocate_write(
        &mut self,
        atom_id: u64,
        wrapped: &[u8; WRAPPED_KEY_SIZE],
    ) -> Result<(u32, u64)> {
        let slot = self.allocate_slot()?;
        match self.write_live(slot, atom_id, wrapped) {
            Ok(generation) => Ok((slot, generation)),
            Err(source) => match self.abort_reserved_slot(slot, atom_id) {
                Ok(()) => Err(source),
                Err(cleanup) => Err(Error::RegionStoreCorrupt(format!(
                    "{source}; additionally failed to clean atom slot {slot}: {cleanup}"
                ))),
            },
        }
    }

    /// Batch allocate and bind; any single-fsync write/read-back failure reclaims all.
    pub(crate) fn allocate_write_batch(
        &mut self,
        items: &[(u64, [u8; WRAPPED_KEY_SIZE])],
    ) -> Result<Vec<(u32, u64)>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let slots = self.allocate_batch(items.len())?;
        let writes: Vec<(u32, u64, [u8; WRAPPED_KEY_SIZE])> = slots
            .iter()
            .zip(items)
            .map(|(&slot, (atom_id, wrapped))| (slot, *atom_id, *wrapped))
            .collect();
        match self.write_live_batch(&writes) {
            Ok(generations) => Ok(slots.into_iter().zip(generations).collect()),
            Err(source) => {
                let mut cleanup_errors = Vec::new();
                for (&slot, (atom_id, _)) in slots.iter().zip(items) {
                    if let Err(error) = self.abort_reserved_slot(slot, *atom_id) {
                        cleanup_errors.push(format!("slot {slot}: {error}"));
                    }
                }
                if cleanup_errors.is_empty() {
                    Err(source)
                } else {
                    Err(Error::RegionStoreCorrupt(format!(
                        "{source}; additionally failed to clean atom allocations: {}",
                        cleanup_errors.join("; ")
                    )))
                }
            }
        }
    }

    /// Reclaims a reserved slot; EMPTY is valid under the store lock; scrubs key residue.
    fn abort_reserved_slot(&mut self, slot: u32, expected_atom_id: u64) -> Result<()> {
        let view = self.view(slot)?;
        match view.record.state {
            SlotState::Live => {
                if view.record.region_id != expected_atom_id {
                    return Err(Error::RegionStoreCorrupt(format!(
                        "reserved atom slot {slot} changed owner from {expected_atom_id} to {}",
                        view.record.region_id
                    )));
                }
                self.tombstone(slot, expected_atom_id, view.record.gen)?;
            }
            SlotState::Tombstone => {
                self.scrub_stale_sibling(slot, &view)?;
                self.restore_free(slot, view.max_gen);
                return Ok(());
            }
            SlotState::Empty => {
                let generation = view.max_gen.checked_add(1).ok_or_else(|| {
                    Error::RegionStoreCorrupt(format!(
                        "atom slot {slot} generation overflow during reservation cleanup"
                    ))
                })?;
                let tombstone = build_slot_block(
                    &self.mac_key,
                    SlotState::Tombstone,
                    0,
                    generation,
                    &[0u8; WRAPPED_KEY_SIZE],
                );
                for copy_b in [view.authoritative_b, !view.authoritative_b] {
                    overwrite_in_place(&self.path, slot_offset(slot, copy_b), &tombstone)?;
                }
                for copy_b in [false, true] {
                    match parse_slot_block(
                        &self.mac_key,
                        &self.read_block_at(slot_offset(slot, copy_b))?,
                    ) {
                        Some(record)
                            if record.state == SlotState::Tombstone && record.gen == generation => {
                        }
                        _ => {
                            return Err(Error::RegionStoreCorrupt(format!(
                                "cleanup of reserved atom slot {slot} did not persist"
                            )))
                        }
                    }
                }
                self.restore_free(slot, generation);
                return Ok(());
            }
        }
        Ok(())
    }

    fn grow(&mut self) -> Result<()> {
        let new_count = self
            .slot_count
            .checked_add(GROW_SLOTS)
            .ok_or_else(|| Error::RegionStoreCorrupt("atom-store slot count overflow".into()))?;
        let gen = self.header_gen()?.checked_add(1).ok_or_else(|| {
            Error::RegionStoreCorrupt("atom-store header generation overflow".into())
        })?;
        let empty = empty_slot_block(&self.mac_key);
        let mut tail = Vec::with_capacity(GROW_SLOTS as usize * 2 * BLOCK);
        for _ in 0..GROW_SLOTS {
            tail.extend_from_slice(&empty);
            tail.extend_from_slice(&empty);
        }
        append_and_sync(&self.path, &tail)?;

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
        let new_gen = view.max_gen.checked_add(1).ok_or_else(|| {
            Error::RegionStoreCorrupt(format!("atom slot {slot} generation overflow"))
        })?;
        if new_gen > MAX_BINDING_GENERATION {
            return Err(Error::RegionStoreCorrupt(format!(
                "atom slot {slot} generation exceeds the database binding range"
            )));
        }
        let block = build_slot_block(&self.mac_key, SlotState::Live, atom_id, new_gen, wrapped);
        let target_b = !view.authoritative_b;
        let off = slot_offset(slot, target_b);
        overwrite_in_place(&self.path, off, &block)?;
        #[cfg(test)]
        if FAIL_LIVE_READBACK.with(std::cell::Cell::take) {
            return Err(Error::RegionStoreCorrupt(
                "injected atom write_live read-back failure".into(),
            ));
        }
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
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        let mut gens = Vec::with_capacity(items.len());
        let mut writes: Vec<(u64, [u8; BLOCK])> = Vec::with_capacity(items.len());
        let mut marker: Option<(u64, u64)> = None;
        for &(slot, atom_id, wrapped) in items {
            let view = self.view_from_file(&mut file, slot)?;
            let new_gen = view.max_gen.checked_add(1).ok_or_else(|| {
                Error::RegionStoreCorrupt(format!("atom slot {slot} generation overflow"))
            })?;
            if new_gen > MAX_BINDING_GENERATION {
                return Err(Error::RegionStoreCorrupt(format!(
                    "atom slot {slot} generation exceeds the database binding range"
                )));
            }
            let block =
                build_slot_block(&self.mac_key, SlotState::Live, atom_id, new_gen, &wrapped);
            let off = slot_offset(slot, !view.authoritative_b);
            writes.push((off, block));
            marker = Some((off, new_gen));
            gens.push(new_gen);
        }
        write_blocks_synced(&self.path, &writes)?;
        #[cfg(test)]
        if FAIL_BATCH_READBACK.with(std::cell::Cell::take) {
            return Err(Error::RegionStoreCorrupt(
                "injected atom write_live_batch read-back failure".into(),
            ));
        }
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

    /// Scrubs the sibling to TOMBSTONE and confirms; a torn erase leaves it keyed.
    fn scrub_stale_sibling(&self, slot: u32, view: &SlotView) -> Result<()> {
        let off = slot_offset(slot, !view.authoritative_b);
        if let Some(r) = parse_slot_block(&self.mac_key, &self.read_block_at(off)?) {
            if r.state == SlotState::Tombstone {
                return Ok(());
            }
        }
        let tomb = build_slot_block(
            &self.mac_key,
            SlotState::Tombstone,
            0,
            view.max_gen,
            &[0u8; WRAPPED_KEY_SIZE],
        );
        overwrite_in_place(&self.path, off, &tomb)?;
        match parse_slot_block(&self.mac_key, &self.read_block_at(off)?) {
            Some(r) if r.state == SlotState::Tombstone => Ok(()),
            _ => Err(Error::RegionStoreCorrupt(format!(
                "sibling scrub of atom slot {slot} did not persist"
            ))),
        }
    }

    /// Cryptographically erase `slot`: overwrite both copies in place with a zeroed
    /// TOMBSTONE (`gen+1`), fsync, and read back the authoritative copy to confirm
    /// before returning. A retry accepts a later tombstone because it proves the
    /// expected key is already gone; a recycled same-owner LIVE slot is never erased.
    pub(crate) fn tombstone(
        &mut self,
        slot: u32,
        expected_atom_id: u64,
        expected_generation: u64,
    ) -> Result<()> {
        let view = self.view(slot)?;
        match view.record.state {
            // A torn erase leaves the sibling keyed and the slot stranded; finish both.
            SlotState::Tombstone => {
                let minimum_tombstone_generation =
                    expected_generation.checked_add(1).ok_or_else(|| {
                        Error::RegionStoreCorrupt(format!(
                            "atom slot {slot} generation cannot advance past {expected_generation}"
                        ))
                    })?;
                if view.record.gen < minimum_tombstone_generation {
                    return Err(Error::RegionStoreCorrupt(format!(
                        "atom slot {slot} is tombstoned at gen {} before {minimum_tombstone_generation}",
                        view.record.gen
                    )));
                }
                self.scrub_stale_sibling(slot, &view)?;
                self.restore_free(slot, view.max_gen);
                return Ok(());
            }
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
        if view.record.gen != expected_generation {
            return Err(Error::RegionStoreCorrupt(format!(
                "atom slot {slot} holds atom {expected_atom_id} at gen {} not {expected_generation}",
                view.record.gen
            )));
        }

        let new_gen = view.max_gen.checked_add(1).ok_or_else(|| {
            Error::RegionStoreCorrupt(format!("atom slot {slot} generation overflow"))
        })?;
        let tomb = build_slot_block(
            &self.mac_key,
            SlotState::Tombstone,
            0,
            new_gen,
            &[0u8; WRAPPED_KEY_SIZE],
        );
        // The live copy first: overwriting it is the erase commit point.
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
        overwrite_in_place(&self.path, slot_offset(slot, !live_copy_b), &tomb)?;
        self.restore_free(slot, new_gen);
        Ok(())
    }

    /// Batch erase, two fsyncs; recycled skips, wrong gen fails loud, receipt per slot.
    pub(crate) fn tombstone_batch(
        &mut self,
        items: &[(u32, u64, u64)],
    ) -> Result<Vec<(u32, u64, u64, u64)>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let mut unique_slots = FxHashSet::default();
        for &(slot, ..) in items {
            if !unique_slots.insert(slot) {
                return Err(Error::RegionStoreCorrupt(format!(
                    "duplicate atom slot {slot} in tombstone batch"
                )));
            }
        }
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        let tomb_block = |mac_key: &[u8; KEY_SIZE], gen: u64| {
            build_slot_block(
                mac_key,
                SlotState::Tombstone,
                0,
                gen,
                &[0u8; WRAPPED_KEY_SIZE],
            )
        };
        let mut live_writes: Vec<(u64, [u8; BLOCK])> = Vec::with_capacity(items.len());
        let mut sibling_writes: Vec<(u64, [u8; BLOCK])> = Vec::with_capacity(items.len());
        let mut confirmed: Vec<(u32, u64, u64, u64)> = Vec::with_capacity(items.len());
        // The receipt claims per-slot confirmation, so every slot is read back.
        let mut readbacks: Vec<(u64, u64)> = Vec::with_capacity(items.len());
        for &(slot, atom_id, expected_gen) in items {
            let view = self.view_from_file(&mut file, slot)?;
            match view.record.state {
                // A torn erase leaves the sibling keyed and the slot stranded; finish both.
                SlotState::Tombstone => {
                    let minimum_tombstone_generation =
                        expected_gen.checked_add(1).ok_or_else(|| {
                            Error::RegionStoreCorrupt(format!(
                                "atom slot {slot} generation cannot advance past {expected_gen}"
                            ))
                        })?;
                    if view.record.gen < minimum_tombstone_generation {
                        return Err(Error::RegionStoreCorrupt(format!(
                            "atom slot {slot} is tombstoned at gen {} before {minimum_tombstone_generation}",
                            view.record.gen
                        )));
                    }
                    self.scrub_stale_sibling(slot, &view)?;
                    self.restore_free(slot, view.max_gen);
                    continue;
                }
                SlotState::Empty => {
                    return Err(Error::RegionStoreCorrupt(format!(
                        "forget of atom slot {slot} which holds no live key"
                    )))
                }
                SlotState::Live => {}
            }
            if view.record.region_id != atom_id {
                // Recycled: the old key is already gone; skip, never wedge.
                continue;
            }
            if view.record.gen != expected_gen {
                return Err(Error::RegionStoreCorrupt(format!(
                    "atom slot {slot} holds atom {atom_id} at gen {} not {expected_gen}",
                    view.record.gen
                )));
            }
            let new_gen = view.max_gen.checked_add(1).ok_or_else(|| {
                Error::RegionStoreCorrupt(format!("atom slot {slot} generation overflow"))
            })?;
            let tomb = tomb_block(&self.mac_key, new_gen);
            let live_off = slot_offset(slot, view.authoritative_b);
            live_writes.push((live_off, tomb));
            sibling_writes.push((slot_offset(slot, !view.authoritative_b), tomb));
            readbacks.push((live_off, new_gen));
            confirmed.push((slot, atom_id, view.record.gen, new_gen));
        }
        if live_writes.is_empty() {
            return Ok(Vec::new()); // every slot was already tombstoned
        }
        // Overwrite all live copies, one fsync: the batch commit point.
        write_blocks_synced(&self.path, &live_writes)?;
        // Per-slot read-back: the proven-destroyed claim must be literally true.
        for (off, new_gen) in readbacks {
            match parse_slot_block(&self.mac_key, &self.read_block_at(off)?) {
                Some(r) if r.state == SlotState::Tombstone && r.gen == new_gen => {}
                _ => {
                    return Err(Error::RegionStoreCorrupt(format!(
                        "tombstone_batch slot at offset {off} did not persist"
                    )))
                }
            }
        }
        #[cfg(any(test, feature = "test-util"))]
        if FAIL_BATCH_BEFORE_SIBLING.with(std::cell::Cell::take) {
            return Err(Error::RegionStoreCorrupt(
                "injected tombstone_batch failure before the sibling scrub".into(),
            ));
        }
        // Overwrite all sibling copies, one fsync; free the slots.
        write_blocks_synced(&self.path, &sibling_writes)?;
        for &(slot, _, _, tombstone_gen) in &confirmed {
            self.restore_free(slot, tombstone_gen);
        }
        Ok(confirmed)
    }

    pub(crate) fn inspect_counts(
        path: &Path,
        file_id: u64,
        mac_key: &[u8; KEY_SIZE],
    ) -> Result<(u32, u32)> {
        key_codec::inspect_store_counts(
            path,
            mac_key,
            ATOM_STORE_MAGIC,
            ATOM_STORE_VERSION,
            file_id,
            "atom",
        )
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
