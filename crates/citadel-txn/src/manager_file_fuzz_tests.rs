//! Damaged and hostile file images. Whatever a mutation does to the header,
//! the commit slots or the pages, open, reads, the integrity walk and a commit
//! end in a result or a typed error, never a panic, and an image that opens,
//! reads and audits clean holds a committed generation.
//!
//! The fixed sweeps flip every bit before the commit slots, mutate every header
//! and slot field with the `xfs_db fuzz` verbs (raw, re-checksummed as a legacy
//! slot, or re-sealed with the MAC key), and flip, replay, misdirect and
//! truncate pages. Seeded stacks of those mutations follow;
//! `CITADEL_FILE_FUZZ_SEEDS=<n>` runs `n` seeds.
use super::*;
use citadel_core::{
    COMMIT_SLOT_OFFSET, COMMIT_SLOT_SIZE, FILE_HEADER_SIZE, FILE_ID_OFFSET, GOD_BYTE_OFFSET,
    HEADER_FLAGS_OFFSET, MAC_SIZE, MERKLE_HASH_SIZE, SLOT_CATALOG_ROOT, SLOT_CHECKSUM, SLOT_DEK_ID,
    SLOT_ENCRYPTION_EPOCH, SLOT_FORMAT_MARKER, SLOT_HIGH_WATER_MARK, SLOT_MAC, SLOT_MAC_SIZE,
    SLOT_MERKLE_ROOT, SLOT_MERKLE_SCHEME, SLOT_NAMED_ENTRIES, SLOT_NAMED_ENTRY_SIZE,
    SLOT_PENDING_FREE_ROOT, SLOT_TOTAL_PAGES, SLOT_TREE_DEPTH, SLOT_TREE_ENTRIES, SLOT_TREE_ROOT,
    SLOT_TXN_ID,
};
use citadel_io::file_manager::SlotFormat;
use std::collections::hash_map::Entry;
use std::panic::{catch_unwind, AssertUnwindSafe};

const DEFAULT_SEEDS: u64 = 4;
const STACKS_PER_SEED: usize = 256;
/// A commit to a mutated image may grow the file this much before the disk
/// refuses, so metadata that claims billions of pages cannot exhaust memory.
const GROWTH_LIMIT: u64 = 64 * 1024 * 1024;
const SENTINEL: &[u8] = b"sentinel";

/// Rows per table; `None` names the default tree.
type Tables = BTreeMap<Option<Vec<u8>>, BTreeMap<Vec<u8>, Vec<u8>>>;

fn key(index: u32) -> Vec<u8> {
    format!("key-{index:03}").into_bytes()
}

/// Commits that leave overflow values, branch pages, a named table, a dropped
/// one and deleted rows, so the file holds every page kind and a free chain.
const COMMITS: [fn(&mut WriteTxn<'_>) -> Result<()>; 6] = [
    |w| {
        for i in 0..10 {
            w.insert(&key(i), &[b'a'; 1_000])?;
        }
        w.insert(b"big", &[b'b'; 3_000]).map(drop)
    },
    |w| {
        w.create_table(b"alpha")?;
        for i in 0..4 {
            w.table_insert(b"alpha", &key(i), &[b'c'; 100])?;
        }
        Ok(())
    },
    |w| {
        w.create_table(b"beta")?;
        w.table_insert(b"beta", b"only", &[b'd'; 2_500])?;
        w.delete(&key(3)).map(drop)
    },
    |w| {
        w.drop_table(b"beta")?;
        w.insert(b"big", &[b'e'; 4_500]).map(drop)
    },
    |w| {
        for i in 0..4 {
            w.table_insert(b"alpha", &key(i), &[b'f'; 150])?;
        }
        Ok(())
    },
    |w| w.delete(&key(5)).map(drop),
];

fn read_tables(mgr: &TxnManager) -> Result<Tables> {
    let mut rtx = mgr.begin_read();
    let mut default = BTreeMap::new();
    rtx.for_each(|key, value| {
        default.insert(key.to_vec(), value.to_vec());
        Ok(())
    })?;
    let mut tables = Tables::from([(None, default)]);
    for (name, _) in rtx.list_tables()? {
        let mut rows = BTreeMap::new();
        rtx.table_for_each(&name, |key, value| {
            rows.insert(key.to_vec(), value.to_vec());
            Ok(())
        })?;
        tables.insert(Some(name), rows);
    }
    Ok(tables)
}

/// The file after each commit and what each commit left readable; the last
/// image is the one every case mutates.
struct Fixture {
    images: Vec<Vec<u8>>,
    generations: Vec<Tables>,
}

impl Fixture {
    fn build() -> Self {
        let (dek, mac_key, dek_id) = test_keys();
        let io = MemIO::new(0);
        let mgr = TxnManager::create_with_sync(
            Box::new(io.share()),
            dek,
            mac_key,
            1,
            0x1234,
            dek_id,
            64,
            SyncMode::Full,
        )
        .unwrap();
        let mut images = vec![io.bytes()];
        let mut generations = vec![read_tables(&mgr).unwrap()];
        for commit in COMMITS {
            let mut wtx = mgr.begin_write().unwrap();
            commit(&mut wtx).unwrap();
            wtx.commit().unwrap();
            images.push(io.bytes());
            generations.push(read_tables(&mgr).unwrap());
        }
        Self {
            images,
            generations,
        }
    }

    fn latest(&self) -> usize {
        self.generations.len() - 1
    }

    fn pages(&self) -> u32 {
        let image = &self.images[self.latest()];
        (0..2)
            .map(|index| CommitSlot::deserialize(slot(image, index).unwrap()).high_water_mark)
            .max()
            .unwrap()
    }

    /// `tables` is an allowed generation, except that a table only the commit
    /// slot records stays unlisted until compaction rebuilds its descriptor.
    fn holds(&self, allowed: Allowed, tables: &Tables, slot: &CommitSlot) -> bool {
        let latest = self.latest();
        let candidates = match allowed {
            Allowed::Latest => latest..=latest,
            Allowed::LatestOrPrevious => latest - 1..=latest,
            Allowed::Any => 0..=latest,
        };
        self.generations[candidates].iter().any(|generation| {
            tables.keys().all(|name| generation.contains_key(name))
                && generation
                    .iter()
                    .all(|(name, rows)| match tables.get(name) {
                        Some(read) => read == rows,
                        None => name
                            .as_deref()
                            .is_some_and(|name| slot.named_entry_root(name).is_some()),
                    })
        })
    }
}

/// Generations an image that opens, reads and audits clean may hold.
#[derive(Clone, Copy, Debug)]
enum Allowed {
    Latest,
    /// The selector or a slot changed, so recovery may fall back one commit.
    LatestOrPrevious,
    Any,
}

#[derive(Clone, Copy)]
struct Field {
    name: &'static str,
    offset: usize,
    len: usize,
}

impl std::fmt::Debug for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.name, self.offset)
    }
}

impl Field {
    const fn new(name: &'static str, offset: usize, len: usize) -> Self {
        Self { name, offset, len }
    }

    fn bytes(self, buf: &mut [u8]) -> &mut [u8] {
        &mut buf[self.offset..self.offset + self.len]
    }
}

const HEADER_FIELDS: [Field; 10] = [
    Field::new("magic", 0, 4),
    Field::new("format_version", 4, 4),
    Field::new("page_size", 8, 4),
    Field::new("body_size", 12, 4),
    Field::new("min_reader_version", 16, 2),
    Field::new("min_writer_version", 18, 2),
    Field::new("god_byte", GOD_BYTE_OFFSET, 1),
    Field::new("flags", HEADER_FLAGS_OFFSET, 1),
    Field::new(
        "padding",
        HEADER_FLAGS_OFFSET + 1,
        FILE_ID_OFFSET - HEADER_FLAGS_OFFSET - 1,
    ),
    Field::new("file_id", FILE_ID_OFFSET, 8),
];

fn slot_fields() -> Vec<Field> {
    let mut fields = vec![
        Field::new("txn_id", SLOT_TXN_ID, 8),
        Field::new("tree_root", SLOT_TREE_ROOT, 4),
        Field::new("tree_depth", SLOT_TREE_DEPTH, 2),
        Field::new("merkle_scheme", SLOT_MERKLE_SCHEME, 2),
        Field::new("tree_entries", SLOT_TREE_ENTRIES, 8),
        Field::new("catalog_root", SLOT_CATALOG_ROOT, 4),
        Field::new("total_pages", SLOT_TOTAL_PAGES, 4),
        Field::new("high_water_mark", SLOT_HIGH_WATER_MARK, 4),
        Field::new("pending_free_root", SLOT_PENDING_FREE_ROOT, 4),
        Field::new("encryption_epoch", SLOT_ENCRYPTION_EPOCH, 4),
        Field::new("dek_id", SLOT_DEK_ID, MAC_SIZE),
        Field::new("checksum", SLOT_CHECKSUM, 8),
        Field::new("merkle_root", SLOT_MERKLE_ROOT, MERKLE_HASH_SIZE),
        Field::new("named_count", SLOT_NAMED_ENTRIES, 2),
        Field::new("format_marker", SLOT_FORMAT_MARKER, 2),
        Field::new("mac", SLOT_MAC, SLOT_MAC_SIZE),
    ];
    for entry in 0..SLOT_NAMED_MAX_ENTRIES_V1 {
        let base = SLOT_NAMED_ENTRIES + 2 + entry * SLOT_NAMED_ENTRY_SIZE;
        fields.extend([
            Field::new("named_hash", base, 4),
            Field::new("named_count", base + 4, 8),
            Field::new("named_root", base + 12, 4),
            Field::new("named_depth", base + 16, 2),
        ]);
    }
    fields
}

/// The mutations of `xfs_db fuzz`; `Random` fills the field from its seed.
#[derive(Clone, Copy, Debug)]
enum Verb {
    Zeroes,
    Ones,
    FirstBit,
    MiddleBit,
    LastBit,
    Add,
    Sub,
    Random(u64),
}

const VERBS: [Verb; 8] = [
    Verb::Zeroes,
    Verb::Ones,
    Verb::FirstBit,
    Verb::MiddleBit,
    Verb::LastBit,
    Verb::Add,
    Verb::Sub,
    Verb::Random(0x5EED),
];

impl Verb {
    fn apply(self, bytes: &mut [u8]) {
        let bits = bytes.len() * 8;
        match self {
            Verb::Zeroes => bytes.fill(0),
            Verb::Ones => bytes.fill(0xFF),
            Verb::FirstBit => flip(bytes, 0),
            Verb::MiddleBit => flip(bytes, bits / 2),
            Verb::LastBit => flip(bytes, bits - 1),
            Verb::Add | Verb::Sub => {
                let width = bytes.len().min(8);
                let mut word = [0u8; 8];
                word[..width].copy_from_slice(&bytes[..width]);
                let value = u64::from_le_bytes(word);
                let value = match self {
                    Verb::Add => value.wrapping_add(1),
                    _ => value.wrapping_sub(1),
                };
                bytes[..width].copy_from_slice(&value.to_le_bytes()[..width]);
            }
            Verb::Random(seed) => {
                let mut rng = SplitMix(seed);
                bytes.fill_with(|| rng.next() as u8);
            }
        }
    }
}

fn flip(bytes: &mut [u8], bit: usize) {
    bytes[bit / 8] ^= 1 << (bit % 8);
}

/// How a mutated slot is re-encoded: left as mutated, given a valid keyless
/// checksum as a legacy slot, or re-sealed as an authentic V1 slot.
#[derive(Clone, Copy, Debug)]
enum Seal {
    Raw,
    Legacy,
    Sealed,
}

const SEALS: [Seal; 3] = [Seal::Raw, Seal::Legacy, Seal::Sealed];

impl Seal {
    fn apply(self, slot: &mut [u8; COMMIT_SLOT_SIZE]) {
        let mut parsed = CommitSlot::deserialize(slot);
        match self {
            Seal::Raw => return,
            Seal::Legacy => parsed.slot_format = SlotFormat::Legacy,
            Seal::Sealed => parsed.seal(&test_keys().1),
        }
        *slot = parsed.serialize();
    }
}

fn slot_range(index: usize) -> std::ops::Range<usize> {
    let start = COMMIT_SLOT_OFFSET + index * COMMIT_SLOT_SIZE;
    start..start + COMMIT_SLOT_SIZE
}

fn slot(image: &[u8], index: usize) -> Option<&[u8; COMMIT_SLOT_SIZE]> {
    image.get(slot_range(index))?.try_into().ok()
}

fn slot_mut(image: &mut [u8], index: usize) -> Option<&mut [u8; COMMIT_SLOT_SIZE]> {
    image.get_mut(slot_range(index))?.try_into().ok()
}

fn page_range(page: u32) -> std::ops::Range<usize> {
    let start = page_offset(PageId(page)) as usize;
    start..start + PAGE_SIZE
}

#[derive(Clone, Copy, Debug)]
enum Mutation {
    Header(Field, Verb),
    Slot {
        index: usize,
        field: Field,
        verb: Verb,
        seal: Seal,
    },
    /// One slot field takes the value that slot held after an older commit.
    SlotFrom {
        index: usize,
        field: Field,
        generation: usize,
        seal: Seal,
    },
    FlipBit {
        offset: usize,
    },
    /// An authentic older image of a page, back at its own position.
    ReplayPage {
        page: u32,
        generation: usize,
    },
    /// An authentic page written at another page's position.
    MisdirectPage {
        from: u32,
        to: u32,
    },
    Truncate(usize),
}

impl Mutation {
    /// Mutations aimed past the end of an already truncated image do nothing.
    fn apply(self, image: &mut Vec<u8>, fixture: &Fixture) {
        match self {
            Mutation::Header(field, verb) => {
                if let Some(header) = image.get_mut(..FILE_HEADER_SIZE) {
                    verb.apply(field.bytes(header));
                }
            }
            Mutation::Slot {
                index,
                field,
                verb,
                seal,
            } => {
                if let Some(slot) = slot_mut(image, index) {
                    verb.apply(field.bytes(slot));
                    seal.apply(slot);
                }
            }
            Mutation::SlotFrom {
                index,
                field,
                generation,
                seal,
            } => {
                let mut older = *slot(&fixture.images[generation], index).unwrap();
                if let Some(slot) = slot_mut(image, index) {
                    field.bytes(slot).copy_from_slice(field.bytes(&mut older));
                    seal.apply(slot);
                }
            }
            Mutation::FlipBit { offset } => {
                if let Some(byte) = image.get_mut(offset / 8) {
                    *byte ^= 1 << (offset % 8);
                }
            }
            Mutation::ReplayPage { page, generation } => {
                let range = page_range(page);
                if let (Some(older), Some(page)) = (
                    fixture.images[generation].get(range.clone()),
                    image.get_mut(range),
                ) {
                    page.copy_from_slice(older);
                }
            }
            Mutation::MisdirectPage { from, to } => {
                let (from, to) = (page_range(from), page_range(to));
                if from.end.max(to.end) <= image.len() {
                    image.copy_within(from, to.start);
                }
            }
            Mutation::Truncate(len) => image.truncate(len),
        }
    }
}

/// Run one stage of a case, turning a panic into a finding.
fn stage<T>(name: &str, run: impl FnOnce() -> T) -> std::result::Result<T, String> {
    catch_unwind(AssertUnwindSafe(run)).map_err(|panic| {
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic");
        format!("{name} panicked: {message}")
    })
}

/// Stage results shared by images that differ only in bytes a stage never
/// reads: slots recovery rejects and, after the audit, the slot it skipped.
#[derive(Default)]
struct Memo {
    audits: FxHashMap<blake3::Hash, bool>,
    sessions: FxHashMap<blake3::Hash, Session>,
}

/// What reading, committing to and reopening one opened state produced.
struct Session {
    tables: Option<Tables>,
    finding: Option<String>,
}

/// The image with every slot `keep` rejects replaced by zeros.
fn digest(image: &[u8], keep: impl Fn(usize) -> bool) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    let slots = slot_range(0).start..slot_range(1).end;
    hasher.update(image.get(..slots.start).unwrap_or(image));
    for index in 0..2 {
        match slot(image, index).filter(|_| keep(index)) {
            Some(bytes) => hasher.update(bytes),
            None => hasher.update(&[0; COMMIT_SLOT_SIZE]),
        };
    }
    hasher.update(image.get(slots.end..).unwrap_or_default());
    hasher.finalize()
}

/// Open, audit, read, commit to and reopen one image.
fn check(
    fixture: &Fixture,
    image: Vec<u8>,
    allowed: Allowed,
    memo: &mut Memo,
) -> std::result::Result<(), String> {
    let (dek, mac_key, _) = test_keys();
    let limit = image.len() as u64 + GROWTH_LIMIT;
    let disk = MemIO::from_bytes(image);
    let open = || {
        let io = CappedCommitIO::new(disk.share(), limit);
        TxnManager::open_with_sync(Box::new(io), dek, mac_key, 1, 64, SyncMode::Full)
    };
    let Ok(mgr) = stage("open", open)? else {
        return Ok(());
    };
    let opened = disk.bytes();
    let usable = |index| {
        slot(&opened, index).is_some_and(|bytes| {
            let slot = CommitSlot::deserialize(bytes);
            slot.verify_checksum() && slot.verify_mac(&mac_key)
        })
    };
    let chosen = mgr.state.lock().active_slot;
    let clean = match memo.audits.entry(digest(&opened, usable)) {
        Entry::Occupied(known) => *known.get(),
        Entry::Vacant(vacant) => {
            let report = stage("integrity check", || mgr.integrity_check())?;
            *vacant.insert(report.is_ok_and(|report| report.is_ok()))
        }
    };
    let session = digest(&opened, |index| index == chosen);
    let Session { tables, finding } = match memo.sessions.entry(session) {
        Entry::Occupied(known) => known.into_mut(),
        Entry::Vacant(vacant) => {
            let tables = stage("read", || read_tables(&mgr))?.ok();
            let finding = commit_and_reopen(mgr, open)?;
            vacant.insert(Session { tables, finding })
        }
    };
    let chosen_slot = CommitSlot::deserialize(slot(&opened, chosen).unwrap());
    let unexpected = tables
        .as_ref()
        .is_some_and(|tables| !fixture.holds(allowed, tables, &chosen_slot));
    if clean && unexpected {
        return Err("an image that reads and audits clean holds no allowed generation".into());
    }
    finding.clone().map_or(Ok(()), Err)
}

/// A commit's failure is a result; a panic, or a commit the reopened file
/// lost, is a finding.
fn commit_and_reopen(
    mgr: TxnManager,
    open: impl Fn() -> Result<TxnManager>,
) -> std::result::Result<Option<String>, String> {
    let committed = stage("commit", || {
        let mut wtx = mgr.begin_write()?;
        wtx.insert(SENTINEL, SENTINEL)?;
        wtx.delete(&key(2))?;
        wtx.commit()
    })?;
    drop(mgr);
    if committed.is_err() {
        return Ok(None);
    }
    let reopened = match stage("reopen", open)? {
        Ok(reopened) => reopened,
        Err(error) => return Ok(Some(format!("reopen after a commit failed: {error}"))),
    };
    Ok(
        match stage("read after reopen", || reopened.begin_read().get(SENTINEL))? {
            Ok(Some(_)) => None,
            other => Some(format!("the committed sentinel reads back as {other:?}")),
        },
    )
}

/// Fails listing each distinct finding with the first mutations that caused it.
fn sweep(fixture: &Fixture, cases: impl IntoIterator<Item = (Vec<Mutation>, Allowed)>) {
    let mut findings = BTreeMap::new();
    let mut memo = Memo::default();
    let mut checked = 0;
    for (mutations, allowed) in cases {
        let mut image = fixture.images[fixture.latest()].clone();
        for mutation in &mutations {
            mutation.apply(&mut image, fixture);
        }
        checked += 1;
        if let Err(finding) = check(fixture, image, allowed, &mut memo) {
            findings.entry(finding).or_insert(mutations);
        }
    }
    assert!(
        findings.is_empty(),
        "{} distinct findings in {checked} cases: {findings:#?}",
        findings.len()
    );
}

fn single(mutation: Mutation, allowed: Allowed) -> (Vec<Mutation>, Allowed) {
    (vec![mutation], allowed)
}

/// The bytes before the commit slots are the only ones no checksum covers.
#[test]
fn every_bit_flip_before_the_commit_slots_is_handled() {
    let fixture = Fixture::build();
    let cases = (0..COMMIT_SLOT_OFFSET * 8)
        .map(|offset| single(Mutation::FlipBit { offset }, Allowed::LatestOrPrevious));
    sweep(&fixture, cases);
}

fn slot_mutations(seals: &[Seal]) -> Vec<(Vec<Mutation>, Allowed)> {
    let mut cases = Vec::new();
    for index in 0..2 {
        for field in slot_fields() {
            for verb in VERBS {
                for &seal in seals {
                    let mutation = Mutation::Slot {
                        index,
                        field,
                        verb,
                        seal,
                    };
                    cases.push(single(mutation, Allowed::LatestOrPrevious));
                }
            }
        }
    }
    cases
}

#[test]
fn every_corrupted_header_and_slot_field_is_handled() {
    let fixture = Fixture::build();
    let header = HEADER_FIELDS.into_iter().flat_map(|field| {
        let allowed = if field.offset == GOD_BYTE_OFFSET {
            Allowed::LatestOrPrevious
        } else {
            Allowed::Latest
        };
        VERBS.map(|verb| single(Mutation::Header(field, verb), allowed))
    });
    sweep(
        &fixture,
        header.chain(slot_mutations(&[Seal::Raw, Seal::Legacy])),
    );
}

/// Slots re-sealed with the MAC key, as a writer bug could leave them.
#[test]
fn every_authentic_but_inconsistent_slot_is_handled() {
    let fixture = Fixture::build();
    let latest = fixture.latest();
    let older = (0..2).flat_map(|index| {
        slot_fields().into_iter().flat_map(move |field| {
            (0..latest).map(move |generation| {
                let mutation = Mutation::SlotFrom {
                    index,
                    field,
                    generation,
                    seal: Seal::Sealed,
                };
                single(mutation, Allowed::LatestOrPrevious)
            })
        })
    });
    let sealed = slot_mutations(&[Seal::Sealed]);
    sweep(&fixture, sealed.into_iter().chain(older));
}

#[test]
fn damaged_replayed_and_truncated_pages_are_handled() {
    let fixture = Fixture::build();
    let pages = fixture.pages();
    let damaged = (0..pages).flat_map(|page| {
        let range = page_range(page);
        [range.start, range.start + PAGE_SIZE / 2, range.end - 1]
            .map(|byte| single(Mutation::FlipBit { offset: byte * 8 }, Allowed::Latest))
            .into_iter()
            .chain((0..fixture.latest()).map(move |generation| {
                single(Mutation::ReplayPage { page, generation }, Allowed::Latest)
            }))
            .chain([single(
                Mutation::MisdirectPage {
                    from: page,
                    to: (page + 1) % pages,
                },
                Allowed::Latest,
            )])
    });
    let truncated = (0..=pages).flat_map(|page| {
        let start = page_offset(PageId(page)) as usize;
        [start, start + PAGE_SIZE / 2].map(|len| single(Mutation::Truncate(len), Allowed::Latest))
    });
    let header_only = [
        single(Mutation::Truncate(0), Allowed::Latest),
        single(Mutation::Truncate(FILE_HEADER_SIZE / 2), Allowed::Latest),
    ];
    sweep(&fixture, damaged.chain(truncated).chain(header_only));
}

/// A random mutation from every family the fixed sweeps cover.
fn random_mutation(rng: &mut SplitMix, fixture: &Fixture, fields: &[Field]) -> Mutation {
    let verb = match VERBS[rng.below(VERBS.len() as u64) as usize] {
        Verb::Random(_) => Verb::Random(rng.next()),
        verb => verb,
    };
    let pages = fixture.pages();
    let generation = rng.below(fixture.latest() as u64) as usize;
    match rng.below(7) {
        0 => Mutation::Header(
            HEADER_FIELDS[rng.below(HEADER_FIELDS.len() as u64) as usize],
            verb,
        ),
        1 => Mutation::Slot {
            index: rng.below(2) as usize,
            field: fields[rng.below(fields.len() as u64) as usize],
            verb,
            seal: SEALS[rng.below(3) as usize],
        },
        2 => Mutation::SlotFrom {
            index: rng.below(2) as usize,
            field: fields[rng.below(fields.len() as u64) as usize],
            generation,
            seal: SEALS[rng.below(3) as usize],
        },
        3 => Mutation::FlipBit {
            offset: rng.below(page_range(pages).start as u64 * 8) as usize,
        },
        4 => Mutation::ReplayPage {
            page: rng.below(pages.into()) as u32,
            generation,
        },
        5 => Mutation::MisdirectPage {
            from: rng.below(pages.into()) as u32,
            to: rng.below(pages.into()) as u32,
        },
        _ => Mutation::Truncate(rng.below(page_range(pages).start as u64) as usize),
    }
}

#[test]
fn stacked_random_mutations_are_handled() {
    let fixture = Fixture::build();
    let fields = slot_fields();
    let seeds = std::env::var("CITADEL_FILE_FUZZ_SEEDS")
        .ok()
        .and_then(|seeds| seeds.parse().ok())
        .unwrap_or(DEFAULT_SEEDS);
    let cases: Vec<_> = (0..seeds)
        .flat_map(|seed| {
            let mut rng = SplitMix(0xF11E ^ seed);
            (0..STACKS_PER_SEED)
                .map(|_| {
                    let depth = 1 + rng.below(4);
                    let stack = (0..depth)
                        .map(|_| random_mutation(&mut rng, &fixture, &fields))
                        .collect();
                    (stack, Allowed::Any)
                })
                .collect::<Vec<_>>()
        })
        .collect();
    sweep(&fixture, cases);
}
