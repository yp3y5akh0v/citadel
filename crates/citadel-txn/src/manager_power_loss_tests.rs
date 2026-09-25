//! Power-loss recovery against each `SyncMode` contract.
//!
//! The simulated disk keeps the image as of the last fsync and every write
//! issued since. At a crash each 512-byte sector keeps some prefix of the
//! writes made to it since that fsync: writes can be lost, reordered across
//! sectors or torn between sectors, while a single sector write is atomic.
use super::*;
use std::collections::BTreeMap;

const SECTOR: usize = 512;

enum Op {
    Write { offset: usize, data: Vec<u8> },
    SetLen(usize),
}

struct Disk {
    durable: Vec<u8>,
    pending: Vec<Op>,
    view: Vec<u8>,
    ops: u64,
    syncs: u64,
    crash_at: Option<u64>,
    crashed: bool,
}

#[derive(Clone)]
struct PowerLossIO(Arc<Mutex<Disk>>);

fn power_lost() -> Error {
    Error::Io(std::io::Error::other("simulated power loss"))
}

fn apply(image: &mut Vec<u8>, op: &Op) {
    match op {
        Op::Write { offset, data } => {
            let end = offset + data.len();
            if image.len() < end {
                image.resize(end, 0);
            }
            image[*offset..end].copy_from_slice(data);
        }
        Op::SetLen(len) => image.resize(*len, 0),
    }
}

impl PowerLossIO {
    fn new(image: Vec<u8>) -> Self {
        Self(Arc::new(Mutex::new(Disk {
            durable: image.clone(),
            pending: Vec::new(),
            view: image,
            ops: 0,
            syncs: 0,
            crash_at: None,
            crashed: false,
        })))
    }

    /// Lose power when `count` more mutating operations have been issued.
    fn arm(&self, count: u64) {
        let mut disk = self.0.lock();
        disk.crash_at = Some(disk.ops + count);
    }

    fn ops(&self) -> u64 {
        self.0.lock().ops
    }

    fn syncs(&self) -> u64 {
        self.0.lock().syncs
    }

    fn pending(&self) -> usize {
        self.0.lock().pending.len()
    }

    fn has_pending(&self) -> bool {
        !self.0.lock().pending.is_empty()
    }

    fn view(&self) -> Vec<u8> {
        self.0.lock().view.clone()
    }

    /// The crash-point operation fails, and so does every later call.
    fn mutate(&self) -> Result<parking_lot::MutexGuard<'_, Disk>> {
        let mut disk = self.0.lock();
        if disk.crashed || disk.crash_at == Some(disk.ops) {
            disk.crashed = true;
            return Err(power_lost());
        }
        disk.ops += 1;
        Ok(disk)
    }

    /// Durable bytes plus the first `count` pending operations in issue order.
    fn ordered_image(&self, count: usize) -> Vec<u8> {
        let disk = self.0.lock();
        let mut image = disk.durable.clone();
        for op in &disk.pending[..count] {
            apply(&mut image, op);
        }
        image
    }

    /// Each sector keeps an independently chosen prefix of the writes that
    /// touched it; each pending length change independently persists or not.
    fn sector_image(&self, rng: &mut SplitMix) -> Vec<u8> {
        let disk = self.0.lock();
        let mut writes_per_sector: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        let mut len = disk.durable.len();
        for (index, op) in disk.pending.iter().enumerate() {
            match op {
                Op::Write { offset, data } if !data.is_empty() => {
                    for sector in offset / SECTOR..=(offset + data.len() - 1) / SECTOR {
                        writes_per_sector.entry(sector).or_default().push(index);
                    }
                }
                Op::Write { .. } => {}
                Op::SetLen(new_len) => {
                    if rng.below(2) == 0 {
                        len = *new_len;
                    }
                }
            }
        }
        let kept: Vec<(usize, &[usize])> = writes_per_sector
            .iter()
            .map(|(&sector, writes)| {
                let keep = rng.below(writes.len() as u64 + 1) as usize;
                (sector, &writes[..keep])
            })
            .collect();
        for &(sector, writes) in &kept {
            for &index in writes {
                if let Op::Write { offset, data } = &disk.pending[index] {
                    let end = (offset + data.len()).min((sector + 1) * SECTOR);
                    len = len.max(end);
                }
            }
        }
        let mut image = disk.durable.clone();
        image.resize(len, 0);
        for (sector, writes) in kept {
            for &index in writes {
                if let Op::Write { offset, data } = &disk.pending[index] {
                    let start = (*offset).max(sector * SECTOR);
                    let end = (offset + data.len()).min((sector + 1) * SECTOR);
                    image[start..end].copy_from_slice(&data[start - offset..end - offset]);
                }
            }
        }
        image
    }
}

impl PageIO for PowerLossIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.read_at(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.write_at(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let disk = self.0.lock();
        if disk.crashed {
            return Err(power_lost());
        }
        let start = offset as usize;
        let end = start + buf.len();
        if end > disk.view.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read past end of simulated disk",
            )));
        }
        buf.copy_from_slice(&disk.view[start..end]);
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        let mut disk = self.mutate()?;
        let op = Op::Write {
            offset: offset as usize,
            data: buf.to_vec(),
        };
        apply(&mut disk.view, &op);
        disk.pending.push(op);
        Ok(())
    }

    fn fsync(&self) -> Result<()> {
        let mut disk = self.mutate()?;
        let Disk {
            durable,
            pending,
            syncs,
            ..
        } = &mut *disk;
        for op in pending.drain(..) {
            apply(durable, &op);
        }
        *syncs += 1;
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        let disk = self.0.lock();
        if disk.crashed {
            return Err(power_lost());
        }
        Ok(disk.view.len() as u64)
    }

    fn truncate(&self, size: u64) -> Result<()> {
        let mut disk = self.mutate()?;
        let op = Op::SetLen(size as usize);
        apply(&mut disk.view, &op);
        disk.pending.push(op);
        Ok(())
    }
}

struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

enum Change {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

type State = BTreeMap<Vec<u8>, Vec<u8>>;

const KEYS: u64 = 24;

/// Small, page-sized and multi-page overflow values, with deletes, so commits
/// allocate, free and reuse pages across generations.
fn workload(seed: u64, txns: usize) -> Vec<Vec<Change>> {
    let mut rng = SplitMix(seed);
    (0..txns)
        .map(|txn| {
            (0..1 + rng.below(6))
                .map(|_| {
                    let key = format!("key-{:02}", rng.below(KEYS)).into_bytes();
                    if rng.below(4) == 0 {
                        return Change::Delete(key);
                    }
                    let len = match rng.below(8) {
                        0 => 12_000,
                        1 => 2_500,
                        2 | 3 => 600,
                        _ => 24,
                    };
                    let mut value = vec![rng.next() as u8; len];
                    value[..8].copy_from_slice(&(txn as u64).to_le_bytes());
                    Change::Put(key, value)
                })
                .collect()
        })
        .collect()
}

/// `states[n]` is the logical content after the first `n` transactions.
fn states(txns: &[Vec<Change>]) -> Vec<State> {
    let mut state = State::new();
    let mut out = vec![state.clone()];
    for txn in txns {
        for change in txn {
            match change {
                Change::Put(key, value) => {
                    state.insert(key.clone(), value.clone());
                }
                Change::Delete(key) => {
                    state.remove(key);
                }
            }
        }
        out.push(state.clone());
    }
    out
}

/// A seeded workload and the conditions it runs under.
#[derive(Clone, Copy)]
struct Scenario {
    mode: SyncMode,
    secure_delete: bool,
    seed: u64,
    txns: usize,
    /// Commits between reopens of the database, if it is reopened at all.
    reopen_every: Option<usize>,
    /// Random sector-level images taken at each crash point.
    sector_images: u64,
}

fn scenario(mode: SyncMode, seed: u64) -> Scenario {
    Scenario {
        mode,
        secure_delete: false,
        seed,
        txns: 10,
        reopen_every: None,
        sector_images: 4,
    }
}

/// Create a database, arm the crash, and run until the disk fails. Returns
/// the disk and how many commits were acknowledged.
fn run(
    scenario: &Scenario,
    txns: &[Vec<Change>],
    crash_after: Option<u64>,
) -> (PowerLossIO, usize) {
    let Scenario {
        mode,
        secure_delete,
        reopen_every,
        ..
    } = *scenario;
    let (dek, mac_key, dek_id) = test_keys();
    let io = PowerLossIO::new(Vec::new());
    let mut mgr = TxnManager::create_with_sync(
        Box::new(io.clone()),
        dek,
        mac_key,
        1,
        0x1234,
        dek_id,
        64,
        mode,
    )
    .unwrap();
    mgr.set_secure_delete(secure_delete);
    if mode != SyncMode::Off {
        assert!(!io.has_pending(), "{mode:?} creation must be durable");
    }
    if let Some(count) = crash_after {
        io.arm(count);
    }
    let mut acked = 0;
    for txn in txns {
        let Ok(mut wtx) = mgr.begin_write() else {
            break;
        };
        let applied = txn.iter().try_for_each(|change| match change {
            Change::Put(key, value) => wtx.insert(key, value).map(drop),
            Change::Delete(key) => wtx.delete(key).map(drop),
        });
        if applied.is_err() || wtx.commit().is_err() {
            break;
        }
        acked += 1;
        if reopen_every.is_some_and(|every| acked % every == 0) {
            drop(mgr);
            let Ok(reopened) =
                TxnManager::open_with_sync(Box::new(io.clone()), dek, mac_key, 1, 64, mode)
            else {
                break;
            };
            mgr = reopened;
            mgr.set_secure_delete(secure_delete);
        }
    }
    (io, acked)
}

fn read_state(mgr: &TxnManager) -> Result<State> {
    let mut rtx = mgr.begin_read();
    let mut state = State::new();
    for key in 0..KEYS {
        let key = format!("key-{key:02}").into_bytes();
        if let Some(value) = rtx.get(&key)? {
            state.insert(key, value);
        }
    }
    Ok(state)
}

/// Open a crash image, require a clean integrity report and one of the
/// `allowed` generations, then prove the recovered database still commits.
fn verify_recovery(
    image: Vec<u8>,
    mode: SyncMode,
    states: &[State],
    allowed: std::ops::RangeInclusive<usize>,
) -> std::result::Result<(), String> {
    let (dek, mac_key, _) = test_keys();
    let io = PowerLossIO::new(image);
    let mgr = TxnManager::open_with_sync(Box::new(io.clone()), dek, mac_key, 1, 64, mode)
        .map_err(|error| format!("open failed: {error:?}"))?;
    let report = mgr
        .integrity_check()
        .map_err(|error| format!("integrity check failed: {error:?}"))?;
    if !report.is_ok() {
        return Err(format!("integrity errors: {:?}", report.errors));
    }
    let state = read_state(&mgr).map_err(|error| format!("read failed: {error:?}"))?;
    if !allowed.clone().any(|n| states.get(n) == Some(&state)) {
        let other = states.iter().position(|candidate| candidate == &state);
        return Err(format!(
            "recovered generation {other:?}, allowed {allowed:?}"
        ));
    }

    let mut wtx = mgr
        .begin_write()
        .map_err(|error| format!("post-recovery begin failed: {error:?}"))?;
    wtx.insert(b"sentinel", b"after recovery")
        .map_err(|error| format!("post-recovery insert failed: {error:?}"))?;
    wtx.commit()
        .map_err(|error| format!("post-recovery commit failed: {error:?}"))?;
    drop(mgr);
    let reopened = TxnManager::open_with_sync(
        Box::new(PowerLossIO::new(io.view())),
        dek,
        mac_key,
        1,
        64,
        mode,
    )
    .map_err(|error| format!("reopen after recovery commit failed: {error:?}"))?;
    let report = reopened
        .integrity_check()
        .map_err(|error| format!("integrity check after recovery commit failed: {error:?}"))?;
    if !report.is_ok() {
        return Err(format!(
            "integrity errors after recovery commit: {:?}",
            report.errors
        ));
    }
    let mut rtx = reopened.begin_read();
    if rtx.get(b"sentinel").map_err(|error| format!("{error:?}"))?
        != Some(b"after recovery".to_vec())
        || read_state(&reopened).map_err(|error| format!("{error:?}"))? != state
    {
        return Err("post-recovery commit changed recovered data".into());
    }
    Ok(())
}

/// Crash at every mutating operation of the workload. Each crash yields
/// in-order prefixes of the pending writes, from none to all of them, and
/// sector-level subsets. A prefix is the same image at every crash point of
/// its fsync epoch, so one already verified with the same acknowledged count
/// is skipped.
fn explore(
    scenario: Scenario,
    allowed: impl Fn(usize) -> std::ops::RangeInclusive<usize>,
) -> (usize, Vec<String>) {
    let txns = workload(scenario.seed, scenario.txns);
    let states = states(&txns);
    let (full_run, acked) = run(&scenario, &txns, None);
    assert_eq!(acked, txns.len());
    let workload_ops = full_run.ops();
    let mut checked = 0;
    let mut violations = Vec::new();
    let mut verified = FxHashSet::default();
    for crash_after in 0..workload_ops {
        let (io, acked) = run(&scenario, &txns, Some(crash_after));
        let pending = io.pending();
        let mut images = Vec::new();
        for count in [0, 1, pending / 2, pending] {
            if count <= pending && verified.insert((io.syncs(), count, acked)) {
                images.push((
                    format!("first {count} of {pending}"),
                    io.ordered_image(count),
                ));
            }
        }
        let mut rng = SplitMix(scenario.seed ^ (crash_after << 20));
        for sample in 0..scenario.sector_images {
            images.push((format!("sectors #{sample}"), io.sector_image(&mut rng)));
        }
        for (label, image) in images {
            checked += 1;
            if let Err(problem) = verify_recovery(image, scenario.mode, &states, allowed(acked)) {
                violations.push(format!(
                    "crash after op {crash_after} ({acked} acked), image {label}: {problem}"
                ));
            }
        }
    }
    (checked, violations)
}

/// Full: the recovered data is the last acknowledged commit or the one that
/// was in flight, never older.
fn full_allowed(acked: usize) -> std::ops::RangeInclusive<usize> {
    acked..=acked + 1
}

/// Normal: the latest acknowledged commit may be lost, the previous one never.
fn normal_allowed(acked: usize) -> std::ops::RangeInclusive<usize> {
    acked.saturating_sub(1)..=acked + 1
}

fn assert_no_violations((checked, violations): (usize, Vec<String>)) {
    assert!(checked > 0);
    assert!(
        violations.is_empty(),
        "{} of {checked} crash images violated the contract; first: {:#?}",
        violations.len(),
        &violations[..violations.len().min(5)]
    );
}

// Each sync mode and each secure-delete setting runs once with reopens, which
// rebuild reclamation state from the selected slot, and once without.

#[test]
fn full_sync_survives_power_loss_at_every_operation() {
    assert_no_violations(explore(scenario(SyncMode::Full, 0xC1DA), full_allowed));
}

#[test]
fn full_sync_secure_delete_survives_power_loss_across_reopens() {
    let secure = Scenario {
        secure_delete: true,
        reopen_every: Some(3),
        ..scenario(SyncMode::Full, 0x5EC0)
    };
    assert_no_violations(explore(secure, full_allowed));
}

#[test]
fn normal_sync_keeps_the_previous_commit_across_power_loss_and_reopens() {
    let reopened = Scenario {
        reopen_every: Some(3),
        ..scenario(SyncMode::Normal, 0x0A11)
    };
    assert_no_violations(explore(reopened, normal_allowed));
}

#[test]
fn normal_sync_secure_delete_keeps_the_previous_commit_across_power_loss() {
    let secure = Scenario {
        secure_delete: true,
        ..scenario(SyncMode::Normal, 0xE2A5)
    };
    assert_no_violations(explore(secure, normal_allowed));
}

/// Off promises process-crash safety only: every issued write reaches the OS.
#[test]
fn off_sync_survives_a_process_crash_at_every_operation() {
    let off = scenario(SyncMode::Off, 0x0FF0);
    let txns = workload(off.seed, off.txns);
    let states = states(&txns);
    let (full_run, _) = run(&off, &txns, None);
    let mut violations = Vec::new();
    for crash_after in 0..full_run.ops() {
        let (io, acked) = run(&off, &txns, Some(crash_after));
        if let Err(problem) = verify_recovery(io.view(), off.mode, &states, full_allowed(acked)) {
            violations.push(format!(
                "crash after op {crash_after} ({acked} acked): {problem}"
            ));
        }
    }
    assert_no_violations((full_run.ops() as usize, violations));
}

/// Negative control: the same oracle must reject Off under power loss, where
/// acknowledged commits are allowed to vanish.
#[test]
fn off_sync_power_loss_is_detected_by_the_harness() {
    let off = Scenario {
        txns: 6,
        sector_images: 2,
        ..scenario(SyncMode::Off, 0x0FF1)
    };
    let (checked, violations) = explore(off, full_allowed);
    assert!(checked > 0);
    assert!(
        !violations.is_empty(),
        "the harness accepted every Off-mode power-loss image; it cannot detect lost commits"
    );
}
