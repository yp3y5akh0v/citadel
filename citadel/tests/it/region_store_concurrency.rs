//! Concurrency regression: the `Database`-owned region key store serializes allocation.

use std::collections::HashSet;
use std::sync::Arc;
use std::thread;

use citadel::core::WRAPPED_KEY_SIZE;
use citadel::{Argon2Profile, DatabaseBuilder, SlotState};

#[test]
fn concurrent_region_store_allocations_never_collide() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"pw")
            .enable_region_keys(true)
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );

    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 40; // 8 * 40 = 320 allocations -> forces growth past the 64 prealloc

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                let mut mine = Vec::new();
                for i in 0..PER_THREAD {
                    let owner = t * 1_000 + i + 1; // unique, nonzero (0 is reserved for empty/tomb)
                    let wrapped = [(owner & 0xff) as u8; WRAPPED_KEY_SIZE];
                    let (slot, _gen) = db.region_store_allocate_write(owner, &wrapped).unwrap();
                    mine.push((slot, owner));
                }
                mine
            })
        })
        .collect();

    let all: Vec<(u32, u64)> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    let unique: HashSet<u32> = all.iter().map(|(s, _)| *s).collect();
    assert_eq!(
        unique.len(),
        all.len(),
        "slot collision: two concurrent allocations shared one slot"
    );

    for (slot, owner) in &all {
        let rec = db.region_store_slot(*slot).unwrap();
        assert_eq!(rec.state, SlotState::Live, "slot {slot} not live");
        assert_eq!(
            rec.region_id, *owner,
            "slot {slot} owner mismatch: a concurrent write clobbered another's key"
        );
    }
}
