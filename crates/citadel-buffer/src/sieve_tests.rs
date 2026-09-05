use super::*;

#[test]
fn basic_insert_and_get() {
    let mut cache = SieveCache::<u32>::new(4);
    cache.insert(1, 100).unwrap();
    cache.insert(2, 200).unwrap();

    assert_eq!(cache.get(1), Some(&100));
    assert_eq!(cache.get(2), Some(&200));
    assert_eq!(cache.get(3), None);
}

#[test]
fn eviction_when_full() {
    let mut cache = SieveCache::<u32>::new(3);
    cache.insert(1, 100).unwrap();
    cache.insert(2, 200).unwrap();
    cache.insert(3, 300).unwrap();
    assert_eq!(cache.len(), 3);

    for entry in &mut cache.entries {
        entry.visited = false;
    }

    let result = cache.insert(4, 400).unwrap();
    assert!(result.is_some());
    assert_eq!(cache.len(), 3);
}

#[test]
fn visited_entries_survive_eviction() {
    let mut cache = SieveCache::<u32>::new(3);
    cache.insert(1, 100).unwrap();
    cache.insert(2, 200).unwrap();
    cache.insert(3, 300).unwrap();

    for entry in &mut cache.entries {
        entry.visited = false;
    }

    cache.get(2);

    let evicted = cache.insert(4, 400).unwrap().unwrap();
    assert_ne!(evicted.0, 2, "visited entry should not be evicted");
    assert!(cache.contains(2));
    assert!(cache.contains(4));
}

#[test]
fn dirty_entries_not_evicted() {
    let mut cache = SieveCache::<u32>::new(2);
    cache.insert(1, 100).unwrap();
    cache.insert(2, 200).unwrap();

    for entry in &mut cache.entries {
        entry.visited = false;
    }

    cache.set_dirty(1);

    let evicted = cache.insert(3, 300).unwrap().unwrap();
    assert_eq!(evicted.0, 2);
    assert!(cache.contains(1));
    assert!(cache.contains(3));
}

#[test]
fn all_dirty_returns_err() {
    let mut cache = SieveCache::<u32>::new(2);
    cache.insert(1, 100).unwrap();
    cache.insert(2, 200).unwrap();

    cache.set_dirty(1);
    cache.set_dirty(2);

    for entry in &mut cache.entries {
        entry.visited = false;
    }

    let result = cache.insert(3, 300);
    assert!(result.is_err());
}

#[test]
fn clear_dirty() {
    let mut cache = SieveCache::<u32>::new(2);
    cache.insert(1, 100).unwrap();
    cache.set_dirty(1);
    assert!(cache.is_dirty(1));

    cache.clear_dirty(1);
    assert!(!cache.is_dirty(1));
}

#[test]
fn dirty_entries_iterator() {
    let mut cache = SieveCache::<u32>::new(4);
    cache.insert(1, 100).unwrap();
    cache.insert(2, 200).unwrap();
    cache.insert(3, 300).unwrap();

    cache.set_dirty(1);
    cache.set_dirty(3);

    let dirty: Vec<_> = cache.dirty_entries().collect();
    assert_eq!(dirty.len(), 2);
    assert_eq!(cache.dirty_count(), 2);
}

#[test]
fn clear_all_dirty() {
    let mut cache = SieveCache::<u32>::new(3);
    cache.insert(1, 100).unwrap();
    cache.insert(2, 200).unwrap();
    cache.set_dirty(1);
    cache.set_dirty(2);

    cache.clear_all_dirty();
    assert_eq!(cache.dirty_count(), 0);
}

#[test]
fn remove_entry() {
    let mut cache = SieveCache::<u32>::new(4);
    cache.insert(1, 100).unwrap();
    cache.insert(2, 200).unwrap();

    let removed = cache.remove(1);
    assert_eq!(removed, Some(100));
    assert!(!cache.contains(1));
    assert_eq!(cache.len(), 1);
}

/// Slots are handed out from a free list, so a vacated slot that is never returned would
/// shrink the usable pool without any error.
#[test]
fn slots_are_reusable_after_eviction_and_removal() {
    const CAP: usize = 8;
    let mut cache = SieveCache::<u32>::new(CAP);

    // Churn well past capacity so eviction recycles slots many times over.
    for k in 0..200u64 {
        cache.insert(k, k as u32).unwrap();
    }
    assert_eq!(cache.len(), CAP, "eviction must keep the pool full");

    // Explicit removals return their slots too.
    let live: Vec<u64> = (0..200u64).filter(|&k| cache.contains(k)).collect();
    for k in &live {
        cache.remove(*k);
    }
    assert_eq!(cache.len(), 0);

    // The whole capacity is still usable, and every key is retrievable.
    for k in 1000..(1000 + CAP as u64) {
        cache.insert(k, k as u32).unwrap();
    }
    assert_eq!(cache.len(), CAP);
    for k in 1000..(1000 + CAP as u64) {
        assert_eq!(cache.get(k), Some(&(k as u32)), "slot for {k} was lost");
    }

    cache.clear();
    for k in 0..CAP as u64 {
        cache.insert(k, k as u32).unwrap();
    }
    assert_eq!(cache.len(), CAP, "clear must return every slot to the pool");
}

#[test]
fn update_existing_key() {
    let mut cache = SieveCache::<u32>::new(4);
    cache.insert(1, 100).unwrap();
    cache.insert(1, 200).unwrap();

    assert_eq!(cache.get(1), Some(&200));
    assert_eq!(cache.len(), 1);
}

thread_local! {
    static DEFAULT_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

struct CountedDefault;

impl Default for CountedDefault {
    fn default() -> Self {
        DEFAULT_CALLS.with(|count| count.set(count.get() + 1));
        Self
    }
}

#[test]
fn vacant_slots_do_not_construct_default_values() {
    DEFAULT_CALLS.with(|count| count.set(0));
    let cache = SieveCache::<CountedDefault>::new(8);
    assert!(cache.is_empty());
    DEFAULT_CALLS.with(|count| assert_eq!(count.get(), 0));
}

#[test]
fn eviction_does_not_construct_a_replacement_value() {
    let mut cache = SieveCache::<CountedDefault>::new(1);
    cache.insert(1, CountedDefault).unwrap();
    DEFAULT_CALLS.with(|count| count.set(0));
    assert_eq!(cache.insert(2, CountedDefault).unwrap().unwrap().0, 1);
    DEFAULT_CALLS.with(|count| assert_eq!(count.get(), 0));
}

#[test]
fn removal_does_not_construct_a_replacement_value() {
    let mut cache = SieveCache::<CountedDefault>::new(1);
    cache.insert(1, CountedDefault).unwrap();
    DEFAULT_CALLS.with(|count| count.set(0));
    assert!(cache.remove(1).is_some());
    DEFAULT_CALLS.with(|count| assert_eq!(count.get(), 0));
}

#[test]
fn clear_drops_values_once_and_keeps_slots_reusable() {
    #[derive(Default)]
    struct Tracked(std::rc::Rc<std::cell::Cell<usize>>);

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    let drops = std::rc::Rc::new(std::cell::Cell::new(0));
    let mut cache = SieveCache::new(2);
    cache.insert(1, Tracked(drops.clone())).unwrap();
    cache.insert(2, Tracked(drops.clone())).unwrap();
    cache.set_dirty(1);
    cache.clear();
    assert_eq!(drops.get(), 2);
    assert!(cache.is_empty());
    assert_eq!(cache.dirty_count(), 0);
    cache.clear();
    assert_eq!(drops.get(), 2);
    cache.insert(3, Tracked(drops.clone())).unwrap();
    drop(cache);
    assert_eq!(drops.get(), 3);
}

#[test]
fn mixed_operations_preserve_eviction_order_and_dirty_pins() {
    #[derive(Clone)]
    struct Entry {
        key: u64,
        value: u32,
        visited: bool,
        dirty: bool,
    }

    const CAPACITY: usize = 7;
    let mut cache = SieveCache::new(CAPACITY);
    let mut reference: Vec<Option<Entry>> = vec![None; CAPACITY];
    let mut free: Vec<_> = (0..CAPACITY).rev().collect();
    let mut hand = 0;
    let mut seed = 0x19d6_832au64;
    for step in 0..5000u32 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = (seed >> 32) % 19;
        let position = reference
            .iter()
            .position(|entry| entry.as_ref().is_some_and(|entry| entry.key == key));
        match seed % 11 {
            0..=3 => {
                let expected = if let Some(index) = position {
                    let entry = reference[index].as_mut().unwrap();
                    entry.value = step;
                    entry.visited = true;
                    Ok(None)
                } else {
                    let mut evicted = None;
                    if free.is_empty() {
                        for _ in 0..2 * CAPACITY {
                            let index = hand;
                            hand = (hand + 1) % CAPACITY;
                            let entry = reference[index].as_mut().unwrap();
                            if entry.dirty {
                                continue;
                            }
                            if entry.visited {
                                entry.visited = false;
                                continue;
                            }
                            let entry = reference[index].take().unwrap();
                            evicted = Some((entry.key, entry.value));
                            free.push(index);
                            break;
                        }
                    }
                    if let Some(index) = free.pop() {
                        reference[index] = Some(Entry {
                            key,
                            value: step,
                            visited: true,
                            dirty: false,
                        });
                        Ok(evicted)
                    } else {
                        Err(())
                    }
                };
                assert_eq!(cache.insert(key, step), expected, "step {step}");
            }
            4 => {
                let expected = position.map(|index| {
                    let entry = reference[index].as_mut().unwrap();
                    entry.visited = true;
                    entry.value
                });
                assert_eq!(cache.get(key).copied(), expected);
            }
            5 => {
                if let Some(index) = position {
                    reference[index].as_mut().unwrap().dirty = true;
                }
                cache.set_dirty(key);
            }
            6 => {
                if let Some(index) = position {
                    reference[index].as_mut().unwrap().dirty = false;
                }
                cache.clear_dirty(key);
            }
            7 => {
                let expected = position.map(|index| {
                    free.push(index);
                    reference[index].take().unwrap().value
                });
                assert_eq!(cache.remove(key), expected);
            }
            8 => {
                for entry in reference.iter_mut().flatten() {
                    entry.dirty = false;
                }
                cache.clear_all_dirty();
            }
            9 => {
                for entry in reference.iter_mut().flatten().filter(|entry| entry.dirty) {
                    entry.value += 1;
                }
                for (_, value) in cache.dirty_entries_mut() {
                    *value += 1;
                }
            }
            _ => {
                reference.fill(None);
                free.clear();
                free.extend((0..CAPACITY).rev());
                hand = 0;
                cache.clear();
            }
        }
        assert_eq!(cache.len(), reference.iter().flatten().count());
        let mut actual: Vec<_> = cache
            .dirty_entries()
            .map(|(key, value)| (key, *value))
            .collect();
        let mut expected: Vec<_> = reference
            .iter()
            .flatten()
            .filter(|entry| entry.dirty)
            .map(|entry| (entry.key, entry.value))
            .collect();
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected);
        assert_eq!(cache.dirty_count(), expected.len());
        for key in 0..19 {
            let entry = reference.iter().flatten().find(|entry| entry.key == key);
            assert_eq!(cache.contains(key), entry.is_some());
            assert_eq!(cache.is_dirty(key), entry.is_some_and(|entry| entry.dirty));
        }
    }
}
