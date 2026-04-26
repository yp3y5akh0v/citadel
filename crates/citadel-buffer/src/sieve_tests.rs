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

#[test]
fn update_existing_key() {
    let mut cache = SieveCache::<u32>::new(4);
    cache.insert(1, 100).unwrap();
    cache.insert(1, 200).unwrap();

    assert_eq!(cache.get(1), Some(&200));
    assert_eq!(cache.len(), 1);
}
