use std::collections::HashMap;

/// SIEVE eviction cache.
///
/// Single FIFO queue with a moving eviction hand and a visited bit.
/// Dirty entries are pinned and never evictable.
///
/// O(1) amortized eviction.
pub struct SieveCache<V> {
    entries: Vec<SieveEntry<V>>,
    /// Maps key -> index in entries vec.
    index: HashMap<u64, usize>,
    /// Moving eviction pointer.
    hand: usize,
    /// Number of occupied slots.
    len: usize,
    capacity: usize,
}

struct SieveEntry<V> {
    key: u64,
    value: V,
    visited: bool,
    dirty: bool,
    occupied: bool,
}

impl<V> SieveEntry<V> {
    fn empty(value: V) -> Self {
        Self {
            key: 0,
            value,
            visited: false,
            dirty: false,
            occupied: false,
        }
    }
}

impl<V: Default> SieveCache<V> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "cache capacity must be > 0");
        let mut entries = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            entries.push(SieveEntry::empty(V::default()));
        }
        Self {
            entries,
            index: HashMap::with_capacity(capacity),
            hand: 0,
            len: 0,
            capacity,
        }
    }

    pub fn get(&mut self, key: u64) -> Option<&V> {
        if let Some(&idx) = self.index.get(&key) {
            self.entries[idx].visited = true;
            Some(&self.entries[idx].value)
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, key: u64) -> Option<&mut V> {
        if let Some(&idx) = self.index.get(&key) {
            self.entries[idx].visited = true;
            Some(&mut self.entries[idx].value)
        } else {
            None
        }
    }

    pub fn contains(&self, key: u64) -> bool {
        self.index.contains_key(&key)
    }

    /// Returns Err if all entries are dirty (pinned) and eviction is impossible.
    #[allow(clippy::result_unit_err)]
    pub fn insert(&mut self, key: u64, value: V) -> Result<Option<(u64, V)>, ()> {
        if let Some(&idx) = self.index.get(&key) {
            self.entries[idx].value = value;
            self.entries[idx].visited = true;
            return Ok(None);
        }

        if self.len < self.capacity {
            let idx = self.find_empty_slot();
            self.entries[idx].key = key;
            self.entries[idx].value = value;
            self.entries[idx].visited = true;
            self.entries[idx].dirty = false;
            self.entries[idx].occupied = true;
            self.index.insert(key, idx);
            self.len += 1;
            return Ok(None);
        }

        let evicted = self.evict()?;
        let idx = self.find_empty_slot();
        self.entries[idx].key = key;
        self.entries[idx].value = value;
        self.entries[idx].visited = true;
        self.entries[idx].dirty = false;
        self.entries[idx].occupied = true;
        self.index.insert(key, idx);
        self.len += 1;

        Ok(Some(evicted))
    }

    fn evict(&mut self) -> Result<(u64, V), ()> {
        let mut scanned = 0;

        loop {
            if scanned >= self.capacity * 2 {
                // All entries are dirty - can't evict
                return Err(());
            }

            let idx = self.hand;
            self.hand = (self.hand + 1) % self.capacity;
            scanned += 1;

            if !self.entries[idx].occupied {
                continue;
            }

            if self.entries[idx].dirty {
                continue;
            }

            if self.entries[idx].visited {
                self.entries[idx].visited = false;
                continue;
            }

            let evicted_key = self.entries[idx].key;
            let evicted_value = std::mem::take(&mut self.entries[idx].value);
            self.entries[idx].occupied = false;
            self.index.remove(&evicted_key);
            self.len -= 1;

            return Ok((evicted_key, evicted_value));
        }
    }

    fn find_empty_slot(&self) -> usize {
        for (i, entry) in self.entries.iter().enumerate() {
            if !entry.occupied {
                return i;
            }
        }
        unreachable!("find_empty_slot called when cache is full");
    }

    pub fn set_dirty(&mut self, key: u64) {
        if let Some(&idx) = self.index.get(&key) {
            self.entries[idx].dirty = true;
        }
    }

    pub fn clear_dirty(&mut self, key: u64) {
        if let Some(&idx) = self.index.get(&key) {
            self.entries[idx].dirty = false;
        }
    }

    pub fn is_dirty(&self, key: u64) -> bool {
        self.index
            .get(&key)
            .map(|&idx| self.entries[idx].dirty)
            .unwrap_or(false)
    }

    pub fn dirty_entries(&self) -> impl Iterator<Item = (u64, &V)> {
        self.entries
            .iter()
            .filter(|e| e.occupied && e.dirty)
            .map(|e| (e.key, &e.value))
    }

    pub fn dirty_entries_mut(&mut self) -> impl Iterator<Item = (u64, &mut V)> {
        self.entries
            .iter_mut()
            .filter(|e| e.occupied && e.dirty)
            .map(|e| (e.key, &mut e.value))
    }

    pub fn clear_all_dirty(&mut self) {
        for entry in &mut self.entries {
            if entry.occupied {
                entry.dirty = false;
            }
        }
    }

    pub fn remove(&mut self, key: u64) -> Option<V> {
        if let Some(idx) = self.index.remove(&key) {
            let value = std::mem::take(&mut self.entries[idx].value);
            self.entries[idx].occupied = false;
            self.len -= 1;
            Some(value)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn dirty_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| e.occupied && e.dirty)
            .count()
    }

    pub fn clear(&mut self) {
        for entry in &mut self.entries {
            entry.occupied = false;
            entry.visited = false;
            entry.dirty = false;
        }
        self.index.clear();
        self.len = 0;
        self.hand = 0;
    }
}

#[cfg(test)]
mod tests {
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

        // Don't access any - all are unvisited, one should be evicted
        // Reset visited flags by clearing them
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

        // Reset all visited flags
        for entry in &mut cache.entries {
            entry.visited = false;
        }

        // Access entry 2 (marks visited)
        cache.get(2);

        // Insert 4 - should evict 1 or 3 (not 2)
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

        // Reset visited
        for entry in &mut cache.entries {
            entry.visited = false;
        }

        // Mark entry 1 dirty
        cache.set_dirty(1);

        // Insert 3 - must evict 2 (not dirty 1)
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

        // Reset visited
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
}
