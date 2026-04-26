use rustc_hash::FxHashMap;

/// SIEVE eviction cache. FIFO queue with a moving eviction hand and a
/// visited bit. Dirty entries are pinned.
pub struct SieveCache<V> {
    entries: Vec<SieveEntry<V>>,
    /// Maps key -> index in entries vec.
    index: FxHashMap<u64, usize>,
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
            index: FxHashMap::with_capacity_and_hasher(capacity, Default::default()),
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
#[path = "sieve_tests.rs"]
mod tests;
