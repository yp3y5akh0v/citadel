//! Validated committed reclaim metadata, updated only after slot publication.

use std::collections::{hash_map::Entry, BTreeSet};
use std::ops::Bound::{Excluded, Included};

use super::*;

struct Segment {
    next: PageId,
    entries: Vec<PendingFreeEntry>,
    // New prefixes sort before every unchanged tail. Within one prefix the
    // larger ordinal is nearer its head; offsets preserve durable entry order.
    order: (TxnId, usize),
}

#[derive(Clone, Copy)]
struct EntryLocation {
    entry: PendingFreeEntry,
    segment: PageId,
    offset: usize,
    metadata: bool,
}

/// One exact committed slot's validated chain. No writer mutates it before
/// durable publication; unchanged segments and indexes remain in place.
pub(crate) struct CommittedReclaim {
    root: PageId,
    high_water_mark: u32,
    slot_txn: TxnId,
    segments: FxHashMap<PageId, Segment>,
    entries: FxHashMap<PageId, EntryLocation>,
    data_by_age: BTreeSet<(TxnId, PageId)>,
    metadata_by_age: BTreeSet<(TxnId, PageId)>,
    ready_data_through: TxnId,
    ready_metadata_through: TxnId,
    #[cfg(test)]
    initial_decoded_entries: usize,
}

impl Default for CommittedReclaim {
    fn default() -> Self {
        Self {
            root: PageId::INVALID,
            high_water_mark: 0,
            slot_txn: TxnId::ZERO,
            segments: FxHashMap::default(),
            entries: FxHashMap::default(),
            data_by_age: BTreeSet::new(),
            metadata_by_age: BTreeSet::new(),
            ready_data_through: TxnId::ZERO,
            ready_metadata_through: TxnId::ZERO,
            #[cfg(test)]
            initial_decoded_entries: 0,
        }
    }
}

enum Change {
    Prefix {
        retired: Vec<PageId>,
        added: Vec<(PageId, Segment)>,
    },
    Rewrite(Box<CommittedReclaim>),
}

pub(crate) struct PreparedReclaim {
    root: PageId,
    txn_id: TxnId,
    change: Change,
    removed: FxHashSet<PageId>,
    ready: ReadyPages,
    ready_data_through: TxnId,
    ready_metadata_through: TxnId,
    #[cfg(test)]
    work: ReclaimWork,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Entry visits in selected head, eligibility, and full-rewrite traversals.
/// This is a structural-work diagnostic, not a count of every copy, index
/// update, or sorting comparison. Prefix commits must not visit the backlog.
pub(crate) struct ReclaimWork {
    pub head_entries: usize,
    pub eligibility_entries: usize,
    /// Newly eligible unchanged-tail entries requiring durable-order sorting.
    pub readiness_sort_entries: usize,
    pub rewrite_entries: usize,
}

impl PreparedReclaim {
    pub(crate) fn root(&self) -> PageId {
        self.root
    }

    /// Seal newly staged pages when exposing a chain without a manager commit.
    /// The manager instead seals its complete dirty set after Merkle updates.
    pub(super) fn seal_staged_pages(&self, pages: &mut impl MutablePageMap) {
        let mut seal = |id| {
            pages
                .get_page_mut(&id)
                .expect("prepared reclaim page must remain staged")
                .update_checksum();
        };
        match &self.change {
            Change::Prefix { added, .. } => {
                for &(id, _) in added {
                    seal(id);
                }
            }
            Change::Rewrite(replacement) => {
                for &id in replacement.segments.keys() {
                    seal(id);
                }
            }
        }
    }

    /// Includes the unchanged loan remainder and only newly eligible entries.
    pub(crate) fn ready_pages(&self) -> ReadyPages {
        self.ready.clone()
    }

    #[cfg(test)]
    pub(crate) fn work(&self) -> ReclaimWork {
        self.work
    }
}

impl CommittedReclaim {
    pub(crate) fn read_committed<P: Deref<Target = Page>>(
        root: PageId,
        high_water_mark: u32,
        slot_txn: TxnId,
        mut load: impl FnMut(PageId) -> Result<P>,
    ) -> Result<Self> {
        let snapshot = ChainSnapshot::read_committed(root, high_water_mark, slot_txn, 0, |id| {
            if id.as_u32() >= high_water_mark {
                return Err(Error::PageOutOfBounds(id));
            }
            let page = load(id)?;
            if page.txn_id() > slot_txn {
                return Err(Error::DatabaseCorrupted);
            }
            Ok(page)
        })?;
        Ok(Self::from_snapshot(
            snapshot,
            high_water_mark,
            slot_txn,
            &FxHashMap::default(),
        ))
    }

    pub(super) fn from_snapshot(
        snapshot: ChainSnapshot,
        high_water_mark: u32,
        slot_txn: TxnId,
        metadata: &FxHashMap<PageId, TxnId>,
    ) -> Self {
        let ChainSnapshot {
            entries,
            page_ids,
            page_lengths,
            ..
        } = snapshot;
        let mut state = Self {
            root: page_ids.first().copied().unwrap_or(PageId::INVALID),
            high_water_mark,
            slot_txn,
            #[cfg(test)]
            initial_decoded_entries: entries.len(),
            ..Self::default()
        };
        let mut offset = 0;
        for (index, (&id, &len)) in page_ids.iter().zip(&page_lengths).enumerate() {
            let segment = Segment {
                next: page_ids.get(index + 1).copied().unwrap_or(PageId::INVALID),
                entries: entries[offset..offset + len].to_vec(),
                order: (slot_txn, page_ids.len() - index),
            };
            state.install_segment(id, segment, |entry| {
                metadata.get(&entry.page_id) == Some(&entry.freed_at_txn)
            });
            offset += len;
        }
        state
    }

    pub(crate) fn matches(&self, root: PageId, high_water_mark: u32, slot_txn: TxnId) -> bool {
        self.root == root && self.high_water_mark == high_water_mark && self.slot_txn == slot_txn
    }

    pub(crate) fn metadata_age(&self, id: PageId) -> Option<TxnId> {
        self.entries
            .get(&id)
            .filter(|entry| entry.metadata)
            .map(|entry| entry.entry.freed_at_txn)
    }

    /// Test snapshot; the manager commit path never clones provenance.
    #[cfg(test)]
    pub(crate) fn metadata_retirements(&self) -> FxHashMap<PageId, TxnId> {
        self.metadata_by_age
            .iter()
            .map(|&(age, id)| (id, age))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn initial_decoded_entries(&self) -> usize {
        self.initial_decoded_entries
    }

    #[cfg(test)]
    pub(crate) fn reset_ready_progress(&mut self) {
        self.ready_data_through = TxnId::ZERO;
        self.ready_metadata_through = TxnId::ZERO;
    }

    fn install_segment(
        &mut self,
        id: PageId,
        segment: Segment,
        metadata: impl Fn(&PendingFreeEntry) -> bool,
    ) {
        for (offset, &entry) in segment.entries.iter().enumerate() {
            match self.entries.entry(entry.page_id) {
                Entry::Occupied(mut occupied) => {
                    // Publication removes consumed IDs before installation.
                    // A surviving retirement keeps its age and classification;
                    // only its location changes when the prefix is repacked.
                    let old = occupied.get_mut();
                    debug_assert_eq!(old.entry, entry);
                    old.segment = id;
                    old.offset = offset;
                }
                Entry::Vacant(vacant) => {
                    let is_metadata = metadata(&entry);
                    let ages = if is_metadata {
                        &mut self.metadata_by_age
                    } else {
                        &mut self.data_by_age
                    };
                    ages.insert((entry.freed_at_txn, entry.page_id));
                    vacant.insert(EntryLocation {
                        entry,
                        segment: id,
                        offset,
                        metadata: is_metadata,
                    });
                }
            }
        }
        self.segments.insert(id, segment);
    }

    fn remove_entry(&mut self, id: PageId) {
        if let Some(entry) = self.entries.remove(&id) {
            let ages = if entry.metadata {
                &mut self.metadata_by_age
            } else {
                &mut self.data_by_age
            };
            ages.remove(&(entry.entry.freed_at_txn, id));
        }
    }

    fn chain_entries(&self) -> impl Iterator<Item = PendingFreeEntry> + '_ {
        let mut next = self.root;
        std::iter::from_fn(move || {
            if !next.is_valid() {
                return None;
            }
            let segment = &self.segments[&next];
            next = segment.next;
            Some(segment.entries.iter().copied())
        })
        .flatten()
    }

    fn chain_ids(&self) -> Vec<PageId> {
        let mut result = Vec::with_capacity(self.segments.len());
        let mut next = self.root;
        while next.is_valid() {
            result.push(next);
            next = self.segments[&next].next;
        }
        result
    }

    /// Keep loans and next-commit metadata together before reader-pinned data.
    /// This is bounded by the changed prefix unless a full rewrite is needed.
    fn pack_entries(
        &self,
        survivors: impl IntoIterator<Item = PendingFreeEntry>,
        retired: &[PageId],
        commit: &ChainCommit<'_>,
    ) -> (Vec<PendingFreeEntry>, usize) {
        let survivors = survivors.into_iter();
        // Filtered head slices retain their bounded upper length even though
        // their lower size hint is zero. Reserve once before packing them.
        let (minimum, maximum) = survivors.size_hint();
        let mut entries = Vec::with_capacity(
            maximum.unwrap_or(minimum) + retired.len() + commit.freed_this_txn.len(),
        );
        entries.extend(retired.iter().map(|&page_id| PendingFreeEntry {
            page_id,
            freed_at_txn: commit.txn_id,
        }));
        let mut pinned = Vec::new();
        for entry in survivors {
            if entry.freed_at_txn <= commit.reclaim_horizon
                || self.metadata_age(entry.page_id) == Some(entry.freed_at_txn)
            {
                entries.push(entry);
            } else {
                pinned.push(entry);
            }
        }
        let near_term = entries.len();
        entries.extend(
            commit
                .freed_this_txn
                .iter()
                .map(|&page_id| PendingFreeEntry {
                    page_id,
                    freed_at_txn: commit.txn_id,
                }),
        );
        entries.extend(pinned);
        (entries, near_term)
    }

    /// Use the minimum page count with a partial head. If a small remainder
    /// would split the near-term loans, enlarge only that head; the final page
    /// then holds the smaller remainder instead.
    fn stage_segments(
        pages: &mut impl MutablePageMap,
        txn_id: TxnId,
        entries: Vec<PendingFreeEntry>,
        structure: &[PageId],
        tail: PageId,
        near_term: usize,
    ) -> Vec<(PageId, Segment)> {
        if entries.is_empty() {
            debug_assert!(structure.is_empty());
            return Vec::new();
        }
        debug_assert_eq!(structure.len(), chain_pages_needed(entries.len()));
        if let &[id] = structure {
            pages.insert_page(id, build_chain_page(txn_id, id, tail, &entries));
            return vec![(
                id,
                Segment {
                    next: tail,
                    entries,
                    order: (txn_id, 1),
                },
            )];
        }
        let head_len = ((entries.len() - 1) % MAX_ENTRIES_PER_PAGE + 1)
            .max(near_term.min(MAX_ENTRIES_PER_PAGE));
        let (head, rest) = entries.split_at(head_len);
        std::iter::once(head)
            .chain(rest.chunks(MAX_ENTRIES_PER_PAGE))
            .zip(structure.iter().copied())
            .enumerate()
            .map(|(index, (chunk, id))| {
                let next = structure.get(index + 1).copied().unwrap_or(tail);
                pages.insert_page(id, build_chain_page(txn_id, id, next, chunk));
                (
                    id,
                    Segment {
                        next,
                        entries: chunk.to_vec(),
                        order: (txn_id, structure.len() - index),
                    },
                )
            })
            .collect()
    }

    /// Prepare writer-private pages/cursors, retaining the committed state on
    /// every error. Only loans published by an earlier commit may be structure.
    /// Stage writer-private pages without checksums. The manager seals all
    /// dirty pages before persistence; direct adapters must seal staged pages.
    pub(crate) fn prepare(
        &self,
        pages: &mut impl MutablePageMap,
        alloc: &mut PageAllocator,
        loans: &mut ReadyPages,
        commit: &ChainCommit<'_>,
    ) -> Result<PreparedReclaim> {
        if self.root != commit.current_root
            || alloc.ready_count() != 0
            || alloc.high_water_mark() < self.high_water_mark
            || commit.txn_id == TxnId::ZERO
            || commit.txn_id <= self.slot_txn
        {
            return Err(Error::DatabaseCorrupted);
        }
        if commit
            .consumed
            .iter()
            .any(|id| !self.entries.contains_key(id))
        {
            return Err(Error::DatabaseCorrupted);
        }
        let mut removed = commit.consumed.clone();
        // The persisted membership proof is extended only by valid deltas.
        // Reusing a consumed ID as a new retirement is valid; an unconsumed
        // duplicate, active structure ID, or duplicate current free is not.
        let mut new_frees = FxHashSet::default();
        for &id in commit.freed_this_txn {
            if !id.is_valid()
                || id.as_u32() >= alloc.high_water_mark()
                || !new_frees.insert(id)
                || self.segments.contains_key(&id)
                || (self.entries.contains_key(&id) && !removed.contains(&id))
            {
                return Err(Error::DatabaseCorrupted);
            }
        }
        let ready_data_through = self
            .ready_data_through
            .max(commit.reclaim_horizon.min(self.slot_txn));
        let ready_metadata_through = self.slot_txn;
        #[cfg(test)]
        let mut work = ReclaimWork::default();
        let mut candidate_loans = loans.clone();
        let loan_cursor = &mut candidate_loans;
        let head = self.segments.get(&self.root);
        let replacement_head = head.filter(|head| {
            if commit.consumed.is_empty()
                || !commit.consumed.iter().all(|id| {
                    self.entries
                        .get(id)
                        .is_some_and(|entry| entry.segment == self.root)
                })
            {
                return false;
            }
            if let Some(replacement) = loan_cursor.last() {
                !removed.contains(&replacement)
                    && self
                        .entries
                        .get(&replacement)
                        .is_some_and(|entry| entry.segment == self.root)
                    && head.entries.len() - commit.consumed.len() + commit.freed_this_txn.len()
                        <= MAX_ENTRIES_PER_PAGE
            } else {
                // A body allocation may consume the last loan. Fresh pages
                // can replace or split this bounded head without rewriting
                // the pinned tail merely to obtain structure storage.
                true
            }
        });

        let (root, change, rewrite_ready) = if commit.consumed.is_empty()
            && (commit.freed_this_txn.is_empty() || loan_cursor.is_empty())
        {
            let replace_head = head.is_some() && !commit.freed_this_txn.is_empty();
            let retired: Vec<_> = if replace_head {
                vec![self.root]
            } else {
                Vec::new()
            };
            let survivors = if replace_head {
                #[cfg(test)]
                {
                    work.head_entries += head.unwrap().entries.len();
                }
                head.unwrap().entries.as_slice()
            } else {
                &[]
            };
            let tail = if replace_head {
                head.unwrap().next
            } else {
                self.root
            };
            let (prefix, near_term) =
                self.pack_entries(survivors.iter().copied(), &retired, commit);
            let mut structure = (0..chain_pages_needed(prefix.len()))
                .map(|_| alloc.allocate())
                .collect::<Result<Vec<_>>>()?;
            structure.reverse();
            let added =
                Self::stage_segments(pages, commit.txn_id, prefix, &structure, tail, near_term);
            let root = structure.first().copied().unwrap_or(tail);
            (root, Change::Prefix { retired, added }, None)
        } else if let Some(head) = replacement_head {
            let replacement = loan_cursor.pop();
            if let Some(id) = replacement {
                removed.insert(id);
            }
            #[cfg(test)]
            {
                work.head_entries += head.entries.len();
            }
            let survivors = head
                .entries
                .iter()
                .filter(|entry| !removed.contains(&entry.page_id))
                .copied();
            let (prefix, near_term) = self.pack_entries(survivors, &[self.root], commit);
            let structure = if let Some(id) = replacement {
                vec![id]
            } else {
                (0..chain_pages_needed(prefix.len()))
                    .map(|_| alloc.allocate())
                    .collect::<Result<Vec<_>>>()?
            };
            let added = Self::stage_segments(
                pages,
                commit.txn_id,
                prefix,
                &structure,
                head.next,
                near_term,
            );
            (
                structure[0],
                Change::Prefix {
                    retired: vec![self.root],
                    added,
                },
                None,
            )
        } else {
            let ids = self.chain_ids();
            #[cfg(test)]
            {
                work.rewrite_entries += self.entries.len();
            }
            let mut entries: Vec<_> = self
                .chain_entries()
                .filter(|entry| !removed.contains(&entry.page_id))
                .collect();
            #[cfg(test)]
            {
                work.rewrite_entries += entries.len();
            }
            let mut indices: FxHashMap<_, _> = entries
                .iter()
                .enumerate()
                .map(|(i, entry)| (entry.page_id, i))
                .collect();
            let new_count = ids.len() + commit.freed_this_txn.len();
            let mut structure = Vec::new();
            let mut taken = Vec::new();
            while structure.len() < chain_pages_needed(entries.len() + new_count) {
                let Some(id) = loan_cursor.pop() else {
                    break;
                };
                let index = indices.remove(&id).ok_or(Error::DatabaseCorrupted)?;
                taken.push(entries.swap_remove(index));
                if let Some(moved) = entries.get(index) {
                    *indices.get_mut(&moved.page_id).unwrap() = index;
                }
                structure.push(id);
            }
            while structure.len() > chain_pages_needed(entries.len() + new_count) {
                let id = structure.pop().unwrap();
                let entry = taken.pop().unwrap();
                indices.insert(id, entries.len());
                entries.push(entry);
                loan_cursor.push(id);
            }
            for &id in &structure {
                removed.insert(id);
            }
            while structure.len() < chain_pages_needed(entries.len() + new_count) {
                structure.push(alloc.allocate()?);
            }
            #[cfg(test)]
            let surviving_len = entries.len();
            #[cfg(test)]
            {
                work.rewrite_entries += entries.len();
            }
            let mut metadata: FxHashMap<_, _> = entries
                .iter()
                .filter_map(|entry| {
                    self.metadata_age(entry.page_id)
                        .map(|age| (entry.page_id, age))
                })
                .collect();
            metadata.extend(ids.iter().map(|&id| (id, commit.txn_id)));
            let (entries, near_term) = self.pack_entries(entries, &ids, commit);
            let eligible = entries[ids.len()..near_term]
                .iter()
                .map(|entry| entry.page_id)
                .collect();
            #[cfg(test)]
            {
                work.rewrite_entries += 3 * entries.len() + surviving_len;
            }
            let root = structure.first().copied().unwrap_or(PageId::INVALID);
            let added = Self::stage_segments(
                pages,
                commit.txn_id,
                entries,
                &structure,
                PageId::INVALID,
                near_term,
            );
            let mut replacement = Self {
                root,
                high_water_mark: alloc.high_water_mark(),
                slot_txn: commit.txn_id,
                ..Self::default()
            };
            for (id, segment) in added {
                replacement.install_segment(id, segment, |entry| {
                    metadata.get(&entry.page_id) == Some(&entry.freed_at_txn)
                });
            }
            (
                root,
                Change::Rewrite(Box::new(replacement)),
                Some(ReadyPages::from_pop_order(eligible)),
            )
        };

        let ready = if let Some(ready) = rewrite_ready {
            ready
        } else {
            let mut eligibility_advanced = false;
            let mut new_tail = Vec::new();
            for (ages, previous, through) in [
                (
                    &self.data_by_age,
                    self.ready_data_through,
                    ready_data_through,
                ),
                (
                    &self.metadata_by_age,
                    self.ready_metadata_through,
                    ready_metadata_through,
                ),
            ] {
                if previous < through {
                    for &(_, id) in ages.range((
                        Excluded((previous, PageId(u32::MAX))),
                        Included((through, PageId(u32::MAX))),
                    )) {
                        #[cfg(test)]
                        {
                            work.eligibility_entries += 1;
                        }
                        if !removed.contains(&id) {
                            eligibility_advanced = true;
                            let location = &self.entries[&id];
                            if location.segment != self.root {
                                // Resolve each unchanged-tail rank once, outside
                                // the sort comparator. Head entries already have
                                // their candidate order in the staged prefix.
                                new_tail.push((
                                    self.segments[&location.segment].order,
                                    location.offset,
                                    id,
                                ));
                            }
                        }
                    }
                }
            }
            // Initial load/reset has not issued readiness from these indexes.
            // Rebuild that first candidate once instead of duplicating an
            // existing caller loan remainder when a cache was revalidated.
            let mut ready = if self.ready_metadata_through == TxnId::ZERO {
                ReadyPages::default()
            } else {
                loan_cursor.clone()
            };
            if eligibility_advanced {
                // Keep already eligible head loans before newly eligible tail
                // data. Only the bounded old-head prefix is detached; its
                // immutable remainder is shared without enumeration.
                while let Some(id) = ready.last() {
                    if self
                        .entries
                        .get(&id)
                        .is_none_or(|entry| entry.segment != self.root)
                    {
                        break;
                    }
                    #[cfg(test)]
                    {
                        work.head_entries += 1;
                    }
                    ready.pop();
                }

                let added = match &change {
                    Change::Prefix { added, .. } => added.as_slice(),
                    Change::Rewrite(_) => unreachable!("rewrites already prepared readiness"),
                };
                let unchanged_head = head.filter(|_| added.is_empty());
                let prefix = added
                    .iter()
                    .flat_map(|(_, segment)| &segment.entries)
                    .chain(
                        unchanged_head
                            .into_iter()
                            .flat_map(|segment| &segment.entries),
                    );
                let mut eligible = Vec::with_capacity(
                    head.map_or(0, |segment| segment.entries.len()) + new_tail.len(),
                );
                // Prefix construction keeps surviving old entries and current
                // retirements; consumed old entries were removed before packing.
                // Both readiness bounds are at most the old slot, strictly below
                // the current transaction. New frees, refrees, and retired
                // structure therefore fail the age bounds. Only metadata beyond
                // the data horizon needs an exact retirement-identity lookup.
                for entry in prefix {
                    #[cfg(test)]
                    {
                        work.head_entries += 1;
                    }
                    if entry.freed_at_txn <= ready_data_through
                        || (entry.freed_at_txn <= ready_metadata_through
                            && self.metadata_age(entry.page_id) == Some(entry.freed_at_txn))
                    {
                        eligible.push(entry.page_id);
                    }
                }
                #[cfg(test)]
                {
                    work.readiness_sort_entries += new_tail.len();
                }
                new_tail
                    .sort_unstable_by_key(|&(order, offset, _)| (std::cmp::Reverse(order), offset));
                eligible.extend(new_tail.into_iter().map(|(_, _, id)| id));
                ready.prepend_pop_order(eligible);
            }
            ready
        };
        *loans = candidate_loans;
        Ok(PreparedReclaim {
            root,
            txn_id: commit.txn_id,
            change,
            removed,
            ready,
            ready_data_through,
            ready_metadata_through,
            #[cfg(test)]
            work,
        })
    }

    /// Return only eligible, unconsumed entries newer than the two erasure
    /// watermarks. The strict old-slot bound retains inactive-slot protection.
    pub(crate) fn zero_candidates(
        &self,
        prepared: &PreparedReclaim,
        data_watermark: TxnId,
        metadata_watermark: TxnId,
    ) -> Vec<(PendingFreeEntry, bool)> {
        let mut result = Vec::new();
        for (ages, watermark, through, metadata) in [
            (
                &self.data_by_age,
                data_watermark,
                prepared.ready_data_through,
                false,
            ),
            (
                &self.metadata_by_age,
                metadata_watermark,
                prepared.ready_metadata_through,
                true,
            ),
        ] {
            if watermark < through {
                for &(age, id) in ages.range((
                    Excluded((watermark, PageId(u32::MAX))),
                    Included((through, PageId(u32::MAX))),
                )) {
                    if age < self.slot_txn && !prepared.removed.contains(&id) {
                        result.push((
                            PendingFreeEntry {
                                page_id: id,
                                freed_at_txn: age,
                            },
                            metadata,
                        ));
                    }
                }
            }
        }
        result
    }

    pub(crate) fn publish(&mut self, prepared: PreparedReclaim, high_water_mark: u32) {
        match prepared.change {
            Change::Rewrite(replacement) => *self = *replacement,
            Change::Prefix { retired, added } => {
                for id in prepared.removed {
                    self.remove_entry(id);
                }
                for id in &retired {
                    self.segments.remove(id);
                }
                for (id, segment) in added {
                    self.install_segment(id, segment, |entry| {
                        entry.freed_at_txn == prepared.txn_id && retired.contains(&entry.page_id)
                    });
                }
            }
        }
        self.root = prepared.root;
        self.slot_txn = prepared.txn_id;
        self.high_water_mark = high_water_mark;
        self.ready_data_through = prepared.ready_data_through;
        self.ready_metadata_through = prepared.ready_metadata_through;
    }

    // The public Vec-returning API materializes its requested output here;
    // manager commits use ready_pages and never enumerate the remainder.
    pub(super) fn available_after(
        &self,
        prepared: &PreparedReclaim,
        horizon: TxnId,
    ) -> Vec<PendingFreeEntry> {
        let eligible = |entry: &PendingFreeEntry| {
            !prepared.removed.contains(&entry.page_id)
                && (entry.freed_at_txn <= horizon
                    || self.metadata_age(entry.page_id) == Some(entry.freed_at_txn))
        };
        match &prepared.change {
            Change::Rewrite(replacement) => replacement
                .chain_entries()
                .filter(|entry| {
                    self.entries
                        .get(&entry.page_id)
                        .is_some_and(|old| old.entry.freed_at_txn == entry.freed_at_txn)
                        && eligible(entry)
                })
                .collect(),
            Change::Prefix { .. } => self.chain_entries().filter(eligible).collect(),
        }
    }
}

#[cfg(test)]
#[path = "committed_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "committed_protocol_tests.rs"]
mod protocol_tests;
