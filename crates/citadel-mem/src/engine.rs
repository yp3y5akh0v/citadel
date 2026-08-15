//! MemoryEngine: region lifecycle on top of citadel's encrypted SQL store.

use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use rustc_hash::{FxHashMap, FxHashSet};
use sha2::{Digest, Sha256};

use citadel::{Database, KeyLifecycleGuard};
use citadel_core::WRAPPED_KEY_SIZE;
use citadel_crypto::blob_seal;
use citadel_crypto::hkdf_utils::{
    derive_atom_wrap_key, derive_identity_mac_key, derive_seal_keys, AtomWrapKey, IdentityMacKey,
};
use citadel_sql::executor::{AnnIndexSource, AnnSegmentInfo};
use citadel_sql::{Connection, ExecutionResult, Value};
use citadel_vector::{AnnIndex, Filter, Metric};
use zeroize::{Zeroize, Zeroizing};

use crate::embed::{Embedder, EmbeddingMetric, Reranker};
use crate::error::{MemError, Result};
use crate::fusion::{fuse_rank, fuse_rerank, rerank_hits, rrf_merge, Candidate};
use crate::types::{
    AtomAttestation, AtomHit, AtomId, AtomInput, AttestVerdict, Edge, EdgeKind, ErasureReceipt,
    EvictionPolicy, EvictionReport, EvolutionReport, FetchQuery, FusionWeights, GraphExpand,
    KindDigest, MultiRecallQuery, RecallQuery, RememberOutcome, RerankStrategy, SlotErasure,
    SourceSnapshot, StoredAtomRetrievalState, StoredEmbeddingsIdentity, StoredRegionIdentity,
    SummaryReport, ERASURE_SCOPE_CAVEAT, STORED_EMBEDDINGS_SCHEMA,
};
use citadel::SlotState;

/// Batch size for encrypted decrypt scans; no ANN/FTS index over ciphertext.
const EXACT_SCAN_LIMIT: usize = 4096;

/// Over-fetch factor for ANN candidates before fusion re-ranking.
const CAND_OVERFETCH: usize = 8;
/// Floor on ANN candidates evaluated (small-k recall stability).
const MIN_CANDIDATES: usize = 64;
/// Min ANN candidates over-fetched before fusion on the plaintext path.
const MIN_OVERFETCH: usize = 4096;

#[cfg(test)]
std::thread_local! {
    /// Test fault point: sealed segment key + chunk table must be reclaimed on failure.
    static FAIL_SEALED_SEGMENT_AFTER_CHUNKS: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    /// Thread-local so parallel unit tests cannot consume another test's fault.
    static FAIL_ENCRYPTED_REGION_AFTER_SLOT: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    /// Fault after tombstone + retire, before row deletes: epoch armed, reopen converges.
    static FAIL_ERASE_BEFORE_ROW_DELETE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    /// Fault after the cascade's segment retire, before its txn: retire precedes commit.
    static FAIL_CASCADE_BEFORE_TXN: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    /// Fault after reconcile's drift-key tombstones, before row deletes: keys die first.
    static FAIL_RECONCILE_AFTER_DRIFT_KEYS: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    /// Fault at the segment-probe entry: reconcile propagates the error, destroys nothing.
    static FAIL_SEGMENT_PROBE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static FAILED_ENCRYPTED_REGION_WRAPPED_KEY:
        std::cell::RefCell<Option<Zeroizing<[u8; WRAPPED_KEY_SIZE]>>> =
            const { std::cell::RefCell::new(None) };
}

/// Stable identifier for a memory region (row id in `memory_regions`).
pub type RegionId = i64;

/// Rolls back not-yet-durable key allocations through the retained span; no live orphans.
struct PendingAtomSlots<'a> {
    kl: Option<&'a KeyLifecycleGuard<'a>>,
    slots: Vec<(u32, u64, u64)>,
    armed: bool,
}

impl<'a> PendingAtomSlots<'a> {
    fn new(kl: Option<&'a KeyLifecycleGuard<'a>>, capacity: usize) -> Self {
        Self {
            kl,
            // Reserve first: tracking stays alloc-free so an OOM unwind reaches Drop.
            slots: Vec::with_capacity(capacity),
            armed: true,
        }
    }

    fn track(&mut self, slot: u32, owner: u64, generation: u64) {
        assert!(
            self.kl.is_some(),
            "atom key allocations happen only inside a sealed lifecycle span"
        );
        self.slots.push((slot, owner, generation));
    }

    fn finish<T>(mut self, result: Result<T>) -> Result<T> {
        match result {
            Ok(value) => {
                self.armed = false;
                Ok(value)
            }
            Err(source) => {
                if self.slots.is_empty() {
                    self.armed = false;
                    return Err(source);
                }
                let kl = self.kl.expect("track() asserted the capability");
                match kl.atom_store_tombstone_batch(&self.slots) {
                    Ok(_) => {
                        self.armed = false;
                        Err(source)
                    }
                    Err(rollback) => Err(MemError::Invalid(format!(
                        "{source}; additionally failed to tombstone pending atom keys: {rollback}"
                    ))),
                }
            }
        }
    }
}

impl Drop for PendingAtomSlots<'_> {
    fn drop(&mut self) {
        if self.armed && !self.slots.is_empty() {
            if let Some(kl) = self.kl {
                let _ = kl.atom_store_tombstone_batch(&self.slots);
            }
        }
    }
}

/// Region-key slot durable before its row commits; rolled back via the retained span.
struct PendingRegionSlot<'a> {
    kl: &'a KeyLifecycleGuard<'a>,
    binding: Option<(u32, u64)>,
}

impl<'a> PendingRegionSlot<'a> {
    fn new(kl: &'a KeyLifecycleGuard<'a>, slot: u32, owner: u64) -> Self {
        Self {
            kl,
            binding: Some((slot, owner)),
        }
    }

    fn finish<T>(mut self, result: Result<T>) -> Result<T> {
        match result {
            Ok(value) => {
                self.binding = None;
                Ok(value)
            }
            Err(source) => {
                let (slot, owner) = self
                    .binding
                    .expect("pending region slot is armed until finish succeeds");
                match self.kl.region_store_tombstone(slot, owner) {
                    Ok(()) => {
                        self.binding = None;
                        Err(source)
                    }
                    Err(rollback) => Err(MemError::Invalid(format!(
                        "{source}; additionally failed to tombstone pending region key: {rollback}"
                    ))),
                }
            }
        }
    }
}

impl Drop for PendingRegionSlot<'_> {
    fn drop(&mut self) {
        if let Some((slot, owner)) = self.binding {
            let _ = self.kl.region_store_tombstone(slot, owner);
        }
    }
}

/// Both per-region secrets derived from one RCK unwrap.
struct RegionKeys {
    atom_wrap: Arc<AtomWrapKey>,
    identity_mac: Arc<IdentityMacKey>,
}

/// A region attached to a live embedder in this process.
struct RegionState {
    id: RegionId,
    dim: u16,
    metric: EmbeddingMetric,
    embedder: Arc<dyn Embedder>,
    /// Encrypted regions: wraps/unwraps each atom's ACK (derived from the RCK).
    atom_wrap: Option<Arc<AtomWrapKey>>,
    /// Encrypted regions: keyed MAC for identity tags (own HKDF label).
    identity_mac: Option<Arc<IdentityMacKey>>,
    /// Lazy in-RAM ANN index over decrypted vectors for sealed recall.
    ann: Arc<RwLock<Option<SealedAnn>>>,
    /// Highest atom id; sealed recall reads it to detect post-snapshot inserts
    /// without a per-call `MAX(id)` scan.
    max_id: Arc<AtomicI64>,
}

/// Region fields needed off-lock by remember/recall.
struct RegionHandle {
    id: RegionId,
    table: String,
    embedder: Arc<dyn Embedder>,
    dim: u16,
    metric: EmbeddingMetric,
    atom_wrap: Option<Arc<AtomWrapKey>>,
    identity_mac: Option<Arc<IdentityMacKey>>,
    ann: Arc<RwLock<Option<SealedAnn>>>,
    max_id: Arc<AtomicI64>,
}

/// Lazy per-region in-RAM PRISM index over decrypted vectors; zeroized on drop
/// so they never outlive the region key. May persist as a sealed segment under
/// its own erasable key, so destroying that slot crypto-erases the SQ8 codes.
struct SealedAnn {
    index: AnnIndex,
    /// Atom `kind` -> PRISM attribute code, for kind-filtered recall.
    kind_codes: FxHashMap<String, u32>,
    /// Rank inputs cached at build so the hot path skips re-fetch/decrypt;
    /// plaintext, zeroized on drop with the index vectors.
    cached: FxHashMap<AtomId, CachedAtom>,
    /// Persisted sealed segment or a scan build.
    source: AnnIndexSource,
    /// [`Database::cache_epoch`] at build; once it moves, stale plaintext is refused.
    build_epoch: u64,
}

/// Per-atom fields a sealed recall needs, decrypted once at index build.
struct CachedAtom {
    kind: String,
    text: String,
    payload: serde_json::Value,
    importance: f32,
    created_micros: i64,
    immutable: bool,
    /// TTL lapse instant; recall skips the atom once wall-clock passes it.
    expires_micros: Option<i64>,
}

impl Drop for CachedAtom {
    fn drop(&mut self) {
        self.text.zeroize();
        zeroize_json_strings(&mut self.payload);
    }
}

/// Nulls every owned string (keys included); returns count so tests prove recursion.
fn zeroize_json_strings(value: &mut serde_json::Value) -> usize {
    let mut scrubbed = 0;
    match value {
        serde_json::Value::String(text) => {
            text.zeroize();
            scrubbed += 1;
        }
        serde_json::Value::Array(values) => {
            for value in values.iter_mut() {
                scrubbed += zeroize_json_strings(value);
            }
            values.clear();
        }
        serde_json::Value::Object(fields) => {
            for (mut key, mut value) in std::mem::take(fields) {
                key.zeroize();
                scrubbed += 1;
                scrubbed += zeroize_json_strings(&mut value);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
    *value = serde_json::Value::Null;
    scrubbed
}

fn zeroize_atom_content(text: &mut String, payload: &mut serde_json::Value) {
    text.zeroize();
    zeroize_json_strings(payload);
}

/// Map the memory metric to PRISM's distance metric.
fn ann_metric(m: EmbeddingMetric) -> Metric {
    match m {
        EmbeddingMetric::Cosine => Metric::Cosine,
        EmbeddingMetric::L2 => Metric::L2,
        EmbeddingMetric::InnerProduct => Metric::InnerProduct,
    }
}

/// Encrypted-first memory engine over a shared [`Database`].
pub struct MemoryEngine {
    db: Arc<Database>,
    regions: Mutex<FxHashMap<String, RegionState>>,
    /// Cross-encoder + strategy applied in `recall` before truncation (`None`
    /// = linear fusion). Snapshotted before the callback so the guard is not
    /// held across a re-entrant reranker.
    reranker: RwLock<Option<(Arc<dyn Reranker>, RerankStrategy)>>,
    /// Per-table `region_id -> MAX(id)` re-attach snapshot, tagged with the
    /// commit generation (any commit invalidates it): one grouped scan for R
    /// regions instead of R full scans.
    attach_max: Mutex<FxHashMap<String, AttachMaxSnapshot>>,
    /// In-process read tracking feeding the `Lru`/`Stale` eviction policies.
    /// Reads stay write-free, so this is engine-lifetime state layered over the
    /// persisted insert-time floor.
    access_stats: Mutex<FxHashMap<RegionId, RegionAccessStats>>,
}

/// Re-attach `MAX(id)` snapshot for one atoms table: `(commit generation at
/// scan, region_id -> MAX(id))`.
type AttachMaxSnapshot = (u64, FxHashMap<RegionId, i64>);

/// One region's in-process read hits: `atom -> (last access micros, count)`.
type RegionAccessStats = FxHashMap<AtomId, (i64, u32)>;

const BOOTSTRAP_SQL: &str = "\
CREATE TABLE IF NOT EXISTS memory_meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS memory_regions (\
 id INTEGER PRIMARY KEY,\
 name TEXT UNIQUE NOT NULL,\
 embedding_dim INTEGER NOT NULL,\
 embedding_metric TEXT NOT NULL,\
 model_id TEXT NOT NULL,\
 encrypted INTEGER NOT NULL DEFAULT 0,\
 rsk_slot INTEGER,\
 rsk_gen INTEGER,\
 created_at TIMESTAMP NOT NULL,\
 metadata JSONB);
CREATE TABLE IF NOT EXISTS memory_edges (\
 src_id INTEGER NOT NULL,\
 dst_id INTEGER NOT NULL,\
 kind TEXT NOT NULL,\
 weight REAL DEFAULT 1.0,\
 evidence_ref JSONB,\
 PRIMARY KEY (src_id, dst_id, kind));
CREATE TABLE IF NOT EXISTS memory_idempotency (\
 region_id INTEGER NOT NULL,\
 kind TEXT NOT NULL,\
 key_mac TEXT NOT NULL,\
 request_mac TEXT NOT NULL,\
 atom_id INTEGER NOT NULL,\
 PRIMARY KEY (region_id, kind, key_mac));
CREATE UNIQUE INDEX IF NOT EXISTS memory_idempotency_atom ON memory_idempotency (atom_id);
INSERT INTO memory_meta (key, value) VALUES ('next_region_id', 1) ON CONFLICT (key) DO NOTHING;
INSERT INTO memory_meta (key, value) VALUES ('next_atom_id', 1) ON CONFLICT (key) DO NOTHING;";

impl MemoryEngine {
    /// Open the engine over a database, creating catalog tables if absent.
    pub fn open(db: Arc<Database>) -> Result<Self> {
        {
            let conn = Connection::open(&db)?;
            if let Some(e) = conn.execute_script(BOOTSTRAP_SQL).error {
                return Err(e.into());
            }
            // Reject a legacy pre-erasure schema (memory_regions lacks the
            // `encrypted` column).
            let has_encrypted_col = conn
                .table_schema("memory_regions")
                .is_some_and(|s| s.columns.iter().any(|c| c.name == "encrypted"));
            if !has_encrypted_col {
                return Err(MemError::Invalid(
                    "incompatible memory schema (pre-region-erasure): recreate the database \
                     or export and reimport its memories"
                        .into(),
                ));
            }
        } // drop the connection's borrow before moving `db`
        let engine = Self {
            db,
            regions: Mutex::new(FxHashMap::default()),
            reranker: RwLock::new(None),
            attach_max: Mutex::new(FxHashMap::default()),
            access_stats: Mutex::new(FxHashMap::default()),
        };
        if engine.db.region_keys_enabled() && engine.db.region_store_path().exists() {
            engine.reconcile_region_store()?;
        }
        if engine.db.region_keys_enabled() && engine.db.atom_store_path().exists() {
            engine.reconcile_atom_store()?;
        }
        engine.reconcile_identity_records()?;
        Ok(engine)
    }

    /// Reclaim slots left live by an interrupted create or bound at a drifted generation.
    fn reconcile_region_store(&self) -> Result<()> {
        // Serialize against in-flight allocate->commit spans on other handles.
        let _kl = self.db.key_lifecycle_lock();
        let live = self.db.region_store_live_bindings()?;
        if live.is_empty() {
            return Ok(());
        }
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            "SELECT id, rsk_slot, rsk_gen FROM memory_regions \
             WHERE encrypted = 1 AND rsk_slot IS NOT NULL",
            &[],
        )?;
        // (slot, owner) -> required gen; a gen-less row fails closed and is reclaimed.
        let mut valid: FxHashMap<(u32, u64), u64> = FxHashMap::default();
        for row in &qr.rows {
            // Checked domains: out-of-range values never truncate into another binding.
            let owner = as_int(&row[0]).ok().and_then(|v| u64::try_from(v).ok());
            let slot = as_int(&row[1]).ok().and_then(|v| u32::try_from(v).ok());
            if let ((Some(owner), Some(slot)), Some(gen)) = ((owner, slot), opt_u64(&row[2])) {
                valid.insert((slot, owner), gen);
            }
        }
        for (slot, owner, gen) in live {
            if valid.get(&(slot, owner)) != Some(&gen) {
                _kl.region_store_tombstone(slot, owner)?;
            }
        }
        Ok(())
    }

    /// Two-way reconcile: tombstone live key slots no committed row references
    /// (interrupted insert), and finish interrupted erasures by deleting rows
    /// whose key is already dead (keys die first, so a crash leaves
    /// undecryptable rows to clean up here).
    fn reconcile_atom_store(&self) -> Result<()> {
        // Serialize against in-flight allocate->commit spans on other handles.
        let _kl = self.db.key_lifecycle_lock();
        // Phase 0 - normalize torn erases before any row or slot binding is forgotten.
        _kl.normalize_atom_store_torn_erases()?;
        let conn = Connection::open(&self.db)?;

        // Phase 1 - inventory only; a live slot attests its exact (owner, gen) alone.
        let live = self.db.atom_store_live_bindings()?;
        let live_gens: FxHashMap<(u32, u64), u64> =
            live.iter().map(|&(s, o, g)| ((s, o), g)).collect();

        // Valid parent = region row whose key binding is exactly live (attachment's bar).
        let region_live: FxHashMap<(u32, u64), u64> = if self.db.region_store_path().exists() {
            self.db
                .region_store_live_bindings()?
                .into_iter()
                .map(|(slot, owner, gen)| ((slot, owner), gen))
                .collect()
        } else {
            FxHashMap::default()
        };
        let regions = conn.query_params(
            "SELECT id, embedding_dim, embedding_metric, rsk_slot, rsk_gen \
             FROM memory_regions WHERE encrypted = 1",
            &[],
        )?;
        let mut region_tables: FxHashMap<RegionId, String> = FxHashMap::default();
        for row in &regions.rows {
            let dim = u16::try_from(as_int(&row[1])?)
                .map_err(|_| MemError::Invalid("stored embedding_dim out of range".into()))?;
            let metric = metric_from_str(as_text(&row[2])?)?;
            let id = as_int(&row[0])?;
            let bound = as_int(&row[3])
                .ok()
                .and_then(|v| u32::try_from(v).ok())
                .zip(u64::try_from(id).ok())
                .and_then(|key| region_live.get(&key))
                .is_some_and(|&gen| opt_u64(&row[4]) == Some(gen));
            if bound {
                region_tables.insert(id, atoms_table(dim, metric, true));
            }
        }
        // Catalog-driven table list: rows of a removed parent region still scan.
        let enc_tables: FxHashSet<String> = conn
            .tables()
            .into_iter()
            .filter(|name| is_encrypted_atoms_table(name))
            .collect();

        // Valid row = exact live binding AND a parent homed in this table; else orphan.
        let mut row_claims: FxHashSet<(u32, u64)> = FxHashSet::default();
        let mut valid_ids: FxHashSet<AtomId> = FxHashSet::default();
        let mut table_orphans: Vec<(String, Vec<AtomId>)> = Vec::new();
        let mut orphan_rids: FxHashSet<RegionId> = FxHashSet::default();
        for table in &enc_tables {
            let qr = conn.query_params(
                &format!("SELECT id, key_slot, region_id, key_gen FROM {table}"),
                &[],
            )?;
            let mut orphans: Vec<AtomId> = Vec::new();
            for r in &qr.rows {
                let id = as_int(&r[0])?;
                let rid = as_int(&r[2])?;
                // Out-of-range slot/gen/id is an orphan, never a truncated alias.
                let slot = as_int(&r[1]).ok().and_then(|v| u32::try_from(v).ok());
                let owner = u64::try_from(id).ok();
                let gen = as_int(&r[3]).ok().and_then(|v| u64::try_from(v).ok());
                match slot.zip(owner).zip(gen) {
                    Some(((slot, owner), gen))
                        if live_gens.get(&(slot, owner)) == Some(&gen)
                            && region_tables.get(&rid) == Some(table) =>
                    {
                        row_claims.insert((slot, owner));
                        valid_ids.insert(id);
                    }
                    _ => {
                        orphans.push(id);
                        orphan_rids.insert(rid);
                    }
                }
            }
            if !orphans.is_empty() {
                table_orphans.push((table.clone(), orphans));
            }
        }

        // Segments from metadata AND the physical catalog, so parentless trees are found.
        let qr = conn.query_params("SELECT key FROM memory_meta WHERE key LIKE 'annseg_%'", &[])?;
        let mut seg_rids: FxHashSet<RegionId> = FxHashSet::default();
        for row in &qr.rows {
            if let Some((_, region)) = as_text(&row[0])?.split_once(':') {
                if let Ok(rid) = region.parse::<RegionId>() {
                    seg_rids.insert(rid);
                }
            }
        }
        let mut inv_trees: Vec<(RegionId, String)> = Vec::new();
        for name in self.db.table_names()? {
            let Ok(name) = String::from_utf8(name) else {
                continue;
            };
            if let Some((rid, _)) = parse_sealed_segment_table(&name) {
                seg_rids.insert(rid);
                inv_trees.push((rid, name));
            }
        }
        let mut seg_cleanup: Vec<(RegionId, Option<String>)> = Vec::new();
        let mut seg_claims: FxHashSet<(u32, u64)> = FxHashSet::default();
        let mut valid_seg_trees: FxHashMap<RegionId, String> = FxHashMap::default();
        for &rid in &seg_rids {
            let seg_table = region_tables
                .get(&rid)
                .filter(|table| enc_tables.contains(*table))
                .map(|table| sealed_segment_table(table, rid));
            let meta = read_annseg_meta(&conn, rid)?;
            let is_valid = match (&meta, &seg_table) {
                (Some((slot, gen, id)), Some(seg_table)) => {
                    let key = (*slot, *id as u64);
                    // Single-owner: colliding metadata loses; the atom's record stays.
                    !row_claims.contains(&key)
                        && !seg_claims.contains(&key)
                        && !orphan_rids.contains(&rid)
                        && live_gens.get(&key) == Some(gen)
                        && self.sealed_segment_tree_state(seg_table)? == SegmentTreeState::Present
                }
                _ => false,
            };
            if is_valid {
                let (slot, _, id) = meta.expect("validity requires complete metadata");
                seg_claims.insert((slot, id as u64));
                valid_seg_trees.insert(rid, seg_table.expect("validity requires a known table"));
            } else {
                seg_cleanup.push((rid, seg_table));
            }
        }

        // Phase 2 - unclaimed live keys die FIRST, before any derived data or row.
        let doomed: Vec<(u32, u64, u64)> = live
            .iter()
            .copied()
            .filter(|&(slot, owner, _)| {
                !row_claims.contains(&(slot, owner)) && !seg_claims.contains(&(slot, owner))
            })
            .collect();
        if !doomed.is_empty() {
            _kl.atom_store_tombstone_batch(&doomed)?;
        }
        #[cfg(test)]
        if FAIL_RECONCILE_AFTER_DRIFT_KEYS.with(std::cell::Cell::take) {
            return Err(MemError::Invalid(
                "injected reconcile failure after key destruction".into(),
            ));
        }

        // Phase 3 - trees first, meta only after, so an interrupted cleanup can retry.
        for (rid, seg_table) in &seg_cleanup {
            for (_, name) in inv_trees.iter().filter(|(tree_rid, _)| tree_rid == rid) {
                self.drop_segment_tree(name)?;
            }
            if let Some(name) = seg_table {
                self.drop_segment_tree(name)?;
            }
            clear_annseg_meta(&conn, *rid)?;
        }
        // A stale foreign tree under a valid segment's region id is residue.
        for (rid, name) in &inv_trees {
            if valid_seg_trees
                .get(rid)
                .is_some_and(|expected| expected != name)
            {
                self.drop_segment_tree(name)?;
            }
        }

        // Phase 4 - rows; a forged duplicate id loses only its row, the graph survives.
        for (table, orphan_ids) in &table_orphans {
            let dead: Vec<AtomId> = orphan_ids
                .iter()
                .copied()
                .filter(|id| !valid_ids.contains(id))
                .collect();
            let dead_list = dead
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let in_list = orphan_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            with_write_txn(&conn, |c| {
                if !dead.is_empty() {
                    c.execute_params(
                        &format!("DELETE FROM memory_idempotency WHERE atom_id IN ({dead_list})"),
                        &[],
                    )?;
                    c.execute_params(
                        &format!(
                            "DELETE FROM memory_edges WHERE src_id IN ({dead_list}) \
                             OR dst_id IN ({dead_list})"
                        ),
                        &[],
                    )?;
                }
                c.execute_params(&format!("DELETE FROM {table} WHERE id IN ({in_list})"), &[])?;
                Ok(())
            })?;
        }
        Ok(())
    }

    /// Open-time sweep: identity records whose region or atom row is gone
    /// (removed by an older binary or direct SQL) are deleted; the lazy
    /// self-heal only covers retried keys. Expired atoms keep their records
    /// (expiry is the lookup's TTL semantics, not an orphan state).
    fn reconcile_identity_records(&self) -> Result<()> {
        let conn = Connection::open(&self.db)?;
        let idents = conn.query_params("SELECT region_id, atom_id FROM memory_idempotency", &[])?;
        if idents.rows.is_empty() {
            return Ok(());
        }
        let mut by_region: FxHashMap<RegionId, Vec<AtomId>> = FxHashMap::default();
        for row in &idents.rows {
            by_region
                .entry(as_int(&row[0])?)
                .or_default()
                .push(as_int(&row[1])?);
        }
        let regions = conn.query_params(
            "SELECT id, embedding_dim, embedding_metric, encrypted FROM memory_regions",
            &[],
        )?;
        let mut tables: FxHashMap<RegionId, String> = FxHashMap::default();
        for row in &regions.rows {
            let dim = u16::try_from(as_int(&row[1])?)
                .map_err(|_| MemError::Invalid("stored embedding_dim out of range".into()))?;
            let metric = metric_from_str(as_text(&row[2])?)?;
            tables.insert(
                as_int(&row[0])?,
                atoms_table(dim, metric, as_exact_bool(&row[3], "encrypted")?),
            );
        }
        let mut orphans: Vec<(RegionId, Vec<AtomId>)> = Vec::new();
        for (region_id, atom_ids) in by_region {
            let live: FxHashSet<AtomId> = match tables.get(&region_id) {
                Some(table) if conn.table_schema(table).is_some() => {
                    let in_list = atom_ids
                        .iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let qr = conn.query_params(
                        &format!(
                            "SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list})"
                        ),
                        &[Value::Integer(region_id)],
                    )?;
                    qr.rows
                        .iter()
                        .map(|r| as_int(&r[0]))
                        .collect::<Result<_>>()?
                }
                // The region row or its table is gone: all records orphaned.
                _ => FxHashSet::default(),
            };
            let gone: Vec<AtomId> = atom_ids
                .into_iter()
                .filter(|id| !live.contains(id))
                .collect();
            if !gone.is_empty() {
                orphans.push((region_id, gone));
            }
        }
        if orphans.is_empty() {
            return Ok(());
        }
        // Only the proven (region, atom) pairs - a global atom_id match
        // could sweep a malformed duplicate in another region.
        with_write_txn(&conn, |c| {
            for (region_id, atom_ids) in &orphans {
                let in_list = atom_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                c.execute_params(
                    &format!(
                        "DELETE FROM memory_idempotency \
                         WHERE region_id = $1 AND atom_id IN ({in_list})"
                    ),
                    &[Value::Integer(*region_id)],
                )?;
            }
            Ok(())
        })
    }

    /// Record recall hits for `Lru`/`Stale` eviction. In-process and
    /// write-free, so recall never serializes behind the writer.
    fn note_access(&self, region_id: RegionId, ids: impl IntoIterator<Item = AtomId>) {
        let now = now_micros();
        let mut stats = self.access_stats.lock().unwrap();
        let region = stats.entry(region_id).or_default();
        for id in ids {
            let entry = region.entry(id).or_insert((0, 0));
            entry.0 = now;
            entry.1 += 1;
        }
    }

    /// Attach a cross-encoder reranker for later `recall`s, per `strategy`.
    pub fn set_reranker(&self, reranker: Arc<dyn Reranker>, strategy: RerankStrategy) {
        *self.reranker.write().unwrap() = Some((reranker, strategy));
    }

    /// Detach any reranker so subsequent `recall`s use linear fusion only.
    pub fn clear_reranker(&self) {
        *self.reranker.write().unwrap() = None;
    }

    /// Get-or-create a plaintext region bound to `embedder` (dim/metric/model
    /// must match).
    pub fn create_region(&self, name: &str, embedder: Arc<dyn Embedder>) -> Result<RegionId> {
        self.create_region_inner(name, embedder, false)
    }

    /// Get-or-create an encrypted region: each atom sealed under its own random
    /// key (ACK) wrapped by a per-region key. `drop_region`/`forget_atom` erase
    /// the region/one atom. Requires `enable_region_keys(true)`.
    pub fn create_encrypted_region(
        &self,
        name: &str,
        embedder: Arc<dyn Embedder>,
    ) -> Result<RegionId> {
        if !self.db.region_keys_enabled() {
            return Err(MemError::Core(citadel_core::Error::RegionKeysDisabled));
        }
        self.create_region_inner(name, embedder, true)
    }

    fn create_region_inner(
        &self,
        name: &str,
        embedder: Arc<dyn Embedder>,
        encrypted: bool,
    ) -> Result<RegionId> {
        let key = name.to_ascii_lowercase();
        let dim = u16::try_from(embedder.dim()).map_err(|_| {
            MemError::Invalid(format!("embedding dim {} too large", embedder.dim()))
        })?;
        let metric = embedder.metric();
        let model_id = embedder.model_id().to_string();

        // Region incarnation is persisted state, not a property of this
        // engine's local attachment map. Hold the lifecycle guard across the
        // lookup/create and state replacement so a cross-engine drop cannot
        // interleave, and always consult the row before accepting a cached
        // handle (drop + recreate binds the same name to a fresh id).
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        // A fresh region has no atoms; only a re-attach needs the MAX(id) scan.
        let (id, keys, init_max) = match self.load_region_row(&conn, &key)? {
            Some(existing) => {
                let attached = self.check_attached_incarnation(
                    &key,
                    existing.id,
                    dim,
                    metric,
                    &model_id,
                    encrypted,
                )?;
                if attached.is_some() && encrypted {
                    if let Err(err) = self.verify_region_key_live(&key, &existing) {
                        self.remove_attached_incarnation(&key, existing.id);
                        return Err(err);
                    }
                }
                if let Some(id) = attached {
                    return Ok(id);
                }
                existing.verify_matches(&key, dim, metric, &model_id, encrypted)?;
                let keys = if encrypted {
                    Some(self.attach_region_key(&key, &existing)?)
                } else {
                    None
                };
                let table = atoms_table(dim, metric, keys.is_some());
                let max = self.reattach_max_id(&conn, &table, existing.id)?;
                (existing.id, keys, max)
            }
            None if encrypted => {
                let (id, keys) =
                    self.insert_encrypted_region(&conn, &key, dim, metric, &model_id, &_kl)?;
                (id, keys, 0)
            }
            None => (
                self.insert_region(&conn, &key, dim, metric, &model_id)?,
                None,
                0,
            ),
        };

        let (atom_wrap, identity_mac) = match keys {
            Some(k) => (Some(k.atom_wrap), Some(k.identity_mac)),
            None => (None, None),
        };
        self.regions.lock().unwrap().insert(
            key,
            RegionState {
                id,
                dim,
                metric,
                embedder,
                atom_wrap,
                identity_mac,
                ann: Arc::new(RwLock::new(None)),
                max_id: Arc::new(AtomicI64::new(init_max)),
            },
        );
        Ok(id)
    }

    /// Fail-if-absent attach: reuse preflights must never create or probe via TOCTOU.
    pub fn attach_existing_region(
        &self,
        name: &str,
        embedder: Arc<dyn Embedder>,
    ) -> Result<RegionId> {
        let key = name.to_ascii_lowercase();
        let dim = u16::try_from(embedder.dim()).map_err(|_| {
            MemError::Invalid(format!("embedding dim {} too large", embedder.dim()))
        })?;
        let metric = embedder.metric();
        let model_id = embedder.model_id().to_string();

        // One lifecycle span so a concurrent drop_region orders around this attach.
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let Some(existing) = self.load_region_row(&conn, &key)? else {
            // A failed attach must not leave a stale local map entry usable by reads.
            self.detach_attached_region(&key, None);
            return Err(MemError::RegionNotFound(key));
        };
        let encrypted = existing.encrypted;
        if encrypted && !self.db.region_keys_enabled() {
            return Err(MemError::Core(citadel_core::Error::RegionKeysDisabled));
        }
        // Already attached in this process (existence is proven above).
        let attached =
            self.check_attached_incarnation(&key, existing.id, dim, metric, &model_id, encrypted)?;
        if attached.is_some() && encrypted {
            if let Err(err) = self.verify_region_key_live(&key, &existing) {
                self.remove_attached_incarnation(&key, existing.id);
                return Err(err);
            }
        }
        if let Some(id) = attached {
            return Ok(id);
        }
        existing.verify_matches(&key, dim, metric, &model_id, encrypted)?;
        let (atom_wrap, identity_mac) = if encrypted {
            let k = self.attach_region_key(&key, &existing)?;
            (Some(k.atom_wrap), Some(k.identity_mac))
        } else {
            (None, None)
        };
        let table = atoms_table(dim, metric, atom_wrap.is_some());
        let init_max = self.reattach_max_id(&conn, &table, existing.id)?;
        self.regions.lock().unwrap().insert(
            key,
            RegionState {
                id: existing.id,
                dim,
                metric,
                embedder,
                atom_wrap,
                identity_mac,
                ann: Arc::new(RwLock::new(None)),
                max_id: Arc::new(AtomicI64::new(init_max)),
            },
        );
        Ok(existing.id)
    }

    /// Drop a region and all its atoms and incident edges. No-op if absent.
    ///
    /// Encrypted: the region key is destroyed (overwrite + fsync + read-back)
    /// before any row delete, so a crash leaves the content undecryptable.
    pub fn drop_region(&self, name: &str) -> Result<()> {
        let key = name.to_ascii_lowercase();
        // Tombstone -> row-delete -> segment-retire is one lifecycle span.
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let Some(row) = self.load_region_row(&conn, &key)? else {
            self.detach_attached_region(&key, None);
            return Ok(());
        };
        let atoms = atoms_table(row.dim, row.metric, row.encrypted);

        // Destroy the key and drop the atom-wrap cache before deleting rows.
        if row.encrypted {
            let slot = row.rsk_slot.ok_or_else(|| {
                MemError::Invalid(format!(
                    "encrypted region '{key}' has no key slot; refusing to delete its \
                     rows without destroying a key"
                ))
            })?;
            // Only THIS row's slot; retries converge; TOMBSTONE still scrubs a torn sibling.
            let rec = self.db.region_store_slot(slot)?;
            if rec.state == SlotState::Tombstone
                || (rec.state == SlotState::Live
                    && rec.region_id == row.id as u64
                    && row.rsk_gen.is_none_or(|g| rec.gen == g))
            {
                _kl.region_store_tombstone(slot, row.id as u64)?;
            }
        }
        self.detach_attached_region(&key, Some(row.id));

        // Reclaim the region's atom key slots (RCK gone, so these are dead).
        if row.encrypted && conn.table_schema(&atoms).is_some() {
            let qr = conn.query_params(
                &format!("SELECT id, key_slot, key_gen FROM {atoms} WHERE region_id = $1"),
                &[Value::Integer(row.id)],
            )?;
            let slots: Vec<(u32, u64, u64)> = qr
                .rows
                .iter()
                .map(|r| {
                    Ok((
                        as_int(&r[1])? as u32,
                        as_int(&r[0])? as u64,
                        as_int(&r[2])? as u64,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            _kl.atom_store_tombstone_batch(&slots)?;
        }
        // The sealed segment holds embedding-derived residue; its row-less key
        // must die with the region.
        if row.encrypted {
            self.retire_sealed_segment_parts(&conn, row.id, &atoms, &_kl)?;
        }

        with_write_txn(&conn, |c| {
            if c.table_schema(&atoms).is_some() {
                c.execute_params(
                    &format!(
                        "DELETE FROM memory_edges WHERE src_id IN \
                         (SELECT id FROM {atoms} WHERE region_id = $1) \
                         OR dst_id IN (SELECT id FROM {atoms} WHERE region_id = $1)"
                    ),
                    &[Value::Integer(row.id)],
                )?;
                c.execute_params(
                    &format!("DELETE FROM {atoms} WHERE region_id = $1"),
                    &[Value::Integer(row.id)],
                )?;
            }
            c.execute_params(
                "DELETE FROM memory_idempotency WHERE region_id = $1",
                &[Value::Integer(row.id)],
            )?;
            c.execute_params(
                "DELETE FROM memory_regions WHERE id = $1",
                &[Value::Integer(row.id)],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn remember(&self, region: &str, atom: AtomInput) -> Result<AtomId> {
        self.remember_derived(region, atom, &[], None)
    }

    /// Atom + DerivedFrom edges in one txn so a crash cannot orphan a derived fact.
    pub fn remember_derived(
        &self,
        region: &str,
        atom: AtomInput,
        sources: &[AtomId],
        evidence_ref: Option<serde_json::Value>,
    ) -> Result<AtomId> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let prep = prepare_atom_row(&h, &key, &atom)?;
        let src_ids = dedup_sources(sources);

        let conn = Connection::open(&self.db)?;
        // Keys precede row commit; guard against a mid-span reconcile reclaim.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), 1);
        let result = with_write_txn(&conn, |c| {
            self.verify_region_live(c, &h, &key)?;
            verify_sources_exist(c, &h, &key, &src_ids)?;
            let id = next_id(c, "next_atom_id")?;
            self.insert_atom_row(c, &h, id, atom, prep, &mut pending)?;
            link_derived_sources(c, id, &src_ids, evidence_ref.as_ref())?;
            Ok(id)
        });
        let id = pending
            .finish(result)
            .inspect_err(|e| self.evict_stale_region(&key, h.id, e))?;
        h.max_id.fetch_max(id, Ordering::Relaxed);
        self.note_sealed_insert(&h);
        Ok(id)
    }

    /// Idempotent remember_derived: (kind, exact text) dedup inside the write txn.
    pub fn remember_if_absent(
        &self,
        region: &str,
        atom: AtomInput,
        sources: &[AtomId],
        evidence_ref: Option<serde_json::Value>,
    ) -> Result<RememberOutcome> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let prep = prepare_atom_row(&h, &key, &atom)?;
        let src_ids = dedup_sources(sources);

        let conn = Connection::open(&self.db)?;
        // Keys precede row commit; guard against a mid-span reconcile reclaim.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), 1);
        let result = with_write_txn(&conn, |c| {
            self.verify_region_live(c, &h, &key)?;
            verify_sources_exist(c, &h, &key, &src_ids)?;
            if let Some(id) = self.find_atom_by_text(c, &h, &atom.kind, &atom.text)? {
                link_derived_sources(c, id, &src_ids, evidence_ref.as_ref())?;
                return Ok(RememberOutcome {
                    id,
                    inserted: false,
                });
            }
            let id = next_id(c, "next_atom_id")?;
            self.insert_atom_row(c, &h, id, atom, prep, &mut pending)?;
            link_derived_sources(c, id, &src_ids, evidence_ref.as_ref())?;
            Ok(RememberOutcome { id, inserted: true })
        });
        let out = pending
            .finish(result)
            .inspect_err(|e| self.evict_stale_region(&key, h.id, e))?;
        h.max_id.fetch_max(out.id, Ordering::Relaxed);
        if out.inserted {
            self.note_sealed_insert(&h);
        }
        Ok(out)
    }

    /// [`remember_if_absent`](Self::remember_if_absent) keyed by a
    /// caller-supplied idempotency key instead of exact-text dedup: the same
    /// key converges on the original atom without touching it or its
    /// provenance; different keys store even identical texts.
    ///
    /// Keys are scoped to `(region, kind)`; the identity record also binds a
    /// canonical tag over every semantic input. While the bound atom is
    /// live, the same key with ANY changed input fails loudly (an edited
    /// retry needs a NEW key); a stale binding self-heals and frees the key.
    /// Encrypted regions store keyed BLAKE3 MACs, so no plaintext equality
    /// tag reaches disk - but the MAC key is REGION-lifetime, so freed-page
    /// residue of a purged record stays a guess-confirmation commitment for
    /// a later RCK holder; only [`drop_region`](Self::drop_region) closes
    /// that channel.
    pub fn remember_if_absent_keyed(
        &self,
        region: &str,
        atom: AtomInput,
        sources: &[AtomId],
        evidence_ref: Option<serde_json::Value>,
        idempotency_key: &str,
    ) -> Result<RememberOutcome> {
        if idempotency_key.is_empty() {
            return Err(MemError::Invalid("empty idempotency key".into()));
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let prep = prepare_atom_row(&h, &key, &atom)?;
        let src_ids = dedup_sources(sources);
        let mac = h.identity_mac.as_deref();
        let key_tag = identity_key_tag(mac, &atom.kind, idempotency_key);
        let request_tag = identity_request_tag(
            mac,
            &key_tag,
            &atom,
            &prep.payload,
            &src_ids,
            evidence_ref.as_ref(),
        )?;

        let conn = Connection::open(&self.db)?;
        // Sealed inserts allocate keys before their rows commit; hold the guard
        // so a concurrent reconcile cannot reclaim them mid-span.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), 1);
        let result = with_write_txn(&conn, |c| {
            self.verify_region_live(c, &h, &key)?;
            // Replay resolves first: the identical retry writes nothing, so
            // it must converge even if a source has since been forgotten.
            if let Some(id) = self.keyed_identity_hit(c, &h, &atom.kind, &key_tag, &request_tag)? {
                return Ok(RememberOutcome {
                    id,
                    inserted: false,
                });
            }
            verify_sources_exist(c, &h, &key, &src_ids)?;
            let id = next_id(c, "next_atom_id")?;
            c.execute_params(
                "INSERT INTO memory_idempotency \
                 (region_id, kind, key_mac, request_mac, atom_id) \
                 VALUES ($1, $2, $3, $4, $5)",
                &[
                    Value::Integer(h.id),
                    Value::Text(atom.kind.as_str().into()),
                    Value::Text(key_tag.as_str().into()),
                    Value::Text(request_tag.as_str().into()),
                    Value::Integer(id),
                ],
            )?;
            self.insert_atom_row(c, &h, id, atom, prep, &mut pending)?;
            link_derived_sources(c, id, &src_ids, evidence_ref.as_ref())?;
            Ok(RememberOutcome { id, inserted: true })
        });
        let out = pending
            .finish(result)
            .inspect_err(|e| self.evict_stale_region(&key, h.id, e))?;
        h.max_id.fetch_max(out.id, Ordering::Relaxed);
        if out.inserted {
            self.note_sealed_insert(&h);
        }
        Ok(out)
    }

    /// [`remember_if_absent_keyed`](Self::remember_if_absent_keyed) that
    /// supersedes instead of refusing: changed input replaces the atom the key
    /// named, an identical retry replays it. Insert, rebind and the old row's
    /// delete commit together, so a key names one atom and writers serialize.
    /// The old key dies after that commit; a crash between leaves an unnamed key
    /// for reconcile. [`forget_atoms`](Self::forget_atoms) erases for compliance.
    pub fn remember_replacing_keyed(
        &self,
        region: &str,
        atom: AtomInput,
        idempotency_key: &str,
    ) -> Result<RememberOutcome> {
        let mut out =
            self.remember_replacing_keyed_batch(region, vec![(atom, idempotency_key.to_string())])?;
        out.pop()
            .ok_or_else(|| MemError::Invalid("keyed replace returned no outcome".into()))
    }

    /// [`remember_replacing_keyed`](Self::remember_replacing_keyed) over a batch
    /// in one transaction, keeping the batched seal-and-allocate: one fsync for
    /// the batch where a per-atom loop pays one each. Outcomes come back per
    /// entry, in order. Keys must be distinct within a batch; two atoms for one
    /// key refuses rather than guessing.
    pub fn remember_replacing_keyed_batch(
        &self,
        region: &str,
        entries: Vec<(AtomInput, String)>,
    ) -> Result<Vec<RememberOutcome>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let (atoms, keys): (Vec<AtomInput>, Vec<String>) = entries.into_iter().unzip();
        let mut distinct = FxHashSet::default();
        for k in &keys {
            if k.is_empty() {
                return Err(MemError::Invalid("empty idempotency key".into()));
            }
            if !distinct.insert(k.as_str()) {
                return Err(MemError::Invalid(format!(
                    "idempotency key {k:?} appears twice in one batch"
                )));
            }
        }
        let vecs = self.vectorise_atoms(&key, &h, &atoms)?;
        let mac = h.identity_mac.as_deref();
        let kinds: Vec<String> = atoms.iter().map(|a| a.kind.clone()).collect();
        let mut tags: Vec<(String, String)> = Vec::with_capacity(atoms.len());
        for (atom, k) in atoms.iter().zip(&keys) {
            let payload_json = serde_json::to_string(&atom.payload)
                .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;
            let key_tag = identity_key_tag(mac, &atom.kind, k);
            let request_tag = identity_request_tag(mac, &key_tag, atom, &payload_json, &[], None)?;
            tags.push((key_tag, request_tag));
        }

        let encrypted = h.atom_wrap.is_some();
        let table = h.table.clone();
        let conn = Connection::open(&self.db)?;
        // One span over both the inserts' key allocation and the supersedes'
        // destruction, so no reconcile reclaims either mid-replace.
        let _kl = encrypted.then(|| self.db.key_lifecycle_lock());
        if let Some(kl) = _kl.as_ref() {
            // Before the txn: a crash cannot leave erased codes under a live segment key.
            self.retire_sealed_segment(&h, &conn, kl)?;
        }
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), atoms.len());
        let result = with_write_txn(&conn, |c| {
            self.verify_region_live(c, &h, &key)?;
            // All resolved before any insert, so no entry supersedes itself.
            let mut bound: Vec<Option<(String, AtomId)>> = Vec::with_capacity(kinds.len());
            for (kind, (key_tag, _)) in kinds.iter().zip(&tags) {
                bound.push(self.live_keyed_binding(c, &h, kind, key_tag)?);
            }
            // An identical retry writes nothing, so it never reaches the id range.
            let mut atoms: Vec<Option<AtomInput>> = atoms.into_iter().map(Some).collect();
            let mut vecs: Vec<Option<Vec<f32>>> = vecs.into_iter().map(Some).collect();
            let mut writing: Vec<usize> = Vec::with_capacity(kinds.len());
            let mut outcomes: Vec<Option<RememberOutcome>> = vec![None; kinds.len()];
            for i in 0..kinds.len() {
                match &bound[i] {
                    Some((request, id)) if request == &tags[i].1 => {
                        outcomes[i] = Some(RememberOutcome {
                            id: *id,
                            inserted: false,
                        });
                    }
                    _ => writing.push(i),
                }
            }
            let fresh: Vec<AtomInput> = writing
                .iter()
                .map(|&i| atoms[i].take().expect("each index is taken once"))
                .collect();
            let fresh_vecs: Vec<Vec<f32>> = writing
                .iter()
                .map(|&i| vecs[i].take().expect("each index is taken once"))
                .collect();
            let ids = self.insert_atom_rows(c, &h, &table, fresh, fresh_vecs, &mut pending)?;

            let mut stale: Vec<AtomId> = Vec::new();
            for (slot, &i) in writing.iter().enumerate() {
                let id = ids[slot];
                let params = [
                    Value::Integer(h.id),
                    Value::Text(kinds[i].as_str().into()),
                    Value::Text(tags[i].0.as_str().into()),
                    Value::Text(tags[i].1.as_str().into()),
                    Value::Integer(id),
                ];
                match bound[i].take() {
                    Some((_, superseded)) => {
                        c.execute_params(
                            "UPDATE memory_idempotency SET request_mac = $4, atom_id = $5 \
                             WHERE region_id = $1 AND kind = $2 AND key_mac = $3",
                            &params,
                        )?;
                        stale.push(superseded);
                    }
                    None => {
                        c.execute_params(
                            "INSERT INTO memory_idempotency \
                             (region_id, kind, key_mac, request_mac, atom_id) \
                             VALUES ($1, $2, $3, $4, $5)",
                            &params,
                        )?;
                    }
                }
                outcomes[i] = Some(RememberOutcome { id, inserted: true });
            }

            let doomed = if stale.is_empty() {
                Vec::new()
            } else {
                let in_list = stale
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                // Read while the rows still name their keys; the delete orphans them.
                let slots = if encrypted {
                    atom_key_slots(c, &h, &in_list)?
                } else {
                    Vec::new()
                };
                delete_atoms_in_txn(c, &h, &in_list)?;
                slots
            };
            let outcomes: Vec<RememberOutcome> = outcomes
                .into_iter()
                .map(|o| o.expect("every entry is replayed or written"))
                .collect();
            Ok((outcomes, doomed))
        });
        let (out, doomed) = pending
            .finish(result)
            .inspect_err(|e| self.evict_stale_region(&key, h.id, e))?;
        if !doomed.is_empty() {
            // Non-empty only on the sealed path, which is where the guard is held.
            let kl = _kl.as_ref().expect("sealed slots imply a lifecycle span");
            kl.atom_store_tombstone_batch(&doomed)?;
        }
        if let Some(max) = out.iter().map(|o| o.id).max() {
            h.max_id.fetch_max(max, Ordering::Relaxed);
        }
        if doomed.is_empty() {
            self.note_sealed_insert(&h);
        } else {
            // A deleted row invalidates the cached index outright.
            *h.ann.write().unwrap() = None;
        }
        Ok(out)
    }

    /// Resolve a keyed write against the identity table inside the caller's
    /// write transaction: `Some(id)` replays the original atom untouched,
    /// `None` means insert fresh (any stale record was self-healed away),
    /// and the same key bound to a different request tag fails loudly.
    fn keyed_identity_hit(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        kind: &str,
        key_tag: &str,
        request_tag: &str,
    ) -> Result<Option<AtomId>> {
        let Some((bound, atom_id)) = self.live_keyed_binding(conn, h, kind, key_tag)? else {
            return Ok(None);
        };
        if bound != request_tag {
            return Err(MemError::Invalid(format!(
                "idempotency key already bound to atom {atom_id} with a different request"
            )));
        }
        Ok(Some(atom_id))
    }

    /// The live atom a key names and the request tag it was bound under, inside
    /// the caller's write transaction. A binding whose atom is gone, expired or
    /// key-erased is deleted here and reads as absent, freeing the key.
    fn live_keyed_binding(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        kind: &str,
        key_tag: &str,
    ) -> Result<Option<(String, AtomId)>> {
        let qr = conn.query_params(
            "SELECT request_mac, atom_id FROM memory_idempotency \
             WHERE region_id = $1 AND kind = $2 AND key_mac = $3",
            &[
                Value::Integer(h.id),
                Value::Text(kind.into()),
                Value::Text(key_tag.into()),
            ],
        )?;
        let Some(row) = qr.rows.first() else {
            return Ok(None);
        };
        let atom_id = as_int(&row[1])?;
        // Liveness first: expired or key-erased (sealed triple bind) targets
        // count as absent and self-heal, freeing the key for rebinding; only
        // a live binding may refuse a changed request.
        let alive = if h.atom_wrap.is_some() {
            let qr = conn.query_params(
                &format!(
                    "SELECT key_slot, key_gen FROM {} WHERE region_id = $1 AND id = $2 \
                     AND (expires_at IS NULL OR expires_at > $3)",
                    h.table
                ),
                &[
                    Value::Integer(h.id),
                    Value::Integer(atom_id),
                    Value::Timestamp(now_micros()),
                ],
            )?;
            match qr.rows.first() {
                None => false,
                Some(row) => {
                    let rec = self.db.atom_store_slot(as_int(&row[0])? as u32)?;
                    rec.state == SlotState::Live
                        && rec.region_id == atom_id as u64
                        && rec.gen == as_int(&row[1])? as u64
                }
            }
        } else {
            let qr = conn.query_params(
                &format!(
                    "SELECT id FROM {} WHERE region_id = $1 AND id = $2 \
                     AND (expires_at IS NULL OR expires_at > $3)",
                    h.table
                ),
                &[
                    Value::Integer(h.id),
                    Value::Integer(atom_id),
                    Value::Timestamp(now_micros()),
                ],
            )?;
            !qr.rows.is_empty()
        };
        if !alive {
            conn.execute_params(
                "DELETE FROM memory_idempotency \
                 WHERE region_id = $1 AND kind = $2 AND key_mac = $3",
                &[
                    Value::Integer(h.id),
                    Value::Text(kind.into()),
                    Value::Text(key_tag.into()),
                ],
            )?;
            return Ok(None);
        }
        Ok(Some((as_text(&row[0])?.to_string(), atom_id)))
    }

    /// [`remember_derived`](Self::remember_derived) with caller-declared
    /// snapshot validation: every `snapshot` member must be present,
    /// unexpired, and still hash to its declared SHA-256, checked in the
    /// same write transaction as the insert and the `DerivedFrom` edges for
    /// `provenance` (a required subset of the snapshot). Validates only what
    /// the caller declares; serializing ingest against derivation is the
    /// controller's job.
    pub fn remember_derived_checked(
        &self,
        region: &str,
        atom: AtomInput,
        snapshot: &[SourceSnapshot],
        provenance: &[AtomId],
        evidence_ref: Option<serde_json::Value>,
    ) -> Result<AtomId> {
        let mut declared: FxHashSet<AtomId> = FxHashSet::default();
        for member in snapshot {
            if !declared.insert(member.id) {
                return Err(MemError::Invalid(format!(
                    "duplicate snapshot id {}",
                    member.id
                )));
            }
        }
        let src_ids = dedup_sources(provenance);
        if let Some(missing) = src_ids.iter().find(|id| !declared.contains(id)) {
            return Err(MemError::Invalid(format!(
                "provenance atom {missing} is not in the declared snapshot"
            )));
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let prep = prepare_atom_row(&h, &key, &atom)?;

        let conn = Connection::open(&self.db)?;
        // Sealed inserts allocate keys before their rows commit; hold the guard
        // so a concurrent reconcile cannot reclaim them mid-span.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), 1);
        let result = with_write_txn(&conn, |c| {
            self.verify_region_live(c, &h, &key)?;
            self.verify_source_snapshot(c, &h, &key, snapshot)?;
            let id = next_id(c, "next_atom_id")?;
            self.insert_atom_row(c, &h, id, atom, prep, &mut pending)?;
            link_derived_sources(c, id, &src_ids, evidence_ref.as_ref())?;
            Ok(id)
        });
        let id = pending
            .finish(result)
            .inspect_err(|e| self.evict_stale_region(&key, h.id, e))?;
        h.max_id.fetch_max(id, Ordering::Relaxed);
        self.note_sealed_insert(&h);
        Ok(id)
    }

    /// Every snapshot member must be present, unexpired, and its stored text
    /// must still hash to the declared digest - all read inside the caller's
    /// write transaction.
    fn verify_source_snapshot(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        region_key: &str,
        snapshot: &[SourceSnapshot],
    ) -> Result<()> {
        let now = now_micros();
        for member in snapshot {
            let id = member.id;
            let text = match &h.atom_wrap {
                None => {
                    let qr = conn.query_params(
                        &format!(
                            "SELECT text_content, expires_at FROM {} \
                             WHERE region_id = $1 AND id = $2",
                            h.table
                        ),
                        &[Value::Integer(h.id), Value::Integer(id)],
                    )?;
                    let Some(row) = qr.rows.first() else {
                        return Err(MemError::Invalid(format!(
                            "source atom {id} not in region '{region_key}'"
                        )));
                    };
                    verify_snapshot_unexpired(&row[1], id, now)?;
                    match &row[0] {
                        Value::Text(t) => t.to_string(),
                        other => {
                            return Err(MemError::Invalid(format!(
                                "atom text is not text: {other:?}"
                            )))
                        }
                    }
                }
                Some(atom_wrap) => {
                    let qr = conn.query_params(
                        &format!(
                            "SELECT sealed, key_slot, key_gen, expires_at FROM {} \
                             WHERE region_id = $1 AND id = $2",
                            h.table
                        ),
                        &[Value::Integer(h.id), Value::Integer(id)],
                    )?;
                    let Some(row) = qr.rows.first() else {
                        return Err(MemError::Invalid(format!(
                            "source atom {id} not in region '{region_key}'"
                        )));
                    };
                    verify_snapshot_unexpired(&row[3], id, now)?;
                    let rec = self.db.atom_store_slot(as_int(&row[1])? as u32)?;
                    if rec.state != SlotState::Live
                        || rec.region_id != id as u64
                        || rec.gen != as_int(&row[2])? as u64
                    {
                        return Err(MemError::Invalid(format!(
                            "source atom {id} not in region '{region_key}'"
                        )));
                    }
                    open_atom_text(atom_wrap, &rec.wrapped, id, as_blob(&row[0])?)?
                }
            };
            let text = Zeroizing::new(text);
            let got: [u8; 32] = Sha256::digest(text.as_bytes()).into();
            if got != member.text_sha256 {
                return Err(MemError::Invalid(format!(
                    "source atom {id} text changed since it was digested"
                )));
            }
        }
        Ok(())
    }

    /// Bump the epoch so other handles rebuild; the caller's current cache re-stamps.
    fn note_sealed_insert(&self, h: &RegionHandle) {
        if h.atom_wrap.is_none() {
            return;
        }
        let new_epoch = self.db.bump_cache_epoch();
        if let Some(sa) = h.ann.write().unwrap().as_mut() {
            if sa.build_epoch == new_epoch - 1 {
                sa.build_epoch = new_epoch;
            }
        }
    }

    /// Write one atom row inside the caller's transaction.
    fn insert_atom_row(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        id: AtomId,
        atom: AtomInput,
        prep: PreparedAtomRow,
        pending: &mut PendingAtomSlots<'_>,
    ) -> Result<()> {
        let table = &h.table;
        if let Some(atom_wrap) = &h.atom_wrap {
            let (sealed, wrapped) = seal_atom(atom_wrap, id, &prep.vec, &atom.text, &prep.payload);
            // Fsync the ACK first so a committed row references a durable key slot.
            let (slot, gen) = self.db.atom_store_allocate_write(id as u64, &wrapped)?;
            pending.track(slot, id as u64, gen);
            insert_sealed_atom(
                conn,
                table,
                id,
                h.id,
                &atom.kind,
                sealed,
                slot,
                gen,
                atom.score,
                atom.confidence,
                prep.immutable,
                prep.created,
                prep.expires,
            )?;
        } else {
            conn.execute_params(
                &format!(
                    "INSERT INTO {table} \
                     (id, region_id, kind, embedding, payload, text_content, score, confidence, \
                      access_count, immutable, created_at, accessed_at, expires_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9, \
                      $10, CURRENT_TIMESTAMP, $11)"
                ),
                &[
                    Value::Integer(id),
                    Value::Integer(h.id),
                    Value::Text(atom.kind.into()),
                    Value::Vector(prep.vec.into()),
                    Value::Text(prep.payload.into()),
                    Value::Text(atom.text.into()),
                    Value::Real(atom.score as f64),
                    Value::Real(atom.confidence as f64),
                    Value::Integer(prep.immutable),
                    prep.created,
                    prep.expires,
                ],
            )?;
        }
        Ok(())
    }

    /// Oldest live atom in the region with this exact `kind` + `text`, if any.
    fn find_atom_by_text(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        kind: &str,
        text: &str,
    ) -> Result<Option<AtomId>> {
        let table = &h.table;
        let Some(atom_wrap) = &h.atom_wrap else {
            let qr = conn.query_params(
                &format!(
                    "SELECT id FROM {table} WHERE region_id = $1 AND kind = $2 \
                     AND text_content = $3 AND (expires_at IS NULL OR expires_at > $4) \
                     ORDER BY id LIMIT 1"
                ),
                &[
                    Value::Integer(h.id),
                    Value::Text(kind.into()),
                    Value::Text(text.into()),
                    Value::Timestamp(now_micros()),
                ],
            )?;
            return qr.rows.first().map(|r| as_int(&r[0])).transpose();
        };
        let qr = conn.query_params(
            &format!(
                "SELECT id, sealed, key_slot, key_gen FROM {table} WHERE region_id = $1 \
                 AND kind = $2 AND (expires_at IS NULL OR expires_at > $3) ORDER BY id"
            ),
            &[
                Value::Integer(h.id),
                Value::Text(kind.into()),
                Value::Timestamp(now_micros()),
            ],
        )?;
        for row in &qr.rows {
            let id = as_int(&row[0])?;
            let rec = self.db.atom_store_slot(as_int(&row[2])? as u32)?;
            // Erased/recycled key: unrecoverable text cannot match - skip like recall.
            if rec.state != SlotState::Live
                || rec.region_id != id as u64
                || rec.gen != as_int(&row[3])? as u64
            {
                continue;
            }
            let row_text = Zeroizing::new(open_atom_text(
                atom_wrap,
                &rec.wrapped,
                id,
                as_blob(&row[1])?,
            )?);
            let hit = row_text.as_str() == text;
            if hit {
                return Ok(Some(id));
            }
        }
        Ok(None)
    }

    /// Embed + store atoms in one transaction; faster than looping `remember`.
    pub fn remember_batch(&self, region: &str, atoms: Vec<AtomInput>) -> Result<Vec<AtomId>> {
        if atoms.is_empty() {
            return Ok(Vec::new());
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;

        let vecs = self.vectorise_atoms(&key, &h, &atoms)?;
        let table = h.table.clone();
        let conn = Connection::open(&self.db)?;
        // Sealed inserts allocate keys before their rows commit; hold the guard
        // so a concurrent reconcile cannot reclaim them mid-span.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), atoms.len());
        let result = with_write_txn(&conn, |c| {
            self.verify_region_live(c, &h, &key)?;
            self.insert_atom_rows(c, &h, &table, atoms, vecs, &mut pending)
        });
        let ids = pending
            .finish(result)
            .inspect_err(|e| self.evict_stale_region(&key, h.id, e))?;
        if let Some(&last) = ids.last() {
            h.max_id.fetch_max(last, Ordering::Relaxed);
            self.note_sealed_insert(&h);
        }
        Ok(ids)
    }

    /// Validate and vectorise atoms before any transaction: one shared input
    /// boundary, and the embedder never runs with a write txn open.
    fn vectorise_atoms(
        &self,
        key: &str,
        h: &RegionHandle,
        atoms: &[AtomInput],
    ) -> Result<Vec<Vec<f32>>> {
        for atom in atoms {
            validate_atom_input(atom)?;
        }

        // Only atoms without a supplied vector reach the embedder.
        let texts: Vec<&str> = atoms
            .iter()
            .filter(|a| a.embedding.is_none())
            .map(|a| a.text.as_str())
            .collect();
        let embedded = if texts.is_empty() {
            Vec::new()
        } else {
            h.embedder.embed(&texts)?
        };
        if embedded.len() != texts.len() {
            return Err(MemError::Invalid(format!(
                "embedder returned {} vectors for {} texts",
                embedded.len(),
                texts.len()
            )));
        }
        let mut taken = 0usize;
        let mut vecs: Vec<Vec<f32>> = Vec::with_capacity(atoms.len());
        for atom in atoms {
            let vector = match &atom.embedding {
                Some(supplied) => supplied.clone(),
                None => {
                    let v = embedded[taken].clone();
                    taken += 1;
                    v
                }
            };
            validate_embedding(key, h.dim, &vector, "passage")?;
            vecs.push(vector);
        }
        Ok(vecs)
    }

    /// Insert vectorised atoms inside the caller's transaction, sealing and
    /// allocating every key in one batch (one fsync, not one per atom).
    fn insert_atom_rows(
        &self,
        c: &Connection<'_>,
        h: &RegionHandle,
        table: &str,
        atoms: Vec<AtomInput>,
        vecs: Vec<Vec<f32>>,
        pending: &mut PendingAtomSlots<'_>,
    ) -> Result<Vec<AtomId>> {
        let n = atoms.len();
        if n == 0 {
            return Ok(Vec::new());
        }
        let start = next_id_range(c, "next_atom_id", n as i64)?;
        let ids: Vec<AtomId> = (0..n as i64).map(|o| start + o).collect();
        {
            if let Some(atom_wrap) = &h.atom_wrap {
                // Seal all atoms, persist their wrapped ACKs with one fsync.
                let mut sealed_blobs: Vec<Vec<u8>> = Vec::with_capacity(n);
                let mut key_items: Vec<(u64, [u8; WRAPPED_KEY_SIZE])> = Vec::with_capacity(n);
                for ((atom, vec), &id) in atoms.iter().zip(&vecs).zip(&ids) {
                    let payload = serde_json::to_string(&atom.payload)
                        .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;
                    let (sealed, wrapped) = seal_atom(atom_wrap, id, vec, &atom.text, &payload);
                    sealed_blobs.push(sealed);
                    key_items.push((id as u64, wrapped));
                }
                let slots = self.db.atom_store_allocate_batch(&key_items)?;
                for (&id, &(slot, generation)) in ids.iter().zip(&slots) {
                    pending.track(slot, id as u64, generation);
                }
                for (((atom, &id), sealed), &(slot, gen)) in
                    atoms.iter().zip(&ids).zip(sealed_blobs).zip(&slots)
                {
                    let expires = atom.expires_at.map(Value::Timestamp).unwrap_or(Value::Null);
                    let created = Value::Timestamp(atom.created_at.unwrap_or_else(now_micros));
                    insert_sealed_atom(
                        c,
                        table,
                        id,
                        h.id,
                        &atom.kind,
                        sealed,
                        slot,
                        gen,
                        atom.score,
                        atom.confidence,
                        i64::from(atom.immutable),
                        created,
                        expires,
                    )?;
                }
            } else {
                let plaintext_sql = format!(
                    "INSERT INTO {table} \
                     (id, region_id, kind, embedding, payload, text_content, score, confidence, \
                      access_count, immutable, created_at, accessed_at, expires_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9, \
                      $10, CURRENT_TIMESTAMP, $11)"
                );
                for ((atom, vec), &id) in atoms.into_iter().zip(vecs).zip(&ids) {
                    let payload = serde_json::to_string(&atom.payload)
                        .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;
                    let expires = atom.expires_at.map(Value::Timestamp).unwrap_or(Value::Null);
                    let created = Value::Timestamp(atom.created_at.unwrap_or_else(now_micros));
                    c.execute_params(
                        &plaintext_sql,
                        &[
                            Value::Integer(id),
                            Value::Integer(h.id),
                            Value::Text(atom.kind.into()),
                            Value::Vector(vec.into()),
                            Value::Text(payload.into()),
                            Value::Text(atom.text.into()),
                            Value::Real(atom.score as f64),
                            Value::Real(atom.confidence as f64),
                            Value::Integer(i64::from(atom.immutable)),
                            created,
                            expires,
                        ],
                    )?;
                }
            }
        }
        Ok(ids)
    }

    /// Fetch up to `limit` atoms of `kind` (optional JSONB `@>` filter).
    /// Encrypted regions decrypt in id order, paging to honor `limit`.
    pub fn fetch(
        &self,
        region: &str,
        kind: &str,
        payload_filter: Option<&serde_json::Value>,
        limit: usize,
    ) -> Result<Vec<AtomHit>> {
        let mut q = FetchQuery::new(limit).with_kind(kind);
        q.payload_filter = payload_filter.cloned();
        self.fetch_range(region, &q)
    }

    /// Deterministic id-order listing; resume by passing the last id as `after_id`.
    pub fn fetch_range(&self, region: &str, q: &FetchQuery) -> Result<Vec<AtomHit>> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if q.limit == 0 {
            return Ok(Vec::new());
        }
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                self.fetch_sealed(&h, q, conn, atom_wrap)
            });
        }

        let mut params: Vec<Value> = vec![Value::Integer(h.id)];
        let mut preds = String::new();
        if let Some(kind) = &q.kind {
            params.push(Value::Text(kind.as_str().into()));
            preds += &format!(" AND kind = ${}", params.len());
        }
        if let Some(filter) = &q.payload_filter {
            let js = serde_json::to_string(filter)
                .map_err(|e| MemError::Invalid(format!("payload_filter not serializable: {e}")))?;
            params.push(Value::Text(js.into()));
            preds += &format!(" AND payload @> CAST(${} AS JSONB)", params.len());
        }
        if let Some(after) = q.after_id {
            params.push(Value::Integer(after));
            preds += &format!(" AND id > ${}", params.len());
        }
        if let Some(from) = q.created_from {
            params.push(Value::Timestamp(from));
            preds += &format!(" AND created_at >= ${}", params.len());
        }
        if let Some(before) = q.created_before {
            params.push(Value::Timestamp(before));
            preds += &format!(" AND created_at < ${}", params.len());
        }
        params.push(Value::Timestamp(now_micros()));
        preds += &format!(
            " AND (expires_at IS NULL OR expires_at > ${})",
            params.len()
        );

        self.with_live_plain_access(&key, &h, |conn| {
            let qr = conn.query_params(
                &format!(
                    "SELECT id, kind, CAST(payload AS TEXT), text_content, score, immutable, created_at \
                     FROM {table} WHERE region_id = $1{preds} \
                     ORDER BY id {dir} LIMIT {limit}",
                    table = h.table,
                    dir = if q.newest { "DESC" } else { "ASC" },
                    limit = q.limit
                ),
                &params,
            )?;
            let mut hits: Vec<AtomHit> =
                qr.rows.iter().map(|row| parse_fetched(row)).collect::<Result<_>>()?;
            // The window was taken from the end; callers still read oldest first.
            if q.newest {
                hits.reverse();
            }
            Ok(hits)
        })
    }

    /// Count atoms of `kind` without materializing them (`kind` is plaintext in
    /// both flavors). A sealed region counts only atoms whose key is live.
    pub fn count(&self, region: &str, kind: &str) -> Result<u64> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let ttl_params = [
            Value::Integer(h.id),
            Value::Text(kind.into()),
            Value::Timestamp(now_micros()),
        ];
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |conn, _, _kl| {
                let wrapped = self.db.atom_store_live_wrapped()?;
                let qr = conn.query_params(
                    &format!(
                        "SELECT id FROM {table} WHERE region_id = $1 AND kind = $2 \
                         AND (expires_at IS NULL OR expires_at > $3)",
                        table = h.table
                    ),
                    &ttl_params,
                )?;
                let mut live = 0u64;
                for row in &qr.rows {
                    if wrapped.contains_key(&(as_int(&row[0])? as u64)) {
                        live += 1;
                    }
                }
                Ok(live)
            });
        }
        self.with_live_plain_access(&key, &h, |conn| {
            let qr = conn.query_params(
                &format!(
                    "SELECT COUNT(*) FROM {table} WHERE region_id = $1 AND kind = $2 \
                     AND (expires_at IS NULL OR expires_at > $3)",
                    table = h.table
                ),
                &ttl_params,
            )?;
            match qr.rows.first().and_then(|r| r.first()) {
                Some(Value::Integer(n)) => Ok(*n as u64),
                other => Err(MemError::Invalid(format!(
                    "COUNT returned no integer: {other:?}"
                ))),
            }
        })
    }

    /// Exact (unnormalized) data path: a binding check for external cache-set locks.
    pub fn database_data_path(&self) -> &Path {
        self.db.data_path()
    }

    /// Persisted region identities; a half-erased encrypted region fails closed.
    pub fn stored_region_identities(&self) -> Result<Vec<StoredRegionIdentity>> {
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            "SELECT name, id, embedding_dim, embedding_metric, model_id, encrypted, \
             rsk_slot, rsk_gen FROM memory_regions",
            &[],
        )?;
        let mut identities = Vec::with_capacity(qr.rows.len());
        for row in &qr.rows {
            let name = as_text(&row[0])?;
            let persisted = parse_region_row(&row[1..])?;
            if persisted.encrypted {
                self.verify_region_key_live(name, &persisted)?;
            }
            identities.push(StoredRegionIdentity::new(
                name.to_owned(),
                persisted.encrypted,
                persisted.dim,
                persisted.metric,
                persisted.model_id,
            ));
        }
        identities.sort_by(|left, right| left.name().cmp(right.name()));
        Ok(identities)
    }

    /// Exact persisted region names, sorted deterministically.
    pub fn stored_region_names(&self) -> Result<Vec<String>> {
        self.stored_region_identities().map(|identities| {
            identities
                .into_iter()
                .map(|identity| identity.name().to_owned())
                .collect()
        })
    }

    /// Stored kinds: storage inventory (expired rows stay visible), never decrypts.
    pub fn stored_atom_kinds(&self, region: &str) -> Result<Vec<String>> {
        let key = region.to_ascii_lowercase();
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let row = self.load_live_region_row(&conn, &key)?;
        let table = atoms_table(row.dim, row.metric, row.encrypted);
        let qr = conn.query_params(
            &format!("SELECT DISTINCT kind FROM {table} WHERE region_id = $1"),
            &[Value::Integer(row.id)],
        )?;
        let mut kinds = qr
            .rows
            .iter()
            .map(|stored| Ok(as_text(&stored[0])?.to_owned()))
            .collect::<Result<Vec<_>>>()?;
        kinds.sort();
        Ok(kinds)
    }

    /// Content-free per-row storage inventory, not recall eligibility; no decryption.
    pub fn stored_atom_retrieval_state(
        &self,
        region: &str,
    ) -> Result<Vec<StoredAtomRetrievalState>> {
        let key = region.to_ascii_lowercase();
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let row = self.load_live_region_row(&conn, &key)?;
        let table = atoms_table(row.dim, row.metric, row.encrypted);
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, score, expires_at FROM {table} WHERE region_id = $1 ORDER BY id"
            ),
            &[Value::Integer(row.id)],
        )?;
        let mut states = qr
            .rows
            .iter()
            .map(|stored| {
                Ok(StoredAtomRetrievalState::new(
                    as_int(&stored[0])?,
                    as_text(&stored[1])?.to_owned(),
                    exact_f32_bits(&stored[2])?,
                    exact_opt_ts(&stored[3])?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        states.sort_by_key(StoredAtomRetrievalState::atom_id);
        Ok(states)
    }

    /// SHA-256 identity of live stored embeddings; row-at-a-time decrypt, zeroized.
    pub fn stored_embeddings_identity(
        &self,
        region: &str,
        kind: &str,
    ) -> Result<StoredEmbeddingsIdentity> {
        self.scan_stored_embeddings(region, kind, None)
    }

    /// Verify exact stored f32 bits; `expected` = every live atom in ascending id order.
    pub fn verify_stored_embeddings_exact(
        &self,
        region: &str,
        kind: &str,
        expected: &[(AtomId, Vec<f32>)],
    ) -> Result<StoredEmbeddingsIdentity> {
        self.scan_stored_embeddings(region, kind, Some(expected))
    }

    fn scan_stored_embeddings(
        &self,
        region: &str,
        kind: &str,
        expected: Option<&[(AtomId, Vec<f32>)]>,
    ) -> Result<StoredEmbeddingsIdentity> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if let Some(expected) = expected {
            validate_expected_embeddings(expected, usize::from(h.dim))?;
        }

        let params = [
            Value::Integer(h.id),
            Value::Text(kind.into()),
            Value::Timestamp(now_micros()),
        ];
        let mut scan = StoredEmbeddingScan::new(&key, kind, h.dim, expected);
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                let qr = conn.query_params(
                    &format!(
                        "SELECT id, sealed, key_slot, key_gen FROM {table} \
                         WHERE region_id = $1 AND kind = $2 \
                         AND (expires_at IS NULL OR expires_at > $3) ORDER BY id",
                        table = h.table
                    ),
                    &params,
                )?;
                for row in &qr.rows {
                    let id = as_int(&row[0])?;
                    let Some(wrapped_ack) =
                        exact_live_atom_wrapped(&self.db, id, &row[2], &row[3])?
                    else {
                        continue;
                    };
                    let mut embedding =
                        open_atom_embedding(atom_wrap, &wrapped_ack, id, as_blob(&row[1])?)?;
                    let result = scan.consume(id, &embedding);
                    embedding.zeroize();
                    result?;
                }
                scan.finish()
            });
        }

        self.with_live_plain_access(&key, &h, |conn| {
            let qr = conn.query_params(
                &format!(
                    "SELECT id, embedding FROM {table} WHERE region_id = $1 AND kind = $2 \
                     AND (expires_at IS NULL OR expires_at > $3) ORDER BY id",
                    table = h.table
                ),
                &params,
            )?;
            for row in &qr.rows {
                let id = as_int(&row[0])?;
                let embedding = match &row[1] {
                    Value::Vector(vector) => vector.as_ref(),
                    other => {
                        return Err(MemError::Invalid(format!(
                            "stored embedding for atom {id} is not a vector: {other:?}"
                        )))
                    }
                };
                scan.consume(id, embedding)?;
            }
            scan.finish()
        })
    }

    /// Freeze the region's ANN index into a persisted segment so a cold attach
    /// loads it instead of rebuilding. Sealed regions seal the segment under a
    /// random erasable-store key, so destroying that slot crypto-erases every
    /// on-disk embedding derivative.
    pub fn persist_ann_index(&self, region: &str) -> Result<AnnSegmentInfo> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |conn, _, _kl| {
                self.persist_sealed_segment(&h, conn, _kl)
            });
        }
        self.with_live_plain_access(&key, &h, |conn| {
            Ok(conn.persist_ann_index(&h.table, "embedding")?)
        })
    }

    /// [`ann_cache_status`](Self::ann_cache_status) plus whether the entry is current.
    pub fn ann_cache_status_current(&self, region: &str) -> Result<Option<(AnnIndexSource, bool)>> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |_, _, _| {
                Ok(h.ann
                    .read()
                    .unwrap()
                    .as_ref()
                    .map(|sa| (sa.source.clone(), sa.build_epoch == self.db.cache_epoch())))
            });
        }
        self.with_live_plain_access(&key, &h, |conn| {
            Ok(conn
                .ann_cache_status(&h.table, "embedding")?
                .map(|(source, _)| (source, true)))
        })
    }

    /// Serving ANN index: `Loaded` or `Built`; `None` if unbuilt or epoch-stale.
    pub fn ann_cache_status(&self, region: &str) -> Result<Option<AnnIndexSource>> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |_, _, _| {
                Ok(h.ann
                    .read()
                    .unwrap()
                    .as_ref()
                    .filter(|sa| sa.build_epoch == self.db.cache_epoch())
                    .map(|sa| sa.source.clone()))
            });
        }
        self.with_live_plain_access(&key, &h, |conn| {
            Ok(conn
                .ann_cache_status(&h.table, "embedding")?
                .map(|(source, _)| source))
        })
    }

    /// Persist a sealed region's ANN graph: scan + decrypt (with the
    /// liveness-aware fingerprint), build the PRISM index, and seal it under a
    /// fresh segment key held only in the erasable store under a pseudo-atom
    /// id. Chunks go to the hidden `__annseg_{table}` tree.
    fn persist_sealed_segment(
        &self,
        h: &RegionHandle,
        conn: &Connection<'_>,
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<AnnSegmentInfo> {
        let atom_wrap = h.atom_wrap.as_ref().expect("sealed persist");

        let mut kind_codes: FxHashMap<String, u32> = FxHashMap::default();
        let mut triples: Vec<(u64, Vec<f32>, Vec<u32>)> = Vec::new();
        let fingerprint = sealed_fp_scan(
            conn,
            &self.db,
            h,
            &mut |id, kind, sealed, wrapped, _, _, _, _| {
                let emb = open_atom_embedding(atom_wrap, wrapped, id, sealed)?;
                let next = kind_codes.len() as u32;
                let code = *kind_codes.entry(kind.to_string()).or_insert(next);
                triples.push((id as u64, emb, vec![code]));
                Ok(true)
            },
        )?
        .0;
        if triples.is_empty() {
            return Err(MemError::Invalid(
                "nothing to persist: the sealed region has no live atoms".into(),
            ));
        }
        let n = triples.len() as u64;
        let index = AnnIndex::build_with_attrs(triples, 1, ann_metric(h.metric), h.dim)
            .map_err(|e| MemError::Invalid(format!("sealed ANN build: {e}")))?;

        // Inner plaintext: [fp 32][config_hash 32][kind_codes][segment body];
        // zeroized after seal.
        let body = Zeroizing::new(citadel_vector::segment::encode(&index));
        let mut inner = Zeroizing::new(Vec::with_capacity(body.len() + 256));
        inner.extend_from_slice(&fingerprint);
        // Pin the PRISM config (incl. search-geometry version): a binary whose
        // active config differs must refuse the segment and rebuild from rows.
        inner.extend_from_slice(&citadel_vector::segment::prism_config_hash(
            &AnnIndex::active_config(ann_metric(h.metric)),
        ));
        inner.extend_from_slice(&(kind_codes.len() as u32).to_le_bytes());
        let mut kinds: Vec<(&String, &u32)> = kind_codes.iter().collect();
        kinds.sort_by_key(|&(_, code)| *code);
        for (kind, &code) in kinds {
            inner.extend_from_slice(&(kind.len() as u32).to_le_bytes());
            inner.extend_from_slice(kind.as_bytes());
            inner.extend_from_slice(&code.to_le_bytes());
        }
        inner.extend_from_slice(&body);

        // Seal under a fresh segment key; the pseudo-atom id binds the AAD and
        // owns the erasable slot. Drawn from the atom-id sequence, so it can
        // never collide with a real atom's slot.
        let pseudo_id = with_write_txn(conn, |c| next_id(c, "next_atom_id"))?;
        use rand::RngCore;
        let mut sk = Zeroizing::new([0u8; citadel_core::KEY_SIZE]);
        rand::thread_rng().fill_bytes(sk.as_mut());
        let seal_keys = derive_seal_keys(&sk);
        let sealed = blob_seal::seal(&seal_keys, pseudo_id as u64, &inner);
        let wrapped_sk = atom_wrap.wrap_atom_key(&sk);

        // Retire any previous segment first (old key must not survive as
        // decryptable residue), then key-before-data like atoms.
        self.retire_sealed_segment(h, conn, kl)?;
        let mut pending = PendingAtomSlots::new(Some(kl), 1);
        let seg_table = sealed_segment_table(&h.table, h.id);
        let result = (|| {
            let (slot, gen) = self
                .db
                .atom_store_allocate_write(pseudo_id as u64, &wrapped_sk)?;
            pending.track(slot, pseudo_id as u64, gen);

            {
                let mut wtx = self.db.begin_write()?;
                match wtx.drop_table(seg_table.as_bytes()) {
                    Ok(()) | Err(citadel_core::Error::TableNotFound(_)) => {}
                    Err(e) => return Err(e.into()),
                }
                wtx.create_table(seg_table.as_bytes())?;
                let chunk_count = sealed.len().div_ceil(SEALED_SEG_CHUNK) as u32;
                wtx.table_insert(
                    seg_table.as_bytes(),
                    &0u32.to_be_bytes(),
                    &chunk_count.to_le_bytes(),
                )?;
                for (i, chunk) in sealed.chunks(SEALED_SEG_CHUNK).enumerate() {
                    wtx.table_insert(seg_table.as_bytes(), &((i + 1) as u32).to_be_bytes(), chunk)?;
                }
                wtx.commit()?;
            }
            #[cfg(test)]
            if FAIL_SEALED_SEGMENT_AFTER_CHUNKS.with(std::cell::Cell::take) {
                return Err(MemError::Invalid(
                    "injected sealed-segment failure after chunk commit".into(),
                ));
            }
            write_annseg_meta(conn, h.id, slot, gen, pseudo_id)?;

            Ok(AnnSegmentInfo {
                segment_b3: *blake3::hash(&sealed).as_bytes(),
                content_fingerprint: fingerprint,
                n,
                dim: h.dim,
                metric_tag: citadel_vector::segment::metric_tag(ann_metric(h.metric)),
                chunk_count: sealed.len().div_ceil(SEALED_SEG_CHUNK) as u32,
            })
        })();
        match pending.finish(result) {
            Ok(info) => Ok(info),
            Err(source) => {
                // Finish every data cleanup step before surfacing the primary failure.
                match self.cleanup_failed_sealed_segment(conn, h.id, &seg_table) {
                    Ok(()) => Err(source),
                    Err(cleanup) => Err(MemError::Invalid(format!(
                        "{source}; additionally failed to clean pending sealed segment: {cleanup}"
                    ))),
                }
            }
        }
    }

    /// Try to serve the persisted segment: unwrap its key, decrypt, decode, and
    /// rehydrate from live rows whose fingerprint must match the sealed one.
    /// Any failure heals (retires the orphan key) and falls back to a scan
    /// build, carrying the reason in `Err(Some(reason))`.
    #[allow(clippy::type_complexity)]
    fn try_load_sealed_segment(
        &self,
        h: &RegionHandle,
        conn: &Connection<'_>,
        epoch: u64,
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<std::result::Result<SealedAnn, Option<String>>> {
        use zeroize::Zeroize;
        let atom_wrap = h.atom_wrap.as_ref().expect("sealed load");
        let Some((slot, gen, pseudo_id)) = read_annseg_meta(conn, h.id)? else {
            return Ok(Err(None));
        };
        let heal =
            |this: &Self, why: &str| -> Result<std::result::Result<SealedAnn, Option<String>>> {
                this.retire_sealed_segment(h, conn, kl)?;
                Ok(Err(Some(why.to_string())))
            };
        let rec = match self.db.atom_store_slot(slot) {
            Ok(rec) => rec,
            Err(e) => return heal(self, &format!("slot read: {e}")),
        };
        if rec.state != citadel::SlotState::Live
            || rec.region_id != pseudo_id as u64
            || rec.gen != gen
        {
            return heal(
                self,
                &format!(
                    "slot mismatch: state={:?} owner={} (want {pseudo_id}) gen={} (want {gen})",
                    rec.state, rec.region_id, rec.gen
                ),
            );
        }

        let seg_table = sealed_segment_table(&h.table, h.id);
        let sealed = {
            let mut rtx = self.db.begin_read();
            let Ok(Some(count_bytes)) = rtx.table_get(seg_table.as_bytes(), &0u32.to_be_bytes())
            else {
                return heal(self, "chunk count row missing");
            };
            let count = u32::from_le_bytes(match count_bytes.as_slice().try_into() {
                Ok(b) => b,
                Err(_) => return heal(self, "chunk count malformed"),
            });
            if !plausible_chunk_count(count) {
                return heal(self, "chunk count implausible");
            }
            let mut sealed = Vec::new();
            for i in 1..=count {
                match rtx.table_get(seg_table.as_bytes(), &i.to_be_bytes()) {
                    Ok(Some(chunk)) => sealed.extend_from_slice(&chunk),
                    _ => return heal(self, "chunk missing"),
                }
            }
            sealed
        };

        let mut sk = match atom_wrap.unwrap_atom_key(&rec.wrapped) {
            Ok(sk) => sk,
            Err(_) => return heal(self, "segment key unwrap failed"),
        };
        let seal_keys = derive_seal_keys(&sk);
        sk.zeroize();
        let inner = match blob_seal::open(&seal_keys, pseudo_id as u64, &sealed) {
            Ok(inner) => Zeroizing::new(inner),
            Err(_) => {
                eprintln!(
                    "citadel-mem: sealed ANN segment for region {} failed authenticated \
                     decryption (corrupt); rebuilding from scan",
                    h.id
                );
                return heal(self, "authenticated decryption failed");
            }
        };
        let parsed = parse_sealed_segment(&inner);
        let Some((stored_fp, stored_cfg, kind_codes, parts)) = parsed else {
            return heal(self, "inner parse/decode failed");
        };
        let active_cfg = citadel_vector::segment::prism_config_hash(&AnnIndex::active_config(
            ann_metric(h.metric),
        ));
        if stored_cfg != active_cfg {
            return heal(self, "prism config changed since the segment was built");
        }

        // Rehydrate by decrypting live rows, placed by the id_map permutation;
        // the recall cache comes from the same decrypt pass.
        let slot_of = parts.internal_of_row();
        let dim = h.dim as usize;
        let mut vectors = Zeroizing::new(vec![0.0f32; parts.n() * dim]);
        let mut filled = 0usize;
        let mut cached: FxHashMap<AtomId, CachedAtom> = FxHashMap::default();
        let mut unknown = false;
        let (live_fp, _) = sealed_fp_scan(
            conn,
            &self.db,
            h,
            &mut |id, kind, sealed_row, wrapped, score, created, immutable, expires| {
                let Some(&slot) = slot_of.get(&(id as u64)) else {
                    unknown = true;
                    return Ok(false);
                };
                let (emb, text, payload) = open_atom(atom_wrap, wrapped, id, sealed_row)?;
                let emb = Zeroizing::new(emb);
                vectors[slot as usize * dim..(slot as usize + 1) * dim].copy_from_slice(&emb);
                filled += 1;
                cached.insert(
                    id,
                    CachedAtom {
                        kind: kind.to_string(),
                        text,
                        payload,
                        importance: score,
                        created_micros: created,
                        immutable,
                        expires_micros: expires,
                    },
                );
                Ok(true)
            },
        )?;
        if unknown || live_fp != stored_fp || filled != parts.n() {
            // Stale (liveness or content moved): expected after forgets that
            // bypassed explicit retirement.
            return heal(
                self,
                &format!(
                    "stale: unknown={unknown} fp_match={} filled={filled}/{}",
                    live_fp == stored_fp,
                    parts.n()
                ),
            );
        }
        let segment_b3 = *blake3::hash(&sealed).as_bytes();
        // Pre-validate while the buffer is still zeroizing-owned; PRISM scrubs on drop.
        if filled != parts.n() || vectors.len() != parts.n() * dim {
            return heal(self, "sealed ANN vector rehydration shape changed");
        }
        let index = match parts.into_index(std::mem::take(vectors.as_mut()), filled) {
            Ok(i) => i,
            Err(e) => return heal(self, &format!("into_index: {e}")),
        };
        Ok(Ok(SealedAnn {
            index,
            kind_codes,
            cached,
            source: AnnIndexSource::Loaded { segment_b3 },
            // Entry epoch, never re-read: pre-bump data must not be stamped current.
            build_epoch: epoch,
        }))
    }

    /// Destroy the sealed segment's key slot (crypto-erasing all on-disk
    /// residue), delete its own chunk keys (other regions may share the tree),
    /// and clear the meta rows. Safe when nothing is persisted.
    fn retire_sealed_segment(
        &self,
        h: &RegionHandle,
        conn: &Connection<'_>,
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<()> {
        self.retire_sealed_segment_parts(conn, h.id, &h.table, kl)
    }

    /// Classify the chunk tree; only well-formed is Present, storage errors propagate.
    fn sealed_segment_tree_state(&self, seg_table: &str) -> Result<SegmentTreeState> {
        #[cfg(test)]
        if FAIL_SEGMENT_PROBE.with(std::cell::Cell::take) {
            return Err(MemError::Invalid("injected segment probe failure".into()));
        }
        let mut rtx = self.db.begin_read();
        let count_bytes = match rtx.table_get(seg_table.as_bytes(), &0u32.to_be_bytes()) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(SegmentTreeState::Incomplete),
            Err(citadel_core::Error::TableNotFound(_)) => return Ok(SegmentTreeState::Absent),
            Err(e) => return Err(e.into()),
        };
        let Ok(count) = <[u8; 4]>::try_from(count_bytes.as_slice()).map(u32::from_le_bytes) else {
            return Ok(SegmentTreeState::Incomplete);
        };
        if !plausible_chunk_count(count) {
            return Ok(SegmentTreeState::Incomplete);
        }
        for i in 1..=count {
            // Presence only - never materialize the ciphertext chunks.
            match rtx.table_contains_key(seg_table.as_bytes(), &i.to_be_bytes()) {
                Ok(true) => {}
                // The span is held: a hole is an interrupted persist, not a vanished tree.
                Ok(false) | Err(citadel_core::Error::TableNotFound(_)) => {
                    return Ok(SegmentTreeState::Incomplete)
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(SegmentTreeState::Present)
    }

    /// Drop a segment chunk tree by physical name; already-absent is fine.
    fn drop_segment_tree(&self, seg_table: &str) -> Result<()> {
        let mut wtx = self.db.begin_write()?;
        match wtx.drop_table(seg_table.as_bytes()) {
            Ok(()) | Err(citadel_core::Error::TableNotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        wtx.commit()?;
        Ok(())
    }

    /// Remove segment residue post-tombstone; tree before meta so a retry re-enters.
    fn cleanup_failed_sealed_segment(
        &self,
        conn: &Connection<'_>,
        region_id: RegionId,
        seg_table: &str,
    ) -> Result<()> {
        self.drop_segment_tree(seg_table)?;
        clear_annseg_meta(conn, region_id)
    }

    /// [`retire_sealed_segment`] by raw parts, for callers without a live
    /// handle. Key-first (fail-secure); a crash is finished by
    /// `reconcile_atom_store` at next open. Tolerates any resumed-retire slot
    /// state (tombstoned or recycled both mean the key is dead).
    fn retire_sealed_segment_parts(
        &self,
        conn: &Connection<'_>,
        region_id: RegionId,
        table: &str,
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<()> {
        let Some((slot, gen, pseudo_id)) = read_annseg_meta(conn, region_id)? else {
            return Ok(());
        };
        let rec = self.db.atom_store_slot(slot)?;
        // TOMBSTONE still scrubs torn siblings; Live must be fully bound and row-less.
        if rec.state == SlotState::Tombstone
            || (rec.state == SlotState::Live
                && rec.region_id == pseudo_id as u64
                && rec.gen == gen
                && !atom_row_exists_anywhere(conn, pseudo_id)?)
        {
            kl.atom_store_tombstone(slot, pseudo_id as u64)?;
        }
        self.drop_segment_tree(&sealed_segment_table(table, region_id))?;
        clear_annseg_meta(conn, region_id)?;
        Ok(())
    }

    pub fn fetch_one(&self, region: &str, atom_id: AtomId) -> Result<Option<AtomHit>> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                self.fetch_one_sealed(&h, atom_id, conn, atom_wrap)
            });
        }
        self.with_live_plain_access(&key, &h, |conn| {
            let qr = conn.query_params(
                &format!(
                    "SELECT id, kind, CAST(payload AS TEXT), text_content, score, immutable, created_at \
                     FROM {table} WHERE id = $1 AND region_id = $2 \
                     AND (expires_at IS NULL OR expires_at > $3)",
                    table = h.table
                ),
                &[
                    Value::Integer(atom_id),
                    Value::Integer(h.id),
                    Value::Timestamp(now_micros()),
                ],
            )?;
            qr.rows.first().map(|row| parse_fetched(row)).transpose()
        })
    }

    /// Most recent atom of `kind` in `region` (highest id), or `None`.
    pub fn fetch_last(&self, region: &str, kind: &str) -> Result<Option<AtomHit>> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                self.fetch_last_sealed(&h, kind, conn, atom_wrap)
            });
        }
        self.with_live_plain_access(&key, &h, |conn| {
            let qr = conn.query_params(
                &format!(
                    "SELECT id, kind, CAST(payload AS TEXT), text_content, score, immutable, created_at \
                     FROM {table} WHERE region_id = $1 AND kind = $2 \
                     AND (expires_at IS NULL OR expires_at > $3) ORDER BY id DESC LIMIT 1",
                    table = h.table
                ),
                &[
                    Value::Integer(h.id),
                    Value::Text(kind.into()),
                    Value::Timestamp(now_micros()),
                ],
            )?;
            qr.rows.first().map(|row| parse_fetched(row)).transpose()
        })
    }

    /// Read edges from `memory_edges`, filtered by any of `src`/`dst`/`kind`.
    pub fn fetch_edges(
        &self,
        src: Option<AtomId>,
        dst: Option<AtomId>,
        kind: Option<EdgeKind>,
    ) -> Result<Vec<Edge>> {
        let mut params: Vec<Value> = Vec::new();
        let mut clauses: Vec<String> = Vec::new();
        if let Some(s) = src {
            params.push(Value::Integer(s));
            clauses.push(format!("src_id = ${}", params.len()));
        }
        if let Some(d) = dst {
            params.push(Value::Integer(d));
            clauses.push(format!("dst_id = ${}", params.len()));
        }
        if let Some(k) = kind {
            params.push(Value::Text(k.as_str().into()));
            clauses.push(format!("kind = ${}", params.len()));
        }
        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };

        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            &format!(
                "SELECT src_id, dst_id, kind, weight, CAST(evidence_ref AS TEXT) \
                 FROM memory_edges{where_clause} ORDER BY src_id, dst_id, kind"
            ),
            &params,
        )?;
        qr.rows.iter().map(|row| parse_edge(row)).collect()
    }

    /// Replace an atom's JSONB payload; errors if it is absent or immutable.
    pub fn update_atom_payload(
        &self,
        region: &str,
        atom_id: AtomId,
        payload: &serde_json::Value,
    ) -> Result<()> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                // Epoch + retire precede the rewrite: the old payload survives nowhere.
                self.db.bump_cache_epoch();
                self.retire_sealed_segment(&h, conn, _kl)?;
                self.update_atom_payload_sealed(&key, &h, atom_id, payload, conn, atom_wrap)?;
                *h.ann.write().unwrap() = None;
                Ok(())
            });
        }
        let js = serde_json::to_string(payload)
            .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;

        self.with_live_plain_access(&key, &h, |conn| {
            with_write_txn(conn, |c| {
                let res = c.execute_params(
                    &format!(
                        "UPDATE {table} SET payload = CAST($1 AS JSONB) \
                         WHERE id = $2 AND region_id = $3 AND immutable = 0",
                        table = h.table
                    ),
                    &[
                        Value::Text(js.into()),
                        Value::Integer(atom_id),
                        Value::Integer(h.id),
                    ],
                )?;
                match res {
                    ExecutionResult::RowsAffected(0) => Err(MemError::Invalid(format!(
                        "atom {atom_id} not found, or immutable, in region '{key}'"
                    ))),
                    _ => Ok(()),
                }
            })
        })
    }

    /// Set fusion importance; same-value skips make a converged pass write nothing.
    pub fn set_importance(&self, region: &str, updates: &[(AtomId, f32)]) -> Result<usize> {
        if updates.is_empty() {
            return Ok(0);
        }
        if let Some((atom_id, _)) = updates
            .iter()
            .find(|(_, importance)| !importance.is_finite())
        {
            return Err(MemError::Invalid(format!(
                "importance for atom {atom_id} must be finite"
            )));
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let apply = |conn: &Connection<'_>| {
            with_write_txn(conn, |c| {
                let mut n = 0usize;
                for &(id, importance) in updates {
                    let res = c.execute_params(
                        &format!(
                            "UPDATE {table} SET score = $1 \
                             WHERE id = $2 AND region_id = $3 AND immutable = 0 \
                             AND score <> $1",
                            table = h.table
                        ),
                        &[
                            Value::Real(importance as f64),
                            Value::Integer(id),
                            Value::Integer(h.id),
                        ],
                    )?;
                    if !matches!(res, ExecutionResult::RowsAffected(0)) {
                        n += 1;
                    }
                }
                Ok(n)
            })
        };
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |conn, _, _kl| {
                let updated = apply(conn)?;
                if updated > 0 {
                    // The index bakes importance in; the bump drops all handles' caches.
                    self.db.bump_cache_epoch();
                    *h.ann.write().unwrap() = None;
                    self.retire_sealed_segment(&h, conn, _kl)?;
                }
                Ok(updated)
            });
        }
        self.with_live_plain_access(&key, &h, apply)
    }

    /// Hybrid recall: ANN retrieval then fusion re-ranking; top `q.k` atoms.
    ///
    /// Encrypted regions decrypt once into an in-RAM PRISM index over the whole
    /// region (cached; post-snapshot tail exact-ranked); keyword is in-Rust
    /// BM25, not SQL `ts_rank`.
    pub fn recall(&self, region: &str, q: RecallQuery) -> Result<Vec<AtomHit>> {
        self.recall_impl(region, q, true)
    }

    /// Multi-query recall: one batch embed, RRF merge with dedup, one rerank pass.
    pub fn recall_many(&self, region: &str, q: MultiRecallQuery) -> Result<Vec<AtomHit>> {
        if q.k == 0 || q.queries.is_empty() {
            return Ok(Vec::new());
        }
        validate_rrf_k(q.rrf_k, "multi-query RRF constant")?;
        for query in &q.queries {
            validate_fusion_weights(query.weights)?;
        }
        // Validate first: a bad RRF strategy must not partially execute sub-queries.
        let reranker = if q.rerank_query.is_some() {
            let snapshot = self.reranker.read().unwrap().clone();
            if let Some((_, strategy)) = &snapshot {
                validate_rerank_strategy(*strategy)?;
            }
            snapshot
        } else {
            None
        };
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;

        let mut queries = q.queries;
        let need: Vec<usize> = queries
            .iter()
            .enumerate()
            .filter(|(_, sq)| sq.embedding.is_none())
            .map(|(i, _)| i)
            .collect();
        if !need.is_empty() {
            let texts: Vec<&str> = need
                .iter()
                .map(|&i| {
                    queries[i].text.as_deref().ok_or_else(|| {
                        MemError::Invalid("recall requires either text or embedding".into())
                    })
                })
                .collect::<Result<_>>()?;
            let embs = h.embedder.embed_queries(&texts)?;
            if embs.len() != need.len() {
                return Err(MemError::Invalid(format!(
                    "embedder returned {} vectors for {} queries",
                    embs.len(),
                    need.len()
                )));
            }
            for (&i, e) in need.iter().zip(embs) {
                queries[i].embedding = Some(e);
            }
        }
        // A bad later vector must not leave earlier sub-queries partially observed.
        for query in &queries {
            if let Some(vector) = &query.embedding {
                validate_embedding(&key, h.dim, vector, "query")?;
            }
        }
        drop(h);

        let mut lists = Vec::with_capacity(queries.len());
        for sq in queries {
            lists.push(self.recall_impl(region, sq, false)?);
        }
        let mut merged = rrf_merge(lists, q.rrf_k);

        match (reranker.as_ref(), &q.rerank_query) {
            (Some((r, strategy)), Some(text)) => {
                Ok(rerank_hits(r.as_ref(), text, merged, *strategy, q.k)?)
            }
            _ => {
                merged.truncate(q.k);
                Ok(merged)
            }
        }
    }

    /// `use_reranker: false`: recall_many reranks the merged pool, not per sub-query.
    fn recall_impl(
        &self,
        region: &str,
        q: RecallQuery,
        use_reranker: bool,
    ) -> Result<Vec<AtomHit>> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if q.k == 0 {
            return Ok(Vec::new());
        }
        validate_fusion_weights(q.weights)?;
        // Validate before storage access so bad RRF config leaves no access accounting.
        let reranker = if use_reranker {
            let snapshot = self.reranker.read().unwrap().clone();
            if let Some((_, strategy)) = &snapshot {
                validate_rerank_strategy(*strategy)?;
            }
            snapshot
        } else {
            None
        };

        let qvec: Vec<f32> = match &q.embedding {
            Some(v) => v.clone(),
            None => {
                let text = q.text.as_deref().ok_or_else(|| {
                    MemError::Invalid("recall requires either text or embedding".into())
                })?;
                embed_query_one(&*h.embedder, text)?
            }
        };
        validate_embedding(&key, h.dim, &qvec, "query")?;

        if h.atom_wrap.is_some() {
            let cands = self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                self.recall_sealed_candidates(&h, &q, &qvec, conn, atom_wrap, _kl)
            })?;
            let as_of = q.as_of_micros.unwrap_or_else(now_micros);
            // Rerankers may re-enter the engine; the lifecycle lock is non-reentrant.
            let mut hits = match (reranker.as_ref(), &q.text) {
                (Some((r, strategy)), Some(text)) => {
                    fuse_rerank(r.as_ref(), text, cands, q.weights, as_of, *strategy, q.k)?
                }
                _ => fuse_rank(cands, q.weights, as_of, q.k),
            };
            if let Some(ge) = &q.graph_expand {
                let seeds: Vec<AtomId> = hits.iter().map(|hit| hit.id).collect();
                let present: FxHashSet<AtomId> = seeds.iter().copied().collect();
                let mut expanded =
                    self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                        let wrapped = self.db.atom_store_live_wrapped()?;
                        let scope = GraphFetchScope {
                            table: &h.table,
                            region_id: h.id,
                            kind_allowlist: &q.kinds,
                            payload_filter: q.payload_filter.as_ref(),
                        };
                        expand_graph_sealed(conn, atom_wrap, &wrapped, scope, &seeds, ge)
                    })?;
                expanded.retain(|hit| !present.contains(&hit.id));
                hits.extend(expanded);
            }
            self.note_access(h.id, hits.iter().map(|a| a.id));
            return Ok(hits);
        }

        let distop = match h.metric {
            EmbeddingMetric::Cosine => "<=>",
            EmbeddingMetric::L2 => "<->",
            EmbeddingMetric::InnerProduct => "<#>",
        };
        let table = h.table.clone();

        // $1 = query vector (reused in SELECT + ORDER BY), $2 = region_id.
        let mut params: Vec<Value> = vec![Value::Vector(qvec.into()), Value::Integer(h.id)];

        // Keyword rank uses the in-Rust BM25 primitive (assign_bm25_ranks)
        // shared with the sealed path; no SQL FTS, no language config.
        let mut where_parts = vec!["region_id = $2".to_string()];
        if !q.kinds.is_empty() {
            let mut ph = Vec::with_capacity(q.kinds.len());
            for kind in &q.kinds {
                params.push(Value::Text(kind.clone().into()));
                ph.push(format!("${}", params.len()));
            }
            where_parts.push(format!("kind IN ({})", ph.join(", ")));
        }
        if let Some(filter) = &q.payload_filter {
            let js = serde_json::to_string(filter)
                .map_err(|e| MemError::Invalid(format!("payload_filter not serializable: {e}")))?;
            params.push(Value::Text(js.into()));
            where_parts.push(format!("payload @> CAST(${} AS JSONB)", params.len()));
        }
        // TTL runs on the wall clock (a lapse is real-world time, unlike as_of
        // grading).
        params.push(Value::Timestamp(now_micros()));
        where_parts.push(format!(
            "(expires_at IS NULL OR expires_at > ${})",
            params.len()
        ));
        if !q.include_superseded {
            where_parts.push(
                "id NOT IN (SELECT dst_id FROM memory_edges WHERE kind = 'supersedes')".into(),
            );
        }

        // Over-fetch trades query latency for better ranking of keyword/recency
        // hits.
        let overfetch = q.k.saturating_mul(4).max(MIN_OVERFETCH);
        let sql = format!(
            "SELECT id, kind, CAST(payload AS TEXT), text_content, score, created_at, \
             embedding {distop} $1, 0.0, immutable \
             FROM {table} WHERE {} ORDER BY embedding {distop} $1 LIMIT {overfetch}",
            where_parts.join(" AND ")
        );

        let mut cands = self.with_live_plain_access(&key, &h, |conn| {
            let qr = conn.query_params(&sql, &params)?;
            qr.rows
                .iter()
                .map(|r| parse_candidate(r))
                .collect::<Result<Vec<_>>>()
        })?;
        let query_terms = query_keyword_terms(q.text.as_deref());
        assign_bm25_ranks(&mut cands, &query_terms);
        let as_of = q.as_of_micros.unwrap_or_else(now_micros);
        let mut hits = match (reranker.as_ref(), &q.text) {
            (Some((r, strategy)), Some(text)) => {
                fuse_rerank(r.as_ref(), text, cands, q.weights, as_of, *strategy, q.k)?
            }
            _ => fuse_rank(cands, q.weights, as_of, q.k),
        };

        if let Some(ge) = &q.graph_expand {
            let seeds: Vec<AtomId> = hits.iter().map(|h| h.id).collect();
            let present: FxHashSet<AtomId> = seeds.iter().copied().collect();
            let mut expanded = self.with_live_plain_access(&key, &h, |conn| {
                let scope = GraphFetchScope {
                    table: &table,
                    region_id: h.id,
                    kind_allowlist: &q.kinds,
                    payload_filter: q.payload_filter.as_ref(),
                };
                expand_graph(conn, scope, &seeds, ge)
            })?;
            expanded.retain(|e| !present.contains(&e.id));
            hits.extend(expanded);
        }
        self.note_access(h.id, hits.iter().map(|a| a.id));
        Ok(hits)
    }

    /// Create or update a directed edge; rejects cycles for acyclic kinds.
    pub fn link(&self, src: AtomId, dst: AtomId, kind: EdgeKind, weight: f32) -> Result<()> {
        self.link_with_evidence(src, dst, kind, weight, None)
    }

    /// [`link`](Self::link) plus a JSONB evidence payload; re-linking replaces both.
    pub fn link_with_evidence(
        &self,
        src: AtomId,
        dst: AtomId,
        kind: EdgeKind,
        weight: f32,
        evidence_ref: Option<serde_json::Value>,
    ) -> Result<()> {
        validate_edge_weight(weight)?;
        let conn = Connection::open(&self.db)?;
        with_write_txn(&conn, |c| {
            link_edge(c, src, dst, kind, weight, evidence_ref.as_ref())
        })
    }

    /// Every id must be live in the region: present, unexpired, and (sealed)
    /// its key slot bound by (state, owner, generation) like every sealed
    /// read. Runs inside the caller's write transaction.
    fn verify_atoms_live(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        region_key: &str,
        ids: &[AtomId],
    ) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let in_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let not_live = |id: AtomId| {
            MemError::Invalid(format!("atom {id} is not live in region '{region_key}'"))
        };
        if h.atom_wrap.is_some() {
            let qr = conn.query_params(
                &format!(
                    "SELECT id, key_slot, key_gen FROM {} WHERE region_id = $1 \
                     AND id IN ({in_list}) AND (expires_at IS NULL OR expires_at > $2)",
                    h.table
                ),
                &[Value::Integer(h.id), Value::Timestamp(now_micros())],
            )?;
            let mut found: FxHashMap<AtomId, (u32, u64)> = FxHashMap::default();
            for row in &qr.rows {
                found.insert(
                    as_int(&row[0])?,
                    (as_int(&row[1])? as u32, as_int(&row[2])? as u64),
                );
            }
            for &id in ids {
                let Some(&(slot, gen)) = found.get(&id) else {
                    return Err(not_live(id));
                };
                let rec = self.db.atom_store_slot(slot)?;
                if rec.state != SlotState::Live || rec.region_id != id as u64 || rec.gen != gen {
                    return Err(not_live(id));
                }
            }
            return Ok(());
        }
        let qr = conn.query_params(
            &format!(
                "SELECT id FROM {} WHERE region_id = $1 AND id IN ({in_list}) \
                 AND (expires_at IS NULL OR expires_at > $2)",
                h.table
            ),
            &[Value::Integer(h.id), Value::Timestamp(now_micros())],
        )?;
        let present: FxHashSet<AtomId> = qr
            .rows
            .iter()
            .map(|r| as_int(&r[0]))
            .collect::<Result<_>>()?;
        if let Some(&missing) = ids.iter().find(|id| !present.contains(id)) {
            return Err(not_live(missing));
        }
        Ok(())
    }

    /// Replace the COMPLETE outgoing `kind` edge set of `src` in one write
    /// transaction: src and every `(dst, weight, evidence)` must be live in
    /// `region`, weights and duplicate destinations validate up front, the
    /// set canonicalizes by ascending destination id, and an empty set
    /// clears. Other kinds are untouched. Acyclic-kind cycle checks run
    /// against the post-delete graph (removing edges can break cycles); a
    /// violation rolls the whole replacement back.
    ///
    /// Crate-private until `(src, kind)`-set ownership is a public contract.
    /// Cached-graph consumers must serialize mutation under a controller
    /// lock and reset [`DiffusionCache`](crate::DiffusionCache)/route memos
    /// after a successful replacement.
    // Runtime consumer arrives with the AgenticMemory controller slice.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn replace_outgoing_edges(
        &self,
        region: &str,
        src: AtomId,
        kind: EdgeKind,
        edges: &[(AtomId, f32, Option<serde_json::Value>)],
    ) -> Result<()> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let mut canonical: Vec<&(AtomId, f32, Option<serde_json::Value>)> = edges.iter().collect();
        canonical.sort_by_key(|e| e.0);
        for e in &canonical {
            validate_edge_weight(e.1)?;
            if e.0 == src {
                return Err(MemError::Cycle { src, dst: e.0 });
            }
        }
        if let Some(pair) = canonical.windows(2).find(|w| w[0].0 == w[1].0) {
            return Err(MemError::Invalid(format!(
                "duplicate destination atom {}",
                pair[0].0
            )));
        }
        let mut live_ids: Vec<AtomId> = canonical.iter().map(|e| e.0).collect();
        live_ids.push(src);
        let live_ids = dedup_sources(&live_ids);

        let conn = Connection::open(&self.db)?;
        // Serialize the liveness predicate with drop_region's key-first
        // erase span, as every encrypted region-bound write does.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        with_write_txn(&conn, |c| {
            self.verify_region_live(c, &h, &key)?;
            self.verify_atoms_live(c, &h, &key, &live_ids)?;
            c.execute_params(
                "DELETE FROM memory_edges WHERE src_id = $1 AND kind = $2",
                &[Value::Integer(src), Value::Text(kind.as_str().into())],
            )?;
            for e in &canonical {
                link_edge(c, src, e.0, kind, e.1, e.2.as_ref())?;
            }
            Ok(())
        })
        .inspect_err(|e| self.evict_stale_region(&key, h.id, e))
    }

    /// Recompute `SimilarTo` neighbor edges and score via recall; encrypted
    /// regions use the same full-region sealed ANN index.
    pub fn evolve(
        &self,
        region: &str,
        atom_id: AtomId,
        neighbors: usize,
        max_distance: f32,
    ) -> Result<EvolutionReport> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let table = h.table.clone();

        let (embedding, access_count, created) = if h.atom_wrap.is_some() {
            self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                let qr = conn.query_params(
                    &format!(
                        "SELECT sealed, access_count, created_at, key_slot, key_gen FROM {table} \
                         WHERE id = $1 AND region_id = $2"
                    ),
                    &[Value::Integer(atom_id), Value::Integer(h.id)],
                )?;
                let row = qr.rows.first().ok_or_else(|| {
                    MemError::Invalid(format!("atom {atom_id} not in region '{key}'"))
                })?;
                let rec = self.db.atom_store_slot(as_int(&row[3])? as u32)?;
                if rec.state != SlotState::Live
                    || rec.region_id != atom_id as u64
                    || rec.gen != as_int(&row[4])? as u64
                {
                    return Err(MemError::Invalid(format!(
                        "atom {atom_id} not in region '{key}'"
                    )));
                }
                let emb = open_atom_embedding(atom_wrap, &rec.wrapped, atom_id, as_blob(&row[0])?)?;
                Ok((emb, as_int(&row[1])?.max(0), as_ts(&row[2])))
            })?
        } else {
            self.with_live_plain_access(&key, &h, |conn| {
                let qr = conn.query_params(
                    &format!(
                        "SELECT embedding, access_count, created_at FROM {table} \
                         WHERE id = $1 AND region_id = $2"
                    ),
                    &[Value::Integer(atom_id), Value::Integer(h.id)],
                )?;
                let row = qr.rows.first().ok_or_else(|| {
                    MemError::Invalid(format!("atom {atom_id} not in region '{key}'"))
                })?;
                let embedding = match &row[0] {
                    Value::Vector(v) => v.to_vec(),
                    other => {
                        return Err(MemError::Invalid(format!(
                            "atom embedding not a vector: {other:?}"
                        )))
                    }
                };
                Ok((embedding, as_int(&row[1])?.max(0), as_ts(&row[2])))
            })?
        };

        let mut found = self.recall(
            &key,
            RecallQuery::by_embedding(embedding, neighbors.saturating_add(1)),
        )?;
        found.retain(|n| n.id != atom_id && n.distance <= max_distance);

        let age_days = (now_micros() - created).max(0) as f32 / 1e6 / 86_400.0;
        let recency = (-std::f32::consts::LN_2 * age_days / 30.0).exp();
        let new_score = recency * (1.0 + (access_count as f32).ln_1p());

        // Serialize the RSK liveness check with drop_region's key-first erase span.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let conn = Connection::open(&self.db)?;
        if h.atom_wrap.is_some() {
            // Epoch + retire precede the commit: pre-evolve scores survive nowhere.
            self.db.bump_cache_epoch();
            self.retire_sealed_segment(
                &h,
                &conn,
                _kl.as_ref().expect("sealed span holds the guard"),
            )?;
        }
        with_write_txn(&conn, |c| {
            self.verify_region_live(c, &h, &key)?;
            for n in &found {
                let weight = 1.0 / (1.0 + n.distance.max(0.0));
                link_edge(c, atom_id, n.id, EdgeKind::SimilarTo, weight, None)?;
            }
            c.execute_params(
                &format!("UPDATE {table} SET score = $1 WHERE id = $2 AND region_id = $3"),
                &[
                    Value::Real(new_score as f64),
                    Value::Integer(atom_id),
                    Value::Integer(h.id),
                ],
            )?;
            Ok(())
        })
        .inspect_err(|e| self.evict_stale_region(&key, h.id, e))?;
        // The cached recall index holds the pre-evolve score; rebuild it on
        // next recall.
        *h.ann.write().unwrap() = None;

        Ok(EvolutionReport {
            links_added: found.len(),
            score: new_score,
        })
    }

    /// Remove atoms matching `policy` and their edges; spares `immutable`
    /// except `PurgeRegion`. Evicted atoms are crypto-erased (key destroyed
    /// before the row); `PredicateMatch` decrypts each atom to test the
    /// payload, other policies use plaintext metadata columns.
    pub fn evict(&self, region: &str, policy: EvictionPolicy) -> Result<EvictionReport> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let table = h.table.clone();

        // Bind the whole eviction to one incarnation; a stale handle must not succeed.
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        if let Err(error) = self.verify_region_live(&conn, &h, &key) {
            self.evict_stale_region(&key, h.id, &error);
            return Err(error);
        }
        // Snapshot this region's in-process access stats out of the lock;
        // `Lru`/`Stale` layer them over the persisted insert-time floor.
        let accessed = self
            .access_stats
            .lock()
            .unwrap()
            .get(&h.id)
            .cloned()
            .unwrap_or_default();
        let ids = match (&h.atom_wrap, &policy) {
            // Payload containment can't be pushed to SQL over sealed rows;
            // filter in Rust after decrypt.
            (Some(atom_wrap), EvictionPolicy::PredicateMatch { predicate }) => {
                self.evict_predicate_sealed_ids(&h, predicate, &conn, atom_wrap.as_ref())?
            }
            _ => evict_target_ids(&conn, &table, h.id, &policy, now_micros(), &accessed)?,
        };
        if ids.is_empty() {
            return Ok(EvictionReport { removed: 0 });
        }

        let in_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        // Segment first, then keys, then rows; the full binding lets retries converge.
        if h.atom_wrap.is_some() {
            self.retire_sealed_segment(&h, &conn, &_kl)?;
            let qr = conn.query_params(
                &format!("SELECT id, key_slot, key_gen FROM {table} WHERE id IN ({in_list})"),
                &[],
            )?;
            let slots: Vec<(u32, u64, u64)> = qr
                .rows
                .iter()
                .map(|row| {
                    Ok((
                        as_int(&row[1])? as u32,
                        as_int(&row[0])? as u64,
                        as_int(&row[2])? as u64,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            _kl.atom_store_tombstone_batch(&slots)?;
        }

        with_write_txn(&conn, |c| {
            // Eviction targets come from region-scoped selectors, so the
            // unqualified by-atom-id identity purge cannot cross regions.
            c.execute(&format!(
                "DELETE FROM memory_idempotency WHERE atom_id IN ({in_list})"
            ))?;
            c.execute(&format!(
                "DELETE FROM memory_edges WHERE src_id IN ({in_list}) OR dst_id IN ({in_list})"
            ))?;
            c.execute(&format!("DELETE FROM {table} WHERE id IN ({in_list})"))?;
            Ok(())
        })?;
        *h.ann.write().unwrap() = None;
        Ok(EvictionReport {
            removed: ids.len() as u64,
        })
    }

    /// Destroy the keys of the region-scoped `ids` (overwrite + fsync +
    /// read-back). The (slot, id, gen) binding lets a retry over recycled
    /// crash residue skip it and still converge on the row delete.
    fn erase_atom_keys(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        in_list: &str,
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<Vec<SlotErasure>> {
        let slots = atom_key_slots(conn, h, in_list)?;
        Ok(kl
            .atom_store_tombstone_batch(&slots)?
            .into_iter()
            .map(|(slot, atom_id, old_gen, new_gen)| SlotErasure {
                slot,
                atom_id: atom_id as AtomId,
                old_gen,
                new_gen,
            })
            .collect())
    }

    /// Erase the keys of `ids` (encrypted only) then delete their rows and
    /// edges, returning `(rows_deleted, slots_erased)`.
    fn erase_and_delete(
        &self,
        region_key: &str,
        h: &RegionHandle,
        ids: &[AtomId],
    ) -> Result<(u64, Vec<SlotErasure>)> {
        // Tombstone -> row-delete -> segment-retire is one lifecycle span.
        let _kl = self.db.key_lifecycle_lock();
        let in_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let conn = Connection::open(&self.db)?;
        self.verify_region_live(&conn, h, region_key)?;

        // Encrypted: segment, keys, then rows; plaintext's row delete is the whole op.
        let slots_erased = if h.atom_wrap.is_some() {
            self.retire_sealed_segment(h, &conn, &_kl)?;
            self.erase_atom_keys(&conn, h, &in_list, &_kl)?
        } else {
            Vec::new()
        };

        #[cfg(test)]
        if FAIL_ERASE_BEFORE_ROW_DELETE.with(std::cell::Cell::take) {
            return Err(MemError::Invalid(
                "injected erase failure after key tombstone, before row delete".into(),
            ));
        }

        let rows_deleted = with_write_txn(&conn, |c| delete_atoms_in_txn(c, h, &in_list))?;
        *h.ann.write().unwrap() = None;
        Ok((rows_deleted, slots_erased))
    }

    /// Delete atoms. Encrypted regions crypto-erase each atom's key
    /// (overwrite/fsync/read-back) before its row, leaving it undecryptable on
    /// a crash. Privileged: ignores `immutable`;
    /// [`forget_atoms`](Self::forget_atoms) is the model-safe variant.
    pub fn delete_atoms(&self, region: &str, ids: &[AtomId]) -> Result<EvictionReport> {
        if ids.is_empty() {
            return Ok(EvictionReport { removed: 0 });
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        // Honest count: nonexistent/deleted ids do not inflate `removed`.
        let (rows_deleted, _) = self
            .erase_and_delete(&key, &h, ids)
            .inspect_err(|error| self.evict_stale_region(&key, h.id, error))?;
        Ok(EvictionReport {
            removed: rows_deleted,
        })
    }

    /// Forget atoms and return a verifiable [`ErasureReceipt`]. On an encrypted
    /// region each atom's key is cryptographically destroyed; on a plaintext
    /// region this is a logical delete (the receipt's `cryptographic_erasure`
    /// is false). Immutable atoms are skipped (reported in `immutable_skipped`)
    /// unless `force` is set.
    pub fn forget_atoms(
        &self,
        region: &str,
        ids: &[AtomId],
        force: bool,
    ) -> Result<ErasureReceipt> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let encrypted = h.atom_wrap.is_some();

        let mut immutable_skipped = Vec::new();
        let mut targets: Vec<AtomId> = ids.to_vec();
        {
            // Even an empty request must certify the incarnation, not a stale handle.
            let _kl = self.db.key_lifecycle_lock();
            let conn = Connection::open(&self.db)?;
            if let Err(error) = self.verify_region_live(&conn, &h, &key) {
                self.evict_stale_region(&key, h.id, &error);
                return Err(error);
            }
            if !force && !ids.is_empty() {
                let in_list = ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let qr = conn.query_params(
                    &format!(
                        "SELECT id FROM {} WHERE region_id = $1 AND id IN ({in_list}) AND immutable = 1",
                        h.table
                    ),
                    &[Value::Integer(h.id)],
                )?;
                let skip: FxHashSet<AtomId> = qr
                    .rows
                    .iter()
                    .map(|r| as_int(&r[0]))
                    .collect::<Result<_>>()?;
                if !skip.is_empty() {
                    targets.retain(|id| !skip.contains(id));
                    immutable_skipped = skip.into_iter().collect();
                    immutable_skipped.sort_unstable();
                }
            }
        }

        let (rows_deleted, slots_erased) = if targets.is_empty() {
            (0, Vec::new())
        } else {
            self.erase_and_delete(&key, &h, &targets)
                .inspect_err(|error| self.evict_stale_region(&key, h.id, error))?
        };

        Ok(build_erasure_receipt(
            encrypted,
            rows_deleted,
            slots_erased,
            immutable_skipped,
        ))
    }

    /// [`forget_atoms`](Self::forget_atoms) extended over the reverse
    /// `DerivedFrom` closure, so no derived copy of forgotten content
    /// survives. Classification, the closure walk, the immutable gate, key
    /// destruction, and every delete run in ONE write transaction: a
    /// concurrent writer observes the cascade entirely or not at all.
    ///
    /// Absent roots are ignored (a retry converges on a zero receipt); a
    /// root owned by another region fails loudly. Cross-table roots read as
    /// absent and the closure is region-scoped (regions are ownership
    /// boundaries) - both best-effort by design. Without `force`, any
    /// immutable atom in the closure refuses the whole cascade.
    pub fn forget_atoms_with_dependents(
        &self,
        region: &str,
        ids: &[AtomId],
        force: bool,
    ) -> Result<ErasureReceipt> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let encrypted = h.atom_wrap.is_some();

        // One lifecycle span; tombstoning inside the txn is single-writer
        // safe - a rollback leaves key-dead rows, healed by reconcile.
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        if encrypted {
            // Before the txn: a crash cannot leave erased codes under a live segment key.
            self.retire_sealed_segment(&h, &conn, &_kl)?;
        }
        #[cfg(test)]
        if FAIL_CASCADE_BEFORE_TXN.with(std::cell::Cell::take) {
            return Err(MemError::Invalid(
                "injected cascade failure after segment retire, before the txn".into(),
            ));
        }
        let (rows_deleted, slots_erased) = with_write_txn(&conn, |c| {
            self.verify_region_live(c, &h, &key)?;
            let roots = classify_cascade_roots(c, &h, &key, ids)?;
            if roots.is_empty() {
                return Ok((0, Vec::new()));
            }
            let closure = dependent_closure(c, &h, &roots)?;
            if !force {
                let blockers = immutable_members(c, &h, &closure)?;
                if !blockers.is_empty() {
                    return Err(MemError::Invalid(format!(
                        "cascade blocked by immutable atoms {blockers:?}; pass force to erase"
                    )));
                }
            }
            let in_list = closure
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let slots_erased = if encrypted {
                self.erase_atom_keys(c, &h, &in_list, &_kl)?
            } else {
                Vec::new()
            };
            let rows_deleted = delete_atoms_in_txn(c, &h, &in_list)?;
            Ok((rows_deleted, slots_erased))
        })
        .inspect_err(|e| self.evict_stale_region(&key, h.id, e))?;

        *h.ann.write().unwrap() = None;
        Ok(build_erasure_receipt(
            encrypted,
            rows_deleted,
            slots_erased,
            Vec::new(),
        ))
    }

    /// Crypto-erase a single atom: destroy its key (overwrite + fsync +
    /// read-back) and delete its row. Siblings and the region are untouched.
    pub fn forget_atom(&self, region: &str, id: AtomId) -> Result<()> {
        self.delete_atoms(region, &[id]).map(|_| ())
    }

    /// Re-authenticate atoms by id, one [`AtomAttestation`] per id in order.
    /// Reads sealed bytes fresh from disk (not the recall cache) and recomputes
    /// the id-bound HMAC, catching tampering or a blob replayed from another
    /// row. Never aborts; every id gets a verdict.
    pub fn verify_atoms(&self, region: &str, ids: &[AtomId]) -> Result<Vec<AtomAttestation>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let table = &h.table;
        let in_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                self.verify_atoms_sealed(&h, ids, &in_list, conn, atom_wrap)
            });
        }

        // Plaintext region: no per-atom MAC. Present ids are
        // PlaintextUnattested, absent ids are Missing.
        self.with_live_plain_access(&key, &h, |conn| {
            let qr = conn.query_params(
                &format!("SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list})"),
                &[Value::Integer(h.id)],
            )?;
            let present: FxHashSet<AtomId> = qr
                .rows
                .iter()
                .map(|r| as_int(&r[0]))
                .collect::<Result<_>>()?;
            Ok(ids
                .iter()
                .map(|&id| AtomAttestation {
                    atom_id: id,
                    verdict: if present.contains(&id) {
                        AttestVerdict::PlaintextUnattested
                    } else {
                        AttestVerdict::Missing
                    },
                    aad_bound: false,
                    key_slot: None,
                    key_gen: None,
                })
                .collect())
        })
    }

    fn verify_atoms_sealed(
        &self,
        h: &RegionHandle,
        ids: &[AtomId],
        in_list: &str,
        conn: &Connection<'_>,
        atom_wrap: &AtomWrapKey,
    ) -> Result<Vec<AtomAttestation>> {
        // Encrypted region: read sealed + key binding fresh (off the recall
        // cache), then re-authenticate each off disk.
        let qr = conn.query_params(
            &format!(
                "SELECT id, key_slot, sealed, key_gen FROM {table} \
                 WHERE region_id = $1 AND id IN ({in_list})",
                table = h.table
            ),
            &[Value::Integer(h.id)],
        )?;
        let mut found: FxHashMap<AtomId, (u32, Vec<u8>, u64)> = FxHashMap::default();
        for row in &qr.rows {
            let id = as_int(&row[0])?;
            let slot = as_int(&row[1])? as u32;
            let sealed = match &row[2] {
                Value::Blob(b) => b.clone(),
                _ => return Err(MemError::Invalid("sealed column is not a blob".into())),
            };
            let gen = as_int(&row[3])? as u64;
            found.insert(id, (slot, sealed, gen));
        }

        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            let Some((slot, sealed, row_gen)) = found.get(&id) else {
                out.push(AtomAttestation {
                    atom_id: id,
                    verdict: AttestVerdict::Missing,
                    aad_bound: false,
                    key_slot: None,
                    key_gen: None,
                });
                continue;
            };
            let rec = self.db.atom_store_slot(*slot)?;
            // The key is gone if the slot is tombstoned, recycled to another
            // owner, or at a different generation: a Live slot attests this row
            // only if owner and generation both bind.
            if rec.state != SlotState::Live || rec.region_id != id as u64 || rec.gen != *row_gen {
                out.push(AtomAttestation {
                    atom_id: id,
                    verdict: AttestVerdict::KeyErased,
                    aad_bound: false,
                    key_slot: Some(*slot),
                    key_gen: Some(rec.gen),
                });
                continue;
            }
            let (verdict, aad_bound) = match atom_wrap.unwrap_atom_key(&rec.wrapped) {
                Ok(mut ack) => {
                    let seal_keys = derive_seal_keys(&ack);
                    ack.zeroize();
                    // HMAC recomputed with aad = atom id, so a flipped byte
                    // (CTR is malleable) or a replayed blob both fail here.
                    match blob_seal::open(&seal_keys, id as u64, sealed) {
                        Ok(mut pt) => {
                            pt.zeroize();
                            (AttestVerdict::Authentic, true)
                        }
                        Err(_) => (AttestVerdict::Tampered, true),
                    }
                }
                // A live slot whose wrapped ACK won't unwrap = slot corruption.
                Err(_) => (AttestVerdict::Tampered, false),
            };
            out.push(AtomAttestation {
                atom_id: id,
                verdict,
                aad_bound,
                key_slot: Some(*slot),
                key_gen: Some(rec.gen),
            });
        }
        Ok(out)
    }

    /// Per-kind counts, time span, and avg score/confidence since
    /// `since_micros` (no LLM).
    pub fn summarize(&self, region: &str, since_micros: i64) -> Result<SummaryReport> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let read = |conn: &Connection<'_>| -> Result<SummaryReport> {
            let qr = conn.query_params(
                &format!(
                    "SELECT kind, COUNT(*), MIN(created_at), MAX(created_at), AVG(score), AVG(confidence) \
                     FROM {table} WHERE region_id = $1 AND created_at > $2 \
                     AND (expires_at IS NULL OR expires_at > $3) GROUP BY kind",
                    table = h.table
                ),
                &[
                    Value::Integer(h.id),
                    Value::Timestamp(since_micros),
                    Value::Timestamp(now_micros()),
                ],
            )?;

            let mut kinds = Vec::with_capacity(qr.rows.len());
            let mut total = 0u64;
            for row in &qr.rows {
                let count = as_int(&row[1])?.max(0) as u64;
                total += count;
                kinds.push(KindDigest {
                    kind: as_text(&row[0])?.to_string(),
                    count,
                    earliest: as_ts(&row[2]),
                    latest: as_ts(&row[3]),
                    avg_score: as_f32(&row[4]),
                    avg_confidence: as_f32(&row[5]),
                });
            }
            Ok(SummaryReport { total, kinds })
        };
        if h.atom_wrap.is_some() {
            self.with_live_sealed_read(&key, &h, |conn, _, _kl| read(conn))
        } else {
            self.with_live_plain_access(&key, &h, read)
        }
    }

    /// Evict the stale entry only if it still holds the failed write's incarnation.
    fn evict_stale_region(&self, key: &str, id: RegionId, err: &MemError) {
        if !matches!(err, MemError::RegionNotFound(_)) {
            return;
        }
        self.remove_attached_incarnation(key, id);
    }

    fn remove_attached_incarnation(&self, key: &str, id: RegionId) {
        self.detach_attached_region(key, Some(id));
    }

    /// Detach and scrub the shared ANN cache so stale handles cannot hold plaintext.
    fn detach_attached_region(&self, key: &str, expected_id: Option<RegionId>) {
        let detached = {
            let mut guard = self.regions.lock().unwrap();
            let matches = expected_id.is_none_or(|id| guard.get(key).is_some_and(|st| st.id == id));
            matches.then(|| guard.remove(key)).flatten()
        };
        if let Some(state) = detached {
            *state.ann.write().unwrap() = None;
        }
    }

    /// Revalidate inside the caller's txn; a row left by a partial erase is NOT live.
    fn verify_region_live(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        region_key: &str,
    ) -> Result<()> {
        let not_found = || MemError::RegionNotFound(region_key.into());
        let qr = conn.query_params(
            "SELECT name, encrypted, rsk_slot, rsk_gen FROM memory_regions WHERE id = $1",
            &[Value::Integer(h.id)],
        )?;
        let Some(row) = qr.rows.first() else {
            return Err(not_found());
        };
        let encrypted = as_exact_bool(&row[1], "encrypted")?;
        if as_text(&row[0])? != region_key || encrypted != h.atom_wrap.is_some() {
            return Err(not_found());
        }
        if !encrypted {
            return Ok(());
        }

        let slot = opt_u32(&row[2])?.ok_or_else(&not_found)?;
        let generation = opt_u64(&row[3]).ok_or_else(&not_found)?;
        let rec = self.db.region_store_slot(slot)?;
        if rec.state != SlotState::Live || rec.region_id != h.id as u64 || rec.gen != generation {
            return Err(not_found());
        }
        Ok(())
    }

    /// Spans the RSK check + callback so reads cannot race a key-first drop; no re-entry.
    fn with_live_sealed_read<T>(
        &self,
        region_key: &str,
        h: &RegionHandle,
        read: impl FnOnce(&Connection<'_>, &AtomWrapKey, &KeyLifecycleGuard<'_>) -> Result<T>,
    ) -> Result<T> {
        let atom_wrap = h
            .atom_wrap
            .as_deref()
            .expect("with_live_sealed_read on plaintext region");
        let kl = self.db.key_lifecycle_lock();
        let result = (|| {
            let conn = Connection::open(&self.db)?;
            self.verify_region_live(&conn, h, region_key)?;
            read(&conn, atom_wrap, &kl)
        })();
        result.inspect_err(|e| self.evict_stale_region(region_key, h.id, e))
    }

    /// Span stops a cross-engine drop/recreate interleave; callbacks must not re-enter.
    fn with_live_plain_access<T>(
        &self,
        region_key: &str,
        h: &RegionHandle,
        access: impl FnOnce(&Connection<'_>) -> Result<T>,
    ) -> Result<T> {
        debug_assert!(h.atom_wrap.is_none());
        let _kl = self.db.key_lifecycle_lock();
        let result = (|| {
            let conn = Connection::open(&self.db)?;
            self.verify_region_live(&conn, h, region_key)?;
            access(&conn)
        })();
        result.inspect_err(|e| self.evict_stale_region(region_key, h.id, e))
    }

    fn region_handle(&self, key: &str) -> Result<RegionHandle> {
        let guard = self.regions.lock().unwrap();
        let st = guard
            .get(key)
            .ok_or_else(|| MemError::RegionNotFound(key.into()))?;
        Ok(RegionHandle {
            id: st.id,
            table: atoms_table(st.dim, st.metric, st.atom_wrap.is_some()),
            embedder: Arc::clone(&st.embedder),
            dim: st.dim,
            metric: st.metric,
            atom_wrap: st.atom_wrap.clone(),
            identity_mac: st.identity_mac.clone(),
            ann: Arc::clone(&st.ann),
            max_id: Arc::clone(&st.max_id),
        })
    }

    /// Check the id before the embedder: stale config must not veto a successor.
    fn check_attached_incarnation(
        &self,
        key: &str,
        persisted_id: RegionId,
        dim: u16,
        metric: EmbeddingMetric,
        model_id: &str,
        encrypted: bool,
    ) -> Result<Option<RegionId>> {
        let stale = self
            .regions
            .lock()
            .unwrap()
            .get(key)
            .is_some_and(|st| st.id != persisted_id);
        if stale {
            // Evict first: failed successor validation must leave reads unattached.
            self.detach_attached_region(key, None);
            return Ok(None);
        }
        self.check_attached(key, dim, metric, model_id, encrypted)
    }

    /// Return the id if `key` is attached and the embedder matches; error on
    /// mismatch.
    fn check_attached(
        &self,
        key: &str,
        dim: u16,
        metric: EmbeddingMetric,
        model_id: &str,
        encrypted: bool,
    ) -> Result<Option<RegionId>> {
        let guard = self.regions.lock().unwrap();
        let Some(st) = guard.get(key) else {
            return Ok(None);
        };
        if st.dim != dim {
            return Err(MemError::DimMismatch {
                region: key.into(),
                expected: st.dim,
                got: dim as usize,
            });
        }
        if st.metric != metric {
            return Err(MemError::MetricMismatch {
                region: key.into(),
                expected: metric_tag(st.metric).into(),
                got: metric_tag(metric).into(),
            });
        }
        if st.embedder.model_id() != model_id {
            return Err(MemError::ModelMismatch {
                region: key.into(),
                expected: st.embedder.model_id().into(),
                got: model_id.into(),
            });
        }
        if st.atom_wrap.is_some() != encrypted {
            return Err(MemError::Invalid(format!(
                "region '{key}' already attached with encrypted={}",
                st.atom_wrap.is_some()
            )));
        }
        Ok(Some(st.id))
    }

    fn load_region_row(&self, conn: &Connection<'_>, key: &str) -> Result<Option<RegionRow>> {
        let qr = conn.query_params(
            "SELECT id, embedding_dim, embedding_metric, model_id, encrypted, rsk_slot, rsk_gen \
             FROM memory_regions WHERE name = $1",
            &[Value::Text(key.into())],
        )?;
        let Some(row) = qr.rows.first() else {
            return Ok(None);
        };
        Ok(Some(parse_region_row(row)?))
    }

    /// Encrypted rows must hold a live RSK binding; inventory callers hold the span.
    fn load_live_region_row(&self, conn: &Connection<'_>, key: &str) -> Result<RegionRow> {
        let Some(row) = self.load_region_row(conn, key)? else {
            return Err(MemError::RegionNotFound(key.to_owned()));
        };
        if row.encrypted {
            self.verify_region_key_live(key, &row)?;
        }
        Ok(row)
    }

    fn insert_region(
        &self,
        conn: &Connection<'_>,
        key: &str,
        dim: u16,
        metric: EmbeddingMetric,
        model_id: &str,
    ) -> Result<RegionId> {
        with_write_txn(conn, |c| {
            let id = next_id(c, "next_region_id")?;
            c.execute_params(
                "INSERT INTO memory_regions \
                 (id, name, embedding_dim, embedding_metric, model_id, encrypted, created_at, metadata) \
                 VALUES ($1, $2, $3, $4, $5, 0, CURRENT_TIMESTAMP, NULL)",
                &[
                    Value::Integer(id),
                    Value::Text(key.into()),
                    Value::Integer(dim as i64),
                    Value::Text(metric_tag(metric).into()),
                    Value::Text(model_id.into()),
                ],
            )?;
            ensure_atoms_table(c, dim, metric, false)?;
            Ok(id)
        })
    }
}

/// Encrypted-region paths: sealed writes, decrypt-then-rank reads, and key
/// lifecycle.
impl MemoryEngine {
    /// `MAX(id)` per region from one `GROUP BY region_id` scan (a per-region
    /// full scan was O(R x table)); tagged with the commit generation, so a hit
    /// is never stale.
    fn reattach_max_id(
        &self,
        conn: &Connection<'_>,
        table: &str,
        region_id: RegionId,
    ) -> Result<i64> {
        let generation = self.db.manager().commit_generation();
        if let Some((tagged, snap)) = self.attach_max.lock().unwrap().get(table) {
            if *tagged == generation {
                return Ok(snap.get(&region_id).copied().unwrap_or(0));
            }
        }
        let mut snap = FxHashMap::default();
        if conn.table_schema(table).is_some() {
            let qr = conn.query_params(
                &format!("SELECT region_id, MAX(id) FROM {table} GROUP BY region_id"),
                &[],
            )?;
            for row in &qr.rows {
                snap.insert(as_int(&row[0])?, as_int(&row[1])?);
            }
        }
        let max = snap.get(&region_id).copied().unwrap_or(0);
        self.attach_max
            .lock()
            .unwrap()
            .insert(table.to_string(), (generation, snap));
        Ok(max)
    }

    /// Attach an existing encrypted region: read its live slot, unwrap the RCK,
    /// derive the atom-wrap key. `RegionForgotten` if the slot was tombstoned
    /// or its generation moved.
    fn live_region_wrapped(&self, name: &str, row: &RegionRow) -> Result<[u8; WRAPPED_KEY_SIZE]> {
        let slot = row
            .rsk_slot
            .ok_or_else(|| MemError::RegionForgotten(name.into()))?;
        let expected_gen = row
            .rsk_gen
            .ok_or_else(|| MemError::RegionForgotten(name.into()))?;
        let rec = self.db.region_store_slot(slot)?;
        if rec.state != SlotState::Live || rec.gen != expected_gen || rec.region_id != row.id as u64
        {
            return Err(MemError::RegionForgotten(name.into()));
        }
        Ok(rec.wrapped)
    }

    fn verify_region_key_live(&self, name: &str, row: &RegionRow) -> Result<()> {
        self.live_region_wrapped(name, row).map(|_| ())
    }

    fn attach_region_key(&self, name: &str, row: &RegionRow) -> Result<RegionKeys> {
        let wrapped = self.live_region_wrapped(name, row)?;
        let mut rck = self.db.unwrap_region_key(&wrapped)?;
        let atom_wrap = derive_atom_wrap_key(&rck);
        let identity_mac = derive_identity_mac_key(&rck);
        rck.zeroize();
        Ok(RegionKeys {
            atom_wrap: Arc::new(atom_wrap),
            identity_mac: Arc::new(identity_mac),
        })
    }

    /// Create a new encrypted region: generate a random RCK, wrap it, persist
    /// the live slot (fsync'd) before inserting the region row, and return the
    /// atom-wrap key.
    fn insert_encrypted_region(
        &self,
        conn: &Connection<'_>,
        key: &str,
        dim: u16,
        metric: EmbeddingMetric,
        model_id: &str,
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<(RegionId, Option<RegionKeys>)> {
        use rand::RngCore;

        // Caller holds the key-lifecycle guard across allocate -> row commit
        // so a concurrent reconcile/drop cannot tombstone the key mid-span.
        // Reserve the region id first so the key slot binds to it.
        let id = with_write_txn(conn, |c| next_id(c, "next_region_id"))?;

        let mut rck = Zeroizing::new([0u8; citadel_core::KEY_SIZE]);
        rand::thread_rng().fill_bytes(rck.as_mut());
        let wrapped = Zeroizing::new(self.db.wrap_region_key(&rck)?);

        // Persist the wrapped key (fsync'd) before the row, so a committed
        // region row always references a durable key.
        let (slot, gen) = self.db.region_store_allocate_write(id as u64, &wrapped)?;
        let pending = PendingRegionSlot::new(kl, slot, id as u64);

        #[cfg(test)]
        if FAIL_ENCRYPTED_REGION_AFTER_SLOT.with(std::cell::Cell::take) {
            FAILED_ENCRYPTED_REGION_WRAPPED_KEY.with(|captured| {
                *captured.borrow_mut() = Some(Zeroizing::new(*wrapped));
            });
            return pending.finish(Err(MemError::Invalid(
                "injected encrypted-region failure after key-slot allocation".into(),
            )));
        }

        let inserted = with_write_txn(conn, |c| {
            c.execute_params(
                "INSERT INTO memory_regions \
                 (id, name, embedding_dim, embedding_metric, model_id, encrypted, rsk_slot, rsk_gen, created_at, metadata) \
                 VALUES ($1, $2, $3, $4, $5, 1, $6, $7, CURRENT_TIMESTAMP, NULL)",
                &[
                    Value::Integer(id),
                    Value::Text(key.into()),
                    Value::Integer(dim as i64),
                    Value::Text(metric_tag(metric).into()),
                    Value::Text(model_id.into()),
                    Value::Integer(slot as i64),
                    Value::Integer(gen as i64),
                ],
            )?;
            ensure_atoms_table(c, dim, metric, true)?;
            Ok(())
        });
        pending.finish(inserted)?;

        let keys = RegionKeys {
            atom_wrap: Arc::new(derive_atom_wrap_key(&rck)),
            identity_mac: Arc::new(derive_identity_mac_key(&rck)),
        };
        Ok((id, Some(keys)))
    }

    /// ANN recall over an encrypted region via an ephemeral in-RAM PRISM index
    /// from decrypted vectors (no ANN/FTS index runs over ciphertext); cached
    /// per region and zeroized on drop.
    ///
    /// Supersession, expiry and the payload filter read plaintext, so they can
    /// only discard after the window is cut. Widen until `k` survive or the
    /// window spans the region.
    fn recall_sealed_candidates(
        &self,
        h: &RegionHandle,
        q: &RecallQuery,
        qvec: &[f32],
        conn: &Connection<'_>,
        atom_wrap: &AtomWrapKey,
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<Vec<Candidate>> {
        let mut cand_k = q.k.saturating_mul(CAND_OVERFETCH).max(MIN_CANDIDATES);
        loop {
            let (cands, spanned) =
                self.sealed_window_candidates(h, q, qvec, conn, atom_wrap, kl, cand_k)?;
            // Short is ambiguous: survivors ran out, or the window did. The window
            // is the nearest `cand_k`, so widening only appends.
            if spanned || cands.len() >= q.k {
                return Ok(cands);
            }
            cand_k = cand_k.saturating_mul(2);
        }
    }

    /// Decrypted candidates for one candidate window, and whether that window
    /// already spanned every atom the scan can reach.
    #[allow(clippy::too_many_arguments)]
    fn sealed_window_candidates(
        &self,
        h: &RegionHandle,
        q: &RecallQuery,
        qvec: &[f32],
        conn: &Connection<'_>,
        atom_wrap: &AtomWrapKey,
        kl: &KeyLifecycleGuard<'_>,
        cand_k: usize,
    ) -> Result<(Vec<Candidate>, bool)> {
        let table = &h.table;
        let mut ranked = self.sealed_ann_candidates(h, conn, qvec, q, cand_k, kl)?;
        // Fewer ids than asked for means every atom was reached. Read before the
        // retains below, which shrink it for reasons that are not exhaustion.
        let spanned = ranked.len() < cand_k;
        if !q.include_superseded {
            // Drop stale versions before any cache read or decrypt.
            let ids: Vec<AtomId> = ranked.iter().map(|&(id, _)| id).collect();
            let stale = superseded_ids(conn, &ids)?;
            ranked.retain(|(id, _)| !stale.contains(id));
        }
        if ranked.is_empty() {
            return Ok((Vec::new(), spanned));
        }

        // Build candidates from the index-build cache, so the hot path touches
        // no SQL, decryption, or key store. Cached atoms are all live (any
        // change rebuilds the index); only post-snapshot tail atoms miss and
        // fall through to the fetch + decrypt below.
        let query_terms = query_keyword_terms(q.text.as_deref());
        // TTL runs on the wall clock (unlike as_of grading).
        let ttl_now = now_micros();
        let mut cands: Vec<Candidate> = Vec::with_capacity(ranked.len());
        let mut misses: Vec<(AtomId, f32)> = Vec::new();
        {
            let guard = h.ann.read().unwrap();
            let cache = guard.as_ref().map(|sa| &sa.cached);
            for &(id, dist) in &ranked {
                match cache.and_then(|c| c.get(&id)) {
                    Some(ca) => {
                        if ca.expires_micros.is_some_and(|e| e <= ttl_now) {
                            continue;
                        }
                        if let Some(filter) = &q.payload_filter {
                            if !json_contains(&ca.payload, filter) {
                                continue;
                            }
                        }
                        cands.push(Candidate {
                            id,
                            kind: ca.kind.clone(),
                            text: ca.text.clone(),
                            payload: ca.payload.clone(),
                            dist,
                            text_rank: 0.0,
                            importance: ca.importance,
                            created_micros: ca.created_micros,
                            immutable: ca.immutable,
                        });
                    }
                    None => misses.push((id, dist)),
                }
            }
        }

        // Tail / cache-miss atoms: fetch and decrypt only these (empty in
        // steady state, so the key store is read only when the index lags).
        if !misses.is_empty() {
            let wrapped = self.db.atom_store_live_wrapped()?;
            let id_params: Vec<Value> = misses.iter().map(|(id, _)| Value::Integer(*id)).collect();
            let placeholders = (1..=id_params.len())
                .map(|i| format!("${i}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT id, kind, sealed, score, created_at, immutable FROM {table} \
                 WHERE id IN ({placeholders}) \
                 AND (expires_at IS NULL OR expires_at > ${})",
                id_params.len() + 1
            );
            let mut id_params = id_params;
            id_params.push(Value::Timestamp(ttl_now));
            let dist_by_id: FxHashMap<AtomId, f32> = misses.iter().copied().collect();
            let qr = conn.query_params(&sql, &id_params)?;
            for row in &qr.rows {
                let id = as_int(&row[0])?;
                let Some(w) = wrapped.get(&(id as u64)) else {
                    continue;
                };
                let (mut text, mut payload) =
                    open_atom_content(atom_wrap, w, id, as_blob(&row[2])?)?;
                if let Some(filter) = &q.payload_filter {
                    if !json_contains(&payload, filter) {
                        zeroize_atom_content(&mut text, &mut payload);
                        continue;
                    }
                }
                cands.push(Candidate {
                    id,
                    kind: as_text(&row[1])?.to_string(),
                    text,
                    payload,
                    dist: dist_by_id.get(&id).copied().unwrap_or(f32::MAX),
                    text_rank: 0.0,
                    importance: as_f32(&row[3]),
                    created_micros: as_ts(&row[4]),
                    immutable: as_bool(&row[5]),
                });
            }
        }

        assign_bm25_ranks(&mut cands, &query_terms);
        Ok((cands, spanned))
    }

    /// Top `cand_k` `(atom_id, distance)` for a sealed region: search the
    /// cached PRISM index (rebuilt if stale) plus an exact scan of atoms
    /// inserted after the snapshot.
    fn sealed_ann_candidates(
        &self,
        h: &RegionHandle,
        conn: &Connection<'_>,
        qvec: &[f32],
        q: &RecallQuery,
        cand_k: usize,
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<Vec<(AtomId, f32)>> {
        let atom_wrap = h
            .atom_wrap
            .as_ref()
            .expect("sealed_ann_candidates on plaintext region");
        let max_id = h.max_id.load(Ordering::Relaxed);
        // Stable within this call: the held guard excludes key destruction.
        let epoch = self.db.cache_epoch();

        // Fast path: a fresh index searches under a shared read lock (recalls
        // don't serialize).
        {
            let guard = h.ann.read().unwrap();
            if let Some(sa) = guard.as_ref() {
                if !sealed_index_stale(sa, max_id, epoch) {
                    return search_sealed_index(
                        sa, qvec, q, cand_k, conn, atom_wrap, &self.db, h, max_id,
                    );
                }
            }
        }

        // Slow path under the write lock (re-checked in case another writer
        // won): load the segment if it verifies, else rebuild from a scan.
        {
            let mut guard = h.ann.write().unwrap();
            let need_full = guard
                .as_ref()
                .map(|sa| sealed_index_stale(sa, max_id, epoch))
                .unwrap_or(true);
            if need_full {
                let load = self.try_load_sealed_segment(h, conn, epoch, kl)?;
                if let Ok(loaded) = load {
                    *guard = Some(loaded);
                } else {
                    let refusal = load.err().flatten();
                    // Pre-scan stamp: a heal may have bumped the epoch past the entry stamp.
                    let build_epoch = self.db.cache_epoch();
                    let mut rows = decrypt_scan(conn, &self.db, atom_wrap, &h.table, h.id, None)?;
                    if rows.is_empty() {
                        *guard = None;
                        return Ok(Vec::new());
                    }
                    let mut kind_codes: FxHashMap<String, u32> = FxHashMap::default();
                    let mut cached: FxHashMap<AtomId, CachedAtom> = FxHashMap::default();
                    let triples: Vec<(u64, Vec<f32>, Vec<u32>)> = rows
                        .drain()
                        .map(
                            |(
                                id,
                                emb,
                                kind,
                                text,
                                payload,
                                importance,
                                created_micros,
                                immutable,
                                expires_micros,
                            )| {
                                let next = kind_codes.len() as u32;
                                let code = *kind_codes.entry(kind.clone()).or_insert(next);
                                cached.insert(
                                    id,
                                    CachedAtom {
                                        kind,
                                        text,
                                        payload,
                                        importance,
                                        created_micros,
                                        immutable,
                                        expires_micros,
                                    },
                                );
                                (id as u64, emb, vec![code])
                            },
                        )
                        .collect();
                    let index = AnnIndex::build_with_attrs(triples, 1, ann_metric(h.metric), h.dim)
                        .map_err(|e| MemError::Invalid(format!("sealed ANN index build: {e}")))?;
                    *guard = Some(SealedAnn {
                        index,
                        kind_codes,
                        cached,
                        source: AnnIndexSource::Built { refusal },
                        build_epoch,
                    });
                }
            }
        }

        let guard = h.ann.read().unwrap();
        let Some(sa) = guard.as_ref() else {
            return Ok(Vec::new());
        };
        search_sealed_index(sa, qvec, q, cand_k, conn, atom_wrap, &self.db, h, max_id)
    }

    fn fetch_sealed(
        &self,
        h: &RegionHandle,
        q: &FetchQuery,
        conn: &Connection<'_>,
        atom_wrap: &AtomWrapKey,
    ) -> Result<Vec<AtomHit>> {
        let wrapped = self.db.atom_store_live_wrapped()?;

        let mut params: Vec<Value> = vec![Value::Integer(h.id)];
        let mut preds = String::new();
        if let Some(kind) = &q.kind {
            params.push(Value::Text(kind.as_str().into()));
            preds += &format!(" AND kind = ${}", params.len());
        }
        if let Some(from) = q.created_from {
            params.push(Value::Timestamp(from));
            preds += &format!(" AND created_at >= ${}", params.len());
        }
        if let Some(before) = q.created_before {
            params.push(Value::Timestamp(before));
            preds += &format!(" AND created_at < ${}", params.len());
        }
        params.push(Value::Timestamp(now_micros()));
        preds += &format!(
            " AND (expires_at IS NULL OR expires_at > ${})",
            params.len()
        );
        // Walking down, `after_id` is a fixed bound, not the ascending watermark.
        if q.newest {
            if let Some(after) = q.after_id {
                params.push(Value::Integer(after));
                preds += &format!(" AND id > ${}", params.len());
            }
        }
        // The payload filter runs after decryption, so page by id until
        // `limit` is met or drained; the id cursor doubles as the watermark.
        let page_param = params.len() + 1;
        let (cmp, dir) = if q.newest {
            ("<", "DESC")
        } else {
            (">", "ASC")
        };
        let sql = format!(
            "SELECT id, kind, sealed, score, immutable, created_at FROM {table} \
             WHERE region_id = $1{preds} AND id {cmp} ${page_param} \
             ORDER BY id {dir} LIMIT {EXACT_SCAN_LIMIT}",
            table = h.table
        );

        let mut out = Vec::new();
        let mut last_id: AtomId = if q.newest {
            i64::MAX
        } else {
            q.after_id.unwrap_or(i64::MIN)
        };
        'pages: loop {
            let mut page_params = params.clone();
            page_params.push(Value::Integer(last_id));
            let qr = conn.query_params(&sql, &page_params)?;
            if qr.rows.is_empty() {
                break;
            }
            let batch = qr.rows.len();
            for row in &qr.rows {
                let id = as_int(&row[0])?;
                last_id = id;
                let Some(w) = wrapped.get(&(id as u64)) else {
                    continue;
                };
                let (mut text, mut payload) =
                    open_atom_content(atom_wrap, w, id, as_blob(&row[2])?)?;
                if let Some(filter) = &q.payload_filter {
                    if !json_contains(&payload, filter) {
                        zeroize_atom_content(&mut text, &mut payload);
                        continue;
                    }
                }
                out.push(AtomHit {
                    id,
                    kind: as_text(&row[1])?.to_string(),
                    payload,
                    text,
                    distance: f32::MAX,
                    score: as_f32(&row[3]),
                    created_at: as_ts(&row[5]),
                    immutable: as_bool(&row[4]),
                });
                if out.len() >= q.limit {
                    break 'pages;
                }
            }
            if batch < EXACT_SCAN_LIMIT {
                break;
            }
        }
        // The window was taken from the end; callers still read oldest first.
        if q.newest {
            out.reverse();
        }
        Ok(out)
    }

    fn fetch_one_sealed(
        &self,
        h: &RegionHandle,
        atom_id: AtomId,
        conn: &Connection<'_>,
        atom_wrap: &AtomWrapKey,
    ) -> Result<Option<AtomHit>> {
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, sealed, score, immutable, key_slot, created_at, key_gen \
                 FROM {table} WHERE id = $1 AND region_id = $2 \
                 AND (expires_at IS NULL OR expires_at > $3)",
                table = h.table
            ),
            &[
                Value::Integer(atom_id),
                Value::Integer(h.id),
                Value::Timestamp(now_micros()),
            ],
        )?;
        let Some(row) = qr.rows.first() else {
            return Ok(None);
        };
        let id = as_int(&row[0])?;
        let rec = self.db.atom_store_slot(as_int(&row[5])? as u32)?;
        // Erased/recycled key = absent atom: the same triple bind recall applies.
        if rec.state != SlotState::Live
            || rec.region_id != id as u64
            || rec.gen != as_int(&row[7])? as u64
        {
            return Ok(None);
        }
        let (text, payload) = open_atom_content(atom_wrap, &rec.wrapped, id, as_blob(&row[2])?)?;
        Ok(Some(AtomHit {
            id,
            kind: as_text(&row[1])?.to_string(),
            payload,
            text,
            distance: f32::MAX,
            score: as_f32(&row[3]),
            created_at: as_ts(&row[6]),
            immutable: as_bool(&row[4]),
        }))
    }

    fn fetch_last_sealed(
        &self,
        h: &RegionHandle,
        kind: &str,
        conn: &Connection<'_>,
        atom_wrap: &AtomWrapKey,
    ) -> Result<Option<AtomHit>> {
        // Skip erased/recycled residue so it cannot mask the genuine latest atom.
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, sealed, score, immutable, key_slot, created_at, key_gen \
                 FROM {table} WHERE region_id = $1 AND kind = $2 \
                 AND (expires_at IS NULL OR expires_at > $3) ORDER BY id DESC",
                table = h.table
            ),
            &[
                Value::Integer(h.id),
                Value::Text(kind.into()),
                Value::Timestamp(now_micros()),
            ],
        )?;
        for row in &qr.rows {
            let id = as_int(&row[0])?;
            let rec = self.db.atom_store_slot(as_int(&row[5])? as u32)?;
            if rec.state != SlotState::Live
                || rec.region_id != id as u64
                || rec.gen != as_int(&row[7])? as u64
            {
                continue;
            }
            let (text, payload) =
                open_atom_content(atom_wrap, &rec.wrapped, id, as_blob(&row[2])?)?;
            return Ok(Some(AtomHit {
                id,
                kind: as_text(&row[1])?.to_string(),
                payload,
                text,
                distance: f32::MAX,
                score: as_f32(&row[3]),
                created_at: as_ts(&row[6]),
                immutable: as_bool(&row[4]),
            }));
        }
        Ok(None)
    }

    /// Re-seal an atom with a replaced payload (embedding and text preserved).
    fn update_atom_payload_sealed(
        &self,
        key: &str,
        h: &RegionHandle,
        atom_id: AtomId,
        payload: &serde_json::Value,
        conn: &Connection<'_>,
        atom_wrap: &AtomWrapKey,
    ) -> Result<()> {
        let new_payload = Zeroizing::new(
            serde_json::to_string(payload)
                .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?,
        );
        let table = h.table.clone();
        with_write_txn(conn, |c| {
            let qr = c.query_params(
                &format!(
                    "SELECT sealed, key_slot, key_gen FROM {table} \
                     WHERE id = $1 AND region_id = $2 AND immutable = 0"
                ),
                &[Value::Integer(atom_id), Value::Integer(h.id)],
            )?;
            let Some(row) = qr.rows.first() else {
                return Err(MemError::Invalid(format!(
                    "atom {atom_id} not found, or immutable, in region '{key}'"
                )));
            };
            // Re-seal under the same ACK (the atom's key is unchanged; only its
            // payload).
            let rec = self.db.atom_store_slot(as_int(&row[1])? as u32)?;
            if rec.state != SlotState::Live
                || rec.region_id != atom_id as u64
                || rec.gen != as_int(&row[2])? as u64
            {
                return Err(MemError::Invalid(format!(
                    "atom {atom_id} not found, or immutable, in region '{key}'"
                )));
            }
            let ack = Zeroizing::new(atom_wrap.unwrap_atom_key(&rec.wrapped)?);
            let seal_keys = derive_seal_keys(&ack);
            let old_blob = Zeroizing::new(blob_seal::open(
                &seal_keys,
                atom_id as u64,
                as_blob(&row[0])?,
            )?);
            let (emb, text, old_payload) = decode_atom_blob(&old_blob)?;
            let emb = Zeroizing::new(emb);
            let text = Zeroizing::new(text);
            let _old_payload = Zeroizing::new(old_payload);
            let blob = Zeroizing::new(encode_atom_blob(&emb, &text, &new_payload));
            let sealed = blob_seal::seal(&seal_keys, atom_id as u64, &blob);
            c.execute_params(
                &format!("UPDATE {table} SET sealed = $1 WHERE id = $2 AND region_id = $3"),
                &[
                    Value::Blob(sealed),
                    Value::Integer(atom_id),
                    Value::Integer(h.id),
                ],
            )?;
            Ok(())
        })
    }

    /// Ids of non-immutable sealed atoms whose payload `@>`-contains
    /// `predicate`. Exhaustive: forgetting must not silently under-delete, so
    /// this pages through every atom by id (unlike the bounded read paths)
    /// until the region is drained.
    fn evict_predicate_sealed_ids(
        &self,
        h: &RegionHandle,
        predicate: &serde_json::Value,
        conn: &Connection<'_>,
        atom_wrap: &AtomWrapKey,
    ) -> Result<Vec<AtomId>> {
        let wrapped = self.db.atom_store_live_wrapped()?;
        let sql = format!(
            "SELECT id, sealed FROM {table} WHERE region_id = $1 AND immutable = 0 \
             AND id > $2 ORDER BY id LIMIT {EXACT_SCAN_LIMIT}",
            table = h.table
        );
        let mut ids = Vec::new();
        let mut last_id: AtomId = i64::MIN;
        loop {
            let qr = conn.query_params(&sql, &[Value::Integer(h.id), Value::Integer(last_id)])?;
            if qr.rows.is_empty() {
                break;
            }
            for row in &qr.rows {
                let id = as_int(&row[0])?;
                last_id = id;
                let Some(w) = wrapped.get(&(id as u64)) else {
                    continue;
                };
                let (mut text, mut payload) =
                    open_atom_content(atom_wrap, w, id, as_blob(&row[1])?)?;
                let matched = json_contains(&payload, predicate);
                zeroize_atom_content(&mut text, &mut payload);
                if matched {
                    ids.push(id);
                }
            }
            if qr.rows.len() < EXACT_SCAN_LIMIT {
                break;
            }
        }
        Ok(ids)
    }
}

/// Cached index needs a rebuild: post-snapshot tail exceeds the cap or 1/4 of
/// indexed atoms.
fn sealed_index_stale(sa: &SealedAnn, max_id: i64, epoch: u64) -> bool {
    sa.build_epoch != epoch || sa.index.tail_is_stale(max_id.max(0) as u64)
}

/// Top `cand_k` `(atom_id, distance)` from the cached index, plus exact-ranked
/// atoms inserted after its snapshot.
#[allow(clippy::too_many_arguments)]
fn search_sealed_index(
    sa: &SealedAnn,
    qvec: &[f32],
    q: &RecallQuery,
    cand_k: usize,
    conn: &Connection<'_>,
    atom_wrap: &AtomWrapKey,
    db: &Database,
    h: &RegionHandle,
    max_id: i64,
) -> Result<Vec<(AtomId, f32)>> {
    // A kind absent from the snapshot can still exist in the tail, so an empty
    // code set skips only the index search, not the tail scan below.
    let filter = if q.kinds.is_empty() {
        Some(Filter::none())
    } else {
        let codes: Vec<u32> = q
            .kinds
            .iter()
            .filter_map(|k| sa.kind_codes.get(k).copied())
            .collect();
        if codes.is_empty() {
            None
        } else {
            Some(Filter::new(vec![(0, codes)]))
        }
    };

    let mut ranked: Vec<(AtomId, f32)> = filter
        .map(|f| sa.index.search_filtered_default_ef(qvec, cand_k, &f))
        .unwrap_or_default()
        .into_iter()
        .map(|(id, d)| (id as AtomId, d))
        .collect();

    // Exact-rank atoms inserted after the snapshot (the key store is read only
    // here, when the index is behind the latest writes).
    let snap = sa.index.snapshot_max as i64;
    if max_id > snap {
        let ttl_now = now_micros();
        let mut tail = decrypt_scan(conn, db, atom_wrap, &h.table, h.id, Some(snap))?;
        for (id, mut emb, kind, mut text, mut payload, _, _, _, expires) in tail.drain() {
            let included = (q.kinds.is_empty() || q.kinds.iter().any(|k| k == &kind))
                && expires.is_none_or(|expires| expires > ttl_now);
            if included {
                ranked.push((id, vec_distance(h.metric, qvec, &emb)));
            }
            emb.zeroize();
            text.zeroize();
            zeroize_json_strings(&mut payload);
        }
    }

    ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    ranked.truncate(cand_k);
    Ok(ranked)
}

/// INSERT one sealed atom into an `_enc` table.
#[allow(clippy::too_many_arguments)]
fn insert_sealed_atom(
    c: &Connection<'_>,
    table: &str,
    id: AtomId,
    region_id: RegionId,
    kind: &str,
    sealed: Vec<u8>,
    key_slot: u32,
    key_gen: u64,
    score: f32,
    confidence: f32,
    immutable: i64,
    created: Value,
    expires: Value,
) -> Result<()> {
    c.execute_params(
        &format!(
            "INSERT INTO {table} \
             (id, region_id, kind, sealed, key_slot, key_gen, score, confidence, access_count, \
              immutable, created_at, accessed_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9, $10, CURRENT_TIMESTAMP, $11)"
        ),
        &[
            Value::Integer(id),
            Value::Integer(region_id),
            Value::Text(kind.into()),
            Value::Blob(sealed),
            Value::Integer(key_slot as i64),
            Value::Integer(key_gen as i64),
            Value::Real(score as f64),
            Value::Real(confidence as f64),
            Value::Integer(immutable),
            created,
            expires,
        ],
    )?;
    Ok(())
}

/// The plaintext payload sealed per encrypted atom: `dim | embedding(f32 LE) |
/// text | payload-json`, each variable field length-prefixed (u32 LE).
fn encode_atom_blob(embedding: &[f32], text: &str, payload_json: &str) -> Vec<u8> {
    let dim = embedding.len() as u16;
    let tb = text.as_bytes();
    let pb = payload_json.as_bytes();
    let mut out = Vec::with_capacity(2 + embedding.len() * 4 + 4 + tb.len() + 4 + pb.len());
    out.extend_from_slice(&dim.to_le_bytes());
    for &f in embedding {
        out.extend_from_slice(&f.to_le_bytes());
    }
    out.extend_from_slice(&(tb.len() as u32).to_le_bytes());
    out.extend_from_slice(tb);
    out.extend_from_slice(&(pb.len() as u32).to_le_bytes());
    out.extend_from_slice(pb);
    out
}

struct AtomBlobParts<'a> {
    dim: usize,
    embedding: &'a [u8],
    text: &'a [u8],
    payload: &'a [u8],
}

/// Validate framing once so narrow decoders borrow fields without redoing bounds.
fn parse_atom_blob(b: &[u8]) -> Result<AtomBlobParts<'_>> {
    let truncated = || MemError::Invalid("sealed atom blob is truncated".into());
    let mut o = 0usize;
    let take = |o: &mut usize, n: usize| -> Result<std::ops::Range<usize>> {
        let end = o
            .checked_add(n)
            .filter(|&end| end <= b.len())
            .ok_or_else(truncated)?;
        let range = *o..end;
        *o = end;
        Ok(range)
    };

    let dim = u16::from_le_bytes(b[take(&mut o, 2)?].try_into().map_err(|_| truncated())?) as usize;
    let embedding_len = dim.checked_mul(4).ok_or_else(truncated)?;
    let embedding = &b[take(&mut o, embedding_len)?];
    let tlen =
        u32::from_le_bytes(b[take(&mut o, 4)?].try_into().map_err(|_| truncated())?) as usize;
    let text = &b[take(&mut o, tlen)?];
    let plen =
        u32::from_le_bytes(b[take(&mut o, 4)?].try_into().map_err(|_| truncated())?) as usize;
    let payload = &b[take(&mut o, plen)?];
    if o != b.len() {
        return Err(MemError::Invalid(
            "sealed atom blob has trailing bytes".into(),
        ));
    }
    Ok(AtomBlobParts {
        dim,
        embedding,
        text,
        payload,
    })
}

fn decode_embedding(parts: &AtomBlobParts<'_>) -> Vec<f32> {
    let mut embedding = Vec::with_capacity(parts.dim);
    for bytes in parts.embedding.chunks_exact(4) {
        embedding.push(f32::from_le_bytes(
            bytes.try_into().expect("validated four-byte chunk"),
        ));
    }
    embedding
}

fn decode_atom_blob(b: &[u8]) -> Result<(Vec<f32>, String, String)> {
    let parts = parse_atom_blob(b)?;
    let mut embedding = decode_embedding(&parts);
    let mut text = match std::str::from_utf8(parts.text) {
        Ok(text) => text.to_owned(),
        Err(_) => {
            embedding.zeroize();
            return Err(MemError::Invalid("sealed text is not valid UTF-8".into()));
        }
    };
    let payload = match std::str::from_utf8(parts.payload) {
        Ok(payload) => payload.to_owned(),
        Err(_) => {
            embedding.zeroize();
            text.zeroize();
            return Err(MemError::Invalid(
                "sealed payload is not valid UTF-8".into(),
            ));
        }
    };
    Ok((embedding, text, payload))
}

/// Decode only the vector; text/payload are framing-checked, never allocated.
fn decode_atom_embedding(b: &[u8]) -> Result<Vec<f32>> {
    Ok(decode_embedding(&parse_atom_blob(b)?))
}

/// Decode only the text; payload bytes are framing-checked, never materialized.
fn decode_atom_text(b: &[u8]) -> Result<String> {
    let parts = parse_atom_blob(b)?;
    std::str::from_utf8(parts.text)
        .map(str::to_owned)
        .map_err(|_| MemError::Invalid("sealed text is not valid UTF-8".into()))
}

/// Decode only user-visible content; the embedding is never copied to a second alloc.
fn decode_atom_content(b: &[u8]) -> Result<(String, String)> {
    let parts = parse_atom_blob(b)?;
    let mut text = std::str::from_utf8(parts.text)
        .map(str::to_owned)
        .map_err(|_| MemError::Invalid("sealed text is not valid UTF-8".into()))?;
    let payload = match std::str::from_utf8(parts.payload) {
        Ok(payload) => payload.to_owned(),
        Err(_) => {
            text.zeroize();
            return Err(MemError::Invalid(
                "sealed payload is not valid UTF-8".into(),
            ));
        }
    };
    Ok((text, payload))
}

/// Streaming identity state shared by plaintext and encrypted stored-vector scans.
struct StoredEmbeddingScan<'a> {
    hasher: Sha256,
    expected: Option<&'a [(AtomId, Vec<f32>)]>,
    count: u64,
    dim: usize,
    region: String,
    kind: String,
}

impl<'a> StoredEmbeddingScan<'a> {
    fn new(region: &str, kind: &str, dim: u16, expected: Option<&'a [(AtomId, Vec<f32>)]>) -> Self {
        let mut hasher = Sha256::new();
        hash_len_prefixed(&mut hasher, STORED_EMBEDDINGS_SCHEMA.as_bytes());
        hash_len_prefixed(&mut hasher, region.as_bytes());
        hash_len_prefixed(&mut hasher, kind.as_bytes());
        hasher.update(u32::from(dim).to_le_bytes());
        Self {
            hasher,
            expected,
            count: 0,
            dim: usize::from(dim),
            region: region.to_owned(),
            kind: kind.to_owned(),
        }
    }

    fn consume(&mut self, id: AtomId, embedding: &[f32]) -> Result<()> {
        if embedding.len() != self.dim {
            return Err(MemError::Invalid(format!(
                "stored embedding for atom {id} has dimension {}, expected {}",
                embedding.len(),
                self.dim
            )));
        }
        if let Some((component, _)) = embedding
            .iter()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            return Err(MemError::Invalid(format!(
                "stored embedding for atom {id} has a non-finite component at index {component}"
            )));
        }

        if let Some(expected) = self.expected {
            let index = usize::try_from(self.count)
                .map_err(|_| MemError::Invalid("stored embedding count overflow".into()))?;
            let Some((expected_id, expected_embedding)) = expected.get(index) else {
                return Err(MemError::Invalid(format!(
                    "stored embedding count exceeds expected count {}",
                    expected.len()
                )));
            };
            if id != *expected_id {
                return Err(MemError::Invalid(format!(
                    "stored embedding id mismatch at index {index}: stored {id}, expected {expected_id}"
                )));
            }
            if let Some(component) = embedding
                .iter()
                .zip(expected_embedding)
                .position(|(stored, expected)| stored.to_bits() != expected.to_bits())
            {
                return Err(MemError::Invalid(format!(
                    "stored embedding bit mismatch for atom {id} at component {component}"
                )));
            }
        }

        self.hasher.update(id.to_le_bytes());
        for value in embedding {
            self.hasher.update(value.to_bits().to_le_bytes());
        }
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| MemError::Invalid("stored embedding count overflow".into()))?;
        Ok(())
    }

    fn finish(mut self) -> Result<StoredEmbeddingsIdentity> {
        if let Some(expected) = self.expected {
            let actual = usize::try_from(self.count)
                .map_err(|_| MemError::Invalid("stored embedding count overflow".into()))?;
            if actual != expected.len() {
                return Err(MemError::Invalid(format!(
                    "stored embedding count mismatch: stored {actual}, expected {}",
                    expected.len()
                )));
            }
        }
        self.hasher.update(self.count.to_le_bytes());
        let digest = self.hasher.finalize();
        Ok(StoredEmbeddingsIdentity::new(
            self.region,
            self.kind,
            self.count,
            self.dim as u32,
            hex_lower(&digest),
        ))
    }
}

fn validate_expected_embeddings(expected: &[(AtomId, Vec<f32>)], dim: usize) -> Result<()> {
    let mut previous = None;
    for (index, (id, embedding)) in expected.iter().enumerate() {
        if previous.is_some_and(|previous_id| *id <= previous_id) {
            return Err(MemError::Invalid(format!(
                "expected embedding ids must be strictly ascending; id {id} at index {index} follows {}",
                previous.expect("checked as some")
            )));
        }
        if embedding.len() != dim {
            return Err(MemError::Invalid(format!(
                "expected embedding for atom {id} has dimension {}, expected {dim}",
                embedding.len()
            )));
        }
        if let Some((component, _)) = embedding
            .iter()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            return Err(MemError::Invalid(format!(
                "expected embedding for atom {id} has a non-finite component at index {component}"
            )));
        }
        previous = Some(*id);
    }
    Ok(())
}

fn hash_len_prefixed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    out
}

/// One decrypted sealed atom with the fields recall caches: `(id, embedding,
/// kind, text, payload, importance, created_micros, immutable,
/// expires_micros)`.
type DecryptedAtom = (
    AtomId,
    Vec<f32>,
    String,
    String,
    serde_json::Value,
    f32,
    i64,
    bool,
    Option<i64>,
);

/// Zeroizes decrypted rows on every early return before ANN/cache take ownership.
struct DecryptedAtoms(Vec<DecryptedAtom>);

impl DecryptedAtoms {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn drain(&mut self) -> std::vec::Drain<'_, DecryptedAtom> {
        self.0.drain(..)
    }
}

impl Drop for DecryptedAtoms {
    fn drop(&mut self) {
        for (_, embedding, _, text, payload, ..) in &mut self.0 {
            embedding.zeroize();
            text.zeroize();
            zeroize_json_strings(payload);
        }
    }
}

/// Resolve the exact ACK binding: id alone could decrypt via a recycled slot.
fn exact_live_atom_wrapped(
    db: &Database,
    atom_id: AtomId,
    slot: &Value,
    generation: &Value,
) -> Result<Option<[u8; WRAPPED_KEY_SIZE]>> {
    let slot = u32::try_from(as_int(slot)?)
        .map_err(|_| MemError::Invalid(format!("atom {atom_id} key_slot is out of range")))?;
    let generation = u64::try_from(as_int(generation)?)
        .map_err(|_| MemError::Invalid(format!("atom {atom_id} key_gen is out of range")))?;
    let owner = u64::try_from(atom_id)
        .map_err(|_| MemError::Invalid(format!("atom id {atom_id} is out of range")))?;
    let record = db.atom_store_slot(slot)?;
    if record.state != SlotState::Live || record.region_id != owner || record.gen != generation {
        return Ok(None);
    }
    Ok(Some(record.wrapped))
}

/// Decrypt a sealed region's atoms (all, or only `id > after`) into
/// [`DecryptedAtom`]s, to (re)build the ANN index and exact-rank the tail.
fn decrypt_scan(
    conn: &Connection<'_>,
    db: &Database,
    atom_wrap: &AtomWrapKey,
    table: &str,
    region_id: RegionId,
    after: Option<i64>,
) -> Result<DecryptedAtoms> {
    if conn.table_schema(table).is_none() {
        return Ok(DecryptedAtoms(Vec::new()));
    }
    let cols = "id, kind, sealed, score, created_at, immutable, expires_at, key_slot, key_gen";
    let (sql, params) = match after {
        Some(a) => (
            format!("SELECT {cols} FROM {table} WHERE region_id = $1 AND id > $2 ORDER BY id"),
            vec![Value::Integer(region_id), Value::Integer(a)],
        ),
        None => (
            format!("SELECT {cols} FROM {table} WHERE region_id = $1 ORDER BY id"),
            vec![Value::Integer(region_id)],
        ),
    };
    let qr = conn.query_params(&sql, &params)?;
    let mut out = DecryptedAtoms(Vec::with_capacity(qr.rows.len()));
    for row in &qr.rows {
        let id = as_int(&row[0])?;
        let kind = as_text(&row[1])?.to_string();
        let Some(wrapped) = exact_live_atom_wrapped(db, id, &row[7], &row[8])? else {
            continue;
        };
        let (emb, text, payload) = open_atom(atom_wrap, &wrapped, id, as_blob(&row[2])?)?;
        out.0.push((
            id,
            emb,
            kind,
            text,
            payload,
            as_f32(&row[3]),
            as_ts(&row[4]),
            as_bool(&row[5]),
            opt_ts(&row[6]),
        ));
    }
    Ok(out)
}

/// Sealed-segment ciphertext chunk size; only bounds per-value buffers
/// (storage chains pages anyway).
const SEALED_SEG_CHUNK: usize = 1024 * 1024;

/// Hidden chunk tree for one region's sealed segment. Per-region (the region is
/// the sealed lifecycle unit, so shared-table regions can't destroy each
/// other's segments), separate from the SQL layer's `__annseg_{table}` name.
fn sealed_segment_table(table: &str, region_id: RegionId) -> String {
    format!("__annseg_r{region_id}__{table}")
}

/// Inverse of [`sealed_segment_table`]; a loose prefix match could drop a foreign tree.
fn parse_sealed_segment_table(name: &str) -> Option<(RegionId, &str)> {
    let rest = name.strip_prefix("__annseg_r")?;
    let (rid, table) = rest.split_once("__")?;
    let rid: RegionId = rid.parse().ok()?;
    parse_encrypted_atoms_table(table)?;
    (sealed_segment_table(table, rid) == name).then_some((rid, table))
}

/// Cap the trusted chunk count so malformed metadata cannot drive unbounded work.
fn plausible_chunk_count(count: u32) -> bool {
    (1..=65_536).contains(&count)
}

/// Pseudo ids are row-less: an atom row with `id` anywhere marks metadata forged/stale.
fn atom_row_exists_anywhere(conn: &Connection<'_>, id: i64) -> Result<bool> {
    for table in conn.tables() {
        if !is_encrypted_atoms_table(&table) {
            continue;
        }
        let qr = conn.query_params(
            &format!("SELECT id FROM {table} WHERE id = $1"),
            &[Value::Integer(id)],
        )?;
        if !qr.rows.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Chunk-tree classification; only a well-formed `Present` tree validates a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentTreeState {
    Absent,
    Incomplete,
    Present,
}

/// One exactly-bound live sealed row delivered to a scan consumer. Returning
/// `false` stops delivery (the fingerprint still covers the remaining rows).
type SealedRowFn<'a> = dyn FnMut(AtomId, &str, &[u8], &[u8; WRAPPED_KEY_SIZE], f32, i64, bool, Option<i64>) -> Result<bool>
    + 'a;

/// Liveness-aware fingerprint of a sealed region (one ORDER BY id scan): each
/// row contributes id, sealed ciphertext, and key-liveness bit, so content
/// changes and crypto-erasures both invalidate a persisted segment.
fn sealed_fp_scan(
    conn: &Connection<'_>,
    db: &Database,
    h: &RegionHandle,
    live: &mut SealedRowFn<'_>,
) -> Result<([u8; 32], bool)> {
    let mut fp = blake3::Hasher::new();
    fp.update(b"citadel-annseg-sealed-fp-v1");
    fp.update(&h.id.to_le_bytes());
    fp.update(&h.dim.to_le_bytes());
    fp.update(&[citadel_vector::segment::metric_tag(ann_metric(h.metric))]);

    let qr = conn.query_params(
        &format!(
            "SELECT id, kind, sealed, score, created_at, immutable, expires_at, \
             key_slot, key_gen FROM {table} \
             WHERE region_id = $1 ORDER BY id",
            table = h.table
        ),
        &[Value::Integer(h.id)],
    )?;
    let mut completed = true;
    for row in &qr.rows {
        let id = as_int(&row[0])?;
        let sealed = as_blob(&row[2])?;
        let wrapped = exact_live_atom_wrapped(db, id, &row[7], &row[8])?;
        let is_live = wrapped.is_some();
        fp.update(&id.to_le_bytes());
        fp.update(&(sealed.len() as u64).to_le_bytes());
        fp.update(sealed);
        fp.update(&[u8::from(is_live)]);
        if let (Some(wrapped), true) = (wrapped.as_ref(), completed) {
            let kind = as_text(&row[1])?;
            if !live(
                id,
                kind,
                sealed,
                wrapped,
                as_f32(&row[3]),
                as_ts(&row[4]),
                as_bool(&row[5]),
                opt_ts(&row[6]),
            )? {
                // Keep hashing the remaining rows (the fingerprint must cover
                // the whole table) but stop delivering them.
                completed = false;
            }
        }
    }
    Ok((*fp.finalize().as_bytes(), completed))
}

/// Parse the sealed segment's inner plaintext: `[fp 32][config_hash
/// 32][kind_count u32][(len u32, kind, code u32)*][segment body]`.
#[allow(clippy::type_complexity)]
fn parse_sealed_segment(
    inner: &[u8],
) -> Option<(
    [u8; 32],
    [u8; 32],
    FxHashMap<String, u32>,
    citadel_vector::segment::SegmentParts,
)> {
    let mut at = 0usize;
    let take = |at: &mut usize, n: usize| -> Option<&[u8]> {
        let end = at.checked_add(n).filter(|&e| e <= inner.len())?;
        let s = &inner[*at..end];
        *at = end;
        Some(s)
    };
    let fp: [u8; 32] = take(&mut at, 32)?.try_into().ok()?;
    let cfg: [u8; 32] = take(&mut at, 32)?.try_into().ok()?;
    let count = u32::from_le_bytes(take(&mut at, 4)?.try_into().ok()?) as usize;
    let mut kind_codes = FxHashMap::default();
    for _ in 0..count {
        let len = u32::from_le_bytes(take(&mut at, 4)?.try_into().ok()?) as usize;
        let kind = String::from_utf8(take(&mut at, len)?.to_vec()).ok()?;
        let code = u32::from_le_bytes(take(&mut at, 4)?.try_into().ok()?);
        kind_codes.insert(kind, code);
    }
    let parts = citadel_vector::segment::decode(&inner[at..]).ok()?;
    Some((fp, cfg, kind_codes, parts))
}

fn annseg_meta_key(region_id: RegionId, field: &str) -> String {
    format!("annseg_{field}:{region_id}")
}

fn read_annseg_field(
    conn: &Connection<'_>,
    region_id: RegionId,
    field: &str,
) -> Result<Option<i64>> {
    let qr = conn.query_params(
        "SELECT value FROM memory_meta WHERE key = $1",
        &[Value::Text(annseg_meta_key(region_id, field).into())],
    )?;
    Ok(match qr.rows.first().map(|r| &r[0]) {
        Some(Value::Integer(v)) => Some(*v),
        _ => None,
    })
}

fn read_annseg_meta(conn: &Connection<'_>, region_id: RegionId) -> Result<Option<(u32, u64, i64)>> {
    let (Some(slot), Some(gen), Some(id)) = (
        read_annseg_field(conn, region_id, "slot")?,
        read_annseg_field(conn, region_id, "gen")?,
        read_annseg_field(conn, region_id, "id")?,
    ) else {
        return Ok(None);
    };
    // Out-of-domain metadata reads as absent; never truncate into a real binding.
    let (Ok(slot), Ok(gen), true) = (u32::try_from(slot), u64::try_from(gen), id >= 0) else {
        return Ok(None);
    };
    Ok(Some((slot, gen, id)))
}

fn write_annseg_meta(
    conn: &Connection<'_>,
    region_id: RegionId,
    slot: u32,
    gen: u64,
    pseudo_id: i64,
) -> Result<()> {
    with_write_txn(conn, |c| {
        for (field, value) in [
            ("slot", slot as i64),
            ("gen", gen as i64),
            ("id", pseudo_id),
        ] {
            let key = annseg_meta_key(region_id, field);
            c.execute_params(
                "DELETE FROM memory_meta WHERE key = $1",
                &[Value::Text(key.clone().into())],
            )?;
            c.execute_params(
                "INSERT INTO memory_meta (key, value) VALUES ($1, $2)",
                &[Value::Text(key.into()), Value::Integer(value)],
            )?;
        }
        Ok(())
    })
}

fn clear_annseg_meta(conn: &Connection<'_>, region_id: RegionId) -> Result<()> {
    with_write_txn(conn, |c| {
        for field in ["slot", "gen", "id"] {
            c.execute_params(
                "DELETE FROM memory_meta WHERE key = $1",
                &[Value::Text(annseg_meta_key(region_id, field).into())],
            )?;
        }
        Ok(())
    })
}

/// Open one sealed atom: unwrap its ACK (wrapped under the region atom-wrap
/// key) and decrypt the blob. The ACK is zeroized before returning.
fn open_atom(
    atom_wrap: &AtomWrapKey,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
    id: AtomId,
    sealed: &[u8],
) -> Result<(Vec<f32>, String, serde_json::Value)> {
    let mut ack = atom_wrap.unwrap_atom_key(wrapped)?;
    let seal_keys = derive_seal_keys(&ack);
    ack.zeroize();
    let blob = Zeroizing::new(blob_seal::open(&seal_keys, id as u64, sealed)?);
    let (emb, text, payload_json) = decode_atom_blob(&blob)?;
    let payload_json = Zeroizing::new(payload_json);
    let payload = serde_json::from_str(&payload_json).unwrap_or(serde_json::Value::Null);
    Ok((emb, text, payload))
}

/// Open only text+payload; the embedding is never materialized as a `Vec<f32>`.
fn open_atom_content(
    atom_wrap: &AtomWrapKey,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
    id: AtomId,
    sealed: &[u8],
) -> Result<(String, serde_json::Value)> {
    let mut ack = atom_wrap.unwrap_atom_key(wrapped)?;
    let seal_keys = derive_seal_keys(&ack);
    ack.zeroize();
    let blob = Zeroizing::new(blob_seal::open(&seal_keys, id as u64, sealed)?);
    let (text, payload_json) = decode_atom_content(&blob)?;
    let payload_json = Zeroizing::new(payload_json);
    let payload = serde_json::from_str(&payload_json).unwrap_or(serde_json::Value::Null);
    Ok((text, payload))
}

/// Open only the vector; the RAII zeroizer scrubs the blob on every return path.
fn open_atom_embedding(
    atom_wrap: &AtomWrapKey,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
    id: AtomId,
    sealed: &[u8],
) -> Result<Vec<f32>> {
    let mut ack = atom_wrap.unwrap_atom_key(wrapped)?;
    let seal_keys = derive_seal_keys(&ack);
    ack.zeroize();
    let blob = Zeroizing::new(blob_seal::open(&seal_keys, id as u64, sealed)?);
    decode_atom_embedding(&blob)
}

/// Open only the text for sealed dedup; other fields stay in the zeroized blob.
fn open_atom_text(
    atom_wrap: &AtomWrapKey,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
    id: AtomId,
    sealed: &[u8],
) -> Result<String> {
    let mut ack = atom_wrap.unwrap_atom_key(wrapped)?;
    let seal_keys = derive_seal_keys(&ack);
    ack.zeroize();
    let blob = Zeroizing::new(blob_seal::open(&seal_keys, id as u64, sealed)?);
    decode_atom_text(&blob)
}

/// Seal one atom under a fresh random ACK, returning `(sealed_blob,
/// wrapped_ack)`. The wrapped ACK is the sole copy; store it (fsync'd) before
/// the atom row commits.
fn seal_atom(
    atom_wrap: &AtomWrapKey,
    id: AtomId,
    embedding: &[f32],
    text: &str,
    payload_json: &str,
) -> (Vec<u8>, [u8; WRAPPED_KEY_SIZE]) {
    use rand::RngCore;
    let mut ack = [0u8; citadel_core::KEY_SIZE];
    rand::thread_rng().fill_bytes(&mut ack);
    let seal_keys = derive_seal_keys(&ack);
    let blob = Zeroizing::new(encode_atom_blob(embedding, text, payload_json));
    let sealed = blob_seal::seal(&seal_keys, id as u64, &blob);
    let wrapped = atom_wrap.wrap_atom_key(&ack);
    ack.zeroize();
    (sealed, wrapped)
}

/// Distance between two vectors, matching citadel-sql's `<->`/`<#>`/`<=>` so
/// sealed decrypt-then-rank recall matches the plaintext index path.
fn vec_distance(metric: EmbeddingMetric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        EmbeddingMetric::L2 => {
            let mut sum = 0.0f64;
            for (x, y) in a.iter().zip(b.iter()) {
                let d = *x as f64 - *y as f64;
                sum += d * d;
            }
            sum.sqrt() as f32
        }
        EmbeddingMetric::InnerProduct => {
            let mut sum = 0.0f64;
            for (x, y) in a.iter().zip(b.iter()) {
                sum += *x as f64 * *y as f64;
            }
            (-sum) as f32
        }
        EmbeddingMetric::Cosine => {
            let (mut dot, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
            for (x, y) in a.iter().zip(b.iter()) {
                let (xf, yf) = (*x as f64, *y as f64);
                dot += xf * yf;
                na += xf * xf;
                nb += yf * yf;
            }
            let denom = na.sqrt() * nb.sqrt();
            if denom == 0.0 {
                f32::MAX
            } else {
                (1.0 - dot / denom) as f32
            }
        }
    }
}

/// Language-agnostic word tokens (UAX#29 boundaries, lowercased); spaceless
/// scripts (CJK/Thai) fall back to per-character tokens.
fn tokenize(text: &str) -> Vec<String> {
    use unicode_segmentation::UnicodeSegmentation;
    text.unicode_words().map(str::to_lowercase).collect()
}

/// Distinct query tokens for the BM25 keyword signal (UAX#29, lowercased,
/// deduped).
fn query_keyword_terms(text: Option<&str>) -> Vec<String> {
    let Some(t) = text else {
        return Vec::new();
    };
    let mut v = tokenize(t);
    v.sort();
    v.dedup();
    v
}

/// Set each candidate's `text_rank` to its Okapi BM25 score for `query_terms`,
/// with the candidate pool as the corpus. IDF down-weights pool-common terms,
/// so no stoplist or stemmer is needed.
fn assign_bm25_ranks(cands: &mut [Candidate], query_terms: &[String]) {
    if query_terms.is_empty() || cands.is_empty() {
        return;
    }
    const K1: f32 = 1.2;
    const B: f32 = 0.75;
    let n = cands.len() as f32;
    // Tokenize each candidate once: per-term frequency + document length.
    let docs: Vec<(FxHashMap<String, u32>, f32)> = cands
        .iter()
        .map(|c| {
            let mut tf: FxHashMap<String, u32> = FxHashMap::default();
            let mut len = 0u32;
            for tok in tokenize(&c.text) {
                *tf.entry(tok).or_insert(0) += 1;
                len += 1;
            }
            (tf, len as f32)
        })
        .collect();
    let avgdl = (docs.iter().map(|(_, l)| *l).sum::<f32>() / n).max(1.0);
    // IDF per query term over the pool (Lucene's +1 form, never negative).
    let idf: Vec<f32> = query_terms
        .iter()
        .map(|t| {
            let df = docs.iter().filter(|(tf, _)| tf.contains_key(t)).count() as f32;
            ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
        })
        .collect();
    for (c, (tf, dl)) in cands.iter_mut().zip(&docs) {
        let mut score = 0.0;
        for (t, &w) in query_terms.iter().zip(&idf) {
            let f = tf.get(t).copied().unwrap_or(0) as f32;
            if f > 0.0 {
                score += w * (f * (K1 + 1.0)) / (f + K1 * (1.0 - B + B * dl / avgdl));
            }
        }
        c.text_rank = score;
    }
}

/// JSONB `@>` containment: every member of `needle` is present in `haystack`.
fn json_contains(haystack: &serde_json::Value, needle: &serde_json::Value) -> bool {
    use serde_json::Value as J;
    match (haystack, needle) {
        (J::Object(h), J::Object(n)) => n
            .iter()
            .all(|(k, nv)| h.get(k).is_some_and(|hv| json_contains(hv, nv))),
        (J::Array(h), J::Array(n)) => n.iter().all(|ne| h.iter().any(|he| json_contains(he, ne))),
        (J::Array(h), ne) => h.iter().any(|he| json_contains(he, ne)),
        (a, b) => a == b,
    }
}

fn as_blob(v: &Value) -> Result<&[u8]> {
    match v {
        Value::Blob(b) => Ok(b.as_slice()),
        other => Err(MemError::Invalid(format!("expected blob, got {other:?}"))),
    }
}

/// Parse an optional `INTEGER` slot index (`NULL` -> `None`).
fn opt_u32(v: &Value) -> Result<Option<u32>> {
    match v {
        Value::Null => Ok(None),
        Value::Integer(i) => u32::try_from(*i)
            .map(Some)
            .map_err(|_| MemError::Invalid("rsk_slot out of range".into())),
        other => Err(MemError::Invalid(format!(
            "expected integer rsk_slot, got {other:?}"
        ))),
    }
}

/// Parse an optional `INTEGER` generation (`NULL` -> `None`).
fn opt_u64(v: &Value) -> Option<u64> {
    match v {
        Value::Integer(i) => Some(*i as u64),
        _ => None,
    }
}

struct RegionRow {
    id: RegionId,
    dim: u16,
    metric: EmbeddingMetric,
    model_id: String,
    encrypted: bool,
    rsk_slot: Option<u32>,
    rsk_gen: Option<u64>,
}

fn parse_region_row(row: &[Value]) -> Result<RegionRow> {
    if row.len() < 7 {
        return Err(MemError::Invalid(format!(
            "region row has {} columns, expected 7",
            row.len()
        )));
    }
    let id = as_int(&row[0])?;
    let dim = u16::try_from(as_int(&row[1])?)
        .map_err(|_| MemError::Invalid("stored embedding_dim out of range".into()))?;
    Ok(RegionRow {
        id,
        dim,
        metric: metric_from_str(as_text(&row[2])?)?,
        model_id: as_text(&row[3])?.to_owned(),
        encrypted: as_exact_bool(&row[4], "encrypted")?,
        rsk_slot: opt_u32(&row[5])?,
        rsk_gen: opt_u64(&row[6]),
    })
}

impl RegionRow {
    fn verify_matches(
        &self,
        region: &str,
        dim: u16,
        metric: EmbeddingMetric,
        model_id: &str,
        encrypted: bool,
    ) -> Result<()> {
        if self.dim != dim {
            return Err(MemError::DimMismatch {
                region: region.into(),
                expected: self.dim,
                got: dim as usize,
            });
        }
        if self.metric != metric {
            return Err(MemError::MetricMismatch {
                region: region.into(),
                expected: metric_tag(self.metric).into(),
                got: metric_tag(metric).into(),
            });
        }
        if self.model_id != model_id {
            return Err(MemError::ModelMismatch {
                region: region.into(),
                expected: self.model_id.clone(),
                got: model_id.into(),
            });
        }
        if self.encrypted != encrypted {
            return Err(MemError::Invalid(format!(
                "region '{region}' exists with encrypted={}, requested encrypted={encrypted}",
                self.encrypted
            )));
        }
        Ok(())
    }
}

/// Atoms table for a (dim, metric) pair (`region_id` isolates regions).
/// Encrypted regions use a distinct `_enc` table of sealed content.
pub(crate) fn atoms_table(dim: u16, metric: EmbeddingMetric, encrypted: bool) -> String {
    let suffix = if encrypted { "_enc" } else { "" };
    format!("memory_atoms_d{}_{}{}", dim, metric_tag(metric), suffix)
}

/// Whether `name` is an encrypted atoms table produced by [`atoms_table`].
fn is_encrypted_atoms_table(name: &str) -> bool {
    parse_encrypted_atoms_table(name).is_some()
}

/// Canonical inverse of [`atoms_table`]; a prefix-sharing user table is never claimed.
fn parse_encrypted_atoms_table(name: &str) -> Option<(u16, EmbeddingMetric)> {
    let rest = name.strip_prefix("memory_atoms_d")?;
    let rest = rest.strip_suffix("_enc")?;
    let (dim, metric) = rest.split_once('_')?;
    let dim: u16 = dim.parse().ok()?;
    let metric = metric_from_str(metric).ok()?;
    (atoms_table(dim, metric, true) == name).then_some((dim, metric))
}

fn ensure_atoms_table(
    conn: &Connection<'_>,
    dim: u16,
    metric: EmbeddingMetric,
    encrypted: bool,
) -> Result<()> {
    let t = atoms_table(dim, metric, encrypted);
    if let Some(schema) = conn.table_schema(&t) {
        // Pre-per-atom-erasure tables lack the key columns; fail loudly here.
        if encrypted {
            let has = |col: &str| schema.columns.iter().any(|c| c.name == col);
            if !has("key_slot") || !has("key_gen") {
                return Err(MemError::Invalid(format!(
                    "encrypted atoms table '{t}' is missing key_slot/key_gen columns \
                     (pre-per-atom-erasure schema); recreate the database"
                )));
            }
        }
        return Ok(());
    }
    if encrypted {
        // Sealed-only: no plaintext column (a stale CoW page can't leak it).
        conn.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {t} (\
             id INTEGER PRIMARY KEY,\
             region_id INTEGER NOT NULL,\
             kind TEXT NOT NULL,\
             sealed BLOB NOT NULL,\
             key_slot INTEGER NOT NULL,\
             key_gen INTEGER NOT NULL,\
             score REAL DEFAULT 0,\
             confidence REAL DEFAULT 1,\
             access_count INTEGER DEFAULT 0,\
             immutable INTEGER DEFAULT 0,\
             created_at TIMESTAMP NOT NULL,\
             accessed_at TIMESTAMP NOT NULL,\
             expires_at TIMESTAMP)"
        ))?;
        conn.execute(&format!(
            "CREATE INDEX IF NOT EXISTS {t}_rk ON {t} (region_id, kind)"
        ))?;
        return Ok(());
    }
    let tag = metric_tag(metric);
    conn.execute(&format!(
        "CREATE TABLE IF NOT EXISTS {t} (\
         id INTEGER PRIMARY KEY,\
         region_id INTEGER NOT NULL,\
         kind TEXT NOT NULL,\
         embedding VECTOR({dim}) NOT NULL,\
         payload JSONB NOT NULL,\
         text_content TEXT,\
         score REAL DEFAULT 0,\
         confidence REAL DEFAULT 1,\
         access_count INTEGER DEFAULT 0,\
         immutable INTEGER DEFAULT 0,\
         created_at TIMESTAMP NOT NULL,\
         accessed_at TIMESTAMP NOT NULL,\
         expires_at TIMESTAMP)"
    ))?;
    conn.execute(&format!(
        "CREATE INDEX IF NOT EXISTS {t}_ann ON {t} USING ann (embedding) \
         WITH (metric = '{tag}', filters = 'region_id,kind')"
    ))?;
    conn.execute(&format!(
        "CREATE INDEX IF NOT EXISTS {t}_rk ON {t} (region_id, kind)"
    ))?;
    conn.execute(&format!(
        "CREATE INDEX IF NOT EXISTS {t}_jsonb ON {t} USING gin (payload) WITH (ops = 'jsonb_path_ops')"
    ))?;
    Ok(())
}

/// Next id for `key` from `memory_meta`. Must run inside a write txn.
fn next_id(conn: &Connection<'_>, key: &str) -> Result<i64> {
    next_id_range(conn, key, 1)
}

/// Reserve `n` contiguous ids for `key`, returning the first.
fn next_id_range(conn: &Connection<'_>, key: &str, n: i64) -> Result<i64> {
    let qr = conn.query_params(
        "SELECT value FROM memory_meta WHERE key = $1",
        &[Value::Text(key.into())],
    )?;
    let cur = qr
        .rows
        .first()
        .map(|r| as_int(&r[0]))
        .transpose()?
        .ok_or_else(|| MemError::Invalid(format!("memory_meta missing key '{key}'")))?;
    conn.execute_params(
        "UPDATE memory_meta SET value = value + $1 WHERE key = $2",
        &[Value::Integer(n), Value::Text(key.into())],
    )?;
    Ok(cur)
}

/// Run `f` inside a BEGIN/COMMIT, rolling back on error.
fn with_write_txn<T>(
    conn: &Connection<'_>,
    f: impl FnOnce(&Connection<'_>) -> Result<T>,
) -> Result<T> {
    conn.execute("BEGIN")?;
    match f(conn) {
        Ok(v) => match conn.execute("COMMIT") {
            Ok(_) => Ok(v),
            Err(e) => {
                // A failed COMMIT may leave the txn active; clean up best-effort.
                let _ = conn.execute("ROLLBACK");
                Err(e.into())
            }
        },
        Err(e) => {
            let _ = conn.execute("ROLLBACK");
            Err(e)
        }
    }
}

pub(crate) fn metric_tag(m: EmbeddingMetric) -> &'static str {
    match m {
        EmbeddingMetric::Cosine => "cosine",
        EmbeddingMetric::L2 => "l2",
        EmbeddingMetric::InnerProduct => "inner",
    }
}

fn metric_from_str(s: &str) -> Result<EmbeddingMetric> {
    match s {
        "cosine" => Ok(EmbeddingMetric::Cosine),
        "l2" => Ok(EmbeddingMetric::L2),
        "inner" => Ok(EmbeddingMetric::InnerProduct),
        other => Err(MemError::Invalid(format!(
            "unknown stored metric '{other}'"
        ))),
    }
}

fn as_int(v: &Value) -> Result<i64> {
    match v {
        Value::Integer(i) => Ok(*i),
        other => Err(MemError::Invalid(format!(
            "expected integer, got {other:?}"
        ))),
    }
}

fn as_text(v: &Value) -> Result<&str> {
    match v {
        Value::Text(s) => Ok(s.as_str()),
        other => Err(MemError::Invalid(format!("expected text, got {other:?}"))),
    }
}

/// Boolean columns (e.g. `immutable`) are stored as INTEGER 0/1.
fn as_bool(v: &Value) -> bool {
    matches!(v, Value::Integer(i) if *i != 0)
}

fn as_exact_bool(v: &Value, field: &str) -> Result<bool> {
    match v {
        Value::Integer(0) => Ok(false),
        Value::Integer(1) => Ok(true),
        other => Err(MemError::Invalid(format!(
            "stored {field} must be INTEGER 0 or 1, got {other:?}"
        ))),
    }
}

/// Embedded, validated, serialized column values for one atom insert.
struct PreparedAtomRow {
    vec: Vec<f32>,
    payload: String,
    created: Value,
    expires: Value,
    immutable: i64,
}

fn prepare_atom_row(h: &RegionHandle, key: &str, atom: &AtomInput) -> Result<PreparedAtomRow> {
    validate_atom_input(atom)?;
    let vec = match &atom.embedding {
        Some(supplied) => supplied.clone(),
        None => embed_one(&*h.embedder, &atom.text)?,
    };
    validate_embedding(key, h.dim, &vec, "passage")?;
    let payload = serde_json::to_string(&atom.payload)
        .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;
    Ok(PreparedAtomRow {
        vec,
        payload,
        created: Value::Timestamp(atom.created_at.unwrap_or_else(now_micros)),
        expires: atom.expires_at.map(Value::Timestamp).unwrap_or(Value::Null),
        immutable: i64::from(atom.immutable),
    })
}

/// Domain tags for keyed-idempotency identity material.
const IK_KEY_DOMAIN: &[u8] = b"citadel-mem-ik-key-v1";
const IK_REQUEST_DOMAIN: &[u8] = b"citadel-mem-ik-req-v1";

/// Hex identity tag over `material`: keyed BLAKE3 for encrypted regions (no
/// plaintext-equality oracle reaches disk), plain BLAKE3 for plaintext ones.
fn identity_tag(mac: Option<&IdentityMacKey>, material: &[u8]) -> String {
    match mac {
        Some(mac) => blake3::keyed_hash(&mac.key, material).to_hex().to_string(),
        None => blake3::hash(material).to_hex().to_string(),
    }
}

fn push_len_prefixed(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn push_opt_micros(buf: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(v) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        None => buf.push(0),
    }
}

/// Identity tag of one caller idempotency key, bound to its `kind` scope so
/// a key reused across kinds leaves no visible equality on disk.
fn identity_key_tag(mac: Option<&IdentityMacKey>, kind: &str, idempotency_key: &str) -> String {
    let mut material = Zeroizing::new(Vec::new());
    push_len_prefixed(&mut material, IK_KEY_DOMAIN);
    push_len_prefixed(&mut material, kind.as_bytes());
    push_len_prefixed(&mut material, idempotency_key.as_bytes());
    identity_tag(mac, &material)
}

/// Canonical tag binding every semantic input of one keyed remember (key
/// tag included, so identical requests under different keys stay distinct
/// on disk). `AtomInput` destructures exhaustively: a new field must extend
/// the material AND bump the domain tag (frozen vectors pin the encoding).
fn identity_request_tag(
    mac: Option<&IdentityMacKey>,
    key_tag: &str,
    atom: &AtomInput,
    payload_json: &str,
    src_ids: &[AtomId],
    evidence_ref: Option<&serde_json::Value>,
) -> Result<String> {
    let AtomInput {
        kind,
        text,
        // The canonical serialization is `payload_json`.
        payload: _,
        score,
        confidence,
        created_at,
        expires_at,
        immutable,
        // Not identity material: hashing it would change every key already written.
        embedding: _,
    } = atom;
    let evidence = match evidence_ref {
        Some(v) => serde_json::to_string(v)
            .map_err(|e| MemError::Invalid(format!("evidence_ref not serializable: {e}")))?,
        None => String::new(),
    };
    let mut material = Zeroizing::new(Vec::new());
    push_len_prefixed(&mut material, IK_REQUEST_DOMAIN);
    push_len_prefixed(&mut material, key_tag.as_bytes());
    push_len_prefixed(&mut material, kind.as_bytes());
    push_len_prefixed(&mut material, text.as_bytes());
    push_len_prefixed(&mut material, payload_json.as_bytes());
    material.extend_from_slice(&score.to_bits().to_le_bytes());
    material.extend_from_slice(&confidence.to_bits().to_le_bytes());
    push_opt_micros(&mut material, *created_at);
    push_opt_micros(&mut material, *expires_at);
    material.push(u8::from(*immutable));
    material.extend_from_slice(&(src_ids.len() as u64).to_le_bytes());
    for &id in src_ids {
        material.extend_from_slice(&id.to_le_bytes());
    }
    push_len_prefixed(&mut material, evidence.as_bytes());
    Ok(identity_tag(mac, &material))
}

/// Nullable `expires_at` gate for snapshot members: present-but-lapsed is a
/// distinct, loud error (unlike recall, which silently hides expired atoms).
fn verify_snapshot_unexpired(v: &Value, id: AtomId, now: i64) -> Result<()> {
    match opt_ts(v) {
        Some(t) if t <= now => Err(MemError::Invalid(format!("source atom {id} expired"))),
        _ => Ok(()),
    }
}

fn dedup_sources(sources: &[AtomId]) -> Vec<AtomId> {
    let mut ids = sources.to_vec();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Verify sources first: a doomed sealed insert would leak fsync'd key slots.
fn verify_sources_exist(
    conn: &Connection<'_>,
    h: &RegionHandle,
    region_key: &str,
    src_ids: &[AtomId],
) -> Result<()> {
    if src_ids.is_empty() {
        return Ok(());
    }
    let table = &h.table;
    let ph: Vec<String> = (2..=src_ids.len() + 1).map(|i| format!("${i}")).collect();
    let mut params: Vec<Value> = Vec::with_capacity(src_ids.len() + 1);
    params.push(Value::Integer(h.id));
    params.extend(src_ids.iter().map(|&s| Value::Integer(s)));
    let qr = conn.query_params(
        &format!(
            "SELECT COUNT(*) FROM {table} WHERE region_id = $1 AND id IN ({})",
            ph.join(", ")
        ),
        &params,
    )?;
    let found = match qr.rows.first().and_then(|r| r.first()) {
        Some(Value::Integer(n)) => *n as usize,
        other => {
            return Err(MemError::Invalid(format!(
                "COUNT returned no integer: {other:?}"
            )))
        }
    };
    if found != src_ids.len() {
        return Err(MemError::Invalid(format!(
            "{} of {} source atoms not found in region '{region_key}'",
            src_ids.len() - found,
            src_ids.len()
        )));
    }
    Ok(())
}

fn link_derived_sources(
    conn: &Connection<'_>,
    id: AtomId,
    src_ids: &[AtomId],
    evidence_ref: Option<&serde_json::Value>,
) -> Result<()> {
    for &src in src_ids {
        link_edge(conn, id, src, EdgeKind::DerivedFrom, 1.0, evidence_ref)?;
    }
    Ok(())
}

/// Inside the caller's write transaction: delete identity records, incident
/// edges, then rows of `in_list`, returning the row DELETE's count. All
/// deletes subselect region-scoped rows, so unverified caller ids can never
/// touch another region's records.
/// `(slot, atom_id, generation)` per atom key, which only its live row names:
/// read this before deleting the rows that hold it.
fn atom_key_slots(
    conn: &Connection<'_>,
    h: &RegionHandle,
    in_list: &str,
) -> Result<Vec<(u32, u64, u64)>> {
    let qr = conn.query_params(
        &format!(
            "SELECT id, key_slot, key_gen FROM {table} \
             WHERE region_id = $1 AND id IN ({in_list})",
            table = h.table
        ),
        &[Value::Integer(h.id)],
    )?;
    qr.rows
        .iter()
        .map(|row| {
            Ok((
                as_int(&row[1])? as u32,
                as_int(&row[0])? as u64,
                as_int(&row[2])? as u64,
            ))
        })
        .collect()
}

fn delete_atoms_in_txn(conn: &Connection<'_>, h: &RegionHandle, in_list: &str) -> Result<u64> {
    let table = &h.table;
    conn.execute_params(
        &format!(
            "DELETE FROM memory_idempotency WHERE atom_id IN \
             (SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list}))"
        ),
        &[Value::Integer(h.id)],
    )?;
    conn.execute_params(
        &format!(
            "DELETE FROM memory_edges WHERE \
             src_id IN (SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list})) \
             OR dst_id IN (SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list}))"
        ),
        &[Value::Integer(h.id)],
    )?;
    let deleted = conn.execute_params(
        &format!("DELETE FROM {table} WHERE region_id = $1 AND id IN ({in_list})"),
        &[Value::Integer(h.id)],
    )?;
    Ok(match deleted {
        ExecutionResult::RowsAffected(n) => n,
        _ => 0,
    })
}

fn build_erasure_receipt(
    encrypted: bool,
    rows_deleted: u64,
    slots_erased: Vec<SlotErasure>,
    immutable_skipped: Vec<AtomId>,
) -> ErasureReceipt {
    let slots_erased_empty = slots_erased.is_empty();
    ErasureReceipt {
        cryptographic_erasure: encrypted,
        rows_deleted,
        erased_count: slots_erased.len() as u64,
        slots_erased,
        immutable_skipped,
        algorithm: if encrypted { "AES-256-KW(RFC3394)" } else { "" },
        wrapped_key_size: if encrypted {
            WRAPPED_KEY_SIZE as u32
        } else {
            0
        },
        // Claim durability only for erasures that passed the key store's
        // overwrite + fsync + read-back gate; an empty one attests nothing.
        fsync: encrypted && !slots_erased_empty,
        readback_confirmed: encrypted && !slots_erased_empty,
        scope_caveat: ERASURE_SCOPE_CAVEAT,
    }
}

/// Split cascade roots: in this region -> seeds, absent -> ignored (retries
/// converge), owned by another region -> loud error naming it. Runs inside
/// the caller's write transaction.
fn classify_cascade_roots(
    conn: &Connection<'_>,
    h: &RegionHandle,
    region_key: &str,
    ids: &[AtomId],
) -> Result<Vec<AtomId>> {
    let roots = dedup_sources(ids);
    if roots.is_empty() {
        return Ok(Vec::new());
    }
    let in_list = roots
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let qr = conn.query_params(
        &format!(
            "SELECT id, region_id FROM {} WHERE id IN ({in_list})",
            h.table
        ),
        &[],
    )?;
    let mut present = Vec::with_capacity(qr.rows.len());
    for row in &qr.rows {
        let id = as_int(&row[0])?;
        let owner = as_int(&row[1])?;
        if owner != h.id {
            let named = conn.query_params(
                "SELECT name FROM memory_regions WHERE id = $1",
                &[Value::Integer(owner)],
            )?;
            let owner_name = match named.rows.first() {
                Some(r) => format!("region '{}'", as_text(&r[0])?),
                None => format!("region id {owner}"),
            };
            return Err(MemError::Invalid(format!(
                "cascade root {id} belongs to {owner_name}, not '{region_key}'"
            )));
        }
        present.push(id);
    }
    present.sort_unstable();
    Ok(present)
}

/// The requested ids plus every region atom whose `DerivedFrom` closure
/// reaches them, to a fixpoint (cycle-safe via the visited set). Runs inside
/// the caller's write transaction.
fn dependent_closure(
    conn: &Connection<'_>,
    h: &RegionHandle,
    ids: &[AtomId],
) -> Result<Vec<AtomId>> {
    let mut visited: FxHashSet<AtomId> = ids.iter().copied().collect();
    let mut wave: Vec<AtomId> = ids.to_vec();
    while !wave.is_empty() {
        let in_list = wave
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let qr = conn.query_params(
            &format!(
                "SELECT e.src_id FROM memory_edges e JOIN {} a \
                 ON a.id = e.src_id AND a.region_id = $1 \
                 WHERE e.kind = $2 AND e.dst_id IN ({in_list})",
                h.table
            ),
            &[
                Value::Integer(h.id),
                Value::Text(EdgeKind::DerivedFrom.as_str().into()),
            ],
        )?;
        let mut next = Vec::new();
        for row in &qr.rows {
            let id = as_int(&row[0])?;
            if visited.insert(id) {
                next.push(id);
            }
        }
        wave = next;
    }
    let mut out: Vec<AtomId> = visited.into_iter().collect();
    out.sort_unstable();
    Ok(out)
}

/// Which of `ids` are immutable in the region, ascending. Runs inside the
/// caller's write transaction.
fn immutable_members(
    conn: &Connection<'_>,
    h: &RegionHandle,
    ids: &[AtomId],
) -> Result<Vec<AtomId>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let in_list = ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let qr = conn.query_params(
        &format!(
            "SELECT id FROM {} WHERE region_id = $1 AND id IN ({in_list}) \
             AND immutable = 1 ORDER BY id",
            h.table
        ),
        &[Value::Integer(h.id)],
    )?;
    qr.rows.iter().map(|r| as_int(&r[0])).collect()
}

/// Which of `ids` are the target of a `supersedes` edge (stale versions).
fn superseded_ids(conn: &Connection<'_>, ids: &[AtomId]) -> Result<FxHashSet<AtomId>> {
    if ids.is_empty() {
        return Ok(FxHashSet::default());
    }
    let ph: Vec<String> = (1..=ids.len()).map(|i| format!("${i}")).collect();
    let params: Vec<Value> = ids.iter().map(|&i| Value::Integer(i)).collect();
    let qr = conn.query_params(
        &format!(
            "SELECT DISTINCT dst_id FROM memory_edges \
             WHERE kind = 'supersedes' AND dst_id IN ({})",
            ph.join(", ")
        ),
        &params,
    )?;
    qr.rows.iter().map(|r| as_int(&r[0])).collect()
}

/// Upsert one edge (caller owns the txn); rejects self-loops (every kind) and
/// cycles (acyclic kinds).
fn link_edge(
    conn: &Connection<'_>,
    src: AtomId,
    dst: AtomId,
    kind: EdgeKind,
    weight: f32,
    evidence_ref: Option<&serde_json::Value>,
) -> Result<()> {
    validate_edge_weight(weight)?;
    if src == dst {
        return Err(MemError::Cycle { src, dst });
    }
    if kind.is_acyclic() && would_cycle(conn, src, dst, kind)? {
        return Err(MemError::Cycle { src, dst });
    }
    let evidence = match evidence_ref {
        Some(v) => Value::Text(
            serde_json::to_string(v)
                .map_err(|e| MemError::Invalid(format!("evidence_ref not serializable: {e}")))?
                .into(),
        ),
        None => Value::Null,
    };
    conn.execute_params(
        "INSERT INTO memory_edges (src_id, dst_id, kind, weight, evidence_ref) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (src_id, dst_id, kind) DO UPDATE \
         SET weight = excluded.weight, evidence_ref = excluded.evidence_ref",
        &[
            Value::Integer(src),
            Value::Integer(dst),
            Value::Text(kind.as_str().into()),
            Value::Real(weight as f64),
            evidence,
        ],
    )?;
    Ok(())
}

fn validate_edge_weight(weight: f32) -> Result<()> {
    if weight.is_finite() {
        Ok(())
    } else {
        Err(MemError::Invalid("edge weight must be finite".into()))
    }
}

fn validate_fusion_weights(weights: FusionWeights) -> Result<()> {
    for (name, value) in [
        ("semantic", weights.semantic),
        ("keyword", weights.keyword),
        ("recency", weights.recency),
        ("importance", weights.importance),
    ] {
        if !value.is_finite() {
            return Err(MemError::Invalid(format!(
                "recall fusion weight '{name}' must be finite"
            )));
        }
    }
    Ok(())
}

fn validate_rrf_k(k: f32, label: &str) -> Result<()> {
    if !k.is_finite() || k <= 0.0 {
        return Err(MemError::Invalid(format!(
            "{label} must be finite and greater than zero"
        )));
    }
    Ok(())
}

fn validate_rerank_strategy(strategy: RerankStrategy) -> Result<()> {
    if let RerankStrategy::Rrf { k } = strategy {
        validate_rrf_k(k, "reranker RRF constant")?;
    }
    Ok(())
}

/// Reject NaN/infinity: codecs must never assign them ordering semantics.
fn validate_atom_input(atom: &AtomInput) -> Result<()> {
    if !atom.score.is_finite() {
        return Err(MemError::Invalid("atom score must be finite".into()));
    }
    if !atom.confidence.is_finite() {
        return Err(MemError::Invalid("atom confidence must be finite".into()));
    }
    Ok(())
}

/// One boundary for both caller-supplied and embedder-produced vectors.
fn validate_embedding(region: &str, expected_dim: u16, vector: &[f32], role: &str) -> Result<()> {
    if vector.len() != expected_dim as usize {
        return Err(MemError::DimMismatch {
            region: region.into(),
            expected: expected_dim,
            got: vector.len(),
        });
    }
    if let Some((component, _)) = vector
        .iter()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(MemError::Invalid(format!(
            "{role} embedding for region '{region}' has a non-finite component at index {component}"
        )));
    }
    Ok(())
}

fn select_ids(conn: &Connection<'_>, sql: &str, params: &[Value]) -> Result<Vec<AtomId>> {
    let qr = conn.query_params(sql, params)?;
    qr.rows.iter().map(|r| as_int(&r[0])).collect()
}

fn evict_target_ids(
    conn: &Connection<'_>,
    table: &str,
    region_id: RegionId,
    policy: &EvictionPolicy,
    now: i64,
    accessed: &FxHashMap<AtomId, (i64, u32)>,
) -> Result<Vec<AtomId>> {
    match policy {
        EvictionPolicy::Stale { older_than_micros } => {
            // Persisted floor (access_count at insert) plus in-process read
            // hits: an atom recalled this lifetime is not "never accessed".
            let mut ids = select_ids(
                conn,
                &format!(
                    "SELECT id FROM {table} WHERE region_id = $1 AND immutable = 0 \
                     AND access_count = 0 AND created_at < $2"
                ),
                &[
                    Value::Integer(region_id),
                    Value::Timestamp(now - older_than_micros),
                ],
            )?;
            ids.retain(|id| !accessed.contains_key(id));
            Ok(ids)
        }
        EvictionPolicy::LowScore {
            score_threshold,
            confidence_threshold,
        } => select_ids(
            conn,
            &format!(
                "SELECT id FROM {table} WHERE region_id = $1 AND immutable = 0 \
                 AND score < $2 AND confidence < $3"
            ),
            &[
                Value::Integer(region_id),
                Value::Real(*score_threshold as f64),
                Value::Real(*confidence_threshold as f64),
            ],
        ),
        EvictionPolicy::Expired => select_ids(
            conn,
            &format!(
                "SELECT id FROM {table} WHERE region_id = $1 AND immutable = 0 \
                 AND expires_at IS NOT NULL AND expires_at <= $2"
            ),
            &[Value::Integer(region_id), Value::Timestamp(now)],
        ),
        EvictionPolicy::PurgeRegion => select_ids(
            conn,
            &format!("SELECT id FROM {table} WHERE region_id = $1"),
            &[Value::Integer(region_id)],
        ),
        EvictionPolicy::PredicateMatch { predicate } => {
            let js = serde_json::to_string(predicate)
                .map_err(|e| MemError::Invalid(format!("predicate not serializable: {e}")))?;
            select_ids(
                conn,
                &format!(
                    "SELECT id FROM {table} WHERE region_id = $1 AND immutable = 0 \
                     AND payload @> CAST($2 AS JSONB)"
                ),
                &[Value::Integer(region_id), Value::Text(js.into())],
            )
        }
        EvictionPolicy::Lru { keep_fraction } => {
            let count_qr = conn.query_params(
                &format!("SELECT COUNT(*) FROM {table} WHERE region_id = $1 AND immutable = 0"),
                &[Value::Integer(region_id)],
            )?;
            let total = count_qr
                .rows
                .first()
                .map(|r| as_int(&r[0]))
                .transpose()?
                .unwrap_or(0)
                .max(0);
            let delete_n = ((total as f32) * (1.0 - keep_fraction.clamp(0.0, 1.0))).floor() as i64;
            if delete_n <= 0 {
                return Ok(Vec::new());
            }
            // Least-recently-accessed first: persisted floor merged with
            // in-process hits, ranked in Rust (ties by ascending id).
            let qr = conn.query_params(
                &format!(
                    "SELECT id, accessed_at, access_count FROM {table} \
                     WHERE region_id = $1 AND immutable = 0"
                ),
                &[Value::Integer(region_id)],
            )?;
            let mut rows: Vec<(i64, u32, AtomId)> = Vec::with_capacity(qr.rows.len());
            for r in &qr.rows {
                let id = as_int(&r[0])?;
                let mut last = as_ts(&r[1]);
                let mut count = as_int(&r[2])?.max(0) as u32;
                if let Some(&(mem_last, mem_count)) = accessed.get(&id) {
                    last = last.max(mem_last);
                    count += mem_count;
                }
                rows.push((last, count, id));
            }
            rows.sort_unstable();
            rows.truncate(delete_n as usize);
            Ok(rows.into_iter().map(|(_, _, id)| id).collect())
        }
    }
}

/// True if adding `src -> dst` would close a cycle over `kind` edges.
fn would_cycle(conn: &Connection<'_>, src: AtomId, dst: AtomId, kind: EdgeKind) -> Result<bool> {
    if src == dst {
        return Ok(true);
    }
    let qr = conn.query_params(
        "WITH RECURSIVE reach(node) AS (\
           SELECT $1 \
           UNION \
           SELECT e.dst_id FROM memory_edges e JOIN reach r ON e.src_id = r.node \
           WHERE e.kind = $3\
         ) SELECT 1 FROM reach WHERE node = $2 LIMIT 1",
        &[
            Value::Integer(dst),
            Value::Integer(src),
            Value::Text(kind.as_str().into()),
        ],
    )?;
    Ok(!qr.rows.is_empty())
}

/// BFS depth of each non-seed atom reachable from `seeds` over `memory_edges`
/// up to `ge.depth` hops (optionally filtered by edge kind). Edges are
/// plaintext for both flavors.
fn graph_walk_depths(
    conn: &Connection<'_>,
    seeds: &[AtomId],
    ge: &GraphExpand,
) -> Result<FxHashMap<AtomId, usize>> {
    let mut params: Vec<Value> = seeds.iter().map(|&s| Value::Integer(s)).collect();
    let seed_ph: Vec<String> = (1..=seeds.len()).map(|i| format!("${i}")).collect();
    let kind_clause = if ge.kinds.is_empty() {
        String::new()
    } else {
        let mut ph = Vec::with_capacity(ge.kinds.len());
        for k in &ge.kinds {
            params.push(Value::Text(k.as_str().into()));
            ph.push(format!("${}", params.len()));
        }
        format!(" AND e.kind IN ({})", ph.join(", "))
    };

    let walk_sql = format!(
        "WITH RECURSIVE walk(node, depth) AS (\
           SELECT e.dst_id, 1 FROM memory_edges e WHERE e.src_id IN ({seeds}){kc} \
           UNION \
           SELECT e.dst_id, w.depth + 1 FROM memory_edges e JOIN walk w ON e.src_id = w.node \
           WHERE w.depth < {maxd}{kc}\
         ) SELECT node, depth FROM walk",
        seeds = seed_ph.join(", "),
        kc = kind_clause,
        maxd = ge.depth,
    );
    let walked = conn.query_params(&walk_sql, &params)?;

    let seed_set: FxHashSet<AtomId> = seeds.iter().copied().collect();
    let mut depth_of: FxHashMap<AtomId, usize> = FxHashMap::default();
    for row in &walked.rows {
        let id = as_int(&row[0])?;
        if seed_set.contains(&id) {
            continue;
        }
        let d = as_int(&row[1])?.max(0) as usize;
        let slot = depth_of.entry(id).or_insert(usize::MAX);
        *slot = (*slot).min(d);
    }
    Ok(depth_of)
}

/// Order graph-reached `(depth, hit)` pairs nearest-first (ties by id),
/// dropping depth.
fn order_graph_hits(mut hits: Vec<(usize, AtomHit)>) -> Vec<AtomHit> {
    hits.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.id.cmp(&b.1.id)));
    hits.into_iter().map(|(_, h)| h).collect()
}

#[derive(Clone, Copy)]
struct GraphFetchScope<'a> {
    table: &'a str,
    region_id: RegionId,
    kind_allowlist: &'a [String],
    /// The query's JSONB containment filter; expansion honours it like the
    /// seeds, so a filtered recall can't widen through edges.
    payload_filter: Option<&'a serde_json::Value>,
}

/// In-clause placeholders `$2..` for `depth_of`'s ids plus the `[region_id,
/// ids..]` param vector. `kind_allowlist` applies to expanded atoms as to the
/// seeds.
fn graph_fetch_params(
    scope: GraphFetchScope<'_>,
    depth_of: &FxHashMap<AtomId, usize>,
) -> (Vec<Value>, String, String) {
    let mut fparams: Vec<Value> = vec![Value::Integer(scope.region_id)];
    let mut fph = Vec::with_capacity(depth_of.len());
    for &id in depth_of.keys() {
        fparams.push(Value::Integer(id));
        fph.push(format!("${}", fparams.len()));
    }
    let kind_clause = if scope.kind_allowlist.is_empty() {
        String::new()
    } else {
        let mut ph = Vec::with_capacity(scope.kind_allowlist.len());
        for kind in scope.kind_allowlist {
            fparams.push(Value::Text(kind.clone().into()));
            ph.push(format!("${}", fparams.len()));
        }
        format!(" AND kind IN ({})", ph.join(", "))
    };
    (fparams, fph.join(", "), kind_clause)
}

/// Walk `memory_edges` from `seeds` up to `ge.depth` hops; reachable atoms,
/// nearest first.
fn expand_graph(
    conn: &Connection<'_>,
    scope: GraphFetchScope<'_>,
    seeds: &[AtomId],
    ge: &GraphExpand,
) -> Result<Vec<AtomHit>> {
    if seeds.is_empty() || ge.depth == 0 {
        return Ok(Vec::new());
    }
    let depth_of = graph_walk_depths(conn, seeds, ge)?;
    if depth_of.is_empty() {
        return Ok(Vec::new());
    }
    let (fparams, in_list, kind_clause) = graph_fetch_params(scope, &depth_of);
    let table = scope.table;
    let fetch_sql = format!(
        "SELECT id, kind, CAST(payload AS TEXT), text_content, immutable, created_at \
         FROM {table} WHERE region_id = $1 AND id IN ({in_list}){kind_clause}"
    );
    let fetched = conn.query_params(&fetch_sql, &fparams)?;

    let mut hits: Vec<(usize, AtomHit)> = Vec::with_capacity(fetched.rows.len());
    for row in &fetched.rows {
        let id = as_int(&row[0])?;
        let depth = *depth_of.get(&id).unwrap_or(&1);
        let payload = parse_payload(&row[2]);
        if let Some(filter) = scope.payload_filter {
            if !json_contains(&payload, filter) {
                continue;
            }
        }
        hits.push((
            depth,
            AtomHit {
                id,
                kind: as_text(&row[1])?.to_string(),
                payload,
                text: opt_text(&row[3]),
                distance: f32::MAX, // graph-reached, not distance-ranked
                score: 1.0 / (depth as f32 + 1.0),
                created_at: as_ts(&row[5]),
                immutable: as_bool(&row[4]),
            },
        ));
    }
    Ok(order_graph_hits(hits))
}

/// Graph expansion for an encrypted region: walk plaintext edges, then decrypt
/// the reachable atoms' sealed content.
fn expand_graph_sealed(
    conn: &Connection<'_>,
    atom_wrap: &AtomWrapKey,
    wrapped: &FxHashMap<u64, [u8; WRAPPED_KEY_SIZE]>,
    scope: GraphFetchScope<'_>,
    seeds: &[AtomId],
    ge: &GraphExpand,
) -> Result<Vec<AtomHit>> {
    if seeds.is_empty() || ge.depth == 0 {
        return Ok(Vec::new());
    }
    let depth_of = graph_walk_depths(conn, seeds, ge)?;
    if depth_of.is_empty() {
        return Ok(Vec::new());
    }
    let (fparams, in_list, kind_clause) = graph_fetch_params(scope, &depth_of);
    let table = scope.table;
    let fetch_sql = format!(
        "SELECT id, kind, sealed, immutable, created_at FROM {table} \
         WHERE region_id = $1 AND id IN ({in_list}){kind_clause}"
    );
    let fetched = conn.query_params(&fetch_sql, &fparams)?;

    let mut hits: Vec<(usize, AtomHit)> = Vec::with_capacity(fetched.rows.len());
    for row in &fetched.rows {
        let id = as_int(&row[0])?;
        let depth = *depth_of.get(&id).unwrap_or(&1);
        let Some(w) = wrapped.get(&(id as u64)) else {
            continue;
        };
        let (mut text, mut payload) = open_atom_content(atom_wrap, w, id, as_blob(&row[2])?)?;
        if let Some(filter) = scope.payload_filter {
            if !json_contains(&payload, filter) {
                zeroize_atom_content(&mut text, &mut payload);
                continue;
            }
        }
        hits.push((
            depth,
            AtomHit {
                id,
                kind: as_text(&row[1])?.to_string(),
                payload,
                text,
                distance: f32::MAX,
                score: 1.0 / (depth as f32 + 1.0),
                created_at: as_ts(&row[4]),
                immutable: as_bool(&row[3]),
            },
        ));
    }
    Ok(order_graph_hits(hits))
}

fn embed_one(embedder: &dyn Embedder, text: &str) -> Result<Vec<f32>> {
    embedder
        .embed(&[text])?
        .into_iter()
        .next()
        .ok_or_else(|| MemError::Invalid("embedder returned no vector".into()))
}

/// Query-side embedding: asymmetric models (E5) encode queries differently.
fn embed_query_one(embedder: &dyn Embedder, text: &str) -> Result<Vec<f32>> {
    embedder
        .embed_queries(&[text])?
        .into_iter()
        .next()
        .ok_or_else(|| MemError::Invalid("embedder returned no vector".into()))
}

/// Columns: id, kind, payload(text), text_content, score, created_at, dist,
/// text_rank, immutable.
fn parse_candidate(row: &[Value]) -> Result<Candidate> {
    if row.len() < 9 {
        return Err(MemError::Invalid("unexpected recall row shape".into()));
    }
    Ok(Candidate {
        id: as_int(&row[0])?,
        kind: as_text(&row[1])?.to_string(),
        payload: parse_payload(&row[2]),
        text: opt_text(&row[3]),
        importance: as_f32(&row[4]),
        created_micros: as_ts(&row[5]),
        dist: dist_value(&row[6]),
        text_rank: as_f32(&row[7]),
        immutable: as_bool(&row[8]),
    })
}

/// Columns: id, kind, payload(text), text_content, score, immutable,
/// created_at.
fn parse_fetched(row: &[Value]) -> Result<AtomHit> {
    if row.len() < 7 {
        return Err(MemError::Invalid("unexpected fetch row shape".into()));
    }
    Ok(AtomHit {
        id: as_int(&row[0])?,
        kind: as_text(&row[1])?.to_string(),
        payload: parse_payload(&row[2]),
        text: opt_text(&row[3]),
        distance: f32::MAX,
        score: as_f32(&row[4]),
        created_at: as_ts(&row[6]),
        immutable: as_bool(&row[5]),
    })
}

/// Columns: src_id, dst_id, kind, weight, evidence_ref(text).
fn parse_edge(row: &[Value]) -> Result<Edge> {
    if row.len() < 5 {
        return Err(MemError::Invalid("unexpected edge row shape".into()));
    }
    let evidence_ref = match &row[4] {
        Value::Null => None,
        other => Some(parse_payload(other)),
    };
    Ok(Edge {
        src_id: as_int(&row[0])?,
        dst_id: as_int(&row[1])?,
        kind: edge_kind_from_str(as_text(&row[2])?)?,
        weight: as_f32(&row[3]),
        evidence_ref,
    })
}

fn edge_kind_from_str(s: &str) -> Result<EdgeKind> {
    Ok(match s {
        "causes" => EdgeKind::Causes,
        "contradicts" => EdgeKind::Contradicts,
        "refines" => EdgeKind::Refines,
        "precedes" => EdgeKind::Precedes,
        "supersedes" => EdgeKind::Supersedes,
        "derived_from" => EdgeKind::DerivedFrom,
        "depends_on" => EdgeKind::DependsOn,
        "similar_to" => EdgeKind::SimilarTo,
        other => return Err(MemError::Invalid(format!("unknown edge kind: {other}"))),
    })
}

fn parse_payload(v: &Value) -> serde_json::Value {
    match v {
        Value::Text(s) => serde_json::from_str(s).unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::Null,
    }
}

fn opt_text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.to_string(),
        _ => String::new(),
    }
}

fn as_f32(v: &Value) -> f32 {
    match v {
        Value::Real(r) => *r as f32,
        Value::Integer(i) => *i as f32,
        _ => 0.0,
    }
}

fn exact_f32_bits(v: &Value) -> Result<u32> {
    match v {
        Value::Real(value) => Ok((*value as f32).to_bits()),
        Value::Integer(value) => Ok((*value as f32).to_bits()),
        other => Err(MemError::Invalid(format!(
            "expected stored f32 score, got {other:?}"
        ))),
    }
}

fn as_ts(v: &Value) -> i64 {
    match v {
        Value::Timestamp(t) => *t,
        Value::Integer(i) => *i,
        _ => 0,
    }
}

/// Nullable TIMESTAMP column (`NULL` -> `None`).
fn opt_ts(v: &Value) -> Option<i64> {
    match v {
        Value::Timestamp(t) => Some(*t),
        Value::Integer(i) => Some(*i),
        _ => None,
    }
}

fn exact_opt_ts(v: &Value) -> Result<Option<i64>> {
    match v {
        Value::Timestamp(value) | Value::Integer(value) => Ok(Some(*value)),
        Value::Null => Ok(None),
        other => Err(MemError::Invalid(format!(
            "expected nullable stored timestamp, got {other:?}"
        ))),
    }
}

/// NULL distance (e.g. cosine of a zero-norm vector) sorts worst.
fn dist_value(v: &Value) -> f32 {
    match v {
        Value::Real(r) => *r as f32,
        Value::Integer(i) => *i as f32,
        _ => f32::MAX,
    }
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;
