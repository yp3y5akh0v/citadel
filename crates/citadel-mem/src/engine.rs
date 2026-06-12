//! MemoryEngine: region lifecycle on top of citadel's encrypted SQL store.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use rustc_hash::{FxHashMap, FxHashSet};

use citadel::Database;
use citadel_core::WRAPPED_KEY_SIZE;
use citadel_crypto::blob_seal;
use citadel_crypto::hkdf_utils::{derive_atom_wrap_key, derive_seal_keys, AtomWrapKey};
use citadel_sql::executor::{AnnIndexSource, AnnSegmentInfo};
use citadel_sql::{Connection, ExecutionResult, Value};
use citadel_vector::{AnnIndex, Filter, Metric};
use zeroize::Zeroize;

use crate::embed::{Embedder, EmbeddingMetric, Reranker};
use crate::error::{MemError, Result};
use crate::fusion::{fuse_rank, fuse_rerank, Candidate};
use crate::types::{
    AtomAttestation, AtomHit, AtomId, AtomInput, AttestVerdict, Edge, EdgeKind, ErasureReceipt,
    EvictionPolicy, EvictionReport, EvolutionReport, GraphExpand, KindDigest, RecallQuery,
    RerankStrategy, SlotErasure, SummaryReport, ERASURE_SCOPE_CAVEAT,
};
use citadel::SlotState;

/// Upper bound on rows scanned for encrypted decrypt-then-rank operations (no ANN/FTS
/// index can operate over ciphertext). Mirrors the plaintext recall exact-scan cap.
const EXACT_SCAN_LIMIT: usize = 4096;

/// Over-fetch ANN candidates by this factor before fusion re-ranking + filtering.
const CAND_OVERFETCH: usize = 8;
/// Always evaluate at least this many ANN candidates (small-k recall stability).
const MIN_CANDIDATES: usize = 64;
/// Rebuild the whole sealed ANN index when the post-snapshot tail exceeds this many atoms.
const REBUILD_TAIL_MAX: usize = 2048;

/// Stable identifier for a memory region (row id in `memory_regions`).
pub type RegionId = i64;

/// A region attached to a live embedder in this process.
struct RegionState {
    id: RegionId,
    dim: u16,
    metric: EmbeddingMetric,
    embedder: Arc<dyn Embedder>,
    /// `Some` for an encrypted region: the region's atom-wrap key (derived from the
    /// random RCK). Each atom is sealed under its own random key (ACK) whose sole
    /// wrapped copy lives in the atom key store; this key wraps/unwraps those ACKs.
    atom_wrap: Option<Arc<AtomWrapKey>>,
    /// Ephemeral in-RAM ANN index over decrypted vectors for sealed recall (lazy).
    ann: Arc<RwLock<Option<SealedAnn>>>,
    /// Highest atom id ever assigned to this region (DB max at attach, bumped by
    /// remember). Sealed recall reads this to detect post-snapshot inserts instead of
    /// running a `MAX(id)` table scan on every call.
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
    ann: Arc<RwLock<Option<SealedAnn>>>,
    max_id: Arc<AtomicI64>,
}

/// Ephemeral, per-region in-RAM PRISM index over DECRYPTED vectors, built lazily on the
/// first sealed recall. The plaintext vectors are zeroized when the index drops (see
/// `PointStore`'s `Drop`), so they never outlive the region key.
///
/// The GRAPH (not the vectors) may be persisted as a sealed segment: the
/// segment ciphertext lives under its own random key held in the ERASABLE atom
/// key store, so destroying that one slot crypto-erases every on-disk
/// derivative (SQ8 codes are near-lossless embedding reconstructions - they
/// must never outlive erasure any more than the atoms themselves).
struct SealedAnn {
    index: AnnIndex,
    /// Atom `kind` -> PRISM attribute code, so a kind-filtered recall maps to a `Filter`.
    kind_codes: FxHashMap<String, u32>,
    /// Everything recall needs to rank a candidate, captured during the single index build
    /// so the hot path reads it from RAM instead of re-fetching every candidate row (a
    /// 400-element `WHERE id IN (...)` per recall) and re-decrypting on every call.
    /// Plaintext, so zeroized on drop alongside the index vectors (never outlives the key).
    cached: FxHashMap<AtomId, CachedAtom>,
    /// Whether this index came from the persisted sealed segment or a scan build.
    source: AnnIndexSource,
}

/// The per-atom fields a sealed recall needs to build a `Candidate`, decrypted once at
/// index build. `score`/`created`/`immutable` are immutable post-insert (or refreshed on
/// the next index rebuild), so caching them is consistent with caching `text`/`payload`.
struct CachedAtom {
    kind: String,
    text: String,
    payload: serde_json::Value,
    importance: f32,
    created_micros: i64,
    immutable: bool,
}

impl Drop for SealedAnn {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        for ca in self.cached.values_mut() {
            ca.text.zeroize();
        }
    }
}

/// Map the memory metric to PRISM's distance metric (1:1).
fn ann_metric(m: EmbeddingMetric) -> Metric {
    match m {
        EmbeddingMetric::Cosine => Metric::Cosine,
        EmbeddingMetric::L2 => Metric::L2,
        EmbeddingMetric::InnerProduct => Metric::InnerProduct,
    }
}

/// Encrypted-first agent memory engine over a shared [`Database`].
pub struct MemoryEngine {
    db: Arc<Database>,
    regions: Mutex<FxHashMap<String, RegionState>>,
    /// Optional cross-encoder applied in `recall` before truncation; `None` = fusion.
    reranker: Option<Arc<dyn Reranker>>,
    rerank_strategy: RerankStrategy,
}

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
INSERT INTO memory_meta (key, value) VALUES ('next_region_id', 1) ON CONFLICT (key) DO NOTHING;
INSERT INTO memory_meta (key, value) VALUES ('next_atom_id', 1) ON CONFLICT (key) DO NOTHING;";

impl MemoryEngine {
    /// Open the memory engine over a database, creating the catalog tables if absent.
    pub fn open(db: Arc<Database>) -> Result<Self> {
        {
            let conn = Connection::open(&db)?;
            if let Some(e) = conn.execute_script(BOOTSTRAP_SQL).error {
                return Err(e.into());
            }
            // Reject a legacy pre-erasure schema (memory_regions lacks the `encrypted` column).
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
            reranker: None,
            rerank_strategy: RerankStrategy::default(),
        };
        if engine.db.region_keys_enabled() && engine.db.region_store_path().exists() {
            engine.reconcile_region_store()?;
        }
        if engine.db.region_keys_enabled() && engine.db.atom_store_path().exists() {
            engine.reconcile_atom_store()?;
        }
        Ok(engine)
    }

    /// Reclaim slots left LIVE by an interrupted create (key written, row never committed).
    fn reconcile_region_store(&self) -> Result<()> {
        let live = self.db.region_store_live_owners()?;
        if live.is_empty() {
            return Ok(());
        }
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            "SELECT id, rsk_slot FROM memory_regions WHERE encrypted = 1 AND rsk_slot IS NOT NULL",
            &[],
        )?;
        let mut valid: FxHashSet<(u32, u64)> = FxHashSet::default();
        for row in &qr.rows {
            if let (Ok(id), Value::Integer(slot)) = (as_int(&row[0]), &row[1]) {
                valid.insert((*slot as u32, id as u64));
            }
        }
        for (slot, owner) in live {
            if !valid.contains(&(slot, owner)) {
                self.db.region_store_tombstone(slot, owner)?;
            }
        }
        Ok(())
    }

    /// Reclaim atom key slots left LIVE by an interrupted insert (key fsync'd, row never
    /// committed): tombstone any LIVE slot not referenced by a committed atom row.
    fn reconcile_atom_store(&self) -> Result<()> {
        let live = self.db.atom_store_live_owners()?;
        if live.is_empty() {
            return Ok(());
        }
        let conn = Connection::open(&self.db)?;
        // Every (key_slot, atom_id) referenced by a committed encrypted atom row.
        let regions = conn.query_params(
            "SELECT DISTINCT embedding_dim, embedding_metric FROM memory_regions WHERE encrypted = 1",
            &[],
        )?;
        let mut valid: FxHashSet<(u32, u64)> = FxHashSet::default();
        for row in &regions.rows {
            let dim = u16::try_from(as_int(&row[0])?)
                .map_err(|_| MemError::Invalid("stored embedding_dim out of range".into()))?;
            let metric = metric_from_str(as_text(&row[1])?)?;
            let table = atoms_table(dim, metric, true);
            if conn.table_schema(&table).is_none() {
                continue;
            }
            let qr = conn.query_params(&format!("SELECT id, key_slot FROM {table}"), &[])?;
            for r in &qr.rows {
                valid.insert((as_int(&r[1])? as u32, as_int(&r[0])? as u64));
            }
        }
        // Persisted sealed-segment keys are row-less BY DESIGN (a pseudo-atom
        // id owns the slot; the ciphertext lives in a hidden chunk tree): the
        // meta rows are their committed reference.
        let qr = conn.query_params(
            "SELECT key, value FROM memory_meta WHERE key LIKE 'annseg_%'",
            &[],
        )?;
        let mut seg_slots: FxHashMap<String, u32> = FxHashMap::default();
        let mut seg_ids: FxHashMap<String, u64> = FxHashMap::default();
        for row in &qr.rows {
            let key = as_text(&row[0])?;
            let value = as_int(&row[1])?;
            if let Some(region) = key.strip_prefix("annseg_slot:") {
                seg_slots.insert(region.to_string(), value as u32);
            } else if let Some(region) = key.strip_prefix("annseg_id:") {
                seg_ids.insert(region.to_string(), value as u64);
            }
        }
        for (region, slot) in &seg_slots {
            if let Some(&id) = seg_ids.get(region) {
                valid.insert((*slot, id));
            }
        }
        for (slot, owner) in live {
            if !valid.contains(&(slot, owner)) {
                self.db.atom_store_tombstone(slot, owner)?;
            }
        }
        Ok(())
    }

    /// Attach a cross-encoder reranker for later `recall`s, combined per `strategy`.
    pub fn set_reranker(&mut self, reranker: Arc<dyn Reranker>, strategy: RerankStrategy) {
        self.reranker = Some(reranker);
        self.rerank_strategy = strategy;
    }

    /// Get-or-create a plaintext region bound to `embedder` (must match dim/metric/model).
    pub fn create_region(&self, name: &str, embedder: Arc<dyn Embedder>) -> Result<RegionId> {
        self.create_region_inner(name, embedder, false)
    }

    /// Get-or-create an *encrypted* region: each atom is sealed under its own random key
    /// (ACK) wrapped by a per-region key in the sidecar store.
    /// [`drop_region`](Self::drop_region) erases the whole region;
    /// [`forget_atom`](Self::forget_atom) erases one. Requires `enable_region_keys(true)`.
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

        // Fast path: already attached in this process, no DB round-trip.
        if let Some(id) = self.check_attached(&key, dim, metric, &model_id, encrypted)? {
            return Ok(id);
        }

        let conn = Connection::open(&self.db)?;
        let (id, atom_wrap) = match self.load_region_row(&conn, &key)? {
            Some(existing) => {
                existing.verify_matches(&key, dim, metric, &model_id, encrypted)?;
                let atom_wrap = if encrypted {
                    Some(self.attach_region_key(&key, &existing)?)
                } else {
                    None
                };
                (existing.id, atom_wrap)
            }
            None if encrypted => {
                self.insert_encrypted_region(&conn, &key, dim, metric, &model_id)?
            }
            None => (
                self.insert_region(&conn, &key, dim, metric, &model_id)?,
                None,
            ),
        };

        // Seed the in-memory max id once (DB max for this region, 0 if the table is
        // absent), so sealed recall never needs a per-call `MAX(id)` scan.
        let init_max = sealed_max_id(&conn, &atoms_table(dim, metric, atom_wrap.is_some()), id)?;
        self.regions.lock().unwrap().insert(
            key,
            RegionState {
                id,
                dim,
                metric,
                embedder,
                atom_wrap,
                ann: Arc::new(RwLock::new(None)),
                max_id: Arc::new(AtomicI64::new(init_max)),
            },
        );
        Ok(id)
    }

    /// Drop a region and all its atoms and incident edges. No-op if absent.
    ///
    /// For an encrypted region this is cryptographic erasure: the region's content
    /// key is destroyed in the sidecar store (overwrite-in-place + fsync + read-back)
    /// BEFORE any row is deleted, so a crash in between still leaves the content
    /// permanently undecryptable rather than recoverable.
    pub fn drop_region(&self, name: &str) -> Result<()> {
        let key = name.to_ascii_lowercase();
        let conn = Connection::open(&self.db)?;
        let Some(row) = self.load_region_row(&conn, &key)? else {
            self.regions.lock().unwrap().remove(&key);
            return Ok(());
        };
        let atoms = atoms_table(row.dim, row.metric, row.encrypted);

        // Destroy the key and drop the atom-wrap cache BEFORE deleting rows (commit-point ordering).
        if row.encrypted {
            let slot = row.rsk_slot.ok_or_else(|| {
                MemError::Invalid(format!(
                    "encrypted region '{key}' has no key slot; refusing to delete its \
                     rows without destroying a key"
                ))
            })?;
            self.db.region_store_tombstone(slot, row.id as u64)?;
        }
        self.regions.lock().unwrap().remove(&key);

        // Reclaim the region's atom key slots (RCK already destroyed, so these are dead) so
        // a dropped region doesn't leak them.
        if row.encrypted && conn.table_schema(&atoms).is_some() {
            let qr = conn.query_params(
                &format!("SELECT id, key_slot FROM {atoms} WHERE region_id = $1"),
                &[Value::Integer(row.id)],
            )?;
            let slots: Vec<(u32, u64)> = qr
                .rows
                .iter()
                .map(|r| Ok((as_int(&r[1])? as u32, as_int(&r[0])? as u64)))
                .collect::<Result<Vec<_>>>()?;
            self.db.atom_store_tombstone_batch(&slots)?;
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
                "DELETE FROM memory_regions WHERE id = $1",
                &[Value::Integer(row.id)],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn remember(&self, region: &str, atom: AtomInput) -> Result<AtomId> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;

        let vec = embed_one(&*h.embedder, &atom.text)?;
        if vec.len() != h.dim as usize {
            return Err(MemError::DimMismatch {
                region: key,
                expected: h.dim,
                got: vec.len(),
            });
        }
        let payload = serde_json::to_string(&atom.payload)
            .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;
        let expires = atom.expires_at.map(Value::Timestamp).unwrap_or(Value::Null);
        let immutable = i64::from(atom.immutable);

        let table = h.table;
        let conn = Connection::open(&self.db)?;
        let id = with_write_txn(&conn, |c| {
            let id = next_id(c, "next_atom_id")?;
            if let Some(atom_wrap) = &h.atom_wrap {
                let (sealed, wrapped) = seal_atom(atom_wrap, id, &vec, &atom.text, &payload);
                // Persist the wrapped ACK (fsync'd) before the row commits, so a committed
                // atom row always references a durable key slot.
                let (slot, gen) = self.db.atom_store_allocate_write(id as u64, &wrapped)?;
                insert_sealed_atom(
                    c,
                    &table,
                    id,
                    h.id,
                    &atom.kind,
                    sealed,
                    slot,
                    gen,
                    atom.score,
                    atom.confidence,
                    immutable,
                    expires,
                )?;
            } else {
                c.execute_params(
                    &format!(
                        "INSERT INTO {table} \
                         (id, region_id, kind, embedding, payload, text_content, score, confidence, \
                          access_count, immutable, created_at, accessed_at, expires_at) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9, \
                          CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $10)"
                    ),
                    &[
                        Value::Integer(id),
                        Value::Integer(h.id),
                        Value::Text(atom.kind.into()),
                        Value::Vector(vec.into()),
                        Value::Text(payload.into()),
                        Value::Text(atom.text.into()),
                        Value::Real(atom.score as f64),
                        Value::Real(atom.confidence as f64),
                        Value::Integer(immutable),
                        expires,
                    ],
                )?;
            }
            Ok(id)
        })?;
        h.max_id.fetch_max(id, Ordering::Relaxed);
        Ok(id)
    }

    /// Embed + store atoms in one transaction; faster than looping `remember`.
    pub fn remember_batch(&self, region: &str, atoms: Vec<AtomInput>) -> Result<Vec<AtomId>> {
        if atoms.is_empty() {
            return Ok(Vec::new());
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;

        let texts: Vec<&str> = atoms.iter().map(|a| a.text.as_str()).collect();
        let vecs = h.embedder.embed(&texts)?;
        if vecs.len() != atoms.len() {
            return Err(MemError::Invalid(format!(
                "embedder returned {} vectors for {} texts",
                vecs.len(),
                atoms.len()
            )));
        }
        for v in &vecs {
            if v.len() != h.dim as usize {
                return Err(MemError::DimMismatch {
                    region: key,
                    expected: h.dim,
                    got: v.len(),
                });
            }
        }

        let n = atoms.len();
        let table = h.table;
        let conn = Connection::open(&self.db)?;
        let ids = with_write_txn(&conn, |c| {
            let start = next_id_range(c, "next_atom_id", n as i64)?;
            let ids: Vec<AtomId> = (0..n as i64).map(|o| start + o).collect();

            if let Some(atom_wrap) = &h.atom_wrap {
                // Seal all atoms, persist their wrapped ACKs with ONE fsync before the rows commit.
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
                for (((atom, &id), sealed), &(slot, gen)) in
                    atoms.iter().zip(&ids).zip(sealed_blobs).zip(&slots)
                {
                    let expires = atom.expires_at.map(Value::Timestamp).unwrap_or(Value::Null);
                    insert_sealed_atom(
                        c,
                        &table,
                        id,
                        h.id,
                        &atom.kind,
                        sealed,
                        slot,
                        gen,
                        atom.score,
                        atom.confidence,
                        i64::from(atom.immutable),
                        expires,
                    )?;
                }
            } else {
                let plaintext_sql = format!(
                    "INSERT INTO {table} \
                     (id, region_id, kind, embedding, payload, text_content, score, confidence, \
                      access_count, immutable, created_at, accessed_at, expires_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9, \
                      CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $10)"
                );
                for ((atom, vec), &id) in atoms.into_iter().zip(vecs).zip(&ids) {
                    let payload = serde_json::to_string(&atom.payload)
                        .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;
                    let expires = atom.expires_at.map(Value::Timestamp).unwrap_or(Value::Null);
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
                            expires,
                        ],
                    )?;
                }
            }
            Ok(ids)
        })?;
        if let Some(&last) = ids.last() {
            h.max_id.fetch_max(last, Ordering::Relaxed);
        }
        Ok(ids)
    }

    /// Fetch atoms of `kind` (optional JSONB `@>` filter) via the `(region_id, kind)` index.
    ///
    /// For an encrypted region this decrypts rows in id order and considers only the
    /// first `EXACT_SCAN_LIMIT` (4096) atoms of the kind (no index runs over ciphertext).
    pub fn fetch(
        &self,
        region: &str,
        kind: &str,
        payload_filter: Option<&serde_json::Value>,
        limit: usize,
    ) -> Result<Vec<AtomHit>> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        if h.atom_wrap.is_some() {
            return self.fetch_sealed(&h, kind, payload_filter, limit);
        }

        let mut params: Vec<Value> = vec![Value::Integer(h.id), Value::Text(kind.into())];
        let mut extra = String::new();
        if let Some(filter) = payload_filter {
            let js = serde_json::to_string(filter)
                .map_err(|e| MemError::Invalid(format!("payload_filter not serializable: {e}")))?;
            params.push(Value::Text(js.into()));
            extra = format!(" AND payload @> CAST(${} AS JSONB)", params.len());
        }

        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, CAST(payload AS TEXT), text_content, score, immutable \
                 FROM {table} WHERE region_id = $1 AND kind = $2{extra} \
                 ORDER BY id LIMIT {limit}",
                table = h.table
            ),
            &params,
        )?;
        qr.rows.iter().map(|row| parse_fetched(row)).collect()
    }

    /// Count atoms of `kind` without materializing them - manifest verification
    /// over a large reference corpus must not pay a full fetch. `kind` is a SQL
    /// column in both region flavors, so no decryption is involved; in a sealed
    /// region a crypto-erased atom still has a row but its key is gone, so the
    /// count includes only atoms whose key is live (uncapped - this is a count,
    /// not the `EXACT_SCAN_LIMIT`-bounded decrypting fetch).
    pub fn count(&self, region: &str, kind: &str) -> Result<u64> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let conn = Connection::open(&self.db)?;
        if h.atom_wrap.is_some() {
            let wrapped = self.db.atom_store_live_wrapped()?;
            let qr = conn.query_params(
                &format!(
                    "SELECT id FROM {table} WHERE region_id = $1 AND kind = $2",
                    table = h.table
                ),
                &[Value::Integer(h.id), Value::Text(kind.into())],
            )?;
            let mut live = 0u64;
            for row in &qr.rows {
                if wrapped.contains_key(&(as_int(&row[0])? as u64)) {
                    live += 1;
                }
            }
            return Ok(live);
        }
        let qr = conn.query_params(
            &format!(
                "SELECT COUNT(*) FROM {table} WHERE region_id = $1 AND kind = $2",
                table = h.table
            ),
            &[Value::Integer(h.id), Value::Text(kind.into())],
        )?;
        match qr.rows.first().and_then(|r| r.first()) {
            Some(Value::Integer(n)) => Ok(*n as u64),
            other => Err(MemError::Invalid(format!(
                "COUNT returned no integer: {other:?}"
            ))),
        }
    }

    /// Freeze the region's ANN index into a persisted segment: subsequent cold
    /// attaches LOAD it in seconds instead of paying the PRISM rebuild, with
    /// the load-time scan re-proving freshness by content. Plaintext regions
    /// persist through the SQL layer (`Connection::persist_ann_index`). SEALED
    /// regions persist the segment as ONE ciphertext under a random key held
    /// in the erasable atom key store: destroying that slot crypto-erases all
    /// on-disk derivatives of the region's embeddings, preserving per-atom
    /// erasure semantics end to end.
    pub fn persist_ann_index(&self, region: &str) -> Result<AnnSegmentInfo> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            return self.persist_sealed_segment(&h);
        }
        let conn = Connection::open(&self.db)?;
        Ok(conn.persist_ann_index(&h.table, "embedding")?)
    }

    /// The identity of the ANN index currently serving this region's recalls:
    /// `Loaded {{ segment_b3 }}` (the persisted segment) or `Built` (a scan
    /// rebuild, with the segment-refusal reason if one was rejected). `None`
    /// when nothing is cached/built yet.
    pub fn ann_cache_status(&self, region: &str) -> Result<Option<AnnIndexSource>> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            return Ok(h.ann.read().unwrap().as_ref().map(|sa| sa.source.clone()));
        }
        let conn = Connection::open(&self.db)?;
        Ok(conn
            .ann_cache_status(&h.table, "embedding")?
            .map(|(source, _)| source))
    }

    /// Persist a SEALED region's ANN graph: scan + decrypt (computing the
    /// liveness-aware fingerprint), pay the PRISM build once, encode the
    /// segment, and seal it under a fresh random segment key whose ONLY copy
    /// lives in the erasable atom key store under a pseudo-atom id. Chunks go
    /// to the hidden `__annseg_{table}` tree, which every SQL mutation of the
    /// region already drops transactionally; the key slot is retired by the
    /// engine's own invalidation sites and healed at load.
    fn persist_sealed_segment(&self, h: &RegionHandle) -> Result<AnnSegmentInfo> {
        use zeroize::Zeroize;
        let atom_wrap = h.atom_wrap.as_ref().expect("sealed persist");
        let conn = Connection::open(&self.db)?;
        let wrapped = self.db.atom_store_live_wrapped()?;

        let mut kind_codes: FxHashMap<String, u32> = FxHashMap::default();
        let mut triples: Vec<(u64, Vec<f32>, Vec<u32>)> = Vec::new();
        let fingerprint = sealed_fp_scan(&conn, h, &wrapped, &mut |id, kind, sealed, _, _, _| {
            let w = wrapped.get(&(id as u64)).expect("live row has a key");
            let (emb, mut text, _payload) = open_atom(atom_wrap, w, id, sealed)?;
            text.zeroize();
            let next = kind_codes.len() as u32;
            let code = *kind_codes.entry(kind.to_string()).or_insert(next);
            triples.push((id as u64, emb, vec![code]));
            Ok(true)
        })?
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
        let body = citadel_vector::segment::encode(&index);
        let mut inner = Vec::with_capacity(body.len() + 256);
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
        // owns the erasable key-store slot.
        // The pseudo-atom id comes from the SAME sequence as real atoms, so the
        // segment's key-store slot can never collide with an atom's.
        let pseudo_id = with_write_txn(&conn, |c| next_id(c, "next_atom_id"))?;
        use rand::RngCore;
        let mut sk = [0u8; citadel_core::KEY_SIZE];
        rand::thread_rng().fill_bytes(&mut sk);
        let seal_keys = derive_seal_keys(&sk);
        let sealed = blob_seal::seal(&seal_keys, pseudo_id as u64, &inner);
        let wrapped_sk = atom_wrap.wrap_atom_key(&sk);
        sk.zeroize();
        inner.zeroize();

        // Retire any previous segment FIRST (old key must not survive as
        // decryptable residue), then key-before-data like atoms.
        self.retire_sealed_segment(h, &conn)?;
        let (slot, gen) = self
            .db
            .atom_store_allocate_write(pseudo_id as u64, &wrapped_sk)?;
        let seg_table = sealed_segment_table(&h.table, h.id);
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
        write_annseg_meta(&conn, h.id, slot, gen, pseudo_id)?;

        Ok(AnnSegmentInfo {
            segment_b3: *blake3::hash(&sealed).as_bytes(),
            content_fingerprint: fingerprint,
            n,
            dim: h.dim,
            metric_tag: citadel_vector::segment::metric_tag(ann_metric(h.metric)),
            chunk_count: sealed.len().div_ceil(SEALED_SEG_CHUNK) as u32,
        })
    }

    /// Try to serve the sealed region's persisted segment: unwrap the segment
    /// key from its erasable slot, decrypt, decode, and rehydrate vectors (and
    /// the recall cache) by decrypting the live rows - whose liveness-aware
    /// fingerprint must match the one sealed inside the segment. ANY failure
    /// heals (retires the orphan key) and falls back to the scan build, with
    /// the refusal reason carried in `Err(Some(reason))` so the rebuilt index
    /// stays queryable about WHY the segment was not used.
    #[allow(clippy::type_complexity)]
    fn try_load_sealed_segment(
        &self,
        h: &RegionHandle,
        conn: &Connection<'_>,
    ) -> Result<std::result::Result<SealedAnn, Option<String>>> {
        use zeroize::Zeroize;
        let atom_wrap = h.atom_wrap.as_ref().expect("sealed load");
        let Some((slot, gen, pseudo_id)) = read_annseg_meta(conn, h.id)? else {
            return Ok(Err(None));
        };
        let heal =
            |this: &Self, why: &str| -> Result<std::result::Result<SealedAnn, Option<String>>> {
                this.retire_sealed_segment(h, conn)?;
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
        let mut inner = match blob_seal::open(&seal_keys, pseudo_id as u64, &sealed) {
            Ok(inner) => inner,
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
            inner.zeroize();
            return heal(self, "inner parse/decode failed");
        };
        let active_cfg = citadel_vector::segment::prism_config_hash(&AnnIndex::active_config(
            ann_metric(h.metric),
        ));
        if stored_cfg != active_cfg {
            inner.zeroize();
            return heal(self, "prism config changed since the segment was built");
        }

        // Rehydrate by decrypting live rows, placed by the id_map PERMUTATION;
        // the recall cache comes from the same decrypt pass for free.
        let wrapped = self.db.atom_store_live_wrapped()?;
        let slot_of = parts.internal_of_row();
        let dim = h.dim as usize;
        let mut vectors = vec![0.0f32; parts.n() * dim];
        let mut filled = 0usize;
        let mut cached: FxHashMap<AtomId, CachedAtom> = FxHashMap::default();
        let mut unknown = false;
        let (live_fp, _) = sealed_fp_scan(
            conn,
            h,
            &wrapped,
            &mut |id, kind, sealed_row, score, created, immutable| {
                let Some(&slot) = slot_of.get(&(id as u64)) else {
                    unknown = true;
                    return Ok(false);
                };
                let w = wrapped.get(&(id as u64)).expect("live row has a key");
                let (emb, text, payload) = open_atom(atom_wrap, w, id, sealed_row)?;
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
                    },
                );
                Ok(true)
            },
        )?;
        if unknown || live_fp != stored_fp || filled != parts.n() {
            inner.zeroize();
            // Stale (liveness or content moved): expected after forgets that
            // bypassed explicit retirement - rebuild honestly.
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
        let index = match parts.into_index(vectors, filled) {
            Ok(i) => i,
            Err(e) => {
                inner.zeroize();
                return heal(self, &format!("into_index: {e}"));
            }
        };
        inner.zeroize();
        Ok(Ok(SealedAnn {
            index,
            kind_codes,
            cached,
            source: AnnIndexSource::Loaded { segment_b3 },
        }))
    }

    /// Destroy the sealed segment's key slot (crypto-erasing all on-disk
    /// segment residue), delete ITS OWN chunk keys (other regions may share
    /// the tree), and clear the meta rows. Safe when nothing is persisted.
    fn retire_sealed_segment(&self, h: &RegionHandle, conn: &Connection<'_>) -> Result<()> {
        let Some((slot, _gen, pseudo_id)) = read_annseg_meta(conn, h.id)? else {
            return Ok(());
        };
        self.db.atom_store_tombstone(slot, pseudo_id as u64)?;
        let seg_table = sealed_segment_table(&h.table, h.id);
        {
            let mut wtx = self.db.begin_write()?;
            match wtx.drop_table(seg_table.as_bytes()) {
                Ok(()) | Err(citadel_core::Error::TableNotFound(_)) => {}
                Err(e) => return Err(e.into()),
            }
            wtx.commit()?;
        }
        clear_annseg_meta(conn, h.id)?;
        Ok(())
    }

    pub fn fetch_one(&self, region: &str, atom_id: AtomId) -> Result<Option<AtomHit>> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            return self.fetch_one_sealed(&h, atom_id);
        }
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, CAST(payload AS TEXT), text_content, score, immutable \
                 FROM {table} WHERE id = $1 AND region_id = $2",
                table = h.table
            ),
            &[Value::Integer(atom_id), Value::Integer(h.id)],
        )?;
        qr.rows.first().map(|row| parse_fetched(row)).transpose()
    }

    /// Most recent atom of `kind` in `region` (highest id), or `None`.
    pub fn fetch_last(&self, region: &str, kind: &str) -> Result<Option<AtomHit>> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            return self.fetch_last_sealed(&h, kind);
        }
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, CAST(payload AS TEXT), text_content, score, immutable \
                 FROM {table} WHERE region_id = $1 AND kind = $2 ORDER BY id DESC LIMIT 1",
                table = h.table
            ),
            &[Value::Integer(h.id), Value::Text(kind.into())],
        )?;
        qr.rows.first().map(|row| parse_fetched(row)).transpose()
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
            self.update_atom_payload_sealed(&key, &h, atom_id, payload)?;
            // The cached recall index holds the pre-update payload; rebuild on next recall.
            *h.ann.write().unwrap() = None;
            let conn = Connection::open(&self.db)?;
            self.retire_sealed_segment(&h, &conn)?;
            return Ok(());
        }
        let js = serde_json::to_string(payload)
            .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;

        let conn = Connection::open(&self.db)?;
        with_write_txn(&conn, |c| {
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
    }

    /// Hybrid recall: ANN retrieval then fusion re-ranking; returns the top `q.k` atoms.
    ///
    /// Encrypted regions cannot use the ANN/FTS indexes (they operate on ciphertext),
    /// so recall decrypts the first `EXACT_SCAN_LIMIT` (4096) atoms by id and ranks
    /// them in Rust; the keyword signal is an in-Rust term overlap (not SQL `ts_rank`).
    /// Regions with more than 4096 atoms are not fully covered on the sealed path.
    pub fn recall(&self, region: &str, q: RecallQuery) -> Result<Vec<AtomHit>> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if q.k == 0 {
            return Ok(Vec::new());
        }

        let qvec: Vec<f32> = match &q.embedding {
            Some(v) => v.clone(),
            None => {
                let text = q.text.as_deref().ok_or_else(|| {
                    MemError::Invalid("recall requires either text or embedding".into())
                })?;
                embed_one(&*h.embedder, text)?
            }
        };
        if qvec.len() != h.dim as usize {
            return Err(MemError::DimMismatch {
                region: key,
                expected: h.dim,
                got: qvec.len(),
            });
        }

        if h.atom_wrap.is_some() {
            return self.recall_sealed(&h, &q, qvec);
        }

        let distop = match h.metric {
            EmbeddingMetric::Cosine => "<=>",
            EmbeddingMetric::L2 => "<->",
            EmbeddingMetric::InnerProduct => "<#>",
        };
        let table = h.table;

        // $1 = query vector (reused in SELECT + ORDER BY), $2 = region_id.
        let mut params: Vec<Value> = vec![Value::Vector(qvec.into()), Value::Integer(h.id)];

        // Keyword rank is computed in Rust via the language-agnostic BM25 primitive
        // (assign_bm25_ranks) shared with the sealed path - no SQL FTS, no language config.
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

        // Over-fetch trades query latency for better ranking of keyword/recency hits.
        const EXACT_SCAN_LIMIT: usize = 4096;
        let overfetch = q.k.saturating_mul(4).max(EXACT_SCAN_LIMIT);
        let sql = format!(
            "SELECT id, kind, CAST(payload AS TEXT), text_content, score, created_at, \
             embedding {distop} $1, 0.0, immutable \
             FROM {table} WHERE {} ORDER BY embedding {distop} $1 LIMIT {overfetch}",
            where_parts.join(" AND ")
        );

        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(&sql, &params)?;
        let mut cands = qr
            .rows
            .iter()
            .map(|r| parse_candidate(r))
            .collect::<Result<Vec<_>>>()?;
        let query_terms = query_keyword_terms(q.text.as_deref());
        assign_bm25_ranks(&mut cands, &query_terms);
        let mut hits = match (&self.reranker, &q.text) {
            (Some(r), Some(text)) => fuse_rerank(
                r.as_ref(),
                text,
                cands,
                q.weights,
                now_micros(),
                self.rerank_strategy,
                q.k,
            )?,
            _ => fuse_rank(cands, q.weights, now_micros(), q.k),
        };

        if let Some(ge) = &q.graph_expand {
            let seeds: Vec<AtomId> = hits.iter().map(|h| h.id).collect();
            let present: FxHashSet<AtomId> = seeds.iter().copied().collect();
            let mut expanded = expand_graph(&conn, &table, h.id, &seeds, ge)?;
            expanded.retain(|e| !present.contains(&e.id));
            hits.extend(expanded);
        }
        Ok(hits)
    }

    /// Create or update a directed edge; rejects cycles for acyclic kinds.
    pub fn link(&self, src: AtomId, dst: AtomId, kind: EdgeKind, weight: f32) -> Result<()> {
        let conn = Connection::open(&self.db)?;
        with_write_txn(&conn, |c| link_edge(c, src, dst, kind, weight))
    }

    /// Recompute neighbor edges and score. Encrypted regions ride the sealed scan
    /// (`EXACT_SCAN_LIMIT`), not the ANN index, so far neighbors may be missed.
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

        let conn = Connection::open(&self.db)?;
        let (embedding, access_count, created) = if let Some(atom_wrap) = &h.atom_wrap {
            let qr = conn.query_params(
                &format!(
                    "SELECT sealed, access_count, created_at, key_slot FROM {table} \
                     WHERE id = $1 AND region_id = $2"
                ),
                &[Value::Integer(atom_id), Value::Integer(h.id)],
            )?;
            let row = qr.rows.first().ok_or_else(|| {
                MemError::Invalid(format!("atom {atom_id} not in region '{key}'"))
            })?;
            let wrapped = self.db.atom_store_slot(as_int(&row[3])? as u32)?.wrapped;
            let (emb, _text, _payload) =
                open_atom(atom_wrap, &wrapped, atom_id, as_blob(&row[0])?)?;
            (emb, as_int(&row[1])?.max(0), as_ts(&row[2]))
        } else {
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
            (embedding, as_int(&row[1])?.max(0), as_ts(&row[2]))
        };

        let mut found = self.recall(
            &key,
            RecallQuery::by_embedding(embedding, neighbors.saturating_add(1)),
        )?;
        found.retain(|n| n.id != atom_id && n.distance <= max_distance);

        let age_days = (now_micros() - created).max(0) as f32 / 1e6 / 86_400.0;
        let recency = (-std::f32::consts::LN_2 * age_days / 30.0).exp();
        let new_score = recency * (1.0 + (access_count as f32).ln_1p());

        with_write_txn(&conn, |c| {
            for n in &found {
                let weight = 1.0 / (1.0 + n.distance.max(0.0));
                link_edge(c, atom_id, n.id, EdgeKind::DerivedFrom, weight)?;
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
        })?;
        // The cached recall index holds the pre-evolve score; rebuild it on next recall.
        *h.ann.write().unwrap() = None;
        if h.atom_wrap.is_some() {
            self.retire_sealed_segment(&h, &conn)?;
        }

        Ok(EvolutionReport {
            links_added: found.len(),
            score: new_score,
        })
    }

    /// Remove atoms matching `policy` and their edges; spares `immutable` except `PurgeRegion`.
    ///
    /// On encrypted regions, `PredicateMatch` is exhaustive (it pages through every
    /// atom, decrypting to test the payload); the other policies act on plaintext
    /// metadata columns of the sealed table. Evicted atoms are cryptographically erased:
    /// each atom's key is destroyed before its row is deleted.
    pub fn evict(&self, region: &str, policy: EvictionPolicy) -> Result<EvictionReport> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let table = h.table.clone();

        let conn = Connection::open(&self.db)?;
        let ids = match (&h.atom_wrap, &policy) {
            // Payload containment cannot be pushed to SQL over sealed rows; filter in
            // Rust after decrypt. Other policies use plaintext metadata columns.
            (Some(_), EvictionPolicy::PredicateMatch { predicate }) => {
                self.evict_predicate_sealed_ids(&h, predicate)?
            }
            _ => evict_target_ids(&conn, &table, h.id, &policy, now_micros())?,
        };
        if ids.is_empty() {
            return Ok(EvictionReport { removed: 0 });
        }

        let in_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        // Erase every evicted atom's key (commit point) before deleting its row.
        if h.atom_wrap.is_some() {
            let qr = conn.query_params(
                &format!("SELECT id, key_slot FROM {table} WHERE id IN ({in_list})"),
                &[],
            )?;
            let slots: Vec<(u32, u64)> = qr
                .rows
                .iter()
                .map(|row| Ok((as_int(&row[1])? as u32, as_int(&row[0])? as u64)))
                .collect::<Result<Vec<_>>>()?;
            self.db.atom_store_tombstone_batch(&slots)?;
        }

        with_write_txn(&conn, |c| {
            c.execute(&format!(
                "DELETE FROM memory_edges WHERE src_id IN ({in_list}) OR dst_id IN ({in_list})"
            ))?;
            c.execute(&format!("DELETE FROM {table} WHERE id IN ({in_list})"))?;
            Ok(())
        })?;
        *h.ann.write().unwrap() = None;
        if h.atom_wrap.is_some() {
            self.retire_sealed_segment(&h, &conn)?;
        }
        Ok(EvictionReport {
            removed: ids.len() as u64,
        })
    }

    /// Erase the keys of `ids` (encrypted regions only) then delete their rows and edges,
    /// returning `(rows_deleted, slots_erased)`. Shared by `delete_atoms` and `forget_atoms`.
    fn erase_and_delete(
        &self,
        h: &RegionHandle,
        ids: &[AtomId],
    ) -> Result<(u64, Vec<SlotErasure>)> {
        let table = &h.table;
        let in_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let conn = Connection::open(&self.db)?;

        // Encrypted path: destroy each atom's key (commit point) BEFORE the row delete, so a
        // crash in between still leaves the content permanently undecryptable. Plaintext path:
        // there is no key to destroy - the row delete below is the whole operation.
        let slots_erased = if h.atom_wrap.is_some() {
            let qr = conn.query_params(
                &format!(
                    "SELECT id, key_slot FROM {table} WHERE region_id = $1 AND id IN ({in_list})"
                ),
                &[Value::Integer(h.id)],
            )?;
            let slots: Vec<(u32, u64)> = qr
                .rows
                .iter()
                .map(|row| Ok((as_int(&row[1])? as u32, as_int(&row[0])? as u64)))
                .collect::<Result<Vec<_>>>()?;
            self.db
                .atom_store_tombstone_batch(&slots)?
                .into_iter()
                .map(|(slot, atom_id, old_gen, new_gen)| SlotErasure {
                    slot,
                    atom_id: atom_id as AtomId,
                    old_gen,
                    new_gen,
                })
                .collect()
        } else {
            Vec::new()
        };

        // Both paths: delete incident edges, then the rows. The row DELETE's own affected-row
        // count is the honest `rows_deleted` for either path.
        let rows_deleted = with_write_txn(&conn, |c| {
            c.execute_params(
                &format!(
                    "DELETE FROM memory_edges WHERE \
                     src_id IN (SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list})) \
                     OR dst_id IN (SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list}))"
                ),
                &[Value::Integer(h.id)],
            )?;
            let deleted = c.execute_params(
                &format!("DELETE FROM {table} WHERE region_id = $1 AND id IN ({in_list})"),
                &[Value::Integer(h.id)],
            )?;
            Ok(match deleted {
                ExecutionResult::RowsAffected(n) => n,
                _ => 0,
            })
        })?;

        // Drop the cached ANN index so erased atoms are not re-ranked on the next
        // recall, and crypto-erase the persisted segment's key: its SQ8 codes are
        // embedding-derived residue that must not outlive the atoms' own keys.
        *h.ann.write().unwrap() = None;
        if h.atom_wrap.is_some() {
            self.retire_sealed_segment(h, &conn)?;
        }
        Ok((rows_deleted, slots_erased))
    }

    /// Delete atoms. For an encrypted region this is per-atom cryptographic erasure: each
    /// atom's key is destroyed in the atom key store (overwrite-in-place + fsync +
    /// read-back) BEFORE its row is deleted, so a crash in between still leaves that
    /// atom's content permanently undecryptable. Sibling atoms and the region are intact.
    /// Privileged: ignores the `immutable` flag. [`forget_atoms`](Self::forget_atoms) is
    /// the model-safe variant with a verifiable receipt.
    pub fn delete_atoms(&self, region: &str, ids: &[AtomId]) -> Result<EvictionReport> {
        if ids.is_empty() {
            return Ok(EvictionReport { removed: 0 });
        }
        let h = self.region_handle(&region.to_ascii_lowercase())?;
        self.erase_and_delete(&h, ids)?;
        Ok(EvictionReport {
            removed: ids.len() as u64,
        })
    }

    /// Forget atoms and return a verifiable [`ErasureReceipt`]. On an encrypted region each
    /// atom's key is cryptographically destroyed; on a plaintext region this is a logical
    /// delete (the receipt's `cryptographic_erasure` is false). Immutable atoms are skipped
    /// (reported in `immutable_skipped`) unless `force` is set.
    pub fn forget_atoms(
        &self,
        region: &str,
        ids: &[AtomId],
        force: bool,
    ) -> Result<ErasureReceipt> {
        let h = self.region_handle(&region.to_ascii_lowercase())?;
        let encrypted = h.atom_wrap.is_some();

        let mut immutable_skipped = Vec::new();
        let mut targets: Vec<AtomId> = ids.to_vec();
        if !force && !ids.is_empty() {
            let in_list = ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let conn = Connection::open(&self.db)?;
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

        let (rows_deleted, slots_erased) = if targets.is_empty() {
            (0, Vec::new())
        } else {
            self.erase_and_delete(&h, &targets)?
        };

        Ok(ErasureReceipt {
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
            fsync: encrypted,
            readback_confirmed: encrypted,
            scope_caveat: ERASURE_SCOPE_CAVEAT,
        })
    }

    /// Cryptographically erase a single atom: destroy its key (overwrite-in-place +
    /// fsync + read-back) and delete its row. Sibling atoms and the region are untouched.
    pub fn forget_atom(&self, region: &str, id: AtomId) -> Result<()> {
        self.delete_atoms(region, &[id]).map(|_| ())
    }

    /// Re-authenticate atoms by id, returning one [`AtomAttestation`] per requested id (in
    /// order). Reads each atom's sealed bytes FRESH from disk - never the in-RAM recall cache -
    /// and recomputes the HMAC bound to the atom id, so a verdict reflects on-disk truth and
    /// catches tampering (a flipped ciphertext byte) or a blob replayed from another row. Never
    /// aborts on a bad atom; every id gets a verdict.
    pub fn verify_atoms(&self, region: &str, ids: &[AtomId]) -> Result<Vec<AtomAttestation>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let h = self.region_handle(&region.to_ascii_lowercase())?;
        let table = &h.table;
        let in_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let conn = Connection::open(&self.db)?;

        // Plaintext region: atoms carry no per-atom MAC. Present ids are PlaintextUnattested
        // (there is nothing to recompute); absent ids are Missing.
        let Some(atom_wrap) = h.atom_wrap.as_ref() else {
            let qr = conn.query_params(
                &format!("SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list})"),
                &[Value::Integer(h.id)],
            )?;
            let present: FxHashSet<AtomId> = qr
                .rows
                .iter()
                .map(|r| as_int(&r[0]))
                .collect::<Result<_>>()?;
            return Ok(ids
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
                .collect());
        };

        // Encrypted region: read sealed + key_slot fresh (off the recall cache), then
        // re-authenticate each off disk.
        let qr = conn.query_params(
            &format!(
                "SELECT id, key_slot, sealed FROM {table} WHERE region_id = $1 AND id IN ({in_list})"
            ),
            &[Value::Integer(h.id)],
        )?;
        let mut found: FxHashMap<AtomId, (u32, Vec<u8>)> = FxHashMap::default();
        for row in &qr.rows {
            let id = as_int(&row[0])?;
            let slot = as_int(&row[1])? as u32;
            let sealed = match &row[2] {
                Value::Blob(b) => b.clone(),
                _ => return Err(MemError::Invalid("sealed column is not a blob".into())),
            };
            found.insert(id, (slot, sealed));
        }

        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            let Some((slot, sealed)) = found.get(&id) else {
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
            if rec.state != SlotState::Live {
                // The key was destroyed (forgotten): content is permanently unrecoverable.
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
                    // The HMAC is recomputed with aad = atom id, so a flipped byte (CTR is
                    // malleable) or a blob replayed from another row both fail here.
                    match blob_seal::open(&seal_keys, id as u64, sealed) {
                        Ok(mut pt) => {
                            pt.zeroize();
                            (AttestVerdict::Authentic, true)
                        }
                        Err(_) => (AttestVerdict::Tampered, true),
                    }
                }
                // A live slot whose wrapped ACK will not unwrap means key-slot corruption.
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

    /// Per-kind counts, time span, and avg score/confidence since `since_micros` (no LLM).
    pub fn summarize(&self, region: &str, since_micros: i64) -> Result<SummaryReport> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            &format!(
                "SELECT kind, COUNT(*), MIN(created_at), MAX(created_at), AVG(score), AVG(confidence) \
                 FROM {table} WHERE region_id = $1 AND created_at > $2 GROUP BY kind",
                table = h.table
            ),
            &[Value::Integer(h.id), Value::Timestamp(since_micros)],
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
            ann: Arc::clone(&st.ann),
            max_id: Arc::clone(&st.max_id),
        })
    }

    /// Return the id if `key` is attached and the embedder matches; error on mismatch.
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
        let id = as_int(&row[0])?;
        let dim = u16::try_from(as_int(&row[1])?)
            .map_err(|_| MemError::Invalid("stored embedding_dim out of range".into()))?;
        let metric = metric_from_str(as_text(&row[2])?)?;
        let model_id = as_text(&row[3])?.to_string();
        let encrypted = as_bool(&row[4]);
        let rsk_slot = opt_u32(&row[5])?;
        let rsk_gen = opt_u64(&row[6]);
        Ok(Some(RegionRow {
            id,
            dim,
            metric,
            model_id,
            encrypted,
            rsk_slot,
            rsk_gen,
        }))
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

/// Encrypted-region paths: sealed writes, decrypt-then-rank reads, and key lifecycle.
impl MemoryEngine {
    /// Attach an existing encrypted region: read its LIVE slot, unwrap the RCK, and
    /// derive the atom-wrap key. Yields [`MemError::RegionForgotten`] if the slot was
    /// tombstoned or its generation no longer matches the region row.
    fn attach_region_key(&self, name: &str, row: &RegionRow) -> Result<Arc<AtomWrapKey>> {
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
        let mut rck = self.db.unwrap_region_key(&rec.wrapped)?;
        let atom_wrap = derive_atom_wrap_key(&rck);
        rck.zeroize();
        Ok(Arc::new(atom_wrap))
    }

    /// Create a new encrypted region: generate a random RCK, wrap it, persist the
    /// LIVE slot (fsync'd) before inserting the region row, and return the atom-wrap key.
    fn insert_encrypted_region(
        &self,
        conn: &Connection<'_>,
        key: &str,
        dim: u16,
        metric: EmbeddingMetric,
        model_id: &str,
    ) -> Result<(RegionId, Option<Arc<AtomWrapKey>>)> {
        use rand::RngCore;

        // Reserve the region id first so the key slot binds to it.
        let id = with_write_txn(conn, |c| next_id(c, "next_region_id"))?;

        let mut rck = [0u8; citadel_core::KEY_SIZE];
        rand::thread_rng().fill_bytes(&mut rck);
        let wrapped = self.db.wrap_region_key(&rck)?;

        // Persist the wrapped key (fsync'd) BEFORE inserting the row, so a committed
        // region row always references a durable key.
        let (slot, gen) = self.db.region_store_allocate_write(id as u64, &wrapped)?;

        with_write_txn(conn, |c| {
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
        })?;

        let atom_wrap = derive_atom_wrap_key(&rck);
        rck.zeroize();
        Ok((id, Some(Arc::new(atom_wrap))))
    }

    /// ANN recall over an encrypted region using an ephemeral in-RAM PRISM index built
    /// from the region's DECRYPTED vectors (no ANN/FTS index can run over ciphertext).
    /// The index is cached per region and its plaintext vectors are zeroized on drop,
    /// so they never outlive the region key.
    fn recall_sealed(
        &self,
        h: &RegionHandle,
        q: &RecallQuery,
        qvec: Vec<f32>,
    ) -> Result<Vec<AtomHit>> {
        let atom_wrap = h
            .atom_wrap
            .as_ref()
            .expect("recall_sealed on plaintext region");
        let table = &h.table;
        let conn = Connection::open(&self.db)?;
        let cand_k = q.k.saturating_mul(CAND_OVERFETCH).max(MIN_CANDIDATES);
        let ranked = self.sealed_ann_candidates(h, &conn, &qvec, q, cand_k)?;
        if ranked.is_empty() {
            return Ok(Vec::new());
        }

        // Build candidates from the per-region cache captured at the single index build, so
        // the hot path touches neither SQL nor decryption nor the key store. Every cached
        // atom is live and current: any delete or payload/score change resets the index
        // (`h.ann = None`), forcing a rebuild that drops the stale entry. Only post-snapshot
        // tail atoms (cache miss) fall through to a small SQL fetch + decrypt below.
        let query_terms = query_keyword_terms(q.text.as_deref());
        let mut cands: Vec<Candidate> = Vec::with_capacity(ranked.len());
        let mut misses: Vec<(AtomId, f32)> = Vec::new();
        {
            let guard = h.ann.read().unwrap();
            let cache = guard.as_ref().map(|sa| &sa.cached);
            for &(id, dist) in &ranked {
                match cache.and_then(|c| c.get(&id)) {
                    Some(ca) => {
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

        // Tail / cache-miss atoms: fetch and decrypt only these (empty in steady state, so
        // the key store is read only when the index is behind, never on the hot path).
        if !misses.is_empty() {
            let wrapped = self.db.atom_store_live_wrapped()?;
            let id_params: Vec<Value> = misses.iter().map(|(id, _)| Value::Integer(*id)).collect();
            let placeholders = (1..=id_params.len())
                .map(|i| format!("${i}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT id, kind, sealed, score, created_at, immutable FROM {table} \
                 WHERE id IN ({placeholders})"
            );
            let dist_by_id: FxHashMap<AtomId, f32> = misses.iter().copied().collect();
            let qr = conn.query_params(&sql, &id_params)?;
            for row in &qr.rows {
                let id = as_int(&row[0])?;
                let Some(w) = wrapped.get(&(id as u64)) else {
                    continue;
                };
                let (_emb, text, payload) = open_atom(atom_wrap, w, id, as_blob(&row[2])?)?;
                if let Some(filter) = &q.payload_filter {
                    if !json_contains(&payload, filter) {
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

        let mut hits = match (&self.reranker, &q.text) {
            (Some(r), Some(text)) => fuse_rerank(
                r.as_ref(),
                text,
                cands,
                q.weights,
                now_micros(),
                self.rerank_strategy,
                q.k,
            )?,
            _ => fuse_rank(cands, q.weights, now_micros(), q.k),
        };

        if let Some(ge) = &q.graph_expand {
            let seeds: Vec<AtomId> = hits.iter().map(|h| h.id).collect();
            let present: FxHashSet<AtomId> = seeds.iter().copied().collect();
            let wrapped = self.db.atom_store_live_wrapped()?;
            let mut expanded =
                expand_graph_sealed(&conn, table, atom_wrap, &wrapped, h.id, &seeds, ge)?;
            expanded.retain(|e| !present.contains(&e.id));
            hits.extend(expanded);
        }
        Ok(hits)
    }

    /// Top `cand_k` `(atom_id, distance)` for a sealed region: search the cached PRISM
    /// index (rebuilt if stale) plus an exact scan of atoms inserted after the snapshot.
    fn sealed_ann_candidates(
        &self,
        h: &RegionHandle,
        conn: &Connection<'_>,
        qvec: &[f32],
        q: &RecallQuery,
        cand_k: usize,
    ) -> Result<Vec<(AtomId, f32)>> {
        let atom_wrap = h
            .atom_wrap
            .as_ref()
            .expect("sealed_ann_candidates on plaintext region");
        let max_id = h.max_id.load(Ordering::Relaxed);

        // Fast path: a fresh index searches under a shared read lock (recalls don't serialize).
        {
            let guard = h.ann.read().unwrap();
            if let Some(sa) = guard.as_ref() {
                if !sealed_index_stale(sa, max_id) {
                    return search_sealed_index(
                        sa, qvec, q, cand_k, conn, atom_wrap, &self.db, h, max_id,
                    );
                }
            }
        }

        // Slow path: load the persisted sealed segment if one verifies, else
        // rebuild from a decrypt scan - under the write lock, re-checking in
        // case another writer won.
        {
            let mut guard = h.ann.write().unwrap();
            let need_full = guard
                .as_ref()
                .map(|sa| sealed_index_stale(sa, max_id))
                .unwrap_or(true);
            if need_full {
                let load = self.try_load_sealed_segment(h, conn)?;
                if let Ok(loaded) = load {
                    *guard = Some(loaded);
                } else {
                    let refusal = load.err().flatten();
                    let wrapped = self.db.atom_store_live_wrapped()?;
                    let rows = decrypt_scan(conn, atom_wrap, &wrapped, &h.table, h.id, None)?;
                    if rows.is_empty() {
                        *guard = None;
                        return Ok(Vec::new());
                    }
                    let mut kind_codes: FxHashMap<String, u32> = FxHashMap::default();
                    let mut cached: FxHashMap<AtomId, CachedAtom> = FxHashMap::default();
                    let triples: Vec<(u64, Vec<f32>, Vec<u32>)> = rows
                        .into_iter()
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
                    });
                }
            }
        }

        // Search under a shared read lock.
        let guard = h.ann.read().unwrap();
        let Some(sa) = guard.as_ref() else {
            return Ok(Vec::new());
        };
        search_sealed_index(sa, qvec, q, cand_k, conn, atom_wrap, &self.db, h, max_id)
    }

    fn fetch_sealed(
        &self,
        h: &RegionHandle,
        kind: &str,
        payload_filter: Option<&serde_json::Value>,
        limit: usize,
    ) -> Result<Vec<AtomHit>> {
        let atom_wrap = h
            .atom_wrap
            .as_ref()
            .expect("fetch_sealed on plaintext region");
        let conn = Connection::open(&self.db)?;
        let wrapped = self.db.atom_store_live_wrapped()?;
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, sealed, score, immutable FROM {table} \
                 WHERE region_id = $1 AND kind = $2 ORDER BY id LIMIT {EXACT_SCAN_LIMIT}",
                table = h.table
            ),
            &[Value::Integer(h.id), Value::Text(kind.into())],
        )?;
        let mut out = Vec::new();
        for row in &qr.rows {
            let id = as_int(&row[0])?;
            let Some(w) = wrapped.get(&(id as u64)) else {
                continue;
            };
            let (_emb, text, payload) = open_atom(atom_wrap, w, id, as_blob(&row[2])?)?;
            if let Some(filter) = payload_filter {
                if !json_contains(&payload, filter) {
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
                immutable: as_bool(&row[4]),
            });
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn fetch_one_sealed(&self, h: &RegionHandle, atom_id: AtomId) -> Result<Option<AtomHit>> {
        let atom_wrap = h
            .atom_wrap
            .as_ref()
            .expect("fetch_one_sealed on plaintext region");
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, sealed, score, immutable, key_slot FROM {table} \
                 WHERE id = $1 AND region_id = $2",
                table = h.table
            ),
            &[Value::Integer(atom_id), Value::Integer(h.id)],
        )?;
        let Some(row) = qr.rows.first() else {
            return Ok(None);
        };
        let id = as_int(&row[0])?;
        let wrapped = self.db.atom_store_slot(as_int(&row[5])? as u32)?.wrapped;
        let (_emb, text, payload) = open_atom(atom_wrap, &wrapped, id, as_blob(&row[2])?)?;
        Ok(Some(AtomHit {
            id,
            kind: as_text(&row[1])?.to_string(),
            payload,
            text,
            distance: f32::MAX,
            score: as_f32(&row[3]),
            immutable: as_bool(&row[4]),
        }))
    }

    fn fetch_last_sealed(&self, h: &RegionHandle, kind: &str) -> Result<Option<AtomHit>> {
        let atom_wrap = h
            .atom_wrap
            .as_ref()
            .expect("fetch_last_sealed on plaintext region");
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, sealed, score, immutable, key_slot FROM {table} \
                 WHERE region_id = $1 AND kind = $2 ORDER BY id DESC LIMIT 1",
                table = h.table
            ),
            &[Value::Integer(h.id), Value::Text(kind.into())],
        )?;
        let Some(row) = qr.rows.first() else {
            return Ok(None);
        };
        let id = as_int(&row[0])?;
        let wrapped = self.db.atom_store_slot(as_int(&row[5])? as u32)?.wrapped;
        let (_emb, text, payload) = open_atom(atom_wrap, &wrapped, id, as_blob(&row[2])?)?;
        Ok(Some(AtomHit {
            id,
            kind: as_text(&row[1])?.to_string(),
            payload,
            text,
            distance: f32::MAX,
            score: as_f32(&row[3]),
            immutable: as_bool(&row[4]),
        }))
    }

    /// Re-seal an atom with a replaced payload (embedding and text preserved).
    fn update_atom_payload_sealed(
        &self,
        key: &str,
        h: &RegionHandle,
        atom_id: AtomId,
        payload: &serde_json::Value,
    ) -> Result<()> {
        let atom_wrap = h
            .atom_wrap
            .as_ref()
            .expect("update_atom_payload_sealed on plaintext");
        let new_payload = serde_json::to_string(payload)
            .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;
        let table = h.table.clone();
        let conn = Connection::open(&self.db)?;
        with_write_txn(&conn, |c| {
            let qr = c.query_params(
                &format!(
                    "SELECT sealed, key_slot FROM {table} \
                     WHERE id = $1 AND region_id = $2 AND immutable = 0"
                ),
                &[Value::Integer(atom_id), Value::Integer(h.id)],
            )?;
            let Some(row) = qr.rows.first() else {
                return Err(MemError::Invalid(format!(
                    "atom {atom_id} not found, or immutable, in region '{key}'"
                )));
            };
            // Re-seal under the SAME ACK (the atom's key is unchanged; only its payload).
            let wrapped = self.db.atom_store_slot(as_int(&row[1])? as u32)?.wrapped;
            let mut ack = atom_wrap.unwrap_atom_key(&wrapped)?;
            let seal_keys = derive_seal_keys(&ack);
            ack.zeroize();
            let old_blob = blob_seal::open(&seal_keys, atom_id as u64, as_blob(&row[0])?)?;
            let (emb, text, _old) = decode_atom_blob(&old_blob)?;
            let blob = encode_atom_blob(&emb, &text, &new_payload);
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

    /// Ids of non-immutable sealed atoms whose payload `@>`-contains `predicate`.
    /// EXHAUSTIVE: forgetting must not silently under-delete, so this pages through
    /// every atom by id (unlike the bounded read paths) until the region is drained.
    fn evict_predicate_sealed_ids(
        &self,
        h: &RegionHandle,
        predicate: &serde_json::Value,
    ) -> Result<Vec<AtomId>> {
        let atom_wrap = h
            .atom_wrap
            .as_ref()
            .expect("evict_predicate_sealed_ids on plaintext");
        let conn = Connection::open(&self.db)?;
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
                let (_emb, _text, payload) = open_atom(atom_wrap, w, id, as_blob(&row[1])?)?;
                if json_contains(&payload, predicate) {
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

/// Cached index needs a rebuild: post-snapshot tail exceeds the cap or 1/4 of indexed atoms.
fn sealed_index_stale(sa: &SealedAnn, max_id: i64) -> bool {
    let snap = sa.index.snapshot_max as i64;
    let tail = (max_id - snap).max(0) as usize;
    tail > REBUILD_TAIL_MAX || tail > sa.index.indexed_len() / 4
}

/// Top `cand_k` `(atom_id, distance)` from the cached index, plus exact-ranked atoms
/// inserted after its snapshot.
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
    let filter = if q.kinds.is_empty() {
        Filter::none()
    } else {
        let codes: Vec<u32> = q
            .kinds
            .iter()
            .filter_map(|k| sa.kind_codes.get(k).copied())
            .collect();
        if codes.is_empty() {
            return Ok(Vec::new());
        }
        Filter::new(vec![(0, codes)])
    };

    let mut ranked: Vec<(AtomId, f32)> = sa
        .index
        .search_filtered_default_ef(qvec, cand_k, &filter)
        .into_iter()
        .map(|(id, d)| (id as AtomId, d))
        .collect();

    // Exact-rank atoms inserted after the snapshot (the key store is read only here, when
    // the index is behind the latest writes).
    let snap = sa.index.snapshot_max as i64;
    if max_id > snap {
        let wrapped = db.atom_store_live_wrapped()?;
        for (id, emb, kind, ..) in
            decrypt_scan(conn, atom_wrap, &wrapped, &h.table, h.id, Some(snap))?
        {
            if !q.kinds.is_empty() && !q.kinds.iter().any(|k| k == &kind) {
                continue;
            }
            ranked.push((id, vec_distance(h.metric, qvec, &emb)));
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
    expires: Value,
) -> Result<()> {
    c.execute_params(
        &format!(
            "INSERT INTO {table} \
             (id, region_id, kind, sealed, key_slot, key_gen, score, confidence, access_count, \
              immutable, created_at, accessed_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $10)"
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
            expires,
        ],
    )?;
    Ok(())
}

/// The plaintext payload sealed per encrypted atom: `dim | embedding(f32 LE) | text |
/// payload-json`, each variable field length-prefixed (u32 LE).
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

fn decode_atom_blob(b: &[u8]) -> Result<(Vec<f32>, String, String)> {
    let truncated = || MemError::Invalid("sealed atom blob is truncated".into());
    let mut o = 0usize;
    let take = |o: &mut usize, n: usize| -> Result<std::ops::Range<usize>> {
        if *o + n > b.len() {
            return Err(MemError::Invalid("sealed atom blob is truncated".into()));
        }
        let r = *o..*o + n;
        *o += n;
        Ok(r)
    };

    let dim = u16::from_le_bytes(b[take(&mut o, 2)?].try_into().map_err(|_| truncated())?) as usize;
    let mut emb = Vec::with_capacity(dim);
    for _ in 0..dim {
        let r = take(&mut o, 4)?;
        emb.push(f32::from_le_bytes(
            b[r].try_into().map_err(|_| truncated())?,
        ));
    }
    let tlen =
        u32::from_le_bytes(b[take(&mut o, 4)?].try_into().map_err(|_| truncated())?) as usize;
    let text = String::from_utf8(b[take(&mut o, tlen)?].to_vec())
        .map_err(|_| MemError::Invalid("sealed text is not valid UTF-8".into()))?;
    let plen =
        u32::from_le_bytes(b[take(&mut o, 4)?].try_into().map_err(|_| truncated())?) as usize;
    let payload = String::from_utf8(b[take(&mut o, plen)?].to_vec())
        .map_err(|_| MemError::Invalid("sealed payload is not valid UTF-8".into()))?;
    Ok((emb, text, payload))
}

/// One decrypted sealed atom with the fields recall caches: `(id, embedding, kind, text,
/// payload, importance, created_micros, immutable)`.
type DecryptedAtom = (
    AtomId,
    Vec<f32>,
    String,
    String,
    serde_json::Value,
    f32,
    i64,
    bool,
);

/// Decrypt atoms of a sealed region (all, or only `id > after`) into [`DecryptedAtom`]s.
/// Used to (re)build the in-RAM ANN index (vectors) and cache the decrypted text/payload,
/// and to exact-rank the post-snapshot tail.
fn decrypt_scan(
    conn: &Connection<'_>,
    atom_wrap: &AtomWrapKey,
    wrapped: &FxHashMap<u64, [u8; WRAPPED_KEY_SIZE]>,
    table: &str,
    region_id: RegionId,
    after: Option<i64>,
) -> Result<Vec<DecryptedAtom>> {
    if conn.table_schema(table).is_none() {
        return Ok(Vec::new());
    }
    let cols = "id, kind, sealed, score, created_at, immutable";
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
    let mut out = Vec::with_capacity(qr.rows.len());
    for row in &qr.rows {
        let id = as_int(&row[0])?;
        let kind = as_text(&row[1])?.to_string();
        let Some(w) = wrapped.get(&(id as u64)) else {
            continue;
        };
        let (emb, text, payload) = open_atom(atom_wrap, w, id, as_blob(&row[2])?)?;
        out.push((
            id,
            emb,
            kind,
            text,
            payload,
            as_f32(&row[3]),
            as_ts(&row[4]),
            as_bool(&row[5]),
        ));
    }
    Ok(out)
}

/// Highest atom id in a sealed region (0 if the table is absent or the region empty).
fn sealed_max_id(conn: &Connection<'_>, table: &str, region_id: RegionId) -> Result<i64> {
    if conn.table_schema(table).is_none() {
        return Ok(0);
    }
    let qr = conn.query_params(
        &format!("SELECT MAX(id) FROM {table} WHERE region_id = $1"),
        &[Value::Integer(region_id)],
    )?;
    match qr.rows.first().map(|r| &r[0]) {
        Some(Value::Integer(m)) => Ok(*m),
        _ => Ok(0),
    }
}

/// Sealed-segment ciphertext chunk size (storage chains pages anyway; this
/// only bounds per-value buffers).
const SEALED_SEG_CHUNK: usize = 1024 * 1024;

/// The hidden chunk tree for ONE sealed region's segment. PER-REGION (the
/// region is the sealed lifecycle unit; regions sharing an atoms table must
/// not destroy each other's segments) and deliberately NOT the SQL layer's
/// `__annseg_{table}` name: sealed staleness is owned by the engine's explicit
/// retirement at every mutation site, with the liveness-aware fingerprint
/// refusing - at load - anything that changed through a channel the engine
/// does not own.
fn sealed_segment_table(table: &str, region_id: RegionId) -> String {
    format!("__annseg_r{region_id}__{table}")
}

/// One LIVE sealed row delivered to a scan consumer:
/// `(id, kind, sealed_bytes, score, created_micros, immutable)`. Returning
/// `false` stops delivery (the fingerprint still covers the remaining rows).
type SealedRowFn<'a> = dyn FnMut(AtomId, &str, &[u8], f32, i64, bool) -> Result<bool> + 'a;

/// The liveness-aware content fingerprint of a sealed region, computed by ONE
/// deterministic scan (ORDER BY id): every row contributes its id, its sealed
/// ciphertext (length-framed), and its key-liveness bit - so both row content
/// changes AND crypto-erasures (which flip liveness without touching rows)
/// invalidate a persisted segment.
fn sealed_fp_scan(
    conn: &Connection<'_>,
    h: &RegionHandle,
    wrapped: &FxHashMap<u64, [u8; WRAPPED_KEY_SIZE]>,
    live: &mut SealedRowFn<'_>,
) -> Result<([u8; 32], bool)> {
    let mut fp = blake3::Hasher::new();
    fp.update(b"citadel-annseg-sealed-fp-v1");
    fp.update(&h.id.to_le_bytes());
    fp.update(&h.dim.to_le_bytes());
    fp.update(&[citadel_vector::segment::metric_tag(ann_metric(h.metric))]);

    let qr = conn.query_params(
        &format!(
            "SELECT id, kind, sealed, score, created_at, immutable FROM {table} \
             WHERE region_id = $1 ORDER BY id",
            table = h.table
        ),
        &[Value::Integer(h.id)],
    )?;
    let mut completed = true;
    for row in &qr.rows {
        let id = as_int(&row[0])?;
        let sealed = as_blob(&row[2])?;
        let is_live = wrapped.contains_key(&(id as u64));
        fp.update(&id.to_le_bytes());
        fp.update(&(sealed.len() as u64).to_le_bytes());
        fp.update(sealed);
        fp.update(&[u8::from(is_live)]);
        if is_live && completed {
            let kind = as_text(&row[1])?;
            if !live(
                id,
                kind,
                sealed,
                as_f32(&row[3]),
                as_ts(&row[4]),
                as_bool(&row[5]),
            )? {
                // Keep hashing the remaining rows (the fingerprint must cover
                // the whole table) but stop delivering them.
                completed = false;
            }
        }
    }
    Ok((*fp.finalize().as_bytes(), completed))
}

/// Parse the sealed segment's inner plaintext:
/// `[fp 32][config_hash 32][kind_count u32][(len u32, kind, code u32)*][segment body]`.
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

fn read_annseg_meta(conn: &Connection<'_>, region_id: RegionId) -> Result<Option<(u32, u64, i64)>> {
    let read = |field: &str| -> Result<Option<i64>> {
        let qr = conn.query_params(
            "SELECT value FROM memory_meta WHERE key = $1",
            &[Value::Text(annseg_meta_key(region_id, field).into())],
        )?;
        Ok(match qr.rows.first().map(|r| &r[0]) {
            Some(Value::Integer(v)) => Some(*v),
            _ => None,
        })
    };
    let (Some(slot), Some(gen), Some(id)) = (read("slot")?, read("gen")?, read("id")?) else {
        return Ok(None);
    };
    Ok(Some((slot as u32, gen as u64, id)))
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

/// Open one sealed atom: unwrap its ACK (wrapped under the region atom-wrap key) and
/// decrypt the blob. The ACK is zeroized before returning.
fn open_atom(
    atom_wrap: &AtomWrapKey,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
    id: AtomId,
    sealed: &[u8],
) -> Result<(Vec<f32>, String, serde_json::Value)> {
    let mut ack = atom_wrap.unwrap_atom_key(wrapped)?;
    let seal_keys = derive_seal_keys(&ack);
    ack.zeroize();
    let blob = blob_seal::open(&seal_keys, id as u64, sealed)?;
    let (emb, text, payload_json) = decode_atom_blob(&blob)?;
    let payload = serde_json::from_str(&payload_json).unwrap_or(serde_json::Value::Null);
    Ok((emb, text, payload))
}

/// Seal one atom under a fresh random ACK; returns `(sealed_blob, wrapped_ack)`. The
/// wrapped ACK is the sole copy and must be stored (fsync'd) in an atom key slot before
/// the atom row is committed, so a committed row always references a durable key.
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
    let blob = encode_atom_blob(embedding, text, payload_json);
    let sealed = blob_seal::seal(&seal_keys, id as u64, &blob);
    let wrapped = atom_wrap.wrap_atom_key(&ack);
    ack.zeroize();
    (sealed, wrapped)
}

/// Distance between two equal-length vectors, matching citadel-sql's `<->`/`<#>`/`<=>`
/// so encrypted decrypt-then-rank recall is comparable to the plaintext index path.
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

/// Language-agnostic word tokens: Unicode UAX#29 word boundaries, lowercased. Handles
/// any whitespace-delimited script with no per-language config; spaceless scripts
/// (CJK/Thai) fall back to per-character tokens, a serviceable lexical feature.
fn tokenize(text: &str) -> Vec<String> {
    use unicode_segmentation::UnicodeSegmentation;
    text.unicode_words().map(str::to_lowercase).collect()
}

/// Distinct query tokens for the BM25 keyword signal (UAX#29, lowercased, deduped).
fn query_keyword_terms(text: Option<&str>) -> Vec<String> {
    let Some(t) = text else {
        return Vec::new();
    };
    let mut v = tokenize(t);
    v.sort();
    v.dedup();
    v
}

/// Set each candidate's `text_rank` to its Okapi BM25 score for `query_terms`, with the
/// candidate pool itself as the corpus. Language-agnostic: IDF down-weights terms common
/// across the pool (stopword-like in ANY language), so no stoplist or stemmer is needed.
/// Shared keyword primitive for the plaintext and sealed recall paths.
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

/// Atoms table for a (dim, metric) pair; the `region_id` column isolates regions.
/// Encrypted regions use a distinct `_enc` table holding only sealed content.
pub(crate) fn atoms_table(dim: u16, metric: EmbeddingMetric, encrypted: bool) -> String {
    let suffix = if encrypted { "_enc" } else { "" };
    format!("memory_atoms_d{}_{}{}", dim, metric_tag(metric), suffix)
}

fn ensure_atoms_table(
    conn: &Connection<'_>,
    dim: u16,
    metric: EmbeddingMetric,
    encrypted: bool,
) -> Result<()> {
    let t = atoms_table(dim, metric, encrypted);
    if let Some(schema) = conn.table_schema(&t) {
        // Pre-per-atom-erasure tables lack the key columns; fail loudly, not deep in a query.
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
        // Sealed-only: no plaintext column (a stale CoW page can't leak content).
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

/// Allocate the next id for `key` from `memory_meta`. Must run inside a write txn.
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
        Ok(v) => {
            conn.execute("COMMIT")?;
            Ok(v)
        }
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

/// Upsert one edge (caller owns the txn); rejects cycles for acyclic kinds.
fn link_edge(
    conn: &Connection<'_>,
    src: AtomId,
    dst: AtomId,
    kind: EdgeKind,
    weight: f32,
) -> Result<()> {
    if kind.is_acyclic() && would_cycle(conn, src, dst, kind)? {
        return Err(MemError::Cycle { src, dst });
    }
    conn.execute_params(
        "INSERT INTO memory_edges (src_id, dst_id, kind, weight, evidence_ref) \
         VALUES ($1, $2, $3, $4, NULL) \
         ON CONFLICT (src_id, dst_id, kind) DO UPDATE SET weight = excluded.weight",
        &[
            Value::Integer(src),
            Value::Integer(dst),
            Value::Text(kind.as_str().into()),
            Value::Real(weight as f64),
        ],
    )?;
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
) -> Result<Vec<AtomId>> {
    match policy {
        EvictionPolicy::Stale { older_than_micros } => select_ids(
            conn,
            &format!(
                "SELECT id FROM {table} WHERE region_id = $1 AND immutable = 0 \
                 AND access_count = 0 AND created_at < $2"
            ),
            &[
                Value::Integer(region_id),
                Value::Timestamp(now - older_than_micros),
            ],
        ),
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
            // Least-recently-accessed first; subquery form so ORDER BY/LIMIT apply.
            select_ids(
                conn,
                &format!(
                    "SELECT id FROM {table} WHERE region_id = $1 AND immutable = 0 \
                     ORDER BY accessed_at ASC, access_count ASC LIMIT {delete_n}"
                ),
                &[Value::Integer(region_id)],
            )
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

/// BFS depth of each non-seed atom reachable from `seeds` over `memory_edges` up to
/// `ge.depth` hops (optionally filtered by edge kind). Shared by the plaintext and
/// sealed graph expanders; edges are plaintext for both.
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

/// Order graph-reached `(depth, hit)` pairs nearest-first (ties by id), dropping depth.
fn order_graph_hits(mut hits: Vec<(usize, AtomHit)>) -> Vec<AtomHit> {
    hits.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.id.cmp(&b.1.id)));
    hits.into_iter().map(|(_, h)| h).collect()
}

/// In-clause placeholders `$2..` for `depth_of`'s ids, plus the `[region_id, ids..]`
/// param vector (`$1` = region_id).
fn graph_fetch_params(
    region_id: RegionId,
    depth_of: &FxHashMap<AtomId, usize>,
) -> (Vec<Value>, String) {
    let mut fparams: Vec<Value> = vec![Value::Integer(region_id)];
    let mut fph = Vec::with_capacity(depth_of.len());
    for &id in depth_of.keys() {
        fparams.push(Value::Integer(id));
        fph.push(format!("${}", fparams.len()));
    }
    (fparams, fph.join(", "))
}

/// Walk `memory_edges` from `seeds` up to `ge.depth` hops; reachable atoms, nearest first.
fn expand_graph(
    conn: &Connection<'_>,
    table: &str,
    region_id: RegionId,
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
    let (fparams, in_list) = graph_fetch_params(region_id, &depth_of);
    let fetch_sql = format!(
        "SELECT id, kind, CAST(payload AS TEXT), text_content, immutable FROM {table} \
         WHERE region_id = $1 AND id IN ({in_list})"
    );
    let fetched = conn.query_params(&fetch_sql, &fparams)?;

    let mut hits: Vec<(usize, AtomHit)> = Vec::with_capacity(fetched.rows.len());
    for row in &fetched.rows {
        let id = as_int(&row[0])?;
        let depth = *depth_of.get(&id).unwrap_or(&1);
        hits.push((
            depth,
            AtomHit {
                id,
                kind: as_text(&row[1])?.to_string(),
                payload: parse_payload(&row[2]),
                text: opt_text(&row[3]),
                distance: f32::MAX, // graph-reached, not distance-ranked
                score: 1.0 / (depth as f32 + 1.0),
                immutable: as_bool(&row[4]),
            },
        ));
    }
    Ok(order_graph_hits(hits))
}

/// Graph expansion for an encrypted region: walk plaintext edges, then decrypt the
/// reachable atoms' sealed content.
fn expand_graph_sealed(
    conn: &Connection<'_>,
    table: &str,
    atom_wrap: &AtomWrapKey,
    wrapped: &FxHashMap<u64, [u8; WRAPPED_KEY_SIZE]>,
    region_id: RegionId,
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
    let (fparams, in_list) = graph_fetch_params(region_id, &depth_of);
    let fetch_sql = format!(
        "SELECT id, kind, sealed, immutable FROM {table} WHERE region_id = $1 AND id IN ({in_list})"
    );
    let fetched = conn.query_params(&fetch_sql, &fparams)?;

    let mut hits: Vec<(usize, AtomHit)> = Vec::with_capacity(fetched.rows.len());
    for row in &fetched.rows {
        let id = as_int(&row[0])?;
        let depth = *depth_of.get(&id).unwrap_or(&1);
        let Some(w) = wrapped.get(&(id as u64)) else {
            continue;
        };
        let (_emb, text, payload) = open_atom(atom_wrap, w, id, as_blob(&row[2])?)?;
        hits.push((
            depth,
            AtomHit {
                id,
                kind: as_text(&row[1])?.to_string(),
                payload,
                text,
                distance: f32::MAX,
                score: 1.0 / (depth as f32 + 1.0),
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

/// Columns: id, kind, payload(text), text_content, score, created_at, dist, text_rank, immutable.
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

/// Columns: id, kind, payload(text), text_content, score, immutable.
fn parse_fetched(row: &[Value]) -> Result<AtomHit> {
    if row.len() < 6 {
        return Err(MemError::Invalid("unexpected fetch row shape".into()));
    }
    Ok(AtomHit {
        id: as_int(&row[0])?,
        kind: as_text(&row[1])?.to_string(),
        payload: parse_payload(&row[2]),
        text: opt_text(&row[3]),
        distance: f32::MAX,
        score: as_f32(&row[4]),
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

fn as_ts(v: &Value) -> i64 {
    match v {
        Value::Timestamp(t) => *t,
        Value::Integer(i) => *i,
        _ => 0,
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
