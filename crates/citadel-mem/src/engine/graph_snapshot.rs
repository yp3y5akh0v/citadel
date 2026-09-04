use super::*;

const PAGE_SIZE: usize = 1_024;

#[cfg(test)]
std::thread_local! {
    static CLOCK: std::cell::Cell<Option<i64>> = const { std::cell::Cell::new(None) };
}

fn graph_now_micros() -> i64 {
    #[cfg(test)]
    if let Some(now) = CLOCK.with(std::cell::Cell::get) {
        return now;
    }
    now_micros()
}

#[cfg(test)]
pub(crate) fn with_test_clock<T>(now: i64, operation: impl FnOnce() -> T) -> T {
    struct Reset(Option<i64>);
    impl Drop for Reset {
        fn drop(&mut self) {
            CLOCK.with(|clock| clock.set(self.0));
        }
    }
    let _reset = Reset(CLOCK.with(|clock| clock.replace(Some(now))));
    operation()
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct GraphRevision {
    commit: u64,
    keys: u64,
}

pub(crate) struct GraphLiveness {
    bindings: Vec<AtomKeyBinding>,
    built_at: i64,
    expires_at: Option<i64>,
}

pub(crate) struct GraphAtoms {
    pub(crate) ids: Vec<AtomId>,
    pub(crate) liveness: GraphLiveness,
}

impl GraphLiveness {
    pub(crate) fn extend(&mut self, other: Self) {
        self.bindings.extend(other.bindings);
        self.expires_at = match (self.expires_at, other.expires_at) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
    }
}

pub(crate) struct GraphSnapshot<'a, 'db> {
    db: &'a Database,
    conn: &'a Connection<'db>,
    region: &'a RegionHandle,
    revision: GraphRevision,
    now: i64,
    cancel: Option<citadel_core::CancelToken>,
}

impl MemoryEngine {
    pub(crate) fn with_graph_snapshot<T>(
        &self,
        region: &str,
        read: impl FnOnce(&GraphSnapshot<'_, '_>) -> Result<T>,
    ) -> Result<T> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let handle = self.region_handle(&key)?;
        let lifecycle = self.db.key_lifecycle_lock();
        let _edges = self.db.memory_edges_lock();
        let revision = GraphRevision {
            commit: self.db.manager().commit_generation(),
            keys: self.db.cache_epoch(),
        };
        let conn = Connection::open(&self.db)?;
        let result = with_read_txn(&conn, |conn| {
            self.verify_region_live(conn, &handle, &key)?;
            let snapshot = GraphSnapshot {
                db: &self.db,
                conn,
                region: &handle,
                revision,
                now: graph_now_micros(),
                cancel,
            };
            snapshot.check_current()?;
            let result = read(&snapshot)?;
            snapshot.check_current()?;
            Ok(result)
        });
        result.inspect_err(|error| self.defer_stale_region(&key, handle.id, error, &lifecycle))
    }
}

impl GraphSnapshot<'_, '_> {
    pub(crate) fn revision(&self) -> GraphRevision {
        self.revision
    }

    pub(crate) fn check_cancel(&self) -> Result<()> {
        check_cancel(self.cancel.as_ref())
    }

    fn check_current(&self) -> Result<()> {
        self.check_cancel()?;
        if self.db.manager().commit_generation() != self.revision.commit
            || self.db.cache_epoch() != self.revision.keys
        {
            return Err(MemError::Invalid(
                "memory graph changed during the read; retry the operation".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn is_live(&self, liveness: &GraphLiveness) -> Result<bool> {
        self.check_cancel()?;
        if self.now < liveness.built_at
            || liveness.expires_at.is_some_and(|expiry| expiry <= self.now)
        {
            return Ok(false);
        }
        for bindings in liveness.bindings.chunks(PAGE_SIZE) {
            self.check_cancel()?;
            if exact_live_atom_bindings_batch(self.db, bindings)?.contains(&false) {
                return Ok(false);
            }
        }
        self.check_cancel()?;
        Ok(true)
    }

    pub(crate) fn ensure_live(&self, liveness: &GraphLiveness) -> Result<()> {
        if !self.is_live(liveness)?
            || liveness
                .expires_at
                .is_some_and(|expiry| expiry <= graph_now_micros())
        {
            return Err(MemError::Invalid(
                "memory graph liveness changed during the read; retry the operation".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn atoms(&self, kind: &str) -> Result<GraphAtoms> {
        let sealed = self.region.atom_wrap.is_some();
        let columns = if sealed {
            "id, expires_at, key_slot, key_gen"
        } else {
            "id, expires_at"
        };
        let mut atoms = GraphAtoms {
            ids: Vec::new(),
            liveness: GraphLiveness {
                bindings: Vec::new(),
                built_at: self.now,
                expires_at: None,
            },
        };
        let mut after = i64::MIN;
        loop {
            self.check_cancel()?;
            let rows = self.conn.query_params(
                &format!(
                    "SELECT {columns} FROM {table} WHERE region_id = $1 AND kind = $2 \
                     AND (expires_at IS NULL OR expires_at > $3) AND id > $4 \
                     ORDER BY id LIMIT {PAGE_SIZE}",
                    table = self.region.table,
                ),
                &[
                    Value::Integer(self.region.id),
                    Value::Text(kind.into()),
                    Value::Timestamp(self.now),
                    Value::Integer(after),
                ],
            )?;
            let bindings = sealed
                .then(|| {
                    rows.rows
                        .iter()
                        .map(|row| atom_key_binding(as_int(&row[0])?, &row[2], &row[3]))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            let live = bindings
                .as_deref()
                .map(|bindings| exact_live_atom_bindings_batch(self.db, bindings))
                .transpose()?;
            for (index, row) in rows.rows.iter().enumerate() {
                self.check_cancel()?;
                after = as_int(&row[0])?;
                if live.as_ref().is_some_and(|live| !live[index]) {
                    continue;
                }
                atoms.ids.push(after);
                if let Some(bindings) = &bindings {
                    atoms.liveness.bindings.push(bindings[index]);
                }
                let expiry = match row[1] {
                    Value::Null => None,
                    Value::Timestamp(expiry) => Some(expiry),
                    _ => return Err(MemError::Invalid("invalid graph atom expiry".into())),
                };
                if let Some(expiry) = expiry {
                    atoms.liveness.expires_at = Some(
                        atoms
                            .liveness
                            .expires_at
                            .map_or(expiry, |old| old.min(expiry)),
                    );
                }
            }
            if rows.rows.len() < PAGE_SIZE {
                break;
            }
        }
        self.check_cancel()?;
        Ok(atoms)
    }

    pub(crate) fn edges(&self, sources: &[AtomId]) -> Result<Vec<(AtomId, AtomId, EdgeKind, f32)>> {
        let mut edges = Vec::new();
        let mut sources = sources.to_vec();
        sources.sort_unstable();
        sources.dedup();
        let universe: FxHashSet<AtomId> = sources.iter().copied().collect();
        let mut budget = GraphEdgeBudget::new();
        for source in sources {
            self.check_cancel()?;
            for row in source_edge_rows(self.conn, source, &mut budget)? {
                self.check_cancel()?;
                let destination = as_int(&row[0])?;
                if !universe.contains(&destination) {
                    continue;
                }
                let kind = edge_kind_from_str(as_text(&row[1])?)?;
                if matches!(kind, EdgeKind::DerivedFrom | EdgeKind::SimilarTo) {
                    edges.push((source, destination, kind, as_f32(&row[2])?));
                }
            }
        }
        edges.sort_by_key(|&(source, destination, kind, _)| {
            (kind != EdgeKind::DerivedFrom, source, destination)
        });
        self.check_cancel()?;
        Ok(edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_writes_cannot_publish_a_mixed_graph_snapshot() {
        let db = Arc::new(
            citadel::DatabaseBuilder::new("")
                .passphrase(b"test-passphrase")
                .argon2_profile(citadel::Argon2Profile::Iot)
                .create_in_memory()
                .unwrap(),
        );
        let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
        eng.create_region("r", Arc::new(crate::MockEmbedder::new(8)))
            .unwrap();
        let atom = eng.remember("r", AtomInput::new("turn", "old")).unwrap();
        let table = atoms_table(8, EmbeddingMetric::Cosine, false);
        let error = eng
            .with_graph_snapshot("r", |snapshot| {
                assert_eq!(snapshot.atoms("turn")?.ids, vec![atom]);
                Connection::open(&db)?.execute(&format!("DELETE FROM {table}"))?;
                assert_eq!(snapshot.atoms("turn")?.ids, vec![atom]);
                Ok(())
            })
            .unwrap_err();
        assert!(error.to_string().contains("graph changed during the read"));
        assert!(eng.fetch_one("r", atom).unwrap().is_none());
    }

    #[test]
    fn epoch_changes_invalidate_an_in_flight_snapshot() {
        let db = Arc::new(
            citadel::DatabaseBuilder::new("")
                .passphrase(b"test-passphrase")
                .argon2_profile(citadel::Argon2Profile::Iot)
                .create_in_memory()
                .unwrap(),
        );
        let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
        eng.create_region("r", Arc::new(crate::MockEmbedder::new(8)))
            .unwrap();
        let error = eng
            .with_graph_snapshot("r", |snapshot| {
                assert!(snapshot.atoms("turn")?.ids.is_empty());
                db.bump_cache_epoch();
                Ok(())
            })
            .unwrap_err();
        assert!(error.to_string().contains("graph changed during the read"));
    }
}
