//! Provenance audit: every derived atom must chain by DerivedFrom to the transcript.

use crate::{AtomId, EdgeKind, FetchQuery, MemoryEngine};
use rustc_hash::FxHashSet;

/// Deeper chains than repair passes compose indicate runaway self-derivation.
pub const AUDIT_DEPTH_CAP: usize = 8;

const AUDIT_PAGE: usize = 1024;

#[derive(Debug, Clone, Default)]
pub struct ProvenanceAudit {
    pub derived_total: usize,
    pub verified: usize,
    pub violations: Vec<ProvenanceViolation>,
}

impl ProvenanceAudit {
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct ProvenanceViolation {
    pub atom_id: AtomId,
    pub reason: String,
}

/// Verify each derived atom's closure reaches a non-derived atom within the cap.
pub fn audit_provenance(
    eng: &MemoryEngine,
    region: &str,
    derived_kind: &str,
) -> crate::Result<ProvenanceAudit> {
    let mut audit = ProvenanceAudit::default();
    let mut after: Option<AtomId> = None;
    loop {
        let mut q = FetchQuery::new(AUDIT_PAGE).with_kind(derived_kind);
        if let Some(id) = after {
            q = q.with_after_id(id);
        }
        let page = eng.fetch_range(region, &q)?;
        let Some(last) = page.last() else {
            break;
        };
        after = Some(last.id);
        for atom in &page {
            audit.derived_total += 1;
            match verify_closure(eng, region, atom.id, derived_kind)? {
                None => audit.verified += 1,
                Some(reason) => audit.violations.push(ProvenanceViolation {
                    atom_id: atom.id,
                    reason,
                }),
            }
        }
    }
    Ok(audit)
}

/// None = holds, Some(reason) = violation; an unreadable store is not a verdict.
fn verify_closure(
    eng: &MemoryEngine,
    region: &str,
    root: AtomId,
    derived_kind: &str,
) -> crate::Result<Option<String>> {
    let mut visited: FxHashSet<AtomId> = FxHashSet::default();
    let mut frontier = vec![root];
    let mut reached_terminal = false;
    for _ in 0..AUDIT_DEPTH_CAP {
        let mut next = Vec::new();
        for id in frontier.drain(..) {
            if !visited.insert(id) {
                continue;
            }
            let edges = eng.fetch_edges(Some(id), None, Some(EdgeKind::DerivedFrom))?;
            if edges.is_empty() {
                return Ok(Some(format!("derived atom {id} has no DerivedFrom edge")));
            }
            for e in &edges {
                let Some(src) = eng.fetch_one(region, e.dst_id)? else {
                    return Ok(Some(format!(
                        "derived atom {id} cites missing atom {}",
                        e.dst_id
                    )));
                };
                if src.kind == derived_kind {
                    next.push(src.id);
                } else {
                    reached_terminal = true;
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    if !frontier.is_empty() {
        return Ok(Some(format!(
            "derivation chain from {root} exceeds depth {AUDIT_DEPTH_CAP}"
        )));
    }
    if !reached_terminal {
        return Ok(Some(format!(
            "derivation closure of {root} never reaches a non-derived atom"
        )));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AtomInput, MemoryEngine, MockEmbedder};
    use citadel::{Argon2Profile, DatabaseBuilder};
    use std::sync::Arc;

    fn engine(dir: &std::path::Path) -> MemoryEngine {
        let db = DatabaseBuilder::new(dir.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let eng = MemoryEngine::open(Arc::new(db)).unwrap();
        eng.create_region("r", Arc::new(MockEmbedder::new(64)))
            .unwrap();
        eng
    }

    #[test]
    fn clean_and_two_level_chains_verify() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let a = eng.remember("r", AtomInput::new("turn", "alpha")).unwrap();
        let b = eng.remember("r", AtomInput::new("turn", "beta")).unwrap();
        let d1 = eng
            .remember_derived("r", AtomInput::new("derived", "ab fact"), &[a, b], None)
            .unwrap();
        eng.remember_derived("r", AtomInput::new("derived", "meta fact"), &[d1], None)
            .unwrap();

        let audit = audit_provenance(&eng, "r", "derived").unwrap();
        assert_eq!(audit.derived_total, 2);
        assert_eq!(audit.verified, 2);
        assert!(audit.ok());
    }

    #[test]
    fn unsourced_derived_atom_is_a_violation() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        eng.remember("r", AtomInput::new("derived", "orphan claim"))
            .unwrap();

        let audit = audit_provenance(&eng, "r", "derived").unwrap();
        assert_eq!(audit.derived_total, 1);
        assert!(!audit.ok());
        assert!(audit.violations[0].reason.contains("no DerivedFrom edge"));
    }

    #[test]
    fn derived_only_cycle_never_reaches_the_transcript() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let d1 = eng.remember("r", AtomInput::new("derived", "c1")).unwrap();
        let d2 = eng.remember("r", AtomInput::new("derived", "c2")).unwrap();
        eng.link(d1, d2, EdgeKind::DerivedFrom, 1.0).unwrap();
        eng.link(d2, d1, EdgeKind::DerivedFrom, 1.0).unwrap();

        let audit = audit_provenance(&eng, "r", "derived").unwrap();
        assert_eq!(audit.derived_total, 2);
        assert_eq!(audit.verified, 0);
        assert!(audit
            .violations
            .iter()
            .all(|v| v.reason.contains("never reaches a non-derived atom")));
    }
}
