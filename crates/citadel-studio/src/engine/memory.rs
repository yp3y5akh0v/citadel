//! Regions, atoms, verdicts, and erasure receipts.

use citadel_mem::{
    AtomAttestation, AtomHit, AtomId, AttestVerdict, EmbeddingMetric, ErasureReceipt, FetchQuery,
    MemoryRegionInfo,
};

use super::error::{IntoStudioError, StudioError};
use super::session::Session;

/// Atoms per page, sized above the largest visible grid window.
pub const PAGE: usize = 500;

/// Region identity and current inventory.
#[derive(Clone, Debug)]
pub struct RegionFacts {
    pub name: String,
    pub dim: u16,
    pub metric: EmbeddingMetric,
    pub model: String,
    /// Plaintext regions have no per-atom MAC or cryptographic erasure.
    pub plaintext: bool,
    /// `None` means unreadable, not empty.
    pub total: Option<u64>,
    pub unreadable: Option<String>,
}

impl RegionFacts {
    fn from(identity: &MemoryRegionInfo, total: Option<u64>, unreadable: Option<String>) -> Self {
        Self {
            name: identity.name().to_owned(),
            dim: identity.dim(),
            metric: identity.metric(),
            model: identity.model_id().to_owned(),
            plaintext: !identity.encrypted(),
            total,
            unreadable,
        }
    }

    pub fn metric_label(&self) -> &'static str {
        match self.metric {
            EmbeddingMetric::Cosine => "cosine",
            EmbeddingMetric::L2 => "l2",
            EmbeddingMetric::InnerProduct => "inner product",
        }
    }
}

/// Memory-grid row combining fetched content with an optional later attestation.
#[derive(Clone, Debug)]
pub struct AtomView {
    pub id: AtomId,
    pub kind: String,
    pub text: String,
    pub created_at: i64,
    pub immutable: bool,
    pub verdict: Option<AttestVerdict>,
    pub aad_bound: bool,
    pub key_slot: Option<u32>,
    pub key_gen: Option<u64>,
    /// Session-local verification time.
    pub verified_at: Option<String>,
}

/// Deterministic page with its continuation cursor.
#[derive(Clone, Debug)]
pub struct AtomPage {
    pub atoms: Vec<AtomView>,
    pub next_after_id: Option<AtomId>,
}

impl std::ops::Deref for AtomPage {
    type Target = [AtomView];

    fn deref(&self) -> &Self::Target {
        &self.atoms
    }
}

impl AtomView {
    fn from(hit: AtomHit) -> Self {
        Self {
            id: hit.id,
            kind: hit.kind,
            text: hit.text,
            created_at: hit.created_at,
            immutable: hit.immutable,
            verdict: None,
            aad_bound: false,
            key_slot: None,
            key_gen: None,
            verified_at: None,
        }
    }

    /// Apply a verdict and its key provenance.
    pub fn attest(&mut self, attestation: &AtomAttestation, at: &str) {
        self.verdict = Some(attestation.verdict);
        self.aad_bound = attestation.aad_bound;
        self.key_slot = attestation.key_slot;
        self.key_gen = attestation.key_gen;
        self.verified_at = Some(at.to_owned());
    }

    /// Apply the region-wide fact that plaintext atoms have no per-atom proof.
    pub fn mark_plaintext_unattested(&mut self) {
        self.verdict = Some(AttestVerdict::PlaintextUnattested);
        self.aad_bound = false;
        self.key_slot = None;
        self.key_gen = None;
        self.verified_at = None;
    }

    pub fn evidence(&self) -> crate::theme::Evidence {
        self.verdict.into()
    }

    /// When the atom was last verified, using the shared unverified label.
    pub fn checked(&self) -> &str {
        match (&self.verified_at, self.verdict) {
            (Some(at), _) => at,
            (None, Some(AttestVerdict::PlaintextUnattested)) => "Not applicable",
            (None, _) => crate::theme::Evidence::Unverified.label(),
        }
    }

    /// Stored microsecond epoch rendered as a date.
    pub fn created(&self) -> String {
        let secs = self.created_at.div_euclid(1_000_000);
        match chrono_free_date(secs) {
            Some(date) => date,
            None => "-".to_owned(),
        }
    }

    /// Verdict label including id binding when present.
    pub fn verdict_label(&self) -> String {
        match (self.verdict, self.aad_bound) {
            (Some(AttestVerdict::Authentic), true) => "AUTHENTIC . aad_bound".to_owned(),
            _ => self.evidence().label().to_owned(),
        }
    }

    pub fn key_label(&self) -> String {
        match (self.key_slot, self.key_gen) {
            (Some(slot), Some(gen)) => format!("{slot} . gen {gen}"),
            _ => "-".to_owned(),
        }
    }
}

/// Convert epoch days to a proleptic Gregorian date.
fn chrono_free_date(secs: i64) -> Option<String> {
    let days = secs.div_euclid(86_400);
    let time = secs.rem_euclid(86_400);
    // Shift to March so leap days end their cycle.
    let z = days.checked_add(719_468)?;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    Some(format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}",
        time / 3_600,
        (time % 3_600) / 60
    ))
}

/// Inventory every region without converting unreadable counts to zero.
pub fn regions(session: &mut Session) -> Result<Vec<RegionFacts>, StudioError> {
    let maintenance = session.memory_maintenance()?;
    let inventory = maintenance
        .inventory()
        .map_err(IntoStudioError::into_studio)?;
    let mut regions: Vec<RegionFacts> = inventory
        .iter()
        .map(|item| {
            RegionFacts::from(
                item.region(),
                item.live_atoms(),
                item.unavailable().map(str::to_owned),
            )
        })
        .collect();
    // The backing query has no ordering guarantee.
    regions.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(regions)
}

/// Fetch one id-ascending page using a keyset cursor.
pub fn atoms(
    session: &mut Session,
    region: &str,
    after: Option<AtomId>,
) -> Result<AtomPage, StudioError> {
    let maintenance = session.memory_maintenance()?;
    let mut query = FetchQuery::new(PAGE);
    query.after_id = after;
    let page = maintenance
        .fetch_page(region, &query)
        .map_err(IntoStudioError::into_studio)?;
    Ok(AtomPage {
        atoms: page.atoms.into_iter().map(AtomView::from).collect(),
        next_after_id: page.next_after_id,
    })
}

/// Verify exactly the visible ids without broadening the claim to the region.
pub fn verify(
    session: &mut Session,
    region: &str,
    ids: &[AtomId],
) -> Result<Vec<AtomAttestation>, StudioError> {
    let maintenance = session.memory_maintenance()?;
    maintenance
        .verify_atoms(region, ids)
        .map_err(IntoStudioError::into_studio)
}

/// Forget atoms without overriding immutable markers and return the engine receipt.
pub fn forget(
    session: &mut Session,
    region: &str,
    ids: &[AtomId],
) -> Result<ErasureReceipt, StudioError> {
    let maintenance = session.memory_maintenance()?;
    maintenance
        .forget_atoms(region, ids, false)
        .map_err(IntoStudioError::into_studio)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demo() -> crate::engine::session::DemoVault {
        crate::engine::session::DemoVault::new()
    }

    fn region(session: &mut Session, name: &str) -> RegionFacts {
        regions(session)
            .unwrap()
            .into_iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("the demo seeds a region called {name}"))
    }

    /// A seeded vault reopened from disk with no regions pre-attached.
    fn reopened(dir: &std::path::Path) -> Session {
        let path = dir.join("reopened.cdl");
        let spec = super::super::session::CreateSpec {
            path: path.clone(),
            passphrase: "reopen-passphrase".to_owned().into(),
            kdf: citadel::KdfAlgorithm::Argon2id,
            profile: citadel::Argon2Profile::Iot,
        };
        let mut created = Session::create(&spec).expect("create");
        super::super::demo::seed(&mut created).expect("seed");
        drop(created);
        Session::open(&path, "reopen-passphrase").expect("reopen from disk")
    }

    #[test]
    fn a_vault_opened_from_disk_lists_its_regions_with_their_atoms() {
        let dir = tempfile::tempdir().unwrap();
        let mut session = reopened(dir.path());

        let listed = regions(&mut session).unwrap();
        let names: Vec<&str> = listed.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["episodic", "scratch", "semantic"]);
        for r in &listed {
            assert!(
                r.unreadable.is_none(),
                "region {} reported unreadable: {:?}",
                r.name,
                r.unreadable
            );
        }
        assert_eq!(
            listed.iter().find(|r| r.name == "episodic").unwrap().total,
            Some(24)
        );
    }

    #[test]
    fn a_vault_opened_from_disk_serves_atoms_verdicts_and_erasure() {
        let dir = tempfile::tempdir().unwrap();
        let mut session = reopened(dir.path());

        let page = atoms(&mut session, "episodic", None).unwrap();
        assert_eq!(page.len(), 24);

        let ids: Vec<AtomId> = page.iter().take(3).map(|a| a.id).collect();
        assert_eq!(verify(&mut session, "episodic", &ids).unwrap().len(), 3);

        // Erasure must remain possible when the model is unavailable.
        let receipt = forget(&mut session, "episodic", &ids[..1]).unwrap();
        assert_eq!(receipt.rows_deleted, 1);
    }

    #[test]
    fn a_forgotten_region_is_unknown_instead_of_empty() {
        let mut session = demo();
        let stored = session
            .query("SELECT id, rsk_slot, rsk_gen FROM memory_regions WHERE name = 'episodic'")
            .unwrap();
        let (
            citadel_sql::Value::Integer(region_id),
            citadel_sql::Value::Integer(slot),
            citadel_sql::Value::Integer(generation),
        ) = (&stored.rows[0][0], &stored.rows[0][1], &stored.rows[0][2])
        else {
            panic!("the encrypted region must name its key binding");
        };
        session
            .database()
            .region_store_tombstone(*slot as u32, *region_id as u64, *generation as u64)
            .unwrap();

        let facts = region(&mut session, "episodic");
        assert_eq!(facts.total, None, "unreadable is not the number zero");
        assert!(facts
            .unreadable
            .as_deref()
            .is_some_and(|detail| detail.contains("forgotten")));
        assert_eq!(
            atoms(&mut session, "episodic", None).unwrap_err().kind,
            super::super::error::Kind::Forgotten
        );
    }

    #[test]
    fn a_region_mid_reembed_is_unknown_instead_of_empty() {
        let mut session = demo();
        let run = session.run(
            "UPDATE memory_regions SET metadata = \
             '{\"reembed_to_model\":\"future\",\"reembed_to_dim\":32,\
               \"reembed_to_metric\":\"cosine\",\"reembed_done_through\":0,\
               \"reembed_phase\":\"vectors\"}'::JSONB WHERE name = 'scratch';",
        );
        assert!(run.failed.is_none(), "{:?}", run.failed);

        let facts = region(&mut session, "scratch");
        assert_eq!(facts.total, None, "unsupported is not the number zero");
        assert!(facts
            .unreadable
            .as_deref()
            .is_some_and(|detail| detail.contains("resume reembed_region")));
    }

    #[test]
    fn regions_carry_their_identity_and_a_real_count() {
        let mut session = demo();
        let listed = regions(&mut session).unwrap();
        let names: Vec<&str> = listed.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["episodic", "scratch", "semantic"]);

        let episodic = region(&mut session, "episodic");
        assert_eq!(
            episodic.total,
            Some(24),
            "the count is the engine's, not a guess"
        );
        assert!(!episodic.plaintext);
        assert_eq!(episodic.model, "mock-fnv1a-bow-v1");
        assert_eq!(episodic.metric_label(), "cosine");
        assert_eq!(region(&mut session, "semantic").total, Some(9));
        assert!(
            region(&mut session, "scratch").plaintext,
            "scratch is the plaintext region"
        );
    }

    #[test]
    fn atoms_page_forward_without_repeating_or_skipping() {
        let mut session = demo();
        let first = atoms(&mut session, "episodic", None).unwrap();
        assert_eq!(first.len(), 24, "one page holds the whole region here");
        assert!(first.iter().all(|a| a.verdict.is_none()), "nothing checked");

        let last = first.last().expect("non-empty").id;
        let next = atoms(&mut session, "episodic", Some(last)).unwrap();
        assert!(next.is_empty(), "there is nothing past the last id");
        assert!(
            next.next_after_id.is_none(),
            "the terminal page has no cursor"
        );
    }

    #[test]
    fn an_untouched_atom_in_an_encrypted_region_is_authentic() {
        let mut session = demo();
        let ids: Vec<AtomId> = atoms(&mut session, "episodic", None)
            .unwrap()
            .iter()
            .map(|a| a.id)
            .collect();
        let verdicts = verify(&mut session, "episodic", &ids).unwrap();

        assert_eq!(verdicts.len(), ids.len());
        assert!(verdicts
            .iter()
            .all(|v| v.verdict == AttestVerdict::Authentic));
        assert!(
            verdicts.iter().all(|v| v.aad_bound),
            "a verdict that is not id-bound proves integrity but not origin"
        );
        assert!(verdicts.iter().all(|v| v.key_slot.is_some()));
    }

    #[test]
    fn a_plaintext_region_cannot_be_attested_at_all() {
        let mut session = demo();
        let ids: Vec<AtomId> = atoms(&mut session, "scratch", None)
            .unwrap()
            .iter()
            .map(|a| a.id)
            .collect();
        let verdicts = verify(&mut session, "scratch", &ids).unwrap();

        assert!(!verdicts.is_empty());
        assert!(verdicts
            .iter()
            .all(|v| v.verdict == AttestVerdict::PlaintextUnattested));
        assert!(
            verdicts
                .iter()
                .all(|v| !v.aad_bound && v.key_slot.is_none()),
            "there is no key to name on a plaintext region"
        );
    }

    #[test]
    fn a_receipt_refuses_to_claim_crypto_erasure_on_a_plaintext_region() {
        let mut session = demo();

        let sealed = atoms(&mut session, "episodic", None).unwrap()[0].id;
        let receipt = forget(&mut session, "episodic", &[sealed]).unwrap();
        assert!(receipt.cryptographic_erasure);
        assert_eq!(receipt.erased_count, 1);
        assert!(
            receipt.readback_confirmed,
            "an encrypted erasure reads back"
        );
        assert!(!receipt.algorithm.is_empty(), "the key wrap is named");

        let plain = atoms(&mut session, "scratch", None).unwrap()[0].id;
        let receipt = forget(&mut session, "scratch", &[plain]).unwrap();
        assert!(
            !receipt.cryptographic_erasure,
            "a plaintext region has no key to destroy"
        );
        assert_eq!(receipt.rows_deleted, 1);
        assert!(
            receipt.algorithm.is_empty(),
            "naming a key-wrap algorithm here would claim a key that never existed"
        );
        assert!(!receipt.readback_confirmed);
    }

    #[test]
    fn a_stored_creation_clock_reads_back_as_its_own_date() {
        let mut session = demo();
        let first = &atoms(&mut session, "episodic", None).unwrap()[0];
        assert_eq!(first.created(), "2025-10-09 08:53");
        assert_eq!(first.checked(), crate::theme::Evidence::Unverified.label());
        assert_eq!(first.key_label(), "-", "nothing has named a key yet");
    }

    #[test]
    fn a_forgotten_atom_leaves_the_region() {
        let mut session = demo();
        let before = region(&mut session, "scratch").total.unwrap();
        let id = atoms(&mut session, "scratch", None).unwrap()[0].id;

        forget(&mut session, "scratch", &[id]).unwrap();

        let after = region(&mut session, "scratch").total.unwrap();
        assert_eq!(after, before - 1);
        assert!(
            atoms(&mut session, "scratch", None)
                .unwrap()
                .iter()
                .all(|a| a.id != id),
            "the forgotten atom is still being listed"
        );
    }
}
