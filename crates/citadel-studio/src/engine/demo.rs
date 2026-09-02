//! Deterministic demo data written through the real encrypted engine.
//!
//! Encrypted and plaintext regions expose authentic and unattested outcomes without
//! fabricating corruption evidence.

use std::sync::Arc;

use citadel_mem::{AtomInput, MockEmbedder};

use super::error::StudioError;
use super::session::Session;

/// Small embedding width that still exercises the vector inspection path.
const DIM: usize = 32;

/// Number of vector-bearing demo documents.
const DOCUMENTS: usize = 1_500;

/// Rows per batched insert.
const BATCH: usize = 250;

const DISPLAY_CENTRES: [(f32, f32); COLLECTIONS.len()] =
    [(-0.58, -0.42), (0.52, -0.48), (-0.48, 0.50), (0.58, 0.44)];
const DISPLAY_RADIUS: f32 = 0.16;
const FEATURE_JITTER: f32 = 0.08;

pub fn seed(session: &mut Session) -> Result<(), StudioError> {
    tables(session)?;
    regions(session)?;
    Ok(())
}

fn tables(session: &Session) -> Result<(), StudioError> {
    script(
        session,
        "CREATE TABLE customers (
             id INTEGER PRIMARY KEY,
             name TEXT NOT NULL,
             tier TEXT NOT NULL DEFAULT 'standard',
             region TEXT NOT NULL,
             signed_up TIMESTAMP
         );
         CREATE TABLE documents (
             id INTEGER PRIMARY KEY,
             customer_id INTEGER,
             title TEXT NOT NULL,
             collection TEXT NOT NULL,
             embedding VECTOR(32)
         );",
    )?;

    let mut sql = String::from("INSERT INTO customers (id, name, tier, region, signed_up) VALUES ");
    for (i, (name, tier, region)) in CUSTOMERS.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        // Fixed dates keep demo output reproducible.
        sql.push_str(&format!(
            "({}, '{name}', '{tier}', '{region}', TIMESTAMP '2026-01-{:02} 09:00:00')",
            i + 1,
            i + 5
        ));
    }
    sql.push(';');
    script(session, &sql)?;

    for chunk in 0..DOCUMENTS.div_ceil(BATCH) {
        let first = chunk * BATCH;
        let last = ((chunk + 1) * BATCH).min(DOCUMENTS);
        let mut sql = String::from(
            "INSERT INTO documents (id, customer_id, title, collection, embedding) VALUES ",
        );
        for i in first..last {
            if i > first {
                sql.push(',');
            }
            let collection = COLLECTIONS[i % COLLECTIONS.len()];
            sql.push_str(&format!(
                "({}, {}, '{} note {}', '{collection}', {})",
                i + 1,
                (i % CUSTOMERS.len()) + 1,
                collection,
                i + 1,
                vector_literal(i)
            ));
        }
        sql.push(';');
        script(session, &sql)?;
    }
    Ok(())
}

/// Deterministic collection clusters with decorrelated document jitter.
fn vector_literal(i: usize) -> String {
    let values = vector_values(i);
    let mut out = String::from("'[");
    for (d, value) in values.into_iter().enumerate() {
        if d > 0 {
            out.push(',');
        }
        out.push_str(&format!("{value:.4}"));
    }
    out.push_str("]'::VECTOR(32)");
    out
}

/// Build vectors that are meaningful both in the displayed dimensions and under the
/// full-dimensional distance metric used by the database.
fn vector_values(i: usize) -> [f32; DIM] {
    let collection = i % COLLECTIONS.len();
    let mut values = [0.0; DIM];

    // A truncated radial Gaussian produces a natural dense centre without outliers.
    // Feeding consecutive LCG outputs into x/y forms visible lattice lines instead.
    const SIGMAS: f32 = 3.0;
    let unit = unit_hash(i as u64, 0, 0x4f1b_bcdd_7916_3e2d);
    let bounded_mass = 1.0 - (-0.5 * SIGMAS * SIGMAS).exp();
    let radius = (-2.0 * (1.0 - unit * bounded_mass).ln()).sqrt() * (DISPLAY_RADIUS / SIGMAS);
    let angle = unit_hash(i as u64, 1, 0xa24b_aed4_963e_e407) * std::f32::consts::TAU;
    values[0] = DISPLAY_CENTRES[collection].0 + radius * angle.cos();
    values[1] = DISPLAY_CENTRES[collection].1 + radius * angle.sin();

    for (dimension, value) in values.iter_mut().enumerate().skip(2) {
        let centre = signed_hash(collection as u64, dimension as u64, 0x9fb2_1c65_1e98_df25) * 0.55;
        let jitter = (signed_hash(i as u64, dimension as u64, 0xd6e8_feb8_6659_fd93)
            + signed_hash(i as u64, dimension as u64, 0x94d0_49bb_1331_11eb))
            * FEATURE_JITTER;
        *value = centre + jitter;
    }
    values
}

fn signed_hash(item: u64, dimension: u64, salt: u64) -> f32 {
    unit_hash(item, dimension, salt) * 2.0 - 1.0
}

/// SplitMix64 finalization gives each item/dimension pair an independent stable value.
fn unit_hash(item: u64, dimension: u64, salt: u64) -> f32 {
    let mut value = item
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(dimension.wrapping_mul(0xbf58_476d_1ce4_e5b9))
        ^ salt;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    ((value >> 40) as u32) as f32 / ((1u32 << 24) - 1) as f32
}

fn regions(session: &mut Session) -> Result<(), StudioError> {
    let engine = session.demo_memory()?;
    let embedder = || Arc::new(MockEmbedder::new(DIM)) as Arc<dyn citadel_mem::Embedder>;

    engine
        .create_encrypted_region("episodic", embedder())
        .map_err(super::error::IntoStudioError::into_studio)?;
    engine
        .create_encrypted_region("semantic", embedder())
        .map_err(super::error::IntoStudioError::into_studio)?;
    // Plaintext exposes `PlaintextUnattested` and non-cryptographic erasure honestly.
    engine
        .create_region("scratch", embedder())
        .map_err(super::error::IntoStudioError::into_studio)?;

    for (region, kind, texts) in [
        ("episodic", "turn", EPISODIC.as_slice()),
        ("semantic", "fact", SEMANTIC.as_slice()),
        ("scratch", "note", SCRATCH.as_slice()),
    ] {
        let atoms = texts
            .iter()
            .enumerate()
            .map(|(i, text)| {
                AtomInput::new(kind, *text)
                    .with_created_at(1_760_000_000_000_000i64 + (i as i64) * 3_600_000_000)
            })
            .collect();
        engine
            .remember_batch(region, atoms)
            .map_err(super::error::IntoStudioError::into_studio)?;
    }
    Ok(())
}

fn script(session: &Session, sql: &str) -> Result<(), StudioError> {
    let run = session.run(sql);
    match run.failed {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

const CUSTOMERS: [(&str, &str, &str); 8] = [
    ("Aldridge Freight", "enterprise", "eu-west"),
    ("Baumann Analytics", "standard", "eu-central"),
    ("Coastline Robotics", "enterprise", "us-east"),
    ("Delta Sequencing", "standard", "us-west"),
    ("Erlend Maritime", "trial", "eu-north"),
    ("Fairweather Labs", "enterprise", "ap-south"),
    ("Grimaldi Textiles", "standard", "eu-south"),
    ("Halvorsen Energy", "trial", "eu-north"),
];

const COLLECTIONS: [&str; 4] = ["research", "support", "contracts", "incident"];

/// Conversation turns.
const EPISODIC: [&str; 24] = [
    "the migration window was moved to the first weekend of the quarter",
    "Aldridge asked for the retention policy in writing before signing",
    "the on-call rota now hands over at 09:00 rather than midnight",
    "throughput regressed after the index rebuild and recovered overnight",
    "staging credentials were rotated on the 14th",
    "the incident review found no customer data left the region",
    "Baumann wants the weekly export in Parquet rather than CSV",
    "the contract renewal is blocked on the security questionnaire",
    "Coastline's pilot fleet reports GPS drift in tunnels",
    "we agreed to cap the trial at ninety days",
    "the backup restore drill completed in eleven minutes",
    "Delta asked whether embeddings can be deleted independently",
    "the answer is yes, per-atom, with a receipt",
    "Erlend's vessels are offline for six hours a day by design",
    "the sync protocol tolerates that without a full resend",
    "Fairweather escalated a latency spike in ap-south",
    "the cause was a cold cache after a deploy, not the index",
    "Grimaldi requested a data processing addendum",
    "legal returned it with two redlines on sub-processors",
    "the demo uses a disposable working copy and ordinary vaults keep their changes",
    "Halvorsen has not logged in since the trial started",
    "the quarterly review is scheduled for the third Tuesday",
    "we owe Coastline a written answer on tunnel drift",
    "nothing in this region has been forgotten yet",
];

/// Distilled statements.
const SEMANTIC: [&str; 9] = [
    "Aldridge Freight is on the enterprise tier in eu-west",
    "retention policy commitments must be given in writing",
    "index rebuilds cause a transient throughput regression",
    "credential rotation happens on the 14th of each month",
    "no customer data has left its region in any reviewed incident",
    "Baumann Analytics prefers Parquet over CSV",
    "trials are capped at ninety days by agreement",
    "per-atom erasure is cryptographic and issues a receipt",
    "cold caches after a deploy look like index latency but are not",
];

/// Plaintext working notes, unattested and deleted without key destruction.
const SCRATCH: [&str; 5] = [
    "draft: tunnel drift reply, needs a number from the fleet team",
    "check whether ap-south cache warmup can be moved into the deploy",
    "ask legal which sub-processor list is current",
    "the export job has no owner since the rota changed",
    "remember to close the ninety-day trial on Halvorsen",
];

#[cfg(test)]
mod tests {
    use super::*;
    use citadel_mem::FetchQuery;

    #[test]
    fn the_seed_is_readable_back_through_the_engine() {
        let mut session = crate::engine::session::DemoVault::new();
        let engine = session.demo_memory().unwrap();

        for (region, expected) in [
            ("episodic", EPISODIC.len() as u64),
            ("semantic", SEMANTIC.len() as u64),
            ("scratch", SCRATCH.len() as u64),
        ] {
            assert_eq!(
                engine.count_region(region).unwrap(),
                expected,
                "{region} should hold what was written to it"
            );
            let page = engine.fetch_range(region, &FetchQuery::new(100)).unwrap();
            assert_eq!(page.len() as u64, expected);
            assert!(page.iter().all(|hit| !hit.text.is_empty()));
        }
    }

    #[test]
    fn one_region_is_encrypted_and_one_is_plaintext() {
        let mut session = crate::engine::session::DemoVault::new();
        let engine = session.demo_memory().unwrap();
        let identities = engine.stored_region_identities().unwrap();

        let encrypted: Vec<_> = identities
            .iter()
            .filter(|r| r.encrypted())
            .map(|r| r.name().to_owned())
            .collect();
        let plaintext: Vec<_> = identities
            .iter()
            .filter(|r| !r.encrypted())
            .map(|r| r.name().to_owned())
            .collect();

        assert_eq!(encrypted, ["episodic", "semantic"]);
        assert_eq!(plaintext, ["scratch"]);
    }

    #[test]
    fn the_documents_table_carries_a_readable_vector_column() {
        let session = crate::engine::session::DemoVault::new();
        let rows = session
            .query("SELECT id, embedding FROM documents ORDER BY id LIMIT 4")
            .unwrap();
        assert_eq!(rows.rows.len(), 4);
        for row in &rows.rows {
            match &row[1] {
                citadel_sql::Value::Vector(v) => assert_eq!(v.len(), DIM),
                other => panic!("embedding should be a vector, got {other:?}"),
            }
        }
        let all = session.query("SELECT COUNT(*) FROM documents").unwrap();
        assert_eq!(
            all.rows[0][0],
            citadel_sql::Value::Integer(DOCUMENTS as i64)
        );
    }

    #[test]
    fn the_seed_is_deterministic() {
        let first = crate::engine::session::DemoVault::new();
        let second = crate::engine::session::DemoVault::new();
        let sql = "SELECT id, title, embedding FROM documents ORDER BY id LIMIT 32";
        assert_eq!(
            first.query(sql).unwrap().rows,
            second.query(sql).unwrap().rows
        );
    }

    #[test]
    fn demo_embeddings_form_diffuse_collection_clusters() {
        const SECTORS: usize = 16;

        for (collection, &(centre_x, centre_y)) in DISPLAY_CENTRES.iter().enumerate() {
            let mut seen = [false; SECTORS];
            let mut sum = (0.0, 0.0);
            let mut count = 0.0;
            for i in (collection..DOCUMENTS).step_by(COLLECTIONS.len()) {
                let values = vector_values(i);
                let (dx, dy) = (values[0] - centre_x, values[1] - centre_y);
                assert!(
                    dx.hypot(dy) <= DISPLAY_RADIUS + 1.0e-6,
                    "document {i} escaped collection {collection}'s display cluster"
                );
                let angle = (dy.atan2(dx) + std::f32::consts::PI) / std::f32::consts::TAU;
                seen[((angle * SECTORS as f32) as usize).min(SECTORS - 1)] = true;
                sum.0 += values[0];
                sum.1 += values[1];
                count += 1.0;
            }
            assert!(seen.into_iter().all(std::convert::identity));
            assert!((sum.0 / count - centre_x).abs() < 0.02);
            assert!((sum.1 / count - centre_y).abs() < 0.02);
        }

        let squared_distance = |a: &[f32; DIM], b: &[f32; DIM]| {
            a.iter()
                .zip(b)
                .map(|(left, right)| (left - right).powi(2))
                .sum::<f32>()
        };
        let same_collection = squared_distance(&vector_values(0), &vector_values(4));
        let other_collection = squared_distance(&vector_values(0), &vector_values(1));
        assert!(same_collection * 4.0 < other_collection);
    }
}
