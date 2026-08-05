//! fetch_range: id-order listing with after_id watermark and event-time window.

use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, FetchQuery, MemoryEngine, MockEmbedder};
use serde_json::json;

const DIM: usize = 64;

fn engine(dir: &std::path::Path) -> MemoryEngine {
    let db = Arc::new(
        DatabaseBuilder::new(dir.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .create()
            .unwrap(),
    );
    MemoryEngine::open(db).unwrap()
}

/// 9 atoms, kinds alternating fact/event, created_at = 1000 * (i + 1).
fn seed(eng: &MemoryEngine, region: &str) -> Vec<i64> {
    (0..9)
        .map(|i| {
            let kind = if i % 2 == 0 { "fact" } else { "event" };
            eng.remember(
                region,
                AtomInput::new(kind, format!("atom number {i}")).with_created_at(1000 * (i + 1)),
            )
            .unwrap()
        })
        .collect()
}

#[test]
fn watermark_pages_are_complete_ordered_and_disjoint() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_region("notes", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let ids = seed(&eng, "notes");

    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let mut q = FetchQuery::new(4);
        q.after_id = after;
        let page = eng.fetch_range("notes", &q).unwrap();
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 4);
        seen.extend(page.iter().map(|h| h.id));
        after = Some(page.last().unwrap().id);
    }
    assert_eq!(seen, ids, "insertion order, nothing skipped or repeated");
}

#[test]
fn kind_is_optional() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_region("notes", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    seed(&eng, "notes");

    let all = eng.fetch_range("notes", &FetchQuery::new(100)).unwrap();
    assert_eq!(all.len(), 9, "no kind filter lists every kind");

    let facts = eng
        .fetch_range("notes", &FetchQuery::new(100).with_kind("fact"))
        .unwrap();
    assert_eq!(facts.len(), 5);
    assert!(facts.iter().all(|h| h.kind == "fact"));
}

#[test]
fn created_window_is_half_open() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_region("notes", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    seed(&eng, "notes"); // created_at: 1000..=9000 step 1000

    let hits = eng
        .fetch_range(
            "notes",
            &FetchQuery::new(100)
                .with_created_from(3000)
                .with_created_before(7000),
        )
        .unwrap();
    let times: Vec<i64> = hits.iter().map(|h| h.created_at).collect();
    assert_eq!(times, vec![3000, 4000, 5000, 6000], "[from, before)");
}

#[test]
fn window_watermark_and_payload_filter_compose() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_region("notes", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let mut tagged = Vec::new();
    for i in 0..6 {
        let payload = if i % 2 == 0 {
            json!({"tag": "keep"})
        } else {
            json!({"tag": "drop"})
        };
        let id = eng
            .remember(
                "notes",
                AtomInput::new("fact", format!("payload atom {i}"))
                    .with_created_at(1000 * (i + 1))
                    .with_payload(payload),
            )
            .unwrap();
        if i % 2 == 0 {
            tagged.push(id);
        }
    }

    let q = FetchQuery::new(100)
        .with_kind("fact")
        .with_payload_filter(json!({"tag": "keep"}))
        .with_after_id(tagged[0])
        .with_created_from(1000)
        .with_created_before(6000);
    let hits = eng.fetch_range("notes", &q).unwrap();
    let got: Vec<i64> = hits.iter().map(|h| h.id).collect();
    assert_eq!(got, vec![tagged[1], tagged[2]], "all predicates AND");
}

#[test]
fn sealed_region_watermark_and_window() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let ids = seed(&eng, "vault");

    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let mut q = FetchQuery::new(2);
        q.after_id = after;
        let page = eng.fetch_range("vault", &q).unwrap();
        if page.is_empty() {
            break;
        }
        seen.extend(page.iter().map(|h| h.id));
        after = Some(page.last().unwrap().id);
    }
    assert_eq!(seen, ids, "sealed pager honors the watermark");

    let hits = eng
        .fetch_range(
            "vault",
            &FetchQuery::new(100)
                .with_kind("event")
                .with_created_from(2000)
                .with_created_before(8000),
        )
        .unwrap();
    let times: Vec<i64> = hits.iter().map(|h| h.created_at).collect();
    assert_eq!(times, vec![2000, 4000, 6000], "sealed window + kind");
    assert!(hits.iter().all(|h| h.text.starts_with("atom number")));
}

#[test]
fn empty_results_and_zero_limit() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_region("notes", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let ids = seed(&eng, "notes");

    assert!(eng
        .fetch_range("notes", &FetchQuery::new(0))
        .unwrap()
        .is_empty());
    assert!(eng
        .fetch_range(
            "notes",
            &FetchQuery::new(10).with_after_id(*ids.last().unwrap())
        )
        .unwrap()
        .is_empty());
    assert!(eng
        .fetch_range("notes", &FetchQuery::new(10).with_created_from(50_000))
        .unwrap()
        .is_empty());
}

#[test]
fn fetch_wrapper_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_region("notes", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    seed(&eng, "notes");

    let hits = eng.fetch("notes", "event", None, 3).unwrap();
    assert_eq!(hits.len(), 3);
    assert!(hits.iter().all(|h| h.kind == "event"));
}
