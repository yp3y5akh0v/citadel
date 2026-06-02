use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, EdgeKind, MemoryEngine, MockEmbedder};
use serde_json::json;
use std::sync::Arc;

fn engine(dir: &std::path::Path) -> MemoryEngine {
    let db = Arc::new(
        DatabaseBuilder::new(dir.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    eng
}

#[test]
fn fetch_returns_atoms_by_kind_without_embedding() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.remember("r", AtomInput::new("task", "first task"))
        .unwrap();
    eng.remember("r", AtomInput::new("task", "second task"))
        .unwrap();
    eng.remember("r", AtomInput::new("fact", "a fact")).unwrap();

    let tasks = eng.fetch("r", "task", None, 100).unwrap();
    assert_eq!(tasks.len(), 2);
    assert!(tasks.iter().all(|h| h.kind == "task"));

    let facts = eng.fetch("r", "fact", None, 100).unwrap();
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].text, "a fact");

    assert!(eng.fetch("r", "task", None, 0).unwrap().is_empty());
    assert!(eng.fetch("r", "nope", None, 100).unwrap().is_empty());
}

#[test]
fn fetch_one_reads_by_id_and_respects_region() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    // Same (dim, metric) as "r" -> shared atoms table, separated only by region_id.
    eng.create_region("other", Arc::new(MockEmbedder::new(64)))
        .unwrap();

    let id = eng
        .remember(
            "r",
            AtomInput::new("task", "solo").with_payload(json!({"status": "pending"})),
        )
        .unwrap();

    let hit = eng.fetch_one("r", id).unwrap().expect("atom present");
    assert_eq!(hit.id, id);
    assert_eq!(hit.kind, "task");
    assert_eq!(hit.text, "solo");
    assert_eq!(hit.payload["status"], "pending");

    assert!(eng.fetch_one("r", 999_999).unwrap().is_none(), "missing id");
    assert!(
        eng.fetch_one("other", id).unwrap().is_none(),
        "atom belongs to 'r', not 'other'"
    );
}

#[test]
fn fetch_filters_by_payload_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.remember(
        "r",
        AtomInput::new("task", "t1").with_payload(json!({"status": "pending"})),
    )
    .unwrap();
    eng.remember(
        "r",
        AtomInput::new("task", "t2").with_payload(json!({"status": "pending"})),
    )
    .unwrap();
    eng.remember(
        "r",
        AtomInput::new("task", "t3").with_payload(json!({"status": "done"})),
    )
    .unwrap();

    let pending = eng
        .fetch("r", "task", Some(&json!({"status": "pending"})), 100)
        .unwrap();
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().all(|h| h.payload["status"] == "pending"));

    let one = eng
        .fetch("r", "task", Some(&json!({"status": "pending"})), 1)
        .unwrap();
    assert_eq!(one.len(), 1, "limit is honored");
}

#[test]
fn fetch_edges_filters_by_src_dst_and_kind() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let a = eng.remember("r", AtomInput::new("task", "alpha")).unwrap();
    let b = eng.remember("r", AtomInput::new("task", "beta")).unwrap();
    let c = eng.remember("r", AtomInput::new("task", "gamma")).unwrap();
    eng.link(a, b, EdgeKind::DependsOn, 1.0).unwrap();
    eng.link(a, c, EdgeKind::Causes, 0.5).unwrap();
    eng.link(b, c, EdgeKind::DependsOn, 1.0).unwrap();

    let from_a = eng.fetch_edges(Some(a), None, None).unwrap();
    assert_eq!(from_a.len(), 2, "a -> b, a -> c");

    let a_depends = eng
        .fetch_edges(Some(a), None, Some(EdgeKind::DependsOn))
        .unwrap();
    assert_eq!(a_depends.len(), 1);
    assert_eq!(a_depends[0].dst_id, b);
    assert_eq!(a_depends[0].kind, EdgeKind::DependsOn);
    assert!((a_depends[0].weight - 1.0).abs() < 1e-6);

    let into_c = eng.fetch_edges(None, Some(c), None).unwrap();
    assert_eq!(into_c.len(), 2, "a -> c, b -> c");

    let all_depends = eng
        .fetch_edges(None, None, Some(EdgeKind::DependsOn))
        .unwrap();
    assert_eq!(all_depends.len(), 2, "a -> b, b -> c");
}

#[test]
fn update_atom_payload_transitions_status() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let id = eng
        .remember(
            "r",
            AtomInput::new("task", "do the thing").with_payload(json!({"status": "pending"})),
        )
        .unwrap();

    eng.update_atom_payload("r", id, &json!({"status": "done", "attempts": 1}))
        .unwrap();

    let done = eng
        .fetch("r", "task", Some(&json!({"status": "done"})), 10)
        .unwrap();
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].id, id);
    assert_eq!(done[0].payload["attempts"], 1);
    assert!(
        eng.fetch("r", "task", Some(&json!({"status": "pending"})), 10)
            .unwrap()
            .is_empty(),
        "old status no longer matches"
    );

    assert!(
        eng.update_atom_payload("r", 999_999, &json!({"status": "x"}))
            .is_err(),
        "updating a missing atom errors"
    );
}

#[test]
fn update_atom_payload_rejects_immutable() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());

    let editable = eng
        .remember(
            "r",
            AtomInput::new("task", "editable").with_payload(json!({"v": 1})),
        )
        .unwrap();
    let locked = eng
        .remember(
            "r",
            AtomInput::new("self_model", "frozen")
                .with_payload(json!({"v": 1}))
                .immutable(),
        )
        .unwrap();

    eng.update_atom_payload("r", editable, &json!({"v": 2}))
        .unwrap();
    let m = eng.fetch_one("r", editable).unwrap().unwrap();
    assert_eq!(m.payload["v"], 2);
    assert!(!m.immutable);

    assert!(
        eng.update_atom_payload("r", locked, &json!({"v": 99}))
            .is_err(),
        "immutable atom must reject update"
    );
    let l = eng.fetch_one("r", locked).unwrap().unwrap();
    assert_eq!(l.payload["v"], 1, "payload unchanged");
    assert!(l.immutable);
}
