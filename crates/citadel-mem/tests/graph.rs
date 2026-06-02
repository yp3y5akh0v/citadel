use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{
    AtomInput, EdgeKind, GraphExpand, MemError, MemoryEngine, MockEmbedder, RecallQuery,
};
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
fn link_creates_edge_and_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let a = eng.remember("r", AtomInput::new("fact", "alpha")).unwrap();
    let b = eng.remember("r", AtomInput::new("fact", "beta")).unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();
    // Re-linking updates weight rather than erroring on the PK.
    eng.link(a, b, EdgeKind::DerivedFrom, 0.5).unwrap();
}

#[test]
fn depends_on_cycle_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let a = eng.remember("r", AtomInput::new("task", "a")).unwrap();
    let b = eng.remember("r", AtomInput::new("task", "b")).unwrap();
    let c = eng.remember("r", AtomInput::new("task", "c")).unwrap();
    eng.link(a, b, EdgeKind::DependsOn, 1.0).unwrap();
    eng.link(b, c, EdgeKind::DependsOn, 1.0).unwrap();
    let err = eng.link(c, a, EdgeKind::DependsOn, 1.0).unwrap_err();
    assert!(matches!(err, MemError::Cycle { .. }), "got {err:?}");
}

#[test]
fn depends_on_self_loop_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let a = eng.remember("r", AtomInput::new("task", "a")).unwrap();
    let err = eng.link(a, a, EdgeKind::DependsOn, 1.0).unwrap_err();
    assert!(matches!(err, MemError::Cycle { .. }), "got {err:?}");
}

#[test]
fn non_dag_kinds_allow_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let a = eng.remember("r", AtomInput::new("fact", "a")).unwrap();
    let b = eng.remember("r", AtomInput::new("fact", "b")).unwrap();
    eng.link(a, b, EdgeKind::Causes, 1.0).unwrap();
    eng.link(b, a, EdgeKind::Causes, 1.0).unwrap();
}

#[test]
fn recall_graph_expand_returns_bounded_chain() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let a = eng
        .remember("r", AtomInput::new("fact", "alpha unique one"))
        .unwrap();
    let b = eng
        .remember("r", AtomInput::new("fact", "beta unique two"))
        .unwrap();
    let c = eng
        .remember("r", AtomInput::new("fact", "gamma unique three"))
        .unwrap();
    let d = eng
        .remember("r", AtomInput::new("fact", "delta unique four"))
        .unwrap();
    let e = eng
        .remember("r", AtomInput::new("fact", "epsilon unique five"))
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();
    eng.link(b, c, EdgeKind::DerivedFrom, 1.0).unwrap();
    eng.link(c, d, EdgeKind::DerivedFrom, 1.0).unwrap();
    eng.link(d, e, EdgeKind::DerivedFrom, 1.0).unwrap();

    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("alpha unique one", 1)
                .with_graph_expand(GraphExpand::new(3, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    let ids: Vec<i64> = hits.iter().map(|h| h.id).collect();

    assert!(ids.contains(&a), "seed a present");
    assert!(ids.contains(&b), "1 hop");
    assert!(ids.contains(&c), "2 hops");
    assert!(ids.contains(&d), "3 hops");
    assert!(!ids.contains(&e), "4 hops exceeds depth 3");
}

#[test]
fn graph_expand_respects_edge_kind_filter() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let a = eng
        .remember("r", AtomInput::new("fact", "alpha unique one"))
        .unwrap();
    let b = eng
        .remember("r", AtomInput::new("fact", "beta unique two"))
        .unwrap();
    eng.link(a, b, EdgeKind::Causes, 1.0).unwrap();

    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("alpha unique one", 1)
                .with_graph_expand(GraphExpand::new(3, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    let ids: Vec<i64> = hits.iter().map(|h| h.id).collect();
    assert!(ids.contains(&a));
    assert!(!ids.contains(&b), "causes edge must not be followed");
}
