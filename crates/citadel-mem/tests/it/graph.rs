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
    eng.link_in_region("r", a, b, EdgeKind::DerivedFrom, 1.0)
        .unwrap();
    // Re-linking updates weight rather than erroring on the PK.
    eng.link_in_region("r", a, b, EdgeKind::DerivedFrom, 0.5)
        .unwrap();
}

#[test]
fn depends_on_cycle_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let a = eng.remember("r", AtomInput::new("task", "a")).unwrap();
    let b = eng.remember("r", AtomInput::new("task", "b")).unwrap();
    let c = eng.remember("r", AtomInput::new("task", "c")).unwrap();
    eng.link_in_region("r", a, b, EdgeKind::DependsOn, 1.0)
        .unwrap();
    eng.link_in_region("r", b, c, EdgeKind::DependsOn, 1.0)
        .unwrap();
    let err = eng
        .link_in_region("r", c, a, EdgeKind::DependsOn, 1.0)
        .unwrap_err();
    assert!(matches!(err, MemError::Cycle { .. }), "got {err:?}");
}

#[test]
fn depends_on_self_loop_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let a = eng.remember("r", AtomInput::new("task", "a")).unwrap();
    let err = eng
        .link_in_region("r", a, a, EdgeKind::DependsOn, 1.0)
        .unwrap_err();
    assert!(matches!(err, MemError::Cycle { .. }), "got {err:?}");
}

#[test]
fn non_dag_kinds_allow_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let a = eng.remember("r", AtomInput::new("fact", "a")).unwrap();
    let b = eng.remember("r", AtomInput::new("fact", "b")).unwrap();
    eng.link_in_region("r", a, b, EdgeKind::Causes, 1.0)
        .unwrap();
    eng.link_in_region("r", b, a, EdgeKind::Causes, 1.0)
        .unwrap();
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
    eng.link_in_region("r", a, b, EdgeKind::DerivedFrom, 1.0)
        .unwrap();
    eng.link_in_region("r", b, c, EdgeKind::DerivedFrom, 1.0)
        .unwrap();
    eng.link_in_region("r", c, d, EdgeKind::DerivedFrom, 1.0)
        .unwrap();
    eng.link_in_region("r", d, e, EdgeKind::DerivedFrom, 1.0)
        .unwrap();

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
    eng.link_in_region("r", a, b, EdgeKind::Causes, 1.0)
        .unwrap();

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

#[test]
fn graph_expand_respects_atom_kind_filter() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let seed = eng
        .remember("r", AtomInput::new("fact", "alpha unique seed"))
        .unwrap();
    let allowed = eng
        .remember("r", AtomInput::new("fact", "allowed fact neighbor"))
        .unwrap();
    let blocked = eng
        .remember("r", AtomInput::new("audit", "blocked audit neighbor"))
        .unwrap();
    eng.link_in_region("r", seed, allowed, EdgeKind::DerivedFrom, 1.0)
        .unwrap();
    eng.link_in_region("r", seed, blocked, EdgeKind::DerivedFrom, 1.0)
        .unwrap();

    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("alpha unique seed", 1)
                .with_kinds(vec!["fact".to_string()])
                .with_graph_expand(GraphExpand::new(1, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    let ids: Vec<i64> = hits.iter().map(|h| h.id).collect();

    assert!(ids.contains(&seed), "seed fact present");
    assert!(ids.contains(&allowed), "allowed fact neighbor present");
    assert!(
        !ids.contains(&blocked),
        "graph-expanded audit atom must honor the query kind filter"
    );
}

/// A payload-filtered recall must stay filtered through graph expansion: an
/// expanded neighbor whose payload fails the filter is excluded, exactly like a
/// direct seed.
#[test]
fn graph_expansion_honours_payload_filter() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let seed = eng
        .remember(
            "r",
            AtomInput::new("fact", "public seed note")
                .with_payload(serde_json::json!({"visibility": "public"})),
        )
        .unwrap();
    let public_nb = eng
        .remember(
            "r",
            AtomInput::new("fact", "public neighbor")
                .with_payload(serde_json::json!({"visibility": "public"})),
        )
        .unwrap();
    let private_nb = eng
        .remember(
            "r",
            AtomInput::new("fact", "private neighbor")
                .with_payload(serde_json::json!({"visibility": "private"})),
        )
        .unwrap();
    eng.link_in_region("r", seed, public_nb, EdgeKind::DerivedFrom, 1.0)
        .unwrap();
    eng.link_in_region("r", seed, private_nb, EdgeKind::DerivedFrom, 1.0)
        .unwrap();

    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("public seed note", 1)
                .with_payload_filter(serde_json::json!({"visibility": "public"}))
                .with_graph_expand(GraphExpand::new(1, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    let ids: Vec<i64> = hits.iter().map(|h| h.id).collect();
    assert!(ids.contains(&seed), "filtered seed present");
    assert!(ids.contains(&public_nb), "matching neighbor expanded");
    assert!(
        !ids.contains(&private_nb),
        "expansion must not bypass the payload filter"
    );
}

/// Sealed graph expansion honours the payload filter exactly like plaintext.
#[test]
fn sealed_graph_expansion_honours_payload_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("edge.db"))
            .passphrase(b"pw")
            .enable_region_keys(true)
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("v", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    let seed = eng
        .remember(
            "v",
            AtomInput::new("fact", "public seed")
                .with_payload(serde_json::json!({"vis": "public"})),
        )
        .unwrap();
    let pub_nb = eng
        .remember(
            "v",
            AtomInput::new("fact", "public friend")
                .with_payload(serde_json::json!({"vis": "public"})),
        )
        .unwrap();
    let priv_nb = eng
        .remember(
            "v",
            AtomInput::new("fact", "private friend")
                .with_payload(serde_json::json!({"vis": "private"})),
        )
        .unwrap();
    eng.link_in_region("v", seed, pub_nb, EdgeKind::DerivedFrom, 1.0)
        .unwrap();
    eng.link_in_region("v", seed, priv_nb, EdgeKind::DerivedFrom, 1.0)
        .unwrap();

    let ids: Vec<i64> = eng
        .recall(
            "v",
            RecallQuery::by_text("public seed", 1)
                .with_payload_filter(serde_json::json!({"vis": "public"}))
                .with_graph_expand(GraphExpand::new(1, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert!(ids.contains(&pub_nb));
    assert!(!ids.contains(&priv_nb), "sealed expansion filters payloads");
}
