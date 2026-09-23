use super::*;
use crate::parser::{TriggerEvent, TriggerGranularity, TriggerTiming};
use crate::types::TriggerDef;

fn sample_trigger(name: &str, target: &str) -> TriggerDef {
    TriggerDef {
        name: name.into(),
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Insert],
        target: target.into(),
        granularity: TriggerGranularity::ForEachRow,
        referencing: None,
        when_sql: None,
        body_sql: "BEGIN INSERT INTO audit VALUES (1); END".into(),
        enabled: true,
        created_at_micros: 1234567,
    }
}

#[test]
fn register_trigger_returns_in_target_lookup() {
    let mut s = SchemaManager::empty();
    s.register_trigger(sample_trigger("t1", "users"));
    let trigs = s.triggers_for("users");
    assert_eq!(trigs.len(), 1);
    assert_eq!(trigs[0].name, "t1");
}

#[test]
fn triggers_sorted_by_name_pg_faithful() {
    let mut s = SchemaManager::empty();
    s.register_trigger(sample_trigger("t_zebra", "users"));
    s.register_trigger(sample_trigger("t_apple", "users"));
    s.register_trigger(sample_trigger("t_mango", "users"));
    let trigs = s.triggers_for("users");
    assert_eq!(trigs[0].name, "t_apple");
    assert_eq!(trigs[1].name, "t_mango");
    assert_eq!(trigs[2].name, "t_zebra");
}

#[test]
fn remove_trigger_removes_from_bucket() {
    let mut s = SchemaManager::empty();
    s.register_trigger(sample_trigger("t1", "users"));
    s.register_trigger(sample_trigger("t2", "users"));
    let removed = s.remove_trigger("t1");
    assert!(removed.is_some());
    let trigs = s.triggers_for("users");
    assert_eq!(trigs.len(), 1);
    assert_eq!(trigs[0].name, "t2");
}

#[test]
fn remove_unknown_trigger_returns_none() {
    let mut s = SchemaManager::empty();
    s.register_trigger(sample_trigger("t1", "users"));
    assert!(s.remove_trigger("does_not_exist").is_none());
}

#[test]
fn set_trigger_enabled_toggles_flag() {
    let mut s = SchemaManager::empty();
    s.register_trigger(sample_trigger("t1", "users"));
    assert!(s.set_trigger_enabled("t1", false));
    assert!(!s.triggers_for("users")[0].enabled);
    assert!(s.set_trigger_enabled("t1", true));
    assert!(s.triggers_for("users")[0].enabled);
}

#[test]
fn set_all_triggers_enabled_flips_target_bucket() {
    let mut s = SchemaManager::empty();
    s.register_trigger(sample_trigger("a", "users"));
    s.register_trigger(sample_trigger("b", "users"));
    s.register_trigger(sample_trigger("c", "users"));
    let count = s.set_all_triggers_enabled("users", false);
    assert_eq!(count, 3);
    for t in s.triggers_for("users") {
        assert!(!t.enabled);
    }
}

#[test]
fn find_trigger_by_name_returns_target() {
    let mut s = SchemaManager::empty();
    s.register_trigger(sample_trigger("audit_users", "users"));
    s.register_trigger(sample_trigger("audit_orders", "orders"));
    let (target, t) = s.find_trigger("audit_orders").unwrap();
    assert_eq!(target, "orders");
    assert_eq!(t.name, "audit_orders");
}

#[test]
fn all_triggers_iterates_across_targets() {
    let mut s = SchemaManager::empty();
    s.register_trigger(sample_trigger("t1", "users"));
    s.register_trigger(sample_trigger("t2", "orders"));
    s.register_trigger(sample_trigger("t3", "users"));
    assert_eq!(s.all_triggers().count(), 3);
}

#[test]
fn generation_bumps_on_register_remove_and_toggle() {
    let mut s = SchemaManager::empty();
    let g0 = s.generation();
    s.register_trigger(sample_trigger("t", "x"));
    let g1 = s.generation();
    assert!(g1 > g0);
    s.set_trigger_enabled("t", false);
    let g2 = s.generation();
    assert!(g2 > g1);
    s.remove_trigger("t");
    let g3 = s.generation();
    assert!(g3 > g2);
}

#[test]
fn restore_snapshot_never_rewinds_generation() {
    let mut s = SchemaManager::empty();
    let snap = s.save_snapshot();
    s.register_trigger(sample_trigger("t1", "users"));
    let before_restore = s.generation();
    s.restore_snapshot(snap);
    assert!(s.generation() > before_restore);
}

#[test]
fn restore_snapshot_without_changes_keeps_generation() {
    // The counter must not move here, or every DML-only savepoint rollback
    // would recompile all prepared statements.
    let mut s = SchemaManager::empty();
    let snap = s.save_snapshot();
    let gen = s.generation();
    s.restore_snapshot(snap);
    assert_eq!(s.generation(), gen);
}

#[test]
fn bump_generation_past_is_strictly_greater() {
    let mut s = SchemaManager::empty();
    let base = s.generation();
    s.bump_generation_past(base + 100);
    assert!(s.generation() > base + 100);
    let cur = s.generation();
    s.bump_generation_past(0);
    assert_eq!(s.generation(), cur);
}

#[test]
fn own_commit_binding_keeps_exact_generation_and_external_ddl_is_admitted() {
    use crate::connection::Connection;
    use crate::types::Value;
    let db = citadel::DatabaseBuilder::new("")
        .passphrase(b"catalog-commit-binding")
        .argon2_profile(citadel::Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 7)").unwrap();
    let own_generation = db.manager().commit_generation();
    assert_eq!(
        conn.inner
            .borrow()
            .schema
            .catalog_binding
            .unwrap()
            .commit_generation,
        Some(own_generation)
    );
    let insert = conn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    let other = Connection::open(&db).unwrap();
    other
        .execute("CREATE UNIQUE INDEX unique_value ON t(value)")
        .unwrap();
    let external_generation = db.manager().commit_generation();
    assert!(external_generation > own_generation);
    // Publishing an older completed writer cannot claim the later commit.
    conn.inner
        .borrow_mut()
        .schema
        .bind_committed_catalog(db.manager().instance_id(), own_generation);
    assert!(insert
        .execute(&[Value::Integer(2), Value::Integer(7)])
        .is_err());
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM t").unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
    insert
        .execute(&[Value::Integer(2), Value::Integer(8)])
        .unwrap();
    assert_eq!(
        conn.inner
            .borrow()
            .schema
            .catalog_binding
            .unwrap()
            .commit_generation,
        Some(db.manager().commit_generation())
    );
}

#[test]
fn commit_binding_does_not_bless_unbound_local_definitions_or_other_owner() {
    let mut schema = SchemaManager::empty();
    schema.catalog_origin = Some(7);
    schema.catalog_stamps = Some([None; 4]);
    schema.catalog_binding = Some(CatalogBinding {
        commit_generation: Some(10),
        local_generation: schema.generation,
    });
    schema.bind_committed_catalog(8, 11);
    assert_eq!(schema.catalog_binding.unwrap().commit_generation, Some(10));
    schema.bump_generation_past(schema.generation);
    schema.bind_committed_catalog(7, 12);
    assert_eq!(schema.catalog_binding.unwrap().commit_generation, Some(10));
}
