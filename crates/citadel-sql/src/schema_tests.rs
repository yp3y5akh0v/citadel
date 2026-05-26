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
