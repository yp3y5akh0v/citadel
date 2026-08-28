use super::*;

#[test]
fn error_display() {
    let e = Error::PageTampered(PageId(42));
    assert!(format!("{e}").contains("page:42"));

    let e = Error::TransactionTooLarge { capacity: 256 };
    assert!(format!("{e}").contains("256"));
}

#[test]
fn error_from_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let e: Error = io_err.into();
    assert!(matches!(e, Error::Io(_)));
}

#[test]
fn post_operation_audit_error_preserves_the_outcome_and_source() {
    let error = Error::AuditFailureAfterOperation {
        operation: "passphrase change",
        source: Box::new(Error::Io(std::io::Error::other("disk full"))),
    };

    assert_eq!(
        error.to_string(),
        "passphrase change completed, but audit logging failed: I/O error: disk full"
    );
}

#[test]
fn post_operation_durability_error_preserves_the_outcome_and_source() {
    let error = Error::DurabilityFailureAfterOperation {
        operation: "passphrase change",
        source: std::io::Error::other("directory sync failed"),
    };

    assert_eq!(
        error.to_string(),
        "passphrase change completed, but its directory entry could not be confirmed durable: directory sync failed"
    );
}

#[test]
fn combined_post_operation_error_preserves_both_failures() {
    let error = Error::DurabilityAndAuditFailureAfterOperation {
        operation: "passphrase change",
        durability: std::io::Error::other("directory sync failed"),
        audit: Box::new(Error::Io(std::io::Error::other("audit disk full"))),
    };

    assert_eq!(
        error.to_string(),
        "passphrase change completed, but its directory entry could not be confirmed durable: directory sync failed; audit logging also failed: I/O error: audit disk full"
    );
}

#[test]
fn named_table_hash_collision_keeps_both_names_and_hash() {
    let error = Error::NamedTableHashCollision {
        requested: "collision_table_134778".into(),
        existing: "collision_table_51661".into(),
        hash: 0xab88_afb6,
    };

    assert_eq!(
        error.to_string(),
        "table name \"collision_table_134778\" collides with existing table \"collision_table_51661\" in commit-slot hash 0xab88afb6"
    );
}

#[test]
fn failed_transaction_tells_the_caller_to_roll_back() {
    assert_eq!(
        Error::TransactionFailed.to_string(),
        "write transaction cannot be committed because an earlier mutation failed; roll it back"
    );
}

#[test]
fn region_in_use_keeps_the_region_id() {
    assert_eq!(
        Error::RegionInUse { region_id: 42 }.to_string(),
        "memory region 42 is in use by another operation"
    );
}

#[test]
fn atom_in_use_keeps_the_atom_id() {
    assert_eq!(
        Error::AtomInUse { atom_id: 73 }.to_string(),
        "memory atom 73 is in use by an external callback"
    );
}
