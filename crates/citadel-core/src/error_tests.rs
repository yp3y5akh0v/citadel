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
