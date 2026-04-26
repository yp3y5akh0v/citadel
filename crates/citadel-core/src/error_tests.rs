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
