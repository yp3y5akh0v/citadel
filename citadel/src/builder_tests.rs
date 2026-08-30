use super::*;

fn with_test_kdf(builder: DatabaseBuilder) -> DatabaseBuilder {
    #[cfg(not(feature = "fips"))]
    {
        builder.argon2_profile(Argon2Profile::Iot)
    }
    #[cfg(feature = "fips")]
    {
        builder
            .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
            .pbkdf2_iterations(600_000)
    }
}

/// Regression: the builder's passphrase copy must be wrapped in `Zeroizing` so
/// it is wiped when the builder drops. It is the only owned heap copy of a
/// passphrase in the workspace, and it outlives the whole Argon2 derivation.
#[test]
fn passphrase_is_held_in_a_zeroizing_owner() {
    let builder = DatabaseBuilder::new("unused.cdl").passphrase(b"correct horse");
    let held: &Option<Zeroizing<Vec<u8>>> = &builder.passphrase;
    assert_eq!(
        held.as_deref().map(Vec::as_slice),
        Some(&b"correct horse"[..])
    );
}

#[test]
fn passphrase_round_trips_through_create_and_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rt.cdl");

    let db = with_test_kdf(DatabaseBuilder::new(&path).passphrase(b"correct horse"))
        .create()
        .expect("create with a passphrase");
    drop(db);

    with_test_kdf(DatabaseBuilder::new(&path).passphrase(b"correct horse"))
        .open()
        .expect("reopen with the same passphrase");
}

#[test]
fn a_wrong_passphrase_still_fails_to_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wrong.cdl");

    let db = with_test_kdf(DatabaseBuilder::new(&path).passphrase(b"correct horse"))
        .create()
        .expect("create with a passphrase");
    drop(db);

    let opened = with_test_kdf(DatabaseBuilder::new(&path).passphrase(b"battery staple")).open();
    assert!(opened.is_err(), "a wrong passphrase must not open the file");
}
