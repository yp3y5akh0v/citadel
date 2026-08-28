use std::io::{Seek, SeekFrom, Write};

use super::*;
use crate::builder::DatabaseBuilder;
#[cfg(not(feature = "fips"))]
use citadel_core::types::Argon2Profile;

const PASSPHRASE: &[u8] = b"inspect-passphrase";

fn builder(path: &Path) -> DatabaseBuilder {
    let builder = DatabaseBuilder::new(path).passphrase(PASSPHRASE);
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

fn create_at(path: &Path) {
    builder(path).create().unwrap();
}

fn present_key(status: &KeyFileStatus) -> (&KeyFileInfo, bool) {
    match status {
        KeyFileStatus::Present {
            info,
            file_id_matches,
        } => (info, *file_id_matches),
        other => panic!("expected a parsed key file, found {other:?}"),
    }
}

#[test]
fn a_vault_describes_its_header_without_a_passphrase() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("described.citadel");
    create_at(&path);

    let info = inspect_vault(&path).unwrap();

    assert_eq!(info.data_path, path);
    assert_eq!(info.key_path, default_key_path(&path));
    assert!(info.page_size > 0);
    assert!(info.active_slot_checksum_valid);
    assert!(!info.recovery_required);

    let (key, matches) = present_key(&info.key_file);
    assert!(matches);
    assert_eq!(key.cipher, CipherId::Aes256Ctr);
    #[cfg(not(feature = "fips"))]
    {
        assert_eq!(key.kdf, KdfAlgorithm::Argon2id);
        assert_eq!(key.kdf_m_cost, Argon2Profile::Iot.m_cost());
    }
    #[cfg(feature = "fips")]
    {
        assert_eq!(key.kdf, KdfAlgorithm::Pbkdf2HmacSha256);
        assert_eq!(key.kdf_m_cost, 600_000);
    }
    assert!(!key.rotation_active);
    assert!(key.slots_v1_required);
    #[cfg(feature = "audit-log")]
    assert!(key.audit_v2_required);
}

#[test]
fn a_foreign_key_file_is_reported_without_guessing_at_the_passphrase() {
    let dir = tempfile::tempdir().unwrap();
    let mine = dir.path().join("mine.citadel");
    let theirs = dir.path().join("theirs.citadel");
    create_at(&mine);
    create_at(&theirs);

    let info = inspect_vault_with_key(&mine, &default_key_path(&theirs)).unwrap();

    let (_, matches) = present_key(&info.key_file);
    assert!(!matches);
}

#[test]
fn a_missing_key_file_is_distinct_from_an_invalid_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("key-state.citadel");
    create_at(&path);
    let key_path = default_key_path(&path);
    let valid = std::fs::read(&key_path).unwrap();

    std::fs::remove_file(&key_path).unwrap();
    let missing = inspect_vault(&path).unwrap();
    assert_eq!(missing.key_file, KeyFileStatus::Missing);

    let mut invalid = valid;
    invalid[0] ^= 0xff;
    std::fs::write(&key_path, invalid).unwrap();
    assert!(matches!(
        inspect_vault(&path).unwrap().key_file,
        KeyFileStatus::Invalid(_)
    ));

    std::fs::write(&key_path, b"short").unwrap();
    assert!(matches!(
        inspect_vault(&path).unwrap().key_file,
        KeyFileStatus::Unreadable(_)
    ));
}

#[test]
fn the_reported_stats_match_a_healthy_opened_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stats.citadel");

    let db = builder(&path).create().unwrap();
    let mut txn = db.begin_write().unwrap();
    for i in 0..32u32 {
        txn.insert(format!("k{i}").as_bytes(), b"v").unwrap();
    }
    txn.commit().unwrap();
    let open_stats = db.stats();
    drop(db);

    let info = inspect_vault(&path).unwrap();

    assert!(info.active_slot_checksum_valid);
    assert_eq!(info.stats, open_stats);
}

#[test]
fn an_invalid_active_slot_is_reported_as_a_claim_not_opened_state() {
    use citadel_core::{COMMIT_SLOT_OFFSET, COMMIT_SLOT_SIZE, FILE_HEADER_SIZE, SLOT_TREE_ENTRIES};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("invalid-active.citadel");
    let db = builder(&path).create().unwrap();
    let mut txn = db.begin_write().unwrap();
    txn.insert(b"real", b"value").unwrap();
    txn.commit().unwrap();
    drop(db);

    let mut header_bytes = [0u8; FILE_HEADER_SIZE];
    header_bytes.copy_from_slice(&std::fs::read(&path).unwrap()[..FILE_HEADER_SIZE]);
    let header = FileHeader::deserialize(&header_bytes).unwrap();
    let offset = COMMIT_SLOT_OFFSET + header.active_slot() * COMMIT_SLOT_SIZE + SLOT_TREE_ENTRIES;
    let claimed = 9_999u64;
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(offset as u64)).unwrap();
    file.write_all(&claimed.to_le_bytes()).unwrap();
    file.sync_all().unwrap();

    let info = inspect_vault(&path).unwrap();
    assert!(!info.active_slot_checksum_valid);
    assert_eq!(info.stats.entry_count, claimed);

    let opened = builder(&path).open().unwrap();
    assert_ne!(opened.stats().entry_count, info.stats.entry_count);
}

#[test]
fn a_file_that_is_not_a_vault_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("not-a-vault.citadel");
    std::fs::write(&path, b"nowhere near long enough").unwrap();

    assert!(inspect_vault(&path).is_err());
}

#[cfg(any(unix, windows))]
#[test]
fn an_open_vault_is_not_inspected_through_a_racing_header_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("locked.citadel");
    let _db = builder(&path).create().unwrap();

    assert!(matches!(inspect_vault(&path), Err(Error::DatabaseLocked)));
}

#[cfg(any(unix, windows))]
#[test]
fn inspection_holds_the_data_lock_while_reading_the_key_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("held-lock.citadel");
    create_at(&path);
    let key_path = default_key_path(&path);

    let info = inspect_vault_with_key_reader(&path, &key_path, |key_path, file_id| {
        assert!(matches!(builder(&path).open(), Err(Error::DatabaseLocked)));
        inspect_key_file(key_path, file_id)
    })
    .unwrap();

    assert!(matches!(info.key_file, KeyFileStatus::Present { .. }));
    builder(&path).open().unwrap();
}

#[cfg(unix)]
#[test]
fn inspection_refuses_data_and_key_symlinks() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("target.citadel");
    create_at(&path);

    let data_link = dir.path().join("data-link.citadel");
    symlink(&path, &data_link).unwrap();
    assert!(inspect_vault_with_key(&data_link, &default_key_path(&path)).is_err());

    let key_link = dir.path().join("key-link");
    symlink(default_key_path(&path), &key_link).unwrap();
    let status = inspect_vault_with_key(&path, &key_link).unwrap().key_file;
    assert!(matches!(status, KeyFileStatus::Unreadable(_)));
}

#[test]
fn the_default_key_path_is_the_data_path_plus_the_key_suffix() {
    let p = Path::new("/vaults/agent.cdl");
    assert_eq!(
        default_key_path(p),
        PathBuf::from("/vaults/agent.cdl.citadel-keys")
    );
}
