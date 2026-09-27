use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(not(feature = "fips"))]
use citadel::Argon2Profile;
#[cfg(feature = "fips")]
use citadel::KdfAlgorithm;
use citadel::{Database, DatabaseBuilder};

const PASSPHRASE: &[u8] = b"inspection-surface-passphrase";

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

fn create_at(path: &Path) -> Database {
    builder(path).create().unwrap()
}

fn flip_file_byte(path: &Path, offset: u64) {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0x80;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn walking_both_commit_slots_does_not_invent_duplicate_pages() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_at(&dir.path().join("slots.citadel"));

    for i in 0..2u32 {
        let mut txn = db.begin_write().unwrap();
        for key in 0..64u32 {
            txn.insert(format!("k{i}-{key}").as_bytes(), b"v").unwrap();
        }
        txn.commit().unwrap();
    }

    let report = db.integrity_check_quiet().unwrap();

    assert!(report.is_ok(), "{:?}", report.errors);
    assert!(report.pages_checked > 0);
}

#[test]
fn an_unwritten_second_slot_is_not_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_at(&dir.path().join("one.citadel"));

    let report = db.integrity_check_quiet().unwrap();

    assert!(report.is_ok(), "{:?}", report.errors);
}

#[test]
fn reading_key_store_facts_does_not_create_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_at(&dir.path().join("plain.citadel"));

    let facts = db.key_store_facts().unwrap();

    assert_eq!(facts.region, None);
    assert_eq!(facts.atom, None);
    assert!(!db.region_store_path().exists());
    assert!(!db.atom_store_path().exists());
}

#[test]
fn key_store_facts_authenticate_without_repairing_torn_copies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("keys.citadel");
    let db = builder(&path).enable_region_keys(true).create().unwrap();
    let wrapped = db.wrap_region_key(&[7u8; citadel::core::KEY_SIZE]).unwrap();

    let (region_slot, region_generation) = db.region_store_allocate_write(41, &wrapped).unwrap();
    db.region_store_tombstone(region_slot, 41, region_generation)
        .unwrap();
    let (atom_slot, atom_generation) = db.atom_store_allocate_write(51, &wrapped).unwrap();
    db.atom_store_tombstone(atom_slot, 51, atom_generation)
        .unwrap();

    let region_path = db.region_store_path();
    let atom_path = db.atom_store_path();
    drop(db);

    let block = citadel::core::REGION_STORE_BLOCK as u64;
    let copy_a = (2 + 2 * u64::from(region_slot)) * block;
    let copy_b = copy_a + block;
    flip_file_byte(&region_path, copy_b);

    let db = builder(&path).enable_region_keys(true).open().unwrap();
    let region_before = std::fs::read(db.region_store_path()).unwrap();
    let atom_before = std::fs::read(db.atom_store_path()).unwrap();
    let facts = db.key_store_facts().unwrap();

    assert_eq!(facts.region.unwrap().tombstoned, 1);
    assert_eq!(facts.atom.unwrap().tombstoned, 1);
    assert_eq!(
        std::fs::read(db.region_store_path()).unwrap(),
        region_before
    );
    assert_eq!(std::fs::read(db.atom_store_path()).unwrap(), atom_before);

    flip_file_byte(&region_path, copy_a);
    let corrupt_before = std::fs::read(&region_path).unwrap();
    assert!(matches!(
        db.key_store_facts(),
        Err(citadel::Error::RegionStoreCorrupt(_))
    ));
    assert_eq!(std::fs::read(&region_path).unwrap(), corrupt_before);
    assert_eq!(std::fs::read(&atom_path).unwrap(), atom_before);
}

#[cfg(unix)]
#[test]
fn key_store_facts_refuse_a_symlink_instead_of_following_it() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("linked.citadel");
    let db = builder(&path).enable_region_keys(true).create().unwrap();
    let wrapped = db.wrap_region_key(&[9u8; citadel::core::KEY_SIZE]).unwrap();
    db.region_store_allocate_write(1, &wrapped).unwrap();

    let store = db.region_store_path();
    let moved = dir.path().join("moved-region-store");
    std::fs::rename(&store, &moved).unwrap();
    symlink(&moved, &store).unwrap();

    assert!(db.key_store_facts().is_err());
    assert!(std::fs::symlink_metadata(&store)
        .unwrap()
        .file_type()
        .is_symlink());
}

#[cfg(feature = "audit-log")]
mod audit {
    use super::*;
    use citadel::{AuditDetail, AuditEventType, CipherId, KdfAlgorithm};

    #[test]
    fn a_quiet_integrity_check_leaves_the_audit_log_alone() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_at(&dir.path().join("quiet.citadel"));

        let before = db.live_audit_entry_count().unwrap();
        let report = db.integrity_check_quiet().unwrap();
        let after = db.live_audit_entry_count().unwrap();

        assert!(report.is_ok(), "{report:?}");
        assert_eq!(after, before);
    }

    #[test]
    fn the_audited_integrity_check_still_records_that_it_ran() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_at(&dir.path().join("audited.citadel"));

        let before = db.live_audit_entry_count().unwrap();
        db.integrity_check().unwrap();

        assert_eq!(db.live_audit_entry_count().unwrap(), before + 1);
    }

    #[test]
    fn every_audit_detail_encoding_decodes() {
        assert_eq!(
            AuditDetail::decode(AuditEventType::DatabaseOpened, &[]),
            AuditDetail::Empty
        );
        assert_eq!(
            AuditDetail::decode(
                AuditEventType::DatabaseCreated,
                &[CipherId::Aes256Ctr as u8, KdfAlgorithm::Argon2id as u8]
            ),
            AuditDetail::Created {
                cipher: CipherId::Aes256Ctr,
                kdf: KdfAlgorithm::Argon2id,
                legacy_cipher_encoding: false,
            }
        );
        let legacy = AuditDetail::decode(
            AuditEventType::DatabaseCreated,
            &[1, KdfAlgorithm::Argon2id as u8],
        );
        assert_eq!(
            legacy,
            AuditDetail::Created {
                cipher: CipherId::Aes256Ctr,
                kdf: KdfAlgorithm::Argon2id,
                legacy_cipher_encoding: true,
            }
        );
        assert!(legacy.to_string().contains("effective AES-256-CTR"));

        let path = "/tmp/backup.citadel";
        let mut encoded = (path.len() as u16).to_le_bytes().to_vec();
        encoded.extend_from_slice(path.as_bytes());
        assert_eq!(
            AuditDetail::decode(AuditEventType::BackupCreated, &encoded),
            AuditDetail::Path(path.to_string())
        );
        assert_eq!(
            AuditDetail::decode(AuditEventType::IntegrityCheckPerformed, &7u32.to_le_bytes()),
            AuditDetail::IntegrityErrors(7)
        );
    }

    #[test]
    fn malformed_detail_stays_raw() {
        let cases: [(AuditEventType, &[u8]); 6] = [
            (AuditEventType::DatabaseOpened, &[1, 2, 3]),
            (AuditEventType::BackupCreated, &[9]),
            (AuditEventType::BackupCreated, &[99, 0, b'a']),
            (AuditEventType::KeyBackupExported, &[2, 0, 0xff, 0xfe]),
            (AuditEventType::IntegrityCheckPerformed, &[1, 2, 3]),
            (AuditEventType::DatabaseCreated, &[200, 0]),
        ];
        for (event, bytes) in cases {
            assert_eq!(
                AuditDetail::decode(event, bytes),
                AuditDetail::Raw(bytes.to_vec())
            );
        }
    }

    #[test]
    fn a_real_creation_event_decodes_to_its_cipher_and_kdf() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("decoded.citadel");
        let db = create_at(&path);
        let log = db.audit_log_path().unwrap();
        drop(db);

        let entries = citadel::read_audit_log(&log).unwrap();
        let created = entries
            .iter()
            .find(|entry| entry.event_type == AuditEventType::DatabaseCreated)
            .unwrap();

        assert!(matches!(
            AuditDetail::decode(created.event_type, &created.detail),
            AuditDetail::Created { .. }
        ));
    }

    #[test]
    fn audit_labels_and_details_are_safe_for_text_output() {
        let events = [
            AuditEventType::DatabaseCreated,
            AuditEventType::DatabaseOpened,
            AuditEventType::DatabaseClosed,
            AuditEventType::PassphraseChanged,
            AuditEventType::KeyBackupExported,
            AuditEventType::BackupCreated,
            AuditEventType::CompactionPerformed,
            AuditEventType::IntegrityCheckPerformed,
        ];
        for event in events {
            assert!(!event.as_str().is_empty());
        }

        assert_eq!(AuditDetail::Empty.to_string(), "");
        assert_eq!(AuditDetail::IntegrityErrors(1).to_string(), "1 error");
        assert_eq!(AuditDetail::IntegrityErrors(0).to_string(), "no errors");
        assert_eq!(
            AuditDetail::Path("line one\nline two".into()).to_string(),
            "\"line one\\nline two\""
        );
    }

    #[test]
    fn one_live_log_lists_and_verifies_as_a_chain_of_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chain.citadel");
        let db = create_at(&path);

        let paths = db.audit_log_paths().unwrap();
        assert_eq!(paths, vec![db.audit_log_path().unwrap()]);

        let verified = db.verify_audit_chain().unwrap();
        assert_eq!(verified.len(), 1);
        assert!(verified[0].1.chain_valid);
    }

    #[test]
    fn malformed_rotated_names_are_not_silently_omitted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("malformed.citadel");
        let db = create_at(&path);
        let mut malformed = db.audit_log_path().unwrap().into_os_string();
        malformed.push(".01");
        std::fs::write(std::path::PathBuf::from(malformed), b"not a segment").unwrap();

        assert!(db.audit_log_paths().is_err());
    }
}
