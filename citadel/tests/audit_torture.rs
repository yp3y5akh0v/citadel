use std::path::Path;

use citadel::{
    AuditConfig, Database, DatabaseBuilder, KdfAlgorithm,
    read_audit_log, verify_audit_log, scan_corrupted_audit_log,
};

fn create_test_db(dir: &Path, passphrase: &[u8]) -> Database {
    DatabaseBuilder::new(dir.join("test.citadel"))
        .passphrase(passphrase)
        .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
        .pbkdf2_iterations(600_000)
        .cache_size(64)
        .create()
        .unwrap()
}

fn audit_path(dir: &Path) -> std::path::PathBuf {
    dir.join("test.citadel.citadel-audit")
}

fn get_audit_key(dir: &Path, passphrase: &[u8]) -> [u8; 32] {
    use citadel::crypto::key_manager::open_key_file;
    use citadel::core::KEY_FILE_SIZE;

    let key_path = dir.join("test.citadel.citadel-keys");
    let key_data = std::fs::read(&key_path).unwrap();
    let key_buf: [u8; KEY_FILE_SIZE] = key_data.try_into().unwrap();
    let (_kf, keys) = open_key_file(&key_buf, passphrase, 0).unwrap_or_else(|_| {
        let data_path = dir.join("test.citadel");
        let mut file = std::fs::File::open(&data_path).unwrap();
        use std::io::{Read, Seek, SeekFrom};
        let mut header_buf = [0u8; citadel::core::FILE_HEADER_SIZE];
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(&mut header_buf).unwrap();
        let header = citadel::io::file_manager::FileHeader::deserialize(&header_buf).unwrap();
        open_key_file(&key_buf, passphrase, header.file_id).unwrap()
    });
    keys.audit_key
}

/// Returns (offset, entry_len) for each on-disk entry.
fn walk_entry_offsets(data: &[u8]) -> Vec<(usize, usize)> {
    let magic_bytes = 0x454E_5452u32.to_le_bytes();
    let mut result = Vec::new();
    let mut offset = 64usize; // past header
    while offset + 8 <= data.len() {
        if data[offset..offset + 4] != magic_bytes {
            break;
        }
        let entry_len = u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap()) as usize;
        if entry_len < 56 || offset + 4 + entry_len > data.len() {
            break;
        }
        result.push((offset, entry_len));
        offset += 4 + entry_len;
    }
    result
}

/// Returns (entry count, audit key).
fn generate_entries(dir: &Path, pass: &[u8], integrity_checks: usize) -> (usize, [u8; 32]) {
    let db = create_test_db(dir, pass);
    for _ in 0..integrity_checks {
        db.integrity_check().unwrap();
    }
    drop(db);

    let entries = read_audit_log(&audit_path(dir)).unwrap();
    let key = get_audit_key(dir, pass);
    (entries.len(), key)
}

// ============================================================================
// Corruption at every entry position
// ============================================================================

#[test]
fn torture_corrupt_each_entry_individually() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 8);
    assert!(total >= 10);

    let ap = audit_path(dir.path());
    let original = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&original);
    assert_eq!(offsets.len(), total);

    for corrupt_idx in 0..total {
        let mut data = original.clone();
        let (off, _elen) = offsets[corrupt_idx];
        data[off] = 0;
        data[off + 1] = 0;
        data[off + 2] = 0;
        data[off + 3] = 0;
        std::fs::write(&ap, &data).unwrap();

        let scan = scan_corrupted_audit_log(&ap).unwrap();
        assert!(
            scan.entries.len() >= total - 1,
            "corrupting entry {} should still recover {} entries, got {}",
            corrupt_idx, total - 1, scan.entries.len()
        );
        assert!(!scan.corruption_offsets.is_empty(),
            "corrupting entry {} should report corruption", corrupt_idx);

        let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();
        let corrupted_seq = (corrupt_idx + 1) as u64;
        assert!(
            !seq_nos.contains(&corrupted_seq),
            "corrupted entry {} (seq {}) should NOT be in recovered set",
            corrupt_idx, corrupted_seq
        );

        for seq in 1..=total as u64 {
            if seq == corrupted_seq {
                continue;
            }
            assert!(
                seq_nos.contains(&seq),
                "entry seq {} should be recovered when entry {} is corrupted",
                seq, corrupt_idx
            );
        }
    }
}

#[test]
fn torture_corrupt_entry_len_each_position() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 6);
    assert!(total >= 8);

    let ap = audit_path(dir.path());
    let original = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&original);

    for corrupt_idx in 0..total {
        let mut data = original.clone();
        let (off, _) = offsets[corrupt_idx];
        data[off + 4] = 0xFF;
        data[off + 5] = 0xFF;
        data[off + 6] = 0xFF;
        data[off + 7] = 0x7F;
        std::fs::write(&ap, &data).unwrap();

        let scan = scan_corrupted_audit_log(&ap).unwrap();
        assert!(
            !scan.corruption_offsets.is_empty(),
            "corrupt entry_len at idx {} must trigger corruption detection",
            corrupt_idx
        );

        let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();
        for seq in 1..=(corrupt_idx as u64) {
            assert!(seq_nos.contains(&seq),
                "entry {} before corruption at {} should be recovered", seq, corrupt_idx);
        }

        for seq in (corrupt_idx + 2) as u64..=total as u64 {
            assert!(seq_nos.contains(&seq),
                "entry {} after corruption at {} should be recovered via sentinel", seq, corrupt_idx);
        }
    }
}

// ============================================================================
// Multiple simultaneous corruptions
// ============================================================================

#[test]
fn torture_multiple_corrupted_entries() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 8);
    assert!(total >= 10);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    let corrupt_indices = [1, 4, 7];
    for &idx in &corrupt_indices {
        let (off, _) = offsets[idx];
        data[off] = 0;
        data[off + 1] = 0;
        data[off + 2] = 0;
        data[off + 3] = 0;
    }
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();

    for &idx in &corrupt_indices {
        let seq = (idx + 1) as u64;
        assert!(!seq_nos.contains(&seq), "corrupted seq {} should be missing", seq);
    }

    let expected_count = total - corrupt_indices.len();
    assert_eq!(scan.entries.len(), expected_count,
        "should recover {} of {} entries", expected_count, total);
}

#[test]
fn torture_consecutive_corrupted_entries() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 8);
    assert!(total >= 10);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    for idx in 2..=4 {
        let (off, _) = offsets[idx];
        data[off] = 0;
        data[off + 1] = 0;
        data[off + 2] = 0;
        data[off + 3] = 0;
    }
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();

    // Entries 1, 2 (before gap) should be present
    assert!(seq_nos.contains(&1));
    assert!(seq_nos.contains(&2));

    // Entries 3, 4, 5 (corrupted) should be missing
    assert!(!seq_nos.contains(&3));
    assert!(!seq_nos.contains(&4));
    assert!(!seq_nos.contains(&5));

    // Entries 6+ (after gap) should be recovered
    for seq in 6..=total as u64 {
        assert!(seq_nos.contains(&seq),
            "entry {} after consecutive corruption gap should be recovered", seq);
    }
}

// ============================================================================
// False positive sentinel rejection
// ============================================================================

#[test]
fn torture_false_positive_magic_in_detail_data() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    // Use backup to generate entries with path detail containing arbitrary bytes
    let backup_path = dir.path().join("ENTR-test.citadel");
    db.backup(&backup_path).unwrap();
    db.integrity_check().unwrap();
    drop(db);

    let ap = audit_path(dir.path());
    let entries = read_audit_log(&ap).unwrap();
    let total = entries.len();

    let key = get_audit_key(dir.path(), pass);
    let result = verify_audit_log(&ap, &key).unwrap();
    assert!(result.chain_valid);

    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);
    let (off, elen) = offsets[0];
    let hmac_start = off + 4 + elen - 32;
    data[hmac_start] = 0x52;
    data[hmac_start + 1] = 0x54;
    data[hmac_start + 2] = 0x4E;
    data[hmac_start + 3] = 0x45;
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert_eq!(scan.entries.len(), total);

    let result = verify_audit_log(&ap, &key).unwrap();
    assert!(!result.chain_valid);
}

// ============================================================================
// Boundary corruption patterns
// ============================================================================

#[test]
fn torture_hmac_corruption_structural_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, key) = generate_entries(dir.path(), pass, 5);
    assert!(total >= 7);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    // Corrupt the first byte of every entry's HMAC
    for &(off, elen) in &offsets {
        let hmac_start = off + 4 + elen - 32;
        data[hmac_start] ^= 0xFF;
    }
    std::fs::write(&ap, &data).unwrap();

    // Scan recovers all entries structurally
    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert_eq!(scan.entries.len(), total,
        "HMAC corruption should not affect structural recovery");
    assert!(scan.corruption_offsets.is_empty(),
        "HMAC-only corruption is not a structural corruption");

    // But chain verification detects the tamper
    let result = verify_audit_log(&ap, &key).unwrap();
    assert!(!result.chain_valid);
    assert_eq!(result.chain_break_at, Some(1)); // first entry HMAC bad
}

#[test]
fn torture_invalid_event_type_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 5);
    assert!(total >= 7);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    // Corrupt entry 3's event_type to 0xFF (invalid)
    let (off, _) = offsets[2];
    // event_type at: magic(4) + entry_len(4) + timestamp(8) + seq_no(8) = offset + 24
    data[off + 24] = 0xFF;
    data[off + 25] = 0xFF;
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();

    // Entry 3 should be skipped (invalid event_type)
    assert!(!seq_nos.contains(&3), "entry with invalid event_type should be skipped");
    // All others recovered
    assert_eq!(scan.entries.len(), total - 1);
}

#[test]
fn torture_invalid_detail_len_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 5);
    assert!(total >= 7);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    // Corrupt entry 4's detail_len to a huge value
    let (off, _) = offsets[3];
    // detail_len at: magic(4) + entry_len(4) + timestamp(8) + seq_no(8) + event_type(2) = offset + 26
    data[off + 26] = 0xFF;
    data[off + 27] = 0xFF;
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();

    // Entry 4 should be skipped
    assert!(!seq_nos.contains(&4), "entry with invalid detail_len should be skipped");
    // Entries before and after recovered
    assert!(seq_nos.contains(&3));
    assert!(seq_nos.contains(&5));
}

// ============================================================================
// Large-scale sentinel scanning
// ============================================================================

#[test]
fn torture_large_scale_scattered_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let config = AuditConfig {
        enabled: true,
        max_file_size: 10 * 1024 * 1024,
        max_rotated_files: 3,
    };

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(pass)
        .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
        .pbkdf2_iterations(600_000)
        .cache_size(64)
        .audit_config(config)
        .create()
        .unwrap();

    for _ in 0..20 {
        db.integrity_check().unwrap();
    }
    drop(db);

    let ap = audit_path(dir.path());
    let entries = read_audit_log(&ap).unwrap();
    let total = entries.len();
    assert!(total >= 22);

    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    let corrupt_indices: Vec<usize> = (3..total).step_by(4).collect();
    for &idx in &corrupt_indices {
        let (off, _) = offsets[idx];
        data[off] = 0;
        data[off + 1] = 0;
        data[off + 2] = 0;
        data[off + 3] = 0;
    }
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    let expected_recovered = total - corrupt_indices.len();
    assert_eq!(scan.entries.len(), expected_recovered);
    assert_eq!(scan.corruption_offsets.len(), corrupt_indices.len());

    let recovered: std::collections::HashSet<u64> =
        scan.entries.iter().map(|e| e.sequence_no).collect();
    for seq in 1..=total as u64 {
        let idx = (seq - 1) as usize;
        if corrupt_indices.contains(&idx) {
            assert!(!recovered.contains(&seq), "corrupted seq {} should not be recovered", seq);
        } else {
            assert!(recovered.contains(&seq), "seq {} should be recovered", seq);
        }
    }
}

// ============================================================================
// Zeroed regions
// ============================================================================

#[test]
fn torture_zeroed_byte_block_spanning_entries() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 8);
    assert!(total >= 10);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    // Zero out bytes covering entries 3, 4, 5 (0-based indices 2, 3, 4)
    let zero_start = offsets[2].0;
    let zero_end = offsets[4].0 + 4 + offsets[4].1; // end of entry 5
    for b in &mut data[zero_start..zero_end] {
        *b = 0;
    }
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();

    // Entries 1, 2 before the zeroed block
    assert!(seq_nos.contains(&1));
    assert!(seq_nos.contains(&2));
    // Entries 3, 4, 5 zeroed
    assert!(!seq_nos.contains(&3));
    assert!(!seq_nos.contains(&4));
    assert!(!seq_nos.contains(&5));
    // Entries 6+ after the zeroed block
    for seq in 6..=total as u64 {
        assert!(seq_nos.contains(&seq), "entry {} past zeroed block should be recovered", seq);
    }
}

#[test]
fn torture_all_entries_zeroed() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 3);
    assert!(total >= 5);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    // Zero everything after header
    for b in &mut data[64..] {
        *b = 0;
    }
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert_eq!(scan.entries.len(), 0);
    assert!(!scan.corruption_offsets.is_empty());
}

// ============================================================================
// Random byte corruption
// ============================================================================

#[test]
fn torture_random_byte_overwrites_no_panic() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 10);
    assert!(total >= 12);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();

    let seed: u64 = 0xDEADBEEF;
    let mut rng = seed;
    for _ in 0..50 {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let pos = 64 + (rng as usize % (data.len() - 64));
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        data[pos] = (rng & 0xFF) as u8;
    }
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert!(scan.entries.len() <= total);
    let strict = read_audit_log(&ap).unwrap();
    assert!(strict.len() <= total);
}

// ============================================================================
// Truncation patterns
// ============================================================================

#[test]
fn torture_truncation_at_every_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, key) = generate_entries(dir.path(), pass, 5);
    assert!(total >= 7);

    let ap = audit_path(dir.path());
    let original = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&original);

    for i in 0..offsets.len() {
        let trunc_at = offsets[i].0;
        std::fs::write(&ap, &original[..trunc_at]).unwrap();

        let entries = read_audit_log(&ap).unwrap();
        assert_eq!(entries.len(), i,
            "truncating at entry {} boundary should yield {} entries", i, i);

        if i > 0 {
            let result = verify_audit_log(&ap, &key).unwrap();
            assert!(result.chain_valid,
                "chain of {} entries before truncation at {} should be valid", i, i);
        }

        let scan = scan_corrupted_audit_log(&ap).unwrap();
        assert_eq!(scan.entries.len(), i);
    }

    for i in 0..offsets.len() {
        let (off, elen) = offsets[i];
        let mid = off + 4 + elen / 2;
        if mid >= original.len() {
            continue;
        }
        std::fs::write(&ap, &original[..mid]).unwrap();

        let entries = read_audit_log(&ap).unwrap();
        assert_eq!(entries.len(), i,
            "truncating mid-entry {} should yield {} complete entries", i, i);
    }
}

// ============================================================================
// Scan correctness vs read_audit_log on healthy file
// ============================================================================

#[test]
fn torture_scan_matches_read_on_healthy_file() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 10);
    assert!(total >= 12);

    let ap = audit_path(dir.path());

    let strict = read_audit_log(&ap).unwrap();
    let scan = scan_corrupted_audit_log(&ap).unwrap();

    assert_eq!(strict.len(), scan.entries.len());
    assert!(scan.corruption_offsets.is_empty());
    for (s, r) in scan.entries.iter().zip(strict.iter()) {
        assert_eq!(s.sequence_no, r.sequence_no);
        assert_eq!(s.event_type, r.event_type);
        assert_eq!(s.timestamp, r.timestamp);
        assert_eq!(s.detail, r.detail);
        assert_eq!(s.hmac, r.hmac);
    }
}

// ============================================================================
// Chain integrity across corruption patterns
// ============================================================================

#[test]
fn torture_chain_break_point_matches_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, key) = generate_entries(dir.path(), pass, 8);
    assert!(total >= 10);

    let ap = audit_path(dir.path());

    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);
    let (off, _) = offsets[4];
    data[off + 8] ^= 0xFF;
    std::fs::write(&ap, &data).unwrap();

    let strict = read_audit_log(&ap).unwrap();
    assert_eq!(strict.len(), total);

    let result = verify_audit_log(&ap, &key).unwrap();
    assert!(!result.chain_valid);
    assert_eq!(result.chain_break_at, Some(5));
    assert_eq!(result.entries_verified, 4);
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
fn torture_header_only_file() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let _ = generate_entries(dir.path(), pass, 0);

    let ap = audit_path(dir.path());
    let data = std::fs::read(&ap).unwrap();
    std::fs::write(&ap, &data[..64]).unwrap();

    let entries = read_audit_log(&ap).unwrap();
    assert_eq!(entries.len(), 0);

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert_eq!(scan.entries.len(), 0);
    assert!(scan.corruption_offsets.is_empty());
}

#[test]
fn torture_header_plus_partial_magic() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let _ = generate_entries(dir.path(), pass, 0);

    let ap = audit_path(dir.path());
    let data = std::fs::read(&ap).unwrap();
    let mut truncated = data[..64].to_vec();
    truncated.extend_from_slice(&[0x52, 0x54, 0x4E]);
    std::fs::write(&ap, &truncated).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert_eq!(scan.entries.len(), 0);
}

#[test]
fn torture_corrupted_file_header() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let _ = generate_entries(dir.path(), pass, 2);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    data[0] = 0xFF;
    std::fs::write(&ap, &data).unwrap();

    let result = read_audit_log(&ap);
    assert!(result.is_err());

    let scan_result = scan_corrupted_audit_log(&ap);
    assert!(scan_result.is_err());
}

#[test]
fn torture_single_entry_corrupted() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let config = AuditConfig {
        enabled: true,
        max_file_size: 10 * 1024 * 1024,
        max_rotated_files: 3,
    };
    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(pass)
        .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
        .pbkdf2_iterations(600_000)
        .cache_size(64)
        .audit_config(config)
        .create()
        .unwrap();
    drop(db);

    let ap = audit_path(dir.path());
    let entries = read_audit_log(&ap).unwrap();
    assert!(entries.len() >= 2);

    let mut data = std::fs::read(&ap).unwrap();
    data[64] = 0;
    data[65] = 0;
    data[66] = 0;
    data[67] = 0;
    std::fs::write(&ap, &data).unwrap();

    let strict = read_audit_log(&ap).unwrap();
    assert_eq!(strict.len(), 0);

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert!(!scan.corruption_offsets.is_empty());
    assert!(scan.entries.len() >= 1);
}

// ============================================================================
// Sequence number and ordering after recovery
// ============================================================================

#[test]
fn torture_recovered_entries_monotonic_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 8);
    assert!(total >= 10);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    // Corrupt entries 3, 6 (0-based 2, 5)
    for &idx in &[2usize, 5] {
        let (off, _) = offsets[idx];
        data[off] = 0;
        data[off + 1] = 0;
        data[off + 2] = 0;
        data[off + 3] = 0;
    }
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    for i in 1..scan.entries.len() {
        assert!(
            scan.entries[i].sequence_no > scan.entries[i - 1].sequence_no,
            "recovered entries must have strictly increasing sequence numbers: {} vs {}",
            scan.entries[i - 1].sequence_no, scan.entries[i].sequence_no
        );
    }

    for i in 1..scan.entries.len() {
        assert!(
            scan.entries[i].timestamp >= scan.entries[i - 1].timestamp,
            "recovered entries must have non-decreasing timestamps"
        );
    }
}

// ============================================================================
// Rotation + corruption interaction
// ============================================================================

#[test]
fn torture_rotation_then_corrupt_rotated() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let config = AuditConfig {
        enabled: true,
        max_file_size: 200,
        max_rotated_files: 2,
    };

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(pass)
        .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
        .pbkdf2_iterations(600_000)
        .cache_size(64)
        .audit_config(config)
        .create()
        .unwrap();

    for _ in 0..15 {
        db.integrity_check().unwrap();
    }
    drop(db);

    let rotated_1 = dir.path().join("test.citadel.citadel-audit.1");
    assert!(rotated_1.exists());

    let mut rot_data = std::fs::read(&rotated_1).unwrap();
    for b in &mut rot_data[64..] {
        *b = 0xFF;
    }
    std::fs::write(&rotated_1, &rot_data).unwrap();

    let ap = audit_path(dir.path());
    let entries = read_audit_log(&ap).unwrap();
    assert!(entries.len() > 0);

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert_eq!(scan.entries.len(), entries.len());
    assert!(scan.corruption_offsets.is_empty());
}

#[test]
fn torture_scan_rotated_file() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let config = AuditConfig {
        enabled: true,
        max_file_size: 200,
        max_rotated_files: 2,
    };

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(pass)
        .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
        .pbkdf2_iterations(600_000)
        .cache_size(64)
        .audit_config(config)
        .create()
        .unwrap();

    for _ in 0..15 {
        db.integrity_check().unwrap();
    }
    drop(db);

    let rotated_1 = dir.path().join("test.citadel.citadel-audit.1");
    if rotated_1.exists() {
        let mut data = std::fs::read(&rotated_1).unwrap();
        let offsets = walk_entry_offsets(&data);
        if offsets.len() > 2 {
            let (off, _) = offsets[1];
            data[off] = 0;
            data[off + 1] = 0;
            data[off + 2] = 0;
            data[off + 3] = 0;
            std::fs::write(&rotated_1, &data).unwrap();

            let scan = scan_corrupted_audit_log(&rotated_1).unwrap();
            assert!(!scan.corruption_offsets.is_empty());
            assert!(scan.entries.len() >= offsets.len() - 1);
        }
    }
}

// ============================================================================
// Verify scan handles appended garbage after valid entries
// ============================================================================

#[test]
fn torture_trailing_garbage_after_valid_entries() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, key) = generate_entries(dir.path(), pass, 5);
    assert!(total >= 7);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    data.extend_from_slice(&[0xAB; 100]);
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert_eq!(scan.entries.len(), total);
    assert!(!scan.corruption_offsets.is_empty());

    let result = verify_audit_log(&ap, &key).unwrap();
    assert!(result.chain_valid);
    assert_eq!(result.entries_verified, total as u64);
}

#[test]
fn torture_trailing_magic_without_full_entry() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 3);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    data.extend_from_slice(&0x454E_5452u32.to_le_bytes());
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert_eq!(scan.entries.len(), total);
}

// ============================================================================
// Small entry_len corruption values
// ============================================================================

#[test]
fn torture_entry_len_below_minimum() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 6);
    assert!(total >= 8);

    let ap = audit_path(dir.path());
    let original = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&original);

    for &bad_len in &[0u32, 1, 40, 55] {
        let mut data = original.clone();
        let (off, _) = offsets[2];
        data[off + 4..off + 8].copy_from_slice(&bad_len.to_le_bytes());
        std::fs::write(&ap, &data).unwrap();

        let scan = scan_corrupted_audit_log(&ap).unwrap();
        assert!(!scan.corruption_offsets.is_empty(),
            "entry_len={} should trigger corruption detection", bad_len);

        let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();
        // Entries 1, 2 before corruption
        assert!(seq_nos.contains(&1), "entry_len={}: entry 1 should survive", bad_len);
        assert!(seq_nos.contains(&2), "entry_len={}: entry 2 should survive", bad_len);
        // Entries 4+ after corruption should be recovered
        for seq in 4..=total as u64 {
            assert!(seq_nos.contains(&seq),
                "entry_len={}: entry {} after corruption should be recovered", bad_len, seq);
        }
    }
}

// ============================================================================
// Off-by-one entry_len
// ============================================================================

#[test]
fn torture_entry_len_off_by_one_too_large() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 6);
    assert!(total >= 8);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    let (off, real_len) = offsets[2];
    let bad_len = (real_len + 1) as u32;
    data[off + 4..off + 8].copy_from_slice(&bad_len.to_le_bytes());
    std::fs::write(&ap, &data).unwrap();

    let strict = read_audit_log(&ap).unwrap();
    assert_eq!(strict.len(), 2);

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();
    assert!(seq_nos.contains(&1));
    assert!(seq_nos.contains(&2));
    for seq in 4..=total as u64 {
        assert!(seq_nos.contains(&seq),
            "entry {} after off-by-one should be recovered via sentinel", seq);
    }
}

#[test]
fn torture_entry_len_off_by_one_too_small() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 6);
    assert!(total >= 8);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    let (off, real_len) = offsets[2];
    let bad_len = (real_len - 1) as u32;
    data[off + 4..off + 8].copy_from_slice(&bad_len.to_le_bytes());
    std::fs::write(&ap, &data).unwrap();

    let strict = read_audit_log(&ap).unwrap();
    assert!(strict.len() <= 3);

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();
    for seq in 4..=total as u64 {
        assert!(seq_nos.contains(&seq),
            "entry {} after off-by-one-small should be recovered via sentinel", seq);
    }
}

#[test]
fn torture_entry_len_off_by_entry_alignment() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 6);
    assert!(total >= 8);

    let ap = audit_path(dir.path());
    let data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    let (off, real_len) = offsets[2];
    for delta in &[-4i64, -2, 2, 4, 8, 16] {
        let bad_len = (real_len as i64 + delta) as u32;
        if bad_len < 56 {
            continue;
        }
        let mut d = data.clone();
        d[off + 4..off + 8].copy_from_slice(&bad_len.to_le_bytes());
        std::fs::write(&ap, &d).unwrap();

        let scan = scan_corrupted_audit_log(&ap).unwrap();
        let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();
        for seq in 4..=total as u64 {
            assert!(seq_nos.contains(&seq),
                "delta={}: entry {} should be recovered", delta, seq);
        }
    }
}

// ============================================================================
// Scan + Verify coherence after corruption
// ============================================================================

#[test]
fn torture_scan_and_verify_coherent_after_gap() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, key) = generate_entries(dir.path(), pass, 8);
    assert!(total >= 10);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    let (off, _) = offsets[3];
    data[off] = 0;
    data[off + 1] = 0;
    data[off + 2] = 0;
    data[off + 3] = 0;
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert_eq!(scan.entries.len(), total - 1);
    let scan_seqs: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();
    assert!(!scan_seqs.contains(&4));

    let strict = read_audit_log(&ap).unwrap();
    assert_eq!(strict.len(), 3);

    let result = verify_audit_log(&ap, &key).unwrap();
    assert!(result.chain_valid);
    assert_eq!(result.entries_verified, 3);

    let header_data = std::fs::read(&ap).unwrap();
    let header_count = u64::from_le_bytes(header_data[24..32].try_into().unwrap());
    assert!(header_count > result.entries_verified);
    assert!(scan.entries.len() > result.entries_verified as usize);
}

// ============================================================================
// Crafted false positive in corrupted data
// ============================================================================

#[test]
fn torture_crafted_false_positive_in_corrupted_region() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, key) = generate_entries(dir.path(), pass, 6);
    assert!(total >= 8);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    let (off3, elen3) = offsets[2];
    let gap_start = off3;
    let gap_end = off3 + 4 + elen3;

    let fake_entry_len: u32 = 56;
    let mut fake = Vec::new();
    fake.extend_from_slice(&0x454E_5452u32.to_le_bytes());
    fake.extend_from_slice(&fake_entry_len.to_le_bytes());
    fake.extend_from_slice(&12345u64.to_le_bytes());
    fake.extend_from_slice(&99u64.to_le_bytes());
    fake.extend_from_slice(&1u16.to_le_bytes());
    fake.extend_from_slice(&0u16.to_le_bytes());
    fake.extend_from_slice(&[0xBB; 32]);

    assert!(gap_end - gap_start >= fake.len());

    for b in &mut data[gap_start..gap_end] {
        *b = 0;
    }
    data[gap_start..gap_start + fake.len()].copy_from_slice(&fake);
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    let scan_seqs: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();
    assert!(scan_seqs.contains(&99));

    let result = verify_audit_log(&ap, &key).unwrap();
    assert!(!result.chain_valid);
    assert!(result.entries_verified >= 2);
}

// ============================================================================
// High-volume stress tests
// ============================================================================

#[test]
fn torture_50_entries_scattered_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    for _ in 0..48 {
        db.integrity_check().unwrap();
    }
    drop(db);

    let ap = audit_path(dir.path());
    let entries = read_audit_log(&ap).unwrap();
    let total = entries.len();
    assert!(total >= 50, "should have at least 50 entries, got {}", total);

    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);
    assert_eq!(offsets.len(), total);

    // Corrupt every 7th entry (indices 6, 13, 20, 27, 34, 41, 48)
    let corrupt_indices: Vec<usize> = (6..total).step_by(7).collect();
    assert!(corrupt_indices.len() >= 6, "should corrupt at least 6 entries");

    for &idx in &corrupt_indices {
        let (off, _) = offsets[idx];
        data[off] = 0;
        data[off + 1] = 0;
        data[off + 2] = 0;
        data[off + 3] = 0;
    }
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    let expected = total - corrupt_indices.len();
    assert_eq!(scan.entries.len(), expected,
        "should recover exactly {} of {} entries", expected, total);
    assert_eq!(scan.corruption_offsets.len(), corrupt_indices.len());

    // Verify exact set
    let recovered: std::collections::HashSet<u64> =
        scan.entries.iter().map(|e| e.sequence_no).collect();
    for seq in 1..=total as u64 {
        let idx = (seq - 1) as usize;
        if corrupt_indices.contains(&idx) {
            assert!(!recovered.contains(&seq));
        } else {
            assert!(recovered.contains(&seq),
                "seq {} should be recovered in 50-entry test", seq);
        }
    }
}

/// Generate 100+ entries across multiple sessions, verify scan and verify
/// both work correctly on a large, healthy file.
#[test]
fn torture_100_entries_healthy_scan_verify() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    for _ in 0..40 {
        db.integrity_check().unwrap();
    }
    drop(db);

    // Reopen twice to add more events
    for _ in 0..2 {
        let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
            .passphrase(pass)
            .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
            .pbkdf2_iterations(600_000)
            .cache_size(64)
            .open()
            .unwrap();
        for _ in 0..25 {
            db.integrity_check().unwrap();
        }
        drop(db);
    }

    let ap = audit_path(dir.path());
    let entries = read_audit_log(&ap).unwrap();
    assert!(entries.len() >= 95, "should have 95+ entries, got {}", entries.len());

    // Scan matches read exactly
    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert_eq!(scan.entries.len(), entries.len());
    assert!(scan.corruption_offsets.is_empty());

    // Verify: full HMAC chain valid
    let key = get_audit_key(dir.path(), pass);
    let result = verify_audit_log(&ap, &key).unwrap();
    assert!(result.chain_valid);
    assert_eq!(result.entries_verified, entries.len() as u64);

    // All sequence numbers form an unbroken 1..N
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.sequence_no, (i + 1) as u64,
            "entry {} should have seq {}", i, i + 1);
    }
}

// ============================================================================
// Entry length boundary cases
// ============================================================================

/// Corrupt the entry_len field to exactly equal the file's remaining bytes.
/// This makes the entry appear to extend to EOF — a subtle boundary case.
#[test]
fn torture_entry_len_extends_to_eof() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 6);
    assert!(total >= 8);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    // Set entry 3's entry_len to consume all remaining file bytes
    let (off, _) = offsets[2];
    let bytes_remaining = data.len() - off - 4; // bytes after the magic
    let bad_len = bytes_remaining as u32;
    data[off + 4..off + 8].copy_from_slice(&bad_len.to_le_bytes());
    std::fs::write(&ap, &data).unwrap();

    // Sequential read stops or reads a bogus mega-entry
    let strict = read_audit_log(&ap).unwrap();
    assert!(strict.len() <= 3, "should stop at or before the corrupted entry");

    // Scan: the corrupt entry's structural validation (event_type/detail_len)
    // should fail, causing scan to skip it and recover entries 4+
    let scan = scan_corrupted_audit_log(&ap).unwrap();
    let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();
    // Entries before corruption must be present
    assert!(seq_nos.contains(&1));
    assert!(seq_nos.contains(&2));
}

/// Overwrite a region with repeated magic bytes (no valid entry structure).
/// Scan should not create phantom entries from the repeated magic.
#[test]
fn torture_repeated_magic_bytes_no_phantom() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 6);
    assert!(total >= 8);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&data);

    // Overwrite entries 3-5 with repeated magic bytes
    let start = offsets[2].0;
    let end = offsets[4].0 + 4 + offsets[4].1;
    let magic = 0x454E_5452u32.to_le_bytes();
    let mut pos = start;
    while pos + 4 <= end {
        data[pos..pos + 4].copy_from_slice(&magic);
        pos += 4;
    }
    std::fs::write(&ap, &data).unwrap();

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    // The repeated magic region should NOT produce valid entries
    // because entry_len/event_type/detail_len won't pass structural checks
    // (magic bytes as entry_len = 0x454E5452 = ~1.16 billion, way too large)
    let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();

    // No phantom entries with sequence numbers that don't belong
    for e in &scan.entries {
        assert!(e.sequence_no >= 1 && e.sequence_no <= total as u64,
            "phantom entry with seq {} detected", e.sequence_no);
    }

    // Entries 1-2 before the overwritten region
    assert!(seq_nos.contains(&1));
    assert!(seq_nos.contains(&2));
    // Entries 6+ after the overwritten region
    for seq in 6..=total as u64 {
        assert!(seq_nos.contains(&seq),
            "entry {} after overwritten region should be recovered", seq);
    }
}

/// Bit-flip in every byte position of a single entry. For each flip,
/// verify scan still recovers all OTHER entries.
#[test]
fn torture_single_bit_flip_every_position() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let (total, _key) = generate_entries(dir.path(), pass, 6);
    assert!(total >= 8);

    let ap = audit_path(dir.path());
    let original = std::fs::read(&ap).unwrap();
    let offsets = walk_entry_offsets(&original);

    // Flip one bit in every byte of entry 4 (0-based idx 3)
    let (off, elen) = offsets[3];
    let entry_disk_size = 4 + elen; // magic + entry_len bytes

    for byte_pos in 0..entry_disk_size {
        let abs_pos = off + byte_pos;
        let mut data = original.clone();
        data[abs_pos] ^= 0x01; // flip lowest bit
        std::fs::write(&ap, &data).unwrap();

        let scan = scan_corrupted_audit_log(&ap).unwrap();
        let seq_nos: Vec<u64> = scan.entries.iter().map(|e| e.sequence_no).collect();

        // All entries except possibly entry 4 should be recovered
        for seq in 1..=total as u64 {
            if seq == 4 {
                continue; // entry 4 may or may not survive depending on which byte flipped
            }
            assert!(seq_nos.contains(&seq),
                "bit flip at byte {} of entry 4: entry {} should survive", byte_pos, seq);
        }
    }
}
