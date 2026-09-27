//! Transaction-ID exhaustion must leave committed data readable and writers
//! unable to publish IDs that recovery will reject.
use super::*;

#[test]
fn the_last_committable_id_reopens_readably_and_refuses_another_writer() {
    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let manager =
        TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 64).unwrap();
    manager
        .next_txn_id
        .store(TxnId::MAX_COMMITTED.as_u64(), Ordering::SeqCst);
    let mut writer = manager.begin_write().unwrap();
    assert_eq!(writer.txn_id(), TxnId::MAX_COMMITTED);
    writer.insert(b"last", b"durable").unwrap();
    writer.commit().unwrap();
    drop(manager);

    let reopened = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 64).unwrap();
    assert_eq!(reopened.current_slot().txn_id, TxnId::MAX_COMMITTED);
    assert_eq!(
        reopened.begin_read().get(b"last").unwrap(),
        Some(b"durable".to_vec())
    );
    for _ in 0..2 {
        assert!(matches!(reopened.begin_write(), Err(Error::TxnIdExhausted)));
        assert!(!reopened.write_active.load(Ordering::SeqCst));
    }
    assert_eq!(
        reopened.begin_read().get(b"last").unwrap(),
        Some(b"durable".to_vec())
    );
}

#[test]
fn aborting_the_last_allocated_id_does_not_reuse_it_or_leak_writer_exclusion() {
    let manager = create_test_manager();
    manager
        .next_txn_id
        .store(TxnId::MAX_COMMITTED.as_u64(), Ordering::SeqCst);
    let writer = manager.begin_write().unwrap();
    assert_eq!(writer.txn_id(), TxnId::MAX_COMMITTED);
    writer.abort();
    for _ in 0..2 {
        let result = manager.begin_write_if_generation(manager.commit_generation());
        assert!(matches!(result, Err(Error::TxnIdExhausted)));
        assert!(!manager.write_active.load(Ordering::SeqCst));
    }
}

#[test]
fn readers_use_the_committed_snapshot_without_consuming_write_ids() {
    let manager = create_test_manager();
    let snapshot = manager.current_slot().txn_id;
    manager
        .next_txn_id
        .store(TxnId::MAX_COMMITTED.as_u64(), Ordering::SeqCst);
    let first = manager.begin_read();
    let second = manager.begin_read();
    assert_eq!(first.txn_id(), snapshot);
    assert_eq!(second.txn_id(), snapshot);
    assert_eq!(
        manager.next_txn_id.load(Ordering::SeqCst),
        TxnId::MAX_COMMITTED.as_u64()
    );
    let writer = manager.begin_write().unwrap();
    assert_eq!(writer.txn_id(), TxnId::MAX_COMMITTED);
}

#[test]
fn exhausted_savepoint_creation_keeps_the_last_valid_writer_unchanged() {
    let manager = create_test_manager();
    manager
        .next_txn_id
        .store(TxnId::MAX_COMMITTED.as_u64(), Ordering::SeqCst);
    let mut writer = manager.begin_write().unwrap();
    writer.insert(b"before", b"savepoint").unwrap();
    assert!(matches!(
        writer.begin_savepoint(),
        Err(Error::TxnIdExhausted)
    ));
    assert_eq!(writer.txn_id(), TxnId::MAX_COMMITTED);
    assert!(!writer.is_poisoned());
    writer.commit().unwrap();
    assert_eq!(
        manager.begin_read().get(b"before").unwrap(),
        Some(b"savepoint".to_vec())
    );
}

#[test]
fn exhausted_rollback_cannot_commit_part_of_a_statement() {
    let manager = create_test_manager();
    let mut seed = manager.begin_write().unwrap();
    seed.insert(b"key", b"durable").unwrap();
    seed.commit().unwrap();
    manager
        .next_txn_id
        .store(TxnId::MAX_COMMITTED.as_u64() - 1, Ordering::SeqCst);
    let mut writer = manager.begin_write().unwrap();
    let snapshot = writer.begin_savepoint().unwrap();
    assert_eq!(writer.txn_id(), TxnId::MAX_COMMITTED);
    writer.insert(b"key", b"partial statement").unwrap();
    assert!(matches!(
        writer.restore_snapshot(snapshot.clone()),
        Err(Error::TxnIdExhausted)
    ));
    assert!(
        writer.is_poisoned(),
        "a rollback without a new CoW ID cannot permit commit"
    );
    assert_eq!(writer.txn_id(), TxnId::MAX_COMMITTED);
    assert!(matches!(
        writer.restore_snapshot(snapshot),
        Err(Error::TxnIdExhausted)
    ));
    assert!(matches!(
        writer.insert(b"more", b"work"),
        Err(Error::TxnIdExhausted)
    ));
    assert!(matches!(writer.commit(), Err(Error::TxnIdExhausted)));
    assert_eq!(
        manager.begin_read().get(b"key").unwrap(),
        Some(b"durable".to_vec())
    );
}

#[test]
fn an_exhausted_id_counter_never_wraps() {
    let manager = create_test_manager();
    manager.next_txn_id.store(u64::MAX, Ordering::SeqCst);
    assert!(matches!(manager.begin_write(), Err(Error::TxnIdExhausted)));
    assert_eq!(manager.next_txn_id.load(Ordering::SeqCst), u64::MAX);
    let _reader = manager.begin_read();
    assert_eq!(manager.next_txn_id.load(Ordering::SeqCst), u64::MAX);
    assert!(!manager.write_active.load(Ordering::SeqCst));
}
