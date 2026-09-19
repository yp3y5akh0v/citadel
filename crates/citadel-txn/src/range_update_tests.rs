use crate::manager::tests::create_test_manager;
use crate::manager::TxnManager;
use crate::ReadBudget;
use citadel_core::types::ValueType;
use citadel_core::{CancelToken, Error};

const TABLE: &[u8] = b"range";
const ROWS: u32 = 300;

fn seed_inline_rows() -> TxnManager {
    let manager = create_test_manager();
    let mut writer = manager.begin_write().unwrap();
    writer.create_table(TABLE).unwrap();
    for key in 0..ROWS {
        writer
            .table_insert(TABLE, &key.to_be_bytes(), &[0; 160])
            .unwrap();
    }
    assert!(writer.named_trees[TABLE].depth > 1);
    writer.commit().unwrap();
    manager
}

#[test]
fn mixed_leaf_runs_preserve_stop_skip_and_scan_counts() {
    // Stop once on an overflow value and once on an inline value, after
    // crossing several leaves and switching between both storage kinds.
    for stop in [247u32, 251] {
        let manager = create_test_manager();
        let mut seed = manager.begin_write().unwrap();
        seed.create_table(TABLE).unwrap();
        for key in 0..ROWS {
            if key % 17 == 0 {
                seed.named_trees
                    .get_mut(TABLE)
                    .unwrap()
                    .insert(
                        &mut seed.pages,
                        &mut seed.alloc,
                        seed.txn_id,
                        &key.to_be_bytes(),
                        ValueType::Tombstone,
                        b"",
                    )
                    .unwrap();
            } else {
                let size = if key % 13 == 0 { 3000 } else { 160 };
                seed.table_insert(TABLE, &key.to_be_bytes(), &vec![0; size])
                    .unwrap();
            }
        }
        assert!(seed.named_trees[TABLE].depth > 1);
        seed.commit().unwrap();

        let mut writer = manager.begin_write().unwrap();
        let before = manager.rows_scanned();
        let mut visited = Vec::new();
        let count = writer
            .table_update_range::<_, Error>(TABLE, &5u32.to_be_bytes(), |key, value| {
                let key = u32::from_be_bytes(key.try_into().unwrap());
                visited.push(key);
                assert_eq!(value.len(), if key % 13 == 0 { 3000 } else { 160 });
                assert!(value.iter().all(|&byte| byte == 0));
                if key == stop {
                    return Ok(None);
                }
                if key % 5 == 0 {
                    return Ok(Some(false));
                }
                value[0] = 1;
                Ok(Some(true))
            })
            .unwrap();
        assert_eq!(manager.rows_scanned() - before, u64::from(stop - 5 + 1));
        assert_eq!(
            visited,
            (5..=stop).filter(|key| key % 17 != 0).collect::<Vec<_>>()
        );
        assert_eq!(
            count,
            (5..stop)
                .filter(|key| key % 17 != 0 && key % 5 != 0)
                .count() as u64
        );
        writer.commit().unwrap();

        let mut reader = manager.begin_read();
        for key in 0..ROWS {
            let value = reader.table_get(TABLE, &key.to_be_bytes()).unwrap();
            if key % 17 == 0 {
                assert!(value.is_none());
                continue;
            }
            let mut expected = vec![0; if key % 13 == 0 { 3000 } else { 160 }];
            if (5..stop).contains(&key) && key % 5 != 0 {
                expected[0] = 1;
            }
            assert_eq!(value, Some(expected), "key {key}, stop {stop}");
        }
    }
}

#[test]
fn range_leaf_mutations_respect_savepoint_and_old_reader() {
    let manager = seed_inline_rows();
    let mut old_reader = manager.begin_read();
    let mut writer = manager.begin_write().unwrap();
    assert_eq!(
        writer
            .table_update_range::<_, Error>(TABLE, b"", |_, value| {
                value[0] = 1;
                Ok(Some(true))
            })
            .unwrap(),
        u64::from(ROWS)
    );
    let snapshot = writer.begin_savepoint();
    assert_eq!(
        writer
            .table_update_range::<_, Error>(TABLE, b"", |_, value| {
                assert_eq!(value[0], 1);
                value[0] = 2;
                Ok(Some(true))
            })
            .unwrap(),
        u64::from(ROWS)
    );
    writer.restore_snapshot(snapshot);
    for key in 0..ROWS {
        assert_eq!(
            writer
                .table_get(TABLE, &key.to_be_bytes())
                .unwrap()
                .unwrap()[0],
            1
        );
    }
    assert_eq!(
        writer
            .table_update_range::<_, Error>(TABLE, &100u32.to_be_bytes(), |key, value| {
                if key == 200u32.to_be_bytes() {
                    return Ok(None);
                }
                value[0] = 3;
                Ok(Some(true))
            })
            .unwrap(),
        100
    );
    writer.commit().unwrap();

    let mut reader = manager.begin_read();
    for key in 0..ROWS {
        assert_eq!(
            old_reader.table_get(TABLE, &key.to_be_bytes()).unwrap(),
            Some(vec![0; 160])
        );
        let mut expected = vec![0; 160];
        expected[0] = if (100..200).contains(&key) { 3 } else { 1 };
        assert_eq!(
            reader.table_get(TABLE, &key.to_be_bytes()).unwrap(),
            Some(expected)
        );
    }
    assert!(manager.integrity_check().unwrap().is_ok());
}

#[test]
fn cancellation_inside_an_inline_leaf_refuses_the_modified_prefix() {
    let manager = seed_inline_rows();
    let mut writer = manager.begin_write().unwrap();
    let token = CancelToken::new();
    writer.set_cancel(Some(token.clone()));
    let before = manager.rows_scanned();
    let mut visited = 0;
    let error = writer
        .table_update_range::<_, Error>(TABLE, b"", |_, value| {
            value[0] = 1;
            visited += 1;
            if visited == 3 {
                token.cancel();
            }
            Ok(Some(true))
        })
        .unwrap_err();
    assert!(matches!(error, Error::Interrupted));
    assert_eq!(visited, 3);
    assert_eq!(manager.rows_scanned() - before, 3);
    writer.set_cancel(None);
    assert!(matches!(writer.check_usable(), Err(Error::Interrupted)));
    assert!(matches!(writer.commit(), Err(Error::Interrupted)));
    let mut reader = manager.begin_read();
    for key in 0..4u32 {
        assert_eq!(
            reader.table_get(TABLE, &key.to_be_bytes()).unwrap(),
            Some(vec![0; 160])
        );
    }
}

#[test]
fn budget_exhaustion_inside_an_inline_leaf_refuses_the_modified_prefix() {
    let manager = seed_inline_rows();
    let mut writer = manager.begin_write().unwrap();
    let budget = ReadBudget::new(160, 3 * 160);
    writer.set_read_budget(Some(budget.clone()));
    let before = manager.rows_scanned();
    let mut visited = 0;
    let error = writer
        .table_update_range::<_, Error>(TABLE, b"", |_, value| {
            value[0] = 1;
            visited += 1;
            Ok(Some(true))
        })
        .unwrap_err();
    assert!(matches!(
        error,
        Error::ReadBudgetExceeded {
            size: 160,
            remaining: 0,
            ..
        }
    ));
    assert_eq!(visited, 3);
    assert_eq!(budget.remaining(), 0);
    assert_eq!(manager.rows_scanned() - before, 4);
    writer.set_read_budget(None);
    assert!(matches!(
        writer.check_usable(),
        Err(Error::TransactionFailed)
    ));
    assert!(matches!(writer.commit(), Err(Error::TransactionFailed)));
    let mut reader = manager.begin_read();
    for key in 0..4u32 {
        assert_eq!(
            reader.table_get(TABLE, &key.to_be_bytes()).unwrap(),
            Some(vec![0; 160])
        );
    }
}
