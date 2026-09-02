use super::*;

#[test]
fn serialize_deserialize_roundtrip() {
    let desc = TableDescriptor {
        root_page: PageId(42),
        entry_count: 1000,
        depth: 3,
        flags: 0,
    };
    let buf = desc.serialize();
    assert_eq!(buf.len(), TABLE_DESCRIPTOR_SIZE);

    let desc2 = TableDescriptor::deserialize(&buf);
    assert_eq!(desc2.root_page, PageId(42));
    assert_eq!(desc2.entry_count, 1000);
    assert_eq!(desc2.depth, 3);
    assert_eq!(desc2.flags, 0);
}

#[test]
fn checked_deserialize_rejects_truncated_and_extended_descriptors() {
    assert!(TableDescriptor::try_deserialize(&[0; TABLE_DESCRIPTOR_SIZE - 1]).is_none());
    assert!(TableDescriptor::try_deserialize(&[0; TABLE_DESCRIPTOR_SIZE + 1]).is_none());
    assert!(TableDescriptor::try_deserialize(&[0; TABLE_DESCRIPTOR_SIZE]).is_some());
}

#[test]
fn legacy_deserialize_keeps_its_prefix_decoding_contract() {
    let desc = TableDescriptor {
        root_page: PageId(17),
        entry_count: 23,
        depth: 2,
        flags: 5,
    };
    let encoded = desc.serialize();

    let from_prefix = TableDescriptor::deserialize(&encoded[..16]);
    let mut extended = encoded.to_vec();
    extended.extend_from_slice(b"future");
    let from_extended = TableDescriptor::deserialize(&extended);

    for decoded in [from_prefix, from_extended] {
        assert_eq!(decoded.root_page, desc.root_page);
        assert_eq!(decoded.entry_count, desc.entry_count);
        assert_eq!(decoded.depth, desc.depth);
        assert_eq!(decoded.flags, desc.flags);
    }
}

fn resolved_descriptor(root: u32) -> TableDescriptor {
    TableDescriptor {
        root_page: PageId(root),
        entry_count: u64::from(root) + 100,
        depth: 3,
        flags: 5,
    }
}

#[test]
fn resolved_catalog_loads_an_exact_name_once() {
    let catalog = ResolvedCatalog::default();
    let expected = resolved_descriptor(17);
    let mut loads = 0;

    for _ in 0..3 {
        let mut descriptor = catalog
            .resolve(b"table", || {
                loads += 1;
                Ok(expected.clone())
            })
            .unwrap();
        assert_eq!(descriptor.serialize(), expected.serialize());
        descriptor.root_page = PageId(99);
        assert_ne!(descriptor.serialize(), expected.serialize());
    }

    assert_eq!(loads, 1);
    assert_eq!(catalog.tables.lock().len(), 1);
}

#[test]
fn resolved_catalog_keeps_colliding_and_non_utf8_names_distinct() {
    const FIRST: &[u8] = b"collision_table_51661";
    const SECOND: &[u8] = b"collision_table_134778";
    assert_eq!(
        citadel_io::file_manager::table_name_hash(FIRST),
        citadel_io::file_manager::table_name_hash(SECOND)
    );
    let names: [&[u8]; 4] = [FIRST, SECOND, b"table\xfe", b"table\xff"];
    let catalog = ResolvedCatalog::default();

    for (index, name) in names.iter().enumerate() {
        let expected = resolved_descriptor(index as u32 + 1);
        let descriptor = catalog.resolve(name, || Ok(expected.clone())).unwrap();
        assert_eq!(descriptor.serialize(), expected.serialize());
    }
    for (index, name) in names.iter().enumerate() {
        let descriptor = catalog
            .resolve(name, || panic!("exact name should be cached"))
            .unwrap();
        assert_eq!(
            descriptor.serialize(),
            resolved_descriptor(index as u32 + 1).serialize()
        );
    }
    assert_eq!(catalog.tables.lock().len(), names.len());
}

#[test]
fn resolved_catalog_retries_errors_without_caching_them() {
    let catalog = ResolvedCatalog::default();
    for error in [
        citadel_core::Error::DatabaseCorrupted,
        citadel_core::Error::Interrupted,
        citadel_core::Error::TableNotFound("table".into()),
    ] {
        assert!(catalog.resolve(b"table", || Err(error)).is_err());
        assert!(catalog.tables.lock().is_empty());
    }

    let expected = resolved_descriptor(23);
    let descriptor = catalog.resolve(b"table", || Ok(expected.clone())).unwrap();
    assert_eq!(descriptor.serialize(), expected.serialize());
    assert_eq!(
        catalog
            .resolve(b"table", || panic!("successful retry should be cached"))
            .unwrap()
            .serialize(),
        expected.serialize()
    );
}

#[test]
fn resolved_catalog_declines_new_entries_when_full_without_losing_hits() {
    let catalog = ResolvedCatalog::default();
    for index in 0..RESOLVED_CATALOG_LIMIT {
        catalog
            .resolve(&index.to_le_bytes(), || {
                Ok(resolved_descriptor(index as u32))
            })
            .unwrap();
    }
    assert_eq!(catalog.tables.lock().len(), RESOLVED_CATALOG_LIMIT);

    let mut loads = 0;
    for _ in 0..2 {
        let descriptor = catalog
            .resolve(b"uncached", || {
                loads += 1;
                Ok(resolved_descriptor(999))
            })
            .unwrap();
        assert_eq!(descriptor.root_page, PageId(999));
    }
    assert_eq!(loads, 2);
    assert_eq!(catalog.tables.lock().len(), RESOLVED_CATALOG_LIMIT);
    assert_eq!(
        catalog
            .resolve(&0usize.to_le_bytes(), || panic!("full cache lost a hit"))
            .unwrap()
            .serialize(),
        resolved_descriptor(0).serialize()
    );
}

#[test]
fn resolved_catalog_concurrent_admission_respects_the_limit() {
    let catalog = ResolvedCatalog::default();
    for index in 0..RESOLVED_CATALOG_LIMIT - 1 {
        catalog
            .resolve(&index.to_le_bytes(), || {
                Ok(resolved_descriptor(index as u32))
            })
            .unwrap();
    }

    let start = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        for index in RESOLVED_CATALOG_LIMIT..RESOLVED_CATALOG_LIMIT + 8 {
            let catalog = &catalog;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                let descriptor = catalog
                    .resolve(&index.to_le_bytes(), || {
                        Ok(resolved_descriptor(index as u32))
                    })
                    .unwrap();
                assert_eq!(descriptor.root_page, PageId(index as u32));
            });
        }
    });
    assert_eq!(catalog.tables.lock().len(), RESOLVED_CATALOG_LIMIT);
}

#[test]
fn resolved_catalog_loader_runs_without_the_cache_lock() {
    let catalog = ResolvedCatalog::default();
    catalog
        .resolve(b"table", || {
            let tables = catalog
                .tables
                .try_lock()
                .expect("loader must run without the cache lock held");
            assert!(tables.is_empty());
            Ok(resolved_descriptor(17))
        })
        .unwrap();
}
