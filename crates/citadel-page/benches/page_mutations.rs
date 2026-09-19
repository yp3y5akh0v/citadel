//! Isolate fragmented leaf insertion and child-split propagation on branch pages.

use std::hint::black_box;

use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_page::{branch_node, leaf_node, page::Page};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

fn fragmented_leaf(value_len: usize) -> (Page, Vec<u8>, Vec<u8>) {
    let mut page = Page::new_for_write(PageId(1), PageType::Leaf, TxnId(1));
    let value = vec![17; value_len];
    let mut count = 0u32;
    while leaf_node::insert_direct(&mut page, &count.to_be_bytes(), ValueType::Inline, &value) {
        count += 1;
    }
    // Deleting alternate slots leaves their bytes below the cell-area frontier.
    // A new cell must reclaim those holes, regardless of the number of live cells.
    for id in (0..count).step_by(2) {
        assert!(leaf_node::delete(&mut page, &id.to_be_bytes()));
    }
    let key = count.to_be_bytes().to_vec();
    // Removing slots also widens the contiguous pointer gap. Exceed that gap
    // explicitly, including for dense pages with hundreds of removed slots.
    let replacement = vec![29; (value_len + 16).max(page.available_space() + 1)];
    assert!(page.available_space() < 7 + key.len() + replacement.len());
    assert!(page.free_space() as usize >= 9 + key.len() + replacement.len());
    (page, key, replacement)
}

fn branch(count: u32, key_len: usize) -> Page {
    let mut page = Page::new_for_write(PageId(1), PageType::Branch, TxnId(1));
    for id in 0..count {
        let mut key = vec![0; key_len];
        key[..4].copy_from_slice(&(id * 2 + 2).to_be_bytes());
        page.write_cell(&branch_node::build_cell(PageId(id + 2), &key))
            .unwrap();
    }
    page.set_right_child(PageId(count + 2));
    page
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("page_mutations");
    for value_len in [8, 128, 1024] {
        let (page, key, value) = fragmented_leaf(value_len);
        let mut check = page.clone();
        assert!(leaf_node::insert_direct(
            &mut check,
            &key,
            ValueType::Inline,
            &value
        ));
        assert_eq!(
            leaf_node::read_cell(&check, check.num_cells() - 1).value,
            value
        );
        leaf_node::read_cells_checked(&check).unwrap();
        group.bench_with_input(
            BenchmarkId::new("fragmented_leaf", value_len),
            &page,
            |b, page| {
                b.iter_batched_ref(
                    || page.clone(),
                    |page| {
                        assert!(leaf_node::insert_direct(
                            black_box(page),
                            &key,
                            ValueType::Inline,
                            &value
                        ));
                        black_box(&*page);
                    },
                    BatchSize::NumIterations(64),
                );
            },
        );
    }
    for (count, key_len) in [(100, 8), (500, 8), (100, 32)] {
        let page = branch(count, key_len);
        let child_idx = count as usize / 2;
        let mut separator = vec![0; key_len];
        separator[..4].copy_from_slice(&(count + 1).to_be_bytes());
        let mut check = page.clone();
        assert!(branch_node::insert_separator(
            &mut check,
            child_idx,
            PageId(20_000),
            &separator,
            PageId(20_001)
        ));
        branch_node::read_cells_checked(&check).unwrap();
        group.bench_with_input(
            BenchmarkId::new("interior_separator", format!("{count}x{key_len}")),
            &page,
            |b, page| {
                b.iter_batched_ref(
                    || page.clone(),
                    |page| {
                        assert!(branch_node::insert_separator(
                            black_box(page),
                            child_idx,
                            PageId(20_000),
                            &separator,
                            PageId(20_001)
                        ));
                        black_box(&*page);
                    },
                    BatchSize::NumIterations(64),
                )
            },
        );
    }
    let page = branch(100, 8);
    let mut separator = [0; 8];
    separator[..4].copy_from_slice(&202u32.to_be_bytes());
    let mut check = page.clone();
    assert!(branch_node::insert_separator(
        &mut check,
        100,
        PageId(20_000),
        &separator,
        PageId(20_001)
    ));
    branch_node::read_cells_checked(&check).unwrap();
    group.bench_function("rightmost_separator", |b| {
        b.iter_batched_ref(
            || page.clone(),
            |page| {
                assert!(branch_node::insert_separator(
                    black_box(page),
                    100,
                    PageId(20_000),
                    &separator,
                    PageId(20_001)
                ));
                black_box(&*page);
            },
            BatchSize::NumIterations(64),
        )
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
