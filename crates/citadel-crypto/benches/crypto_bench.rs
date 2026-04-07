use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use citadel_core::types::PageId;
use citadel_core::{BODY_SIZE, DEK_SIZE, MAC_KEY_SIZE, PAGE_SIZE};
use citadel_crypto::page_cipher;

fn bench_encrypt_page(c: &mut Criterion) {
    let dek = [0xAA; DEK_SIZE];
    let mac_key = [0xBB; MAC_KEY_SIZE];
    let body = [0x42u8; BODY_SIZE];
    let mut out = [0u8; PAGE_SIZE];

    let mut group = c.benchmark_group("crypto");
    group.throughput(Throughput::Bytes(BODY_SIZE as u64));

    group.bench_function("encrypt_page", |b| {
        b.iter(|| {
            page_cipher::encrypt_page(&dek, &mac_key, PageId(1), 1, &body, &mut out);
        });
    });

    group.bench_function("decrypt_page", |b| {
        page_cipher::encrypt_page(&dek, &mac_key, PageId(1), 1, &body, &mut out);
        let encrypted = out;
        let mut decrypted = [0u8; BODY_SIZE];
        b.iter(|| {
            page_cipher::decrypt_page(&dek, &mac_key, PageId(1), 1, &encrypted, &mut decrypted)
                .unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_encrypt_page);
criterion_main!(benches);
