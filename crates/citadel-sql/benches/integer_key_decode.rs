//! Public integer-key decoding controls with encoding and validation outside timing.

use std::hint::black_box;
use std::time::Duration;

use citadel_sql::encoding::{decode_key_value, decode_pk_integer, encode_key_value};
use citadel_sql::Value;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

struct EncodedKey {
    bytes: [u8; 10],
    len: u8,
}

impl EncodedKey {
    fn new(value: i64) -> Self {
        let encoded = encode_key_value(&Value::Integer(value));
        assert!(encoded.len() <= 10);
        let mut bytes = [0; 10];
        bytes[..encoded.len()].copy_from_slice(&encoded);
        Self {
            bytes,
            len: encoded.len() as u8,
        }
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

struct Corpus {
    name: &'static str,
    keys: Vec<EncodedKey>,
}

impl Corpus {
    fn checked(name: &'static str, values: &[i64]) -> Self {
        let keys: Vec<_> = values.iter().copied().map(EncodedKey::new).collect();
        for (key, &value) in keys.iter().zip(values) {
            let (decoded, consumed) = decode_key_value(key.as_slice()).unwrap();
            assert_eq!(decoded, Value::Integer(value));
            assert_eq!(consumed, key.as_slice().len());
            assert_eq!(decode_pk_integer(key.as_slice()).unwrap(), value);
        }
        Self { name, keys }
    }
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn scan_ids() -> Corpus {
    // Match h2h::common::citadel_100k exactly. Its age column uses another codec.
    let values: Vec<_> = (0..100_000i64).collect();
    let corpus = Corpus::checked("scan_ids", &values);
    let mut widths = [0usize; 9];
    for key in &corpus.keys {
        widths[usize::from(key.len) - 2] += 1;
    }
    assert_eq!(widths, [1, 255, 65_280, 34_464, 0, 0, 0, 0, 0]);
    corpus
}

fn mixed_width_sign() -> Corpus {
    let mut values = Vec::with_capacity(4099);
    let mut state = 0x73a5_912e_d8c4_60bfu64;
    for width in 1..=8 {
        let low = 1u64 << ((width - 1) * 8);
        let high = if width == 8 {
            i64::MAX as u64
        } else {
            (1u64 << (width * 8)) - 1
        };
        for index in 0..256 {
            let magnitude = match index {
                0 => low,
                1 => low + 1,
                2 => high - 1,
                3 => high,
                _ => low + next_random(&mut state) % (high - low + 1),
            } as i64;
            values.extend([magnitude, -magnitude]);
        }
    }
    values.extend([0, i64::MIN, i64::MAX]);
    // Avoid measuring a repeating positive/negative pair or width-grouped pattern.
    for i in (1..values.len()).rev() {
        let j = (next_random(&mut state) % (i as u64 + 1)) as usize;
        values.swap(i, j);
    }
    let corpus = Corpus::checked("mixed_width_sign", &values);
    let mut positive = [0usize; 9];
    let mut negative = [0usize; 9];
    let mut zeros = 0;
    for (key, value) in corpus.keys.iter().zip(values) {
        let width = usize::from(key.len) - 2;
        if value > 0 {
            positive[width] += 1;
        } else if value < 0 {
            negative[width] += 1;
        } else {
            zeros += 1;
        }
    }
    assert_eq!(positive, [0, 256, 256, 256, 256, 256, 256, 256, 257]);
    assert_eq!(negative, positive);
    assert_eq!(zeros, 1);
    assert_eq!(corpus.keys.len(), 4099);
    corpus
}

fn bench(c: &mut Criterion) {
    let corpora = [scan_ids(), mixed_width_sign()];
    let mut group = c.benchmark_group("integer_key_decode");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(30);
    for corpus in &corpora {
        group.throughput(Throughput::Elements(corpus.keys.len() as u64));
        group.bench_function(BenchmarkId::new(corpus.name, corpus.keys.len()), |b| {
            b.iter(|| {
                for key in black_box(corpus.keys.as_slice()) {
                    // Consume each real decoded Value and length without a timed checksum,
                    // output-vector writes, SQL result caching, or per-key allocation.
                    black_box(decode_key_value(key.as_slice()).unwrap());
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
