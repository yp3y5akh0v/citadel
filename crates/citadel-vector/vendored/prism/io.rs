use super::point::PointStore;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{self, BufReader, Read};
use std::path::Path;

/// Load vectors from .fvecs format (used by SIFT1M, GIST1M, etc.).
///
/// Format: each vector is preceded by a 4-byte little-endian int (dimension),
/// followed by `dim` little-endian f32 values.
pub fn load_fvecs(path: &Path) -> io::Result<(Vec<f32>, usize)> {
    let file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    // Read first dimension to determine stride
    let dim = reader.read_i32::<LittleEndian>()? as usize;
    let vector_bytes = 4 + dim * 4; // 4 for dim header + dim*4 for data
    let n = file_len as usize / vector_bytes;

    let mut vectors = Vec::with_capacity(n * dim);

    // Re-read from beginning
    drop(reader);
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);

    for _ in 0..n {
        let d = reader.read_i32::<LittleEndian>()? as usize;
        debug_assert_eq!(d, dim);
        for _ in 0..dim {
            vectors.push(reader.read_f32::<LittleEndian>()?);
        }
    }

    Ok((vectors, dim))
}

/// Load vectors from .bvecs format (unsigned byte vectors).
pub fn load_bvecs(path: &Path) -> io::Result<(Vec<f32>, usize)> {
    let file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    let dim = reader.read_i32::<LittleEndian>()? as usize;
    let vector_bytes = 4 + dim;
    let n = file_len as usize / vector_bytes;

    let mut vectors = Vec::with_capacity(n * dim);

    drop(reader);
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);

    for _ in 0..n {
        let d = reader.read_i32::<LittleEndian>()? as usize;
        debug_assert_eq!(d, dim);
        let mut buf = vec![0u8; dim];
        reader.read_exact(&mut buf)?;
        for b in buf {
            vectors.push(b as f32);
        }
    }

    Ok((vectors, dim))
}

/// Load integer vectors from .ivecs format (ground truth indices).
pub fn load_ivecs(path: &Path) -> io::Result<Vec<Vec<u32>>> {
    let file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    let k = reader.read_i32::<LittleEndian>()? as usize;
    let vector_bytes = 4 + k * 4;
    let n = file_len as usize / vector_bytes;

    let mut result = Vec::with_capacity(n);

    drop(reader);
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);

    for _ in 0..n {
        let d = reader.read_i32::<LittleEndian>()? as usize;
        debug_assert_eq!(d, k);
        let mut ids = Vec::with_capacity(k);
        for _ in 0..k {
            ids.push(reader.read_i32::<LittleEndian>()? as u32);
        }
        result.push(ids);
    }

    Ok(result)
}

/// Build a PointStore from loaded fvecs + synthetic attributes.
///
/// Generates `k` attribute dimensions with specified cardinalities.
/// Attributes are assigned round-robin for deterministic testing.
pub fn build_store_with_synthetic_attrs(
    vectors: Vec<f32>,
    dim: usize,
    cardinalities: &[usize],
) -> PointStore {
    let n = vectors.len() / dim;
    let k = cardinalities.len();
    let mut attrs = Vec::with_capacity(k);
    // Strided assignment: attr_j(i) = (i / stride_j) % card_j
    // Guarantees all card_0 * card_1 * ... * card_{k-1} combinations are populated
    let mut strides = Vec::with_capacity(k);
    let mut stride = 1usize;
    for &card in cardinalities {
        strides.push(stride);
        stride *= card;
    }
    for (j, &card) in cardinalities.iter().enumerate() {
        let s = strides[j];
        let attr_j: Vec<u32> = (0..n).map(|i| ((i / s) % card) as u32).collect();
        attrs.push(attr_j);
    }
    PointStore::from_parts(vectors, dim, attrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_store_with_synthetic_attrs() {
        let vectors = vec![0.0f32; 100 * 4];
        let store = build_store_with_synthetic_attrs(vectors, 4, &[10, 5, 3]);
        assert_eq!(store.len, 100);
        assert_eq!(store.k(), 3);
        // Check cardinalities are within bounds
        assert!(store.cardinality(0) <= 10);
        assert!(store.cardinality(1) <= 5);
        assert!(store.cardinality(2) <= 3);
    }
}
