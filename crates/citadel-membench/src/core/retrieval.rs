//! Shared retrieval validation.

use crate::core::error::{BenchError, Result};

pub fn validate_embeddings(embeddings: &[Vec<f32>], expected: usize, dim: usize) -> Result<()> {
    if embeddings.len() != expected
        || embeddings
            .iter()
            .any(|vector| vector.len() != dim || vector.iter().any(|x| !x.is_finite()))
    {
        return Err(BenchError::Dataset(format!(
            "query embedder must return {expected} finite {dim}-dimensional vectors, got {} vectors",
            embeddings.len()
        )));
    }
    Ok(())
}
