//! Exact classification of memory-owned tables for public inspection surfaces.

use citadel_mem::owns_table;

#[test]
fn the_fixed_schema_tables_are_owned() {
    for name in [
        "memory_meta",
        "memory_regions",
        "memory_edges",
        "memory_similarity_policies",
        "memory_similarity_edges",
        "memory_idempotency",
    ] {
        assert!(owns_table(name), "{name} is created by the memory schema");
    }
}

#[test]
fn atoms_tables_are_owned_at_every_shape() {
    for name in [
        "memory_atoms_d32_cosine",
        "memory_atoms_d32_cosine_enc",
        "memory_atoms_d1536_l2",
        "memory_atoms_d768_inner_enc",
    ] {
        assert!(owns_table(name), "{name} is an atoms table");
    }
}

#[test]
fn a_user_table_sharing_the_prefix_is_never_claimed() {
    for name in [
        "memory_notes",
        "memory_atoms",
        "memory_atoms_d32",
        "memory_atoms_dxx_cosine",
        "memory_atoms_d32_manhattan",
        "memory_atoms_d32_cosine_backup",
        "memory_regions_2024",
        "customers",
        "documents",
    ] {
        assert!(!owns_table(name), "{name} is not the engine's to claim");
    }
}
