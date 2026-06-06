//! Unit tests for the `citadel-mcp` binary's CLI parsing, model catalog, and
//! `pull` helpers (no network). Externalized per the repo convention.

use super::Args;

#[test]
fn args_parse_defaults_and_overrides() {
    let a = Args::parse(&["--db".into(), "m.cdl".into()]).unwrap();
    assert_eq!(a.db, "m.cdl");
    assert_eq!(a.region, "default");
    assert!(a.encrypted, "encrypted is the default");
    assert_eq!(a.embedder, "mock", "mock is the default embedder");

    let a = Args::parse(&[
        "--db".into(),
        "m.cdl".into(),
        "--region".into(),
        "notes".into(),
        "--region-mode".into(),
        "plaintext".into(),
        "--embedder".into(),
        "bge-small".into(),
    ])
    .unwrap();
    assert_eq!(a.region, "notes");
    assert!(!a.encrypted);
    assert_eq!(a.embedder, "bge-small");

    assert!(Args::parse(&[]).is_err(), "--db is required");
    assert!(Args::parse(&["--bogus".into()]).is_err(), "unknown flag");
}

#[cfg(feature = "candle-embed")]
#[test]
fn model_spec_maps_known_names_and_rejects_unknown() {
    use super::model_spec;
    let (cfg, repo) = model_spec("bge-small").expect("bge-small is known");
    assert_eq!(repo, "BAAI/bge-small-en-v1.5");
    assert_eq!(cfg.model_id, "bge-small-en-v1.5");
    assert_eq!(
        model_spec("minilm").unwrap().1,
        "sentence-transformers/all-MiniLM-L6-v2"
    );
    assert!(model_spec("does-not-exist").is_none());
}

#[cfg(feature = "candle-embed")]
#[test]
fn reranker_spec_maps_known_name_and_rejects_unknown() {
    use super::reranker_spec;
    assert_eq!(
        reranker_spec("ms-marco-minilm"),
        Some("cross-encoder/ms-marco-MiniLM-L-6-v2")
    );
    assert!(reranker_spec("does-not-exist").is_none());
}

#[cfg(feature = "candle-embed")]
#[test]
fn args_parse_accepts_reranker_flags() {
    use super::Args;
    let a = Args::parse(&[
        "--db".into(),
        "m.cdl".into(),
        "--reranker".into(),
        "ms-marco-minilm".into(),
        "--reranker-dir".into(),
        "/models/ce".into(),
    ])
    .unwrap();
    assert_eq!(a.reranker.as_deref(), Some("ms-marco-minilm"));
    assert_eq!(a.reranker_dir.as_deref(), Some("/models/ce"));
}

#[cfg(feature = "hub")]
#[test]
fn hub_url_builds_resolve_url() {
    use super::hub_url;
    assert_eq!(
        hub_url("BAAI/bge-small-en-v1.5", "model.safetensors"),
        "https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main/model.safetensors"
    );
}

#[cfg(feature = "hub")]
#[test]
fn resolve_models_dir_prefers_explicit_override() {
    use super::resolve_models_dir;
    let dir = resolve_models_dir(Some("/tmp/custom-models")).unwrap();
    assert_eq!(dir, std::path::PathBuf::from("/tmp/custom-models"));
}
