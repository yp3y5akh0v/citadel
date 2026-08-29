#[cfg(feature = "hub")]
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "hub", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "hub", serde(rename_all = "snake_case"))]
pub(super) enum ModelKind {
    Embedder,
    Reranker,
}

impl ModelKind {
    #[cfg(feature = "hub")]
    fn label(self) -> &'static str {
        match self {
            Self::Embedder => "embedder",
            Self::Reranker => "reranker",
        }
    }

    #[cfg(feature = "hub")]
    fn directory_flag(self) -> &'static str {
        match self {
            Self::Embedder => "--model-dir",
            Self::Reranker => "--reranker-dir",
        }
    }
}

#[derive(Debug)]
#[cfg_attr(not(feature = "hub"), allow(dead_code))]
struct ArtifactSpec {
    name: &'static str,
    size: u64,
    sha256: &'static str,
}

#[derive(Debug)]
#[cfg_attr(not(feature = "hub"), allow(dead_code))]
pub(super) struct SnapshotSpec {
    pub(super) name: &'static str,
    pub(super) kind: ModelKind,
    pub(super) repo: &'static str,
    pub(super) revision: &'static str,
    files: [ArtifactSpec; 3],
}

#[derive(Debug)]
#[cfg(feature = "hub")]
pub(super) struct ModelArtifacts {
    pub(super) config: Vec<u8>,
    pub(super) tokenizer: Vec<u8>,
    pub(super) weights: Vec<u8>,
    pub(super) dir: PathBuf,
}

const SNAPSHOTS: [SnapshotSpec; 7] = [
    SnapshotSpec {
        name: "e5-large",
        kind: ModelKind::Embedder,
        repo: "intfloat/e5-large",
        revision: "4dc6d853a804b9c8886ede6dda8a073b7dc08a81",
        files: [
            ArtifactSpec {
                name: "config.json",
                size: 611,
                sha256: "d960925459889922fd6b3078e05d89c5b0fadd0018d26421699066cdd98bfb3a",
            },
            ArtifactSpec {
                name: "tokenizer.json",
                size: 466_081,
                sha256: "5fd1c882abbd30517dced455a2c9768945ec726b96727927e4959348d9de550b",
            },
            ArtifactSpec {
                name: "model.safetensors",
                size: 1_340_616_616,
                sha256: "93e04bd3ec4911982905395e3c48ee739ddb8aa88380b8ce27f1fd20bfa5fa8e",
            },
        ],
    },
    SnapshotSpec {
        name: "e5-large-v2",
        kind: ModelKind::Embedder,
        repo: "intfloat/e5-large-v2",
        revision: "f169b11e22de13617baa190a028a32f3493550b6",
        files: [
            ArtifactSpec {
                name: "config.json",
                size: 616,
                sha256: "2394ce5c9ffac96c88496de5d7a92f5d3fec1015b9d25f9f85545f13dfd7a269",
            },
            ArtifactSpec {
                name: "tokenizer.json",
                size: 711_396,
                sha256: "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
            },
            ArtifactSpec {
                name: "model.safetensors",
                size: 1_340_616_616,
                sha256: "d741c1a688a6169af0ecb5a047c44645cd992c31e1bf431269f98bba9ae2911a",
            },
        ],
    },
    SnapshotSpec {
        name: "bge-small",
        kind: ModelKind::Embedder,
        repo: "BAAI/bge-small-en-v1.5",
        revision: "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a",
        files: [
            ArtifactSpec {
                name: "config.json",
                size: 743,
                sha256: "094f8e891b932f2000c92cfc663bac4c62069f5d8af5b5278c4306aef3084750",
            },
            ArtifactSpec {
                name: "tokenizer.json",
                size: 711_396,
                sha256: "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
            },
            ArtifactSpec {
                name: "model.safetensors",
                size: 133_466_304,
                sha256: "3c9f31665447c8911517620762200d2245a2518d6e7208acc78cd9db317e21ad",
            },
        ],
    },
    SnapshotSpec {
        name: "bge-base",
        kind: ModelKind::Embedder,
        repo: "BAAI/bge-base-en-v1.5",
        revision: "a5beb1e3e68b9ab74eb54cfd186867f64f240e1a",
        files: [
            ArtifactSpec {
                name: "config.json",
                size: 777,
                sha256: "bc00af31a4a31b74040d73370aa83b62da34c90b75eb77bfa7db039d90abd591",
            },
            ArtifactSpec {
                name: "tokenizer.json",
                size: 711_396,
                sha256: "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
            },
            ArtifactSpec {
                name: "model.safetensors",
                size: 437_955_512,
                sha256: "c7c1988aae201f80cf91a5dbbd5866409503b89dcaba877ca6dba7dd0a5167d7",
            },
        ],
    },
    SnapshotSpec {
        name: "bge-large",
        kind: ModelKind::Embedder,
        repo: "BAAI/bge-large-en-v1.5",
        revision: "d4aa6901d3a41ba39fb536a557fa166f842b0e09",
        files: [
            ArtifactSpec {
                name: "config.json",
                size: 779,
                sha256: "446712fac367857b4b1302762fe1cd7bfa8b3c4b77b4dc5d77c4025407660896",
            },
            ArtifactSpec {
                name: "tokenizer.json",
                size: 711_396,
                sha256: "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
            },
            ArtifactSpec {
                name: "model.safetensors",
                size: 1_340_616_616,
                sha256: "45e1954914e29bd74080e6c1510165274ff5279421c89f76c418878732f64ae7",
            },
        ],
    },
    SnapshotSpec {
        name: "minilm",
        kind: ModelKind::Embedder,
        repo: "sentence-transformers/all-MiniLM-L6-v2",
        revision: "1110a243fdf4706b3f48f1d95db1a4f5529b4d41",
        files: [
            ArtifactSpec {
                name: "config.json",
                size: 612,
                sha256: "953f9c0d463486b10a6871cc2fd59f223b2c70184f49815e7efbcab5d8908b41",
            },
            ArtifactSpec {
                name: "tokenizer.json",
                size: 466_247,
                sha256: "be50c3628f2bf5bb5e3a7f17b1f74611b2561a3a27eeab05e5aa30f411572037",
            },
            ArtifactSpec {
                name: "model.safetensors",
                size: 90_868_376,
                sha256: "53aa51172d142c89d9012cce15ae4d6cc0ca6895895114379cacb4fab128d9db",
            },
        ],
    },
    SnapshotSpec {
        name: "ms-marco-minilm",
        kind: ModelKind::Reranker,
        repo: "cross-encoder/ms-marco-MiniLM-L-6-v2",
        revision: "233902d25c440f23af6f7d6e94d2946bac0bee0a",
        files: [
            ArtifactSpec {
                name: "config.json",
                size: 794,
                sha256: "380e02c93f431831be65d99a4e7e5f67c133985bf2e77d9d4eba46847190bacc",
            },
            ArtifactSpec {
                name: "tokenizer.json",
                size: 711_396,
                sha256: "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
            },
            ArtifactSpec {
                name: "model.safetensors",
                size: 90_870_598,
                sha256: "821d1aa69520101d6e0737f78a042ae25b19e5cb9160701909d10434f4aeb0ae",
            },
        ],
    },
];

pub(super) fn embedder_snapshot(name: &str) -> Option<&'static SnapshotSpec> {
    SNAPSHOTS
        .iter()
        .find(|spec| spec.kind == ModelKind::Embedder && spec.name == name)
}

pub(super) fn reranker_snapshot(name: &str) -> Option<&'static SnapshotSpec> {
    SNAPSHOTS
        .iter()
        .find(|spec| spec.kind == ModelKind::Reranker && spec.name == name)
}

#[cfg(feature = "hub")]
mod hub {
    use super::{ArtifactSpec, ModelArtifacts, SnapshotSpec};
    use serde::{Deserialize, Serialize};
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    use std::fs::{self, File, OpenOptions};
    use std::io::{BufReader, BufWriter, Read, Write};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    const MANIFEST_VERSION: u32 = 1;
    const MANIFEST_FILE: &str = "citadel-model.json";
    const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
    const COPY_BUFFER_BYTES: usize = 64 * 1024;
    static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SnapshotManifest {
        version: u32,
        name: String,
        kind: super::ModelKind,
        repo: String,
        revision: String,
        files: Vec<ManifestArtifact>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ManifestArtifact {
        name: String,
        size: u64,
        sha256: String,
        blake3: String,
    }

    struct FetchResponse {
        reader: Box<dyn Read>,
        content_length: Option<u64>,
    }

    struct StagingDir {
        path: PathBuf,
        armed: bool,
    }

    impl StagingDir {
        fn create(parent: &Path, revision: &str) -> Result<Self, String> {
            fs::create_dir_all(parent)
                .map_err(|error| format!("create model cache {}: {error}", parent.display()))?;
            for _ in 0..1024 {
                let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
                let path = parent.join(format!(
                    ".{revision}.partial-{}-{sequence}",
                    std::process::id()
                ));
                match fs::create_dir(&path) {
                    Ok(()) => return Ok(Self { path, armed: true }),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(error) => {
                        return Err(format!(
                            "create model staging directory {}: {error}",
                            path.display()
                        ));
                    }
                }
            }
            Err(format!(
                "could not allocate a unique staging directory below {}",
                parent.display()
            ))
        }

        fn finalize(mut self) {
            self.armed = false;
        }
    }

    impl Drop for StagingDir {
        fn drop(&mut self) {
            if self.armed {
                let _ = fs::remove_dir_all(&self.path);
            }
        }
    }

    pub(super) fn load_snapshot(
        models_root: &Path,
        spec: &SnapshotSpec,
    ) -> Result<ModelArtifacts, String> {
        let dir = snapshot_dir(models_root, spec);
        if !path_exists(&dir)? {
            if legacy_cache_exists(models_root, spec)? {
                return Err(format!(
                    "the cached {} '{}' at {} uses the unverifiable pre-2.1 layout; run \
                     `citadeldb-mcp pull {}` to create its pinned snapshot, or pass {} <dir> to a \
                     compatible local checkpoint",
                    spec.kind.label(),
                    spec.name,
                    models_root.join(spec.name).display(),
                    spec.name,
                    spec.kind.directory_flag()
                ));
            }
            return Err(format!(
                "{} '{}' is not downloaded at revision {}; run `citadeldb-mcp pull {}` first, or \
                 pass {} <dir> to a compatible local checkpoint",
                spec.kind.label(),
                spec.name,
                spec.revision,
                spec.name,
                spec.kind.directory_flag()
            ));
        }

        let manifest = read_validated_manifest(&dir, spec)
            .map_err(|error| invalid_snapshot_error(&dir, spec, &error))?;
        let config = read_validated_artifact(&dir, &spec.files[0], &manifest.files[0])
            .map_err(|error| invalid_snapshot_error(&dir, spec, &error))?;
        let tokenizer = read_validated_artifact(&dir, &spec.files[1], &manifest.files[1])
            .map_err(|error| invalid_snapshot_error(&dir, spec, &error))?;
        let weights = read_validated_artifact(&dir, &spec.files[2], &manifest.files[2])
            .map_err(|error| invalid_snapshot_error(&dir, spec, &error))?;
        Ok(ModelArtifacts {
            config,
            tokenizer,
            weights,
            dir,
        })
    }

    pub(super) fn pull_snapshot(
        models_root: &Path,
        spec: &SnapshotSpec,
    ) -> Result<PathBuf, String> {
        pull_snapshot_with(models_root, spec, fetch_url)
    }

    fn pull_snapshot_with<F>(
        models_root: &Path,
        spec: &SnapshotSpec,
        mut fetch: F,
    ) -> Result<PathBuf, String>
    where
        F: FnMut(&str) -> Result<FetchResponse, String>,
    {
        let destination = snapshot_dir(models_root, spec);
        if path_exists(&destination)? {
            verify_snapshot(&destination, spec)
                .map_err(|error| invalid_snapshot_error(&destination, spec, &error))?;
            return Ok(destination);
        }

        let parent = models_root.join(spec.name);
        let staging = StagingDir::create(&parent, spec.revision)?;
        let mut files = Vec::with_capacity(spec.files.len());
        eprintln!(
            "citadeldb-mcp: pulling '{}' at {} from huggingface.co",
            spec.repo, spec.revision
        );
        for artifact in &spec.files {
            let url = hub_url(spec, artifact.name);
            let response = fetch(&url)?;
            files.push(write_downloaded_artifact(
                response,
                &staging.path.join(artifact.name),
                artifact,
                &url,
            )?);
        }
        write_manifest(&staging.path, spec, files)?;

        match fs::rename(&staging.path, &destination) {
            Ok(()) => {
                staging.finalize();
                Ok(destination)
            }
            Err(finalize_error) if path_exists(&destination)? => {
                verify_snapshot(&destination, spec).map_err(|winner_error| {
                    format!(
                        "finalize model snapshot {}: {finalize_error}; the concurrently finalized \
                         snapshot is invalid: {winner_error}",
                        destination.display()
                    )
                })?;
                Ok(destination)
            }
            Err(error) => Err(format!(
                "finalize model snapshot {}: {error}",
                destination.display()
            )),
        }
    }

    fn snapshot_dir(models_root: &Path, spec: &SnapshotSpec) -> PathBuf {
        models_root.join(spec.name).join(spec.revision)
    }

    fn path_exists(path: &Path) -> Result<bool, String> {
        match fs::symlink_metadata(path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(format!("inspect {}: {error}", path.display())),
        }
    }

    fn legacy_cache_exists(models_root: &Path, spec: &SnapshotSpec) -> Result<bool, String> {
        let legacy = models_root.join(spec.name);
        for artifact in &spec.files {
            if path_exists(&legacy.join(artifact.name))? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn invalid_snapshot_error(dir: &Path, spec: &SnapshotSpec, detail: &str) -> String {
        format!(
            "cached {} '{}' at {} is invalid: {detail}; remove that revision directory and run \
             `citadeldb-mcp pull {}` again",
            spec.kind.label(),
            spec.name,
            dir.display(),
            spec.name
        )
    }

    fn hub_url(spec: &SnapshotSpec, file: &str) -> String {
        format!(
            "https://huggingface.co/{}/resolve/{}/{}",
            spec.repo, spec.revision, file
        )
    }

    fn fetch_url(url: &str) -> Result<FetchResponse, String> {
        let response = ureq::get(url)
            .call()
            .map_err(|error| format!("GET {url}: {error}"))?;
        let content_length = response
            .header("Content-Length")
            .and_then(|value| value.parse().ok());
        Ok(FetchResponse {
            reader: response.into_reader(),
            content_length,
        })
    }

    fn write_downloaded_artifact(
        mut response: FetchResponse,
        target: &Path,
        expected: &ArtifactSpec,
        url: &str,
    ) -> Result<ManifestArtifact, String> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target)
            .map_err(|error| format!("create {}: {error}", target.display()))?;
        let mut writer = BufWriter::new(file);
        let mut sha256 = Sha256::new();
        let mut blake3 = blake3::Hasher::new();
        let mut buffer = [0u8; COPY_BUFFER_BYTES];
        let mut size = 0u64;
        let mut last_percent = u64::MAX;
        loop {
            let read = response
                .reader
                .read(&mut buffer)
                .map_err(|error| format!("read {url}: {error}"))?;
            if read == 0 {
                break;
            }
            size = size
                .checked_add(read as u64)
                .ok_or_else(|| format!("downloaded size overflow for {url}"))?;
            if size > expected.size {
                return Err(format!(
                    "downloaded {} is larger than the catalog size: expected {} bytes, received \
                     more than {size}",
                    expected.name, expected.size
                ));
            }
            writer
                .write_all(&buffer[..read])
                .map_err(|error| format!("write {}: {error}", target.display()))?;
            sha256.update(&buffer[..read]);
            blake3.update(&buffer[..read]);
            if let Some(total) = response.content_length.filter(|total| *total > 0) {
                let percent = size.saturating_mul(100) / total;
                if percent != last_percent {
                    eprint!("\r  {} {percent}%", expected.name);
                    let _ = std::io::stderr().flush();
                    last_percent = percent;
                }
            }
        }
        writer
            .flush()
            .map_err(|error| format!("flush {}: {error}", target.display()))?;

        if size != expected.size {
            return Err(format!(
                "downloaded {} has the wrong size: expected {} bytes, received {size}",
                expected.name, expected.size
            ));
        }
        let actual_sha256 = hex_lower(&sha256.finalize());
        if actual_sha256 != expected.sha256 {
            return Err(format!(
                "downloaded {} failed SHA-256 verification: expected {}, got {actual_sha256}",
                expected.name, expected.sha256
            ));
        }
        writer
            .get_ref()
            .sync_all()
            .map_err(|error| format!("sync {}: {error}", target.display()))?;
        if response.content_length.is_some_and(|length| length > 0) {
            eprintln!();
        } else {
            eprintln!("  {} ({size} bytes)", expected.name);
        }
        Ok(ManifestArtifact {
            name: expected.name.to_string(),
            size,
            sha256: actual_sha256,
            blake3: blake3.finalize().to_hex().to_string(),
        })
    }

    fn write_manifest(
        dir: &Path,
        spec: &SnapshotSpec,
        files: Vec<ManifestArtifact>,
    ) -> Result<(), String> {
        let manifest = SnapshotManifest {
            version: MANIFEST_VERSION,
            name: spec.name.to_string(),
            kind: spec.kind,
            repo: spec.repo.to_string(),
            revision: spec.revision.to_string(),
            files,
        };
        let mut bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|error| format!("serialize model manifest: {error}"))?;
        bytes.push(b'\n');
        let path = dir.join(MANIFEST_FILE);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| format!("create {}: {error}", path.display()))?;
        file.write_all(&bytes)
            .map_err(|error| format!("write {}: {error}", path.display()))?;
        file.sync_all()
            .map_err(|error| format!("sync {}: {error}", path.display()))?;
        Ok(())
    }

    fn verify_snapshot(dir: &Path, spec: &SnapshotSpec) -> Result<(), String> {
        let manifest = read_validated_manifest(dir, spec)?;
        for (expected, recorded) in spec.files.iter().zip(&manifest.files) {
            verify_artifact(dir, expected, recorded)?;
        }
        Ok(())
    }

    fn read_validated_manifest(
        dir: &Path,
        spec: &SnapshotSpec,
    ) -> Result<SnapshotManifest, String> {
        verify_directory_shape(dir, spec)?;
        let path = dir.join(MANIFEST_FILE);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("read manifest metadata {}: {error}", path.display()))?;
        if !metadata.file_type().is_file() {
            return Err(format!("manifest {} is not a regular file", path.display()));
        }
        if metadata.len() > MAX_MANIFEST_BYTES {
            return Err(format!(
                "manifest {} is too large: {} bytes",
                path.display(),
                metadata.len()
            ));
        }
        let bytes = fs::read(&path)
            .map_err(|error| format!("read manifest {}: {error}", path.display()))?;
        let manifest: SnapshotManifest = serde_json::from_slice(&bytes)
            .map_err(|error| format!("parse manifest {}: {error}", path.display()))?;
        validate_manifest(&manifest, spec)?;
        Ok(manifest)
    }

    fn verify_directory_shape(dir: &Path, spec: &SnapshotSpec) -> Result<(), String> {
        let metadata = fs::symlink_metadata(dir)
            .map_err(|error| format!("read snapshot metadata {}: {error}", dir.display()))?;
        if !metadata.file_type().is_dir() {
            return Err(format!("snapshot {} is not a directory", dir.display()));
        }
        let mut seen = [false; 4];
        for entry in fs::read_dir(dir)
            .map_err(|error| format!("list snapshot {}: {error}", dir.display()))?
        {
            let entry =
                entry.map_err(|error| format!("list snapshot {}: {error}", dir.display()))?;
            let file_type = entry.file_type().map_err(|error| {
                format!("inspect snapshot entry {}: {error}", entry.path().display())
            })?;
            if !file_type.is_file() {
                return Err(format!(
                    "snapshot entry {} is not a regular file",
                    entry.path().display()
                ));
            }
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(|| {
                format!(
                    "snapshot entry {} is not valid UTF-8",
                    entry.path().display()
                )
            })?;
            let index = if name == MANIFEST_FILE {
                3
            } else {
                spec.files
                    .iter()
                    .position(|expected| expected.name == name)
                    .ok_or_else(|| format!("unexpected file '{}' in {}", name, dir.display()))?
            };
            seen[index] = true;
        }
        for (index, present) in seen.into_iter().enumerate() {
            if !present {
                let name = if index == 3 {
                    MANIFEST_FILE
                } else {
                    spec.files[index].name
                };
                return Err(format!("snapshot is missing {name}"));
            }
        }
        Ok(())
    }

    fn validate_manifest(manifest: &SnapshotManifest, spec: &SnapshotSpec) -> Result<(), String> {
        if manifest.version != MANIFEST_VERSION {
            return Err(format!(
                "unsupported manifest version {}; expected {MANIFEST_VERSION}",
                manifest.version
            ));
        }
        if manifest.name != spec.name
            || manifest.kind != spec.kind
            || manifest.repo != spec.repo
            || manifest.revision != spec.revision
        {
            return Err(format!(
                "manifest identity does not match catalog entry '{}@{}'",
                spec.repo, spec.revision
            ));
        }
        if manifest.files.len() != spec.files.len() {
            return Err(format!(
                "manifest has {} files; expected {}",
                manifest.files.len(),
                spec.files.len()
            ));
        }
        for (recorded, expected) in manifest.files.iter().zip(&spec.files) {
            if recorded.name != expected.name
                || recorded.size != expected.size
                || recorded.sha256 != expected.sha256
            {
                return Err(format!(
                    "manifest entry for '{}' does not match the pinned catalog",
                    expected.name
                ));
            }
            if !is_lower_hex(&recorded.blake3, 64) {
                return Err(format!(
                    "manifest entry for '{}' has an invalid BLAKE3 digest",
                    expected.name
                ));
            }
        }
        Ok(())
    }

    fn verify_artifact(
        dir: &Path,
        expected: &ArtifactSpec,
        recorded: &ManifestArtifact,
    ) -> Result<(), String> {
        let path = dir.join(expected.name);
        let file =
            File::open(&path).map_err(|error| format!("open {}: {error}", path.display()))?;
        let mut reader = BufReader::new(file);
        let mut sha256 = Sha256::new();
        let mut blake3 = blake3::Hasher::new();
        let mut size = 0u64;
        let mut buffer = [0u8; COPY_BUFFER_BYTES];
        loop {
            let read = reader
                .read(&mut buffer)
                .map_err(|error| format!("read {}: {error}", path.display()))?;
            if read == 0 {
                break;
            }
            size = size
                .checked_add(read as u64)
                .ok_or_else(|| format!("file size overflow for {}", path.display()))?;
            if size > expected.size {
                return Err(format!(
                    "{} is larger than the pinned size of {} bytes",
                    expected.name, expected.size
                ));
            }
            sha256.update(&buffer[..read]);
            blake3.update(&buffer[..read]);
        }
        validate_hashes(
            expected,
            recorded,
            size,
            &hex_lower(&sha256.finalize()),
            &blake3.finalize().to_hex(),
        )
    }

    fn read_validated_artifact(
        dir: &Path,
        expected: &ArtifactSpec,
        recorded: &ManifestArtifact,
    ) -> Result<Vec<u8>, String> {
        let path = dir.join(expected.name);
        let bytes = fs::read(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
        let size = u64::try_from(bytes.len())
            .map_err(|_| format!("file size does not fit u64 for {}", path.display()))?;
        let mut sha256 = Sha256::new();
        sha256.update(&bytes);
        let actual_sha256 = hex_lower(&sha256.finalize());
        let actual_blake3 = blake3::hash(&bytes).to_hex();
        validate_hashes(expected, recorded, size, &actual_sha256, &actual_blake3)?;
        Ok(bytes)
    }

    fn validate_hashes(
        expected: &ArtifactSpec,
        recorded: &ManifestArtifact,
        size: u64,
        sha256: &str,
        blake3: &str,
    ) -> Result<(), String> {
        if size != expected.size {
            return Err(format!(
                "{} has the wrong size: expected {}, got {size}",
                expected.name, expected.size
            ));
        }
        if sha256 != expected.sha256 {
            return Err(format!(
                "{} failed SHA-256 verification: expected {}, got {sha256}",
                expected.name, expected.sha256
            ));
        }
        if blake3 != recorded.blake3 {
            return Err(format!(
                "{} failed BLAKE3 verification: expected {}, got {blake3}",
                expected.name, recorded.blake3
            ));
        }
        Ok(())
    }

    fn hex_lower(bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
        }
        output
    }

    fn is_lower_hex(value: &str, len: usize) -> bool {
        value.len() == len
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::model_cache::{reranker_snapshot, ModelKind, SnapshotSpec, SNAPSHOTS};
        use std::collections::HashSet;
        use std::io::Cursor;
        use std::sync::{Arc, Barrier, Mutex};

        const TEST_CONFIG: &[u8] = b"config";
        const TEST_TOKENIZER: &[u8] = b"tokenizer";
        const TEST_WEIGHTS: &[u8] = b"weights";
        const TEST_SPEC: SnapshotSpec = SnapshotSpec {
            name: "test-model",
            kind: ModelKind::Embedder,
            repo: "example/test-model",
            revision: "0123456789abcdef0123456789abcdef01234567",
            files: [
                ArtifactSpec {
                    name: "config.json",
                    size: 6,
                    sha256: "b79606fb3afea5bd1609ed40b622142f1c98125abcfe89a76a661b0e8e343910",
                },
                ArtifactSpec {
                    name: "tokenizer.json",
                    size: 9,
                    sha256: "5f97e3774c51edd1d63706c2ec3826c564a067794770cdab0f8c4797971cacf9",
                },
                ArtifactSpec {
                    name: "model.safetensors",
                    size: 7,
                    sha256: "9a129038d9a00aed0cf6a7ea059ca50a813449061ab87848cf1a13eafdf33b2c",
                },
            ],
        };

        fn body_for_url(url: &str) -> Vec<u8> {
            if url.ends_with("/config.json") {
                TEST_CONFIG.to_vec()
            } else if url.ends_with("/tokenizer.json") {
                TEST_TOKENIZER.to_vec()
            } else if url.ends_with("/model.safetensors") {
                TEST_WEIGHTS.to_vec()
            } else {
                panic!("unexpected URL {url}");
            }
        }

        fn fetch_bytes(bytes: Vec<u8>) -> FetchResponse {
            FetchResponse {
                content_length: Some(bytes.len() as u64),
                reader: Box::new(Cursor::new(bytes)),
            }
        }

        fn pull_test_snapshot(root: &Path) -> PathBuf {
            pull_snapshot_with(root, &TEST_SPEC, |url| Ok(fetch_bytes(body_for_url(url)))).unwrap()
        }

        fn staging_entries(root: &Path) -> Vec<PathBuf> {
            let parent = root.join(TEST_SPEC.name);
            match fs::read_dir(parent) {
                Ok(entries) => entries
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.starts_with('.'))
                    })
                    .collect(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(error) => panic!("list staging entries: {error}"),
            }
        }

        #[test]
        fn catalog_pins_every_artifact_to_a_full_revision_and_sha256() {
            assert_eq!(SNAPSHOTS.len(), 7);
            let expected = [
                ("e5-large", "4dc6d853a804b9c8886ede6dda8a073b7dc08a81"),
                ("e5-large-v2", "f169b11e22de13617baa190a028a32f3493550b6"),
                ("bge-small", "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a"),
                ("bge-base", "a5beb1e3e68b9ab74eb54cfd186867f64f240e1a"),
                ("bge-large", "d4aa6901d3a41ba39fb536a557fa166f842b0e09"),
                ("minilm", "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"),
                (
                    "ms-marco-minilm",
                    "233902d25c440f23af6f7d6e94d2946bac0bee0a",
                ),
            ];
            let mut names = HashSet::new();
            let mut catalog_digest = Sha256::new();
            for (spec, (name, revision)) in SNAPSHOTS.iter().zip(expected) {
                assert_eq!(spec.name, name);
                assert_eq!(spec.revision, revision);
                assert!(names.insert(spec.name), "duplicate catalog name");
                assert!(is_lower_hex(spec.revision, 40));
                assert_eq!(spec.files.len(), 3);
                for value in [spec.name, spec.kind.label(), spec.repo, spec.revision] {
                    catalog_digest.update((value.len() as u64).to_le_bytes());
                    catalog_digest.update(value.as_bytes());
                }
                for file in &spec.files {
                    assert!(file.size > 0);
                    assert!(is_lower_hex(file.sha256, 64));
                    catalog_digest.update((file.name.len() as u64).to_le_bytes());
                    catalog_digest.update(file.name.as_bytes());
                    catalog_digest.update(file.size.to_le_bytes());
                    catalog_digest.update(file.sha256.as_bytes());
                }
            }
            assert_eq!(
                hex_lower(&catalog_digest.finalize()),
                "43264317674b437f2ea23174a2aa23f5b8490ec418203c269eb01cdbf4f45786"
            );
        }

        #[test]
        fn urls_use_one_immutable_revision_and_never_main() {
            for file in &TEST_SPEC.files {
                let url = hub_url(&TEST_SPEC, file.name);
                assert!(url.contains(TEST_SPEC.revision));
                assert!(!url.contains("/resolve/main/"));
            }
            assert_eq!(
                hub_url(&TEST_SPEC, "model.safetensors"),
                "https://huggingface.co/example/test-model/resolve/\
                 0123456789abcdef0123456789abcdef01234567/model.safetensors"
            );
        }

        #[test]
        fn pull_makes_the_snapshot_visible_only_after_every_file_is_verified() {
            let temp = tempfile::tempdir().unwrap();
            let final_dir = snapshot_dir(temp.path(), &TEST_SPEC);
            let urls = Arc::new(Mutex::new(Vec::new()));
            let seen = Arc::clone(&urls);
            let pulled = pull_snapshot_with(temp.path(), &TEST_SPEC, |url| {
                assert!(
                    !final_dir.exists(),
                    "staging became visible before finalization"
                );
                seen.lock().unwrap().push(url.to_string());
                Ok(fetch_bytes(body_for_url(url)))
            })
            .unwrap();
            assert_eq!(pulled, final_dir);
            assert!(final_dir.join(MANIFEST_FILE).is_file());
            assert_eq!(urls.lock().unwrap().len(), 3);

            let loaded = load_snapshot(temp.path(), &TEST_SPEC).unwrap();
            assert_eq!(loaded.config, TEST_CONFIG);
            assert_eq!(loaded.tokenizer, TEST_TOKENIZER);
            assert_eq!(loaded.weights, TEST_WEIGHTS);
            assert_eq!(loaded.dir, final_dir);
        }

        #[test]
        fn a_failed_download_is_never_finalized_and_staging_is_removed() {
            for failed_index in 0..3 {
                let temp = tempfile::tempdir().unwrap();
                let mut index = 0usize;
                let error = pull_snapshot_with(temp.path(), &TEST_SPEC, |url| {
                    let current = index;
                    index += 1;
                    if current == failed_index {
                        Err(format!("injected failure for {url}"))
                    } else {
                        Ok(fetch_bytes(body_for_url(url)))
                    }
                })
                .unwrap_err();
                assert!(error.contains("injected failure"), "{error}");
                assert!(!snapshot_dir(temp.path(), &TEST_SPEC).exists());
                assert!(staging_entries(temp.path()).is_empty());
            }
        }

        #[test]
        fn first_download_rejects_wrong_size_and_sha256() {
            for (corrupt, expected_error) in [
                (b"weight".to_vec(), "wrong size"),
                (b"weights!".to_vec(), "larger than"),
                (b"weightx".to_vec(), "SHA-256"),
            ] {
                let temp = tempfile::tempdir().unwrap();
                let error = pull_snapshot_with(temp.path(), &TEST_SPEC, |url| {
                    let bytes = if url.ends_with("model.safetensors") {
                        corrupt.clone()
                    } else {
                        body_for_url(url)
                    };
                    Ok(fetch_bytes(bytes))
                })
                .unwrap_err();
                assert!(error.contains(expected_error), "{error}");
                assert!(!snapshot_dir(temp.path(), &TEST_SPEC).exists());
                assert!(staging_entries(temp.path()).is_empty());
            }
        }

        #[test]
        fn a_valid_snapshot_makes_pull_idempotent_without_network() {
            let temp = tempfile::tempdir().unwrap();
            let expected = pull_test_snapshot(temp.path());
            let actual = pull_snapshot_with(temp.path(), &TEST_SPEC, |_| {
                panic!("a valid immutable snapshot must not be downloaded twice")
            })
            .unwrap();
            assert_eq!(actual, expected);
        }

        #[test]
        fn concurrent_pulls_finalize_one_complete_snapshot() {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().to_path_buf();
            let barrier = Arc::new(Barrier::new(2));
            let mut workers = Vec::new();
            for _ in 0..2 {
                let root = root.clone();
                let barrier = Arc::clone(&barrier);
                workers.push(std::thread::spawn(move || {
                    let mut first = true;
                    pull_snapshot_with(&root, &TEST_SPEC, |url| {
                        if first {
                            first = false;
                            barrier.wait();
                        }
                        Ok(fetch_bytes(body_for_url(url)))
                    })
                }));
            }
            let left = workers.remove(0).join().unwrap().unwrap();
            let right = workers.remove(0).join().unwrap().unwrap();
            assert_eq!(left, right);
            load_snapshot(&root, &TEST_SPEC).unwrap();
            assert!(staging_entries(&root).is_empty());
        }

        #[test]
        fn a_changed_cached_file_is_refused_and_never_repaired_implicitly() {
            let temp = tempfile::tempdir().unwrap();
            let dir = pull_test_snapshot(temp.path());
            fs::write(dir.join("model.safetensors"), b"weightx").unwrap();

            let error = load_snapshot(temp.path(), &TEST_SPEC).unwrap_err();
            assert!(error.contains("SHA-256"), "{error}");
            assert!(error.contains("remove that revision directory"), "{error}");
            let error = pull_snapshot_with(temp.path(), &TEST_SPEC, |_| {
                panic!("an invalid immutable snapshot must not be overwritten")
            })
            .unwrap_err();
            assert!(error.contains("SHA-256"), "{error}");
        }

        #[test]
        fn a_manifest_with_the_wrong_identity_is_refused() {
            let temp = tempfile::tempdir().unwrap();
            let dir = pull_test_snapshot(temp.path());
            let path = dir.join(MANIFEST_FILE);
            let mut value: serde_json::Value =
                serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            value["revision"] = serde_json::Value::String("f".repeat(40));
            fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();

            let error = load_snapshot(temp.path(), &TEST_SPEC).unwrap_err();
            assert!(error.contains("identity does not match"), "{error}");
        }

        #[test]
        fn a_manifest_blake3_mismatch_is_refused() {
            let temp = tempfile::tempdir().unwrap();
            let dir = pull_test_snapshot(temp.path());
            let path = dir.join(MANIFEST_FILE);
            let mut value: serde_json::Value =
                serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            value["files"][0]["blake3"] = serde_json::Value::String("0".repeat(64));
            fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();

            let error = load_snapshot(temp.path(), &TEST_SPEC).unwrap_err();
            assert!(error.contains("BLAKE3 verification"), "{error}");
        }

        #[test]
        fn missing_unexpected_and_non_regular_files_are_refused() {
            let temp = tempfile::tempdir().unwrap();
            let dir = pull_test_snapshot(temp.path());
            fs::create_dir(dir.join("nested")).unwrap();
            let error = load_snapshot(temp.path(), &TEST_SPEC).unwrap_err();
            assert!(error.contains("not a regular file"), "{error}");
            fs::remove_dir(dir.join("nested")).unwrap();
            fs::write(dir.join("unexpected"), b"x").unwrap();
            let error = load_snapshot(temp.path(), &TEST_SPEC).unwrap_err();
            assert!(error.contains("unexpected file"), "{error}");
            fs::remove_file(dir.join("unexpected")).unwrap();
            fs::remove_file(dir.join("tokenizer.json")).unwrap();
            let error = load_snapshot(temp.path(), &TEST_SPEC).unwrap_err();
            assert!(error.contains("missing tokenizer.json"), "{error}");
        }

        #[test]
        fn a_legacy_flat_cache_is_rejected_with_a_repull_instruction() {
            let temp = tempfile::tempdir().unwrap();
            let legacy = temp.path().join(TEST_SPEC.name);
            fs::create_dir_all(&legacy).unwrap();
            fs::write(legacy.join("model.safetensors"), TEST_WEIGHTS).unwrap();

            let error = load_snapshot(temp.path(), &TEST_SPEC).unwrap_err();
            assert!(error.contains("pre-2.1 layout"), "{error}");
            assert!(error.contains("citadeldb-mcp pull test-model"), "{error}");
            assert!(error.contains("--model-dir"), "{error}");
        }

        #[test]
        fn a_missing_snapshot_names_the_matching_bring_your_own_flag() {
            let temp = tempfile::tempdir().unwrap();
            let embedder_error = load_snapshot(temp.path(), &TEST_SPEC).unwrap_err();
            assert!(embedder_error.contains("--model-dir"), "{embedder_error}");

            let reranker = reranker_snapshot("ms-marco-minilm").unwrap();
            let reranker_error = load_snapshot(temp.path(), reranker).unwrap_err();
            assert!(
                reranker_error.contains("--reranker-dir"),
                "{reranker_error}"
            );
        }
    }
}

#[cfg(feature = "hub")]
pub(super) fn load_snapshot(
    models_root: &std::path::Path,
    spec: &SnapshotSpec,
) -> Result<ModelArtifacts, String> {
    hub::load_snapshot(models_root, spec)
}

#[cfg(feature = "hub")]
pub(super) fn pull_snapshot(
    models_root: &std::path::Path,
    spec: &SnapshotSpec,
) -> Result<PathBuf, String> {
    hub::pull_snapshot(models_root, spec)
}
