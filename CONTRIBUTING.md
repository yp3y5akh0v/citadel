# Contributing

Bug reports, tests, and documentation fixes are all welcome, and small changes
are as useful as large ones.

For anything substantial, open an issue first. Citadel is an encrypted database
and some areas carry constraints that are not obvious from the code, so it is
easier to settle the approach before the work.

## Getting set up

The toolchain is pinned in `rust-toolchain.toml`, so local checks match CI:

```
rustup toolchain install                        # 1.97.0 + clippy, rustfmt, wasm32
rustup toolchain install 1.88 --profile minimal # the MSRV job
```

`citadeldb-python` is a pyo3 crate. It is a workspace member but not a
default-member, so a bare `cargo build` skips it while `--workspace` pulls it in.
Its build script needs a Python 3 interpreter on `PATH`; if neither `python3` nor
`python` resolves to a real one it fails with `no Python 3.x interpreter found`
and the command stops there. This is most common on Windows, where the Microsoft
Store aliases shadow a real install. Point pyo3 at an interpreter:

```
$env:PYO3_PYTHON = "C:\path\to\python.exe"      # PowerShell
export PYO3_PYTHON=/usr/bin/python3             # sh
```

## What CI checks

`fmt`, `clippy` and the MSRV check run on every pull request against `master`.
The test job runs after `fmt` and `clippy` pass.

```
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo +1.88 check --workspace --exclude citadeldb-python
cargo test --workspace          # CI runs this on Linux, Windows and macOS
```

Two further jobs cover ground the commands above skip. Run the parts that match
what you touched:

```
# citadel-ai backends, and the wasm build that proves they cfg away
cargo clippy -p citadeldb-ai --all-targets --features claude,openai,ollama,file-tools,command-tool -- -D warnings
cargo test -p citadeldb-ai --features claude,openai,ollama,file-tools,command-tool
cargo clippy -p citadeldb-mcp --all-targets --no-default-features -- -D warnings
cargo clippy -p citadeldb-mcp --all-targets --no-default-features --features candle-embed -- -D warnings
cargo test -p citadeldb-mcp --no-default-features
cargo clippy -p citadeldb-python --all-targets --features candle-embed -- -D warnings
cargo build -p citadeldb-ai --target wasm32-unknown-unknown --features claude,openai,ollama

# the Python bindings, stubs or facades
pip install maturin pytest numpy mypy
maturin build --profile dev --out dist
pip install --no-index --find-links dist citadeldb
pytest python/tests -q
$env:MYPYPATH = "python"; mypy -p citadeldb      # PowerShell
MYPYPATH=python mypy -p citadeldb                # sh
```

Two more legs run only on Linux:

```
cargo test --workspace --features citadeldb/io-uring
cargo test --workspace -p citadeldb --test fips --features citadeldb/fips
```

No CI job builds the benchmark binaries, so run this yourself if you touch
`citadel-membench`:

```
cargo build -p citadeldb-membench --features openai,candle-embed --bins
```

## Worth knowing

- Package names are `citadeldb-*`, lib names `citadel_*`, directories
  `citadel-*`, so `-p citadeldb-mem` works and `-p citadel-mem` does not. The
  root crate breaks the pattern: directory `citadel/`, package `citadeldb`, lib
  `citadel`. The CLI (`citadeldb-cli`) ships a binary called `citadel`. The
  `citadel-cortex`, `citadel-erdos` and `citadel-lean` directories are not
  workspace members, so `-p` cannot select them.
- The `locomo` and `longmemeval` binaries have `required-features`, so
  `--all-targets` skips them silently and they can break while everything is green.
- On a pull request, changes touching only `**.md`, `site/**`, `scripts/**`,
  `LICENSE-*`, `server.json`, `.github/FUNDING.yml` or `.github/*.png` run no CI
  at all (`paths-ignore` in `.github/workflows/ci.yml`).
- Tests needing a real embedder model do not run by default. The Rust ones are
  `#[ignore]`d and need `--features candle-embed`, `-- --ignored`, and a local
  model directory in the environment variable each test names.
  `python/tests/test_candle.py` skips instead, and needs a wheel built with
  `--features candle-embed`.

## Encrypted-data constraints

Atoms in an encrypted memory region are sealed under a random per-atom key whose
only persistent copy is a wrapped blob in a sidecar key store. Destroying that
copy is what `forget` does. The rules that keep that true:

- Atom text stays inside the sealed envelope. A new table, index, or cache that
  holds it needs to be sealed too, or keyed so `forget` reaches it.
- The sidecar key stores overwrite slots in place via
  `durable::overwrite_in_place` rather than writing a temp file and renaming, so
  the old bytes do not persist.
- Key material is wrapped in `Zeroizing` or held in a type that zeroizes on
  `Drop`, so it does not outlive its owner in freed memory.
- Key destruction goes through `KeyLifecycleGuard`, which bumps the cache epoch
  so decrypted caches built under an old key state are refused.
- HKDF info strings are part of the on-disk format. Keys are re-derived from them
  on every open rather than stored, so a released string is frozen.
- The erasure tests in `crates/citadel-mem/tests/` (`region_erasure`,
  `per_atom_erasure`, `crash_residue`, `sealed_cache_epoch`) encode these rules,
  so they are the fastest way to check a change holds them.

## Sending a change

Pull requests go against `master`, whose history has no merge commits. Recent
commit subjects follow `type: summary` (`fix:`, `feat:`, `docs:`, `perf:`,
`chore:`), and one logical change per PR is easiest to review.

Security issues should go to the contact in [SECURITY.md](SECURITY.md) rather
than a public issue.
