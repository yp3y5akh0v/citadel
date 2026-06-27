#!/usr/bin/env bash
# Maintainer release tool: publish all citadeldb crates to crates.io.
# Run on a clean tree after `cargo login`.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

version=$(grep -m1 '^version = "' Cargo.toml | sed 's/.*"\(.*\)".*/\1/')
echo "Publishing version $version"

# Order matters: each crate's deps must already be on the index.
crates=(
  citadeldb-core citadeldb-crypto citadeldb-io citadeldb-page citadeldb-buffer
  citadeldb-txn citadeldb-sync citadeldb citadeldb-sql-json-path citadeldb-vector
  citadeldb-sql citadeldb-mem citadeldb-ai citadeldb-mcp citadeldb-cli
)

for c in "${crates[@]}"; do
  done=false
  for try in $(seq 1 10); do
    if out=$(cargo publish -p "$c" 2>&1); then echo "OK: $c"; done=true; break; fi
    if grep -qE "already uploaded|is already" <<<"$out"; then echo "SKIP: $c"; done=true; break; fi
    if grep -q "failed to select a version" <<<"$out"; then
      echo "index lag at $c ($try/10)"; sleep 30; continue
    fi
    echo "FAILED: $c"; echo "$out"; exit 1
  done
  [ "$done" = true ] || { echo "GAVE UP: $c"; exit 1; }
  # Next crate won't resolve until the index serves this version.
  for w in $(seq 1 20); do
    idx=$(curl -fsSL "https://index.crates.io/ci/ta/$c" 2>/dev/null || echo "")
    grep -q "\"vers\":\"$version\"" <<<"$idx" && break
    echo "  waiting for index: $c $version ($w/20)"; sleep 15
  done
done
echo "ALL PUBLISHED"
