#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ] || [ ! -d "$1" ]; then
  echo "usage: $0 <artifact-directory> <vMAJOR.MINOR.PATCH>" >&2
  exit 2
fi
ARTIFACT_ROOT=$1
TAG=$2
if [[ ! "$TAG" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "::error::release tag must be a stable vMAJOR.MINOR.PATCH tag"
  exit 1
fi

TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT
EXPECTED="$TMP_ROOT/expected"
ACTUAL="$TMP_ROOT/actual"

add_package() {
  printf '%s\n%s.sha256\n' "$1" "$1" >> "$EXPECTED"
}

for prefix in citadel citadel-fips; do
  add_package "${prefix}-${TAG}-x86_64-unknown-linux-musl.tar.gz"
  add_package "${prefix}-${TAG}-aarch64-unknown-linux-gnu.tar.gz"
  add_package "${prefix}-${TAG}-x86_64-apple-darwin.tar.gz"
  add_package "${prefix}-${TAG}-aarch64-apple-darwin.tar.gz"
  add_package "${prefix}-${TAG}-x86_64-pc-windows-msvc.zip"
done
for prefix in citadel-ffi citadel-ffi-fips; do
  add_package "${prefix}-${TAG}-x86_64-unknown-linux-gnu.tar.gz"
  add_package "${prefix}-${TAG}-aarch64-unknown-linux-gnu.tar.gz"
  add_package "${prefix}-${TAG}-x86_64-apple-darwin.tar.gz"
  add_package "${prefix}-${TAG}-aarch64-apple-darwin.tar.gz"
  add_package "${prefix}-${TAG}-x86_64-pc-windows-msvc.zip"
done
add_package "citadel-studio-${TAG}-x86_64-unknown-linux-gnu.AppImage"
add_package "citadel-studio-${TAG}-x86_64-apple-darwin.dmg"
add_package "citadel-studio-${TAG}-aarch64-apple-darwin.dmg"
add_package "citadel-studio-${TAG}-x86_64-pc-windows-msvc.msi"

find "$ARTIFACT_ROOT" -type f -printf '%f\n' | sort > "$ACTUAL"
sort -o "$EXPECTED" "$EXPECTED"
if [ -n "$(uniq -d "$ACTUAL")" ]; then
  echo "::error::duplicate release artifact basename"
  uniq -d "$ACTUAL"
  exit 1
fi
if ! diff -u "$EXPECTED" "$ACTUAL"; then
  echo "::error::downloaded artifacts do not match the native release contract"
  exit 1
fi

while IFS= read -r name; do
  [[ "$name" == *.sha256 ]] && continue
  package=$(find "$ARTIFACT_ROOT" -type f -name "$name" -print -quit)
  checksum="${package}.sha256"
  mapfile -t records < "$checksum"
  if [ "${#records[@]}" -ne 1 ]; then
    echo "::error::$checksum must contain exactly one checksum record"
    exit 1
  fi
  IFS=' ' read -r digest recorded extra <<< "${records[0]}"
  recorded=${recorded#\*}
  if [[ ! "$digest" =~ ^[0-9a-fA-F]{64}$ ]] \
      || [ "$recorded" != "$name" ] || [ -n "$extra" ]; then
    echo "::error::$checksum does not name exactly $name"
    exit 1
  fi
  actual=$(sha256sum "$package")
  actual=${actual%% *}
  if [ "${digest,,}" != "$actual" ]; then
    echo "::error::checksum mismatch for $name"
    exit 1
  fi
  echo "$name: OK"
done < "$EXPECTED"
