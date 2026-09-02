#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

: "${TAG:?}" "${TARGET:?}" "${SHORT_VERSION:?}" "${SOURCE_SHA:?}" "${GITHUB_ENV:?}"

ACTUAL_SHA=$(git rev-parse HEAD)
if [ "$ACTUAL_SHA" != "$SOURCE_SHA" ]; then
  echo "::error::packaging checkout $ACTUAL_SHA does not match planned source $SOURCE_SHA"
  exit 1
fi

if [ "$TAG" != "v$SHORT_VERSION" ]; then
  echo "::error::tag $TAG and MSI version $SHORT_VERSION disagree"
  exit 1
fi
if [[ ! "$SHORT_VERSION" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "::error::MSI version must be numeric MAJOR.MINOR.PATCH"
  exit 1
fi
IFS=. read -r MAJOR MINOR PATCH <<< "$SHORT_VERSION"
if (( 10#$MAJOR > 255 || 10#$MINOR > 255 || 10#$PATCH > 65535 )); then
  echo "::error::MSI version $SHORT_VERSION exceeds Windows Installer limits"
  exit 1
fi
case "$TARGET" in
  x86_64-pc-windows-msvc) WIX_ARCH=x64 ;;
  *) echo "::error::unsupported Studio MSI target: $TARGET"; exit 1 ;;
esac

RELEASE_DIR=${RELEASE_DIR:-dist/release}
mkdir -p "$RELEASE_DIR"
MSI_NAME="citadel-studio-${TAG}-${TARGET}.msi"
MSI="$RELEASE_DIR/$MSI_NAME"
TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT
WIX=$(command -v wix || true)
if [ -z "$WIX" ]; then
  echo "::error::WiX 6.0.1 must be installed before packaging"
  exit 1
fi

# WiX resolves relative sources from the working directory, not the .wxs file.
STAGING="$TMP_ROOT/staging"
mkdir -p "$STAGING"
cp "target/${TARGET}/release/citadel-studio.exe" packaging/windows/license.rtf "$STAGING/"
cp crates/citadel-studio/assets/icon.ico "$STAGING/"
cp LICENSE-APACHE crates/citadel-studio/licenses/* "$STAGING/"
cp crates/sql-json-path/NOTICE "$STAGING/sql-json-path-NOTICE.txt"
cp packaging/licenses/THIRD_PARTY_LICENSES.html "$STAGING/"

# Native WiX requires a Windows bind path.
"$WIX" build packaging/windows/studio.wxs \
  -arch "$WIX_ARCH" \
  -ext WixToolset.UI.wixext \
  -bindpath "$(cygpath -w "$STAGING")" \
  -d Version="$SHORT_VERSION" \
  -pdb "$TMP_ROOT/citadel-studio.wixpdb" \
  -o "$MSI"
# Suppress the per-machine path warning only for a per-user package.
grep -q 'Scope="perUser"' packaging/windows/studio.wxs || {
  echo "::error::ICE91 suppression requires an explicitly per-user package"
  exit 1
}
"$WIX" msi validate -sice ICE91 -pdb "$TMP_ROOT/citadel-studio.wixpdb" "$MSI"

(
  cd "$RELEASE_DIR"
  sha256sum "$MSI_NAME" > "${MSI_NAME}.sha256"
)
echo "STUDIO_ARCHIVE=$MSI" >> "$GITHUB_ENV"
