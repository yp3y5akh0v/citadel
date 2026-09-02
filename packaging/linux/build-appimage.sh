#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

: "${TAG:?}" "${TARGET:?}" "${GITHUB_ENV:?}"
: "${GITHUB_REPOSITORY:?}" "${SOURCE_SHA:?}" "${RELEASE_DATE:?}"

if [[ ! "$TAG" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "::error::AppImage tag must be a stable vMAJOR.MINOR.PATCH tag"
  exit 1
fi
if [ "$TARGET" != x86_64-unknown-linux-gnu ]; then
  echo "::error::unsupported Studio AppImage target: $TARGET"
  exit 1
fi
if [[ ! "$GITHUB_REPOSITORY" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] \
    || [[ ! "$RELEASE_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "::error::invalid repository name or release date"
  exit 1
fi

ACTUAL_SHA=$(git rev-parse HEAD)
if [ "$ACTUAL_SHA" != "$SOURCE_SHA" ]; then
  echo "::error::packaging checkout $ACTUAL_SHA does not match planned source $SOURCE_SHA"
  exit 1
fi

RELEASE_DIR=${RELEASE_DIR:-dist/release}
mkdir -p "$RELEASE_DIR"
OUT_NAME="citadel-studio-${TAG}-${TARGET}.AppImage"
OUT="$RELEASE_DIR/$OUT_NAME"
ID=dev.citadeldb.studio
TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT
APPDIR="$TMP_ROOT/AppDir"
mkdir -p "$APPDIR/usr/bin" "$APPDIR/usr/share/applications" \
         "$APPDIR/usr/share/metainfo" \
         "$APPDIR/usr/share/icons/hicolor/256x256/apps"
cp "target/${TARGET}/release/citadel-studio" "$APPDIR/usr/bin/"
mkdir -p "$APPDIR/usr/share/doc/citadel-studio"
cp LICENSE-APACHE crates/citadel-studio/licenses/* \
   "$APPDIR/usr/share/doc/citadel-studio/"
cp crates/sql-json-path/NOTICE \
   "$APPDIR/usr/share/doc/citadel-studio/sql-json-path-NOTICE.txt"
test -f "$APPDIR/usr/share/doc/citadel-studio/THIRD_PARTY_NOTICES.md"
test -f "$APPDIR/usr/share/doc/citadel-studio/sql-json-path-NOTICE.txt"
cp packaging/licenses/THIRD_PARTY_LICENSES.html \
   "$APPDIR/usr/share/doc/citadel-studio/"
cp packaging/licenses/AppImage-runtime-LICENSE.txt \
   "$APPDIR/usr/share/doc/citadel-studio/"
cp crates/citadel-studio/assets/icon-256.png \
   "$APPDIR/usr/share/icons/hicolor/256x256/apps/$ID.png"
cp crates/citadel-studio/assets/icon-256.png "$APPDIR/$ID.png"
cp "packaging/linux/$ID.desktop" "$APPDIR/usr/share/applications/"
cp "packaging/linux/$ID.desktop" "$APPDIR/"
ln -s usr/bin/citadel-studio "$APPDIR/AppRun"

# appimagetool requires the .appdata.xml suffix.
RAW=https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${SOURCE_SHA}/crates/citadel-studio/docs
cat > "$APPDIR/usr/share/metainfo/$ID.appdata.xml" <<META
<?xml version="1.0" encoding="UTF-8"?>
<component type="desktop-application">
  <id>$ID</id>
  <metadata_license>CC0-1.0</metadata_license>
  <project_license>Apache-2.0</project_license>
  <name>Citadel Studio</name>
  <summary>Browse and prove an encrypted database</summary>
  <developer_name>Yuriy Peysakhov</developer_name>
  <description>
    <p>
      Native desktop client for CitadelDB, an encrypted-first embedded database
      that doubles as memory for AI agents.
    </p>
    <p>
      Browse a vault, run SQL, explore an embedding space, and see what can
      actually be proven about every row. Verdicts always appear with the scope
      they cover, so a check over rows that cannot be attested reports what it
      really covered.
    </p>
  </description>
  <launchable type="desktop-id">$ID.desktop</launchable>
  <screenshots>
    <screenshot type="default">
      <image>$RAW/memory.png</image>
      <caption>Memory browser with per-row proof state</caption>
    </screenshot>
    <screenshot>
      <image>$RAW/query.png</image>
      <caption>SQL editor and results</caption>
    </screenshot>
    <screenshot>
      <image>$RAW/vector.png</image>
      <caption>Vector space explorer</caption>
    </screenshot>
  </screenshots>
  <url type="homepage">https://citadeldb.dev</url>
  <url type="bugtracker">https://github.com/${GITHUB_REPOSITORY}/issues</url>
  <url type="vcs-browser">https://github.com/${GITHUB_REPOSITORY}</url>
  <content_rating type="oars-1.1"/>
  <releases>
    <release version="${TAG#v}" date="${RELEASE_DATE}"/>
  </releases>
</component>
META

sudo apt-get install --no-install-recommends -y desktop-file-utils appstream xauth xvfb
desktop-file-validate "$APPDIR/usr/share/applications/$ID.desktop"
appstreamcli validate --no-net "$APPDIR/usr/share/metainfo/$ID.appdata.xml"

while read -r url; do
  SHOT="crates/citadel-studio/docs/$(basename "$url")"
  if [ ! -f "$SHOT" ]; then
    echo "::error::metainfo names a screenshot that is not in the tree: $SHOT"
    exit 1
  fi
done < <(grep -o '<image>[^<]*</image>' "$APPDIR/usr/share/metainfo/$ID.appdata.xml" \
         | cut -d'>' -f2 | cut -d'<' -f1)

LINUXDEPLOY="$TMP_ROOT/linuxdeploy.AppImage"
curl -fsSL -o "$LINUXDEPLOY" \
  https://github.com/linuxdeploy/linuxdeploy/releases/download/1-alpha-20251107-1/linuxdeploy-x86_64.AppImage
echo "c20cd71e3a4e3b80c3483cef793cda3f4e990aca14014d23c544ca3ce1270b4d  $LINUXDEPLOY" \
  | sha256sum -c -
chmod +x "$LINUXDEPLOY"

# Override the plugin's unpinned runtime download.
RUNTIME="$TMP_ROOT/runtime-x86_64"
curl -fsSL -o "$RUNTIME" \
  https://github.com/AppImage/type2-runtime/releases/download/20251108/runtime-x86_64
echo "2fca8b443c92510f1483a883f60061ad09b46b978b2631c807cd873a47ec260d  $RUNTIME" \
  | sha256sum -c -

# AppStream was validated offline; skip the plugin's network validation.
ARCH=x86_64 LDAI_VERSION="${TAG#v}" LDAI_OUTPUT="$OUT" LDAI_NO_APPSTREAM=1 \
  LDAI_RUNTIME_FILE="$RUNTIME" \
  "$LINUXDEPLOY" --appimage-extract-and-run \
  --appdir "$APPDIR" \
  --executable "$APPDIR/usr/bin/citadel-studio" \
  --desktop-file "$APPDIR/usr/share/applications/$ID.desktop" \
  --icon-file "$APPDIR/usr/share/icons/hicolor/256x256/apps/$ID.png" \
  --output appimage

ldd "$APPDIR/usr/bin/citadel-studio" | tee "$TMP_ROOT/ldd.txt"
if grep -q 'not found' "$TMP_ROOT/ldd.txt"; then
  echo "::error::linuxdeploy left an unresolved Studio library"
  exit 1
fi

# Exit 124 means the app remained running until timeout.
set +e
APPIMAGE_EXTRACT_AND_RUN=1 LIBGL_ALWAYS_SOFTWARE=1 WGPU_BACKEND=gl \
  timeout 10s xvfb-run -a "$OUT" >"$TMP_ROOT/smoke.log" 2>&1
SMOKE_STATUS=$?
set -e
if [ "$SMOKE_STATUS" -ne 124 ]; then
  cat "$TMP_ROOT/smoke.log"
  echo "::error::Studio AppImage exited during launch smoke test ($SMOKE_STATUS)"
  exit 1
fi

(
  cd "$RELEASE_DIR"
  sha256sum "$OUT_NAME" > "${OUT_NAME}.sha256"
)
echo "STUDIO_ARCHIVE=$OUT" >> "$GITHUB_ENV"
