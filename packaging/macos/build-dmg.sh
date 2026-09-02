#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

: "${TAG:?}" "${TARGET:?}" "${SHORT_VERSION:?}" "${SOURCE_SHA:?}"
: "${GITHUB_ENV:?}"
if [ "$TAG" != "v$SHORT_VERSION" ] \
    || [[ ! "$SHORT_VERSION" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "::error::macOS package tag and numeric version disagree"
  exit 1
fi
case "$TARGET" in
  x86_64-apple-darwin | aarch64-apple-darwin) ;;
  *) echo "::error::unsupported Studio DMG target: $TARGET"; exit 1 ;;
esac
ACTUAL_SHA=$(git rev-parse HEAD)
if [ "$ACTUAL_SHA" != "$SOURCE_SHA" ]; then
  echo "::error::packaging checkout $ACTUAL_SHA does not match planned source $SOURCE_SHA"
  exit 1
fi

RELEASE_DIR=${RELEASE_DIR:-dist/release}
mkdir -p "$RELEASE_DIR"
DMG_NAME="citadel-studio-${TAG}-${TARGET}.dmg"
DMG="$RELEASE_DIR/$DMG_NAME"
APP="Citadel Studio.app"
TMP_ROOT=$(mktemp -d)
STAGING="$TMP_ROOT/staging"
MNT=""
cleanup() {
  if [ -n "$MNT" ]; then
    hdiutil detach "$MNT" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

LICENCES="$STAGING/$APP/Contents/Resources/Licences"
mkdir -p "$STAGING/$APP/Contents/MacOS" "$LICENCES"
cp "target/${TARGET}/release/citadel-studio" "$STAGING/$APP/Contents/MacOS/"
cp LICENSE-APACHE crates/citadel-studio/licenses/* "$LICENCES/"
cp crates/sql-json-path/NOTICE "$LICENCES/sql-json-path-NOTICE.txt"
cp packaging/licenses/THIRD_PARTY_LICENSES.html "$LICENCES/"

# iconutil requires these exact filenames.
ICONSET="$TMP_ROOT/AppIcon.iconset"
mkdir -p "$ICONSET"
MAIN=crates/citadel-studio/assets/icon.png
# The 1024px master supplies icon_512x512@2x without upscaling.
W=$(sips -g pixelWidth  "$MAIN" | tail -1 | tr -dc 0-9)
H=$(sips -g pixelHeight "$MAIN" | tail -1 | tr -dc 0-9)
if [ "$W" != 1024 ] || [ "$H" != 1024 ]; then
  echo "::error::$MAIN must be 1024x1024, got ${W}x${H}"
  exit 1
fi
sips -z 16   16   "$MAIN"  --out "$ICONSET/icon_16x16.png"       >/dev/null
sips -z 32   32   "$MAIN"  --out "$ICONSET/icon_16x16@2x.png"    >/dev/null
sips -z 32   32   "$MAIN"  --out "$ICONSET/icon_32x32.png"       >/dev/null
sips -z 64   64   "$MAIN"  --out "$ICONSET/icon_32x32@2x.png"    >/dev/null
sips -z 128  128  "$MAIN"  --out "$ICONSET/icon_128x128.png"     >/dev/null
sips -z 256  256  "$MAIN"  --out "$ICONSET/icon_128x128@2x.png"  >/dev/null
sips -z 256  256  "$MAIN"  --out "$ICONSET/icon_256x256.png"     >/dev/null
sips -z 512  512  "$MAIN"  --out "$ICONSET/icon_256x256@2x.png"  >/dev/null
sips -z 512  512  "$MAIN"  --out "$ICONSET/icon_512x512.png"     >/dev/null
cp "$MAIN" "$ICONSET/icon_512x512@2x.png"
iconutil -c icns "$ICONSET" -o "$STAGING/$APP/Contents/Resources/AppIcon.icns"
sips -g pixelHeight "$STAGING/$APP/Contents/Resources/AppIcon.icns" >/dev/null

cat > "$STAGING/$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>Citadel Studio</string>
  <key>CFBundleDisplayName</key><string>Citadel Studio</string>
  <key>CFBundleIdentifier</key><string>dev.citadeldb.studio</string>
  <key>CFBundleVersion</key><string>${SHORT_VERSION}</string>
  <key>CFBundleShortVersionString</key><string>${SHORT_VERSION}</string>
  <key>CFBundleExecutable</key><string>citadel-studio</string>
  <key>CFBundleIconFile</key><string>AppIcon</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>NSHumanReadableCopyright</key><string>Copyright © 2026 Yuriy Peysakhov</string>
  <key>LSMinimumSystemVersion</key><string>11.0</string>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
PLIST

plutil -lint "$STAGING/$APP/Contents/Info.plist"

# Remove extended attributes rejected by codesign (QA1940).
xattr -cr "$STAGING/$APP"
# Ad-hoc signing provides integrity, not publisher identity.
codesign --force --sign - "$STAGING/$APP"
codesign --verify --deep --strict --verbose=2 "$STAGING/$APP"

ln -s /Applications "$STAGING/Applications"
rm -f "$STAGING/.DS_Store"
# Fix the DMG filesystem independently of runner defaults.
hdiutil create -volname "Citadel Studio" -srcfolder "$STAGING" \
  -fs HFS+ -ov -format UDZO "$DMG"
hdiutil verify "$DMG"

MNT="$TMP_ROOT/mount"
mkdir -p "$MNT"
hdiutil attach "$DMG" -readonly -nobrowse -mountpoint "$MNT"
test -x "$MNT/$APP/Contents/MacOS/citadel-studio"
test -L "$MNT/Applications"
test -f "$MNT/$APP/Contents/Resources/Licences/THIRD_PARTY_NOTICES.md"
test -f "$MNT/$APP/Contents/Resources/Licences/sql-json-path-NOTICE.txt"
test -f "$MNT/$APP/Contents/Resources/Licences/THIRD_PARTY_LICENSES.html"
plutil -lint "$MNT/$APP/Contents/Info.plist"
codesign --verify --deep --strict "$MNT/$APP"
hdiutil detach "$MNT"
MNT=""

(
  cd "$RELEASE_DIR"
  shasum -a 256 "$DMG_NAME" > "${DMG_NAME}.sha256"
)
echo "STUDIO_ARCHIVE=$DMG" >> "$GITHUB_ENV"
