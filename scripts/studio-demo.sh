#!/usr/bin/env bash
# Maintainer tool: re-record the Studio screenshots and demo from the application.
# Needs ffmpeg.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

if ! command -v ffmpeg >/dev/null; then
  echo "ffmpeg is required to encode the demo"
  exit 1
fi

# The stills are the same frames the headless UI tests verify.
for test in memory_screen query_screen vector_screen; do
  cargo test -p citadeldb-studio --test shots "$test" -- --exact
done
cp target/studio-shots/12-memory.png crates/citadel-studio/docs/memory.png
cp target/studio-shots/14-query.png crates/citadel-studio/docs/query.png
cp target/studio-shots/15-vector.png crates/citadel-studio/docs/vector.png

# Writes target/studio-demo/NNNN.png.
cargo test -p citadeldb-studio --test shots -- --ignored demo_reel

# Preserve native frame size; resampling weakens inter-frame compression.
ffmpeg -loglevel error -y -framerate 12 -i target/studio-demo/%04d.png \
  -vf "split[s0][s1];[s0]palettegen=max_colors=224:stats_mode=diff[p];\
[s1][p]paletteuse=dither=bayer:bayer_scale=3:diff_mode=rectangle" \
  -loop 0 crates/citadel-studio/docs/demo.gif

ls -l crates/citadel-studio/docs/{memory,query,vector}.png \
  crates/citadel-studio/docs/demo.gif
