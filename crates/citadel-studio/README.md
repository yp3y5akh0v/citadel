# citadeldb-studio

Native desktop client for [CitadelDB](https://github.com/yp3y5akh0v/citadel). Browse an
encrypted vault, run SQL, inspect stored vectors, and distinguish authenticated data from
views that carry no per-row proof.

<img src="docs/demo.gif" width="860"
     alt="Opening the disposable demo, authenticating visible memory rows, running SQL, and inspecting stored vectors">

Stored-data views clearly show verification status and scope, including how many visible
items were checked.

## Quick start

```
cargo run -p citadeldb-studio
```

Opens on Home. `Open disposable demo` needs no file and no passphrase, and is the fastest
way to see everything below. Its encrypted working copy is real, but every change is
discarded when the demo closes. Vaults you create or open keep their changes normally.

## SQL

Syntax highlighting, wrap-aware line numbers, and results carrying the plan that produced
them. `Explain` plans without execution, while `Analyze (runs)` executes the statement
and reports actual time plus scanned and emitted rows. `Format` preserves leading
comments and hints, and refuses inline comments it cannot restore exactly. Vector cells
render as a summarisation line over a magnitude strip, so two embeddings in adjacent rows
are comparable without reading a digit.

## Vectors

Pan, zoom and pick a bounded sample of a vector column. The plot maps the first two stored
dimensions and normalizes each independently over that sample; it is not dimensionality
reduction, and screen distance is not index distance. A compute shader accumulates
subpixel-aware radial coverage, so sparse points remain legible and overlap stays
order-independent. If different evidence states share a pixel, the worst state wins.

## Tests

```
cargo test -p citadeldb-studio
```

Drives the application headless through `egui_kittest` on a real wgpu device using the
same `Studio::wire` path as the window. Screen renders are written to
`target/studio-shots/` and asserted non-blank.

`docs/` holds the demo above, re-recorded by `scripts/studio-demo.sh`, and the three
stills the Linux package's AppStream metadata points at.

## License

Apache-2.0. Bundled fonts remain under their upstream licences; see
[Third-party notices](licenses/THIRD_PARTY_NOTICES.md).
