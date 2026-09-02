# Website

Requires Node.js 20+, Zola 0.22.1, and GitHub CLI authenticated with
`gh auth login` or `GH_TOKEN`. From the repository root:

```sh
node site/scripts/release.cjs
node --test site/tests/*.test.cjs
zola --root site check --skip-external-links
zola --root site serve
```

Release metadata is generated from public GitHub assets. GitHub Pages builds the
playground from that release's commit; local playground builds use
`scripts/publish-wasm.sh` and the generated files in `crates/citadel-wasm/pkg`.
