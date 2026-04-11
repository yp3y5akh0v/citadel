const { execSync } = require("child_process");
const { readFileSync, writeFileSync, copyFileSync } = require("fs");
const { join } = require("path");

const root = execSync("git rev-parse --show-toplevel", { encoding: "utf8" }).trim();
const pkg = join(root, "crates", "citadel-wasm", "pkg");

// Build wasm package
execSync("wasm-pack build crates/citadel-wasm --target web --release --scope citadeldb", {
  cwd: root,
  stdio: "inherit",
});

// Patch generated package.json
const manifest = JSON.parse(readFileSync(join(pkg, "package.json"), "utf8"));

manifest.name = "@citadeldb/wasm";
manifest.files = [
  "citadel_wasm_bg.wasm",
  "citadel_wasm.js",
  "citadel_wasm.d.ts",
  "LICENSE-APACHE",
  "LICENSE-MIT",
];
manifest.sideEffects = ["./citadel_wasm.js", "./snippets/*"];

writeFileSync(join(pkg, "package.json"), JSON.stringify(manifest, null, 2) + "\n");

// Copy license files
copyFileSync(join(root, "LICENSE-APACHE"), join(pkg, "LICENSE-APACHE"));
copyFileSync(join(root, "LICENSE-MIT"), join(pkg, "LICENSE-MIT"));

console.log(`\nPackage ready at crates/citadel-wasm/pkg/`);
console.log(`To publish: cd crates/citadel-wasm/pkg && npm publish --access public`);
