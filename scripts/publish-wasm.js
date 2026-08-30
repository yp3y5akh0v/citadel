const { execSync } = require("child_process");
const { readFileSync, writeFileSync, copyFileSync } = require("fs");
const { join } = require("path");

const root = execSync("git rev-parse --show-toplevel", { encoding: "utf8" }).trim();
const pkg = join(root, "crates", "citadel-wasm", "pkg");

// Build wasm package. Size opt level is scoped here so native builds keep speed.
execSync("wasm-pack build crates/citadel-wasm --target web --release --scope citadeldb -- --locked", {
  cwd: root,
  stdio: "inherit",
  env: { ...process.env, CARGO_PROFILE_RELEASE_OPT_LEVEL: "z" },
});

// Patch generated package.json
const manifest = JSON.parse(readFileSync(join(pkg, "package.json"), "utf8"));

manifest.name = "@citadeldb/wasm";
if (!Array.isArray(manifest.files)) {
  throw new Error("wasm-pack package.json did not contain a files list");
}
if (!manifest.files.includes("LICENSE-APACHE")) {
  manifest.files.push("LICENSE-APACHE");
}
manifest.sideEffects = ["./citadel_wasm.js", "./snippets/*"];

writeFileSync(join(pkg, "package.json"), JSON.stringify(manifest, null, 2) + "\n");

// Copy license files
copyFileSync(join(root, "LICENSE-APACHE"), join(pkg, "LICENSE-APACHE"));

console.log(`\nPackage ready at crates/citadel-wasm/pkg/`);
console.log(`To publish: cd crates/citadel-wasm/pkg && npm publish --access public`);
