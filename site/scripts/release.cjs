const { execFileSync } = require('node:child_process');
const { createHash, randomUUID } = require('node:crypto');
const { appendFileSync, mkdirSync, renameSync, rmSync, writeFileSync } = require('node:fs');
const { join } = require('node:path');

const REPO = 'yp3y5akh0v/citadel';
const REPO_URL = `https://github.com/${REPO}`;
const TARGETS = [
  ['Linux x86_64', 'x86_64-unknown-linux-musl', 'x86_64-unknown-linux-gnu', 'tar.gz'],
  ['Linux ARM64', 'aarch64-unknown-linux-gnu', 'aarch64-unknown-linux-gnu', 'tar.gz'],
  ['macOS Apple Silicon', 'aarch64-apple-darwin', 'aarch64-apple-darwin', 'tar.gz'],
  ['macOS Intel', 'x86_64-apple-darwin', 'x86_64-apple-darwin', 'tar.gz'],
  ['Windows x86_64', 'x86_64-pc-windows-msvc', 'x86_64-pc-windows-msvc', 'zip'],
];
const STUDIO_TARGETS = {
  windows: 'x86_64-pc-windows-msvc.msi',
  macos_arm64: 'aarch64-apple-darwin.dmg',
  macos_x86_64: 'x86_64-apple-darwin.dmg',
  linux: 'x86_64-unknown-linux-gnu.AppImage',
};

function version(tag) {
  const match = /^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.exec(tag);
  return match ? match.slice(1).map(BigInt) : null;
}

function compareVersions(left, right) {
  const a = version(left);
  const b = version(right);
  for (let i = 0; i < 3; i++) {
    if (a[i] !== b[i]) return a[i] > b[i] ? 1 : -1;
  }
  return 0;
}

function selectRelease(releases) {
  const stable = releases.filter(r => r.draft === false && r.prerelease === false
    && Number.isFinite(Date.parse(r.published_at)) && version(r.tag_name));
  stable.sort((a, b) => compareVersions(b.tag_name, a.tag_name));
  if (!stable.length) throw new Error('No published stable Citadel release found');
  return stable[0];
}

function buildManifest(release, assets, commit) {
  if (selectRelease([release]) !== release || !/^[0-9a-f]{40}$/.test(commit)) {
    throw new Error('Invalid released commit');
  }
  const tag = release.tag_name;
  if (release.html_url !== `${REPO_URL}/releases/tag/${tag}`) {
    throw new Error('Release URL does not belong to Citadel');
  }
  const byName = new Map();
  for (const asset of assets) {
    if (byName.has(asset.name)) throw new Error(`Duplicate release asset: ${asset.name}`);
    byName.set(asset.name, asset);
  }
  function asset(name) {
    const found = byName.get(name);
    if (!found || found.state !== 'uploaded' || !Number.isSafeInteger(found.id)
      || found.id <= 0 || !Number.isSafeInteger(found.size) || found.size <= 0) {
      throw new Error(`Missing or incomplete release asset: ${name}`);
    }
    if (found.browser_download_url !== `${REPO_URL}/releases/download/${tag}/${name}`) {
      throw new Error(`Invalid release asset URL: ${name}`);
    }
    if (!/^sha256:[0-9a-f]{64}$/.test(found.digest)) {
      throw new Error(`Missing SHA-256 digest: ${name}`);
    }
    return { id: found.id, name, url: found.browser_download_url, size: found.size, digest: found.digest };
  }
  function archive(name) {
    return { ...asset(name), checksum: asset(`${name}.sha256`) };
  }
  const native = TARGETS.map(([platform, cliTarget, ffiTarget, extension]) => ({
    platform,
    extension,
    cli: archive(`citadel-${tag}-${cliTarget}.${extension}`),
    ffi: archive(`citadel-ffi-${tag}-${ffiTarget}.${extension}`),
    cli_fips: archive(`citadel-fips-${tag}-${cliTarget}.${extension}`),
    ffi_fips: archive(`citadel-ffi-fips-${tag}-${ffiTarget}.${extension}`),
  }));
  const studio = {};
  // Studio installers first ship in 2.2; the 2.1 release has only CLI/FFI assets.
  if (compareVersions(tag, 'v2.2.0') >= 0 || assets.some(a => a.name.startsWith('citadel-studio-'))) {
    for (const [platform, suffix] of Object.entries(STUDIO_TARGETS)) {
      studio[platform] = archive(`citadel-studio-${tag}-${suffix}`);
    }
  }
  return { tag, version: tag.slice(1), url: release.html_url, commit, native, studio };
}

function github(endpoint, paginate = false) {
  const args = ['api', '--method', 'GET', `repos/${REPO}/${endpoint}`];
  if (paginate) args.push('--paginate', '--slurp');
  const result = JSON.parse(execFileSync('gh', args, {
    encoding: 'utf8', timeout: 30_000, maxBuffer: 16 * 1024 * 1024,
  }));
  return paginate ? result.flat() : result;
}

function resolveRelease(api = github) {
  const release = selectRelease(api('releases?per_page=100', true));
  if (!Number.isSafeInteger(release.id) || release.id <= 0) throw new Error('Invalid release ID');
  const assets = api(`releases/${release.id}/assets?per_page=100`, true);
  const commit = api(`commits/tags/${release.tag_name}`).sha;
  return buildManifest(release, assets, commit);
}

function fingerprint(manifest) {
  return createHash('sha256').update(JSON.stringify(manifest)).digest('hex');
}

function isCurrent(manifest, expectedFingerprint, masterSha, expectedSiteSha) {
  return /^[0-9a-f]{40}$/.test(masterSha) && masterSha === expectedSiteSha
    && fingerprint(manifest) === expectedFingerprint;
}

function output(name, value) {
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${value}\n`);
}

function writeManifest(manifest, directory) {
  mkdirSync(directory, { recursive: true });
  const temporary = join(directory, `release.json.${randomUUID()}.tmp`);
  writeFileSync(temporary, `${JSON.stringify(manifest, null, 2)}\n`, { flag: 'wx' });
  try {
    renameSync(temporary, join(directory, 'release.json'));
  } finally {
    rmSync(temporary, { force: true });
  }
}

function main(args) {
  if (args.length > 1 || (args.length === 1 && args[0] !== '--check-current')) {
    throw new Error('Usage: node site/scripts/release.cjs [--check-current]');
  }
  const manifest = resolveRelease();
  if (args[0] === '--check-current') {
    const current = isCurrent(manifest, process.env.EXPECTED_RELEASE,
      github('commits/heads/master').sha, process.env.EXPECTED_SITE_SHA);
    output('current', current);
    console.log(current ? 'Site and release are current' : 'A newer site or release superseded this build');
    return;
  }
  writeManifest(manifest, join(__dirname, '..', 'data'));
  output('release_sha', manifest.commit);
  output('release_fingerprint', fingerprint(manifest));
  console.log(`Downloads: ${manifest.tag}; Studio installers: ${Object.keys(manifest.studio).length}`);
}

module.exports = { REPO_URL, selectRelease, buildManifest, resolveRelease, fingerprint, isCurrent, writeManifest };
if (require.main === module) {
  try { main(process.argv.slice(2)); } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
