const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const { cpSync, mkdtempSync, readFileSync, rmSync } = require('node:fs');
const { tmpdir } = require('node:os');
const { basename, dirname, join, resolve } = require('node:path');
const { test } = require('node:test');
const {
  selectRelease, buildManifest, resolveRelease, fingerprint, isCurrent, writeManifest,
} = require('../scripts/release.cjs');

const REPO = 'https://github.com/yp3y5akh0v/citadel';
const COMMIT = 'a'.repeat(40);
const SITE_COMMIT = 'b'.repeat(40);
const NATIVE_NAMES = [
  'citadel-VERSION-x86_64-unknown-linux-musl.tar.gz',
  'citadel-VERSION-aarch64-unknown-linux-gnu.tar.gz',
  'citadel-VERSION-x86_64-apple-darwin.tar.gz',
  'citadel-VERSION-aarch64-apple-darwin.tar.gz',
  'citadel-VERSION-x86_64-pc-windows-msvc.zip',
  'citadel-ffi-VERSION-x86_64-unknown-linux-gnu.tar.gz',
  'citadel-ffi-VERSION-aarch64-unknown-linux-gnu.tar.gz',
  'citadel-ffi-VERSION-x86_64-apple-darwin.tar.gz',
  'citadel-ffi-VERSION-aarch64-apple-darwin.tar.gz',
  'citadel-ffi-VERSION-x86_64-pc-windows-msvc.zip',
  'citadel-fips-VERSION-x86_64-unknown-linux-musl.tar.gz',
  'citadel-fips-VERSION-aarch64-unknown-linux-gnu.tar.gz',
  'citadel-fips-VERSION-x86_64-apple-darwin.tar.gz',
  'citadel-fips-VERSION-aarch64-apple-darwin.tar.gz',
  'citadel-fips-VERSION-x86_64-pc-windows-msvc.zip',
  'citadel-ffi-fips-VERSION-x86_64-unknown-linux-gnu.tar.gz',
  'citadel-ffi-fips-VERSION-aarch64-unknown-linux-gnu.tar.gz',
  'citadel-ffi-fips-VERSION-x86_64-apple-darwin.tar.gz',
  'citadel-ffi-fips-VERSION-aarch64-apple-darwin.tar.gz',
  'citadel-ffi-fips-VERSION-x86_64-pc-windows-msvc.zip',
];
const STUDIO_NAMES = [
  'citadel-studio-VERSION-x86_64-unknown-linux-gnu.AppImage',
  'citadel-studio-VERSION-x86_64-apple-darwin.dmg',
  'citadel-studio-VERSION-aarch64-apple-darwin.dmg',
  'citadel-studio-VERSION-x86_64-pc-windows-msvc.msi',
];

function fixture(tag = 'v2.2.0', studio = true) {
  const names = [...NATIVE_NAMES, ...(studio ? STUDIO_NAMES : [])]
    .map(name => name.replace('VERSION', tag));
  const assets = names.flatMap(name => [name, `${name}.sha256`]).map((name, index) => ({
    id: index + 1, name, state: 'uploaded', size: 1024,
    digest: `sha256:${'a'.repeat(64)}`,
    browser_download_url: `${REPO}/releases/download/${tag}/${name}`,
  }));
  const release = {
    id: 123, tag_name: tag, html_url: `${REPO}/releases/tag/${tag}`,
    draft: false, prerelease: false, published_at: '2026-09-01T00:00:00Z',
  };
  return { release, assets, manifest: () => buildManifest(release, assets, COMMIT) };
}

test('selects highest stable version, not publish order, lexical order, draft or prerelease', () => {
  const releases = ['v2.1.1', 'v2.10.0', 'v2.9.0', 'v2.2.0'].map(tag => fixture(tag).release);
  releases.push({ ...fixture('v3.0.0').release, draft: true });
  releases.push({ ...fixture('v4.0.0').release, prerelease: true });
  releases.push(fixture('v5.0.0-rc.1').release);
  assert.equal(selectRelease(releases).tag_name, 'v2.10.0');
  assert.equal(selectRelease(releases.reverse()).tag_name, 'v2.10.0');
});

test('rejects missing public stable releases and malformed version tags', () => {
  for (const tag of ['2.2.0', 'v02.2.0', 'v2.02.0', 'v2.2.00', 'v2.2.0+build', 'v2.2.0/evil']) {
    assert.throws(() => selectRelease([fixture(tag).release]), /No published stable/);
  }
  for (const fields of [{ draft: true }, { prerelease: true }, { published_at: null }, { draft: undefined }]) {
    assert.throws(() => selectRelease([{ ...fixture().release, ...fields }]), /No published stable/);
  }
  assert.throws(() => selectRelease([]), /No published stable/);
});

test('2.1 downloads retain all native packages without inventing Studio downloads', () => {
  const { assets, manifest } = fixture('v2.1.0', false);
  const data = manifest();
  assert.equal(data.native.length, 5);
  assert.deepEqual(data.studio, {});
  assert.equal(assets.length, 40);
  assert.match(data.native[0].cli.url, /x86_64-unknown-linux-musl/);
  assert.match(data.native[0].ffi.url, /x86_64-unknown-linux-gnu/);
});

test('2.2 exposes all four Studio installers with their actual checksum assets', () => {
  const data = fixture().manifest();
  assert.deepEqual(Object.keys(data.studio), ['windows', 'macos_arm64', 'macos_x86_64', 'linux']);
  assert.match(data.studio.windows.url, /x86_64-pc-windows-msvc\.msi$/);
  assert.match(data.studio.macos_arm64.url, /aarch64-apple-darwin\.dmg$/);
  assert.match(data.studio.macos_x86_64.url, /x86_64-apple-darwin\.dmg$/);
  assert.match(data.studio.linux.url, /x86_64-unknown-linux-gnu\.AppImage$/);
});

test('every required package and checksum is individually enforced', () => {
  for (const [tag, studio] of [['v2.1.0', false], ['v2.2.0', true]]) {
    const { release, assets } = fixture(tag, studio);
    for (const missing of assets) {
      assert.throws(() => buildManifest(release, assets.filter(a => a !== missing), COMMIT),
        /Missing or incomplete release asset/, missing.name);
    }
  }
});

test('future stable versions require Studio, and earlier versions cannot expose a partial set', () => {
  for (const tag of ['v2.2.0', 'v2.10.0', 'v3.0.0']) {
    assert.throws(() => fixture(tag, false).manifest(), /Missing or incomplete/);
  }
  assert.deepEqual(fixture('v2.1.99', false).manifest().studio, {});
  const { release, assets } = fixture('v2.1.1', true);
  assert.throws(() => buildManifest(release, assets.slice(0, -1), COMMIT), /Missing or incomplete/);
});

test('rejects duplicate, empty, unfinished, unhashable and redirected assets', () => {
  const { release, assets } = fixture();
  assert.throws(() => buildManifest(release, [...assets, assets[0]], COMMIT), /Duplicate/);
  for (const fields of [
    { size: 0 }, { size: -1 }, { size: 1.5 }, { state: 'starter' }, { id: 0 },
    { digest: null }, { digest: 'sha256:wrong' },
    { browser_download_url: assets[0].browser_download_url.replace('github.com', 'example.com') },
    { browser_download_url: assets[0].browser_download_url.replace('v2.2.0', 'v2.1.0') },
    { browser_download_url: `${assets[0].browser_download_url}?token=secret` },
    { browser_download_url: assets[0].browser_download_url.replace('https://', 'http://') },
    { browser_download_url: assets[0].browser_download_url.replace('/citadel/', '/another/') },
  ]) {
    assert.throws(() => buildManifest(release, [{ ...assets[0], ...fields }, ...assets.slice(1)], COMMIT));
  }
  assert.throws(() => buildManifest({ ...release, html_url: 'https://example.com' }, assets, COMMIT), /Release URL/);
  assert.throws(() => buildManifest(release, assets, 'master'), /Invalid released commit/);
});

test('resolver fetches paginated assets and uses the tag commit, not target_commitish', () => {
  const { release, assets } = fixture();
  release.target_commitish = 'master';
  const calls = [];
  const data = resolveRelease((endpoint, paginate) => {
    calls.push([endpoint, paginate]);
    if (endpoint === 'releases?per_page=100') return [release];
    if (endpoint === 'releases/123/assets?per_page=100') return assets;
    if (endpoint === 'commits/tags/v2.2.0') return { sha: COMMIT };
    throw new Error(`Unexpected endpoint ${endpoint}`);
  });
  assert.equal(data.commit, COMMIT);
  assert.deepEqual(calls, [
    ['releases?per_page=100', true], ['releases/123/assets?per_page=100', true], ['commits/tags/v2.2.0', undefined],
  ]);
});

test('an incomplete latest release fails instead of falling back to an older release', () => {
  const latest = fixture();
  assert.throws(() => resolveRelease(endpoint => {
    if (endpoint === 'releases?per_page=100') return [fixture('v2.1.0', false).release, latest.release];
    if (endpoint.startsWith('commits/')) return { sha: COMMIT };
    return latest.assets.slice(1);
  }), /Missing or incomplete/);
  assert.throws(() => resolveRelease(() => { throw new Error('API unavailable'); }), /API unavailable/);
});

test('downloads and API ordering do not invalidate a build, but changed source or assets do', () => {
  const f = fixture();
  const data = f.manifest();
  const hash = fingerprint(data);
  const shuffled = buildManifest(f.release, f.assets.toReversed().map(a => ({ ...a, download_count: 999 })), COMMIT);
  assert.equal(fingerprint(shuffled), hash);
  assert.equal(isCurrent(data, hash, SITE_COMMIT, SITE_COMMIT), true);
  assert.equal(isCurrent(data, hash, COMMIT, SITE_COMMIT), false);
  assert.equal(isCurrent(data, hash, undefined, undefined), false);
  f.assets[0].id += 100;
  assert.equal(isCurrent(f.manifest(), hash, SITE_COMMIT, SITE_COMMIT), false);
  assert.equal(isCurrent(fixture('v2.3.0').manifest(), hash, SITE_COMMIT, SITE_COMMIT), false);
});

for (const [tag, studio] of [['v2.1.0', false], ['v2.2.0', true]]) {
  test(`Zola renders ${tag} using only available release download URLs`, () => {
    const directory = mkdtempSync(join(tmpdir(), 'citadel-site-release-test-'));
    try {
      const source = resolve(__dirname, '..');
      cpSync(source, directory, {
        recursive: true,
        filter: path => !['public', 'wasm'].includes(basename(path))
          && path !== join(source, 'data', 'release.json'),
      });
      const f = fixture(tag, studio);
      writeManifest(f.manifest(), join(directory, 'data'));
      execFileSync('zola', ['--root', directory, 'build'], { encoding: 'utf8', timeout: 30_000 });
      const homepage = readFileSync(join(directory, 'public/index.html'), 'utf8');
      const benchmarkScripts = [...homepage.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/g)]
        .filter(match => /\bid=(?:"sqlBenchmarks"|sqlBenchmarks)(?=\s|$)/.test(match[1]));
      assert.equal(benchmarkScripts.length, 1);
      assert.match(benchmarkScripts[0][1], /\btype=(?:"application\/json"|application\/json)(?=\s|$)/);
      assert.deepEqual(JSON.parse(benchmarkScripts[0][2]),
        JSON.parse(readFileSync(join(source, 'data/sql-benchmarks.json'), 'utf8')));
      const html = readFileSync(join(directory, 'public/download/index.html'), 'utf8');
      const links = [...html.matchAll(/\bhref=(?:"([^"]*)"|([^\s>]+))/g)]
        .map(match => match[1] ?? match[2])
        .filter(url => url.startsWith(`${REPO}/releases/download/`));
      const expected = f.assets
        .filter(a => !a.name.endsWith('.sha256') || a.name.startsWith('citadel-studio-'))
        .map(a => a.browser_download_url);
      assert.deepEqual(links.toSorted(), expected.toSorted());
      assert.equal(html.includes('Studio installers are not included'), !studio);
      if (studio) {
        const studioSection = html.slice(
          html.indexOf('<section class=block id=studio>'),
          html.indexOf('<section class=block id=archives>'),
        );
        const buttonLabels = [...studioSection.matchAll(
          /<a class="btn sm" href=(?:"[^"]+"|[^\s>]+)>([^<]+)<\/a>/g,
        )].map(match => match[1]);
        assert.equal((studioSection.match(/<p class=download-actions>/g) ?? []).length, 4);
        assert.deepEqual(buttonLabels, [
          'Download MSI',
          'Apple Silicon DMG',
          'Intel DMG',
          'Download AppImage',
        ]);
      }
      assert.ok(html.includes(`citadeldb / ${tag}`));
      assert.ok(!html.includes('version-chip.js'));
      assert.ok(!html.includes('does not include GitHub build attestations'));
      const demo = readFileSync(join(directory, 'public/demo/index.html'), 'utf8');
      assert.ok(demo.includes(`Playground / ${tag}`));
    } finally {
      assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
      assert.ok(basename(directory).startsWith('citadel-site-release-test-'));
      rmSync(directory, { recursive: true, force: true });
    }
  });
}

test('download link styles preserve the button foreground', () => {
  const css = readFileSync(join(__dirname, '..', 'static/css/style.css'), 'utf8');
  assert.match(css, /\.btn\s*\{[^}]*color:\s*var\(--accent-ink\)/s);
  assert.match(css, /\.binding p a:not\(\.btn\)/);
  assert.match(css, /\.binding \.download-actions\s*\{[^}]*gap:\s*12px/s);
  assert.match(css, /\.btn:focus-visible\s*\{[^}]*outline:/s);
});
