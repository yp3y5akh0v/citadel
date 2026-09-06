const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { test } = require('node:test');
const vm = require('node:vm');

const SITE = join(__dirname, '..');
const HOME_SOURCE = readFileSync(join(SITE, 'static/js/home.js'), 'utf8');
const DATA_TEXT = readFileSync(join(SITE, 'data/sql-benchmarks.json'), 'utf8');
const DATA = JSON.parse(DATA_TEXT);
const INVENTORY = {
  execution: `covered_count covered_range delete delete_returning fk_cascade fk_cascade_delete_only
    fts_match fts_phrase fts_rank_first_execution insert insert_gen_stored insert_gen_virtual
    insert_returning insert_select join_param json_extract savepoint_create savepoint_nested
    savepoint_rollback scan sort_paginate_pk truncate update update_gen_propagate update_returning
    upsert_all_new upsert_counter upsert_dedup upsert_mixed upsert_returning wide_proj_2col
    wide_proj_3col wide_proj_full wide_proj_pk window_agg window_rank with_dml`.split(/\s+/).sort(),
  cached: `correlated_exists correlated_in correlated_scalar count cte distinct filter fts_rank
    full_outer_join group_by join jsonb_contains partial_index_point point recursive_cte
    select_gen_virtual sort sort_nocase sum union view_filter view_point`.split(/\s+/).sort(),
  only: `date_arith date_extract date_groupby date_range_scan date_sort json_table lateral`.split(' ').sort(),
  index: ['fts_index', 'json_gin'],
};

function element(fragment = false) {
  return {
    fragment, children: [], className: '', innerHTML: '',
    appendChild(child) {
      this.children.push(...(child.fragment ? child.children : [child]));
      return child;
    },
  };
}

function render(data = DATA) {
  const tables = new Map([['benchTable', element()], ['memoTable', element()]]);
  const document = {
    getElementById: id => id === 'sqlBenchmarks'
      ? (data === null ? null : { textContent: JSON.stringify(data) }) : tables.get(id) || null,
    querySelectorAll: () => [],
    querySelector: () => null,
    createElement: () => element(),
    createDocumentFragment: () => element(true),
  };
  vm.runInNewContext(HOME_SOURCE, {
    document,
    window: { addEventListener() {} },
    IntersectionObserver: class { observe() {} unobserve() {} },
  }, { filename: 'home.js', timeout: 1000 });
  return new Map([...tables].map(([id, table]) => {
    const rows = new Map(table.children.map(row => {
      assert.equal(row.className, 'row');
      assert.doesNotMatch(row.innerHTML, /NaN|Infinity/);
      const name = row.innerHTML.match(/class="name mono">([^<]+)</);
      assert.ok(name, 'rendered row has a benchmark name');
      return [name[1], row.innerHTML];
    }));
    assert.equal(rows.size, table.children.length, 'benchmark names are unique');
    return [id, rows];
  }));
}

function fixture(name, ratio, citadel = 100_000) {
  return { name, source: 'fixture', samples_ns: {
    citadel: [citadel, citadel], sqlite: [citadel * ratio, citadel * ratio],
  } };
}

const fixtureData = (execution, cached = execution) => ({ execution, cached });
const visibleText = html => html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
const mean = values => (values[0] + values[1]) / 2;
const rounded = value => Number(value.toPrecision(3));

function cells(html) {
  return {
    times: [...html.matchAll(/class="num">([^<]+)</g)].map(match => match[1]),
    ratio: html.match(/class="ratio(?: ratio-neutral)?">([^<]+)</)[1],
  };
}

function measurement(text) {
  const match = text.match(/^([\d.]+(?:e[+-]?\d+)?)\s*(ns|us|µs|ms|s|x|×)$/i);
  assert.ok(match, `valid displayed measurement: ${text}`);
  const scale = { ns: 1, us: 1e3, 'µs': 1e3, ms: 1e6, s: 1e9, x: 1, '×': 1 }[match[2]];
  return Number(match[1]) * scale;
}

function assertTime(text, ns) {
  const unit = text.split(' ')[1];
  const scale = { ns: 1, us: 1e3, ms: 1e6, s: 1e9 }[unit];
  assert.ok(scale, `supported time unit: ${unit}`);
  assert.equal(measurement(text), rounded(ns / scale) * scale);
}

function assertNeutral(html) {
  assert.doesNotMatch(html, /class="bar"|width:/, 'no positive-speedup bar for a tie or loss');
  assert.match(html, /class="ratio ratio-neutral"/);
  assert.match(html, /class="relative-neutral"/);
}

function width(html) {
  const match = html.match(/width:\s*([\d.]+)%/);
  assert.ok(match, 'positive-speedup row has a percentage bar');
  const value = Number(match[1]);
  assert.ok(Number.isFinite(value) && value > 0 && value <= 100);
  return value;
}

test('both tables identify SQLite wins and retain the SQLite/Citadel ratio', () => {
  const data = fixtureData([fixture('citadel_win', 2), fixture('sqlite_win', 0.55), fixture('larger_loss', 0.25)]);
  for (const rows of render(data).values()) {
    assert.equal(width(rows.get('citadel_win')), 100);
    assert.match(visibleText(rows.get('sqlite_win')), /SQLite 1\.82× faster/);
    assert.equal(measurement(cells(rows.get('sqlite_win')).ratio), 0.55);
    assert.match(visibleText(rows.get('larger_loss')), /SQLite 4× faster/);
    assert.equal(measurement(cells(rows.get('larger_loss')).ratio), 0.25);
    assertNeutral(rows.get('sqlite_win'));
    assertNeutral(rows.get('larger_loss'));
  }
});

test('empty tables and pages without embedded benchmark data render safely', () => {
  for (const data of [fixtureData([]), null]) {
    for (const rows of render(data).values()) assert.equal(rows.size, 0);
  }
});

test('all ties and all losses stay neutral without invalid logarithmic widths', () => {
  for (const rows of render(fixtureData([fixture('tie_a', 1), fixture('tie_b', 1)])).values()) {
    for (const html of rows.values()) {
      assert.match(visibleText(html), /Equal measured time/);
      assert.equal(measurement(cells(html).ratio), 1);
      assertNeutral(html);
    }
  }
  for (const rows of render(fixtureData([fixture('loss_a', 0.5), fixture('loss_b', 0.25)])).values()) {
    assert.match(visibleText(rows.get('loss_a')), /SQLite 2×/);
    assert.match(visibleText(rows.get('loss_b')), /SQLite 4×/);
    for (const html of rows.values()) assertNeutral(html);
  }
});

test('each unsorted table scales bars against its actual largest ratio', () => {
  const tables = render(fixtureData([
    fixture('small', 2), fixture('maximum', 16), fixture('middle', 4),
  ], [fixture('cached_small', 10), fixture('cached_maximum', 100)]));
  const execution = tables.get('benchTable');
  assert.equal(width(execution.get('small')), 25);
  assert.equal(width(execution.get('maximum')), 100);
  assert.equal(width(execution.get('middle')), 50);
  assert.equal(width(tables.get('memoTable').get('cached_small')), 50);
  assert.equal(width(tables.get('memoTable').get('cached_maximum')), 100);
});

test('ratios use unrounded mean-of-two medians and times use three significant digits', () => {
  const data = fixtureData([
    { name: 'unequal_passes', samples_ns: { citadel: [1000, 9000], sqlite: [9000, 9000] } },
    { name: 'rounding', samples_ns: { citadel: [1004, 1004], sqlite: [1046, 1046] } },
    ...[123, 1234, 1234567, 1234567890].map((ns, index) => fixture(`unit_${index}`, 2, ns)),
  ]);
  for (const rows of render(data).values()) {
    assert.deepEqual(cells(rows.get('unequal_passes')), { times: ['5 us', '9 us'], ratio: '1.8×' });
    assert.deepEqual(cells(rows.get('rounding')), { times: ['1 us', '1.05 us'], ratio: '1.04×' });
    assert.deepEqual([0, 1, 2, 3].map(index => cells(rows.get(`unit_${index}`)).times[0]),
      ['123 ns', '1.23 us', '1.23 ms', '1.23 s']);
  }
});

test('public data contains all 129 IDs with valid samples and public source provenance', () => {
  assert.equal(DATA.schema_version, 1);
  assert.equal(DATA.measurement.passes, 2);
  assert.equal(DATA.measurement.samples_per_pass, 100);
  assert.deepEqual(Object.keys(DATA.sources).sort(), ['baseline', 'post_fix_fts', 'post_fix_update', 'post_fix_window']);
  for (const name of ['baseline', 'post_fix_fts', 'post_fix_window', 'post_fix_update']) {
    const source = DATA.sources[name];
    assert.deepEqual(Object.keys(source).sort(), ['executable_sha256', 'revision']);
    assert.match(source.revision, /^[a-f0-9]{40}$/);
    assert.match(source.executable_sha256, /^[a-f0-9]{64}$/);
  }
  assert.deepEqual(DATA.execution.filter(row => row.source === 'post_fix_update')
    .map(row => row.name).sort(), [
    'update', 'update_gen_propagate', 'update_returning',
    'upsert_counter', 'upsert_mixed', 'upsert_returning',
  ]);
  assert.doesNotMatch(DATA_TEXT, /[A-Z]:[\\/]|\/(?:Users|home)\/|notes[\\/]|\.exe\b|source[\\/]repos/i);
  assert.doesNotMatch(DATA_TEXT, /</, 'embedded JSON cannot contain HTML tag delimiters');
  const names = new Set();
  const sourceIds = { baseline: 0, post_fix_fts: 0, post_fix_window: 0, post_fix_update: 0 };
  for (const [category, expectedNames] of Object.entries(INVENTORY)) {
    assert.deepEqual(DATA[category].map(row => row.name).sort(), expectedNames);
    for (const row of DATA[category]) {
      assert.ok(!names.has(row.name), `unique case: ${row.name}`);
      names.add(row.name);
      assert.ok(Object.hasOwn(DATA.sources, row.source), `known source: ${row.name}`);
      const expectedArms = category === 'only' ? ['citadel']
        : category === 'index' ? [row.name === 'fts_index' ? 'fts_index' : 'gin_index', 'seq_scan']
          : ['citadel', 'sqlite'];
      assert.deepEqual(Object.keys(row.samples_ns).sort(), expectedArms);
      for (const samples of Object.values(row.samples_ns)) {
        assert.equal(samples.length, 2, row.name);
        assert.ok(samples.every(value => typeof value === 'number' && Number.isFinite(value) && value > 0), row.name);
        sourceIds[row.source]++;
      }
    }
  }
  assert.equal(names.size, 68);
  assert.deepEqual(sourceIds, { baseline: 103, post_fix_fts: 10, post_fix_window: 4, post_fix_update: 12 });
  assert.deepEqual(DATA.configuration, {
    citadel: { sync: 'Off', page_size: 8208, cached_body_bytes: 8160, cache_pages: 4096 },
    sqlite: { journal_mode: 'MEMORY', synchronous: 'OFF', page_size: 8192, cache_pages: 4096 },
  });
});

test('production script renders exactly 37 execution and 22 cached cases from real data', () => {
  const tables = render();
  for (const [category, tableId] of [['execution', 'benchTable'], ['cached', 'memoTable']]) {
    const rows = tables.get(tableId);
    assert.deepEqual([...rows.keys()].sort(), INVENTORY[category]);
    for (const row of DATA[category]) {
      const html = rows.get(row.name);
      const display = cells(html);
      const citadel = mean(row.samples_ns.citadel);
      const sqlite = mean(row.samples_ns.sqlite);
      assertTime(display.times[0], citadel);
      assertTime(display.times[1], sqlite);
      assert.equal(measurement(display.ratio), rounded(sqlite / citadel));
      if (sqlite <= citadel) assertNeutral(html);
      else width(html);
    }
  }
  assert.equal(tables.get('benchTable').size, 37);
  assert.equal(tables.get('memoTable').size, 22);
});

test('README tables match all 68 public cases and the configured 59 head-to-head pairs', () => {
  const readme = readFileSync(join(SITE, '../README.md'), 'utf8');
  const config = readFileSync(join(SITE, 'config.toml'), 'utf8');
  assert.equal(Number(config.match(/^benchmarks\s*=\s*(\d+)\s*$/m)?.[1]), 59);
  assert.ok(/\b59\s+(?:head-to-head|cross-engine|cases)\b/.test(readme), 'README states 59 paired cases');
  const rows = new Map();
  for (const block of readme.matchAll(/^```[^\r\n]*\r?\n([\s\S]*?)^```/gm)) {
    if (!/^Benchmark\s+(?:Citadel|Without index)/m.test(block[1])) continue;
    for (const line of block[1].split(/\r?\n/)) {
      if (!/^[a-z][a-z0-9_]*\s{2,}\d/.test(line)) continue;
      const [name, ...values] = line.trim().split(/\s{2,}/);
      assert.ok(!rows.has(name), `unique README row: ${name}`);
      rows.set(name, values);
    }
  }
  assert.deepEqual([...rows.keys()].sort(), Object.values(INVENTORY).flat().sort());
  const browser = render();
  for (const [category, cases] of Object.entries(INVENTORY)) {
    for (const name of cases) {
      const row = DATA[category].find(row => row.name === name);
      const values = rows.get(name);
      const indexed = name === 'fts_index' ? 'fts_index' : 'gin_index';
      const first = mean(row.samples_ns[category === 'index' ? 'seq_scan' : 'citadel']);
      assertTime(values[0], first);
      if (category === 'only') {
        assert.deepEqual(values.slice(1), ['-', '-'], name);
        continue;
      }
      assert.equal(values.length, 3, name);
      const second = mean(row.samples_ns[category === 'index' ? indexed : 'sqlite']);
      assertTime(values[1], second);
      assert.equal(measurement(values[2]), rounded(category === 'index' ? first / second : second / first), name);
      if (category !== 'index') {
        const html = browser.get(category === 'execution' ? 'benchTable' : 'memoTable').get(name);
        const display = cells(html);
        assert.deepEqual(values.map(measurement), [...display.times, display.ratio].map(measurement), name);
      }
    }
  }
});
