const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { test } = require('node:test');
const vm = require('node:vm');

function playground(run = () => []) {
  const elements = new Map();
  const document = {
    readyState: 'complete',
    querySelectorAll: () => [],
    getElementById(id) {
      if (id === 'pageRing' || id === 'sessTime') return null;
      if (!elements.has(id)) elements.set(id, {
        value: '', innerHTML: '', textContent: '',
        addEventListener() {},
        classList: { add() {}, remove() {}, toggle() {} },
      });
      return elements.get(id);
    },
  };
  const context = { window: {}, document, performance, setTimeout() {} };
  vm.runInNewContext(readFileSync(join(__dirname, '../static/js/playground.js'), 'utf8'), context);
  context.window.CitadelPlayground.onReady(run);
  return {
    format(sql) {
      document.getElementById('sqlInput').value = sql;
      elements.get('fmtBtn').onclick();
      return elements.get('sqlInput').value;
    },
    run(sql) {
      elements.get('sqlInput').value = sql;
      elements.get('runBtn').onclick();
      return elements.get('resultBody').innerHTML;
    },
    highlighted: () => elements.get('hl').innerHTML,
  };
}

const protectedText = [
  "'select from'",
  "'it''s select\nfrom'",
  '"select"',
  '"a""from"',
  '`select from`',
  '$$select from$$',
  '$body$select $other$ from$body$',
  String.raw`E'it\'s select from'`,
  String.raw`'\' || 'select from'`,
  "U&'select\\0061'",
  "N'select from', B'0101', X'FF'",
  String.raw`X'\'select'`,
  "R'''select 'from' where'''",
  'R"""select "from" where"""',
  '$Body$select $body$ from$Body$',
  '$1$ select from $1$',
  '$1tag$select from where$1tag$',
  '1::integer',
  '/* select /* from */ where */ 1',
  '1 -- select from\r\n',
  '1 -- select from\r',
  ':limit, @where, $select, $1',
  'select$from, éselect, selecté, name_from',
  'ſelect, ın, ſum',
];

for (const text of protectedText) {
  test(`format preserves ${JSON.stringify(text)}`, () => {
    const ui = playground();
    assert.equal(ui.format(`select ${text} from users;`), `SELECT ${text} FROM users;`);
  });
}

test('format only changes keyword case and is idempotent', () => {
  const ui = playground();
  const expected = "SELECT name\nFROM users WHERE age > 30 ORDER BY name LIMIT 2;";
  assert.equal(ui.format('select name\nfrom users where age > 30 order by name limit 2;'), expected);
  assert.equal(ui.format(expected), expected);
});

test('format leaves spacing and CRLF unchanged', () => {
  const ui = playground();
  assert.equal(ui.format("\tselect  'from'\r\nfrom\tusers;  "), "\tSELECT  'from'\r\nFROM\tusers;  ");
});

test('format retains DO NOTHING keyword casing', () => {
  assert.equal(playground().format('insert into users values (1) on conflict do nothing;'),
    'INSERT INTO users VALUES (1) ON CONFLICT DO NOTHING;');
});

for (const text of ["'select from", '"select from', '/* select from', '$tag$select from']) {
  test(`format preserves unfinished ${JSON.stringify(text)}`, () => {
    assert.equal(playground().format(`select ${text}`), `SELECT ${text}`);
  });
}

test('highlighter preserves and escapes a quoted value', () => {
  const ui = playground();
  ui.format("select '<select>''from' /* where */");
  assert.match(ui.highlighted(), /<span class="s">'&lt;select&gt;''from'<\/span>/);
  assert.match(ui.highlighted(), /<span class="c">\/\* where \*\/<\/span>/);
});

test('highlighter recognizes function names containing underscores', () => {
  const ui = playground();
  ui.format('select row_number() over (), date_trunc(\'year\', now());');
  assert.match(ui.highlighted(), /<span class="fn">row_number<\/span>/);
  assert.match(ui.highlighted(), /<span class="fn">date_trunc<\/span>/);
});

test('affected rows do not claim an open transaction was committed', () => {
  const ui = playground(() => [{ type: 'ok' }, { type: 'rowsAffected', value: 1 }]);
  const html = ui.run('BEGIN; INSERT INTO users VALUES (1, \'Alice\', 30);');
  assert.match(html, /1 row affected/);
  assert.doesNotMatch(html, /committed/i);
});
