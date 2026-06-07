window.CitadelPlayground = (() => {
  const SEED = [
    `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER);`,
    `INSERT INTO users (id, name, age) VALUES
      (50000,'alice',42),(50001,'bob',27),(50002,'carol',35),(50003,'dmitri',51),
      (50004,'eun-ji',29),(50005,'farah',38),(50006,'gunnar',46),(50007,'hiroshi',33),
      (50008,'isla',24),(50009,'jun',60),(50010,'kamila',31),(50011,'liu',44);`,
    `CREATE TABLE events (id INTEGER PRIMARY KEY, user_id INTEGER, kind TEXT, ts TIMESTAMP);`,
    `INSERT INTO events (id, user_id, kind, ts) VALUES
      (1,50000,'login','2024-02-03 14:02:11'),(2,50000,'query','2024-02-03 14:05:02'),
      (3,50001,'login','2024-02-04 09:11:44'),(4,50002,'commit','2024-02-15 22:30:09'),
      (5,50000,'rekey','2024-02-28 11:04:51'),(6,50003,'login','2024-03-02 08:00:00'),
      (7,50004,'sync','2024-03-10 16:44:18'),(8,50000,'backup','2024-03-29 12:00:00');`,
    `CREATE TABLE ref_table (id INTEGER PRIMARY KEY, val INTEGER);`,
    `INSERT INTO ref_table (id, val) VALUES
      (1,42),(2,27),(3,35),(4,51),(5,29),(6,38),(7,46),(8,33),(9,24),(10,60);`,
    `CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, data JSONB);`,
    `INSERT INTO docs (id, title, data) VALUES
      (1,'btree','{"tags":["db","rust"],"stars":42}'),
      (2,'crypto','{"tags":["aes","hmac"],"stars":51}'),
      (3,'sync','{"tags":["noise","merkle"],"stars":27}'),
      (4,'memory','{"tags":["vector","mcp"],"stars":38}');`,
  ];

  const SCHEMAS = {
    users: [
      { name: 'id', type: 'integer', pk: true },
      { name: 'name', type: 'text' },
      { name: 'age', type: 'integer' },
    ],
    events: [
      { name: 'id', type: 'integer', pk: true },
      { name: 'user_id', type: 'integer' },
      { name: 'kind', type: 'text' },
      { name: 'ts', type: 'timestamp' },
    ],
    ref_table: [
      { name: 'id', type: 'integer', pk: true },
      { name: 'val', type: 'integer' },
    ],
    docs: [
      { name: 'id', type: 'integer', pk: true },
      { name: 'title', type: 'text' },
      { name: 'data', type: 'jsonb' },
    ],
  };

  const TABLES = [
    { name: 'users', count: 12 },
    { name: 'events', count: 8 },
    { name: 'ref_table', count: 10 },
    { name: 'docs', count: 4 },
  ];

  const SNIPPETS = {
    point: { kind: 'Basic', label: 'Point lookup on users', sql: `-- Point lookup on the primary key\nSELECT id, name, age\nFROM users\nWHERE id = 50000;` },
    window: { kind: 'Window', label: 'ROW_NUMBER + rolling SUM', sql: `-- Window functions\nSELECT\n  id, name, age,\n  ROW_NUMBER() OVER (ORDER BY id) AS rn,\n  SUM(age) OVER (ORDER BY id ROWS 50 PRECEDING) AS rolling\nFROM users\nORDER BY id;` },
    cte: { kind: 'Recursive CTE', label: 'Sum of 1..100', sql: `-- Recursive CTE\nWITH RECURSIVE seq(x) AS (\n  SELECT 1\n  UNION ALL\n  SELECT x + 1 FROM seq WHERE x < 100\n)\nSELECT SUM(x) AS total FROM seq;` },
    dates: { kind: 'Dates', label: 'DATE_TRUNC by month', sql: `-- Native DATE / TIMESTAMP\nSELECT DATE_TRUNC('month', ts) AS month,\n       COUNT(*) AS n\nFROM events\nGROUP BY 1\nORDER BY 1;` },
    groupby: { kind: 'Aggregate', label: 'GROUP BY kind', sql: `-- Grouped aggregate\nSELECT kind, COUNT(*) AS n\nFROM events\nGROUP BY kind\nORDER BY n DESC, kind;` },
    correlated: { kind: 'Correlated', label: 'EXISTS subquery', sql: `-- Correlated subquery\nSELECT COUNT(*) AS matched\nFROM users u\nWHERE EXISTS (\n  SELECT 1 FROM ref_table r WHERE r.val = u.age\n);` },
    json: { kind: 'JSON / JSONB', label: 'Extract with ->>', sql: `-- JSONB operators\nSELECT id, title,\n       data->>'stars' AS stars,\n       data->'tags'   AS tags\nFROM docs\nORDER BY id;` },
    upsert: { kind: 'UPSERT', label: 'ON CONFLICT ... RETURNING', sql: `-- UPSERT with RETURNING\nINSERT INTO users (id, name, age)\nVALUES (50000, 'alice', 43)\nON CONFLICT (id) DO UPDATE SET age = excluded.age\nRETURNING id, name, age;` },
    explain: { kind: 'EXPLAIN', label: 'B+ tree query plan', sql: `-- Inspect the plan\nEXPLAIN\nSELECT id, name\nFROM users\nWHERE id = 50000;` },
  };

  const DEFAULT_SQL = SNIPPETS.point.sql;

  const ta = document.getElementById('sqlInput');
  const hl = document.getElementById('hl');
  const gutter = document.getElementById('gutter');
  const resultBody = document.getElementById('resultBody');
  const rowCountEl = document.getElementById('rowCount');
  const statusEl = document.getElementById('pgStatus');
  let runFn = null;
  let lastResultHtml = '';
  const log = [];

  const KW = new Set('SELECT FROM WHERE AND OR ORDER BY LIMIT OFFSET GROUP HAVING JOIN LEFT RIGHT INNER FULL OUTER CROSS LATERAL ON AS WITH RECURSIVE UNION ALL INTERSECT EXCEPT INSERT INTO VALUES UPDATE SET DELETE CREATE TABLE ALTER ADD COLUMN DROP INDEX VIEW MATERIALIZED TRIGGER PRIMARY KEY NOT NULL DEFAULT UNIQUE CHECK REFERENCES FOREIGN INTERVAL DATE TIME TIMESTAMP JSONB OVER PARTITION ROWS RANGE PRECEDING FOLLOWING CURRENT ROW BETWEEN DESC ASC EXPLAIN DISTINCT IN IS LIKE CASE WHEN THEN ELSE END CONFLICT DO RETURNING EXCLUDED EXISTS'.split(' '));
  const FN = new Set('COUNT SUM AVG MIN MAX ROW_NUMBER RANK DENSE_RANK NTILE LAG LEAD FIRST_VALUE LAST_VALUE DATE_TRUNC DATE_PART EXTRACT NOW LENGTH UPPER LOWER COALESCE CAST JSON_EXTRACT TO_TSVECTOR TS_RANK'.split(' '));
  function esc(t) { return t.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
  function highlight(s) {
    let out = '', i = 0;
    const n = s.length;
    while (i < n) {
      const c = s[i];
      if (c === '-' && s[i + 1] === '-') { let j = i; while (j < n && s[j] !== '\n') j++; out += `<span class="c">${esc(s.slice(i, j))}</span>`; i = j; continue; }
      if (c === "'") { let j = i + 1; while (j < n && s[j] !== "'" && s[j] !== '\n') j++; if (s[j] === "'") j++; out += `<span class="s">${esc(s.slice(i, j))}</span>`; i = j; continue; }
      if (c === '"') { let j = i + 1; while (j < n && s[j] !== '"' && s[j] !== '\n') j++; if (s[j] === '"') j++; out += `<span class="s">${esc(s.slice(i, j))}</span>`; i = j; continue; }
      if (c >= '0' && c <= '9') { let j = i; while (j < n && /[0-9.]/.test(s[j])) j++; out += `<span class="n">${esc(s.slice(i, j))}</span>`; i = j; continue; }
      if (/[A-Za-z_]/.test(c)) {
        let j = i; while (j < n && /[A-Za-z0-9_]/.test(s[j])) j++;
        const word = s.slice(i, j), upper = word.toUpperCase();
        if (KW.has(upper)) out += `<span class="k">${esc(word)}</span>`;
        else if (FN.has(upper) && s[j] === '(') out += `<span class="fn">${esc(word)}</span>`;
        else out += esc(word);
        i = j; continue;
      }
      out += esc(c); i++;
    }
    return out;
  }
  function updateGutter() {
    const lines = ta.value.split('\n').length;
    let g = '';
    for (let i = 1; i <= lines; i++) g += `<div>${String(i).padStart(2, '0')}</div>`;
    gutter.innerHTML = g;
  }
  function render() { hl.innerHTML = highlight(ta.value) + '\n'; updateGutter(); }

  const schemaPanel = document.getElementById('schema');
  const schemaTitle = document.getElementById('schemaTitle');
  function showSchema(tbl) {
    schemaTitle.textContent = tbl;
    const s = SCHEMAS[tbl];
    if (!s) { schemaPanel.innerHTML = '<span class="schema-empty">no table selected</span>'; return; }
    schemaPanel.innerHTML = s.map(c =>
      `<div class="col${c.pk ? ' pk' : ''}"><b>${c.name}</b><i>${c.type}${c.pk ? ' / pk' : ''}</i></div>`).join('');
  }

  const pageRing = document.getElementById('pageRing');
  let ringCells = [];
  if (pageRing) {
    for (let i = 0; i < 60; i++) { const d = document.createElement('div'); d.classList.add('on'); pageRing.appendChild(d); }
    ringCells = [...pageRing.querySelectorAll('div')];
  }
  function pulseRing(count) {
    ringCells.forEach(c => c.classList.remove('hot'));
    for (let i = 0; i < count; i++) ringCells[(Math.random() * ringCells.length) | 0]?.classList.add('hot');
    setTimeout(() => ringCells.forEach(c => c.classList.remove('hot')), 900);
  }

  function renderTable(cols, rows) {
    let th = '<tr>';
    cols.forEach(c => th += `<th>${esc(c)}</th>`);
    th += '</tr>';
    let body = '';
    rows.forEach(r => {
      body += '<tr>';
      r.forEach((v, i) => {
        let cls = '';
        if (i === 0) cls = 'id';
        else if (typeof v === 'number') cls = 'num';
        else if (v === null || v === undefined) cls = 'null';
        const cell = (v === null || v === undefined) ? 'NULL' : (typeof v === 'object' ? JSON.stringify(v) : String(v));
        body += `<td class="${cls}">${esc(cell)}</td>`;
      });
      body += '</tr>';
    });
    return `<table><thead>${th}</thead><tbody>${body}</tbody></table>`;
  }

  function runStatements(sql) {
    if (!runFn) return { html: '<div class="result-empty">engine still loading...</div>', rows: 0, ms: 0, plan: null };
    const t0 = performance.now();
    let results;
    try { results = runFn(sql); }
    catch (e) { return { html: `<div class="result-msg"><span class="err">Error:</span> ${esc(String(e))}</div>`, rows: 0, ms: performance.now() - t0, plan: null, error: String(e) }; }
    const ms = performance.now() - t0;
    let html = '', rows = 0;
    for (const r of results) {
      if (r.type === 'query') {
        html += renderTable(r.columns, r.rows);
        html += `<div class="result-msg" style="padding-top:10px">${r.rows.length} row${r.rows.length === 1 ? '' : 's'}</div>`;
        rows += r.rows.length;
      } else if (r.type === 'rowsAffected') {
        html += `<div class="result-msg"><span class="ok">OK</span> / ${r.value} row${r.value === 1 ? '' : 's'} affected / committed</div>`;
      } else if (r.type === 'error') {
        html += `<div class="result-msg"><span class="err">Error:</span> ${esc(r.message)}</div>`;
      } else {
        html += `<div class="result-msg"><span class="ok">OK</span></div>`;
      }
    }
    if (!html) html = '<div class="result-empty">(no output)</div>';
    return { html, rows, ms };
  }

  function setKpi(id, val) { const el = document.getElementById(id); if (el) el.textContent = val; }
  function fmtMs(ms) { return ms < 1 ? ms.toFixed(2) : ms.toFixed(1); }

  function reflectTable(sql) {
    const m = sql.match(/\bFROM\s+([a-zA-Z_]\w*)/i);
    const tbl = m && SCHEMAS[m[1].toLowerCase()] ? m[1].toLowerCase() : null;
    if (!tbl) return;
    showSchema(tbl);
    document.querySelectorAll('.tbl-item').forEach(i => i.classList.toggle('active', i.dataset.tbl === tbl));
  }

  function runQuery() {
    const sql = ta.value.trim();
    if (!sql || !runFn) return;
    setActiveResultTab('rows');
    const res = runStatements(sql);
    lastResultHtml = res.html;
    resultBody.innerHTML = res.html;
    rowCountEl.textContent = res.rows;
    reflectTable(sql);
    setKpi('mExec', fmtMs(res.ms) + ' ms');
    setKpi('mRows', res.rows);
    const kpiMs = document.getElementById('kpiMs');
    if (kpiMs) kpiMs.innerHTML = fmtMs(res.ms) + '<span style="color:var(--fg-3);font-weight:400;font-size:14px;margin-left:2px">ms</span>';
    pulseRing(Math.max(2, Math.min(10, res.rows + 2)));
    log.unshift({ sql: sql.split('\n').filter(l => l.trim() && !l.trim().startsWith('--'))[0] || sql, ms: res.ms, rows: res.rows, err: res.error });
    if (log.length > 12) log.pop();
  }

  function setActiveResultTab(panel) {
    document.querySelectorAll('.result-tab').forEach(t => t.classList.toggle('active', t.dataset.panel === panel));
  }
  function showPlan() {
    let sql = ta.value.trim();
    if (!sql) { resultBody.innerHTML = '<div class="result-empty">Write a SELECT, then open the Plan tab.</div>'; return; }
    if (!/^\s*EXPLAIN\b/i.test(sql)) sql = 'EXPLAIN ' + sql;
    const res = runStatements(sql);
    resultBody.innerHTML = res.html || '<div class="result-empty">No plan available for this statement.</div>';
  }
  function showLog() {
    if (!log.length) { resultBody.innerHTML = '<div class="result-empty">No queries run yet.</div>'; return; }
    const lines = log.map(e => {
      const status = e.err ? `<span class="err">error</span>` : `<span class="ok">ok</span>  rows=${e.rows}`;
      return `<span style="color:var(--fg-3)">[${fmtMs(e.ms)} ms]</span> ${status}  ${esc(e.sql.slice(0, 60))}`;
    }).join('\n');
    resultBody.innerHTML = `<pre class="result-msg">${lines}</pre>`;
  }

  function init() {
    ta.value = DEFAULT_SQL;
    render();
    showSchema('users');

    ta.addEventListener('input', render);
    ta.addEventListener('scroll', () => { hl.scrollTop = ta.scrollTop; hl.scrollLeft = ta.scrollLeft; });
    ta.addEventListener('keydown', (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); runQuery(); }
      if (e.key === 'Tab') {
        e.preventDefault();
        const s = ta.selectionStart, en = ta.selectionEnd;
        ta.value = ta.value.slice(0, s) + '  ' + ta.value.slice(en);
        ta.selectionStart = ta.selectionEnd = s + 2;
        render();
      }
    });

    document.querySelectorAll('.tbl-item').forEach(it => it.addEventListener('click', () => {
      document.querySelectorAll('.tbl-item').forEach(i => i.classList.remove('active'));
      it.classList.add('active');
      const tbl = it.dataset.tbl;
      showSchema(tbl);
      ta.value = `SELECT * FROM ${tbl}\nLIMIT 20;`;
      render(); runQuery();
    }));

    document.querySelectorAll('.snippet-item').forEach(it => it.addEventListener('click', () => {
      const snip = SNIPPETS[it.dataset.snippet];
      if (!snip) return;
      ta.value = snip.sql; render(); runQuery();
    }));

    document.querySelectorAll('.result-tab').forEach(tab => tab.addEventListener('click', () => {
      setActiveResultTab(tab.dataset.panel);
      if (tab.dataset.panel === 'plan') showPlan();
      else if (tab.dataset.panel === 'log') showLog();
      else resultBody.innerHTML = lastResultHtml || '<div class="result-empty">Run a query.</div>';
    }));

    const runBtn = document.getElementById('runBtn');
    const fmtBtn = document.getElementById('fmtBtn');
    const resetBtn = document.getElementById('resetBtn');
    if (runBtn) runBtn.onclick = runQuery;
    if (fmtBtn) fmtBtn.onclick = () => {
      ta.value = ta.value.replace(/\b(select|from|where|and|or|order by|limit|offset|group by|having|join|left|right|inner|full|outer|cross|lateral|on|as|with|recursive|union all|union|intersect|except|insert|into|values|update|set|delete|create|table|view|materialized|trigger|alter|add|column|drop|index|primary key|not null|default|unique|check|references|foreign|interval|date|time|timestamp|over|partition by|rows|range|preceding|following|current row|between|desc|asc|explain|distinct|in|is|like|case|when|then|else|end|on conflict|do update|do nothing|returning|exists)\b/gi, m => m.toUpperCase());
      render();
    };
    if (resetBtn) resetBtn.onclick = () => { ta.value = DEFAULT_SQL; render(); runQuery(); };

    const sessEl = document.getElementById('sessTime');
    if (sessEl) {
      const t0 = performance.now();
      setInterval(() => {
        const s = ((performance.now() - t0) / 1000) | 0;
        const hh = String((s / 3600) | 0).padStart(2, '0');
        const mm = String(((s / 60) | 0) % 60).padStart(2, '0');
        const ss = String(s % 60).padStart(2, '0');
        sessEl.textContent = `${hh}:${mm}:${ss}`;
      }, 1000);
    }
  }

  function setStatus(text, cls) {
    if (!statusEl) return;
    statusEl.textContent = text;
    statusEl.className = 'meta-status' + (cls ? ' ' + cls : '');
  }

  function onReady(fn) {
    runFn = fn;
    const runBtn = document.getElementById('runBtn');
    if (runBtn) runBtn.disabled = false;
    setStatus('sealed', 'ok');
    setKpi('kpiTables', TABLES.length);
    setKpi('kpiRows', TABLES.reduce((a, t) => a + t.count, 0));
    runQuery();
  }

  function onError(msg) {
    setStatus('load failed', 'err');
    if (resultBody) resultBody.innerHTML = `<div class="result-msg"><span class="err">Failed to load WebAssembly engine:</span> ${esc(msg)}</div>`;
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();

  return { SEED, onReady, onError };
})();
