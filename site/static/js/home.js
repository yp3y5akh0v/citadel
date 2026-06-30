(() => {
  const installCmd = document.getElementById('installCmd');
  const copyBtn = document.getElementById('copyInstall');
  if (!copyBtn) return;
  copyBtn.onclick = () => {
    navigator.clipboard?.writeText(installCmd.textContent);
    const t = copyBtn.textContent;
    copyBtn.textContent = 'copied'; copyBtn.style.color = 'var(--ok)';
    setTimeout(() => { copyBtn.textContent = t; copyBtn.style.color = ''; }, 1200);
  };
})();

(() => {
  const unlock = document.getElementById('unlock');
  if (!unlock) return;
  const hexEl = document.getElementById('unlockHex');
  const rowsEl = document.getElementById('unlockRows');
  const form = document.getElementById('unlockForm');
  const input = document.getElementById('unlockPass');
  const errEl = document.getElementById('unlockErr');
  const statusEl = document.getElementById('unlockStatus');
  const btn = document.getElementById('unlockBtn');

  let seed = 0xC17AD3E1 >>> 0;
  const rnd = () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed; };
  const hex2 = () => rnd().toString(16).slice(-2).padStart(2, '0');

  function renderHex() {
    const ROWS = 12, COLS = 24;
    const out = [];
    let offset = 0;
    for (let r = 0; r < ROWS; r++) {
      const off = offset.toString(16).padStart(4, '0');
      let line = `<span class="off">${off}</span>`;
      for (let c = 0; c < COLS; c++) {
        const idx = r * COLS + c;
        const byte = hex2();
        let cls = '';
        if (idx < 16) cls = 'iv';
        else if (idx >= ROWS * COLS - 32) cls = 'mac';
        line += cls ? `<span class="${cls}">${byte}</span> ` : `${byte} `;
      }
      out.push(`<span class="hex-line" data-row="${r}">${line}</span>`);
      offset += COLS;
      if (r === 5) {
        out.push(`<span class="hex-line ellipsis" aria-hidden="true">... 7,920 more bytes of ciphertext ...</span>`);
      }
    }
    hexEl.innerHTML = out.join('\n');
    return [...hexEl.querySelectorAll('.hex-line:not(.ellipsis)')];
  }
  let hexLines = renderHex();

  function renderRows() {
    const decoded = [
      '<span class="schema">-- users (id INTEGER PK, name TEXT, age INTEGER)</span>',
      '<span class="sep">+-------+--------+-----+</span>',
      '<span class="hdr">|  id   | name   | age |</span>',
      '<span class="sep">+-------+--------+-----+</span>',
      '| <span class="num">50000</span> | <span class="str">alice</span>  | <span class="num"> 42</span> |',
      '| <span class="num">50001</span> | <span class="str">bob</span>    | <span class="num"> 27</span> |',
      '| <span class="num">50002</span> | <span class="str">carol</span>  | <span class="num"> 35</span> |',
      '| <span class="num">50003</span> | <span class="str">dmitri</span> | <span class="num"> 51</span> |',
      '<span class="sep">+-------+--------+-----+</span>',
      '<span class="schema">4 of 100,000 rows / decrypted with DEK / MAC verified ok</span>',
    ];
    rowsEl.innerHTML = decoded.map(s => `<span class="dec-line">${s}</span>`).join('\n');
    return [...rowsEl.querySelectorAll('.dec-line')];
  }
  let rowLines = renderRows();

  let busy = false;
  const PASS = 'open-sesame';

  function setErr(text, kind) {
    errEl.className = 'unlock-err' + (kind ? ' ' + kind : '');
    errEl.innerHTML = text;
  }

  function attempt(value) {
    if (busy) return;
    if (value.trim() !== PASS) {
      unlock.classList.add('shake');
      setErr('<code>HMAC-SHA256 mismatch</code> / file remains sealed', 'fail');
      statusEl.textContent = 'sealed';
      setTimeout(() => unlock.classList.remove('shake'), 500);
      return;
    }
    busy = true;
    input.blur();
    btn.disabled = true;
    statusEl.textContent = 'verifying...';
    setErr('Argon2id derived master key / AES-KW unwrapped REK / HKDF split DEK + MAC key', '');
    unlock.classList.add('decrypting');
    rowLines.forEach(l => l.classList.remove('lit'));
    const dissolveStep = 108;
    hexLines.forEach((line, i) => {
      setTimeout(() => line.classList.add('dissolved'), 120 + i * dissolveStep);
    });
    setTimeout(() => {
      unlock.classList.remove('decrypting');
      unlock.classList.add('open');
      statusEl.textContent = 'open';
      setErr('<code>MAC verified ok</code> / page decrypted to memory / 8,160 B body', 'ok');
      rowLines.forEach((l, i) => setTimeout(() => l.classList.add('lit'), i * 90));
      busy = false;
      btn.disabled = false;
    }, 1700);
  }

  function relock() {
    unlock.classList.remove('open');
    rowLines.forEach(l => l.classList.remove('lit'));
    input.value = '';
    statusEl.textContent = 'sealed';
    setErr('Argon2id / AES-256-CTR / HMAC-SHA256 / enter passphrase to decrypt', '');
    hexLines = renderHex();
  }

  form.addEventListener('submit', (e) => {
    e.preventDefault();
    if (unlock.classList.contains('open')) { relock(); return; }
    attempt(input.value);
  });
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && unlock.classList.contains('open')) relock();
  });
})();

(() => {
  const SNIPPETS = {
    rust: {
      file: 'src/main.rs',
      code: `<span class="k">use</span> citadel::<span class="t">DatabaseBuilder</span>;
<span class="k">use</span> citadel_sql::<span class="t">Connection</span>;

<span class="k">let</span> db = <span class="t">DatabaseBuilder</span>::<span class="fn">new</span>(<span class="s">"my.db"</span>)
    .<span class="fn">passphrase</span>(<span class="s">b"secret"</span>)
    .<span class="fn">create</span>()?;

<span class="k">let</span> <span class="k">mut</span> conn = <span class="t">Connection</span>::<span class="fn">open</span>(&amp;db)?;
conn.<span class="fn">execute</span>(<span class="s">"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);"</span>)?;
conn.<span class="fn">execute</span>(<span class="s">"INSERT INTO users (id, name) VALUES (1, 'Alice');"</span>)?;
<span class="k">let</span> result = conn.<span class="fn">query</span>(<span class="s">"SELECT * FROM users;"</span>)?;`,
    },
    sql: {
      file: 'examples.sql',
      code: `<span class="c">-- window functions, recursive CTEs, native dates</span>
<span class="k">WITH RECURSIVE</span> seq(x) <span class="k">AS</span> (
  <span class="k">SELECT</span> <span class="n">1</span>
  <span class="k">UNION ALL</span>
  <span class="k">SELECT</span> x + <span class="n">1</span> <span class="k">FROM</span> seq <span class="k">WHERE</span> x &lt; <span class="n">1000</span>
)
<span class="k">SELECT SUM</span>(x) <span class="k">FROM</span> seq;

<span class="k">SELECT</span>
  id, age,
  <span class="fn">ROW_NUMBER</span>() <span class="k">OVER</span> (<span class="k">PARTITION BY</span> age <span class="k">ORDER BY</span> id) <span class="k">AS</span> rn,
  <span class="fn">SUM</span>(age) <span class="k">OVER</span> (<span class="k">ORDER BY</span> id <span class="k">ROWS</span> <span class="n">50</span> <span class="k">PRECEDING</span>) <span class="k">AS</span> rolling
<span class="k">FROM</span> users;

<span class="k">SELECT</span> <span class="fn">DATE_TRUNC</span>(<span class="s">'month'</span>, ts), <span class="fn">COUNT</span>(*)
<span class="k">FROM</span> events
<span class="k">WHERE</span> ts <span class="k">BETWEEN</span> <span class="k">DATE</span> <span class="s">'2024-02-01'</span>
                <span class="k">AND</span> <span class="k">DATE</span> <span class="s">'2024-03-31'</span>
<span class="k">GROUP BY</span> <span class="n">1</span>;`,
    },
    kv: {
      file: 'kv_usage.rs',
      code: `<span class="c">// Key-value API: same database, same transaction semantics</span>
<span class="k">let</span> <span class="k">mut</span> wtx = db.<span class="fn">begin_write</span>()?;
wtx.<span class="fn">insert</span>(<span class="s">b"key"</span>, <span class="s">b"value"</span>)?;
wtx.<span class="fn">commit</span>()?;

<span class="k">let</span> <span class="k">mut</span> rtx = db.<span class="fn">begin_read</span>();
<span class="fn">assert_eq!</span>(rtx.<span class="fn">get</span>(<span class="s">b"key"</span>)?.<span class="fn">unwrap</span>(), <span class="s">b"value"</span>);

<span class="c">// Named tables co-exist with SQL tables</span>
<span class="k">let</span> <span class="k">mut</span> wtx = db.<span class="fn">begin_write</span>()?;
wtx.<span class="fn">create_table</span>(<span class="s">b"sessions"</span>)?;
wtx.<span class="fn">table_insert</span>(<span class="s">b"sessions"</span>, <span class="s">b"token-abc"</span>, <span class="s">b"user-42"</span>)?;
wtx.<span class="fn">commit</span>()?;`,
    },
    python: {
      file: 'app.py',
      code: `<span class="k">import</span> citadeldb

<span class="k">with</span> citadeldb.<span class="fn">connect</span>(<span class="s">"my.db"</span>, key=<span class="s">"secret"</span>, create=<span class="k">True</span>) <span class="k">as</span> db:
    db.<span class="fn">execute</span>(<span class="s">"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);"</span>)
    db.<span class="fn">execute</span>(<span class="s">"INSERT INTO users VALUES (1, 'Alice');"</span>)
    rows = db.<span class="fn">query</span>(<span class="s">"SELECT * FROM users;"</span>).rows  <span class="c"># [(1, 'Alice')]</span>`,
    },
    wasm: {
      file: 'main.js',
      code: `<span class="k">import</span> { <span class="t">CitadelDb</span> } <span class="k">from</span> <span class="s">"@citadeldb/wasm"</span>;

<span class="k">const</span> db = <span class="k">new</span> <span class="t">CitadelDb</span>(<span class="s">"secret"</span>);
db.<span class="fn">run</span>(<span class="s">"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);"</span>);
db.<span class="fn">run</span>(<span class="s">"INSERT INTO t (id, name) VALUES (1, 'Alice');"</span>);

<span class="k">const</span> result = db.<span class="fn">run</span>(<span class="s">"SELECT * FROM t;"</span>);
<span class="c">// [{ type: "query", columns: ["id","name"], rows: [[1,"Alice"]] }]</span>`,
    },
    c: {
      file: 'example.c',
      code: `<span class="k">#include</span> <span class="s">"citadel.h"</span>

<span class="t">CitadelDb</span> *db = <span class="n">NULL</span>;
<span class="fn">citadel_create</span>(<span class="s">"my.db"</span>, <span class="s">"secret"</span>, <span class="n">6</span>, &amp;db);

<span class="t">CitadelSqlConn</span> *conn = <span class="n">NULL</span>;
<span class="fn">citadel_sql_open</span>(db, &amp;conn);
<span class="t">CitadelSqlResult</span> *result = <span class="n">NULL</span>;
<span class="fn">citadel_sql_execute</span>(conn, <span class="s">"SELECT * FROM users;"</span>, &amp;result);

<span class="fn">citadel_close</span>(db);`,
    },
  };
  const qsCode = document.getElementById('qsCode');
  const qsFile = document.getElementById('qsFile');
  if (!qsCode) return;
  function setTab(key) {
    document.querySelectorAll('.qs-tab').forEach(t => t.classList.toggle('active', t.dataset.tab === key));
    qsCode.innerHTML = SNIPPETS[key].code;
    qsFile.textContent = SNIPPETS[key].file;
  }
  document.querySelectorAll('.qs-tab').forEach(t => t.onclick = () => setTab(t.dataset.tab));
  setTab('rust');
  document.getElementById('qsCopy').onclick = () => {
    navigator.clipboard?.writeText(qsCode.innerText);
    const b = document.getElementById('qsCopy');
    const o = b.textContent; b.textContent = 'copied'; b.style.color = 'var(--ok)';
    setTimeout(() => { b.textContent = o; b.style.color = ''; }, 1200);
  };
})();

(() => {
  const table = document.getElementById('benchTable');
  if (!table) return;
  const BENCH = [
    ['correlated_in', '6.52 ms', '1.97 s', 302],
    ['full_outer_join', '70.6 µs', '20.6 ms', 292],
    ['correlated_scalar', '324 µs', '19.2 ms', 59],
    ['count', '605 ns', '21.0 µs', 35],
    ['point', '1.12 µs', '12.5 µs', 11],
    ['fts_rank', '4.85 ms', '41.8 ms', 8.6],
    ['group_by', '1.38 ms', '10.3 ms', 7.5],
    ['union', '27.6 µs', '148 µs', 5.3],
    ['cte', '1.30 ms', '6.10 ms', 4.7],
    ['jsonb_contains', '5.63 ms', '26.2 ms', 4.6],
    ['view_point', '3.29 µs', '12.3 µs', 3.7],
    ['truncate', '20.6 µs', '56.7 µs', 2.75],
    ['window_agg', '28.8 ms', '76.1 ms', 2.65],
    ['fts_match', '2.87 ms', '7.54 ms', 2.63],
    ['upsert_dedup', '12.4 µs', '32.3 µs', 2.61],
    ['json_extract', '12.2 ms', '31.3 ms', 2.57],
    ['partial_index_point', '4.78 µs', '12.2 µs', 2.54],
    ['insert_returning', '70.9 µs', '172 µs', 2.42],
    ['fts_phrase', '4.04 ms', '9.05 ms', 2.24],
    ['upsert_returning', '79.2 µs', '174 µs', 2.19],
    ['window_rank', '60.6 ms', '127 ms', 2.09],
    ['savepoint_create', '345 ns', '716 ns', 2.08],
    ['sort', '1.34 ms', '2.67 ms', 1.99],
    ['filter', '973 µs', '1.87 ms', 1.92],
    ['view_filter', '980 µs', '1.81 ms', 1.85],
    ['scan', '5.03 ms', '9.33 ms', 1.85],
    ['savepoint_nested', '188 µs', '348 µs', 1.85],
    ['savepoint_rollback', '1.25 ms', '2.26 ms', 1.80],
    ['insert_select', '553 µs', '936 µs', 1.69],
    ['join', '59.6 µs', '95.3 µs', 1.60],
    ['update', '18.6 µs', '29.2 µs', 1.56],
    ['insert', '33.1 µs', '51.3 µs', 1.55],
    ['upsert_all_new', '32.5 µs', '50.2 µs', 1.55],
    ['upsert_counter', '36.3 µs', '55.0 µs', 1.51],
    ['wide_proj_full', '4.69 ms', '7.06 ms', 1.51],
    ['wide_proj_pk', '315 µs', '462 µs', 1.46],
    ['delete_returning', '120 µs', '172 µs', 1.44],
    ['recursive_cte', '86.7 µs', '123 µs', 1.42],
    ['delete', '52.0 µs', '73.5 µs', 1.41],
    ['correlated_exists', '5.02 ms', '6.87 ms', 1.37],
    ['distinct', '2.84 ms', '3.86 ms', 1.36],
    ['fk_cascade_delete_only', '59.8 µs', '77.5 µs', 1.30],
    ['with_dml', '82.0 µs', '105 µs', 1.28],
    ['wide_proj_3col', '943 µs', '1.20 ms', 1.27],
    ['sum', '1.55 ms', '1.93 ms', 1.24],
    ['wide_proj_2col', '510 µs', '623 µs', 1.22],
    ['sort_nocase', '2.72 ms', '3.30 ms', 1.21],
    ['insert_gen_virtual', '45.8 µs', '54.2 µs', 1.19],
    ['upsert_mixed', '50.7 µs', '57.8 µs', 1.14],
    ['select_gen_virtual', '15.9 µs', '17.8 µs', 1.12],
    ['insert_gen_stored', '49.8 µs', '55.3 µs', 1.11],
    ['fk_cascade', '80.7 µs', '87.5 µs', 1.09],
    ['update_gen_propagate', '43.9 µs', '45.5 µs', 1.03],
    ['update_returning', '146 µs', '148 µs', 1.01],
  ];
  BENCH.forEach(([name, c, s, r]) => {
    const logR = Math.log10(r) / Math.log10(302);
    const w = Math.max(6, logR * 100);
    const row = document.createElement('div');
    row.className = 'row';
    row.innerHTML = `
      <div class="name mono">${name}</div>
      <div class="bar-wrap"><div class="bar"><i data-w="${w.toFixed(1)}%"></i></div></div>
      <div class="num">${c}</div>
      <div class="num">${s}</div>
      <div class="ratio">${r.toFixed(r < 10 ? 2 : 0)}×</div>`;
    table.appendChild(row);
  });
  const barEls = table.querySelectorAll('.bar i');
  const obs = new IntersectionObserver((e) => {
    if (e[0].isIntersecting) {
      barEls.forEach((b, i) => setTimeout(() => b.style.width = b.dataset.w, 40 * i));
      obs.disconnect();
    }
  }, { threshold: 0.15 });
  obs.observe(table);
})();

(() => {
  document.querySelectorAll('section.block, .cta-band, .proof').forEach(el => el.classList.add('reveal'));
  const revealObs = new IntersectionObserver((entries) => {
    entries.forEach(e => {
      if (e.isIntersecting) { e.target.classList.add('in'); revealObs.unobserve(e.target); }
    });
  }, { threshold: 0.08, rootMargin: '0px 0px -40px 0px' });
  document.querySelectorAll('.reveal').forEach(el => revealObs.observe(el));
})();

(() => {
  const kpiVals = document.querySelectorAll('.kpi .val em');
  const kpiObs = new IntersectionObserver((entries) => {
    entries.forEach(e => {
      if (!e.isIntersecting) return;
      const el = e.target;
      const raw = el.textContent;
      const m = raw.match(/([\d.]+)(.*)/);
      if (!m) return;
      const target = parseFloat(m[1]);
      const suffix = m[2];
      const dur = 900;
      const start = performance.now();
      const tick = (t) => {
        const p = Math.min(1, (t - start) / dur);
        const eased = 1 - Math.pow(1 - p, 3);
        const val = target * eased;
        el.textContent = (target % 1 === 0 ? Math.round(val) : val.toFixed(target < 10 ? 1 : 2)) + suffix;
        if (p < 1) requestAnimationFrame(tick);
        else el.textContent = raw;
      };
      requestAnimationFrame(tick);
      kpiObs.unobserve(el);
    });
  }, { threshold: 0.5 });
  kpiVals.forEach(el => kpiObs.observe(el));
})();

(() => {
  document.querySelectorAll('.nav-links a[href*="#"], a[href^="#"]').forEach(a => {
    a.addEventListener('click', () => {
      const href = a.getAttribute('href');
      const i = href.indexOf('#');
      if (i < 0) return;
      const target = document.getElementById(href.slice(i + 1));
      if (!target) return;
      target.classList.add('in');
      target.querySelectorAll('.reveal').forEach(el => el.classList.add('in'));
    });
  });

  const navLinks = [...document.querySelectorAll('.nav-links a[href*="#"]')];
  const homeLink = document.querySelector('.nav-links a[data-top="1"]');
  const sectionIds = navLinks
    .map(a => (a.getAttribute('href').match(/#(.+)$/) || [])[1])
    .filter(Boolean);
  const sections = sectionIds.map(id => document.getElementById(id)).filter(Boolean);
  const navObs = new IntersectionObserver((entries) => {
    entries.forEach(en => {
      if (!en.isIntersecting) return;
      const id = en.target.id;
      navLinks.forEach(a => {
        const h = (a.getAttribute('href').match(/#(.+)$/) || [])[1];
        a.classList.toggle('active', h === id);
      });
      if (homeLink) homeLink.classList.remove('active');
    });
  }, { rootMargin: '-40% 0px -55% 0px' });
  sections.forEach(s => navObs.observe(s));

  const syncHomeActive = () => {
    if (!homeLink) return;
    if (window.scrollY < 120) {
      navLinks.forEach(a => a.classList.remove('active'));
      homeLink.classList.add('active');
    }
  };
  syncHomeActive();
  window.addEventListener('scroll', syncHomeActive, { passive: true });

  document.querySelectorAll('.btn').forEach(btn => {
    btn.addEventListener('mousemove', (e) => {
      const r = btn.getBoundingClientRect();
      const x = e.clientX - r.left - r.width / 2;
      const y = e.clientY - r.top - r.height / 2;
      btn.style.transform = `translate(${x * 0.08}px, ${y * 0.12}px)`;
    });
    btn.addEventListener('mouseleave', () => { btn.style.transform = ''; });
  });
})();

(() => {
  const root = document.getElementById('keyHierarchy');
  if (!root) return;
  const prefersReduced = matchMedia('(prefers-reduced-motion: reduce)').matches;

  function hexSeed(s, bytes = 8) {
    let h = 2166136261 >>> 0;
    for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); }
    let out = '';
    for (let i = 0; i < bytes; i++) {
      h = Math.imul(h ^ (h >>> 13), 1540483477);
      const b = (h & 0xff).toString(16).padStart(2, '0');
      out += (i && i % 2 === 0 ? ' ' : '') + b;
    }
    return out + '...';
  }

  const stages = [...root.querySelectorAll('.kh-stage')];
  const progress = document.getElementById('khProgress');
  for (let i = 0; i < stages.length; i++) {
    const d = document.createElement('span');
    d.className = 'pd';
    progress.appendChild(d);
  }
  const pds = [...progress.children];

  const passMask = document.getElementById('khPassMask');
  const saltHex = document.getElementById('khSaltHex');
  const argonGrid = document.getElementById('khArgon');
  const argonProg = document.getElementById('khArgonProg');
  const mkHex = document.getElementById('khMkHex');
  const rekHex = document.getElementById('khRekHex');
  const dekHex = document.getElementById('khDekHex');
  const macHex = document.getElementById('khMacHex');
  const ivHex = document.getElementById('khIvHex');
  const page = document.getElementById('khPage');
  const pIv = page.querySelector('.p-iv');
  const pCt = page.querySelector('.p-ct');
  const pMac = page.querySelector('.p-mac');
  const dfDb = document.getElementById('khDfDb');
  const dfPage = document.getElementById('khDfPage');
  const dfDbSize = document.getElementById('khDfDbSize');
  const counter = document.getElementById('khCounter');
  const counterPage = document.getElementById('khCounterPage');

  const ARGON_CELLS = 64;
  for (let i = 0; i < ARGON_CELLS; i++) {
    const m = document.createElement('div');
    m.className = 'm';
    argonGrid.appendChild(m);
  }
  const argonCells = [...argonGrid.children];

  const wait = (ms) => new Promise(r => setTimeout(r, prefersReduced ? Math.min(ms, 60) : ms));

  function setActive(i) {
    stages.forEach((s, idx) => {
      s.classList.toggle('lit', idx === i);
      s.classList.toggle('done', idx < i);
    });
    pds.forEach((d, idx) => {
      d.classList.toggle('active', idx === i);
      d.classList.toggle('done', idx < i);
    });
  }

  function resetAll() {
    stages.forEach(s => s.classList.remove('lit', 'done'));
    pds.forEach(d => d.classList.remove('active', 'done'));
    passMask.textContent = '';
    saltHex.textContent = '-';
    argonCells.forEach(c => c.classList.remove('on', 'hot'));
    argonProg.textContent = 'filling memory...';
    mkHex.textContent = '-';
    rekHex.textContent = '-';
    dekHex.textContent = '-';
    macHex.textContent = '-';
    ivHex.textContent = '-';
    pIv.classList.remove('on'); pCt.classList.remove('on'); pMac.classList.remove('on');
    dfDb.classList.remove('writing');
    dfDbSize.textContent = '+ 8,208 B';
  }

  async function stage1(session) {
    setActive(0);
    await wait(900);
    const N = 11;
    for (let i = 1; i <= N; i++) { passMask.textContent = '•'.repeat(i); await wait(180); }
    await wait(1100);
    saltHex.textContent = hexSeed('salt:' + session, 6);
    await wait(2600);
  }
  async function stage2(session) {
    setActive(1);
    await wait(700);
    for (let pass = 1; pass <= 3; pass++) {
      argonProg.textContent = 'pass ' + pass + ' / 3 / 64 MiB';
      const order = [...Array(ARGON_CELLS).keys()];
      let seed = 1337 + session * 97 + pass * 13;
      const rnd = () => { seed = (seed * 1664525 + 1013904223) | 0; return (seed >>> 0) / 0xffffffff; };
      for (let i = order.length - 1; i > 0; i--) {
        const j = Math.floor(rnd() * (i + 1));
        [order[i], order[j]] = [order[j], order[i]];
      }
      for (let i = 0; i < order.length; i++) {
        const c = argonCells[order[i]];
        c.classList.add('on', 'hot');
        if (i % 4 === 0) await wait(48);
        if (i >= 6) argonCells[order[i - 6]].classList.remove('hot');
      }
      argonCells.forEach(c => c.classList.remove('hot'));
      await wait(450);
    }
    argonProg.textContent = 'derived 32 B master key';
    mkHex.textContent = hexSeed('mk:' + session, 8);
    await wait(2800);
  }
  async function stage3(session) {
    setActive(2);
    await wait(1200);
    rekHex.textContent = hexSeed('rek:' + session, 8);
    await wait(3200);
  }
  async function stage4(session) {
    setActive(3);
    await wait(1300);
    dekHex.textContent = hexSeed('dek:' + session, 8);
    await wait(1100);
    macHex.textContent = hexSeed('mac:' + session, 8);
    await wait(3200);
  }
  async function stage5(session, pageId) {
    setActive(4);
    await wait(800);
    ivHex.textContent = hexSeed('iv:' + session + ':' + pageId, 6);
    pIv.classList.add('on');
    await wait(1400);
    pCt.classList.add('on');
    await wait(2600);
    pMac.classList.add('on');
    await wait(2800);
  }
  async function stage6(session, pageId, totalPages) {
    setActive(5);
    dfPage.textContent = '#' + pageId;
    dfDbSize.textContent = (totalPages * 8208).toLocaleString() + ' B total';
    dfDb.classList.add('writing');
    await wait(3200);
    dfDb.classList.remove('writing');
    await wait(2200);
  }

  async function runCycle(session, pageId, totalPages, firstCycleThisSession) {
    resetAll();
    if (counterPage) counterPage.textContent = pageId;
    await wait(800);
    if (firstCycleThisSession) {
      await stage1(session);
      await stage2(session);
      await stage3(session);
      await stage4(session);
    } else {
      passMask.textContent = '•'.repeat(11);
      saltHex.textContent = hexSeed('salt:' + session, 6);
      argonCells.forEach(c => c.classList.add('on'));
      argonProg.textContent = '32 B master key in memory';
      mkHex.textContent = hexSeed('mk:' + session, 8);
      rekHex.textContent = hexSeed('rek:' + session, 8);
      dekHex.textContent = hexSeed('dek:' + session, 8);
      macHex.textContent = hexSeed('mac:' + session, 8);
      stages.forEach((s, idx) => { if (idx < 4) s.classList.add('done'); });
      pds.forEach((d, idx) => { if (idx < 4) d.classList.add('done'); });
      await wait(800);
    }
    await stage5(session, pageId);
    await stage6(session, pageId, totalPages);
    stages.forEach(s => s.classList.add('done'));
    stages.forEach(s => s.classList.remove('lit'));
    pds.forEach(d => { d.classList.remove('active'); d.classList.add('done'); });
    counter.textContent = 'sealed page ' + pageId + ' / DEK/MAC reused for next';
    await wait(3600);
  }

  let visible = true;
  const io = new IntersectionObserver((entries) => {
    for (const e of entries) visible = e.isIntersecting;
  }, { threshold: 0.05 });
  io.observe(root);

  (async function loop() {
    let session = 0;
    let pageId = 42;
    let totalPages = 41;
    let pagesThisSession = 0;
    while (true) {
      if (visible) {
        totalPages++;
        const firstCycleThisSession = (pagesThisSession === 0);
        await runCycle(session, pageId, totalPages, firstCycleThisSession);
        pageId++;
        pagesThisSession++;
        if (pagesThisSession >= 3) { session++; pagesThisSession = 0; }
      } else {
        await wait(600);
      }
    }
  })();
})();

(() => {
  const root = document.getElementById('commitAnim');
  if (!root) return;

  const bufPages = [...root.querySelectorAll('.ca-buffer .bp-page')];
  const slotA = root.querySelector('.ca-slot[data-slot="A"]');
  const slotB = root.querySelector('.ca-slot[data-slot="B"]');
  const slotBPages = [...slotB.querySelectorAll('.sp')];
  const godbyte = root.querySelector('.ca-godbyte');
  const gbOptA = godbyte.querySelector('.gb-opt[data-slot="A"]');
  const gbOptB = godbyte.querySelector('.gb-opt[data-slot="B"]');
  const gbBit0 = root.querySelector('#gbBit0');
  const merkle = root.querySelector('#caMerkle');
  const mLeaves = [...merkle.querySelectorAll('.m-leaves .m-node')];
  const mMids = [...merkle.querySelectorAll('.m-mids .m-node')];
  const mRoot = merkle.querySelector('#caMerkleRoot');
  const mEdges = {
    l0: merkle.querySelector('[data-edge="l0"]'),
    l1: merkle.querySelector('[data-edge="l1"]'),
    l2: merkle.querySelector('[data-edge="l2"]'),
    l3: merkle.querySelector('[data-edge="l3"]'),
    m0: merkle.querySelector('[data-edge="m0"]'),
    m1: merkle.querySelector('[data-edge="m1"]'),
  };
  const arrow = root.querySelector('.ca-arrow');
  const arrowLbl = root.querySelector('#caArrowLabel');
  const stepNum = root.querySelector('#caStep');
  const stepLbl = root.querySelector('#caLabel');
  const rootHashB = root.querySelector('[data-slot-root="B"]');
  const chips = [...root.querySelectorAll('.ca-chip')];
  const pauseBtn = root.querySelector('[data-ca="pause"]');
  const narrText = root.querySelector('#caNarrText');

  const DIRTY_PIDS = ['p1', 'p3'];
  const CLEAN_PIDS = ['p2', 'p4'];
  const PID_LABEL = { p1: '#42', p2: '#43', p3: '#44', p4: '#45' };

  async function narrate(text) {
    if (narrText.textContent === text) return;
    narrText.classList.add('swap');
    await wait(220);
    narrText.textContent = text;
    narrText.classList.remove('swap');
    await wait(180);
  }

  function resetAll() {
    bufPages.forEach(p => {
      p.classList.remove('flushing');
      if (DIRTY_PIDS.includes(p.dataset.pid)) {
        p.classList.add('dirty');
        p.querySelector('.state').textContent = 'modified';
      }
    });
    slotBPages.forEach(p => {
      p.classList.remove('written', 'hashing');
      p.classList.add('ghost');
      p.textContent = '';
      p.style.opacity = '';
    });
    slotB.classList.remove('next');
    slotB.querySelector('.badge').textContent = 'shadow';
    slotB.querySelector('.badge').className = 'badge shadow';
    rootHashB.innerHTML = '-';
    merkle.classList.remove('on');
    mLeaves.forEach(n => n.classList.remove('lit', 'reused'));
    mMids.forEach(n => n.classList.remove('lit'));
    mRoot.classList.remove('lit');
    Object.values(mEdges).forEach(e => e.classList.remove('lit'));
    godbyte.classList.remove('flipped', 'flipping', 'charging');
    gbBit0.classList.remove('flipping');
    gbBit0.textContent = '0';
    gbOptA.classList.add('active');
    gbOptB.classList.remove('active');
    slotA.querySelector('.badge').textContent = 'active';
    slotA.querySelector('.badge').className = 'badge active';
    arrow.classList.remove('active');
    arrowLbl.textContent = '';
    narrText.textContent = 'Two dirty pages in the buffer pool are ready to commit. Before a single byte touches disk, the active slot stays untouched, so a crash right now leaves the last good snapshot intact.';
  }

  function setChip(n) {
    chips.forEach(c => {
      const i = +c.dataset.step;
      c.classList.toggle('active', i === n);
      c.classList.toggle('done', i < n);
    });
    stepNum.textContent = String(n);
    stepLbl.textContent = chips[n - 1].textContent.replace(/^\d+\s*/, '').trim();
  }

  async function step1() {
    setChip(1);
    await narrate('Copy-on-write: the dirty pages get encrypted and written to fresh locations in slot B. The live slot A never changes, so a crash mid-write is harmless - recovery just keeps using A.');
    arrowLbl.textContent = 'encode / seal (AES-256-CTR + HMAC)';
    arrow.classList.add('active');
    await wait(1000);
    for (let k = 0; k < DIRTY_PIDS.length; k++) {
      const pid = DIRTY_PIDS[k];
      const bp = bufPages.find(p => p.dataset.pid === pid);
      const sp = slotBPages.find(p => p.dataset.pid === pid);
      if (!bp || !sp) continue;
      bp.classList.add('flushing');
      bp.querySelector('.state').textContent = 'sealing...';
      await wait(1400);
      sp.classList.remove('ghost');
      sp.classList.add('written');
      sp.textContent = PID_LABEL[pid];
      await wait(800);
      bp.classList.remove('flushing', 'dirty');
      bp.classList.add('clean');
      bp.querySelector('.state').textContent = 'clean';
      await wait(1000);
    }
    for (const pid of CLEAN_PIDS) {
      const sp = slotBPages.find(p => p.dataset.pid === pid);
      if (!sp) continue;
      sp.classList.remove('ghost');
      sp.textContent = PID_LABEL[pid];
      sp.style.opacity = '0.6';
      await wait(420);
    }
    arrow.classList.remove('active');
    arrowLbl.textContent = '';
    await wait(2200);
  }

  async function step2() {
    setChip(2);
    await narrate('Only the modified pages are re-hashed. Clean pages keep their existing BLAKE3 hash from disk, so the tree only does work where work is needed. The new root summarizes the whole snapshot.');
    merkle.classList.add('on');
    await wait(1500);
    const dirtyLeafIdx = [0, 2];
    const cleanLeafIdx = [1, 3];
    cleanLeafIdx.forEach(i => mLeaves[i].classList.add('reused'));
    await wait(900);
    for (const i of dirtyLeafIdx) {
      slotBPages[i].classList.add('hashing');
      mLeaves[i].classList.add('lit');
      await wait(1500);
      slotBPages[i].classList.remove('hashing');
      await wait(500);
    }
    await wait(1300);
    mEdges.l0.classList.add('lit');
    mEdges.l1.classList.add('lit');
    await wait(1050);
    mMids[0].classList.add('lit');
    await wait(1400);
    mEdges.l2.classList.add('lit');
    mEdges.l3.classList.add('lit');
    await wait(1050);
    mMids[1].classList.add('lit');
    await wait(1400);
    mEdges.m0.classList.add('lit');
    mEdges.m1.classList.add('lit');
    await wait(1050);
    mRoot.classList.add('lit');
    await wait(1300);
    const target = '3fb7a...2c91';
    for (let i = 0; i < target.length; i++) {
      rootHashB.textContent = target.slice(0, i + 1);
      await wait(160);
    }
    await wait(2800);
  }

  async function step3() {
    setChip(3);
    await narrate('The root hash and metadata go into slot B, the inactive 240-byte commit record. Slot A is still what everyone reads. If we crash here, slot B is half-written garbage and gets ignored.');
    await wait(1100);
    const b = slotB.querySelector('.badge');
    b.textContent = 'next';
    b.className = 'badge next';
    slotB.classList.add('next');
    const a = slotA.querySelector('.badge');
    a.textContent = 'current';
    await wait(3200);
  }

  async function step4() {
    setChip(4);
    await narrate('A single byte flips in the file header. That one atomic write switches the active slot from A to B - the entire snapshot is promoted. The previous slot becomes the rollback copy.');
    await wait(1200);
    godbyte.classList.add('charging');
    await wait(600);
    godbyte.classList.remove('charging');
    godbyte.classList.add('flipping');
    gbBit0.classList.add('flipping');
    await wait(250);
    gbBit0.textContent = '1';
    gbOptA.classList.remove('active');
    gbOptB.classList.add('active');
    await wait(250);
    godbyte.classList.add('flipped');
    godbyte.classList.remove('flipping');
    gbBit0.classList.remove('flipping');
    await wait(1300);
    slotA.querySelector('.badge').textContent = 'stale';
    slotA.querySelector('.badge').className = 'badge stale';
    slotB.querySelector('.badge').textContent = 'active';
    slotB.querySelector('.badge').className = 'badge active';
    await wait(3600);
  }

  let running = true;
  let userPaused = false;
  let cancelTokens = [];
  function wait(ms) {
    return new Promise(res => {
      const t = setTimeout(res, ms);
      cancelTokens.push(() => { clearTimeout(t); res(); });
    });
  }
  function cancelWaits() {
    while (cancelTokens.length) { try { cancelTokens.pop()(); } catch (_) {} }
  }

  let jumpTo = null;
  async function cycle() {
    resetAll();
    await wait(1800);
    await step1(); if (jumpTo !== null) return;
    await step2(); if (jumpTo !== null) return;
    await step3(); if (jumpTo !== null) return;
    await step4(); if (jumpTo !== null) return;
    await wait(3600);
  }

  async function loop() {
    while (true) {
      if (jumpTo !== null) {
        const n = jumpTo; jumpTo = null;
        resetAll();
        if (n >= 1) await step1();
        if (n >= 2) await step2();
        if (n >= 3) await step3();
        if (n >= 4) await step4();
        await wait(1200);
        continue;
      }
      if (!running) { await wait(250); continue; }
      await cycle();
    }
  }

  chips.forEach(c => c.addEventListener('click', () => { jumpTo = +c.dataset.step; cancelWaits(); }));
  pauseBtn.addEventListener('click', () => {
    userPaused = !userPaused;
    running = !userPaused;
    pauseBtn.querySelector('.ico-pause').style.display = userPaused ? 'none' : '';
    pauseBtn.querySelector('.ico-play').style.display = userPaused ? '' : 'none';
    if (!running) cancelWaits();
  });
  const io = new IntersectionObserver(entries => {
    if (userPaused) return;
    running = entries[0].isIntersecting;
    if (!running) cancelWaits();
  }, { threshold: 0.15 });
  io.observe(root);

  loop();
})();
