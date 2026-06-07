(async () => {
  const el = document.querySelector('[data-version-label]');
  if (!el) return;
  try {
    const r = await fetch('https://crates.io/api/v1/crates/citadeldb', {
      headers: { Accept: 'application/json' },
    });
    if (!r.ok) return;
    const j = await r.json();
    const v = j && j.crate && (j.crate.max_stable_version || j.crate.newest_version);
    if (v) el.textContent = 'citadeldb / v' + v;
  } catch (_) { /* keep fallback */ }
})();
