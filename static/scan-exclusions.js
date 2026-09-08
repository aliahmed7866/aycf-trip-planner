(() => {
  const form = document.getElementById('exclusion-form');
  if (!form) return;
  const norm = value => value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const matches = (row, terms) => terms.every(term => norm(row.dataset.search).includes(term));
  document.getElementById('exclusion-search').addEventListener('input', event => {
    const terms = norm(event.target.value).trim().split(/\s+/).filter(Boolean);
    let visible = 0;
    form.querySelectorAll('[data-country-group]').forEach(group => {
      let count = 0;
      group.querySelectorAll('[data-airport-row]').forEach(row => {
        row.hidden = !matches(row, terms);
        if (!row.hidden) count++;
      });
      group.hidden = !count && !terms.every(term => norm(group.dataset.country).includes(term));
      if (!group.hidden) { visible++; if (terms.length) group.open = true; }
    });
    document.getElementById('exclusion-no-results').hidden = visible > 0;
  });
  document.getElementById('route-exclusion-search').addEventListener('input', event => {
    const terms = norm(event.target.value).trim().split(/\s+/).filter(Boolean);
    form.querySelectorAll('[data-route-row]').forEach(row => { row.hidden = !matches(row, terms); });
  });
  let timer, generation = 0;
  const preview = document.getElementById('exclusion-preview');
  function changed() {
    document.getElementById('exclusion-dirty').textContent = 'Unsaved changes.';
    const current = ++generation;
    clearTimeout(timer);
    preview.textContent = 'Updating estimate…';
    timer = setTimeout(async () => {
      try {
        const response = await fetch(form.dataset.previewUrl, {method: 'POST', body: new FormData(form), headers: {'Accept': 'application/json'}});
        if (!response.ok) throw new Error('Preview unavailable');
        const data = await response.json();
        if (current !== generation) return;
        preview.replaceChildren();
        const title = document.createElement('strong');
        title.textContent = `${data.route_count} routes · ${data.request_units} airport/date requests · ~${data.estimated_minutes} min`;
        const detail = document.createElement('div');
        detail.className = 'small muted';
        detail.textContent = `${data.saved_requests} requests avoided per four-day scan. Estimates exclude retries and login.`;
        preview.append(title, detail);
        document.getElementById('exclusion-conflicts').textContent = !data.route_count ? 'All scan routes are excluded. Re-enable places before starting a scan.' : data.blocked_preferences.length ? `Preferred destinations paused: ${data.blocked_preferences.join(', ')}` : '';
      } catch (_) {
        if (current === generation) preview.textContent = 'Could not update the estimate. Save and reload to see the scan size.';
      }
    }, 250);
  }
  form.addEventListener('change', changed);
  document.getElementById('clear-exclusions').addEventListener('click', () => {
    form.querySelectorAll('input[type="checkbox"]').forEach(input => { input.checked = false; });
    changed();
  });
})();
