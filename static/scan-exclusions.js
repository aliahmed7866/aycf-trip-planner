(() => {
  const form = document.getElementById('exclusion-form');
  if (!form) return;
  const norm = value => value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const terms = value => norm(value).trim().split(/\s+/).filter(Boolean);
  const matches = (row, query) => query.every(term => norm(row.dataset.search).includes(term));
  const placeSearch = document.getElementById('exclusion-search');
  const routeSearch = document.getElementById('route-exclusion-search');
  const onlyExcluded = document.getElementById('show-excluded-only');
  const preview = document.getElementById('exclusion-preview');
  const status = document.getElementById('exclusion-dirty');
  const saveError = document.getElementById('exclusion-save-error');
  const groups = [...form.querySelectorAll('[data-country-group]')];
  const airportRows = [...form.querySelectorAll('[data-airport-row]')];
  const routeRows = [...form.querySelectorAll('[data-route-row]')];
  let timer, generation = 0, controller, dirty = false, saving = false;
  let excludedAirports = new Set(), excludedCountries = new Set();

  function inheritedReason(key, country) {
    if (excludedCountries.has(norm(country))) return 'Excluded by country';
    if (excludedAirports.has(key)) return 'Excluded by airport or city';
    return '';
  }
  function syncInherited() {
    excludedCountries = new Set([...form.querySelectorAll('[name="excluded_countries"]:checked')].map(input => norm(input.value)));
    excludedAirports = new Set();
    airportRows.forEach(row => {
      if (row.querySelector('input').checked) {
        excludedAirports.add(row.dataset.key);
        (row.dataset.members || '').split(',').filter(Boolean).forEach(key => excludedAirports.add(key));
      }
    });
    groups.forEach(group => {
      const rows = [...group.querySelectorAll('[data-airport-row]')];
      rows.forEach(row => {
        const countryBlocked = excludedCountries.has(norm(group.dataset.country));
        const cityBlocked = excludedAirports.has(row.dataset.key) && !row.querySelector('input').checked;
        row.dataset.inherited = String(countryBlocked || cityBlocked);
        row.querySelector('[data-inherited-note]').textContent = countryBlocked ? 'Excluded by country — clear the country choice to restore.' : cityBlocked ? 'Excluded by city — clear the city choice to restore.' : '';
      });
      const count = rows.filter(row => row.querySelector('input').checked || row.dataset.inherited === 'true').length;
      group.querySelector('[data-country-count]').textContent = `(${rows.length})${excludedCountries.has(norm(group.dataset.country)) ? ' · Country excluded' : count ? ` · ${count} excluded` : ''}`;
    });
    routeRows.forEach(row => {
      const reason = inheritedReason(row.dataset.origin, row.dataset.originCountry) || inheritedReason(row.dataset.destination, row.dataset.destinationCountry);
      row.dataset.inherited = String(Boolean(reason));
      row.querySelector('[data-inherited-note]').textContent = reason ? `${reason}; this route choice is retained separately.` : '';
    });
  }
  function filter() {
    const placeTerms = terms(placeSearch.value);
    let visible = 0;
    groups.forEach(group => {
      let count = 0;
      group.querySelectorAll('[data-airport-row]').forEach(row => {
        row.hidden = !matches(row, placeTerms) || (onlyExcluded.checked && !row.querySelector('input').checked && row.dataset.inherited !== 'true');
        if (!row.hidden) count++;
      });
      const countryMatch = placeTerms.every(term => norm(group.dataset.country).includes(term));
      group.hidden = !count && !(countryMatch && (!onlyExcluded.checked || excludedCountries.has(norm(group.dataset.country))));
      if (!group.hidden) { visible++; if (placeTerms.length || onlyExcluded.checked) group.open = true; }
    });
    document.getElementById('exclusion-no-results').hidden = visible > 0;
    const routeTerms = terms(routeSearch.value);
    let visibleRoutes = 0;
    routeRows.forEach(row => {
      row.hidden = !matches(row, routeTerms) || (onlyExcluded.checked && !row.querySelector('input').checked && row.dataset.inherited !== 'true');
      if (!row.hidden) visibleRoutes++;
    });
    document.getElementById('route-exclusion-no-results').hidden = visibleRoutes > 0;
  }
  function changed() {
    dirty = true;
    status.textContent = 'Unsaved changes.';
    saveError.textContent = '';
    syncInherited(); filter();
    const current = ++generation;
    clearTimeout(timer);
    if (controller) controller.abort();
    preview.textContent = 'Updating estimate…';
    timer = setTimeout(async () => {
      controller = new AbortController();
      try {
        const response = await fetch(form.dataset.previewUrl, {method: 'POST', body: new FormData(form), headers: {'Accept': 'application/json'}, signal: controller.signal});
        if (!response.ok) throw new Error('Preview unavailable');
        const data = await response.json();
        if (current !== generation) return;
        const title = document.createElement('strong');
        title.textContent = `${data.route_count} routes · ${data.request_units} airport/date requests · ~${data.estimated_minutes} min`;
        const detail = document.createElement('div');
        detail.className = 'small muted';
        detail.textContent = `${data.saved_requests} requests avoided · ${data.window.days} days · ${data.window.label}. Estimates exclude retries and login.`;
        preview.replaceChildren(title, detail);
        const messages = [];
        if (!data.route_count) messages.push('No routes remain in this scan scope. Re-enable places or adjust scanner configuration.');
        if (data.blocked_preferences.length) messages.push(`Preferred destinations paused: ${data.blocked_preferences.join(', ')}.`);
        if (data.blocked_watches) messages.push(`${data.blocked_watches} watch(es) paused.`);
        document.getElementById('exclusion-conflicts').textContent = messages.join(' ');
      } catch (error) {
        if (current === generation && error.name !== 'AbortError') preview.textContent = 'Could not update the estimate. Your choices are still here; try saving or reload.';
      }
    }, 250);
  }
  placeSearch.addEventListener('input', filter);
  routeSearch.addEventListener('input', filter);
  onlyExcluded.addEventListener('change', filter);
  form.addEventListener('change', event => { if (event.target.name.startsWith('excluded_')) changed(); });
  document.getElementById('clear-exclusions').addEventListener('click', () => {
    form.querySelectorAll('input[name^="excluded_"]').forEach(input => { input.checked = false; });
    changed();
  });
  window.addEventListener('beforeunload', event => {
    if (dirty && !saving) { event.preventDefault(); event.returnValue = ''; }
  });
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (saving) return;
    const body = new FormData(form);
    saving = true; clearTimeout(timer); generation++;
    if (controller) controller.abort();
    const controls = [...form.querySelectorAll('input, button')];
    controls.forEach(control => { control.disabled = true; });
    status.textContent = 'Saving…'; saveError.textContent = '';
    try {
      const response = await fetch(form.action, {method: 'POST', body, headers: {'Accept': 'application/json'}});
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data.ok) throw new Error(data.error || 'Could not save. Reload if your form has expired.');
      dirty = false;
      window.location.assign(data.url);
    } catch (error) {
      saving = false; controls.forEach(control => { control.disabled = false; });
      status.textContent = 'Not saved — your choices are retained on this page.';
      saveError.textContent = error.message;
    }
  });
  syncInherited(); filter();
})();
