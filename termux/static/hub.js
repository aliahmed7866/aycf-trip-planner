(() => {
  const workspace = document.getElementById('main');
  if (!workspace) return;
  const cards = document.getElementById('app-cards');
  const search = document.getElementById('app-search');
  const connection = document.getElementById('connection-status');
  const refresh = document.getElementById('refresh-status');
  let filter = 'all';
  let timer, refreshing = false, submitting = false;
  let lastChecked = Date.now();

  function filterCards() {
    const query = search.value.trim().toLocaleLowerCase();
    let visible = 0;
    cards.querySelectorAll('[data-app-name]').forEach(card => {
      const matches = filter === 'all' || (filter === 'attention' ? card.dataset.attention === 'true' : card.dataset.state === filter);
      card.hidden = !matches || !card.dataset.appName.includes(query);
      if (!card.hidden) visible++;
    });
    document.getElementById('no-apps').hidden = visible > 0;
    const count = document.getElementById('filter-count');
    if (count) count.textContent = `${visible} shown`;
  }
  function formatTimes() {
    cards.querySelectorAll('time[data-timestamp]').forEach(item => {
      const date = new Date(Number(item.dataset.timestamp) * 1000);
      if (!Number.isFinite(date.getTime())) return;
      item.dateTime = date.toISOString();
      item.textContent = date.toLocaleString(undefined, {month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'});
      item.title = date.toISOString();
    });
  }
  search.addEventListener('input', filterCards);
  filterCards();
  if (workspace.dataset.section === 'home') return;
  document.querySelectorAll('[data-filter]').forEach(button => button.addEventListener('click', () => {
    filter = button.dataset.filter;
    document.querySelectorAll('[data-filter]').forEach(item => item.setAttribute('aria-pressed', String(item === button)));
    filterCards();
  }));
  document.addEventListener('submit', event => {
    const form = event.target.closest('form[data-operation]');
    if (!form) return;
    if (submitting) { event.preventDefault(); return; }
    submitting = true;
    clearTimeout(timer);
    const feedback = document.getElementById('operation-feedback');
    feedback.textContent = form.dataset.operation;
    feedback.hidden = false;
    const button = form.querySelector('button');
    button.disabled = true;
    button.setAttribute('aria-busy', 'true');
    button.textContent = 'Working…';
  });

  async function refreshStatus() {
    clearTimeout(timer);
    if (refreshing || submitting || document.hidden) return;
    refreshing = true;
    if (refresh) refresh.disabled = true;
    let delay = 20000;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    try {
      const url = new URL(workspace.dataset.statusUrl, location.href);
      url.searchParams.set('section', workspace.dataset.section);
      const response = await fetch(url, {cache: 'no-store', signal: controller.signal});
      if (response.status === 401) throw new Error('auth');
      if (!response.ok) throw new Error('offline');
      const data = await response.json();
      if (submitting) return;
      const template = document.createElement('template');
      template.innerHTML = data.cards;
      const incoming = [...template.content.querySelectorAll('[data-app-id]')];
      const existing = [...cards.querySelectorAll('[data-app-id]')];
      for (const next of incoming) {
        const current = existing.find(item => item.dataset.appId === next.dataset.appId);
        if (!current) { cards.append(next); continue; }
        // Do not remove a control while someone is using their keyboard or touch.
        if (current.contains(document.activeElement)) {
          // Keep the focused control, but never freeze health or action availability.
          current.dataset.state = next.dataset.state;
          current.dataset.attention = next.dataset.attention;
          for (const selector of ['.badge', '.service-facts', '.update-row small']) {
            const previous = current.querySelector(selector);
            const replacement = next.querySelector(selector);
            if (previous && replacement) previous.replaceWith(replacement.cloneNode(true));
          }
          current.querySelectorAll(':scope > .update-status').forEach(item => item.remove());
          next.querySelectorAll(':scope > .update-status').forEach(item => {
            current.querySelector('.service-facts').before(item.cloneNode(true));
          });
          current.querySelectorAll('form').forEach(form => {
            const incomingForm = [...next.querySelectorAll('form')].find(item => item.getAttribute('action') === form.getAttribute('action'));
            const button = form.querySelector('button');
            const incomingButton = incomingForm?.querySelector('button');
            if (button && incomingButton) {
              button.disabled = incomingButton.disabled;
              button.textContent = incomingButton.textContent;
            }
          });
          continue;
        }
        const open = [...current.querySelectorAll('details')].map(item => item.open);
        next.querySelectorAll('details').forEach((item, index) => { item.open = Boolean(open[index]); });
        current.replaceWith(next);
      }
      existing.filter(item => !incoming.some(next => next.dataset.appId === item.dataset.appId))
        .forEach(item => { if (!item.contains(document.activeElement)) item.remove(); });
      document.getElementById('running-count').textContent = data.totals.running;
      document.getElementById('attention-count').textContent = data.totals.attention;
      document.querySelectorAll('input[name="csrf_token"]').forEach(input => { input.value = data.csrf; });
      lastChecked = Date.now();
      connection.textContent = 'Live · checked ' + new Date(lastChecked).toLocaleTimeString(undefined, {hour: '2-digit', minute: '2-digit'});
      connection.classList.remove('stale');
      formatTimes();
      filterCards();
      delay = data.busy ? 5000 : 20000;
    } catch (error) {
      const checked = new Date(lastChecked).toLocaleTimeString(undefined, {hour: '2-digit', minute: '2-digit'});
      connection.textContent = error.message === 'auth' ? 'Session ended · reload to sign in' : `Reconnecting · last checked ${checked}`;
      connection.classList.add('stale');
      delay = 10000;
    } finally {
      clearTimeout(timeout);
      refreshing = false;
      if (refresh) refresh.disabled = false;
      if (!submitting && !document.hidden) timer = setTimeout(refreshStatus, delay);
    }
  }
  if (refresh) refresh.addEventListener('click', refreshStatus);
  document.addEventListener('visibilitychange', () => {
    clearTimeout(timer);
    if (!document.hidden) refreshStatus();
  });
  window.addEventListener('online', refreshStatus);
  window.addEventListener('pageshow', event => { if (event.persisted) location.reload(); });
  formatTimes();
  filterCards();
  timer = setTimeout(refreshStatus, 5000);
})();
