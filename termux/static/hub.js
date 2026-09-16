(() => {
  const search = document.getElementById('app-search');
  if (search) search.addEventListener('input', () => {
    const query = search.value.trim().toLocaleLowerCase();
    let visible = 0;
    document.querySelectorAll('[data-app-name]').forEach(card => {
      card.hidden = !card.dataset.appName.includes(query);
      if (!card.hidden) visible++;
    });
    document.getElementById('no-apps').hidden = visible > 0;
  });
  document.querySelectorAll('form[data-operation]').forEach(form => {
    form.addEventListener('submit', event => {
      if (form.dataset.pending) { event.preventDefault(); return; }
      form.dataset.pending = 'true';
      const feedback = document.getElementById('operation-feedback');
      feedback.textContent = form.dataset.operation + ' Setup may take a few minutes.';
      feedback.hidden = false;
      form.querySelector('button').disabled = true;
    });
  });
  window.addEventListener('pageshow', event => { if (event.persisted) location.reload(); });
})();
