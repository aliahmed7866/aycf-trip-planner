/* Small, offline interactions shared by the installed planner and web app. */
(() => {
  const feedback = document.getElementById('app-feedback');
  let timer;
  function announce(message) {
    if (!feedback) return;
    clearTimeout(timer);
    feedback.textContent = message;
    feedback.hidden = false;
    timer = setTimeout(() => { feedback.hidden = true; }, 4500);
  }
  document.querySelectorAll('[data-copy-trip]').forEach(button => {
    button.addEventListener('click', async () => {
      const text = document.getElementById(button.dataset.copyTrip)?.value;
      if (!text) return;
      try {
        if (navigator.clipboard?.writeText) await navigator.clipboard.writeText(text);
        else {
          const field = document.createElement('textarea');
          field.value = text;
          field.style.cssText = 'position:fixed;top:0;left:0;opacity:0';
          document.body.appendChild(field);
          try {
            field.select();
            if (!document.execCommand('copy')) throw new Error('Clipboard unavailable');
          } finally { field.remove(); button.focus(); }
        }
        announce('Trip copied. Paste it into your notes or a message.');
      } catch (_) { announce('Copy is unavailable. Open flight details to select the itinerary.'); }
    });
  });
  // Leave validation to each form. Lock only after its submit handlers have run.
  document.addEventListener('submit', event => {
    const form = event.target;
    if (!form.matches('[data-busy-form]')) return;
    if (form.dataset.submitting) { event.preventDefault(); return; }
    queueMicrotask(() => {
      if (event.defaultPrevented) return;
      form.dataset.submitting = 'true';
      form.setAttribute('aria-busy', 'true');
      const button = event.submitter;
      if (button) {
        button.dataset.originalText = button.textContent;
        button.textContent = form.dataset.busyForm || 'Working…';
        button.disabled = true;
      }
    });
  });
  window.addEventListener('pageshow', () => {
    document.querySelectorAll('[data-submitting]').forEach(form => {
      delete form.dataset.submitting;
      form.removeAttribute('aria-busy');
      form.querySelectorAll('[data-original-text]').forEach(button => {
        button.textContent = button.dataset.originalText;
        button.disabled = false;
        delete button.dataset.originalText;
      });
    });
  });
})();
