(() => {
  const drawer = document.querySelector('#cart-drawer');
  const body = document.querySelector('#cart-body');
  let opener;
  const toast = message => {
    const el = document.querySelector('#toast');
    el.textContent = message; el.hidden = false;
    clearTimeout(el.timer); el.timer = setTimeout(() => { el.hidden = true; }, 4500);
  };
  function openDrawer() {
    if (!drawer.open) { opener = document.activeElement; drawer.showModal(); document.body.classList.add('drawer-open'); }
  }
  function closeDrawer() { drawer.close(); }
  drawer.addEventListener('close', () => { document.body.classList.remove('drawer-open'); opener?.focus(); });
  document.addEventListener('click', async e => {
    const open = e.target.closest('[data-open-cart]');
    if (open) {
      e.preventDefault();
      try {
        const response = await fetch('/cart', {headers: {'X-Mahrukh-Drawer': '1'}});
        if (!response.ok) throw new Error();
        body.innerHTML = await response.text(); openDrawer();
      } catch { window.location.href = open.href; }
    }
    if (e.target.closest('[data-close-cart]')) closeDrawer();
    if (e.target === drawer) { const r = drawer.getBoundingClientRect(); if (e.clientX < r.left || e.clientX > r.right) closeDrawer(); }
    const thumbnail = e.target.closest('[data-image]');
    if (thumbnail) {
      document.querySelector('#main-product-image').src = thumbnail.dataset.image;
      document.querySelectorAll('[data-image]').forEach(b => b.setAttribute('aria-pressed', String(b === thumbnail)));
    }
  });
  document.addEventListener('submit', async e => {
    const form = e.target;
    if (form.dataset.confirm && !window.confirm(form.dataset.confirm)) { e.preventDefault(); return; }
    if (!form.matches('[data-cart-form]')) return;
    e.preventDefault();
    const data = new FormData(form);
    if (e.submitter?.name) data.set(e.submitter.name, e.submitter.value);
    const buttons = [...form.querySelectorAll('button')]; buttons.forEach(b => b.disabled = true);
    try {
      const response = await fetch(form.action, {method: 'POST', body: data, headers: {'X-Mahrukh-Drawer': '1'}});
      if (!response.ok) {
        const doc = new DOMParser().parseFromString(await response.text(), 'text/html');
        toast(doc.querySelector('main .page p:not(.eyebrow)')?.textContent || 'Unable to update the bag. Reload and try again.'); return;
      }
      body.innerHTML = await response.text();
      document.querySelectorAll('[data-cart-count]').forEach(el => el.textContent = response.headers.get('X-Cart-Count'));
      // Keep the non-modal cart page consistent after edits.
      if (location.pathname === '/cart' && !drawer.open) { location.reload(); return; }
      openDrawer();
    } catch { toast('Connection interrupted. Reload the bag to check whether your update was saved.'); }
    finally { buttons.forEach(b => b.disabled = false); }
  });
})();
