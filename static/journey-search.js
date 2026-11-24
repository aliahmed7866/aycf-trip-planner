(() => {
  const form = document.getElementById('scan-form');
  if (!form) return;
  const field = document.getElementById('destination-search');
  const box = document.getElementById('destination-options');
  const chips = document.getElementById('destination-selected');
  const values = document.getElementById('destination-values');
  const legacy = document.getElementById('legacy-destination');
  const mode = document.getElementById('destination-mode-text');
  const anywhere = document.getElementById('anywhere-btn');
  const returnChoice = document.getElementById('return_trip');
  const returnDate = document.getElementById('return-start');
  const error = document.getElementById('search-error');
  const origins = [...document.querySelectorAll('.origin-choice')];
  const all = JSON.parse(document.getElementById('journey-destinations').textContent);
  const selected = [];
  const normalize = text => text.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  function close() { box.style.display = 'none'; field.setAttribute('aria-expanded', 'false'); }
  function sync() {
    const canReturn = selected.length > 0 && origins.some(input => input.checked);
    if (!canReturn) returnChoice.checked = false;
    returnChoice.disabled = !canReturn;
    returnDate.disabled = !canReturn || !returnChoice.checked;
    legacy.value = selected[0] || '';
    mode.textContent = selected.length ? `${selected.length} destination${selected.length === 1 ? '' : 's'} selected.` : 'Anywhere is selected until you add a destination.';
    anywhere.setAttribute('aria-pressed', String(!selected.length));
    error.hidden = true;
  }
  function draw() {
    chips.replaceChildren(); values.replaceChildren();
    selected.forEach(name => {
      const chip = document.createElement('button');
      chip.type = 'button'; chip.className = 'btn btn-outline-light btn-sm';
      chip.textContent = name + ' ×'; chip.setAttribute('aria-label', 'Remove ' + name);
      chip.addEventListener('click', () => { selected.splice(selected.indexOf(name), 1); draw(); field.focus(); });
      chips.append(chip);
      const hidden = document.createElement('input');
      hidden.type = 'hidden'; hidden.name = 'destinations'; hidden.value = name;
      values.append(hidden);
    });
    sync();
  }
  function add(name) { if (!selected.includes(name)) selected.push(name); field.value = ''; close(); draw(); field.focus(); close(); }
  function render() {
    const query = normalize(field.value.trim());
    const matches = all.filter(item => !selected.includes(item.name) && normalize(item.name + ' ' + item.code).includes(query)).slice(0, 50);
    box.replaceChildren();
    matches.forEach(item => {
      const button = document.createElement('button');
      button.type = 'button'; button.className = 'btn btn-outline-light text-start w-100 mb-1';
      button.textContent = item.name + (item.code ? ' · ' + item.code : '');
      button.addEventListener('click', () => add(item.name));
      box.append(button);
    });
    if (!matches.length) {
      const message = document.createElement('p'); message.className = 'small muted p-2 mb-0';
      message.textContent = 'No matching destination in this scan. Try another name or airport code.'; box.append(message);
    }
    box.style.display = 'block'; field.setAttribute('aria-expanded', 'true');
  }
  field.addEventListener('input', render); field.addEventListener('focus', render);
  field.addEventListener('keydown', event => {
    if (event.key === 'ArrowDown') { event.preventDefault(); render(); box.querySelector('button')?.focus(); }
    if (event.key === 'Escape') { event.preventDefault(); close(); }
    if (event.key === 'Enter' && field.value.trim()) {
      event.preventDefault();
      const exact = all.find(item => [item.name, item.code].some(value => normalize(value) === normalize(field.value.trim())));
      if (exact) add(exact.name); else { render(); box.querySelector('button')?.focus(); }
    }
  });
  box.addEventListener('keydown', event => {
    const buttons = [...box.querySelectorAll('button')], index = buttons.indexOf(document.activeElement);
    if (['ArrowDown', 'ArrowUp'].includes(event.key)) {
      event.preventDefault(); buttons[(index + (event.key === 'ArrowDown' ? 1 : buttons.length - 1)) % buttons.length]?.focus();
    }
    if (event.key === 'Escape') { field.focus(); close(); }
  });
  document.addEventListener('click', event => { if (event.target !== field && !box.contains(event.target)) close(); });
  form.addEventListener('focusout', () => { setTimeout(() => { if (document.activeElement !== field && !box.contains(document.activeElement)) close(); }, 0); });
  anywhere.addEventListener('click', () => { selected.splice(0); field.value = ''; close(); draw(); });
  origins.forEach(input => input.addEventListener('change', sync)); returnChoice.addEventListener('change', sync);
  form.addEventListener('submit', event => {
    let message = '';
    if (!origins.some(input => input.checked) && !selected.length) message = 'Choose a starting airport or add a destination first.';
    if (field.value.trim()) message = 'Choose your typed destination from the suggestions, or clear it to continue.';
    if (message) { event.preventDefault(); error.textContent = message; error.hidden = false; field.focus(); close(); }
  });
  draw();
})();
