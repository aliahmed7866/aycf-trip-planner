(() => {
  const form = document.getElementById('scan-form');
  const input = form.querySelector('#destination-search');
  const chips = form.querySelector('#destination-selected');
  const anywhere = form.querySelector('#anywhere-btn');
  const returnTrip = form.querySelector('#return_trip');
  const returnDate = form.querySelector('#return-start');
  const legacy = form.querySelector('#legacy-destination');
  const message = form.querySelector('#destination-mode-text');
  const cities = new Map(Array.from(input.list.options, opt => [opt.value.toLowerCase(), opt.value]));
  const selected = new Set();
  const typedCity = () => cities.get(input.value.trim().toLowerCase());

  function sync() {
    const from = Boolean(form.querySelector('.origin-choice:checked'));
    const hasDestination = selected.size > 0 || Boolean(typedCity());
    legacy.value = selected.values().next().value || '';
    returnTrip.disabled = !from || !hasDestination;
    if (returnTrip.disabled) returnTrip.checked = false;
    returnDate.disabled = !returnTrip.checked;
    anywhere.setAttribute('aria-pressed', String(!hasDestination));
    message.textContent = hasDestination
      ? (from ? '' : 'Searching from any scanned origin.')
      : 'Choose a starting airport to search anywhere.';
  }

  function addTyped() {
    const name = typedCity();
    if (!name) {
      input.setCustomValidity('Choose a destination from the list.');
      input.reportValidity();
      return false;
    }
    if (!selected.has(name)) {
      selected.add(name);
      const chip = document.createElement('button');
      chip.type = 'button';
      chip.className = 'btn btn-sm btn-outline-light';
      chip.textContent = name + ' ×';
      chip.setAttribute('aria-label', 'Remove ' + name);
      const value = document.createElement('input');
      value.type = 'hidden';
      value.name = 'destinations';
      value.value = name;
      chip.addEventListener('click', () => {
        selected.delete(name);
        chip.remove();
        value.remove();
        input.focus();
        sync();
      });
      chips.append(chip, value);
    }
    input.value = '';
    input.setCustomValidity('');
    sync();
    return true;
  }

  input.addEventListener('input', () => { input.setCustomValidity(''); sync(); });
  form.querySelector('#add-destination').addEventListener('click', () => {
    if (addTyped()) input.focus();
  });
  input.addEventListener('keydown', event => {
    if (event.key === 'Enter' && input.value.trim() && !event.isComposing) {
      event.preventDefault();
      addTyped();
    }
  });
  anywhere.addEventListener('click', () => {
    selected.clear();
    chips.replaceChildren();
    input.value = '';
    input.setCustomValidity('');
    sync();
  });
  form.addEventListener('change', () => { input.setCustomValidity(''); sync(); });
  form.addEventListener('submit', event => {
    if (input.value.trim() && !addTyped()) {
      event.preventDefault();
    } else if (!selected.size && !form.querySelector('.origin-choice:checked')) {
      event.preventDefault();
      input.setCustomValidity('Choose a starting airport or a destination.');
      input.reportValidity();
    }
  });
  sync();
})();
