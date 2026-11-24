(() => {
  const form = document.getElementById('short-trip-form');
  if (!form) return;
  const presets = JSON.parse(document.getElementById('trip-presets').textContent);
  const leave = document.getElementById('leave-after'), back = document.getElementById('return-by');
  const note = document.getElementById('preset-note');
  const buttons = [...document.querySelectorAll('[data-trip-preset]')];
  buttons.forEach(button => button.addEventListener('click', () => {
    const value = presets[button.dataset.tripPreset];
    leave.value = value?.leave_after || '';
    back.value = value?.return_by || '';
    buttons.forEach(item => item.setAttribute('aria-pressed', String(item === button)));
    note.textContent = value ? 'Weekend dates added in UK time. Flights beyond your scan’s released dates will not appear yet.' : 'Dates cleared. Find any complete trip available in the current scan.';
  }));
  [leave, back].forEach(input => input.addEventListener('input', () => {
    buttons.forEach(button => button.setAttribute('aria-pressed', String(button.dataset.tripPreset === 'any' && !leave.value && !back.value)));
    note.textContent = 'Your chosen dates are in UK time. Allow time to travel home after landing.';
  }));
  document.getElementById('match-return-airports').addEventListener('click', () => {
    const selected = new Set([...form.querySelectorAll('[name=origins]:checked')].map(input => input.value));
    form.querySelectorAll('[name=returns]').forEach(input => { input.checked = selected.has(input.value); });
    note.textContent = selected.size ? 'Return airports now match your departure airports.' : 'Choose at least one departure airport, then copy it to your return.';
  });
})();
