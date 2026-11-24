function fillWizzLogin(expectedUrl, email, password) {
  function trusted(value) {
    try {
      const url = new URL(value);
      return url.protocol === 'https:' && !url.username && !url.password &&
        (!url.port || url.port === '443') &&
        ['multipass.wizzair.com', 'wizzair.com', 'www.wizzair.com'].includes(url.hostname);
    } catch (_) { return false; }
  }
  // Never fill a frame, a redirected document, a GET form or an external action.
  if (window !== window.top || location.href !== expectedUrl || !trusted(location.href)) return 'blocked';
  const visible = field => !field.disabled && !field.readOnly && field.getClientRects().length > 0;
  for (const form of document.forms) {
    if (form.method.toLowerCase() !== 'post' || !trusted(form.action) ||
        form.target && !['_self', '_top'].includes(form.target)) continue;
    const secrets = [...form.querySelectorAll('input[type="password"]')].filter(visible);
    if (secrets.length !== 1 || secrets[0].autocomplete === 'new-password') continue;
    const address = [...form.querySelectorAll('input')].find(field => visible(field) &&
      ['email', 'text'].includes(field.type) &&
      /email|user|login|identifier/i.test([field.name, field.id, field.autocomplete, field.placeholder].join(' ')));
    if (!address) continue;
    if (address.value && address.value !== email || secrets[0].value) return 'edited';
    const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
    for (const [field, value] of [[address, email], [secrets[0], password]]) {
      setter.call(field, value);
      field.dispatchEvent(new Event('input', {bubbles: true}));
      field.dispatchEvent(new Event('change', {bubbles: true}));
    }
    // Wizz's submit button and any verification remain under the user's control.
    return 'filled';
  }
  return 'waiting';
}
