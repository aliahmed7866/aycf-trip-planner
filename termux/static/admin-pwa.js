(() => {
  const standalone = window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
  if (standalone) return;

  let promptEvent = null;
  let registration = null;
  const button = document.createElement("button");
  button.type = "button";
  button.textContent = "Preparing Admin Hub…";
  button.setAttribute("aria-label", "Install Phone Admin Hub on this phone");
  button.className = "secondary install-button";
  // Keep installation in the page flow so it never covers phone navigation.
  const installHost = document.querySelector(".page-footer, .login");
  if (installHost) installHost.appendChild(button);

  window.addEventListener("beforeinstallprompt", event => {
    event.preventDefault();
    promptEvent = event;
    button.textContent = "Install Admin Hub";
  });

  window.addEventListener("appinstalled", () => button.remove());

  async function prepare() {
    if (!("serviceWorker" in navigator) || !window.isSecureContext) {
      button.textContent = "Check Admin Hub install";
      return;
    }
    try {
      registration = await navigator.serviceWorker.register("/service-worker.js", { scope: "/" });
      await navigator.serviceWorker.ready;
      // The worker claims this tab on activation. Do not reload active controls
      // or discard a search while native installation becomes available.
    } catch (_) {
      button.textContent = "Check Admin Hub install";
      return;
    }
    setTimeout(() => {
      if (!promptEvent) button.textContent = "Check Admin Hub install";
    }, 1500);
  }

  button.addEventListener("click", async () => {
    if (promptEvent) {
      promptEvent.prompt();
      const choice = await promptEvent.userChoice;
      if (choice.outcome === "accepted") button.remove();
      promptEvent = null;
      return;
    }

    let manifestStatus = "not checked";
    try {
      const link = document.querySelector('link[rel="manifest"]');
      const response = link ? await fetch(link.href, { cache: "no-store" }) : null;
      manifestStatus = response ? `${response.status} ${response.headers.get("content-type") || "unknown type"}` : "link missing";
    } catch (_) {
      manifestStatus = "unreachable";
    }
    const active = Boolean(registration && registration.active);
    const controlled = Boolean(navigator.serviceWorker && navigator.serviceWorker.controller);
    window.alert(
      "Chrome has not made native app installation available yet.\n\n" +
      `Secure context: ${window.isSecureContext ? "yes" : "no"}\n` +
      `Service worker active: ${active ? "yes" : "no"}\n` +
      `Page controlled: ${controlled ? "yes" : "no"}\n` +
      `Manifest: ${manifestStatus}\n\n` +
      "Refresh once if activation has just completed. Do not use a browser shortcut."
    );
  });

  prepare();
})();
