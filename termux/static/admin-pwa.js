(() => {
  const standalone = window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
  if (standalone) return;

  let promptEvent = null;
  let registration = null;
  const reloadKey = "pwa-controlled-reload-v1";
  const button = document.createElement("button");
  button.type = "button";
  button.textContent = "Preparing Admin Hub…";
  button.setAttribute("aria-label", "Install Phone Admin Hub on this phone");
  Object.assign(button.style, {
    position: "fixed", right: "14px", bottom: "calc(18px + env(safe-area-inset-bottom))",
    zIndex: "9999", display: "block", padding: "11px 15px", border: "1px solid #7658f6",
    borderRadius: "999px", background: "#6547e8", color: "#fff", fontWeight: "800",
    boxShadow: "0 12px 34px rgba(0,0,0,.35)"
  });
  document.body.appendChild(button);

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
      if (!navigator.serviceWorker.controller && sessionStorage.getItem(reloadKey) !== "1") {
        sessionStorage.setItem(reloadKey, "1");
        location.reload();
        return;
      }
      if (navigator.serviceWorker.controller) sessionStorage.removeItem(reloadKey);
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
