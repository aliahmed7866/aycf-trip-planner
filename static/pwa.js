(() => {
  if ("serviceWorker" in navigator) {
    window.addEventListener("load", () => {
      navigator.serviceWorker.register("/service-worker.js").catch(() => {});
    });
  }
  if (window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true) return;

  let promptEvent = null;
  const button = document.createElement("button");
  button.type = "button";
  button.textContent = "Install AYCF";
  button.setAttribute("aria-label", "Install AYCF Flight OS on this phone");
  Object.assign(button.style, {
    position: "fixed", right: "14px", bottom: "calc(18px + env(safe-area-inset-bottom))",
    zIndex: "9999", display: "block", padding: "11px 15px", border: "1px solid #8f6cff",
    borderRadius: "999px", background: "#6f4cff", color: "#fff", fontWeight: "800",
    boxShadow: "0 12px 34px rgba(0,0,0,.35)"
  });
  document.body.appendChild(button);

  window.addEventListener("beforeinstallprompt", event => {
    event.preventDefault();
    promptEvent = event;
  });

  button.addEventListener("click", async () => {
    if (promptEvent) {
      promptEvent.prompt();
      const choice = await promptEvent.userChoice;
      if (choice.outcome === "accepted") button.remove();
      promptEvent = null;
      return;
    }
    window.alert("Open Chrome's three-dot menu and choose Install app or Add to Home screen. If the page was already open during an update, refresh it once first.");
  });

  window.addEventListener("appinstalled", () => button.remove());
})();
