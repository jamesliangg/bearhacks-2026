const urlInput = document.getElementById("url") as HTMLInputElement;
const saveBtn = document.getElementById("save") as HTMLButtonElement;
const testBtn = document.getElementById("test") as HTMLButtonElement;
const status = document.getElementById("status") as HTMLDivElement;

(async () => {
  const r = await chrome.storage.sync.get("backendBaseUrl");
  urlInput.value = (r?.backendBaseUrl as string) || "http://localhost:8003";
})();

saveBtn.addEventListener("click", async () => {
  await chrome.storage.sync.set({ backendBaseUrl: urlInput.value.trim() });
  status.textContent = "Saved.";
  status.className = "ok";
});

testBtn.addEventListener("click", async () => {
  try {
    const r = await fetch(`${urlInput.value.trim()}/healthz`);
    const ok = r.ok;
    status.textContent = ok ? "Reachable." : `HTTP ${r.status}`;
    status.className = ok ? "ok" : "err";
  } catch (e) {
    status.textContent = `Error: ${e}`;
    status.className = "err";
  }
});
