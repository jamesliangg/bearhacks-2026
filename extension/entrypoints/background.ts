import { defineBackground } from "wxt/sandbox";

export default defineBackground(() => {
  chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    if (msg?.type === "predict") {
      (async () => {
        try {
          const r = await fetch(`${msg.baseUrl}/predict`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ items: msg.items })
          });
          const body = await r.json();
          sendResponse({ ok: true, body });
        } catch (e: any) {
          sendResponse({ ok: false, error: String(e) });
        }
      })();
      return true; // async
    }
    return false;
  });
});
