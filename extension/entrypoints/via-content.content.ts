import { defineContentScript } from "wxt/sandbox";
import { predict } from "../lib/api";
import { renderBadge } from "../lib/badge";
import { parseTrips } from "../lib/parser";

export default defineContentScript({
  matches: [
    "https://reservia.viarail.ca/*",
    "https://www.viarail.ca/*"
  ],
  runAt: "document_idle",
  main() {
    const run = debounce(async () => {
      const trips = parseTrips();
      if (trips.length === 0) return;
      try {
        const preds = await predict(trips.map(t => t.item));
        const byKey = new Map(
          preds.map(p => [`${p.train_number}|${p.service_date}`, p])
        );
        for (const trip of trips) {
          const key = `${trip.item.train_number}|${trip.item.service_date}`;
          const pred = byKey.get(key);
          if (pred) renderBadge(trip.element, pred);
        }
      } catch (e) {
        console.warn("[via-delay] prediction failed", e);
      }
    }, 400);

    run();
    const obs = new MutationObserver(run);
    obs.observe(document.body, { childList: true, subtree: true });
  }
});

function debounce<T extends (...a: any[]) => void>(fn: T, ms: number): T {
  let t: number | undefined;
  return ((...a: any[]) => {
    if (t) window.clearTimeout(t);
    t = window.setTimeout(() => fn(...a), ms);
  }) as T;
}
