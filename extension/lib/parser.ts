import type { PredictItem } from "./api";

/**
 * Scan the VIA Rail results page for train rows and extract the metadata
 * needed for a prediction call. The VIA markup changes periodically, so this
 * function uses several selectors and regex fallbacks.
 */
export interface ParsedTrip {
  element: HTMLElement;
  item: PredictItem;
}

const TRAIN_RE = /\b(?:train|tr\.?|#)\s*0*(\d{1,4})\b/i;

function findDateInPage(): string {
  // Look for a query param ?date=YYYY-MM-DD or a visible ISO date.
  const url = new URL(window.location.href);
  const q = url.searchParams.get("date") || url.searchParams.get("outboundDate");
  if (q && /^\d{4}-\d{2}-\d{2}$/.test(q)) return q;
  const m = document.body.innerText.match(/\b(20\d{2}-\d{2}-\d{2})\b/);
  if (m) return m[1];
  return new Date().toISOString().slice(0, 10);
}

export function parseTrips(root: ParentNode = document): ParsedTrip[] {
  const candidates = Array.from(
    root.querySelectorAll<HTMLElement>(
      '[data-testid*="trip"], [class*="trip"], [class*="segment"], [class*="result-row"], li[role="listitem"]'
    )
  );

  const date = findDateInPage();
  const results: ParsedTrip[] = [];
  const seen = new WeakSet<HTMLElement>();

  for (const el of candidates) {
    if (seen.has(el)) continue;
    const text = el.innerText || "";
    const m = text.match(TRAIN_RE);
    if (!m) continue;
    const trainNumber = m[1];
    if (el.querySelector(".via-delay-badge")) continue;
    seen.add(el);
    results.push({
      element: el,
      item: {
        train_number: trainNumber,
        service_date: date
      }
    });
  }
  return results;
}
