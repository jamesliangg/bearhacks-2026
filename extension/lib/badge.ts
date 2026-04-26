import { SEVERITY_COLORS, severityFor } from "./severity";
import type { Prediction } from "./api";
import { recentDelays } from "./api";

export function renderBadge(host: HTMLElement, pred: Prediction) {
  if (host.querySelector(":scope > .via-delay-badge, .via-delay-badge")) return;
  const sev = pred.severity || severityFor(pred.p50_delay_min);
  const colors = SEVERITY_COLORS[sev];

  const wrap = document.createElement("span");
  wrap.className = "via-delay-badge";
  wrap.style.cssText = `
    display:inline-flex;align-items:center;gap:6px;margin-left:8px;
    padding:4px 8px;border-radius:999px;font:600 12px/1.2 system-ui,sans-serif;
    background:${colors.bg};color:${colors.fg};cursor:help;vertical-align:middle;
    box-shadow:0 1px 2px rgba(0,0,0,.15);
    white-space:nowrap;
    pointer-events:auto;
  `;

  const dot = document.createElement("span");
  dot.style.cssText =
    "width:8px;height:8px;border-radius:50%;background:#fff;opacity:.9;";
  wrap.appendChild(dot);

  const label = document.createElement("span");
  const p50 = Math.round(pred.p50_delay_min);
  const p90 = Math.round(pred.p90_delay_min);
  label.textContent =
    p50 <= 1 ? "On time (pred.)" : `~${p50} min late · up to ${p90}`;
  wrap.appendChild(label);

  const tip = [
    `Severity: ${colors.label}`,
    `Weekday effect: ${fmt(pred.factors.weekday_effect)}`,
    `Weather effect: ${fmt(pred.factors.weather_effect)}`,
    `Recent trend: ${fmt(pred.factors.recent_trend_effect)}`,
    `Route: ${fmt(pred.factors.route_effect)}`,
    `Model: ${pred.model_id}`
  ].join("\n");
  wrap.title = tip;

  const trainHead = host.querySelector<HTMLElement>("#train-num-1, .trip-head");
  if (trainHead) {
    trainHead.style.display ||= "inline-flex";
    trainHead.style.alignItems ||= "center";
    trainHead.insertAdjacentElement("beforeend", wrap);
  } else {
    host.prepend(wrap);
  }

  wrap.addEventListener("click", async (e) => {
    e.preventDefault();
    e.stopPropagation();

    const existing = host.querySelector<HTMLElement>(".via-delay-popover");
    if (existing) {
      existing.remove();
      return;
    }

    const pop = document.createElement("div");
    pop.className = "via-delay-popover";
    pop.style.cssText = [
      "position:relative",
      "margin-top:6px",
      "padding:10px 12px",
      "border:1px solid rgba(0,0,0,.15)",
      "border-radius:10px",
      "background:#fff",
      "color:#111",
      "font:500 12px/1.35 system-ui,sans-serif",
      "max-width:340px",
      "box-shadow:0 6px 20px rgba(0,0,0,.18)"
    ].join(";");

    const header = document.createElement("div");
    header.style.cssText = "display:flex;justify-content:space-between;gap:12px;align-items:center;margin-bottom:8px;";
    header.innerHTML = `<div style="font-weight:700">#${pred.train_number} · ${pred.service_date}</div><div style="opacity:.7">Model: ${pred.model_id}</div>`;
    pop.appendChild(header);

    const factors = document.createElement("div");
    factors.innerHTML = `
      <div style="font-weight:700;margin-bottom:4px;">Factors (min)</div>
      <div>Weekday: ${fmt1(pred.factors.weekday_effect)}</div>
      <div>Weather: ${fmt1(pred.factors.weather_effect)}</div>
      <div>Recent trend: ${fmt1(pred.factors.recent_trend_effect)}</div>
      <div>Route: ${fmt1(pred.factors.route_effect)}</div>
    `;
    pop.appendChild(factors);

    const recent = document.createElement("div");
    recent.style.cssText = "margin-top:10px;";
    recent.innerHTML = `<div style="font-weight:700;margin-bottom:4px;">5 most recent final delays</div><div style="opacity:.7">Loading…</div>`;
    pop.appendChild(recent);

    // Attach popover near header without affecting layout much.
    (trainHead?.parentElement || host).appendChild(pop);

    try {
      const rows = await recentDelays(pred.train_number, pred.service_date);
      const lines = rows.length
        ? rows.map(r => `<div>${r.service_date}: <span style="font-weight:700">${r.final_delay_min == null ? "—" : (Math.round(r.final_delay_min * 10) / 10).toFixed(1)}</span></div>`).join("")
        : `<div style="opacity:.7">No history yet.</div>`;
      recent.innerHTML = `<div style="font-weight:700;margin-bottom:4px;">5 most recent final delays</div>${lines}`;
    } catch (err) {
      recent.innerHTML = `<div style="font-weight:700;margin-bottom:4px;">5 most recent final delays</div><div style="color:#b42318">Failed to load.</div>`;
    }
  });

  // Some VIA rows intercept clicks; capture-phase handler ensures we still toggle.
  wrap.addEventListener(
    "pointerdown",
    (e) => {
      e.stopPropagation();
    },
    { capture: true }
  );
}

function fmt(v: number): string {
  const s = v >= 0 ? "+" : "";
  return `${s}${v.toFixed(1)} min`;
}

function fmt1(v: number): string {
  const r = Math.round(v * 10) / 10;
  const s = r >= 0 ? "+" : "";
  return `${s}${r.toFixed(1)}`;
}
