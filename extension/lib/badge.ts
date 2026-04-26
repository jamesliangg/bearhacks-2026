import { SEVERITY_COLORS, severityFor } from "./severity";
import type { Prediction } from "./api";

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
}

function fmt(v: number): string {
  const s = v >= 0 ? "+" : "";
  return `${s}${v.toFixed(1)} min`;
}
