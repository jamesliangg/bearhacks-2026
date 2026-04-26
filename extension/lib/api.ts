export interface PredictItem {
  train_number: string;
  service_date: string;          // YYYY-MM-DD
  origin?: string;
  destination?: string;
  scheduled_departure?: string;  // ISO
}

export interface Prediction {
  train_number: string;
  service_date: string;
  p50_delay_min: number;
  p90_delay_min: number;
  severity: "on_time" | "minor" | "moderate" | "significant" | "severe";
  factors: {
    weekday_effect: number;
    weather_effect: number;
    recent_trend_effect: number;
    route_effect: number;
  };
  model_id: string;
  as_of: string;
}

export interface RecentDelay {
  service_date: string;
  final_delay_min: number | null;
}

const DEFAULT_BASE = "http://localhost:8003";

export async function getBaseUrl(): Promise<string> {
  try {
    const r = await chrome.storage.sync.get("backendBaseUrl");
    return (r?.backendBaseUrl as string) || DEFAULT_BASE;
  } catch {
    return DEFAULT_BASE;
  }
}

export async function predict(items: PredictItem[]): Promise<Prediction[]> {
  const base = await getBaseUrl();
  const res = await fetch(`${base}/predict`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ items })
  });
  if (!res.ok) throw new Error(`predict failed: ${res.status}`);
  const body = await res.json();
  return body.predictions as Prediction[];
}

export async function recentDelays(trainNumber: string, serviceDate: string): Promise<RecentDelay[]> {
  const base = await getBaseUrl();
  const res = await fetch(`${base}/recent/${encodeURIComponent(trainNumber)}/${encodeURIComponent(serviceDate)}`);
  if (!res.ok) throw new Error(`recent failed: ${res.status}`);
  const body = await res.json();
  return (body.recent || []) as RecentDelay[];
}
