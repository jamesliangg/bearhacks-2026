export type Severity =
  | "on_time"
  | "minor"
  | "moderate"
  | "significant"
  | "severe";

export const SEVERITY_COLORS: Record<Severity, { bg: string; fg: string; label: string }> = {
  on_time:     { bg: "#1a7f37", fg: "#ffffff", label: "On time" },
  minor:       { bg: "#bf8700", fg: "#ffffff", label: "Minor delay" },
  moderate:    { bg: "#d1651c", fg: "#ffffff", label: "Moderate delay" },
  significant: { bg: "#cf222e", fg: "#ffffff", label: "Significant delay" },
  severe:      { bg: "#82071e", fg: "#ffffff", label: "Severe delay" }
};

export function severityFor(minutes: number): Severity {
  if (minutes < 5) return "on_time";
  if (minutes < 15) return "minor";
  if (minutes < 30) return "moderate";
  if (minutes < 60) return "significant";
  return "severe";
}
