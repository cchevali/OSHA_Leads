declare global {
  interface Window {
    plausible?: (eventName: string, options?: Record<string, unknown>) => void;
  }
}

const lastEventAt = new Map<string, number>();

function _hostMatches(allowedHostsCsv: string, currentHost: string): boolean {
  const allowed = allowedHostsCsv
    .split(",")
    .map((h) => h.trim())
    .filter(Boolean);
  return allowed.includes(currentHost);
}

export function isAnalyticsEnabled(): boolean {
  if (typeof window === "undefined") return false;

  const enabled = (process.env.NEXT_PUBLIC_PLAUSIBLE_ENABLED || "").toLowerCase() === "true";
  if (!enabled) return false;

  const allowedHosts = process.env.NEXT_PUBLIC_SITE_HOST || "";
  if (!allowedHosts) return false;

  return _hostMatches(allowedHosts, window.location.hostname);
}

export function getPlausibleDomain(): string {
  return (
    process.env.NEXT_PUBLIC_PLAUSIBLE_DOMAIN ||
    // Reasonable fallback: if not set, use the first host in NEXT_PUBLIC_SITE_HOST.
    (process.env.NEXT_PUBLIC_SITE_HOST || "").split(",")[0].trim()
  );
}

export function trackEvent(eventName: string, debounceMs: number = 800): void {
  if (typeof window === "undefined") return;
  if (!isAnalyticsEnabled()) return;

  const now = Date.now();
  const last = lastEventAt.get(eventName) ?? 0;
  if (now - last < debounceMs) return;
  lastEventAt.set(eventName, now);

  // Plausible is loaded via script tag in app/layout.tsx.
  window.plausible?.(eventName);
}
