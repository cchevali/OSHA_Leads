declare global {
  interface Window {
    plausible?: (eventName: string, options?: Record<string, unknown>) => void;
  }
}

const lastEventAt = new Map<string, number>();

export function trackEvent(eventName: string, debounceMs: number = 800): void {
  if (typeof window === "undefined") return;

  const now = Date.now();
  const last = lastEventAt.get(eventName) ?? 0;
  if (now - last < debounceMs) return;
  lastEventAt.set(eventName, now);

  // Plausible is loaded via script tag in app/layout.tsx.
  window.plausible?.(eventName);
}

