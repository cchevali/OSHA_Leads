"use client";

import site from "@/config/site.json";
import { trackEvent } from "@/lib/analytics";

interface CTAButtonsProps {
  variant?: "light" | "dark";
}

const buildMailto = (subject: string, body: string) => {
  const params = new URLSearchParams({
    subject,
    body
  });
  return `mailto:${site.ctaEmail}?${params.toString()}`;
};

export default function CTAButtons({ variant = "light" }: CTAButtonsProps) {
  const secondaryClasses =
    variant === "dark"
      ? "border-white/30 text-white hover:border-white/60"
      : "border-ink/15 text-ink hover:border-ink/40";

  return (
    <div className="flex flex-wrap items-center gap-3">
      <a
        href={buildMailto(site.ctaSampleSubject, site.ctaSampleBody)}
        onClick={() => trackEvent("cta_mailto_request_sample")}
        className="inline-flex items-center justify-center rounded-full bg-ocean px-4 py-2 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark"
      >
        Request a sample
      </a>
      <a
        href={buildMailto(site.ctaTerritorySubject, site.ctaTerritoryBody)}
        onClick={() => trackEvent("cta_mailto_territory_firm")}
        className={`inline-flex items-center justify-center rounded-full border px-4 py-2 text-sm font-semibold transition ${secondaryClasses}`}
      >
        Reply with your territory + firm name
      </a>
    </div>
  );
}