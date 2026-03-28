"use client";

import Link from "next/link";
import { trackEvent } from "@/lib/analytics";
import site from "@/config/site.json";

type CTAButtonsProps = {
  variant?: "light" | "dark";
};

export default function CTAButtons({ variant = "light" }: CTAButtonsProps) {
  const territoryMailto = `mailto:${site.ctaEmail}?${new URLSearchParams({
    subject: site.ctaTerritorySubject,
    body: site.ctaTerritoryBody
  }).toString()}`;
  const primaryClass =
    "inline-flex items-center justify-center rounded-full bg-ocean px-4 py-2 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark";
  const secondaryClass =
    variant === "dark"
      ? "inline-flex items-center justify-center rounded-full border border-white/30 px-4 py-2 text-sm font-semibold text-white transition hover:border-white/70"
      : "inline-flex items-center justify-center rounded-full border border-cardBorder px-4 py-2 text-sm font-semibold text-ink transition hover:border-ink/40";
  const tertiaryClass =
    variant === "dark"
      ? "text-sm font-semibold text-white/75 underline-offset-4 transition hover:text-white hover:underline"
      : "text-sm font-semibold text-inkMuted underline-offset-4 transition hover:text-ink hover:underline";

  return (
    <div className="flex flex-wrap items-center gap-3">
      <Link
        href="/contact?source=site_cta&intent=sample"
        onClick={() => trackEvent("cta_request_sample")}
        className={primaryClass}
      >
        Request a sample
      </Link>
      <Link
        href="/contact?source=site_cta&intent=founding_pilot"
        onClick={() => trackEvent("cta_start_founding_pilot")}
        className={secondaryClass}
      >
        Start a 30-day founding pilot
      </Link>
      <a
        href={territoryMailto}
        onClick={() => trackEvent("cta_reply_with_territory")}
        className={tertiaryClass}
      >
        Tell us your state or region
      </a>
    </div>
  );
}
