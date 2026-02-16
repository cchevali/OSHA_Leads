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
  return (
    <a
      href={buildMailto(site.ctaSampleSubject, site.ctaSampleBody)}
      onClick={() => trackEvent("cta_mailto_request_sample")}
      className="inline-flex items-center justify-center rounded-full bg-ocean px-4 py-2 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark"
    >
      Request a trial feed
    </a>
  );
}
