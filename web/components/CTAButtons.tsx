"use client";

import Link from "next/link";
import { trackEvent } from "@/lib/analytics";

type CTAButtonsProps = {
  variant?: "light" | "dark";
};

export default function CTAButtons({ variant = "light" }: CTAButtonsProps) {
  return (
    <Link
      href="/contact"
      onClick={() => trackEvent("cta_request_trial")}
      className="inline-flex items-center justify-center rounded-full bg-ocean px-4 py-2 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark"
    >
      Request a trial feed
    </Link>
  );
}
