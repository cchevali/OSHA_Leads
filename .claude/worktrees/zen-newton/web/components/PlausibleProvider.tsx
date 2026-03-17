"use client";

import { useEffect, useState } from "react";
import Script from "next/script";
import { getPlausibleDomain, isAnalyticsEnabled } from "@/lib/analytics";

export default function PlausibleProvider() {
  const [enabled, setEnabled] = useState(false);

  useEffect(() => {
    setEnabled(isAnalyticsEnabled());
  }, []);

  if (!enabled) return null;

  const domain = getPlausibleDomain();
  if (!domain) return null;

  return (
    <Script
      strategy="afterInteractive"
      defer
      data-domain={domain}
      src="https://plausible.io/js/script.manual.js"
    />
  );
}

