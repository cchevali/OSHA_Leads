const UTM_SOURCE = "microflowops";
const UTM_MEDIUM = "site";
const UTM_CAMPAIGN = "pricing_core";

export function buildStripeCheckoutUrl(baseUrl: string): string {
  const url = new URL(baseUrl);
  url.searchParams.set("utm_source", UTM_SOURCE);
  url.searchParams.set("utm_medium", UTM_MEDIUM);
  url.searchParams.set("utm_campaign", UTM_CAMPAIGN);
  return url.toString();
}

export type CheckoutCta = {
  href: string;
  isExternal: boolean;
};

function isPlaceholderCheckoutUrl(url: string | null | undefined): boolean {
  if (!url) return true;
  return url.includes("PLACEHOLDER_");
}

export function resolveCheckoutCta(
  stripeUrl: string | null | undefined,
  fallbackHref: string
): CheckoutCta {
  const candidate = stripeUrl ?? "";

  if (isPlaceholderCheckoutUrl(candidate)) {
    return { href: fallbackHref, isExternal: false };
  }

  try {
    return { href: buildStripeCheckoutUrl(candidate), isExternal: true };
  } catch {
    return { href: fallbackHref, isExternal: false };
  }
}
