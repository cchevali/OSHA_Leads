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
