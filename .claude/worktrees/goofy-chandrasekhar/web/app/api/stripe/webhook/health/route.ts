import { NextResponse } from "next/server";

export const runtime = "nodejs";

export async function GET() {
  const stripePriceIdCorePresent = Boolean((process.env.STRIPE_PRICE_ID_CORE || "").trim());
  const stripePriceIdMultiPresent = Boolean((process.env.STRIPE_PRICE_ID_MULTI || "").trim());
  const webStripeWebhookSecretPresent = Boolean((process.env.WEB_STRIPE_WEBHOOK_SECRET || "").trim());
  const stripeModeHint =
    stripePriceIdCorePresent && stripePriceIdMultiPresent && webStripeWebhookSecretPresent
      ? "live-config-present"
      : "missing-config";

  return NextResponse.json({
    ok: true,
    stripe_price_id_core_present: stripePriceIdCorePresent,
    stripe_price_id_multi_present: stripePriceIdMultiPresent,
    web_stripe_webhook_secret_present: webStripeWebhookSecretPresent,
    stripe_mode_hint: stripeModeHint
  });
}
