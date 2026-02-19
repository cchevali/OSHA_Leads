import { NextResponse } from "next/server";
import { runSubscriptionRegistryCommand } from "@/lib/subscriptionRegistry";

export const runtime = "nodejs";

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function sanitizeText(value: unknown, maxLength: number): string {
  if (typeof value !== "string") return "";
  return value.trim().slice(0, maxLength);
}

function normalizeCbsaCodes(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const normalized = value
    .map((item) => String(item || "").trim().replace(/\D/g, "").padStart(5, "0"))
    .filter((item) => item.length === 5);
  return Array.from(new Set(normalized));
}

export async function POST(request: Request) {
  let payload: {
    subscriber_key?: unknown;
    email?: unknown;
    plan_code?: unknown;
    cbsa_codes?: unknown;
  };
  try {
    payload = (await request.json()) as typeof payload;
  } catch {
    return NextResponse.json({ ok: false, err_code: "ERR_ONBOARDING_INVALID_INPUT" }, { status: 400 });
  }

  const subscriberKey = sanitizeText(payload.subscriber_key, 80);
  const email = sanitizeText(payload.email, 254).toLowerCase();
  const planCode = sanitizeText(payload.plan_code, 40).toLowerCase();
  const cbsaCodes = normalizeCbsaCodes(payload.cbsa_codes);

  if (!EMAIL_REGEX.test(email)) {
    return NextResponse.json({ ok: false, err_code: "ERR_ONBOARDING_EMAIL_REQUIRED" }, { status: 400 });
  }
  if (cbsaCodes.length === 0) {
    return NextResponse.json({ ok: false, err_code: "ERR_ONBOARDING_CBSA_REQUIRED" }, { status: 400 });
  }

  const result = runSubscriptionRegistryCommand("onboarding-submit", {
    subscriber_key: subscriberKey,
    email,
    plan_code: planCode,
    cbsa_codes: cbsaCodes,
    source: "web_onboarding_api"
  });

  const responsePayload = result.payload;
  if (!result.ok) {
    if (responsePayload.err_code === "ERR_MAX_METROS_EXCEEDED") {
      return NextResponse.json(responsePayload, { status: 409 });
    }
    return NextResponse.json(
      { ok: false, err_code: String(responsePayload.err_code || "ERR_ONBOARDING_INTERNAL") },
      { status: 500 }
    );
  }

  return NextResponse.json(responsePayload);
}
