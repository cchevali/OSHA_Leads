import { NextResponse } from "next/server";
import { runSubscriptionRegistryCommand } from "@/lib/subscriptionRegistry";

export const runtime = "nodejs";

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const PLAN_MAX_RECIPIENTS: Record<string, number> = {
  pilot: 6,
  core: 6,
  multi: 15
};

type Recipient = {
  email: string;
  name?: string;
};

function sanitizeText(value: unknown, maxLength: number): string {
  if (typeof value !== "string") return "";
  return value.trim().slice(0, maxLength);
}

function collapseWhitespace(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

function normalizeCbsaCodes(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const normalized = value
    .map((item) => String(item || "").trim().replace(/\D/g, "").padStart(5, "0"))
    .filter((item) => item.length === 5);
  return Array.from(new Set(normalized));
}

function normalizePlanCode(value: string): string {
  const normalized = value.trim().toLowerCase().replace("-", "_");
  if (normalized === "multi_territory") return "multi";
  if (normalized === "trial") return "pilot";
  if (normalized in PLAN_MAX_RECIPIENTS) return normalized;
  return "core";
}

function normalizeRecipients(value: unknown): { recipients: Recipient[]; err_code?: string } {
  if (!Array.isArray(value)) {
    return { recipients: [], err_code: "ERR_ONBOARDING_RECIPIENT_REQUIRED" };
  }
  const recipients: Recipient[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (!item || typeof item !== "object") {
      return { recipients: [], err_code: "ERR_ONBOARDING_RECIPIENT_INVALID" };
    }
    const row = item as Record<string, unknown>;
    const email = sanitizeText(row.email, 254).toLowerCase();
    if (!EMAIL_REGEX.test(email)) {
      return { recipients: [], err_code: "ERR_ONBOARDING_RECIPIENT_INVALID" };
    }
    if (seen.has(email)) continue;
    seen.add(email);
    const name = collapseWhitespace(sanitizeText(row.name, 160));
    recipients.push(name ? { email, name } : { email });
  }
  if (recipients.length === 0) {
    return { recipients: [], err_code: "ERR_ONBOARDING_RECIPIENT_REQUIRED" };
  }
  return { recipients };
}

export async function POST(request: Request) {
  let payload: {
    subscriber_key?: unknown;
    email?: unknown;
    plan_code?: unknown;
    cbsa_codes?: unknown;
    recipients?: unknown;
  };
  try {
    payload = (await request.json()) as typeof payload;
  } catch {
    return NextResponse.json({ ok: false, err_code: "ERR_ONBOARDING_INVALID_INPUT" }, { status: 400 });
  }

  const subscriberKey = sanitizeText(payload.subscriber_key, 80);
  const email = sanitizeText(payload.email, 254).toLowerCase();
  const planCode = normalizePlanCode(sanitizeText(payload.plan_code, 40));
  const cbsaCodes = normalizeCbsaCodes(payload.cbsa_codes);
  const normalizedRecipients = normalizeRecipients(payload.recipients);

  if (!EMAIL_REGEX.test(email)) {
    return NextResponse.json({ ok: false, err_code: "ERR_ONBOARDING_EMAIL_REQUIRED" }, { status: 400 });
  }
  if (cbsaCodes.length === 0) {
    return NextResponse.json({ ok: false, err_code: "ERR_ONBOARDING_CBSA_REQUIRED" }, { status: 400 });
  }
  if (normalizedRecipients.err_code) {
    return NextResponse.json({ ok: false, err_code: normalizedRecipients.err_code }, { status: 400 });
  }
  const maxRecipients = PLAN_MAX_RECIPIENTS[planCode] ?? PLAN_MAX_RECIPIENTS.core;
  if (normalizedRecipients.recipients.length > maxRecipients) {
    return NextResponse.json(
      {
        ok: false,
        err_code: "ERR_MAX_RECIPIENTS_EXCEEDED",
        selected_recipients: normalizedRecipients.recipients.length,
        max_recipients: maxRecipients,
        plan_code: planCode
      },
      { status: 409 }
    );
  }

  const result = runSubscriptionRegistryCommand("onboarding-submit", {
    subscriber_key: subscriberKey,
    email,
    plan_code: planCode,
    cbsa_codes: cbsaCodes,
    recipients: normalizedRecipients.recipients,
    source: "web_onboarding_api"
  });

  const responsePayload = result.payload;
  if (!result.ok) {
    if (responsePayload.err_code === "ERR_MAX_METROS_EXCEEDED") {
      return NextResponse.json(responsePayload, { status: 409 });
    }
    if (responsePayload.err_code === "ERR_MAX_RECIPIENTS_EXCEEDED") {
      return NextResponse.json(responsePayload, { status: 409 });
    }
    if (
      responsePayload.err_code === "ERR_ONBOARDING_RECIPIENT_REQUIRED" ||
      responsePayload.err_code === "ERR_ONBOARDING_RECIPIENT_INVALID"
    ) {
      return NextResponse.json(responsePayload, { status: 400 });
    }
    return NextResponse.json(
      { ok: false, err_code: String(responsePayload.err_code || "ERR_ONBOARDING_INTERNAL") },
      { status: 500 }
    );
  }

  return NextResponse.json(responsePayload);
}
