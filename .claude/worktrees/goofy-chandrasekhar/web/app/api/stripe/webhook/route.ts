import crypto from "node:crypto";
import { NextResponse } from "next/server";
import { runSubscriptionRegistryCommand } from "@/lib/subscriptionRegistry";

export const runtime = "nodejs";

const DEFAULT_TOLERANCE_SECONDS = 300;

function parseStripeSignatureHeader(value: string): { timestamp: string; signatures: string[] } | null {
  const parts = value.split(",").map((item) => item.trim());
  let timestamp = "";
  const signatures: string[] = [];
  for (const part of parts) {
    const [key, val] = part.split("=", 2);
    if (!key || !val) continue;
    if (key === "t") timestamp = val;
    if (key === "v1") signatures.push(val);
  }
  if (!timestamp || signatures.length === 0) {
    return null;
  }
  return { timestamp, signatures };
}

function verifyStripeSignature(payload: string, headerValue: string, secret: string): boolean {
  const parsed = parseStripeSignatureHeader(headerValue);
  if (!parsed) return false;
  const timestampNum = Number.parseInt(parsed.timestamp, 10);
  if (!Number.isFinite(timestampNum)) return false;
  const ageSeconds = Math.floor(Date.now() / 1000) - timestampNum;
  if (Math.abs(ageSeconds) > DEFAULT_TOLERANCE_SECONDS) return false;

  const signedPayload = `${parsed.timestamp}.${payload}`;
  const expected = crypto.createHmac("sha256", secret).update(signedPayload, "utf8").digest("hex");
  const expectedBuffer = Buffer.from(expected, "utf8");

  for (const candidate of parsed.signatures) {
    const candidateBuffer = Buffer.from(candidate, "utf8");
    if (candidateBuffer.length !== expectedBuffer.length) {
      continue;
    }
    if (crypto.timingSafeEqual(candidateBuffer, expectedBuffer)) {
      return true;
    }
  }
  return false;
}

export async function POST(request: Request) {
  const secret = (process.env.WEB_STRIPE_WEBHOOK_SECRET || "").trim();
  if (!secret) {
    return NextResponse.json({ ok: false, err_code: "ERR_STRIPE_WEBHOOK_SECRET_MISSING" }, { status: 500 });
  }

  const signatureHeader = request.headers.get("stripe-signature") || "";
  const rawBody = await request.text();
  if (!verifyStripeSignature(rawBody, signatureHeader, secret)) {
    return NextResponse.json({ ok: false, err_code: "ERR_STRIPE_WEBHOOK_SIGNATURE_INVALID" }, { status: 400 });
  }

  let eventPayload: Record<string, unknown>;
  try {
    eventPayload = JSON.parse(rawBody) as Record<string, unknown>;
  } catch {
    return NextResponse.json({ ok: false, err_code: "ERR_STRIPE_WEBHOOK_INVALID_JSON" }, { status: 400 });
  }

  const result = runSubscriptionRegistryCommand("stripe-ingest", eventPayload);
  const responsePayload = result.payload;
  const eventId = String(responsePayload.event_id || "");
  const token = String(responsePayload.token || "");
  console.log(`STRIPE_WEBHOOK token=${token} event_id=${eventId} exit_code=${result.exitCode}`);

  if (responsePayload.ok === false && token.startsWith("ERR_")) {
    return NextResponse.json(responsePayload, { status: 422 });
  }
  return NextResponse.json(responsePayload);
}
