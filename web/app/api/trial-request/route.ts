import { NextResponse } from "next/server";
import nodemailer from "nodemailer";
import site from "@/config/site.json";

export const runtime = "nodejs";

const DEFAULT_TRIAL_TO = "support@microflowops.com";
const EMAIL_SUBJECT_CONFIRMATION = "We received your MicroFlowOps request";
const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const RATE_LIMIT_WINDOW_MS = 15 * 60 * 1000;
const RATE_LIMIT_MAX = 5;
const rateLimitByIp = new Map<string, number[]>();
const EMAIL_SEND_ERROR = "EMAIL_SEND_ERROR";

type RequestIntent = "sample" | "founding_pilot" | "territory_reply";

type TrialRequestPayload = {
  company?: unknown;
  email?: unknown;
  metros?: unknown;
  notes?: unknown;
  recipients?: unknown;
  honeypot?: unknown;
  source?: unknown;
  intent?: unknown;
};

type Recipient = {
  email: string;
  name?: string;
};

function sanitizeText(value: unknown, maxLength: number): string {
  if (typeof value !== "string") return "";
  return value
    .trim()
    .replace(/[\r\n]+/g, " ")
    .replace(/\s+/g, " ")
    .slice(0, maxLength);
}

function sanitizeMultilineText(value: unknown, maxLength: number): string {
  if (typeof value !== "string") return "";
  return value.trim().replace(/\s+/g, " ").slice(0, maxLength);
}

function collapseWhitespace(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

function normalizeRecipients(value: unknown): Recipient[] | null {
  if (!Array.isArray(value)) return null;
  const recipients: Recipient[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (!item || typeof item !== "object") return null;
    const row = item as Record<string, unknown>;
    const email = sanitizeText(row.email, 254).toLowerCase();
    if (!EMAIL_REGEX.test(email)) return null;
    if (seen.has(email)) continue;
    seen.add(email);
    const name = collapseWhitespace(sanitizeText(row.name, 160));
    recipients.push(name ? { email, name } : { email });
  }
  if (recipients.length === 0 || recipients.length > 6) return null;
  return recipients;
}

function getClientIp(request: Request): string {
  const forwardedFor = request.headers.get("x-forwarded-for");
  if (forwardedFor) {
    return forwardedFor.split(",")[0]?.trim() || "unknown";
  }

  return request.headers.get("x-real-ip")?.trim() || "unknown";
}

function checkRateLimit(ip: string, nowMs: number): { allowed: boolean; retryAfterSeconds: number } {
  const existing = rateLimitByIp.get(ip) ?? [];
  const recent = existing.filter((timestamp) => nowMs - timestamp < RATE_LIMIT_WINDOW_MS);
  if (recent.length >= RATE_LIMIT_MAX) {
    const oldestWithinWindow = recent[0];
    const retryAfterMs = Math.max(1, RATE_LIMIT_WINDOW_MS - (nowMs - oldestWithinWindow));
    rateLimitByIp.set(ip, recent);
    return {
      allowed: false,
      retryAfterSeconds: Math.ceil(retryAfterMs / 1000)
    };
  }

  recent.push(nowMs);
  rateLimitByIp.set(ip, recent);
  return { allowed: true, retryAfterSeconds: 0 };
}

function hasSmtpConfig(): boolean {
  return Boolean(
    process.env.WEB_SMTP_HOST &&
      process.env.WEB_SMTP_PORT &&
      process.env.WEB_SMTP_USER &&
      process.env.WEB_SMTP_PASS &&
      process.env.WEB_SMTP_FROM
  );
}

function getSmtpPort(): number {
  return Number.parseInt(process.env.WEB_SMTP_PORT || "", 10);
}

function normalizeIntent(value: string): RequestIntent {
  const normalized = (value || "").trim().toLowerCase();
  if (normalized === "founding_pilot") return "founding_pilot";
  if (normalized === "territory_reply") return "territory_reply";
  return "sample";
}

export async function POST(request: Request) {
  const ip = getClientIp(request);
  const userAgent = request.headers.get("user-agent") || "unknown";

  try {
    let payload: TrialRequestPayload;
    try {
      payload = (await request.json()) as TrialRequestPayload;
    } catch (error) {
      if (error instanceof SyntaxError) {
        return NextResponse.json(
          {
            ok: false,
            err_code: "ERR_TRIAL_REQUEST_INVALID_INPUT"
          },
          { status: 400 }
        );
      }

      console.error("ERR_TRIAL_REQUEST_INTERNAL", error);
      return NextResponse.json(
        {
          ok: false,
          err_code: "ERR_TRIAL_REQUEST_INTERNAL"
        },
        { status: 500 }
      );
    }

    const company = sanitizeText(payload.company, 160);
    const email = sanitizeText(payload.email, 254).toLowerCase();
    const metros = sanitizeText(payload.metros, 300);
    const notes = sanitizeMultilineText(payload.notes, 2000);
    const recipients = normalizeRecipients(payload.recipients);
    const honeypot = sanitizeText(payload.honeypot, 120);
    const source = sanitizeText(payload.source, 80);
    const intent = sanitizeText(payload.intent, 80);
    const normalizedIntent = normalizeIntent(intent);
    const requestTypeLabel =
      normalizedIntent === "founding_pilot"
        ? "Founding Pilot"
        : normalizedIntent === "territory_reply"
          ? "Territory reply"
          : "Sample request";

    if (honeypot) {
      console.warn(`WARN_HONEYPOT_TRIGGERED ip=${ip} ua=${userAgent}`);
      return NextResponse.json({ ok: true });
    }

    const nowMs = Date.now();
    const rateLimit = checkRateLimit(ip, nowMs);
    if (!rateLimit.allowed) {
      console.warn(`WARN_RATE_LIMIT_IP ip=${ip} window_seconds=900 max=5`);
      return NextResponse.json(
        {
          ok: false,
          err_code: "ERR_RATE_LIMIT_IP",
          retry_after_seconds: rateLimit.retryAfterSeconds
        },
        { status: 429 }
      );
    }

    if (!company || !metros || !EMAIL_REGEX.test(email) || !recipients) {
      return NextResponse.json(
        {
          ok: false,
          err_code: "ERR_TRIAL_REQUEST_INVALID_INPUT"
        },
        { status: 400 }
      );
    }

    const smtpConfigured = hasSmtpConfig();
    const smtpPort = getSmtpPort();
    if (!smtpConfigured || Number.isNaN(smtpPort) || smtpPort <= 0) {
      if (process.env.NODE_ENV === "production") {
        return NextResponse.json(
          {
            ok: false,
            err_code: "ERR_WEB_SMTP_NOT_CONFIGURED"
          },
          { status: 500 }
        );
      }

      console.warn("WARN_WEB_SMTP_NOT_CONFIGURED");
      return NextResponse.json({ ok: true });
    }

    const transporter = nodemailer.createTransport({
      host: process.env.WEB_SMTP_HOST,
      port: smtpPort,
      secure: smtpPort === 465,
      auth: {
        user: process.env.WEB_SMTP_USER,
        pass: process.env.WEB_SMTP_PASS
      }
    });

    const submittedAt = new Date(nowMs).toISOString();
    const supportTo = process.env.WEB_TRIAL_TO || DEFAULT_TRIAL_TO;
    const supportSubject = `${requestTypeLabel}: ${company} (${email})`;
    const supportText = [
      "New MicroFlowOps request",
      "",
      `Request type: ${requestTypeLabel}`,
      `Company: ${company}`,
      `Email: ${email}`,
      `Coverage requested: ${metros}`,
      `Recipients (${recipients.length}):`,
      ...recipients.map((recipient, index) =>
        `  ${index + 1}. ${recipient.name ? `${recipient.name} <${recipient.email}>` : recipient.email}`
      ),
      `Notes: ${notes || "(none)"}`,
      "",
      `Source: ${source || "(unspecified)"}`,
      `Intent: ${normalizedIntent}`,
      `Manual qualification required: ${normalizedIntent === "founding_pilot" ? "yes" : "no"}`,
      `Submitted at (UTC): ${submittedAt}`,
      `IP: ${ip}`,
      `User-Agent: ${userAgent}`,
      "",
      normalizedIntent === "founding_pilot"
        ? "Offer context: Founding Pilot is $149 for 30 days in one state. Manual qualification required before activation."
        : "Offer context: Request a sample first, then qualify territory fit for a founding pilot or standard plan.",
      "",
      `Mailing address: ${site.mailingAddress}`
    ].join("\n");

    try {
      await transporter.sendMail({
        from: process.env.WEB_SMTP_FROM,
        to: supportTo,
        subject: supportSubject,
        text: supportText
      });

      const confirmationText = [
        `Hi ${company},`,
        "",
        "We received your MicroFlowOps request.",
        normalizedIntent === "founding_pilot"
          ? "We will review founding pilot fit manually and reply same business day."
          : normalizedIntent === "territory_reply"
            ? "We will review your territory and reply with the best next step."
            : "We will review your territory and send the best next step for a sample.",
        "",
        `We captured this coverage: ${metros}`,
        `Recipients: ${recipients.length}`,
        "Counties, cities, metros, or OSHA areas all work.",
        "Best for safety consulting and training firms already doing outbound or business development.",
        normalizedIntent === "founding_pilot"
          ? "Founding Pilot is $149 for 30 days in one state. Activation happens only after manual qualification."
          : "If the fit looks right, we can talk through a 30-day Founding Pilot or an ongoing standard plan.",
        "",
        "Request received. We'll respond same business day.",
        "If you don't hear back, email support@microflowops.com",
        "",
        "Thanks,",
        "MicroFlowOps"
      ].join("\n");

      await transporter.sendMail({
        from: process.env.WEB_SMTP_FROM,
        to: email,
        subject: EMAIL_SUBJECT_CONFIRMATION,
        text: confirmationText
      });
    } catch (error) {
      console.error("ERR_TRIAL_REQUEST_EMAIL_SEND_FAILED", error);
      throw new Error(EMAIL_SEND_ERROR);
    }

    return NextResponse.json({ ok: true });
  } catch (error) {
    if (error instanceof Error && error.message === EMAIL_SEND_ERROR) {
      return NextResponse.json(
        {
          ok: false,
          err_code: "ERR_TRIAL_REQUEST_EMAIL_SEND_FAILED"
        },
        { status: 502 }
      );
    }

    console.error("ERR_TRIAL_REQUEST_INTERNAL", error);
    return NextResponse.json(
      {
        ok: false,
        err_code: "ERR_TRIAL_REQUEST_INTERNAL"
      },
      { status: 500 }
    );
  }
}
