import { NextResponse } from "next/server";
import nodemailer from "nodemailer";
import site from "@/config/site.json";

export const runtime = "nodejs";

const DEFAULT_TRIAL_TO = "support@microflowops.com";
const EMAIL_SUBJECT_CONFIRMATION = "We received your MicroFlowOps trial request";
const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const RATE_LIMIT_WINDOW_MS = 15 * 60 * 1000;
const RATE_LIMIT_MAX = 5;
const rateLimitByIp = new Map<string, number[]>();
const EMAIL_SEND_ERROR = "EMAIL_SEND_ERROR";

type TrialRequestPayload = {
  company?: unknown;
  email?: unknown;
  metros?: unknown;
  notes?: unknown;
  honeypot?: unknown;
  source?: unknown;
  intent?: unknown;
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
    const honeypot = sanitizeText(payload.honeypot, 120);
    const source = sanitizeText(payload.source, 80);
    const intent = sanitizeText(payload.intent, 80);

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

    if (!company || !metros || !EMAIL_REGEX.test(email)) {
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
    const supportSubject = `Trial feed request: ${company} (${email})`;
    const supportText = [
      "New trial feed request",
      "",
      `Company: ${company}`,
      `Email: ${email}`,
      `Metros: ${metros}`,
      `Notes: ${notes || "(none)"}`,
      "",
      `Source: ${source || "(unspecified)"}`,
      `Intent: ${intent || "(unspecified)"}`,
      `Submitted at (UTC): ${submittedAt}`,
      `IP: ${ip}`,
      `User-Agent: ${userAgent}`,
      "",
      "Offer context: 14-day trial, up to 4 metros, no credit card.",
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
        "We received your MicroFlowOps trial request.",
        "14-day trial, up to 4 metros, no credit card.",
        "",
        `We captured: ${metros}`,
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
