"use client";

import { useState } from "react";
import { trackEvent } from "@/lib/analytics";
import site from "@/config/site.json";

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const MAX_TRIAL_RECIPIENTS = 6;

type RecipientRow = {
  name: string;
  email: string;
};

type TrialRecipient = {
  email: string;
  name?: string;
};

type TrialRequestResponse =
  | { ok: true }
  | {
      ok: false;
      err_code?: string;
      retry_after_seconds?: number;
    };

type TrialRequestFormProps = {
  source?: string;
  intent?: string;
};

function collapseWhitespace(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

function normalizeRecipients(rows: RecipientRow[]): { recipients: TrialRecipient[]; error: string } {
  const recipients: TrialRecipient[] = [];
  const seen = new Set<string>();
  for (const row of rows) {
    const email = row.email.trim().toLowerCase();
    const name = collapseWhitespace(row.name);
    if (!EMAIL_REGEX.test(email)) {
      return { recipients: [], error: "Enter a valid email for each recipient row." };
    }
    if (seen.has(email)) {
      continue;
    }
    seen.add(email);
    recipients.push(name ? { email, name } : { email });
  }
  if (recipients.length === 0) {
    return { recipients: [], error: "At least one recipient is required." };
  }
  if (recipients.length > MAX_TRIAL_RECIPIENTS) {
    return { recipients: [], error: `Trial supports up to ${MAX_TRIAL_RECIPIENTS} recipients.` };
  }
  return { recipients, error: "" };
}

export default function TrialRequestForm({ source = "", intent = "" }: TrialRequestFormProps) {
  const [company, setCompany] = useState("");
  const [email, setEmail] = useState("");
  const [metros, setMetros] = useState("");
  const [notes, setNotes] = useState("");
  const [recipientRows, setRecipientRows] = useState<RecipientRow[]>([{ name: "", email: "" }]);
  const [honeypot, setHoneypot] = useState("");
  const [emailError, setEmailError] = useState("");
  const [metrosError, setMetrosError] = useState("");
  const [recipientsError, setRecipientsError] = useState("");
  const [submitError, setSubmitError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitted, setSubmitted] = useState(false);

  function updateRecipientRow(index: number, key: keyof RecipientRow, value: string): void {
    setRecipientRows((prev) => prev.map((row, i) => (i === index ? { ...row, [key]: value } : row)));
    setRecipientsError("");
  }

  function addRecipientRow(): void {
    setRecipientRows((prev) => {
      if (prev.length >= MAX_TRIAL_RECIPIENTS) return prev;
      return [...prev, { name: "", email: "" }];
    });
    setRecipientsError("");
  }

  function removeRecipientRow(index: number): void {
    if (index === 0) return;
    setRecipientRows((prev) => prev.filter((_, i) => i !== index));
    setRecipientsError("");
  }

  async function handleSubmit(event: React.FormEvent) {
    event.preventDefault();
    trackEvent("trial_form_submit");

    const trimmedEmail = email.trim();
    const trimmedMetros = metros.trim();
    const normalizedRecipients = normalizeRecipients(recipientRows);

    let hasError = false;
    if (!EMAIL_REGEX.test(trimmedEmail)) {
      setEmailError("Enter a valid email address.");
      hasError = true;
    } else {
      setEmailError("");
    }

    if (!trimmedMetros) {
      setMetrosError("Coverage to monitor is required.");
      hasError = true;
    } else {
      setMetrosError("");
    }

    if (normalizedRecipients.error) {
      setRecipientsError(normalizedRecipients.error);
      hasError = true;
    } else {
      setRecipientsError("");
    }

    if (hasError) {
      return;
    }

    setSubmitError("");
    setIsSubmitting(true);

    try {
      const queryParams = new URLSearchParams(window.location.search);
      const sourceValue = queryParams.get("source") || source;
      const intentValue = queryParams.get("intent") || intent;

      const response = await fetch("/api/trial-request", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          company,
          email: trimmedEmail,
          metros: trimmedMetros,
          notes,
          recipients: normalizedRecipients.recipients,
          honeypot,
          source: sourceValue,
          intent: intentValue
        })
      });

      const result = (await response.json()) as TrialRequestResponse;
      if (response.ok && result.ok) {
        setSubmitted(true);
        return;
      }

      const errCode = !result.ok ? result.err_code : "";
      if (errCode === "ERR_RATE_LIMIT_IP") {
        setSubmitError("Too many attempts right now. Please try again in a few minutes.");
      } else if (errCode === "ERR_TRIAL_REQUEST_INVALID_INPUT") {
        setSubmitError("Please check your inputs and try again.");
      } else {
        setSubmitError("We could not submit your request right now. Please try again.");
      }
    } catch {
      setSubmitError("We could not submit your request right now. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  }

  if (submitted) {
    return (
      <div className="rounded-2xl border border-ocean/30 bg-ocean/10 p-6 text-center">
        <p className="font-display text-xl text-ink">Request received. We’ll respond same business day.</p>
        <p className="mt-2 text-sm text-inkMuted">If you don’t hear back, email {site.ctaEmail}</p>
        <button
          type="button"
          onClick={() => {
            setSubmitted(false);
            setSubmitError("");
          }}
          className="mt-4 text-xs font-semibold text-inkMuted underline transition hover:text-ink"
        >
          Submit another request
        </button>
      </div>
    );
  }

  return (
    <form onSubmit={handleSubmit} className="grid gap-4">
      <label className="grid gap-1.5 text-sm text-inkMuted">
        Company
        <input
          required
          type="text"
          value={company}
          onChange={(event) => setCompany(event.target.value)}
          placeholder="Acme Safety Consulting"
          className="rounded-xl border border-cardBorder bg-surface px-3 py-2.5 text-ink placeholder:text-inkMuted/50 outline-none transition focus:border-ocean"
        />
      </label>
      <label className="grid gap-1.5 text-sm text-inkMuted">
        Company Email (billing/admin contact)
        <input
          required
          type="email"
          value={email}
          onChange={(event) => setEmail(event.target.value)}
          placeholder="you@company.com"
          className="rounded-xl border border-cardBorder bg-surface px-3 py-2.5 text-ink placeholder:text-inkMuted/50 outline-none transition focus:border-ocean"
        />
        {emailError ? <p className="text-xs font-semibold text-red-700">{emailError}</p> : null}
      </label>

      <div className="grid gap-3 rounded-xl border border-cardBorder bg-card p-4">
        <div className="flex items-center justify-between gap-3">
          <p className="text-sm font-semibold text-ink">Recipients (up to 6)</p>
          <button
            type="button"
            onClick={addRecipientRow}
            disabled={recipientRows.length >= MAX_TRIAL_RECIPIENTS}
            className="text-xs font-semibold text-ocean underline disabled:cursor-not-allowed disabled:opacity-50"
          >
            Add recipient
          </button>
        </div>

        <div className="grid gap-2">
          <p className="text-xs font-semibold uppercase tracking-wide text-inkMuted">Primary recipient</p>
          <div className="grid gap-2 md:grid-cols-[1fr_1fr]">
            <input
              type="text"
              value={recipientRows[0]?.name || ""}
              onChange={(event) => updateRecipientRow(0, "name", event.target.value)}
              placeholder="Name (optional)"
              className="rounded-xl border border-cardBorder bg-surface px-3 py-2 text-ink outline-none focus:border-ocean"
            />
            <input
              required
              type="email"
              value={recipientRows[0]?.email || ""}
              onChange={(event) => updateRecipientRow(0, "email", event.target.value)}
              placeholder="recipient@company.com"
              className="rounded-xl border border-cardBorder bg-surface px-3 py-2 text-ink outline-none focus:border-ocean"
            />
          </div>
        </div>

        {recipientRows.length > 1 ? (
          <div className="grid gap-2">
            <p className="text-xs font-semibold uppercase tracking-wide text-inkMuted">Additional recipients</p>
            {recipientRows.slice(1).map((row, offset) => {
              const index = offset + 1;
              return (
                <div key={`recipient-${index}`} className="grid gap-2 md:grid-cols-[1fr_1fr_auto]">
                  <input
                    type="text"
                    value={row.name}
                    onChange={(event) => updateRecipientRow(index, "name", event.target.value)}
                    placeholder="Name (optional)"
                    className="rounded-xl border border-cardBorder bg-surface px-3 py-2 text-ink outline-none focus:border-ocean"
                  />
                  <input
                    required
                    type="email"
                    value={row.email}
                    onChange={(event) => updateRecipientRow(index, "email", event.target.value)}
                    placeholder="recipient@company.com"
                    className="rounded-xl border border-cardBorder bg-surface px-3 py-2 text-ink outline-none focus:border-ocean"
                  />
                  <button
                    type="button"
                    onClick={() => removeRecipientRow(index)}
                    className="rounded-xl border border-cardBorder px-3 py-2 text-xs font-semibold text-ink transition hover:border-ink/40"
                  >
                    Remove
                  </button>
                </div>
              );
            })}
          </div>
        ) : null}

        {recipientsError ? <p className="text-xs font-semibold text-red-700">{recipientsError}</p> : null}
      </div>

      <label className="grid gap-1.5 text-sm text-inkMuted">
        Coverage to monitor (counties, cities, metros, or OSHA areas)
        <input
          required
          type="text"
          value={metros}
          onChange={(event) => setMetros(event.target.value)}
          placeholder="e.g. Orange County, Ventura County, Los Angeles, Riverside/San Bernardino"
          className="rounded-xl border border-cardBorder bg-surface px-3 py-2.5 text-ink placeholder:text-inkMuted/50 outline-none transition focus:border-ocean"
        />
        {metrosError ? <p className="text-xs font-semibold text-red-700">{metrosError}</p> : null}
        <p className="text-xs text-inkMuted">
          Counties, cities, metros, or OSHA areas work — we translate coverage for you.
        </p>
      </label>
      <label className="grid gap-1.5 text-sm text-inkMuted">
        Notes (optional)
        <textarea
          value={notes}
          onChange={(event) => setNotes(event.target.value)}
          rows={4}
          placeholder="Anything else we should know?"
          className="rounded-xl border border-cardBorder bg-surface px-3 py-2.5 text-ink placeholder:text-inkMuted/50 outline-none transition focus:border-ocean"
        />
      </label>
      <div className="absolute -left-[9999px] top-auto h-0 w-0 overflow-hidden opacity-0" aria-hidden="true">
        <label htmlFor="fax_number_check">Leave this field blank</label>
        <input
          id="fax_number_check"
          type="text"
          name="fax_number_check"
          value={honeypot}
          onChange={(event) => setHoneypot(event.target.value)}
          autoComplete="off"
          tabIndex={-1}
          aria-hidden="true"
        />
      </div>
      {submitError ? <p className="text-xs font-semibold text-red-700">{submitError}</p> : null}
      <div className="pt-1">
        <button
          type="submit"
          disabled={isSubmitting}
          className="inline-flex w-full items-center justify-center rounded-full bg-ocean px-5 py-2.5 text-sm font-semibold text-white shadow-glow transition enabled:hover:bg-oceanDark disabled:cursor-not-allowed disabled:opacity-60"
        >
          {isSubmitting ? "Submitting..." : "Request trial feed"}
        </button>
      </div>
    </form>
  );
}
