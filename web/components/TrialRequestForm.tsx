"use client";

import { useState } from "react";
import { trackEvent } from "@/lib/analytics";
import site from "@/config/site.json";

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

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

export default function TrialRequestForm({ source = "", intent = "" }: TrialRequestFormProps) {
  const [company, setCompany] = useState("");
  const [email, setEmail] = useState("");
  const [metros, setMetros] = useState("");
  const [notes, setNotes] = useState("");
  const [honeypot, setHoneypot] = useState("");
  const [emailError, setEmailError] = useState("");
  const [metrosError, setMetrosError] = useState("");
  const [submitError, setSubmitError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitted, setSubmitted] = useState(false);

  async function handleSubmit(event: React.FormEvent) {
    event.preventDefault();
    trackEvent("trial_form_submit");

    const trimmedEmail = email.trim();
    const trimmedMetros = metros.trim();

    let hasError = false;
    if (!EMAIL_REGEX.test(trimmedEmail)) {
      setEmailError("Enter a valid email address.");
      hasError = true;
    } else {
      setEmailError("");
    }

    if (!trimmedMetros) {
      setMetrosError("Metros to cover is required.");
      hasError = true;
    } else {
      setMetrosError("");
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
        <p className="mt-2 text-sm text-inkMuted">
          If you don’t hear back, email {site.ctaEmail}
        </p>
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
        Email
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
      <label className="grid gap-1.5 text-sm text-inkMuted">
        Metros to cover (cities or states work too)
        <input
          required
          type="text"
          value={metros}
          onChange={(event) => setMetros(event.target.value)}
          placeholder="e.g. Miami–Fort Lauderdale, Orlando, Tampa–St. Petersburg, Jacksonville"
          className="rounded-xl border border-cardBorder bg-surface px-3 py-2.5 text-ink placeholder:text-inkMuted/50 outline-none transition focus:border-ocean"
        />
        {metrosError ? <p className="text-xs font-semibold text-red-700">{metrosError}</p> : null}
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
