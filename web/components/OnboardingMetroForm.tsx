"use client";

import { useMemo, useState, type FormEvent } from "react";
import Link from "next/link";
import MetroPicker from "@/components/MetroPicker";
import type { CbsaOption } from "@/lib/cbsa";

type OnboardingMetroFormProps = {
  options: CbsaOption[];
  initialPlanCode: string;
  initialEmail: string;
  initialSubscriberKey: string;
};

type RecipientRow = {
  name: string;
  email: string;
};

type Recipient = {
  email: string;
  name?: string;
};

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

const PLAN_CAPS: Record<string, number> = {
  pilot: 4,
  core: 4,
  multi: 10
};

const PLAN_RECIPIENT_CAPS: Record<string, number> = {
  pilot: 6,
  core: 6,
  multi: 15
};

const PLAN_LABELS: Record<string, string> = {
  pilot: "Pilot",
  core: "Core",
  multi: "Multi-Territory"
};

function normalizePlanCode(value: string): string {
  const normalized = value.trim().toLowerCase().replace("-", "_");
  if (normalized === "multi_territory") return "multi";
  if (normalized === "trial") return "pilot";
  if (normalized in PLAN_CAPS) return normalized;
  return "core";
}

function collapseWhitespace(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

function dedupeRecipients(rows: RecipientRow[], maxRecipients: number): { recipients: Recipient[]; error: string } {
  const recipients: Recipient[] = [];
  const seen = new Set<string>();
  for (const row of rows) {
    const email = row.email.trim().toLowerCase();
    if (!EMAIL_REGEX.test(email)) {
      return { recipients: [], error: "Enter a valid email for each recipient row." };
    }
    if (seen.has(email)) continue;
    seen.add(email);
    const name = collapseWhitespace(row.name);
    recipients.push(name ? { email, name } : { email });
  }
  if (recipients.length === 0) {
    return { recipients: [], error: "At least one recipient is required." };
  }
  if (recipients.length > maxRecipients) {
    return { recipients: [], error: `${maxRecipients} recipients maximum for this plan.` };
  }
  return { recipients, error: "" };
}

type SubmitState = {
  status: "idle" | "submitting" | "success" | "error";
  message: string;
  contactPath: string;
};

export default function OnboardingMetroForm({
  options,
  initialPlanCode,
  initialEmail,
  initialSubscriberKey
}: OnboardingMetroFormProps) {
  const [email, setEmail] = useState(initialEmail);
  const [subscriberKey, setSubscriberKey] = useState(initialSubscriberKey);
  const [planCode, setPlanCode] = useState(normalizePlanCode(initialPlanCode));
  const [query, setQuery] = useState("");
  const [selectedCodes, setSelectedCodes] = useState<string[]>([]);
  const [recipientRows, setRecipientRows] = useState<RecipientRow[]>([{ name: "", email: initialEmail }]);
  const [recipientError, setRecipientError] = useState("");
  const [submitState, setSubmitState] = useState<SubmitState>({
    status: "idle",
    message: "",
    contactPath: "/contact?source=onboarding&intent=expand"
  });

  const maxMetros = useMemo(() => PLAN_CAPS[planCode] ?? 4, [planCode]);
  const maxRecipients = useMemo(() => PLAN_RECIPIENT_CAPS[planCode] ?? 6, [planCode]);

  function updateRecipientRow(index: number, key: keyof RecipientRow, value: string): void {
    setRecipientRows((prev) => prev.map((row, i) => (i === index ? { ...row, [key]: value } : row)));
    setRecipientError("");
    setSubmitState((prev) => ({ ...prev, status: "idle", message: "" }));
  }

  function addRecipientRow(): void {
    setRecipientRows((prev) => {
      if (prev.length >= maxRecipients) return prev;
      return [...prev, { name: "", email: "" }];
    });
    setRecipientError("");
  }

  function removeRecipientRow(index: number): void {
    if (index === 0) return;
    setRecipientRows((prev) => prev.filter((_, i) => i !== index));
    setRecipientError("");
  }

  function toggleCode(code: string): void {
    setSubmitState((prev) => ({ ...prev, status: "idle", message: "" }));
    setSelectedCodes((prev) => {
      if (prev.includes(code)) {
        return prev.filter((item) => item !== code);
      }
      if (prev.length >= maxMetros) {
        setSubmitState({
          status: "error",
          message: `You selected ${prev.length + 1} metros, but ${PLAN_LABELS[planCode] || "this plan"} allows up to ${maxMetros}.`,
          contactPath: "/contact?source=onboarding&intent=expand"
        });
        return prev;
      }
      return [...prev, code];
    });
  }

  async function submit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (selectedCodes.length > maxMetros) {
      setSubmitState({
        status: "error",
        message: `Selected metros exceed your cap (${maxMetros}).`,
        contactPath: "/contact?source=onboarding&intent=expand"
      });
      return;
    }
    if (!email.trim()) {
      setSubmitState({
        status: "error",
        message: "Email is required.",
        contactPath: "/contact?source=onboarding&intent=expand"
      });
      return;
    }

    const normalizedRecipients = dedupeRecipients(recipientRows, maxRecipients);
    if (normalizedRecipients.error) {
      setRecipientError(normalizedRecipients.error);
      setSubmitState({
        status: "error",
        message: "Please fix recipient details and resubmit.",
        contactPath: "/contact?source=onboarding&intent=expand"
      });
      return;
    }
    setRecipientError("");

    setSubmitState({ status: "submitting", message: "Saving coverage...", contactPath: "" });
    const response = await fetch("/api/onboarding", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        subscriber_key: subscriberKey,
        email,
        plan_code: planCode,
        cbsa_codes: selectedCodes,
        recipients: normalizedRecipients.recipients
      })
    });
    const payload = (await response.json()) as {
      ok?: boolean;
      err_code?: string;
      max_metros?: number;
      selected_count?: number;
      contact_path?: string;
      max_recipients?: number;
      selected_recipients?: number;
    };
    if (!response.ok || !payload.ok) {
      const contactPath = payload.contact_path || "/contact?source=onboarding&intent=expand";
      if (payload.err_code === "ERR_MAX_METROS_EXCEEDED") {
        setSubmitState({
          status: "error",
          message: `This submission selected ${payload.selected_count || selectedCodes.length} metros and exceeds your plan cap (${payload.max_metros || maxMetros}).`,
          contactPath
        });
        return;
      }
      if (payload.err_code === "ERR_MAX_RECIPIENTS_EXCEEDED") {
        setSubmitState({
          status: "error",
          message: `This submission included ${payload.selected_recipients || 0} recipients and exceeds your plan cap (${payload.max_recipients || maxRecipients}).`,
          contactPath
        });
        return;
      }
      setSubmitState({
        status: "error",
        message: payload.err_code || "ERR_ONBOARDING_SUBMISSION_FAILED",
        contactPath
      });
      return;
    }

    setSubmitState({
      status: "success",
      message: "Coverage and recipients saved. Your onboarding details have been recorded.",
      contactPath: ""
    });
  }

  return (
    <form onSubmit={submit} className="mt-6 grid gap-4">
      <label className="grid gap-2 text-sm text-inkMuted">
        Subscriber key
        <input
          value={subscriberKey}
          onChange={(event) => setSubscriberKey(event.target.value)}
          placeholder="e.g. sub_abc123"
          className="rounded-xl border border-cardBorder bg-surface px-3 py-2 text-ink outline-none focus:border-ocean"
        />
      </label>
      <label className="grid gap-2 text-sm text-inkMuted">
        Company Email (billing/admin contact)
        <input
          required
          type="email"
          value={email}
          onChange={(event) => setEmail(event.target.value)}
          className="rounded-xl border border-cardBorder bg-surface px-3 py-2 text-ink outline-none focus:border-ocean"
        />
      </label>
      <label className="grid gap-2 text-sm text-inkMuted">
        Plan
        <select
          value={planCode}
          onChange={(event) => setPlanCode(normalizePlanCode(event.target.value))}
          className="rounded-xl border border-cardBorder bg-surface px-3 py-2 text-ink outline-none focus:border-ocean"
        >
          <option value="pilot">Pilot (max 4 metros, 6 recipients)</option>
          <option value="core">Core (max 4 metros, 6 recipients)</option>
          <option value="multi">Multi-Territory (max 10 metros, 15 recipients)</option>
        </select>
      </label>

      <div className="grid gap-3 rounded-xl border border-cardBorder bg-card p-4">
        <div className="flex items-center justify-between gap-3">
          <p className="text-sm font-semibold text-ink">Recipients (max {maxRecipients})</p>
          <button
            type="button"
            onClick={addRecipientRow}
            disabled={recipientRows.length >= maxRecipients}
            className="text-xs font-semibold text-ocean underline disabled:cursor-not-allowed disabled:opacity-50"
          >
            Add recipient
          </button>
        </div>
        <p className="text-xs text-inkMuted">Primary recipient is used first for display/order. Additional recipients receive individual emails.</p>

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
                <div key={`onboarding-recipient-${index}`} className="grid gap-2 md:grid-cols-[1fr_1fr_auto]">
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
        {recipientError ? <p className="text-xs font-semibold text-red-700">{recipientError}</p> : null}
      </div>

      <MetroPicker
        options={options}
        selectedCodes={selectedCodes}
        maxMetros={maxMetros}
        query={query}
        setQuery={setQuery}
        onToggle={toggleCode}
      />

      {submitState.message ? (
        <div
          className={`rounded-xl border px-3 py-2 text-sm ${
            submitState.status === "success"
              ? "border-green-200 bg-green-50 text-green-900"
              : submitState.status === "error"
                ? "border-amber-200 bg-amber-50 text-amber-900"
                : "border-cardBorder bg-card text-inkMuted"
          }`}
        >
          <p>{submitState.message}</p>
          {submitState.status === "error" && submitState.contactPath ? (
            <p className="mt-1">
              <Link href={submitState.contactPath} className="font-semibold text-ocean underline">
                Contact us for expanded coverage
              </Link>
            </p>
          ) : null}
        </div>
      ) : null}

      <div className="pt-2">
        <button
          type="submit"
          disabled={submitState.status === "submitting"}
          className="inline-flex items-center justify-center rounded-full bg-ocean px-5 py-2 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark disabled:cursor-not-allowed disabled:opacity-70"
        >
          {submitState.status === "submitting" ? "Saving..." : "Save onboarding details"}
        </button>
      </div>
    </form>
  );
}
