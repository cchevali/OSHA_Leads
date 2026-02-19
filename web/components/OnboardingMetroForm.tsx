"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import MetroPicker from "@/components/MetroPicker";
import type { CbsaOption } from "@/lib/cbsa";

type OnboardingMetroFormProps = {
  options: CbsaOption[];
  initialPlanCode: string;
  initialEmail: string;
  initialSubscriberKey: string;
};

const PLAN_CAPS: Record<string, number> = {
  pilot: 4,
  core: 4,
  multi: 10
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
  const [submitState, setSubmitState] = useState<SubmitState>({
    status: "idle",
    message: "",
    contactPath: "/contact?source=onboarding&intent=expand"
  });

  const maxMetros = useMemo(() => PLAN_CAPS[planCode] ?? 4, [planCode]);

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

  async function submit(event: React.FormEvent<HTMLFormElement>): Promise<void> {
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

    setSubmitState({ status: "submitting", message: "Saving coverage...", contactPath: "" });
    const response = await fetch("/api/onboarding", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        subscriber_key: subscriberKey,
        email,
        plan_code: planCode,
        cbsa_codes: selectedCodes
      })
    });
    const payload = (await response.json()) as {
      ok?: boolean;
      err_code?: string;
      max_metros?: number;
      selected_count?: number;
      contact_path?: string;
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
      setSubmitState({
        status: "error",
        message: payload.err_code || "ERR_ONBOARDING_SUBMISSION_FAILED",
        contactPath
      });
      return;
    }

    setSubmitState({
      status: "success",
      message: "Coverage saved. Your CBSA allowlist has been recorded.",
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
        Email
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
          <option value="pilot">Pilot (max 4 metros)</option>
          <option value="core">Core (max 4 metros)</option>
          <option value="multi">Multi-Territory (max 10 metros)</option>
        </select>
      </label>

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
