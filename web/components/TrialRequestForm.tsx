"use client";

import { useState, useCallback } from "react";
import { trackEvent } from "@/lib/analytics";
import site from "@/config/site.json";

export default function TrialRequestForm() {
    const [company, setCompany] = useState("");
    const [email, setEmail] = useState("");
    const [territory, setTerritory] = useState("");
    const [submitted, setSubmitted] = useState(false);

    const handleSubmit = useCallback(
        (e: React.FormEvent) => {
            e.preventDefault();
            trackEvent("trial_form_submit");

            const body = [
                `Hi MicroFlowOps,`,
                ``,
                `I would like to request a trial feed.`,
                ``,
                `Organization: ${company}`,
                `Email: ${email}`,
                `Territory: ${territory}`,
                ``,
                `Thanks`
            ].join("\n");

            const mailtoUrl = `mailto:${site.ctaEmail}?${new URLSearchParams({
                subject: "Requesting a trial feed (OSHA Activity Signals)",
                body
            }).toString()}`;

            // Try opening the email client
            window.location.href = mailtoUrl;

            // Show success state regardless — the email client may open in background
            setSubmitted(true);
        },
        [company, email, territory]
    );

    if (submitted) {
        return (
            <div className="rounded-2xl border border-ocean/30 bg-ocean/10 p-6 text-center">
                <p className="font-display text-xl text-ink">Request sent!</p>
                <p className="mt-2 text-sm text-inkMuted">
                    Your email client should have opened with the details pre-filled. If it didn&apos;t,
                    email us directly at{" "}
                    <a href={`mailto:${site.ctaEmail}`} className="font-semibold text-ocean underline">
                        {site.ctaEmail}
                    </a>{" "}
                    with your company, email, and territory.
                </p>
                <button
                    type="button"
                    onClick={() => setSubmitted(false)}
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
                    onChange={(e) => setCompany(e.target.value)}
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
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder="you@company.com"
                    className="rounded-xl border border-cardBorder bg-surface px-3 py-2.5 text-ink placeholder:text-inkMuted/50 outline-none transition focus:border-ocean"
                />
            </label>
            <label className="grid gap-1.5 text-sm text-inkMuted">
                Territory
                <input
                    required
                    type="text"
                    value={territory}
                    onChange={(e) => setTerritory(e.target.value)}
                    placeholder="Ohio, Pennsylvania, or OSHA Area Office"
                    className="rounded-xl border border-cardBorder bg-surface px-3 py-2.5 text-ink placeholder:text-inkMuted/50 outline-none transition focus:border-ocean"
                />
            </label>
            <div className="pt-1">
                <button
                    type="submit"
                    className="inline-flex w-full items-center justify-center rounded-full bg-ocean px-5 py-2.5 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark"
                >
                    Request trial feed
                </button>
            </div>
        </form>
    );
}
