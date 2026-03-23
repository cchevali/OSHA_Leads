"use client";

import { useState } from "react";
import { PLANS, recommendPlan } from "@/config/territories";

const PRESETS = [
    { label: "Florida major metros (4)", metros: 4 },
    { label: "Southern California example (6)", metros: 6 },
];

export default function CoverageEstimator() {
    const [metros, setMetros] = useState(4);

    const rec = recommendPlan(metros);
    const plan = PLANS[rec];

    function applyPreset(count: number) {
        setMetros(count);
    }

    return (
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-xl text-ink">Coverage Estimator</h3>
            <p className="mt-2 text-sm text-inkMuted">
                About how many billed metros will your footprint map to? Counties, cities, metros, or OSHA
                areas work as inputs, and we confirm the mapping during onboarding.
            </p>

            {/* Presets */}
            <div className="mt-4 flex flex-wrap gap-2">
                {PRESETS.map((p) => (
                    <button
                        key={p.label}
                        type="button"
                        onClick={() => applyPreset(p.metros)}
                        className={`rounded-full border px-3 py-1.5 text-xs font-semibold transition ${metros === p.metros
                            ? "border-ocean bg-ocean/10 text-ocean"
                            : "border-cardBorder text-inkMuted hover:border-ink/40"
                            }`}
                    >
                        {p.label}
                    </button>
                ))}
            </div>

            {/* Dropdown */}
            <div className="mt-5 max-w-xs">
                <label className="grid gap-1.5 text-sm text-inkMuted">
                    About how many billed metros?
                    <select
                        value={metros}
                        onChange={(e) => setMetros(Number(e.target.value))}
                        className="rounded-xl border border-cardBorder bg-surface px-3 py-2.5 text-ink outline-none transition focus:border-ocean"
                    >
                        {Array.from({ length: 12 }, (_, i) => i + 1).map((n) => (
                            <option key={n} value={n}>
                                {n} {n === 1 ? "metro" : "metros"}
                            </option>
                        ))}
                        <option value={13}>13+ (Enterprise)</option>
                    </select>
                </label>
            </div>

            {/* Result */}
            {plan && (
                <div className="mt-6 rounded-2xl border border-ocean/20 bg-ocean/5 p-5">
                    <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-2">
                        <div>
                            <p className="text-sm font-semibold text-ink">
                                Recommended: {plan.name}
                            </p>
                            <p className="mt-1 text-sm text-inkMuted">
                                {rec === "enterprise"
                                    ? "Contact us for a custom quote — includes statewide and OSHA office alignment."
                                    : `Up to ${plan.metros} metros in billed coverage included.`}
                            </p>
                            <p className="mt-1 text-sm text-inkMuted">We&apos;ll confirm your coverage during onboarding.</p>
                        </div>
                        {plan.price !== null ? (
                            <p className="font-display text-3xl text-ink">
                                ${plan.price}
                                <span className="text-base text-inkMuted">/mo</span>
                            </p>
                        ) : (
                            <p className="font-display text-2xl text-ink">Custom</p>
                        )}
                    </div>
                </div>
            )}
            <p className="mt-4 text-xs text-inkMuted">
                Examples: Miami + Orlando + Tampa Bay + Jacksonville = 4 metros → Core. LA + Orange +
                Riverside/San Bernardino + Ventura + Santa Barbara + Kern typically maps to
                Multi-Territory.
            </p>
        </div>
    );
}
