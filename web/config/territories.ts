/**
 * Plan constants for the metro-based pricing model.
 *
 * Coverage is based on metro areas (MSA-style). Self-serve plans include
 * a set number of metros. OSHA area office alignment is available on
 * Enterprise or confirmed during onboarding.
 */

export const PLANS = {
    pilot: {
        name: "Pilot",
        price: 0,
        metros: 4,
        recipients: 6,
        label: "$0",
        note: "14 days · up to 4 metros",
    },
    core: {
        name: "Core",
        price: 299,
        metros: 4,
        recipients: 6,
        label: "$299",
        note: "per month · up to 4 metros",
    },
    multi: {
        name: "Multi-Territory",
        price: 499,
        metros: 10,
        recipients: 15,
        label: "$499",
        note: "per month · up to 10 metros",
    },
    enterprise: {
        name: "Enterprise",
        price: null,
        metros: null,
        recipients: null,
        label: "Custom",
        note: "10+ metros · statewide · OSHA office alignment",
    },
} as const;

// ---------------------------------------------------------------------------
// Coverage Estimator helper
// ---------------------------------------------------------------------------

export type RecommendedPlan = "core" | "multi" | "enterprise";

export function recommendPlan(metros: number): RecommendedPlan {
    if (metros <= 4) return "core";
    if (metros <= 10) return "multi";
    return "enterprise";
}
