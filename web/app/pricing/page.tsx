import type { Metadata } from "next";
import Link from "next/link";
import SectionHeading from "@/components/SectionHeading";
import CTAButtons from "@/components/CTAButtons";
import CoverageEstimator from "@/components/CoverageEstimator";
import site from "@/config/site.json";
import { resolveCheckoutCta } from "@/lib/checkout";

export const metadata: Metadata = {
  alternates: { canonical: "/pricing" }
};

export default function PricingPage() {
  const coreCheckout = resolveCheckoutCta(site.stripePaymentLinkCore, "/contact");
  const multiCheckout = resolveCheckoutCta(site.stripePaymentLinkMulti, "/contact");
  const trialMailto = `mailto:${site.ctaEmail}?${new URLSearchParams({
    subject: site.ctaSampleSubject,
    body: site.ctaSampleBody
  }).toString()}`;
  const contactMailto = `mailto:${site.ctaEmail}?${new URLSearchParams({
    subject: "Enterprise inquiry",
    body: "Hi MicroFlowOps,\n\nI am interested in enterprise or multi-state coverage.\n\nOrganization:\nMetros or states needed:\n\nThanks"
  }).toString()}`;

  const plans = [
    {
      name: "Pilot",
      price: "$0",
      note: "14 days",
      highlight: false,
      features: [
        "Up to 4 metros",
        "Daily email brief",
        "Priority scoring",
        "Up to 6 recipients"
      ],
      ctaLabel: "Start free pilot",
      ctaHref: trialMailto,
      ctaExternal: false,
      ctaStyle: "outline" as const
    },
    {
      name: "Core",
      price: "$299",
      note: "per month",
      highlight: true,
      features: [
        "Up to 4 metros",
        "Daily email delivery",
        "Coverage filters tuned to your metros",
        "Up to 6 recipients",
        "Weekly summary included"
      ],
      ctaLabel: "Subscribe — $299/mo",
      ctaHref: coreCheckout.href,
      ctaExternal: coreCheckout.isExternal,
      ctaStyle: "primary" as const
    },
    {
      name: "Multi-Territory",
      price: "$499",
      note: "per month",
      highlight: false,
      features: [
        "Up to 10 metros",
        "Everything in Core",
        "Up to 15 recipients",
        "Priority support"
      ],
      ctaLabel: "Subscribe — $499/mo",
      ctaHref: multiCheckout.href,
      ctaExternal: multiCheckout.isExternal,
      ctaStyle: "outline" as const
    }
  ];

  return (
    <div className="space-y-16 pb-24 pt-12">
      {/* Hero */}
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Pricing"
          title="Pick a plan. Tell us your metros. We handle the rest."
          description="Coverage is based on metro areas. Choose the plan that fits your footprint — we confirm everything during onboarding. No per-metro billing, no surprises."
          align="center"
        />
        <p className="mx-auto mt-4 max-w-2xl text-center text-sm font-semibold text-ocean">
          Founding customer rate locked for 12 months while your subscription remains active.
        </p>
      </section>

      {/* Plan cards */}
      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="grid gap-6 md:grid-cols-3">
          {plans.map((plan) => (
            <div
              key={plan.name}
              className={`flex flex-col rounded-3xl border p-6 shadow-soft ${plan.highlight ? "border-ocean bg-card" : "border-cardBorder bg-card"
                }`}
            >
              <div className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">
                  {plan.name}
                </p>
                <p className="font-display text-4xl text-ink">{plan.price}</p>
                <p className="text-sm text-inkMuted">{plan.note}</p>
              </div>
              <ul className="mt-6 flex-1 space-y-3 text-sm text-inkMuted">
                {plan.features.map((feature) => (
                  <li key={feature}>{feature}</li>
                ))}
              </ul>
              <div className="mt-6">
                <a
                  href={plan.ctaHref}
                  {...(plan.ctaExternal ? { target: "_blank", rel: "noopener noreferrer" } : {})}
                  className={`inline-flex w-full items-center justify-center rounded-full px-4 py-2.5 text-sm font-semibold transition ${plan.ctaStyle === "primary"
                    ? "bg-ocean text-white shadow-glow hover:bg-oceanDark"
                    : "border border-cardBorder text-ink hover:border-ink/40"
                    }`}
                >
                  {plan.ctaLabel}
                </a>
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* Enterprise */}
      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <h3 className="font-display text-2xl text-ink">Enterprise</h3>
          <p className="mt-3 text-inkMuted">
            Need 10+ metros, statewide coverage, OSHA area office alignment, or CRM integration?
            We will build a plan around your footprint.
          </p>
          <div className="mt-4">
            <a
              href={contactMailto}
              className="inline-flex items-center justify-center rounded-full border border-cardBorder px-4 py-2.5 text-sm font-semibold text-ink transition hover:border-ink/40"
            >
              Contact us
            </a>
          </div>
        </div>
      </section>

      {/* How it works */}
      <section className="mx-auto w-full max-w-5xl px-6">
        <SectionHeading
          eyebrow="How coverage works"
          title="Coverage is based on metro areas."
          description="A metro area is a major city and its surrounding suburbs — roughly aligned with Census MSA boundaries. Tell us your metros during onboarding and we will configure your alerts."
        />

        <div className="mt-8 space-y-4">
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h4 className="font-display text-lg text-ink">Trust flow</h4>
            <p className="mt-3 text-sm text-inkMuted">
              1) Pick a plan → 2) Tell us your metros → 3) We confirm fit. We will not increase billing without your explicit approval.
            </p>
          </div>

          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h4 className="font-display text-lg text-ink">Example: Florida major metros (4)</h4>
            <ul className="mt-3 space-y-2 text-sm text-inkMuted">
              <li>Miami–Fort Lauderdale–West Palm Beach</li>
              <li>Orlando</li>
              <li>Tampa–St. Petersburg</li>
              <li>Jacksonville</li>
            </ul>
            <p className="mt-3 text-sm font-semibold text-ink">→ 4 metros → Core at $299/mo</p>
          </div>

          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h4 className="font-display text-lg text-ink">OSHA Area Office alignment</h4>
            <p className="mt-3 text-sm text-inkMuted">
              If you prefer OSHA Area Office alignment, we support that on Enterprise or can confirm the mapping during onboarding.
            </p>
          </div>
        </div>
      </section>

      {/* Coverage Estimator */}
      <section className="mx-auto w-full max-w-5xl px-6">
        <CoverageEstimator />
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <h3 className="font-display text-2xl text-ink">Questions before subscribing?</h3>
          <p className="mt-3 text-inkMuted">
            See plan selection, metro definitions, coverage changes, and OSHA Area Office alignment details.
          </p>
          <div className="mt-4">
            <Link
              href="/faq"
              className="inline-flex items-center justify-center rounded-full border border-cardBorder px-4 py-2.5 text-sm font-semibold text-ink transition hover:border-ink/40"
            >
              Read the FAQ
            </Link>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl bg-inkFixed px-8 py-10 text-white shadow-soft">
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <h2 className="font-display text-3xl">Try it free for 14 days.</h2>
              <p className="mt-3 text-white/70">
                Up to 4 metros included. We will send a sample alert and configure a trial feed so you can evaluate signal quality.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
