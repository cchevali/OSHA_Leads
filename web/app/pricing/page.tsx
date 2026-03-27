import type { Metadata } from "next";
import Link from "next/link";
import SectionHeading from "@/components/SectionHeading";
import CTAButtons from "@/components/CTAButtons";
import site from "@/config/site.json";
import { resolveCheckoutCta } from "@/lib/checkout";

export const metadata: Metadata = {
  title: "Pricing",
  description:
    "Sample first, then choose Founding Pilot, Standard, or Multi-Territory. Tell us your state, metro, counties, or OSHA area and we confirm fit before activation.",
  alternates: { canonical: "/pricing" }
};

export default function PricingPage() {
  const standardCheckout = resolveCheckoutCta(site.stripePaymentLinkCore, "/contact?source=pricing&intent=sample");
  const multiCheckout = resolveCheckoutCta(site.stripePaymentLinkMulti, "/contact?source=pricing&intent=territory_reply");
  const foundingPilotPath = "/contact?source=pricing&intent=founding_pilot";
  const samplePath = "/contact?source=pricing&intent=sample";
  const territoryMailto = `mailto:${site.ctaEmail}?${new URLSearchParams({
    subject: site.ctaTerritorySubject,
    body: site.ctaTerritoryBody
  }).toString()}`;
  const confirmFirstMailto = `mailto:${site.ctaEmail}?${new URLSearchParams({
    subject: "Replying with my territory",
    body: "Hi MicroFlowOps,\n\nI would like to confirm fit for my territory.\n\nOrganization:\nState, metro, counties, or OSHA area:\nRecipients:\nCurrent outbound motion:\n\nThanks"
  }).toString()}`;

  const plans = [
    {
      name: "Founding Pilot",
      price: "$149",
      note: "30 days",
      highlight: true,
      features: [
        "One state",
        "Daily lead digest",
        "Manual fit confirmation before activation",
        "Best first paid step after a sample"
      ],
      ctaLabel: "Start founding pilot",
      ctaHref: foundingPilotPath,
      ctaExternal: false,
      ctaStyle: "primary" as const
    },
    {
      name: "Standard",
      price: "$299",
      note: "per month",
      highlight: false,
      features: [
        "Ongoing daily delivery",
        "One primary territory",
        "Up to 6 recipients",
        "Weekly summary included"
      ],
      ctaLabel: "Subscribe - $299/mo",
      ctaHref: standardCheckout.href,
      ctaExternal: standardCheckout.isExternal,
      ctaStyle: "outline" as const
    },
    {
      name: "Multi-Territory",
      price: "$499",
      note: "per month",
      highlight: false,
      features: [
        "Broader ongoing coverage",
        "Everything in Standard",
        "Up to 15 recipients",
        "Priority support"
      ],
      ctaLabel: "Subscribe - $499/mo",
      ctaHref: multiCheckout.href,
      ctaExternal: multiCheckout.isExternal,
      ctaStyle: "outline" as const
    }
  ];

  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Pricing"
          title="Sample first. Then choose the plan that fits."
          description="Founding Pilot is $149. Standard is $299. Multi-Territory is $499."
          align="center"
        />
        <p className="mx-auto mt-4 max-w-2xl text-center text-sm text-inkMuted">
          Best for safety consulting and training firms that already do outbound or business development.
        </p>
        <p className="mx-auto mt-3 max-w-2xl text-center text-sm text-inkMuted">
          Less useful for teams looking for a full compliance workflow or teams not doing outreach.
        </p>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="grid gap-6 md:grid-cols-3">
          {plans.map((plan) => (
            <div
              key={plan.name}
              className={`flex flex-col rounded-3xl border p-6 shadow-soft ${
                plan.highlight ? "border-ocean bg-card" : "border-cardBorder bg-card"
              }`}
            >
              <div className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">{plan.name}</p>
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
                  className={`inline-flex w-full items-center justify-center rounded-full px-4 py-2.5 text-sm font-semibold transition ${
                    plan.ctaStyle === "primary"
                      ? "bg-ocean text-white shadow-glow hover:bg-oceanDark"
                      : "border border-cardBorder text-ink hover:border-ink/40"
                  }`}
                >
                  {plan.ctaLabel}
                </a>
                {plan.name === "Founding Pilot" ? (
                  <p className="mt-2 text-xs text-inkMuted">
                    Request a sample first if you want to see the lead quality before we activate the pilot.
                  </p>
                ) : (
                  <>
                    <p className="mt-2 text-xs text-inkMuted">
                      Best once you already know the territory and lead quality are a fit.
                    </p>
                    <a
                      href={confirmFirstMailto}
                      className="mt-2 inline-flex text-xs font-semibold text-ocean underline transition hover:text-oceanDark"
                    >
                      Reply with territory before subscribing
                    </a>
                  </>
                )}
              </div>
            </div>
          ))}
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <h3 className="font-display text-2xl text-ink">What most buyers do first</h3>
          <p className="mt-3 text-inkMuted">
            Request a sample, confirm the lead quality, then decide whether Founding Pilot, Standard, or
            Multi-Territory is the right next step.
          </p>
          <div className="mt-4 flex flex-wrap items-center gap-4">
            <Link
              href={samplePath}
              className="inline-flex items-center justify-center rounded-full bg-ocean px-4 py-2.5 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark"
            >
              Request a sample
            </Link>
            <a
              href={territoryMailto}
              className="text-sm font-semibold text-inkMuted underline-offset-4 transition hover:text-ink hover:underline"
            >
              Tell us your territory
            </a>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft text-center">
          <p className="text-sm text-inkMuted">
            Tell us your state, metro, counties, or OSHA area. We confirm fit before activation.
          </p>
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <h3 className="font-display text-2xl text-ink">Questions before you decide?</h3>
          <p className="mt-3 text-inkMuted">
            See quick answers on samples, territory fit, pricing, and what happens next.
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
              <h2 className="font-display text-3xl">See a sample for your territory first.</h2>
              <p className="mt-3 text-white/70">
                Then decide whether Founding Pilot, Standard, or Multi-Territory is the right fit.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
