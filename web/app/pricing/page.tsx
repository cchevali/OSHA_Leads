import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import CTAButtons from "@/components/CTAButtons";
import site from "@/config/site.json";
import { buildStripeCheckoutUrl } from "@/lib/checkout";

export const metadata: Metadata = {
  alternates: { canonical: "/pricing" }
};

export default function PricingPage() {
  const stripeCheckoutUrl = buildStripeCheckoutUrl(site.stripePaymentLink);
  const trialMailto = `mailto:${site.ctaEmail}?${new URLSearchParams({
    subject: site.ctaSampleSubject,
    body: site.ctaSampleBody
  }).toString()}`;
  const contactMailto = `mailto:${site.ctaEmail}?${new URLSearchParams({
    subject: "Growth plan inquiry",
    body: "Hi MicroFlowOps,\n\nI am interested in the Growth plan. Can you share more details?\n\nOrganization:\nTerritory:\n\nThanks"
  }).toString()}`;

  const plans = [
    {
      name: "Pilot",
      price: "$0",
      note: "14 days",
      highlight: false,
      features: [
        "One territory (your choice)",
        "Daily email brief",
        "Priority scoring",
        "Sample alert preview"
      ],
      ctaLabel: "Start free pilot",
      ctaHref: trialMailto,
      ctaExternal: false,
      ctaStyle: "outline" as const
    },
    {
      name: "Core",
      price: "$399",
      note: "per territory / month",
      highlight: true,
      features: [
        "Daily email delivery",
        "Territory-specific filters",
        "Up to 6 recipients",
        "Weekly summary add-on"
      ],
      ctaLabel: "Subscribe — $399/mo",
      ctaHref: stripeCheckoutUrl,
      ctaExternal: true,
      ctaStyle: "primary" as const
    },
    {
      name: "Growth",
      price: "$699",
      note: "per territory / month",
      highlight: false,
      badge: "Coming Soon",
      features: [
        "Everything in Core",
        "Expanded enrichment",
        "Custom scoring rules",
        "Priority support"
      ],
      ctaLabel: "Contact us",
      ctaHref: contactMailto,
      ctaExternal: false,
      ctaStyle: "outline" as const
    }
  ];

  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Pricing"
          title="Simple territory-based pricing."
          description="Start with one territory and scale as coverage expands."
          align="center"
        />
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="grid gap-6 md:grid-cols-3">
          {plans.map((plan) => (
            <div
              key={plan.name}
              className={`flex flex-col rounded-3xl border p-6 shadow-soft ${
                plan.highlight ? "border-ocean bg-white" : "border-black/10 bg-white/85"
              }`}
            >
              <div className="space-y-2">
                <div className="flex items-center gap-2">
                  <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">
                    {plan.name}
                  </p>
                  {"badge" in plan && plan.badge && (
                    <span className="rounded-full bg-amber-100 px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-wider text-amber-700">
                      {plan.badge}
                    </span>
                  )}
                </div>
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
                      : "border border-ink/15 text-ink hover:border-ink/40"
                  }`}
                >
                  {plan.ctaLabel}
                </a>
              </div>
            </div>
          ))}
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
          <h3 className="font-display text-2xl text-ink">Enterprise and multi-territory</h3>
          <p className="mt-3 text-inkMuted">
            Need multi-state coverage, custom reporting, or CRM integration? We will build a plan
            around your footprint.
          </p>
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl bg-ink px-8 py-10 text-white shadow-soft">
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <h2 className="font-display text-3xl">Try it free for 14 days.</h2>
              <p className="mt-3 text-white/70">
                We will send a no-commitment sample alert and trial feed so you can evaluate the signal quality before subscribing.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
