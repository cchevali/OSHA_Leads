import SectionHeading from "@/components/SectionHeading";
import CTAButtons from "@/components/CTAButtons";
import CopyEmailTemplate from "@/components/CopyEmailTemplate";
import site from "@/config/site.json";

const plans = [
  {
    name: "Pilot",
    price: "$0",
    note: "14 days",
    highlight: false,
    features: [
      "One territory (Texas Triangle)",
      "Daily email brief",
      "Priority scoring",
      "Sample alert preview"
    ]
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
    ]
  },
  {
    name: "Growth",
    price: "$699",
    note: "per territory / month",
    highlight: false,
    features: [
      "SMS urgent alerts",
      "Expanded enrichment",
      "Custom scoring rules",
      "Priority support"
    ]
  }
];

export default function PricingPage() {
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
              className={`rounded-3xl border p-6 shadow-soft ${
                plan.highlight ? "border-ocean bg-white" : "border-black/10 bg-white/85"
              }`}
            >
              <div className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">
                  {plan.name}
                </p>
                <p className="font-display text-4xl text-ink">{plan.price}</p>
                <p className="text-sm text-inkMuted">{plan.note}</p>
              </div>
              <ul className="mt-6 space-y-3 text-sm text-inkMuted">
                {plan.features.map((feature) => (
                  <li key={feature}>{feature}</li>
                ))}
              </ul>
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

      <section className="mx-auto w-full max-w-6xl px-6">
        <SectionHeading
          eyebrow="Copy/Paste"
          title="Copy/Paste Email Template"
          description="Copy the exact subject/body used by the email buttons."
        />
        <div className="mt-8 space-y-6">
          <CopyEmailTemplate
            title="Request a sample"
            subject={site.ctaSampleSubject}
            body={site.ctaSampleBody}
          />
          <CopyEmailTemplate
            title="Reply with your territory + firm name"
            subject={site.ctaTerritorySubject}
            body={site.ctaTerritoryBody}
          />
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl bg-ink px-8 py-10 text-white shadow-soft">
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <h2 className="font-display text-3xl">Want a sample alert first?</h2>
              <p className="mt-3 text-white/70">
                We will send a no-commitment preview so you can see the signal quality.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}