import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import CTAButtons from "@/components/CTAButtons";

export const metadata: Metadata = {
  alternates: { canonical: "/how-it-works" }
};

const steps = [
  {
    title: "Signal capture",
    body: "We monitor new OSHA inspection activity and flag fresh updates as soon as they appear."
  },
  {
    title: "Enrichment",
    body: "We attach industry, location, enforcement history, and category context."
  },
  {
    title: "Scoring",
    body: "Signals are ranked by severity, recency, and commercial intent."
  },
  {
    title: "Delivery",
    body: "Your team gets a concise brief by email or SMS at the same time every morning."
  }
];

const safeguards = [
  "Suppression and unsubscribe enforcement on every send.",
  "Audit logs for every delivery attempt.",
  "Territory-specific filtering and hard caps per territory.",
  "Optional pilot mode before live delivery."
];

export default function HowItWorksPage() {
  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="How it works"
          title="From public activity to a short, ranked morning brief."
          description="Our daily pipeline turns public OSHA inspection activity into usable signals without manual research."
          align="center"
        />
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="grid gap-6 md:grid-cols-2">
          {steps.map((step) => (
            <div key={step.title} className="rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
              <h3 className="font-display text-2xl text-ink">{step.title}</h3>
              <p className="mt-3 text-inkMuted">{step.body}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <SectionHeading
          eyebrow="Delivery cadence"
          title="Daily by default, tuned for your team."
          description="Set the window, select territories, and we deliver each morning."
        />
        <div className="mt-8 rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
          <ul className="space-y-3 text-sm text-inkMuted">
            <li>Typical delivery: 7:00 AM CT for Texas Triangle coverage.</li>
            <li>Daily or weekly summaries supported.</li>
            <li>SMS available for urgent, high-severity signals.</li>
          </ul>
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <SectionHeading
          eyebrow="Safeguards"
          title="Operational guardrails are built in."
          description="We keep sends clean and compliant with a few critical controls."
        />
        <div className="mt-8 grid gap-4 md:grid-cols-2">
          {safeguards.map((item) => (
            <div key={item} className="rounded-3xl border border-black/10 bg-white/85 p-5">
              <p className="text-sm text-inkMuted">{item}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl bg-ink px-8 py-10 text-white shadow-soft">
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <h2 className="font-display text-3xl">Ready to pilot your territory?</h2>
              <p className="mt-3 text-white/70">
                We can send a sample alert within 24 hours and tune the filters to match your needs.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
