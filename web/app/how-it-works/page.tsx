import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import CTAButtons from "@/components/CTAButtons";

export const metadata: Metadata = {
  alternates: { canonical: "/how-it-works" }
};

const steps = [
  {
    title: "We watch public OSHA activity",
    body: "We track newly observed public OSHA activity so your team can see it early."
  },
  {
    title: "We package the best leads for your state or region",
    body: "You tell us the state or region you care about, and we package the strongest leads for that footprint."
  },
  {
    title: "We email a short daily digest",
    body: "Your team gets a concise daily digest it can review, verify, and use for outreach."
  }
];

const buyerNotes = [
  "Sample = one example digest for your state or region.",
  "Each lead links back to the public OSHA record for quick verification.",
  "Need live proof? Ask about a 14-day trial."
];

export default function HowItWorksPage() {
  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="How It Works"
          title="We watch public OSHA activity, package the best leads for your state or region, and email a short daily digest."
          description="Built for safety consulting and training firms already doing outbound, not for teams looking for a full compliance workflow."
          align="center"
        />
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="grid gap-6 md:grid-cols-3">
          {steps.map((step) => (
            <div key={step.title} className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
              <h3 className="font-display text-2xl text-ink">{step.title}</h3>
              <p className="mt-3 text-inkMuted">{step.body}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <SectionHeading
          eyebrow="What Buyers See"
          title="Simple enough to understand in a minute."
          description="A clear sample, clear state or region fit, and one quick verification path."
        />
        <div className="mt-8 grid gap-4 md:grid-cols-3">
          {buyerNotes.map((item) => (
            <div key={item} className="rounded-3xl border border-cardBorder bg-card p-5">
              <p className="text-sm text-inkMuted">{item}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl bg-inkFixed px-8 py-10 text-white shadow-soft">
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <h2 className="font-display text-3xl">See a sample for your state or region.</h2>
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
