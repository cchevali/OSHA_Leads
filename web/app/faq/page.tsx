import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import CTAButtons from "@/components/CTAButtons";

export const metadata: Metadata = {
  alternates: { canonical: "/faq" }
};

const faqs = [
  {
    q: "Do I need to count metros exactly?",
    a: "No. List the counties, cities, metros, or OSHA areas you care about and we map coverage during onboarding. We will not change billing without your explicit approval."
  },
  {
    q: "Can I send counties instead of metros?",
    a: "Yes. Counties, cities, metros, or OSHA areas work — we translate coverage for you. For example, LA + Orange + Riverside/San Bernardino + Ventura + Santa Barbara + Kern typically maps to Multi-Territory, with exact mapping confirmed before billing."
  },
  {
    q: "How do I choose a plan?",
    a: "If you are unsure, start with Core. It covers up to 4 metros. If your footprint is larger, we can move you to Multi-Territory or Enterprise only with your approval."
  },
  {
    q: "What counts as a metro?",
    a: "A metro is a major city plus nearby surrounding communities, similar to an MSA-style boundary. You can list counties, city names, or OSHA areas and we will confirm the exact mapping."
  },
  {
    q: "Can I change my metros later?",
    a: "Yes. You can swap your metro selections once per billing cycle at no charge. Just email us with the change."
  },
  {
    q: "Do you support OSHA Area Office alignment?",
    a: "Yes. If you prefer OSHA Area Office alignment, we support that on Enterprise or can confirm the mapping during onboarding. Self-serve plans are metro-based."
  },
  {
    q: "Will my price change?",
    a: "Founding customer rate locked for 12 months while your subscription remains active. We will not increase your billing without explicit approval."
  },
  {
    q: "Is onboarding handled over email only?",
    a: "Yes. No calls are required; onboarding is handled via a short form plus email confirmation, which keeps everything documented."
  },
  {
    q: "Where does the data come from?",
    a: "We use public OSHA inspection activity and related public records. We do not purchase private datasets."
  },
  {
    q: "How quickly are new inspections visible?",
    a: "Signals show up as soon as the inspection is observable in public sources. We refresh daily."
  },
  {
    q: "Do you provide legal advice or citation deadlines?",
    a: "No. Alerts are informational only. We include deadlines only when the public record explicitly supports them."
  },
  {
    q: "How do unsubscribe requests work?",
    a: "Every alert includes opt-out instructions. Requests are honored immediately and applied to future sends."
  },
  {
    q: "Can you integrate with our CRM?",
    a: "We can deliver alerts as CSV, email, or webhook for teams that need CRM ingestion."
  }
];

export default function FaqPage() {
  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="FAQ"
          title="Answers to common questions."
          description="If you need something specific, email us and we will help."
          align="center"
        />
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="space-y-4">
          {faqs.map((item) => (
            <div key={item.q} className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
              <h3 className="font-display text-xl text-ink">{item.q}</h3>
              <p className="mt-3 text-inkMuted">{item.a}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl bg-inkFixed px-8 py-10 text-white shadow-soft">
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <h2 className="font-display text-3xl">Still have questions?</h2>
              <p className="mt-3 text-white/70">
                Email us for a same-day response, or request a trial feed to see the signals firsthand.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
