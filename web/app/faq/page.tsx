import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import CTAButtons from "@/components/CTAButtons";

export const metadata: Metadata = {
  alternates: { canonical: "/faq" }
};

const faqs = [
  {
    q: "Who is this best for?",
    a: "MicroFlowOps is best for safety consulting and training firms that already do outbound or business development with employers."
  },
  {
    q: "Who is this not ideal for?",
    a: "It is less useful for teams looking for a full compliance workflow or teams not doing outreach."
  },
  {
    q: "What happens first?",
    a: "Start with a sample. Sample = one example digest for your state or region. If the state or region and lead quality look right, the next step is Founding Pilot at $149, then Standard at $299 or Multi-Territory at $499."
  },
  {
    q: "What does a usable lead include?",
    a: "At minimum: company name, location, signal type, observed timing, and a public source link your team can verify quickly."
  },
  {
    q: "How do I verify a lead?",
    a: "Verify in 30 seconds. Each sample item links back to the public OSHA record so you can confirm the location and timing quickly."
  },
  {
    q: "How does the Founding Pilot work?",
    a: "Founding Pilot is $149 for 30 days in one state. We review fit manually before activation."
  },
  {
    q: "Do standard plans still exist?",
    a: "Yes. Standard is $299 and Multi-Territory is $499 once you are ready for ongoing coverage."
  },
  {
    q: "How does state or region setup work?",
    a: "Tell us your state or region. State, metro, counties, or OSHA area all work. We confirm fit before activation."
  },
  {
    q: "Is onboarding handled over email only?",
    a: "Yes. No calls are required. We can handle the sample, state or region fit, and next-step planning over email."
  },
  {
    q: "Where does the data come from?",
    a: "We use public OSHA inspection activity and related public records. We do not purchase private datasets."
  },
  {
    q: "Do you provide legal advice or citation deadlines?",
    a: "No. This is an informational lead product, not legal advice, and not a full compliance workflow."
  },
  {
    q: "How do unsubscribe requests work?",
    a: "Every alert includes opt-out instructions. Requests are honored immediately and applied to future sends."
  }
];

export default function FaqPage() {
  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="FAQ"
          title="Answers to common questions."
          description="Short answers on fit, verification, state or region setup, and pricing."
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
              <h2 className="font-display text-3xl">Need a sample for your state or region?</h2>
              <p className="mt-3 text-white/70">
                Request a sample first, then decide whether Founding Pilot, Standard, or Multi-Territory is
                the right next step.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
