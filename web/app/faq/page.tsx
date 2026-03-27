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
    q: "Why is the public sample frozen instead of live?",
    a: "The frozen sample stays populated with real rows so buyers can inspect the lead format, proof, and verification flow at any time."
  },
  {
    q: "What does a usable lead include?",
    a: "At minimum: company name, city/state, signal type, observed timing, and a public source link. When available, we also include website, phone, public contact details, and a short reason the signal may matter now."
  },
  {
    q: "How do I verify a lead?",
    a: "Verify in 30 seconds. Each sample item links back to the public OSHA record so you can confirm the location and timing quickly."
  },
  {
    q: "How does the Founding Pilot work?",
    a: "Founding Pilot is $149 for 30 days in one state. It uses the current request flow, and every pilot is manually qualified before activation."
  },
  {
    q: "Do standard plans still exist?",
    a: "Yes. Core and Multi-Territory remain available as the standard ongoing plans after you confirm territory fit."
  },
  {
    q: "How do territories work?",
    a: "Send states, metros, counties, or OSHA areas. We confirm fit from the labels you already use."
  },
  {
    q: "Is onboarding handled over email only?",
    a: "Yes. No calls are required; we can qualify sample requests, founding pilots, and territory fit over email."
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
          description="Short answers on fit, proof, territory setup, and the Founding Pilot."
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
              <h2 className="font-display text-3xl">Need a sample for your territory?</h2>
              <p className="mt-3 text-white/70">
                Request a sample first, then decide whether the Founding Pilot or a standard plan is the right
                next step.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
