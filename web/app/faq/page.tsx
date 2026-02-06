import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";

export const metadata: Metadata = {
  alternates: { canonical: "/faq" }
};

const faqs = [
  {
    q: "Is onboarding handled over email only?",
    a: "Yes. Email-only onboarding keeps everything documented. No calls are required."
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
    q: "Can we choose which territories to monitor?",
    a: "Yes. Each subscription is territory-based and can be tuned to specific states or metro areas."
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
            <div key={item.q} className="rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
              <h3 className="font-display text-xl text-ink">{item.q}</h3>
              <p className="mt-3 text-inkMuted">{item.a}</p>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
