import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import OnboardingMetroForm from "@/components/OnboardingMetroForm";
import site from "@/config/site.json";
import { loadCbsaOptions } from "@/lib/cbsa";

export const metadata: Metadata = {
  alternates: { canonical: "/onboarding" }
};

type OnboardingPageProps = {
  searchParams?: {
    plan?: string;
    email?: string;
    subscriber_key?: string;
  };
};

export default function OnboardingPage({ searchParams }: OnboardingPageProps) {
  const options = loadCbsaOptions();
  const initialPlanCode = String(searchParams?.plan || "core");
  const initialEmail = String(searchParams?.email || "");
  const initialSubscriberKey = String(searchParams?.subscriber_key || "");
  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Onboarding"
          title="Set your metro coverage"
          description="Select your metros as Census CBSA/MSA boundaries (city + suburbs). We enforce your plan cap on submission so coverage is deterministic."
          align="center"
        />
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <h2 className="font-display text-2xl text-ink">Submit onboarding details</h2>
          <p className="mt-3 text-sm text-inkMuted">
            Core supports up to 4 metros. Multi-Territory supports up to 10 metros. If you need expansion beyond your cap,
            submission is blocked and routed to contact.
          </p>
          <OnboardingMetroForm
            options={options}
            initialPlanCode={initialPlanCode}
            initialEmail={initialEmail}
            initialSubscriberKey={initialSubscriberKey}
          />
          <p className="mt-4 text-xs text-inkMuted">
            Need help mapping cities to CBSA codes? Email {site.ctaEmail}.
          </p>
        </div>
      </section>
    </div>
  );
}
