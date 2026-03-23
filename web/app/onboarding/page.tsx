import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import OnboardingMetroForm from "@/components/OnboardingMetroForm";
import site from "@/config/site.json";
import { loadCbsaOptions } from "@/lib/cbsa";

const COVERAGE_HELPER =
  "Counties, cities, metros, or OSHA areas work — we translate coverage for you.";

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
          title="Set your coverage"
          description="Tell us your footprint and we translate it to Census CBSA/MSA boundaries before saving your coverage."
          align="center"
        />
        <p className="mt-4 text-center text-sm font-semibold text-inkMuted">{COVERAGE_HELPER}</p>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <h2 className="font-display text-2xl text-ink">Submit onboarding details</h2>
          <p className="mt-3 text-sm text-inkMuted">
            A metro area is a Census CBSA/MSA (city + suburbs). We use that mapping so plan coverage stays deterministic.
          </p>
          <p className="mt-3 text-sm text-inkMuted">
            Core supports up to 4 metros. Multi-Territory supports up to 10 metros. If you need expansion beyond your cap,
            submission is blocked and routed to contact.
          </p>
          <p className="mt-2 text-sm text-inkMuted">
            No calls required; onboarding is handled via a short form + email confirmation.
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
