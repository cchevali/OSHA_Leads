import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import OnboardingMetroForm from "@/components/OnboardingMetroForm";
import site from "@/config/site.json";
import { loadCbsaOptions } from "@/lib/cbsa";

const COVERAGE_HELPER =
  "Tell us your state, metro, counties, or OSHA area. We confirm fit before activation.";

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
          title="Set your state or region"
          description="Tell us the state or region you want covered and we will save it for your ongoing plan."
          align="center"
        />
        <p className="mt-4 text-center text-sm font-semibold text-inkMuted">{COVERAGE_HELPER}</p>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <h2 className="font-display text-2xl text-ink">Submit onboarding details</h2>
          <p className="mt-3 text-sm text-inkMuted">
            We use metro mapping behind the scenes so ongoing coverage stays consistent.
          </p>
          <p className="mt-3 text-sm text-inkMuted">
            Standard supports one primary state or region setup. Multi-Territory supports broader coverage. If you need more than your current plan allows, we will route you to contact.
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
