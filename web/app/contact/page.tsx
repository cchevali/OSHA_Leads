import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import TrialRequestForm from "@/components/TrialRequestForm";
import site from "@/config/site.json";
import { buildStripeCheckoutUrl } from "@/lib/checkout";

export const metadata: Metadata = {
  alternates: { canonical: "/contact" }
};

export default function ContactPage() {
  const stripeCheckoutUrl = buildStripeCheckoutUrl(site.stripePaymentLink);

  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Contact"
          title="Request a trial feed."
          description="Tell us your territory and we will start a 7-day trial. No credit card needed."
          align="center"
        />
      </section>

      <section id="trial" className="mx-auto w-full max-w-5xl px-6">
        <div className="grid gap-6 md:grid-cols-2">
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-2xl text-ink">Start a free trial</h3>
            <p className="mt-2 mb-5 text-sm text-inkMuted">
              We will send a sample alert and set up a trial feed for your territory.
            </p>
            <TrialRequestForm />
          </div>
          <div className="flex flex-col gap-6">
            <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
              <h3 className="font-display text-2xl text-ink">Email us directly</h3>
              <p className="mt-3 text-inkMuted">
                We respond same business day. Include your territory, recipients, and any timing
                preferences.
              </p>
              <p className="mt-4 text-sm font-semibold text-ink">{site.ctaEmail}</p>
            </div>
            <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
              <h3 className="font-display text-2xl text-ink">Already decided?</h3>
              <p className="mt-3 text-inkMuted">
                Subscribe directly and we will activate your territory within 24 hours.
              </p>
              <div className="mt-4">
                <a
                  href={stripeCheckoutUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center justify-center rounded-full bg-ocean px-4 py-2 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark"
                >
                  Subscribe — $399/mo
                </a>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <h3 className="font-display text-2xl text-ink">Mailing address</h3>
          <p className="mt-3 text-inkMuted">{site.mailingAddress}</p>
        </div>
      </section>
    </div>
  );
}
