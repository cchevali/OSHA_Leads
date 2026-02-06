import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import site from "@/config/site.json";

export const metadata: Metadata = {
  alternates: { canonical: "/privacy" }
};

export default function PrivacyPage() {
  return (
    <div className="space-y-12 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Privacy"
          title="Privacy policy"
          description="Last updated February 6, 2026."
          align="center"
        />
      </section>

      <section className="mx-auto w-full max-w-4xl px-6">
        <div className="space-y-6 rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
          <p className="text-sm text-inkMuted">
            {site.legalName || site.brandName} (&quot;we&quot;, &quot;us&quot;) provides OSHA activity alerts for business
            users. This policy explains what we collect and how we use it.
          </p>

          <div className="space-y-3 text-sm text-inkMuted">
            <p className="font-semibold text-ink">Information we collect</p>
            <p>
              Business contact details you provide, delivery preferences, and operational logs for
              alert delivery. We do not collect sensitive personal data.
            </p>
          </div>

          <div className="space-y-3 text-sm text-inkMuted">
            <p className="font-semibold text-ink">Analytics</p>
            <p>
              We use Plausible Analytics to understand basic site usage and measure conversion-intent
              actions (for example: clicking the mailto CTAs and using the copy buttons). This
              analytics is intended to be privacy-friendly and does not use advertising profiles.
            </p>
          </div>

          <div className="space-y-3 text-sm text-inkMuted">
            <p className="font-semibold text-ink">How we use data</p>
            <p>
              We use your data to deliver alerts, respond to inquiries, and improve service quality.
              We do not sell personal data.
            </p>
          </div>

          <div className="space-y-3 text-sm text-inkMuted">
            <p className="font-semibold text-ink">Unsubscribe and suppression</p>
            <p>
              Every alert includes opt-out instructions. Requests are honored immediately and
              suppressed from future sends.
            </p>
          </div>

          <div className="space-y-3 text-sm text-inkMuted">
            <p className="font-semibold text-ink">Contact</p>
            <p>
              For privacy requests, email {site.ctaEmail} or write to {site.mailingAddress}.
            </p>
          </div>
        </div>
      </section>
    </div>
  );
}
