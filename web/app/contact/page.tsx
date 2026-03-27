import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import TrialRequestForm from "@/components/TrialRequestForm";
import CopyEmailButton from "@/components/CopyEmailButton";
import site from "@/config/site.json";
import { resolveCheckoutCta } from "@/lib/checkout";

export const metadata: Metadata = {
  title: "Contact",
  description:
    "Request a sample, start a manually qualified founding pilot, or tell MicroFlowOps your state, metro, counties, or OSHA area for fit confirmation.",
  alternates: { canonical: "/contact" }
};

export default function ContactPage() {
  const coreFallbackMailto = `mailto:${site.ctaEmail}?${new URLSearchParams({
    subject: "Standard plan inquiry",
    body: "Hi MicroFlowOps,\n\nI want to discuss an ongoing standard plan.\n\nOrganization:\nState, metro, counties, or OSHA area:\nRecipients:\n\nThanks"
  }).toString()}`;
  const stripeCheckout = resolveCheckoutCta(site.stripePaymentLinkCore, coreFallbackMailto);

  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Contact"
          title="Request a sample or start a founding pilot."
          description="Share your territory and recipients. We send a sample first, then confirm whether Founding Pilot or a standard plan fits."
          align="center"
        />
        <p className="mt-4 text-center text-sm text-inkMuted">
          Best for safety consulting and training firms that already do outbound or business development.
        </p>
        <p className="mt-3 text-center text-sm text-inkMuted">
          Founding Pilot is $149 for 30 days in one state. Manual qualification required before activation.
        </p>
      </section>

      <section id="trial" className="mx-auto w-full max-w-5xl px-6">
        <div className="grid gap-6 md:grid-cols-2">
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-2xl text-ink">Request a sample</h3>
            <p className="mb-5 mt-2 text-sm text-inkMuted">
              Tell us your state, metro, counties, or OSHA area, add the recipients who should see the sample,
              and we will respond same business day.
            </p>
            <p className="mb-2 text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">
              Verify in 30 seconds
            </p>
            <p className="mb-5 text-sm text-inkMuted">
              Every sample includes observed timing plus a direct link to the public OSHA record so your team
              can verify the lead fast.
            </p>
            <TrialRequestForm source="contact" />
          </div>
          <div className="flex flex-col gap-6">
            <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
              <h3 className="font-display text-2xl text-ink">Founding Pilot</h3>
              <p className="mt-3 text-inkMuted">
                30 days, one state, $149. Start with a sample if you want, then we manually confirm fit before
                activation.
              </p>
            </div>
            <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
              <h3 className="font-display text-2xl text-ink">Tell us your territory</h3>
              <p className="mt-3 text-inkMuted">
                Tell us your state, metro, counties, or OSHA area. We confirm fit before activation and reply
                same business day.
              </p>
              <div className="mt-4 flex flex-wrap items-center gap-4">
                <p className="text-sm font-semibold text-ink">{site.ctaEmail}</p>
                <CopyEmailButton email={site.ctaEmail} />
              </div>
              <a
                href={`mailto:${site.ctaEmail}?${new URLSearchParams({
                  subject: site.ctaTerritorySubject,
                  body: site.ctaTerritoryBody
                }).toString()}`}
                className="mt-3 inline-flex text-xs font-semibold text-ocean underline transition hover:text-oceanDark"
              >
                Open in email app
              </a>
            </div>
            <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
              <h3 className="font-display text-2xl text-ink">Already decided on a standard plan?</h3>
              <p className="mt-3 text-inkMuted">
                Standard and Multi-Territory remain available once you know the territory and lead quality are a fit.
              </p>
              <div className="mt-4">
                <a
                  href={stripeCheckout.href}
                  {...(stripeCheckout.isExternal ? { target: "_blank", rel: "noopener noreferrer" } : {})}
                  className="inline-flex items-center justify-center rounded-full border border-cardBorder px-4 py-2 text-sm font-semibold text-ink transition hover:border-ink/40"
                >
                  Subscribe - $299/mo
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
