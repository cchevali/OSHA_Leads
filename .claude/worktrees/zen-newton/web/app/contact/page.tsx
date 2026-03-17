import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import TrialRequestForm from "@/components/TrialRequestForm";
import CopyEmailButton from "@/components/CopyEmailButton";
import site from "@/config/site.json";
import { resolveCheckoutCta } from "@/lib/checkout";

export const metadata: Metadata = {
  title: "Contact",
  description:
    "Request a trial feed, confirm metro coverage, or contact MicroFlowOps about OSHA activity signal delivery for your team.",
  alternates: { canonical: "/contact" }
};

export default function ContactPage() {
  const coreFallbackMailto = `mailto:${site.ctaEmail}?${new URLSearchParams({
    subject: "Core plan inquiry",
    body: "Hi MicroFlowOps,\n\nI want to start Core at $299/mo.\n\nOrganization:\nMetros to cover:\nRecipients:\n\nThanks"
  }).toString()}`;
  const stripeCheckout = resolveCheckoutCta(site.stripePaymentLinkCore, coreFallbackMailto);

  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Contact"
          title="Request a trial feed."
          description="Tell us your metros and we will start a 14-day trial — up to 4 metros included. No credit card needed."
          align="center"
        />
      </section>

      <section id="trial" className="mx-auto w-full max-w-5xl px-6">
        <div className="grid gap-6 md:grid-cols-2">
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-2xl text-ink">Start a free trial</h3>
            <p className="mt-2 mb-5 text-sm text-inkMuted">
              We will send a sample alert and set up a trial feed for your metros. Add up to 6 recipients for the pilot, and keep your company email as the billing/admin contact.
            </p>
            <TrialRequestForm />
          </div>
          <div className="flex flex-col gap-6">
            <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
              <h3 className="font-display text-2xl text-ink">Email us directly</h3>
              <p className="mt-3 text-inkMuted">
                We respond same business day. Include your metros (or cities/states — we will translate), recipient names/emails, and any timing preferences.
              </p>
              <div className="mt-4 flex flex-wrap items-center gap-4">
                <p className="text-sm font-semibold text-ink">{site.ctaEmail}</p>
                <CopyEmailButton email={site.ctaEmail} />
              </div>
              <a
                href={`mailto:${site.ctaEmail}`}
                className="mt-3 inline-flex text-xs font-semibold text-ocean underline transition hover:text-oceanDark"
              >
                Open in email app (optional)
              </a>
            </div>
            <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
              <h3 className="font-display text-2xl text-ink">Already decided?</h3>
              <p className="mt-3 text-inkMuted">
                Subscribe directly and we will activate your coverage within 24 hours.
              </p>
              <div className="mt-4">
                <a
                  href={stripeCheckout.href}
                  {...(stripeCheckout.isExternal ? { target: "_blank", rel: "noopener noreferrer" } : {})}
                  className="inline-flex items-center justify-center rounded-full bg-ocean px-4 py-2 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark"
                >
                  Subscribe — $299/mo
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
