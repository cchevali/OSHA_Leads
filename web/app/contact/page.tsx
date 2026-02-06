import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import CTAButtons from "@/components/CTAButtons";
import CopyEmailTemplate from "@/components/CopyEmailTemplate";
import site from "@/config/site.json";

export const metadata: Metadata = {
  alternates: { canonical: "/contact" }
};

export default function ContactPage() {
  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Contact"
          title="Email us for a same-day response."
          description="Email-only onboarding keeps everything fast and documented."
          align="center"
        />
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="grid gap-6 md:grid-cols-2">
          <div className="rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
            <h3 className="font-display text-2xl text-ink">Request a sample</h3>
            <p className="mt-3 text-inkMuted">
              Tell us your firm name and territory. We will send a sample alert and recommended
              setup.
            </p>
            <div className="mt-4">
              <CTAButtons />
            </div>
          </div>
          <div className="rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
            <h3 className="font-display text-2xl text-ink">Email</h3>
            <p className="mt-3 text-inkMuted">
              We respond same business day. Include your territory, recipients, and any timing
              preferences.
            </p>
            <p className="mt-4 text-sm font-semibold text-ink">{site.ctaEmail}</p>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <SectionHeading
          eyebrow="Copy/Paste"
          title="Copy/Paste Email Template"
          description="If you prefer, copy the exact subject/body we use in the buttons above."
        />
        <div className="mt-8 space-y-6">
          <CopyEmailTemplate
            title="Request a sample"
            subject={site.ctaSampleSubject}
            body={site.ctaSampleBody}
            subjectEventName="copy_subject_request_sample"
            bodyEventName="copy_body_request_sample"
          />
          <CopyEmailTemplate
            title="Reply with your territory + firm name"
            subject={site.ctaTerritorySubject}
            body={site.ctaTerritoryBody}
            subjectEventName="copy_subject_territory_firm"
            bodyEventName="copy_body_territory_firm"
          />
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
          <h3 className="font-display text-2xl text-ink">Mailing address</h3>
          <p className="mt-3 text-inkMuted">{site.mailingAddress}</p>
        </div>
      </section>
    </div>
  );
}
