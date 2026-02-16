import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import CTAButtons from "@/components/CTAButtons";

export const metadata: Metadata = {
  alternates: { canonical: "/sample" }
};

export default function SamplePage() {
  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Sample"
          title="See a real OSHA Activity Signals alert."
          description="This is a realistic preview using dummy data. Each morning, you get a brief like this filtered to your territory."
          align="center"
        />
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">
            Daily digest preview
          </p>
          <h3 className="mt-2 font-display text-2xl text-ink">
            Example Territory — Morning Brief
          </h3>
          <p className="mt-2 text-sm text-inkMuted">
            Cropped from a real digest render showing the header, scored signals, and compliance
            footer. All data below is dummy.
          </p>
          <div className="mt-6 flex justify-center">
            <img
              src="/assets/sample-digest-preview.png"
              alt="Sample OSHA Lead Digest preview (dummy data)"
              className="w-full max-w-[480px] rounded-2xl border border-cardBorder bg-card shadow-soft"
              loading="lazy"
            />
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl bg-ink px-8 py-10 text-white shadow-soft">
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <h2 className="font-display text-3xl">Want this in your inbox?</h2>
              <p className="mt-3 text-white/70">
                Request a trial feed and we will tailor the signals to your territory.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
