import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import site from "@/config/site.json";

export const metadata: Metadata = {
  alternates: { canonical: "/onboarding" }
};

export default function OnboardingPage() {
  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Onboarding"
          title="Onboarding"
          description="Activation within 24 hours. If you already paid, submit details below or reply to your confirmation email."
          align="center"
        />
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
          <h2 className="font-display text-2xl text-ink">Submit onboarding details</h2>
          <p className="mt-3 text-sm text-inkMuted">
            This sends your onboarding details through your default email client to {site.ctaEmail}.
          </p>
          <form
            action={`mailto:${site.ctaEmail}`}
            method="post"
            encType="text/plain"
            className="mt-6 grid gap-4"
          >
            <input type="hidden" name="subject" value="Onboarding details" />
            <label className="grid gap-2 text-sm text-inkMuted">
              Company
              <input
                required
                type="text"
                name="company"
                className="rounded-xl border border-black/10 bg-white px-3 py-2 text-ink outline-none focus:border-ocean"
              />
            </label>
            <label className="grid gap-2 text-sm text-inkMuted">
              Contact name
              <input
                required
                type="text"
                name="contact_name"
                className="rounded-xl border border-black/10 bg-white px-3 py-2 text-ink outline-none focus:border-ocean"
              />
            </label>
            <label className="grid gap-2 text-sm text-inkMuted">
              Email
              <input
                required
                type="email"
                name="email"
                className="rounded-xl border border-black/10 bg-white px-3 py-2 text-ink outline-none focus:border-ocean"
              />
            </label>
            <label className="grid gap-2 text-sm text-inkMuted">
              Territory (state/region)
              <input
                required
                type="text"
                name="territory"
                className="rounded-xl border border-black/10 bg-white px-3 py-2 text-ink outline-none focus:border-ocean"
              />
            </label>
            <label className="grid gap-2 text-sm text-inkMuted">
              Recipients (comma-separated)
              <input
                required
                type="text"
                name="recipients"
                className="rounded-xl border border-black/10 bg-white px-3 py-2 text-ink outline-none focus:border-ocean"
              />
            </label>
            <label className="grid gap-2 text-sm text-inkMuted">
              Notes
              <textarea
                name="notes"
                rows={5}
                className="rounded-xl border border-black/10 bg-white px-3 py-2 text-ink outline-none focus:border-ocean"
              />
            </label>
            <div className="pt-2">
              <button
                type="submit"
                className="inline-flex items-center justify-center rounded-full bg-ocean px-5 py-2 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark"
              >
                Send onboarding details
              </button>
            </div>
          </form>
        </div>
      </section>
    </div>
  );
}
