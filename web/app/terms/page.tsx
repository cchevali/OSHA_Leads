import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import site from "@/config/site.json";

export const metadata: Metadata = {
  alternates: { canonical: "/terms" }
};

export default function TermsPage() {
  return (
    <div className="space-y-12 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Terms"
          title="Terms of service"
          description="Last updated February 5, 2026."
          align="center"
        />
      </section>

      <section className="mx-auto w-full max-w-4xl px-6">
        <div className="space-y-6 rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
          <p className="text-sm text-inkMuted">
            By using {site.legalName || site.brandName} services, you agree to these terms.
          </p>
          <div className="space-y-3 text-sm text-inkMuted">
            <p className="font-semibold text-ink">Service scope</p>
            <p>
              We provide informational alerts based on public OSHA activity. We do not provide legal
              advice and are not affiliated with OSHA or any government agency.
            </p>
          </div>
          <div className="space-y-3 text-sm text-inkMuted">
            <p className="font-semibold text-ink">User responsibilities</p>
            <p>
              Users are responsible for verifying information and complying with all applicable laws
              when contacting prospects.
            </p>
          </div>
          <div className="space-y-3 text-sm text-inkMuted">
            <p className="font-semibold text-ink">Delivery and availability</p>
            <p>
              We strive for daily delivery but cannot guarantee uninterrupted availability.
            </p>
          </div>
          <div className="space-y-3 text-sm text-inkMuted">
            <p className="font-semibold text-ink">Contact</p>
            <p>
              Questions about these terms can be sent to {site.ctaEmail}.
            </p>
          </div>
        </div>
      </section>
    </div>
  );
}
