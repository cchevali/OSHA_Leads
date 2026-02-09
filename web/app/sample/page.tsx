import type { Metadata } from "next";
import SectionHeading from "@/components/SectionHeading";
import CTAButtons from "@/components/CTAButtons";

export const metadata: Metadata = {
  alternates: { canonical: "/sample" }
};

const sampleSignals = [
  {
    severityTier: "High",
    company: "Northstar Scaffolding Co.",
    location: "Example City, ST",
    industry: "Construction",
    naics: "238990",
    penaltyRange: "$15k-$55k",
    signalType: "Accident",
    observedDate: "2026-02-03",
    postedDate: "2026-02-05"
  },
  {
    severityTier: "Medium",
    company: "Riverbend Metal Fab LLC",
    location: "Example City, ST",
    industry: "Fabricated Metal Products",
    naics: "332322",
    penaltyRange: "$5k-$25k",
    signalType: "Complaint",
    observedDate: "2026-02-01",
    postedDate: "2026-02-04"
  },
  {
    severityTier: "Low",
    company: "Clearview Logistics Inc.",
    location: "Example City, ST",
    industry: "General Freight Trucking",
    naics: "484110",
    penaltyRange: "$0-$15k",
    signalType: "Referral",
    observedDate: "2026-01-30",
    postedDate: "2026-02-02"
  },
  {
    severityTier: "High",
    company: "Summit Cold Storage Partners",
    location: "Example City, ST",
    industry: "Refrigerated Warehousing",
    naics: "493120",
    penaltyRange: "$25k-$110k",
    signalType: "Accident",
    observedDate: "2026-01-29",
    postedDate: "2026-02-01"
  }
];

export default function SamplePage() {
  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <SectionHeading
          eyebrow="Sample"
          title="See a real-world OSHA Activity Signals alert."
          description="This is a realistic example using recent-style dummy data. We only include the most relevant signals."
          align="center"
        />
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-black/10 bg-white/90 p-6 shadow-soft">
          <div className="flex flex-col gap-4 border-b border-black/10 pb-4 md:flex-row md:items-center md:justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">
                OSHA Activity Signals
              </p>
              <h3 className="font-display text-2xl text-ink">Example Territory - Daily Brief</h3>
            </div>
            <p className="text-sm text-inkMuted">Delivered each morning</p>
          </div>
          <div className="mt-6 space-y-4">
            <p className="text-sm text-inkMuted">
              Severity tier is a heuristic based on severity, recency, and signal type. This is not legal advice.
            </p>
            <div className="space-y-4">
              {sampleSignals.map((signal) => (
                <div
                  key={signal.company}
                  className="rounded-2xl border border-black/10 bg-white/80 p-4"
                >
                  <p className="text-sm font-semibold text-ink">
                    {signal.severityTier} severity - {signal.company}
                  </p>
                  <p className="mt-2 text-sm text-inkMuted">
                    {signal.signalType} - {signal.location} - {signal.industry} (NAICS {signal.naics}) - Penalty{" "}
                    {signal.penaltyRange}
                  </p>
                  <p className="mt-2 text-sm text-inkMuted">
                    Observed: {signal.observedDate} - Posted: {signal.postedDate}
                  </p>
                </div>
              ))}
            </div>
            <div className="rounded-2xl border border-black/10 bg-white/70 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">Disclaimer</p>
              <ul className="mt-2 space-y-2 text-xs text-inkMuted">
                <li>Not affiliated with OSHA.</li>
                <li>Uses public enforcement data; freshness varies.</li>
                <li>Business contact only; opt-out honored.</li>
              </ul>
            </div>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-black/10 bg-white/90 p-6 shadow-soft">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">Sample format</p>
          <h3 className="mt-2 font-display text-2xl text-ink">Digest preview (dummy data)</h3>
          <p className="mt-2 text-sm text-inkMuted">
            Cropped from a real digest render to show the header, tier summary, preferences, signals, and compliance
            footer.
          </p>
          <div className="mt-6 flex justify-center">
            {/* Plain <img> keeps this page simple and avoids extra Next/Image config. */}
            <img
              src="/assets/sample-digest-preview.png"
              alt="Sample OSHA Lead Digest preview (dummy data)"
              className="w-full max-w-[480px] rounded-2xl border border-black/10 bg-white shadow-soft"
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
                We can tailor the signals to your territory and start a trial feed.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
