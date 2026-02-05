import SectionHeading from "@/components/SectionHeading";
import CTAButtons from "@/components/CTAButtons";

const sampleSignals = [
  {
    priority: "High",
    company: "Pedco Roofing, Inc.",
    city: "Houston, TX",
    type: "Accident",
    opened: "2026-01-29",
    observed: "2026-02-03"
  },
  {
    priority: "Medium",
    company: "Mmm Welders and Assemblers LLC",
    city: "Mansfield, TX",
    type: "Referral",
    opened: "2026-01-22",
    observed: "2026-01-30"
  },
  {
    priority: "Medium",
    company: "Pyramid Waterproofing, Inc.",
    city: "Houston, TX",
    type: "Complaint",
    opened: "2026-01-27",
    observed: "2026-01-30"
  },
  {
    priority: "High",
    company: "STI LLC",
    city: "Tilden, TX",
    type: "Accident",
    opened: "2026-01-26",
    observed: "2026-01-30"
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
              <h3 className="font-display text-2xl text-ink">Texas Triangle - Daily Brief</h3>
            </div>
            <p className="text-sm text-inkMuted">Delivered 7:00 AM CT</p>
          </div>
          <div className="mt-6 space-y-4">
            <p className="text-sm text-inkMuted">
              Priority is a heuristic based on severity, recency, and signal type. This is not legal
              advice.
            </p>
            <div className="space-y-4">
              {sampleSignals.map((signal) => (
                <div key={signal.company} className="rounded-2xl border border-black/10 bg-white/80 p-4">
                  <p className="text-sm font-semibold text-ink">
                    {signal.priority} priority - {signal.company}
                  </p>
                  <p className="mt-2 text-sm text-inkMuted">
                    {signal.type} - {signal.city} - Opened: {signal.opened} - Observed: {signal.observed}
                  </p>
                </div>
              ))}
            </div>
            <p className="text-xs text-inkMuted">
              Deadlines are included only when the public record supports them. Unsubscribe honored.
            </p>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-dashed border-black/20 bg-white/70 p-10 text-center text-sm text-inkMuted">
          Screenshot slot for an approved customer alert preview
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl bg-ink px-8 py-10 text-white shadow-soft">
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <h2 className="font-display text-3xl">Want this in your inbox?</h2>
              <p className="mt-3 text-white/70">
                We can tailor the signals to your territory and send a sample alert today.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
