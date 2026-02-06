import type { Metadata } from "next";
import CTAButtons from "@/components/CTAButtons";
import SectionHeading from "@/components/SectionHeading";
import site from "@/config/site.json";

export const metadata: Metadata = {
  alternates: { canonical: "/" }
};

export default function HomePage() {
  return (
    <div className="space-y-20 pb-24 pt-12">
      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="grid gap-10 lg:grid-cols-[1.1fr_0.9fr] lg:items-center">
          <div className="space-y-6">
            <p className="inline-flex items-center rounded-full border border-black/10 bg-white/70 px-4 py-2 text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">
              OSHA activity intelligence
            </p>
            <h1 className="font-display text-4xl text-ink md:text-5xl lg:text-6xl">
              Daily OSHA activity signals that surface new inspections before citations post.
            </h1>
            <p className="text-lg text-inkMuted md:text-xl">
              {site.brandName} delivers early, territory-specific signals to help OSHA defense
              teams and safety consultants prioritize outreach while the window is still open.
            </p>
            <CTAButtons />
            <p className="text-sm text-inkMuted">
              Built for the Texas Triangle. Expand to new territories in days, not weeks.
            </p>
          </div>
          <div className="bg-paper rounded-3xl border border-black/10 p-6 shadow-soft">
            <div className="space-y-4">
              <div className="flex items-center justify-between text-xs font-semibold text-inkMuted">
                <span>OSHA Activity Signals - Texas Triangle</span>
                <span>Daily 7:00 AM CT</span>
              </div>
              <div className="space-y-3 rounded-2xl bg-white/80 p-4">
                <p className="text-sm font-semibold text-ink">
                  Priority signals (sample)
                </p>
                <div className="space-y-2 text-sm text-inkMuted">
                  <p>High - Accident - Houston, TX - Opened: 2026-01-29</p>
                  <p>Medium - Complaint - Dallas, TX - Opened: 2026-01-27</p>
                  <p>Medium - Referral - Austin, TX - Opened: 2026-01-26</p>
                </div>
              </div>
              <div className="rounded-2xl border border-dashed border-black/20 bg-white/60 p-5 text-sm text-inkMuted">
                Screenshot slot for customer-approved sample alert
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <SectionHeading
          eyebrow="Who it is for"
          title="Built for teams who live inside OSHA timelines."
          description="We focus on employer-side defense and safety consulting, where early awareness changes the odds of winning the relationship."
        />
        <div className="mt-10 grid gap-6 md:grid-cols-2">
          <div className="rounded-3xl border border-black/10 bg-white/80 p-6 shadow-soft">
            <h3 className="font-display text-2xl text-ink">OSHA defense attorneys</h3>
            <p className="mt-3 text-inkMuted">
              Know about new inspections early, route them by risk, and get in touch before
              citation timelines compress.
            </p>
          </div>
          <div className="rounded-3xl border border-black/10 bg-white/80 p-6 shadow-soft">
            <h3 className="font-display text-2xl text-ink">Safety consultants</h3>
            <p className="mt-3 text-inkMuted">
              Spot new activity in your patch, prioritize the highest intent signals, and win
              work while the need is urgent.
            </p>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <SectionHeading
          eyebrow="Why pay"
          title="Timing and relevance are the whole game."
          description="We do the filtering so your team only sees activity that is both timely and likely to convert."
        />
        <div className="mt-10 grid gap-6 md:grid-cols-3">
          {[
            {
              title: "Early visibility",
              body: "Inspections appear before citations. We surface activity as soon as it becomes observable."
            },
            {
              title: "Signal over noise",
              body: "Every alert is scored for urgency, scope, and commercial intent."
            },
            {
              title: "Territory fit",
              body: "Signals are filtered by your states and metro areas, not national firehoses."
            }
          ].map((item, index) => (
            <div
              key={item.title}
              className="rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft motion-safe:animate-fade-up"
              style={{ animationDelay: `${index * 120}ms` }}
            >
              <h3 className="font-display text-xl text-ink">{item.title}</h3>
              <p className="mt-3 text-inkMuted">{item.body}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <SectionHeading
          eyebrow="How it works"
          title="A daily pipeline that never misses the window."
          description="We transform public OSHA activity into a short, ranked brief you can act on immediately."
        />
        <div className="mt-10 grid gap-6 lg:grid-cols-4">
          {[
            {
              step: "01",
              title: "Signals",
              body: "Track new inspections and activity updates the moment they appear."
            },
            {
              step: "02",
              title: "Enrich",
              body: "Add industry, location, severity, and history context."
            },
            {
              step: "03",
              title: "Score",
              body: "Rank by urgency and commercial intent."
            },
            {
              step: "04",
              title: "Deliver",
              body: "Send a clean email or SMS alert to your team each morning."
            }
          ].map((item) => (
            <div key={item.step} className="rounded-3xl border border-black/10 bg-white/85 p-6">
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">
                {item.step}
              </p>
              <h3 className="mt-3 font-display text-xl text-ink">{item.title}</h3>
              <p className="mt-3 text-inkMuted">{item.body}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <SectionHeading
          eyebrow="Territories"
          title="Subscribe by territory, scale by intent."
          description="Start with the Texas Triangle and add new territories as your coverage grows."
        />
        <div className="mt-10 grid gap-6 md:grid-cols-2">
          <div className="rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
            <h3 className="font-display text-xl text-ink">Texas Triangle</h3>
            <p className="mt-3 text-inkMuted">
              Dallas-Fort Worth, Houston, Austin, and San Antonio. Daily signals at 7:00 AM CT.
            </p>
          </div>
          <div className="rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
            <h3 className="font-display text-xl text-ink">Custom territories</h3>
            <p className="mt-3 text-inkMuted">
              Add any multi-state or metro-focused territory. We configure filters and alert
              cadence to match your team.
            </p>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <SectionHeading
          eyebrow="Compliance"
          title="Clear boundaries, clean data handling."
          description="We keep the service useful without crossing legal or privacy lines."
        />
        <div className="mt-8 rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
          <ul className="space-y-3 text-sm text-inkMuted">
            <li>No legal advice. Alerts are informational signals only.</li>
            <li>Deadlines are included only when the public record supports them.</li>
            <li>Business contact data only. No personal contact scraping.</li>
            <li>Unsubscribe requests are honored immediately.</li>
            <li>Not affiliated with OSHA or any government agency.</li>
          </ul>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="rounded-3xl bg-ink px-8 py-10 text-white shadow-soft">
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-white/70">
                Ready to see signals
              </p>
              <h2 className="mt-3 font-display text-3xl">Let us set up your territory.</h2>
              <p className="mt-3 text-white/70">
                We will send a sample alert and configure your subscriber key in under 48 hours.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
