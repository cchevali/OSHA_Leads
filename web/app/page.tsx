import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import CTAButtons from "@/components/CTAButtons";
import SectionHeading from "@/components/SectionHeading";
import site from "@/config/site.json";
import sampleSignals from "./sample/sample_signals.json";

const COVERAGE_HELPER =
  "Counties, cities, metros, or OSHA areas work — we translate coverage for you.";

type SampleSignalRow = {
  activity_nr: string;
  inspection_type: string;
  establishment_name: string;
  city: string;
  state: string;
  opened_date: string | null;
  observed_at_utc: string | null;
  source_url: string;
};

type SampleTerritory = {
  territory_id: string;
  territory_name: string;
  updated_at_utc: string | null;
  rows: SampleSignalRow[];
};

export const metadata: Metadata = {
  title: "OSHA Activity Signals for Safety and Defense Teams",
  description:
    "MicroFlowOps surfaces public OSHA inspection activity with timestamps and territory routing so teams can act before citation timelines compress.",
  alternates: { canonical: "/" }
};

function parseUtc(value: string | null | undefined): Date | null {
  const text = (value || "").trim();
  if (!text) {
    return null;
  }
  const dt = new Date(text);
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function formatUtcStamp(value: string | null | undefined): string {
  const dt = parseUtc(value);
  if (!dt) {
    return "Recent public snapshot";
  }
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZone: "UTC",
    hour12: true
  }).format(dt);
}

function formatOpenedDate(value: string | null | undefined): string {
  const text = (value || "").trim();
  if (!text) {
    return "Unknown";
  }
  const dt = new Date(`${text}T00:00:00Z`);
  if (Number.isNaN(dt.getTime())) {
    return text;
  }
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC"
  }).format(dt);
}

export default function HomePage() {
  const founderBlurb =
    "Built by a data engineer for teams that need earlier visibility into public OSHA activity.";
  const [snapshot] = sampleSignals as SampleTerritory[];
  const snapshotRows = (snapshot?.rows || []).slice(0, 3);
  const leadProofRow = snapshot?.rows?.[0] ?? null;

  return (
    <div className="space-y-20 pb-24 pt-12">
      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="grid gap-10 lg:grid-cols-[1.1fr_0.9fr] lg:items-center">
          <div className="space-y-6">
            <p className="inline-flex items-center rounded-full border border-cardBorder bg-card px-4 py-2 text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">
              Nationwide OSHA enforcement alerts mapped to your footprint
            </p>
            <h1 className="font-display text-4xl text-ink md:text-5xl lg:text-6xl">
              Daily OSHA enforcement signals that surface new inspections before citations post.
            </h1>
            <p className="text-lg text-inkMuted md:text-xl">
              {site.brandName} delivers nationwide OSHA enforcement signal alerts to help safety-facing
              teams prioritize outreach while the window is still open.
            </p>
            <p className="text-sm font-semibold text-inkMuted">{COVERAGE_HELPER}</p>
            <CTAButtons />
            <div className="rounded-2xl border border-cardBorder bg-card p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">
                Founder
              </p>
              <p className="mt-2 text-sm text-inkMuted">{founderBlurb}</p>
              <p className="mt-2 text-sm font-semibold text-inkMuted">Not legal advice.</p>
            </div>
            <p className="text-sm text-inkMuted">
              Core starts at $299/mo for up to 4 metros in billed coverage. {COVERAGE_HELPER}
            </p>
          </div>
          <div className="rounded-3xl border border-cardBorder bg-paper p-6 shadow-soft">
            <div className="space-y-4">
              <div className="flex items-center justify-between text-xs font-semibold text-inkMuted">
                <span>Frozen recent public snapshot</span>
                <span>{formatUtcStamp(snapshot?.updated_at_utc)} UTC</span>
              </div>
              <div className="space-y-3 rounded-2xl bg-card p-4">
                <p className="text-sm font-semibold text-ink">
                  Frozen proof rows from {snapshot?.territory_name || "the latest snapshot"}
                </p>
                {snapshotRows.length > 0 ? (
                  <div className="space-y-3 text-sm text-inkMuted">
                    {snapshotRows.map((row) => (
                      <div key={row.activity_nr} className="rounded-2xl border border-cardBorder bg-surface p-3">
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <p className="text-sm font-semibold text-ink">{row.establishment_name}</p>
                          <p className="text-xs font-semibold text-inkMuted">{row.inspection_type}</p>
                        </div>
                        <p className="mt-1 text-sm text-inkMuted">
                          {row.city}, {row.state} - Opened {formatOpenedDate(row.opened_date)} - Observed{" "}
                          {formatUtcStamp(row.observed_at_utc)} UTC
                        </p>
                      </div>
                    ))}
                  </div>
                ) : (
                  <Image
                    src="/assets/alert-proof-snapshot.svg"
                    alt="Frozen MicroFlowOps alert snapshot"
                    width={1200}
                    height={780}
                    className="w-full rounded-2xl border border-cardBorder bg-white"
                  />
                )}
                <div className="flex items-center justify-between gap-4 pt-2">
                  <p className="text-xs text-inkMuted">
                    Opened and observed timestamps make the public record easy to verify.
                  </p>
                  <Link
                    href="/sample"
                    className="text-sm font-semibold text-ocean underline-offset-4 transition hover:text-oceanDark hover:underline"
                  >
                    View full sample
                  </Link>
                </div>
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
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-2xl text-ink">OSHA defense attorneys</h3>
            <p className="mt-3 text-inkMuted">
              Know about new inspections early, route them by risk, and get in touch before
              citation timelines compress.
            </p>
          </div>
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
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
              title: "Footprint fit",
              body: "Signals are filtered to your footprint, not a national firehose. Counties, cities, metros, or OSHA areas work."
            }
          ].map((item, index) => (
            <div
              key={item.title}
              className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft motion-safe:animate-fade-up"
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
              body: "Send a concise, ranked alert to your team each morning by email."
            }
          ].map((item) => (
            <div key={item.step} className="rounded-3xl border border-cardBorder bg-card p-6">
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
          eyebrow="Proof"
          title="See the alert, then verify the OSHA record."
          description="Show the buyer the commercial value: a ranked alert, a source record, and a quick reason to act."
        />
        <div className="mt-10 grid gap-6 lg:grid-cols-2">
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">
              Real alert screenshot
            </p>
            <Image
              src="/assets/alert-proof-snapshot.svg"
              alt="Frozen MicroFlowOps alert snapshot"
              width={1200}
              height={780}
              className="mt-4 w-full rounded-2xl border border-cardBorder bg-white"
            />
          </div>
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">
              OSHA record verification screenshot
            </p>
            <Image
              src="/assets/osha-record-verification.svg"
              alt="OSHA record verification snapshot"
              width={1200}
              height={780}
              className="mt-4 w-full rounded-2xl border border-cardBorder bg-white"
            />
          </div>
        </div>
        <div className="mt-6 rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <p className="text-sm font-semibold text-ink">Why this was actionable</p>
          <p className="mt-3 text-inkMuted">
            {leadProofRow ? (
              <>
                In this snapshot, {leadProofRow.establishment_name} in {leadProofRow.city}, {leadProofRow.state}
                {" "}shows an opened date of {formatOpenedDate(leadProofRow.opened_date)} and an observed
                {" "}timestamp of {formatUtcStamp(leadProofRow.observed_at_utc)} UTC. A buyer can verify that
                {" "}against the public OSHA record in about 30 seconds.
              </>
            ) : (
              <>
                A buyer can see the alert, click through to the public OSHA record, and confirm the opened
                {" "}date and location in about 30 seconds.
              </>
            )}
          </p>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <SectionHeading
          eyebrow="Territories"
          title="Coverage based on your footprint."
          description="Plans are still metro-based for billing, but counties, cities, metros, or OSHA areas all work as inputs."
        />
        <div className="mt-10 grid gap-6 md:grid-cols-2">
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-xl text-ink">Metro-based billing</h3>
            <p className="mt-3 text-inkMuted">
              Pick a plan based on how many metros you need in billed coverage. Core covers up to 4,
              Multi-Territory up to 10. Daily morning delivery.
            </p>
          </div>
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-xl text-ink">Onboarding translates coverage</h3>
            <p className="mt-3 text-inkMuted">
              {COVERAGE_HELPER} We will not increase billing without explicit approval.
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
        <div className="mt-8 rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <p className="text-sm font-semibold text-ink">Disclaimer</p>
          <ul className="mt-3 space-y-3 text-sm text-inkMuted">
            <li>Not affiliated with OSHA.</li>
            <li>Uses public enforcement data; freshness varies.</li>
            <li>Business contact only; opt-out honored.</li>
            <li>No legal advice. Alerts are informational signals only.</li>
            <li>Deadlines are included only when the public record supports them.</li>
          </ul>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="rounded-3xl bg-inkFixed px-8 py-10 text-white shadow-soft">
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-white/70">
                Ready to see signals
              </p>
              <h2 className="mt-3 font-display text-3xl">Request a trial feed for your footprint.</h2>
              <p className="mt-3 text-white/70">
                We will send a sample alert, map your coverage, and set up a short trial feed so you can
                evaluate signal quality.
              </p>
              <p className="mt-3 text-sm font-semibold text-white/80">{COVERAGE_HELPER}</p>
              <div className="mt-4 rounded-2xl border border-white/15 bg-white/5 p-4">
                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-white/70">
                  Verify in 30 seconds
                </p>
                <p className="mt-2 text-sm text-white/80">
                  Every item includes opened/observed timestamps and a direct link to the public OSHA
                  record.
                </p>
              </div>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
