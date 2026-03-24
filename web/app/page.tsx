import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import CTAButtons from "@/components/CTAButtons";
import SectionHeading from "@/components/SectionHeading";
import site from "@/config/site.json";
import sampleSignals from "./sample/sample_signals.json";

const COVERAGE_HELPER =
  "Send counties, cities, metros, or OSHA areas.";

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
    "Built by a data engineer for earlier visibility into public OSHA activity.";
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
              Daily OSHA inspection signals before citations post.
            </h1>
            <p className="text-lg text-inkMuted md:text-xl">
              {site.brandName} surfaces public OSHA inspection activity with timestamps and routing for
              safety-facing teams.
            </p>
            <p className="text-sm font-semibold text-inkMuted">
              Core starts at $299/mo for up to 4 billed metros. {COVERAGE_HELPER}
            </p>
            <CTAButtons />
            <div className="grid gap-3 text-sm font-semibold text-ink md:grid-cols-3">
              <Link href="/sample" className="rounded-2xl border border-cardBorder bg-card px-4 py-3">
                Frozen public sample
              </Link>
              <Link href="/sample" className="rounded-2xl border border-cardBorder bg-card px-4 py-3">
                Verify in 30 seconds
              </Link>
              <Link href="/pricing" className="rounded-2xl border border-cardBorder bg-card px-4 py-3">
                We confirm mapping before billing
              </Link>
            </div>
            <div className="rounded-2xl border border-cardBorder bg-card p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">
                Founder
              </p>
              <p className="mt-2 text-sm text-inkMuted">{founderBlurb} Not legal advice.</p>
            </div>
          </div>
          <div className="rounded-3xl border border-cardBorder bg-paper p-6 shadow-soft">
            <div className="space-y-4">
              <div className="flex items-center justify-between text-xs font-semibold text-inkMuted">
                <span>Recent public snapshot</span>
                <span>{formatUtcStamp(snapshot?.updated_at_utc)} UTC</span>
              </div>
              <div className="space-y-3 rounded-2xl bg-card p-4">
                <p className="text-sm font-semibold text-ink">
                  Proof rows from {snapshot?.territory_name || "the latest snapshot"}
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
                    Opened and observed timestamps link back to the public OSHA record.
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
          description="Employer-side defense and safety teams use early awareness to act while the need is still fresh."
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
          description="We filter the feed so your team gets timely signals that still deserve attention."
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
              body: "Signals are filtered to the footprint your team actually covers, not a national firehose."
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
          description="Public OSHA activity becomes a short, ranked brief your team can act on the same morning."
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
          title="See the alert, then check the public record."
          description="The value is simple: a ranked alert, a source record, and a concrete reason to move."
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
          <p className="text-sm font-semibold text-ink">What a buyer can confirm</p>
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
          title="Coverage mapped to your footprint."
          description="Plans bill by metro count, but onboarding maps the footprint you already use."
        />
        <div className="mt-10 grid gap-6 md:grid-cols-2">
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-xl text-ink">Metro-based billing</h3>
            <p className="mt-3 text-inkMuted">
              Core covers up to 4 billed metros. Multi-Territory covers up to 10, with daily morning
              delivery.
            </p>
          </div>
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-xl text-ink">Coverage inputs</h3>
            <p className="mt-3 text-inkMuted">
              Share the footprint labels your team already uses and we map them during onboarding.
            </p>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <SectionHeading
          eyebrow="Compliance"
          title="Clear boundaries, clean data handling."
          description="Useful operational signals, with clear limits."
        />
        <div className="mt-8 rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <ul className="space-y-3 text-sm text-inkMuted">
            <li>Not affiliated with OSHA.</li>
            <li>Uses public enforcement data; freshness varies.</li>
            <li>Business contact only; opt-out honored.</li>
            <li>Deadlines appear only when the public record supports them.</li>
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
                We will send a sample alert, map your footprint, and start a short trial feed so you can
                judge signal quality for yourself.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
