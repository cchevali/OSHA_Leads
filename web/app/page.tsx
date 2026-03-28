import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import CTAButtons from "@/components/CTAButtons";
import SectionHeading from "@/components/SectionHeading";
import site from "@/config/site.json";
import sampleSignals from "./sample/sample_signals.json";

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
  title: "Outreach-Ready OSHA Leads for Safety Consulting Firms",
  description:
    "Outreach-ready OSHA leads for safety consulting firms. See newly observed public OSHA activity before citations post, packaged for business development by state or region.",
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
  const [snapshot] = sampleSignals as SampleTerritory[];
  const snapshotRows = (snapshot?.rows || []).slice(0, 3);
  const leadProofRow = snapshot?.rows?.[0] ?? null;

  return (
    <div className="space-y-16 pb-24 pt-12">
      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="grid gap-10 lg:grid-cols-[1.1fr_0.9fr] lg:items-center">
          <div className="space-y-6">
            <p className="inline-flex items-center rounded-full border border-cardBorder bg-card px-4 py-2 text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">
              Outreach-ready OSHA leads for safety consulting firms
            </p>
            <h1 className="font-display text-4xl text-ink md:text-5xl lg:text-6xl">
              See newly observed public OSHA activity before citations post.
            </h1>
            <p className="text-lg text-inkMuted md:text-xl">
              {site.brandName} packages public OSHA activity into usable daily leads for firms doing
              outbound or business development with employers in a state or region.
            </p>
            <p className="text-sm font-semibold text-inkMuted">
              Sample = one example digest for your state or region.
            </p>
            <p className="text-sm font-semibold text-inkMuted">
              Founding Pilot: $149 for 30 days in one state. Standard and Multi-Territory remain
              available for ongoing coverage.
            </p>
            <p className="text-sm font-semibold text-inkMuted">Need live proof? Ask about a 14-day trial.</p>
            <CTAButtons />
            <div className="grid gap-3 text-sm font-semibold text-ink md:grid-cols-3">
              <div className="rounded-2xl border border-cardBorder bg-card px-4 py-3">
                Best for outbound teams already selling safety services
              </div>
              <div className="rounded-2xl border border-cardBorder bg-card px-4 py-3">
                Leads packaged for fast follow-up in your state or region
              </div>
              <div className="rounded-2xl border border-cardBorder bg-card px-4 py-3">
                Public-source verification included on every sample
              </div>
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
                  Sample rows from {snapshot?.territory_name || "the latest snapshot"}
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
                    Every sample item links back to the public OSHA record so a buyer can verify it quickly.
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
          eyebrow="Who It Is For"
          title="Built for safety consulting and training firms that already do outbound."
          description="The goal is simple: identify employers in your state or region that may need help now, then follow up while the signal is still fresh."
        />
        <div className="mt-10 grid gap-6 md:grid-cols-2">
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-2xl text-ink">Best fit</h3>
            <p className="mt-3 text-inkMuted">
              Best for safety consulting and training firms that already do outbound or business
              development.
            </p>
          </div>
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-2xl text-ink">Not ideal for</h3>
            <p className="mt-3 text-inkMuted">
              Less useful for teams looking for a full compliance workflow or teams not doing outreach.
            </p>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <SectionHeading
          eyebrow="What You Get"
          title="A short daily digest your team can review fast."
          description="Enough detail to decide whether a lead is worth follow-up, without turning the homepage into a long workflow explanation."
        />
        <div className="mt-10 grid gap-6 lg:grid-cols-2">
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-2xl text-ink">What is in the digest</h3>
            <ul className="mt-4 space-y-3 text-sm text-inkMuted">
              <li>Company name</li>
              <li>City and state</li>
              <li>Inspection or signal type</li>
              <li>Observed date</li>
              <li>Public source verification</li>
              <li>When available: website, phone, and public contact details</li>
            </ul>
          </div>
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <h3 className="font-display text-2xl text-ink">What &quot;usable&quot; means</h3>
            <p className="mt-3 text-inkMuted">
              The lead should be clear enough that a business development team can review it, verify the public
              source, and decide whether to reach out without building a long research memo first.
            </p>
            <p className="mt-3 text-inkMuted">
              Verify in 30 seconds.{" "}
              {leadProofRow ? (
                <>
                  {leadProofRow.establishment_name} in {leadProofRow.city}, {leadProofRow.state} shows an opened
                  date of {formatOpenedDate(leadProofRow.opened_date)} and an observed timestamp of{" "}
                  {formatUtcStamp(leadProofRow.observed_at_utc)} UTC on the public OSHA record.
                </>
              ) : (
                <>Each sample links back to the public OSHA record so your team can confirm the lead quickly.</>
              )}
            </p>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">Boundaries</p>
          <ul className="mt-4 space-y-3 text-sm text-inkMuted">
            <li>Not affiliated with OSHA.</li>
            <li>Uses public OSHA activity data; freshness varies by source visibility.</li>
            <li>Built for outreach-ready lead review, not a full compliance workflow.</li>
            <li>Business contact only; opt-out honored.</li>
          </ul>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="rounded-3xl bg-inkFixed px-8 py-10 text-white shadow-soft">
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-white/70">
                Start with a sample
              </p>
              <h2 className="mt-3 font-display text-3xl">Request a sample for your state or region.</h2>
              <p className="mt-3 text-white/70">
                We will send a sample first, then confirm whether Founding Pilot, Standard, or
                Multi-Territory is the right next step.
              </p>
            </div>
            <CTAButtons variant="dark" />
          </div>
        </div>
      </section>
    </div>
  );
}
