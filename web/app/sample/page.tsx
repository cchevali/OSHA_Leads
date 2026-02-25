import type { Metadata } from "next";
import Link from "next/link";
import CTAButtons from "@/components/CTAButtons";
import sampleSignals from "./sample_signals.json";

export const metadata: Metadata = {
  title: "Live OSHA Sample Feed",
  description:
    "Live sample feed using real public OSHA inspection data across multiple territories, with opened/observed timestamps and source links.",
  alternates: { canonical: "/sample" }
};

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

const ABOVE_FOLD_TERRITORY_LIMIT = 3;
const STALE_AFTER_MS = 7 * 24 * 60 * 60 * 1000;
const FOUNDER_BLURB =
  "I'm Chase. I built MicroFlowOps to surface public OSHA inspection activity faster than teams can find it manually. My background is data engineering, not law, so the product focuses on monitoring, timestamps, and territory routing.";

function parseUtc(value: string | null | undefined): Date | null {
  const text = (value || "").trim();
  if (!text) {
    return null;
  }
  const dt = new Date(text);
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function formatDateShort(value: string | null | undefined): string {
  const dt = parseUtc(value);
  if (!dt) {
    return "";
  }
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    timeZone: "UTC"
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

function formatUpdatedLabel(updatedAtUtc: string | null, nowMs: number): {
  label: string;
  stale: boolean;
} {
  const dt = parseUtc(updatedAtUtc);
  if (!dt) {
    return { label: "Recent OSHA activity sample", stale: true };
  }
  const stale = nowMs - dt.getTime() > STALE_AFTER_MS;
  if (stale) {
    return { label: "Recent OSHA activity sample", stale: true };
  }
  const stamp = new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZone: "UTC",
    hour12: true
  }).format(dt);
  return { label: `Updated ${stamp} UTC`, stale: false };
}

function SignalRowCard({ row }: { row: SampleSignalRow }) {
  return (
    <article className="rounded-2xl border border-cardBorder bg-surface p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="rounded-full border border-cardBorder bg-card px-2.5 py-1 text-xs font-semibold text-ink">
          {row.inspection_type}
        </p>
        <p className="text-xs font-semibold text-inkMuted">
          Observed {row.observed_at_utc ? formatDateShort(row.observed_at_utc) : "Unknown"}
        </p>
      </div>
      <h3 className="mt-3 text-base font-semibold text-ink">{row.establishment_name}</h3>
      <p className="mt-2 text-sm text-inkMuted">
        {row.city || "Unknown city"}, {row.state || "US"}
      </p>
      <p className="mt-2 text-sm font-semibold text-ink">
        Opened: {formatOpenedDate(row.opened_date)}
      </p>
      <div className="mt-3">
        <a
          href={row.source_url}
          target="_blank"
          rel="noopener noreferrer"
          className="text-sm font-semibold text-ocean underline-offset-4 transition hover:text-oceanDark hover:underline"
        >
          View OSHA record
        </a>
      </div>
    </article>
  );
}

export default function SamplePage() {
  const typedTerritories = sampleSignals as SampleTerritory[];
  const visibleTerritories = typedTerritories.slice(0, ABOVE_FOLD_TERRITORY_LIMIT);
  const hiddenTerritories = typedTerritories.slice(ABOVE_FOLD_TERRITORY_LIMIT);
  const nowMs = Date.now();
  const allVisibleStale = visibleTerritories.every((territory) =>
    formatUpdatedLabel(territory.updated_at_utc, nowMs).stale
  );

  return (
    <div className="space-y-12 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <div className="space-y-4 text-center">
          <h1 className="font-display text-4xl text-ink md:text-5xl">
            Sample: Live OSHA Activity Feed
          </h1>
          <p className="text-base text-inkMuted md:text-lg">
            Nationwide sample: multiple metros. Real public OSHA inspection activity, refreshed into a
            committed preview feed.
          </p>
          <p className="mx-auto max-w-3xl text-sm text-inkMuted">{FOUNDER_BLURB}</p>
          <p className="text-sm font-semibold text-inkMuted">Not legal advice.</p>
          <p className="text-sm text-inkMuted">
            Not affiliated with OSHA or any government agency.
          </p>
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="bg-paper rounded-3xl border border-cardBorder p-6 shadow-soft">
          <div className="flex flex-wrap items-center justify-between gap-3 text-xs font-semibold text-inkMuted">
            <span>Live public sample feed</span>
            <span>Opened + observed timestamps included</span>
          </div>
          <div className="mt-4 rounded-2xl bg-card p-4">
            <div className="rounded-2xl border border-cardBorder bg-surface p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">
                Verify in 30 seconds
              </p>
              <p className="mt-2 text-sm text-inkMuted">
                Every item includes opened/observed timestamps and a direct link to the public OSHA record.
              </p>
            </div>

            {allVisibleStale ? (
              <p className="mt-4 text-xs font-semibold text-inkMuted">Sample refresh is delayed.</p>
            ) : null}

            <div className="mt-4 space-y-6">
              {visibleTerritories.map((territory) => {
                const freshness = formatUpdatedLabel(territory.updated_at_utc, nowMs);
                return (
                  <section
                    key={territory.territory_id}
                    className="rounded-2xl border border-cardBorder bg-surface p-4"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <div>
                        <h2 className="text-base font-semibold text-ink">{territory.territory_name}</h2>
                        <p className="text-xs font-semibold text-inkMuted">{freshness.label}</p>
                      </div>
                      <p className="text-xs font-semibold text-inkMuted">
                        {territory.rows.length} item{territory.rows.length === 1 ? "" : "s"}
                      </p>
                    </div>

                    {territory.rows.length > 0 ? (
                      <div className="mt-4 grid gap-4 md:grid-cols-2">
                        {territory.rows.map((row) => (
                          <SignalRowCard key={`${territory.territory_id}-${row.activity_nr}`} row={row} />
                        ))}
                      </div>
                    ) : (
                      <p className="mt-4 text-sm text-inkMuted">
                        No current rows in this sample snapshot for {territory.territory_name}.
                      </p>
                    )}
                  </section>
                );
              })}
            </div>

            <div className="mt-6 flex flex-wrap items-center gap-4">
              <CTAButtons />
              <Link
                href="/contact"
                className="text-sm font-semibold text-inkMuted underline-offset-4 transition hover:text-ink hover:underline"
              >
                Contact
              </Link>
            </div>

            <div className="mt-4 rounded-2xl border border-cardBorder bg-surface p-4">
              <p className="text-sm font-semibold text-ink">See a live sample feed (real public data)</p>
              <p className="mt-2 text-sm text-inkMuted">
                <a
                  href="https://microflowops.com/sample"
                  className="font-semibold text-ocean underline-offset-4 transition hover:text-oceanDark hover:underline"
                >
                  https://microflowops.com/sample
                </a>
              </p>
              <p className="mt-2 text-sm text-inkMuted">
                If you want daily coverage for your metros, reply with the cities you care about and
                I&apos;ll set up a trial.
              </p>
            </div>

            {hiddenTerritories.length > 0 ? (
              <details className="mt-6">
                <summary className="cursor-pointer text-sm font-semibold text-ink">
                  View additional territories
                </summary>
                <div className="mt-4 space-y-6">
                  {hiddenTerritories.map((territory) => {
                    const freshness = formatUpdatedLabel(territory.updated_at_utc, nowMs);
                    return (
                      <section
                        key={territory.territory_id}
                        className="rounded-2xl border border-cardBorder bg-surface p-4"
                      >
                        <div className="flex flex-wrap items-center justify-between gap-3">
                          <h2 className="text-base font-semibold text-ink">{territory.territory_name}</h2>
                          <p className="text-xs font-semibold text-inkMuted">{freshness.label}</p>
                        </div>
                        {territory.rows.length > 0 ? (
                          <div className="mt-4 grid gap-4 md:grid-cols-2">
                            {territory.rows.map((row) => (
                              <SignalRowCard
                                key={`${territory.territory_id}-${row.activity_nr}`}
                                row={row}
                              />
                            ))}
                          </div>
                        ) : (
                          <p className="mt-4 text-sm text-inkMuted">
                            No current rows in this sample snapshot for {territory.territory_name}.
                          </p>
                        )}
                      </section>
                    );
                  })}
                </div>
              </details>
            ) : null}
          </div>
        </div>
      </section>
    </div>
  );
}
