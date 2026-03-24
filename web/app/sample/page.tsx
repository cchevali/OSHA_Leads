import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import CTAButtons from "@/components/CTAButtons";
import sampleSignals from "./sample_signals.json";

export const metadata: Metadata = {
  title: "Sample OSHA Alert Snapshot",
  description:
    "Frozen public sample showing MicroFlowOps alert proof, public OSHA record verification, and recent committed rows.",
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

function SignalRowCard({ row }: { row: SampleSignalRow }) {
  return (
    <article className="rounded-2xl border border-cardBorder bg-surface p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="rounded-full border border-cardBorder bg-card px-2.5 py-1 text-xs font-semibold text-ink">
          {row.inspection_type}
        </p>
        <p className="text-xs font-semibold text-inkMuted">
          Observed {formatUtcStamp(row.observed_at_utc)} UTC
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
  const [snapshot] = sampleSignals as SampleTerritory[];
  const rows = snapshot?.rows ?? [];

  return (
    <div className="space-y-12 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <div className="space-y-4 text-center">
          <h1 className="font-display text-4xl text-ink md:text-5xl">
            Sample: alert and OSHA record
          </h1>
          <p className="text-base text-inkMuted md:text-lg">
            A fixed public example showing the alert format, the opened date, the observed timestamp,
            and the matching OSHA record.
          </p>
          <p className="mx-auto max-w-3xl text-sm text-inkMuted">
            Public example using OSHA data. Not affiliated with OSHA. Not legal advice.
          </p>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-paper p-6 shadow-soft">
          <div className="flex flex-wrap items-center justify-between gap-3 text-xs font-semibold text-inkMuted">
            <span>Sample alert and OSHA record</span>
            <span>Public example</span>
          </div>
          <div className="mt-4 grid gap-6 lg:grid-cols-2">
            <div className="rounded-2xl border border-cardBorder bg-surface p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">
                Alert screenshot
              </p>
              <p className="mt-2 text-sm text-inkMuted">
                Recent MicroFlowOps alert format with ranked rows and timestamps.
              </p>
              <Image
                src="/assets/alert-proof-snapshot.svg"
                alt="Frozen MicroFlowOps alert snapshot"
                width={1200}
                height={780}
                className="mt-4 w-full rounded-2xl border border-cardBorder bg-white"
              />
            </div>
            <div className="rounded-2xl border border-cardBorder bg-surface p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">
                Verify in 30 seconds
              </p>
              <p className="mt-2 text-sm text-inkMuted">
                Open the source record and confirm the signal quickly.
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
          <div className="mt-6 rounded-2xl border border-cardBorder bg-surface p-5">
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">
              What a buyer can confirm
            </p>
            <p className="mt-2 text-sm text-inkMuted">
              In this example, Rhino Specialty Services in Midland, Texas opened on March 3, 2026 and
              was observed on March 6, 2026. A buyer can confirm both details on the public OSHA
              record before deciding whether to route the alert for outreach.
            </p>
          </div>
        </div>
      </section>

      {snapshot && rows.length > 0 ? (
        <section className="mx-auto w-full max-w-5xl px-6">
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <h2 className="font-display text-2xl text-ink">
                  {snapshot.territory_name} sample rows
                </h2>
                <p className="mt-2 text-sm text-inkMuted">
                  Captured {formatUtcStamp(snapshot.updated_at_utc)} UTC from public OSHA records.
                </p>
              </div>
              <p className="text-sm font-semibold text-inkMuted">
                {rows.length} populated item{rows.length === 1 ? "" : "s"}
              </p>
            </div>
            <div className="mt-6 grid gap-4 md:grid-cols-2">
              {rows.map((row) => (
                <SignalRowCard key={`${snapshot.territory_id}-${row.activity_nr}`} row={row} />
              ))}
            </div>
          </div>
        </section>
      ) : null}

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <p className="text-sm font-semibold text-ink">Need your footprint instead?</p>
          <p className="mt-2 text-sm text-inkMuted">
            Send the coverage and recipients you want watched. We confirm mapping before billing.
          </p>
          <div className="mt-4 flex flex-wrap items-center gap-4">
            <CTAButtons />
            <Link
              href="/pricing"
              className="text-sm font-semibold text-inkMuted underline-offset-4 transition hover:text-ink hover:underline"
            >
              Review pricing
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}
