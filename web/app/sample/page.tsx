import type { Metadata } from "next";
import Link from "next/link";
import CTAButtons from "@/components/CTAButtons";
import sampleSignals from "./sample_signals.json";

export const metadata: Metadata = {
  alternates: { canonical: "/sample" }
};

type SampleSignal = {
  id: string;
  priorityTier: "High" | "Medium" | "Low";
  signalType: "Accident" | "Complaint" | "Referral" | "Programmed";
  establishmentName: string;
  metro: string;
  city: string;
  state: string;
  openedDate: string;
  observedAt: string;
};

const ABOVE_FOLD_CARD_LIMIT = 4;

function SignalCard({ signal }: { signal: SampleSignal }) {
  return (
    <article className="rounded-2xl border border-cardBorder bg-surface p-4">
      <div className="flex items-start justify-between gap-3">
        <p className="rounded-full border border-cardBorder bg-card px-2.5 py-1 text-xs font-semibold text-ink">
          {signal.priorityTier} · {signal.signalType}
        </p>
        <p className="text-xs font-semibold text-inkMuted">{signal.observedAt}</p>
      </div>
      <h3 className="mt-3 text-base font-semibold text-ink">{signal.establishmentName}</h3>
      <p className="mt-2 text-sm text-inkMuted">
        {signal.metro} metro · {signal.city}, {signal.state}
      </p>
      <p className="mt-2 text-sm font-semibold text-ink">Opened: {signal.openedDate}</p>
    </article>
  );
}

export default function SamplePage() {
  const typedSignals = sampleSignals as SampleSignal[];
  const visible = typedSignals.slice(0, ABOVE_FOLD_CARD_LIMIT);
  const hidden = typedSignals.slice(ABOVE_FOLD_CARD_LIMIT);

  return (
    <div className="space-y-12 pb-24 pt-12">
      <section className="mx-auto w-full max-w-4xl px-6">
        <div className="space-y-4 text-center">
          <h1 className="font-display text-4xl text-ink md:text-5xl">
            Sample: Daily OSHA Activity Signals
          </h1>
          <p className="text-base text-inkMuted md:text-lg">
            This is a representative preview using anonymized company names and realistic
            locations/timing.
          </p>
        </div>
      </section>

      <section className="mx-auto w-full max-w-5xl px-6">
        <div className="bg-paper rounded-3xl border border-cardBorder p-6 shadow-soft">
          <div className="flex flex-wrap items-center justify-between gap-3 text-xs font-semibold text-inkMuted">
            <span>Daily OSHA Activity Signals</span>
            <span>Morning brief · 09:00 CT</span>
          </div>
          <div className="mt-4 rounded-2xl bg-card p-4">
            <p className="text-xs font-semibold text-inkMuted">
              Company names are anonymized. Not affiliated with OSHA or any government agency.
            </p>

            <div className="mt-4 grid gap-4 md:grid-cols-2">
              {visible.map((signal) => (
                <SignalCard key={signal.id} signal={signal} />
              ))}
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

            {hidden.length > 0 ? (
              <details className="mt-6">
                <summary className="cursor-pointer text-sm font-semibold text-ink">
                  View full sample
                </summary>
                <div className="mt-4 grid gap-4 md:grid-cols-2">
                  {hidden.map((signal) => (
                    <SignalCard key={signal.id} signal={signal} />
                  ))}
                </div>
              </details>
            ) : null}
          </div>
        </div>
      </section>
    </div>
  );
}
