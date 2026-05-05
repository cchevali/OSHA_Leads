import type { ScenarioResult } from "@/lib/famscorecard/types";

const statusStyles: Record<ScenarioResult["status"], { badge: string; panel: string; label: string }> = {
  no: {
    badge: "bg-red-100 text-red-700 dark:bg-red-500/15 dark:text-red-200",
    panel: "border-red-200/80 bg-red-50/60 dark:border-red-500/30 dark:bg-red-500/8",
    label: "No"
  },
  "maybe-later": {
    badge: "bg-amber-100 text-amber-700 dark:bg-amber-500/15 dark:text-amber-100",
    panel: "border-amber-200/80 bg-amber-50/60 dark:border-amber-500/30 dark:bg-amber-500/8",
    label: "Maybe later"
  },
  "maybe-now": {
    badge: "bg-emerald-100 text-emerald-700 dark:bg-emerald-500/15 dark:text-emerald-100",
    panel: "border-emerald-200/80 bg-emerald-50/60 dark:border-emerald-500/30 dark:bg-emerald-500/8",
    label: "Maybe now"
  }
};

interface ScenarioScoreCardProps {
  rank: number;
  scenario: ScenarioResult;
}

export default function ScenarioScoreCard({ rank, scenario }: ScenarioScoreCardProps) {
  const tone = statusStyles[scenario.status];
  const solvedTone = statusStyles[scenario.fitIfSolvedStatus];

  return (
    <article className={`rounded-[28px] border p-5 shadow-soft ${tone.panel}`}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.26em] text-inkMuted">Rank {rank}</p>
          <h3 className="mt-2 font-display text-2xl text-ink">{scenario.label}</h3>
        </div>
        <div className="text-right">
          <span className={`inline-flex rounded-full px-3 py-1 text-sm font-semibold ${tone.badge}`}>
            {tone.label}
          </span>
          <p className="mt-2 text-2xl font-semibold text-ink">{scenario.score}/100</p>
          <p className="text-sm text-inkMuted">{scenario.confidence} confidence</p>
        </div>
      </div>

      <p className="mt-4 text-sm leading-6 text-inkMuted">{scenario.explanation}</p>

      <div className="mt-5 grid gap-4 md:grid-cols-2">
        <div className="rounded-2xl border border-cardBorder/80 bg-card/70 p-4">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-inkMuted">Current fit</p>
          <span className={`mt-3 inline-flex rounded-full px-3 py-1 text-sm font-semibold ${tone.badge}`}>
            {tone.label}
          </span>
          <p className="mt-3 text-2xl font-semibold text-ink">{scenario.score}/100</p>
          <p className="mt-2 text-sm leading-6 text-inkMuted">How the scenario reads under the current unsolved constraints.</p>
        </div>
        <div className="rounded-2xl border border-cardBorder/80 bg-card/70 p-4">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-inkMuted">Fit if solved</p>
          <span className={`mt-3 inline-flex rounded-full px-3 py-1 text-sm font-semibold ${solvedTone.badge}`}>
            {solvedTone.label}
          </span>
          <p className="mt-3 text-2xl font-semibold text-ink">{scenario.fitIfSolvedScore}/100</p>
          <p className="mt-2 text-sm leading-6 text-inkMuted">{scenario.fitIfSolvedSummary}</p>
          {scenario.fitIfSolvedTargets.length > 0 ? (
            <p className="mt-3 text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">
              Assuming solved: {scenario.fitIfSolvedTargets.join(" + ")}
            </p>
          ) : null}
        </div>
      </div>

      <div className="mt-5 grid gap-4 md:grid-cols-2">
        <div className="rounded-2xl border border-cardBorder/80 bg-card/70 p-4">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-inkMuted">Top positives</p>
          <ul className="mt-3 space-y-2 text-sm text-ink">
            {scenario.topPositives.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
        <div className="rounded-2xl border border-cardBorder/80 bg-card/70 p-4">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-inkMuted">Top concerns</p>
          <ul className="mt-3 space-y-2 text-sm text-ink">
            {scenario.topConcerns.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
      </div>

      <div className="mt-5 grid gap-4 lg:grid-cols-3">
        <div className="rounded-2xl border border-cardBorder/80 bg-card/70 p-4">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-inkMuted">Structural mismatch</p>
          <ul className="mt-3 space-y-2 text-sm text-ink">
            {(scenario.structuralMismatches.length > 0 ? scenario.structuralMismatches : ["No major family-fit mismatch surfaced"]).map(
              (item) => (
                <li key={item}>{item}</li>
              )
            )}
          </ul>
        </div>
        <div className="rounded-2xl border border-cardBorder/80 bg-card/70 p-4">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-inkMuted">Timing issue</p>
          <ul className="mt-3 space-y-2 text-sm text-ink">
            {(scenario.timingIssues.length > 0 ? scenario.timingIssues : ["No separate timing issue surfaced"]).map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
        <div className="rounded-2xl border border-cardBorder/80 bg-card/70 p-4">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-inkMuted">Solvable blocker</p>
          <ul className="mt-3 space-y-2 text-sm text-ink">
            {(scenario.solvableBlockers.length > 0 ? scenario.solvableBlockers : ["No major solvable blocker surfaced"]).map(
              (item) => (
                <li key={item}>{item}</li>
              )
            )}
          </ul>
        </div>
      </div>
    </article>
  );
}
