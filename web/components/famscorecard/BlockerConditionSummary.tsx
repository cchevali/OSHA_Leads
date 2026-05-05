interface BlockerConditionSummaryProps {
  title: string;
  eyebrow: string;
  items: string[];
  tone?: "neutral" | "warn";
}

export default function BlockerConditionSummary({
  title,
  eyebrow,
  items,
  tone = "neutral"
}: BlockerConditionSummaryProps) {
  const palette =
    tone === "warn"
      ? "border-clay/25 bg-clay/10"
      : "border-cardBorder bg-card";

  return (
    <section className={`rounded-[28px] border p-5 shadow-soft ${palette}`}>
      <p className="text-xs font-semibold uppercase tracking-[0.26em] text-inkMuted">{eyebrow}</p>
      <h3 className="mt-2 font-display text-2xl text-ink">{title}</h3>
      <div className="mt-4 space-y-3">
        {items.length > 0 ? (
          items.map((item) => (
            <div key={item} className="rounded-2xl border border-cardBorder/75 bg-surface px-4 py-3 text-sm leading-6 text-ink">
              {item}
            </div>
          ))
        ) : (
          <div className="rounded-2xl border border-cardBorder/75 bg-surface px-4 py-3 text-sm leading-6 text-inkMuted">
            Nothing sharp surfaced here from the current answers.
          </div>
        )}
      </div>
    </section>
  );
}
