import type { ReactNode } from "react";

interface QuestionCardProps {
  title: string;
  hint?: string;
  children: ReactNode;
}

export default function QuestionCard({ title, hint, children }: QuestionCardProps) {
  return (
    <section className="rounded-[28px] border border-cardBorder bg-card p-5 shadow-soft sm:p-6">
      <div className="space-y-2">
        <h3 className="font-display text-xl text-ink sm:text-2xl">{title}</h3>
        {hint ? <p className="text-sm leading-6 text-inkMuted sm:text-base">{hint}</p> : null}
      </div>
      <div className="mt-4">{children}</div>
    </section>
  );
}
