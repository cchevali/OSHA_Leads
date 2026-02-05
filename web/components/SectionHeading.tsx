import { ReactNode } from "react";

interface SectionHeadingProps {
  eyebrow?: string;
  title: string;
  description?: ReactNode;
  align?: "left" | "center";
}

export default function SectionHeading({
  eyebrow,
  title,
  description,
  align = "left"
}: SectionHeadingProps) {
  const alignment = align === "center" ? "text-center" : "text-left";
  return (
    <div className={`space-y-3 ${alignment}`}>
      {eyebrow ? (
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-ocean">
          {eyebrow}
        </p>
      ) : null}
      <h2 className="font-display text-3xl text-ink md:text-4xl">{title}</h2>
      {description ? (
        <p className="text-base text-inkMuted md:text-lg">{description}</p>
      ) : null}
    </div>
  );
}
