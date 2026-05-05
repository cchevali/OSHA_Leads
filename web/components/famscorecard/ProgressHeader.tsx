"use client";

import { useEffect, useState } from "react";

interface ProgressHeaderProps {
  stepNumber: number;
  totalSteps: number;
  title: string;
  description: string;
  savedLabel?: string;
}

type LegacyMediaQueryList = MediaQueryList & {
  addListener?: (listener: (event: MediaQueryListEvent) => void) => void;
  removeListener?: (listener: (event: MediaQueryListEvent) => void) => void;
};

function bindMediaQueryListener(
  mediaQuery: MediaQueryList,
  onChange: (event: MediaQueryListEvent) => void
) {
  if ("addEventListener" in mediaQuery) {
    mediaQuery.addEventListener("change", onChange);
    return () => mediaQuery.removeEventListener("change", onChange);
  }

  const legacyMediaQuery = mediaQuery as LegacyMediaQueryList;
  legacyMediaQuery.addListener?.(onChange);
  return () => legacyMediaQuery.removeListener?.(onChange);
}

export default function ProgressHeader({
  stepNumber,
  totalSteps,
  title,
  description,
  savedLabel
}: ProgressHeaderProps) {
  const percent = Math.round((stepNumber / totalSteps) * 100);
  const [isCompactMobile, setIsCompactMobile] = useState(false);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const mediaQuery = window.matchMedia("(max-width: 767px)");
    let frame = 0;

    const sync = (mobile = mediaQuery.matches) => {
      setIsCompactMobile(mobile && window.scrollY > 28);
    };

    const onScroll = () => {
      if (frame) {
        return;
      }

      frame = window.requestAnimationFrame(() => {
        frame = 0;
        sync();
      });
    };

    const unbindMedia = bindMediaQueryListener(mediaQuery, (event) => {
      sync(event.matches);
    });

    sync();
    window.addEventListener("scroll", onScroll, { passive: true });

    return () => {
      window.removeEventListener("scroll", onScroll);
      window.cancelAnimationFrame(frame);
      unbindMedia();
    };
  }, []);

  return (
    <div className="sticky top-[60px] z-30 border-b border-cardBorder bg-sand/92 backdrop-blur transition-all duration-200 sm:top-[73px]">
      <div
        className={`mx-auto w-full max-w-5xl px-4 transition-all duration-200 sm:px-6 ${
          isCompactMobile ? "py-2.5 sm:py-4" : "py-4"
        }`}
      >
        <div className={`flex justify-between gap-3 ${isCompactMobile ? "items-center" : "items-start"}`}>
          <div className="min-w-0">
            <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-ocean sm:text-xs sm:tracking-[0.28em]">
              Step {stepNumber} of {totalSteps}
            </p>
            <h1
              className={`font-display text-ink transition-all duration-200 ${
                isCompactMobile
                  ? "mt-1 line-clamp-1 text-base leading-5 sm:mt-2 sm:text-3xl"
                  : "mt-2 text-2xl sm:text-3xl"
              }`}
            >
              {title}
            </h1>
            <p
              className={`max-w-2xl text-sm leading-6 text-inkMuted transition-all duration-200 sm:text-base ${
                isCompactMobile ? "hidden sm:block sm:mt-2" : "mt-2"
              }`}
            >
              {description}
            </p>
          </div>
          <div
            className={`shrink-0 rounded-full border border-cardBorder bg-card text-right transition-all duration-200 ${
              isCompactMobile ? "px-2.5 py-1.5" : "px-3 py-2"
            }`}
          >
            <p className={`font-semibold text-ink ${isCompactMobile ? "text-sm sm:text-lg" : "text-lg"}`}>
              {percent}%
            </p>
            <p className="hidden text-xs text-inkMuted sm:block">{savedLabel || "Saved locally"}</p>
          </div>
        </div>
        <div className={`overflow-hidden rounded-full bg-surface transition-all duration-200 ${isCompactMobile ? "mt-2 h-1.5" : "mt-4 h-2"}`}>
          <div
            className="h-full rounded-full bg-gradient-to-r from-ocean to-sunrise transition-all duration-300"
            style={{ width: `${percent}%` }}
          />
        </div>
      </div>
    </div>
  );
}
