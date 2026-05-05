"use client";

import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import ThemeToggle from "@/components/ThemeToggle";
import site from "@/config/site.json";

const navItems = [
  { label: "How It Works", href: "/how-it-works" },
  { label: "Pricing", href: "/pricing" },
  { label: "Sample", href: "/sample" },
  { label: "FAQ", href: "/faq" },
  { label: "Contact", href: "/contact" }
];

export default function Nav() {
  const [open, setOpen] = useState(false);
  const pathname = usePathname();

  if (pathname.startsWith("/famscorecard")) {
    return (
      <header className="sticky top-0 z-40 border-b border-cardBorder bg-sand/92 backdrop-blur">
        <div className="mx-auto flex w-full max-w-5xl items-center justify-between px-4 py-3 sm:px-6 sm:py-4">
          <div className="min-w-0">
            <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-ocean sm:text-xs sm:tracking-[0.26em]">
              Private household decision tool
            </p>
            <Link href="/famscorecard" className="mt-1 block font-display text-base tracking-tight text-ink sm:mt-2 sm:text-lg">
              Family Fit Scorecard
            </Link>
          </div>
          <div className="flex items-center gap-2 sm:gap-3">
            <span className="hidden rounded-full border border-cardBorder bg-card px-3 py-2 text-xs font-semibold text-inkMuted sm:inline-flex">
              Saved locally on this device
            </span>
            <ThemeToggle />
          </div>
        </div>
      </header>
    );
  }

  return (
    <header className="sticky top-0 z-40 border-b border-cardBorder bg-sand/80 backdrop-blur">
      <div className="mx-auto flex w-full max-w-6xl items-center justify-between px-6 py-4">
        <Link href="/" className="font-display text-lg tracking-tight text-ink">
          {site.brandName}
        </Link>
        <nav className="hidden items-center gap-6 text-sm font-medium text-inkMuted md:flex">
          {navItems.map((item) => (
            <Link key={item.href} href={item.href} className="transition hover:text-ink">
              {item.label}
            </Link>
          ))}
        </nav>
        <div className="flex items-center gap-2">
          <ThemeToggle />
          <Link
            href="/contact?source=nav&intent=sample"
            className="hidden items-center justify-center rounded-full bg-ocean px-4 py-2 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark md:inline-flex"
          >
            Request a sample
          </Link>
          <button
            aria-label="Toggle menu"
            onClick={() => setOpen(!open)}
            className="flex flex-col items-center justify-center gap-1.5 rounded-lg p-2 md:hidden"
          >
            <span className={`block h-0.5 w-5 bg-ink transition-transform ${open ? "translate-y-2 rotate-45" : ""}`} />
            <span className={`block h-0.5 w-5 bg-ink transition-opacity ${open ? "opacity-0" : ""}`} />
            <span className={`block h-0.5 w-5 bg-ink transition-transform ${open ? "-translate-y-2 -rotate-45" : ""}`} />
          </button>
        </div>
      </div>
      {open && (
        <div className="border-t border-cardBorder bg-sand/95 px-6 py-4 md:hidden">
          <nav className="flex flex-col gap-3 text-sm font-medium text-inkMuted">
            {navItems.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                onClick={() => setOpen(false)}
                className="py-1 transition hover:text-ink"
              >
                {item.label}
              </Link>
            ))}
          </nav>
          <div className="mt-4">
            <Link
              href="/contact?source=nav_mobile&intent=sample"
              onClick={() => setOpen(false)}
              className="inline-flex items-center justify-center rounded-full bg-ocean px-4 py-2 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark"
            >
              Request a sample
            </Link>
          </div>
        </div>
      )}
    </header>
  );
}
