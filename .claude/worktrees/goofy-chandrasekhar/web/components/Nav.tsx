"use client";

import { useState } from "react";
import Link from "next/link";
import CTAButtons from "@/components/CTAButtons";
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
          <div className="hidden md:flex">
            <CTAButtons />
          </div>
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
            <CTAButtons />
          </div>
        </div>
      )}
    </header>
  );
}
