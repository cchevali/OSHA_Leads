"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import site from "@/config/site.json";

const footerLinks = [
  { label: "How It Works", href: "/how-it-works" },
  { label: "Pricing", href: "/pricing" },
  { label: "Sample", href: "/sample" },
  { label: "FAQ", href: "/faq" },
  { label: "Contact", href: "/contact" },
  { label: "Privacy", href: "/privacy" },
  { label: "Terms", href: "/terms" }
];

export default function Footer() {
  const pathname = usePathname();

  if (pathname.startsWith("/famscorecard")) {
    return (
      <footer className="border-t border-cardBorder bg-sand/70">
        <div className="mx-auto w-full max-w-5xl px-4 py-6 sm:px-6">
          <div className="flex flex-col gap-3 text-xs text-inkMuted sm:flex-row sm:items-center sm:justify-between">
            <p>Private household decision tool. Answers stay in this browser in this version.</p>
            <div className="flex flex-wrap items-center gap-3">
              <Link href="/privacy" className="transition hover:text-ink">
                Privacy
              </Link>
              <p>© {new Date().getFullYear()} {site.legalName || site.brandName}</p>
            </div>
          </div>
        </div>
      </footer>
    );
  }

  return (
    <footer className="border-t border-cardBorder bg-surface">
      <div className="mx-auto w-full max-w-6xl px-6 py-12">
        <div className="flex flex-col gap-8 md:flex-row md:items-start md:justify-between">
          <div className="space-y-3">
            <p className="font-display text-lg text-ink">{site.brandName}</p>
            <p className="max-w-xs text-sm text-inkMuted">
              Outreach-ready OSHA leads for safety consulting and training firms doing business development.
            </p>
            <p className="text-xs text-inkMuted">{site.mailingAddress}</p>
          </div>
          <div className="grid grid-cols-2 gap-3 text-sm text-inkMuted md:grid-cols-3">
            {footerLinks.map((item) => (
              <Link key={item.href} href={item.href} className="transition hover:text-ink">
                {item.label}
              </Link>
            ))}
          </div>
        </div>
        <div className="mt-10 flex flex-col gap-2 text-xs text-inkMuted md:flex-row md:items-center md:justify-between">
          <p>Not affiliated with OSHA or any government agency. Informational only.</p>
          <p>© {new Date().getFullYear()} {site.legalName || site.brandName}. All rights reserved.</p>
        </div>
      </div>
    </footer>
  );
}
