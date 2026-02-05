import Link from "next/link";
import CTAButtons from "@/components/CTAButtons";
import site from "@/config/site.json";

const navItems = [
  { label: "How It Works", href: "/how-it-works" },
  { label: "Pricing", href: "/pricing" },
  { label: "Sample", href: "/sample" },
  { label: "FAQ", href: "/faq" },
  { label: "Contact", href: "/contact" }
];

export default function Nav() {
  return (
    <header className="sticky top-0 z-40 border-b border-black/5 bg-sand/80 backdrop-blur">
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
        <div className="hidden md:flex">
          <CTAButtons />
        </div>
      </div>
    </header>
  );
}
