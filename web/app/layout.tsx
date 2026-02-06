import "./globals.css";
import type { Metadata } from "next";
import type { ReactNode } from "react";
import Script from "next/script";
import { Space_Grotesk, Source_Sans_3 } from "next/font/google";
import Nav from "@/components/Nav";
import Footer from "@/components/Footer";
import site from "@/config/site.json";

const display = Space_Grotesk({
  subsets: ["latin"],
  variable: "--font-display",
  display: "swap"
});

const body = Source_Sans_3({
  subsets: ["latin"],
  variable: "--font-body",
  display: "swap"
});

const plausibleDomain = new URL(site.siteUrl).hostname;

export const metadata: Metadata = {
  metadataBase: new URL(site.siteUrl),
  title: {
    default: `${site.brandName} | OSHA Activity Signals`,
    template: `%s | ${site.brandName}`
  },
  description:
    "Daily OSHA activity signals that surface new inspections before citations post. Built for OSHA defense attorneys and safety consultants across the Texas Triangle.",
  openGraph: {
    type: "website",
    title: `${site.brandName} | OSHA Activity Signals`,
    description:
      "Daily OSHA activity signals that surface new inspections before citations post. Built for OSHA defense attorneys and safety consultants across the Texas Triangle.",
    url: site.siteUrl,
    siteName: site.brandName,
    images: [
      {
        url: "/og.svg",
        width: 1200,
        height: 630,
        alt: `${site.brandName} - OSHA Activity Signals`
      }
    ]
  },
  twitter: {
    card: "summary_large_image",
    title: `${site.brandName} | OSHA Activity Signals`,
    description:
      "Daily OSHA activity signals that surface new inspections before citations post. Built for OSHA defense attorneys and safety consultants across the Texas Triangle.",
    images: ["/og.svg"]
  }
};

export default function RootLayout({
  children
}: {
  children: ReactNode;
}) {
  return (
    <html lang="en" className={`${display.variable} ${body.variable}`}>
      <body className="antialiased">
        <Script
          strategy="afterInteractive"
          defer
          data-domain={plausibleDomain}
          src="https://plausible.io/js/script.manual.js"
        />
        <Nav />
        <main>{children}</main>
        <Footer />
      </body>
    </html>
  );
}