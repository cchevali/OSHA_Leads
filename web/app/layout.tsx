import "./globals.css";
import type { Metadata } from "next";
import type { ReactNode } from "react";
import { Space_Grotesk, Source_Sans_3 } from "next/font/google";
import Nav from "@/components/Nav";
import Footer from "@/components/Footer";
import PlausibleProvider from "@/components/PlausibleProvider";
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

export const metadata: Metadata = {
  metadataBase: new URL(site.siteUrl),
  alternates: {
    canonical: "/"
  },
  title: {
    default: `${site.brandName} | OSHA Activity Signals`,
    template: `%s | ${site.brandName}`
  },
  description:
    "Daily public OSHA activity signals for employer-side defense and safety teams. Counties, cities, metros, or OSHA areas work, and every sample item links back to the public record.",
  robots:
    process.env.VERCEL_ENV === "production" && process.env.NODE_ENV === "production"
      ? { index: true, follow: true }
      : { index: false, follow: false, nocache: true },
  openGraph: {
    type: "website",
    title: `${site.brandName} | OSHA Activity Signals`,
    description:
      "Daily public OSHA activity signals for employer-side defense and safety teams. Counties, cities, metros, or OSHA areas work, and every sample item links back to the public record.",
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
      "Daily public OSHA activity signals for employer-side defense and safety teams. Counties, cities, metros, or OSHA areas work, and every sample item links back to the public record.",
    images: ["/og.svg"]
  }
};

/* Anti-flash script: runs before React hydrates to set the correct theme class
   immediately, preventing a white flash on dark-mode pages. */
const themeScript = `
(function(){
  try {
    var s = localStorage.getItem('theme');
    var d = s === 'light' ? false : true;
    document.documentElement.classList.toggle('dark', d);
  } catch(e) {
    document.documentElement.classList.add('dark');
  }
})();
`;

export default function RootLayout({
  children
}: {
  children: ReactNode;
}) {
  return (
    <html lang="en" className={`dark ${display.variable} ${body.variable}`} suppressHydrationWarning>
      <head>
        <script dangerouslySetInnerHTML={{ __html: themeScript }} />
      </head>
      <body className="antialiased">
        <PlausibleProvider />
        <Nav />
        <main>{children}</main>
        <Footer />
      </body>
    </html>
  );
}
