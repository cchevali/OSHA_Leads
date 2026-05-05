import type { Metadata } from "next";
import FamilyFitScorecardApp from "@/components/famscorecard/FamilyFitScorecardApp";

const title = "Family Fit Scorecard";
const description =
  "A calm mobile-first scorecard for couples weighing stay-put, buy-local, and buy-plus-relocate scenarios. Balanced by design, with 'not now' as a valid outcome.";

export const metadata: Metadata = {
  title,
  description,
  alternates: { canonical: "/famscorecard" },
  openGraph: {
    title: `${title} | MicroFlowOps`,
    description,
    url: "/famscorecard",
    images: [
      {
        url: "/og-famscorecard.svg",
        width: 1200,
        height: 630,
        alt: "Family Fit Scorecard by MicroFlowOps"
      }
    ]
  },
  twitter: {
    card: "summary_large_image",
    title: `${title} | MicroFlowOps`,
    description,
    images: ["/og-famscorecard.svg"]
  }
};

export default function FamilyScorecardPage() {
  return <FamilyFitScorecardApp />;
}
