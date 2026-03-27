import { existsSync } from "fs";
import { readFile } from "fs/promises";
import path from "path";
import type { Metadata } from "next";
import { notFound } from "next/navigation";
import CopyEmailTemplate from "@/components/CopyEmailTemplate";
import SectionHeading from "@/components/SectionHeading";
import site from "@/config/site.json";

export const metadata: Metadata = {
  title: "Local Outreach Follow-Up Preview",
  description: "Development-only preview for outreach follow-up templates 2 and 3.",
  robots: { index: false, follow: false, nocache: true }
};

export const dynamic = "force-dynamic";

type PreviewTemplate = {
  key: string;
  title: string;
  description: string;
  textFile: string;
  htmlFile: string;
  subjectOptions: string[];
};

const previewTemplates: PreviewTemplate[] = [
  {
    key: "followup_2",
    title: "Email 2: Timing + fit",
    description: "Reinforces why timing matters and who the product is best for.",
    textFile: "outreach_followup_2.txt",
    htmlFile: "outreach_followup_2.html",
    subjectOptions: [
      "Why timing matters on these leads",
      "Territory-specific OSHA leads",
      "Useful if your team does outbound"
    ]
  },
  {
    key: "followup_3",
    title: "Email 3: Founding Pilot",
    description: "Introduces the $149 Founding Pilot only after the sample-first touch.",
    textFile: "outreach_followup_3.txt",
    htmlFile: "outreach_followup_3.html",
    subjectOptions: [
      "30-day founding pilot",
      "Pilot for one state",
      "$149 founding pilot"
    ]
  }
];

const previewTokens = {
  GREETING_LINE_TEXT: "Hi Morgan,",
  GREETING_LINE_HTML: "Hi Morgan,",
  MICROFLOWOPS_URL: site.siteUrl,
  SUPPORT_EMAIL: site.ctaEmail,
  MAILING_ADDRESS: site.mailingAddress,
  STATE_FULL_NAME: "Texas",
  FOOTER_OPT_OUT_TEXT_BLOCK: "To stop hearing from me, reply with unsubscribe.",
  FOOTER_OPT_OUT_HTML: `<p style="margin: 0; font-size: 12px; color: rgb(102, 102, 102);">To stop hearing from me, <a href="mailto:${site.ctaEmail}?subject=unsubscribe" style="color: rgb(102, 102, 102);">reply with unsubscribe</a>.</p>`
} as const;

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function applyTokens(template: string): string {
  return Object.entries(previewTokens).reduce((output, [token, replacement]) => {
    const curlyToken = `{{${token}}}`;
    const rawTokenPattern = new RegExp(`\\b${escapeRegExp(token)}\\b`, "g");
    return output.replaceAll(curlyToken, replacement).replace(rawTokenPattern, replacement);
  }, template);
}

function resolveOutreachDir(): string {
  const candidates = [
    path.resolve(process.cwd(), "outreach"),
    path.resolve(process.cwd(), "..", "outreach")
  ];

  for (const candidate of candidates) {
    if (
      existsSync(path.join(candidate, "outreach_followup_2.txt")) &&
      existsSync(path.join(candidate, "outreach_followup_2.html")) &&
      existsSync(path.join(candidate, "outreach_followup_3.txt")) &&
      existsSync(path.join(candidate, "outreach_followup_3.html"))
    ) {
      return candidate;
    }
  }

  throw new Error("ERR_OUTREACH_PREVIEW_DIR_NOT_FOUND");
}

export default async function LocalOutreachFollowupPreviewPage() {
  if (process.env.NODE_ENV !== "development") {
    notFound();
  }

  const outreachDir = resolveOutreachDir();
  const previews = await Promise.all(
    previewTemplates.map(async (template) => {
      const [textTemplate, htmlTemplate] = await Promise.all([
        readFile(path.join(outreachDir, template.textFile), "utf-8"),
        readFile(path.join(outreachDir, template.htmlFile), "utf-8")
      ]);

      return {
        ...template,
        textBody: applyTokens(textTemplate),
        htmlBody: applyTokens(htmlTemplate)
      };
    })
  );

  return (
    <div className="space-y-12 pb-24 pt-12">
      <section className="mx-auto w-full max-w-5xl px-6">
        <SectionHeading
          eyebrow="Local Preview"
          title="Outreach follow-up templates 2 and 3"
          description="Development-only route for checking the rendered follow-up copy with sample values."
        />
        <div className="mt-4 rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
          <p className="text-sm text-inkMuted">
            This page is intentionally hidden from navigation and returns a 404 unless the app is running in local
            development.
          </p>
          <p className="mt-3 text-sm text-inkMuted">
            Sample values used here: contact name <span className="font-semibold text-ink">Morgan</span>, territory{" "}
            <span className="font-semibold text-ink">Texas</span>.
          </p>
        </div>
      </section>

      {previews.map((preview) => (
        <section key={preview.key} className="mx-auto w-full max-w-5xl px-6">
          <div className="rounded-3xl border border-cardBorder bg-card p-6 shadow-soft">
            <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
              <div>
                <h2 className="font-display text-3xl text-ink">{preview.title}</h2>
                <p className="mt-2 text-sm text-inkMuted">{preview.description}</p>
              </div>
              <div className="rounded-full border border-cardBorder px-4 py-1.5 text-xs font-semibold uppercase tracking-[0.25em] text-inkMuted">
                Dev only
              </div>
            </div>

            <div className="mt-6 rounded-2xl border border-cardBorder bg-surface p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">Subject options</p>
              <ul className="mt-3 space-y-2 text-sm text-ink">
                {preview.subjectOptions.map((subject) => (
                  <li key={subject}>{subject}</li>
                ))}
              </ul>
            </div>

            <div className="mt-6">
              <CopyEmailTemplate
                title={`${preview.title} plain text`}
                subject={preview.subjectOptions[0]}
                body={preview.textBody}
                subjectEventName={`local_preview_${preview.key}_copy_subject`}
                bodyEventName={`local_preview_${preview.key}_copy_body`}
              />
            </div>

            <div className="mt-6 rounded-3xl border border-cardBorder bg-surface p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">Rendered HTML</p>
              <iframe
                title={`${preview.title} HTML preview`}
                srcDoc={preview.htmlBody}
                sandbox=""
                className="mt-4 h-[920px] w-full rounded-2xl border border-cardBorder bg-white"
              />
            </div>
          </div>
        </section>
      ))}
    </div>
  );
}
