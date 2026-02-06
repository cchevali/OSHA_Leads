"use client";

import { useCallback, useState } from "react";
import { trackEvent } from "@/lib/analytics";

async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    // Fallback for older browsers / stricter permissions.
    try {
      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.setAttribute("readonly", "true");
      textarea.style.position = "fixed";
      textarea.style.top = "-1000px";
      textarea.style.left = "-1000px";
      document.body.appendChild(textarea);
      textarea.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(textarea);
      return ok;
    } catch {
      return false;
    }
  }
}

interface CopyEmailTemplateProps {
  title: string;
  subject: string;
  body: string;
  subjectEventName: string;
  bodyEventName: string;
}

export default function CopyEmailTemplate({
  title,
  subject,
  body,
  subjectEventName,
  bodyEventName
}: CopyEmailTemplateProps) {
  const [subjectStatus, setSubjectStatus] = useState<string>("");
  const [bodyStatus, setBodyStatus] = useState<string>("");

  const handleCopySubject = useCallback(async () => {
    trackEvent(subjectEventName);
    const ok = await copyToClipboard(subject);
    setSubjectStatus(ok ? "Copied" : "Copy failed");
    window.setTimeout(() => setSubjectStatus(""), 1800);
  }, [subject, subjectEventName]);

  const handleCopyBody = useCallback(async () => {
    trackEvent(bodyEventName);
    const ok = await copyToClipboard(body);
    setBodyStatus(ok ? "Copied" : "Copy failed");
    window.setTimeout(() => setBodyStatus(""), 1800);
  }, [body, bodyEventName]);

  return (
    <div className="rounded-3xl border border-black/10 bg-white/85 p-6 shadow-soft">
      <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
        <div>
          <h3 className="font-display text-2xl text-ink">{title}</h3>
          <p className="mt-2 text-sm text-inkMuted">
            Copy and paste into an email. No forms, no calls.
          </p>
        </div>
      </div>

      <div className="mt-6 grid gap-6 lg:grid-cols-2">
        <div className="rounded-2xl border border-black/10 bg-white/80 p-4">
          <div className="flex items-center justify-between gap-4">
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">
              Subject
            </p>
            <div className="flex items-center gap-3">
              {subjectStatus ? (
                <p className="text-xs font-semibold text-inkMuted">{subjectStatus}</p>
              ) : null}
              <button
                type="button"
                onClick={handleCopySubject}
                className="rounded-full border border-ink/15 px-3 py-1.5 text-xs font-semibold text-ink transition hover:border-ink/40 hover:bg-white/70"
              >
                Copy
              </button>
            </div>
          </div>
          <pre className="mt-3 whitespace-pre-wrap rounded-xl bg-sand/60 p-3 text-sm text-ink">
            {subject}
          </pre>
        </div>

        <div className="rounded-2xl border border-black/10 bg-white/80 p-4">
          <div className="flex items-center justify-between gap-4">
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">
              Body
            </p>
            <div className="flex items-center gap-3">
              {bodyStatus ? (
                <p className="text-xs font-semibold text-inkMuted">{bodyStatus}</p>
              ) : null}
              <button
                type="button"
                onClick={handleCopyBody}
                className="rounded-full border border-ink/15 px-3 py-1.5 text-xs font-semibold text-ink transition hover:border-ink/40 hover:bg-white/70"
              >
                Copy
              </button>
            </div>
          </div>
          <pre className="mt-3 whitespace-pre-wrap rounded-xl bg-sand/60 p-3 text-sm text-ink">
            {body}
          </pre>
        </div>
      </div>
    </div>
  );
}
