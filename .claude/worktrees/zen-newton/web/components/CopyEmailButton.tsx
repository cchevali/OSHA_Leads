"use client";

import { useCallback, useState } from "react";
import { trackEvent } from "@/lib/analytics";

async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    try {
      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.setAttribute("readonly", "true");
      textarea.style.position = "fixed";
      textarea.style.top = "-1000px";
      textarea.style.left = "-1000px";
      document.body.appendChild(textarea);
      textarea.select();
      const copied = document.execCommand("copy");
      document.body.removeChild(textarea);
      return copied;
    } catch {
      return false;
    }
  }
}

type CopyEmailButtonProps = {
  email: string;
};

export default function CopyEmailButton({ email }: CopyEmailButtonProps) {
  const [status, setStatus] = useState("");

  const handleCopy = useCallback(async () => {
    trackEvent("contact_copy_email");
    const ok = await copyToClipboard(email);
    setStatus(ok ? "Copied" : "Copy failed");
    window.setTimeout(() => setStatus(""), 1800);
  }, [email]);

  return (
    <div className="flex items-center gap-3">
      <button
        type="button"
        onClick={handleCopy}
        className="rounded-full border border-cardBorder px-3 py-1.5 text-xs font-semibold text-ink transition hover:border-ink/40 hover:bg-surface"
      >
        Copy email
      </button>
      {status ? <p className="text-xs font-semibold text-inkMuted">{status}</p> : null}
    </div>
  );
}
