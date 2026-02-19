import "server-only";

import fs from "node:fs";
import path from "node:path";

export type CbsaOption = {
  cbsaCode: string;
  label: string;
  state: string;
};

let cachedOptions: CbsaOption[] | null = null;

function parseCsvLine(line: string): string[] {
  const output: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let idx = 0; idx < line.length; idx += 1) {
    const char = line[idx];
    if (char === '"') {
      if (inQuotes && line[idx + 1] === '"') {
        current += '"';
        idx += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (char === "," && !inQuotes) {
      output.push(current);
      current = "";
      continue;
    }
    current += char;
  }
  output.push(current);
  return output;
}

function resolveCbsaCsvPath(): string {
  const candidates = [
    path.join(process.cwd(), "..", "data", "geo", "cbsa_meta.csv"),
    path.join(process.cwd(), "data", "geo", "cbsa_meta.csv"),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return candidates[0];
}

export function loadCbsaOptions(): CbsaOption[] {
  if (cachedOptions) {
    return cachedOptions;
  }
  const csvPath = resolveCbsaCsvPath();
  if (!fs.existsSync(csvPath)) {
    cachedOptions = [];
    return cachedOptions;
  }
  const lines = fs.readFileSync(csvPath, "utf-8").split(/\r?\n/).filter((line) => line.trim().length > 0);
  const options: CbsaOption[] = [];
  for (const line of lines.slice(1)) {
    const [cbsaRaw, labelRaw] = parseCsvLine(line);
    const cbsaCode = String(cbsaRaw || "").trim().replace(/\D/g, "").padStart(5, "0");
    const label = String(labelRaw || "").trim();
    if (!cbsaCode || !label) {
      continue;
    }
    const stateMatch = label.match(/,\s*([A-Z]{2})$/);
    options.push({
      cbsaCode,
      label,
      state: stateMatch ? stateMatch[1] : "",
    });
  }
  cachedOptions = options.sort((a, b) => a.label.localeCompare(b.label));
  return cachedOptions;
}
