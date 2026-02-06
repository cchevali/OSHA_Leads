/* eslint-disable no-console */
import { spawn } from "node:child_process";
import fs from "node:fs";
import fsp from "node:fs/promises";
import http from "node:http";
import path from "node:path";

const PORT = 3333;
const HOST = "127.0.0.1";

const REQUIRED_ROUTES = [
  "/",
  "/how-it-works",
  "/pricing",
  "/sample",
  "/faq",
  "/contact",
  "/privacy",
  "/terms",
  "/sitemap.xml",
  "/robots.txt"
];

const CANONICAL_HOST = "microflowops.com";
const WWW_HOST = "www.microflowops.com";

function npmBin() {
  return process.platform === "win32" ? "npm.cmd" : "npm";
}

function run(cmd, args, opts = {}) {
  return new Promise((resolve) => {
    const child = spawn(cmd, args, {
      stdio: "inherit",
      // On Windows, npm is typically a .cmd wrapper and requires a shell to execute reliably.
      shell: process.platform === "win32",
      ...opts
    });
    child.on("exit", (code) => resolve(code ?? 1));
    child.on("error", () => resolve(1));
  });
}

async function collectFiles(rootDir, relDir) {
  const base = path.join(rootDir, relDir);
  const out = [];
  async function walk(dir) {
    const entries = await fsp.readdir(dir, { withFileTypes: true });
    for (const e of entries) {
      if (e.name.startsWith(".")) continue;
      const full = path.join(dir, e.name);
      if (e.isDirectory()) {
        if (e.name === "node_modules" || e.name === ".next") continue;
        await walk(full);
      } else {
        if (!/\.(tsx|ts|js|mjs|json|md|css)$/.test(e.name)) continue;
        out.push(full);
      }
    }
  }
  if (fs.existsSync(base)) await walk(base);
  return out;
}

function findBannedCTAs(text) {
  const matches = [];
  const patterns = [
    { id: "calendly", re: /\bcalendly(\.com)?\b/i },
    { id: "schedule_call", re: /\bschedule\s+(a\s+)?call\b/i },
    { id: "book_call", re: /\bbook\s+(a\s+)?call\b/i },
    { id: "book_time", re: /\bbook\s+time\b/i },
    { id: "call_us", re: /\b(call us|give us a call)\b/i },
    { id: "zoom_call", re: /\bzoom\s+call\b/i }
  ];
  for (const p of patterns) {
    const m = text.match(p.re);
    if (m) matches.push({ id: p.id, match: m[0] });
  }
  return matches;
}

async function scanEmailOnly(rootDir) {
  const scanRoots = ["app", "components", "config", "lib", "public"];
  const findings = [];
  for (const rel of scanRoots) {
    const files = await collectFiles(rootDir, rel);
    for (const file of files) {
      const raw = await fsp.readFile(file, "utf8");
      const banned = findBannedCTAs(raw);
      if (banned.length) {
        findings.push({
          file: path.relative(rootDir, file),
          banned
        });
      }
    }
  }
  return findings;
}

function httpRequest({ pathname, hostHeader }) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: HOST,
        port: PORT,
        path: pathname,
        method: "GET",
        headers: {
          Host: hostHeader
        }
      },
      (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const body = Buffer.concat(chunks).toString("utf8");
          resolve({
            status: res.statusCode ?? 0,
            headers: res.headers,
            body
          });
        });
      }
    );
    req.on("error", reject);
    req.end();
  });
}

async function waitForServerReady({ env }) {
  const nextBin = path.join("node_modules", "next", "dist", "bin", "next");
  const child = spawn(process.execPath, [nextBin, "start", "-p", String(PORT)], {
    env: { ...process.env, ...env, NODE_ENV: "production" },
    stdio: ["ignore", "pipe", "pipe"],
    shell: false
  });

  let stdout = "";
  let stderr = "";
  const onOut = (buf) => {
    stdout += buf.toString("utf8");
  };
  const onErr = (buf) => {
    stderr += buf.toString("utf8");
  };
  child.stdout.on("data", onOut);
  child.stderr.on("data", onErr);

  const startedAt = Date.now();
  const deadlineMs = 45_000;
  while (Date.now() - startedAt < deadlineMs) {
    try {
      const res = await httpRequest({ pathname: "/robots.txt", hostHeader: CANONICAL_HOST });
      if (res.status > 0) {
        return { child, stdout, stderr };
      }
    } catch {
      // ignore
    }
    await new Promise((r) => setTimeout(r, 250));
  }

  try {
    child.kill("SIGTERM");
  } catch {
    // ignore
  }
  throw new Error(`Next server failed to start within ${deadlineMs}ms.\nstdout:\n${stdout}\nstderr:\n${stderr}`);
}

function extractRobotsMeta(html) {
  const re = /<meta[^>]+name=["']robots["'][^>]*>/i;
  const m = html.match(re);
  if (!m) return "";
  return m[0];
}

function includesNoindexNofollow(metaTag) {
  const lower = metaTag.toLowerCase();
  return lower.includes("noindex") && lower.includes("nofollow");
}

async function verifyRoutes(hostHeader) {
  const failures = [];
  for (const route of REQUIRED_ROUTES) {
    const res = await httpRequest({ pathname: route, hostHeader });
    const ct = String(res.headers["content-type"] || "");
    if (res.status !== 200) {
      failures.push({ route, status: res.status, ct });
      continue;
    }
    if (route.endsWith(".xml")) {
      if (!ct.includes("xml") && !res.body.startsWith("<?xml") && !res.body.includes("<urlset")) {
        failures.push({ route, status: res.status, ct, reason: "expected xml" });
      }
      continue;
    }
    if (route.endsWith(".txt")) {
      if (!ct.includes("text/plain") && !res.body.toLowerCase().includes("user-agent")) {
        failures.push({ route, status: res.status, ct, reason: "expected robots text" });
      }
      continue;
    }
    if (!ct.includes("text/html") || !res.body.toLowerCase().includes("<html")) {
      failures.push({ route, status: res.status, ct, reason: "expected html" });
    }
  }
  return failures;
}

async function verifyNoindexPreview() {
  const resHome = await httpRequest({ pathname: "/", hostHeader: CANONICAL_HOST });
  const tag = extractRobotsMeta(resHome.body);
  if (!tag) return { ok: false, reason: "missing meta robots tag" };
  if (!includesNoindexNofollow(tag)) return { ok: false, reason: `meta robots not noindex/nofollow: ${tag}` };

  const resRobots = await httpRequest({ pathname: "/robots.txt", hostHeader: CANONICAL_HOST });
  if (!/disallow:\s*\//i.test(resRobots.body)) {
    return { ok: false, reason: "robots.txt does not disallow / in preview" };
  }
  return { ok: true };
}

async function verifyIndexableProduction() {
  const resHome = await httpRequest({ pathname: "/", hostHeader: CANONICAL_HOST });
  const tag = extractRobotsMeta(resHome.body);
  if (!tag) return { ok: false, reason: "missing meta robots tag" };
  const lower = tag.toLowerCase();
  if (lower.includes("noindex") || lower.includes("nofollow")) {
    return { ok: false, reason: `meta robots is noindex/nofollow in production: ${tag}` };
  }
  const resRobots = await httpRequest({ pathname: "/robots.txt", hostHeader: CANONICAL_HOST });
  if (!/allow:\s*\//i.test(resRobots.body)) {
    return { ok: false, reason: "robots.txt does not allow / in production" };
  }
  return { ok: true };
}

async function verifyWwwRedirect() {
  const res = await httpRequest({ pathname: "/sample?x=1", hostHeader: WWW_HOST });
  const location = String(res.headers.location || "");
  if (res.status !== 301) return { ok: false, reason: `expected 301, got ${res.status}` };
  if (!location.includes(CANONICAL_HOST)) return { ok: false, reason: `location missing canonical host: ${location}` };
  if (!location.includes("/sample")) return { ok: false, reason: `location missing path: ${location}` };
  return { ok: true, location };
}

function printResultTable(results) {
  console.log("\nGo-Live Readiness Gate");
  console.log("======================");
  for (const r of results) {
    const status = r.ok ? "PASS" : "FAIL";
    console.log(`${status}  (${r.id}) ${r.title}`);
    if (!r.ok && r.details) {
      console.log(`      ${r.details}`);
    }
  }
  const failed = results.filter((r) => !r.ok);
  console.log("\nSummary");
  console.log("-------");
  if (failed.length === 0) {
    console.log("PASS: all criteria satisfied.");
  } else {
    console.log(`FAIL: ${failed.length} criteria failed.`);
  }
}

async function main() {
  const rootDir = process.cwd();
  const results = [];

  // (1) lint + build
  const lintCode = await run(npmBin(), ["run", "lint"]);
  // Build twice: preview and production. Next.js metadata is evaluated at build-time for static pages.
  const buildPreviewCode =
    lintCode === 0
      ? await run(npmBin(), ["run", "build"], { env: { ...process.env, VERCEL_ENV: "preview" } })
      : 1;
  results.push({
    id: "1",
    title: "web/ lint passes and production build succeeds",
    ok: lintCode === 0 && buildPreviewCode === 0,
    details: lintCode !== 0 ? "lint failed" : buildPreviewCode !== 0 ? "build failed" : ""
  });
  if (!(lintCode === 0 && buildPreviewCode === 0)) {
    printResultTable(results);
    process.exit(1);
  }

  // (3) email-only scan
  const ctaFindings = await scanEmailOnly(rootDir);
  results.push({
    id: "3",
    title: "site is email-only (no Calendly / call CTAs)",
    ok: ctaFindings.length === 0,
    details: ctaFindings.length
      ? `found banned CTA patterns in: ${ctaFindings.map((f) => f.file).join(", ")}`
      : ""
  });

  if (ctaFindings.length) {
    console.log("\nBanned CTA findings (first 10):");
    for (const f of ctaFindings.slice(0, 10)) {
      console.log(`- ${f.file}: ${f.banned.map((b) => `${b.id}(${b.match})`).join(", ")}`);
    }
  }

  // Start server twice: preview (noindex) and production (indexable)
  let server = null;
  try {
    // Preview mode checks (2,4,5)
    server = await waitForServerReady({ env: { VERCEL_ENV: "preview" } });

    const routeFailures = await verifyRoutes(CANONICAL_HOST);
    results.push({
      id: "2",
      title: "required routes render (pages + sitemap.xml + robots.txt)",
      ok: routeFailures.length === 0,
      details: routeFailures.length ? JSON.stringify(routeFailures[0]) : ""
    });

    const noindex = await verifyNoindexPreview();
    results.push({
      id: "4",
      title: "preview/localhost enforces noindex/nofollow (metadata + robots.txt)",
      ok: noindex.ok,
      details: noindex.ok ? "" : noindex.reason
    });

    const redir = await verifyWwwRedirect();
    results.push({
      id: "5",
      title: "canonical www -> apex redirect works (301) via proxy.ts",
      ok: redir.ok,
      details: redir.ok ? `location=${redir.location}` : redir.reason
    });
  } finally {
    if (server?.child) {
      try {
        server.child.kill("SIGTERM");
      } catch {
        // ignore
      }
    }
  }

  // Production build + indexability check (VERCEL_ENV must be set at build time for static metadata).
  const buildProdCode = await run(npmBin(), ["run", "build"], { env: { ...process.env, VERCEL_ENV: "production" } });
  results.push({
    id: "1b",
    title: "production build succeeds (VERCEL_ENV=production)",
    ok: buildProdCode === 0,
    details: buildProdCode === 0 ? "" : "build failed"
  });
  if (buildProdCode !== 0) {
    printResultTable(results);
    process.exit(1);
  }

  try {
    server = await waitForServerReady({ env: { VERCEL_ENV: "production" } });
    const prod = await verifyIndexableProduction();
    results.push({
      id: "4b",
      title: "production is indexable (metadata + robots.txt allow)",
      ok: prod.ok,
      details: prod.ok ? "" : prod.reason
    });
  } finally {
    if (server?.child) {
      try {
        server.child.kill("SIGTERM");
      } catch {
        // ignore
      }
    }
  }

  // (6) docs gate is informational; enforce presence of Domain Doctor runbook.
  const hasDomainDoctorDocs =
    fs.existsSync(path.join(rootDir, "..", "DOMAIN_DOCTOR_RUNBOOK.md")) &&
    fs.existsSync(path.join(rootDir, "..", "domain_doctor.py")) &&
    fs.existsSync(path.join(rootDir, "..", "LAUNCH_CHECKLIST.md"));
  results.push({
    id: "6",
    title: "docs: only remaining domain go-live gate is DNS + Vercel validation (Domain Doctor -> Launch Checklist)",
    ok: hasDomainDoctorDocs,
    details: hasDomainDoctorDocs ? "" : "missing DOMAIN_DOCTOR_RUNBOOK.md/domain_doctor.py/LAUNCH_CHECKLIST.md"
  });

  printResultTable(results);
  const failed = results.filter((r) => !r.ok);
  process.exit(failed.length ? 1 : 0);
}

main().catch((err) => {
  console.error(`GATE_ERROR ${err?.message || err}`);
  process.exit(1);
});
