/* eslint-disable no-console */

import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_ORIGIN = "https://microflowops.com";
const args = new Set(process.argv.slice(2));
const origin = (process.env.BIZREVIEW_PRODUCTION_ORIGIN || DEFAULT_ORIGIN).replace(/\/$/, "");
const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = join(scriptDirectory, "..", "..");
const localBuildRoot = join(repositoryRoot, "web", "public", "bizreview");
const documentRoutes = [
  { route: "/bizreview", artifact: "index.html" },
  { route: "/bizreview/", artifact: "index.html" },
  { route: "/bizreview/details/", artifact: join("details", "index.html") },
];
const manifestRoute = "/bizreview/version.json";

function hash(body) {
  return createHash("sha256").update(body).digest("hex");
}

function buildIdentity(version, source) {
  const identity = {
    commit: version.commit,
    buildTime: version.buildTime,
    schemaVersion: version.schemaVersion,
    dealStateRevision: version.dealStateRevision,
  };

  if (
    !identity.commit ||
    !identity.buildTime ||
    !Number.isInteger(identity.schemaVersion) ||
    !identity.dealStateRevision
  ) {
    throw new Error(
      `ERR_BIZREVIEW_PARITY_MANIFEST source=${source} identity=${JSON.stringify(identity)}`,
    );
  }

  return identity;
}

async function loadExpectedBuild() {
  const manifestPath = join(localBuildRoot, "version.json");
  const manifestBody = await readFile(manifestPath);
  const version = JSON.parse(manifestBody.toString("utf8"));
  const identity = buildIdentity(version, manifestPath);
  const documents = await Promise.all(
    documentRoutes.map(async ({ route, artifact }) => {
      const path = join(localBuildRoot, artifact);
      const body = await readFile(path);
      return { route, artifact, path, body, sha256: hash(body) };
    }),
  );

  return { identity, manifestBody, manifestPath, documents };
}

async function printConfig(expected) {
  console.log(
    JSON.stringify(
      {
        origin,
        expected: expected.identity,
        localManifest: expected.manifestPath,
        routes: [
          ...expected.documents.map(({ route, path, sha256 }) => ({ route, path, sha256 })),
          { route: manifestRoute, path: expected.manifestPath, sha256: hash(expected.manifestBody) },
        ],
      },
      null,
      2,
    ),
  );
}

function header(response, name) {
  return response.headers.get(name) || "-";
}

function extractHtmlIdentity(html, route) {
  const commit = html.match(/BizReview build\s+([^<\s]+)/)?.[1];
  const buildTime = html.match(/Built\s+([^<\s]+)/)?.[1];
  const schemaVersion = Number(html.match(/Schema v(\d+)/)?.[1]);
  const dealStateRevision = html.match(/Deal state\s+([^<\s]+)/)?.[1];

  if (!commit || !buildTime || !Number.isInteger(schemaVersion) || !dealStateRevision) {
    throw new Error(`ERR_BIZREVIEW_PARITY_IDENTITY route=${route} missing footer build identity`);
  }

  return { commit, buildTime, schemaVersion, dealStateRevision };
}

function hasRevalidatingDocumentCacheControl(value) {
  const cacheControl = value.toLowerCase();
  return (
    cacheControl.includes("no-cache") ||
    (cacheControl.includes("max-age=0") && cacheControl.includes("must-revalidate"))
  );
}

async function fetchRoute(route) {
  const url = `${origin}${route}`;
  const response = await fetch(url, {
    headers: { "cache-control": "no-cache" },
  });
  const cache = {
    age: header(response, "age"),
    cacheControl: header(response, "cache-control"),
    xVercelCache: header(response, "x-vercel-cache"),
    cfCacheStatus: header(response, "cf-cache-status"),
    xCache: header(response, "x-cache"),
  };
  console.log(`PARITY_FETCH route=${route} status=${response.status} ${JSON.stringify(cache)}`);

  if (!response.ok) {
    throw new Error(`ERR_BIZREVIEW_PARITY_HTTP route=${route} status=${response.status}`);
  }

  const body = Buffer.from(await response.arrayBuffer());
  return { route, body, cache };
}

async function main() {
  const expected = await loadExpectedBuild();

  if (args.has("--print-config")) {
    await printConfig(expected);
    return;
  }

  if (args.has("--dry-run")) {
    console.log("DRY_RUN_BIZREVIEW_PARITY");
    await printConfig(expected);
    return;
  }

  const unknownArgs = [...args].filter((arg) => arg !== "--print-config" && arg !== "--dry-run");
  if (unknownArgs.length) {
    throw new Error(`ERR_BIZREVIEW_PARITY_ARGS unsupported=${unknownArgs.join(",")}`);
  }

  const [liveDocuments, liveManifest] = await Promise.all([
    Promise.all(expected.documents.map(({ route }) => fetchRoute(route))),
    fetchRoute(manifestRoute),
  ]);

  const liveVersion = JSON.parse(liveManifest.body.toString("utf8"));
  const liveIdentity = buildIdentity(liveVersion, manifestRoute);
  if (JSON.stringify(liveIdentity) !== JSON.stringify(expected.identity)) {
    throw new Error(
      `ERR_BIZREVIEW_PARITY_MANIFEST_MISMATCH expected=${JSON.stringify(expected.identity)} actual=${JSON.stringify(liveIdentity)}`,
    );
  }

  for (const [index, document] of liveDocuments.entries()) {
    const localDocument = expected.documents[index];
    if (!hasRevalidatingDocumentCacheControl(document.cache.cacheControl)) {
      throw new Error(
        `ERR_BIZREVIEW_PARITY_CACHE route=${document.route} cache-control=${document.cache.cacheControl}`,
      );
    }

    const identity = extractHtmlIdentity(document.body.toString("utf8"), document.route);
    if (JSON.stringify(identity) !== JSON.stringify(expected.identity)) {
      throw new Error(
        `ERR_BIZREVIEW_PARITY_IDENTITY_MISMATCH route=${document.route} expected=${JSON.stringify(expected.identity)} actual=${JSON.stringify(identity)}`,
      );
    }

    const liveSha256 = hash(document.body);
    if (!document.body.equals(localDocument.body)) {
      throw new Error(
        `ERR_BIZREVIEW_PARITY_BODY_MISMATCH route=${document.route} local=${localDocument.sha256} live=${liveSha256} artifact=${localDocument.path}`,
      );
    }

    console.log(
      `PARITY_BODY route=${document.route} bytes=${document.body.length} sha256=${liveSha256}`,
    );
  }

  console.log(
    `PASS_BIZREVIEW_PRODUCTION_PARITY commit=${expected.identity.commit} build=${expected.identity.buildTime} schema=${expected.identity.schemaVersion} dealState=${expected.identity.dealStateRevision}`,
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
