/* eslint-disable no-console */

const DEFAULT_ORIGIN = "https://microflowops.com";
const REQUIRED_SCHEMA_VERSION = 14;
const args = new Set(process.argv.slice(2));
const origin = (process.env.BIZREVIEW_PRODUCTION_ORIGIN || DEFAULT_ORIGIN).replace(/\/$/, "");
const routes = [
  "/bizreview",
  "/bizreview/",
  "/bizreview/details/",
  "/bizreview/version.json",
];

function printConfig() {
  console.log(
    JSON.stringify(
      {
        origin,
        requiredSchemaVersion: REQUIRED_SCHEMA_VERSION,
        routes,
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

  if (!commit || !buildTime || !Number.isInteger(schemaVersion)) {
    throw new Error(`ERR_BIZREVIEW_PARITY_IDENTITY route=${route} missing footer build identity`);
  }

  return { commit, buildTime, schemaVersion };
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

  return { route, response, cache };
}

async function main() {
  if (args.has("--print-config")) {
    printConfig();
    return;
  }

  if (args.has("--dry-run")) {
    console.log("DRY_RUN_BIZREVIEW_PARITY");
    printConfig();
    return;
  }

  const unknownArgs = [...args].filter((arg) => arg !== "--print-config" && arg !== "--dry-run");
  if (unknownArgs.length) {
    throw new Error(`ERR_BIZREVIEW_PARITY_ARGS unsupported=${unknownArgs.join(",")}`);
  }

  const fetched = [];
  for (const route of routes) fetched.push(await fetchRoute(route));

  const manifest = fetched.at(-1);
  const version = await manifest.response.json();
  const expected = {
    commit: version.commit,
    buildTime: version.buildTime,
    schemaVersion: version.schemaVersion,
  };

  if (!expected.commit || !expected.buildTime || expected.schemaVersion !== REQUIRED_SCHEMA_VERSION) {
    throw new Error(
      `ERR_BIZREVIEW_PARITY_MANIFEST commit=${expected.commit || "missing"} buildTime=${expected.buildTime || "missing"} schema=${expected.schemaVersion}`,
    );
  }

  for (const document of fetched.slice(0, -1)) {
    if (!hasRevalidatingDocumentCacheControl(document.cache.cacheControl)) {
      throw new Error(
        `ERR_BIZREVIEW_PARITY_CACHE route=${document.route} cache-control=${document.cache.cacheControl}`,
      );
    }
    const identity = extractHtmlIdentity(await document.response.text(), document.route);
    if (
      identity.commit !== expected.commit ||
      identity.buildTime !== expected.buildTime ||
      identity.schemaVersion !== expected.schemaVersion
    ) {
      throw new Error(
        `ERR_BIZREVIEW_PARITY_MISMATCH route=${document.route} expected=${JSON.stringify(expected)} actual=${JSON.stringify(identity)}`,
      );
    }
  }

  console.log(`PASS_BIZREVIEW_PRODUCTION_PARITY commit=${expected.commit} build=${expected.buildTime} schema=${expected.schemaVersion}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
