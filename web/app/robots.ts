import type { MetadataRoute } from "next";
import site from "@/config/site.json";

export default function robots(): MetadataRoute.Robots {
  const base = site.siteUrl.replace(/\/$/, "");
  const isIndexable =
    process.env.VERCEL_ENV === "production" && process.env.NODE_ENV === "production";

  return {
    rules: [
      isIndexable
        ? { userAgent: "*", allow: "/" }
        : { userAgent: "*", disallow: "/" }
    ],
    host: base,
    sitemap: `${base}/sitemap.xml`
  };
}
