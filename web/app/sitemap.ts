import type { MetadataRoute } from "next";
import site from "@/config/site.json";

export default function sitemap(): MetadataRoute.Sitemap {
  const base = site.siteUrl.replace(/\/$/, "");
  const routes = [
    "",
    "/how-it-works",
    "/pricing",
    "/sample",
    "/faq",
    "/contact",
    "/privacy",
    "/terms"
  ];

  return routes.map((route) => ({
    url: `${base}${route}`,
    lastModified: new Date().toISOString()
  }));
}
