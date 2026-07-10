const BIZTRACKER_ORIGIN = "https://microflowops-biztracker.vercel.app";
const BIZREVIEW_DOCUMENT_CACHE_CONTROL = "no-cache, max-age=0, must-revalidate";
const BIZREVIEW_STATIC_ASSET_CACHE_CONTROL = "public, max-age=31536000, immutable";

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
  async headers() {
    return [
      {
        source: "/bizreview/:path*",
        headers: [
          {
            key: "Cache-Control",
            value: BIZREVIEW_DOCUMENT_CACHE_CONTROL
          }
        ]
      },
      {
        source: "/bizreview/_next/static/:path*",
        headers: [
          {
            key: "Cache-Control",
            value: BIZREVIEW_STATIC_ASSET_CACHE_CONTROL
          }
        ]
      },
      {
        source: "/bizreview/version.json",
        headers: [
          {
            key: "Cache-Control",
            value: "no-store, max-age=0"
          }
        ]
      }
    ];
  },
  async rewrites() {
    return {
      beforeFiles: [
        {
          source: "/biztracker",
          destination: `${BIZTRACKER_ORIGIN}/biztracker`
        },
        {
          source: "/biztracker/:path*",
          destination: `${BIZTRACKER_ORIGIN}/biztracker/:path*`
        }
      ],
      afterFiles: [
        {
          source: "/bizreview",
          destination: "/bizreview/index.html"
        },
        {
          source: "/bizreview/:path*",
          destination: "/bizreview/:path*/index.html"
        }
      ]
    };
  }
};

export default nextConfig;
