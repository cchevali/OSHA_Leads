const BIZTRACKER_ORIGIN = "https://microflowops-biztracker.vercel.app";

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
  async headers() {
    return [
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
