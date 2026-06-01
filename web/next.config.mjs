const BIZTRACKER_ORIGIN = "https://microflowops-biztracker.vercel.app";
const BIZREVIEW_ORIGIN = "https://microflowops-bizreview.vercel.app";

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
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
        },
        {
          source: "/bizreview",
          destination: `${BIZREVIEW_ORIGIN}/bizreview`
        },
        {
          source: "/bizreview/:path*",
          destination: `${BIZREVIEW_ORIGIN}/bizreview/:path*`
        }
      ]
    };
  }
};

export default nextConfig;
