const BIZTRACKER_ORIGIN = "https://microflowops-biztracker.vercel.app";

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
        }
      ]
    };
  }
};

export default nextConfig;
