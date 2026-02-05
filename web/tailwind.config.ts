import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}"
  ],
  theme: {
    extend: {
      fontFamily: {
        display: ["var(--font-display)", "ui-sans-serif", "system-ui"],
        body: ["var(--font-body)", "ui-sans-serif", "system-ui"]
      },
      colors: {
        ink: "#0f172a",
        inkMuted: "#334155",
        sand: "#f6f1e7",
        sandStrong: "#efe6d7",
        ocean: "#0f766e",
        oceanDark: "#0b5f58",
        sunrise: "#f59e0b",
        clay: "#c2410c"
      },
      boxShadow: {
        soft: "0 18px 45px -24px rgba(15, 23, 42, 0.45)",
        glow: "0 12px 40px -20px rgba(15, 118, 110, 0.6)"
      }
    }
  },
  plugins: []
};

export default config;
