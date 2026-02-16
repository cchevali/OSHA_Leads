import type { Config } from "tailwindcss";

const config: Config = {
  darkMode: "class",
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
        ink: "var(--color-ink)",
        inkMuted: "var(--color-ink-muted)",
        sand: "var(--color-sand)",
        sandStrong: "var(--color-sand-strong)",
        ocean: "#0f766e",
        oceanDark: "#0b5f58",
        sunrise: "#f59e0b",
        clay: "#c2410c",
        card: "var(--color-card)",
        cardBorder: "var(--color-card-border)",
        surface: "var(--color-surface)"
      },
      boxShadow: {
        soft: "0 18px 45px -24px var(--shadow-color)",
        glow: "0 12px 40px -20px rgba(15, 118, 110, 0.6)"
      }
    }
  },
  plugins: []
};

export default config;
