import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "MicroFlowOps - Under Construction",
  description:
    "MicroFlowOps is currently under construction while the site is being updated.",
  alternates: { canonical: "/" }
};

export default function HomePage() {
  return (
    <main className="flex min-h-[70vh] items-center justify-center px-6 py-24">
      <section className="mx-auto w-full max-w-3xl text-center">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-inkMuted">
          MicroFlowOps
        </p>
        <h1 className="mt-5 font-display text-4xl text-ink md:text-6xl">
          Under Construction
        </h1>
        <p className="mx-auto mt-5 max-w-2xl text-lg text-inkMuted">
          The homepage is being updated. Existing tools and routed app pages remain
          available while this page is refreshed.
        </p>
      </section>
    </main>
  );
}
