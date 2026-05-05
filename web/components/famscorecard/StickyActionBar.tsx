interface StickyActionBarProps {
  canGoBack: boolean;
  onBack: () => void;
  onContinue: () => void;
  continueLabel: string;
  backLabel?: string;
  disabled?: boolean;
}

export default function StickyActionBar({
  canGoBack,
  onBack,
  onContinue,
  continueLabel,
  backLabel = "Back",
  disabled = false
}: StickyActionBarProps) {
  return (
    <div
      className="fixed inset-x-0 bottom-0 z-40 border-t border-cardBorder bg-sand/95 backdrop-blur"
      data-famscorecard-screen-only="true"
    >
      <div className="mx-auto flex w-full max-w-5xl items-center gap-3 px-4 pb-[calc(1rem+env(safe-area-inset-bottom))] pt-3 sm:px-6">
        {canGoBack ? (
          <button
            type="button"
            onClick={onBack}
            className="min-h-14 flex-1 rounded-full border border-cardBorder bg-card px-5 text-base font-semibold text-ink transition hover:border-ocean/35"
          >
            {backLabel}
          </button>
        ) : (
          <div className="hidden flex-1 sm:block" />
        )}
        <button
          type="button"
          onClick={onContinue}
          disabled={disabled}
          className="min-h-14 flex-[1.25] rounded-full bg-ocean px-5 text-base font-semibold text-white shadow-glow transition hover:bg-oceanDark disabled:cursor-not-allowed disabled:bg-ocean/45 disabled:shadow-none"
        >
          {continueLabel}
        </button>
      </div>
    </div>
  );
}
