"use client";

type MetroOption = {
  cbsaCode: string;
  label: string;
  state: string;
};

type MetroPickerProps = {
  options: MetroOption[];
  selectedCodes: string[];
  maxMetros: number;
  query: string;
  setQuery: (value: string) => void;
  onToggle: (code: string) => void;
};

export default function MetroPicker({
  options,
  selectedCodes,
  maxMetros,
  query,
  setQuery,
  onToggle
}: MetroPickerProps) {
  const selectedSet = new Set(selectedCodes);
  const normalizedQuery = query.trim().toLowerCase();
  const filtered = options.filter((option) => {
    if (!normalizedQuery) return true;
    const searchText = `${option.label} ${option.state} ${option.cbsaCode}`.toLowerCase();
    return searchText.includes(normalizedQuery);
  });

  return (
    <div className="space-y-3">
      <label className="grid gap-2 text-sm text-inkMuted">
        Search metros (CBSA/MSA)
        <input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Type metro, state, or CBSA code"
          className="rounded-xl border border-cardBorder bg-surface px-3 py-2 text-ink outline-none focus:border-ocean"
        />
      </label>

      <div className="rounded-2xl border border-cardBorder bg-surface p-3">
        <p className="text-xs text-inkMuted">
          Selected {selectedCodes.length} of {maxMetros} metros.
        </p>
        <div className="mt-2 max-h-64 space-y-2 overflow-y-auto pr-1">
          {filtered.map((option) => {
            const selected = selectedSet.has(option.cbsaCode);
            return (
              <button
                key={option.cbsaCode}
                type="button"
                onClick={() => onToggle(option.cbsaCode)}
                className={`flex w-full items-center justify-between rounded-xl border px-3 py-2 text-left text-sm transition ${
                  selected
                    ? "border-ocean bg-ocean/10 text-ink"
                    : "border-cardBorder bg-card text-inkMuted hover:border-ocean/50"
                }`}
              >
                <span className="font-medium">{option.label}</span>
                <span className="text-xs font-semibold">{option.cbsaCode}</span>
              </button>
            );
          })}
          {filtered.length === 0 && <p className="text-sm text-inkMuted">No metros found.</p>}
        </div>
      </div>
    </div>
  );
}
