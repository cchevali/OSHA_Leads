import type { ReactNode } from "react";

interface CheckboxOption {
  id: string;
  label: string;
  description?: ReactNode;
}

interface CheckboxCardsProps {
  options: CheckboxOption[];
  values: Record<string, boolean>;
  onToggle: (id: string, checked: boolean) => void;
}

export default function CheckboxCards({ options, values, onToggle }: CheckboxCardsProps) {
  return (
    <fieldset>
      <legend className="sr-only">Deal breakers</legend>
      <div className="space-y-3">
        {options.map((option) => {
          const checked = Boolean(values[option.id]);
          return (
            <label
              key={option.id}
              className={`block cursor-pointer rounded-2xl border p-4 transition focus-within:ring-2 focus-within:ring-ocean/45 focus-within:ring-offset-2 focus-within:ring-offset-sand ${
                checked
                  ? "border-clay bg-clay/10 text-ink"
                  : "border-cardBorder bg-surface text-ink hover:border-ocean/35"
              }`}
            >
              <span className="flex items-start gap-3">
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={(event) => onToggle(option.id, event.target.checked)}
                  className="mt-1 h-5 w-5 rounded border-cardBorder text-ocean focus:ring-ocean"
                />
                <span className="block">
                  <span className="block text-base font-semibold">{option.label}</span>
                  {option.description ? (
                    <span className={`mt-1 block text-sm ${checked ? "text-ink" : "text-inkMuted"}`}>
                      {option.description}
                    </span>
                  ) : null}
                </span>
              </span>
            </label>
          );
        })}
      </div>
    </fieldset>
  );
}
