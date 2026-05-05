import type { ReactNode } from "react";

interface ChoiceOption<T extends string | number> {
  value: T;
  label: string;
  description?: ReactNode;
}

interface ChoiceGroupProps<T extends string | number> {
  name: string;
  value: T | null;
  onChange: (value: T) => void;
  options: ReadonlyArray<ChoiceOption<T>>;
  columns?: 1 | 2;
}

export default function ChoiceGroup<T extends string | number>({
  name,
  value,
  onChange,
  options,
  columns = 1
}: ChoiceGroupProps<T>) {
  return (
    <fieldset>
      <legend className="sr-only">{name}</legend>
      <div className={columns === 2 ? "grid gap-3 sm:grid-cols-2" : "space-y-3"}>
        {options.map((option) => {
          const checked = value === option.value;
          return (
            <label
              key={String(option.value)}
              className={`block cursor-pointer rounded-2xl border p-4 transition focus-within:ring-2 focus-within:ring-ocean/45 focus-within:ring-offset-2 focus-within:ring-offset-sand ${
                checked
                  ? "border-ocean bg-ocean/10 text-ink shadow-glow"
                  : "border-cardBorder bg-surface text-ink hover:border-ocean/35"
              }`}
            >
              <input
                type="radio"
                name={name}
                value={String(option.value)}
                checked={checked}
                onChange={() => onChange(option.value)}
                className="sr-only"
              />
              <span className="block text-base font-semibold">{option.label}</span>
              {option.description ? (
                <span className={`mt-1 block text-sm ${checked ? "text-ink" : "text-inkMuted"}`}>
                  {option.description}
                </span>
              ) : null}
            </label>
          );
        })}
      </div>
    </fieldset>
  );
}
