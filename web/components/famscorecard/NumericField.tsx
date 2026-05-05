interface NumericFieldProps {
  id: string;
  label: string;
  hint?: string;
  prefix?: string;
  suffix?: string;
  value: number | null;
  placeholder?: string;
  onChange: (value: number | null) => void;
}

export default function NumericField({
  id,
  label,
  hint,
  prefix,
  suffix,
  value,
  placeholder,
  onChange
}: NumericFieldProps) {
  return (
    <label htmlFor={id} className="block rounded-2xl border border-cardBorder bg-surface p-4">
      <span className="block text-base font-semibold text-ink">{label}</span>
      {hint ? <span className="mt-1 block text-sm text-inkMuted">{hint}</span> : null}
      <span className="mt-3 flex items-center gap-3 rounded-2xl border border-cardBorder bg-card px-4 py-3">
        {prefix ? <span className="text-sm font-semibold text-inkMuted">{prefix}</span> : null}
        <input
          id={id}
          type="number"
          inputMode="numeric"
          min={0}
          value={value ?? ""}
          placeholder={placeholder}
          onChange={(event) => {
            const raw = event.target.value.trim();
            onChange(raw ? Number(raw) : null);
          }}
          className="w-full bg-transparent text-lg font-semibold text-ink outline-none placeholder:text-inkMuted/70"
        />
        {suffix ? <span className="text-sm font-semibold text-inkMuted">{suffix}</span> : null}
      </span>
    </label>
  );
}
