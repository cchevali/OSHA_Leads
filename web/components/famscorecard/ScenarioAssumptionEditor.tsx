import {
  formatCurrency,
  HOUSING_COST_FIT_OPTIONS,
  KID_TRAVEL_EASE_OPTIONS,
  MOVE_TIMING_OPTIONS,
  SCENARIO_FIT_OPTIONS,
  SCENARIO_META,
  VISIT_BURDEN_SHARE_OPTIONS,
  VISIT_SYSTEM_FIT_OPTIONS
} from "@/lib/famscorecard/questionnaire";
import type { ScenarioAssumptions, ScenarioId } from "@/lib/famscorecard/types";

interface ScenarioAssumptionEditorProps {
  scenarioId: ScenarioId;
  assumptions: ScenarioAssumptions;
  minimumSafeIncome: number | null;
  minimumCashCushion: number | null;
  onChange: <K extends keyof ScenarioAssumptions>(field: K, value: ScenarioAssumptions[K]) => void;
}

function SelectField({
  label,
  value,
  onChange,
  options
}: {
  label: string;
  value: string | number;
  onChange: (value: string) => void;
  options: Array<{ value: string | number; label: string }>;
}) {
  return (
    <label className="block rounded-2xl border border-cardBorder bg-card p-4">
      <span className="block text-sm font-semibold text-ink">{label}</span>
      <select
        value={String(value)}
        onChange={(event) => onChange(event.target.value)}
        className="mt-3 min-h-12 w-full rounded-2xl border border-cardBorder bg-surface px-4 text-base text-ink outline-none transition focus:border-ocean"
      >
        {options.map((option) => (
          <option key={String(option.value)} value={String(option.value)}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}

export default function ScenarioAssumptionEditor({
  scenarioId,
  assumptions,
  minimumSafeIncome,
  minimumCashCushion,
  onChange
}: ScenarioAssumptionEditorProps) {
  const scenario = SCENARIO_META[scenarioId];

  return (
    <div className="rounded-[28px] border border-cardBorder bg-card p-5 shadow-soft">
      <div className="space-y-2">
        <p className="text-xs font-semibold uppercase tracking-[0.26em] text-ocean">{scenario.shortLabel}</p>
        <h3 className="font-display text-2xl text-ink">{scenario.label}</h3>
        <p className="text-sm leading-6 text-inkMuted">{scenario.description}</p>
      </div>

      <div className="mt-5 grid gap-4 sm:grid-cols-2">
        <SelectField
          label={`Expected income vs. ${formatCurrency(minimumSafeIncome)} safety floor`}
          value={assumptions.incomeFit}
          onChange={(value) => onChange("incomeFit", Number(value) as ScenarioAssumptions["incomeFit"])}
          options={[...SCENARIO_FIT_OPTIONS]}
        />
        <SelectField
          label="Income stability"
          value={assumptions.incomeStability}
          onChange={(value) => onChange("incomeStability", Number(value) as ScenarioAssumptions["incomeStability"])}
          options={[...SCENARIO_FIT_OPTIONS]}
        />
        <SelectField
          label="Insurance quality"
          value={assumptions.insuranceQuality}
          onChange={(value) => onChange("insuranceQuality", Number(value) as ScenarioAssumptions["insuranceQuality"])}
          options={[...SCENARIO_FIT_OPTIONS]}
        />
        <SelectField
          label={`Cash cushion vs. ${formatCurrency(minimumCashCushion)} minimum`}
          value={assumptions.cashCushionFit}
          onChange={(value) => onChange("cashCushionFit", Number(value) as ScenarioAssumptions["cashCushionFit"])}
          options={[...SCENARIO_FIT_OPTIONS]}
        />
        <SelectField
          label="Distance from family support"
          value={assumptions.familySupportAccess}
          onChange={(value) =>
            onChange("familySupportAccess", Number(value) as ScenarioAssumptions["familySupportAccess"])
          }
          options={[...SCENARIO_FIT_OPTIONS]}
        />
        <SelectField
          label="Extended-family access"
          value={assumptions.familyVisitEase}
          onChange={(value) => onChange("familyVisitEase", Number(value) as ScenarioAssumptions["familyVisitEase"])}
          options={[...SCENARIO_FIT_OPTIONS]}
        />
        <SelectField
          label="Spontaneity of visits"
          value={assumptions.visitSpontaneity}
          onChange={(value) =>
            onChange("visitSpontaneity", Number(value) as ScenarioAssumptions["visitSpontaneity"])
          }
          options={[...SCENARIO_FIT_OPTIONS]}
        />
        <SelectField
          label="Travel burden with kids"
          value={assumptions.kidTravelEase}
          onChange={(value) => onChange("kidTravelEase", Number(value) as ScenarioAssumptions["kidTravelEase"])}
          options={[...KID_TRAVEL_EASE_OPTIONS]}
        />
        <SelectField
          label="Visit burden on your household"
          value={assumptions.householdTravelBurdenFit}
          onChange={(value) =>
            onChange("householdTravelBurdenFit", Number(value) as ScenarioAssumptions["householdTravelBurdenFit"])
          }
          options={[...VISIT_BURDEN_SHARE_OPTIONS]}
        />
        <SelectField
          label="Repeatable visit system"
          value={assumptions.visitSystemSupport}
          onChange={(value) =>
            onChange("visitSystemSupport", Number(value) as ScenarioAssumptions["visitSystemSupport"])
          }
          options={[...VISIT_SYSTEM_FIT_OPTIONS]}
        />
        <SelectField
          label="Move timing"
          value={assumptions.moveTiming}
          onChange={(value) => onChange("moveTiming", value as ScenarioAssumptions["moveTiming"])}
          options={MOVE_TIMING_OPTIONS}
        />
        <SelectField
          label="Schedule control"
          value={assumptions.scheduleControl}
          onChange={(value) => onChange("scheduleControl", Number(value) as ScenarioAssumptions["scheduleControl"])}
          options={[...SCENARIO_FIT_OPTIONS]}
        />
        <SelectField
          label="Overall disruption level"
          value={assumptions.disruptionLevel}
          onChange={(value) => onChange("disruptionLevel", Number(value) as ScenarioAssumptions["disruptionLevel"])}
          options={[...SCENARIO_FIT_OPTIONS]}
        />
        <SelectField
          label="Housing and lifestyle fit"
          value={assumptions.housingLifestyleFit}
          onChange={(value) =>
            onChange("housingLifestyleFit", Number(value) as ScenarioAssumptions["housingLifestyleFit"])
          }
          options={[...SCENARIO_FIT_OPTIONS]}
        />
        <SelectField
          label="Housing cost"
          value={assumptions.housingCostFit}
          onChange={(value) => onChange("housingCostFit", value as ScenarioAssumptions["housingCostFit"])}
          options={HOUSING_COST_FIT_OPTIONS}
        />
        <div className="sm:col-span-2">
          <SelectField
            label="Business day-to-day burden"
            value={assumptions.operationalResilience}
            onChange={(value) =>
              onChange("operationalResilience", Number(value) as ScenarioAssumptions["operationalResilience"])
            }
            options={[...SCENARIO_FIT_OPTIONS]}
          />
        </div>
      </div>
    </div>
  );
}
