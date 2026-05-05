import {
  AssessmentResults,
  BabyAgeValue,
  DealBreakerId,
  HousingCostFit,
  MoveTimingValue,
  PlannedVisitTradeoffAnswer,
  ScaleValue,
  SecondaryIncomeBurdenValue,
  SecondaryIncomeRoleType,
  SecondaryIncomeSafetyValue,
  ScenarioId,
  ScorecardState,
  StepId,
  VisitSystemHelpValue,
  WeeklyLifeTradeoffValue
} from "@/lib/famscorecard/types";

export const SCHEMA_VERSION = 7;
export const STORAGE_KEY = "microflowops:famscorecard:v7";
export const LEGACY_STORAGE_KEYS = [
  "microflowops:famscorecard:v6",
  "microflowops:famscorecard:v5",
  "microflowops:famscorecard:v4",
  "microflowops:famscorecard:v3",
  "microflowops:famscorecard:v2",
  "microflowops:famscorecard:v1"
] as const;

const V1_STEP_ORDER = [
  "intro",
  "priorities",
  "family",
  "finance",
  "lifestyle",
  "relocation",
  "business",
  "nonNegotiables",
  "scenarioReview",
  "results"
] as const;

const V2_STEP_ORDER = [
  "intro",
  "priorities",
  "family",
  "finance",
  "lifestyle",
  "relocation",
  "trust",
  "businessModel",
  "nonNegotiables",
  "results"
] as const;

const V3_STEP_ORDER = [
  "intro",
  "priorities",
  "family",
  "familyAccess",
  "finance",
  "lifestyle",
  "relocation",
  "trust",
  "businessModel",
  "nonNegotiables",
  "results"
] as const;

const V4_STEP_ORDER = [
  "intro",
  "priorities",
  "family",
  "familyAccess",
  "finance",
  "secondaryIncome",
  "lifestyle",
  "relocation",
  "trust",
  "businessModel",
  "nonNegotiables",
  "results"
] as const;

const V5_STEP_ORDER = [
  "intro",
  "priorities",
  "family",
  "familyAccess",
  "finance",
  "secondaryIncome",
  "lifestyle",
  "relocation",
  "trust",
  "businessModel",
  "nonNegotiables",
  "results"
] as const;

const V6_STEP_ORDER = [
  "intro",
  "priorities",
  "family",
  "familyAccess",
  "finance",
  "secondaryIncome",
  "lifestyle",
  "relocation",
  "trust",
  "businessModel",
  "nonNegotiables",
  "results"
] as const;

type LegacyStepId =
  | (typeof V1_STEP_ORDER)[number]
  | (typeof V2_STEP_ORDER)[number]
  | (typeof V3_STEP_ORDER)[number]
  | (typeof V4_STEP_ORDER)[number]
  | (typeof V5_STEP_ORDER)[number]
  | (typeof V6_STEP_ORDER)[number];

const LEGACY_STEP_MAP: Record<LegacyStepId, StepId> = {
  intro: "intro",
  priorities: "priorities",
  family: "family",
  familyAccess: "familyAccess",
  finance: "finance",
  secondaryIncome: "secondaryIncome",
  lifestyle: "lifestyle",
  relocation: "relocation",
  business: "trust",
  trust: "trust",
  businessModel: "businessModel",
  nonNegotiables: "nonNegotiables",
  scenarioReview: "results",
  results: "results"
};

export const STEPS: Array<{
  id: StepId;
  title: string;
  description: string;
}> = [
  {
    id: "intro",
    title: "Family Fit Scorecard",
    description:
      "Clarify whether buying a business and possibly relocating fits your household right now, later, or not at all."
  },
  {
    id: "priorities",
    title: "Household priorities",
    description: "Start with what matters most to family life before weighing any plan."
  },
  {
    id: "family",
    title: "Family support and baby stage",
    description: "Surface timing, support, and disruption realities before anything else."
  },
  {
    id: "familyAccess",
    title: "Extended family access",
    description:
      "Treat convenience, spontaneity, visit burden, and family-distance tradeoffs as a real household factor."
  },
  {
    id: "finance",
    title: "Financial safety and risk",
    description: "Set the household safety rails before comparing change paths."
  },
  {
    id: "secondaryIncome",
    title: "Secondary income buffer",
    description:
      "Assess whether optional outside income would actually reduce transition stress or simply add more household burden."
  },
  {
    id: "lifestyle",
    title: "Family lifestyle and schedule impact",
    description: "Pressure-test whether this would help family life or simply shift the strain."
  },
  {
    id: "relocation",
    title: "Relocation tolerance",
    description: "Clarify what distance, travel friction, and lifestyle change actually fit."
  },
  {
    id: "trust",
    title: "Trust in the plan",
    description: "Measure current trust, comfort, and household confidence in the path."
  },
  {
    id: "businessModel",
    title: "Business model reality",
    description: "Only then look at the structural business traits that protect family life."
  },
  {
    id: "nonNegotiables",
    title: "Non-negotiables",
    description: "Mark the lines that should stop the process instead of being argued past."
  },
  {
    id: "results",
    title: "Review and results",
    description: "Adjust scenario assumptions, compare paths, and review the household-fit conclusion."
  }
];

const FAMILY_ACCESS_STEP_INDEX = STEPS.findIndex((step) => step.id === "familyAccess");
const SECONDARY_INCOME_STEP_INDEX = STEPS.findIndex((step) => step.id === "secondaryIncome");

export const SCENARIO_META: Record<
  ScenarioId,
  { label: string; shortLabel: string; description: string }
> = {
  stay: {
    label: "Stay put",
    shortLabel: "Stay put",
    description:
      "Keeps the strongest extended-family access, lowest visit friction, and least moving disruption, but may preserve less schedule control."
  },
  buyLocal: {
    label: "Buy a business without major relocation",
    shortLabel: "Buy local",
    description:
      "Adds ownership risk and operating burden while keeping family access relatively workable and visit logistics much lighter."
  },
  buyRelocate: {
    label: "Buy a business with relocation",
    shortLabel: "Buy + relocate",
    description:
      "Carries the highest disruption, lowest spontaneity of family visits, and strongest need for timing and family-distance guardrails."
  }
};

export const BABY_AGE_OPTIONS: Array<{ value: BabyAgeValue; label: string }> = [
  { value: "under6", label: "Under 6 months" },
  { value: "6to12", label: "6-12 months" },
  { value: "12to18", label: "12-18 months" },
  { value: "18to24", label: "18-24 months" },
  { value: "2plus", label: "2+ years" },
  { value: "not-sure", label: "Not sure yet" }
];

export const MOVE_TIMING_OPTIONS: Array<{ value: MoveTimingValue; label: string }> = [
  { value: "no-move", label: "No move needed" },
  { value: "under6", label: "Within 6 months" },
  { value: "6to12", label: "In 6-12 months" },
  { value: "12to18", label: "In 12-18 months" },
  { value: "18to24", label: "In 18-24 months" },
  { value: "2plus", label: "2+ years out" }
];

export const IMPORTANCE_OPTIONS = [
  { value: 1, label: "Not important" },
  { value: 2, label: "A little important" },
  { value: 3, label: "Moderately important" },
  { value: 4, label: "Very important" },
  { value: 5, label: "Essential" }
] as const;

export const DISRUPTION_OPTIONS = [
  { value: 1, label: "Not very disruptive" },
  { value: 2, label: "Manageable" },
  { value: 3, label: "Meaningful" },
  { value: 4, label: "Very disruptive" },
  { value: 5, label: "Too disruptive right now" }
] as const;

export const CONCERN_OPTIONS = [
  { value: 1, label: "Not concerned" },
  { value: 2, label: "Slightly concerned" },
  { value: 3, label: "Moderately concerned" },
  { value: 4, label: "Very concerned" },
  { value: 5, label: "Extremely concerned" }
] as const;

export const RELIANCE_OPTIONS = [
  { value: 1, label: "Rarely" },
  { value: 2, label: "Once in a while" },
  { value: 3, label: "A few times a month" },
  { value: 4, label: "Weekly" },
  { value: 5, label: "Several times a week" }
] as const;

export const OPENNESS_OPTIONS = [
  { value: 1, label: "Not open" },
  { value: 2, label: "A little open" },
  { value: 3, label: "Somewhat open" },
  { value: 4, label: "Quite open" },
  { value: 5, label: "Very open" }
] as const;

export const CONFIDENCE_OPTIONS = [
  { value: 1, label: "Not confident" },
  { value: 2, label: "A little confident" },
  { value: 3, label: "Somewhat confident" },
  { value: 4, label: "Quite confident" },
  { value: 5, label: "Very confident" }
] as const;

export const TOLERANCE_OPTIONS = [
  { value: 1, label: "Almost none" },
  { value: 2, label: "A little" },
  { value: 3, label: "Some" },
  { value: 4, label: "A fair amount" },
  { value: 5, label: "Quite a bit" }
] as const;

export const VOLATILITY_OPTIONS = [
  { value: 1, label: "Only extreme swings" },
  { value: 2, label: "Large swings" },
  { value: 3, label: "Moderate swings" },
  { value: 4, label: "Even modest swings" },
  { value: 5, label: "Very little volatility" }
] as const;

export const ALIGNMENT_OPTIONS = [
  { value: 1, label: "Not aligned" },
  { value: 2, label: "Some tension" },
  { value: 3, label: "Mixed" },
  { value: 4, label: "Mostly aligned" },
  { value: 5, label: "Fully aligned" }
] as const;

export const WORTH_IT_OPTIONS = [
  { value: 1, label: "Not worth it" },
  { value: 2, label: "Probably not" },
  { value: 3, label: "Maybe" },
  { value: 4, label: "Probably yes" },
  { value: 5, label: "Yes, if the fit is real" }
] as const;

export const DELAY_OPTIONS = [
  { value: "yes", label: "Yes, a delay would help a lot" },
  { value: "maybe", label: "Maybe, depending on the setup" },
  { value: "no", label: "No, delay would not change much" }
] as const;

export const LOCAL_ACQUISITION_OPTIONS = [
  { value: "yes", label: "Yes" },
  { value: "not-sure", label: "Not sure" },
  { value: "no", label: "No" }
] as const;

export const PLANNED_VISIT_TRADEOFF_OPTIONS: Array<{
  value: PlannedVisitTradeoffAnswer;
  label: string;
}> = [
  { value: "yes", label: "Yes, likely acceptable" },
  { value: "maybe", label: "Maybe, depending on the overall setup" },
  { value: "no", label: "No, the loss of easy access would still feel too costly" }
];

export const WEEKLY_LIFE_TRADEOFF_OPTIONS: Array<{
  value: WeeklyLifeTradeoffValue;
  label: string;
}> = [
  { value: "slightly", label: "Slightly better" },
  { value: "moderately", label: "Moderately better" },
  { value: "clearly", label: "Clearly better" },
  { value: "much", label: "Much better" },
  { value: "not-worth-it", label: "I do not think the tradeoff would be worth it" }
];

export const VISIT_SYSTEM_HELP_OPTIONS: Array<{
  value: VisitSystemHelpValue;
  label: string;
}> = [
  { value: "yes", label: "Yes, materially" },
  { value: "somewhat", label: "Somewhat" },
  { value: "not-much", label: "Not much" },
  { value: "no", label: "No" }
];

export const SECONDARY_INCOME_BUFFER_OPTIONS: Array<{
  value: SecondaryIncomeSafetyValue;
  label: string;
}> = [
  { value: "material", label: "Yes, materially safer" },
  { value: "somewhat", label: "Somewhat safer" },
  { value: "not-much", label: "Not much difference" },
  { value: "no-plan", label: "No, I would not want that to be part of the plan" }
];

export const SECONDARY_INCOME_BURDEN_OPTIONS: Array<{
  value: SecondaryIncomeBurdenValue;
  label: string;
}> = [
  { value: "helpful", label: "Helpful and stabilizing" },
  { value: "depends", label: "Potentially helpful, depending on schedule" },
  { value: "neutral", label: "Neutral" },
  { value: "burden", label: "More burden than benefit" },
  { value: "not-acceptable", label: "Not acceptable" }
];

export const SECONDARY_INCOME_ROLE_OPTIONS: Array<{
  value: SecondaryIncomeRoleType;
  label: string;
}> = [
  { value: "school-community", label: "School, library, or community role" },
  { value: "administrative", label: "Administrative or office role" },
  { value: "remote", label: "Flexible remote role" },
  { value: "not-sure", label: "Not sure" },
  { value: "not-planning", label: "I would not want to plan around this" }
];

export const HOUSING_RULE_OPTIONS = [
  { value: "any", label: "Housing cost can be higher if the family fit is strong" },
  { value: "comparable", label: "Housing should be lower or roughly comparable" },
  { value: "lower", label: "Housing needs to be clearly lower" }
] as const;

export const SCENARIO_FIT_OPTIONS = [
  { value: 1, label: "Poor fit" },
  { value: 2, label: "Below target" },
  { value: 3, label: "Mixed" },
  { value: 4, label: "Solid" },
  { value: 5, label: "Strong fit" }
] as const;

export const HOUSING_COST_FIT_OPTIONS: Array<{ value: HousingCostFit; label: string }> = [
  { value: "higher", label: "Higher cost" },
  { value: "comparable", label: "Comparable cost" },
  { value: "lower", label: "Lower cost" }
];

export const KID_TRAVEL_EASE_OPTIONS = [
  { value: 1, label: "Very hard" },
  { value: 2, label: "Heavy lift" },
  { value: 3, label: "Mixed" },
  { value: 4, label: "Manageable" },
  { value: 5, label: "Straightforward" }
] as const;

export const VISIT_BURDEN_SHARE_OPTIONS = [
  { value: 1, label: "Mostly on your household" },
  { value: 2, label: "Often on your household" },
  { value: 3, label: "Mixed" },
  { value: 4, label: "Mostly shared or flexible" },
  { value: 5, label: "Rarely on your household" }
] as const;

export const VISIT_SYSTEM_FIT_OPTIONS = [
  { value: 1, label: "No real system" },
  { value: 2, label: "Thin plan" },
  { value: 3, label: "Partial system" },
  { value: 4, label: "Solid system" },
  { value: 5, label: "Repeatable system" }
] as const;

export const DEAL_BREAKER_COPY: Record<
  DealBreakerId,
  { label: string; description: string }
> = {
  familySupportDistance: {
    label: "Too far from family support",
    description: "If practical help drops too much, the scenario should stop instead of being rationalized."
  },
  weakInsurance: {
    label: "Weak health insurance",
    description: "Comparable or better coverage is required for the household to feel safe."
  },
  incomeUncertainty: {
    label: "Too much income uncertainty",
    description: "Year-one income cannot feel too shaky for the family to absorb."
  },
  moveBeforeBabyAge: {
    label: "Move before baby reaches the acceptable age",
    description: "Your timing threshold should act like a stop sign, not a suggestion."
  },
  eveningWeekendBurden: {
    label: "Too much evening or weekend burden",
    description: "Family life cannot absorb too much after-hours operator strain."
  },
  sellerDependentBusiness: {
    label: "Seller-dependent business",
    description: "If the business only works when one person saves the day, it is out."
  },
  cashCushion: {
    label: "Not enough cash cushion",
    description: "Post-close cash needs to stay at or above the comfort line you set."
  },
  familyLifestyle: {
    label: "Bad schools or weak family lifestyle fit",
    description: "If the overall family setup is not clearly good enough, the scenario should fail."
  },
  spouseTiming: {
    label: "Household not comfortable with the timing",
    description: "If the timing does not feel mutually supportable, the timing is wrong."
  }
};

const MOVE_TIMING_VALUES = MOVE_TIMING_OPTIONS.map((option) => option.value);
const BABY_AGE_VALUES = BABY_AGE_OPTIONS.map((option) => option.value);
const DELAY_VALUES = DELAY_OPTIONS.map((option) => option.value);
const HOUSING_RULE_VALUES = HOUSING_RULE_OPTIONS.map((option) => option.value);
const LOCAL_ACQUISITION_VALUES = LOCAL_ACQUISITION_OPTIONS.map((option) => option.value);
const HOUSING_COST_FIT_VALUES = HOUSING_COST_FIT_OPTIONS.map((option) => option.value);
const PLANNED_VISIT_TRADEOFF_VALUES = PLANNED_VISIT_TRADEOFF_OPTIONS.map((option) => option.value);
const WEEKLY_LIFE_TRADEOFF_VALUES = WEEKLY_LIFE_TRADEOFF_OPTIONS.map((option) => option.value);
const VISIT_SYSTEM_HELP_VALUES = VISIT_SYSTEM_HELP_OPTIONS.map((option) => option.value);
const SECONDARY_INCOME_BUFFER_VALUES = SECONDARY_INCOME_BUFFER_OPTIONS.map((option) => option.value);
const SECONDARY_INCOME_BURDEN_VALUES = SECONDARY_INCOME_BURDEN_OPTIONS.map((option) => option.value);
function createDealBreakers(): Record<DealBreakerId, boolean> {
  return {
    familySupportDistance: false,
    weakInsurance: false,
    incomeUncertainty: false,
    moveBeforeBabyAge: false,
    eveningWeekendBurden: false,
    sellerDependentBusiness: false,
    cashCushion: false,
    familyLifestyle: false,
    spouseTiming: false
  };
}

export function createDefaultState(): ScorecardState {
  return {
    schemaVersion: SCHEMA_VERSION,
    currentStep: 0,
    lastSavedAt: null,
    priorities: {
      familyLifestyle: null,
      stayCloseSupport: null
    },
    family: {
      moveDisruption: null,
      delayedMoveChangesAnswer: null,
      minimumBabyAgeForMove: null,
      familySupportReliance: null
    },
    familyAccess: {
      youngKidTravelDifficulty: null,
      plannedVisitsAcceptable: null,
      weeklyLifeImprovementNeeded: null,
      repeatableVisitSystemHelp: null
    },
    finance: {
      minimumSafeIncome: null,
      insuranceContinuity: null,
      insurancePathConfidence: null,
      yearOneUncertaintyTolerance: null,
      minimumCashCushion: null
    },
    secondaryIncome: {
      outsideIncomeSafetyBuffer: null,
      outsideIncomeRoleFeel: null,
      notRelyOnOutsideIncomeImportance: null
    },
    lifestyle: {
      afterHoursBurdenTolerance: null
    },
    relocation: {
      outOfStateOpenness: null,
      localAcquisitionAcceptable: null
    },
    trust: {
      operatingPlanTrust: null
    },
    businessModel: {
      stableTeamImportance: null
    },
    nonNegotiables: {
      spouseTimingAlignment: null,
      dealBreakers: createDealBreakers()
    },
    scenarios: {
      stay: {
        incomeFit: 4,
        incomeStability: 5,
        insuranceQuality: 5,
        cashCushionFit: 4,
        familySupportAccess: 5,
        familyVisitEase: 5,
        visitSpontaneity: 5,
        kidTravelEase: 5,
        householdTravelBurdenFit: 5,
        visitSystemSupport: 4,
        moveTiming: "no-move",
        scheduleControl: 2,
        disruptionLevel: 5,
        housingLifestyleFit: 3,
        housingCostFit: "comparable",
        operationalResilience: 5
      },
      buyLocal: {
        incomeFit: 3,
        incomeStability: 3,
        insuranceQuality: 3,
        cashCushionFit: 3,
        familySupportAccess: 4,
        familyVisitEase: 4,
        visitSpontaneity: 4,
        kidTravelEase: 4,
        householdTravelBurdenFit: 4,
        visitSystemSupport: 4,
        moveTiming: "no-move",
        scheduleControl: 4,
        disruptionLevel: 3,
        housingLifestyleFit: 3,
        housingCostFit: "comparable",
        operationalResilience: 3
      },
      buyRelocate: {
        incomeFit: 3,
        incomeStability: 3,
        insuranceQuality: 3,
        cashCushionFit: 3,
        familySupportAccess: 3,
        familyVisitEase: 2,
        visitSpontaneity: 2,
        kidTravelEase: 2,
        householdTravelBurdenFit: 2,
        visitSystemSupport: 3,
        moveTiming: "12to18",
        scheduleControl: 4,
        disruptionLevel: 3,
        housingLifestyleFit: 4,
        housingCostFit: "lower",
        operationalResilience: 3
      }
    }
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function clampScale(value: number, fallback: ScaleValue = 3): ScaleValue {
  if (!Number.isFinite(value)) {
    return fallback;
  }

  return Math.min(5, Math.max(1, Math.round(value))) as ScaleValue;
}

function parseScale(value: unknown): ScaleValue | null {
  return typeof value === "number" && value >= 1 && value <= 5 ? clampScale(value) : null;
}

function parseNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function parseStringValue<T extends string>(value: unknown, allowed: readonly T[]): T | null {
  return typeof value === "string" && allowed.includes(value as T) ? (value as T) : null;
}

function averageScale(values: Array<ScaleValue | null | undefined>, fallback: ScaleValue = 3): ScaleValue {
  const present = values.filter((value): value is ScaleValue => typeof value === "number");
  if (present.length === 0) {
    return fallback;
  }

  return clampScale(present.reduce((sum, value) => sum + value, 0) / present.length, fallback);
}

function invertScale(value: ScaleValue | null | undefined): ScaleValue | null {
  return typeof value === "number" ? clampScale(6 - value) : null;
}

function migrateDealBreakers(raw: unknown) {
  const base = createDealBreakers();
  if (!isRecord(raw)) {
    return base;
  }

  return {
    familySupportDistance: Boolean(raw.familySupportDistance),
    weakInsurance: Boolean(raw.weakInsurance),
    incomeUncertainty: Boolean(raw.incomeUncertainty),
    moveBeforeBabyAge: Boolean(raw.moveBeforeBabyAge),
    eveningWeekendBurden: Boolean(raw.eveningWeekendBurden),
    sellerDependentBusiness: Boolean(raw.sellerDependentBusiness),
    cashCushion: Boolean(raw.cashCushion),
    familyLifestyle: Boolean(raw.familyLifestyle),
    spouseTiming: Boolean(raw.spouseTiming)
  };
}

function migrateScenario(
  raw: unknown,
  base: ScorecardState["scenarios"][ScenarioId]
): ScorecardState["scenarios"][ScenarioId] {
  if (!isRecord(raw)) {
    return base;
  }

  return {
    incomeFit: parseScale(raw.incomeFit) ?? base.incomeFit,
    incomeStability: parseScale(raw.incomeStability) ?? base.incomeStability,
    insuranceQuality: parseScale(raw.insuranceQuality) ?? base.insuranceQuality,
    cashCushionFit: parseScale(raw.cashCushionFit) ?? base.cashCushionFit,
    familySupportAccess: parseScale(raw.familySupportAccess) ?? base.familySupportAccess,
    familyVisitEase: parseScale(raw.familyVisitEase) ?? base.familyVisitEase,
    visitSpontaneity: parseScale(raw.visitSpontaneity) ?? base.visitSpontaneity,
    kidTravelEase: parseScale(raw.kidTravelEase) ?? base.kidTravelEase,
    householdTravelBurdenFit:
      parseScale(raw.householdTravelBurdenFit) ?? base.householdTravelBurdenFit,
    visitSystemSupport: parseScale(raw.visitSystemSupport) ?? base.visitSystemSupport,
    moveTiming: parseStringValue(raw.moveTiming, MOVE_TIMING_VALUES) ?? base.moveTiming,
    scheduleControl: parseScale(raw.scheduleControl) ?? base.scheduleControl,
    disruptionLevel: parseScale(raw.disruptionLevel) ?? base.disruptionLevel,
    housingLifestyleFit: parseScale(raw.housingLifestyleFit) ?? base.housingLifestyleFit,
    housingCostFit: parseStringValue(raw.housingCostFit, HOUSING_COST_FIT_VALUES) ?? base.housingCostFit,
    operationalResilience: parseScale(raw.operationalResilience) ?? base.operationalResilience
  };
}

function resolveCurrentStep(stepIndex: unknown, schemaVersion: unknown): number {
  if (typeof stepIndex !== "number" || Number.isNaN(stepIndex)) {
    return 0;
  }

  const normalizedIndex = Math.max(0, Math.floor(stepIndex));
  if (schemaVersion === SCHEMA_VERSION) {
    return Math.min(normalizedIndex, STEPS.length - 1);
  }

  const legacyOrder =
    schemaVersion === 6
      ? V6_STEP_ORDER
      : schemaVersion === 5
      ? V5_STEP_ORDER
      : schemaVersion === 4
      ? V4_STEP_ORDER
      : schemaVersion === 3
        ? V3_STEP_ORDER
        : schemaVersion === 2
          ? V2_STEP_ORDER
          : V1_STEP_ORDER;
  const legacyId = legacyOrder[normalizedIndex];
  if (!legacyId) {
    return Math.min(normalizedIndex, STEPS.length - 1);
  }

  const nextId = LEGACY_STEP_MAP[legacyId];
  const nextIndex = STEPS.findIndex((step) => step.id === nextId);
  return nextIndex >= 0 ? nextIndex : 0;
}

export function migrateStoredState(raw: unknown): ScorecardState | null {
  if (!isRecord(raw)) {
    return null;
  }

  const base = createDefaultState();

  const priorities = isRecord(raw.priorities) ? raw.priorities : {};
  const family = isRecord(raw.family) ? raw.family : {};
  const familyAccess = isRecord(raw.familyAccess) ? raw.familyAccess : {};
  const finance = isRecord(raw.finance) ? raw.finance : {};
  const secondaryIncome = isRecord(raw.secondaryIncome) ? raw.secondaryIncome : {};
  const lifestyle = isRecord(raw.lifestyle) ? raw.lifestyle : {};
  const relocation = isRecord(raw.relocation) ? raw.relocation : {};
  const trust = isRecord(raw.trust) ? raw.trust : {};
  const businessModel = isRecord(raw.businessModel) ? raw.businessModel : {};
  const legacyBusiness = isRecord(raw.business) ? raw.business : {};
  const nonNegotiables = isRecord(raw.nonNegotiables) ? raw.nonNegotiables : {};
  const scenarios = isRecord(raw.scenarios) ? raw.scenarios : {};

  const legacyBusinessConfidence = parseScale(legacyBusiness.serviceBusinessConfidence);
  const legacyFreedomLoss = parseScale(legacyBusiness.freedomLossConcern);
  const legacyRecurringPreference = parseScale(lifestyle.recurringRevenuePreference);
  const legacyOwnerHeroAvoidance = parseScale(lifestyle.ownerHeroAvoidance);
  const legacyEveningWeekendTolerance = parseScale(lifestyle.eveningWeekendTolerance);
  const legacyTravelTolerance = parseScale(relocation.travelFrictionTolerance);
  const legacyDistanceOpenness = averageScale(
    [
      parseScale(relocation.outOfStateOpenness),
      parseScale(relocation.fartherFromParentsOpenness),
      legacyTravelTolerance
    ],
    3
  );
  const normalizedLegacyTravelTolerance = legacyTravelTolerance ?? 3;
  const migratedPlannedVisitsAcceptable =
    parseStringValue(familyAccess.plannedVisitsAcceptable, PLANNED_VISIT_TRADEOFF_VALUES) ??
    (legacyDistanceOpenness >= 4 ? "yes" : legacyDistanceOpenness <= 2 ? "no" : "maybe");
  const improvementThreshold =
    averageScale(
      [
        parseScale(lifestyle.familyLifeImprovementImportance),
        parseScale(relocation.relocationMustImproveLifestyle),
        parseScale(priorities.familyLifestyle)
      ],
      4
    );
  const migratedWeeklyLifeImprovementNeeded =
    parseStringValue(familyAccess.weeklyLifeImprovementNeeded, WEEKLY_LIFE_TRADEOFF_VALUES) ??
    (improvementThreshold >= 5
      ? "much"
      : improvementThreshold === 4
        ? "clearly"
        : improvementThreshold === 3
          ? "moderately"
          : "slightly");
  const migratedVisitSystemHelp =
    parseStringValue(familyAccess.repeatableVisitSystemHelp, VISIT_SYSTEM_HELP_VALUES) ??
    (normalizedLegacyTravelTolerance >= 4
      ? "yes"
      : normalizedLegacyTravelTolerance === 3
        ? "somewhat"
        : normalizedLegacyTravelTolerance === 2
          ? "not-much"
          : "no");

  const nextState: ScorecardState = {
    ...base,
    schemaVersion: SCHEMA_VERSION,
    currentStep: resolveCurrentStep(raw.currentStep, raw.schemaVersion),
    lastSavedAt: typeof raw.lastSavedAt === "string" ? raw.lastSavedAt : null,
    priorities: {
      familyLifestyle: parseScale(priorities.familyLifestyle),
      stayCloseSupport: parseScale(priorities.stayCloseSupport)
    },
    family: {
      moveDisruption: parseScale(family.moveDisruption),
      delayedMoveChangesAnswer: parseStringValue(family.delayedMoveChangesAnswer, DELAY_VALUES),
      minimumBabyAgeForMove: parseStringValue(family.minimumBabyAgeForMove, BABY_AGE_VALUES),
      familySupportReliance: parseScale(family.familySupportReliance)
    },
    familyAccess: {
      youngKidTravelDifficulty:
        parseScale(familyAccess.youngKidTravelDifficulty) ??
        averageScale(
          [
            parseScale(familyAccess.parentTravelLimitationConcern),
            parseScale(familyAccess.householdTravelBurdenConcern),
            parseScale(family.moveDisruption),
            parseScale(family.familySupportReliance),
            parseScale(family.familyHelpDrive)
          ],
          4
        ),
      plannedVisitsAcceptable: migratedPlannedVisitsAcceptable,
      weeklyLifeImprovementNeeded: migratedWeeklyLifeImprovementNeeded,
      repeatableVisitSystemHelp: migratedVisitSystemHelp
    },
    finance: {
      minimumSafeIncome: parseNumber(finance.minimumSafeIncome),
      insuranceContinuity: parseScale(finance.insuranceContinuity),
      insurancePathConfidence: parseScale(finance.insurancePathConfidence),
      yearOneUncertaintyTolerance: parseScale(finance.yearOneUncertaintyTolerance),
      minimumCashCushion: parseNumber(finance.minimumCashCushion)
    },
    secondaryIncome: {
      outsideIncomeSafetyBuffer: parseStringValue(
        secondaryIncome.outsideIncomeSafetyBuffer,
        SECONDARY_INCOME_BUFFER_VALUES
      ),
      outsideIncomeRoleFeel: parseStringValue(
        secondaryIncome.outsideIncomeRoleFeel,
        SECONDARY_INCOME_BURDEN_VALUES
      ),
      notRelyOnOutsideIncomeImportance: parseScale(
        secondaryIncome.notRelyOnOutsideIncomeImportance
      )
    },
    lifestyle: {
      afterHoursBurdenTolerance:
        parseScale(lifestyle.afterHoursBurdenTolerance) ?? legacyEveningWeekendTolerance
    },
    relocation: {
      outOfStateOpenness: parseScale(relocation.outOfStateOpenness),
      localAcquisitionAcceptable: parseStringValue(relocation.localAcquisitionAcceptable, LOCAL_ACQUISITION_VALUES)
    },
    trust: {
      operatingPlanTrust:
        parseScale(trust.operatingPlanTrust) ??
        averageScale(
          [
            legacyBusinessConfidence,
            invertScale(parseScale(trust.harderJobConcern) ?? legacyFreedomLoss),
            legacyOwnerHeroAvoidance,
            legacyRecurringPreference
          ],
          3
        )
    },
    businessModel: {
      stableTeamImportance:
        parseScale(businessModel.stableTeamImportance) ??
        averageScale([legacyOwnerHeroAvoidance, legacyRecurringPreference], 4)
    },
    nonNegotiables: {
      spouseTimingAlignment: parseScale(nonNegotiables.spouseTimingAlignment),
      dealBreakers: migrateDealBreakers(nonNegotiables.dealBreakers)
    },
    scenarios: {
      stay: migrateScenario(scenarios.stay, base.scenarios.stay),
      buyLocal: migrateScenario(scenarios.buyLocal, base.scenarios.buyLocal),
      buyRelocate: migrateScenario(scenarios.buyRelocate, base.scenarios.buyRelocate)
    }
  };

  const familyAccessRelevant =
    (nextState.priorities.stayCloseSupport ?? 0) >= 3 ||
    (nextState.family.familySupportReliance ?? 0) >= 3;
  const familyAccessAnswered =
    nextState.familyAccess.plannedVisitsAcceptable !== null &&
    (!familyAccessRelevant || nextState.familyAccess.youngKidTravelDifficulty !== null);
  if (!familyAccessAnswered && nextState.currentStep > FAMILY_ACCESS_STEP_INDEX) {
    nextState.currentStep = FAMILY_ACCESS_STEP_INDEX;
  }

  const secondaryIncomeRelevant =
    nextState.secondaryIncome.outsideIncomeSafetyBuffer === "material" ||
    nextState.secondaryIncome.outsideIncomeSafetyBuffer === "somewhat";
  const secondaryIncomeAnswered =
    nextState.secondaryIncome.outsideIncomeSafetyBuffer !== null &&
    (!secondaryIncomeRelevant ||
      (nextState.secondaryIncome.outsideIncomeRoleFeel !== null &&
        nextState.secondaryIncome.notRelyOnOutsideIncomeImportance !== null));
  if (!secondaryIncomeAnswered && nextState.currentStep > SECONDARY_INCOME_STEP_INDEX) {
    nextState.currentStep = SECONDARY_INCOME_STEP_INDEX;
  }

  return nextState;
}

export function loadStoredState(storage: Pick<Storage, "getItem">): {
  state: ScorecardState | null;
  sourceKey: string | null;
} {
  for (const key of [STORAGE_KEY, ...LEGACY_STORAGE_KEYS]) {
    const raw = storage.getItem(key);
    if (!raw) {
      continue;
    }

    try {
      const migrated = migrateStoredState(JSON.parse(raw));
      if (migrated) {
        return { state: migrated, sourceKey: key };
      }
    } catch {
      continue;
    }
  }

  return { state: null, sourceKey: null };
}

export function formatCurrency(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "your stated threshold";
  }

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0
  }).format(value);
}

export function getBabyAgeLabel(value: BabyAgeValue | null): string {
  return BABY_AGE_OPTIONS.find((option) => option.value === value)?.label || "the age you marked";
}

export function getPlannedVisitTradeoffLabel(value: PlannedVisitTradeoffAnswer | null): string {
  return PLANNED_VISIT_TRADEOFF_OPTIONS.find((option) => option.value === value)?.label || "Not answered";
}

export function getWeeklyLifeTradeoffLabel(value: WeeklyLifeTradeoffValue | null): string {
  return WEEKLY_LIFE_TRADEOFF_OPTIONS.find((option) => option.value === value)?.label || "Not answered";
}

export function getVisitSystemHelpLabel(value: VisitSystemHelpValue | null): string {
  return VISIT_SYSTEM_HELP_OPTIONS.find((option) => option.value === value)?.label || "Not answered";
}

export function getSecondaryIncomeBufferLabel(value: SecondaryIncomeSafetyValue | null): string {
  return SECONDARY_INCOME_BUFFER_OPTIONS.find((option) => option.value === value)?.label || "Not answered";
}

export function getSecondaryIncomeBurdenLabel(value: SecondaryIncomeBurdenValue | null): string {
  return SECONDARY_INCOME_BURDEN_OPTIONS.find((option) => option.value === value)?.label || "Not answered";
}

export function getSecondaryIncomeRoleLabel(value: SecondaryIncomeRoleType | null): string {
  return SECONDARY_INCOME_ROLE_OPTIONS.find((option) => option.value === value)?.label || "Not answered";
}

export function getMoveTimingLabel(value: MoveTimingValue): string {
  return MOVE_TIMING_OPTIONS.find((option) => option.value === value)?.label || value;
}

export function getScenarioLabel(scenarioId: ScenarioId): string {
  return SCENARIO_META[scenarioId].label;
}

export function buildSummaryTitle(results: AssessmentResults): string {
  return `${results.headline} - ${SCENARIO_META[results.bestScenarioId].label}`;
}
