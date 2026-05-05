import {
  formatCurrency,
  getBabyAgeLabel,
  getMoveTimingLabel,
  getScenarioLabel
} from "@/lib/famscorecard/questionnaire";
import type {
  AssessmentResults,
  ConfidenceLabel,
  DealBreakerId,
  ScenarioAssumptions,
  ScenarioBlocker,
  ScenarioId,
  ScenarioResult,
  ScenarioStatus,
  ScorecardState,
  SecondaryIncomeRoleState
} from "@/lib/famscorecard/types";

type ContributionId =
  | "financial_safety"
  | "insurance"
  | "family_access"
  | "timing"
  | "family_burden"
  | "family_life"
  | "trust_readiness"
  | "business_model"
  | "local_path"
  | "relocation_readiness";

interface Contribution {
  id: ContributionId;
  label: string;
  weight: number;
  fit: number;
  positiveText: string;
  concernText: string;
  cautionText: string;
}

interface BlockerDraft extends ScenarioBlocker {
  conceptId: ContributionId | "secondary_income_dependency";
  conditionText: string;
}

interface ScenarioEvaluation {
  scenarioId: ScenarioId;
  contributions: Contribution[];
  blockers: BlockerDraft[];
  score: number;
  status: ScenarioStatus;
  rankingScore: number;
  confidence: ConfidenceLabel;
}

export interface ScenarioDebugContribution {
  id: ContributionId;
  fit: number;
  weight: number;
}

export interface ScenarioDebugData {
  scenarioId: ScenarioId;
  score: number;
  status: ScenarioStatus;
  blockers: Array<{
    id: string;
    category: ScenarioBlocker["category"];
    severity: ScenarioBlocker["severity"];
    conceptId: ContributionId | "secondary_income_dependency";
  }>;
  contributions: ScenarioDebugContribution[];
  desiredRunwayMonths: number;
}

export const WIRED_DEAL_BREAKERS: DealBreakerId[] = [
  "familySupportDistance",
  "weakInsurance",
  "incomeUncertainty",
  "moveBeforeBabyAge",
  "eveningWeekendBurden",
  "sellerDependentBusiness",
  "cashCushion",
  "familyLifestyle",
  "spouseTiming"
];

const SOLVABLE_FIT_IF_SOLVED_IDS = new Set([
  "insurance_gap",
  "income_uncertainty",
  "cash_cushion",
  "family_burden",
  "plan_readiness",
  "seller_dependency"
]);

const moveTimingOrder: Record<ScenarioAssumptions["moveTiming"], number> = {
  "no-move": 99,
  under6: 0,
  "6to12": 1,
  "12to18": 2,
  "18to24": 3,
  "2plus": 4
};

const babyAgeOrder: Record<NonNullable<ScorecardState["family"]["minimumBabyAgeForMove"]>, number> = {
  under6: 0,
  "6to12": 1,
  "12to18": 2,
  "18to24": 3,
  "2plus": 4,
  "not-sure": 2
};

const plannedVisitScoreMap = {
  yes: 1,
  maybe: 0.62,
  no: 0.1,
  null: 0.52
} as const;

const visitSystemHelpScoreMap = {
  yes: 0.82,
  somewhat: 0.62,
  "not-much": 0.38,
  no: 0.18,
  null: 0.48
} as const;

const localPathScoreMap = {
  yes: 1,
  "not-sure": 0.62,
  no: 0.22,
  null: 0.52
} as const;

const housingCostScoreMap = {
  lower: 0.82,
  comparable: 0.65,
  higher: 0.35
} as const;

const weeklyLifeThresholdMap = {
  slightly: 0.5,
  moderately: 0.62,
  clearly: 0.75,
  much: 0.88,
  "not-worth-it": 1.05,
  null: 0.64
} as const;

function clamp01(value: number) {
  return Math.max(0, Math.min(1, value));
}

function scaleOrDefault(value: number | null | undefined, fallback = 3) {
  return typeof value === "number" ? value : fallback;
}

function normalizeScale(value: number | null | undefined, fallback = 3) {
  return (scaleOrDefault(value, fallback) - 1) / 4;
}

function average(values: number[]) {
  return values.reduce((sum, value) => sum + value, 0) / Math.max(values.length, 1);
}

function averagePresentNormalized(
  values: Array<number | null | undefined>,
  fallback = 0.5
) {
  const present = values.filter((value): value is number => typeof value === "number");
  if (present.length === 0) {
    return fallback;
  }

  return average(present.map((value) => normalizeScale(value)));
}

function weightedAverage(items: Array<{ fit: number; weight: number }>) {
  const totalWeight = items.reduce((sum, item) => sum + item.weight, 0);
  if (totalWeight <= 0) {
    return 0;
  }

  return items.reduce((sum, item) => sum + item.fit * item.weight, 0) / totalWeight;
}

function importanceWeight(scale: number | null | undefined, floor = 0.9, spread = 0.45) {
  return floor + normalizeScale(scale) * spread;
}

function lowToleranceWeight(scale: number | null | undefined, floor = 0.95, spread = 0.5) {
  return floor + (1 - normalizeScale(scale)) * spread;
}

function plannedVisitScore(state: ScorecardState) {
  return plannedVisitScoreMap[state.familyAccess.plannedVisitsAcceptable ?? "null"];
}

function visitSystemHelpScore(state: ScorecardState) {
  return visitSystemHelpScoreMap[state.familyAccess.repeatableVisitSystemHelp ?? "null"];
}

function localPathScore(state: ScorecardState) {
  return localPathScoreMap[state.relocation.localAcquisitionAcceptable ?? "null"];
}

function housingCostScore(assumptions: ScenarioAssumptions) {
  return housingCostScoreMap[assumptions.housingCostFit];
}

function weeklyLifeThresholdFit(state: ScorecardState, familyLifeFit: number) {
  const threshold = weeklyLifeThresholdMap[state.familyAccess.weeklyLifeImprovementNeeded ?? "null"];
  if (threshold > 1) {
    return 0.08;
  }

  return clamp01(1 - Math.max(0, threshold - familyLifeFit) * 2.1);
}

function mapDelayImportance(state: ScorecardState) {
  if (state.family.delayedMoveChangesAnswer === "yes") {
    return 1;
  }

  if (state.family.delayedMoveChangesAnswer === "maybe") {
    return 0.68;
  }

  return 0.24;
}

function moveTimingAlignment(state: ScorecardState, assumptions: ScenarioAssumptions) {
  if (assumptions.moveTiming === "no-move") {
    return 1;
  }

  if (
    state.family.delayedMoveChangesAnswer !== "yes" &&
    state.family.delayedMoveChangesAnswer !== "maybe"
  ) {
    return 0.78;
  }

  const minimumAge = state.family.minimumBabyAgeForMove;
  if (!minimumAge) {
    return 0.58;
  }

  const scenarioOrder = moveTimingOrder[assumptions.moveTiming];
  const minimumOrder = babyAgeOrder[minimumAge];
  const gap = minimumOrder - scenarioOrder;

  if (gap <= 0) {
    return clamp01(0.82 + Math.min(Math.abs(gap), 2) * 0.08);
  }

  if (gap === 1) {
    return 0.28;
  }

  return 0.08;
}

function desiredRunwayMonths(state: ScorecardState) {
  const annualFloor = Math.max(state.finance.minimumSafeIncome ?? 0, 1);
  const monthlyFloor = annualFloor / 12;
  const cushion = Math.max(state.finance.minimumCashCushion ?? 0, 0);
  return cushion / monthlyFloor;
}

function buildFinancialFit(state: ScorecardState, assumptions: ScenarioAssumptions) {
  // Numeric thresholds matter here in two ways:
  // 1. `incomeFit` is already defined relative to the user's stated safety floor.
  // 2. `minimumCashCushion / minimumSafeIncome` yields desired runway months, which tightens or relaxes
  //    the cushion requirement without inventing business-level economics that the tool does not know.
  const uncertaintyTolerance = normalizeScale(state.finance.yearOneUncertaintyTolerance);
  const runwayMonths = desiredRunwayMonths(state);
  const runwayStrictness = clamp01((runwayMonths - 2) / 4);
  const incomeFit = normalizeScale(assumptions.incomeFit);
  const incomeStabilityFit = normalizeScale(assumptions.incomeStability);
  const cashFit = normalizeScale(assumptions.cashCushionFit);

  const requiredIncome = 0.52 + (1 - uncertaintyTolerance) * 0.24;
  const requiredStability = 0.48 + (1 - uncertaintyTolerance) * 0.34;
  const requiredCash = 0.44 + runwayStrictness * 0.34;

  const incomeAlignment = clamp01(1 - Math.max(0, requiredIncome - incomeFit) * 1.4);
  const stabilityAlignment = clamp01(1 - Math.max(0, requiredStability - incomeStabilityFit) * 1.45);
  const cashAlignment = clamp01(1 - Math.max(0, requiredCash - cashFit) * 1.45);

  return {
    fit: average([incomeAlignment, stabilityAlignment, cashAlignment]),
    runwayMonths,
    requiredIncome,
    requiredStability,
    requiredCash,
    incomeFit,
    incomeStabilityFit,
    cashFit
  };
}

function buildInsuranceFit(
  state: ScorecardState,
  assumptions: ScenarioAssumptions,
  scenarioId: ScenarioId
) {
  if (scenarioId === "stay") {
    return normalizeScale(assumptions.insuranceQuality);
  }

  const pathConfidence =
    (state.finance.insuranceContinuity ?? 0) >= 3
      ? normalizeScale(state.finance.insurancePathConfidence)
      : normalizeScale(assumptions.insuranceQuality);

  return average([
    normalizeScale(assumptions.insuranceQuality),
    pathConfidence
  ]);
}

function buildFamilyAccessFit(state: ScorecardState, assumptions: ScenarioAssumptions, scenarioId: ScenarioId) {
  const support = normalizeScale(assumptions.familySupportAccess);
  const visitEase = normalizeScale(assumptions.familyVisitEase);
  const spontaneity = normalizeScale(assumptions.visitSpontaneity);
  const kidTravelEase = normalizeScale(assumptions.kidTravelEase);
  const burdenShare = normalizeScale(assumptions.householdTravelBurdenFit);
  const visitSystem = normalizeScale(assumptions.visitSystemSupport);

  if (scenarioId === "buyRelocate") {
    return average([
      support,
      visitEase,
      spontaneity,
      kidTravelEase,
      burdenShare,
      visitSystem * 0.65 + 0.35 * visitEase
    ]);
  }

  if (scenarioId === "buyLocal") {
    return average([support, visitEase, spontaneity, kidTravelEase, burdenShare]);
  }

  return average([support, visitEase, spontaneity, visitSystem]);
}

function buildTimingFit(state: ScorecardState, assumptions: ScenarioAssumptions) {
  return average([
    moveTimingAlignment(state, assumptions),
    normalizeScale(assumptions.disruptionLevel)
  ]);
}

function buildFamilyBurdenFit(state: ScorecardState, assumptions: ScenarioAssumptions) {
  const protectionFit = normalizeScale(assumptions.operationalResilience);
  const lowToleranceNeed = 1 - normalizeScale(state.lifestyle.afterHoursBurdenTolerance);
  const alignment = clamp01(1 - Math.max(0, lowToleranceNeed - protectionFit) * 1.5);
  return average([protectionFit, alignment]);
}

function buildFamilyLifeFit(state: ScorecardState, assumptions: ScenarioAssumptions) {
  const scheduleUpside = 0.5 + (normalizeScale(assumptions.scheduleControl) - 0.5) * 0.6;
  return average([
    scheduleUpside,
    normalizeScale(assumptions.housingLifestyleFit),
    housingCostScore(assumptions)
  ]);
}

function buildTrustFit(state: ScorecardState) {
  return normalizeScale(state.trust.operatingPlanTrust);
}

function buildBusinessModelFit(assumptions: ScenarioAssumptions) {
  return normalizeScale(assumptions.operationalResilience);
}

function buildRelocationFit(state: ScorecardState, familyLifeFit: number) {
  const visitSystemFit =
    state.familyAccess.plannedVisitsAcceptable === "no"
      ? 0.18
      : average([
          visitSystemHelpScore(state),
          normalizeScale(state.scenarios.buyRelocate.visitSystemSupport)
        ]);

  return average([
    normalizeScale(state.relocation.outOfStateOpenness),
    plannedVisitScore(state),
    weeklyLifeThresholdFit(state, familyLifeFit),
    visitSystemFit
  ]);
}

function cloneState(state: ScorecardState): ScorecardState {
  return {
    ...state,
    priorities: { ...state.priorities },
    family: { ...state.family },
    familyAccess: { ...state.familyAccess },
    finance: { ...state.finance },
    secondaryIncome: { ...state.secondaryIncome },
    lifestyle: { ...state.lifestyle },
    relocation: { ...state.relocation },
    trust: { ...state.trust },
    businessModel: { ...state.businessModel },
    nonNegotiables: {
      spouseTimingAlignment: state.nonNegotiables.spouseTimingAlignment,
      dealBreakers: { ...state.nonNegotiables.dealBreakers }
    },
    scenarios: {
      stay: { ...state.scenarios.stay },
      buyLocal: { ...state.scenarios.buyLocal },
      buyRelocate: { ...state.scenarios.buyRelocate }
    }
  };
}

function applyResolutions(state: ScorecardState, scenarioId: ScenarioId, blockerIds: string[]) {
  const nextState = cloneState(state);
  const scenario = nextState.scenarios[scenarioId];

  // "Fit if solved" uses targeted scenario or plan adjustments rather than opaque score floors.
  // Each blocker maps to the minimum concrete change that would credibly count as "handled."
  for (const blockerId of blockerIds) {
    switch (blockerId) {
      case "insurance_gap":
        scenario.insuranceQuality = Math.max(scenario.insuranceQuality, 4) as ScenarioAssumptions["insuranceQuality"];
        nextState.finance.insurancePathConfidence = Math.max(
          nextState.finance.insurancePathConfidence ?? 3,
          4
        ) as NonNullable<ScorecardState["finance"]["insurancePathConfidence"]>;
        break;
      case "income_uncertainty":
        scenario.incomeFit = Math.max(scenario.incomeFit, 4) as ScenarioAssumptions["incomeFit"];
        scenario.incomeStability = Math.max(
          scenario.incomeStability,
          4
        ) as ScenarioAssumptions["incomeStability"];
        break;
      case "cash_cushion":
        scenario.cashCushionFit = Math.max(
          scenario.cashCushionFit,
          4
        ) as ScenarioAssumptions["cashCushionFit"];
        break;
      case "family_burden":
        scenario.operationalResilience = Math.max(
          scenario.operationalResilience,
          4
        ) as ScenarioAssumptions["operationalResilience"];
        break;
      case "plan_readiness":
        nextState.trust.operatingPlanTrust = Math.max(
          nextState.trust.operatingPlanTrust ?? 3,
          4
        ) as NonNullable<ScorecardState["trust"]["operatingPlanTrust"]>;
        break;
      case "seller_dependency":
        scenario.operationalResilience = Math.max(
          scenario.operationalResilience,
          4
        ) as ScenarioAssumptions["operationalResilience"];
        break;
      default:
        break;
    }
  }

  return nextState;
}

function deriveStatus(score: number, blockers: BlockerDraft[]) {
  const structural = blockers.filter((blocker) => blocker.category === "structural-mismatch");
  const timing = blockers.filter((blocker) => blocker.category === "timing-issue");
  const solvable = blockers.filter((blocker) => blocker.category === "solvable-blocker");

  if (structural.some((blocker) => blocker.severity === "hard-fail")) {
    return "no";
  }

  if (timing.some((blocker) => blocker.severity === "hard-fail")) {
    return "no";
  }

  if (structural.length > 0) {
    return score >= 62 ? "maybe-later" : "no";
  }

  if (timing.length > 0) {
    return score >= 54 ? "maybe-later" : "no";
  }

  if (solvable.some((blocker) => blocker.severity === "hard-fail")) {
    return score >= 58 ? "maybe-later" : "no";
  }

  if (solvable.length > 0) {
    if (score >= 76 && solvable.length === 1) {
      return "maybe-now";
    }

    return score >= 54 ? "maybe-later" : "no";
  }

  if (score >= 70) {
    return "maybe-now";
  }

  return score >= 56 ? "maybe-later" : "no";
}

function deriveRankingScore(score: number, blockers: BlockerDraft[]) {
  const structuralPenalty = blockers.filter((blocker) => blocker.category === "structural-mismatch").length * 12;
  const timingPenalty = blockers.filter((blocker) => blocker.category === "timing-issue").length * 8;
  const solvablePenalty = blockers.filter((blocker) => blocker.category === "solvable-blocker").length * 5;
  const hardFailPenalty = blockers.filter((blocker) => blocker.severity === "hard-fail").length * 6;
  return Math.max(0, Math.round(score - structuralPenalty - timingPenalty - solvablePenalty - hardFailPenalty));
}

function deriveConfidence(score: number, blockers: BlockerDraft[]): ConfidenceLabel {
  if (
    blockers.some((blocker) => blocker.severity === "hard-fail") ||
    score < 52 ||
    blockers.filter((blocker) => blocker.category === "structural-mismatch").length > 1
  ) {
    return "Lower";
  }

  if (blockers.length > 0 || score < 70) {
    return "Moderate";
  }

  return "Higher";
}

function pushBlocker(blockers: BlockerDraft[], blocker: BlockerDraft) {
  if (!blockers.some((existing) => existing.id === blocker.id)) {
    blockers.push(blocker);
  }
}

function buildEvaluation(state: ScorecardState, scenarioId: ScenarioId): ScenarioEvaluation {
  const assumptions = state.scenarios[scenarioId];
  const dealBreakers = state.nonNegotiables.dealBreakers;
  const contributions: Contribution[] = [];
  const blockers: BlockerDraft[] = [];

  const financial = buildFinancialFit(state, assumptions);
  const insuranceFit = buildInsuranceFit(state, assumptions, scenarioId);
  const familyAccessFit = buildFamilyAccessFit(state, assumptions, scenarioId);
  const timingFit = buildTimingFit(state, assumptions);
  const familyBurdenFit = buildFamilyBurdenFit(state, assumptions);
  const familyLifeFit = buildFamilyLifeFit(state, assumptions);
  const trustFit = buildTrustFit(state);
  const businessModelFit = buildBusinessModelFit(assumptions);
  const relocationFit = scenarioId === "buyRelocate" ? buildRelocationFit(state, familyLifeFit) : null;
  const localPathFit = scenarioId === "buyLocal" ? localPathScore(state) : null;
  const supportNeed = averagePresentNormalized([
    state.priorities.stayCloseSupport,
    state.family.familySupportReliance
  ]);

  // Unique-concept scoring:
  // - Each household concern gets one contribution bucket.
  // - Family-life quality is intentionally reused inside relocation readiness because relocation needs
  //   the day-to-day upside to be strong enough, not merely "positive."
  // - Operational resilience intentionally affects both family burden and business model because the
  //   same day-to-day chaos creates household strain and owner-dependence risk.
  contributions.push({
    id: "financial_safety",
    label: "Financial safety",
    weight:
      1 +
      average([
        1 - normalizeScale(state.finance.yearOneUncertaintyTolerance),
        clamp01((financial.runwayMonths - 2) / 4)
      ]) *
        0.45,
    fit: financial.fit,
    positiveText: "The financial floor, stability, and post-close cushion look broadly workable.",
    concernText: "Income safety or post-close cushion still looks too thin for the household threshold you set.",
    cautionText: "Income safety or post-close cushion could still feel tighter than ideal under this path."
  });
  contributions.push({
    id: "insurance",
    label: "Insurance continuity",
    weight: importanceWeight(state.finance.insuranceContinuity, 0.95, 0.4),
    fit: insuranceFit,
    positiveText: "Insurance continuity looks reasonably reachable for the household.",
    concernText: "Insurance continuity still looks too unsettled for the current household requirement.",
    cautionText: "Insurance continuity still has some open questions under this path."
  });
  contributions.push({
    id: "family_access",
    label: "Family support and access",
    weight: 0.95 + supportNeed * 0.55,
    fit: familyAccessFit,
    positiveText: "This path preserves enough practical support and workable family access.",
    concernText: "This path appears to give up too much practical support or easy family access.",
    cautionText: "This path would make family support and easy access meaningfully less convenient."
  });
  contributions.push({
    id: "timing",
    label: "Timing and disruption",
    weight: 0.95 + average([normalizeScale(state.family.moveDisruption), mapDelayImportance(state)]) * 0.45,
    fit: timingFit,
    positiveText: "Timing and disruption look manageable under the current household constraints.",
    concernText: "The timing or disruption level still clashes with the household's current stage.",
    cautionText: "The timing or disruption level still asks a fair amount from the household's current stage."
  });
  contributions.push({
    id: "family_burden",
    label: "Family burden",
    weight: lowToleranceWeight(state.lifestyle.afterHoursBurdenTolerance),
    fit: familyBurdenFit,
    positiveText: "Day-to-day burden looks more containable for family life.",
    concernText: "After-hours load still looks too likely to spill into evenings, weekends, or solo-parenting.",
    cautionText: "After-hours load could still spill into evenings, weekends, or solo-parenting more than ideal."
  });
  contributions.push({
    id: "family_life",
    label: "Weekly family life",
    weight: importanceWeight(state.priorities.familyLifestyle, 0.95, 0.3),
    fit: familyLifeFit,
    positiveText: "The weekly family-life upside looks meaningful enough to matter.",
    concernText: "The weekly family-life improvement does not yet look strong enough to justify the change.",
    cautionText: "The weekly family-life improvement is present, but may not feel decisive enough yet."
  });

  if (scenarioId !== "stay") {
    contributions.push({
      id: "trust_readiness",
      label: "Trust in the plan",
      weight: 1,
      fit: trustFit,
      positiveText: "The current operating plan feels concrete enough to support household trust.",
      concernText: "The plan still reads too much like a harder job or an under-specified transition.",
      cautionText: "The plan still has enough uncertainty that it could feel harder in practice than it looks on paper."
    });
    contributions.push({
      id: "business_model",
      label: "Business model stability",
      weight:
        state.businessModel.stableTeamImportance === null
          ? 0.85
          : importanceWeight(state.businessModel.stableTeamImportance, 0.9, 0.4),
      fit: businessModelFit,
      positiveText: "The business model looks more teachable and less dependent on owner heroics.",
      concernText: "The business still looks too dependent on owner rescue work or thin operating systems.",
      cautionText: "The business still looks somewhat dependent on owner rescue work or thin operating systems."
    });
  }

  if (scenarioId === "buyLocal" && localPathFit !== null) {
    contributions.push({
      id: "local_path",
      label: "Local path viability",
      weight: 1.05,
      fit: localPathFit,
      positiveText: "A local acquisition reads meaningfully easier on the household than relocation.",
      concernText: "The household is not clearly convinced that a local acquisition solves the real tradeoff.",
      cautionText: "It is not yet obvious that a local acquisition fully solves the real household tradeoff."
    });
  }

  if (scenarioId === "buyRelocate" && relocationFit !== null) {
    contributions.push({
      id: "relocation_readiness",
      label: "Relocation readiness",
      weight: 1.1,
      fit: relocationFit,
      positiveText: "Relocation still falls within the household's real comfort range if the upside is real.",
      concernText: "Relocation still does not feel worth the family-distance cost under the current answers.",
      cautionText: "Relocation still asks for a meaningful family-distance tradeoff under the current answers."
    });
  }

  if (scenarioId === "buyRelocate") {
    const timingBlocked =
      assumptions.moveTiming !== "no-move" &&
      state.family.minimumBabyAgeForMove !== null &&
      moveTimingOrder[assumptions.moveTiming] < babyAgeOrder[state.family.minimumBabyAgeForMove];
    if (
      state.family.delayedMoveChangesAnswer !== "no" &&
      (timingBlocked || dealBreakers.moveBeforeBabyAge)
    ) {
      pushBlocker(blockers, {
        id: "baby_timing",
        label: "Relocation is earlier than the household timing window",
        detail: `The current relocation timing (${getMoveTimingLabel(
          assumptions.moveTiming
        )}) still lands before the minimum age that starts to feel acceptable (${getBabyAgeLabel(
          state.family.minimumBabyAgeForMove
        )}).`,
        category: "timing-issue",
        severity: dealBreakers.moveBeforeBabyAge || timingFit <= 0.18 ? "hard-fail" : "timing",
        conceptId: "timing",
        conditionText: `Relocation would need to wait until at least ${getBabyAgeLabel(
          state.family.minimumBabyAgeForMove
        )}.`
      });
    }
  }

  if (
    state.nonNegotiables.spouseTimingAlignment !== null &&
    ((state.nonNegotiables.spouseTimingAlignment <= 2) ||
      (dealBreakers.spouseTiming && state.nonNegotiables.spouseTimingAlignment <= 3))
  ) {
    pushBlocker(blockers, {
      id: "timing_alignment",
      label: "The household does not feel aligned on timing yet",
      detail: "The current timing still reads more like strain or hesitation than a clear shared green light.",
      category: "timing-issue",
      severity:
        state.nonNegotiables.spouseTimingAlignment <= 2 || dealBreakers.spouseTiming
          ? "hard-fail"
          : "timing",
      conceptId: "timing",
      conditionText: "Timing needs to feel mutually supportable before this path is treated as viable."
    });
  }

  if (
    scenarioId !== "stay" &&
    ((state.finance.insuranceContinuity ?? 0) >= 4 || dealBreakers.weakInsurance) &&
    insuranceFit <= 0.62
  ) {
    pushBlocker(blockers, {
      id: "insurance_gap",
      label: "Insurance continuity is still not solved clearly enough",
      detail: "Coverage looks important to the household, but the current answers still do not show a confident-enough transition path.",
      category: "solvable-blocker",
      severity: dealBreakers.weakInsurance || insuranceFit <= 0.3 ? "hard-fail" : "condition",
      conceptId: "insurance",
      conditionText: "A credible plan for acceptable transition coverage would need to be lined up before closing."
    });
  }

  if (
    scenarioId !== "stay" &&
    (financial.incomeFit <= 0.48 ||
      financial.incomeStabilityFit <= 0.44 ||
      dealBreakers.incomeUncertainty)
  ) {
    pushBlocker(blockers, {
      id: "income_uncertainty",
      label: "Income safety still looks too uncertain for the household floor",
      detail: `The current draft does not yet clear a safe-enough path to roughly ${formatCurrency(
        state.finance.minimumSafeIncome
      )} with the level of stability the household seems to need.`,
      category: "solvable-blocker",
      severity:
        dealBreakers.incomeUncertainty || (financial.incomeFit <= 0.22 && financial.incomeStabilityFit <= 0.22)
          ? "hard-fail"
          : "condition",
      conceptId: "financial_safety",
      conditionText: "Expected household income and year-one stability would need to clear your safety floor more confidently."
    });
  }

  if (
    scenarioId !== "stay" &&
    (financial.cashFit <= financial.requiredCash - 0.08 || dealBreakers.cashCushion)
  ) {
    pushBlocker(blockers, {
      id: "cash_cushion",
      label: "The post-close cash cushion still looks too thin",
      detail: "Based on your stated floor and cushion target, this path still looks shy of the buffer that would feel safe.",
      category: "solvable-blocker",
      severity: dealBreakers.cashCushion || financial.cashFit <= 0.2 ? "hard-fail" : "condition",
      conceptId: "financial_safety",
      conditionText: `Post-close cash would need to cover roughly ${desiredRunwayMonths(state).toFixed(
        1
      )} months of your stated safety floor, or the household would need a lower cushion requirement.`
    });
  }

  if (
    scenarioId !== "stay" &&
    supportNeed >= 0.6 &&
    (familyAccessFit <= 0.48 || dealBreakers.familySupportDistance)
  ) {
    pushBlocker(blockers, {
      id: "family_support_distance",
      label: "The loss of easy family support still looks too costly",
      detail: "The current path appears to give up too much practical support continuity or too much easy reach of family.",
      category: "structural-mismatch",
      severity: dealBreakers.familySupportDistance || familyAccessFit <= 0.24 ? "hard-fail" : "condition",
      conceptId: "family_access",
      conditionText: "The path would need to preserve more practical family support and easier access than it currently does."
    });
  }

  if (scenarioId === "buyRelocate" && relocationFit !== null && relocationFit <= 0.52) {
    pushBlocker(blockers, {
      id: "relocation_readiness",
      label: "Relocation still does not feel worth the family-distance cost",
      detail: "The current answers suggest distance, planned-visit tradeoffs, or the required weekly-life upside still do not line up cleanly enough.",
      category: "structural-mismatch",
      severity:
        state.familyAccess.plannedVisitsAcceptable === "no" ||
        state.familyAccess.weeklyLifeImprovementNeeded === "not-worth-it"
          ? "hard-fail"
          : "condition",
      conceptId: "relocation_readiness",
      conditionText: "Relocation would need clearer day-to-day household gains and a more acceptable family-distance tradeoff."
    });
  }

  if (scenarioId === "buyLocal" && localPathFit !== null && localPathFit <= 0.48) {
    pushBlocker(blockers, {
      id: "local_path",
      label: "A local acquisition does not yet read clearly easier than relocation",
      detail: "The household has not clearly signaled that local ownership would feel materially different from the relocation version.",
      category: "structural-mismatch",
      severity: state.relocation.localAcquisitionAcceptable === "no" ? "hard-fail" : "condition",
      conceptId: "local_path",
      conditionText: "The local path would need to feel meaningfully easier on support, timing, or family burden than relocating."
    });
  }

  if (
    scenarioId !== "stay" &&
    (familyBurdenFit <= 0.48 || dealBreakers.eveningWeekendBurden)
  ) {
    pushBlocker(blockers, {
      id: "family_burden",
      label: "The likely after-hours burden still looks too heavy for family life",
      detail: "The current operating burden still looks too likely to spill into evenings, weekends, or added solo-parenting strain.",
      category: "solvable-blocker",
      severity: dealBreakers.eveningWeekendBurden || familyBurdenFit <= 0.24 ? "hard-fail" : "condition",
      conceptId: "family_burden",
      conditionText: "The operating model would need clearer schedule protection and lower after-hours spillover."
    });
  }

  if (
    scenarioId !== "stay" &&
    (trustFit <= 0.5 || (state.trust.operatingPlanTrust ?? 0) <= 2)
  ) {
    pushBlocker(blockers, {
      id: "plan_readiness",
      label: "The operating plan still does not feel trustworthy enough",
      detail: "The household does not yet have enough confidence that the current plan improves life rather than creating a harder job.",
      category: "solvable-blocker",
      severity: trustFit <= 0.22 ? "hard-fail" : "condition",
      conceptId: "trust_readiness",
      conditionText: "The day-to-day operating plan would need to feel more concrete, teachable, and believable."
    });
  }

  if (
    scenarioId !== "stay" &&
    (businessModelFit <= 0.48 || dealBreakers.sellerDependentBusiness)
  ) {
    pushBlocker(blockers, {
      id: "seller_dependency",
      label: "The business still looks too dependent on owner heroics",
      detail: "The current operating model still reads as too dependent on one person covering the gaps.",
      category: "solvable-blocker",
      severity: dealBreakers.sellerDependentBusiness || businessModelFit <= 0.24 ? "hard-fail" : "condition",
      conceptId: "business_model",
      conditionText: "The business would need a steadier team and clearer operating systems before it supports family life."
    });
  }

  if (
    scenarioId !== "stay" &&
    (familyLifeFit <= 0.44 || dealBreakers.familyLifestyle)
  ) {
    pushBlocker(blockers, {
      id: "family_lifestyle",
      label: "The family-life upside still does not look strong enough",
      detail: "The expected day-to-day family setup still does not look clearly better enough to justify the change path.",
      category: "structural-mismatch",
      severity: dealBreakers.familyLifestyle || familyLifeFit <= 0.2 ? "hard-fail" : "condition",
      conceptId: "family_life",
      conditionText: "The path would need a clearer weekly-life improvement, not just a business or income story."
    });
  }

  if (scenarioId !== "stay") {
    const dependsOnOutsideIncome =
      state.secondaryIncome.outsideIncomeSafetyBuffer === "material" &&
      (state.secondaryIncome.outsideIncomeRoleFeel === "burden" ||
        state.secondaryIncome.outsideIncomeRoleFeel === "not-acceptable" ||
        (state.secondaryIncome.notRelyOnOutsideIncomeImportance ?? 0) >= 4);

    if (dependsOnOutsideIncome) {
      pushBlocker(blockers, {
        id: "secondary_income_dependency",
        label: "The base case still looks too dependent on outside spouse income",
        detail: "Optional secondary income reads more like a required crutch than a cushion under the current answers.",
        category: "solvable-blocker",
        severity: state.secondaryIncome.outsideIncomeRoleFeel === "not-acceptable" ? "hard-fail" : "condition",
        conceptId: "secondary_income_dependency",
        conditionText: "The plan needs to stand on its own before optional outside income is treated as extra cushion."
      });
    }
  }

  const score = Math.round(
    weightedAverage(contributions.map((item) => ({ fit: item.fit, weight: item.weight }))) * 100
  );
  const status = deriveStatus(score, blockers);
  const rankingScore = deriveRankingScore(score, blockers);
  const confidence = deriveConfidence(score, blockers);

  return {
    scenarioId,
    contributions,
    blockers,
    score,
    status,
    rankingScore,
    confidence
  };
}

function blockerPriority(blocker: BlockerDraft) {
  const severityRank =
    blocker.severity === "hard-fail" ? 0 : blocker.severity === "timing" ? 1 : 2;
  const conceptRank = {
    family_access: 0,
    timing: 1,
    insurance: 2,
    financial_safety: 3,
    family_burden: 4,
    family_life: 5,
    relocation_readiness: 6,
    local_path: 7,
    trust_readiness: 8,
    business_model: 9,
    secondary_income_dependency: 10
  }[blocker.conceptId];

  return severityRank * 20 + conceptRank;
}

function buildFitIfSolved(state: ScorecardState, scenarioId: ScenarioId, evaluation: ScenarioEvaluation) {
  const targets = evaluation.blockers
    .filter((blocker) => blocker.category === "solvable-blocker" && SOLVABLE_FIT_IF_SOLVED_IDS.has(blocker.id))
    .sort((left, right) => blockerPriority(left) - blockerPriority(right))
    .slice(0, 2);

  if (targets.length === 0) {
    return {
      score: evaluation.score,
      status: evaluation.status,
      targets: [] as string[],
      summary: "No clearly solvable blocker is carrying this result more than the underlying family fit."
    };
  }

  const resolved = buildEvaluation(
    applyResolutions(state, scenarioId, targets.map((target) => target.id)),
    scenarioId
  );

  return {
    score: resolved.score,
    status: resolved.status,
    targets: targets.map((target) => target.label),
    summary: `Assumes ${targets
      .map((target) => target.conditionText.replace(/\.$/, ""))
      .join(" and ")}.`
  };
}

function formatList(items: string[]) {
  if (items.length === 0) {
    return "";
  }

  if (items.length === 1) {
    return items[0];
  }

  return `${items.slice(0, -1).join(", ")}, and ${items[items.length - 1]}`;
}

function buildExplanation(
  scenarioId: ScenarioId,
  status: ScenarioStatus,
  positives: string[],
  concerns: string[],
  hasConditions: boolean
) {
  const scenarioLabel = getScenarioLabel(scenarioId);
  const statusCopy =
    status === "maybe-now"
      ? hasConditions
        ? "reads as viable now with conditions"
        : "reads as viable now"
      : status === "maybe-later"
        ? "looks more like a maybe-later path"
        : "does not look like a fit right now";

  const positive = positives[0] ?? "some household factors still line up";
  const concern = concerns[0] ?? "key blockers still remain";

  return `${scenarioLabel} ${statusCopy} because ${positive.toLowerCase()}, but ${concern.toLowerCase()}.`;
}

function describeConcern(contribution: Contribution, blockers: BlockerDraft[]) {
  const hasRelatedBlocker = blockers.some((blocker) => blocker.conceptId === contribution.id);
  const useHarshCopy = hasRelatedBlocker || contribution.fit <= 0.4;
  return useHarshCopy ? contribution.concernText : contribution.cautionText;
}

function buildScenarioResult(state: ScorecardState, scenarioId: ScenarioId): ScenarioResult {
  const evaluation = buildEvaluation(state, scenarioId);
  const fitIfSolved = buildFitIfSolved(state, scenarioId, evaluation);
  const scenarioLabel = getScenarioLabel(scenarioId);

  const contributionStrength = [...evaluation.contributions].sort(
    (left, right) => right.fit * right.weight - left.fit * left.weight
  );
  const contributionRisks = [...evaluation.contributions].sort(
    (left, right) => left.fit * left.weight - right.fit * right.weight
  );

  const topPositives = contributionStrength.slice(0, 3).map((item) => item.positiveText);
  const sortedBlockers = [...evaluation.blockers].sort(
    (left, right) => blockerPriority(left) - blockerPriority(right)
  );
  const topConcerns = contributionRisks
    .slice(0, 3)
    .map((item) => describeConcern(item, sortedBlockers));
  const requiredConditions = Array.from(
    new Set(sortedBlockers.map((blocker) => blocker.conditionText))
  ).slice(0, 5);

  return {
    id: scenarioId,
    label: scenarioLabel,
    status: evaluation.status,
    score: evaluation.score,
    rankingScore: evaluation.rankingScore,
    confidence: evaluation.confidence,
    topPositives,
    topConcerns,
    blockers: sortedBlockers.map((blocker) => blocker.label),
    requiredConditions,
    hardFails: sortedBlockers,
    fitIfSolvedScore: fitIfSolved.score,
    fitIfSolvedStatus: fitIfSolved.status,
    fitIfSolvedTargets: fitIfSolved.targets,
    fitIfSolvedSummary: fitIfSolved.summary,
    structuralMismatches: sortedBlockers
      .filter((blocker) => blocker.category === "structural-mismatch")
      .map((blocker) => blocker.label),
    timingIssues: sortedBlockers
      .filter((blocker) => blocker.category === "timing-issue")
      .map((blocker) => blocker.label),
    solvableBlockers: sortedBlockers
      .filter((blocker) => blocker.category === "solvable-blocker")
      .map((blocker) => blocker.label),
    explanation: buildExplanation(
      scenarioId,
      evaluation.status,
      topPositives,
      topConcerns,
      requiredConditions.length > 0
    )
  };
}

function buildSecondaryIncomeRole(state: ScorecardState): {
  role: SecondaryIncomeRoleState;
  notes: string[];
} {
  const buffer = state.secondaryIncome.outsideIncomeSafetyBuffer;
  const burden = state.secondaryIncome.outsideIncomeRoleFeel;
  const noRelianceImportance = state.secondaryIncome.notRelyOnOutsideIncomeImportance ?? 3;

  if (buffer === "no-plan" || buffer === "not-much") {
    return {
      role: "Not needed",
      notes: [
        "The current answers do not treat optional outside income as a central part of the safety plan.",
        "This scorecard does not assume spouse income as a hidden requirement."
      ]
    };
  }

  if (burden === "burden" || burden === "not-acceptable" || noRelianceImportance >= 4) {
    return {
      role: "Unacceptable if required",
      notes: [
        "Optional outside income only helps if it remains optional and does not add more family burden than benefit.",
        "The current answers suggest the base plan should stand on its own before outside income is treated as cushion."
      ]
    };
  }

  if (buffer === "material") {
    return {
      role: "Meaningfully improves comfort",
      notes: [
        "Optional outside income looks like a real comfort boost, but not something the plan should rely on by default.",
        "If it is used at all, it works better as extra cushion than as the foundation of the plan."
      ]
    };
  }

  return {
    role: "Helpful buffer",
    notes: [
      "Optional outside income appears to provide some household cushion without being the whole answer.",
      "The current answers still point to judging the base case on its own merits first."
    ]
  };
}

function buildFamilyAccessTradeoffs(state: ScorecardState, relocateScenario: ScenarioResult) {
  const items = [
    "The main tradeoff is not whether family visits disappear, but that they become harder, less flexible, and more dependent on planning."
  ];

  if ((state.priorities.stayCloseSupport ?? 0) >= 4 || (state.family.familySupportReliance ?? 0) >= 4) {
    items.push("Easy reach of family support looks materially valuable to this household right now.");
  }

  if ((state.familyAccess.youngKidTravelDifficulty ?? 0) >= 4) {
    items.push("Long-distance travel with young kids currently reads as a heavy enough lift to matter on its own.");
  }

  if (state.familyAccess.plannedVisitsAcceptable === "no") {
    items.push("Fewer but more deliberate visits still do not feel like an acceptable substitute for easy access.");
  } else if (
    state.familyAccess.repeatableVisitSystemHelp === "yes" ||
    state.familyAccess.repeatableVisitSystemHelp === "somewhat"
  ) {
    items.push("A repeatable visit system could reduce some stress, but the tool does not treat that as equivalent to low-friction access.");
  }

  if (relocateScenario.status === "no") {
    items.push("Based on the current answers, relocation still looks more like a family-access problem than a simple travel-planning problem.");
  }

  return items.slice(0, 4);
}

function buildRelocationWorthItConditions(state: ScorecardState, relocateScenario: ScenarioResult) {
  const items = Array.from(new Set(relocateScenario.requiredConditions));

  if (
    state.familyAccess.weeklyLifeImprovementNeeded === "much" ||
    state.familyAccess.weeklyLifeImprovementNeeded === "clearly"
  ) {
    items.unshift("Weekly family life would need to improve materially, not just slightly, to justify harder family access.");
  }

  if (state.familyAccess.plannedVisitsAcceptable === "no") {
    items.unshift("The household would need to feel differently about losing easy, spontaneous visits before relocation becomes attractive.");
  }

  return items.slice(0, 5);
}

function buildBestLines(
  bestScenario: ScenarioResult,
  bestBuyScenario: ScenarioResult,
  relocateScenario: ScenarioResult
) {
  const bestOverallLine = `Best overall current path: ${bestScenario.label} (${bestScenario.status.replace("-", " ")}).`;
  const bestBuyLine = `Best business-buy path: ${bestBuyScenario.label} (${bestBuyScenario.status.replace("-", " ")}).`;
  const relocationLine = `Relocation: ${
    relocateScenario.status === "maybe-now"
      ? relocateScenario.requiredConditions.length > 0
        ? "viable now with conditions"
        : "viable now"
      : relocateScenario.status === "maybe-later"
        ? "more of a maybe-later path"
        : "not a fit right now"
  }.`;
  return { bestOverallLine, bestBuyLine, relocationLine };
}

function buildHeadline(bestScenario: ScenarioResult, bestBuyScenario: ScenarioResult) {
  if (bestScenario.status === "no" && bestBuyScenario.status === "no") {
    return "No path looks clearly workable right now";
  }

  if (bestScenario.id === "stay") {
    return "Stay put looks strongest right now";
  }

  if (bestScenario.id === "buyLocal") {
    return "A local acquisition looks strongest right now";
  }

  return "Buying and relocating looks strongest right now";
}

function buildHeadlineDetail(
  bestScenario: ScenarioResult,
  bestBuyScenario: ScenarioResult,
  relocateScenario: ScenarioResult
) {
  const lines = buildBestLines(bestScenario, bestBuyScenario, relocateScenario);
  return `${lines.bestOverallLine} ${lines.bestBuyLine} ${lines.relocationLine}`;
}

function buildNarrativeSummary(
  bestScenario: ScenarioResult,
  bestBuyScenario: ScenarioResult,
  relocateScenario: ScenarioResult
) {
  const parts = [buildHeadlineDetail(bestScenario, bestBuyScenario, relocateScenario)];

  if (bestBuyScenario.structuralMismatches.length > 0) {
    parts.push(`The biggest structural issue on the strongest buy path is ${bestBuyScenario.structuralMismatches[0].toLowerCase()}.`);
  }

  if (bestBuyScenario.solvableBlockers.length > 0) {
    parts.push(`The clearest solvable blocker is ${bestBuyScenario.solvableBlockers[0].toLowerCase()}.`);
  }

  if (bestBuyScenario.timingIssues.length > 0) {
    parts.push(`There is also a timing issue: ${bestBuyScenario.timingIssues[0].toLowerCase()}.`);
  }

  if (bestBuyScenario.fitIfSolvedTargets.length > 0) {
    parts.push(`If ${formatList(bestBuyScenario.fitIfSolvedTargets).toLowerCase()} were handled cleanly, the buy path improves to ${bestBuyScenario.fitIfSolvedStatus.replace("-", " ")}.`);
  }

  return parts.join(" ");
}

function buildSummaryText(
  headline: string,
  headlineDetail: string,
  rankedScenarios: ScenarioResult[],
  results: Omit<AssessmentResults, "summaryText" | "headline" | "headlineDetail" | "rankedScenarios">
) {
  const rankingLines = rankedScenarios
    .map((scenario, index) => {
      const fitIfSolvedSuffix =
        scenario.fitIfSolvedTargets.length > 0
          ? ` | If solved: ${scenario.fitIfSolvedScore}/100 (${scenario.fitIfSolvedStatus.replace("-", " ")})`
          : "";
      return `${index + 1}. ${scenario.label}: ${scenario.score}/100 (${scenario.status.replace("-", " ")})${fitIfSolvedSuffix}`;
    })
    .join("\n");

  const sections = [
    headline,
    headlineDetail,
    "",
    "Scenario ranking",
    rankingLines,
    "",
    "Biggest blockers",
    results.topBlockers.map((item) => `- ${item}`).join("\n") || "- None surfaced",
    "",
    "Structural mismatch",
    results.topStructuralIssues.map((item) => `- ${item}`).join("\n") || "- None surfaced",
    "",
    "Timing issue",
    results.topTimingIssues.map((item) => `- ${item}`).join("\n") || "- None surfaced",
    "",
    "Solvable blocker",
    results.topSolvableBlockers.map((item) => `- ${item}`).join("\n") || "- None surfaced",
    "",
    "Buy local conditions",
    results.buyLocalConditions.map((item) => `- ${item}`).join("\n") || "- None surfaced",
    "",
    "Buy and move conditions",
    results.buyRelocateConditions.map((item) => `- ${item}`).join("\n") || "- None surfaced",
    "",
    "Family-access tradeoffs",
    results.familyAccessTradeoffs.map((item) => `- ${item}`).join("\n") || "- None surfaced",
    "",
    "When relocation could feel worth it",
    results.familyDistanceWorthItConditions.map((item) => `- ${item}`).join("\n") || "- None surfaced",
    "",
    "Optional secondary income",
    `State: ${results.secondaryIncomeRole}`,
    results.secondaryIncomeNotes.map((item) => `- ${item}`).join("\n"),
    "",
    "Narrative summary",
    results.narrativeSummary
  ];

  return sections.join("\n").trim();
}

export function scoreScenarioDebug(state: ScorecardState, scenarioId: ScenarioId): ScenarioDebugData {
  const evaluation = buildEvaluation(state, scenarioId);
  return {
    scenarioId,
    score: evaluation.score,
    status: evaluation.status,
    blockers: evaluation.blockers.map((blocker) => ({
      id: blocker.id,
      category: blocker.category,
      severity: blocker.severity,
      conceptId: blocker.conceptId
    })),
    contributions: evaluation.contributions.map((contribution) => ({
      id: contribution.id,
      fit: contribution.fit,
      weight: contribution.weight
    })),
    desiredRunwayMonths: desiredRunwayMonths(state)
  };
}

export function scoreAssessment(state: ScorecardState): AssessmentResults {
  const scenarios = (["stay", "buyLocal", "buyRelocate"] as ScenarioId[]).map((scenarioId) =>
    buildScenarioResult(state, scenarioId)
  );
  const rankedScenarios = [...scenarios].sort((left, right) => right.rankingScore - left.rankingScore);
  const bestScenario = rankedScenarios[0];
  const buyLocalScenario = scenarios.find((scenario) => scenario.id === "buyLocal")!;
  const relocateScenario = scenarios.find((scenario) => scenario.id === "buyRelocate")!;
  const bestBuyScenario =
    buyLocalScenario.rankingScore >= relocateScenario.rankingScore ? buyLocalScenario : relocateScenario;
  const secondaryIncome = buildSecondaryIncomeRole(state);
  const { bestOverallLine, bestBuyLine, relocationLine } = buildBestLines(
    bestScenario,
    bestBuyScenario,
    relocateScenario
  );
  const headline = buildHeadline(bestScenario, bestBuyScenario);
  const headlineDetail = buildHeadlineDetail(bestScenario, bestBuyScenario, relocateScenario);
  const narrativeSummary = buildNarrativeSummary(bestScenario, bestBuyScenario, relocateScenario);
  const familyAccessTradeoffs = buildFamilyAccessTradeoffs(state, relocateScenario);
  const familyDistanceWorthItConditions = buildRelocationWorthItConditions(state, relocateScenario);

  const resultBase = {
    topBlockers: bestBuyScenario.blockers.slice(0, 3),
    topConditions: bestBuyScenario.requiredConditions.slice(0, 3),
    topStructuralIssues: bestBuyScenario.structuralMismatches.slice(0, 3),
    topTimingIssues: bestBuyScenario.timingIssues.slice(0, 3),
    topSolvableBlockers: bestBuyScenario.solvableBlockers.slice(0, 3),
    bestScenarioId: bestScenario.id,
    bestBuyScenarioId: bestBuyScenario.id,
    bestOverallLine,
    bestBuyLine,
    relocationLine,
    babyTimingIsMajorLimiter: relocateScenario.hardFails.some((blocker) => blocker.id === "baby_timing"),
    narrativeSummary,
    familyAccessTradeoffs,
    familyDistanceWorthItConditions,
    secondaryIncomeRole: secondaryIncome.role,
    secondaryIncomeNotes: secondaryIncome.notes,
    buyLocalConditions: buyLocalScenario.requiredConditions.slice(0, 4),
    buyRelocateConditions: relocateScenario.requiredConditions.slice(0, 4)
  } satisfies Omit<AssessmentResults, "summaryText" | "headline" | "headlineDetail" | "rankedScenarios">;

  return {
    headline,
    headlineDetail,
    rankedScenarios,
    ...resultBase,
    summaryText: buildSummaryText(headline, headlineDetail, rankedScenarios, resultBase)
  };
}
