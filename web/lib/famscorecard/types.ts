export type ScaleValue = 1 | 2 | 3 | 4 | 5;

export type ScenarioId = "stay" | "buyLocal" | "buyRelocate";

export type StepId =
  | "intro"
  | "priorities"
  | "family"
  | "familyAccess"
  | "finance"
  | "secondaryIncome"
  | "lifestyle"
  | "relocation"
  | "trust"
  | "businessModel"
  | "nonNegotiables"
  | "results";

export type MoveTimingValue =
  | "no-move"
  | "under6"
  | "6to12"
  | "12to18"
  | "18to24"
  | "2plus";

export type BabyAgeValue =
  | "under6"
  | "6to12"
  | "12to18"
  | "18to24"
  | "2plus"
  | "not-sure";

export type DelayAnswer = "yes" | "maybe" | "no";

export type HousingCostRule = "any" | "comparable" | "lower";

export type LocalAcquisitionAnswer = "yes" | "not-sure" | "no";

export type HousingCostFit = "higher" | "comparable" | "lower";

export type PlannedVisitTradeoffAnswer = "yes" | "maybe" | "no";

export type WeeklyLifeTradeoffValue =
  | "slightly"
  | "moderately"
  | "clearly"
  | "much"
  | "not-worth-it";

export type VisitSystemHelpValue = "yes" | "somewhat" | "not-much" | "no";

export type SecondaryIncomeSafetyValue = "material" | "somewhat" | "not-much" | "no-plan";

export type SecondaryIncomeBurdenValue =
  | "helpful"
  | "depends"
  | "neutral"
  | "burden"
  | "not-acceptable";

export type YesMaybeNoValue = "yes" | "maybe" | "no";

export type SecondaryIncomeRoleType =
  | "school-community"
  | "administrative"
  | "remote"
  | "not-sure"
  | "not-planning";

export type DealBreakerId =
  | "familySupportDistance"
  | "weakInsurance"
  | "incomeUncertainty"
  | "moveBeforeBabyAge"
  | "eveningWeekendBurden"
  | "sellerDependentBusiness"
  | "cashCushion"
  | "familyLifestyle"
  | "spouseTiming";

export type ScenarioStatus = "no" | "maybe-later" | "maybe-now";

export type ConfidenceLabel = "Higher" | "Moderate" | "Lower";

export type SecondaryIncomeRoleState =
  | "Not needed"
  | "Helpful buffer"
  | "Meaningfully improves comfort"
  | "Unacceptable if required";

export type BlockerCategory =
  | "structural-mismatch"
  | "timing-issue"
  | "solvable-blocker";

export interface HouseholdPriorities {
  familyLifestyle: ScaleValue | null;
  stayCloseSupport: ScaleValue | null;
}

export interface FamilyStageAnswers {
  moveDisruption: ScaleValue | null;
  delayedMoveChangesAnswer: DelayAnswer | null;
  minimumBabyAgeForMove: BabyAgeValue | null;
  familySupportReliance: ScaleValue | null;
}

export interface FamilyAccessAnswers {
  youngKidTravelDifficulty: ScaleValue | null;
  plannedVisitsAcceptable: PlannedVisitTradeoffAnswer | null;
  weeklyLifeImprovementNeeded: WeeklyLifeTradeoffValue | null;
  repeatableVisitSystemHelp: VisitSystemHelpValue | null;
}

export interface FinancialSafetyAnswers {
  minimumSafeIncome: number | null;
  insuranceContinuity: ScaleValue | null;
  insurancePathConfidence: ScaleValue | null;
  yearOneUncertaintyTolerance: ScaleValue | null;
  minimumCashCushion: number | null;
}

export interface SecondaryIncomeAnswers {
  outsideIncomeSafetyBuffer: SecondaryIncomeSafetyValue | null;
  outsideIncomeRoleFeel: SecondaryIncomeBurdenValue | null;
  notRelyOnOutsideIncomeImportance: ScaleValue | null;
}

export interface LifestyleAnswers {
  afterHoursBurdenTolerance: ScaleValue | null;
}

export interface RelocationAnswers {
  outOfStateOpenness: ScaleValue | null;
  localAcquisitionAcceptable: LocalAcquisitionAnswer | null;
}

export interface TrustPlanAnswers {
  operatingPlanTrust: ScaleValue | null;
}

export interface BusinessModelRealityAnswers {
  stableTeamImportance: ScaleValue | null;
}

export interface NonNegotiableAnswers {
  spouseTimingAlignment: ScaleValue | null;
  dealBreakers: Record<DealBreakerId, boolean>;
}

export interface ScenarioAssumptions {
  incomeFit: ScaleValue;
  incomeStability: ScaleValue;
  insuranceQuality: ScaleValue;
  cashCushionFit: ScaleValue;
  familySupportAccess: ScaleValue;
  familyVisitEase: ScaleValue;
  visitSpontaneity: ScaleValue;
  kidTravelEase: ScaleValue;
  householdTravelBurdenFit: ScaleValue;
  visitSystemSupport: ScaleValue;
  moveTiming: MoveTimingValue;
  scheduleControl: ScaleValue;
  disruptionLevel: ScaleValue;
  housingLifestyleFit: ScaleValue;
  housingCostFit: HousingCostFit;
  operationalResilience: ScaleValue;
}

export interface ScorecardState {
  schemaVersion: number;
  currentStep: number;
  lastSavedAt: string | null;
  priorities: HouseholdPriorities;
  family: FamilyStageAnswers;
  familyAccess: FamilyAccessAnswers;
  finance: FinancialSafetyAnswers;
  secondaryIncome: SecondaryIncomeAnswers;
  lifestyle: LifestyleAnswers;
  relocation: RelocationAnswers;
  trust: TrustPlanAnswers;
  businessModel: BusinessModelRealityAnswers;
  nonNegotiables: NonNegotiableAnswers;
  scenarios: Record<ScenarioId, ScenarioAssumptions>;
}

export interface RankedReason {
  label: string;
  detail: string;
  weight: number;
  fit: number;
}

export interface ScenarioBlocker {
  id: string;
  label: string;
  detail: string;
  category: BlockerCategory;
  severity: "hard-fail" | "timing" | "condition";
}

export interface ScenarioResult {
  id: ScenarioId;
  label: string;
  status: ScenarioStatus;
  score: number;
  rankingScore: number;
  confidence: ConfidenceLabel;
  topPositives: string[];
  topConcerns: string[];
  blockers: string[];
  requiredConditions: string[];
  hardFails: ScenarioBlocker[];
  fitIfSolvedScore: number;
  fitIfSolvedStatus: ScenarioStatus;
  fitIfSolvedTargets: string[];
  fitIfSolvedSummary: string;
  structuralMismatches: string[];
  timingIssues: string[];
  solvableBlockers: string[];
  explanation: string;
}

export interface AssessmentResults {
  headline: string;
  headlineDetail: string;
  rankedScenarios: ScenarioResult[];
  topBlockers: string[];
  topConditions: string[];
  topStructuralIssues: string[];
  topTimingIssues: string[];
  topSolvableBlockers: string[];
  bestScenarioId: ScenarioId;
  bestBuyScenarioId: ScenarioId;
  bestOverallLine: string;
  bestBuyLine: string;
  relocationLine: string;
  babyTimingIsMajorLimiter: boolean;
  narrativeSummary: string;
  familyAccessTradeoffs: string[];
  familyDistanceWorthItConditions: string[];
  secondaryIncomeRole: SecondaryIncomeRoleState;
  secondaryIncomeNotes: string[];
  buyLocalConditions: string[];
  buyRelocateConditions: string[];
  summaryText: string;
}
