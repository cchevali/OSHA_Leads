import test from "node:test";
import assert from "node:assert/strict";
import { isStepComplete } from "./flow";
import {
  createDefaultState,
  DEAL_BREAKER_COPY,
  migrateStoredState
} from "./questionnaire";
import {
  scoreAssessment,
  scoreScenarioDebug,
  WIRED_DEAL_BREAKERS
} from "./scoring";
import type { DealBreakerId, ScenarioId, ScorecardState } from "./types";

function buildAnsweredState(): ScorecardState {
  const state = createDefaultState();

  state.priorities.familyLifestyle = 5;
  state.priorities.stayCloseSupport = 4;

  state.family.moveDisruption = 4;
  state.family.delayedMoveChangesAnswer = "yes";
  state.family.minimumBabyAgeForMove = "12to18";
  state.family.familySupportReliance = 4;

  state.familyAccess.plannedVisitsAcceptable = "maybe";
  state.familyAccess.youngKidTravelDifficulty = 4;
  state.familyAccess.weeklyLifeImprovementNeeded = "clearly";
  state.familyAccess.repeatableVisitSystemHelp = "somewhat";

  state.finance.minimumSafeIncome = 180000;
  state.finance.insuranceContinuity = 5;
  state.finance.insurancePathConfidence = 3;
  state.finance.yearOneUncertaintyTolerance = 2;
  state.finance.minimumCashCushion = 90000;

  state.secondaryIncome.outsideIncomeSafetyBuffer = "somewhat";
  state.secondaryIncome.outsideIncomeRoleFeel = "depends";
  state.secondaryIncome.notRelyOnOutsideIncomeImportance = 4;

  state.lifestyle.afterHoursBurdenTolerance = 2;

  state.relocation.outOfStateOpenness = 3;
  state.relocation.localAcquisitionAcceptable = "yes";

  state.trust.operatingPlanTrust = 3;

  state.businessModel.stableTeamImportance = 4;

  state.nonNegotiables.spouseTimingAlignment = 3;

  return state;
}

function blockerIds(state: ScorecardState, scenarioId: ScenarioId) {
  return scoreScenarioDebug(state, scenarioId).blockers.map((blocker) => blocker.id);
}

test("migrated and fresh users score the same when visible answers match", () => {
  const fresh = buildAnsweredState();
  const legacyRaw = {
    schemaVersion: 5,
    currentStep: 11,
    priorities: {
      familyLifestyle: fresh.priorities.familyLifestyle,
      stayCloseSupport: fresh.priorities.stayCloseSupport,
      incomeGrowth: 1,
      reduceJobRisk: 5
    },
    family: {
      moveDisruption: fresh.family.moveDisruption,
      delayedMoveChangesAnswer: fresh.family.delayedMoveChangesAnswer,
      minimumBabyAgeForMove: fresh.family.minimumBabyAgeForMove,
      familySupportReliance: fresh.family.familySupportReliance,
      isolationConcern: 5,
      familyHelpDrive: 5
    },
    familyAccess: {
      plannedVisitsAcceptable: fresh.familyAccess.plannedVisitsAcceptable,
      youngKidTravelDifficulty: fresh.familyAccess.youngKidTravelDifficulty,
      weeklyLifeImprovementNeeded: fresh.familyAccess.weeklyLifeImprovementNeeded,
      repeatableVisitSystemHelp: fresh.familyAccess.repeatableVisitSystemHelp,
      parentTravelLimitationConcern: 5,
      householdTravelBurdenConcern: 5
    },
    finance: {
      minimumSafeIncome: fresh.finance.minimumSafeIncome,
      minimumCashCushion: fresh.finance.minimumCashCushion,
      insuranceContinuity: fresh.finance.insuranceContinuity,
      insurancePathConfidence: fresh.finance.insurancePathConfidence,
      yearOneUncertaintyTolerance: fresh.finance.yearOneUncertaintyTolerance,
      housingCostRule: "comparable"
    },
    secondaryIncome: {
      outsideIncomeSafetyBuffer: fresh.secondaryIncome.outsideIncomeSafetyBuffer,
      outsideIncomeRoleFeel: fresh.secondaryIncome.outsideIncomeRoleFeel,
      notRelyOnOutsideIncomeImportance: fresh.secondaryIncome.notRelyOnOutsideIncomeImportance
    },
    lifestyle: {
      afterHoursBurdenTolerance: fresh.lifestyle.afterHoursBurdenTolerance,
      temporaryStressWorthIt: 5,
      recurringRevenuePreference: 1,
      ownerHeroAvoidance: 5,
      eveningWeekendTolerance: 2
    },
    relocation: {
      outOfStateOpenness: fresh.relocation.outOfStateOpenness,
      localAcquisitionAcceptable: fresh.relocation.localAcquisitionAcceptable,
      travelFrictionTolerance: 1
    },
    trust: {
      operatingPlanTrust: fresh.trust.operatingPlanTrust,
      harderJobConcern: 4,
      businessPurchaseComfort: 5,
      waitForStabilityPreference: 5
    },
    businessModel: {
      stableTeamImportance: fresh.businessModel.stableTeamImportance,
      nonHeroBusinessImportance: 5
    },
    nonNegotiables: {
      spouseTimingAlignment: fresh.nonNegotiables.spouseTimingAlignment,
      dealBreakers: fresh.nonNegotiables.dealBreakers
    },
    scenarios: fresh.scenarios
  };

  const migrated = migrateStoredState(legacyRaw);
  assert.ok(migrated);

  const freshResults = scoreAssessment(fresh);
  const migratedResults = scoreAssessment(migrated);

  assert.deepEqual(
    freshResults.rankedScenarios.map((scenario) => ({
      id: scenario.id,
      score: scenario.score,
      status: scenario.status,
      rankingScore: scenario.rankingScore
    })),
    migratedResults.rankedScenarios.map((scenario) => ({
      id: scenario.id,
      score: scenario.score,
      status: scenario.status,
      rankingScore: scenario.rankingScore
    }))
  );
});

test("every visible deal breaker is wired and can surface a matching blocker", () => {
  assert.deepEqual(
    [...Object.keys(DEAL_BREAKER_COPY)].sort(),
    [...WIRED_DEAL_BREAKERS].sort()
  );

  const cases: Array<{
    id: DealBreakerId;
    scenarioId: ScenarioId;
    expectedBlockerId: string;
    mutate?: (state: ScorecardState) => void;
  }> = [
    { id: "familySupportDistance", scenarioId: "buyRelocate", expectedBlockerId: "family_support_distance" },
    { id: "weakInsurance", scenarioId: "buyLocal", expectedBlockerId: "insurance_gap" },
    { id: "incomeUncertainty", scenarioId: "buyLocal", expectedBlockerId: "income_uncertainty" },
    { id: "moveBeforeBabyAge", scenarioId: "buyRelocate", expectedBlockerId: "baby_timing" },
    { id: "eveningWeekendBurden", scenarioId: "buyLocal", expectedBlockerId: "family_burden" },
    { id: "sellerDependentBusiness", scenarioId: "buyLocal", expectedBlockerId: "seller_dependency" },
    { id: "cashCushion", scenarioId: "buyLocal", expectedBlockerId: "cash_cushion" },
    { id: "familyLifestyle", scenarioId: "buyLocal", expectedBlockerId: "family_lifestyle" },
    { id: "spouseTiming", scenarioId: "buyLocal", expectedBlockerId: "timing_alignment" }
  ];

  for (const item of cases) {
    const state = buildAnsweredState();
    state.nonNegotiables.dealBreakers[item.id] = true;
    item.mutate?.(state);
    assert.ok(
      blockerIds(state, item.scenarioId).includes(item.expectedBlockerId),
      `${item.id} should surface ${item.expectedBlockerId}`
    );
  }
});

test("numeric income and cash thresholds materially affect financial blocker logic", () => {
  const lowRequirement = buildAnsweredState();
  lowRequirement.finance.minimumSafeIncome = 120000;
  lowRequirement.finance.minimumCashCushion = 20000;

  const highRequirement = buildAnsweredState();
  highRequirement.finance.minimumSafeIncome = 180000;
  highRequirement.finance.minimumCashCushion = 120000;

  const lowDebug = scoreScenarioDebug(lowRequirement, "buyLocal");
  const highDebug = scoreScenarioDebug(highRequirement, "buyLocal");

  assert.notEqual(lowDebug.desiredRunwayMonths, highDebug.desiredRunwayMonths);
  assert.ok(highDebug.score < lowDebug.score);
  assert.ok(highDebug.blockers.some((blocker) => blocker.id === "cash_cushion"));
});

test("insurance continuity only changes the insurance concept and blocker path", () => {
  const safeState = buildAnsweredState();
  const insuranceProblem = buildAnsweredState();
  insuranceProblem.finance.insurancePathConfidence = 1;
  insuranceProblem.scenarios.buyLocal.insuranceQuality = 1;

  const safeDebug = scoreScenarioDebug(safeState, "buyLocal");
  const insuranceDebug = scoreScenarioDebug(insuranceProblem, "buyLocal");

  const safeFinancial = safeDebug.contributions.find((contribution) => contribution.id === "financial_safety");
  const badFinancial = insuranceDebug.contributions.find((contribution) => contribution.id === "financial_safety");
  const safeInsurance = safeDebug.contributions.find((contribution) => contribution.id === "insurance");
  const badInsurance = insuranceDebug.contributions.find((contribution) => contribution.id === "insurance");

  assert.ok(safeFinancial && badFinancial && safeInsurance && badInsurance);
  assert.equal(safeFinancial.fit, badFinancial.fit);
  assert.ok(badInsurance.fit < safeInsurance.fit);
  assert.ok(insuranceDebug.blockers.some((blocker) => blocker.id === "insurance_gap"));
});

test("stay put ignores transition insurance-path confidence", () => {
  const unresolved = buildAnsweredState();
  unresolved.finance.insurancePathConfidence = 1;
  unresolved.scenarios.stay.insuranceQuality = 5;

  const solved = buildAnsweredState();
  solved.finance.insurancePathConfidence = 5;
  solved.scenarios.stay.insuranceQuality = 5;

  const unresolvedStay = scoreScenarioDebug(unresolved, "stay");
  const solvedStay = scoreScenarioDebug(solved, "stay");
  const unresolvedBuy = scoreScenarioDebug(unresolved, "buyLocal");
  const solvedBuy = scoreScenarioDebug(solved, "buyLocal");

  const unresolvedStayInsurance = unresolvedStay.contributions.find(
    (contribution) => contribution.id === "insurance"
  );
  const solvedStayInsurance = solvedStay.contributions.find(
    (contribution) => contribution.id === "insurance"
  );

  assert.ok(unresolvedStayInsurance && solvedStayInsurance);
  assert.equal(unresolvedStay.score, solvedStay.score);
  assert.equal(unresolvedStayInsurance.fit, solvedStayInsurance.fit);
  assert.ok(unresolvedBuy.score < solvedBuy.score);
});

test("headline distinguishes best overall path from best acquisition path", () => {
  const state = buildAnsweredState();
  const results = scoreAssessment(state);

  assert.equal(results.bestScenarioId, "stay");
  assert.equal(results.bestBuyScenarioId, "buyLocal");
  assert.match(results.headline, /Stay put/i);
  assert.match(results.bestOverallLine, /Stay put/i);
  assert.match(results.bestBuyLine, /Buy a business without major relocation/i);
  assert.match(results.relocationLine, /Relocation/i);
});

test("maybe-now scenarios without blockers use softer concern copy and no conditions wording", () => {
  const state = createDefaultState();
  state.priorities.familyLifestyle = 5;
  state.priorities.stayCloseSupport = 3;
  state.family.moveDisruption = 2;
  state.family.delayedMoveChangesAnswer = "no";
  state.family.familySupportReliance = 2;
  state.familyAccess.plannedVisitsAcceptable = "yes";
  state.familyAccess.youngKidTravelDifficulty = 3;
  state.familyAccess.weeklyLifeImprovementNeeded = "clearly";
  state.familyAccess.repeatableVisitSystemHelp = "yes";
  state.finance.minimumSafeIncome = 180000;
  state.finance.minimumCashCushion = 60000;
  state.finance.insuranceContinuity = 5;
  state.finance.insurancePathConfidence = 4;
  state.finance.yearOneUncertaintyTolerance = 3;
  state.secondaryIncome.outsideIncomeSafetyBuffer = "not-much";
  state.lifestyle.afterHoursBurdenTolerance = 3;
  state.relocation.outOfStateOpenness = 5;
  state.relocation.localAcquisitionAcceptable = "not-sure";
  state.trust.operatingPlanTrust = 4;
  state.businessModel.stableTeamImportance = 4;
  state.nonNegotiables.spouseTimingAlignment = 4;

  state.scenarios.buyRelocate.incomeFit = 4;
  state.scenarios.buyRelocate.incomeStability = 4;
  state.scenarios.buyRelocate.insuranceQuality = 4;
  state.scenarios.buyRelocate.cashCushionFit = 4;
  state.scenarios.buyRelocate.familySupportAccess = 3;
  state.scenarios.buyRelocate.familyVisitEase = 3;
  state.scenarios.buyRelocate.visitSpontaneity = 3;
  state.scenarios.buyRelocate.kidTravelEase = 3;
  state.scenarios.buyRelocate.householdTravelBurdenFit = 3;
  state.scenarios.buyRelocate.visitSystemSupport = 4;
  state.scenarios.buyRelocate.moveTiming = "12to18";
  state.scenarios.buyRelocate.scheduleControl = 5;
  state.scenarios.buyRelocate.disruptionLevel = 3;
  state.scenarios.buyRelocate.housingLifestyleFit = 5;
  state.scenarios.buyRelocate.housingCostFit = "lower";
  state.scenarios.buyRelocate.operationalResilience = 4;

  const results = scoreAssessment(state);
  const relocate = results.rankedScenarios.find((scenario) => scenario.id === "buyRelocate");

  assert.ok(relocate);
  assert.equal(relocate.status, "maybe-now");
  assert.equal(relocate.requiredConditions.length, 0);
  assert.equal(results.relocationLine, "Relocation: viable now.");
  assert.match(relocate.explanation, /reads as viable now because/i);
  assert.ok(
    relocate.topConcerns.includes(
      "This path would make family support and easy access meaningfully less convenient."
    )
  );
  assert.ok(
    !relocate.topConcerns.includes(
      "This path appears to give up too much practical support or easy family access."
    )
  );
});

test("relocate defaults are no longer pre-judged when the household is broadly open", () => {
  const state = createDefaultState();
  state.priorities.familyLifestyle = 5;
  state.priorities.stayCloseSupport = 2;
  state.family.moveDisruption = 2;
  state.family.delayedMoveChangesAnswer = "maybe";
  state.family.minimumBabyAgeForMove = "12to18";
  state.familyAccess.plannedVisitsAcceptable = "maybe";
  state.finance.minimumSafeIncome = 180000;
  state.finance.minimumCashCushion = 60000;
  state.finance.insuranceContinuity = 3;
  state.finance.yearOneUncertaintyTolerance = 3;
  state.lifestyle.afterHoursBurdenTolerance = 3;
  state.relocation.outOfStateOpenness = 4;
  state.relocation.localAcquisitionAcceptable = "yes";
  state.familyAccess.weeklyLifeImprovementNeeded = "moderately";
  state.familyAccess.repeatableVisitSystemHelp = "somewhat";
  state.trust.operatingPlanTrust = 4;
  state.businessModel.stableTeamImportance = 4;
  state.nonNegotiables.spouseTimingAlignment = 4;

  const relocate = scoreScenarioDebug(state, "buyRelocate");

  assert.ok(relocate.score >= 45);
  assert.ok(!relocate.blockers.some((blocker) => blocker.id === "family_support_distance"));
  assert.ok(!relocate.blockers.some((blocker) => blocker.id === "baby_timing"));
});

test("reduced question flow keeps conditional branching and completion rules intact", () => {
  const familyState = createDefaultState();
  familyState.priorities.familyLifestyle = 5;
  familyState.priorities.stayCloseSupport = 2;
  familyState.family.moveDisruption = 3;
  familyState.family.delayedMoveChangesAnswer = "no";
  assert.equal(isStepComplete(familyState, 2), true);

  const familyAccessState = buildAnsweredState();
  familyAccessState.priorities.stayCloseSupport = 2;
  familyAccessState.family.familySupportReliance = 2;
  familyAccessState.familyAccess.youngKidTravelDifficulty = null;
  assert.equal(isStepComplete(familyAccessState, 3), true);

  const relocationState = buildAnsweredState();
  relocationState.familyAccess.weeklyLifeImprovementNeeded = null;
  relocationState.familyAccess.repeatableVisitSystemHelp = null;
  assert.equal(isStepComplete(relocationState, 7), false);
  relocationState.familyAccess.weeklyLifeImprovementNeeded = "clearly";
  relocationState.familyAccess.repeatableVisitSystemHelp = "somewhat";
  assert.equal(isStepComplete(relocationState, 7), true);

  const financeState = createDefaultState();
  financeState.finance.minimumSafeIncome = 150000;
  financeState.finance.minimumCashCushion = 50000;
  financeState.finance.insuranceContinuity = 2;
  financeState.finance.yearOneUncertaintyTolerance = 3;
  assert.equal(isStepComplete(financeState, 4), true);
  financeState.finance.insuranceContinuity = 5;
  assert.equal(isStepComplete(financeState, 4), false);
  financeState.finance.insurancePathConfidence = 3;
  assert.equal(isStepComplete(financeState, 4), true);

  const secondaryIncomeState = createDefaultState();
  secondaryIncomeState.secondaryIncome.outsideIncomeSafetyBuffer = "no-plan";
  assert.equal(isStepComplete(secondaryIncomeState, 5), true);
  secondaryIncomeState.secondaryIncome.outsideIncomeSafetyBuffer = "somewhat";
  assert.equal(isStepComplete(secondaryIncomeState, 5), false);
  secondaryIncomeState.secondaryIncome.outsideIncomeRoleFeel = "depends";
  secondaryIncomeState.secondaryIncome.notRelyOnOutsideIncomeImportance = 4;
  assert.equal(isStepComplete(secondaryIncomeState, 5), true);

  const businessModelState = createDefaultState();
  businessModelState.trust.operatingPlanTrust = 2;
  businessModelState.relocation.outOfStateOpenness = 2;
  businessModelState.relocation.localAcquisitionAcceptable = "no";
  assert.equal(isStepComplete(businessModelState, 9), true);
  businessModelState.trust.operatingPlanTrust = 4;
  assert.equal(isStepComplete(businessModelState, 9), false);
  businessModelState.businessModel.stableTeamImportance = 4;
  assert.equal(isStepComplete(businessModelState, 9), true);
});
