import { STEPS } from "@/lib/famscorecard/questionnaire";
import type { ScorecardState } from "@/lib/famscorecard/types";

export function shouldAskMinimumBabyAge(state: ScorecardState["family"]) {
  return state.delayedMoveChangesAnswer === "yes" || state.delayedMoveChangesAnswer === "maybe";
}

export function shouldAskSupportReliance(state: ScorecardState) {
  return (state.priorities.stayCloseSupport ?? 0) >= 3;
}

export function shouldAskTravelBurdenDetail(state: ScorecardState) {
  return (
    (state.priorities.stayCloseSupport ?? 0) >= 3 ||
    (state.family.familySupportReliance ?? 0) >= 3
  );
}

export function shouldAskInsurancePathConfidence(state: ScorecardState) {
  return (state.finance.insuranceContinuity ?? 0) >= 3;
}

export function shouldAskRelocationTradeoff(state: ScorecardState) {
  return (state.relocation.outOfStateOpenness ?? 0) >= 3;
}

export function shouldAskVisitSystemFollowUp(state: ScorecardState) {
  return (
    shouldAskRelocationTradeoff(state) &&
    state.familyAccess.plannedVisitsAcceptable !== null &&
    state.familyAccess.plannedVisitsAcceptable !== "no"
  );
}

export function shouldAskSecondaryIncomeFollowUp(state: ScorecardState["secondaryIncome"]) {
  return state.outsideIncomeSafetyBuffer === "material" || state.outsideIncomeSafetyBuffer === "somewhat";
}

export function shouldAskPredictableHours(_state: ScorecardState["secondaryIncome"]) {
  return false;
}

export function shouldAskTemporaryStressWorthIt(_state: ScorecardState) {
  return false;
}

export function shouldAskBusinessModelDetail(state: ScorecardState) {
  return (
    (state.trust.operatingPlanTrust ?? 0) >= 3 ||
    state.relocation.localAcquisitionAcceptable === "yes" ||
    (state.relocation.outOfStateOpenness ?? 0) >= 3
  );
}

export function isStepComplete(state: ScorecardState, stepIndex: number) {
  switch (STEPS[stepIndex].id) {
    case "intro":
      return true;
    case "priorities":
      return (
        state.priorities.familyLifestyle !== null &&
        state.priorities.stayCloseSupport !== null
      );
    case "family":
      return (
        state.family.moveDisruption !== null &&
        state.family.delayedMoveChangesAnswer !== null &&
        (!shouldAskSupportReliance(state) || state.family.familySupportReliance !== null) &&
        (!shouldAskMinimumBabyAge(state.family) || state.family.minimumBabyAgeForMove !== null)
      );
    case "familyAccess":
      return (
        state.familyAccess.plannedVisitsAcceptable !== null &&
        (!shouldAskTravelBurdenDetail(state) || state.familyAccess.youngKidTravelDifficulty !== null)
      );
    case "finance":
      return (
        state.finance.minimumSafeIncome !== null &&
        state.finance.minimumSafeIncome > 0 &&
        state.finance.minimumCashCushion !== null &&
        state.finance.minimumCashCushion >= 0 &&
        state.finance.insuranceContinuity !== null &&
        (!shouldAskInsurancePathConfidence(state) || state.finance.insurancePathConfidence !== null) &&
        state.finance.yearOneUncertaintyTolerance !== null
      );
    case "secondaryIncome":
      return (
        state.secondaryIncome.outsideIncomeSafetyBuffer !== null &&
        (!shouldAskSecondaryIncomeFollowUp(state.secondaryIncome) ||
          (state.secondaryIncome.outsideIncomeRoleFeel !== null &&
            state.secondaryIncome.notRelyOnOutsideIncomeImportance !== null))
      );
    case "lifestyle":
      return state.lifestyle.afterHoursBurdenTolerance !== null;
    case "relocation":
      return (
        state.relocation.outOfStateOpenness !== null &&
        state.relocation.localAcquisitionAcceptable !== null &&
        (!shouldAskRelocationTradeoff(state) || state.familyAccess.weeklyLifeImprovementNeeded !== null) &&
        (!shouldAskVisitSystemFollowUp(state) || state.familyAccess.repeatableVisitSystemHelp !== null)
      );
    case "trust":
      return state.trust.operatingPlanTrust !== null;
    case "businessModel":
      return !shouldAskBusinessModelDetail(state) || state.businessModel.stableTeamImportance !== null;
    case "nonNegotiables":
      return state.nonNegotiables.spouseTimingAlignment !== null;
    case "results":
      return true;
    default:
      return false;
  }
}
