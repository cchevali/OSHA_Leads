"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";
import BlockerConditionSummary from "@/components/famscorecard/BlockerConditionSummary";
import CheckboxCards from "@/components/famscorecard/CheckboxCards";
import ChoiceGroup from "@/components/famscorecard/ChoiceGroup";
import NumericField from "@/components/famscorecard/NumericField";
import ProgressHeader from "@/components/famscorecard/ProgressHeader";
import QuestionCard from "@/components/famscorecard/QuestionCard";
import ScenarioAssumptionEditor from "@/components/famscorecard/ScenarioAssumptionEditor";
import ScenarioScoreCard from "@/components/famscorecard/ScenarioScoreCard";
import StickyActionBar from "@/components/famscorecard/StickyActionBar";
import {
  ALIGNMENT_OPTIONS,
  BABY_AGE_OPTIONS,
  buildSummaryTitle,
  CONCERN_OPTIONS,
  CONFIDENCE_OPTIONS,
  createDefaultState,
  DEAL_BREAKER_COPY,
  DELAY_OPTIONS,
  DISRUPTION_OPTIONS,
  formatCurrency,
  getBabyAgeLabel,
  getPlannedVisitTradeoffLabel,
  getVisitSystemHelpLabel,
  getWeeklyLifeTradeoffLabel,
  IMPORTANCE_OPTIONS,
  LEGACY_STORAGE_KEYS,
  loadStoredState,
  LOCAL_ACQUISITION_OPTIONS,
  PLANNED_VISIT_TRADEOFF_OPTIONS,
  OPENNESS_OPTIONS,
  RELIANCE_OPTIONS,
  SCENARIO_META,
  SECONDARY_INCOME_BUFFER_OPTIONS,
  SECONDARY_INCOME_BURDEN_OPTIONS,
  STEPS,
  STORAGE_KEY,
  TOLERANCE_OPTIONS,
  VISIT_SYSTEM_HELP_OPTIONS,
  WEEKLY_LIFE_TRADEOFF_OPTIONS
} from "@/lib/famscorecard/questionnaire";
import {
  isStepComplete,
  shouldAskBusinessModelDetail,
  shouldAskInsurancePathConfidence,
  shouldAskMinimumBabyAge,
  shouldAskRelocationTradeoff,
  shouldAskSecondaryIncomeFollowUp,
  shouldAskSupportReliance,
  shouldAskTravelBurdenDetail,
  shouldAskVisitSystemFollowUp
} from "@/lib/famscorecard/flow";
import { scoreAssessment } from "@/lib/famscorecard/scoring";
import type {
  DealBreakerId,
  ScenarioAssumptions,
  ScenarioId,
  ScenarioResult,
  ScorecardState
} from "@/lib/famscorecard/types";

const STATUS_META: Record<
  ScenarioResult["status"],
  {
    badge: string;
    panel: string;
    label: string;
  }
> = {
  no: {
    badge: "bg-red-100 text-red-700 dark:bg-red-500/15 dark:text-red-200",
    panel: "border-red-200/80 bg-red-50/60 dark:border-red-500/30 dark:bg-red-500/8",
    label: "No"
  },
  "maybe-later": {
    badge: "bg-amber-100 text-amber-700 dark:bg-amber-500/15 dark:text-amber-100",
    panel: "border-amber-200/80 bg-amber-50/60 dark:border-amber-500/30 dark:bg-amber-500/8",
    label: "Maybe later"
  },
  "maybe-now": {
    badge: "bg-emerald-100 text-emerald-700 dark:bg-emerald-500/15 dark:text-emerald-100",
    panel: "border-emerald-200/80 bg-emerald-50/60 dark:border-emerald-500/30 dark:bg-emerald-500/8",
    label: "Maybe now"
  }
};

function formatSavedLabel(timestamp: string | null) {
  if (!timestamp) {
    return "Saved locally";
  }

  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return "Saved locally";
  }

  return `Saved ${new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  }).format(date)}`;
}

function formatLocalDate() {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric"
  }).format(new Date());
}

function getDelayLabel(value: ScorecardState["family"]["delayedMoveChangesAnswer"]) {
  return DELAY_OPTIONS.find((option) => option.value === value)?.label || "Not answered";
}

function getLocalAcquisitionLabel(value: ScorecardState["relocation"]["localAcquisitionAcceptable"]) {
  if (value === "yes") {
    return "Materially different";
  }

  if (value === "no") {
    return "Not materially different";
  }

  if (value === "not-sure") {
    return "Still unclear";
  }

  return "Not answered";
}

function getConfidenceLabel(value: number | null) {
  return CONFIDENCE_OPTIONS.find((option) => option.value === value)?.label || "Not answered";
}

function ReviewStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-cardBorder bg-card px-4 py-3">
      <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">{label}</p>
      <p className="mt-2 text-sm font-semibold text-ink">{value}</p>
    </div>
  );
}

function OutcomeCard({
  label,
  scenario,
  detail
}: {
  label: string;
  scenario: ScenarioResult;
  detail: string;
}) {
  const tone = STATUS_META[scenario.status];

  return (
    <div className={`rounded-[24px] border p-4 ${tone.panel}`}>
      <div className="flex items-center justify-between gap-3">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">{label}</p>
        <span className={`inline-flex rounded-full px-3 py-1 text-xs font-semibold ${tone.badge}`}>
          {tone.label}
        </span>
      </div>
      <p className="mt-3 text-lg font-semibold text-ink">{scenario.label}</p>
      <p className="mt-2 text-sm leading-6 text-inkMuted">{detail}</p>
    </div>
  );
}

export default function FamilyFitScorecardApp() {
  const [state, setState] = useState<ScorecardState>(() => createDefaultState());
  const [hasHydrated, setHasHydrated] = useState(false);
  const [restored, setRestored] = useState(false);
  const [copyStatus, setCopyStatus] = useState<"idle" | "copied" | "error">("idle");
  const [activeScenario, setActiveScenario] = useState<ScenarioId>("stay");
  const stepTopRef = useRef<HTMLDivElement | null>(null);
  const stepContentRef = useRef<HTMLDivElement | null>(null);
  const previousStepRef = useRef(0);

  useEffect(() => {
    try {
      const { state: storedState, sourceKey } = loadStoredState(window.localStorage);
      if (storedState) {
        setState(storedState);
        setRestored(true);

        if (sourceKey && sourceKey !== STORAGE_KEY) {
          window.localStorage.setItem(STORAGE_KEY, JSON.stringify(storedState));
          LEGACY_STORAGE_KEYS.forEach((key) => window.localStorage.removeItem(key));
        }
      }
    } catch {
      setState(createDefaultState());
    } finally {
      setHasHydrated(true);
    }
  }, []);

  useEffect(() => {
    if (!hasHydrated) {
      return;
    }

    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  }, [hasHydrated, state]);

  useEffect(() => {
    if (typeof window === "undefined" || !("scrollRestoration" in window.history)) {
      return;
    }

    const previous = window.history.scrollRestoration;
    window.history.scrollRestoration = "manual";

    return () => {
      window.history.scrollRestoration = previous;
    };
  }, []);

  useEffect(() => {
    if (copyStatus === "idle") {
      return;
    }

    const timeout = window.setTimeout(() => setCopyStatus("idle"), 2200);
    return () => window.clearTimeout(timeout);
  }, [copyStatus]);

  const commit = (updater: (previous: ScorecardState) => ScorecardState) => {
    setState((previous) => ({
      ...updater(previous),
      lastSavedAt: new Date().toISOString()
    }));
  };

  const resetStepScrollPosition = () => {
    if (typeof window === "undefined") {
      return;
    }

    const activeElement = document.activeElement;
    if (activeElement instanceof HTMLElement && activeElement !== document.body) {
      activeElement.blur();
    }

    if (stepContentRef.current) {
      stepContentRef.current.scrollTop = 0;
      stepContentRef.current.scrollLeft = 0;
    }

    if (document.scrollingElement) {
      document.scrollingElement.scrollTop = 0;
      document.scrollingElement.scrollLeft = 0;
    }

    document.documentElement.scrollTop = 0;
    document.body.scrollTop = 0;
    window.scrollTo(0, 0);
    stepTopRef.current?.scrollIntoView({ block: "start", inline: "nearest", behavior: "auto" });
  };

  useEffect(() => {
    if (!hasHydrated) {
      previousStepRef.current = state.currentStep;
      return;
    }

    if (previousStepRef.current === state.currentStep) {
      return;
    }

    previousStepRef.current = state.currentStep;

    let frameOne = 0;
    let frameTwo = 0;
    const timeout = window.setTimeout(() => {
      resetStepScrollPosition();
    }, 80);

    resetStepScrollPosition();
    frameOne = window.requestAnimationFrame(() => {
      resetStepScrollPosition();
      frameTwo = window.requestAnimationFrame(() => {
        resetStepScrollPosition();
      });
    });

    return () => {
      window.clearTimeout(timeout);
      window.cancelAnimationFrame(frameOne);
      window.cancelAnimationFrame(frameTwo);
    };
  }, [hasHydrated, state.currentStep]);

  const results = scoreAssessment(state);
  const currentStep = STEPS[state.currentStep];
  const totalSteps = STEPS.length;
  const stepNumber = state.currentStep + 1;
  const canContinue = isStepComplete(state, state.currentStep);
  const buyLocalScenario =
    results.rankedScenarios.find((scenario) => scenario.id === "buyLocal") ?? results.rankedScenarios[0];
  const relocateScenario =
    results.rankedScenarios.find((scenario) => scenario.id === "buyRelocate") ?? results.rankedScenarios[0];
  const bestBuyScenario =
    results.rankedScenarios.find((scenario) => scenario.id === results.bestBuyScenarioId) ?? buyLocalScenario;
  const bestOverallScenario =
    results.rankedScenarios.find((scenario) => scenario.id === results.bestScenarioId) ?? results.rankedScenarios[0];

  const updatePriorities = <K extends keyof ScorecardState["priorities"]>(
    field: K,
    value: NonNullable<ScorecardState["priorities"][K]>
  ) =>
    commit((previous) => {
      const nextPriorities = {
        ...previous.priorities,
        [field]: value
      };
      const nextFamily = { ...previous.family };
      const nextFamilyAccess = { ...previous.familyAccess };

      if (field === "stayCloseSupport" && typeof value === "number" && value < 3) {
        nextFamily.familySupportReliance = null;
        nextFamilyAccess.youngKidTravelDifficulty = null;
      }

      return {
        ...previous,
        priorities: nextPriorities,
        family: nextFamily,
        familyAccess: nextFamilyAccess
      };
    });

  const updateFamily = (field: keyof ScorecardState["family"], value: string | number | null) =>
    commit((previous) => {
      const nextFamily = {
        ...previous.family,
        [field]: value
      };

      if (field === "delayedMoveChangesAnswer" && value === "no") {
        nextFamily.minimumBabyAgeForMove = null;
      }

      return {
        ...previous,
        family: nextFamily
      };
    });

  const updateFamilyAccess = (
    field: keyof ScorecardState["familyAccess"],
    value: string | number | null
  ) =>
    commit((previous) => {
      const nextFamilyAccess = {
        ...previous.familyAccess,
        [field]: value
      };

      if (field === "plannedVisitsAcceptable" && value === "no") {
        nextFamilyAccess.repeatableVisitSystemHelp = null;
      }

      return {
        ...previous,
        familyAccess: nextFamilyAccess
      };
    });

  const updateFinance = (field: keyof ScorecardState["finance"], value: string | number | null) =>
    commit((previous) => {
      const nextFinance = {
        ...previous.finance,
        [field]: value
      };

      if (field === "insuranceContinuity" && typeof value === "number" && value < 3) {
        nextFinance.insurancePathConfidence = null;
      }

      return {
        ...previous,
        finance: nextFinance
      };
    });

  const updateLifestyle = <K extends keyof ScorecardState["lifestyle"]>(
    field: K,
    value: NonNullable<ScorecardState["lifestyle"][K]>
  ) =>
    commit((previous) => ({
      ...previous,
      lifestyle: {
        ...previous.lifestyle,
        [field]: value
      }
    }));

  const updateSecondaryIncome = (
    field: keyof ScorecardState["secondaryIncome"],
    value: string | number | null
  ) =>
    commit((previous) => ({
      ...previous,
      secondaryIncome: {
        ...previous.secondaryIncome,
        [field]: value
      }
    }));

  const updateRelocation = (field: keyof ScorecardState["relocation"], value: string | number) =>
    commit((previous) => {
      const nextRelocation = {
        ...previous.relocation,
        [field]: value
      };
      const nextFamilyAccess = { ...previous.familyAccess };

      if (field === "outOfStateOpenness" && typeof value === "number" && value < 3) {
        nextFamilyAccess.weeklyLifeImprovementNeeded = null;
        nextFamilyAccess.repeatableVisitSystemHelp = null;
      }

      return {
        ...previous,
        relocation: nextRelocation,
        familyAccess: nextFamilyAccess
      };
    });

  const updateTrust = <K extends keyof ScorecardState["trust"]>(
    field: K,
    value: NonNullable<ScorecardState["trust"][K]>
  ) =>
    commit((previous) => ({
      ...previous,
      trust: {
        ...previous.trust,
        [field]: value
      }
    }));

  const updateBusinessModel = <K extends keyof ScorecardState["businessModel"]>(
    field: K,
    value: NonNullable<ScorecardState["businessModel"][K]>
  ) =>
    commit((previous) => ({
      ...previous,
      businessModel: {
        ...previous.businessModel,
        [field]: value
      }
    }));

  const updateDealBreaker = (id: DealBreakerId, checked: boolean) =>
    commit((previous) => ({
      ...previous,
      nonNegotiables: {
        ...previous.nonNegotiables,
        dealBreakers: {
          ...previous.nonNegotiables.dealBreakers,
          [id]: checked
        }
      }
    }));

  const updateScenario = <K extends keyof ScenarioAssumptions>(
    scenarioId: ScenarioId,
    field: K,
    value: ScenarioAssumptions[K]
  ) =>
    commit((previous) => ({
      ...previous,
      scenarios: {
        ...previous.scenarios,
        [scenarioId]: {
          ...previous.scenarios[scenarioId],
          [field]: value
        }
      }
    }));

  const goBack = () => {
    if (state.currentStep === 0) {
      return;
    }

    const activeElement = document.activeElement;
    if (activeElement instanceof HTMLElement && activeElement !== document.body) {
      activeElement.blur();
    }
    commit((previous) => ({
      ...previous,
      currentStep: Math.max(0, previous.currentStep - 1)
    }));
  };

  const goForward = () => {
    if (state.currentStep === totalSteps - 1) {
      window.print();
      return;
    }

    if (!canContinue) {
      return;
    }

    const activeElement = document.activeElement;
    if (activeElement instanceof HTMLElement && activeElement !== document.body) {
      activeElement.blur();
    }
    commit((previous) => ({
      ...previous,
      currentStep: Math.min(totalSteps - 1, previous.currentStep + 1)
    }));
  };

  const resetAssessment = () => {
    if (!window.confirm("Reset the saved assessment on this browser?")) {
      return;
    }

    const next = createDefaultState();
    setState(next);
    setActiveScenario("stay");
    setRestored(false);
    setCopyStatus("idle");
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    LEGACY_STORAGE_KEYS.forEach((key) => window.localStorage.removeItem(key));
  };

  const copySummary = async () => {
    try {
      await navigator.clipboard.writeText(results.summaryText);
      setCopyStatus("copied");
    } catch {
      setCopyStatus("error");
    }
  };

  const continueLabel =
    currentStep.id === "intro"
      ? "Start assessment"
      : currentStep.id === "results"
        ? "Print / save PDF"
        : "Continue";

  const earliestMoveLabel = shouldAskMinimumBabyAge(state.family)
    ? getBabyAgeLabel(state.family.minimumBabyAgeForMove)
    : "Timing is not the main issue";
  const delayLabel = getDelayLabel(state.family.delayedMoveChangesAnswer);
  const localAcquisitionLabel = getLocalAcquisitionLabel(state.relocation.localAcquisitionAcceptable);
  const plannedVisitsLabel = getPlannedVisitTradeoffLabel(state.familyAccess.plannedVisitsAcceptable);
  const weeklyLifeTradeoffLabel = getWeeklyLifeTradeoffLabel(state.familyAccess.weeklyLifeImprovementNeeded);
  const visitSystemHelpLabel = getVisitSystemHelpLabel(state.familyAccess.repeatableVisitSystemHelp);
  const insurancePathLabel = shouldAskInsurancePathConfidence(state)
    ? getConfidenceLabel(state.finance.insurancePathConfidence)
    : "Not a major constraint";

  let sectionBody: ReactNode = null;

  switch (currentStep.id) {
    case "intro":
      sectionBody = (
        <div className="space-y-6">
          <section className="animate-fade-up rounded-[32px] border border-cardBorder bg-paper p-6 shadow-soft sm:p-8">
            <div className="max-w-3xl space-y-5">
              <p className="inline-flex rounded-full border border-cardBorder bg-card px-4 py-2 text-xs font-semibold uppercase tracking-[0.28em] text-ocean">
                Careful private decision aid
              </p>
              <h2 className="font-display text-4xl text-ink sm:text-5xl">
                Clarify whether buying a business and possibly relocating fits the household right now.
              </h2>
              <p className="text-lg leading-8 text-inkMuted">
                This scorecard is meant to help a couple pressure-test family impact, timing, support
                loss, and risk before getting pulled into a business story. &quot;Not now&quot; is a
                valid outcome.
              </p>
              <div className="grid gap-3 sm:grid-cols-3">
                <div className="rounded-2xl border border-cardBorder bg-card px-4 py-4">
                  <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">Outcome</p>
                  <p className="mt-2 text-lg font-semibold text-ink">No</p>
                </div>
                <div className="rounded-2xl border border-cardBorder bg-card px-4 py-4">
                  <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">Outcome</p>
                  <p className="mt-2 text-lg font-semibold text-ink">Maybe later</p>
                </div>
                <div className="rounded-2xl border border-cardBorder bg-card px-4 py-4">
                  <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">Outcome</p>
                  <p className="mt-2 text-lg font-semibold text-ink">Maybe now</p>
                </div>
              </div>
            </div>
          </section>

          <section className="grid gap-4 md:grid-cols-[1.15fr_0.85fr]">
            <div className="rounded-[28px] border border-cardBorder bg-card p-6 shadow-soft">
              <p className="text-xs font-semibold uppercase tracking-[0.24em] text-ocean">How it works</p>
              <div className="mt-4 space-y-4 text-sm leading-7 text-inkMuted sm:text-base">
                <p>1. Set priorities, timing limits, support needs, and household non-negotiables.</p>
                <p>2. Review the default assumptions for staying put, buying local, and buying plus relocating.</p>
                <p>3. Get a balanced ranking, top blockers, needed conditions, and a printable summary.</p>
              </div>
            </div>
            <div className="rounded-[28px] border border-cardBorder bg-card p-6 shadow-soft">
              <p className="text-xs font-semibold uppercase tracking-[0.24em] text-ocean">Privacy</p>
              <p className="mt-4 text-sm leading-7 text-inkMuted sm:text-base">
                Answers stay in this browser for save-and-resume. Nothing is sent anywhere in this first
                version unless you choose to print or copy your summary.
              </p>
              {restored ? (
                <p className="mt-4 rounded-2xl border border-ocean/20 bg-ocean/10 px-4 py-3 text-sm font-semibold text-ink">
                  Your last saved answers were restored on this device.
                </p>
              ) : null}
            </div>
          </section>
        </div>
      );
      break;

    case "priorities":
      sectionBody = (
        <div className="space-y-4">
          <QuestionCard title="How important is it that any change clearly improve weekly family life, not just income?">
            <ChoiceGroup
              name="familyLifestyle"
              value={state.priorities.familyLifestyle}
              onChange={(value) => updatePriorities("familyLifestyle", value)}
              options={IMPORTANCE_OPTIONS}
            />
          </QuestionCard>
          <QuestionCard title="How important is staying within reasonably easy reach of family support or extended family?">
            <ChoiceGroup
              name="stayCloseSupport"
              value={state.priorities.stayCloseSupport}
              onChange={(value) => updatePriorities("stayCloseSupport", value)}
              options={IMPORTANCE_OPTIONS}
            />
          </QuestionCard>
        </div>
      );
      break;

    case "family":
      sectionBody = (
        <div className="space-y-4">
          <QuestionCard title="How disruptive would a move feel with a baby this young?">
            <ChoiceGroup
              name="moveDisruption"
              value={state.family.moveDisruption}
              onChange={(value) => updateFamily("moveDisruption", value)}
              options={DISRUPTION_OPTIONS}
            />
          </QuestionCard>
          <QuestionCard title="Would delaying a move until the baby is older materially change how this feels?">
            <ChoiceGroup
              name="delayedMoveChangesAnswer"
              value={state.family.delayedMoveChangesAnswer}
              onChange={(value) => updateFamily("delayedMoveChangesAnswer", value)}
              options={DELAY_OPTIONS}
            />
          </QuestionCard>
          {shouldAskMinimumBabyAge(state.family) ? (
            <QuestionCard
              title="If timing does matter, what is the minimum baby age where a move starts to feel more acceptable?"
              hint="This answer materially affects relocation viability."
            >
              <ChoiceGroup
                name="minimumBabyAgeForMove"
                value={state.family.minimumBabyAgeForMove}
                onChange={(value) => updateFamily("minimumBabyAgeForMove", value)}
                options={BABY_AGE_OPTIONS}
                columns={2}
              />
            </QuestionCard>
          ) : state.family.delayedMoveChangesAnswer === "no" ? (
            <section className="animate-fade-up rounded-[28px] border border-cardBorder bg-card px-5 py-4 shadow-soft">
              <p className="text-sm leading-7 text-inkMuted">
                You indicated timing may not be the main issue. The results will weigh support loss,
                disruption, trust, and family burden more heavily than a baby-age cutoff.
              </p>
            </section>
          ) : null}
          {shouldAskSupportReliance(state) ? (
            <QuestionCard title="How much do you rely on nearby family help or the current support rhythm right now?">
              <ChoiceGroup
                name="familySupportReliance"
                value={state.family.familySupportReliance}
                onChange={(value) => updateFamily("familySupportReliance", value)}
                options={RELIANCE_OPTIONS}
              />
            </QuestionCard>
          ) : (
            <section className="rounded-[28px] border border-cardBorder bg-card px-5 py-4 shadow-soft">
              <p className="text-sm leading-7 text-inkMuted">
                Because easy family reach does not currently read as a major priority, the scorecard skips
                the extra support-rhythm question here.
              </p>
            </section>
          )}
        </div>
      );
      break;

    case "familyAccess":
      sectionBody = (
        <div className="space-y-4">
          <QuestionCard title="Would fewer but more deliberate, carefully planned family visits feel acceptable if weekly household life were meaningfully better?">
            <ChoiceGroup
              name="plannedVisitsAcceptable"
              value={state.familyAccess.plannedVisitsAcceptable}
              onChange={(value) => updateFamilyAccess("plannedVisitsAcceptable", value)}
              options={PLANNED_VISIT_TRADEOFF_OPTIONS}
            />
          </QuestionCard>
          {shouldAskTravelBurdenDetail(state) ? (
            <QuestionCard
              title="How heavy would long-distance family visiting feel if most of the travel logistics had to be managed by your household with young kids?"
              hint="This captures travel burden, parent travel limits, and the loss of low-friction visits in one place."
            >
              <ChoiceGroup
                name="youngKidTravelDifficulty"
                value={state.familyAccess.youngKidTravelDifficulty}
                onChange={(value) => updateFamilyAccess("youngKidTravelDifficulty", value)}
                options={CONCERN_OPTIONS}
              />
            </QuestionCard>
          ) : (
            <section className="rounded-[28px] border border-cardBorder bg-card px-5 py-4 shadow-soft">
              <p className="text-sm leading-7 text-inkMuted">
                You did not mark extended-family access as a major constraint so this step stays focused on
                the main tradeoff rather than deeper travel detail.
              </p>
            </section>
          )}
        </div>
      );
      break;

    case "finance":
      sectionBody = (
        <div className="space-y-4">
          <NumericField
            id="minimumSafeIncome"
            label="Minimum household income required to feel safe"
            hint="Use your real number. It anchors whether a scenario clears the household safety bar."
            prefix="$"
            placeholder="180000"
            value={state.finance.minimumSafeIncome}
            onChange={(value) => updateFinance("minimumSafeIncome", value)}
          />
          <QuestionCard title="How important is strong health insurance continuity for the household?">
            <ChoiceGroup
              name="insuranceContinuity"
              value={state.finance.insuranceContinuity}
              onChange={(value) => updateFinance("insuranceContinuity", value)}
              options={IMPORTANCE_OPTIONS}
            />
          </QuestionCard>
          {shouldAskInsurancePathConfidence(state) ? (
            <QuestionCard
              title="How confident are you that there is a credible path to acceptable health coverage during the transition?"
              hint="This only shows up when continuity matters enough to change the decision."
            >
              <ChoiceGroup
                name="insurancePathConfidence"
                value={state.finance.insurancePathConfidence}
                onChange={(value) => updateFinance("insurancePathConfidence", value)}
                options={CONFIDENCE_OPTIONS}
              />
            </QuestionCard>
          ) : (
            <section className="rounded-[28px] border border-cardBorder bg-card px-5 py-4 shadow-soft">
              <p className="text-sm leading-7 text-inkMuted">
                Because insurance continuity does not currently read as a major constraint, the scorecard
                skips the extra transition-path question here.
              </p>
            </section>
          )}
          <QuestionCard title="How much year-one income uncertainty or month-to-month volatility feels acceptable?">
            <ChoiceGroup
              name="yearOneUncertaintyTolerance"
              value={state.finance.yearOneUncertaintyTolerance}
              onChange={(value) => updateFinance("yearOneUncertaintyTolerance", value)}
              options={TOLERANCE_OPTIONS}
            />
          </QuestionCard>
          <NumericField
            id="minimumCashCushion"
            label="Minimum cash cushion desired after closing or a move"
            hint="Use the lowest post-close buffer that still feels safe."
            prefix="$"
            placeholder="75000"
            value={state.finance.minimumCashCushion}
            onChange={(value) => updateFinance("minimumCashCushion", value)}
          />
        </div>
      );
      break;

    case "secondaryIncome":
      sectionBody = (
        <div className="space-y-4">
          <section className="rounded-[28px] border border-cardBorder bg-card px-5 py-4 shadow-soft">
            <p className="text-sm leading-7 text-inkMuted">
              This section treats outside income as optional household cushion only. It is not assumed,
              expected, or used to rescue a weak plan unless your answers explicitly support it.
            </p>
          </section>
          <QuestionCard title="Would optional part-time income from outside the business make this path feel safer?">
            <ChoiceGroup
              name="outsideIncomeSafetyBuffer"
              value={state.secondaryIncome.outsideIncomeSafetyBuffer}
              onChange={(value) => updateSecondaryIncome("outsideIncomeSafetyBuffer", value)}
              options={SECONDARY_INCOME_BUFFER_OPTIONS}
            />
          </QuestionCard>
          {shouldAskSecondaryIncomeFollowUp(state.secondaryIncome) ? (
            <>
              <QuestionCard title="How would a part-time role outside the business feel during a transition period?">
                <ChoiceGroup
                  name="outsideIncomeRoleFeel"
                  value={state.secondaryIncome.outsideIncomeRoleFeel}
                  onChange={(value) => updateSecondaryIncome("outsideIncomeRoleFeel", value)}
                  options={SECONDARY_INCOME_BURDEN_OPTIONS}
                />
              </QuestionCard>
              <QuestionCard title="How important is it that this plan not rely on you taking outside work in order to feel responsible?">
                <ChoiceGroup
                  name="notRelyOnOutsideIncomeImportance"
                  value={state.secondaryIncome.notRelyOnOutsideIncomeImportance}
                  onChange={(value) => updateSecondaryIncome("notRelyOnOutsideIncomeImportance", value)}
                  options={IMPORTANCE_OPTIONS}
                />
              </QuestionCard>
            </>
          ) : (
            <section className="rounded-[28px] border border-cardBorder bg-card px-5 py-4 shadow-soft">
              <p className="text-sm leading-7 text-inkMuted">
                You did not indicate that outside income is part of the safety plan, so the tool does not
                ask for extra detail here.
              </p>
            </section>
          )}
        </div>
      );
      break;

    case "lifestyle":
      sectionBody = (
        <div className="space-y-4">
          <QuestionCard title="How much extra evening, weekend, or solo-parenting burden would feel acceptable to the family?">
            <ChoiceGroup
              name="afterHoursBurdenTolerance"
              value={state.lifestyle.afterHoursBurdenTolerance}
              onChange={(value) => updateLifestyle("afterHoursBurdenTolerance", value)}
              options={TOLERANCE_OPTIONS}
            />
          </QuestionCard>
        </div>
      );
      break;

    case "relocation":
      sectionBody = (
        <div className="space-y-4">
          <QuestionCard title="How open is the household to relocation at all if the overall fit is strong?">
            <ChoiceGroup
              name="outOfStateOpenness"
              value={state.relocation.outOfStateOpenness}
              onChange={(value) => updateRelocation("outOfStateOpenness", value)}
              options={OPENNESS_OPTIONS}
            />
          </QuestionCard>
          <QuestionCard title="Would a local business purchase feel materially different from an out-of-state move?">
            <ChoiceGroup
              name="localAcquisitionAcceptable"
              value={state.relocation.localAcquisitionAcceptable}
              onChange={(value) => updateRelocation("localAcquisitionAcceptable", value)}
              options={LOCAL_ACQUISITION_OPTIONS}
              columns={2}
            />
          </QuestionCard>
          {shouldAskRelocationTradeoff(state) ? (
            <QuestionCard title="How much better would weekly family life need to be in order to justify harder access to extended family?">
              <ChoiceGroup
                name="weeklyLifeImprovementNeeded"
                value={state.familyAccess.weeklyLifeImprovementNeeded}
                onChange={(value) => updateFamilyAccess("weeklyLifeImprovementNeeded", value)}
                options={WEEKLY_LIFE_TRADEOFF_OPTIONS}
              />
            </QuestionCard>
          ) : (
            <section className="rounded-[28px] border border-cardBorder bg-card px-5 py-4 shadow-soft">
              <p className="text-sm leading-7 text-inkMuted">
                Because relocation does not currently read as very open, the scorecard does not ask for
                deeper relocation tradeoff detail yet.
              </p>
            </section>
          )}
          {shouldAskVisitSystemFollowUp(state) ? (
            <QuestionCard
              title="Would having a practical repeatable visit system make relocation more manageable?"
              hint="A practical system can reduce stress, but may not change the underlying loss of convenience."
            >
              <ChoiceGroup
                name="repeatableVisitSystemHelp"
                value={state.familyAccess.repeatableVisitSystemHelp}
                onChange={(value) => updateFamilyAccess("repeatableVisitSystemHelp", value)}
                options={VISIT_SYSTEM_HELP_OPTIONS}
              />
            </QuestionCard>
          ) : null}
        </div>
      );
      break;

    case "trust":
      sectionBody = (
        <div className="space-y-4">
          <QuestionCard title="How much trust do you have that the current plan would improve family life rather than become a harder job?">
            <ChoiceGroup
              name="operatingPlanTrust"
              value={state.trust.operatingPlanTrust}
              onChange={(value) => updateTrust("operatingPlanTrust", value)}
              options={CONFIDENCE_OPTIONS}
            />
          </QuestionCard>
        </div>
      );
      break;

    case "businessModel":
      sectionBody = (
        <div className="space-y-4">
          {shouldAskBusinessModelDetail(state) ? (
            <QuestionCard title="How important is it that the business already have a stable team, organized systems, and day-to-day work that does not depend on constant owner heroics?">
              <ChoiceGroup
                name="stableTeamImportance"
                value={state.businessModel.stableTeamImportance}
                onChange={(value) => updateBusinessModel("stableTeamImportance", value)}
                options={IMPORTANCE_OPTIONS}
              />
            </QuestionCard>
          ) : (
            <section className="rounded-[28px] border border-cardBorder bg-card px-5 py-4 shadow-soft">
              <p className="text-sm leading-7 text-inkMuted">
                Because the path itself does not yet read as concrete enough, the scorecard skips deeper
                business-model detail until the household is at least somewhat open to the acquisition path.
              </p>
            </section>
          )}
        </div>
      );
      break;

    case "nonNegotiables":
      sectionBody = (
        <div className="space-y-4">
          <QuestionCard
            title="How aligned does the household feel on the timing right now?"
            hint="This helps distinguish a real shared green light from a plan that only works on paper."
          >
            <ChoiceGroup
              name="spouseTimingAlignment"
              value={state.nonNegotiables.spouseTimingAlignment}
              onChange={(value) =>
                commit((previous) => ({
                  ...previous,
                  nonNegotiables: {
                    ...previous.nonNegotiables,
                    spouseTimingAlignment: value
                  }
                }))
              }
              options={ALIGNMENT_OPTIONS}
            />
          </QuestionCard>
          <QuestionCard
            title="Mark anything that should act like a deal breaker."
            hint="Leave an item unchecked if it should influence the score but not automatically stop the scenario."
          >
            <CheckboxCards
              options={(Object.entries(DEAL_BREAKER_COPY) as Array<[DealBreakerId, (typeof DEAL_BREAKER_COPY)[DealBreakerId]]>).map(
                ([id, copy]) => ({
                  id,
                  label: copy.label,
                  description: copy.description
                })
              )}
              values={state.nonNegotiables.dealBreakers}
              onToggle={(id, checked) => updateDealBreaker(id as DealBreakerId, checked)}
            />
          </QuestionCard>
        </div>
      );
      break;

    case "results":
      sectionBody = (
        <div className="space-y-6" data-print-section="true">
          <section className="rounded-[32px] border border-cardBorder bg-paper p-6 shadow-soft sm:p-8">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div className="max-w-3xl">
                <p className="text-xs font-semibold uppercase tracking-[0.28em] text-ocean">
                  Household fit conclusion
                </p>
                <h2 className="mt-3 font-display text-4xl text-ink sm:text-5xl">{results.headline}</h2>
                <p className="mt-4 text-base leading-7 text-inkMuted sm:text-lg">{results.headlineDetail}</p>
                <p className="mt-4 text-base leading-7 text-ink">{results.narrativeSummary}</p>
                {results.babyTimingIsMajorLimiter ? (
                  <p className="mt-4 rounded-2xl border border-amber-300/70 bg-amber-50 px-4 py-3 text-sm font-semibold text-amber-800 dark:border-amber-500/30 dark:bg-amber-500/10 dark:text-amber-100">
                    Timing note: your answers suggest relocation does not feel acceptable until the baby is
                    older. Immediate relocation is being scored against that line.
                  </p>
                ) : null}
              </div>
              <div className="rounded-[24px] border border-cardBorder bg-card px-5 py-4 text-right">
                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">Summary date</p>
                <p className="mt-2 text-lg font-semibold text-ink">{formatLocalDate()}</p>
              </div>
            </div>

            <div className="mt-6 grid gap-4 md:grid-cols-3">
              <OutcomeCard
                label="Best overall path"
                scenario={bestOverallScenario}
                detail={results.bestOverallLine}
              />
              <OutcomeCard
                label="Best business-buy path"
                scenario={bestBuyScenario}
                detail={results.bestBuyLine}
              />
              <OutcomeCard
                label="Relocation"
                scenario={relocateScenario}
                detail={results.relocationLine}
              />
            </div>

            <div className="mt-6 flex flex-wrap gap-3" data-famscorecard-screen-only="true">
              <button
                type="button"
                onClick={copySummary}
                className="min-h-12 rounded-full border border-cardBorder bg-card px-5 text-sm font-semibold text-ink transition hover:border-ocean/35"
              >
                {copyStatus === "copied"
                  ? "Summary copied"
                  : copyStatus === "error"
                    ? "Copy failed"
                    : "Copy summary"}
              </button>
              <button
                type="button"
                onClick={() => window.print()}
                className="min-h-12 rounded-full bg-ocean px-5 text-sm font-semibold text-white shadow-glow transition hover:bg-oceanDark"
              >
                Print / save PDF
              </button>
              <button
                type="button"
                onClick={resetAssessment}
                className="min-h-12 rounded-full border border-cardBorder bg-card px-5 text-sm font-semibold text-ink transition hover:border-ocean/35"
              >
                Reset assessment
              </button>
            </div>
          </section>

          <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
            <ReviewStat label="Earliest acceptable move" value={earliestMoveLabel} />
            <ReviewStat label="Would delay help?" value={delayLabel} />
            <ReviewStat label="Planned visits tradeoff" value={plannedVisitsLabel} />
            <ReviewStat label="Weekly life must be" value={weeklyLifeTradeoffLabel} />
            <ReviewStat label="Visit system help" value={visitSystemHelpLabel} />
            <ReviewStat label="Income safety floor" value={formatCurrency(state.finance.minimumSafeIncome)} />
            <ReviewStat label="Insurance path" value={insurancePathLabel} />
            <ReviewStat label="Local vs. relocate" value={localAcquisitionLabel} />
            <ReviewStat label="Secondary income role" value={results.secondaryIncomeRole} />
          </section>

          <section className="rounded-[28px] border border-cardBorder bg-paper p-4 shadow-soft sm:p-5">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div className="max-w-2xl">
                <p className="text-xs font-semibold uppercase tracking-[0.24em] text-ocean">
                  Review scenario assumptions
                </p>
                <h3 className="mt-2 font-display text-2xl text-ink">Adjust the built-in scenario assumptions</h3>
                <p className="mt-2 text-sm leading-6 text-inkMuted">
                  The default assumptions are only a starting point. Edit them to reflect the real path you
                  want to compare, and the results will recalculate immediately.
                </p>
              </div>
              <div className="rounded-2xl border border-cardBorder bg-card px-4 py-3">
                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-inkMuted">Current leader</p>
                <p className="mt-2 text-base font-semibold text-ink">{SCENARIO_META[results.bestScenarioId].label}</p>
              </div>
            </div>

            <div className="mt-5 flex flex-wrap gap-2">
              {(Object.keys(SCENARIO_META) as ScenarioId[]).map((scenarioId) => {
                const active = activeScenario === scenarioId;
                return (
                  <button
                    key={scenarioId}
                    type="button"
                    onClick={() => setActiveScenario(scenarioId)}
                    className={`min-h-12 rounded-full px-4 text-sm font-semibold transition ${
                      active
                        ? "bg-ocean text-white shadow-glow"
                        : "border border-cardBorder bg-card text-ink hover:border-ocean/35"
                    }`}
                  >
                    {SCENARIO_META[scenarioId].shortLabel}
                  </button>
                );
              })}
            </div>
            <div className="mt-4">
              <ScenarioAssumptionEditor
                scenarioId={activeScenario}
                assumptions={state.scenarios[activeScenario]}
                minimumSafeIncome={state.finance.minimumSafeIncome}
                minimumCashCushion={state.finance.minimumCashCushion}
                onChange={(field, value) => updateScenario(activeScenario, field, value)}
              />
            </div>
          </section>

          <section className="space-y-4">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.24em] text-ocean">Best scenario ranking</p>
              <h3 className="mt-2 font-display text-2xl text-ink">How the three paths compare right now</h3>
            </div>
            {results.rankedScenarios.map((scenario, index) => (
              <ScenarioScoreCard key={scenario.id} rank={index + 1} scenario={scenario} />
            ))}
          </section>

          <section className="grid gap-4 lg:grid-cols-2">
            <BlockerConditionSummary
              eyebrow="Biggest blockers on the strongest buy path"
              title="What is keeping the leading acquisition path from working right now?"
              items={results.topBlockers}
              tone="warn"
            />
            <BlockerConditionSummary
              eyebrow="Needed conditions on the strongest buy path"
              title="What would need to become true before that acquisition path feels workable?"
              items={results.topConditions}
            />
          </section>

          <section className="grid gap-4 lg:grid-cols-3">
            <BlockerConditionSummary
              eyebrow="Structural mismatch"
              title="What looks like a real family-fit problem?"
              items={results.topStructuralIssues}
              tone="warn"
            />
            <BlockerConditionSummary
              eyebrow="Timing issue"
              title="What looks more like a timing problem?"
              items={results.topTimingIssues}
            />
            <BlockerConditionSummary
              eyebrow="Solvable blocker"
              title="What looks fixable if handled cleanly?"
              items={results.topSolvableBlockers}
            />
          </section>

          <section className="grid gap-4 lg:grid-cols-3">
            <section className="rounded-[28px] border border-cardBorder bg-card p-5 shadow-soft">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="max-w-2xl">
                  <p className="text-xs font-semibold uppercase tracking-[0.26em] text-inkMuted">
                    Secondary income buffer
                  </p>
                  <h3 className="mt-2 font-display text-2xl text-ink">
                    What role, if any, optional secondary income plays in this decision
                  </h3>
                </div>
                <span className="rounded-full border border-cardBorder bg-surface px-4 py-2 text-sm font-semibold text-ink">
                  {results.secondaryIncomeRole}
                </span>
              </div>
              <div className="mt-4 space-y-3">
                {results.secondaryIncomeNotes.map((item) => (
                  <div
                    key={item}
                    className="rounded-2xl border border-cardBorder/75 bg-surface px-4 py-3 text-sm leading-6 text-ink"
                  >
                    {item}
                  </div>
                ))}
              </div>
            </section>
            <BlockerConditionSummary
              eyebrow="Family access"
              title="What family-access tradeoffs this path creates"
              items={results.familyAccessTradeoffs}
              tone="warn"
            />
            <BlockerConditionSummary
              eyebrow="Worth the distance?"
              title="What would need to be true for relocation to feel worth the family-distance cost"
              items={results.familyDistanceWorthItConditions}
            />
          </section>

          <section className="grid gap-4 lg:grid-cols-2">
            <BlockerConditionSummary
              eyebrow="Buy local"
              title="What would need to become true for buy local to work?"
              items={results.buyLocalConditions}
            />
            <BlockerConditionSummary
              eyebrow="Buy and move"
              title="What would need to become true for buy and move to work?"
              items={results.buyRelocateConditions}
            />
          </section>

          <section className="rounded-[28px] border border-cardBorder bg-card p-6 shadow-soft">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-ocean">Printable summary</p>
            <h3 className="mt-2 font-display text-2xl text-ink">{buildSummaryTitle(results)}</h3>
            <pre className="mt-4 whitespace-pre-wrap font-body text-sm leading-7 text-ink">{results.summaryText}</pre>
          </section>
        </div>
      );
      break;

    default:
      sectionBody = null;
      break;
  }

  return (
    <div className="pb-32 pt-6 sm:pt-8" data-famscorecard-page>
      <div ref={stepTopRef} aria-hidden="true" />
      <style jsx global>{`
        @media print {
          header,
          footer,
          [data-famscorecard-screen-only="true"] {
            display: none !important;
          }

          body {
            background: #ffffff !important;
            color: #0f172a !important;
          }

          main {
            padding: 0 !important;
          }

          [data-famscorecard-page] {
            padding: 0 !important;
          }
        }
      `}</style>
      <ProgressHeader
        stepNumber={stepNumber}
        totalSteps={totalSteps}
        title={currentStep.title}
        description={currentStep.description}
        savedLabel={formatSavedLabel(state.lastSavedAt)}
      />
      <div ref={stepContentRef} className="mx-auto w-full max-w-5xl px-4 pt-6 sm:px-6">
        {sectionBody}
        {!canContinue && currentStep.id !== "results" ? (
          <p className="mt-4 text-sm font-semibold text-clay">Answer each item on this screen to continue.</p>
        ) : null}
      </div>
      <StickyActionBar
        canGoBack={state.currentStep > 0}
        onBack={goBack}
        onContinue={goForward}
        continueLabel={continueLabel}
        disabled={!canContinue && currentStep.id !== "results"}
      />
    </div>
  );
}
