# V1 Customer Validated (Canonical)

Canonical instruction authority remains `AGENTS.md` at repo root.

Purpose: preserve customer-validated V1 requirements and operator truths while deprecating legacy standalone docs.

## Source Snapshot (Legacy V1 Docs)

### `docs/legacy/COLD_EMAIL_README.md` (historical path: `COLD_EMAIL_README.md`)
- Last commit touching source: `704355f` (2026-02-02, Chase Chevali) "Cold email: require reply-to, enforce suppression, set footer address"
- What this doc asserts:
  - Outbound send path uses `outbound_cold_email.py` with dry-run/live modes, score-tier selection, and send logging in `out/cold_email_log.csv`.
  - Inbound reply handling uses `inbound_inbox_triage.py` with Gmail OAuth, message classification, suppression updates, and daily digest notification.
  - Outbound sends must enforce suppression checks before send.
  - Live outbound footer must include real mailing address and unsubscribe option.
  - Operator-visible artifacts include `out/cold_email_log.csv`, `out/inbox_triage_log.csv`, `out/inbox_state.json`, and `out/suppression.csv`.

### `docs/legacy/COLD_EMAIL_IMPLEMENTATION_PLAN.md` (historical path: `COLD_EMAIL_IMPLEMENTATION_PLAN.md`)
- Last commit touching source: `a3cb531` (2026-02-02, Chase Chevali) "Docs: switch paths to C:\\dev\\OSHA_Leads"
- What this doc asserts:
  - Outbound V1 selection logic is deterministic and score-tiered (`>=8`, then `>=6`, then `>=4`) with recency preference.
  - Recipient input contract includes `email`, `first_name`, `last_name`, `firm_name`, `segment`, `state_pref`.
  - V1 outbound content includes sample leads with urgency cues and compliance footer controls.
  - Inbound V1 triage classes include unsubscribe, bounce, interested, question, bug/feature, out-of-office, and other.
  - Reply classification drives concrete actions: suppression updates, notifications, drafts, and engineering tickets.

### `docs/legacy/CUSTOMER_ONBOARDING.md` (historical path: `CUSTOMER_ONBOARDING.md`)
- Last commit touching source: `a3cb531` (2026-02-02, Chase Chevali) "Docs: switch paths to C:\\dev\\OSHA_Leads"
- What this doc asserts:
  - New-customer onboarding is configuration-driven via `customers/*.json` and does not require code edits.
  - Onboarding sequence requires dry-run verification before first live send.
  - Early V1 used pilot mode controls to restrict recipients before full production rollout.
  - Operator verification includes log/artifact checks after first send.
  - Daily scheduling is an explicit operator responsibility after successful first send.

### `docs/legacy/TARGET_LIST_FACTORY_STATUS.md` (historical path: `TARGET_LIST_FACTORY_STATUS.md`)
- Last commit touching source: `a3cb531` (2026-02-02, Chase Chevali) "Docs: switch paths to C:\\dev\\OSHA_Leads"
- What this doc asserts:
  - Target list factory workflow is file-first and depends on CSV tracking plus dedupe normalization.
  - V1 sourcing relies on repeatable industry directories and explicit territory quotas.
  - Prospect quality controls include role normalization, duplicate-domain handling, and status lifecycle codes.
  - Operator output includes a prioritized outreach-ready subset and a deduped master list.
  - Dedupe script behavior is deterministic and no-external-dependency.

### `docs/legacy/lead_definition_v0_1.md` (historical path: `lead_definition_v0_1.md`)
- Last commit touching source: `251b30a` (2026-02-02, Chase Chevali) "Initial commit"
- What this doc asserts:
  - Canonical lead identity is inspection-level (`lead_id = osha:inspection:{activity_nr}`).
  - "New lead" status is tied to `first_seen_at` recency and required-field completeness.
  - Sendable lead minimum fields include inspection id, establishment, state, city/zip, open date, and source URL.
  - Re-ingest updates existing rows without creating duplicates and preserves existing non-null data.
  - Scoring is deterministic rule-based ranking with explicit point contributions.

### `docs/legacy/PROJECT_STATUS_REPORT.md` (historical path: `PROJECT_STATUS_REPORT.md`)
- Last commit touching source: `251b30a` (2026-02-02, Chase Chevali) "Initial commit"
- What this doc asserts:
  - V1 outbound and inbound automation were both operational as an MVP pipeline.
  - Compliance and deliverability gates include sender identity alignment, mailing-address validation, and unsubscribe support.
  - Freshness gates block outbound when pipeline/signal age thresholds are exceeded.
  - Outbound kill switch is an explicit runtime control and defaults safe.
  - Production readiness depends on identity, OAuth, and unsubscribe endpoint completion.

### `docs/legacy/PROSPECTING_SOP.md` (historical path: `PROSPECTING_SOP.md`)
- Last commit touching source: `251b30a` (2026-02-02, Chase Chevali) "Initial commit"
- What this doc asserts:
  - Territory prospecting targets a minimum of 30 qualified prospects with no duplicate domains.
  - V1 sourcing pipeline uses three repeatable source categories.
  - Dedupe/normalize is required after each batch and standardizes domain/state/role fields.
  - Contact priority is decision-maker first (owner/executive, then safety leaders, then operations/compliance).
  - Handoff requires a quality checklist before outreach execution.

### `docs/legacy/SESSION_HANDOFF.md` (historical path: `SESSION_HANDOFF.md`)
- Last commit touching source: `251b30a` (2026-02-02, Chase Chevali) "Initial commit"
- What this doc asserts:
  - V1 ingestion, bundle generation, delivery, and metrics tracking were active operator paths.
  - V1 trial/customer config was subscriber and territory scoped with daily operation expectations.
  - Email behavior included pilot gating, suppression checks, list-unsubscribe headers, and multipart delivery.
  - Daily operator workflow required explicit SMTP environment setup and daily run execution.
  - Expansion and production hardening were expected follow-on operations.

## What landed the first interested customer

- A territory-first target list process produced outreach-ready prospects from repeatable sources with domain dedupe and role prioritization.
- Outbound messaging sent recent OSHA signal samples with state context, urgency cues, and clear reply path.
- Compliance controls were present at send time: suppression enforcement, list-unsubscribe behavior, and physical-address footer.
- Inbound triage converted replies into actions quickly: immediate interested notifications, unsubscribe/bounce suppression updates, and operator digesting.
- Trial/onboarding path was config-driven and fast enough for same-session customer setup plus first-send validation.

## V1 workflow (targeting -> copy -> send -> handling replies -> trial -> conversion)

1. Targeting
   - Build territory prospect pools from repeatable directories.
   - Normalize/dedupe prospects by domain and prioritize decision-maker roles.
   - Require minimum prospect completeness before handoff.
2. Copy
   - Generate outreach with 2-5 recent OSHA lead examples.
   - Include sender identity, reply-to, and compliance footer markers.
3. Send
   - Enforce suppression before send.
   - Respect daily send caps/rate limits and log each send event.
   - Keep kill-switch and dry-run as first-class controls.
4. Handling replies
   - Poll inbox, classify response intent, and write suppression updates for unsubscribes/bounces.
   - Trigger immediate notification for interested replies.
   - Generate structured follow-up artifacts (digest/drafts/tickets) for operator action.
5. Trial
   - Onboard customer via config file with territory/state and recipient settings.
   - Run dry-run validation first, then perform controlled baseline/daily sends.
   - Verify logs/artifacts after initial delivery.
6. Conversion
   - Track lifecycle transitions (`replied`, `trial_started`, `converted`, `do_not_contact`) as explicit operational state.
   - Preserve opt-out/suppression handling throughout lifecycle transitions.

## V1 lead definition criteria and high-signal heuristics

### Lead definition criteria

- Canonical inspection identity is activity-number based and unique.
- "New" is first-observed recency (`first_seen_at` window), not repeated observations.
- Sendable lead requires: `activity_nr`, `establishment_name`, `site_state`, (`site_city` or `site_zip`), `date_opened`, `source_url`.
- Missing required fields force review path and exclusion from sendable output.
- Re-ingest updates observation metadata and fills nulls without duplicate row creation.

### High-signal heuristics

- Score-based prioritization is deterministic and rule-based.
- Signal tiers prioritize higher score bands first; fallback to lower bands only when needed for volume.
- Inspection-type weighting and recency are primary ordering factors.
- Additional weighting signals include construction NAICS, violations presence, and emphasis program markers.

## V1 onboarding steps and required operator actions

1. Create customer config from template and set required fields (customer id, geography, windows, recipients).
2. Execute dry-run delivery and require all validation checks to pass before live send.
3. Confirm send controls are set correctly (pilot restrictions for early trial phases; production controls when promoted).
4. Run first baseline/live send through the canonical entrypoint.
5. Verify send artifacts/logs and recipient receipt.
6. Schedule daily operation and monitor run outputs.
7. Record lifecycle outcomes and suppression/opt-out events as ongoing operator work.

## V1 invariants

- Suppression and opt-out handling are hard gates for all outreach sends.
- List-Unsubscribe headers and footer opt-out behavior must be preserved.
- No duplicate unsubscribe-link behavior is allowed in outbound content.
- Dry-run must remain side-effect-safe; live send requires explicit operator intent.
- Lead identity/dedupe must remain deterministic; first observation semantics must not regress.
- Freshness/readiness gates must block stale or misconfigured send operations.
- Operator flow must stay Windows-first with single copy/pasteable commands from repo root.
- Documentation/process changes must not change outreach behavior.

## Where this lives now

| V1 requirement/process | Canonical location(s) now | Notes |
|---|---|---|
| Windows-first operator execution and secrets wrapper contract | `docs/RUNBOOK.md`, `AGENTS.md` | Canonical command style and secrets flow are centralized there. |
| Outbound operations flow with suppression/one-click gates and doctor sequence | `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md` | Runbook is operator procedure; Architecture is boundary/data-flow reference. |
| Compliance invariants (suppression, opt-out, no duplicate unsubscribe links) | `AGENTS.md`, `docs/PROJECT_BRIEF.md`, `docs/ARCHITECTURE.md` | Policy + invariant split across contract and architecture summary. |
| CRM-lite outreach source-of-truth and lifecycle recording | `docs/ARCHITECTURE.md`, `docs/DECISIONS.md` | ADR-0002 captures source-of-truth decision rationale. |
| Doctor-first readiness gating | `docs/RUNBOOK.md`, `docs/DECISIONS.md` | ADR-0003 plus daily operator command sequence. |
| Lead dedupe and first-seen semantics for digest/trial operations | `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md` | Runbook details operator checks; architecture now summarizes invariant. |
| Project context single-file upload workflow | `docs/RUNBOOK.md`, `tools/project_context_pack.py` | Runbook is operator-facing; tooling enforces source inputs. |
| Canonical authority of `AGENTS.md` and spine alignment | `docs/PROJECT_BRIEF.md`, `docs/ARCHITECTURE.md`, `docs/DECISIONS.md`, `docs/RUNBOOK.md` | ADR-0004 defines contract authority decision. |

## Legacy -> Canonical pointers

| Legacy file (archived) | Canonical replacement | Use archived copy for |
|---|---|---|
| `docs/legacy/COLD_EMAIL_README.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md` | Historical setup examples and early environment notes. |
| `docs/legacy/COLD_EMAIL_IMPLEMENTATION_PLAN.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/ARCHITECTURE.md`, `docs/DECISIONS.md` | Historical implementation intent before later ADRs. |
| `docs/legacy/CUSTOMER_ONBOARDING.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/RUNBOOK.md` | Historical onboarding checklist wording. |
| `docs/legacy/TARGET_LIST_FACTORY_STATUS.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/ARCHITECTURE.md` | Historical rollout/status snapshot for target-list factory. |
| `docs/legacy/lead_definition_v0_1.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md` | Original scoring/lead-definition statement. |
| `docs/legacy/PROJECT_STATUS_REPORT.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/DECISIONS.md` | Historical readiness snapshot and blockers. |
| `docs/legacy/PROSPECTING_SOP.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/RUNBOOK.md` | Historical collection heuristics and role mapping detail. |
| `docs/legacy/SESSION_HANDOFF.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md` | Historical handoff context and dated operational state. |
