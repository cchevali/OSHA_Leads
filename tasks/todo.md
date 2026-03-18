# Task TODO

Current task: retire automated prospect growth so the repo and operator workflow are manual-intake-only.

Plan verified against `AGENTS.md` and the current repo state on 2026-03-18.

## Plan

- [x] Remove automated prospect growth entrypoints, source adapters, scrapers, and prospect AI-assist tooling.
- [x] Remove scheduler/runtime hooks that invoked prospect generation or replenishment.
- [x] Update operator docs and env tooling so manual CSV intake is the only supported prospect-growth path.
- [x] Re-run tests and prepare the cleaned repo state for commit/push.

## Review

- Status: Passed
- Evidence: `py -3 -m unittest -q` passed on 2026-03-18 with `Ran 573 tests ... OK`; automated prospect-generation modules, prospect-source adapters, prospect AI-assist tooling, and scheduler hooks were removed; `PROJECT_CONTEXT_PACK.md` was rebuilt after the docs cleanup.
- Notes: manual prospect intake now centers on `outreach\crm_admin.py seed`.
