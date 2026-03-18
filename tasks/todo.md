# Task TODO

Current task: instrument the existing AIHA lane so we can measure, by state and by run, how many firms become usable only after canonical website-contact resolution and how many of those survive to packet-eligible and selected, without changing contact policy or adding guessed-email fallback.

Plan verified against `AGENTS.md` and the current prospect generation / AI-assist packet flow on 2026-03-17.

## Plan

- [x] Inspect the current AIHA generator and packet-selection path and add narrow lane-yield instrumentation without changing canonical contact policy.
- [x] Add deterministic tests that prove generator-side snapshotting and packet-side stage measurement for AIHA site-contact-only firms.
- [x] Replay the canonical AIHA lane on a temp clone of `C:\osha_data` for 2026-03-17 and capture the by-state / by-run evidence.
- [x] Record the sufficiency decision in durable docs so the next packet is scoped around the required second accessible public directory lane.

## Review

- Status: Passed
- Evidence: `py -3 -m unittest -q` passed on 2026-03-17 with `Ran 758 tests ... OK`; a temp-clone replay of `run_prospect_generation.py --for-date 2026-03-17` plus `tools\dump_prospect_ai_assist_review.py --for-date 2026-03-17` produced `aiha_lane_yield_20260317.json` with `usable_only_total=0` and `accepted_total=0` across `TX,CA,FL`, and `manifest.json` with `candidate_count_before_filters=21`, `candidate_count_after_filters=1`, `selected_row_count=1`, `raw_target=30`, `AIHA SITE-CONTACT ONLY: usable=0 generator_accepted=0 packet_eligible=0 selected=0`, and `gap_total=107`.
- Notes: AIHA remains valid as a directory lane, but the March 17, 2026 measurement shows no website-contact-only uplift and materially underfills the current replenishment target, so a second accessible public directory lane is required.
