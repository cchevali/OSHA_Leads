# AGENTS Contract (Canonical)

## Mission
Operate OSHA_Leads as a compliant, Windows-first outbound + trial operations system without changing product behavior unless explicitly requested in a Task Packet.

## Product
OSHA_Leads provides operational monitoring, alerting, and outreach support for business contacts. It does not provide legal advice.

## Buyers/Offer
Primary buyers are operators and safety-facing teams that need timely OSHA-related signals and disciplined outbound trial operations.

## Execution Strategy
- Use Task Packets as implementation source of truth.
- Prefer minimal, non-breaking changes.
- Keep durable context in repo docs, not chat history.
- Choose one highest-odds execution path; no forks/options in planner output.

## Guardrails
- No legal advice content.
- Enforce suppression, opt-out handling, and outreach logging.
- Preserve List-Unsubscribe headers and footer opt-out links.
- Never duplicate unsubscribe links in outbound content.
- Do not change outreach cadence, scoring, enrichment, templates, or sending logic unless explicitly required.
- Use PowerShell `Select-String` or Python for search in this repo; avoid non-default tooling such as `rg`.

## Operator CLI Contract (Windows-First)
- Every operator action is a single copy/pasteable PowerShell command from repo root (for example `C:\dev\OSHA_Leads`).
- If secrets are required, command pattern is:
  - `.\run_with_secrets.ps1 -- py -3 <script> [args]`
- Automation entrypoints must expose:
  - `--print-config` for side-effect-free resolved config output.
  - `--dry-run` for no-send/no-live-side-effect execution and no partial artifact behavior.

## Secrets/SOPS Guardrail
- Do not rely on interactive SOPS editor mode.
- Do not commit `.env.sops` unless explicitly instructed.
- Missing/invalid secret or config states must emit clear `ERR_*` tokens.
- Use the no-editor helper flow documented in `docs/RUNBOOK.md` (including `scripts\set_outreach_env.ps1`).

## Template Integrity Rule
- Do not modify outreach email copy/templates during docs/process-only tasks.
- Preserve compliance markers and unsubscribe behavior in all template-related changes.

## Repo Context Spine
- `docs/PROJECT_BRIEF.md`: mission, positioning, invariants, do-not-break list.
- `docs/ARCHITECTURE.md`: system boundaries and data flow.
- `docs/DECISIONS.md`: ADR history and policy decisions.
- `docs/RUNBOOK.md`: canonical operator commands and verification steps.

## Task Packet Standard
- Treat the current Task Packet as binding scope and acceptance source.
- Do not ask for extra context unless a referenced file is missing.
- Keep changes tightly scoped to stated goals/non-goals.

## Required Codex Output Format
- Changed files list
- Summary (<= 8 lines)
- Commands run (commands only)
- Remaining TODOs (<= 5 bullets)

## Acceptance Gates
- `py -3 -m unittest -q` must exit `0`.
- `git status --porcelain` must show only intended task-related changes (no stray artifacts).
- Docs integrity checks:
  - `AGENTS.md` exists at repo root.
  - Spine docs reference `AGENTS.md` as canonical instruction contract.
  - `docs/TODO.md` contains both human-only and Codex-owned sections.

## Single Source Of Truth Workflow
- `AGENTS.md` is the canonical instruction contract.
- ChatGPT Project Instructions should remain a thin wrapper that points to this file.
- When `AGENTS.md` changes, re-upload updated `AGENTS.md` to ChatGPT Project Files.
