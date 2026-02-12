# Project Brief

Canonical instruction authority: `AGENTS.md` at repo root.

## What This Is

OSHA_Leads is an intelligence + alerting system for operational teams.

- No legal advice. We provide monitoring, summaries, and operational heads-ups only.
- Business contacts only. No personal/sensitive enrichment.

## Current Priority: Outbound Concierge (Growth Engine)

The current growth engine is an "outbound concierge" motion with CRM auto-run as the operational default:

- Discover and prioritize prospects into `out/crm.sqlite`.
- Run daily outreach automation via `run_outreach_auto.py` (select -> prioritize -> send -> record).
- Use mail-merge CSV generation as a debug/compatibility path, not the default send path.
- Process replies manually and mark lifecycle events (`replied`, `trial_started`, `converted`, `do_not_contact`).

Weekly target (initial): **100-200 new prospects/week**.

Success metric (funnel): **reply -> call -> paid** (track conversion per batch).

## Compliance & Invariants

- All outreach exports/sends must include an opt-out mechanism.
- Suppression must be enforced for all exports and sends (email and, where available, domain).

## Do Not Break

- Windows-first operator flow: commands are single copy/paste PowerShell commands from repo root.
- Secrets-required commands run via `.\run_with_secrets.ps1 -- py -3 ...`.
- Automation scripts keep `--print-config` and `--dry-run` behaviors side-effect-safe.
- Preserve List-Unsubscribe + footer opt-out links; do not duplicate unsubscribe links.
- This repo provides operational monitoring and outreach tooling, not legal advice.
- Documentation/process alignment work must not alter product behavior.
