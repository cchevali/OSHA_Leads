# Architecture

## Modules (High Level)

- Ingest + data store: OSHA inspections -> `data/osha.sqlite`
- Digest delivery: build customer-facing alerts and send to subscribers
- Suppression/opt-out: local suppression list (`out/suppression.csv`) and optional one-click unsubscribe service
- Outreach export (this repo): prospects CSV -> mail-merge outbox CSV for external sending

## Outreach Export Data Flow

1. Input: a prospects CSV matching `outreach/prospects_schema.md`.
2. Generator: `outreach/generate_mailmerge.py`
   - Filters to a geo batch (`--state`) and labels the export batch (`--batch`)
   - Normalizes + dedupes by email (case-insensitive)
   - Enforces suppression:
     - local CSV suppression (`out/suppression.csv`)
     - optional domain/email suppression from `suppression_list` in SQLite (when available)
   - Generates opt-out links:
     - HTTPS one-click when `UNSUB_ENDPOINT_BASE` + `UNSUB_SECRET` are available
     - otherwise exits non-zero (unless `--allow-mailto-fallback` is provided)
   - Emits an outbox CSV with `subject` and `body` columns for mail-merge
   - Emits a batch manifest CSV alongside the outbox export (exported vs dropped with reasons)
   - Writes an append-only per-run log to `outreach/outreach_runs/<YYYY-MM-DD>_<batch>.jsonl`
3. Output: the outbox CSV is handed to an external sender (no SMTP/provider integration in this repo).

## Operational Artifacts

- `outreach/outreach_runs/`: per-run logs (counts + metadata)
- `out/unsub_tokens.csv`: token store for one-click unsubscribe links (when enabled)
- `out/suppression.csv`: suppression list enforced by exports and sending paths
