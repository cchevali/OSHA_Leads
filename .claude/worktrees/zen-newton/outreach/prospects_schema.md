# Outreach Prospects CSV Schema (Business Contacts Only)

This schema defines the *input* CSV format for `outreach/generate_mailmerge.py`.

Business contact fields only. Do not add personal/sensitive enrichment.

## Required columns

- `prospect_id`: stable internal id for the prospect (string). Used to derive `subscriber_key` (do not use raw email).
- `first_name`
- `last_name`
- `firm`: company name
- `title`: role/title
- `email`
- `state`: 2-letter US state
- `city`
- `territory_code`: source/segment code (kept for traceability; export uses the batch `--batch` as the outbound territory)
- `source`: where the record came from (e.g., "osha", "manual", "referral")
- `notes`: free-form operational notes

## Output columns

The mail-merge outbox CSV includes all required columns above, plus:

- `batch`: the export batch id (e.g., `TX_W2`)
- `subscriber_key`: deterministic, derived from `prospect_id` (safe charset for `unsubscribe_server.py`)
- `unsubscribe_url`: one-click URL when `UNSUB_ENDPOINT_BASE` + `UNSUB_SECRET` are set; otherwise a `mailto:` fallback
- `prefs_url`: preference URL (`/prefs`) when one-click tokens are available; otherwise blank
- `subject`: plain-text email subject
- `body`: plain-text email body with placeholders filled

## Template

Default template path: `outreach/outreach_plain.txt`

## Batch Manifest

Each run also writes a manifest CSV alongside the outbox export:

- `..._manifest.csv`: includes `prospect_id`, `email`, `status`, and `reason` for QA/audit.
