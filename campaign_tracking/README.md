# Campaign Tracking (Ops CSVs)

This folder contains committed CSV templates (headers only) for lightweight campaign tracking. These are intended as an operator ledger to:
- prevent double-sends
- measure reply rates
- enforce unsubscribe/suppression consistently

## Templates (Committed)

- `campaign_tracking/templates/tx_triangle_sent_template.csv.example`
- `campaign_tracking/templates/tx_triangle_replied_template.csv.example`
- `campaign_tracking/templates/tx_triangle_unsub_template.csv.example`

## Working Location (Not Committed)

Write active campaign logs under `out/campaign_tracking/` (the `out/` folder is gitignored), for example:
- `out/campaign_tracking/tx_triangle_2026-02/sent.csv`
- `out/campaign_tracking/tx_triangle_2026-02/replied.csv`
- `out/campaign_tracking/tx_triangle_2026-02/unsub.csv`

## Suggested Dedupe Rules

- Normalize emails to lowercase.
- Before sending, check `sent.csv` for an existing row with `status=sent` for the same email.
- If a prospect replies "unsubscribe", record it in `unsub.csv` immediately and add the email and/or domain to the suppression list used by sending (so future sends are blocked).

## Relationship to System Logs

The automation may also write machine logs under `out/` (for example, cold email send logs and inbox triage logs). The CSVs in this folder are the minimal campaign ledger for operator visibility and reporting.
