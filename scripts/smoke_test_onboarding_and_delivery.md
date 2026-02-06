# Smoke Test: Onboarding + Delivery (Dry-Run, Deterministic)

Goal: verify the email-only onboarding pipeline provisions a subscriber without manual DB edits, and the daily delivery path can render a digest in `--dry-run` while:
- excluding suppressed recipients
- detecting duplicate dry-run renders on a second run

This is intentionally lightweight (no test framework). The wrapper script exits non-zero on failure.

## What It Tests

1. Creates a temporary copy of `data/osha.sqlite`.
2. Inserts a suppression-list entry for a test email.
3. Onboards a dummy subscriber into the temp DB (dry-run onboarding, so no confirmation emails are sent).
4. Runs `deliver_daily.py --dry-run --skip-ingest` against the temp DB and the generated customer config.
5. Runs the same delivery command a second time and expects `[SKIP_DUPLICATE_DRYRUN]`.

## Run

```powershell
cd C:\dev\OSHA_Leads
powershell -ExecutionPolicy Bypass -File scripts\smoke_test_onboarding_and_delivery.ps1
```

## Expected Output Signals

The wrapper asserts these strings are present in the delivery logs:
- `DRYRUN_SUPPRESSED` includes the suppressed test email (or suppression count > 0)
- `[SKIP_DUPLICATE_DRYRUN]` on the second delivery run

