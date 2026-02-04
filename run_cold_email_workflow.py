#!/usr/bin/env python3
"""
Run the full cold-email workflow:
1) Refresh data via run_daily_pipeline.bat
2) Produce ranked leads (by state)
3) Produce recipient target list (business-contact-only fields)
4) Dedupe recipients and (optionally) send or dry-run outbound

Usage:
  python run_cold_email_workflow.py --state TX --dry-run --limit 3
  python run_cold_email_workflow.py --state TX --send --limit 3
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, date
from pathlib import Path

import outbound_cold_email as oce


SCRIPT_DIR = Path(__file__).parent.resolve()
OUT_DIR = SCRIPT_DIR / "out"
PIPELINE_BAT = SCRIPT_DIR / "run_daily_pipeline.bat"
METRICS_PATH = OUT_DIR / "daily_send_metrics.csv"


def parse_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def is_open_recent(lead: dict, as_of: date, recency_days: int) -> bool:
    status = (lead.get("case_status") or "").strip().upper()
    if status != "OPEN":
        return False
    opened = parse_date(lead.get("date_opened", ""))
    if not opened:
        return False
    return opened >= (as_of - timedelta(days=recency_days))


def sort_ranked(leads: list[dict]) -> list[dict]:
    def sort_key(lead: dict):
        score = lead.get("lead_score", 0)
        opened = parse_date(lead.get("date_opened", "")) or date.min
        first_seen = lead.get("first_seen_at", "")
        try:
            first_seen_dt = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
            first_seen_ts = first_seen_dt.timestamp()
        except Exception:
            first_seen_ts = 0
        return (-int(score), -opened.toordinal(), -first_seen_ts)

    return sorted(leads, key=sort_key)


def write_ranked_leads_csv(leads: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "priority",
        "lead_score",
        "activity_nr",
        "establishment_name",
        "site_city",
        "site_state",
        "date_opened",
        "inspection_type",
        "case_status",
        "naics_desc",
        "first_seen_at",
        "last_seen_at",
        "source_url",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for lead in leads:
            row = {
                "priority": oce.get_priority_label(lead.get("lead_score", 0)),
                "lead_score": lead.get("lead_score", ""),
                "activity_nr": lead.get("activity_nr", ""),
                "establishment_name": lead.get("establishment_name", ""),
                "site_city": lead.get("site_city", ""),
                "site_state": lead.get("site_state", ""),
                "date_opened": lead.get("date_opened", ""),
                "inspection_type": lead.get("inspection_type", ""),
                "case_status": lead.get("case_status", ""),
                "naics_desc": lead.get("naics_desc", ""),
                "first_seen_at": lead.get("first_seen_at", ""),
                "last_seen_at": lead.get("last_seen_at", ""),
                "source_url": lead.get("source_url", ""),
            }
            writer.writerow(row)


def summarize_priority(leads: list[dict]) -> str:
    counts = Counter()
    last_opened = None
    for lead in leads:
        label = oce.get_priority_label(lead.get("lead_score", 0))
        counts[label] += 1
        opened = parse_date(lead.get("date_opened", ""))
        if opened and (last_opened is None or opened > last_opened):
            last_opened = opened
    parts = []
    if counts["High"]:
        parts.append(f"High:{counts['High']}")
    if counts["Medium"]:
        parts.append(f"Medium:{counts['Medium']}")
    if counts["Low"]:
        parts.append(f"Low:{counts['Low']}")
    if last_opened:
        parts.append(f"last_opened:{last_opened.isoformat()}")
    return " ".join(parts)


def write_recipient_targets(leads: list[dict], out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    groups: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for lead in leads:
        name = (lead.get("establishment_name") or "").strip()
        city = (lead.get("site_city") or "").strip()
        state = (lead.get("site_state") or "").strip().upper()
        if not name or not state:
            continue
        key = (name.lower(), city.lower(), state)
        groups[key].append(lead)

    fieldnames = [
        "company_name",
        "domain",
        "contact_name",
        "contact_role",
        "contact_email",
        "city",
        "state",
        "industry",
        "lead_priority_summary",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for (_, city_l, state), leads_group in sorted(groups.items(), key=lambda x: (x[0][2], x[0][1], x[0][0])):
            sample = leads_group[0]
            industry = ""
            industries = [l.get("naics_desc", "") for l in leads_group if l.get("naics_desc")]
            if industries:
                industry = Counter(industries).most_common(1)[0][0]
            writer.writerow({
                "company_name": sample.get("establishment_name", ""),
                "domain": "",
                "contact_name": "",
                "contact_role": "",
                "contact_email": "",
                "city": sample.get("site_city", ""),
                "state": sample.get("site_state", ""),
                "industry": industry,
                "lead_priority_summary": summarize_priority(leads_group),
            })

    return len(groups)


def filter_recipients_for_state(recipients: list[dict], state: str) -> list[dict]:
    state = state.upper()
    filtered = []
    for rec in recipients:
        pref = (rec.get("state_pref") or "").strip().upper()
        if pref and pref != state:
            continue
        filtered.append(rec)
    return filtered


def write_recipients_csv(recipients: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["email", "first_name", "last_name", "firm_name", "segment", "state_pref"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in recipients:
            writer.writerow({
                "email": rec.get("email", ""),
                "first_name": rec.get("first_name", ""),
                "last_name": rec.get("last_name", ""),
                "firm_name": rec.get("firm_name", ""),
                "segment": rec.get("segment", ""),
                "state_pref": rec.get("state_pref", ""),
            })


def collect_outbound_metrics(campaign_id: str, start_time: datetime, end_time: datetime) -> tuple[int, int, int]:
    """Return (sent, skipped_no_high_med, skipped_one_click_failed) for this run."""
    sent = 0
    skipped_no_high_med = 0
    skipped_one_click_failed = 0
    log_path = oce.LOG_PATH
    if not log_path.exists():
        return sent, skipped_no_high_med, skipped_one_click_failed
    with open(log_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("campaign_id") != campaign_id:
                continue
            ts = row.get("timestamp", "")
            try:
                row_time = datetime.fromisoformat(ts)
            except Exception:
                continue
            if row_time < start_time or row_time > end_time:
                continue
            status = row.get("status", "")
            error = row.get("error", "")
            if status == "sent":
                sent += 1
            if status == "skipped" and error == "no_high_med_leads":
                skipped_no_high_med += 1
            if status == "failed" and error in ("one_click_failed", "one_click_preflight_failed"):
                skipped_one_click_failed += 1
    return sent, skipped_no_high_med, skipped_one_click_failed


def append_metrics_row(
    state: str,
    intended: int,
    sent: int,
    skipped_suppressed: int,
    skipped_no_high_med: int,
    skipped_one_click_failed: int,
) -> None:
    """Append daily send metrics row to CSV and print summary."""
    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "date",
        "state",
        "intended",
        "sent",
        "skipped_suppressed",
        "skipped_no_high_med",
        "skipped_one_click_failed",
        "bounces_detected",
        "unsub_count",
    ]
    row = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "state": state,
        "intended": intended,
        "sent": sent,
        "skipped_suppressed": skipped_suppressed,
        "skipped_no_high_med": skipped_no_high_med,
        "skipped_one_click_failed": skipped_one_click_failed,
        "bounces_detected": "not_implemented",
        "unsub_count": "not_implemented",
    }
    write_header = not METRICS_PATH.exists()
    with open(METRICS_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
    print("\nMETRICS (daily_send_metrics.csv)")
    for key in fields:
        print(f"  {key}: {row[key]}")


def render_previews(recipients: list[dict], leads: list[dict], state: str, count: int = 3) -> None:
    config = oce.load_config()
    mailing_address = os.getenv("MAILING_ADDRESS", "")
    if not mailing_address:
        mailing_address = "Address Missing"

    sample_recipients = recipients[:count]
    for idx, recipient in enumerate(sample_recipients, start=1):
        samples, reason = oce.select_sample_leads_with_reason(
            leads, config, recipient.get("email", ""), "preview", state
        )
        if not samples:
            print(f"[WARN] Preview {idx}: no samples ({reason}) for {recipient.get('email')}")
            continue
        subject = oce.generate_email_subject(recipient, samples, is_test=True)
        unsub_token = oce.compute_unsub_token(recipient.get("email", ""), "preview")
        text_body, html_body = oce.generate_email_body(
            recipient, samples, unsub_token, mailing_address
        )
        text_path = OUT_DIR / f"preview_email_{idx}.txt"
        html_path = OUT_DIR / f"preview_email_{idx}.html"
        with open(text_path, "w", encoding="utf-8") as f:
            f.write(f"Subject: {subject}\n\n{text_body}")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_body)
        print(f"[OK] Preview {idx} written: {text_path.name}, {html_path.name}")


def load_latest_run() -> dict:
    run_path = OUT_DIR / "latest_run.json"
    if not run_path.exists():
        raise FileNotFoundError(f"Missing {run_path}")
    with open(run_path, "r", encoding="utf-8") as f:
        meta = json.load(f)
    if not meta.get("max_last_seen_at"):
        raise ValueError("latest_run.json missing max_last_seen_at")
    return meta


def main():
    parser = argparse.ArgumentParser(description="Run cold email workflow")
    parser.add_argument("--state", default="TX", help="Target state/territory (default: TX)")
    parser.add_argument("--dry-run", action="store_true", help="Run outbound in dry-run mode")
    parser.add_argument("--send", action="store_true", help="Run outbound live (requires OUTBOUND_ENABLED=true)")
    parser.add_argument("--limit", type=int, default=3, help="Max recipients to send/dry-run (default: 3)")
    parser.add_argument("--recipients-file", type=str, default=None, help="Override recipients CSV path")
    parser.add_argument("--skip-pipeline", action="store_true", help="Skip run_daily_pipeline.bat")
    parser.add_argument("--require-one-click", action="store_true", help="Abort live sends if one-click fails")
    args = parser.parse_args()

    if args.send and args.dry_run:
        print("[ERROR] Choose either --send or --dry-run, not both.")
        sys.exit(1)

    dry_run = not args.send
    if args.send:
        dry_run = False
        if not args.require_one_click:
            print("[ERROR] --require-one-click is mandatory for live sends during ramp.")
            sys.exit(1)

    if not args.skip_pipeline:
        if not PIPELINE_BAT.exists():
            print(f"[ERROR] Missing pipeline script: {PIPELINE_BAT}")
            sys.exit(1)
        print("[INFO] Running daily pipeline...")
        result = subprocess.run(["cmd", "/c", str(PIPELINE_BAT)], cwd=SCRIPT_DIR)
        if result.returncode != 0:
            print(f"[ERROR] Pipeline failed with code {result.returncode}")
            sys.exit(result.returncode)

    # Verify latest_run.json
    try:
        meta = load_latest_run()
        print(f"[OK] latest_run.json max_last_seen_at: {meta.get('max_last_seen_at')}")
    except Exception as exc:
        print(f"[ERROR] Freshness metadata missing: {exc}")
        sys.exit(1)

    # Load leads and filter by state + recency/open
    leads = oce.load_leads()
    if not leads:
        print("[ERROR] No leads loaded")
        sys.exit(1)

    config = oce.load_config()
    as_of = datetime.now().date()
    eligible = [l for l in leads if is_open_recent(l, as_of, config["recency_days"])]
    state = args.state.upper()
    eligible_state = [l for l in eligible if (l.get("site_state") or "").upper() == state]
    high_med = [l for l in eligible_state if l.get("lead_score", 0) >= 6]

    ranked_state = sort_ranked(eligible_state)
    ranked_path = OUT_DIR / f"ranked_leads_{state.lower()}.csv"
    write_ranked_leads_csv(ranked_state, ranked_path)
    print(f"[OK] Ranked leads written: {ranked_path}")

    targets_path = OUT_DIR / f"recipient_targets_{state.lower()}.csv"
    target_count = write_recipient_targets(ranked_state, targets_path)
    print(f"[OK] Recipient targets written: {targets_path} ({target_count} rows)")

    # Load recipients for sending
    recipients_path = Path(args.recipients_file) if args.recipients_file else None
    recipients = oce.load_recipients(recipients_path)
    if recipients_path:
        print(f"[INFO] Using recipients file: {recipients_path}")

    recipients = filter_recipients_for_state(recipients, state)
    suppression = oce.load_suppression_list()
    sent_all_time = oce.get_already_sent_all_time()

    sendable = []
    suppressed_count = 0
    already_sent_count = 0
    for r in recipients:
        email = r.get("email", "").strip().lower()
        if not email:
            continue
        if email in sent_all_time:
            already_sent_count += 1
            continue
        if oce.is_suppressed(email, suppression):
            suppressed_count += 1
            continue
        sendable.append(r)

    sendable_path = OUT_DIR / f"recipients_sendable_{state.lower()}.csv"
    write_recipients_csv(sendable, sendable_path)
    print(f"[OK] Sendable recipients written: {sendable_path}")

    print(f"\nCOUNTS")
    print(f"  Eligible leads ({state}, open + {config['recency_days']}d): {len(eligible_state)}")
    print(f"  High/Med selected ({state}): {len(high_med)}")
    print(f"  Recipients after suppression/dedupe: {len(sendable)}")

    # Render previews (use sendable; fall back to state recipients; else synthetic)
    preview_recipients = sendable[:]
    if not preview_recipients:
        preview_recipients = recipients[:]
        if preview_recipients:
            print("[WARN] No sendable recipients; rendering previews from pre-dedupe list.")
    if not preview_recipients:
        print("[WARN] No recipients found; rendering previews with synthetic recipients.")
    while len(preview_recipients) < 3:
        idx = len(preview_recipients) + 1
        preview_recipients.append({
            "email": f"preview{idx}@example.com",
            "first_name": f"Preview{idx}",
            "last_name": "User",
            "firm_name": "Sample Safety Consulting",
            "segment": "",
            "state_pref": state,
        })

    render_previews(preview_recipients, ranked_state, state, count=3)

    # Run outbound send/dry-run
    if not sendable:
        print("[INFO] No recipients to send.")
        append_metrics_row(
            state=state,
            intended=0,
            sent=0,
            skipped_suppressed=suppressed_count,
            skipped_no_high_med=0,
            skipped_one_click_failed=0,
        )
        sys.exit(0)
    
    outbound_cmd = [
        sys.executable,
        str(SCRIPT_DIR / "outbound_cold_email.py"),
        "--recipients-file",
        str(sendable_path),
        "--leads-file",
        str(oce.LEADS_PATH),
        "--limit",
        str(args.limit),
    ]
    if dry_run:
        outbound_cmd.append("--dry-run")
    if args.require_one_click:
        outbound_cmd.append("--require-one-click")

    intended = min(args.limit, len(sendable))
    start_time = datetime.now()
    print(f"[INFO] Running outbound: {' '.join(outbound_cmd)}")
    result = subprocess.run(outbound_cmd, cwd=SCRIPT_DIR)

    end_time = datetime.now()
    sent, skipped_no_high_med, skipped_one_click_failed = collect_outbound_metrics(
        campaign_id=oce.get_campaign_id(),
        start_time=start_time,
        end_time=end_time,
    )
    append_metrics_row(
        state=state,
        intended=intended,
        sent=sent,
        skipped_suppressed=suppressed_count,
        skipped_no_high_med=skipped_no_high_med,
        skipped_one_click_failed=skipped_one_click_failed,
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
