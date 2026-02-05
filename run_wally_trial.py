#!/usr/bin/env python3
"""Run Wally trial workflow: preflight, estimate counts, preview, live send, and schedule."""

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

from export_daily import export_daily


def load_environment(repo_root: Path) -> None:
    if load_dotenv is None:
        return
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path, override=False)


def resolve_customer_path(customer_arg: str, repo_root: Path) -> Path:
    candidate = Path(customer_arg)
    if candidate.exists():
        return candidate

    by_name = repo_root / "customers" / customer_arg
    if by_name.exists():
        return by_name

    if not customer_arg.lower().endswith(".json"):
        by_name_json = repo_root / "customers" / f"{customer_arg}.json"
        if by_name_json.exists():
            return by_name_json

    return candidate


def parse_recipients(config: dict) -> list[str]:
    recipients = config.get("recipients") or config.get("email_recipients") or []
    if not isinstance(recipients, list):
        return []
    cleaned = []
    seen = set()
    for recipient in recipients:
        email = str(recipient).strip().lower()
        if email and email not in seen:
            seen.add(email)
            cleaned.append(email)
    return cleaned


def preflight(customer_path: Path, require_smtp: bool = True) -> tuple[bool, str]:
    if not customer_path.exists():
        return False, f"CONFIG_ERROR missing variables: CUSTOMER_CONFIG({customer_path})"

    with open(customer_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    missing = []
    brand_name = (config.get("brand_name") or os.getenv("BRAND_NAME") or "").strip()
    mailing_address = (config.get("mailing_address") or os.getenv("MAILING_ADDRESS") or "").strip()

    if not brand_name:
        missing.append("BRAND_NAME")
    if not mailing_address:
        missing.append("MAILING_ADDRESS")

    recipients = parse_recipients(config)
    if not recipients:
        missing.append("RECIPIENTS")

    if require_smtp:
        for key in ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS"]:
            if not os.getenv(key, "").strip():
                missing.append(key)

    if missing:
        return False, f"CONFIG_ERROR missing variables: {', '.join(missing)}"
    return True, "PREFLIGHT_OK"


def estimate_daily_counts(
    db_path: str,
    out_dir: str,
    territory_code: str,
    content_filter: str,
    lookback_days: int,
) -> Path:
    rows = []
    today = date.today()

    for offset in range(lookback_days):
        as_of = today - timedelta(days=offset)
        stats = export_daily(
            db_path=db_path,
            outdir=out_dir,
            as_of_date=as_of.isoformat(),
            territory_code=territory_code,
            content_filter=content_filter,
        )
        rows.append(
            {
                "as_of_date": as_of.isoformat(),
                "sendable_leads": stats["sendable_leads"],
                "excluded_by_territory": stats["excluded_by_territory"],
                "excluded_by_content_filter": stats["excluded_by_content_filter"],
                "deduped_records_removed": stats["deduped_records_removed"],
            }
        )

    rows.sort(key=lambda row: row["as_of_date"])
    output_path = Path(out_dir) / f"wally_trial_daily_counts_{today.isoformat()}.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "as_of_date",
                "sendable_leads",
                "excluded_by_territory",
                "excluded_by_content_filter",
                "deduped_records_removed",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    return output_path


def run_preview_send(db_path: str, customer_config: str, chase_email: str) -> None:
    cmd = [
        sys.executable,
        "send_digest_email.py",
        "--db",
        db_path,
        "--customer",
        customer_config,
        "--mode",
        "daily",
        "--recipient-override",
        chase_email,
        "--dry-run",
        "--disable-pilot-guard",
    ]
    subprocess.run(cmd, check=True)


def run_live_send(db_path: str, customer_config: str, admin_email: str, send_live: bool) -> None:
    cmd = [
        sys.executable,
        "deliver_daily.py",
        "--db",
        db_path,
        "--customer",
        customer_config,
        "--mode",
        "daily",
        "--since-days",
        "14",
        "--admin-email",
        admin_email,
    ]
    if send_live:
        cmd.append("--send-live")
    subprocess.run(cmd, check=True)


def write_batch_runner(batch_path: Path, project_root: Path, customer_config: str, db_path: str, admin_email: str) -> None:
    lines = [
        "@echo off",
        f"cd /d \"{project_root}\"",
        "if not exist out mkdir out",
        "set RUN_TMP=out\\wally_trial_last_run.log",
        "echo [%date% %time%] Wally trial run start >> out\\wally_trial_task.log",
        (
            f"python deliver_daily.py --db \"{db_path}\" --customer \"{customer_config}\" "
            f"--mode daily --since-days 14 --admin-email \"{admin_email}\" "
            "> \"%RUN_TMP%\" 2>&1"
        ),
        "set RUN_EXIT=%ERRORLEVEL%",
        "type \"%RUN_TMP%\" >> out\\wally_trial_task.log",
        "findstr /C:\"CONFIG_ERROR\" \"%RUN_TMP%\" >nul",
        "if %ERRORLEVEL%==0 echo [%date% %time%] CONFIG_ERROR detected >> out\\wally_trial_task.log",
        "if %RUN_EXIT% NEQ 0 echo [%date% %time%] ERROR: Wally trial run failed >> out\\wally_trial_task.log",
        "if %RUN_EXIT% EQU 0 echo [%date% %time%] SUCCESS: Wally trial run completed >> out\\wally_trial_task.log",
        "exit /b %RUN_EXIT%",
    ]
    batch_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def enable_schedule(task_name: str, batch_path: Path) -> None:
    cmd = [
        "schtasks",
        "/Create",
        "/F",
        "/SC",
        "DAILY",
        "/ST",
        "08:00",
        "/TN",
        task_name,
        "/TR",
        f'cmd /c "{batch_path}"',
    ]
    subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Wally trial workflow")
    parser.add_argument("customer_path", nargs="?", default="", help="Customer config path or name (optional)")
    parser.add_argument("--db", default="data/osha.sqlite")
    parser.add_argument("--customer", default="customers/wally_trial_tx_triangle_v1.json")
    parser.add_argument("--out-dir", default="out")
    parser.add_argument("--territory-code", default="TX_TRIANGLE_V1")
    parser.add_argument("--content-filter", default="high_medium")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--chase-email", default="cchevali@gmail.com")
    parser.add_argument("--admin-email", default="support@microflowops.com")
    parser.add_argument("--send-live", action="store_true", help="Trigger first live send to Wally")
    parser.add_argument("--enable-schedule", action="store_true", help="Create 08:00 local scheduled task")
    parser.add_argument("--task-name", default="OSHA Wally Trial Daily")
    parser.add_argument("--preflight-only", action="store_true", help="Check config/env and exit")

    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent
    load_environment(repo_root)

    customer_arg = args.customer_path if args.customer_path else args.customer
    customer_path = resolve_customer_path(customer_arg, repo_root)

    if args.preflight_only:
        ok, msg = preflight(customer_path, require_smtp=True)
        print(msg)
        raise SystemExit(0 if ok else 1)

    Path(args.out_dir).mkdir(parents=True, exist_ok=True)

    counts_path = estimate_daily_counts(
        db_path=args.db,
        out_dir=args.out_dir,
        territory_code=args.territory_code,
        content_filter=args.content_filter,
        lookback_days=args.lookback_days,
    )
    print(f"Daily-count estimate written: {counts_path}")

    run_preview_send(args.db, str(customer_path), args.chase_email)
    print(f"Preview dry-run sent to Chase override: {args.chase_email}")

    if args.send_live:
        run_live_send(args.db, str(customer_path), args.admin_email, True)
        print("First live send triggered via deliver_daily.py")

    batch_path = repo_root / "run_wally_trial_daily.bat"
    write_batch_runner(
        batch_path=batch_path,
        project_root=repo_root,
        customer_config=str(customer_path),
        db_path=args.db,
        admin_email=args.admin_email,
    )
    print(f"Batch runner written: {batch_path.name}")

    if args.enable_schedule:
        enable_schedule(args.task_name, batch_path.resolve())
        print(f"Scheduled task enabled: {args.task_name} at 08:00 local (set host timezone to America/Chicago)")


if __name__ == "__main__":
    main()
