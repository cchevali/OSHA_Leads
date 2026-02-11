#!/usr/bin/env python3
"""Run Wally trial workflow: preflight, estimate counts, preview, live send, and schedule."""

import argparse
import csv
import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlparse, urlunparse

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

    # Best-effort prefs endpoint reachability check. Do not fail preflight; just warn so operators
    # can see broken prefs endpoints before a run. The digest sender independently disables links
    # when endpoints are unavailable.
    prefs_ok, prefs_detail = _prefs_links_reachable(timeout_s=2.0)
    if not prefs_ok and prefs_detail not in {"env_disabled"}:
        print(f"PREFS_LINKS_DISABLED detail={prefs_detail}", flush=True)
    return True, "PREFLIGHT_OK"


def _prefs_links_reachable(timeout_s: float = 2.0) -> tuple[bool, str]:
    """
    Detect whether the unsub prefs endpoints exist and are reachable.

    When disabled/missing/unreachable, callers should set PREFS_LINKS_DISABLED to avoid shipping broken links.
    """
    # Unit tests should be deterministic and offline-safe.
    if "unittest" in sys.modules:
        return True, "skipped_unittest"

    if os.getenv("PREFS_LINKS_DISABLED", "").strip().lower() in {"1", "true", "yes"}:
        return False, "env_disabled"

    base_endpoint = (os.getenv("PREFS_ENDPOINT_BASE", "") or "https://unsub.microflowops.com").strip()
    if not base_endpoint:
        return False, "missing_base"

    try:
        parsed = urlparse(base_endpoint)
        base = urlunparse(parsed._replace(path="", params="", query="", fragment="")).rstrip("/")
    except Exception:
        base = base_endpoint.rstrip("/")

    url = f"{base}/prefs/enable_lows?t=invalid.invalid"

    # Prefer requests (nicer TLS + redirects), fall back to stdlib urllib.
    try:
        import requests  # type: ignore

        resp = requests.get(url, timeout=timeout_s, allow_redirects=False)
        if resp.status_code == 404:
            return False, "http_404"
        if resp.status_code >= 500:
            return False, f"http_{resp.status_code}"
        return True, f"http_{resp.status_code}"
    except Exception:
        pass

    try:
        import urllib.request
        import urllib.error

        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = int(getattr(resp, "status", 200))
            if status == 404:
                return False, "http_404"
            if status >= 500:
                return False, f"http_{status}"
            return True, f"http_{status}"
    except urllib.error.HTTPError as e:
        code = int(getattr(e, "code", 0) or 0)
        if code == 404:
            return False, "http_404"
        if code >= 500:
            return False, f"http_{code}"
        return True, f"http_{code}"
    except Exception as exc:
        return False, f"error={type(exc).__name__}"


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


def _load_subscriber_last_sent_at(db_path: str, subscriber_key: str) -> str | None:
    if not subscriber_key:
        return None
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT last_sent_at FROM subscribers WHERE subscriber_key = ? LIMIT 1", (subscriber_key,))
        row = cur.fetchone()
        if not row:
            return None
        return row[0]
    finally:
        try:
            conn.close()
        except Exception:
            pass


def run_test_send(db_path: str, customer_config: str) -> None:
    # Test-only laptop entrypoint: force snapshot send to Chase, without mutating send state.
    chase_email = "cchevali+oshasmoke@gmail.com"
    print(f"TEST_SEND variant=starter_snapshot recipient={chase_email} state_mutation=NO", flush=True)

    with open(customer_config, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    subscriber_key = (cfg.get("subscriber_key") or "").strip()
    last_sent_before = _load_subscriber_last_sent_at(db_path, subscriber_key)

    cmd = [
        sys.executable,
        "send_digest_email.py",
        "--db",
        db_path,
        "--customer",
        customer_config,
        "--mode",
        "daily",
        "--smoke-cchevali",
        "--force-starter-snapshot",
        "--no-state-mutation",
        "--log-level",
        "ERROR",
    ]
    subprocess.run(cmd, check=True)

    last_sent_after = _load_subscriber_last_sent_at(db_path, subscriber_key)
    if last_sent_after != last_sent_before:
        raise SystemExit(
            f"TEST_SEND_STATE_MUTATION last_sent_at_before={last_sent_before!r} last_sent_at_after={last_sent_after!r}"
        )


def run_test_send_daily(db_path: str, customer_config: str) -> None:
    # Test-only laptop entrypoint: render the daily "new since last send" variant to Chase,
    # without mutating send state.
    chase_email = "cchevali+oshasmoke@gmail.com"
    print(f"TEST_SEND variant=daily_new_since_last_send recipient={chase_email} state_mutation=NO", flush=True)

    with open(customer_config, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    subscriber_key = (cfg.get("subscriber_key") or "").strip()
    last_sent_before = _load_subscriber_last_sent_at(db_path, subscriber_key)

    # Resilience: the real PC config is git-ignored; default snapshot_when_0_new to YES for the Wally trial
    # when the key is missing, without mutating the source file.
    cfg_for_send = cfg
    is_wally_trial = (cfg.get("customer_id") == "wally_trial_tx_triangle_v1") or (subscriber_key == "wally_trial")
    if is_wally_trial and "snapshot_when_0_new" not in cfg:
        print("TRIAL_DEFAULT snapshot_when_0_new=YES (config_missing)")
        cfg_for_send = dict(cfg)
        cfg_for_send["snapshot_when_0_new"] = True
        cfg_for_send.setdefault("snapshot_recent_limit", 8)

        tmp_path = Path("out") / "wally_trial_test_send_daily.customer.json"
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path.write_text(json.dumps(cfg_for_send, indent=2) + "\n", encoding="utf-8")
        customer_config = str(tmp_path)

    cmd = [
        sys.executable,
        "send_digest_email.py",
        "--db",
        db_path,
        "--customer",
        customer_config,
        "--mode",
        "daily",
        "--smoke-cchevali",
        "--no-state-mutation",
        "--log-level",
        "ERROR",
    ]
    subprocess.run(cmd, check=True)

    last_sent_after = _load_subscriber_last_sent_at(db_path, subscriber_key)
    if last_sent_after != last_sent_before:
        raise SystemExit(
            f"TEST_SEND_STATE_MUTATION last_sent_at_before={last_sent_before!r} last_sent_at_after={last_sent_after!r}"
        )


def write_batch_runner(batch_path: Path, project_root: Path, customer_config: str, db_path: str, admin_email: str) -> None:
    customer_rel = _relative_batch_path(project_root, customer_config)
    lines = [
        "@echo off",
        "cd /d \"%~dp0\"",
        "if not exist out mkdir out",
        "set RUN_TMP=out\\wally_trial_last_run.log",
        "echo [%date% %time%] Wally trial run start >> out\\wally_trial_task.log",
        "echo [%date% %time%] === RUN HEADER === >> out\\wally_trial_task.log",
        "echo [%date% %time%] batch=%~f0 >> out\\wally_trial_task.log",
        "echo [%date% %time%] cwd=%cd% >> out\\wally_trial_task.log",
        "for /f \"delims=\" %%p in ('where python 2^>nul') do echo [%date% %time%] python=%%p >> out\\wally_trial_task.log",
        "if errorlevel 1 echo [%date% %time%] python=NOT_FOUND >> out\\wally_trial_task.log",
        (
            "powershell -NoProfile -ExecutionPolicy Bypass "
            f"-File \"%~dp0scripts\\run_with_secrets.ps1\" "
            f"python deliver_daily.py --db \"{db_path}\" --customer \"%~dp0{customer_rel}\" "
            f"--mode daily --since-days 14 --admin-email \"{admin_email}\" --send-live "
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
    batch_text = _sanitize_task_path(batch_path)
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
        build_task_action(batch_text),
    ]
    subprocess.run(cmd, check=True)


def _sanitize_task_path(path: Path) -> str:
    batch_text = str(path).strip()
    while batch_text.endswith('"') or batch_text.endswith("'"):
        batch_text = batch_text[:-1]
    return batch_text.strip()


def _relative_batch_path(project_root: Path, path_text: str) -> str:
    path = Path(path_text)
    try:
        root = project_root.resolve()
        if path.is_absolute():
            rel = path.resolve().relative_to(root)
        else:
            rel = path
        rel_text = str(rel)
    except Exception:
        rel_text = path.name
    rel_text = rel_text.replace("/", "\\").lstrip("\\/")
    return rel_text


def build_task_action(batch_text: str) -> str:
    return f'cmd /c ""{batch_text}""'


def _strip_quotes(value: str) -> str:
    text = (value or "").strip()
    if text.startswith('"') and text.endswith('"') and len(text) >= 2:
        text = text[1:-1]
    return text.strip()


def _normalize_command(command: str) -> str:
    cleaned = _strip_quotes(command)
    base = os.path.basename(cleaned).lower()
    if base in ("cmd.exe", "cmd"):
        return "cmd"
    return cleaned


def format_task_to_run(command: str, arguments: str | None) -> str:
    cmd = _normalize_command(command)
    args = (arguments or "").strip()
    if args:
        return f"{cmd} {args}"
    return cmd


def extract_exec_action(xml_text: str) -> str | None:
    try:
        root = ET.fromstring(xml_text.strip())
    except Exception:
        return None
    namespace = ""
    if root.tag.startswith("{"):
        namespace = root.tag.split("}", 1)[0] + "}"
    exec_node = root.find(f".//{namespace}Exec")
    if exec_node is None:
        return None
    command = exec_node.findtext(f"{namespace}Command", default="").strip()
    if not command:
        return None
    arguments = exec_node.findtext(f"{namespace}Arguments", default="").strip()
    return format_task_to_run(command, arguments)


def query_task_to_run(task_name: str) -> str | None:
    cmd = ["schtasks", "/Query", "/TN", task_name, "/XML"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    return extract_exec_action(result.stdout)


def verify_schedule_action(task_name: str, expected_action: str) -> None:
    actual = query_task_to_run(task_name)
    verify_schedule_action_from_actual(expected_action, actual)


def verify_schedule_action_from_actual(expected_action: str, actual: str | None) -> None:
    hint = "run --enable-schedule"
    if not actual:
        print(f"SCHEDULE_CHECK_FAILED expected={expected_action} actual=MISSING_TASK_TO_RUN hint={hint}")
        raise SystemExit(1)
    if actual != expected_action:
        print(f"SCHEDULE_CHECK_FAILED expected={expected_action} actual={actual} hint={hint}")
        raise SystemExit(1)
    print(f"SCHEDULE_OK /TR={actual}")


def run_doctor(customer_path: Path, repo_root: Path, task_name: str, check_scheduler: bool) -> int:
    # Non-sending health check: validate config/env (including SMTP vars) and, if available,
    # verify the Task Scheduler action matches the repo's expected batch runner.
    ok, msg = preflight(customer_path, require_smtp=True)
    if not ok:
        print(f"DOCTOR_FAIL preflight={msg}")
        return 1

    # Scheduler verification runs only on the operator PC (Task Scheduler is local-machine state).
    # Default: skip so --doctor never calls schtasks unless explicitly opted in.
    if not check_scheduler:
        print("DOCTOR_NOTE scheduler_check=SKIPPED (opt-in)")
    else:
        # Task Scheduler verification (best-effort): do not attempt to create/modify tasks.
        if "query_task_to_run" in globals() and "build_task_action" in globals():
            try:
                batch_path = (repo_root / "run_wally_trial_daily.bat").resolve()
                expected_action = build_task_action(_sanitize_task_path(batch_path))
                actual = query_task_to_run(task_name)
                if not actual:
                    print("DOCTOR_NOTE scheduler_check=SKIPPED (task missing or schtasks unavailable)")
                elif actual != expected_action:
                    print(f"DOCTOR_FAIL scheduler_check=BAD expected={expected_action} actual={actual}")
                    return 1
                else:
                    print(f"DOCTOR_NOTE scheduler_check=OK /TR={actual}")
            except Exception as e:
                print(f"DOCTOR_NOTE scheduler_check=SKIPPED error={type(e).__name__}")
        else:
            print("DOCTOR_NOTE scheduler_check=SKIPPED (not implemented)")

    print("DOCTOR_OK")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Wally trial workflow")
    parser.add_argument("customer_path", nargs="?", default="", help="Customer config path or name (optional)")
    parser.add_argument("--db", default="data/osha.sqlite")
    parser.add_argument("--customer", default="customers/wally_trial_tx_triangle_v1.json")
    parser.add_argument("--out-dir", default="out")
    parser.add_argument("--territory-code", default="TX_TRIANGLE_V1")
    parser.add_argument("--content-filter", default="high_medium")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument(
        "--chase-email",
        default=(os.getenv("OSHA_SMOKE_TO") or os.getenv("CHASE_EMAIL") or "cchevali+oshasmoke@gmail.com"),
    )
    parser.add_argument("--admin-email", default="support@microflowops.com")
    parser.add_argument("--send-live", action="store_true", help="Trigger first live send to Wally")
    parser.add_argument(
        "--test-send",
        action="store_true",
        help="Laptop-safe: force a Starter Snapshot send to cchevali+oshasmoke@gmail.com without mutating send state",
    )
    parser.add_argument(
        "--test-send-daily",
        action="store_true",
        help="Laptop-safe: send the daily 'new since last send' variant to cchevali+oshasmoke@gmail.com without mutating send state",
    )
    parser.add_argument("--enable-schedule", action="store_true", help="Create 08:00 local scheduled task")
    parser.add_argument("--check-schedule", action="store_true", help="Verify scheduled task action only")
    parser.add_argument("--task-name", default="OSHA Wally Trial Daily")
    parser.add_argument("--preflight-only", action="store_true", help="Check config/env and exit")
    parser.add_argument(
        "--doctor",
        action="store_true",
        help="Non-sending health check (same validations as --preflight-only; scheduler check is opt-in)",
    )
    parser.add_argument(
        "--doctor-check-scheduler",
        action="store_true",
        help="When used with --doctor: verify Task Scheduler /TR action via schtasks (operator PC only)",
    )

    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent
    load_environment(repo_root)

    customer_arg = args.customer_path if args.customer_path else args.customer
    customer_path = resolve_customer_path(customer_arg, repo_root)

    if args.test_send_daily:
        # Allow a single "night-before" command: scheduler verification (PC-only) + non-mutating daily test send.
        if args.doctor_check_scheduler:
            code = run_doctor(
                customer_path=customer_path,
                repo_root=repo_root,
                task_name=args.task_name,
                check_scheduler=True,
            )
            if code != 0:
                raise SystemExit(code)
        run_test_send_daily(db_path=args.db, customer_config=str(customer_path))
        raise SystemExit(0)

    if args.test_send:
        run_test_send(db_path=args.db, customer_config=str(customer_path))
        raise SystemExit(0)

    if args.doctor:
        raise SystemExit(
            run_doctor(
                customer_path=customer_path,
                repo_root=repo_root,
                task_name=args.task_name,
                check_scheduler=bool(args.doctor_check_scheduler),
            )
        )

    if args.preflight_only:
        ok, msg = preflight(customer_path, require_smtp=True)
        print(msg)
        raise SystemExit(0 if ok else 1)

    batch_path = repo_root / "run_wally_trial_daily.bat"
    batch_path_resolved = batch_path.resolve()
    expected_action = build_task_action(_sanitize_task_path(batch_path_resolved))

    if args.check_schedule:
        verify_schedule_action(args.task_name, expected_action)
        raise SystemExit(0)

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

    write_batch_runner(
        batch_path=batch_path,
        project_root=repo_root,
        customer_config=str(customer_path),
        db_path=args.db,
        admin_email=args.admin_email,
    )
    print(f"Batch runner written: {batch_path.name}")

    if args.enable_schedule:
        enable_schedule(args.task_name, batch_path_resolved)
        verify_schedule_action(args.task_name, expected_action)
        print(f"Scheduled task enabled: {args.task_name} at 08:00 local (set host timezone to America/Chicago)")


if __name__ == "__main__":
    main()
