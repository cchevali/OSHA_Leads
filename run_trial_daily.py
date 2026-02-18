from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import crm_light
from lead_filters import load_territory_definitions

_RE_SUBSCRIBER_KEY = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")
DEFAULT_SENDS_LIMIT = 10
DEFAULT_EXPIRED_BEHAVIOR = "notify_once"


@dataclass(frozen=True)
class TrialPolicy:
    subscriber_key: str
    email: str
    territory_code: str
    tz: str
    start_date: str
    sends_limit: int
    expired_behavior: str
    successful_sends: int
    expired: bool


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_subscriber_key(value: str) -> str:
    return (value or "").strip().lower()


def _validate_subscriber_key(value: str) -> str:
    sk = _normalize_subscriber_key(value)
    if not sk or not _RE_SUBSCRIBER_KEY.match(sk):
        raise ValueError("invalid subscriber_key (expected 1-80 chars from [A-Za-z0-9_.-])")
    return sk


def _resolve_sends_limit(trial_state: dict[str, Any] | None) -> int:
    if trial_state and trial_state.get("sends_limit") is not None:
        try:
            n = int(trial_state.get("sends_limit"))
            if n >= 1:
                return n
        except Exception:
            pass
    raw = (os.getenv("TRIAL_SENDS_LIMIT_DEFAULT") or "").strip()
    if raw:
        try:
            n = int(raw)
            if n >= 1:
                return n
        except Exception:
            return DEFAULT_SENDS_LIMIT
    return DEFAULT_SENDS_LIMIT


def _resolve_expired_behavior() -> str:
    raw = (os.getenv("TRIAL_EXPIRED_BEHAVIOR_DEFAULT") or "").strip().lower()
    if raw in {"notify_once", "none"}:
        return raw
    return DEFAULT_EXPIRED_BEHAVIOR


def _resolve_start_date(trial_state: dict[str, Any] | None) -> str:
    if trial_state and str(trial_state.get("start_date") or "").strip():
        return str(trial_state.get("start_date")).strip()
    raw = (os.getenv("TRIAL_START_DATE_DEFAULT") or "").strip()
    if raw:
        return raw
    return ""


def _resolve_conversion_url() -> str:
    return (os.getenv("TRIAL_CONVERSION_URL") or "").strip()


def _territory_states(territory_code: str) -> list[str]:
    defs = load_territory_definitions()
    terr = defs.get((territory_code or "").strip().upper(), {})
    states = terr.get("states") or []
    if isinstance(states, list):
        out: list[str] = []
        for s in states:
            t = str(s or "").strip().upper()
            if t and t not in out:
                out.append(t)
        return out
    return []


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _resolve_customer_config_path(subscriber_key: str, customer_arg: str, out_root: Path) -> Path:
    raw = (customer_arg or "").strip()
    if raw:
        p = Path(raw)
        if p.exists():
            return p
        if not p.is_absolute():
            candidate = (Path(__file__).resolve().parent / p).resolve()
            if candidate.exists():
                return candidate
        return p

    for c in (
        Path("customers") / f"{subscriber_key}.json",
        Path("customers") / f"{subscriber_key}_trial.json",
    ):
        if c.exists():
            return c
    return out_root / "trials" / subscriber_key / "customer.json"


def _generate_minimal_customer_config(policy: TrialPolicy) -> dict[str, Any]:
    states = _territory_states(policy.territory_code) or ["TX"]
    return {
        "customer_id": policy.subscriber_key,
        "subscriber_key": policy.subscriber_key,
        "subscriber_name": policy.subscriber_key,
        "active": True,
        "territory_code": policy.territory_code,
        "send_time_local": "08:00",
        "send_window_minutes": 60,
        "timezone": policy.tz or "America/Chicago",
        "content_filter": "high_medium",
        "include_low_fallback": True,
        "states": states,
        "opened_window_days": 14,
        "new_only_days": 1,
        "recipients": [policy.email],
        "email_recipients": [policy.email],
        "pilot_mode": True,
        "pilot_whitelist": [policy.email],
        "brand_name": (os.getenv("BRAND_NAME") or "MicroFlowOps").strip() or "MicroFlowOps",
        "mailing_address": (
            os.getenv("MAILING_ADDRESS") or "11539 Links Dr, Reston, VA 20190"
        ).strip()
        or "11539 Links Dr, Reston, VA 20190",
        "allow_live_send": True,
        "trial_catchup_enabled": True,
    }


def _load_or_build_customer_config(policy: TrialPolicy, customer_path: Path, out_root: Path) -> Path:
    try:
        if customer_path.exists():
            cfg = json.loads(customer_path.read_text(encoding="utf-8"))
        else:
            cfg = _generate_minimal_customer_config(policy)
    except Exception:
        cfg = _generate_minimal_customer_config(policy)
    cfg = dict(cfg)
    cfg["trial_catchup_enabled"] = True
    runtime_path = out_root / "trials" / policy.subscriber_key / "customer.runtime.json"
    _write_json(runtime_path, cfg)
    return runtime_path


def _write_conversion_artifact_legacy(policy: TrialPolicy, out_root: Path) -> Path:
    path = out_root / "trials" / policy.subscriber_key / "conversion_email.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    body = (
        f"To: {policy.email}\n"
        "Subject: Trial ended - ready to convert?\n\n"
        "Hi,\n\n"
        "Your trial has ended based on successful daily sends.\n\n"
        f"subscriber_key: {policy.subscriber_key}\n"
        f"territory_code: {policy.territory_code}\n"
        f"trial_start_date: {policy.start_date}\n"
        f"successful_sends: {policy.successful_sends}\n"
        f"sends_limit: {policy.sends_limit}\n\n"
        "If you'd like to continue receiving daily alerts, reply to this email and we can set up ongoing delivery.\n"
    )
    path.write_text(body, encoding="utf-8")
    return path


def _write_conversion_artifact(policy: TrialPolicy, out_root: Path) -> tuple[Path, Path]:
    artifact_dir = out_root / "trials" / policy.subscriber_key
    text_path = artifact_dir / "conversion_email.txt"
    html_path = artifact_dir / "conversion_email.html"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    conversion_url = _resolve_conversion_url()
    if conversion_url:
        cta_text = f"To continue, activate here: {conversion_url}\n"
        cta_html = f'<p>To continue, activate here: <a href="{conversion_url}">{conversion_url}</a></p>'
    else:
        cta_text = "To continue receiving daily alerts, reply to this email and we can set up ongoing delivery.\n"
        cta_html = "<p>To continue receiving daily alerts, reply to this email and we can set up ongoing delivery.</p>"

    text_body = (
        f"To: {policy.email}\n"
        "Subject: Trial ended - ready to convert?\n\n"
        "Hi,\n\n"
        "Your trial has ended based on successful daily sends.\n\n"
        f"subscriber_key: {policy.subscriber_key}\n"
        f"territory_code: {policy.territory_code}\n"
        f"trial_start_date: {policy.start_date}\n"
        f"successful_sends: {policy.successful_sends}\n"
        f"sends_limit: {policy.sends_limit}\n\n"
        f"{cta_text}"
    )
    html_body = (
        "<!doctype html>\n<html><body><p>Hi,</p>"
        "<p>Your trial has ended based on successful daily sends.</p>"
        f"<p>subscriber_key: {policy.subscriber_key}"
        f"<br>territory_code: {policy.territory_code}"
        f"<br>trial_start_date: {policy.start_date}"
        f"<br>successful_sends: {policy.successful_sends}"
        f"<br>sends_limit: {policy.sends_limit}</p>"
        f"{cta_html}</body></html>\n"
    )
    text_path.write_text(text_body, encoding="utf-8")
    html_path.write_text(html_body, encoding="utf-8")
    return text_path, html_path


def _run_send_digest_test_daily(db_path: str, customer_runtime_path: Path, dry_run: bool) -> tuple[int, str]:
    cmd = [
        sys.executable,
        "send_digest_email.py",
        "--db",
        db_path,
        "--customer",
        str(customer_runtime_path),
        "--mode",
        "daily",
        "--smoke-cchevali",
        "--no-state-mutation",
        "--log-level",
        "ERROR",
    ]
    if dry_run:
        cmd.append("--dry-run")
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).resolve().parent),
    )
    out = (proc.stdout or "") + (("\n" + proc.stderr) if proc.stderr else "")
    return proc.returncode, out


def _run_deliver_daily(
    db_path: str,
    customer_runtime_path: Path,
    send_live: bool,
    dry_run: bool,
) -> tuple[int, str]:
    cmd = [
        sys.executable,
        "deliver_daily.py",
        "--db",
        db_path,
        "--customer",
        str(customer_runtime_path),
        "--mode",
        "daily",
        "--since-days",
        "14",
        "--admin-email",
        "support@microflowops.com",
    ]
    if dry_run:
        cmd.append("--dry-run")
    elif send_live:
        cmd.append("--send-live")
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).resolve().parent),
    )
    out = (proc.stdout or "") + (("\n" + proc.stderr) if proc.stderr else "")
    return proc.returncode, out


def _try_extract_last_send_start_mode_from_log_text(text: str) -> str | None:
    for line in reversed((text or "").splitlines()):
        s = (line or "").strip()
        if "SEND_START mode=" not in s:
            continue
        if "mode=LIVE" in s:
            return "LIVE"
        if "mode=SAFE" in s:
            return "SAFE"
        return "UNKNOWN"
    return None


def _try_extract_latest_send_start_mode(customer_id: str) -> str | None:
    latest_path = crm_light.data_dir() / "latest.json"
    if not latest_path.exists():
        return None
    try:
        latest = json.loads(latest_path.read_text(encoding="utf-8"))
        run_id = str(latest.get("run_id") or "").strip()
        run_dir = latest_path.parent / "runs" / run_id
        send_result = run_dir / "send_result.json"
        if not send_result.exists():
            return None
        payload = json.loads(send_result.read_text(encoding="utf-8"))
        if str(payload.get("customer_id") or "") != str(customer_id or ""):
            return None
        log_path = Path(str(payload.get("log_path") or "")).expanduser()
        if not log_path.exists():
            return None
        text = log_path.read_text(encoding="utf-8", errors="replace")
        return _try_extract_last_send_start_mode_from_log_text(text)
    except Exception:
        return None


def _resolve_policy(subscriber_key: str, crm_db_path: str | Path | None) -> TrialPolicy:
    crm_light.ensure_database(crm_db_path)
    with crm_light.open_conn(crm_db_path) as conn:
        crm_light.init_schema(conn)
        sub = crm_light.get_subscriber(conn, subscriber_key)
        if not sub:
            raise RuntimeError(
                "CONFIG_ERROR trial subscriber missing in crm_light (run run_trial_admin.py add-trial)"
            )
        trial = crm_light.get_trial_state(conn, subscriber_key)
        if not trial:
            raise RuntimeError(
                "CONFIG_ERROR trial_state missing in crm_light (run run_trial_admin.py add-trial)"
            )
        start_date = _resolve_start_date(trial)
        if not start_date:
            raise RuntimeError(
                "CONFIG_ERROR start_date missing (set in trial_state or TRIAL_START_DATE_DEFAULT)"
            )
        sends_limit = _resolve_sends_limit(trial)
        behavior = _resolve_expired_behavior()
        successful = crm_light.count_successful_sends(conn, subscriber_key, start_date)
        expired = bool(successful >= sends_limit)
        return TrialPolicy(
            subscriber_key=subscriber_key,
            email=str(sub.get("email") or "").strip().lower(),
            territory_code=str(sub.get("territory_code") or "").strip().upper(),
            tz=str(sub.get("tz") or "").strip(),
            start_date=start_date,
            sends_limit=sends_limit,
            expired_behavior=behavior,
            successful_sends=successful,
            expired=expired,
        )


def run_trial_daily(
    subscriber_key: str,
    leads_db: str,
    crm_db: str | Path | None,
    customer_arg: str,
    send_live: bool,
    dry_run: bool,
    test_send_daily: bool,
    print_config: bool,
) -> int:
    sk = _validate_subscriber_key(subscriber_key)
    policy = _resolve_policy(sk, crm_db)
    out_root = crm_light.data_dir()
    run_id = f"trial_{sk}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    print(f"subscriber_key={policy.subscriber_key}")
    print(f"crm_db={str(Path(crm_db)) if crm_db else str(crm_light.crm_light_db_path())}")
    print(f"leads_db={leads_db}")
    print(f"start_date={policy.start_date}")
    print(f"sends_limit={policy.sends_limit}")
    print(f"successful_sends={policy.successful_sends}")
    print(f"expired_behavior={policy.expired_behavior}")
    conversion_url = _resolve_conversion_url()
    print(f"trial_conversion_url_present={'YES' if conversion_url else 'NO'}")
    print(f"expired={'YES' if policy.expired else 'NO'}")
    print(f"dry_run={'YES' if dry_run else 'NO'}")

    if print_config:
        return 0

    crm_light.ensure_database(crm_db)
    with crm_light.open_conn(crm_db) as conn:
        crm_light.init_schema(conn)

        if policy.expired:
            crm_light.append_send_event(
                conn,
                subscriber_key=policy.subscriber_key,
                variant="daily",
                status="SKIP_TRIAL_EXPIRED",
                run_id=run_id,
                meta={
                    "start_date": policy.start_date,
                    "successful_sends": policy.successful_sends,
                    "sends_limit": policy.sends_limit,
                    "expired_behavior": policy.expired_behavior,
                },
                ts_utc="",
            )
            crm_light.set_trial_ended_at(conn, policy.subscriber_key, _now_utc_iso())
            if policy.expired_behavior == "notify_once":
                trial_state = crm_light.get_trial_state(conn, policy.subscriber_key) or {}
                if not str(trial_state.get("notified_at_utc") or "").strip():
                    text_artifact, html_artifact = _write_conversion_artifact(policy, out_root)
                    crm_light.set_trial_notified_at(conn, policy.subscriber_key, _now_utc_iso())
                    print(f"CONVERSION_ARTIFACT text_path={text_artifact}")
                    print(f"CONVERSION_ARTIFACT html_path={html_artifact}")
            return 0

        customer_path = _resolve_customer_config_path(policy.subscriber_key, customer_arg, out_root)
        customer_runtime = _load_or_build_customer_config(policy, customer_path, out_root)

        if test_send_daily:
            code, out = _run_send_digest_test_daily(leads_db, customer_runtime, dry_run=dry_run)
            status = "DRY_RUN" if dry_run else ("SENT" if code == 0 else "ERROR")
            crm_light.append_send_event(
                conn,
                subscriber_key=policy.subscriber_key,
                variant="test_send_daily",
                status=status,
                run_id=run_id,
                meta={"exit_code": code},
                ts_utc="",
            )
            print(f"TRIAL_EVENT status={status}")
            if out.strip():
                print(out.rstrip())
            return 0 if code == 0 else code

        code, out = _run_deliver_daily(leads_db, customer_runtime, send_live=send_live, dry_run=dry_run)
        status = "ERROR"
        customer_id = ""
        try:
            customer_id = str(
                json.loads(customer_runtime.read_text(encoding="utf-8")).get("customer_id") or ""
            )
        except Exception:
            customer_id = ""

        if dry_run:
            status = "DRY_RUN" if code == 0 else "ERROR"
        else:
            mode = _try_extract_latest_send_start_mode(customer_id=customer_id)
            if code == 0 and mode == "LIVE":
                status = "SENT"
            elif code == 0 and mode == "SAFE":
                status = "SAFE_MODE"
            elif code == 0:
                status = "UNKNOWN"
            else:
                status = "ERROR"

        crm_light.append_send_event(
            conn,
            subscriber_key=policy.subscriber_key,
            variant="daily",
            status=status,
            run_id=run_id,
            meta={"exit_code": code},
            ts_utc="",
        )
        print(f"TRIAL_EVENT status={status}")
        if out.strip():
            print(out.rstrip())
        return 0 if code == 0 else code


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Run daily trial workflow for a subscriber_key (CRM-light + send ledger)."
    )
    ap.add_argument("--subscriber-key", required=True)
    ap.add_argument("--db", default="data/osha.sqlite", help="Leads SQLite db (default: data/osha.sqlite)")
    ap.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")
    ap.add_argument("--customer", default="", help="Optional customer config path to use.")
    ap.add_argument("--send-live", action="store_true", help="Allow live send (passes through existing safety gates).")
    ap.add_argument("--dry-run", action="store_true", help="Never send; record DRY_RUN in send_events.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved trial policy (non-secret) and exit.")
    ap.add_argument(
        "--test-send-daily",
        action="store_true",
        help="Laptop-safe: render daily digest to OSHA_SMOKE_TO with --no-state-mutation (records DRY_RUN/SENT).",
    )

    args = ap.parse_args(argv)
    crm_db: Path | None = None
    if (args.crm_db or "").strip():
        crm_db = Path(args.crm_db).expanduser()
    try:
        return run_trial_daily(
            args.subscriber_key,
            leads_db=str(args.db),
            crm_db=crm_db,
            customer_arg=str(args.customer),
            send_live=bool(args.send_live),
            dry_run=bool(args.dry_run),
            test_send_daily=bool(args.test_send_daily),
            print_config=bool(args.print_config),
        )
    except RuntimeError as exc:
        msg = str(exc)
        if msg.startswith("CONFIG_ERROR"):
            print(msg, file=sys.stderr)
        else:
            print(f"CONFIG_ERROR {msg}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
