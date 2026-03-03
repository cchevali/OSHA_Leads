from __future__ import annotations

import argparse
import json
import os
import re
import smtplib
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path
from typing import Any

import crm_light
import run_trial_admin
from lead_filters import load_territory_definitions, resolve_territory_code

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

_RE_SUBSCRIBER_KEY = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")
_RE_DRAFT_TO = re.compile(r"^To:\s*(.+)$", re.IGNORECASE)
_RE_DRAFT_SUBJECT = re.compile(r"^Subject:\s*(.+)$", re.IGNORECASE)
_RE_UNRESOLVED_STRIPE_BRACE = re.compile(r"\{[^}\n]*stripe_link[^}\n]*\}", re.IGNORECASE)
_RE_UNRESOLVED_STRIPE_ANGLE = re.compile(r"<\s*stripe_link\s*>", re.IGNORECASE)
DEFAULT_SENDS_LIMIT = 14
DEFAULT_EXPIRED_BEHAVIOR = "notify_once"
TRIAL_WEEKDAYS_ONLY = True
TRIAL_WEEKEND_SKIP_TOKEN = "SKIP_NON_WEEKDAY"


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


def _resolve_trial_timezone(tz_name: str) -> Any:
    if ZoneInfo is not None:
        try:
            return ZoneInfo((tz_name or "").strip() or "America/Chicago")
        except Exception:
            try:
                return ZoneInfo("America/Chicago")
            except Exception:
                pass
    return timezone.utc


def _weekday_name_token(idx: int) -> str:
    names = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    if 0 <= int(idx) < len(names):
        return names[int(idx)]
    return "unknown"


def _trial_local_day_context(tz_name: str) -> dict[str, Any]:
    zone = _resolve_trial_timezone(tz_name)
    now_local = datetime.now(zone)
    weekday_idx = int(now_local.weekday())
    return {
        "timezone": str((tz_name or "").strip() or "America/Chicago"),
        "local_date": now_local.date().isoformat(),
        "weekday_idx": weekday_idx,
        "weekday_name": _weekday_name_token(weekday_idx),
        "is_weekend": weekday_idx >= 5,
    }


def _event_meta(
    *,
    source: str,
    send_mode: str,
    primary_recipient: str,
    local_date: str,
    exit_code: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source": (source or "").strip() or "run_trial_daily",
    }
    normalized_mode = (send_mode or "").strip().upper()
    normalized_recipient = (primary_recipient or "").strip().lower()
    normalized_date = (local_date or "").strip()
    if normalized_mode:
        payload["send_mode"] = normalized_mode
    if normalized_recipient:
        payload["primary_recipient"] = normalized_recipient
    if normalized_date:
        payload["local_date"] = normalized_date
    if exit_code is not None:
        payload["exit_code"] = int(exit_code)
    return payload


def _resolve_conversion_url() -> str:
    return (os.getenv("TRIAL_CONVERSION_URL") or "").strip()


def _parse_conversion_artifact(text: str) -> tuple[str, str, str]:
    lines = (text or "").splitlines(keepends=True)
    recipient = ""
    subject = ""
    body_offset = -1
    offset = 0
    for line in lines:
        if not recipient:
            m_to = _RE_DRAFT_TO.match((line or "").strip())
            if m_to:
                recipient = m_to.group(1).strip().lower()
                offset += len(line)
                continue
        if not subject:
            m_subject = _RE_DRAFT_SUBJECT.match((line or "").strip())
            if m_subject:
                subject = m_subject.group(1).strip()
                body_offset = offset + len(line)
                break
        offset += len(line)
    if not recipient:
        raise RuntimeError("CONFIG_ERROR conversion artifact missing To header")
    if not subject:
        raise RuntimeError("CONFIG_ERROR conversion artifact missing Subject header")
    body = (text or "")[body_offset:] if body_offset >= 0 else ""
    if not body.strip():
        raise RuntimeError("CONFIG_ERROR conversion artifact missing body")
    return recipient, subject, body


def _has_unresolved_conversion_link(text: str) -> bool:
    value = (text or "")
    lower = value.lower()
    if "stripe_link" in lower:
        return True
    if "{stripe_link}" in lower or "<stripe_link>" in lower:
        return True
    if _RE_UNRESOLVED_STRIPE_BRACE.search(value):
        return True
    if _RE_UNRESOLVED_STRIPE_ANGLE.search(value):
        return True
    return False


def _send_conversion_email_from_artifact(
    *,
    artifact_path: Path,
    subscriber_key: str,
    territory_code: str,
) -> tuple[bool, str, str]:
    try:
        text = artifact_path.read_text(encoding="utf-8")
        recipient, subject, body = _parse_conversion_artifact(text)
    except Exception as exc:
        return False, "", str(exc)

    smtp_host = (os.getenv("SMTP_HOST") or "").strip()
    smtp_port_text = (os.getenv("SMTP_PORT") or "").strip()
    smtp_user = (os.getenv("SMTP_USER") or "").strip()
    smtp_pass = (os.getenv("SMTP_PASS") or "").strip()

    missing = [k for k, v in (("SMTP_HOST", smtp_host), ("SMTP_PORT", smtp_port_text), ("SMTP_USER", smtp_user), ("SMTP_PASS", smtp_pass)) if not v]
    if missing:
        return False, "", f"missing SMTP env: {','.join(missing)}"

    try:
        smtp_port = int(smtp_port_text)
    except Exception:
        return False, "", "invalid SMTP_PORT"

    from_email = (os.getenv("FROM_EMAIL") or smtp_user).strip() or smtp_user
    reply_to = (os.getenv("REPLY_TO_EMAIL") or from_email).strip() or from_email
    support_email = (os.getenv("SUPPORT_EMAIL") or "support@microflowops.com").strip() or "support@microflowops.com"

    msg = EmailMessage()
    msg["Message-ID"] = make_msgid()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = recipient
    msg["Reply-To"] = reply_to
    msg["X-Customer-ID"] = subscriber_key
    msg["X-Territory-Code"] = territory_code
    msg["List-Unsubscribe"] = f"<mailto:{support_email}?subject=unsubscribe>"
    msg.set_content(body)

    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        return True, str(msg.get("Message-ID") or ""), ""
    except Exception as exc:
        return False, "", str(exc)


def _territory_states(territory_code: str) -> list[str]:
    defs = load_territory_definitions()
    canonical = resolve_territory_code((territory_code or "").strip().upper(), defs)
    terr = defs.get(canonical, {})
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
        primary_recipient = str(sub.get("email") or "").strip().lower()
        tz_name = str(sub.get("tz") or "").strip() or "America/Chicago"
        successful = crm_light.count_trial_delivery_days(
            conn,
            subscriber_key,
            start_date,
            tz_name=tz_name,
            primary_recipient=primary_recipient,
            weekdays_only=True,
        )
        expired_by_sends = bool(successful >= sends_limit)
        expired = expired_by_sends
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
    allow_weekend_send: bool = False,
) -> int:
    sk = _validate_subscriber_key(subscriber_key)
    resolved_crm_db = crm_light.resolve_crm_db_path(crm_db)
    policy = _resolve_policy(sk, resolved_crm_db)
    out_root = crm_light.data_dir()
    run_id = f"trial_{sk}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    print(f"subscriber_key={policy.subscriber_key}")
    print(f"crm_db={resolved_crm_db}")
    print(f"leads_db={leads_db}")
    print(f"start_date={policy.start_date}")
    print(f"sends_limit={policy.sends_limit}")
    print(f"successful_sends={policy.successful_sends}")
    print(f"expired_behavior={policy.expired_behavior}")
    conversion_url = _resolve_conversion_url()
    print(f"trial_conversion_url_present={'YES' if conversion_url else 'NO'}")
    print(f"expired={'YES' if policy.expired else 'NO'}")
    print(f"dry_run={'YES' if dry_run else 'NO'}")
    day_ctx = _trial_local_day_context(policy.tz)
    print(f"TRIAL_WEEKDAYS_ONLY={1 if TRIAL_WEEKDAYS_ONLY else 0}")
    print(f"trial_effective_timezone={day_ctx['timezone']}")
    print(f"trial_effective_local_date={day_ctx['local_date']}")
    print(f"trial_effective_weekday={day_ctx['weekday_name']}")
    print(f"trial_allow_weekend_send={'YES' if allow_weekend_send else 'NO'}")

    if print_config:
        return 0

    if TRIAL_WEEKDAYS_ONLY and (not allow_weekend_send) and bool(day_ctx["is_weekend"]):
        print(
            f"{TRIAL_WEEKEND_SKIP_TOKEN} subscriber_key={policy.subscriber_key} "
            f"local_date={day_ctx['local_date']} weekday={day_ctx['weekday_name']} gate=trial_weekdays_only"
        )
        return 0

    crm_light.ensure_database(resolved_crm_db)
    with crm_light.open_conn(resolved_crm_db) as conn:
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
                    text_artifact = out_root / "trials" / policy.subscriber_key / "conversion_email.txt"
                    if text_artifact.exists():
                        text_artifact = text_artifact.resolve()
                    else:
                        text_artifact = run_trial_admin.write_conversion_draft(
                            subscriber_key=policy.subscriber_key,
                            crm_db_path=resolved_crm_db,
                            emit_stdout=False,
                        )
                    print(f"CONVERSION_ARTIFACT text_path={text_artifact}")
                    if send_live and not dry_run:
                        artifact_text = ""
                        try:
                            artifact_text = text_artifact.read_text(encoding="utf-8")
                        except Exception as exc:
                            artifact_text = ""
                            print(f"WARN_CONVERSION_ARTIFACT_READ_FAILED detail={exc}")
                        if _has_unresolved_conversion_link(artifact_text):
                            print(f"ERR_CONVERSION_LINK_MISSING subscriber_key={policy.subscriber_key}")
                            crm_light.append_send_event(
                                conn,
                                subscriber_key=policy.subscriber_key,
                                variant="conversion",
                                status="CONVERSION_LINK_MISSING",
                                run_id=run_id,
                                meta={
                                    "artifact_path": str(text_artifact),
                                    "reason": "stripe_link_placeholder",
                                },
                                ts_utc="",
                            )
                            return 0
                        sent, message_id, error_detail = _send_conversion_email_from_artifact(
                            artifact_path=text_artifact,
                            subscriber_key=policy.subscriber_key,
                            territory_code=policy.territory_code,
                        )
                        status = "CONVERSION_SENT" if sent else "CONVERSION_SEND_ERROR"
                        meta: dict[str, Any] = {
                            "start_date": policy.start_date,
                            "successful_sends": policy.successful_sends,
                            "sends_limit": policy.sends_limit,
                            "expired_behavior": policy.expired_behavior,
                            "artifact_path": str(text_artifact),
                        }
                        if message_id:
                            meta["message_id"] = message_id
                        if error_detail:
                            meta["error"] = error_detail
                        crm_light.append_send_event(
                            conn,
                            subscriber_key=policy.subscriber_key,
                            variant="conversion",
                            status=status,
                            run_id=run_id,
                            meta=meta,
                            ts_utc="",
                        )
                        if sent:
                            crm_light.set_trial_notified_at(conn, policy.subscriber_key, _now_utc_iso())
                            print(f"CONVERSION_EMAIL_SENT to={policy.email}")
                        else:
                            print(f"WARN_CONVERSION_EMAIL_SEND_FAILED detail={error_detail}")
                    else:
                        print("CONVERSION_EMAIL_PENDING send_live=NO")
            return 0

        local_today = str(day_ctx["local_date"])
        if send_live and not dry_run and crm_light.has_trial_delivery_on_local_date(
            conn,
            subscriber_key=policy.subscriber_key,
            start_date=policy.start_date,
            tz_name=policy.tz,
            primary_recipient=policy.email,
            local_date_text=local_today,
        ):
            crm_light.append_send_event(
                conn,
                subscriber_key=policy.subscriber_key,
                variant="daily",
                status="SKIP_ALREADY_SENT_LOCAL_DATE",
                run_id=run_id,
                meta={"local_date": local_today, "timezone": policy.tz},
                ts_utc="",
            )
            print(f"TRIAL_EVENT status=SKIP_ALREADY_SENT_LOCAL_DATE local_date={local_today}")
            return 0

        customer_path = _resolve_customer_config_path(policy.subscriber_key, customer_arg, out_root)
        customer_runtime = _load_or_build_customer_config(policy, customer_path, out_root)

        if test_send_daily:
            code, out = _run_send_digest_test_daily(leads_db, customer_runtime, dry_run=dry_run)
            status = "DRY_RUN" if dry_run else ("TEST_SENT" if code == 0 else "ERROR")
            event_mode = "DRY_RUN" if dry_run else ("TEST" if code == 0 else "ERROR")
            crm_light.append_send_event(
                conn,
                subscriber_key=policy.subscriber_key,
                variant="test_send_daily",
                status=status,
                run_id=run_id,
                meta=_event_meta(
                    source="run_trial_daily_test_send",
                    send_mode=event_mode,
                    primary_recipient=policy.email,
                    local_date=local_today,
                    exit_code=code,
                ),
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
            event_mode = "DRY_RUN" if code == 0 else "ERROR"
        else:
            mode = _try_extract_latest_send_start_mode(customer_id=customer_id)
            if code == 0 and mode == "LIVE":
                status = "SENT"
                event_mode = "LIVE"
            elif code == 0 and mode == "SAFE":
                status = "SAFE_MODE"
                event_mode = "SAFE"
            elif code == 0:
                status = "UNKNOWN"
                event_mode = "UNKNOWN"
            else:
                status = "ERROR"
                event_mode = "ERROR"

        crm_light.append_send_event(
            conn,
            subscriber_key=policy.subscriber_key,
            variant="daily",
            status=status,
            run_id=run_id,
            meta=_event_meta(
                source="run_trial_daily_deliver",
                send_mode=event_mode,
                primary_recipient=policy.email,
                local_date=local_today,
                exit_code=code,
            ),
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
        "--allow-weekend-send",
        action="store_true",
        help="Emergency/manual override: allow trial send path on Sat/Sun (default blocked).",
    )
    ap.add_argument(
        "--test-send-daily",
        action="store_true",
        help="Laptop-safe: render daily digest to OSHA_SMOKE_TO with --no-state-mutation (records DRY_RUN/TEST_SENT).",
    )

    args = ap.parse_args(argv)
    crm_db = crm_light.resolve_crm_db_path(str(args.crm_db or "").strip() or None)
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
            allow_weekend_send=bool(args.allow_weekend_send),
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
