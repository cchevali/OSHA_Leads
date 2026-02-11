import argparse
import csv
import os
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

ERR_AUTO_ENV = "ERR_AUTO_ENV"
ERR_AUTO_SMOKE_TO_MISSING = "ERR_AUTO_SMOKE_TO_MISSING"
ERR_AUTO_SUMMARY_TO_MISMATCH = "ERR_AUTO_SUMMARY_TO_MISMATCH"
ERR_AUTO_BATCH_FAILED = "ERR_AUTO_BATCH_FAILED"
ERR_AUTO_SUMMARY_SEND = "ERR_AUTO_SUMMARY_SEND"

PASS_AUTO_DRY_RUN = "PASS_AUTO_DRY_RUN"
PASS_AUTO_ALREADY_RAN = "PASS_AUTO_ALREADY_RAN"
PASS_AUTO_EXPORT = "PASS_AUTO_EXPORT"
PASS_AUTO_SUMMARY = "PASS_AUTO_SUMMARY"


def _norm_email(s: str) -> str:
    return (s or "").strip().lower()


def _parse_states(raw: str) -> list[str]:
    states = []
    for token in (raw or "").split(","):
        s = token.strip().upper()
        if not s:
            continue
        if s not in states:
            states.append(s)
    return states


def _daily_limit() -> int:
    raw = (os.getenv("OUTREACH_DAILY_LIMIT") or "200").strip()
    try:
        n = int(raw)
    except Exception:
        return 200
    return max(1, n)


def _prospects_path() -> Path:
    raw = (os.getenv("OUTREACH_PROSPECTS_PATH") or "").strip()
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else (REPO_ROOT / p)
    return REPO_ROOT / "outreach" / "sample_prospects.csv"


def _out_root() -> Path:
    raw = (os.getenv("OUTREACH_OUTPUT_ROOT") or "").strip()
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else (REPO_ROOT / p)
    return REPO_ROOT / "out" / "outreach"


def _choose_state(states: list[str], today: datetime) -> str:
    if not states:
        return ""
    idx = today.weekday() % len(states)
    return states[idx]


def _batch_id(state: str, today: datetime) -> str:
    return f"{today.date().isoformat()}_{state}"


def _run_cmd(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=str(REPO_ROOT), capture_output=True, text=True)


def _parse_key_value_lines(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in (text or "").splitlines():
        s = (line or "").strip()
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _build_limited_input(source: Path, state: str, limit: int) -> Path:
    with open(source, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = [dict(r) for r in reader]

    kept_state = 0
    limited_rows: list[dict] = []
    for r in rows:
        row_state = (r.get("state") or "").strip().upper()
        if row_state == state:
            if kept_state < limit:
                limited_rows.append(r)
                kept_state += 1
            continue
        limited_rows.append(r)

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8")
    tmp_path = Path(tmp.name)
    with tmp:
        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
        writer.writeheader()
        for row in limited_rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
    return tmp_path


def _resolve_summary_recipient(explicit_to: str) -> tuple[bool, str, str]:
    expected = _norm_email(os.getenv("OSHA_SMOKE_TO", ""))
    if not expected or "@" not in expected:
        return False, "", f"{ERR_AUTO_SMOKE_TO_MISSING} OSHA_SMOKE_TO not set"
    got = _norm_email(explicit_to) if (explicit_to or "").strip() else expected
    if got != expected:
        return False, "", f"{ERR_AUTO_SUMMARY_TO_MISMATCH} expected={expected} got={got}"
    return True, got, ""


def _send_summary_email(to_email: str, subject: str, text_body: str, html_body: str) -> tuple[bool, str]:
    try:
        import send_digest_email as sde
    except Exception as e:
        return False, f"import_send_digest_email_failed {e}"

    try:
        branding = sde.resolve_branding({})
        reply_to = (branding.get("reply_to") or os.getenv("REPLY_TO_EMAIL") or "support@microflowops.com").strip()
        mailto = f"mailto:{reply_to}?subject=unsubscribe"
        list_unsub = f"<{mailto}>"
        list_unsub_post = None

        ok, _msg_id, err = sde.send_email(
            recipient=to_email,
            subject=subject,
            html_body=html_body,
            text_body=text_body,
            customer_id="",
            territory_code="OUTREACH_AUTO",
            branding=branding,
            dry_run=False,
            list_unsub=list_unsub,
            list_unsub_post=list_unsub_post,
            label="outreach_auto_summary",
        )
        if not ok:
            return False, err or "send_failed"
        return True, ""
    except Exception as e:
        return False, str(e)


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily outreach automation wrapper (preflight + batch export + ops summary).")
    ap.add_argument("--dry-run", action="store_true", help="Validate and print actions only. No outputs, no email.")
    ap.add_argument("--to", default="", help="Optional summary recipient override; must equal OSHA_SMOKE_TO.")
    args = ap.parse_args()

    states = _parse_states(os.getenv("OUTREACH_STATES", "TX"))
    if not states:
        print(f"{ERR_AUTO_ENV} OUTREACH_STATES missing", file=sys.stderr)
        return 2

    prospects_path = _prospects_path()
    if not prospects_path.exists():
        print(f"{ERR_AUTO_ENV} prospects_missing path={prospects_path}", file=sys.stderr)
        return 2

    limit = _daily_limit()
    today = datetime.now()
    state = _choose_state(states, today)
    batch = _batch_id(state, today)
    out_root = _out_root()
    out_dir = out_root / batch
    outbox = out_dir / f"outbox_{batch}.csv"
    manifest = out_dir / f"outbox_{batch}_manifest.csv"

    if args.dry_run:
        ok_to, summary_to, _msg = _resolve_summary_recipient(args.to)
        summary = summary_to if ok_to else "(missing OSHA_SMOKE_TO)"
        print(f"{PASS_AUTO_DRY_RUN} state={state} batch={batch} prospects={prospects_path} daily_limit={limit}")
        print(f"{PASS_AUTO_DRY_RUN} outbox={outbox}")
        print(f"{PASS_AUTO_DRY_RUN} manifest={manifest}")
        print(f"{PASS_AUTO_DRY_RUN} summary_to={summary}")
        return 0

    ok_to, summary_to, msg = _resolve_summary_recipient(args.to)
    if not ok_to:
        print(msg, file=sys.stderr)
        return 2

    if outbox.exists() and manifest.exists():
        print(f"{PASS_AUTO_ALREADY_RAN} batch={batch} out_dir={out_dir}")
        return 0

    tmp_input = _build_limited_input(prospects_path, state=state, limit=limit)
    try:
        cmd = [
            sys.executable,
            str(REPO_ROOT / "outreach" / "run_outreach_batch.py"),
            "--state",
            state,
            "--batch",
            batch,
            "--input",
            str(tmp_input),
            "--out-root",
            str(out_root),
        ]
        run = _run_cmd(cmd)
        if run.stdout:
            print(run.stdout.strip())
        if run.stderr:
            print(run.stderr.strip(), file=sys.stderr)
        if run.returncode != 0:
            print(f"{ERR_AUTO_BATCH_FAILED} code={run.returncode}", file=sys.stderr)
            return run.returncode
    finally:
        try:
            tmp_input.unlink(missing_ok=True)
        except Exception:
            pass

    kv = _parse_key_value_lines(run.stdout or "")
    exported = int((kv.get("exported_count") or "0").strip() or 0)
    dropped = int((kv.get("dropped_count") or "0").strip() or 0)
    outbox_path = kv.get("outbox_path") or str(outbox)
    manifest_path = kv.get("manifest_path") or str(manifest)
    run_log_path = kv.get("run_log_path") or str(out_dir / "run_log.jsonl")

    print(
        f"{PASS_AUTO_EXPORT} batch={batch} state={state} exported_count={exported} "
        f"dropped_count={dropped} outbox={outbox_path} manifest={manifest_path} run_log={run_log_path}"
    )

    subject = f"[AUTO] Outreach {batch} exported={exported} dropped={dropped}"
    text_body = (
        "Outreach auto-run summary\n"
        f"- state: {state}\n"
        f"- batch: {batch}\n"
        f"- exported_count: {exported}\n"
        f"- dropped_count: {dropped}\n"
        f"- outbox: {outbox_path}\n"
        f"- manifest: {manifest_path}\n"
        f"- run_log: {run_log_path}\n"
    )
    html_body = (
        "<div style=\"font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Arial, sans-serif;\">"
        "<h3>Outreach Auto-Run Summary</h3>"
        f"<p><strong>state:</strong> {state}<br>"
        f"<strong>batch:</strong> {batch}<br>"
        f"<strong>exported_count:</strong> {exported}<br>"
        f"<strong>dropped_count:</strong> {dropped}</p>"
        f"<p><strong>outbox:</strong> {outbox_path}<br>"
        f"<strong>manifest:</strong> {manifest_path}<br>"
        f"<strong>run_log:</strong> {run_log_path}</p>"
        "</div>"
    )
    ok_send, err = _send_summary_email(summary_to, subject, text_body, html_body)
    if not ok_send:
        print(f"{ERR_AUTO_SUMMARY_SEND} {err}", file=sys.stderr)
        return 1

    print(f"{PASS_AUTO_SUMMARY} to={summary_to} batch={batch}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
