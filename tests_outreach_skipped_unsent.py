import csv
import io
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "outreach" / "run_outreach_skipped_unsent.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store
from outreach import run_outreach_skipped_unsent as extra


def _default_signal_db_source() -> Path:
    canonical = Path(r"C:\osha_data\osha.sqlite")
    if canonical.exists():
        return canonical
    backup_root = (REPO_ROOT / "out" / "backups" / "legacy_db_quarantine").resolve()
    candidates = sorted(backup_root.glob("**/legacy_repo_osha.sqlite"), key=lambda item: item.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0]
    raise FileNotFoundError("missing default OSHA signal DB fixture source")


def _write_suppression(path: Path, emails: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["email"])
        writer.writeheader()
        for email in emails or []:
            writer.writerow({"email": email})


def _write_manifest(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "batch",
        "state",
        "prospect_id",
        "email",
        "domain",
        "segment",
        "role_or_title",
        "state_pref",
        "status",
        "reason",
        "rank_reason",
        "rank_tuple",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _seed_crm(path: Path, rows: list[dict]) -> None:
    conn = crm_store.connect(path)
    try:
        crm_store.init_schema(conn)
        cur = conn.cursor()
        for row in rows:
            cur.execute(
                """
                INSERT INTO prospects(
                    prospect_id, firm, contact_name, email, title, city, state, website, source,
                    score, status, created_at, last_contacted_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["prospect_id"],
                    row.get("firm", ""),
                    row.get("contact_name", ""),
                    row["email"],
                    row.get("title", ""),
                    row.get("city", ""),
                    row.get("state", "TX"),
                    row.get("website", ""),
                    row.get("source", "test"),
                    int(row.get("score", 0)),
                    row.get("status", "new"),
                    row.get("created_at", "2026-01-01T00:00:00+00:00"),
                    row.get("last_contacted_at"),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _seed_signal_db(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inspections (
                activity_nr TEXT,
                site_state TEXT,
                date_opened TEXT,
                inspection_type TEXT,
                scope TEXT,
                case_status TEXT,
                establishment_name TEXT,
                site_city TEXT,
                site_zip TEXT,
                naics TEXT,
                naics_desc TEXT,
                violations_count INTEGER,
                emphasis TEXT,
                lead_score INTEGER,
                first_seen_at TEXT,
                last_seen_at TEXT,
                changed_at TEXT,
                source_url TEXT,
                parse_invalid INTEGER
            )
            """
        )
        conn.execute("DELETE FROM inspections")
        for idx, row in enumerate(rows, start=1):
            activity_nr = str(row.get("activity_nr", 1000000 + idx))
            date_opened = str(row.get("date_opened", "2026-03-25"))
            recent_iso = f"{datetime.now(timezone.utc).date().isoformat()}T12:00:00+00:00"
            first_seen_at = str(row.get("first_seen_at", recent_iso))
            last_seen_at = str(row.get("last_seen_at", first_seen_at))
            changed_at = str(row.get("changed_at", first_seen_at))
            conn.execute(
                """
                INSERT INTO inspections(
                    activity_nr,
                    site_state,
                    date_opened,
                    inspection_type,
                    scope,
                    case_status,
                    establishment_name,
                    site_city,
                    site_zip,
                    naics,
                    naics_desc,
                    violations_count,
                    emphasis,
                    lead_score,
                    first_seen_at,
                    last_seen_at,
                    changed_at,
                    source_url,
                    parse_invalid
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    activity_nr,
                    str(row.get("site_state", "")),
                    date_opened,
                    str(row.get("inspection_type", "Complaint")),
                    str(row.get("scope", "Partial")),
                    str(row.get("case_status", "Open")),
                    str(row.get("establishment_name", f"Test Site {idx}")),
                    str(row.get("site_city", "Austin")),
                    str(row.get("site_zip", "73301")),
                    str(row.get("naics", "541620")),
                    str(row.get("naics_desc", "Environmental Consulting Services")),
                    int(row.get("violations_count", 0)),
                    str(row.get("emphasis", "")),
                    int(row.get("lead_score", 11)),
                    first_seen_at,
                    last_seen_at,
                    changed_at,
                    str(row.get("source_url", f"https://example.com/inspection?id={activity_nr}")),
                    int(row.get("parse_invalid", 0)),
                ),
            )
        conn.commit()
    finally:
        conn.close()


class TestOutreachSkippedUnsent(unittest.TestCase):
    _STRIP_ENV_PREFIXES = (
        "MFO_",
        "PROSPECT_AUTOGROW_",
        "PROSPECT_ENRICH_",
        "OUTREACH_",
        "APOLLO_",
        "HUNTER_",
        "AI_TRIAGE_",
        "TRIAL_",
    )
    _STRIP_ENV_KEYS = (
        "DATA_DIR",
        "SIGNAL_FRESHNESS_MAX_DAYS",
        "UNSUB_ENDPOINT_BASE",
        "UNSUB_SECRET",
    )

    def _test_env(self, env_overrides: dict[str, str | None], base_env: dict[str, str] | None = None) -> dict[str, str]:
        env = dict(base_env) if base_env is not None else os.environ.copy()
        for key in list(env.keys()):
            if key in self._STRIP_ENV_KEYS or any(key.startswith(prefix) for prefix in self._STRIP_ENV_PREFIXES):
                env.pop(key, None)
        env["PYTHONPATH"] = str(REPO_ROOT)
        for key, value in env_overrides.items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value
        data_dir_raw = str(env.get("DATA_DIR") or "").strip()
        signal_db_raw = str(env.get("TEST_SIGNAL_DB_PATH") or "").strip()
        if data_dir_raw:
            default_signal_db = Path(data_dir_raw) / "osha.sqlite"
            default_signal_db.parent.mkdir(parents=True, exist_ok=True)
            if signal_db_raw:
                source_path = Path(signal_db_raw)
                if source_path.exists():
                    shutil.copyfile(source_path, default_signal_db)
            elif not default_signal_db.exists():
                shutil.copyfile(_default_signal_db_source(), default_signal_db)
        env.setdefault("CANONICAL_HOSTNAME", socket.gethostname().strip().lower())
        env.setdefault("RUNTIME_ROLE", "dev_client")
        env.setdefault("OUTREACH_STATE_SPREAD_MODE", "single_state")
        return env

    def _run(self, args: list[str], env_overrides: dict[str, str | None]) -> subprocess.CompletedProcess:
        env = self._test_env(env_overrides)
        data_dir_raw = str(env.get("DATA_DIR") or "").strip()
        signal_db_raw = str(env.pop("TEST_SIGNAL_DB_PATH", "") or "").strip()
        if data_dir_raw:
            default_signal_db = Path(data_dir_raw) / "osha.sqlite"
            default_signal_db.parent.mkdir(parents=True, exist_ok=True)
            if signal_db_raw:
                shutil.copyfile(signal_db_raw, default_signal_db)
            elif not default_signal_db.exists():
                shutil.copyfile(_default_signal_db_source(), default_signal_db)
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args],
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )

    def test_dry_run_uses_manifest_only_and_ignores_non_manifest_crm_rows(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            manifest_path = tmp / "skipped_manifest.csv"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_role_tx",
                        "contact_name": "Role TX",
                        "firm": "F",
                        "email": "info@role-tx.com",
                        "title": "Owner",
                        "state": "TX",
                    },
                    {
                        "prospect_id": "p_blocked_ca",
                        "contact_name": "Blocked CA",
                        "firm": "F",
                        "email": "blocked.ca@example.com",
                        "title": "Owner",
                        "state": "CA",
                        "source": "ai_assist_manual",
                    },
                    {
                        "prospect_id": "p_suppressed_tx",
                        "contact_name": "Suppressed TX",
                        "firm": "F",
                        "email": "suppressed@example.com",
                        "title": "Owner",
                        "state": "TX",
                    },
                    {
                        "prospect_id": "p_role_ny",
                        "contact_name": "Role NY",
                        "firm": "F",
                        "email": "info@role-ny.com",
                        "title": "Owner",
                        "state": "NY",
                    },
                    {
                        "prospect_id": "p_not_in_manifest",
                        "contact_name": "Role OH",
                        "firm": "F",
                        "email": "info@role-oh.com",
                        "title": "Owner",
                        "state": "OH",
                    },
                ],
            )
            conn = crm_store.connect(crm_db)
            try:
                conn.execute(
                    """
                    UPDATE prospects
                    SET default_send_eligible = CASE prospect_id
                        WHEN 'p_blocked_ca' THEN 0
                        ELSE 1
                    END,
                    source_fit_tier = 'recoverable_consultant'
                    """
                )
                conn.commit()
            finally:
                conn.close()

            _write_manifest(
                manifest_path,
                [
                    {
                        "batch": "2026-03-24_TX",
                        "state": "TX",
                        "prospect_id": "p_role_tx",
                        "email": "info@role-tx.com",
                        "status": "dropped",
                        "reason": "role_inbox_email",
                    },
                    {
                        "batch": "2026-03-24_CA",
                        "state": "CA",
                        "prospect_id": "p_blocked_ca",
                        "email": "blocked.ca@example.com",
                        "status": "dropped",
                        "reason": "not_default_send_eligible",
                    },
                    {
                        "batch": "2026-03-24_TX",
                        "state": "TX",
                        "prospect_id": "p_suppressed_tx",
                        "email": "suppressed@example.com",
                        "status": "dropped",
                        "reason": "role_inbox_email",
                    },
                    {
                        "batch": "2026-03-24_NY",
                        "state": "NY",
                        "prospect_id": "p_role_ny",
                        "email": "info@role-ny.com",
                        "status": "dropped",
                        "reason": "role_inbox_email",
                    },
                ],
            )
            _write_suppression(data_dir / "suppression.csv", emails=["suppressed@example.com"])

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX,CA,FL,PA,OH",
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            proc = self._run(
                [
                    "--dry-run",
                    "--for-date",
                    "2026-03-25",
                    "--manifest",
                    str(manifest_path),
                    "--states",
                    "TX,CA",
                    "--limit",
                    "10",
                ],
                env,
            )
            self.assertEqual(proc.returncode, 0, msg=(proc.stderr or "") + "\n" + (proc.stdout or ""))
            out = proc.stdout or ""
            self.assertIn("PASS_SKIPPED_EXTRA_DRY_RUN", out)
            self.assertIn("would_contact_prospect_ids=p_role_tx,p_blocked_ca", out)
            self.assertIn("selected_by_state=CA:1,TX:1", out)
            self.assertIn("selected_by_reason=not_default_send_eligible:1,role_inbox_email:1", out)
            self.assertIn("skipped_by_reason=state_scope_excluded:1,suppressed_compliance:1", out)

            manifest_line = next((line for line in out.splitlines() if "manifest_path=" in line), "")
            self.assertTrue(manifest_line, msg=out)
            output_manifest_path = Path(manifest_line.split("manifest_path=", 1)[1].strip())
            self.assertTrue(output_manifest_path.exists(), msg=f"missing manifest: {output_manifest_path}")
            with open(output_manifest_path, "r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            selected_rows = [row for row in rows if (row.get("status") or "") == "selected"]
            self.assertEqual([row.get("prospect_id") for row in selected_rows], ["p_role_tx", "p_blocked_ca"])
            dropped = {(row.get("prospect_id") or ""): (row.get("reason") or "") for row in rows if (row.get("status") or "") == "dropped"}
            self.assertEqual(dropped.get("p_suppressed_tx"), "suppressed_compliance")
            self.assertEqual(dropped.get("p_role_ny"), "state_scope_excluded")
            self.assertNotIn("p_not_in_manifest", {row.get("prospect_id") for row in rows})

            conn = sqlite3.connect(str(crm_db))
            try:
                count = int(conn.execute("SELECT COUNT(*) FROM outreach_events").fetchone()[0])
                self.assertEqual(count, 0)
            finally:
                conn.close()

    def test_print_config_emits_target_counts(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            manifest_path = tmp / "skipped_manifest.csv"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_role",
                        "contact_name": "Role",
                        "firm": "F",
                        "email": "info@role.com",
                        "title": "Owner",
                        "state": "TX",
                    }
                ],
            )
            _write_manifest(
                manifest_path,
                [
                    {
                        "batch": "2026-03-24_TX",
                        "state": "TX",
                        "prospect_id": "p_role",
                        "email": "info@role.com",
                        "status": "dropped",
                        "reason": "role_inbox_email",
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            proc = self._run(
                [
                    "--print-config",
                    "--for-date",
                    "2026-03-25",
                    "--manifest",
                    str(manifest_path),
                    "--states",
                    "TX",
                    "--limit",
                    "5",
                ],
                env,
            )
            self.assertEqual(proc.returncode, 0, msg=(proc.stderr or "") + "\n" + (proc.stdout or ""))
            out = proc.stdout or ""
            self.assertIn("PASS_SKIPPED_EXTRA_PRINT_CONFIG", out)
            self.assertIn(f"manifest_path={manifest_path.resolve()}", out)
            self.assertIn("requested_states=TX", out)
            self.assertIn("requested_limit=5", out)
            self.assertIn("sendable_extra_count=1", out)
            self.assertIn("selected_by_state=TX:1", out)
            self.assertIn("selected_by_reason=role_inbox_email:1", out)
            self.assertIn("no_signal_states=(none)", out)

    def test_live_same_day_guard_blocks_without_override(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            manifest_path = tmp / "skipped_manifest.csv"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_role",
                        "contact_name": "Role",
                        "firm": "F",
                        "email": "info@role.com",
                        "title": "Owner",
                        "state": "TX",
                    }
                ],
            )
            _write_manifest(
                manifest_path,
                [
                    {
                        "batch": "2026-03-24_TX",
                        "state": "TX",
                        "prospect_id": "p_role",
                        "email": "info@role.com",
                        "status": "dropped",
                        "reason": "role_inbox_email",
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            weekday_now = {
                "timezone": "America/New_York",
                "datetime": datetime(2026, 3, 25, 9, 0, 0),
                "date": date(2026, 3, 25),
                "date_text": "2026-03-25",
                "weekday_idx": 2,
                "weekday_name": "wed",
                "is_weekend": False,
            }
            with mock.patch.dict(os.environ, self._test_env(env), clear=True):
                with mock.patch.object(extra.roa, "_outreach_local_now", return_value=weekday_now), mock.patch.object(
                    extra.roa, "_sent_batches_for_day", return_value=["2026-03-25_TX"]
                ), mock.patch.object(
                    extra.roa, "run_runtime_preflight", return_value=mock.Mock(ok=True)
                ), mock.patch.object(
                    extra.roa, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]
                ):
                    with mock.patch.object(
                        sys,
                        "argv",
                        [
                            "run_outreach_skipped_unsent.py",
                            "--manifest",
                            str(manifest_path),
                            "--states",
                            "TX",
                            "--limit",
                            "5",
                            "--confirm-live-send",
                        ],
                    ):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = extra.main()
            self.assertEqual(rc, 0, msg=err.getvalue() + "\n" + out.getvalue())
            self.assertIn("OUTREACH_SKIPPED_EXTRA_SKIP_ALREADY_SENT_TODAY=1", out.getvalue())

    def test_dry_run_blocks_when_renderable_signals_empty(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            manifest_path = tmp / "skipped_manifest.csv"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_role",
                        "contact_name": "Role",
                        "firm": "F",
                        "email": "info@role.com",
                        "title": "Owner",
                        "state": "TX",
                    }
                ],
            )
            _write_manifest(
                manifest_path,
                [
                    {
                        "batch": "2026-03-24_TX",
                        "state": "TX",
                        "prospect_id": "p_role",
                        "email": "info@role.com",
                        "status": "dropped",
                        "reason": "role_inbox_email",
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            signal_ctx = {
                "recent_leads_original": [],
                "recent_leads": [],
                "last_refresh_et": "2026-03-25 08:00 ET",
                "signal_tokens": {"RECENT_SIGNALS_LINES": "", "RECENT_SIGNALS_HTML": ""},
                "triage_ctx": {},
                "raw_signal_count": 1,
                "recent_signal_source_count": 0,
                "renderable_signal_count": 0,
                "signal_fetch_status": "recent_leads_query_failed",
                "signal_fetch_error_token": "ERR_OUTREACH_SIGNAL_FETCH_FAILED",
                "signal_fetch_detail": "OperationalError",
                "signal_window_days": 14,
            }

            with mock.patch.dict(os.environ, self._test_env(env), clear=True):
                with mock.patch.object(extra.roa, "_prepare_signal_content_with_triage", return_value=dict(signal_ctx)):
                    with mock.patch.object(
                        sys,
                        "argv",
                        [
                            "run_outreach_skipped_unsent.py",
                            "--dry-run",
                            "--for-date",
                            "2026-03-25",
                            "--manifest",
                            str(manifest_path),
                            "--states",
                            "TX",
                            "--limit",
                            "5",
                        ],
                    ):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = extra.main()

            self.assertEqual(rc, 0, msg=err.getvalue() + "\n" + out.getvalue())
            text = out.getvalue()
            self.assertIn(
                "OUTREACH_SKIPPED_EXTRA_SKIP_NO_SIGNALS state=TX window_days=14 raw_signal_count=1 recent_signal_source_count=0 renderable_signal_count=0 signal_fetch_status=recent_leads_query_failed",
                text,
            )
            self.assertIn(
                "ERR_OUTREACH_SIGNAL_FETCH_FAILED state=TX status=recent_leads_query_failed detail=OperationalError",
                text,
            )
            self.assertIn("would_contact_prospect_ids=(none)", text)

    def test_live_requires_manifest_states_limit_and_confirm(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            manifest_path = tmp / "skipped_manifest.csv"
            _write_manifest(
                manifest_path,
                [
                    {
                        "batch": "2026-03-24_TX",
                        "state": "TX",
                        "prospect_id": "p_role",
                        "email": "info@role.com",
                        "status": "dropped",
                        "reason": "role_inbox_email",
                    }
                ],
            )

            cases = [
                (["run_outreach_skipped_unsent.py"], extra.ERR_SKIPPED_EXTRA_MANIFEST_REQUIRED),
                (["run_outreach_skipped_unsent.py", "--manifest", str(manifest_path)], extra.ERR_SKIPPED_EXTRA_STATES_REQUIRED),
                (
                    ["run_outreach_skipped_unsent.py", "--manifest", str(manifest_path), "--states", "TX"],
                    extra.ERR_SKIPPED_EXTRA_LIMIT_REQUIRED,
                ),
                (
                    [
                        "run_outreach_skipped_unsent.py",
                        "--manifest",
                        str(manifest_path),
                        "--states",
                        "TX",
                        "--limit",
                        "5",
                    ],
                    extra.ERR_SKIPPED_EXTRA_CONFIRM_REQUIRED,
                ),
            ]

            for argv, expected_token in cases:
                with self.subTest(expected_token=expected_token):
                    with mock.patch.object(sys, "argv", argv):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = extra.main()
                    self.assertEqual(rc, 2, msg=err.getvalue() + "\n" + out.getvalue())
                    self.assertIn(expected_token, err.getvalue())


if __name__ == "__main__":
    unittest.main()
