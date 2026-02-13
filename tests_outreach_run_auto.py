import csv
import io
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "outreach" / "run_outreach_auto.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store
from outreach import run_outreach_auto as roa


def _write_suppression(path: Path, emails: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["email"])
        w.writeheader()
        for email in emails or []:
            w.writerow({"email": email})


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


class TestOutreachRunAuto(unittest.TestCase):
    def _run(self, args: list[str], env_overrides: dict[str, str | None]) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        for k, v in env_overrides.items():
            if v is None:
                env.pop(k, None)
            else:
                env[k] = v
        return subprocess.run(
            [sys.executable, str(SCRIPT)] + args,
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )

    def test_dry_run_prints_selected_ids_and_writes_no_db_changes(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_new",
                        "contact_name": "Alice New",
                        "firm": "ACME",
                        "email": "alice@example.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 2,
                    },
                    {
                        "prospect_id": "p_old",
                        "contact_name": "Bob Old",
                        "firm": "ACME",
                        "email": "bob@example.com",
                        "title": "Safety Manager",
                        "state": "TX",
                        "score": 2,
                        "status": "contacted",
                        "last_contacted_at": "2026-01-05T00:00:00+00:00",
                    },
                    {
                        "prospect_id": "p_sup",
                        "contact_name": "Cara Sup",
                        "firm": "ACME",
                        "email": "suppressed@example.com",
                        "title": "Founder",
                        "state": "TX",
                        "score": 1,
                    },
                ],
            )
            _write_suppression(data_dir / "suppression.csv", emails=["suppressed@example.com"])

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
                "UNSUB_ENDPOINT_BASE": None,
                "UNSUB_SECRET": None,
            }
            p = self._run(["--dry-run"], env)
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("PASS_AUTO_DRY_RUN", out)
            self.assertIn("would_contact_prospect_ids=p_new", out)
            self.assertIn("skipped_count=2", out)

            conn = sqlite3.connect(str(crm_db))
            try:
                cnt = int(conn.execute("SELECT COUNT(*) FROM outreach_events").fetchone()[0])
                self.assertEqual(cnt, 0)
                last_contacted = conn.execute(
                    "SELECT COALESCE(last_contacted_at, '') FROM prospects WHERE prospect_id = 'p_new'"
                ).fetchone()[0]
                self.assertEqual(last_contacted, "")
            finally:
                conn.close()

    def test_no_repeat_gate_and_allow_repeat_override(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p1",
                        "contact_name": "A",
                        "firm": "F",
                        "email": "a@example.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 1,
                        "status": "contacted",
                        "last_contacted_at": "2026-01-05T00:00:00+00:00",
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
            }

            p1 = self._run(["--dry-run"], env)
            self.assertEqual(p1.returncode, 0, msg=p1.stderr + "\n" + p1.stdout)
            self.assertIn("would_contact_prospect_ids=(none)", p1.stdout)

            p2 = self._run(["--dry-run", "--allow-repeat"], env)
            self.assertEqual(p2.returncode, 0, msg=p2.stderr + "\n" + p2.stdout)
            self.assertIn("would_contact_prospect_ids=p1", p2.stdout)

    def test_to_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p1",
                        "contact_name": "A",
                        "firm": "F",
                        "email": "a@example.com",
                        "title": "Owner",
                        "state": "TX",
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            p = self._run(["--to", "wrong@example.com"], env)
            self.assertNotEqual(p.returncode, 0)
            self.assertIn("ERR_AUTO_SUMMARY_TO_MISMATCH", (p.stderr or "") + (p.stdout or ""))

    def test_print_config_outputs_resolved_fields(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX,CA",
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            p = self._run(["--print-config"], env)
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            out = p.stdout or ""
            self.assertIn("PASS_AUTO_PRINT_CONFIG", out)
            self.assertIn(f"data_dir={data_dir.resolve()}", out)
            self.assertIn(f"crm_db={(data_dir / 'crm.sqlite').resolve()}", out)
            self.assertIn(f"suppression_csv={(data_dir / 'suppression.csv').resolve()}", out)
            self.assertIn("outreach_daily_limit=200 source=default", out)
            self.assertIn("outreach_states=TX,CA", out)
            self.assertIn("selected_state=", out)
            self.assertIn("batch_id=", out)
            self.assertIn("trial_conversion_url_present=NO", out)

    def test_print_config_outputs_limit_source_env_and_trial_conversion_present(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX,CA",
                "OUTREACH_DAILY_LIMIT": "17",
                "OSHA_SMOKE_TO": "allow@example.com",
                "TRIAL_CONVERSION_URL": "https://buy.stripe.com/test123",
            }
            p = self._run(["--print-config"], env)
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            out = p.stdout or ""
            self.assertIn("outreach_daily_limit=17 source=env", out)
            self.assertIn("trial_conversion_url_present=YES", out)

    def test_doctor_missing_env_returns_single_err_line(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p1",
                        "contact_name": "A",
                        "firm": "F",
                        "email": "a@example.com",
                        "title": "Owner",
                        "state": "TX",
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": None,
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
                "OUTREACH_SUPPRESSION_MAX_AGE_HOURS": "240",
            }
            with mock.patch.dict(os.environ, {}, clear=False):
                for key, value in env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

                with mock.patch.object(roa, "_doctor_check_secrets_decrypt", return_value=(True, "")):
                    with mock.patch.object(sys, "argv", ["run_outreach_auto.py", "--doctor"]):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = roa.main()

            self.assertEqual(rc, 2)
            err_lines = [ln.strip() for ln in (err.getvalue() or "").splitlines() if ln.strip()]
            self.assertEqual(len(err_lines), 1, msg=err.getvalue())
            self.assertTrue(err_lines[0].startswith("ERR_DOCTOR_ENV_MISSING_"), msg=err_lines[0])

    def test_doctor_success_pass_tokens_only_and_no_db_mutation(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_new",
                        "contact_name": "Alice New",
                        "firm": "ACME",
                        "email": "alice@example.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 2,
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            conn = sqlite3.connect(str(crm_db))
            try:
                before_events = int(conn.execute("SELECT COUNT(*) FROM outreach_events").fetchone()[0])
                before_last_contacted = conn.execute(
                    "SELECT COALESCE(last_contacted_at, '') FROM prospects WHERE prospect_id = 'p_new'"
                ).fetchone()[0]
            finally:
                conn.close()

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
                "OUTREACH_SUPPRESSION_MAX_AGE_HOURS": "240",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(roa, "_doctor_context_pack_soft_check") as m_context, mock.patch.object(
                    roa, "_doctor_check_secrets_decrypt"
                ) as m_secrets, mock.patch.object(roa, "_doctor_check_unsub") as m_unsub, mock.patch.object(
                    roa, "_doctor_check_provider"
                ) as m_provider, mock.patch.object(roa, "_doctor_check_dry_run_artifact") as m_dry_run:
                    m_context.side_effect = lambda: None
                    m_secrets.side_effect = lambda: (print("PASS_DOCTOR_SECRETS_DECRYPT diagnostics=ok"), (True, ""))[1]
                    m_unsub.side_effect = lambda: (print("PASS_DOCTOR_UNSUB version_status=200 unsubscribe_status=400"), (True, ""))[1]
                    m_provider.side_effect = lambda: (print("PASS_DOCTOR_PROVIDER_CONFIG smtp_port=465"), (True, ""))[1]
                    m_dry_run.side_effect = lambda allow_repeat=False: (
                        print("PASS_DOCTOR_DRY_RUN_ARTIFACT dry_run_token=PASS_AUTO_DRY_RUN"),
                        (True, ""),
                    )[1]

                    with mock.patch.object(sys, "argv", ["run_outreach_auto.py", "--doctor"]):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = roa.main()

            self.assertEqual(rc, 0, msg=err.getvalue() + "\n" + out.getvalue())
            self.assertEqual((err.getvalue() or "").strip(), "")
            out_lines = [ln.strip() for ln in (out.getvalue() or "").splitlines() if ln.strip()]
            self.assertGreater(len(out_lines), 0)
            for line in out_lines:
                self.assertTrue(line.startswith("PASS_DOCTOR_"), msg=line)
            self.assertTrue(any(line.startswith("PASS_DOCTOR_COMPLETE") for line in out_lines))

            conn = sqlite3.connect(str(crm_db))
            try:
                after_events = int(conn.execute("SELECT COUNT(*) FROM outreach_events").fetchone()[0])
                after_last_contacted = conn.execute(
                    "SELECT COALESCE(last_contacted_at, '') FROM prospects WHERE prospect_id = 'p_new'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(before_events, after_events)
            self.assertEqual(before_last_contacted, after_last_contacted)

    def test_doctor_context_pack_warn_lines_do_not_fail(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_new",
                        "contact_name": "Alice New",
                        "firm": "ACME",
                        "email": "alice@example.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 2,
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
                "OUTREACH_SUPPRESSION_MAX_AGE_HOURS": "240",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(roa, "_doctor_context_pack_soft_check") as m_context, mock.patch.object(
                    roa, "_doctor_check_secrets_decrypt"
                ) as m_secrets, mock.patch.object(roa, "_doctor_check_unsub") as m_unsub, mock.patch.object(
                    roa, "_doctor_check_provider"
                ) as m_provider, mock.patch.object(roa, "_doctor_check_dry_run_artifact") as m_dry_run:

                    def _fake_context_warn() -> None:
                        print("WARN_CONTEXT_PACK_STALE SOURCE_HASHES mismatch")
                        print("Upload PROJECT_CONTEXT_PACK.md to ChatGPT Project Settings -> Files")
                        print("Then run: py -3 tools/project_context_pack.py --mark-uploaded")

                    m_context.side_effect = _fake_context_warn
                    m_secrets.side_effect = lambda: (print("PASS_DOCTOR_SECRETS_DECRYPT diagnostics=ok"), (True, ""))[1]
                    m_unsub.side_effect = lambda: (print("PASS_DOCTOR_UNSUB version_status=200 unsubscribe_status=400"), (True, ""))[1]
                    m_provider.side_effect = lambda: (print("PASS_DOCTOR_PROVIDER_CONFIG smtp_port=465"), (True, ""))[1]
                    m_dry_run.side_effect = lambda allow_repeat=False: (
                        print("PASS_DOCTOR_DRY_RUN_ARTIFACT dry_run_token=PASS_AUTO_DRY_RUN"),
                        (True, ""),
                    )[1]

                    with mock.patch.object(sys, "argv", ["run_outreach_auto.py", "--doctor"]):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = roa.main()

            self.assertEqual(rc, 0, msg=err.getvalue() + "\n" + out.getvalue())
            self.assertEqual((err.getvalue() or "").strip(), "")
            text = out.getvalue()
            self.assertIn("WARN_CONTEXT_PACK_STALE", text)
            self.assertIn("Upload PROJECT_CONTEXT_PACK.md to ChatGPT Project Settings -> Files", text)
            self.assertIn("PASS_DOCTOR_COMPLETE", text)


if __name__ == "__main__":
    unittest.main()
