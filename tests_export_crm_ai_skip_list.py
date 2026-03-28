import csv
import io
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from outreach import crm_store
from tools import export_crm_ai_skip_list as export_tool


class TestExportCrmAiSkipList(unittest.TestCase):
    def _seed_prospect(
        self,
        db_path: Path,
        *,
        prospect_id: str,
        firm: str,
        email: str,
        website: str,
        state: str,
        city: str,
        status: str,
        source: str,
        created_at: str,
    ) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = crm_store.connect(db_path)
        try:
            crm_store.init_schema(conn)
            conn.execute(
                """
                INSERT INTO prospects(
                    prospect_id, firm, contact_name, email, title, city, state, website, source,
                    source_fit_tier, default_send_eligible, email_status, enrichment_lane,
                    score, status, created_at
                ) VALUES (?, ?, '', ?, '', ?, ?, ?, ?, 'recoverable_consultant', 1, '', '', 0, ?, ?)
                """,
                (
                    prospect_id,
                    firm,
                    email,
                    city,
                    state,
                    website,
                    source,
                    status,
                    created_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def test_print_config_uses_default_ai_assist_audit_path(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = export_tool.main(["--print-config"])

            self.assertEqual(rc, 0)
            text = out.getvalue()
            self.assertIn("PASS_CRM_AI_SKIP_EXPORT_PRINT_CONFIG", text)
            self.assertIn(f"crm_db={(data_dir / 'crm.sqlite').resolve()}", text)
            self.assertIn(
                f"output_path={(data_dir / 'audits' / 'prospect_ai_assist' / export_tool.OUTPUT_FILENAME).resolve()}",
                text,
            )

    def test_export_groups_existing_crm_rows_by_root_domain_or_firm(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            output_path = tmp / "exports" / export_tool.OUTPUT_FILENAME
            self._seed_prospect(
                db_path,
                prospect_id="acme_1",
                firm="Acme Safety LLC",
                email="info@acme-safety.com",
                website="https://www.acme-safety.com/about",
                state="TX",
                city="Dallas",
                status="new",
                source="AIHA",
                created_at="2026-03-10T08:00:00+00:00",
            )
            self._seed_prospect(
                db_path,
                prospect_id="acme_2",
                firm="Acme Safety",
                email="owner@contact.acme-safety.com",
                website="https://contact.acme-safety.com",
                state="TX",
                city="Fort Worth",
                status="replied",
                source="manual_user_supplied",
                created_at="2026-03-11T09:00:00+00:00",
            )
            self._seed_prospect(
                db_path,
                prospect_id="bravo_1",
                firm="Bravo EHS Partners",
                email="hello@bravoehs.test",
                website="",
                state="CA",
                city="Oakland",
                status="do_not_contact",
                source="BLUEBOOK",
                created_at="2026-03-09T07:00:00+00:00",
            )

            stdout = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(stdout):
                rc = export_tool.main(["--output", str(output_path)])

            self.assertEqual(rc, 0)
            self.assertTrue(output_path.exists(), msg="expected skip-list export CSV")
            self.assertIn("PASS_CRM_AI_SKIP_EXPORT", stdout.getvalue())

            with output_path.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["firm"], "Acme Safety LLC")
            self.assertEqual(rows[0]["firm_key"], "ACMESAFETY")
            self.assertEqual(rows[0]["root_domain"], "acme-safety.com")
            self.assertEqual(rows[0]["website"], "https://contact.acme-safety.com")
            self.assertEqual(rows[0]["states"], "TX")
            self.assertEqual(rows[0]["cities"], "Dallas|Fort Worth")
            self.assertEqual(rows[0]["crm_statuses"], "new|replied")
            self.assertEqual(rows[0]["crm_sources"], "AIHA|manual_user_supplied")
            self.assertEqual(rows[0]["contact_email_samples"], "info@acme-safety.com|owner@contact.acme-safety.com")
            self.assertEqual(rows[0]["crm_record_count"], "2")
            self.assertEqual(rows[0]["first_created_at"], "2026-03-10T08:00:00+00:00")
            self.assertEqual(rows[0]["last_created_at"], "2026-03-11T09:00:00+00:00")

            self.assertEqual(rows[1]["firm"], "Bravo EHS Partners")
            self.assertEqual(rows[1]["root_domain"], "bravoehs.test")
            self.assertEqual(rows[1]["crm_statuses"], "do_not_contact")
            self.assertEqual(rows[1]["crm_record_count"], "1")

    def test_dry_run_does_not_write_output(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            output_path = tmp / "exports" / export_tool.OUTPUT_FILENAME
            self._seed_prospect(
                db_path,
                prospect_id="dry_1",
                firm="Dry Run Safety",
                email="team@dry-run-safety.test",
                website="https://dry-run-safety.test",
                state="FL",
                city="Miami",
                status="new",
                source="AIHA",
                created_at="2026-03-12T10:00:00+00:00",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with (
                mock.patch.dict(os.environ, env, clear=False),
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                rc = export_tool.main(["--output", str(output_path), "--dry-run"])

            self.assertEqual(rc, 0, msg=stderr.getvalue())
            self.assertFalse(output_path.exists(), msg="dry-run should not write skip-list CSV")
            self.assertIn("PASS_CRM_AI_SKIP_EXPORT_DRY_RUN", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
