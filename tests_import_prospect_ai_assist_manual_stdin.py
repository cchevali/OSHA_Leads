import io
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest import mock

from outreach import crm_store
from tools import import_prospect_ai_assist_review as import_tool


class TestImportProspectAiAssistManualStdin(unittest.TestCase):
    def _seed_crm_prospect(self, db_path: Path, *, firm: str, website: str, state: str = "TX", email: str = "") -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        effective_email = email or f"{firm.replace(' ', '').lower()}@seed-mail.test"
        conn = crm_store.connect(db_path)
        try:
            crm_store.init_schema(conn)
            conn.execute(
                """
                INSERT INTO prospects(
                    prospect_id, firm, contact_name, email, title, city, state, website, source,
                    source_fit_tier, default_send_eligible, email_status, enrichment_lane,
                    score, status, created_at
                ) VALUES (?, ?, '', ?, '', '', ?, ?, 'seed', 'recoverable_consultant', 1, '', '', 0, 'new', ?)
                """,
                (
                    f"seed_{firm}_{state}".replace(" ", "_").lower(),
                    firm,
                    effective_email,
                    state,
                    website,
                    "2026-03-06T00:00:00+00:00",
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _run_main(
        self,
        argv: list[str],
        *,
        stdin_text: str = "",
        env: dict[str, str] | None = None,
    ) -> tuple[int, str, str]:
        out = io.StringIO()
        err = io.StringIO()
        with (
            mock.patch.dict(os.environ, env or dict(os.environ), clear=False),
            mock.patch.object(import_tool.sys, "stdin", io.StringIO(stdin_text)),
            redirect_stdout(out),
            redirect_stderr(err),
        ):
            rc = import_tool.main(argv)
        return rc, out.getvalue(), err.getvalue()

    def test_print_config_uses_manual_batch_id_for_stdin(self):
        env = dict(os.environ)
        with mock.patch.object(
            import_tool,
            "_local_now",
            return_value=datetime.fromisoformat("2026-03-23T09:15:00-04:00"),
        ):
            rc, out, err = self._run_main(["--stdin", "--print-config"], env=env)
        self.assertEqual(rc, 0, msg=out + err)
        self.assertIn("AI_ASSIST_BATCH_ID=2026-03-23_AIASSIST_MANUAL_091500", out)
        self.assertIn("AI_ASSIST_IMPORT_INPUT_MODE=stdin", out)
        self.assertIn("AI_ASSIST_IMPORT_INPUT_PATH=<stdin>", out)

    def test_import_stdin_accepts_plain_csv_in_dry_run(self):
        csv_text = "\n".join(
            [
                "state,decision,firm,website,contact_name,title,email,source_urls,confidence,evidence_snippet",
                "PA,accept,Keystone Safety,https://keystone.example,Pat Keystone,Owner,pat@keystone.example,https://keystone.example/about,92,Owner listed on site",
            ]
        )
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX,CA,FL,PA,OH,IL,NJ,LA,MI,GA,AL,WI,TN"
            rc, out, err = self._run_main(
                ["--stdin", "--dry-run", "--batch", "2026-03-23_AIASSIST_MANUAL_091500"],
                stdin_text=csv_text,
                env=env,
            )
        self.assertEqual(rc, 0, msg=out + err)
        self.assertIn("AI_ASSIST_IMPORT_INPUT_MODE=stdin", out)
        self.assertIn("AI_ASSIST_IMPORT_STDIN_BYTES=", out)
        self.assertIn("AI_ASSIST_ACCEPTED_TOTAL=1", out)
        self.assertIn("PASS_AI_ASSIST_IMPORT status=DRY_RUN", out)

    def test_import_stdin_accepts_fenced_csv_block(self):
        csv_text = "\n".join(
            [
                "```csv",
                "state,decision,firm,website,contact_name,title,email,source_urls,confidence,evidence_snippet",
                "OH,accept,Buckeye Safety,https://buckeye.example,Bailey Buckeye,Principal,bailey@buckeye.example,https://buckeye.example/team,88,Principal listed on team page",
                "```",
            ]
        )
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX,CA,FL,PA,OH,IL,NJ,LA,MI,GA,AL,WI,TN"
            rc, out, err = self._run_main(
                ["--stdin", "--dry-run", "--batch", "2026-03-23_AIASSIST_MANUAL_091501"],
                stdin_text=csv_text,
                env=env,
            )
        self.assertEqual(rc, 0, msg=out + err)
        self.assertIn("AI_ASSIST_ACCEPTED_TOTAL=1", out)
        self.assertIn("PASS_AI_ASSIST_IMPORT status=DRY_RUN", out)

    def test_file_import_accepts_fenced_csv_block_for_clipboard_style_payloads(self):
        csv_text = "\n".join(
            [
                "```csv",
                "state,decision,firm,website,contact_name,title,email,source_urls,confidence,evidence_snippet",
                "PA,accept,Allegheny Safety,https://allegheny.example,Alex Allegheny,Principal,alex@allegheny.example,https://allegheny.example/team,87,Principal listed on team page",
                "```",
            ]
        )
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            input_path = Path(d) / "clipboard_payload.csv"
            input_path.write_text(csv_text, encoding="utf-8")
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX,CA,FL,PA,OH,IL,NJ,LA,MI,GA,AL,WI,TN"
            rc, out, err = self._run_main(
                ["--input", str(input_path), "--dry-run", "--batch", "2026-03-23_AIASSIST_MANUAL_091501"],
                env=env,
            )
        self.assertEqual(rc, 0, msg=out + err)
        self.assertIn("AI_ASSIST_IMPORT_INPUT_MODE=file", out)
        self.assertIn("AI_ASSIST_ACCEPTED_TOTAL=1", out)
        self.assertIn("PASS_AI_ASSIST_IMPORT status=DRY_RUN", out)

    def test_import_stdin_rejects_extra_commentary(self):
        stdin_text = "\n".join(
            [
                "Here are the results:",
                "state,decision,firm,website,contact_name,title,email,source_urls,confidence,evidence_snippet",
                "TX,accept,Example Safety,https://example.test,Ava Example,Owner,ava@example.test,https://example.test/about,90,Owner listed on site",
            ]
        )
        rc, out, err = self._run_main(["--stdin", "--batch", "2026-03-23_AIASSIST_MANUAL_091502"], stdin_text=stdin_text)
        self.assertEqual(rc, 2, msg=out + err)
        self.assertIn("ERR_AI_ASSIST_IMPORT_INPUT detail=stdin_commentary_detected", err)

    def test_import_rejects_out_of_scope_stdin_state(self):
        stdin_text = "\n".join(
            [
                "state,decision,firm,website,contact_name,title,email,source_urls,confidence,evidence_snippet",
                "OH,accept,Out of Scope Safety,https://scope.test,Olive Scope,Owner,olive@scope.test,https://scope.test/about,90,Owner listed on site",
            ]
        )
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX,CA"
            rc, out, err = self._run_main(
                ["--stdin", "--batch", "2026-03-23_AIASSIST_MANUAL_091503"],
                stdin_text=stdin_text,
                env=env,
            )
            self.assertEqual(rc, 0, msg=out + err)
            conn = crm_store.connect(data_dir / "crm.sqlite")
            try:
                row = conn.execute(
                    f"""
                    SELECT verification_status, rejection_reason
                    FROM {crm_store.AI_ASSIST_CANDIDATE_TABLE}
                    WHERE batch_id = '2026-03-23_AIASSIST_MANUAL_091503'
                    """
                ).fetchone()
            finally:
                conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(str(row[0] or ""), "rejected_by_verification")
        self.assertEqual(str(row[1] or ""), "state_out_of_scope")

    def test_import_manual_text_accepts_each_added_state(self):
        blocks = [
            ("PA", "Penn Safety Group", "pat@penn.example"),
            ("OH", "Buckeye Safety Group", "bailey@buckeye.example"),
            ("IL", "Prairie Safety Group", "ivy@prairie.example"),
            ("NJ", "Garden Safety Group", "jules@garden.example"),
            ("LA", "Pelican Safety Group", "lane@pelican.example"),
            ("MI", "Lakes Safety Group", "mika@lakes.example"),
            ("GA", "Peach Safety Group", "gale@peach.example"),
            ("AL", "Rocket Safety Group", "aria@rocket.example"),
            ("WI", "Badger Safety Group", "wren@badger.example"),
            ("TN", "Volunteer Safety Group", "tess@volunteer.example"),
        ]
        stdin_text = "\n\n".join(
            [
                "\n".join(
                    [
                        f"Company: {firm}",
                        f"Email: {email.upper()}",
                        f"Website: www.{email.split('@', 1)[1]}",
                        f"State: {state.lower()}",
                        "Notes: owner listed on website",
                    ]
                )
                for state, firm, email in blocks
            ]
        )
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX,CA,FL,PA,OH,IL,NJ,LA,MI,GA,AL,WI,TN"
            rc, out, err = self._run_main(
                ["--stdin", "--dry-run", "--batch", "2026-03-23_AIASSIST_MANUAL_091504"],
                stdin_text=stdin_text,
                env=env,
            )
        self.assertEqual(rc, 0, msg=out + err)
        self.assertIn("AI_ASSIST_IMPORT_PARSE_MODE=manual_text", out)
        self.assertIn("AI_ASSIST_ACCEPTED_TOTAL=10", out)
        self.assertIn("AI_ASSIST_VERIFIED_TOTAL=10", out)
        self.assertIn("AI_ASSIST_CREATED_NEW_TOTAL=10", out)
        for state, _firm, _email in blocks:
            self.assertIn(f"AI_ASSIST_ACCEPTED_TOTAL_STATE_{state}=1", out)
            self.assertIn(f"AI_ASSIST_VERIFIED_TOTAL_STATE_{state}=1", out)
        self.assertIn("PASS_AI_ASSIST_IMPORT status=DRY_RUN", out)

    def test_import_manual_text_updates_existing_contact_without_duplicate(self):
        stdin_text = "\n".join(
            [
                "Company: Keystone Safety, LLC",
                "Contact: Pat Keystone",
                "Email: PAT@KEYSTONE.EXAMPLE",
                "Website: keystone.example",
                "State: pennsylvania",
                "Title: Owner",
                "Notes: manually supplied",
            ]
        )
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            db_path = data_dir / "crm.sqlite"
            self._seed_crm_prospect(
                db_path,
                firm="Keystone Safety LLC",
                website="",
                state="",
                email="pat@keystone.example",
            )
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX,CA,FL,PA,OH,IL,NJ,LA,MI,GA,AL,WI,TN"
            rc, out, err = self._run_main(
                ["--stdin", "--batch", "2026-03-23_AIASSIST_MANUAL_091505"],
                stdin_text=stdin_text,
                env=env,
            )
            self.assertEqual(rc, 0, msg=out + err)
            conn = crm_store.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT prospect_id, firm, contact_name, email, title, state, website
                    FROM prospects
                    WHERE email = 'pat@keystone.example'
                    """
                ).fetchall()
                audit_row = conn.execute(
                    f"""
                    SELECT verification_status, rejection_reason
                    FROM {crm_store.AI_ASSIST_CANDIDATE_TABLE}
                    WHERE batch_id = '2026-03-23_AIASSIST_MANUAL_091505'
                    """
                ).fetchone()
            finally:
                conn.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0][1] or ""), "Keystone Safety, LLC")
        self.assertEqual(str(rows[0][2] or ""), "Pat Keystone")
        self.assertEqual(str(rows[0][3] or ""), "pat@keystone.example")
        self.assertEqual(str(rows[0][4] or ""), "Owner")
        self.assertEqual(str(rows[0][5] or ""), "PA")
        self.assertEqual(str(rows[0][6] or ""), "https://keystone.example")
        self.assertIn("AI_ASSIST_UPDATED_CONTACT_TOTAL=1", out)
        self.assertEqual(str(audit_row[0] or ""), "verified")
        self.assertEqual(str(audit_row[1] or ""), "updated_existing_contact")

    def test_import_manual_text_creates_second_contact_for_existing_company_match(self):
        stdin_text = "\n".join(
            [
                "Company: Known Domain Co, LLC",
                "Contact: Nora Domain",
                "Email: nora@known-domain.example",
                "Website: known-domain.example",
                "State: texas",
                "Title: Owner",
            ]
        )
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            db_path = data_dir / "crm.sqlite"
            self._seed_crm_prospect(
                db_path,
                firm="Existing Domain Co",
                website="https://known-domain.example",
                state="TX",
                email="known@known-domain.example",
            )
            self._seed_crm_prospect(
                db_path,
                firm="Shared Firm LLC",
                website="https://shared-firm-crm.example",
                state="PA",
                email="owner@shared-firm-crm.example",
            )
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX,CA,FL,PA,OH,IL,NJ,LA,MI,GA,AL,WI,TN"
            rc, out, err = self._run_main(
                ["--stdin", "--batch", "2026-03-23_AIASSIST_MANUAL_091506"],
                stdin_text=stdin_text,
                env=env,
            )
            self.assertEqual(rc, 0, msg=out + err)
            conn = crm_store.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT email, firm, website, state
                    FROM prospects
                    WHERE website = 'https://known-domain.example' OR email LIKE '%known-domain.example'
                    ORDER BY email
                    """
                ).fetchall()
                audit_row = conn.execute(
                    f"""
                    SELECT verification_status, rejection_reason
                    FROM {crm_store.AI_ASSIST_CANDIDATE_TABLE}
                    WHERE batch_id = '2026-03-23_AIASSIST_MANUAL_091506'
                    """
                ).fetchone()
            finally:
                conn.close()
        self.assertEqual([str(row[0] or "") for row in rows], ["known@known-domain.example", "nora@known-domain.example"])
        self.assertTrue(all(str(row[2] or "") == "https://known-domain.example" for row in rows))
        self.assertIn("AI_ASSIST_CREATED_SECOND_CONTACT_TOTAL=1", out)
        self.assertEqual(str(audit_row[0] or ""), "verified")
        self.assertEqual(str(audit_row[1] or ""), "created_second_contact_existing_company")

    def test_import_manual_text_stages_missing_email_and_rejects_invalid_state(self):
        stdin_text = "\n\n".join(
            [
                "\n".join(
                    [
                        "Company: Lean Safety Group",
                        "Website: lean-safety.example",
                        "State: MI",
                        "Notes: missing email should stage",
                    ]
                ),
                "\n".join(
                    [
                        "Company: Invalid State Safety",
                        "Email: invalid@state.example",
                        "State: middleground",
                    ]
                ),
            ]
        )
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            db_path = data_dir / "crm.sqlite"
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX,CA,FL,PA,OH,IL,NJ,LA,MI,GA,AL,WI,TN"
            rc, out, err = self._run_main(
                ["--stdin", "--batch", "2026-03-23_AIASSIST_MANUAL_091507"],
                stdin_text=stdin_text,
                env=env,
            )
            self.assertEqual(rc, 0, msg=out + err)
            conn = crm_store.connect(db_path)
            try:
                prospects = conn.execute("SELECT email FROM prospects").fetchall()
                audit_rows = conn.execute(
                    f"""
                    SELECT firm, verification_status, rejection_reason
                    FROM {crm_store.AI_ASSIST_CANDIDATE_TABLE}
                    WHERE batch_id = '2026-03-23_AIASSIST_MANUAL_091507'
                    ORDER BY firm
                    """
                ).fetchall()
            finally:
                conn.close()
        self.assertEqual(prospects, [])
        audit_map = {str(firm or ""): (str(status or ""), str(reason or "")) for firm, status, reason in audit_rows}
        self.assertEqual(audit_map["Invalid State Safety"], ("rejected_by_verification", "invalid_state_value"))
        self.assertEqual(audit_map["Lean Safety Group"], ("staged_incomplete", "staged_missing_email"))
        self.assertIn("AI_ASSIST_STAGED_TOTAL=1", out)
        self.assertIn("PASS_AI_ASSIST_IMPORT status=OK", out)


if __name__ == "__main__":
    unittest.main()
