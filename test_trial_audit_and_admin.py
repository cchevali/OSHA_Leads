from __future__ import annotations

import csv
import contextlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import crm_light
import run_trial_admin
import send_digest_email
import trial_audit


class TestTrialAuditAndAdmin(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._tmp_path = Path(self._tmp.name)
        self._old_data_dir = os.environ.get("DATA_DIR")
        self._old_trial_limit_default = os.environ.get("TRIAL_SENDS_LIMIT_DEFAULT")
        os.environ["DATA_DIR"] = str(self._tmp_path / "data")
        os.environ["TRIAL_SENDS_LIMIT_DEFAULT"] = "14"

    def tearDown(self) -> None:
        if self._old_data_dir is None:
            os.environ.pop("DATA_DIR", None)
        else:
            os.environ["DATA_DIR"] = self._old_data_dir
        if self._old_trial_limit_default is None:
            os.environ.pop("TRIAL_SENDS_LIMIT_DEFAULT", None)
        else:
            os.environ["TRIAL_SENDS_LIMIT_DEFAULT"] = self._old_trial_limit_default
        self._tmp.cleanup()

    def _build_leads_db(self, path: Path) -> None:
        conn = sqlite3.connect(str(path))
        try:
            conn.execute(
                """
                CREATE TABLE inspections (
                    lead_key TEXT,
                    activity_nr TEXT,
                    lead_score INTEGER,
                    date_opened TEXT,
                    first_seen_at TEXT,
                    last_seen_at TEXT,
                    inspection_type TEXT,
                    establishment_name TEXT,
                    site_city TEXT,
                    site_state TEXT,
                    source_url TEXT,
                    parse_invalid INTEGER DEFAULT 0
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def test_digest_diff_reports_missing_and_unexpected(self) -> None:
        diff = trial_audit.digest_diff(
            expected_keys=["lead_a", "lead_b"],
            rendered_keys=["lead_b", "lead_c"],
        )
        self.assertEqual(diff["missing"], ["lead_a"])
        self.assertEqual(diff["unexpected"], ["lead_c"])

    def test_load_rendered_digest_prefers_payload_artifact(self) -> None:
        payload_path = trial_audit.sent_payload_path("wally_trial", "2026-02-20", data_root=crm_light.data_dir())
        payload_path.parent.mkdir(parents=True, exist_ok=True)
        payload_path.write_text(
            json.dumps(
                {
                    "subscriber_key": "wally_trial",
                    "selected_lead_keys": ["osha:activity:1001", "osha:activity:1002"],
                    "low_available_lead_keys": ["osha:activity:1003"],
                    "tier_counts": {"high": 1, "medium": 1, "low": 1},
                    "lows_enabled": False,
                    "subject": "test",
                    "render_sha256": "abc",
                }
            ),
            encoding="utf-8",
        )

        rendered = trial_audit.load_rendered_digest_for_date(
            repo_root=self._tmp_path,
            leads_db_path=str(self._tmp_path / "osha.sqlite"),
            subscriber_key="wally_trial",
            for_date="2026-02-20",
            customer_config={},
            data_root=crm_light.data_dir(),
        )
        self.assertEqual(rendered["source"], "payload_artifact")
        self.assertEqual(rendered["shown_lead_keys"], ["osha:activity:1001", "osha:activity:1002"])
        self.assertEqual(rendered["tier_counts"]["low"], 1)

    def test_trial_payload_persistence_enabled_writes_and_updates(self) -> None:
        now_local = datetime.fromisoformat("2026-02-20T08:30:00")
        enabled, payload_path, local_date = send_digest_email._resolve_trial_payload_target(
            persist_payload_root=str(self._tmp_path / "out"),
            subscriber_key="wally_trial",
            mode="daily",
            live_allowed=True,
            dry_run=False,
            no_state_mutation=False,
            now_local=now_local,
        )
        self.assertTrue(enabled)
        self.assertEqual(local_date, "2026-02-20")
        self.assertIsNotNone(payload_path)
        assert payload_path is not None
        send_digest_email._write_trial_payload(
            payload_path,
            {
                "sent_at_utc": "2026-02-20T14:30:00+00:00",
                "subscriber_key": "wally_trial",
                "selected_lead_keys": ["osha:activity:1"],
            },
        )
        self.assertTrue(payload_path.exists())
        send_digest_email._update_trial_payload(payload_path, {"smtp_sent_at_utc": "2026-02-20T14:31:00+00:00"})
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        self.assertEqual(payload.get("smtp_sent_at_utc"), "2026-02-20T14:31:00+00:00")

    def test_payload_persistence_default_off_for_outreach(self) -> None:
        now_local = datetime.fromisoformat("2026-02-20T08:30:00")
        enabled, payload_path, local_date = send_digest_email._resolve_trial_payload_target(
            persist_payload_root="",
            subscriber_key="sunbelt_ops",
            mode="daily",
            live_allowed=True,
            dry_run=False,
            no_state_mutation=False,
            now_local=now_local,
        )
        self.assertFalse(enabled)
        self.assertIsNone(payload_path)
        self.assertEqual(local_date, "2026-02-20")

    def test_load_rendered_digest_fallback_from_run_log_debug(self) -> None:
        leads_db = self._tmp_path / "osha.sqlite"
        self._build_leads_db(leads_db)
        conn = sqlite3.connect(str(leads_db))
        try:
            conn.execute(
                """
                INSERT INTO inspections (
                    lead_key, activity_nr, lead_score, date_opened, first_seen_at, last_seen_at,
                    inspection_type, establishment_name, site_city, site_state, source_url, parse_invalid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    "osha:activity:2001",
                    "2001",
                    11,
                    "2026-02-20",
                    "2026-02-20T12:00:00+00:00",
                    "2026-02-20T12:00:00+00:00",
                    "Complaint",
                    "High Co",
                    "Dallas",
                    "TX",
                    "https://example.com/2001",
                ),
            )
            conn.execute(
                """
                INSERT INTO inspections (
                    lead_key, activity_nr, lead_score, date_opened, first_seen_at, last_seen_at,
                    inspection_type, establishment_name, site_city, site_state, source_url, parse_invalid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    "osha:activity:2002",
                    "2002",
                    3,
                    "2026-02-20",
                    "2026-02-20T12:01:00+00:00",
                    "2026-02-20T12:01:00+00:00",
                    "Referral",
                    "Low Co",
                    "Austin",
                    "TX",
                    "https://example.com/2002",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        out_dir = self._tmp_path / "out"
        out_dir.mkdir(parents=True, exist_ok=True)
        debug_path = out_dir / "territory_debug_20260220.csv"
        with open(debug_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "inspection_nr",
                    "lead_key",
                    "site_city",
                    "site_zip",
                    "mail_zip",
                    "site_county",
                    "inspection_office",
                    "resolved_cbsa",
                    "resolution_source",
                    "territory_code",
                    "matched",
                    "match_reason",
                    "unmatched_reason",
                    "dataset_incomplete",
                ],
            )
            writer.writeheader()
            writer.writerow({"inspection_nr": "2001.001", "lead_key": "osha:activity:2001", "matched": "Y"})
            writer.writerow({"inspection_nr": "2002.001", "lead_key": "osha:activity:2002", "matched": "Y"})

        run_log = out_dir / "run_log_2026-02-20.txt"
        run_log.write_text(
            "\n".join(
                [
                    "RUN_DIAGNOSTICS ingested_total=2 new_inserted=0 existing_updated=2 selected_for_digest=1 dedupe_dropped_due_to_first_seen_before_window=0",
                    f"TERRITORY_DEBUG_WRITTEN path={debug_path} rows=2",
                    "LOW_SIGNALS_PREF lows_enabled=NO low_today=1 cta=enable",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        rendered = trial_audit.load_rendered_digest_for_date(
            repo_root=self._tmp_path,
            leads_db_path=str(leads_db),
            subscriber_key="wally_trial",
            for_date="2026-02-20",
            customer_config={"low_signals_limit": 8},
            data_root=crm_light.data_dir(),
        )
        self.assertEqual(rendered["source"], "fallback_log_artifacts")
        self.assertEqual(rendered["shown_lead_keys"], ["osha:activity:2001"])
        self.assertEqual(rendered["tier_counts"]["low"], 1)

    def test_scope_enhancement_latch_blocks_resend(self) -> None:
        crm_db = crm_light.ensure_database(None)
        with crm_light.open_conn(crm_db) as conn:
            crm_light.init_schema(conn)
            crm_light.upsert_subscriber(
                conn,
                subscriber_key="wally_trial",
                email="wally@example.com",
                territory_code="TX_TRI",
                tz="America/Chicago",
                status="trial",
            )
            crm_light.upsert_trial_state(
                conn,
                subscriber_key="wally_trial",
                start_date="2026-02-04",
                sends_limit=14,
            )

        send_calls = {"n": 0}
        original_report = run_trial_admin.generate_missed_signals_report
        original_send = run_trial_admin.send_email
        original_extend = run_trial_admin.extend_all_trials
        try:
            def _fake_report(**_kwargs):  # type: ignore[no-untyped-def]
                csv_path = crm_light.data_dir() / "trials" / "wally_trial" / "audit" / "fake.csv"
                csv_path.parent.mkdir(parents=True, exist_ok=True)
                csv_path.write_text("lead_key\n", encoding="utf-8")
                return {
                    "subscriber_key": "wally_trial",
                    "territory_code": "TX_TRI",
                    "missed_rows": [],
                    "missed_total": 0,
                    "csv_path": csv_path,
                    "customer_config": {
                        "customer_id": "wally_trial_tx_triangle_v1",
                        "brand_name": "MicroFlowOps",
                        "mailing_address": "11539 Links Dr, Reston, VA 20190",
                    },
                    "primary_recipient": "wally@example.com",
                    "recipients": ["wally@example.com"],
                }

            def _fake_send_email(**_kwargs):  # type: ignore[no-untyped-def]
                send_calls["n"] += 1
                return True, "<msgid>", ""

            def _fake_extend_all_trials(**_kwargs):  # type: ignore[no-untyped-def]
                return {
                    "days": 7,
                    "weekday_delta": 5,
                    "reason": "scope_enhancement_2026-02-20",
                    "scanned": 1,
                    "applied": 1,
                    "skipped_expired": 0,
                    "skipped_idempotent": 0,
                }

            run_trial_admin.generate_missed_signals_report = _fake_report  # type: ignore[assignment]
            run_trial_admin.send_email = _fake_send_email  # type: ignore[assignment]
            run_trial_admin.extend_all_trials = _fake_extend_all_trials  # type: ignore[assignment]

            code1 = run_trial_admin.scope_enhancement(
                subscriber_key="wally_trial",
                leads_db_path=str(self._tmp_path / "osha.sqlite"),
                crm_db_path=crm_db,
                from_date="2026-02-04",
                to_date="2026-02-20",
                extend_days=7,
                send_live=True,
                customer_config_path="",
            )
            self.assertEqual(code1, 0)
            self.assertEqual(send_calls["n"], 1)

            code2 = run_trial_admin.scope_enhancement(
                subscriber_key="wally_trial",
                leads_db_path=str(self._tmp_path / "osha.sqlite"),
                crm_db_path=crm_db,
                from_date="2026-02-04",
                to_date="2026-02-20",
                extend_days=7,
                send_live=True,
                customer_config_path="",
            )
            self.assertEqual(code2, 0)
            self.assertEqual(send_calls["n"], 1)
        finally:
            run_trial_admin.generate_missed_signals_report = original_report  # type: ignore[assignment]
            run_trial_admin.send_email = original_send  # type: ignore[assignment]
            run_trial_admin.extend_all_trials = original_extend  # type: ignore[assignment]

        with crm_light.open_conn(crm_db) as conn:
            rows = conn.execute(
                """
                SELECT status
                FROM send_events
                WHERE subscriber_key = 'wally_trial' AND variant = 'SCOPE_ENHANCEMENT'
                ORDER BY id ASC
                """
            ).fetchall()
        statuses = [str(row["status"]) for row in rows]
        self.assertIn("SCOPE_ENHANCEMENT_SENT", statuses)
        self.assertIn("SKIP_SCOPE_ENHANCEMENT_ALREADY_SENT", statuses)

    def test_append_event_defaults_ts_utc_when_missing(self) -> None:
        crm_db = crm_light.ensure_database(None)
        with crm_light.open_conn(crm_db) as conn:
            crm_light.init_schema(conn)
            crm_light.upsert_subscriber(
                conn,
                subscriber_key="wally_trial",
                email="wgs@indigocompliance.com",
                territory_code="TX_TRI",
                tz="America/Chicago",
                status="trial",
            )
            crm_light.upsert_trial_state(
                conn,
                subscriber_key="wally_trial",
                start_date="2026-02-04",
                sends_limit=14,
            )

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = run_trial_admin.append_event(
                subscriber_key="wally_trial",
                ts_utc="",
                status="SENT",
                variant="DAILY",
                run_id="scheduler_wally_trial_20260302T131400Z",
                primary_recipient="wgs@indigocompliance.com",
                send_mode="LIVE",
                local_date="2026-03-02",
                meta_source="wally_trial_scheduler",
                crm_db_path=crm_db,
            )
        self.assertEqual(code, 0)
        text = buf.getvalue()
        self.assertIn("OK append-event", text)
        self.assertRegex(text, r"ts_utc=\d{4}-\d{2}-\d{2}T")
        self.assertIn("+00:00", text)

        with crm_light.open_conn(crm_db) as conn:
            row = conn.execute(
                """
                SELECT ts_utc, status, variant, run_id, meta_json
                FROM send_events
                WHERE subscriber_key = 'wally_trial'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertIn("+00:00", str(row["ts_utc"] or ""))
        self.assertEqual(str(row["status"] or ""), "SENT")
        self.assertEqual(str(row["variant"] or ""), "DAILY")
        self.assertEqual(str(row["run_id"] or ""), "scheduler_wally_trial_20260302T131400Z")

    def test_extend_all_trials_idempotent_and_math(self) -> None:
        crm_db = crm_light.ensure_database(None)
        with crm_light.open_conn(crm_db) as conn:
            crm_light.init_schema(conn)
            crm_light.upsert_subscriber(
                conn,
                subscriber_key="trial_active",
                email="active@example.com",
                territory_code="TX_TRI",
                tz="America/Chicago",
                status="trial",
            )
            crm_light.upsert_trial_state(
                conn,
                subscriber_key="trial_active",
                start_date="2026-02-04",
                sends_limit=14,
            )
            crm_light.append_send_event(
                conn,
                subscriber_key="trial_active",
                variant="DAILY",
                status="SENT",
                run_id="seed_active",
                meta={"send_mode": "LIVE", "primary_recipient": "active@example.com"},
                ts_utc="2026-02-10T15:00:00+00:00",
            )
            crm_light.upsert_subscriber(
                conn,
                subscriber_key="trial_expired",
                email="expired@example.com",
                territory_code="TX_TRI",
                tz="America/Chicago",
                status="trial",
            )
            crm_light.upsert_trial_state(
                conn,
                subscriber_key="trial_expired",
                start_date="2026-02-04",
                sends_limit=1,
            )
            crm_light.append_send_event(
                conn,
                subscriber_key="trial_expired",
                variant="DAILY",
                status="SENT",
                run_id="seed_expired",
                meta={"send_mode": "LIVE", "primary_recipient": "expired@example.com"},
                ts_utc="2026-02-10T15:00:00+00:00",
            )

        first = run_trial_admin.extend_all_trials(
            days=7,
            reason="scope_enhancement_2026-02-20",
            crm_db_path=crm_db,
        )
        self.assertEqual(first["weekday_delta"], 5)
        self.assertEqual(first["applied"], 1)
        self.assertEqual(first["skipped_expired"], 1)

        with crm_light.open_conn(crm_db) as conn:
            row = conn.execute(
                "SELECT sends_limit FROM trial_state WHERE subscriber_key = 'trial_active'"
            ).fetchone()
            self.assertEqual(int(row["sends_limit"]), 19)

        second = run_trial_admin.extend_all_trials(
            days=7,
            reason="scope_enhancement_2026-02-20",
            crm_db_path=crm_db,
        )
        self.assertEqual(second["applied"], 0)
        self.assertGreaterEqual(second["skipped_idempotent"], 1)
        with crm_light.open_conn(crm_db) as conn:
            row = conn.execute(
                "SELECT sends_limit FROM trial_state WHERE subscriber_key = 'trial_active'"
            ).fetchone()
            self.assertEqual(int(row["sends_limit"]), 19)

    def test_extend_all_trials_rejects_non_multiple_of_7(self) -> None:
        crm_db = crm_light.ensure_database(None)
        with self.assertRaises(ValueError) as ctx:
            run_trial_admin.extend_all_trials(
                days=8,
                reason="scope_enhancement_2026-02-20",
                crm_db_path=crm_db,
            )
        self.assertEqual(str(ctx.exception), "ERR_TRIAL_EXTENSION_DAYS_NOT_MULTIPLE_OF_7")

    def test_normalize_trials_is_deterministic_and_idempotent(self) -> None:
        crm_db = crm_light.ensure_database(None)
        with crm_light.open_conn(crm_db) as conn:
            crm_light.init_schema(conn)
            crm_light.upsert_subscriber(
                conn,
                subscriber_key="trial_legacy",
                email="legacy@example.com",
                territory_code="TX_TRI",
                tz="America/Chicago",
                status="trial",
            )
            crm_light.upsert_trial_state(
                conn,
                subscriber_key="trial_legacy",
                start_date="2026-02-04",
                sends_limit=10,
            )
            crm_light.upsert_subscriber(
                conn,
                subscriber_key="trial_custom",
                email="custom@example.com",
                territory_code="TX_TRI",
                tz="America/Chicago",
                status="trial",
            )
            crm_light.upsert_trial_state(
                conn,
                subscriber_key="trial_custom",
                start_date="2026-02-04",
                sends_limit=9,
            )
            crm_light.record_trial_adjustment_once(
                conn,
                subscriber_key="trial_custom",
                adjustment_key="custom_limit|manual",
                adjustment_type="CUSTOM_LIMIT",
                delta_sends=0,
                reason="custom_limit_manual",
                meta={"sends_limit": 9},
                commit=False,
            )
            for idx, ts in enumerate(
                [
                    "2026-02-20T16:41:08.052314+00:00",
                    "2026-02-20T17:13:32.292294+00:00",
                    "2026-02-20T17:32:21.442847+00:00",
                ]
            ):
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_legacy",
                    variant="SCOPE_ENHANCEMENT",
                    status="SCOPE_ENHANCEMENT_SENT",
                    run_id=f"scope_{idx}",
                    meta={"from_date": "2026-02-04", "to_date": "2026-02-20"},
                    ts_utc=ts,
                )
            conn.commit()

        first = run_trial_admin.normalize_trials(apply=True, crm_db_path=crm_db)
        self.assertEqual(first["updated_limits"], 1)
        self.assertEqual(first["superseded_events"], 2)

        with crm_light.open_conn(crm_db) as conn:
            legacy_limit = conn.execute(
                "SELECT sends_limit FROM trial_state WHERE subscriber_key = 'trial_legacy'"
            ).fetchone()
            custom_limit = conn.execute(
                "SELECT sends_limit FROM trial_state WHERE subscriber_key = 'trial_custom'"
            ).fetchone()
            self.assertEqual(int(legacy_limit["sends_limit"]), 14)
            self.assertEqual(int(custom_limit["sends_limit"]), 9)
            statuses = [
                str(row["status"] or "")
                for row in conn.execute(
                    """
                    SELECT status
                    FROM send_events
                    WHERE subscriber_key = 'trial_legacy'
                      AND variant = 'SCOPE_ENHANCEMENT'
                    ORDER BY ts_utc ASC, id ASC
                    """
                ).fetchall()
            ]
            self.assertEqual(statuses.count("SCOPE_ENHANCEMENT_SENT"), 1)
            self.assertEqual(statuses.count("SUPERSEDED"), 2)

        second = run_trial_admin.normalize_trials(apply=True, crm_db_path=crm_db)
        self.assertEqual(second["updated_limits"], 0)
        self.assertEqual(second["superseded_events"], 0)

    def test_show_includes_effective_fields_and_note(self) -> None:
        crm_db = crm_light.ensure_database(None)
        with crm_light.open_conn(crm_db) as conn:
            crm_light.init_schema(conn)
            crm_light.upsert_subscriber(
                conn,
                subscriber_key="trial_show",
                email="show@example.com",
                territory_code="TX_TRI",
                tz="America/Chicago",
                status="trial",
            )
            crm_light.upsert_trial_state(
                conn,
                subscriber_key="trial_show",
                start_date="2026-02-04",
                sends_limit=10,
            )
            crm_light.append_send_event(
                conn,
                subscriber_key="trial_show",
                variant="DAILY",
                status="SENT",
                run_id="live_1",
                meta={"send_mode": "LIVE", "primary_recipient": "show@example.com"},
                ts_utc="2026-02-10T15:00:00+00:00",
            )
            crm_light.append_send_event(
                conn,
                subscriber_key="trial_show",
                variant="DAILY",
                status="SENT",
                run_id="live_dup",
                meta={"send_mode": "LIVE", "primary_recipient": "show@example.com"},
                ts_utc="2026-02-10T16:00:00+00:00",
            )

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = run_trial_admin.show_trial("trial_show", crm_db_path=crm_db, recent=5)
        self.assertEqual(code, 0)
        output = buf.getvalue()
        self.assertIn("effective_sends_limit=10", output)
        self.assertIn("default_sends_limit=14", output)
        self.assertIn("sends_remaining=9", output)
        self.assertIn("expiry_basis=UNIQUE_WEEKDAY_SENT_DAYS", output)
        self.assertIn("NOTE sent_rows_raw includes duplicates; expiry uses sent_count only", output)

    def test_init_schema_migration_idempotent_for_trial_tables(self) -> None:
        crm_db = crm_light.ensure_database(None)
        with crm_light.open_conn(crm_db) as conn:
            crm_light.init_schema(conn)
            crm_light.init_schema(conn)
            tables = {
                str(row["name"])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('trial_adjustments','trial_latches')"
                ).fetchall()
            }
        self.assertIn("trial_adjustments", tables)
        self.assertIn("trial_latches", tables)


if __name__ == "__main__":
    unittest.main()
