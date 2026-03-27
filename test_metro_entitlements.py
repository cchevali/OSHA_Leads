import csv
import gzip
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import crm_light
import send_digest_email
from geo import zip_cbsa
from lead_filters import filter_by_cbsa_allowlist


class TestMetroEntitlements(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)
        self.crm_db = self.tmp_path / "crm.sqlite"
        self._orig_zip_to_cbsa = zip_cbsa.ZIP_TO_CBSA_PATH
        self._orig_cbsa_meta = zip_cbsa.CBSA_META_PATH
        self._orig_dataset_meta = zip_cbsa.ZIP_TO_CBSA_DATASET_META_PATH
        self._orig_sources_path = zip_cbsa.ZIP_TO_CBSA_SOURCES_PATH

        zip_map_path = self.tmp_path / "zip_to_cbsa.csv.gz"
        with gzip.open(zip_map_path, "wt", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["ZIP5", "CBSA"])
            writer.writerow(["75035", "19100"])
            writer.writerow(["78701", "12420"])

        meta_path = self.tmp_path / "cbsa_meta.csv"
        with open(meta_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["CBSA", "metro_label"])
            writer.writerow(["19100", "Dallas-Fort Worth-Arlington, TX"])
            writer.writerow(["12420", "Austin-Round Rock-Georgetown, TX"])

        dataset_meta_path = self.tmp_path / "zip_to_cbsa.meta.json"
        dataset_meta_path.write_text(
            json.dumps(
                {
                    "source_label": "HUD USPS ZIP-CBSA 2025 Q4",
                    "dataset_incomplete": False,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        sources_path = self.tmp_path / "SOURCES.md"
        sources_path.write_text("# test\n", encoding="utf-8")

        zip_cbsa.ZIP_TO_CBSA_PATH = zip_map_path
        zip_cbsa.CBSA_META_PATH = meta_path
        zip_cbsa.ZIP_TO_CBSA_DATASET_META_PATH = dataset_meta_path
        zip_cbsa.ZIP_TO_CBSA_SOURCES_PATH = sources_path
        zip_cbsa.clear_caches()

    def tearDown(self) -> None:
        zip_cbsa.ZIP_TO_CBSA_PATH = self._orig_zip_to_cbsa
        zip_cbsa.CBSA_META_PATH = self._orig_cbsa_meta
        zip_cbsa.ZIP_TO_CBSA_DATASET_META_PATH = self._orig_dataset_meta
        zip_cbsa.ZIP_TO_CBSA_SOURCES_PATH = self._orig_sources_path
        zip_cbsa.clear_caches()
        self._tmp.cleanup()

    def _patch_env_without_stripe_price_map(self, **overrides: str) -> mock._patch_dict:
        env = os.environ.copy()
        for key in list(env.keys()):
            if key.startswith("STRIPE_PRICE_"):
                env.pop(key, None)
        env.update(overrides)
        return mock.patch.dict(os.environ, env, clear=True)

    def test_pricing_copy_contract(self) -> None:
        pricing_path = Path("web/app/pricing/page.tsx")
        text = pricing_path.read_text(encoding="utf-8")
        self.assertIn("Founding Pilot", text)
        self.assertIn("$149", text)
        self.assertIn("One state", text)
        self.assertIn("We qualify fit manually.", text)
        self.assertIn("Up to 4 metros", text)
        self.assertIn("Up to 10 metros", text)
        self.assertIn("Reply with your state or metro", text)

    def test_onboarding_enforces_max_metros(self) -> None:
        crm_light.ensure_database(self.crm_db)
        with crm_light.open_conn(self.crm_db) as conn:
            crm_light.init_schema(conn)
            rejected = crm_light.upsert_subscriber_onboarding(
                conn,
                subscriber_key="sub_core",
                email="core@example.com",
                plan_code="core",
                cbsa_codes=["19100", "12420", "26420", "41700", "21340"],
                dry_run=False,
            )
            self.assertFalse(rejected.get("ok"))
            self.assertEqual(rejected.get("err_code"), "ERR_MAX_METROS_EXCEEDED")

            accepted = crm_light.upsert_subscriber_onboarding(
                conn,
                subscriber_key="sub_core",
                email="core@example.com",
                plan_code="core",
                cbsa_codes=["19100", "12420", "26420", "41700"],
                dry_run=False,
            )
            self.assertTrue(accepted.get("ok"))
            entitlement = crm_light.get_subscriber_entitlement(conn, subscriber_key="sub_core")
            self.assertIsNotNone(entitlement)
            self.assertEqual(int((entitlement or {}).get("max_metros") or 0), 4)
            allowlist = crm_light.get_subscriber_cbsa_allowlist(conn, "sub_core")
            self.assertEqual(len(allowlist), 4)

    def test_onboarding_recipients_fallback_to_email_when_missing(self) -> None:
        crm_light.ensure_database(self.crm_db)
        with crm_light.open_conn(self.crm_db) as conn:
            crm_light.init_schema(conn)
            accepted = crm_light.upsert_subscriber_onboarding(
                conn,
                subscriber_key="sub_fallback",
                email="Admin@Example.com",
                plan_code="core",
                cbsa_codes=["19100"],
                dry_run=False,
            )
            self.assertTrue(accepted.get("ok"))
            self.assertEqual(accepted.get("recipients"), [{"email": "admin@example.com"}])
            entitlement = crm_light.get_subscriber_entitlement(conn, subscriber_key="sub_fallback")
            self.assertEqual(crm_light.entitlement_recipient_emails(entitlement), ["admin@example.com"])

    def test_onboarding_recipients_normalize_and_dedupe_preserve_order(self) -> None:
        crm_light.ensure_database(self.crm_db)
        with crm_light.open_conn(self.crm_db) as conn:
            crm_light.init_schema(conn)
            accepted = crm_light.upsert_subscriber_onboarding(
                conn,
                subscriber_key="sub_multi_recips",
                email="billing@example.com",
                plan_code="core",
                cbsa_codes=["19100"],
                recipients=[
                    {"email": "  Primary@Example.com ", "name": "  Jane   Doe "},
                    {"email": "ops@example.com", "name": " Ops "},
                    {"email": "PRIMARY@example.com", "name": "Duplicate"},
                ],
                dry_run=False,
            )
            self.assertTrue(accepted.get("ok"))
            self.assertEqual(
                accepted.get("recipients"),
                [
                    {"email": "primary@example.com", "name": "Jane Doe"},
                    {"email": "ops@example.com", "name": "Ops"},
                ],
            )
            entitlement = crm_light.get_subscriber_entitlement(conn, subscriber_key="sub_multi_recips")
            self.assertEqual(
                crm_light.entitlement_recipient_emails(entitlement),
                ["primary@example.com", "ops@example.com"],
            )

    def test_onboarding_enforces_max_recipients_by_plan(self) -> None:
        crm_light.ensure_database(self.crm_db)
        with crm_light.open_conn(self.crm_db) as conn:
            crm_light.init_schema(conn)
            core_reject = crm_light.upsert_subscriber_onboarding(
                conn,
                subscriber_key="sub_core_cap",
                email="billing@example.com",
                plan_code="core",
                cbsa_codes=["19100"],
                recipients=[{"email": f"user{i}@example.com"} for i in range(1, 8)],
                dry_run=False,
            )
            self.assertFalse(core_reject.get("ok"))
            self.assertEqual(core_reject.get("err_code"), "ERR_MAX_RECIPIENTS_EXCEEDED")
            self.assertEqual(core_reject.get("max_recipients"), 6)
            self.assertEqual(core_reject.get("selected_recipients"), 7)

            core_accept = crm_light.upsert_subscriber_onboarding(
                conn,
                subscriber_key="sub_core_cap",
                email="billing@example.com",
                plan_code="core",
                cbsa_codes=["19100"],
                recipients=[{"email": f"user{i}@example.com"} for i in range(1, 7)],
                dry_run=False,
            )
            self.assertTrue(core_accept.get("ok"))
            self.assertEqual(core_accept.get("max_recipients"), 6)
            self.assertEqual(core_accept.get("selected_recipients"), 6)

            multi_reject = crm_light.upsert_subscriber_onboarding(
                conn,
                subscriber_key="sub_multi_cap",
                email="billing2@example.com",
                plan_code="multi",
                cbsa_codes=["19100"],
                recipients=[{"email": f"multi{i}@example.com"} for i in range(1, 17)],
                dry_run=False,
            )
            self.assertFalse(multi_reject.get("ok"))
            self.assertEqual(multi_reject.get("err_code"), "ERR_MAX_RECIPIENTS_EXCEEDED")
            self.assertEqual(multi_reject.get("max_recipients"), 15)

    def test_entitlement_recipient_emails_falls_back_to_legacy_email(self) -> None:
        self.assertEqual(
            crm_light.entitlement_recipient_emails({"email": "Legacy@Example.com"}),
            ["legacy@example.com"],
        )

    def test_send_collect_recipients_prefers_registry_entitlement_recipients(self) -> None:
        config = {
            "recipients": ["config1@example.com", "config2@example.com"],
            "email_recipients": ["config1@example.com", "config2@example.com"],
        }
        subscriber_profile = {
            "email": "profile@example.com",
            "recipients": ["profile1@example.com", "profile2@example.com"],
        }
        entitlement = {
            "email": "billing@example.com",
            "recipients_json": json.dumps(
                [
                    {"email": "Primary@Example.com", "name": "Primary User"},
                    {"email": "ops@example.com"},
                ]
            ),
        }
        recipients = send_digest_email.collect_recipients(config, subscriber_profile, None, entitlement=entitlement)
        self.assertEqual(recipients, ["primary@example.com", "ops@example.com"])

        override = send_digest_email.collect_recipients(
            config,
            subscriber_profile,
            "override@example.com",
            entitlement=entitlement,
        )
        self.assertEqual(override, ["override@example.com"])

    def test_paid_send_hard_fails_when_dataset_incomplete(self) -> None:
        entitlement = {"plan_code": "core"}
        with mock.patch.object(send_digest_email, "zip_cbsa_dataset_status", return_value={"dataset_incomplete": True}), mock.patch.object(
            send_digest_email,
            "_is_trial_subscriber",
            return_value=False,
        ):
            ok, token = send_digest_email._enforce_zip_cbsa_dataset_gate(
                subscriber_key="sub_paid",
                entitlement=entitlement,
            )
        self.assertFalse(ok)
        self.assertIn("ERR_PAID_SEND_DATASET_INCOMPLETE", token)

    def test_cbsa_allowlist_reason_tokens_cover_common_exclusions(self) -> None:
        allowlist = ["19100"]
        leads = [
            {
                "activity_nr": "missing",
                "site_zip": "",
                "mail_zip": "",
                "site_city": "Dallas",
                "source_url": "https://example.com?id=1",
            },
            {
                "activity_nr": "unknown",
                "site_zip": "99999",
                "mail_zip": "",
                "site_city": "Dallas",
                "source_url": "https://example.com?id=2",
            },
            {
                "activity_nr": "mismatch",
                "site_zip": "78701",
                "mail_zip": "",
                "site_city": "Austin",
                "source_url": "https://example.com?id=3",
            },
        ]
        _filtered, _stats, debug_rows = filter_by_cbsa_allowlist(leads, allowlist, include_debug=True)
        reasons = {str(row.get("inspection_nr")): str(row.get("match_reason")) for row in debug_rows}
        self.assertIn("CBSA_UNRESOLVED|ZIP_MISSING", reasons.get("1", ""))
        self.assertIn("CBSA_UNRESOLVED|ZIP_UNKNOWN", reasons.get("2", ""))
        self.assertIn("CBSA_MISMATCH", reasons.get("3", ""))

    def test_stripe_ingestion_is_idempotent(self) -> None:
        crm_light.ensure_database(self.crm_db)
        event_payload = {
            "id": "evt_001",
            "type": "customer.subscription.created",
            "data": {
                "object": {
                    "id": "sub_001",
                    "customer": "cus_001",
                    "status": "active",
                    "metadata": {
                        "subscriber_key": "sub_paid_core",
                        "customer_email": "paid@example.com",
                    },
                    "items": {"data": [{"price": {"id": "price_core_001"}}]},
                }
            },
        }
        with mock.patch.dict(os.environ, {"STRIPE_PRICE_ID_CORE": "price_core_001"}, clear=False):
            with crm_light.open_conn(self.crm_db) as conn:
                crm_light.init_schema(conn)
                first = crm_light.ingest_stripe_subscription_event(conn, event_payload, dry_run=False)
                second = crm_light.ingest_stripe_subscription_event(conn, event_payload, dry_run=False)
                self.assertEqual(first.get("token"), "STRIPE_EVENT_PROCESSED")
                self.assertEqual(second.get("token"), "STRIPE_EVENT_DUPLICATE")
                row = conn.execute("SELECT COUNT(1) c FROM subscriptions WHERE stripe_subscription_id = ?", ("sub_001",)).fetchone()
                self.assertEqual(int(row["c"] if row else 0), 1)
                event_rows = conn.execute("SELECT COUNT(1) c FROM stripe_event_log WHERE event_id = ?", ("evt_001",)).fetchone()
                self.assertEqual(int(event_rows["c"] if event_rows else 0), 1)

    def test_stripe_ingestion_missing_price_map_fails_fast_without_writes(self) -> None:
        crm_light.ensure_database(self.crm_db)
        event_payload = {
            "id": "evt_002",
            "type": "customer.subscription.created",
            "data": {
                "object": {
                    "id": "sub_002",
                    "customer": "cus_002",
                    "status": "active",
                    "metadata": {"customer_email": "missingmap@example.com"},
                    "items": {"data": [{"price": {"id": "price_missing_map"}}]},
                }
            },
        }
        with mock.patch.dict(
            os.environ,
            {
                "STRIPE_PRICE_ID_CORE": "",
                "STRIPE_PRICE_ID_MULTI": "",
                "STRIPE_PRICE_ID_PILOT": "",
            },
            clear=False,
        ):
            with crm_light.open_conn(self.crm_db) as conn:
                crm_light.init_schema(conn)
                result = crm_light.ingest_stripe_subscription_event(conn, event_payload, dry_run=False)
                self.assertFalse(result.get("ok"))
                self.assertEqual(result.get("token"), "ERR_STRIPE_PRICE_MAP_MISSING")
                subs = conn.execute("SELECT COUNT(1) c FROM subscriptions").fetchone()
                self.assertEqual(int(subs["c"] if subs else 0), 0)
                events = conn.execute("SELECT COUNT(1) c FROM stripe_event_log").fetchone()
                self.assertEqual(int(events["c"] if events else 0), 0)

    def test_stripe_ingestion_retry_after_missing_env_succeeds(self) -> None:
        crm_light.ensure_database(self.crm_db)
        event_payload = {
            "id": "evt_003",
            "type": "customer.subscription.created",
            "data": {
                "object": {
                    "id": "sub_003",
                    "customer": "cus_003",
                    "status": "active",
                    "metadata": {"customer_email": "retry@example.com"},
                    "items": {"data": [{"price": {"id": "price_core_retry"}}]},
                }
            },
        }
        with crm_light.open_conn(self.crm_db) as conn:
            crm_light.init_schema(conn)
            with self._patch_env_without_stripe_price_map():
                first = crm_light.ingest_stripe_subscription_event(conn, event_payload, dry_run=False)
                self.assertFalse(first.get("ok"))
                self.assertEqual(first.get("token"), "ERR_STRIPE_PRICE_MAP_MISSING")
                events_before = conn.execute("SELECT COUNT(1) c FROM stripe_event_log").fetchone()
                self.assertEqual(int(events_before["c"] if events_before else 0), 0)
            with self._patch_env_without_stripe_price_map(STRIPE_PRICE_ID_CORE="price_core_retry"):
                second = crm_light.ingest_stripe_subscription_event(conn, event_payload, dry_run=False)
                self.assertTrue(second.get("ok"))
                self.assertEqual(second.get("token"), "STRIPE_EVENT_PROCESSED")
                third = crm_light.ingest_stripe_subscription_event(conn, event_payload, dry_run=False)
                self.assertEqual(third.get("token"), "STRIPE_EVENT_DUPLICATE")
                subs = conn.execute("SELECT COUNT(1) c FROM subscriptions WHERE stripe_subscription_id = ?", ("sub_003",)).fetchone()
                self.assertEqual(int(subs["c"] if subs else 0), 1)


if __name__ == "__main__":
    unittest.main()
