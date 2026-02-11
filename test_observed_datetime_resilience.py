import io
import sqlite3
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from email_footer import build_footer_html
from send_digest_email import (
    _observed_datetime,
    _observed_timestamp,
    compute_territory_health,
    generate_digest_html,
)

REPO_ROOT = Path(__file__).resolve().parent
SCHEMA_FILE = REPO_ROOT / "schema.sql"


class TestObservedDatetimeResilience(unittest.TestCase):
    def test_observed_timestamp_prefers_first_seen(self) -> None:
        lead = {
            "first_seen_at": "2026-02-11T08:00:00+00:00",
            "last_seen_at": "2026-02-11T10:00:00+00:00",
            "changed_at": "2026-02-11T11:00:00+00:00",
        }
        stamp = _observed_timestamp(lead, ZoneInfo("UTC"))
        self.assertEqual("2026-02-11 08:00 UTC", stamp)

    def test_observed_datetime_handles_mixed_naive_and_aware(self) -> None:
        lead = {
            "changed_at": "2026-02-11 09:15:00",
            "first_seen_at": "2026-02-11T09:10:00+00:00",
            "last_seen_at": None,
        }
        observed = _observed_datetime(lead)
        self.assertIsNotNone(observed)
        assert observed is not None
        self.assertIsNotNone(observed.tzinfo)
        self.assertEqual("2026-02-11T09:15:00+00:00", observed.isoformat())

    def test_observed_datetime_skips_bad_candidate_and_logs_warning(self) -> None:
        lead = {
            "inspection_id": "insp-123",
            "changed_at": 12345,
            "first_seen_at": "2026-02-11T09:10:00+00:00",
            "last_seen_at": None,
        }
        buf = io.StringIO()
        with redirect_stdout(buf):
            observed = _observed_datetime(lead)
        self.assertIsNotNone(observed)
        out = buf.getvalue()
        self.assertIn("WARN_OBSERVED_DT_COERCE_FAIL", out)
        self.assertIn("lead_id=insp-123", out)
        self.assertIn("field=changed_at", out)

    def test_generate_digest_html_with_mixed_candidates_does_not_crash(self) -> None:
        config = {
            "states": ["TX"],
            "top_k_overall": 25,
            "top_k_per_state": 10,
        }
        branding = {
            "brand_name": "Acme Safety",
            "mailing_address": "123 Main St, Austin, TX 78701",
            "from_email": "alerts@acme.com",
            "reply_to": "support@acme.com",
            "from_display_name": "Acme Safety Alerts",
        }
        footer_html = build_footer_html(
            brand_name=branding["brand_name"],
            mailing_address=branding["mailing_address"],
            disclaimer="Informational only. Not legal advice.",
            reply_to=branding["reply_to"],
            unsub_url=None,
        )
        leads = [
            {
                "activity_nr": "900000001",
                "lead_id": "lead-1",
                "date_opened": "2026-02-11",
                "inspection_type": "Complaint",
                "establishment_name": "Resilience Co",
                "site_city": "Austin",
                "site_state": "TX",
                "lead_score": 7,
                "first_seen_at": "2026-02-11T09:10:00+00:00",
                "changed_at": "2026-02-11 09:15:00",
                "source_url": "https://example.com/lead/900000001",
            }
        ]

        html = generate_digest_html(
            leads=leads,
            low_fallback=[],
            config=config,
            gen_date="2026-02-11",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=branding,
            footer_html=footer_html,
            tier_counts={"high": 0, "medium": 1, "low": 0},
        )
        self.assertIn("Resilience Co", html)

    def test_compute_territory_health_with_mixed_candidates_does_not_crash(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA_FILE.read_text(encoding="utf-8"))
        now_text_aware = "2026-02-11T09:10:00+00:00"
        now_text_naive = "2026-02-11 09:15:00"
        conn.execute(
            """
            INSERT INTO inspections (
                activity_nr, date_opened, inspection_type, scope, case_status,
                establishment_name, site_city, site_state, site_zip,
                lead_score, first_seen_at, last_seen_at, parse_invalid, source_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                "900000009",
                "2026-02-11",
                "Complaint",
                "Partial",
                "OPEN",
                "Health Co",
                "Austin",
                "TX",
                "78701",
                7,
                now_text_aware,
                now_text_aware,
                "https://example.com/lead/900000009",
            ),
        )

        cols = [row[1] for row in conn.execute("PRAGMA table_info(inspections)").fetchall()]
        if "changed_at" in cols:
            conn.execute("UPDATE inspections SET changed_at = ? WHERE activity_nr = ?", (now_text_naive, "900000009"))
        conn.commit()

        health = compute_territory_health(
            conn=conn,
            territory_code="TX_TRIANGLE_V1",
            states=["TX"],
            now_utc=datetime(2026, 2, 11, 10, 0, 0),
        )
        self.assertIn("window_24", health)
        self.assertIn("window_14", health)
        self.assertGreaterEqual(int(health["window_24"]["tx_total"]), 1)
        conn.close()


if __name__ == "__main__":
    unittest.main()
