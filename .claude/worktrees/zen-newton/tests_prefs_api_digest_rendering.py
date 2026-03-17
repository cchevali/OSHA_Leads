import io
import os
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from email_footer import build_footer_html
import send_digest_email
from send_digest_email import fetch_lows_enabled_pref, generate_digest_html


class _FakeHTTPResponse:
    def __init__(self, body: str, status: int = 200):
        self._body = body.encode("utf-8")
        self.status = status

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestDigestPrefsApi(unittest.TestCase):
    def setUp(self) -> None:
        self._env_before = dict(os.environ)
        os.environ["MFO_PREFS_BASE_URL"] = "https://unsub.example.internal"
        os.environ["MFO_INTERNAL_KEY"] = "test_key"
        send_digest_email._PREFS_CACHE.clear()

        self.config = {
            "states": ["TX"],
            "top_k_overall": 25,
            "top_k_per_state": 10,
        }
        self.branding = {
            "brand_name": "Acme Safety",
            "mailing_address": "123 Main St, Austin, TX 78701",
            "from_email": "alerts@acme.com",
            "reply_to": "support@acme.com",
            "from_display_name": "Acme Safety Alerts",
        }
        self.footer_html = build_footer_html(
            brand_name=self.branding["brand_name"],
            mailing_address=self.branding["mailing_address"],
            disclaimer="This report contains public OSHA inspection data for informational purposes only. Not legal advice.",
            reply_to=self.branding["reply_to"],
            unsub_url=None,
        )

    def tearDown(self) -> None:
        send_digest_email._PREFS_CACHE.clear()
        os.environ.clear()
        os.environ.update(self._env_before)

    def test_prefs_api_true_includes_low_priority_rows(self) -> None:
        def _urlopen(req, timeout=3):
            headers = {k.lower(): v for k, v in (getattr(req, "headers", {}) or {}).items()}
            self.assertEqual("test_key", headers.get("x-mfo-internal-key"))
            self.assertIn("/api/prefs_state?", getattr(req, "full_url", ""))
            return _FakeHTTPResponse("{\"lows_enabled\":true,\"updated_at_iso\":\"2026-02-10T00:00:00Z\"}")

        with patch("send_digest_email.urllib.request.urlopen", side_effect=_urlopen):
            include = fetch_lows_enabled_pref("wally_trial", "TX_TRIANGLE_V1")
        self.assertTrue(include)

        tier_counts = {"high": 0, "medium": 0, "low": 1}
        low_priority = [
            {
                "establishment_name": "LowCo",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Planned",
                "date_opened": "2026-02-06",
                "lead_score": 1,
                "source_url": "https://example.com/low",
            }
        ]
        html = generate_digest_html(
            leads=[],
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-06",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts=tier_counts,
            enable_lows_url="https://unsub.example/prefs/enable_lows?token=x.y",
            include_lows=include,
            low_priority=low_priority,
            footer_html=self.footer_html,
            summary_label="Newly observed today: 0 signals",
        )
        self.assertIn("Low priority (1)", html)
        self.assertIn("LowCo", html)
        self.assertNotIn("(not shown)", html)
        self.assertNotIn("Enable lows", html)
        self.assertIn("Disable lows", html)

    def test_prefs_api_failure_defaults_disabled_and_logs(self) -> None:
        buf = io.StringIO()
        with redirect_stdout(buf):
            with patch("send_digest_email.urllib.request.urlopen", side_effect=Exception("boom")):
                include = fetch_lows_enabled_pref("wally_trial", "TX_TRIANGLE_V1")
        self.assertFalse(include)
        self.assertIn("PREFS_FETCH_FAIL", buf.getvalue())

        tier_counts = {"high": 0, "medium": 0, "low": 1}
        low_priority = [
            {
                "establishment_name": "LowCo",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Planned",
                "date_opened": "2026-02-06",
                "lead_score": 1,
                "source_url": "https://example.com/low",
            }
        ]
        html = generate_digest_html(
            leads=[],
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-06",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts=tier_counts,
            enable_lows_url="https://unsub.example/prefs/enable_lows?token=x.y",
            include_lows=include,
            low_priority=low_priority,
            footer_html=self.footer_html,
            summary_label="Newly observed today: 0 signals",
        )
        self.assertNotIn("Low priority (", html)
        self.assertNotIn("LowCo", html)
        self.assertIn("Low signals:", html)
        self.assertIn("OFF", html)
        self.assertIn("(1 available today)", html)
        self.assertIn("(not shown)", html)
        self.assertIn("Enable lows", html)


if __name__ == "__main__":
    unittest.main()

