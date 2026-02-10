import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
import io
from contextlib import redirect_stdout
from http.server import HTTPServer
from pathlib import Path
from datetime import datetime, timedelta
import sqlite3

import unsubscribe_server
import unsubscribe_utils


def _http(url: str, method: str = "GET") -> tuple[int, str]:
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = resp.read().decode("utf-8", errors="replace")
            return int(resp.status), data
    except urllib.error.HTTPError as e:
        data = ""
        try:
            data = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        return int(e.code), data


def _http_full(url: str, method: str = "GET") -> tuple[int, str, dict]:
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = resp.read().decode("utf-8", errors="replace")
            headers = dict(resp.headers.items())
            return int(resp.status), data, headers
    except urllib.error.HTTPError as e:
        data = ""
        try:
            data = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        headers = dict(e.headers.items()) if getattr(e, "headers", None) else {}
        return int(e.code), data, headers


def _http_full_headers(url: str, method: str = "GET", headers: dict | None = None) -> tuple[int, str, dict]:
    req = urllib.request.Request(url, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = resp.read().decode("utf-8", errors="replace")
            resp_headers = dict(resp.headers.items())
            return int(resp.status), data, resp_headers
    except urllib.error.HTTPError as e:
        data = ""
        try:
            data = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        resp_headers = dict(e.headers.items()) if getattr(e, "headers", None) else {}
        return int(e.code), data, resp_headers


class TestUnsubPrefsEndpoints(unittest.TestCase):
    def setUp(self) -> None:
        self._env_before = dict(os.environ)
        os.environ["UNSUB_SECRET"] = "test_unsub_secret"
        os.environ["UNSUB_TOKEN_TTL_DAYS"] = "45"
        # Avoid cross-test flakiness from the per-subscriber_key preview throttle.
        os.environ["PREFS_PREVIEW_RATE_LIMIT_S"] = "0"
        os.environ["MFO_INTERNAL_API_KEY"] = "test_internal_api_key"

        self._tmp = tempfile.TemporaryDirectory()
        out = Path(self._tmp.name)

        # Create a tiny SQLite DB for the immediate low-priority preview on the enable_lows confirmation page.
        # The unsubscribe server reads this path via env.
        self._db_path = out / "osha.sqlite"
        conn = sqlite3.connect(str(self._db_path))
        conn.execute(
            """
            CREATE TABLE inspections (
                activity_nr TEXT,
                date_opened TEXT,
                inspection_type TEXT,
                scope TEXT,
                case_status TEXT,
                establishment_name TEXT,
                site_city TEXT,
                site_state TEXT,
                site_zip TEXT,
                area_office TEXT,
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
        now = datetime.now()
        opened = now.date().isoformat()
        first_seen = (now - timedelta(days=1)).isoformat()
        last_seen = now.isoformat()
        conn.execute(
            """
            INSERT INTO inspections (
                activity_nr, date_opened, inspection_type, scope, case_status, establishment_name,
                site_city, site_state, site_zip, area_office, naics, naics_desc, violations_count,
                emphasis, lead_score, first_seen_at, last_seen_at, changed_at, source_url, parse_invalid
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "A-LOW-1",
                opened,
                "Planned",
                "General",
                "Open",
                "LowCo",
                "Austin",
                "TX",
                "78701",
                "",
                "000000",
                "",
                0,
                "",
                1,
                first_seen,
                last_seen,
                None,
                "https://example.com/low",
                0,
            ),
        )
        # Out-of-territory low row should be excluded by territory filter.
        conn.execute(
            """
            INSERT INTO inspections (
                activity_nr, date_opened, inspection_type, scope, case_status, establishment_name,
                site_city, site_state, site_zip, area_office, naics, naics_desc, violations_count,
                emphasis, lead_score, first_seen_at, last_seen_at, changed_at, source_url, parse_invalid
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "A-LOW-2",
                opened,
                "Planned",
                "General",
                "Open",
                "FarCo",
                "El Paso",
                "TX",
                "79901",
                "",
                "000000",
                "",
                0,
                "",
                1,
                first_seen,
                last_seen,
                None,
                "https://example.com/far",
                0,
            ),
        )
        conn.commit()
        conn.close()
        os.environ["OSHA_DB_PATH"] = str(self._db_path)

        # Patch storage paths into the temp dir so tests don't touch repo state.
        self._orig_paths = {
            "OUT_DIR": unsubscribe_utils.OUT_DIR,
            "UNSUB_TOKEN_STORE_PATH": unsubscribe_utils.UNSUB_TOKEN_STORE_PATH,
            "SUPPRESSION_PATH": unsubscribe_utils.SUPPRESSION_PATH,
            "UNSUBSCRIBE_EVENTS_PATH": unsubscribe_utils.UNSUBSCRIBE_EVENTS_PATH,
            "PREFS_PATH": unsubscribe_utils.PREFS_PATH,
        }
        unsubscribe_utils.OUT_DIR = out
        unsubscribe_utils.UNSUB_TOKEN_STORE_PATH = out / "unsub_tokens.csv"
        unsubscribe_utils.SUPPRESSION_PATH = out / "suppression.csv"
        unsubscribe_utils.UNSUBSCRIBE_EVENTS_PATH = out / "unsubscribe_events.csv"
        unsubscribe_utils.PREFS_PATH = out / "prefs.csv"

        self._server = HTTPServer(("127.0.0.1", 0), unsubscribe_server.UnsubHandler)
        self._port = int(self._server.server_port)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def tearDown(self) -> None:
        try:
            self._server.shutdown()
            self._server.server_close()
        except Exception:
            pass

        # Restore patched paths.
        unsubscribe_utils.OUT_DIR = self._orig_paths["OUT_DIR"]
        unsubscribe_utils.UNSUB_TOKEN_STORE_PATH = self._orig_paths["UNSUB_TOKEN_STORE_PATH"]
        unsubscribe_utils.SUPPRESSION_PATH = self._orig_paths["SUPPRESSION_PATH"]
        unsubscribe_utils.UNSUBSCRIBE_EVENTS_PATH = self._orig_paths["UNSUBSCRIBE_EVENTS_PATH"]
        unsubscribe_utils.PREFS_PATH = self._orig_paths["PREFS_PATH"]

        try:
            self._tmp.cleanup()
        except Exception:
            pass

        os.environ.clear()
        os.environ.update(self._env_before)

    def _base(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def test_routes_exist(self) -> None:
        for path in ["/prefs/enable_lows", "/prefs/disable_lows"]:
            status, _ = _http(self._base() + path, method="HEAD")
            self.assertEqual(200, status)
        status, _, headers = _http_full(self._base() + "/__version", method="GET")
        self.assertEqual(200, status)
        self.assertIn("X-MFO-Unsub-SHA", headers)

    def test_enable_then_disable_updates_preference(self) -> None:
        email = "recipient@example.com"
        territory = "TX_TRIANGLE_V1"
        campaign_id = f"prefs|wally_trial|terr={territory}"
        token = unsubscribe_utils.create_unsub_token(email, campaign_id)
        # Repro: subscriber_key in the email link is not derived from the clicked recipient's email
        # (fanout recipients share a single subscriber_key).
        subscriber_key = "wally_trial"

        with redirect_stdout(io.StringIO()):
            status, body = _http(
                self._base()
                + f"/prefs/enable_lows?token={token}&subscriber_key={subscriber_key}&territory_code={territory}"
            )
        self.assertEqual(200, status)
        self.assertIn("Preference updated", body)
        self.assertIn("Low-priority signals enabled", body)
        self.assertIn("Recent low-priority preview (last 14 days)", body)
        self.assertIn("LowCo", body)
        self.assertNotIn("FarCo", body)
        self.assertTrue(unsubscribe_utils.get_include_lows_pref(email, subscriber_key, territory))

        with redirect_stdout(io.StringIO()):
            # Back-compat: accept both token= and t=.
            status, body = _http(
                self._base()
                + f"/prefs/disable_lows?t={token}&subscriber_key={subscriber_key}&territory_code={territory}"
            )
        self.assertEqual(200, status)
        self.assertIn("Preference updated", body)
        self.assertFalse(unsubscribe_utils.get_include_lows_pref(email, subscriber_key, territory))

    def test_api_prefs_requires_key_and_reflects_latest_subscriber_preference(self) -> None:
        email = "recipient@example.com"
        territory = "TX_TRIANGLE_V1"
        campaign_id = f"prefs|wally_trial|terr={territory}"
        token = unsubscribe_utils.create_unsub_token(email, campaign_id)
        subscriber_key = "wally_trial"

        # Unauthorized without header.
        status, _, _ = _http_full(self._base() + f"/api/prefs?subscriber_key={subscriber_key}&territory_code={territory}")
        self.assertEqual(401, status)

        with redirect_stdout(io.StringIO()):
            status, _ = _http(
                self._base()
                + f"/prefs/enable_lows?token={token}&subscriber_key={subscriber_key}&territory_code={territory}"
            )
        self.assertEqual(200, status)

        status, body, _ = _http_full_headers(
            self._base() + f"/api/prefs?subscriber_key={subscriber_key}&territory_code={territory}",
            headers={"X-MFO-API-Key": "test_internal_api_key"},
        )
        self.assertEqual(200, status)
        import json as _json

        payload = _json.loads(body)
        self.assertTrue(bool(payload.get("lows_enabled")))

        with redirect_stdout(io.StringIO()):
            status, _ = _http(
                self._base()
                + f"/prefs/disable_lows?token={token}&subscriber_key={subscriber_key}&territory_code={territory}"
            )
        self.assertEqual(200, status)

        status, body, _ = _http_full_headers(
            self._base() + f"/api/prefs?subscriber_key={subscriber_key}&territory_code={territory}",
            headers={"X-MFO-API-Key": "test_internal_api_key"},
        )
        self.assertEqual(200, status)
        payload = _json.loads(body)
        self.assertFalse(bool(payload.get("lows_enabled")))

    def test_enable_lows_preview_missing_still_200_persists_and_warns(self) -> None:
        email = "recipient@example.com"
        territory = "TX_TRIANGLE_V1"
        campaign_id = f"prefs|wally_trial|terr={territory}"
        token = unsubscribe_utils.create_unsub_token(email, campaign_id)
        subscriber_key = "wally_trial"

        # Force preview DB missing/unavailable.
        os.environ["OSHA_DB_PATH"] = str(Path(self._tmp.name) / "missing.sqlite")

        buf = io.StringIO()
        with redirect_stdout(buf):
            status, body = _http(
                self._base()
                + f"/prefs/enable_lows?token={token}&subscriber_key={subscriber_key}&territory_code={territory}"
            )
        self.assertEqual(200, status)
        self.assertTrue(unsubscribe_utils.get_include_lows_pref(email, subscriber_key, territory))

        # Header must stay stable for UX/tests even if data is unavailable.
        self.assertIn("Recent low-priority preview (last 14 days)", body)
        self.assertIn("Preview unavailable right now.", body)

        # Structured warning log: best-effort, no 500.
        logs = buf.getvalue()
        self.assertIn("\"event\":\"PREVIEW_UNAVAILABLE\"", logs)
        self.assertIn("\"subscriber_key\":\"wally_trial\"", logs)
        self.assertIn("\"territory_code\":\"TX_TRIANGLE_V1\"", logs)

    def test_invalid_token_rejected(self) -> None:
        status, body, headers = _http_full(
            self._base()
            + "/prefs/enable_lows?token=invalid.invalid&territory_code=TX_TRIANGLE_V1&subscriber_key=sub_tx_triangle_v1_0000000000"
        )
        self.assertEqual(400, status)
        self.assertIn("invalid", body.lower())
        self.assertIn("X-MFO-Unsub-SHA", headers)
        self.assertNotIn("subscriber_key format", body.lower())

    def test_missing_campaign_territory_still_works_with_query_params(self) -> None:
        email = "recipient@example.com"
        territory = "TX_TRIANGLE_V1"
        token = unsubscribe_utils.create_unsub_token(email, "prefs|wally_trial")
        subscriber_key = "wally_trial"
        status, body = _http(
            self._base() + f"/prefs/enable_lows?t={token}&subscriber_key={subscriber_key}&territory_code={territory}"
        )
        self.assertEqual(200, status)
        self.assertIn("preference updated", body.lower())

    def test_missing_territory_code_rejected(self) -> None:
        email = "recipient@example.com"
        territory = "TX_TRIANGLE_V1"
        token = unsubscribe_utils.create_unsub_token(email, f"prefs|wally_trial|terr={territory}")
        subscriber_key = "wally_trial"
        status, body = _http(self._base() + f"/prefs/enable_lows?t={token}&subscriber_key={subscriber_key}")
        self.assertEqual(400, status)
        self.assertIn("territory_code", body.lower())

    def test_valid_token_with_fanout_subscriber_key_succeeds_and_writes_prefs(self) -> None:
        # Reproduce the production failure: token email != subscriber_key owner.
        primary = "primary@example.com"
        fanout = "coworker@example.com"
        territory = "TX_TRIANGLE_V1"
        subscriber_key = "wally_trial"

        token = unsubscribe_utils.create_unsub_token(fanout, f"prefs|wally_trial|terr={territory}")
        status, body = _http(
            self._base() + f"/prefs/enable_lows?token={token}&subscriber_key={subscriber_key}&territory_code={territory}"
        )
        self.assertEqual(200, status)
        self.assertIn("preference updated", body.lower())

        # Writes should be keyed by (recipient_from_token, subscriber_key, territory_code).
        self.assertTrue(unsubscribe_utils.get_include_lows_pref(fanout, subscriber_key, territory))
        # Ensure we did not implicitly toggle a legacy (subscriber_key blank) record.
        self.assertFalse(unsubscribe_utils.get_include_lows_pref(fanout, None, territory))
        # Ensure this does not affect the primary recipient's prefs.
        self.assertFalse(unsubscribe_utils.get_include_lows_pref(primary, subscriber_key, territory))

    def test_realistic_subscriber_key_allowed_and_invalid_subscriber_key_rejected(self) -> None:
        email = "recipient@example.com"
        territory = "TX_TRIANGLE_V1"
        token = unsubscribe_utils.create_unsub_token(email, f"prefs|wally_trial|terr={territory}")
        import urllib.parse

        # Realistic link key: letters/digits/._- only.
        subscriber_key = "wally.trial-2026_v1"
        status, body, headers = _http_full(
            self._base() + f"/prefs/enable_lows?token={token}&subscriber_key={subscriber_key}&territory_code={territory}"
        )
        self.assertEqual(200, status)
        self.assertIn("preference updated", body.lower())
        self.assertIn("X-MFO-Unsub-SHA", headers)

        # Truly invalid key: contains space.
        bad_key = "wally trial"
        qs = urllib.parse.urlencode({"token": token, "subscriber_key": bad_key, "territory_code": territory})
        status, body, _ = _http_full(self._base() + f"/prefs/enable_lows?{qs}")
        self.assertEqual(400, status)
        self.assertIn("subscriber_key", body.lower())
        self.assertIn("wally trial", body.lower())

        # Truly invalid key: too long (>80).
        too_long = "a" * 81
        qs = urllib.parse.urlencode({"token": token, "subscriber_key": too_long, "territory_code": territory})
        status, body, _ = _http_full(self._base() + f"/prefs/enable_lows?{qs}")
        self.assertEqual(400, status)
        self.assertIn("subscriber_key", body.lower())

    def test_version_endpoint_returns_same_sha_as_header(self) -> None:
        status, body, headers = _http_full(self._base() + "/__version")
        self.assertEqual(200, status)
        self.assertIn("X-MFO-Unsub-SHA", headers)
        sha_header = (headers.get("X-MFO-Unsub-SHA") or "").strip()
        self.assertTrue(sha_header)

        import json as _json
        payload = _json.loads(body)
        self.assertEqual(payload.get("git_sha"), sha_header)


if __name__ == "__main__":
    unittest.main()
