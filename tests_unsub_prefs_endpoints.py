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


class TestUnsubPrefsEndpoints(unittest.TestCase):
    def setUp(self) -> None:
        self._env_before = dict(os.environ)
        os.environ["UNSUB_SECRET"] = "test_unsub_secret"
        os.environ["UNSUB_TOKEN_TTL_DAYS"] = "45"

        self._tmp = tempfile.TemporaryDirectory()
        out = Path(self._tmp.name)

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

    def test_enable_then_disable_updates_preference(self) -> None:
        email = "recipient@example.com"
        campaign_id = "prefs|wally_trial|terr=TX_TRIANGLE_V1"
        token = unsubscribe_utils.create_unsub_token(email, campaign_id)

        with redirect_stdout(io.StringIO()):
            status, body = _http(self._base() + f"/prefs/enable_lows?t={token}")
        self.assertEqual(200, status)
        self.assertIn("Preference updated", body)
        self.assertTrue(unsubscribe_utils.get_include_lows_pref(email, "TX_TRIANGLE_V1"))

        with redirect_stdout(io.StringIO()):
            status, body = _http(self._base() + f"/prefs/disable_lows?t={token}")
        self.assertEqual(200, status)
        self.assertIn("Preference updated", body)
        self.assertFalse(unsubscribe_utils.get_include_lows_pref(email, "TX_TRIANGLE_V1"))

    def test_invalid_token_rejected(self) -> None:
        status, body = _http(self._base() + "/prefs/enable_lows?t=invalid.invalid")
        self.assertEqual(400, status)
        self.assertIn("invalid", body.lower())

    def test_missing_territory_rejected(self) -> None:
        email = "recipient@example.com"
        # Missing terr=... in campaign_id
        token = unsubscribe_utils.create_unsub_token(email, "prefs|wally_trial")
        status, body = _http(self._base() + f"/prefs/enable_lows?t={token}")
        self.assertEqual(400, status)
        self.assertIn("territory", body.lower())


if __name__ == "__main__":
    unittest.main()
