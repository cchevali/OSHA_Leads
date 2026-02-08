import os
import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import outbound_cold_email as oce


class TestOneClickGate(unittest.TestCase):
    def setUp(self):
        os.environ["UNSUB_ENDPOINT_BASE"] = "https://unsub.example.com/unsubscribe"
        os.environ["UNSUB_SECRET"] = "testsecret"

    def tearDown(self):
        os.environ.pop("UNSUB_ENDPOINT_BASE", None)
        os.environ.pop("UNSUB_SECRET", None)

    @patch("outbound_cold_email.register_unsub_token")
    def test_preflight_optional_allows_failure(self, mock_register):
        mock_register.return_value = (False, 500, "boom")
        buf = io.StringIO()
        with redirect_stdout(buf):
            ok = oce.preflight_one_click("test@example.com", "camp", dry_run=False, require_one_click=False)
        self.assertFalse(ok)
        self.assertIn("one-click preflight failed", buf.getvalue())
        self.assertIn("status=500", buf.getvalue())
        self.assertIn("error=boom", buf.getvalue())
        self.assertTrue(mock_register.called)

    @patch("outbound_cold_email.register_unsub_token")
    def test_preflight_required_blocks_failure(self, mock_register):
        mock_register.return_value = (False, 500, "boom")
        buf = io.StringIO()
        with redirect_stdout(buf):
            with self.assertRaises(RuntimeError):
                oce.preflight_one_click("test@example.com", "camp", dry_run=False, require_one_click=True)
        # Required failure raises before printing a warning.
        self.assertEqual(buf.getvalue(), "")

    @patch("outbound_cold_email.register_unsub_token")
    def test_preflight_success(self, mock_register):
        mock_register.return_value = (True, 200, "")
        buf = io.StringIO()
        with redirect_stdout(buf):
            ok = oce.preflight_one_click("test@example.com", "camp", dry_run=False, require_one_click=True)
        self.assertTrue(ok)
        self.assertIn("One-click preflight succeeded", buf.getvalue())
        self.assertIn("status=200", buf.getvalue())
        self.assertTrue(mock_register.called)


if __name__ == "__main__":
    unittest.main()
