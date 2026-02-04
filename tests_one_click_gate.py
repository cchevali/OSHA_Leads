import os
import unittest
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
        ok = oce.preflight_one_click("test@example.com", "camp", dry_run=False, require_one_click=False)
        self.assertFalse(ok)

    @patch("outbound_cold_email.register_unsub_token")
    def test_preflight_required_blocks_failure(self, mock_register):
        mock_register.return_value = (False, 500, "boom")
        with self.assertRaises(RuntimeError):
            oce.preflight_one_click("test@example.com", "camp", dry_run=False, require_one_click=True)

    @patch("outbound_cold_email.register_unsub_token")
    def test_preflight_success(self, mock_register):
        mock_register.return_value = (True, 200, "")
        ok = oce.preflight_one_click("test@example.com", "camp", dry_run=False, require_one_click=True)
        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()
