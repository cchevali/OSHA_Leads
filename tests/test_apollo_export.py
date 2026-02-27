import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest import mock

from tools import apollo_export


class TestApolloExport(unittest.TestCase):
    def _run(self, argv: list[str], env_overrides: dict[str, str | None] | None = None) -> tuple[int, str]:
        merged = dict(os.environ)
        for key, value in (env_overrides or {}).items():
            if value is None:
                merged.pop(key, None)
            else:
                merged[key] = value
        buf = io.StringIO()
        with mock.patch.dict(os.environ, merged, clear=True):
            with redirect_stdout(buf):
                rc = apollo_export.main(argv)
        return rc, buf.getvalue()

    def test_print_config_with_data_dir_set(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            with mock.patch("tools.apollo_export._check_playwright_runtime", return_value=(True, "")):
                rc, out = self._run(["--print-config"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(rc, 0, msg=out)
            self.assertIn(f"apollo_export_profile_path={(data_dir / 'apollo_export' / 'browser_profile').resolve()}", out)
            self.assertIn(f"apollo_export_inbox_path={(data_dir / 'prospect_generation' / 'inbox').resolve()}", out)
            self.assertIn("apollo_export_playwright_installed=YES", out)
            self.assertIn("browser_channel=chrome", out)
            self.assertNotIn("WARN_APOLLO_EXPORT_DATA_DIR_UNSET", out)

    def test_print_config_without_data_dir_emits_warn(self):
        with mock.patch("tools.apollo_export._check_playwright_runtime", return_value=(True, "")):
            rc, out = self._run(["--print-config"], {"DATA_DIR": None})
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("WARN_APOLLO_EXPORT_DATA_DIR_UNSET", out)
        self.assertIn("browser_channel=chrome", out)
        self.assertIn(f"apollo_export_profile_path={(apollo_export.REPO_ROOT / 'out' / 'apollo_export' / 'browser_profile').resolve()}", out)

    def test_print_config_chrome_channel_override(self):
        with mock.patch("tools.apollo_export._check_playwright_runtime", return_value=(True, "")):
            rc, out = self._run(["--print-config", "--chrome-channel", "chromium"])
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("browser_channel=chromium", out)

    def test_missing_search_url_errors_for_dry_run(self):
        rc, out = self._run(["--dry-run"])
        self.assertEqual(rc, 1, msg=out)
        self.assertIn("ERR_APOLLO_EXPORT_NO_SEARCH_URL", out)

    def test_missing_search_url_errors_for_live(self):
        rc, out = self._run([])
        self.assertEqual(rc, 1, msg=out)
        self.assertIn("ERR_APOLLO_EXPORT_NO_SEARCH_URL", out)

    def test_missing_profile_errors(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            rc, out = self._run(["--search-url", "https://app.apollo.io/#/people"], {"DATA_DIR": str(data_dir)})
        self.assertEqual(rc, 1, msg=out)
        self.assertIn("ERR_APOLLO_EXPORT_NO_PROFILE", out)

    def test_missing_playwright_errors(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            profile_path = data_dir / "apollo_export" / "browser_profile"
            profile_path.mkdir(parents=True, exist_ok=True)
            with mock.patch("tools.apollo_export._check_playwright_runtime", return_value=(False, "missing")):
                rc, out = self._run(["--search-url", "https://app.apollo.io/#/people"], {"DATA_DIR": str(data_dir)})
        self.assertEqual(rc, 1, msg=out)
        self.assertIn("ERR_APOLLO_EXPORT_PLAYWRIGHT_MISSING", out)

    def test_output_filename_convention(self):
        name = apollo_export._build_output_filename(datetime(2026, 2, 27, 15, 4, 5))
        self.assertEqual(name, "apollo_export_20260227_150405.csv")

    def test_no_results_warn_is_exit_zero_with_status_ok(self):
        stats = apollo_export.ExportStats(status="OK")
        with mock.patch(
            "tools.apollo_export._run_export_mode",
            return_value=(stats, apollo_export.WARN_APOLLO_EXPORT_NO_RESULTS),
        ):
            rc, out = self._run(["--search-url", "https://app.apollo.io/#/people"])
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("WARN_APOLLO_EXPORT_NO_RESULTS", out)
        self.assertIn("APOLLO_EXPORT_COMPLETE=status=OK", out)


if __name__ == "__main__":
    unittest.main()
