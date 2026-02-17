import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SAMPLE_PAGE = ROOT / "web" / "app" / "sample" / "page.tsx"
SAMPLE_DATA = ROOT / "web" / "app" / "sample" / "sample_signals.json"


class TestWebSamplePagePreview(unittest.TestCase):
    def test_no_template_placeholder_strings(self):
        combined_text = SAMPLE_PAGE.read_text(encoding="utf-8") + "\n" + SAMPLE_DATA.read_text(
            encoding="utf-8"
        )
        self.assertNotIn("Example Company", combined_text)
        self.assertNotIn("EXAMPLE_TERRITORY", combined_text)
        self.assertNotIn("dummy data", combined_text)
        self.assertNotIn("dummy", combined_text)

    def test_disclaimer_string_exists(self):
        page_text = SAMPLE_PAGE.read_text(encoding="utf-8")
        self.assertIn(
            "Company names are anonymized. Not affiliated with OSHA or any government agency.",
            page_text,
        )

    def test_above_fold_card_limit_is_enforced(self):
        page_text = SAMPLE_PAGE.read_text(encoding="utf-8")
        match = re.search(r"const ABOVE_FOLD_CARD_LIMIT = (\d+);", page_text)
        self.assertIsNotNone(match)
        if match is None:
            self.fail("ABOVE_FOLD_CARD_LIMIT constant missing from sample page.")
        above_fold_limit = int(match.group(1))
        self.assertGreaterEqual(above_fold_limit, 3)
        self.assertLessEqual(above_fold_limit, 5)
        self.assertIn(".slice(0, ABOVE_FOLD_CARD_LIMIT)", page_text)

        sample_data = json.loads(SAMPLE_DATA.read_text(encoding="utf-8"))
        self.assertGreater(len(sample_data), above_fold_limit)


if __name__ == "__main__":
    unittest.main()
