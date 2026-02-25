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
        self.assertNotIn("EXAMPLE_TERRITORY", combined_text)
        self.assertNotIn("dummy data", combined_text)
        self.assertNotIn("dummy", combined_text)

    def test_founder_and_disclaimer_strings_exist(self):
        page_text = SAMPLE_PAGE.read_text(encoding="utf-8")
        self.assertIn(
            "I'm Chase. I built MicroFlowOps to surface public OSHA inspection activity faster than teams can find it manually.",
            page_text,
        )
        self.assertIn("Not legal advice.", page_text)
        self.assertIn("Nationwide sample: multiple metros", page_text)
        self.assertIn("Verify in 30 seconds", page_text)

    def test_above_fold_territory_limit_is_enforced(self):
        page_text = SAMPLE_PAGE.read_text(encoding="utf-8")
        match = re.search(r"const ABOVE_FOLD_TERRITORY_LIMIT = (\d+);", page_text)
        self.assertIsNotNone(match)
        if match is None:
            self.fail("ABOVE_FOLD_TERRITORY_LIMIT constant missing from sample page.")
        above_fold_limit = int(match.group(1))
        self.assertGreaterEqual(above_fold_limit, 2)
        self.assertLessEqual(above_fold_limit, 5)
        self.assertIn(".slice(0, ABOVE_FOLD_TERRITORY_LIMIT)", page_text)

        sample_data = json.loads(SAMPLE_DATA.read_text(encoding="utf-8"))
        self.assertIsInstance(sample_data, list)
        self.assertGreaterEqual(len(sample_data), above_fold_limit)
        first = sample_data[0]
        self.assertIn("territory_id", first)
        self.assertIn("territory_name", first)
        self.assertIn("updated_at_utc", first)
        self.assertIn("rows", first)
        self.assertIsInstance(first["rows"], list)
        if first["rows"]:
            row = first["rows"][0]
            self.assertEqual(
                set(row.keys()),
                {
                    "activity_nr",
                    "inspection_type",
                    "establishment_name",
                    "city",
                    "state",
                    "opened_date",
                    "observed_at_utc",
                    "source_url",
                },
            )


if __name__ == "__main__":
    unittest.main()
