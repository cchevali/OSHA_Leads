import json
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
        self.assertNotIn("Sample refresh is delayed.", combined_text)

    def test_founder_and_disclaimer_strings_exist(self):
        page_text = SAMPLE_PAGE.read_text(encoding="utf-8")
        self.assertIn(
            "Built by a data engineer for earlier visibility into public OSHA activity.",
            page_text,
        )
        self.assertIn("Not legal advice.", page_text)
        self.assertIn("Frozen public sample", page_text)
        self.assertIn("Verify in 30 seconds", page_text)
        self.assertIn("We confirm mapping before billing", page_text)
        self.assertIn("Why this was actionable", page_text)

    def test_sample_snapshot_schema_is_populated(self):
        page_text = SAMPLE_PAGE.read_text(encoding="utf-8")
        self.assertIn("const [snapshot] = sampleSignals as SampleTerritory[];", page_text)

        sample_data = json.loads(SAMPLE_DATA.read_text(encoding="utf-8"))
        self.assertIsInstance(sample_data, list)
        self.assertGreaterEqual(len(sample_data), 1)
        first = sample_data[0]
        self.assertIn("territory_id", first)
        self.assertIn("territory_name", first)
        self.assertIn("updated_at_utc", first)
        self.assertIn("rows", first)
        self.assertIsInstance(first["rows"], list)
        self.assertGreaterEqual(len(first["rows"]), 1)
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
