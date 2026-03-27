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

    def test_sample_page_uses_outreach_ready_trust_language(self):
        page_text = SAMPLE_PAGE.read_text(encoding="utf-8")
        self.assertIn("Sample OSHA Lead Digest", page_text)
        self.assertIn("Frozen sample digest", page_text)
        self.assertIn("Sample = one example digest for your state or region.", page_text)
        self.assertIn("Public example using OSHA data.", page_text)
        self.assertIn("Not affiliated with OSHA. Not legal advice.", page_text)
        self.assertIn("Verify in 30 seconds", page_text)
        self.assertIn("What is included", page_text)
        self.assertIn("What usable means", page_text)
        self.assertIn("What a buyer can confirm", page_text)
        self.assertIn("Founding Pilot", page_text)
        self.assertNotIn("Sample: alert and OSHA record", page_text)
        self.assertNotIn("We confirm mapping before billing", page_text)
        self.assertNotIn("Proof snapshot", page_text)

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
