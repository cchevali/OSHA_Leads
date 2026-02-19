import csv
import gzip
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from geo import zip_cbsa
from lead_filters import filter_by_territory


class TestZipCbsaResolution(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._tmp_path = Path(self._tmp.name)
        self._orig_zip_to_cbsa = zip_cbsa.ZIP_TO_CBSA_PATH
        self._orig_cbsa_meta = zip_cbsa.CBSA_META_PATH
        self._orig_dataset_meta = zip_cbsa.ZIP_TO_CBSA_DATASET_META_PATH
        self._orig_sources_path = zip_cbsa.ZIP_TO_CBSA_SOURCES_PATH

        zip_map_path = self._tmp_path / "zip_to_cbsa.csv.gz"
        with gzip.open(zip_map_path, "wt", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["ZIP5", "CBSA"])
            writer.writerow(["75035", "19100"])
            writer.writerow(["78701", "12420"])

        meta_path = self._tmp_path / "cbsa_meta.csv"
        with open(meta_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["CBSA", "metro_label"])
            writer.writerow(["19100", "Dallas-Fort Worth-Arlington, TX"])
            writer.writerow(["12420", "Austin-Round Rock-Georgetown, TX"])

        dataset_meta_path = self._tmp_path / "zip_to_cbsa.meta.json"
        dataset_meta_path.write_text(
            json.dumps(
                {
                    "source_label": "HUD USPS ZIP-CBSA 2025 Q4",
                    "dataset_incomplete": False,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        sources_path = self._tmp_path / "SOURCES.md"
        sources_path.write_text("# test\n", encoding="utf-8")

        zip_cbsa.ZIP_TO_CBSA_PATH = zip_map_path
        zip_cbsa.CBSA_META_PATH = meta_path
        zip_cbsa.ZIP_TO_CBSA_DATASET_META_PATH = dataset_meta_path
        zip_cbsa.ZIP_TO_CBSA_SOURCES_PATH = sources_path
        zip_cbsa.clear_caches()

    def tearDown(self) -> None:
        zip_cbsa.ZIP_TO_CBSA_PATH = self._orig_zip_to_cbsa
        zip_cbsa.CBSA_META_PATH = self._orig_cbsa_meta
        zip_cbsa.ZIP_TO_CBSA_DATASET_META_PATH = self._orig_dataset_meta
        zip_cbsa.ZIP_TO_CBSA_SOURCES_PATH = self._orig_sources_path
        zip_cbsa.clear_caches()
        self._tmp.cleanup()

    def test_resolve_zip_75035_to_dfw_cbsa(self) -> None:
        self.assertEqual(zip_cbsa.resolve_cbsa("75035"), "19100")
        self.assertEqual(zip_cbsa.resolve_metro_label("19100"), "Dallas-Fort Worth-Arlington, TX")

    def test_tx_tri_matches_frisco_zip_and_alias_parity(self) -> None:
        lead = {
            "activity_nr": "1874533",
            "site_state": "TX",
            "site_city": "Frisco",
            "site_zip": "75035",
            "mail_zip": "",
            "area_office": "",
        }
        expected = None
        for code in ("TX_TRI", "TX_TRIANGLE_V1", "TX_TRIANGLE", "TX_TRI_V1"):
            filtered, stats, debug_rows = filter_by_territory([lead], code, include_debug=True)
            self.assertEqual(len(filtered), 1)
            self.assertEqual(stats["matched_by_cbsa"], 1)
            self.assertEqual(debug_rows[0]["match_reason"], "CBSA_MATCH")
            if expected is None:
                expected = stats
            else:
                self.assertEqual(stats, expected)

    def test_fallback_is_only_used_when_cbsa_unresolved(self) -> None:
        unknown_zip_office = {
            "activity_nr": "x1",
            "site_state": "TX",
            "site_city": "Frisco",
            "site_zip": "99999",
            "mail_zip": "",
            "area_office": "Dallas Area Office",
        }
        filtered, stats, debug_rows = filter_by_territory([unknown_zip_office], "TX_TRI", include_debug=True)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(stats["matched_by_office"], 1)
        self.assertEqual(debug_rows[0]["match_reason"], "FALLBACK_USED|OFFICE_MATCH|ZIP_UNKNOWN")

        missing_zip_office = {
            "activity_nr": "x2",
            "site_state": "TX",
            "site_city": "Plano",
            "site_zip": "",
            "mail_zip": "",
            "area_office": "Dallas Area Office",
        }
        filtered2, stats2, debug_rows2 = filter_by_territory([missing_zip_office], "TX_TRI", include_debug=True)
        self.assertEqual(len(filtered2), 1)
        self.assertEqual(stats2["matched_by_office"], 1)
        self.assertEqual(debug_rows2[0]["match_reason"], "FALLBACK_USED|OFFICE_MATCH|ZIP_MISSING")

    def test_seed_dataset_warning_token_emitted_once(self) -> None:
        seed_meta_path = self._tmp_path / "zip_to_cbsa.meta.json"
        seed_meta_path.write_text(
            json.dumps(
                {
                    "source_label": "HUD USPS ZIP-CBSA seed bootstrap (coverage incomplete)",
                    "dataset_incomplete": True,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        zip_cbsa.ZIP_TO_CBSA_DATASET_META_PATH = seed_meta_path
        zip_cbsa.clear_caches()

        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertEqual(zip_cbsa.resolve_cbsa("75035"), "19100")
            self.assertEqual(zip_cbsa.resolve_cbsa("78701"), "12420")

        output = buf.getvalue()
        token = 'WARN_ZIP_CBSA_DATASET_INCOMPLETE source_label="HUD USPS ZIP-CBSA seed bootstrap (coverage incomplete)"'
        self.assertIn(token, output)
        self.assertEqual(output.count("WARN_ZIP_CBSA_DATASET_INCOMPLETE"), 1)


if __name__ == "__main__":
    unittest.main()
