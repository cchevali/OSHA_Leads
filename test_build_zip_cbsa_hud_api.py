import csv
import gzip
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


def _load_build_zip_cbsa_module():
    module_path = Path(__file__).resolve().parent / "tools" / "build_zip_cbsa.py"
    spec = importlib.util.spec_from_file_location("build_zip_cbsa_module", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load tools/build_zip_cbsa.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


build_zip_cbsa = _load_build_zip_cbsa_module()


class TestBuildZipCbsaHudApi(unittest.TestCase):
    def test_help_includes_hud_api_flags(self) -> None:
        buf = io.StringIO()
        with mock.patch.object(sys, "argv", ["build_zip_cbsa.py", "--help"]):
            with self.assertRaises(SystemExit) as ctx, redirect_stdout(buf):
                build_zip_cbsa.main()
        self.assertEqual(ctx.exception.code, 0)
        out = buf.getvalue()
        self.assertIn("--hud-api", out)
        self.assertIn("--hud-year", out)
        self.assertIn("--hud-quarter", out)
        self.assertIn("--hud-cache-root", out)

    def test_hud_api_requires_token(self) -> None:
        argv = ["build_zip_cbsa.py", "--hud-api"]
        buf = io.StringIO()
        with mock.patch.object(sys, "argv", argv), mock.patch.dict(os.environ, {}, clear=True), redirect_stdout(buf):
            rc = build_zip_cbsa.main()
        out = buf.getvalue()
        self.assertEqual(rc, 1)
        self.assertIn("ERR_HUD_API_TOKEN_MISSING env=HUD_API_TOKEN", out)
        self.assertNotIn("Traceback", out)

    def test_hud_api_mode_uses_mocked_response_and_writes_normalized_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            out_path = tmp / "zip_to_cbsa.csv.gz"
            meta_path = tmp / "cbsa_meta.csv"
            zip_meta_path = tmp / "zip_to_cbsa.meta.json"
            sources_path = tmp / "SOURCES.md"
            cache_root = tmp / "cache"

            payload = {
                "data": [
                    {
                        "year": "2026",
                        "quarter": "Q1",
                        "input": "76574",
                        "crosswalk_type": "zip-cbsa",
                        "results": [
                            {"geoid": "12420", "res_ratio": 0.9},
                            {"geoid": "19100", "res_ratio": 0.1},
                        ],
                    },
                    {
                        "year": "2026",
                        "quarter": "Q1",
                        "input": "77396",
                        "crosswalk_type": "zip-cbsa",
                        "results": [{"geoid": "26420", "res_ratio": 0.8}],
                    },
                    {
                        "year": "2026",
                        "quarter": "Q1",
                        "input": "75703",
                        "crosswalk_type": "zip-cbsa",
                        "results": [{"geoid": "46340", "res_ratio": 0.7}],
                    },
                ]
            }

            def fake_hud_get_json(*, query: str, token: str, year: int | None, quarter: int | None):
                self.assertEqual(query, "TX")
                self.assertEqual(token, "test_token")
                self.assertEqual(year, 2026)
                self.assertEqual(quarter, 1)
                return payload, "https://www.huduser.gov/hudapi/public/usps?type=3&query=TX&year=2026&quarter=1"

            argv = [
                "build_zip_cbsa.py",
                "--hud-api",
                "--hud-year",
                "2026",
                "--hud-quarter",
                "1",
                "--hud-cache-root",
                str(cache_root),
                "--out",
                str(out_path),
                "--meta",
                str(meta_path),
                "--zip-meta-json",
                str(zip_meta_path),
                "--sources",
                str(sources_path),
            ]

            with mock.patch.object(sys, "argv", argv), mock.patch.dict(
                os.environ,
                {"HUD_API_TOKEN": "test_token"},
                clear=True,
            ), mock.patch.object(
                build_zip_cbsa,
                "HUD_API_STATE_CODES",
                ("TX",),
            ), mock.patch.object(
                build_zip_cbsa,
                "_hud_get_json",
                side_effect=fake_hud_get_json,
            ):
                rc = build_zip_cbsa.main()
            self.assertEqual(rc, 0)

            with gzip.open(out_path, "rt", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                mapping = {row["ZIP5"]: row["CBSA"] for row in reader}
            self.assertEqual(mapping.get("76574"), "12420")
            self.assertEqual(mapping.get("77396"), "26420")
            self.assertEqual(mapping.get("75703"), "46340")

            cache_state_path = cache_root / "2026_Q1" / "state_TX.json"
            self.assertTrue(cache_state_path.exists())
            cached = json.loads(cache_state_path.read_text(encoding="utf-8"))
            self.assertEqual(cached.get("type"), "3")
            self.assertEqual(cached.get("year"), 2026)
            self.assertEqual(cached.get("quarter"), "Q1")

    def test_explicit_period_404_falls_back_to_latest_and_completes(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            out_path = tmp / "zip_to_cbsa.csv.gz"
            meta_path = tmp / "cbsa_meta.csv"
            zip_meta_path = tmp / "zip_to_cbsa.meta.json"
            sources_path = tmp / "SOURCES.md"
            cache_root = tmp / "cache"

            latest_payload = {
                "data": [
                    {
                        "year": "2025",
                        "quarter": "Q4",
                        "input": "22031",
                        "results": [{"geoid": "19100", "res_ratio": 1.0}],
                    }
                ]
            }
            state_payload = {
                "data": [
                    {
                        "year": "2025",
                        "quarter": "Q4",
                        "input": "76574",
                        "results": [{"geoid": "12420", "res_ratio": 1.0}],
                    }
                ]
            }
            calls: list[tuple[str, int | None, int | None]] = []

            def fake_hud_get_json(*, query: str, token: str, year: int | None, quarter: int | None):
                calls.append((query, year, quarter))
                if query == "22031":
                    return latest_payload, "https://www.huduser.gov/hudapi/public/usps?type=3&query=22031"
                if query == "TX" and year == 2026 and quarter == 1:
                    raise build_zip_cbsa.HudApiRequestError(
                        query="TX",
                        status=404,
                        detail=None,
                        url="https://www.huduser.gov/hudapi/public/usps?type=3&query=TX&year=2026&quarter=1",
                    )
                self.assertEqual(year, 2025)
                self.assertEqual(quarter, 4)
                return state_payload, f"https://www.huduser.gov/hudapi/public/usps?type=3&query={query}&year=2025&quarter=4"

            argv = [
                "build_zip_cbsa.py",
                "--hud-api",
                "--hud-year",
                "2026",
                "--hud-quarter",
                "1",
                "--hud-cache-root",
                str(cache_root),
                "--out",
                str(out_path),
                "--meta",
                str(meta_path),
                "--zip-meta-json",
                str(zip_meta_path),
                "--sources",
                str(sources_path),
            ]

            buf = io.StringIO()
            with mock.patch.object(sys, "argv", argv), mock.patch.dict(
                os.environ,
                {"HUD_API_TOKEN": "test_token"},
                clear=True,
            ), mock.patch.object(
                build_zip_cbsa,
                "HUD_API_STATE_CODES",
                ("TX",),
            ), mock.patch.object(
                build_zip_cbsa,
                "_hud_get_json",
                side_effect=fake_hud_get_json,
            ), redirect_stdout(buf):
                rc = build_zip_cbsa.main()
            out = buf.getvalue()
            self.assertEqual(rc, 0)
            self.assertIn("WARN_HUD_API_PERIOD_FALLBACK requested=2026Q1 used=2025Q4", out)
            self.assertTrue(any(query == "22031" and year is None and quarter is None for query, year, quarter in calls))
            self.assertTrue(any(query == "TX" and year == 2025 and quarter == 4 for query, year, quarter in calls))


if __name__ == "__main__":
    unittest.main()
