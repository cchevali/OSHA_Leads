import io
import os
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from scoring import ai_triage


class TestAiTriageAutoImport(unittest.TestCase):
    def setUp(self) -> None:
        ai_triage._DISABLED_EMITTED = False
        ai_triage._UNAVAILABLE_EMITTED = False
        ai_triage._AUTO_IMPORT_DONE = False

    def test_get_or_compute_uses_cache_without_openai_key(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            env = {
                "DATA_DIR": str(data_dir),
                "AI_TRIAGE_ENABLED": "1",
                "AI_REVIEW_AUTO_IMPORT_ENABLED": "0",
                "OPENAI_API_KEY": "",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                p_hash = ai_triage.prompt_hash()
                conn = ai_triage.connect_ai_cache()
                try:
                    ai_triage.put_cached(
                        conn,
                        item_key="cache_hit_1",
                        prompt_hash=p_hash,
                        model="manual_import",
                        payload={
                            "priority": "HIGH",
                            "reason": "cached value",
                            "prompt_hash": p_hash,
                            "prompt_version": ai_triage.AI_PROMPT_VERSION,
                            "model": "manual_import",
                            "cached": 1,
                        },
                    )
                finally:
                    conn.close()

                out = io.StringIO()
                with redirect_stdout(out):
                    result = ai_triage.get_or_compute(
                        item_key="cache_hit_1",
                        mode="trial_render",
                        item={"activity_nr": "cache_hit_1"},
                        detail_row={},
                    )
                self.assertIsNotNone(result)
                self.assertEqual(str(result.get("priority")), "HIGH")
                self.assertEqual(int(result.get("cached") or 0), 1)
                self.assertNotIn("missing_openai_api_key", out.getvalue())

    def test_auto_import_loads_newest_csv_and_returns_cached_priority(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            imports_dir = tmp / "imports"
            imports_dir.mkdir(parents=True, exist_ok=True)

            old_csv = imports_dir / "ai_review_20260301.csv"
            old_csv.write_text(
                "activity_nr,ai_priority,ai_reason\n"
                "newest_pick,LOW,older file\n",
                encoding="utf-8",
            )
            new_csv = imports_dir / "ai_review_20260302.csv"
            new_csv.write_text(
                "activity_nr,ai_priority,ai_reason\n"
                "newest_pick,HIGH,newest file\n",
                encoding="utf-8",
            )

            now = time.time()
            os.utime(old_csv, (now - 120, now - 120))
            os.utime(new_csv, (now - 10, now - 10))

            env = {
                "DATA_DIR": str(data_dir),
                "AI_TRIAGE_ENABLED": "1",
                "AI_REVIEW_IMPORT_DIR": str(imports_dir),
                "AI_REVIEW_IMPORT_MAX_AGE_HOURS": "24",
                "OPENAI_API_KEY": "",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                out = io.StringIO()
                with redirect_stdout(out):
                    result = ai_triage.get_or_compute(
                        item_key="newest_pick",
                        mode="trial_render",
                        item={"activity_nr": "newest_pick"},
                        detail_row={},
                    )
                self.assertIsNotNone(result)
                self.assertEqual(str(result.get("priority")), "HIGH")
                self.assertEqual(int(result.get("cached") or 0), 1)
                text = out.getvalue()
                self.assertIn("AI_REVIEW_AUTO_IMPORT_APPLIED", text)
                self.assertIn("ai_review_20260302.csv", text)
                self.assertNotIn("missing_openai_api_key", text)

    def test_auto_import_stale_file_warns_and_skips(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            imports_dir = tmp / "imports"
            imports_dir.mkdir(parents=True, exist_ok=True)

            stale_csv = imports_dir / "ai_review_20260301.csv"
            stale_csv.write_text(
                "activity_nr,ai_priority,ai_reason\n"
                "stale_item,HIGH,stale file\n",
                encoding="utf-8",
            )
            old = time.time() - (72 * 3600)
            os.utime(stale_csv, (old, old))

            env = {
                "DATA_DIR": str(data_dir),
                "AI_TRIAGE_ENABLED": "1",
                "AI_REVIEW_IMPORT_DIR": str(imports_dir),
                "AI_REVIEW_IMPORT_MAX_AGE_HOURS": "24",
                "OPENAI_API_KEY": "",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                out = io.StringIO()
                with redirect_stdout(out):
                    result = ai_triage.get_or_compute(
                        item_key="stale_item",
                        mode="trial_render",
                        item={"activity_nr": "stale_item"},
                        detail_row={},
                    )
                self.assertIsNone(result)
                text = out.getvalue()
                self.assertIn("WARN_AI_REVIEW_AUTO_IMPORT_STALE", text)
                self.assertIn("missing_openai_api_key", text)

    def test_auto_import_invalid_rows_counted_but_nonfatal(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            imports_dir = tmp / "imports"
            imports_dir.mkdir(parents=True, exist_ok=True)

            csv_path = imports_dir / "ai_review_20260302.csv"
            csv_path.write_text(
                "activity_nr,ai_priority,ai_reason\n"
                "valid_1,HIGH,good row\n"
                "missing_priority,,bad row\n"
                "bad_priority,URGENT,bad row\n",
                encoding="utf-8",
            )

            env = {
                "DATA_DIR": str(data_dir),
                "AI_TRIAGE_ENABLED": "1",
                "AI_REVIEW_IMPORT_DIR": str(imports_dir),
                "AI_REVIEW_IMPORT_MAX_AGE_HOURS": "24",
                "OPENAI_API_KEY": "",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                out = io.StringIO()
                with redirect_stdout(out):
                    result = ai_triage.get_or_compute(
                        item_key="valid_1",
                        mode="trial_render",
                        item={"activity_nr": "valid_1"},
                        detail_row={},
                    )
                self.assertIsNotNone(result)
                self.assertEqual(str(result.get("priority")), "HIGH")
                text = out.getvalue()
                self.assertIn("AI_REVIEW_AUTO_IMPORT_APPLIED", text)
                self.assertIn("imported=1", text)
                self.assertIn("rejected_invalid=2", text)

    def test_auto_import_prefers_canonical_signals_subdir_before_legacy_root(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            canonical_dir = data_dir / "imports" / "signals_ai_review"
            legacy_dir = data_dir / "imports"
            canonical_dir.mkdir(parents=True, exist_ok=True)
            legacy_dir.mkdir(parents=True, exist_ok=True)

            canonical_csv = canonical_dir / "ai_review_20260302.csv"
            canonical_csv.write_text(
                "activity_nr,ai_priority,ai_reason\n"
                "prefer_canonical,HIGH,canonical folder wins\n",
                encoding="utf-8",
            )
            legacy_csv = legacy_dir / "ai_review_20260303.csv"
            legacy_csv.write_text(
                "activity_nr,ai_priority,ai_reason\n"
                "prefer_canonical,LOW,legacy root should not win\n",
                encoding="utf-8",
            )

            now = time.time()
            os.utime(canonical_csv, (now - 120, now - 120))
            os.utime(legacy_csv, (now - 10, now - 10))

            env = {
                "DATA_DIR": str(data_dir),
                "AI_TRIAGE_ENABLED": "1",
                "AI_REVIEW_IMPORT_MAX_AGE_HOURS": "24",
                "OPENAI_API_KEY": "",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                out = io.StringIO()
                with redirect_stdout(out):
                    result = ai_triage.get_or_compute(
                        item_key="prefer_canonical",
                        mode="trial_render",
                        item={"activity_nr": "prefer_canonical"},
                        detail_row={},
                    )

            self.assertIsNotNone(result)
            self.assertEqual(str(result.get("priority")), "HIGH")
            text = out.getvalue()
            self.assertIn("AI_REVIEW_AUTO_IMPORT_APPLIED", text)
            self.assertIn("ai_review_20260302.csv", text)
            self.assertNotIn("WARN_AI_REVIEW_AUTO_IMPORT_LEGACY_DIR=1", text)


if __name__ == "__main__":
    unittest.main()
