import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from scoring import ai_triage


class TestAiTriageCacheBehavior(unittest.TestCase):
    def setUp(self) -> None:
        ai_triage._DISABLED_EMITTED = False
        ai_triage._UNAVAILABLE_EMITTED = False

    def test_cache_hit_returns_payload_without_api_key(self):
        with tempfile.TemporaryDirectory() as d:
            cache_path = Path(d) / "ai_cache.sqlite"
            prompt_hash = ai_triage.prompt_hash()
            conn = ai_triage.connect_ai_cache(cache_path)
            try:
                ai_triage.put_cached(
                    conn,
                    item_key="activity_1",
                    prompt_hash=prompt_hash,
                    model="manual_import",
                    payload={
                        "priority": "HIGH",
                        "reason": "cached_reason",
                        "prompt_hash": prompt_hash,
                        "prompt_version": ai_triage.AI_PROMPT_VERSION,
                        "model": "manual_import",
                        "cached": 1,
                    },
                )
            finally:
                conn.close()

            with mock.patch.dict(os.environ, {"AI_TRIAGE_ENABLED": "1", "OPENAI_API_KEY": ""}, clear=False):
                result = ai_triage.get_or_compute(
                    item_key="activity_1",
                    mode="trial_render",
                    item={"activity_nr": "activity_1"},
                    detail_row={},
                    cache_db_path=cache_path,
                )

            self.assertIsNotNone(result)
            self.assertEqual(result.get("priority"), "HIGH")
            self.assertEqual(int(result.get("cached") or 0), 1)

    def test_cache_miss_without_api_key_returns_none_and_warns(self):
        with tempfile.TemporaryDirectory() as d:
            cache_path = Path(d) / "ai_cache.sqlite"
            out = io.StringIO()
            with mock.patch.dict(os.environ, {"AI_TRIAGE_ENABLED": "1", "OPENAI_API_KEY": ""}, clear=False):
                with redirect_stdout(out):
                    result = ai_triage.get_or_compute(
                        item_key="activity_2",
                        mode="trial_render",
                        item={"activity_nr": "activity_2"},
                        detail_row={},
                        cache_db_path=cache_path,
                    )

            self.assertIsNone(result)
            self.assertIn("WARN_AI_TRIAGE_UNAVAILABLE detail=missing_openai_api_key", out.getvalue())


if __name__ == "__main__":
    unittest.main()
