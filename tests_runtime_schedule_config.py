import json
import tempfile
import unittest
from pathlib import Path

from runtime_schedule_config import (
    DEFAULT_EVENING_PREP_LOCAL_HHMM,
    DEFAULT_OUTREACH_SEND_LOCAL_HHMM,
    DEFAULT_TRIAL_SEND_LOCAL_HHMM,
    SCHEDULE_SCHEMA,
    load_runtime_schedule,
    schedule_config_path,
    write_runtime_schedule,
)


class TestRuntimeScheduleConfig(unittest.TestCase):
    def test_load_runtime_schedule_defaults_when_missing(self):
        with tempfile.TemporaryDirectory() as d:
            schedule = load_runtime_schedule(d)
        self.assertFalse(schedule.exists)
        self.assertEqual(schedule.source, "default")
        self.assertEqual(schedule.schema, SCHEDULE_SCHEMA)
        self.assertEqual(schedule.outreach_send_local_hhmm, DEFAULT_OUTREACH_SEND_LOCAL_HHMM)
        self.assertEqual(schedule.trial_default_send_local_hhmm, DEFAULT_TRIAL_SEND_LOCAL_HHMM)
        self.assertEqual(schedule.evening_prep_local_hhmm, DEFAULT_EVENING_PREP_LOCAL_HHMM)

    def test_write_runtime_schedule_round_trips_values(self):
        with tempfile.TemporaryDirectory() as d:
            written = write_runtime_schedule(
                d,
                outreach_send_local_hhmm="10:10",
                trial_default_send_local_hhmm="11:11",
                evening_prep_local_hhmm="21:20",
                updated_by="unit_test",
            )
            reloaded = load_runtime_schedule(d)
            raw = json.loads(schedule_config_path(d).read_text(encoding="utf-8"))
        self.assertTrue(written.exists)
        self.assertEqual(written.source, "file")
        self.assertEqual(reloaded.outreach_send_local_hhmm, "10:10")
        self.assertEqual(reloaded.trial_default_send_local_hhmm, "11:11")
        self.assertEqual(reloaded.evening_prep_local_hhmm, "21:20")
        self.assertEqual(raw["schema"], SCHEDULE_SCHEMA)
        self.assertEqual(raw["updated_by"], "unit_test")
        self.assertTrue(str(raw.get("updated_at_utc") or "").strip())

    def test_load_runtime_schedule_rejects_invalid_json(self):
        with tempfile.TemporaryDirectory() as d:
            path = schedule_config_path(d)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{not-json}", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_runtime_schedule(Path(d))
