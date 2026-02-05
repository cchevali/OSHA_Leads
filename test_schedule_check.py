import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from run_wally_trial import build_task_action, extract_exec_action, verify_schedule_action_from_actual


FIXTURES_DIR = Path(__file__).resolve().parent / "tests" / "fixtures"


class TestScheduleCheck(unittest.TestCase):
    def test_schedule_ok_from_xml(self):
        xml_text = (FIXTURES_DIR / "task_action_ok.xml").read_text(encoding="utf-8")
        actual = extract_exec_action(xml_text)
        expected = build_task_action(r"C:\dev\OSHA_Leads\run_wally_trial_daily.bat")

        buf = io.StringIO()
        with redirect_stdout(buf):
            verify_schedule_action_from_actual(expected, actual)

        self.assertEqual(buf.getvalue().strip(), f"SCHEDULE_OK /TR={expected}")

    def test_schedule_mismatch_from_xml(self):
        xml_text = (FIXTURES_DIR / "task_action_bad.xml").read_text(encoding="utf-8")
        actual = extract_exec_action(xml_text)
        expected = build_task_action(r"C:\dev\OSHA_Leads\run_wally_trial_daily.bat")

        buf = io.StringIO()
        with redirect_stdout(buf):
            with self.assertRaises(SystemExit) as ctx:
                verify_schedule_action_from_actual(expected, actual)

        self.assertEqual(ctx.exception.code, 1)
        lines = buf.getvalue().strip().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0], f"SCHEDULE_CHECK_FAILED expected={expected} actual={actual}")


if __name__ == "__main__":
    unittest.main()
