import io
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from outreach import run_runtime_state_migrate as migrate


class _Preflight:
    def __init__(self, ok: bool = True):
        self.ok = ok


class TestRunRuntimeStateMigrate(unittest.TestCase):
    def test_wrapper_shim_exists(self):
        script = Path(__file__).resolve().parent / "run_runtime_state_migrate.py"
        text = script.read_text(encoding="utf-8")
        self.assertIn("from outreach.run_runtime_state_migrate import main", text)
        self.assertIn("raise SystemExit(main())", text)

    def test_print_config_mode_emits_tokens(self):
        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(migrate, "run_runtime_preflight", return_value=_Preflight(True)),
            mock.patch.object(migrate, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
        ):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = migrate.main(["--print-config"])
        out = buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("RUNTIME_STATE_MIGRATE_DATA_DIR=", out)
        self.assertIn("PASS_RUNTIME_STATE_MIGRATE_PRINT_CONFIG status=OK", out)
        self.assertIn("PASS_RUNTIME_STATE_MIGRATE_COMPLETE status=PRINT_CONFIG", out)

    def test_dry_run_plans_expected_actions(self):
        with tempfile.TemporaryDirectory() as d:
            repo_root = Path(d) / "repo"
            data_root = Path(d) / "data"
            (repo_root / "data").mkdir(parents=True, exist_ok=True)
            (repo_root / "out").mkdir(parents=True, exist_ok=True)
            (repo_root / "data" / "osha.sqlite").write_text("osha", encoding="utf-8")
            (repo_root / "out" / "crm_light.sqlite").write_text("trial", encoding="utf-8")
            with (
                mock.patch.dict(os.environ, {"DATA_DIR": str(data_root)}, clear=False),
                mock.patch.object(migrate, "_repo_root", return_value=repo_root),
                mock.patch.object(migrate, "run_runtime_preflight", return_value=_Preflight(True)),
                mock.patch.object(migrate, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            ):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = migrate.main(["--dry-run"])
            out = buf.getvalue()
            self.assertEqual(rc, 0, msg=out)
            self.assertIn("RUNTIME_STATE_MIGRATE_ACTION_COUNT=3", out)
            self.assertIn("RUNTIME_STATE_MIGRATE_ACTION_PLAN=copy_repo_osha_to_canonical", out)
            self.assertIn("RUNTIME_STATE_MIGRATE_ACTION_PLAN=copy_legacy_trial_to_canonical", out)
            self.assertIn("RUNTIME_STATE_MIGRATE_ACTION_PLAN=archive_legacy_trial_db", out)
            self.assertIn("PASS_RUNTIME_STATE_MIGRATE_COMPLETE status=DRY_RUN", out)

    def test_invalid_mode_combo_fails(self):
        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(migrate, "run_runtime_preflight", return_value=_Preflight(True)),
            mock.patch.object(migrate, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
        ):
            out_buf = io.StringIO()
            err_buf = io.StringIO()
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = migrate.main(["--doctor", "--dry-run"])
        self.assertEqual(rc, 2)
        self.assertIn("ERR_RUNTIME_STATE_MIGRATE_CONFIG", err_buf.getvalue())


if __name__ == "__main__":
    unittest.main()
