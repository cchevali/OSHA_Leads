import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import runtime_data_dir


REPO_ROOT = Path(__file__).resolve().parent


class TestRuntimeDataDir(unittest.TestCase):
    def test_resolve_osha_db_path_defaults_to_data_dir_layout_when_data_dir_unset(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            resolution = runtime_data_dir.resolve_osha_db_path(REPO_ROOT)
        self.assertEqual(resolution.source, "data_dir")
        self.assertEqual(resolution.effective_path, (REPO_ROOT / "out" / "osha.sqlite").resolve(strict=False))

    def test_resolve_osha_db_path_uses_data_dir_when_configured(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d).resolve()
            with mock.patch.dict(os.environ, {"DATA_DIR": str(data_dir)}, clear=True):
                resolution = runtime_data_dir.resolve_osha_db_path(REPO_ROOT)
        self.assertEqual(resolution.source, "data_dir")
        self.assertEqual(resolution.effective_path, (data_dir / "osha.sqlite").resolve(strict=False))


if __name__ == "__main__":
    unittest.main()
