import json
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SECRETS_TOOLING = REPO_ROOT / "scripts" / "secrets_tooling.ps1"


def _ps_single_quoted(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


class TestDataDirPolicy(unittest.TestCase):
    def _resolve_policy(self, inherited_expr: str, dotenv_expr: str) -> dict:
        self.assertTrue(SECRETS_TOOLING.exists(), msg=f"missing script: {SECRETS_TOOLING}")
        cmd = (
            f". {_ps_single_quoted(str(SECRETS_TOOLING))}; "
            f"$r = Resolve-MfoDataDirPolicy -RepoRoot {_ps_single_quoted(str(REPO_ROOT))} "
            f"-InheritedDataDir {inherited_expr} -DotenvDataDir {dotenv_expr}; "
            "$r | ConvertTo-Json -Compress"
        )
        proc = subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", cmd],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, msg=(proc.stderr or "") + "\n" + (proc.stdout or ""))
        text = (proc.stdout or "").strip()
        self.assertTrue(text, msg="missing policy json output")
        return json.loads(text)

    def test_inherited_absolute_beats_dotenv_relative(self):
        result = self._resolve_policy(
            inherited_expr=_ps_single_quoted(r"C:\osha_data"),
            dotenv_expr=_ps_single_quoted("out"),
        )
        self.assertEqual(str(result.get("Source") or ""), "inherited")
        self.assertFalse(bool(result.get("UseDefaultFallback")))
        self.assertEqual(str(result.get("EffectivePath") or "").lower(), str(Path(r"C:\osha_data")).lower())
        self.assertIn("WARN_ENV_CONFLICT=1 key=DATA_DIR", str(result.get("ConflictWarnToken") or ""))
        self.assertIn("using=inherited", str(result.get("ConflictWarnToken") or ""))

    def test_dotenv_absolute_used_when_inherited_empty(self):
        dotenv_path = str((REPO_ROOT / "tmp_data_dir_dotenv").resolve(strict=False))
        result = self._resolve_policy(
            inherited_expr=_ps_single_quoted(""),
            dotenv_expr=_ps_single_quoted(dotenv_path),
        )
        self.assertEqual(str(result.get("Source") or ""), "dotenv")
        self.assertFalse(bool(result.get("UseDefaultFallback")))
        self.assertEqual(str(result.get("EffectivePath") or "").lower(), dotenv_path.lower())
        self.assertEqual(str(result.get("ConflictWarnToken") or ""), "")
        self.assertEqual(str(result.get("NotAbsoluteWarnToken") or ""), "")

    def test_dotenv_relative_falls_back_to_repo_out(self):
        result = self._resolve_policy(
            inherited_expr="$null",
            dotenv_expr=_ps_single_quoted("out"),
        )
        expected = str((REPO_ROOT / "out").resolve(strict=False))
        self.assertEqual(str(result.get("Source") or ""), "default")
        self.assertTrue(bool(result.get("UseDefaultFallback")))
        self.assertEqual(str(result.get("EffectivePath") or "").lower(), expected.lower())
        self.assertIn("WARN_DATA_DIR_NOT_ABSOLUTE=1", str(result.get("NotAbsoluteWarnToken") or ""))
        self.assertIn("value=out", str(result.get("NotAbsoluteWarnToken") or ""))


if __name__ == "__main__":
    unittest.main()
