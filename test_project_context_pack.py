import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from tools import project_context_pack as pcp


def _write_file(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_required_docs(repo_root: Path) -> None:
    _write_file(repo_root / "AGENTS.md", "# AGENTS\n")
    _write_file(repo_root / "docs/PROJECT_BRIEF.md", "# PROJECT_BRIEF\n")
    _write_file(repo_root / "docs/ARCHITECTURE.md", "# ARCHITECTURE\n")
    _write_file(repo_root / "docs/DECISIONS.md", "# DECISIONS\n")
    _write_file(repo_root / "docs/RUNBOOK.md", "# RUNBOOK\n")
    _write_file(repo_root / "docs/TODO.md", "# TODO\n")


class TestProjectContextPack(unittest.TestCase):
    def _run_fingerprint_lines(self, repo_root: Path) -> list[str]:
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = pcp.fingerprint_pack(repo_root)
        self.assertEqual(code, 0)
        return buf.getvalue().splitlines()

    def _parse_fingerprint(self, lines: list[str]) -> dict[str, str]:
        self.assertEqual(
            lines,
            [
                line
                for line in lines
                if line.startswith("PACK_GIT_SHA=")
                or line.startswith("PACK_BUILD_UTC=")
                or line.startswith("PACK_HASH=")
                or line.startswith("UPLOAD_MARKED=")
                or line.startswith("UPLOAD_MARKED_AT_UTC=")
            ],
        )
        self.assertEqual(len(lines), 5)
        expected_keys = [
            "PACK_GIT_SHA",
            "PACK_BUILD_UTC",
            "PACK_HASH",
            "UPLOAD_MARKED",
            "UPLOAD_MARKED_AT_UTC",
        ]
        parsed: dict[str, str] = {}
        for idx, key in enumerate(expected_keys):
            line = lines[idx]
            self.assertTrue(line.startswith(f"{key}="), line)
            parsed[key] = line.split("=", 1)[1]
        return parsed

    def test_deterministic_pack_generation_given_fixed_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _seed_required_docs(root)
            fixed_sha = "abc123"
            fixed_utc = "2026-02-12T00:00:00Z"
            a = pcp.generate_pack_text(root, pack_git_sha=fixed_sha, pack_build_utc=fixed_utc)
            b = pcp.generate_pack_text(root, pack_git_sha=fixed_sha, pack_build_utc=fixed_utc)
            self.assertEqual(a, b)
            self.assertIn("PACK_GIT_SHA=abc123", a)
            self.assertIn("PACK_BUILD_UTC=2026-02-12T00:00:00Z", a)

    def test_strict_check_detects_stale_when_source_doc_changes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _seed_required_docs(root)
            pack = pcp.generate_pack_text(root, pack_git_sha="sha0", pack_build_utc="2026-02-12T00:00:00Z")
            (root / pcp.PACK_FILENAME).write_text(pack, encoding="utf-8")
            self.assertEqual(pcp.mark_uploaded(root), 0)

            _write_file(root / "docs/TODO.md", "# TODO\nchanged\n")

            buf = io.StringIO()
            with redirect_stdout(buf):
                code = pcp.check_pack(root, soft=False, current_git_sha="sha0")
            out = buf.getvalue()
            self.assertEqual(code, 1)
            self.assertIn("ERR_CONTEXT_PACK_STALE", out)
            self.assertIn(pcp.UPLOAD_INSTRUCTION, out)

    def test_strict_check_fails_when_upload_marker_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _seed_required_docs(root)
            pack = pcp.generate_pack_text(root, pack_git_sha="sha1", pack_build_utc="2026-02-12T00:00:00Z")
            (root / pcp.PACK_FILENAME).write_text(pack, encoding="utf-8")

            buf = io.StringIO()
            with redirect_stdout(buf):
                code = pcp.check_pack(root, soft=False, current_git_sha="sha1")
            out = buf.getvalue()
            self.assertEqual(code, 1)
            self.assertIn("ERR_CONTEXT_PACK_UPLOAD_STATE_MISSING", out)

    def test_soft_check_emits_warning_and_exits_zero(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _seed_required_docs(root)

            buf = io.StringIO()
            with redirect_stdout(buf):
                code = pcp.check_pack(root, soft=True, current_git_sha="sha2")
            out = buf.getvalue()
            self.assertEqual(code, 0)
            self.assertIn("WARN_CONTEXT_PACK_MISSING", out)
            self.assertIn("PASS_CONTEXT_PACK_CHECK mode=soft", out)
            self.assertIn(pcp.UPLOAD_INSTRUCTION, out)

    def test_mark_uploaded_writes_expected_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _seed_required_docs(root)
            pack = pcp.generate_pack_text(root, pack_git_sha="sha3", pack_build_utc="2026-02-12T00:00:00Z")
            (root / pcp.PACK_FILENAME).write_text(pack, encoding="utf-8")
            self.assertEqual(pcp.mark_uploaded(root), 0)

            state_path = root / pcp.UPLOAD_STATE_PATH
            self.assertTrue(state_path.exists())
            state = json.loads(state_path.read_text(encoding="utf-8"))
            meta = pcp.parse_pack_metadata(pack)
            self.assertEqual(state["pack_hash"], meta["pack_hash"])
            self.assertEqual(state["pack_git_sha"], "sha3")
            self.assertTrue(state.get("marked_uploaded_utc"))

    def test_fingerprint_matching_pack_and_marker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _seed_required_docs(root)
            sha = "a" * 40
            built_utc = "2026-02-12T00:00:00Z"
            pack = pcp.generate_pack_text(root, pack_git_sha=sha, pack_build_utc=built_utc)
            (root / pcp.PACK_FILENAME).write_text(pack, encoding="utf-8")
            self.assertEqual(pcp.mark_uploaded(root), 0)

            parsed = self._parse_fingerprint(self._run_fingerprint_lines(root))
            meta = pcp.parse_pack_metadata(pack)
            self.assertEqual(parsed["PACK_GIT_SHA"], sha)
            self.assertEqual(parsed["PACK_BUILD_UTC"], built_utc)
            self.assertEqual(parsed["PACK_HASH"], meta["pack_hash"])
            self.assertEqual(parsed["UPLOAD_MARKED"], "YES")
            self.assertRegex(parsed["UPLOAD_MARKED_AT_UTC"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

    def test_fingerprint_missing_marker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _seed_required_docs(root)
            pack = pcp.generate_pack_text(
                root,
                pack_git_sha=("b" * 40),
                pack_build_utc="2026-02-12T00:00:00Z",
            )
            (root / pcp.PACK_FILENAME).write_text(pack, encoding="utf-8")

            parsed = self._parse_fingerprint(self._run_fingerprint_lines(root))
            self.assertEqual(parsed["UPLOAD_MARKED"], "NO")
            self.assertEqual(parsed["UPLOAD_MARKED_AT_UTC"], "UNKNOWN")

    def test_fingerprint_stale_marker_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _seed_required_docs(root)
            pack_1 = pcp.generate_pack_text(
                root,
                pack_git_sha=("c" * 40),
                pack_build_utc="2026-02-12T00:00:00Z",
            )
            (root / pcp.PACK_FILENAME).write_text(pack_1, encoding="utf-8")
            self.assertEqual(pcp.mark_uploaded(root), 0)

            pack_2 = pcp.generate_pack_text(
                root,
                pack_git_sha=("d" * 40),
                pack_build_utc="2026-02-12T00:00:01Z",
            )
            (root / pcp.PACK_FILENAME).write_text(pack_2, encoding="utf-8")

            parsed = self._parse_fingerprint(self._run_fingerprint_lines(root))
            self.assertEqual(parsed["UPLOAD_MARKED"], "NO")
            self.assertRegex(parsed["UPLOAD_MARKED_AT_UTC"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

    def test_fingerprint_invalid_marker_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _seed_required_docs(root)
            pack = pcp.generate_pack_text(
                root,
                pack_git_sha=("e" * 40),
                pack_build_utc="2026-02-12T00:00:00Z",
            )
            (root / pcp.PACK_FILENAME).write_text(pack, encoding="utf-8")
            state_path = root / pcp.UPLOAD_STATE_PATH
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text("{not-json}", encoding="utf-8")

            parsed = self._parse_fingerprint(self._run_fingerprint_lines(root))
            self.assertEqual(parsed["UPLOAD_MARKED"], "UNKNOWN")
            self.assertEqual(parsed["UPLOAD_MARKED_AT_UTC"], "UNKNOWN")

    def test_fingerprint_missing_pack_with_marker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _seed_required_docs(root)
            state_path = root / pcp.UPLOAD_STATE_PATH
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "pack_hash": "f" * 64,
                        "pack_git_sha": "f" * 40,
                        "marked_uploaded_utc": "2026-02-12T00:00:00Z",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            parsed = self._parse_fingerprint(self._run_fingerprint_lines(root))
            self.assertEqual(parsed["PACK_GIT_SHA"], "UNKNOWN")
            self.assertEqual(parsed["PACK_BUILD_UTC"], "UNKNOWN")
            self.assertEqual(parsed["PACK_HASH"], "UNKNOWN")
            self.assertEqual(parsed["UPLOAD_MARKED"], "UNKNOWN")
            self.assertEqual(parsed["UPLOAD_MARKED_AT_UTC"], "2026-02-12T00:00:00Z")

    def test_cli_requires_exactly_one_mode_including_fingerprint(self) -> None:
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as cm:
                pcp.main(["--build", "--fingerprint"])
        self.assertEqual(cm.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
