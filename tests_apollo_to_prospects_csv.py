import csv
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "tools" / "apollo_to_prospects_csv.py"
CORE_FIELDS = [
    "prospect_id",
    "firm",
    "email",
    "title",
    "city",
    "state",
    "source",
    "contact_name",
    "website",
]


def _write_apollo_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "First name",
        "Last name",
        "Title",
        "Company name",
        "Email",
        "Email status",
        "Email confidence",
        "Website",
        "City",
        "State",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class TestApolloToProspectsCsv(unittest.TestCase):
    def _run(self, args: list[str], env_overrides: dict[str, str | None]) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        for k, v in env_overrides.items():
            if v is None:
                env.pop(k, None)
            else:
                env[k] = v
        return subprocess.run(
            [sys.executable, str(SCRIPT)] + args,
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )

    def test_print_config_uses_default_output_under_data_dir(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            input_csv = tmp / "apollo_export.csv"
            _write_apollo_csv(input_csv, [])

            p = self._run(["--input", str(input_csv), "--print-config"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("PASS_APOLLO_CONVERT_PRINT_CONFIG", out)
            self.assertIn(f"data_dir={data_dir.resolve()}", out)
            self.assertIn(
                f"output_path={(data_dir / 'imports' / 'prospects_apollo.csv').resolve()}",
                out,
            )
            self.assertIn(
                f"diagnostics_path={(data_dir / 'imports' / 'prospects_apollo_diagnostics.json').resolve()}",
                out,
            )

    def test_convert_maps_schema_dedupes_drops_and_writes_diagnostics(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            input_csv = tmp / "apollo_export.csv"
            _write_apollo_csv(
                input_csv,
                [
                    {
                        "First name": "Zoe",
                        "Last name": "Ames",
                        "Title": "Owner",
                        "Company name": "Zeta Co",
                        "Email": "Zeta@Example.com",
                        "Email status": "verified",
                        "Email confidence": "0.98",
                        "Website": "https://www.zeta.example.com/path",
                        "City": "Houston",
                        "State": "tx",
                    },
                    {
                        "First name": "No",
                        "Last name": "Email",
                        "Title": "Manager",
                        "Company name": "No Email Co",
                        "Email": "",
                        "Email status": "unknown",
                        "Email confidence": "0.00",
                        "Website": "noemail.example.com",
                        "City": "Dallas",
                        "State": "TX",
                    },
                    {
                        "First name": "Bad",
                        "Last name": "Email",
                        "Title": "Consultant",
                        "Company name": "Bad Email Co",
                        "Email": "bad-email",
                        "Email status": "guess",
                        "Email confidence": "0.12",
                        "Website": "bad.example.com",
                        "City": "Austin",
                        "State": "TX",
                    },
                    {
                        "First name": "Dup",
                        "Last name": "Case",
                        "Title": "VP",
                        "Company name": "Dup Co",
                        "Email": "zeta@example.com",
                        "Email status": "verified",
                        "Email confidence": "0.50",
                        "Website": "dup.example.com",
                        "City": "San Antonio",
                        "State": "TX",
                    },
                    {
                        "First name": "Amy",
                        "Last name": "Beta",
                        "Title": "Safety Director",
                        "Company name": "Alpha Co",
                        "Email": "alpha@example.com",
                        "Email status": "verified",
                        "Email confidence": "0.88",
                        "Website": "alpha.example.com",
                        "City": "Miami",
                        "State": "fl",
                    },
                ],
            )
            output_path = data_dir / "imports" / "prospects_apollo.csv"
            diagnostics_path = data_dir / "imports" / "prospects_apollo_diagnostics.json"

            p = self._run(["--input", str(input_csv)], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertTrue(output_path.exists(), msg="expected converted output CSV")
            self.assertTrue(diagnostics_path.exists(), msg="expected diagnostics JSON")

            with open(output_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                fieldnames = list(reader.fieldnames or [])

            self.assertEqual(fieldnames[: len(CORE_FIELDS)], CORE_FIELDS)
            self.assertEqual(fieldnames[len(CORE_FIELDS) :], ["apollo_email_confidence", "apollo_email_status"])
            self.assertEqual([r.get("email") for r in rows], ["alpha@example.com", "zeta@example.com"])
            self.assertEqual(rows[0].get("contact_name"), "Amy Beta")
            self.assertEqual(rows[0].get("state"), "FL")
            self.assertEqual(rows[0].get("firm"), "Alpha Co")
            self.assertEqual(rows[0].get("source"), "apollo_export_csv")
            self.assertTrue(str(rows[0].get("prospect_id") or "").startswith("gen_"))
            self.assertEqual(rows[1].get("contact_name"), "Zoe Ames")
            self.assertEqual(rows[1].get("state"), "TX")
            self.assertEqual(rows[1].get("firm"), "Zeta Co")
            self.assertEqual(rows[1].get("apollo_email_status"), "verified")
            self.assertEqual(rows[1].get("apollo_email_confidence"), "0.98")

            payload = json.loads(diagnostics_path.read_text(encoding="utf-8"))
            self.assertEqual(int(payload.get("input_rows") or 0), 5)
            self.assertEqual(int(payload.get("output_rows") or 0), 2)
            self.assertEqual(int(payload.get("dropped_no_email") or 0), 1)
            self.assertEqual(int(payload.get("dropped_invalid_email") or 0), 1)
            self.assertEqual(int(payload.get("deduped") or 0), 1)
            self.assertEqual(str(payload.get("input_path") or ""), str(input_csv.resolve()))
            self.assertEqual(str(payload.get("output_path") or ""), str(output_path.resolve()))
            self.assertEqual(str(payload.get("diagnostics_path") or ""), str(diagnostics_path.resolve()))
            self.assertEqual(bool(payload.get("dry_run")), False)

    def test_dry_run_does_not_write_output_or_diagnostics(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            input_csv = tmp / "apollo_export.csv"
            _write_apollo_csv(
                input_csv,
                [
                    {
                        "First name": "Dry",
                        "Last name": "Run",
                        "Title": "Owner",
                        "Company name": "Dry Co",
                        "Email": "dry@example.com",
                        "Email status": "verified",
                        "Email confidence": "1.0",
                        "Website": "dry.example.com",
                        "City": "Dallas",
                        "State": "TX",
                    }
                ],
            )
            output_path = data_dir / "imports" / "prospects_apollo.csv"
            diagnostics_path = data_dir / "imports" / "prospects_apollo_diagnostics.json"

            p = self._run(["--input", str(input_csv), "--dry-run"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("PASS_APOLLO_CONVERT_DRY_RUN", p.stdout or "")
            self.assertFalse(output_path.exists())
            self.assertFalse(diagnostics_path.exists())


if __name__ == "__main__":
    unittest.main()
