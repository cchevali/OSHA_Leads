import csv
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_prospect_generation.py"


class TestProspectGeneration(unittest.TestCase):
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

    def _run_discovery(self, args: list[str], env_overrides: dict[str, str | None]) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        for k, v in env_overrides.items():
            if v is None:
                env.pop(k, None)
            else:
                env[k] = v
        return subprocess.run(
            [sys.executable, str(REPO_ROOT / "run_prospect_discovery.py")] + args,
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )

    def test_module_importable_and_main_callable(self):
        from outreach import run_prospect_generation as generator

        self.assertTrue(callable(getattr(generator, "main", None)))

    def test_print_config_side_effect_free(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            out_path = data_dir / "prospect_discovery" / "prospects_latest.csv"
            p = self._run(["--print-config"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("PASS_GENERATOR_PRINT_CONFIG", out)
            self.assertIn(f"output_path={out_path.resolve()}", out)
            self.assertFalse(out_path.exists(), msg="--print-config must not write output")

    def test_dry_run_no_writes(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            out_path = data_dir / "prospect_discovery" / "prospects_latest.csv"
            p = self._run(["--dry-run"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("GENERATOR_OUTPUT_PATH=", out)
            self.assertIn("GENERATOR_ROWS_READ=", out)
            self.assertIn("GENERATOR_ROWS_WRITTEN=", out)
            self.assertIn("GENERATOR_COMPLETE status=DRY_RUN", out)
            self.assertFalse(out_path.exists(), msg="--dry-run must not write output")

    def test_transform_mapping_and_invalid_email_exclusion(self):
        from outreach import run_prospect_generation as generator

        rows = [
            {
                "company_name": "Firm A",
                "domain": "f1.com",
                "contact_email": "User@F1.com",
                "contact_role": "Owner",
                "city": "Houston",
                "state": "tx",
            },
            {
                "company_name": "Firm B",
                "domain": "f2.com",
                "contact_email": "bad-email",
                "contact_role": "Owner",
                "city": "Austin",
                "state": "TX",
            },
            {
                "company_name": "Firm C",
                "domain": "f3.com",
                "contact_email": "user@f1.com",
                "contact_role": "Partner",
                "city": "Dallas",
                "state": "TX",
            },
        ]
        out = generator._to_discovery_rows(rows)
        self.assertEqual(len(out), 1)
        row = out[0]
        self.assertTrue(str(row["prospect_id"]).startswith("gen_"))
        self.assertEqual(row["firm"], "Firm A")
        self.assertEqual(row["email"], "user@f1.com")
        self.assertEqual(row["title"], "Owner")
        self.assertEqual(row["city"], "Houston")
        self.assertEqual(row["state"], "TX")
        self.assertEqual(row["source"], "seed_recipients_pools")

    def test_live_generator_then_discovery_then_plan_non_zero_pool(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            suppression = data_dir / "suppression.csv"
            suppression.parent.mkdir(parents=True, exist_ok=True)
            suppression.write_text("email\n", encoding="utf-8")

            p_gen = self._run([], {"DATA_DIR": str(data_dir), "OUTREACH_STATES": "TX"})
            self.assertEqual(p_gen.returncode, 0, msg=p_gen.stderr + "\n" + p_gen.stdout)
            self.assertIn("GENERATOR_COMPLETE status=OK", p_gen.stdout or "")

            p_disc = self._run_discovery(
                [],
                {
                    "DATA_DIR": str(data_dir),
                    "OUTREACH_STATES": "TX",
                    "PROSPECT_DISCOVERY_INPUT": None,
                    "DISCOVERY_INPUT_CSV": None,
                },
            )
            self.assertEqual(p_disc.returncode, 0, msg=p_disc.stderr + "\n" + p_disc.stdout)
            self.assertIn("DISCOVERY_COMPLETE status=OK", p_disc.stdout or "")

            env = os.environ.copy()
            env["PYTHONPATH"] = str(REPO_ROOT)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX"
            p_plan = subprocess.run(
                [sys.executable, str(REPO_ROOT / "run_outreach_auto.py"), "--plan", "--for-date", "2026-02-13"],
                cwd=str(REPO_ROOT),
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(p_plan.returncode, 0, msg=p_plan.stderr + "\n" + p_plan.stdout)

            pool_total = None
            for line in (p_plan.stdout or "").splitlines():
                if line.startswith("OUTREACH_PLAN_POOL_TOTAL="):
                    pool_total = int((line.split("=", 1)[1] or "0").strip())
                    break
            self.assertIsNotNone(pool_total, msg=p_plan.stdout)
            self.assertGreater(pool_total, 0, msg=p_plan.stdout)

            out_path = data_dir / "prospect_discovery" / "prospects_latest.csv"
            self.assertTrue(out_path.exists(), msg=f"missing output: {out_path}")
            with open(out_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                self.assertIn("prospect_id", reader.fieldnames or [])
                self.assertIn("email", reader.fieldnames or [])
                self.assertIn("source", reader.fieldnames or [])


if __name__ == "__main__":
    unittest.main()
