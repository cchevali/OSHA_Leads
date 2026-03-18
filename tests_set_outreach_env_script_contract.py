import os
import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "set_outreach_env.ps1"
INSTALL_SCRIPT_PATH = REPO_ROOT / "scripts" / "install_scheduled_tasks.ps1"
RUN_AUTO_SCRIPT_PATH = REPO_ROOT / "outreach" / "run_outreach_auto.py"


def _cached_env_sops() -> str:
    proc = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "diff", "--cached", "--name-only", "--", ".env.sops"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return "ERR"
    return (proc.stdout or "").strip()


class TestSetOutreachEnvScriptContract(unittest.TestCase):
    def test_script_exists(self):
        self.assertTrue(SCRIPT_PATH.exists(), msg=f"missing script: {SCRIPT_PATH}")

    def test_script_contains_manual_only_contract(self):
        text = SCRIPT_PATH.read_text(encoding="utf-8")

        required_params = [
            "OutreachDailyLimit",
            "OutreachStates",
            "OshaSmokeTo",
            "OutreachSuppressionMaxAgeHours",
            "OutreachFallbackOnEmptyState",
            "OutreachSkipRoleInboxes",
            "TrialSendsLimitDefault",
            "TrialExpiredBehaviorDefault",
            "TrialConversionUrl",
            "AiTriageEnabled",
            "AiTriageOpenAiModel",
            "SignalFreshnessMaxDays",
            "StripePriceIdCore",
            "StripePriceIdMulti",
            "StripePriceIdPilot",
            "WebStripeWebhookSecret",
            "TaskSchedUser",
            "TaskSchedPassword",
            "RuntimeRole",
            "CanonicalHostname",
            "ArtifactSyncDir",
            "TaskLogRoot",
            "RunSummaryRoot",
            "PrintConfig",
        ]
        required_tokens = [
            "ERR_ENV_SOPS_STAGED",
            "ERR_SET_OUTREACH_ENV_TOOLING",
            "ERR_SET_OUTREACH_ENV_DECRYPT",
            "ERR_SET_OUTREACH_ENV_ARGS",
            "ERR_SET_OUTREACH_ENV_ENCRYPT",
            "ERR_SET_OUTREACH_ENV_WRITE",
            "ERR_SET_OUTREACH_ENV_VERIFY",
            "ERR_SET_OUTREACH_ENV_PRINT_CONFIG",
            "ERR_SET_OUTREACH_ENV_PRINT_CONFIG_MISSING_KEYS",
            "PASS_SET_OUTREACH_ENV_APPLY",
            "PASS_SET_OUTREACH_ENV_VERIFY",
            "PASS_SET_OUTREACH_ENV_PRINT_CONFIG",
            "PASS_SET_OUTREACH_ENV_DATA_DIR",
            "PASS_SET_OUTREACH_ENV_COMPLETE",
        ]

        for param in required_params:
            self.assertIn(param, text)
        for token in required_tokens:
            self.assertIn(token, text)

        self.assertIn("git -C $repoRoot diff --cached --name-only -- .env.sops", text)
        self.assertIn("Resolve-EnvSopsPath -RepoRoot $repoRoot", text)
        self.assertIn("OUTREACH_FALLBACK_ON_EMPTY_STATE", text)
        self.assertIn("OUTREACH_SKIP_ROLE_INBOXES", text)
        self.assertIn("outreach_skip_role_inboxes=", text)
        self.assertIn("outreach_states=", text)
        self.assertIn("AI_TRIAGE_ENABLED", text)
        self.assertIn("AI_TRIAGE_OPENAI_MODEL", text)
        self.assertIn("SIGNAL_FRESHNESS_MAX_DAYS", text)
        self.assertIn("OPENAI_API_KEY", text)
        self.assertIn("missing_shell_OPENAI_API_KEY", text)
        self.assertIn("ai_triage_enabled=", text)
        self.assertIn("ai_triage_openai_model=", text)
        self.assertIn("signal_freshness_max_days=", text)
        self.assertIn("openai_api_key_present=", text)
        self.assertIn("TASK_SCHED_USER", text)
        self.assertIn("TASK_SCHED_PASSWORD", text)
        self.assertIn("RUNTIME_ROLE", text)
        self.assertIn("CANONICAL_HOSTNAME", text)
        self.assertIn("ARTIFACT_SYNC_DIR", text)
        self.assertIn("TASK_LOG_ROOT", text)
        self.assertIn("RUN_SUMMARY_ROOT", text)
        self.assertIn("runtime_role=", text)
        self.assertIn("canonical_hostname=", text)
        self.assertIn("artifact_sync_dir=", text)
        self.assertIn("task_log_root=", text)
        self.assertIn("run_summary_root=", text)
        self.assertIn("Test-ValidAbsoluteDataDir", text)
        self.assertIn("invalid_DataDir_absolute_required", text)
        self.assertIn("Pass-Token $PASS_SET_OUTREACH_ENV_DATA_DIR", text)
        self.assertIn("Remove-MapKey", text)
        self.assertIn("'PROSPECT_AUTOGROW_ENABLED'", text)
        self.assertIn("'PROSPECT_AI_ASSIST_REVIEW_ENABLED'", text)
        self.assertIn("'PROSPECT_ENRICH_MAX_SITES_PER_RUN'", text)
        self.assertIn("'APOLLO_API_KEY'", text)
        self.assertNotIn("Resolve-ImplementedAutogrowSources", text)
        self.assertNotIn("autogrow_source_registry.json", text)
        self.assertNotIn("WARN_SET_OUTREACH_ENV_SCOPE_DRIFT", text)
        self.assertNotIn("prospect_ai_assist_review_enabled=", text)
        self.assertNotIn("prospect_ai_assist_review_raw_target=", text)
        self.assertNotIn("prospect_ai_assist_review_packet_size=", text)
        self.assertNotIn("prospect_autogrow_states=", text)
        self.assertNotIn("apollo_api_key_present=", text)
        self.assertNotIn("apollo_enrich_enabled=", text)
        self.assertNotIn("apollo_enrich_max_per_run=", text)
        self.assertNotIn("apollo_person_locations_mode=", text)
        self.assertNotIn("prospect_enrich_max_sites_per_run=", text)
        self.assertNotIn("prospect_enrich_http_sleep_ms=", text)
        self.assertNotIn("Write-Output ('OPENAI_API_KEY='", text)
        self.assertNotIn("Write-Output ('TASK_SCHED_PASSWORD='", text)
        self.assertNotIn("CSV_IMPORT", text)

    def test_doctor_and_installer_flows_do_not_stage_env_sops(self):
        before = _cached_env_sops()
        self.assertEqual(before, "", msg=f".env.sops is already staged before test: {before}")

        env = os.environ.copy()
        env.setdefault("OUTREACH_STATES", "TX")
        env.setdefault("OUTREACH_DAILY_LIMIT", "10")
        env.setdefault("OSHA_SMOKE_TO", "audit@example.com")
        env.setdefault("OUTREACH_SUPPRESSION_MAX_AGE_HOURS", "240")

        doctor = subprocess.run(
            [sys.executable, str(RUN_AUTO_SCRIPT_PATH), "--doctor"],
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertIn(
            doctor.returncode,
            (0, 2),
            msg=(doctor.stdout or "") + "\n" + (doctor.stderr or ""),
        )

        install = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(INSTALL_SCRIPT_PATH),
                "--dry-run",
            ],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        self.assertEqual(install.returncode, 0, msg=(install.stdout or "") + "\n" + (install.stderr or ""))

        after = _cached_env_sops()
        self.assertEqual(after, "", msg=f".env.sops was staged by flow: {after}")


if __name__ == "__main__":
    unittest.main()
