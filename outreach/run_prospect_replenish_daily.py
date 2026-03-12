import argparse
import os
import re
import subprocess
import sys
from pathlib import Path


ERR_PROSPECT_REPLENISH_CONFIG = "ERR_PROSPECT_REPLENISH_CONFIG"
ERR_PROSPECT_REPLENISH_STAGE = "ERR_PROSPECT_REPLENISH_STAGE"
PASS_PROSPECT_REPLENISH_PRINT_CONFIG = "PASS_PROSPECT_REPLENISH_PRINT_CONFIG"
PASS_PROSPECT_REPLENISH_DOCTOR = "PASS_PROSPECT_REPLENISH_DOCTOR"
PASS_PROSPECT_REPLENISH_COMPLETE = "PASS_PROSPECT_REPLENISH_COMPLETE"

DEFAULT_AUTOGROW_ENABLED = "1"
DEFAULT_AUTOGROW_SOURCES = "AIHA,OHS_BG"
DEFAULT_AUTOGROW_SAFETY_NET_ENABLED = "1"
DEFAULT_AI_ASSIST_REVIEW_ENABLED = "1"


def _error(detail: str) -> int:
    print(f"{ERR_PROSPECT_REPLENISH_CONFIG} {detail}", file=sys.stderr)
    return 2


def _emit(key: str, value: str | int) -> None:
    print(f"{key}={value}")


def _cmd_text(parts: list[str]) -> str:
    return " ".join(str(p) for p in parts)


def _defaulted_env() -> dict[str, str]:
    env = dict(os.environ)
    env.setdefault("PROSPECT_AUTOGROW_ENABLED", DEFAULT_AUTOGROW_ENABLED)
    env.setdefault("PROSPECT_AUTOGROW_SOURCES", DEFAULT_AUTOGROW_SOURCES)
    env.setdefault("PROSPECT_AUTOGROW_SAFETY_NET_ENABLED", DEFAULT_AUTOGROW_SAFETY_NET_ENABLED)
    env.setdefault("PROSPECT_AI_ASSIST_REVIEW_ENABLED", DEFAULT_AI_ASSIST_REVIEW_ENABLED)
    return env


def _run_with_secrets_cmd(repo_root: Path, script_and_args: list[str]) -> list[str]:
    return [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(repo_root / "run_with_secrets.ps1"),
        "--",
        "py",
        "-3",
        *script_and_args,
    ]


def _run_stage(
    repo_root: Path,
    stage_name: str,
    script_and_args: list[str],
    env: dict[str, str],
) -> tuple[int, str]:
    cmd = _run_with_secrets_cmd(repo_root=repo_root, script_and_args=script_and_args)
    _emit("PROSPECT_REPLENISH_STAGE", stage_name)
    _emit("PROSPECT_REPLENISH_STAGE_COMMAND", _cmd_text(cmd))
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(repo_root),
            env=env,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        print(
            f"{ERR_PROSPECT_REPLENISH_STAGE} stage={stage_name} code=-1 err={exc}",
            file=sys.stderr,
        )
        return 2, ""

    combined = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr)
    if proc.returncode != 0:
        print(
            f"{ERR_PROSPECT_REPLENISH_STAGE} stage={stage_name} code={proc.returncode}",
            file=sys.stderr,
        )
        return 2, combined
    return 0, combined


def _last_token_value(text: str, token: str) -> str:
    matches = re.findall(rf"(?m)^{re.escape(token)}=(.*)$", text or "")
    if not matches:
        return ""
    return str(matches[-1]).strip()


def _int_or_default(value: str, default: int) -> int:
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return int(text)
    except Exception:
        return default


def _for_date_args(for_date: str) -> list[str]:
    value = str(for_date or "").strip()
    if not value:
        return []
    return ["--for-date", value]


def _emit_effective_defaults(env: dict[str, str]) -> None:
    _emit("PROSPECT_REPLENISH_EFFECTIVE_AUTOGROW_ENABLED", str(env.get("PROSPECT_AUTOGROW_ENABLED") or ""))
    _emit("PROSPECT_REPLENISH_EFFECTIVE_AUTOGROW_SOURCES", str(env.get("PROSPECT_AUTOGROW_SOURCES") or ""))
    _emit(
        "PROSPECT_REPLENISH_EFFECTIVE_SAFETY_NET_ENABLED",
        str(env.get("PROSPECT_AUTOGROW_SAFETY_NET_ENABLED") or ""),
    )
    _emit(
        "PROSPECT_REPLENISH_EFFECTIVE_AI_ASSIST_REVIEW_ENABLED",
        str(env.get("PROSPECT_AI_ASSIST_REVIEW_ENABLED") or ""),
    )


def _run_ai_assist_pending_imports(*, dry_run: bool) -> int:
    if bool(dry_run):
        return 0
    from tools import import_prospect_ai_assist_review as ai_assist_import

    _emit("PROSPECT_REPLENISH_STAGE", "ai_assist_pending_import")
    _emit("PROSPECT_REPLENISH_STAGE_COMMAND", "module:tools.import_prospect_ai_assist_review.run_pending_imports")
    return int(ai_assist_import.run_pending_imports(dry_run=False))


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Run deterministic daily prospect replenishment pipeline: doctor -> generation -> discovery -> ai assist dump."
    )
    ap.add_argument("--print-config", action="store_true", help="Print resolved commands/config and exit.")
    ap.add_argument("--doctor", action="store_true", help="Run wrapper readiness checks and exit.")
    ap.add_argument("--dry-run", action="store_true", help="No-write preview run.")
    ap.add_argument("--for-date", default="", help="Optional YYYY-MM-DD date override for generation/discovery parity.")
    return ap


def _emit_print_config(repo_root: Path, env: dict[str, str], for_date: str) -> None:
    for_date_args = _for_date_args(for_date)
    doctor_cmd = _run_with_secrets_cmd(repo_root, ["run_prospect_generation.py", "--doctor"])
    generation_cmd = _run_with_secrets_cmd(repo_root, ["run_prospect_generation.py", *for_date_args])
    discovery_cmd = _run_with_secrets_cmd(repo_root, ["run_prospect_discovery.py", *for_date_args])
    ai_assist_cmd = _run_with_secrets_cmd(repo_root, ["tools/dump_prospect_ai_assist_review.py", *for_date_args])
    dry_generation_cmd = _run_with_secrets_cmd(repo_root, ["run_prospect_generation.py", "--dry-run", *for_date_args])
    dry_discovery_cmd = _run_with_secrets_cmd(repo_root, ["run_prospect_discovery.py", "--print-config", *for_date_args])
    dry_ai_assist_cmd = _run_with_secrets_cmd(
        repo_root,
        ["tools/dump_prospect_ai_assist_review.py", "--dry-run", *for_date_args],
    )

    _emit_effective_defaults(env)
    _emit("PROSPECT_REPLENISH_REPO_ROOT", str(repo_root))
    _emit("PROSPECT_REPLENISH_FOR_DATE", str(for_date or "(unset)"))
    _emit("PROSPECT_REPLENISH_LIVE_STAGE_1_COMMAND", _cmd_text(doctor_cmd))
    _emit("PROSPECT_REPLENISH_LIVE_STAGE_2_COMMAND", _cmd_text(generation_cmd))
    _emit("PROSPECT_REPLENISH_LIVE_STAGE_3_COMMAND", _cmd_text(discovery_cmd))
    _emit("PROSPECT_REPLENISH_LIVE_STAGE_4_COMMAND", _cmd_text(ai_assist_cmd))
    _emit("PROSPECT_REPLENISH_DRY_RUN_STAGE_1_COMMAND", _cmd_text(dry_generation_cmd))
    _emit("PROSPECT_REPLENISH_DRY_RUN_STAGE_2_COMMAND", _cmd_text(dry_discovery_cmd))
    _emit("PROSPECT_REPLENISH_DRY_RUN_STAGE_3_COMMAND", _cmd_text(dry_ai_assist_cmd))
    print(f"{PASS_PROSPECT_REPLENISH_PRINT_CONFIG} status=OK")
    print(f"{PASS_PROSPECT_REPLENISH_COMPLETE} status=PRINT_CONFIG")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    selected_modes = int(bool(args.print_config)) + int(bool(args.doctor)) + int(bool(args.dry_run))
    if selected_modes > 1:
        return _error("modes_mutually_exclusive")

    repo_root = Path(__file__).resolve().parents[1]
    if not (repo_root / "run_with_secrets.ps1").exists():
        return _error(f"missing_run_with_secrets path={(repo_root / 'run_with_secrets.ps1')}")

    env = _defaulted_env()
    for_date = str(args.for_date or "").strip()

    if args.print_config:
        _emit_print_config(repo_root=repo_root, env=env, for_date=for_date)
        return 0

    _emit_effective_defaults(env)

    if args.doctor:
        rc, _ = _run_stage(
            repo_root=repo_root,
            stage_name="doctor_generation",
            script_and_args=["run_prospect_generation.py", "--doctor"],
            env=env,
        )
        if rc != 0:
            return rc
        rc, _ = _run_stage(
            repo_root=repo_root,
            stage_name="doctor_discovery",
            script_and_args=["run_prospect_discovery.py", "--print-config"],
            env=env,
        )
        if rc != 0:
            return rc
        rc, _ = _run_stage(
            repo_root=repo_root,
            stage_name="doctor_ai_assist_dump",
            script_and_args=["tools/dump_prospect_ai_assist_review.py", "--print-config"],
            env=env,
        )
        if rc != 0:
            return rc
        print(f"{PASS_PROSPECT_REPLENISH_DOCTOR} status=OK")
        print(f"{PASS_PROSPECT_REPLENISH_COMPLETE} status=DOCTOR")
        return 0

    generation_args = ["run_prospect_generation.py", *(_for_date_args(for_date))]
    discovery_args = ["run_prospect_discovery.py", *(_for_date_args(for_date))]
    ai_assist_args = ["tools/dump_prospect_ai_assist_review.py", *(_for_date_args(for_date))]
    if args.dry_run:
        generation_args = ["run_prospect_generation.py", "--dry-run", *(_for_date_args(for_date))]
        discovery_args = ["run_prospect_discovery.py", "--print-config", *(_for_date_args(for_date))]
        ai_assist_args = ["tools/dump_prospect_ai_assist_review.py", "--dry-run", *(_for_date_args(for_date))]

    generation_stage_name = "generation_dry_run" if args.dry_run else "generation_live"
    discovery_stage_name = "discovery_print_config" if args.dry_run else "discovery_live"
    ai_assist_stage_name = "ai_assist_dump_dry_run" if args.dry_run else "ai_assist_dump_live"

    if not args.dry_run:
        rc, _ = _run_stage(
            repo_root=repo_root,
            stage_name="doctor_generation",
            script_and_args=["run_prospect_generation.py", "--doctor"],
            env=env,
        )
        if rc != 0:
            return rc
        rc = _run_ai_assist_pending_imports(dry_run=False)
        if rc != 0:
            return rc

    rc, generation_text = _run_stage(
        repo_root=repo_root,
        stage_name=generation_stage_name,
        script_and_args=generation_args,
        env=env,
    )
    if rc != 0:
        return rc

    rc, discovery_text = _run_stage(
        repo_root=repo_root,
        stage_name=discovery_stage_name,
        script_and_args=discovery_args,
        env=env,
    )
    if rc != 0:
        return rc

    rc, ai_assist_text = _run_stage(
        repo_root=repo_root,
        stage_name=ai_assist_stage_name,
        script_and_args=ai_assist_args,
        env=env,
    )
    if rc != 0:
        return rc

    selected_state = _last_token_value(generation_text, "GENERATOR_AUTOGROW_SELECTED_STATE")
    backlog_current = _int_or_default(_last_token_value(generation_text, "GENERATOR_AUTOGROW_BACKLOG_CURRENT"), 0)
    new_needed = _int_or_default(_last_token_value(generation_text, "GENERATOR_AUTOGROW_NEW_NEEDED"), 0)
    generated_rows = _int_or_default(_last_token_value(generation_text, "GENERATOR_ROWS_WRITTEN"), 0)

    discovery_rows_read = 0
    discovery_rows_upserted = 0
    if not args.dry_run:
        discovery_rows_read = _int_or_default(_last_token_value(discovery_text, "DISCOVERY_ROWS_READ"), 0)
        discovery_rows_upserted = _int_or_default(_last_token_value(discovery_text, "DISCOVERY_PROSPECTS_UPSERTED"), 0)

    _emit("PROSPECT_REPLENISH_SELECTED_STATE", selected_state or "")
    _emit("PROSPECT_REPLENISH_BACKLOG_CURRENT", backlog_current)
    _emit("PROSPECT_REPLENISH_NEW_NEEDED", new_needed)
    _emit("PROSPECT_REPLENISH_GENERATOR_ROWS_WRITTEN", generated_rows)
    _emit("PROSPECT_REPLENISH_DISCOVERY_ROWS_READ", discovery_rows_read)
    _emit("PROSPECT_REPLENISH_DISCOVERY_PROSPECTS_UPSERTED", discovery_rows_upserted)
    _emit(
        "PROSPECT_REPLENISH_AI_ASSIST_GAP_TOTAL",
        _int_or_default(_last_token_value(ai_assist_text, "AI_ASSIST_DUMP_GAP_TOTAL"), 0),
    )
    _emit(
        "PROSPECT_REPLENISH_AI_ASSIST_CANDIDATES_REQUESTED_TOTAL",
        _int_or_default(_last_token_value(ai_assist_text, "AI_ASSIST_DUMP_CANDIDATES_REQUESTED_TOTAL"), 0),
    )
    _emit(
        "PROSPECT_REPLENISH_AI_ASSIST_OUTPUT_PATH",
        _last_token_value(ai_assist_text, "AI_ASSIST_DUMP_OUTPUT_PATH"),
    )

    status = "DRY_RUN" if args.dry_run else "OK"
    print(f"{PASS_PROSPECT_REPLENISH_COMPLETE} status={status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
