import argparse
import csv
import subprocess
import sys
from pathlib import Path

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


REPO_ROOT = Path(__file__).resolve().parents[1]


def _safe_batch_name(batch: str) -> str:
    raw = (batch or "").strip()
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in raw).strip("_")
    return safe or "batch"


def _run_cmd(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        args,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )


def _parse_key_value_lines(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in (text or "").splitlines():
        s = (line or "").strip()
        if not s or "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _count_manifest_dropped(path: Path) -> int:
    if not path.exists():
        return 0
    dropped = 0
    with open(path, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if (row.get("status") or "").strip().lower() == "dropped":
                dropped += 1
    return dropped


def _mirror_latest_run_log(src_path: Path, dst_dir: Path) -> Path | None:
    if not src_path.exists():
        return None

    last_line = ""
    with open(src_path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                last_line = s
    if not last_line:
        return None

    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / "run_log.jsonl"
    with open(dst, "a", encoding="utf-8") as f:
        f.write(last_line + "\n")
    return dst


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Run outreach batch end-to-end: preflight then generate_mailmerge into out/outreach/<batch>/."
    )
    ap.add_argument("--state", required=True, help="2-letter state (e.g., TX)")
    ap.add_argument("--batch", required=True, help="Batch id (e.g., TX_W2)")
    ap.add_argument("--input", required=True, help="Prospects CSV input path")
    ap.add_argument("--out-root", default=str(Path("out") / "outreach"), help="Output root directory")
    ap.add_argument("--db", default="", help="Optional db path pass-through to generate_mailmerge")
    ap.add_argument(
        "--allow-mailto-fallback",
        action="store_true",
        help="Pass through to generate_mailmerge (preview/local only)",
    )
    args = ap.parse_args()

    batch_safe = _safe_batch_name(args.batch)
    out_dir = (Path(args.out_root) / batch_safe).resolve()
    outbox = out_dir / f"outbox_{batch_safe}.csv"

    # Step 1: preflight gate (no writes)
    preflight_cmd = [sys.executable, str(REPO_ROOT / "outreach" / "preflight_outreach.py")]
    preflight = _run_cmd(preflight_cmd)
    if preflight.stdout:
        print(preflight.stdout.strip())
    if preflight.stderr:
        print(preflight.stderr.strip(), file=sys.stderr)
    if preflight.returncode != 0:
        print("ERR_RUN_BATCH_PREFLIGHT", file=sys.stderr)
        return preflight.returncode

    # Step 2: generate outbox/manifest
    gen_cmd = [
        sys.executable,
        str(REPO_ROOT / "outreach" / "generate_mailmerge.py"),
        "--input",
        str(Path(args.input)),
        "--batch",
        str(args.batch),
        "--state",
        str(args.state),
        "--out",
        str(outbox),
    ]
    if args.db:
        gen_cmd.extend(["--db", str(args.db)])
    if args.allow_mailto_fallback:
        gen_cmd.append("--allow-mailto-fallback")

    generated = _run_cmd(gen_cmd)
    if generated.stdout:
        print(generated.stdout.strip())
    if generated.stderr:
        print(generated.stderr.strip(), file=sys.stderr)
    if generated.returncode != 0:
        # Defensive cleanup on failed run.
        manifest_failed = outbox.with_name(f"{outbox.stem}_manifest.csv")
        for p in [outbox, manifest_failed]:
            if p.exists():
                try:
                    p.unlink()
                except Exception:
                    pass
        if out_dir.exists():
            try:
                if not any(out_dir.iterdir()):
                    out_dir.rmdir()
            except Exception:
                pass
        return generated.returncode

    kv = _parse_key_value_lines(generated.stdout or "")
    exported = int((kv.get("exported") or "0").strip() or 0)

    manifest_path = Path(kv.get("manifest") or outbox.with_name(f"{outbox.stem}_manifest.csv"))
    dropped = _count_manifest_dropped(manifest_path)

    run_log_src = Path(kv.get("run_log") or "")
    mirrored_log = _mirror_latest_run_log(run_log_src, out_dir) if run_log_src else None

    print(f"batch={args.batch}")
    print(f"state={args.state}")
    print(f"exported_count={exported}")
    print(f"dropped_count={dropped}")
    print(f"outbox_path={outbox}")
    print(f"manifest_path={manifest_path}")
    if mirrored_log:
        print(f"run_log_path={mirrored_log}")

    outbox_win = str(outbox).replace("/", "\\")
    print("next_steps:")
    print(f"1) QA CSV + manifest in {str(out_dir).replace('/', '\\')}")
    print(f"2) .\\run_with_secrets.ps1 -- py -3 outreach\\send_test_cold_email.py --outbox {outbox_win}")
    print("3) Upload outbox to your external sender after QA")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
