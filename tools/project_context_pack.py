#!/usr/bin/env python3
"""Build/check/mark local Project Context Pack upload state."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

PACK_FILENAME = "PROJECT_CONTEXT_PACK.md"
UPLOAD_STATE_PATH = Path(".local") / "project_upload_state.json"
PACK_HASH_PLACEHOLDER = "__PACK_HASH_PLACEHOLDER__"
UPLOAD_INSTRUCTION = "Upload PROJECT_CONTEXT_PACK.md to ChatGPT Project Settings -> Files"

REQUIRED_DOCS = [
    "AGENTS.md",
    "docs/PROJECT_BRIEF.md",
    "docs/ARCHITECTURE.md",
    "docs/DECISIONS.md",
    "docs/RUNBOOK.md",
    "docs/V1_CUSTOMER_VALIDATED.md",
    "docs/TODO.md",
]
OPTIONAL_DOCS = [
    "docs/READINESS_AUDIT.md",
]
UNKNOWN_VALUE = "UNKNOWN"
UTC_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
SHA1_HEX_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass
class Issue:
    code: str
    message: str


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _git_head_sha(repo_root: Path) -> str:
    try:
        out = subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        sha = (out or "").strip()
        if sha:
            return sha
    except Exception:
        pass
    return "UNKNOWN"


def _source_doc_paths(repo_root: Path) -> list[Path]:
    docs: list[Path] = []
    for rel in REQUIRED_DOCS:
        path = repo_root / rel
        if not path.exists():
            raise FileNotFoundError(f"required source doc missing: {rel}")
        docs.append(path)
    for rel in OPTIONAL_DOCS:
        path = repo_root / rel
        if path.exists():
            docs.append(path)
    return docs


def _relpath(repo_root: Path, path: Path) -> str:
    return path.resolve().relative_to(repo_root.resolve()).as_posix()


def source_hashes(repo_root: Path) -> dict[str, str]:
    pairs = {}
    for path in _source_doc_paths(repo_root):
        rel = _relpath(repo_root, path)
        pairs[rel] = _sha256_file(path)
    return dict(sorted(pairs.items()))


def _canonicalize_pack_hash_input(text: str) -> str:
    marker = "PACK_HASH="
    lines = []
    for line in text.splitlines():
        if line.startswith(marker):
            lines.append(marker + PACK_HASH_PLACEHOLDER)
        else:
            lines.append(line)
    return "\n".join(lines) + "\n"


def compute_pack_hash_from_text(text: str) -> str:
    canonical = _canonicalize_pack_hash_input(text)
    return _sha256_bytes(canonical.encode("utf-8"))


def _render_pack_text(
    *,
    repo_root: Path,
    pack_git_sha: str,
    pack_build_utc: str,
    hashes: dict[str, str],
) -> str:
    header = [
        "# PROJECT_CONTEXT_PACK",
        "",
        f"PACK_GIT_SHA={pack_git_sha}",
        f"PACK_BUILD_UTC={pack_build_utc}",
        "SOURCE_HASHES: " + " ".join([f"{k}={v}" for k, v in hashes.items()]),
        f"PACK_HASH={PACK_HASH_PLACEHOLDER}",
        "",
        "Generated from canonical repo docs. Upload this single file to ChatGPT Project Settings -> Files.",
        "",
    ]

    body: list[str] = []
    for rel in hashes.keys():
        text = (repo_root / rel).read_text(encoding="utf-8")
        body.extend(
            [
                f"## {rel}",
                "```md",
                text.rstrip("\n"),
                "```",
                "",
            ]
        )
    template = "\n".join(header + body).rstrip("\n") + "\n"
    pack_hash = compute_pack_hash_from_text(template)
    return template.replace(PACK_HASH_PLACEHOLDER, pack_hash)


def generate_pack_text(
    repo_root: Path,
    *,
    pack_git_sha: str | None = None,
    pack_build_utc: str | None = None,
) -> str:
    git_sha = pack_git_sha or _git_head_sha(repo_root)
    build_utc = pack_build_utc or _utc_now()
    hashes = source_hashes(repo_root)
    return _render_pack_text(repo_root=repo_root, pack_git_sha=git_sha, pack_build_utc=build_utc, hashes=hashes)


def parse_pack_metadata(text: str) -> dict[str, object]:
    meta: dict[str, object] = {"source_hashes": {}}
    lines = text.splitlines()
    for line in lines:
        if line.startswith("PACK_GIT_SHA="):
            meta["pack_git_sha"] = line.split("=", 1)[1].strip()
        elif line.startswith("PACK_BUILD_UTC="):
            meta["pack_build_utc"] = line.split("=", 1)[1].strip()
        elif line.startswith("PACK_HASH="):
            meta["pack_hash"] = line.split("=", 1)[1].strip()
        elif line.startswith("SOURCE_HASHES:"):
            payload = line.split(":", 1)[1].strip()
            parts = payload.split(" ") if payload else []
            hashes: dict[str, str] = {}
            for part in parts:
                if not part or "=" not in part:
                    continue
                k, v = part.split("=", 1)
                hashes[k.strip()] = v.strip()
            meta["source_hashes"] = hashes
    return meta


def build_pack(repo_root: Path) -> int:
    try:
        text = generate_pack_text(repo_root)
    except FileNotFoundError as e:
        print(f"ERR_CONTEXT_PACK_SOURCE_MISSING {e}")
        return 1
    pack_path = repo_root / PACK_FILENAME
    pack_path.write_text(text, encoding="utf-8")
    meta = parse_pack_metadata(text)
    print(f"PASS_CONTEXT_PACK_BUILT path={pack_path} pack_hash={meta.get('pack_hash','')}")
    return 0


def _emit_issue(issue: Issue, soft: bool) -> None:
    prefix = "WARN_" if soft else "ERR_"
    token = issue.code.replace("ERR_", prefix) if issue.code.startswith("ERR_") else issue.code
    print(f"{token} {issue.message}".strip())


def _load_upload_state(repo_root: Path) -> dict[str, object] | None:
    path = repo_root / UPLOAD_STATE_PATH
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"_invalid": True}


def _normalized_value(value: object, pattern: re.Pattern[str]) -> str:
    text = str(value or "").strip()
    if pattern.fullmatch(text):
        return text
    return UNKNOWN_VALUE


def fingerprint_pack(repo_root: Path) -> int:
    pack_git_sha = UNKNOWN_VALUE
    pack_build_utc = UNKNOWN_VALUE
    pack_hash = UNKNOWN_VALUE

    pack_path = repo_root / PACK_FILENAME
    if pack_path.exists():
        try:
            meta = parse_pack_metadata(pack_path.read_text(encoding="utf-8"))
            pack_git_sha = _normalized_value(meta.get("pack_git_sha"), SHA1_HEX_RE)
            pack_build_utc = _normalized_value(meta.get("pack_build_utc"), UTC_ISO_RE)
            pack_hash = _normalized_value(meta.get("pack_hash"), SHA256_HEX_RE)
        except Exception:
            pass

    upload_marked = UNKNOWN_VALUE
    upload_marked_at_utc = UNKNOWN_VALUE
    state = _load_upload_state(repo_root)
    if state is None:
        upload_marked = "NO"
    elif state.get("_invalid"):
        upload_marked = UNKNOWN_VALUE
    else:
        marked_hash = _normalized_value(state.get("pack_hash"), SHA256_HEX_RE)
        upload_marked_at_utc = _normalized_value(state.get("marked_uploaded_utc"), UTC_ISO_RE)
        if marked_hash == UNKNOWN_VALUE:
            upload_marked = UNKNOWN_VALUE
        elif pack_hash == UNKNOWN_VALUE:
            upload_marked = UNKNOWN_VALUE
        elif marked_hash == pack_hash:
            upload_marked = "YES"
        else:
            upload_marked = "NO"

    print(f"PACK_GIT_SHA={pack_git_sha}")
    print(f"PACK_BUILD_UTC={pack_build_utc}")
    print(f"PACK_HASH={pack_hash}")
    print(f"UPLOAD_MARKED={upload_marked}")
    print(f"UPLOAD_MARKED_AT_UTC={upload_marked_at_utc}")
    return 0


def check_pack(repo_root: Path, *, soft: bool = False, current_git_sha: str | None = None) -> int:
    issues: list[Issue] = []
    pack_path = repo_root / PACK_FILENAME
    if not pack_path.exists():
        issues.append(Issue("ERR_CONTEXT_PACK_MISSING", f"missing file {PACK_FILENAME}"))
    else:
        text = pack_path.read_text(encoding="utf-8")
        meta = parse_pack_metadata(text)
        embedded_sha = str(meta.get("pack_git_sha") or "")
        embedded_hash = str(meta.get("pack_hash") or "")
        embedded_source_hashes = dict(meta.get("source_hashes") or {})
        computed_pack_hash = compute_pack_hash_from_text(text)
        if not embedded_sha or not embedded_hash:
            issues.append(Issue("ERR_CONTEXT_PACK_PARSE", "required metadata missing from pack header"))
        elif embedded_hash != computed_pack_hash:
            issues.append(Issue("ERR_CONTEXT_PACK_HASH_MISMATCH", "embedded PACK_HASH does not match file content"))

        expected_git_sha = current_git_sha or _git_head_sha(repo_root)
        try:
            expected_source_hashes = source_hashes(repo_root)
        except FileNotFoundError as e:
            issues.append(Issue("ERR_CONTEXT_PACK_SOURCE_MISSING", str(e)))
            expected_source_hashes = {}
        if embedded_sha and expected_git_sha and embedded_sha != expected_git_sha:
            issues.append(Issue("ERR_CONTEXT_PACK_STALE", f"PACK_GIT_SHA mismatch expected={expected_git_sha} actual={embedded_sha}"))
        if embedded_source_hashes and expected_source_hashes and embedded_source_hashes != expected_source_hashes:
            issues.append(Issue("ERR_CONTEXT_PACK_STALE", "SOURCE_HASHES mismatch"))

        state = _load_upload_state(repo_root)
        if state is None:
            issues.append(Issue("ERR_CONTEXT_PACK_UPLOAD_STATE_MISSING", f"missing {UPLOAD_STATE_PATH.as_posix()}"))
        elif state.get("_invalid"):
            issues.append(Issue("ERR_CONTEXT_PACK_UPLOAD_STATE_INVALID", f"invalid JSON in {UPLOAD_STATE_PATH.as_posix()}"))
        else:
            marked_hash = str(state.get("pack_hash") or "")
            if not marked_hash:
                issues.append(Issue("ERR_CONTEXT_PACK_UPLOAD_STATE_INVALID", "pack_hash missing in upload state"))
            elif embedded_hash and marked_hash != embedded_hash:
                issues.append(Issue("ERR_CONTEXT_PACK_UPLOAD_STATE_STALE", "marked uploaded hash does not match current pack"))

    if issues:
        for issue in issues:
            _emit_issue(issue, soft=soft)
        print(UPLOAD_INSTRUCTION)
        print("Then run: py -3 tools/project_context_pack.py --mark-uploaded")
        if soft:
            print(f"PASS_CONTEXT_PACK_CHECK mode=soft warnings={len(issues)}")
            return 0
        return 1

    print(f"PASS_CONTEXT_PACK_CHECK mode={'soft' if soft else 'strict'}")
    return 0


def mark_uploaded(repo_root: Path) -> int:
    pack_path = repo_root / PACK_FILENAME
    if not pack_path.exists():
        print(f"ERR_CONTEXT_PACK_MISSING missing file {PACK_FILENAME}")
        return 1
    text = pack_path.read_text(encoding="utf-8")
    meta = parse_pack_metadata(text)
    pack_hash = str(meta.get("pack_hash") or "")
    pack_git_sha = str(meta.get("pack_git_sha") or "")
    if not pack_hash or not pack_git_sha:
        print("ERR_CONTEXT_PACK_PARSE required metadata missing from pack header")
        return 1

    state = {
        "pack_hash": pack_hash,
        "pack_git_sha": pack_git_sha,
        "marked_uploaded_utc": _utc_now(),
    }
    state_path = repo_root / UPLOAD_STATE_PATH
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    print(f"PASS_CONTEXT_PACK_MARKED_UPLOADED state={state_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build/check local Project Context Pack upload freshness.")
    ap.add_argument("--build", action="store_true", help="Generate PROJECT_CONTEXT_PACK.md from canonical docs.")
    ap.add_argument("--check", action="store_true", help="Validate context pack freshness and upload state.")
    ap.add_argument("--soft", action="store_true", help="With --check: warning-only mode (exit 0 on issues).")
    ap.add_argument("--mark-uploaded", action="store_true", help="Mark current pack hash as uploaded (local state only).")
    ap.add_argument("--fingerprint", action="store_true", help="Print deterministic low-token pack and upload marker status.")
    args = ap.parse_args(argv)

    modes = [args.build, args.check, args.mark_uploaded, args.fingerprint]
    if sum(1 for m in modes if m) != 1:
        ap.error("choose exactly one of --build, --check, --mark-uploaded, --fingerprint")
    if args.soft and not args.check:
        ap.error("--soft is only valid with --check")

    repo_root = _repo_root()
    if args.build:
        return build_pack(repo_root)
    if args.check:
        return check_pack(repo_root, soft=bool(args.soft))
    if args.fingerprint:
        return fingerprint_pack(repo_root)
    return mark_uploaded(repo_root)


if __name__ == "__main__":
    raise SystemExit(main())
