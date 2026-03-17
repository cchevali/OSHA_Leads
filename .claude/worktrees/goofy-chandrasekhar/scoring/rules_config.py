from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_SCORING_DIR = REPO_ROOT / "data" / "scoring"


@dataclass(frozen=True)
class NaicsBoostRule:
    naics_prefix: str
    label: str
    boost_points: int


@dataclass(frozen=True)
class NaicsSuppressRule:
    naics_prefix: str
    label: str
    reason: str


@dataclass(frozen=True)
class EnterprisePatternRule:
    pattern: str
    match_type: str
    reason: str


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [{str(k or "").strip(): str(v or "").strip() for k, v in row.items()} for row in reader]


def _norm_digits(value: object) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _clean_prefix(value: str) -> str:
    return str(value or "").strip().upper().replace(" ", "")


def _naics_match_prefix(code_digits: str, prefix: str) -> bool:
    p = _clean_prefix(prefix)
    if not p or not code_digits:
        return False
    # Range prefix form: 44-45*.
    m = re.fullmatch(r"(\d{2})-(\d{2})\*", p)
    if m:
        start = int(m.group(1))
        end = int(m.group(2))
        if len(code_digits) < 2:
            return False
        try:
            first = int(code_digits[:2])
        except Exception:
            return False
        return start <= first <= end
    if p.endswith("*"):
        base = p[:-1]
    else:
        base = p
    base_digits = _norm_digits(base)
    if not base_digits:
        return False
    return code_digits.startswith(base_digits)


@lru_cache(maxsize=1)
def load_naics_boost_rules() -> list[NaicsBoostRule]:
    path = DATA_SCORING_DIR / "naics_emphasis_boost.csv"
    out: list[NaicsBoostRule] = []
    for row in _read_csv_rows(path):
        prefix = _clean_prefix(row.get("naics_prefix", ""))
        if not prefix:
            continue
        try:
            boost_points = int(str(row.get("boost_points", "0")).strip() or "0")
        except Exception:
            boost_points = 0
        out.append(
            NaicsBoostRule(
                naics_prefix=prefix,
                label=str(row.get("label", "")).strip(),
                boost_points=max(0, int(boost_points)),
            )
        )
    return out


@lru_cache(maxsize=1)
def load_naics_suppress_rules() -> tuple[list[NaicsSuppressRule], list[str]]:
    path = DATA_SCORING_DIR / "naics_suppress.csv"
    suppress: list[NaicsSuppressRule] = []
    allow_prefixes: list[str] = []
    for row in _read_csv_rows(path):
        prefix = _clean_prefix(row.get("naics_prefix", ""))
        if not prefix:
            continue
        if prefix.startswith("!"):
            allow_prefixes.append(prefix[1:])
            continue
        suppress.append(
            NaicsSuppressRule(
                naics_prefix=prefix,
                label=str(row.get("label", "")).strip(),
                reason=str(row.get("reason", "")).strip() or "NAICS_SUPPRESS",
            )
        )
    return suppress, allow_prefixes


@lru_cache(maxsize=1)
def load_enterprise_pattern_rules() -> list[EnterprisePatternRule]:
    path = DATA_SCORING_DIR / "enterprise_names.csv"
    out: list[EnterprisePatternRule] = []
    for row in _read_csv_rows(path):
        pattern = str(row.get("pattern", "")).strip()
        if not pattern:
            continue
        match_type = str(row.get("match_type", "")).strip().lower() or "contains"
        if match_type not in {"exact", "contains", "regex"}:
            continue
        out.append(
            EnterprisePatternRule(
                pattern=pattern,
                match_type=match_type,
                reason=str(row.get("reason", "")).strip() or "ENTERPRISE_NATIONAL",
            )
        )
    return out


def match_naics_boost(code: object) -> list[NaicsBoostRule]:
    code_digits = _norm_digits(code)
    if not code_digits:
        return []
    out: list[NaicsBoostRule] = []
    for rule in load_naics_boost_rules():
        if _naics_match_prefix(code_digits, rule.naics_prefix):
            out.append(rule)
    return out


def match_naics_suppress(code: object) -> NaicsSuppressRule | None:
    code_digits = _norm_digits(code)
    if not code_digits:
        return None
    suppress_rules, allow_prefixes = load_naics_suppress_rules()
    for allow_prefix in allow_prefixes:
        if _naics_match_prefix(code_digits, allow_prefix):
            return None
    for rule in suppress_rules:
        if _naics_match_prefix(code_digits, rule.naics_prefix):
            return rule
    return None


def match_enterprise_pattern(name: object) -> EnterprisePatternRule | None:
    text = str(name or "").strip()
    text_low = text.lower()
    if not text_low:
        return None
    for rule in load_enterprise_pattern_rules():
        pattern = rule.pattern
        if rule.match_type == "exact":
            if text_low == pattern.lower():
                return rule
            continue
        if rule.match_type == "contains":
            if pattern.lower() in text_low:
                return rule
            continue
        if rule.match_type == "regex":
            try:
                if re.search(pattern, text, flags=re.IGNORECASE):
                    return rule
            except re.error:
                continue
    return None

