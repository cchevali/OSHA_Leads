import importlib
import sys
from pathlib import Path


ERR_DISCOVERY_IMPL_MISSING = "ERR_DISCOVERY_IMPL_MISSING"


def _print_missing_impl_help() -> None:
    repo_root = Path(__file__).resolve().parent
    expected_file = repo_root / "outreach" / "run_prospect_discovery.py"
    print(
        f"{ERR_DISCOVERY_IMPL_MISSING} expected_module=outreach.run_prospect_discovery expected_file={expected_file}",
        file=sys.stderr,
    )
    print(
        "REMEDIATION: .\\run_with_secrets.ps1 -- py -3 run_prospect_discovery.py --dry-run",
        file=sys.stderr,
    )
    print(
        "REMEDIATION: .\\run_with_secrets.ps1 -- py -3 outreach\\crm_admin.py seed --input C:\\path\\to\\prospects.csv",
        file=sys.stderr,
    )


def main() -> int:
    try:
        mod = importlib.import_module("outreach.run_prospect_discovery")
    except ModuleNotFoundError as exc:
        missing_name = str(getattr(exc, "name", "") or "")
        if missing_name not in {"outreach.run_prospect_discovery", "outreach"}:
            raise
        _print_missing_impl_help()
        return 2

    target = getattr(mod, "main", None)
    if not callable(target):
        _print_missing_impl_help()
        return 2

    try:
        rc = target(sys.argv[1:])
    except TypeError:
        rc = target()

    if isinstance(rc, int):
        return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
