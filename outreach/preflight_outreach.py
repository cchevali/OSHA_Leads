import os
import sys
from pathlib import Path
from urllib.parse import urlparse

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass

# When invoked as `py -3 outreach/preflight_outreach.py`, sys.path[0] is `outreach/`.
# Add repo root so `import outreach` resolves reliably.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


PASS = "PASS"
FAIL = "FAIL"


def _host_only(url: str) -> str:
    try:
        u = urlparse((url or "").strip())
        if u.scheme and u.netloc:
            return f"{u.scheme}://{u.netloc}"
    except Exception:
        pass
    return ""


def main() -> int:
    # Keep this script side-effect free: no outputs, no logs, no writes.
    try:
        from outreach import generate_mailmerge as gm
    except Exception as e:
        print(f"{FAIL} ERR_PREFLIGHT_IMPORT {e}", file=sys.stderr)
        return 1

    ok = True

    # Suppression gate (must exist; do not create it here).
    try:
        suppressed = gm._load_local_suppression_set()
        sup_path = ""
        try:
            import unsubscribe_utils as uu

            sup_path = str(getattr(uu, "SUPPRESSION_PATH"))
        except Exception:
            sup_path = "out/suppression.csv"
        print(f"{PASS} suppression_present path={sup_path} entries={len(suppressed)}")
    except Exception as e:
        msg = str(e or "").strip()
        token = gm.ERR_SUPPRESSION_REQUIRED if hasattr(gm, "ERR_SUPPRESSION_REQUIRED") else "ERR_SUPPRESSION_REQUIRED"
        if token in msg:
            print(f"{FAIL} {msg}", file=sys.stderr)
        else:
            print(f"{FAIL} {token} suppression_check_failed err={msg}", file=sys.stderr)
        ok = False

    # One-click config gate (required for real exports).
    try:
        present, reason = gm._one_click_config_present()
        if present:
            base = (os.getenv("UNSUB_ENDPOINT_BASE") or "").strip()
            print(f"{PASS} one_click_present base={_host_only(base) or 'configured'}")
        else:
            print(f"{FAIL} {gm.ERR_ONE_CLICK_REQUIRED} {reason}".strip(), file=sys.stderr)
            ok = False
    except Exception as e:
        print(f"{FAIL} {gm.ERR_ONE_CLICK_REQUIRED} preflight_exception {e}", file=sys.stderr)
        ok = False

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
