import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from PIL import Image, ImageChops

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from email_footer import build_footer_html
from send_digest_email import generate_digest_html


def _find_msedge() -> str | None:
    exe = shutil.which("msedge")
    if exe:
        return exe
    candidates = [
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    ]
    for c in candidates:
        if Path(c).exists():
            return c
    return None


def _now_utc_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _dummy_lead(i: int, tier: str) -> dict:
    score = {"high": 11, "medium": 7, "low": 3}.get(tier, 6)
    return {
        "establishment_name": f"Example Company {i:03d} LLC",
        "site_city": "Example City",
        "site_state": "ST",
        "inspection_type": "Complaint" if i % 3 else "Accident",
        "date_opened": "2026-02-05",
        "lead_score": score,
        "source_url": f"https://www.osha.gov/ords/imis/establishment.inspection_detail?id=99999{i:03d}",
        "first_seen_at": "2026-02-06T12:00:00+00:00",
    }


def _crop_to_content(png_in: Path, png_out: Path) -> None:
    img = Image.open(png_in).convert("RGB")
    # Match digest background: #f7f9fc
    bg = Image.new("RGB", img.size, (0xF7, 0xF9, 0xFC))
    diff = ImageChops.difference(img, bg)
    bbox = diff.getbbox()
    if not bbox:
        img.save(png_out)
        return
    pad = 14
    left = max(0, bbox[0] - pad)
    top = max(0, bbox[1] - pad)
    right = min(img.width, bbox[2] + pad)
    bottom = min(img.height, bbox[3] + pad)
    cropped = img.crop((left, top, right, bottom))
    cropped.save(png_out, optimize=True)


def main() -> int:
    out_dir = ROOT / "web" / "public" / "assets"
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / "sample-digest-preview.html"
    png_raw = out_dir / "sample-digest-preview.raw.png"
    png_path = out_dir / "sample-digest-preview.png"

    branding = {
        "brand_name": "MicroFlowOps",
        "brand_legal_name": "MicroFlowOps",
        "mailing_address": "11539 Links Dr, Reston, VA 20190",
        "from_email": "alerts@example.com",
        "reply_to": "support@microflowops.com",
        "from_display_name": "MicroFlowOps OSHA Alerts",
    }
    footer_html = build_footer_html(
        brand_name=branding["brand_legal_name"],
        mailing_address=branding["mailing_address"],
        disclaimer="Informational only. Not legal advice.",
        reply_to=branding["reply_to"],
        unsub_url=None,
    )

    # Keep to ~8 rows so the resulting asset stays readable while still showing real structure + footer.
    leads = [_dummy_lead(i, tier=("high" if i in {1, 6} else "medium")) for i in range(1, 9)]
    tier_counts = {
        "high": 2,
        "medium": 6,
        "low": 12,
    }
    enable_lows_url = "https://unsub.microflowops.com/prefs/enable_lows?t=dummy.dummy"

    html = generate_digest_html(
        leads=leads,
        low_fallback=[],
        config={"states": ["ST"], "top_k_overall": 200, "top_k_per_state": 200},
        gen_date=_now_utc_date(),
        mode="daily",
        territory_code="EXAMPLE_TERRITORY",
        content_filter="high_medium",
        include_low_fallback=False,
        branding=branding,
        tier_counts=tier_counts,
        enable_lows_url=enable_lows_url,
        include_lows=False,
        low_priority=[],
        signals_limit=len(leads),
        report_label="Example Territory - Daily Brief",
        footer_html=footer_html,
        summary_label=f"Newly observed today: {len(leads)} signals",
        coverage_line="Sample format (dummy data)",
        health_summary_html=None,
        tz=None,
    )
    html_path.write_text(html, encoding="utf-8")

    edge = _find_msedge()
    if not edge:
        raise SystemExit("msedge_not_found")

    url = html_path.resolve().as_uri()
    cmd = [
        edge,
        "--headless",
        "--disable-gpu",
        "--hide-scrollbars",
        # Mobile-ish width so the resulting asset is readable when embedded on mobile.
        # Use a tall viewport so the screenshot includes the footer/compliance area without needing "full page" APIs.
        "--window-size=420,3600",
        f"--screenshot={str(png_raw.resolve())}",
        url,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
    if proc.returncode != 0 or not png_raw.exists() or png_raw.stat().st_size <= 0:
        tail = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()[-1200:]
        raise SystemExit(f"edge_screenshot_failed: {tail}")

    _crop_to_content(png_raw, png_path)
    try:
        png_raw.unlink(missing_ok=True)
    except Exception:
        pass

    print(f"WROTE {html_path}")
    print(f"WROTE {png_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
