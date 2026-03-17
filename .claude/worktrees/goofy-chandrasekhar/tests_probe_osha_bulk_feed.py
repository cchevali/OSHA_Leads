import io
import unittest
import zipfile
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from tools import probe_osha_bulk_feed as tool


class _FakeResponse:
    def __init__(self, *, status_code: int, content: bytes, headers: dict[str, str] | None = None, url: str = ""):
        self.status_code = int(status_code)
        self.content = bytes(content or b"")
        self.headers = dict(headers or {})
        self.url = str(url or "")


def _zip_payload_from_csv(csv_text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("osha_inspection_sample.csv", csv_text)
    return buf.getvalue()


class TestProbeOshaBulkFeed(unittest.TestCase):
    def test_non_zip_response_emits_warn_and_err(self):
        def _fake_get(url, timeout):  # noqa: ANN001
            return _FakeResponse(
                status_code=200,
                content=b"<html><body>not a zip payload</body></html>",
                headers={"Content-Type": "text/html"},
                url=str(url),
            )

        out = io.StringIO()
        with mock.patch.object(tool.requests, "get", side_effect=_fake_get):
            with redirect_stdout(out):
                rc = tool.main(["--states", "WA"])
        text = out.getvalue()
        self.assertNotEqual(rc, 0, msg=text)
        self.assertIn("WARN_OSHA_BULK_FEED_NON_ZIP", text)
        self.assertIn("ERR_OSHA_BULK_FEED_UNAVAILABLE", text)

    def test_probe_fallback_to_yesterday_and_emit_state_counts(self):
        csv_text = (
            "site_state,date_opened,activity_nr\n"
            "WA,2026-03-01,1001\n"
            "TX,2026-03-02,1002\n"
            "WA,2026-02-01,1003\n"
        )
        zip_bytes = _zip_payload_from_csv(csv_text)
        responses = [
            _FakeResponse(
                status_code=200,
                content=b"<html><body>shell page</body></html>",
                headers={"Content-Type": "text/html"},
                url="https://enfxfr.dol.gov/data_catalog/OSHA/osha_inspection_today.csv.zip",
            ),
            _FakeResponse(
                status_code=200,
                content=zip_bytes,
                headers={"Content-Type": "application/zip"},
                url="https://enfxfr.dol.gov/data_catalog/OSHA/osha_inspection_yesterday.csv.zip",
            ),
        ]

        out = io.StringIO()
        with mock.patch.object(tool.requests, "get", side_effect=responses):
            with redirect_stdout(out):
                rc = tool.main(["--states", "WA,TX", "--since-days", "365"])
        text = out.getvalue()
        self.assertEqual(rc, 0, msg=text)
        self.assertIn("WARN_OSHA_BULK_FEED_NON_ZIP", text)
        self.assertIn("OSHA_BULK_FEED_OK", text)
        self.assertIn("OSHA_BULK_FEED_STATE state=WA rows=2 open_date_max=2026-03-01", text)
        self.assertIn("OSHA_BULK_FEED_STATE state=TX rows=1 open_date_max=2026-03-02", text)
        self.assertIn("PASS_OSHA_BULK_FEED_COMPLETE status=OK", text)


if __name__ == "__main__":
    unittest.main()
