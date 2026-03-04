import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import ingest_osha


def _inspection(activity_nr: str, state: str, detail_url: str) -> dict:
    return {
        "activity_nr": activity_nr,
        "site_state": state,
        "detail_url": detail_url,
    }


def _build_ingestion_log_table(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE ingestion_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_started_at TEXT,
                states_queried TEXT,
                since_days INTEGER,
                status TEXT,
                run_completed_at TEXT,
                results_found INTEGER,
                details_fetched INTEGER,
                rows_inserted INTEGER,
                rows_updated INTEGER,
                errors_count INTEGER,
                error_message TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


class TestIngestFetchPlan(unittest.TestCase):
    def test_round_robin_plan_includes_wa_under_cap(self):
        states = ["CA", "OR", "WA"]
        inspections_by_state = {
            "CA": [_inspection(f"ca{i:03d}", "CA", f"url-ca-{i}") for i in range(59)],
            "OR": [_inspection(f"or{i:03d}", "OR", f"url-or-{i}") for i in range(50)],
            "WA": [_inspection(f"wa{i:03d}", "WA", f"url-wa-{i}") for i in range(53)],
        }
        plan, planned_by_state = ingest_osha._build_round_robin_fetch_plan(
            inspections_by_state=inspections_by_state,
            states=states,
            max_details=100,
        )

        self.assertEqual(len(plan), 100)
        self.assertEqual(planned_by_state.get("CA"), 34)
        self.assertEqual(planned_by_state.get("OR"), 33)
        self.assertEqual(planned_by_state.get("WA"), 33)
        self.assertGreater(int(planned_by_state.get("WA") or 0), 0)
        sequence = [str(item.get("site_state") or "") for item in plan[:6]]
        self.assertEqual(sequence, ["CA", "OR", "WA", "CA", "OR", "WA"])

    def test_run_ingestion_dedupes_cross_state_activity_once(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = Path(d) / "osha.sqlite"
            _build_ingestion_log_table(db_path)

            fetch_urls: list[str] = []

            class _FakeResponse:
                text = "<html></html>"

            def _fake_search(_session, state, _since_date):  # noqa: ANN001
                if state == "CA":
                    return [
                        _inspection("1001", "CA", "url-ca-1001"),
                        _inspection("2002", "CA", "url-ca-2002"),
                    ]
                if state == "OR":
                    return [
                        _inspection("2002", "OR", "url-or-2002"),
                        _inspection("3003", "OR", "url-or-3003"),
                    ]
                if state == "WA":
                    return [_inspection("4004", "WA", "url-wa-4004")]
                return []

            def _fake_fetch(_session, url, retries=3):  # noqa: ANN001, ARG001
                fetch_urls.append(str(url))
                return _FakeResponse()

            with mock.patch.object(ingest_osha, "ensure_inspection_columns", return_value=None), mock.patch.object(
                ingest_osha, "get_session", return_value=object()
            ), mock.patch.object(
                ingest_osha, "search_osha_inspections", side_effect=_fake_search
            ), mock.patch.object(
                ingest_osha, "fetch_with_retry", side_effect=_fake_fetch
            ), mock.patch.object(
                ingest_osha, "parse_inspection_detail", return_value={}
            ), mock.patch.object(
                ingest_osha, "upsert_inspection", return_value=(True, False)
            ), self.assertLogs(ingest_osha.logger, level="INFO") as captured:
                stats = ingest_osha.run_ingestion(
                    db_path=str(db_path),
                    since_days=14,
                    states=["CA", "OR", "WA"],
                    max_details=10,
                )

            self.assertEqual(int(stats.get("results_found") or 0), 5)
            self.assertEqual(int(stats.get("details_fetched") or 0), 4)
            self.assertEqual(len(fetch_urls), 4)
            self.assertIn("url-ca-2002", fetch_urls)
            self.assertNotIn("url-or-2002", fetch_urls)
            joined = "\n".join(captured.output)
            self.assertIn("INGEST_CANDIDATES_BY_STATE state=WA count=1", joined)
            self.assertIn("INGEST_FETCH_PLAN_BY_STATE state=WA planned=1", joined)


if __name__ == "__main__":
    unittest.main()
