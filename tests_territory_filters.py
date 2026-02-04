import unittest

from lead_filters import (
    apply_content_filter,
    dedupe_by_activity_nr,
    filter_by_territory,
    normalize_content_filter,
)


class TestLeadFilters(unittest.TestCase):
    def test_normalize_content_filter(self):
        self.assertEqual(normalize_content_filter("High+Medium"), "high_medium")
        self.assertEqual(normalize_content_filter("high_only"), "high_only")
        self.assertEqual(normalize_content_filter("all"), "all")

    def test_content_filter_high_medium(self):
        leads = [
            {"activity_nr": "1", "lead_score": 10},
            {"activity_nr": "2", "lead_score": 6},
            {"activity_nr": "3", "lead_score": 5},
        ]
        filtered, excluded = apply_content_filter(leads, "high_medium")
        self.assertEqual([row["activity_nr"] for row in filtered], ["1", "2"])
        self.assertEqual(excluded, 1)

    def test_territory_matches_office_and_fallback_city(self):
        leads = [
            {"activity_nr": "1", "site_state": "TX", "area_office": "Austin Area Office", "site_city": "Round Rock"},
            {"activity_nr": "2", "site_state": "TX", "area_office": "", "site_city": "Houston"},
            {"activity_nr": "3", "site_state": "TX", "area_office": "El Paso Area Office", "site_city": "El Paso"},
            {"activity_nr": "4", "site_state": "OK", "area_office": "Dallas Area Office", "site_city": "Dallas"},
        ]

        filtered, stats = filter_by_territory(leads, "TX_TRIANGLE_V1")

        self.assertEqual([row["activity_nr"] for row in filtered], ["1", "2"])
        self.assertEqual(stats["matched_by_office"], 1)
        self.assertEqual(stats["matched_by_fallback"], 1)
        self.assertEqual(stats["excluded_state"], 1)
        self.assertEqual(stats["excluded_territory"], 1)

    def test_dedupe_by_activity_nr_keeps_best_score(self):
        leads = [
            {"activity_nr": "100", "lead_score": 6, "first_seen_at": "2026-02-01T08:00:00"},
            {"activity_nr": "100", "lead_score": 9, "first_seen_at": "2026-02-01T09:00:00"},
            {"activity_nr": "101", "lead_score": 7, "first_seen_at": "2026-02-01T07:00:00"},
        ]
        deduped, removed = dedupe_by_activity_nr(leads)
        self.assertEqual(removed, 1)
        by_id = {row["activity_nr"]: row for row in deduped}
        self.assertEqual(by_id["100"]["lead_score"], 9)
        self.assertEqual(by_id["101"]["lead_score"], 7)


if __name__ == "__main__":
    unittest.main()
