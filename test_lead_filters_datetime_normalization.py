import unittest

from lead_filters import dedupe_by_activity_nr


class TestLeadFiltersDatetimeNormalization(unittest.TestCase):
    def test_dedupe_prefers_lead_key_when_present(self) -> None:
        leads = [
            {
                "lead_key": "osha:activity:9001",
                "activity_nr": "A-500",
                "lead_id": "older",
                "lead_score": 6,
                "first_seen_at": "2026-02-10T09:00:00+00:00",
                "last_seen_at": "2026-02-10T10:00:00+00:00",
                "date_opened": "2026-02-10",
            },
            {
                "lead_key": "osha:activity:9001",
                "activity_nr": "A-501",
                "lead_id": "newer",
                "lead_score": 9,
                "first_seen_at": "2026-02-11T09:00:00+00:00",
                "last_seen_at": "2026-02-11T10:00:00+00:00",
                "date_opened": "2026-02-11",
            },
        ]

        deduped, removed = dedupe_by_activity_nr(leads)
        self.assertEqual(1, len(deduped))
        self.assertEqual(1, removed)
        self.assertEqual("newer", deduped[0]["lead_id"])

    def test_mixed_naive_and_aware_do_not_raise(self) -> None:
        leads = [
            {
                "activity_nr": "A-100",
                "lead_id": "lead-naive",
                "lead_score": 7,
                "first_seen_at": "2026-02-11 09:00:00",
                "last_seen_at": "2026-02-11 09:30:00",
                "date_opened": "2026-02-11",
            },
            {
                "activity_nr": "A-100",
                "lead_id": "lead-aware",
                "lead_score": 7,
                "first_seen_at": "2026-02-11T08:00:00+00:00",
                "last_seen_at": "2026-02-11T10:00:00+00:00",
                "date_opened": "2026-02-11T00:00:00+00:00",
            },
        ]

        deduped, removed = dedupe_by_activity_nr(leads)
        self.assertEqual(1, len(deduped))
        self.assertEqual(1, removed)
        self.assertEqual("A-100", deduped[0]["activity_nr"])

    def test_deterministic_winner_with_normalized_datetimes(self) -> None:
        leads = [
            {
                "activity_nr": "A-200",
                "lead_id": "older",
                "lead_score": 9,
                "first_seen_at": "2026-02-10 09:00:00",
                "last_seen_at": "2026-02-10 10:00:00",
                "date_opened": "2026-02-10",
            },
            {
                "activity_nr": "A-200",
                "lead_id": "newer",
                "lead_score": 9,
                "first_seen_at": "2026-02-11T09:00:00+00:00",
                "last_seen_at": "2026-02-11T10:00:00+00:00",
                "date_opened": "2026-02-11T00:00:00+00:00",
            },
        ]

        deduped, _removed = dedupe_by_activity_nr(leads)
        self.assertEqual("newer", deduped[0]["lead_id"])

    def test_deterministic_sorted_output_across_activity_keys(self) -> None:
        leads = [
            {
                "activity_nr": "A-301",
                "lead_id": "lead-high-naive",
                "lead_score": 10,
                "first_seen_at": "2026-02-10 09:00:00",
                "last_seen_at": "2026-02-10 10:00:00",
                "date_opened": "2026-02-10",
            },
            {
                "activity_nr": "A-302",
                "lead_id": "lead-high-aware",
                "lead_score": 10,
                "first_seen_at": "2026-02-11T09:00:00+00:00",
                "last_seen_at": "2026-02-11T10:00:00+00:00",
                "date_opened": "2026-02-11T00:00:00+00:00",
            },
            {
                "activity_nr": "A-303",
                "lead_id": "lead-med-aware",
                "lead_score": 8,
                "first_seen_at": "2026-02-12T09:00:00+00:00",
                "last_seen_at": "2026-02-12T10:00:00+00:00",
                "date_opened": "2026-02-12T00:00:00+00:00",
            },
        ]

        deduped, removed = dedupe_by_activity_nr(leads)
        self.assertEqual(0, removed)
        self.assertEqual(
            ["lead-high-aware", "lead-high-naive", "lead-med-aware"],
            [row["lead_id"] for row in deduped],
        )

    def test_invalid_or_missing_datetimes_fallback_safely(self) -> None:
        leads = [
            {
                "activity_nr": "A-400",
                "lead_id": "invalid-dates",
                "lead_score": 6,
                "first_seen_at": "not-a-date",
                "last_seen_at": "",
                "date_opened": None,
            },
            {
                "activity_nr": "A-400",
                "lead_id": "valid-aware",
                "lead_score": 6,
                "first_seen_at": "2026-02-11T11:00:00+00:00",
                "last_seen_at": "2026-02-11T12:00:00+00:00",
                "date_opened": "2026-02-11",
            },
        ]

        deduped, removed = dedupe_by_activity_nr(leads)
        self.assertEqual(1, len(deduped))
        self.assertEqual(1, removed)
        self.assertEqual("valid-aware", deduped[0]["lead_id"])


if __name__ == "__main__":
    unittest.main()
