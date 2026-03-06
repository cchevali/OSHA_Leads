import unittest

from outreach import run_prospect_generation as generator


class TestOutreachGenerationMetadata(unittest.TestCase):
    def test_to_discovery_rows_normalizes_metadata_defaults(self):
        rows = [
            {
                "email": "owner@contractor.com",
                "company_name": "Contractor Co",
                "contact_role": "Owner",
                "city": "100 Main St, Austin, TX 78701",
                "state": "Texas",
                "source": "STATE_LIC",
            },
            {
                "email": "legacy@consultingco.com",
                "company_name": "Consulting Co",
                "contact_role": "Consultant",
                "city": "Suite 400, San Diego",
                "state": "CALIFORNIA",
                "source": "legacy_directory_export",
            },
        ]
        out = generator._to_discovery_rows(rows)
        self.assertEqual(len(out), 2)
        by_email = {str(r.get("email") or ""): r for r in out}

        state_lic = by_email["owner@contractor.com"]
        self.assertEqual(state_lic["state"], "TX")
        self.assertEqual(state_lic["city"], "Austin")
        self.assertEqual(state_lic["source_fit_tier"], "adjacent_contractor")
        self.assertEqual(state_lic["default_send_eligible"], "0")

        legacy = by_email["legacy@consultingco.com"]
        self.assertEqual(legacy["state"], "CA")
        self.assertEqual(legacy["city"], "San Diego")
        self.assertEqual(legacy["source_fit_tier"], "recoverable_consultant")
        self.assertEqual(legacy["default_send_eligible"], "1")

    def test_generator_row_observability_counts_source_tier_sendable(self):
        rows = [
            {"email": "a@seed.com", "source": "seed_recipients_pools", "source_fit_tier": "", "default_send_eligible": ""},
            {"email": "b@aiha.com", "source": "aiha_consultants_listing:12-13", "source_fit_tier": "", "default_send_eligible": "1"},
            {"email": "c@state.com", "source": "STATE_LIC", "source_fit_tier": "", "default_send_eligible": ""},
            {"email": "d@unknown.com", "source": "legacy_export", "source_fit_tier": "", "default_send_eligible": ""},
            {"email": "e@apollo.com", "source": "apollo_export_csv", "source_fit_tier": "", "default_send_eligible": "true"},
        ]
        obs = generator._generator_row_observability(rows)
        source_counts = dict(obs.get("source_counts") or {})
        tier_counts = dict(obs.get("tier_counts") or {})

        self.assertEqual(int(source_counts.get("SEED", 0)), 1)
        self.assertEqual(int(source_counts.get("AIHA", 0)), 1)
        self.assertEqual(int(source_counts.get("STATE_LIC", 0)), 1)
        self.assertEqual(int(source_counts.get("UNKNOWN", 0)), 1)
        self.assertEqual(int(source_counts.get("APOLLO", 0)), 1)

        self.assertEqual(int(tier_counts.get("core_consultant", 0)), 1)
        self.assertEqual(int(tier_counts.get("recoverable_consultant", 0)), 3)
        self.assertEqual(int(tier_counts.get("adjacent_contractor", 0)), 1)
        self.assertEqual(int(obs.get("default_send_eligible_total") or 0), 4)


if __name__ == "__main__":
    unittest.main()
