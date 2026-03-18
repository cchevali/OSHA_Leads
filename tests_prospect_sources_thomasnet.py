import unittest

from outreach import prospect_sources_thomasnet as thomasnet


class TestProspectSourcesThomasnet(unittest.TestCase):
    def test_parse_thomasnet_result_page_extracts_profile_and_website(self):
        html = """
        <div class="search-result">
          <h3>Acme Safety Consulting</h3>
          <a href="/company/acme-safety-consulting-20000123/profile">Thomasnet profile</a>
          <a href="https://acmesafety.example.com">Company website</a>
          <div>Safety consulting services and OSHA compliance support</div>
        </div>
        """
        rows, parse_mode = thomasnet.parse_thomasnet_result_page(
            html,
            page_url="https://www.thomasnet.com/products/safety-consulting-services.html",
        )

        self.assertEqual(parse_mode, "THOMASNET_RESULTS")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["firm"], "Acme Safety Consulting")
        self.assertEqual(rows[0]["website"], "https://acmesafety.example.com")
        self.assertIn("thomasnet.com/company", rows[0]["profile_url"])

    def test_parse_thomasnet_profile_page_extracts_nonfree_email_and_site(self):
        html = """
        <div>
          <a href="mailto:info@acmesafety.example.com">info@acmesafety.example.com</a>
          <a href="https://acmesafety.example.com/contact">Visit Site</a>
          <div>Occupational health and safety consulting specialists</div>
        </div>
        """
        row, parse_mode = thomasnet.parse_thomasnet_profile_page(
            html,
            profile_url="https://www.thomasnet.com/company/acme-safety-consulting-20000123/profile",
        )

        self.assertEqual(parse_mode, "THOMASNET_PROFILE")
        self.assertEqual(row["email"], "info@acmesafety.example.com")
        self.assertEqual(row["website"], "https://acmesafety.example.com/contact")

    def test_evaluate_thomasnet_qualification_passes_with_multistate_contactable_volume(self):
        rows = []
        for state, count in (("TX", 6), ("CA", 5), ("FL", 4)):
            for idx in range(count):
                rows.append(
                    {
                        "firm": f"{state} Safety Consulting {idx}",
                        "state": state,
                        "website": f"https://{state.lower()}-{idx}.example.com",
                        "email": f"info@{state.lower()}-{idx}.example.com",
                        "blob": "Safety consulting and OSHA compliance services",
                    }
                )

        result = thomasnet.evaluate_thomasnet_qualification(
            rows=rows,
            crm_domains={"tx-0.example.com", "ca-0.example.com"},
            crm_firm_keys={"txsafetyconsulting0"},
            suppressed_emails={"info@fl-3.example.com"},
        )

        self.assertTrue(result["qualified"])
        self.assertGreaterEqual(result["unique_consultancy_relevant_firms"], 15)
        self.assertGreaterEqual(result["website_link_rate"], 0.8)
        self.assertGreaterEqual(result["public_site_email_yield"], 0.25)
        self.assertGreaterEqual(result["states_meeting_target"], 2)
        self.assertLess(result["crm_overlap_rate"], 0.5)
        self.assertGreaterEqual(result["state_contactable_rows"]["TX"], 2)
        self.assertGreaterEqual(result["state_contactable_rows"]["CA"], 2)

    def test_evaluate_thomasnet_qualification_fails_when_thresholds_are_missed(self):
        rows = [
            {
                "firm": "Sparse Industrial Listing",
                "state": "TX",
                "website": "",
                "email": "",
                "blob": "Industrial services",
            },
            {
                "firm": "Another Sparse Listing",
                "state": "TX",
                "website": "https://sparse.example.com",
                "email": "",
                "blob": "General supplier",
            },
        ]

        result = thomasnet.evaluate_thomasnet_qualification(rows=rows)

        self.assertFalse(result["qualified"])
        self.assertLess(result["unique_consultancy_relevant_firms"], 15)
        self.assertEqual(result["states_meeting_target"], 0)


if __name__ == "__main__":
    unittest.main()
