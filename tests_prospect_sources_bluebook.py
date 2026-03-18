import unittest

from outreach import prospect_sources_bluebook as bluebook


class TestProspectSourcesBluebook(unittest.TestCase):
    def test_parse_bluebook_search_page_extracts_company_cards(self):
        html = """
        <div class="single_result_wrapper" data-proviewid="12345">
          <h3 class="cname">Acme Safety Consulting</h3>
          <a href="/iProView/12345/acme-safety-consulting.html">Profile</a>
          <a href="/iProView/12345/locations-contacts.html">Locations & Contacts</a>
        </div>
        """
        rows, parse_mode = bluebook.parse_bluebook_search_page(
            html,
            page_url="https://www.thebluebook.com/search.html?page=1",
        )

        self.assertEqual(parse_mode, "BLUEBOOK_SEARCH_RESULTS")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["firm"], "Acme Safety Consulting")
        self.assertEqual(rows[0]["profile_id"], "12345")
        self.assertEqual(rows[0]["contact_url"], "https://www.thebluebook.com/iProView/12345/locations-contacts.html")

    def test_parse_bluebook_contact_page_accepts_website_backed_consultancy_row(self):
        html = """
        <div class="card">
          <div class="col-12 mb-2">Houston, TX 77002</div>
          <div class="mt-3"><b>Jane Safety</b> Principal Consultant</div>
          <div>OSHA compliance consulting and safety training</div>
          <a class="pvLoc-website" href="https://acmesafety.example.com">Website</a>
          <a class="pvLoc-phone">(713) 555-0100</a>
        </div>
        """
        rows, parse_mode = bluebook.parse_bluebook_contact_page(
            html,
            state="TX",
            contact_url="https://www.thebluebook.com/iProView/12345/locations-contacts.html",
            profile_url="https://www.thebluebook.com/iProView/12345/acme-safety-consulting.html",
            profile_id="12345",
            firm="Acme Safety Consulting",
        )

        self.assertEqual(parse_mode, "BLUEBOOK_CONTACT_PAGE")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["firm"], "Acme Safety Consulting")
        self.assertEqual(rows[0]["website"], "https://acmesafety.example.com")
        self.assertEqual(rows[0]["contact_name"], "Jane Safety")
        self.assertEqual(rows[0]["title"], "Principal Consultant")
        self.assertEqual(rows[0]["state"], "TX")
        self.assertEqual(rows[0]["source"], "bluebook:12345")

    def test_parse_bluebook_contact_page_accepts_nonfree_source_email(self):
        html = """
        <div class="card">
          <div class="col-12 mb-2">Dallas, TX 75201</div>
          <div>Industrial hygiene consulting and OSHA training</div>
          <div>Contact us at info@acmesafety.example.com</div>
        </div>
        """
        rows, _parse_mode = bluebook.parse_bluebook_contact_page(
            html,
            state="TX",
            contact_url="https://www.thebluebook.com/iProView/12345/locations-contacts.html",
            profile_url="https://www.thebluebook.com/iProView/12345/acme-safety-consulting.html",
            profile_id="12345",
            firm="Acme Safety Consulting",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["email"], "info@acmesafety.example.com")

    def test_parse_bluebook_contact_page_rejects_free_email_without_website_and_state_mismatch(self):
        free_email_html = """
        <div class="card">
          <div class="col-12 mb-2">Dallas, TX 75201</div>
          <div>OSHA consulting and safety compliance</div>
          <div>Contact owner@gmail.com</div>
        </div>
        """
        rows, parse_mode = bluebook.parse_bluebook_contact_page(
            free_email_html,
            state="TX",
            contact_url="https://www.thebluebook.com/iProView/12345/locations-contacts.html",
            profile_url="https://www.thebluebook.com/iProView/12345/acme-safety-consulting.html",
            profile_id="12345",
            firm="Acme Safety Consulting",
        )
        self.assertEqual(parse_mode, "FAILED")
        self.assertEqual(rows, [])

        mismatch_html = """
        <div class="card">
          <div class="col-12 mb-2">Los Angeles, CA 90001</div>
          <div>Safety consulting and industrial hygiene support</div>
          <a class="pvLoc-website" href="https://acmesafety.example.com">Website</a>
        </div>
        """
        rows, parse_mode = bluebook.parse_bluebook_contact_page(
            mismatch_html,
            state="TX",
            contact_url="https://www.thebluebook.com/iProView/12345/locations-contacts.html",
            profile_url="https://www.thebluebook.com/iProView/12345/acme-safety-consulting.html",
            profile_id="12345",
            firm="Acme Safety Consulting",
        )
        self.assertEqual(parse_mode, "FAILED")
        self.assertEqual(rows, [])

    def test_doctor_probe_bluebook_uses_search_parser(self):
        html = """
        <div class="single_result_wrapper" data-proviewid="12345">
          <h3 class="cname">Acme Safety Consulting</h3>
          <a href="/iProView/12345/locations-contacts.html">Locations & Contacts</a>
        </div>
        """

        def _fetcher(_url: str):  # type: ignore[no-untyped-def]
            return 200, html, "https://www.thebluebook.com/search.html?page=1"

        result = bluebook.doctor_probe_bluebook(fetcher=_fetcher)
        self.assertTrue(result["ok"])
        self.assertEqual(result["rows_found"], 1)
        self.assertEqual(result["parse_mode"], "BLUEBOOK_SEARCH_RESULTS")


if __name__ == "__main__":
    unittest.main()
