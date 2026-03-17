import unittest

from outreach import state_lic_precision


class TestStateLicPrecision(unittest.TestCase):
    def test_tx_environmental_air_conditioning_is_hard_negative_for_packet_mode(self):
        row = {
            "firm": "ATX Environmental Solutions",
            "state": "TX",
            "license_type": "A/C Contractor",
            "license_subtype": "Environmental Air Conditioning",
            "city": "Austin",
            "license_number": "AC-100",
            "source_record_id": "tdlr:AC-100",
        }
        result = state_lic_precision.classify_state_lic_row(row, mode="packet_eligible")
        self.assertFalse(result["state_lic_packet_eligible"])
        self.assertFalse(result["state_lic_consultant_fit"])
        self.assertEqual(result["state_lic_hard_negative_class"], "tx_environmental_air_conditioning")
        self.assertEqual(result["state_lic_packet_exclusion_reason"], "hard_negative_class")

    def test_plain_environmental_is_neutral(self):
        result = state_lic_precision.classify_state_lic_row(
            {
                "firm": "Environmental Matters LLC",
                "state": "TX",
                "license_type": "Electrical Contractor",
                "city": "Dallas",
                "license_number": "EC-200",
                "source_record_id": "tdlr:EC-200",
            },
            mode="consultant_fit",
        )
        self.assertEqual(result["state_lic_positive_families"], [])
        self.assertNotIn("environmental", result["state_lic_fit_reasons"])
        self.assertFalse(result["state_lic_consultant_fit"])

    def test_positive_credentials_allow_neutral_name_to_pass(self):
        result = state_lic_precision.classify_state_lic_row(
            {
                "firm": "Laszcz-Davis Group",
                "state": "CA",
                "contact_name": "Chris Laszcz-Davis, CIH, CSP",
                "city": "Oakland",
                "license_number": "CIH-300",
                "source_record_id": "lic:CIH-300",
            },
            mode="packet_eligible",
        )
        self.assertIn("credentials", result["state_lic_positive_families"])
        self.assertTrue(result["state_lic_consultant_fit"])
        self.assertTrue(result["state_lic_packet_eligible"])

    def test_send_eligible_requires_nonfree_work_email(self):
        base_row = {
            "firm": "Safety Compliance Group",
            "state": "TX",
            "contact_name": "Taylor Safe, CSP",
            "license_type": "Electrical Contractor",
            "city": "Houston",
        }
        without_email = state_lic_precision.classify_state_lic_row(base_row, mode="send_eligible")
        with_work_email = state_lic_precision.classify_state_lic_row(
            {**base_row, "email": "taylor@safetycompliance.example.com"},
            mode="send_eligible",
        )
        self.assertFalse(without_email["state_lic_send_eligible"])
        self.assertTrue(with_work_email["state_lic_send_eligible"])


if __name__ == "__main__":
    unittest.main()
