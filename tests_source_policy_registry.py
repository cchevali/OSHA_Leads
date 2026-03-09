import unittest

from outreach import source_policy


class TestSourcePolicyRegistry(unittest.TestCase):
    def test_registry_exposes_supported_and_implemented_sources(self):
        supported = source_policy.supported_autogrow_sources(include_unimplemented=True)
        implemented = source_policy.implemented_autogrow_sources()
        self.assertIn("AIHA", supported)
        self.assertIn("BBB", supported)
        self.assertIn("AIHA", implemented)
        self.assertNotIn("BBB", implemented)

    def test_validate_autogrow_sources_distinguishes_invalid_and_unimplemented(self):
        invalid, unimplemented = source_policy.validate_autogrow_source_tokens(["AIHA", "BBB", "NOPE"])
        self.assertEqual(invalid, ["NOPE"])
        self.assertEqual(unimplemented, ["BBB"])

    def test_autogrow_source_order_uses_registry_priority(self):
        ordered = source_policy.autogrow_source_order(["STATE_LIC", "AIHA", "APOLLO"])
        self.assertEqual(ordered, ["AIHA", "APOLLO", "STATE_LIC"])


if __name__ == "__main__":
    unittest.main()
