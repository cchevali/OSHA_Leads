import unittest

from outreach import contact_normalization


class TestContactNormalization(unittest.TestCase):
    def test_valid_email_accepts_plain_business_email(self):
        self.assertTrue(contact_normalization.valid_email("Owner@ExampleSafety.com"))
        self.assertEqual(contact_normalization.normalize_email("Owner@ExampleSafety.com"), "owner@examplesafety.com")

    def test_valid_email_rejects_markdown_and_mailto_wrappers(self):
        self.assertEqual(
            contact_normalization.normalize_email("[owner@example.com](mailto:owner@example.com)"),
            "owner@example.com",
        )
        self.assertFalse(contact_normalization.valid_email("[owner@example.com](mailto:owner@example.com)"))
        self.assertTrue(contact_normalization.valid_email(contact_normalization.normalize_email("[owner@example.com](mailto:owner@example.com)")))
        self.assertFalse(contact_normalization.valid_email("[owner@example.com](mailto:other@example.com)"))
        self.assertFalse(contact_normalization.valid_email("mailto:owner@example.com extra"))

    def test_extract_http_urls_normalizes_and_dedupes(self):
        raw = (
            "[https://example.com/about|https://example.com/contact]"
            "(https://example.com/about|https://example.com/contact)"
        )
        self.assertEqual(
            contact_normalization.normalize_source_urls(raw),
            "https://example.com/about|https://example.com/contact",
        )

    def test_normalize_contact_name_and_evidence_strip_markdown_noise(self):
        self.assertEqual(
            contact_normalization.normalize_contact_name("Russell](https://example.com/%22,%22Russell) Carr"),
            "Russell Carr",
        )
        self.assertEqual(
            contact_normalization.normalize_evidence_snippet(
                "Site](https://example.com/contact|https://example.com/about%22,95,%22Site) presents owner info"
            ),
            "Site presents owner info",
        )


if __name__ == "__main__":
    unittest.main()
