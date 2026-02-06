import unittest

from email_footer import build_footer_html, build_footer_text


class TestEmailFooterWebsite(unittest.TestCase):
    def test_footer_standardizes_microflowops_brand_text(self):
        text = build_footer_text(
            brand_name="Micro Flow Ops",
            mailing_address="11539 Links Dr, Reston, VA 20190",
            disclaimer="Informational only. Not legal advice.",
            reply_to="support@microflowops.com",
            unsub_url=None,
            include_separator=True,
        )

        self.assertIn("\nMicroFlowOps\n", "\n" + text + "\n")
        self.assertNotIn("\nMicro Flow Ops\n", "\n" + text + "\n")

    def test_footer_standardizes_microflowops_brand_html(self):
        html = build_footer_html(
            brand_name="Micro Flow Ops",
            mailing_address="11539 Links Dr, Reston, VA 20190",
            disclaimer="Informational only. Not legal advice.",
            reply_to="support@microflowops.com",
            unsub_url=None,
        )

        self.assertIn(">MicroFlowOps<", html)
        self.assertNotIn(">Micro Flow Ops<", html)

    def test_footer_text_includes_website_without_unsub_url(self):
        text = build_footer_text(
            brand_name="Acme Safety",
            mailing_address="123 Main St, Example City, ST 00000",
            disclaimer="Informational only. Not legal advice.",
            reply_to="support@acme.example",
            unsub_url=None,
            include_separator=True,
        )

        self.assertIn("More: https://microflowops.com", text)
        self.assertIn('Opt out: reply with "unsubscribe" or email support@acme.example (subject: unsubscribe)', text)

        # Basic ordering guard: website reference should sit above opt-out instructions.
        self.assertLess(text.index("More: https://microflowops.com"), text.index("Opt out:"))

    def test_footer_text_includes_website_with_unsub_url(self):
        unsub_url = "https://example.com/unsub?token=abc"
        text = build_footer_text(
            brand_name="Acme Safety",
            mailing_address="123 Main St, Example City, ST 00000",
            disclaimer="Informational only. Not legal advice.",
            reply_to="support@acme.example",
            unsub_url=unsub_url,
            include_separator=True,
        )

        self.assertIn("More: https://microflowops.com", text)
        self.assertIn('Opt out: reply with "unsubscribe" or click here to unsubscribe.', text)
        self.assertIn(unsub_url, text)
        self.assertLess(text.index("More: https://microflowops.com"), text.index("Opt out:"))

    def test_footer_html_includes_website_link(self):
        html = build_footer_html(
            brand_name="Acme Safety",
            mailing_address="123 Main St, Example City, ST 00000",
            disclaimer="Informational only. Not legal advice.",
            reply_to="support@acme.example",
            unsub_url=None,
        )

        self.assertIn('href="https://microflowops.com"', html)
        self.assertIn(">microflowops.com</a>", html)
        self.assertIn('mailto:support@acme.example?subject=unsubscribe', html)


if __name__ == "__main__":
    unittest.main()

