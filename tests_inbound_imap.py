import email
import email.policy
import unittest
from unittest import mock

from inbound_inbox_triage import (
    decode_header_value,
    extract_plain_body,
    extract_original_sender,
    resolve_imap_settings,
    resolve_inbound_backend,
)


class TestInboundImapParsing(unittest.TestCase):
    def test_rfc822_parse_subject_and_body(self):
        msg = email.message.EmailMessage()
        msg["From"] = "Jane Doe <jane@example.com>"
        msg["Subject"] = "Re: Unsubscribe"
        msg.set_content("Please unsubscribe me.")

        raw = msg.as_bytes(policy=email.policy.default)
        parsed = email.message_from_bytes(raw, policy=email.policy.default)

        subject = decode_header_value(parsed.get("Subject", ""))
        body = extract_plain_body(parsed)

        self.assertEqual(subject, "Re: Unsubscribe")
        self.assertIn("unsubscribe", body.lower())

    def test_forwarded_sender_extraction(self):
        from_email = "support@microflowops.com"
        reply_to = ""
        body = """Forwarded message\nFrom: Original Person <orig@example.com>\nSubject: Please unsubscribe\n"""

        extracted = extract_original_sender(from_email, reply_to, body)
        self.assertEqual(extracted, "orig@example.com")

    def test_reply_to_takes_precedence(self):
        from_email = "support@microflowops.com"
        reply_to = "reply@example.com"
        body = "From: someone@example.com"

        extracted = extract_original_sender(from_email, reply_to, body)
        self.assertEqual(extracted, "reply@example.com")

    def test_resolve_inbound_backend_infers_imap_from_saved_bounce_settings(self):
        with mock.patch.dict(
            "os.environ",
            {"BOUNCE_IMAP_USER": "ops@example.com", "BOUNCE_IMAP_PASS": "secret"},
            clear=True,
        ):
            self.assertEqual(resolve_inbound_backend(), "imap")

    def test_resolve_imap_settings_falls_back_to_bounce_mailbox_values(self):
        with mock.patch.dict(
            "os.environ",
            {
                "BOUNCE_IMAP_HOST": "imappro.zoho.com",
                "BOUNCE_IMAP_PORT": "993",
                "BOUNCE_IMAP_USER": "ops@example.com",
                "BOUNCE_IMAP_PASS": "secret",
                "BOUNCE_IMAP_FOLDER": "INBOX",
            },
            clear=True,
        ):
            settings = resolve_imap_settings()
        self.assertEqual(settings["host"], "imappro.zoho.com")
        self.assertEqual(settings["port"], "993")
        self.assertEqual(settings["user"], "ops@example.com")
        self.assertEqual(settings["password"], "secret")
        self.assertEqual(settings["folder"], "INBOX")


if __name__ == "__main__":
    unittest.main()
