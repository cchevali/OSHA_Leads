import email
import unittest

from inbound_inbox_triage import decode_header_value, extract_plain_body, extract_original_sender


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


if __name__ == "__main__":
    unittest.main()
