import email
import email.policy
import unittest
from unittest import mock

from inbound_inbox_triage import (
    classify_email,
    decode_header_value,
    extract_plain_body,
    extract_original_sender,
    handle_bounce_category,
    looks_like_moderation_bounce,
    parse_bounce_details,
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

    def test_zoho_moderation_notice_is_treated_as_bounce_not_hot_interest(self):
        subject = "Email held for Moderation - alerts@microflowops.com"
        body = (
            "This message was created automatically by mail delivery software.\n\n"
            "A message that you sent could not be delivered to one or more of its recipients. "
            "This is a permanent error.\n\n"
            "jeff@g8safety.com, ERROR CODE :421 - Host not reachable.\n"
        )
        self.assertTrue(looks_like_moderation_bounce(subject, body))
        self.assertEqual(classify_email(subject, body, "noreply@zoho.com"), "bounce")

    def test_parse_bounce_details_recognizes_soft_moderation_bounce(self):
        subject = "Email held for Moderation - alerts@microflowops.com"
        body = (
            "This message was created automatically by mail delivery software.\n"
            "A message that you sent could not be delivered to one or more of its recipients. "
            "This is a permanent error.\n"
            "jeff@g8safety.com, ERROR CODE :421 - Host not reachable.\n"
        )
        parsed = parse_bounce_details(subject, "noreply@zoho.com", {}, body, "<m-soft>")
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.recipient_email, "jeff@g8safety.com")
        self.assertEqual(parsed.bounce_class, "soft")

    def test_handle_bounce_category_skips_suppression_for_soft_bounce(self):
        subject = "Email held for Moderation - alerts@microflowops.com"
        body = (
            "This message was created automatically by mail delivery software.\n"
            "jeff@g8safety.com, ERROR CODE :421 - Host not reachable.\n"
        )
        with mock.patch("inbound_inbox_triage.add_to_suppression") as mocked_add:
            result = handle_bounce_category(
                subject=subject,
                from_email="noreply@zoho.com",
                headers={},
                body=body,
                message_id="<m-soft>",
                dry_run=False,
            )
        mocked_add.assert_not_called()
        self.assertEqual(result["action"], "soft_bounce_no_suppression")
        self.assertFalse(result["suppression_changed"])
        self.assertEqual(result["bounce_class"], "soft")

    def test_handle_bounce_category_suppresses_hard_bounce(self):
        subject = "Email held for Moderation - alerts@microflowops.com"
        body = (
            "This message was created automatically by mail delivery software.\n"
            "rivera@precisionair.com, ERROR CODE :557 - You are not allowed to send mail to rivera@precisionair.com\n"
        )
        with mock.patch("inbound_inbox_triage.add_to_suppression") as mocked_add:
            result = handle_bounce_category(
                subject=subject,
                from_email="noreply@zoho.com",
                headers={},
                body=body,
                message_id="<m-hard>",
                dry_run=False,
            )
        mocked_add.assert_called_once()
        self.assertEqual(result["action"], "suppressed_recipient_hard_bounce")
        self.assertTrue(result["suppression_changed"])
        self.assertEqual(result["bounce_class"], "hard")


if __name__ == "__main__":
    unittest.main()
