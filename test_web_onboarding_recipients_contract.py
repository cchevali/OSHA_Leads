import unittest
from pathlib import Path


class TestWebOnboardingRecipientsContract(unittest.TestCase):
    def test_onboarding_and_trial_routes_accept_recipients(self) -> None:
        onboarding_api = Path("web/app/api/onboarding/route.ts").read_text(encoding="utf-8")
        trial_api = Path("web/app/api/trial-request/route.ts").read_text(encoding="utf-8")
        self.assertIn("recipients", onboarding_api)
        self.assertIn("ERR_ONBOARDING_RECIPIENT_REQUIRED", onboarding_api)
        self.assertIn("ERR_ONBOARDING_RECIPIENT_INVALID", onboarding_api)
        self.assertIn("ERR_MAX_RECIPIENTS_EXCEEDED", onboarding_api)
        self.assertIn("recipients", trial_api)

    def test_forms_submit_structured_recipients(self) -> None:
        trial_form = Path("web/components/TrialRequestForm.tsx").read_text(encoding="utf-8")
        onboarding_form = Path("web/components/OnboardingMetroForm.tsx").read_text(encoding="utf-8")
        self.assertIn("recipients:", trial_form)
        self.assertIn("Recipients (up to 6)", trial_form)
        self.assertIn("Company Email (billing/admin contact)", trial_form)
        self.assertIn("recipients:", onboarding_form)
        self.assertIn("Company Email (billing/admin contact)", onboarding_form)
        self.assertIn("max 4 metros, 6 recipients", onboarding_form)
        self.assertIn("15 recipients", onboarding_form)

    def test_onboarding_copy_harmonized_no_calls_required(self) -> None:
        faq = Path("web/app/faq/page.tsx").read_text(encoding="utf-8")
        onboarding_page = Path("web/app/onboarding/page.tsx").read_text(encoding="utf-8")
        pricing = Path("web/app/pricing/page.tsx").read_text(encoding="utf-8")
        phrase = "No calls required; onboarding is handled via a short form + email confirmation."
        self.assertIn("No calls are required; we can qualify sample requests, founding pilots, and territory fit over email.", faq)
        self.assertIn(phrase, onboarding_page)
        self.assertIn("Request a sample", pricing)
        self.assertIn("We qualify fit manually.", pricing)


if __name__ == "__main__":
    unittest.main()
