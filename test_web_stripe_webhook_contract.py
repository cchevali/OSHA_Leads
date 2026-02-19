import unittest
from pathlib import Path


class TestWebStripeWebhookContract(unittest.TestCase):
    def test_webhook_route_enforces_secret_and_err_tokens(self) -> None:
        route_path = Path("web/app/api/stripe/webhook/route.ts")
        text = route_path.read_text(encoding="utf-8")
        self.assertIn("ERR_STRIPE_WEBHOOK_SECRET_MISSING", text)
        self.assertIn("ERR_STRIPE_WEBHOOK_SIGNATURE_INVALID", text)
        self.assertIn("ERR_STRIPE_WEBHOOK_INVALID_JSON", text)
        self.assertIn("status: 500", text)
        self.assertIn("status: 400", text)
        self.assertIn("status: 422", text)

    def test_registry_op_emits_deterministic_err_tokens(self) -> None:
        ops_path = Path("scripts/subscription_registry_ops.py")
        text = ops_path.read_text(encoding="utf-8")
        self.assertIn("ERR_SUBSCRIPTION_REGISTRY_OP", text)
        self.assertIn("ERR_PAYLOAD_REQUIRED", text)
        self.assertIn("ERR_PAYLOAD_OBJECT_REQUIRED", text)


if __name__ == "__main__":
    unittest.main()
