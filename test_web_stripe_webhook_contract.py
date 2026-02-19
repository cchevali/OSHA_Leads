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

    def test_webhook_health_route_contract(self) -> None:
        route_path = Path("web/app/api/stripe/webhook/health/route.ts")
        text = route_path.read_text(encoding="utf-8")

        self.assertIn("export async function GET()", text)
        self.assertIn("ok: true", text)
        self.assertIn("stripe_price_id_core_present", text)
        self.assertIn("stripe_price_id_multi_present", text)
        self.assertIn("web_stripe_webhook_secret_present", text)
        self.assertIn("stripe_mode_hint", text)
        self.assertIn("live-config-present", text)
        self.assertIn("missing-config", text)
        self.assertNotIn("runSubscriptionRegistryCommand", text)
        self.assertNotIn("subscription_registry_ops.py", text)
        self.assertNotIn("spawnSync", text)
        self.assertNotIn("WEB_STRIPE_WEBHOOK_SECRET=", text)
        self.assertNotIn("STRIPE_PRICE_ID_CORE=", text)
        self.assertNotIn("STRIPE_PRICE_ID_MULTI=", text)


if __name__ == "__main__":
    unittest.main()
