import unittest

from scoring import ai_triage


class TestAiTriageProfiles(unittest.TestCase):
    def test_unknown_profile_falls_back_to_default(self):
        self.assertEqual(ai_triage.normalize_profile_key("unknown_profile"), "default")

    def test_profile_changes_prompt_hash(self):
        default_hash = ai_triage.prompt_hash("default")
        facs_hash = ai_triage.prompt_hash("facs_trial")
        self.assertNotEqual(default_hash, facs_hash)


if __name__ == "__main__":
    unittest.main()
