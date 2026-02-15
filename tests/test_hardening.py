import importlib
import json
import os
import tempfile
import unittest


class HardeningTests(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self._prev_state_dir = os.environ.get("GENESYS_STATE_DIR")
        os.environ["GENESYS_STATE_DIR"] = self._tmpdir.name

        # Reload modules so they pick up isolated state directory for this test run.
        self.auth_manager_mod = importlib.reload(importlib.import_module("src.auth_manager"))
        self.auth_mod = importlib.reload(importlib.import_module("src.auth"))
        self.api_mod = importlib.reload(importlib.import_module("src.api"))

    def tearDown(self):
        if self._prev_state_dir is None:
            os.environ.pop("GENESYS_STATE_DIR", None)
        else:
            os.environ["GENESYS_STATE_DIR"] = self._prev_state_dir
        self._tmpdir.cleanup()

    def test_org_code_validation_rejects_traversal(self):
        manager = self.auth_manager_mod.AuthManager()
        ok, msg = manager.add_organization("../evil", "admin", "Password123!")
        self.assertFalse(ok)
        self.assertIn("Organization code", msg)

        with self.assertRaises(ValueError):
            self.auth_mod._safe_org_code("../../etc")

    def test_429_retry_budget_is_bounded(self):
        api = self.api_mod.GenesysAPI(
            {"access_token": "dummy", "api_host": "https://example.invalid"}
        )
        self.assertFalse(api._can_retry_429(api.HTTP_429_MAX_RETRIES + 1))

        total_wait = 0.0
        wait_s = None
        projected = 0.0
        # Force the total wait cap to be exceeded quickly.
        api.HTTP_429_MAX_TOTAL_WAIT_SECONDS = 1
        for attempt in range(1, 10):
            wait_s, projected = api._next_429_wait(None, attempt, total_wait)
            if wait_s is None:
                break
            total_wait = projected
        self.assertIsNone(wait_s)
        self.assertGreaterEqual(projected, 1.0)

    def test_auth_manager_skips_malformed_org_users_file(self):
        good_org = os.path.join(self._tmpdir.name, "goodorg")
        bad_org = os.path.join(self._tmpdir.name, "badorg")
        os.makedirs(good_org, exist_ok=True)
        os.makedirs(bad_org, exist_ok=True)

        with open(os.path.join(good_org, "users.json"), "w", encoding="utf-8") as f:
            json.dump(
                {
                    "alice": {
                        "password": "placeholder",
                        "role": "Admin",
                        "metrics": [],
                    }
                },
                f,
            )
        with open(os.path.join(bad_org, "users.json"), "w", encoding="utf-8") as f:
            f.write("{this is not valid json")

        manager = self.auth_manager_mod.AuthManager()
        self.assertIn("goodorg", manager.users)
        self.assertIn("alice", manager.users["goodorg"])
        self.assertNotIn("badorg", manager.users)


if __name__ == "__main__":
    unittest.main()
