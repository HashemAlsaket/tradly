from __future__ import annotations

import unittest
from unittest.mock import patch

from tradly.ops import refresh_registry_runtime


class RefreshRegistryRuntimeTests(unittest.TestCase):
    def test_steps_run_in_expected_order(self) -> None:
        seen: list[tuple[list[str], str, str]] = []

        def _fake_run(cmd, cwd=None, env=None):
            seen.append((cmd, cwd, env.get("PYTHONPATH", "")))

            class _Result:
                returncode = 0

            return _Result()

        with patch("tradly.ops.refresh_registry_runtime.subprocess.run", side_effect=_fake_run):
            rc = refresh_registry_runtime.main()

        self.assertEqual(rc, 0)
        self.assertEqual(
            [cmd for cmd, _, _ in seen],
            [cmd for _, cmd in refresh_registry_runtime.STEPS],
        )
        self.assertTrue(all(cwd for _, cwd, _ in seen))
        self.assertTrue(all("src" in pythonpath for _, _, pythonpath in seen))

    def test_failure_stops_later_steps(self) -> None:
        seen: list[list[str]] = []

        def _fake_run(cmd, cwd=None, env=None):
            seen.append(cmd)

            class _Result:
                returncode = 7 if len(seen) == 2 else 0

            return _Result()

        with patch("tradly.ops.refresh_registry_runtime.subprocess.run", side_effect=_fake_run):
            rc = refresh_registry_runtime.main()

        self.assertEqual(rc, 7)
        self.assertEqual(seen, [cmd for _, cmd in refresh_registry_runtime.STEPS[:2]])


if __name__ == "__main__":
    unittest.main()
