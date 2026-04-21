from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(shutil.which("node") is None, reason="node is required for dashboard frontend module tests")
def test_dashboard_frontend_node_module_tests_pass() -> None:
    result = subprocess.run(
        [
            "node",
            "--test",
            "--experimental-default-type=module",
            "tests/js/dashboard_frontend.test.mjs",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        "dashboard frontend node tests failed\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
