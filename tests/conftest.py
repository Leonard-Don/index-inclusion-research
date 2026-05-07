from __future__ import annotations

import re
import sys
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

README_REPO_CARD_BADGE_LINE_RE = re.compile(
    r"(?:\[(?:!\[[^\]]+\]\([^)]+\))\]\([^)]+\)|!\[[^\]]+\]\([^)]+\))"
)


def _readme_repo_card_badge_lines(readme: str) -> list[str]:
    """Return only the leading Markdown badge block rendered on the GitHub repo card."""
    badge_lines: list[str] = []
    for line in readme.splitlines():
        if not badge_lines and (not line.strip() or line.startswith("# ")):
            continue
        if README_REPO_CARD_BADGE_LINE_RE.fullmatch(line):
            badge_lines.append(line)
            continue
        if not badge_lines:
            return []
        break
    return badge_lines


@pytest.fixture
def readme_repo_card_badge_lines() -> Callable[[str], list[str]]:
    return _readme_repo_card_badge_lines


@pytest.fixture
def synthetic_real_tables_dir(tmp_path: Path) -> Path:
    """Return a tmp_path with a minimal `results/real_tables/` skeleton.

    Use this in dashboard tests that don't need the committed CSVs in
    `results/real_tables/`. Pass the parent (i.e. tmp_path itself) as the
    `root` arg to dashboard helpers so they read from the synthetic tree.

    Extend this fixture by writing additional CSVs into the returned dir
    inside the test, or copy the pattern in
    `tests/test_dashboard_metrics.py::test_build_demand_curve_cards_*`.
    """
    tables_dir = tmp_path / "results" / "real_tables"
    tables_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {"market": "US", "n_events": 10},
            {"market": "CN", "n_events": 6},
        ]
    ).to_csv(tables_dir / "event_counts.csv", index=False)
    return tables_dir
