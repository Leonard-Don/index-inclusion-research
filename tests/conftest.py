from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


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
