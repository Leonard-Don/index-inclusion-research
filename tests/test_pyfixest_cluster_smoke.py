from __future__ import annotations

import math

import pandas as pd
import pytest

from index_inclusion_research.analysis.pyfixest_cluster import (
    estimate_announcement_day_cluster_se,
)


def test_pyfixest_announcement_day_cluster_se_smoke() -> None:
    pytest.importorskip("pyfixest", reason="pyfixest is an optional methods dependency")
    rows: list[dict[str, float | int]] = []
    for event_id in range(1, 9):
        treatment_effect = 0.015 + 0.001 * event_id
        baseline = 0.002 * event_id
        for relative_day in (-2, -1, 0, 1, 2):
            rows.append(
                {
                    "event_id": event_id,
                    "relative_day": relative_day,
                    "abnormal_return": baseline
                    + 0.0003 * relative_day
                    + (treatment_effect if relative_day == 0 else 0.0),
                }
            )

    result = estimate_announcement_day_cluster_se(pd.DataFrame(rows))

    assert result.n_obs == 40
    assert result.cluster_count == 8
    assert result.coefficient == pytest.approx(0.0195)
    assert result.std_error > 0
    assert result.std_error < abs(result.coefficient)
    assert math.isfinite(result.p_value)
