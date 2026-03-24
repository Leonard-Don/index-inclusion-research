from __future__ import annotations

import numpy as np
import pandas as pd

from index_inclusion_research.analysis import fit_local_linear_rdd, run_rdd_suite


def test_fit_local_linear_rdd_detects_positive_jump() -> None:
    distance = np.array([-1.0, -0.5, -0.2, 0.1, 0.4, 0.9])
    inclusion = (distance >= 0).astype(int)
    outcome = 0.02 + 0.03 * inclusion + 0.01 * distance
    frame = pd.DataFrame(
        {
            "distance_to_cutoff": distance,
            "inclusion": inclusion,
            "car_m1_p1": outcome,
        }
    )
    result = fit_local_linear_rdd(frame, "car_m1_p1", bandwidth=1.0)
    assert result["n_obs"] == 6
    assert result["tau"] > 0.02


def test_run_rdd_suite_returns_one_row_per_outcome() -> None:
    frame = pd.DataFrame(
        {
            "distance_to_cutoff": [-0.9, -0.6, -0.3, -0.1, 0.1, 0.3, 0.6, 0.9],
            "inclusion": [0, 0, 0, 0, 1, 1, 1, 1],
            "car_m1_p1": [0.00, 0.01, 0.01, 0.02, 0.03, 0.04, 0.04, 0.05],
            "volume_change": [0.00, 0.01, 0.02, 0.02, 0.08, 0.09, 0.10, 0.11],
        }
    )
    summary = run_rdd_suite(frame, outcome_cols=["car_m1_p1", "volume_change"], bandwidth=1.0)
    assert list(summary["outcome"]) == ["car_m1_p1", "volume_change"]
