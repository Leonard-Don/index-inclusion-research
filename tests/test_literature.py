from __future__ import annotations

import numpy as np
import pandas as pd

from index_inclusion_research.literature import compute_did_summary, compute_retention_summary


def test_compute_retention_summary_reports_reversal_and_ratio() -> None:
    event_level = pd.DataFrame(
        [
            {"event_id": "a", "market": "US", "event_phase": "announce", "inclusion": 1, "car_p0_p20": 0.10, "car_p0_p120": 0.04},
            {"event_id": "b", "market": "US", "event_phase": "announce", "inclusion": 1, "car_p0_p20": 0.06, "car_p0_p120": 0.03},
        ]
    )
    summary = compute_retention_summary(event_level)
    row = summary.iloc[0]
    assert np.isclose(row["short_mean_car"], 0.08)
    assert np.isclose(row["long_mean_car"], 0.035)
    assert np.isclose(row["car_reversal"], 0.045)
    assert np.isclose(row["retention_ratio"], 0.4375)


def test_compute_did_summary_compares_treated_and_control_changes() -> None:
    panel = pd.DataFrame(
        [
            {"event_id": "t1", "matched_to_event_id": "g1", "market": "CN", "event_phase": "announce", "inclusion": 1, "relative_day": -1, "ar": 0.01, "turnover": 0.02, "volume": 100},
            {"event_id": "t1", "matched_to_event_id": "g1", "market": "CN", "event_phase": "announce", "inclusion": 1, "relative_day": 1, "ar": 0.05, "turnover": 0.03, "volume": 130},
            {"event_id": "c1", "matched_to_event_id": "g1", "market": "CN", "event_phase": "announce", "inclusion": 0, "relative_day": -1, "ar": 0.00, "turnover": 0.02, "volume": 100},
            {"event_id": "c1", "matched_to_event_id": "g1", "market": "CN", "event_phase": "announce", "inclusion": 0, "relative_day": 1, "ar": 0.01, "turnover": 0.02, "volume": 105},
        ]
    )
    summary = compute_did_summary(panel, pre_window=(-1, -1), post_window=(1, 1))
    ar_row = summary.loc[summary["metric"] == "abnormal_return"].iloc[0]
    assert np.isclose(ar_row["treated_post_minus_pre"], 0.04)
    assert np.isclose(ar_row["control_post_minus_pre"], 0.01)
    assert np.isclose(ar_row["did_estimate"], 0.03)
