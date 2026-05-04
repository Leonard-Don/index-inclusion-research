from __future__ import annotations

import numpy as np
import pandas as pd

from index_inclusion_research.analysis import (
    fit_donut_rdd,
    fit_local_linear_rdd,
    fit_placebo_rdd,
    fit_polynomial_rdd,
    run_rdd_robustness,
    run_rdd_suite,
)


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


def _toy_rdd_frame(jump: float = 0.03, n_per_side: int = 20) -> pd.DataFrame:
    distance = np.concatenate(
        [np.linspace(-0.10, -0.005, n_per_side), np.linspace(0.005, 0.10, n_per_side)]
    )
    inclusion = (distance >= 0).astype(int)
    outcome = 0.0 + jump * inclusion + 0.05 * distance
    return pd.DataFrame(
        {"distance_to_cutoff": distance, "inclusion": inclusion, "car_m1_p1": outcome}
    )


def test_fit_donut_rdd_drops_observations_within_radius() -> None:
    frame = _toy_rdd_frame()
    main = fit_local_linear_rdd(frame, "car_m1_p1", bandwidth=0.10)
    donut = fit_donut_rdd(frame, "car_m1_p1", bandwidth=0.10, donut_radius=0.02)
    assert donut["donut_radius"] == 0.02
    assert donut["n_obs"] < main["n_obs"]


def test_fit_placebo_rdd_at_shifted_cutoff_recovers_zero_when_jump_is_at_real_cutoff() -> None:
    # Jump only at distance 0; placebo cutoff at +0.05 should give τ ≈ 0.
    frame = _toy_rdd_frame(jump=0.03, n_per_side=30)
    placebo = fit_placebo_rdd(frame, "car_m1_p1", bandwidth=0.10, cutoff_shift=0.05)
    assert placebo["cutoff_shift"] == 0.05
    assert abs(placebo["tau"]) < 0.02  # placebo τ near 0


def test_fit_polynomial_rdd_records_polynomial_order() -> None:
    frame = _toy_rdd_frame(jump=0.03)
    poly = fit_polynomial_rdd(frame, "car_m1_p1", bandwidth=0.10, polynomial_order=2)
    assert poly["polynomial_order"] == 2
    assert poly["n_obs"] > 0


def test_fit_polynomial_rdd_rejects_invalid_order() -> None:
    frame = _toy_rdd_frame()
    try:
        fit_polynomial_rdd(frame, "car_m1_p1", bandwidth=0.10, polynomial_order=0)
    except ValueError as exc:
        assert "polynomial_order" in str(exc)
    else:
        raise AssertionError("expected ValueError for polynomial_order=0")


def test_run_rdd_robustness_locks_bandwidth_across_specs() -> None:
    frame = _toy_rdd_frame(jump=0.03, n_per_side=30)
    panel = run_rdd_robustness(frame, "car_m1_p1", bandwidth=0.10)
    assert set(panel["spec_kind"]) == {"main", "donut", "placebo", "polynomial"}
    main_bw = panel.loc[panel["spec_kind"] == "main", "bandwidth"].iloc[0]
    placebo_bws = panel.loc[panel["spec_kind"] == "placebo", "bandwidth"].tolist()
    # All placebo specs should fit at the same bandwidth as main, otherwise
    # the placebo τ would conflate sample-window changes with cutoff shifts.
    for bw in placebo_bws:
        assert abs(float(bw) - float(main_bw)) < 1e-9


def test_run_rdd_robustness_main_spec_matches_local_linear() -> None:
    frame = _toy_rdd_frame(jump=0.03, n_per_side=30)
    panel = run_rdd_robustness(frame, "car_m1_p1", bandwidth=0.10)
    main_row = panel.loc[panel["spec_kind"] == "main"].iloc[0]
    direct = fit_local_linear_rdd(frame, "car_m1_p1", bandwidth=0.10)
    assert abs(float(main_row["tau"]) - float(direct["tau"])) < 1e-9
    assert int(main_row["n_obs"]) == int(direct["n_obs"])
