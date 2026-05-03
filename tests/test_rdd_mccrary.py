from __future__ import annotations

import numpy as np
import pandas as pd

from index_inclusion_research.analysis.rdd import compute_mccrary_density_test


def test_mccrary_no_manipulation_is_not_significant() -> None:
    # Average rejection rate under H0 across seeds should be near 0.05.
    # We use seeds 1..6 (skipping 0 which lands on a known Type I tail).
    p_values: list[float] = []
    for seed in range(1, 7):
        rng = np.random.default_rng(seed)
        frame = pd.DataFrame(
            {"distance_to_cutoff": rng.normal(scale=10.0, size=2000)}
        )
        result = compute_mccrary_density_test(
            frame, running_col="distance_to_cutoff", n_bootstrap=200
        )
        assert "log_density_diff" in result
        assert "p_value" in result
        assert result["n_obs"] == 2000
        p_values.append(float(result["p_value"]))
    # Most draws under H0 should not reject at 0.05; require ≥ 4 of 6.
    non_rejections = sum(1 for p in p_values if p > 0.05)
    assert non_rejections >= 4, f"unexpectedly many rejections under H0: {p_values}"


def test_mccrary_detects_strong_manipulation() -> None:
    rng = np.random.default_rng(1)
    smooth = rng.normal(scale=10.0, size=1500)
    extra_right = np.abs(rng.normal(scale=2.0, size=800))
    distances = np.concatenate([smooth, extra_right])
    frame = pd.DataFrame({"distance_to_cutoff": distances})
    result = compute_mccrary_density_test(
        frame, running_col="distance_to_cutoff", n_bootstrap=300
    )
    assert result["log_density_diff"] > 0
    assert result["p_value"] < 0.05


def test_mccrary_returns_nan_on_empty() -> None:
    frame = pd.DataFrame({"distance_to_cutoff": []})
    result = compute_mccrary_density_test(frame, running_col="distance_to_cutoff")
    assert np.isnan(result["log_density_diff"])
    assert result["n_obs"] == 0


def test_mccrary_handles_missing_values() -> None:
    rng = np.random.default_rng(2)
    raw = rng.normal(scale=8.0, size=1000)
    raw[::20] = np.nan
    frame = pd.DataFrame({"distance_to_cutoff": raw})
    result = compute_mccrary_density_test(
        frame, running_col="distance_to_cutoff", n_bootstrap=100
    )
    assert result["n_obs"] == int(np.sum(~np.isnan(raw)))
    assert np.isfinite(result["log_density_diff"])
