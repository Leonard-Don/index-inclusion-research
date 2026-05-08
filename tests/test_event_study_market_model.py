from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from index_inclusion_research.analysis.event_study import (
    compute_market_model_abnormal_returns,
    estimate_market_model,
)


def test_estimate_market_model_recovers_known_alpha_beta() -> None:
    rng = np.random.default_rng(0)
    benchmark = rng.normal(0.0, 0.01, size=200)
    alpha_true = 0.0004
    beta_true = 1.25
    ret = alpha_true + beta_true * benchmark + rng.normal(0.0, 1e-6, size=200)

    alpha, beta = estimate_market_model(ret, benchmark)

    assert alpha == pytest.approx(alpha_true, abs=1e-5)
    assert beta == pytest.approx(beta_true, abs=1e-3)


def test_estimate_market_model_zero_variance_returns_nan() -> None:
    benchmark = np.zeros(50)
    ret = np.linspace(-0.01, 0.01, 50)

    alpha, beta = estimate_market_model(ret, benchmark)

    assert np.isnan(alpha)
    assert np.isnan(beta)


def test_estimate_market_model_too_few_observations_returns_nan() -> None:
    alpha, beta = estimate_market_model([0.01], [0.005])
    assert np.isnan(alpha)
    assert np.isnan(beta)


def test_compute_market_model_abnormal_returns_matches_residual() -> None:
    rng = np.random.default_rng(7)
    rows: list[dict[str, object]] = []
    truth: dict[tuple[str, str], tuple[float, float]] = {}
    for event_id in ("e0", "e1"):
        alpha = rng.uniform(-0.0005, 0.0005)
        beta = rng.uniform(0.8, 1.4)
        truth[(event_id, "announce")] = (alpha, beta)
        for relative_day in range(-30, 11):
            bench = rng.normal(0.0, 0.01)
            ret = alpha + beta * bench + (0.02 if relative_day == 0 else 0.0)
            rows.append(
                {
                    "event_id": event_id,
                    "event_phase": "announce",
                    "relative_day": relative_day,
                    "ret": ret,
                    "benchmark_ret": bench,
                }
            )
    panel = pd.DataFrame(rows)

    result = compute_market_model_abnormal_returns(
        panel,
        estimation_window=(-30, -2),
    )

    assert "ar_market_model" in result.columns
    assert len(result) == len(panel)

    # For each event, AR_it = ret_it - (alpha_hat + beta_hat * benchmark_ret_it)
    for (event_id, phase), (alpha_true, beta_true) in truth.items():
        sub = result.loc[
            (result["event_id"] == event_id) & (result["event_phase"] == phase)
        ]
        expected_ar = sub["ret"] - (alpha_true + beta_true * sub["benchmark_ret"])
        # On the event day (relative_day=0) shock dominates: AR ≈ 0.02
        event_day = sub.loc[sub["relative_day"] == 0, "ar_market_model"].iloc[0]
        assert event_day == pytest.approx(0.02, abs=5e-3)
        # Estimation-window residuals are tiny (no noise), so AR ≈ 0
        est_residuals = sub.loc[
            sub["relative_day"].between(-30, -2), "ar_market_model"
        ]
        assert est_residuals.abs().max() < 1e-6
        # Residuals match expected formula across the whole window
        np.testing.assert_allclose(
            sub["ar_market_model"].to_numpy(),
            expected_ar.to_numpy(),
            atol=1e-6,
        )


def test_compute_market_model_abnormal_returns_preserves_index_ordering() -> None:
    rng = np.random.default_rng(3)
    rows: list[dict[str, object]] = []
    for relative_day in range(-25, 6):
        bench = rng.normal(0.0, 0.01)
        rows.append(
            {
                "event_id": "x",
                "event_phase": "announce",
                "relative_day": relative_day,
                "ret": 0.0002 + 1.1 * bench,
                "benchmark_ret": bench,
            }
        )
    panel = pd.DataFrame(rows)
    shuffled = panel.sample(frac=1.0, random_state=11)

    result = compute_market_model_abnormal_returns(
        shuffled, estimation_window=(-25, -2)
    )

    # Output rows align 1:1 with input rows in the same order.
    assert list(result.index) == list(shuffled.index)
    assert list(result["relative_day"]) == list(shuffled["relative_day"])


def test_compute_market_model_abnormal_returns_handles_duplicate_index_labels() -> None:
    rows: list[dict[str, object]] = []
    for event_id, alpha, beta in [("a", 0.001, 1.0), ("b", 0.003, 2.0)]:
        for relative_day, benchmark_ret in [(-3, 0.01), (-2, 0.02), (0, 0.03)]:
            rows.append(
                {
                    "event_id": event_id,
                    "event_phase": "announce",
                    "relative_day": relative_day,
                    "ret": alpha + beta * benchmark_ret,
                    "benchmark_ret": benchmark_ret,
                }
            )
    panel = pd.DataFrame(rows)
    panel.index = [0, 1, 2, 0, 1, 2]

    result = compute_market_model_abnormal_returns(panel, estimation_window=(-3, -2))

    event_a = result[result["event_id"] == "a"]
    event_b = result[result["event_id"] == "b"]
    assert event_a["market_model_alpha"].iloc[0] == pytest.approx(0.001)
    assert event_a["market_model_beta"].iloc[0] == pytest.approx(1.0)
    assert event_a["ar_market_model"].abs().max() < 1e-12
    assert event_b["market_model_alpha"].iloc[0] == pytest.approx(0.003)
    assert event_b["market_model_beta"].iloc[0] == pytest.approx(2.0)
    assert event_b["ar_market_model"].abs().max() < 1e-12


def test_compute_market_model_abnormal_returns_skips_events_without_estimation_data() -> None:
    rows: list[dict[str, object]] = []
    # Only event-window rows; no estimation window observations available.
    for relative_day in range(-1, 2):
        rows.append(
            {
                "event_id": "thin",
                "event_phase": "announce",
                "relative_day": relative_day,
                "ret": 0.01,
                "benchmark_ret": 0.005,
            }
        )
    panel = pd.DataFrame(rows)

    result = compute_market_model_abnormal_returns(
        panel, estimation_window=(-30, -2)
    )

    assert result["ar_market_model"].isna().all()


def test_compute_market_model_abnormal_returns_empty_panel() -> None:
    result = compute_market_model_abnormal_returns(
        pd.DataFrame(), estimation_window=(-30, -2)
    )
    assert result.empty
