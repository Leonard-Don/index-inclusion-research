from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from index_inclusion_research.analysis.event_study import (
    compute_market_model_abnormal_returns,
    estimate_market_model,
    summarize_market_model_estimation_obs,
)
from index_inclusion_research.build_price_panel import build_arg_parser


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


def test_compute_market_model_abnormal_returns_records_estimation_obs_count() -> None:
    """Expose a per-event diagnostic so a NaN ``ar_market_model`` is auditable.

    Why: ``compute_market_model_abnormal_returns`` silently returns NaN when an
    event/phase has too few estimation rows (or a degenerate benchmark). Once
    the column flows into the panel, downstream dashboards / paper tables only
    see the NaN — they cannot tell whether the estimation window was thin, the
    benchmark variance was zero, or the row simply lay outside the estimation
    window. A deterministic ``market_model_estimation_obs`` column lets every
    consumer audit the failure mode straight from the panel without re-running
    the estimation or reading logs.

    The count must reflect the number of *paired* (ret, benchmark_ret) rows
    inside the estimation window — i.e., what ``estimate_market_model``
    actually consumes after dropping NaNs — and must be present on every row
    of the event/phase, whether estimation succeeded or not.
    """
    rows: list[dict[str, object]] = []
    # Event "fat": full estimation window, all rows non-null -> count = 5.
    for relative_day in range(-5, 1):
        rows.append(
            {
                "event_id": "fat",
                "event_phase": "announce",
                "relative_day": relative_day,
                "ret": 0.001 + 1.2 * (0.0005 * relative_day),
                "benchmark_ret": 0.0005 * relative_day,
            }
        )
    # Event "thin": only one estimation-window observation -> count = 1, AR NaN.
    rows.extend(
        [
            {
                "event_id": "thin",
                "event_phase": "announce",
                "relative_day": -2,
                "ret": 0.01,
                "benchmark_ret": 0.004,
            },
            {
                "event_id": "thin",
                "event_phase": "announce",
                "relative_day": 0,
                "ret": 0.02,
                "benchmark_ret": 0.005,
            },
        ]
    )
    # Event "nan_bench": estimation-window benchmark all NaN -> count = 0, AR NaN.
    rows.extend(
        [
            {
                "event_id": "nan_bench",
                "event_phase": "announce",
                "relative_day": rel,
                "ret": 0.01,
                "benchmark_ret": float("nan"),
            }
            for rel in (-4, -3, -2)
        ]
        + [
            {
                "event_id": "nan_bench",
                "event_phase": "announce",
                "relative_day": 0,
                "ret": 0.05,
                "benchmark_ret": 0.001,
            }
        ]
    )
    panel = pd.DataFrame(rows)

    result = compute_market_model_abnormal_returns(
        panel, estimation_window=(-5, -1)
    )

    assert "market_model_estimation_obs" in result.columns
    fat = result.loc[result["event_id"] == "fat"]
    thin = result.loc[result["event_id"] == "thin"]
    nan_bench = result.loc[result["event_id"] == "nan_bench"]

    # Every row in an event/phase shares the same diagnostic count.
    assert fat["market_model_estimation_obs"].nunique() == 1
    assert thin["market_model_estimation_obs"].nunique() == 1
    assert nan_bench["market_model_estimation_obs"].nunique() == 1

    assert int(fat["market_model_estimation_obs"].iloc[0]) == 5
    assert int(thin["market_model_estimation_obs"].iloc[0]) == 1
    assert int(nan_bench["market_model_estimation_obs"].iloc[0]) == 0

    # Diagnostic is recorded even when AR cannot be estimated.
    assert thin["ar_market_model"].isna().all()
    assert nan_bench["ar_market_model"].isna().all()
    # Successful event still gets a finite AR.
    assert fat["ar_market_model"].notna().all()


def test_summarize_market_model_estimation_obs_counts_events_under_existing_gate() -> None:
    """Aggregate the per-row diagnostic into a one-row audit summary.

    Why: ``compute_market_model_abnormal_returns`` already writes
    ``market_model_estimation_obs`` on every panel row, but downstream
    consumers (paper bundle scripts, dashboards, doctor checks) need a
    deterministic, fixed-shape rollup so they can answer "how many event ×
    phase rows fell below the OLS gate of two paired observations?" without
    re-deriving the count themselves. Anchoring the threshold to the model's
    existing minimum-observation gate (``len(frame) < 2`` inside
    ``estimate_market_model``) avoids inventing a new user-facing policy.

    The summary must:
    - have exactly one row (a single audit record per panel),
    - count distinct event/phase combos rather than panel rows,
    - separate AR finiteness from below-minimum estimation obs (the latter
      is a strict subset of the former by construction, but distinguishing
      them lets users see thin estimation windows vs. zero-variance
      benchmarks at a glance),
    - record the threshold itself so future drift is auditable.
    """
    rows: list[dict[str, object]] = []
    # Event "fat": 5 estimation obs, finite AR.
    for relative_day in range(-5, 1):
        rows.append(
            {
                "event_id": "fat",
                "event_phase": "announce",
                "relative_day": relative_day,
                "ret": 0.001 + 1.2 * (0.0005 * relative_day),
                "benchmark_ret": 0.0005 * relative_day,
            }
        )
    # Event "thin": 1 estimation obs -> below gate, AR NaN.
    rows.extend(
        [
            {
                "event_id": "thin",
                "event_phase": "announce",
                "relative_day": -2,
                "ret": 0.01,
                "benchmark_ret": 0.004,
            },
            {
                "event_id": "thin",
                "event_phase": "announce",
                "relative_day": 0,
                "ret": 0.02,
                "benchmark_ret": 0.005,
            },
        ]
    )
    # Event "nan_bench": 0 estimation obs (benchmark all NaN), AR NaN.
    rows.extend(
        [
            {
                "event_id": "nan_bench",
                "event_phase": "announce",
                "relative_day": rel,
                "ret": 0.01,
                "benchmark_ret": float("nan"),
            }
            for rel in (-4, -3, -2)
        ]
        + [
            {
                "event_id": "nan_bench",
                "event_phase": "announce",
                "relative_day": 0,
                "ret": 0.05,
                "benchmark_ret": 0.001,
            }
        ]
    )
    panel = pd.DataFrame(rows)
    augmented = compute_market_model_abnormal_returns(
        panel, estimation_window=(-5, -1)
    )

    summary = summarize_market_model_estimation_obs(augmented)

    assert isinstance(summary, pd.DataFrame)
    assert len(summary) == 1
    expected_columns = {
        "n_events_total",
        "n_events_finite_ar",
        "n_events_nan_ar",
        "n_events_below_min_obs",
        "minimum_estimation_obs",
    }
    assert expected_columns.issubset(summary.columns)
    record = summary.iloc[0]
    assert int(record["n_events_total"]) == 3
    assert int(record["n_events_finite_ar"]) == 1
    assert int(record["n_events_nan_ar"]) == 2
    # "thin" (1 obs) and "nan_bench" (0 obs) both fall below the OLS gate of 2.
    assert int(record["n_events_below_min_obs"]) == 2
    assert int(record["minimum_estimation_obs"]) == 2


def test_summarize_market_model_estimation_obs_returns_zeros_when_columns_missing() -> None:
    """Calling on a panel without the diagnostic columns yields a deterministic
    empty rollup, not a KeyError.

    Why: the helper is meant to be safe to invoke against any event panel,
    including those built without ``--include-market-model-ar``. Returning a
    one-row, all-zero summary lets pipelines emit the diagnostic
    unconditionally and lets dashboards render a "no market-model AR run"
    state without branching on column presence.
    """
    panel = pd.DataFrame(
        [
            {"event_id": "x", "event_phase": "announce", "ar": 0.001},
            {"event_id": "y", "event_phase": "effective", "ar": 0.002},
        ]
    )

    summary = summarize_market_model_estimation_obs(panel)

    assert len(summary) == 1
    record = summary.iloc[0]
    assert int(record["n_events_total"]) == 0
    assert int(record["n_events_finite_ar"]) == 0
    assert int(record["n_events_nan_ar"]) == 0
    assert int(record["n_events_below_min_obs"]) == 0
    assert int(record["minimum_estimation_obs"]) == 2


def test_cli_reference_documents_market_model_ar_flag_and_output_columns() -> None:
    """docs/cli_reference.md must document the build-price-panel market-model
    AR opt-in flag and every column the helper appends.

    Why: ``index-inclusion-build-price-panel --include-market-model-ar`` is
    the only user-facing path to the new ``ar_market_model`` /
    ``market_model_alpha`` / ``market_model_beta`` columns; if the flag or any
    column rename slips in without the docs moving, downstream researchers,
    dashboard loaders and the paper bundle scripts will silently consume a
    schema that no longer matches the published CLI reference.

    The flag is read from ``build_arg_parser()`` and the columns are read by
    actually running ``compute_market_model_abnormal_returns`` on a tiny
    panel, so this guard tracks the source of truth instead of a hand-copied
    string list.
    """
    parser = build_arg_parser()
    market_model_actions = [
        action
        for action in parser._actions  # type: ignore[attr-defined]
        if any("market-model-ar" in opt for opt in (action.option_strings or ()))
    ]
    assert market_model_actions, (
        "build-price-panel must expose a --*market-model-ar* opt-in flag"
    )
    flag_strings = {
        option
        for action in market_model_actions
        for option in action.option_strings
    }

    sample_panel = pd.DataFrame(
        [
            {
                "event_id": "guard",
                "event_phase": "announce",
                "relative_day": rel,
                "ret": 0.001 * rel,
                "benchmark_ret": 0.0005 * rel,
            }
            for rel in range(-5, 1)
        ]
    )
    augmented = compute_market_model_abnormal_returns(
        sample_panel, estimation_window=(-5, -1)
    )
    appended_columns = [
        column for column in augmented.columns if column not in sample_panel.columns
    ]
    assert appended_columns, (
        "compute_market_model_abnormal_returns must append market-model columns"
    )

    cli_reference = (
        Path(__file__).resolve().parents[1] / "docs" / "cli_reference.md"
    ).read_text(encoding="utf-8")

    for flag in flag_strings:
        assert flag in cli_reference, (
            f"docs/cli_reference.md must document build-price-panel flag {flag!r}"
        )
    for column in appended_columns:
        assert column in cli_reference, (
            f"docs/cli_reference.md must document market-model AR output column {column!r}"
        )
