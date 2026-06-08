from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from index_inclusion_research.analysis.robustness_event_study import (
    CLUSTERED_CAR_COLUMNS,
    PARALLEL_TRENDS_COLUMNS,
    PERMUTATION_CAR_COLUMNS,
    PLACEBO_CAR_COLUMNS,
    compute_event_clustered_car_se,
    compute_main_car_permutation_test,
    compute_parallel_trends_aar,
    compute_placebo_car_distribution,
    export_event_clustered_car_se,
    export_main_car_permutation_test,
    export_placebo_car_distribution,
)
from index_inclusion_research.loaders import save_dataframe

# --------------------------------------------------------------------------- #
# Synthetic, fully-seeded fixtures
# --------------------------------------------------------------------------- #


def _matched_panel(
    *,
    n_events: int = 12,
    treatment_jump: float = 0.04,
    seed: int = 7,
) -> pd.DataFrame:
    """Matched treated/control panel with parallel pre-event AAR.

    Treated and control share the same iid pre-event noise process; treated
    additionally receive a one-day jump at relative_day 0 (and a small positive
    drift afterwards). So the AAR gap is ~0 for relative_day < 0 and opens up
    from day 0 onward.
    """
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    rel_days = list(range(-20, 6))
    for event_idx in range(n_events):
        for group, treat in (("trt", 1), ("ctrl", 0)):
            event_id = f"E{event_idx:03d}-{group}"
            for rel in rel_days:
                ar = float(rng.normal(0.0, 0.01))
                if treat == 1 and rel == 0:
                    ar += treatment_jump
                if treat == 1 and rel > 0:
                    ar += 0.002
                rows.append(
                    {
                        "event_id": event_id,
                        "matched_to_event_id": f"E{event_idx:03d}",
                        "market": "CN",
                        "event_phase": "announce",
                        "inclusion": 1,
                        "treatment_group": treat,
                        "event_date": pd.Timestamp("2021-01-04") + pd.Timedelta(days=event_idx),
                        "relative_day": rel,
                        "ar": ar,
                    }
                )
    return pd.DataFrame(rows)


def _event_level_with_effect(
    *,
    n_events: int = 24,
    effect: float = 0.05,
    n_dates: int = 6,
    seed: int = 11,
) -> pd.DataFrame:
    """Per-event CAR frame with a real positive mean and date clusters."""
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    dates = [pd.Timestamp("2021-01-04") + pd.Timedelta(days=7 * i) for i in range(n_dates)]
    for i in range(n_events):
        rows.append(
            {
                "event_id": f"E{i:03d}",
                "market": "CN",
                "event_phase": "announce",
                "inclusion": 1,
                "treatment_group": 1,
                "event_date": dates[i % n_dates],
                "batch_id": f"batch-{i % n_dates}",
                "car_m1_p1": effect + float(rng.normal(0.0, 0.02)),
            }
        )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# 1. Parallel-trends AAR
# --------------------------------------------------------------------------- #


def test_parallel_trends_empty_input_anchors_schema() -> None:
    result = compute_parallel_trends_aar(pd.DataFrame())
    assert result.empty
    assert set(PARALLEL_TRENDS_COLUMNS).issubset(result.columns)


def test_parallel_trends_pre_window_overlaps_event_window_diverges() -> None:
    panel = _matched_panel()
    aar = compute_parallel_trends_aar(panel)

    assert not aar.empty
    assert set(PARALLEL_TRENDS_COLUMNS).issubset(aar.columns)
    # One row per (market, phase, relative_day); 26 days, single cell.
    assert len(aar) == 26

    pre = aar.loc[aar["relative_day"].between(-20, -2)]
    event_day = aar.loc[aar["relative_day"] == 0].iloc[0]

    # Quasi-parallel pre-window: average |gap| is small relative to the jump.
    mean_abs_pre_gap = pre["aar_gap"].abs().mean()
    assert mean_abs_pre_gap < 0.01
    # Event day: treated jumps well above control.
    assert event_day["aar_gap"] > 0.02
    assert abs(event_day["aar_gap"]) > 2 * mean_abs_pre_gap


def test_parallel_trends_counts_match_groups() -> None:
    panel = _matched_panel(n_events=8)
    aar = compute_parallel_trends_aar(panel)
    row = aar.iloc[0]
    assert int(row["n_treated"]) == 8
    assert int(row["n_control"]) == 8


# --------------------------------------------------------------------------- #
# 2. Placebo pseudo-event CAR distribution
# --------------------------------------------------------------------------- #


def test_placebo_empty_input_anchors_schema() -> None:
    result = compute_placebo_car_distribution(pd.DataFrame(), [(-1, 1)])
    assert result.empty
    assert set(PLACEBO_CAR_COLUMNS).issubset(result.columns)


def test_placebo_real_car_lands_in_tail_for_strong_effect() -> None:
    panel = _matched_panel(n_events=20, treatment_jump=0.06)
    # Only treated drive the placebo distribution (controls are filtered out).
    result = compute_placebo_car_distribution(
        panel,
        [(-1, 1)],
        n_placebo_draws=300,
        seed=2026,
    )
    assert not result.empty
    row = result.iloc[0]
    assert np.isfinite(row["real_mean_car"])
    assert row["real_mean_car"] > 0
    # Strong real effect should sit in the tail -> small empirical p-value.
    assert 0.0 < row["empirical_p_value"] <= 0.05
    # Placebo null should be centred near zero.
    assert abs(row["placebo_mean"]) < abs(row["real_mean_car"])
    assert int(row["n_placebo_draws"]) > 0


def test_placebo_is_deterministic_under_fixed_seed() -> None:
    panel = _matched_panel(n_events=16, treatment_jump=0.05)
    first = compute_placebo_car_distribution(panel, [(-1, 1)], n_placebo_draws=200, seed=99)
    second = compute_placebo_car_distribution(panel, [(-1, 1)], n_placebo_draws=200, seed=99)
    pd.testing.assert_frame_equal(first, second)


def test_placebo_too_few_events_emits_nan_pvalue() -> None:
    panel = _matched_panel(n_events=2, treatment_jump=0.05)
    result = compute_placebo_car_distribution(panel, [(-1, 1)], n_placebo_draws=50, seed=1)
    assert not result.empty
    assert np.isnan(result.iloc[0]["empirical_p_value"])


def test_placebo_export_round_trips(tmp_path: Path) -> None:
    panel = _matched_panel(n_events=12, treatment_jump=0.05)
    result = compute_placebo_car_distribution(panel, [(-1, 1)], n_placebo_draws=100, seed=5)
    out_path = export_placebo_car_distribution(result, output_dir=tmp_path)
    assert out_path.exists()
    reloaded = pd.read_csv(out_path)
    assert set(PLACEBO_CAR_COLUMNS).issubset(reloaded.columns)


# --------------------------------------------------------------------------- #
# 3a. Permutation test for the main CAR
# --------------------------------------------------------------------------- #


def test_permutation_empty_input_anchors_schema() -> None:
    result = compute_main_car_permutation_test(pd.DataFrame())
    assert result.empty
    assert set(PERMUTATION_CAR_COLUMNS).issubset(result.columns)


def test_permutation_detects_real_effect() -> None:
    event_level = _event_level_with_effect(n_events=30, effect=0.05, seed=3)
    result = compute_main_car_permutation_test(event_level, n_permutations=2000, seed=42)
    assert not result.empty
    row = result.iloc[0]
    assert row["observed_mean_car"] == pytest.approx(
        event_level["car_m1_p1"].mean(), rel=1e-9
    )
    # Strong positive mean -> sign-flip null rarely as extreme -> small p.
    assert 0.0 < row["empirical_p_value"] < 0.05
    # Sign-flip null is symmetric about zero.
    assert abs(row["permutation_mean"]) < abs(row["observed_mean_car"])


def test_permutation_null_effect_p_value_is_large() -> None:
    rng = np.random.default_rng(123)
    rows = [
        {
            "market": "CN",
            "event_phase": "announce",
            "treatment_group": 1,
            "car_m1_p1": float(rng.normal(0.0, 0.02)),
        }
        for _ in range(40)
    ]
    result = compute_main_car_permutation_test(
        pd.DataFrame(rows), n_permutations=2000, seed=7
    )
    row = result.iloc[0]
    assert row["empirical_p_value"] > 0.10


def test_permutation_is_deterministic_under_fixed_seed() -> None:
    event_level = _event_level_with_effect(n_events=20, effect=0.04, seed=8)
    first = compute_main_car_permutation_test(event_level, n_permutations=1000, seed=55)
    second = compute_main_car_permutation_test(event_level, n_permutations=1000, seed=55)
    pd.testing.assert_frame_equal(first, second)


def test_permutation_export_round_trips(tmp_path: Path) -> None:
    event_level = _event_level_with_effect(n_events=20, effect=0.04, seed=2)
    result = compute_main_car_permutation_test(event_level, n_permutations=500, seed=2)
    out_path = export_main_car_permutation_test(result, output_dir=tmp_path)
    assert out_path.exists()
    reloaded = pd.read_csv(out_path)
    assert set(PERMUTATION_CAR_COLUMNS).issubset(reloaded.columns)


# --------------------------------------------------------------------------- #
# 3b. Event/batch-clustered CRV1 SE
# --------------------------------------------------------------------------- #


def _correlated_same_date_event_level() -> pd.DataFrame:
    """Per-event CARs where same-date events are positively correlated."""
    date_shocks = {
        "2021-01-04": 0.030,
        "2021-02-01": -0.010,
        "2021-03-01": 0.045,
    }
    wobble = [0.001, -0.001, 0.0008, -0.0008]
    rows: list[dict[str, object]] = []
    event_id = 0
    for the_date, shock in date_shocks.items():
        for offset in wobble:
            event_id += 1
            rows.append(
                {
                    "event_id": event_id,
                    "market": "CN",
                    "event_phase": "announce",
                    "inclusion": 1,
                    "treatment_group": 1,
                    "event_date": pd.Timestamp(the_date),
                    "batch_id": the_date,
                    "car_m1_p1": shock + offset,
                }
            )
    return pd.DataFrame(rows)


def test_clustered_empty_input_anchors_schema() -> None:
    result = compute_event_clustered_car_se(pd.DataFrame())
    assert result.empty
    assert set(CLUSTERED_CAR_COLUMNS).issubset(result.columns)


def test_clustered_se_widens_under_positive_within_date_correlation() -> None:
    event_level = _correlated_same_date_event_level()
    result = compute_event_clustered_car_se(event_level, cluster_col="event_date")
    row = result.iloc[0]
    assert np.isfinite(row["se_iid"])
    assert np.isfinite(row["se_clustered"])
    # Positive within-cluster correlation -> clustered SE >= iid SE.
    assert row["se_clustered"] >= row["se_iid"]
    assert int(row["n_clusters"]) == 3
    assert int(row["n_events"]) == 12


def test_clustered_se_can_cluster_on_batch_id() -> None:
    event_level = _correlated_same_date_event_level()
    result = compute_event_clustered_car_se(event_level, cluster_col="batch_id")
    row = result.iloc[0]
    assert row["cluster_col"] == "batch_id"
    assert int(row["n_clusters"]) == 3
    assert np.isfinite(row["se_clustered"])


def test_clustered_se_nan_when_single_cluster() -> None:
    event_level = _correlated_same_date_event_level().assign(
        event_date=pd.Timestamp("2021-01-04")
    )
    result = compute_event_clustered_car_se(event_level, cluster_col="event_date")
    row = result.iloc[0]
    assert np.isfinite(row["se_iid"])
    assert np.isnan(row["se_clustered"])
    assert np.isnan(row["p_clustered"])


def test_clustered_mean_car_matches_simple_mean() -> None:
    event_level = _event_level_with_effect(n_events=18, effect=0.03, seed=4)
    result = compute_event_clustered_car_se(event_level, cluster_col="event_date")
    row = result.iloc[0]
    assert row["mean_car"] == pytest.approx(event_level["car_m1_p1"].mean(), rel=1e-9)


def test_clustered_export_round_trips(tmp_path: Path) -> None:
    event_level = _correlated_same_date_event_level()
    result = compute_event_clustered_car_se(event_level, cluster_col="event_date")
    out_path = export_event_clustered_car_se(result, output_dir=tmp_path)
    assert out_path.exists()
    reloaded = pd.read_csv(out_path)
    assert set(CLUSTERED_CAR_COLUMNS).issubset(reloaded.columns)


def test_empty_frames_round_trip_via_save_dataframe(tmp_path: Path) -> None:
    """Each empty schema-anchored frame must survive a CSV round-trip."""
    for frame, name in (
        (compute_parallel_trends_aar(pd.DataFrame()), "aar.csv"),
        (compute_placebo_car_distribution(pd.DataFrame(), [(-1, 1)]), "placebo.csv"),
        (compute_main_car_permutation_test(pd.DataFrame()), "perm.csv"),
        (compute_event_clustered_car_se(pd.DataFrame()), "clustered.csv"),
    ):
        path = tmp_path / name
        save_dataframe(frame, path)
        reloaded = pd.read_csv(path)
        assert reloaded.empty
