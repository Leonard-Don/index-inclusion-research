from __future__ import annotations

import numpy as np
import pandas as pd

from index_inclusion_research.analysis.event_study import (
    compute_patell_bmp_summary,
)


def _make_panel(n_events: int = 30, *, seed: int = 0, shock: float = 0.03) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    for event_id in range(n_events):
        for relative_day in range(-20, 21):
            base = rng.normal(scale=0.01)
            event_shock = shock if relative_day == 0 else 0.0
            rows.append(
                {
                    "event_id": f"e{event_id:03d}",
                    "event_phase": "announce",
                    "market": "TEST",
                    "inclusion": 1,
                    "relative_day": relative_day,
                    "ar": base + event_shock,
                }
            )
    return pd.DataFrame(rows)


def test_patell_bmp_runs_and_has_expected_columns() -> None:
    panel = _make_panel()
    summary = compute_patell_bmp_summary(
        panel,
        car_windows=[(-1, 1), (-3, 3)],
        estimation_window=(-20, -2),
    )
    expected = {
        "market",
        "event_phase",
        "inclusion",
        "window",
        "n_events",
        "patell_z",
        "patell_p",
        "bmp_t",
        "bmp_p",
    }
    assert expected.issubset(summary.columns)
    assert (summary["n_events"] > 0).all()


def test_patell_detects_event_day_shock() -> None:
    panel = _make_panel(n_events=50, seed=42, shock=0.03)
    summary = compute_patell_bmp_summary(
        panel,
        car_windows=[(-1, 1)],
        estimation_window=(-20, -2),
    )
    row = summary.iloc[0]
    assert row["patell_z"] > 3.0
    assert row["patell_p"] < 0.01
    assert row["bmp_t"] > 3.0


def test_patell_no_shock_returns_non_significant() -> None:
    panel = _make_panel(n_events=80, seed=1, shock=0.0)
    summary = compute_patell_bmp_summary(
        panel,
        car_windows=[(-1, 1)],
        estimation_window=(-20, -2),
    )
    row = summary.iloc[0]
    assert abs(row["patell_z"]) < 2.5
    assert row["patell_p"] > 0.05


def test_patell_skips_events_with_zero_estimation_variance() -> None:
    rows: list[dict[str, object]] = []
    for event_id in range(5):
        for relative_day in range(-20, 21):
            rows.append(
                {
                    "event_id": f"const{event_id}",
                    "event_phase": "announce",
                    "market": "TEST",
                    "inclusion": 1,
                    "relative_day": relative_day,
                    "ar": 0.0,
                }
            )
    panel = pd.DataFrame(rows)
    summary = compute_patell_bmp_summary(
        panel,
        car_windows=[(-1, 1)],
        estimation_window=(-20, -2),
    )
    assert summary.empty or summary["n_events"].iloc[0] == 0


def test_patell_empty_panel_returns_empty_frame() -> None:
    summary = compute_patell_bmp_summary(
        pd.DataFrame(),
        car_windows=[(-1, 1)],
    )
    assert summary.empty
