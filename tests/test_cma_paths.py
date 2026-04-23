from __future__ import annotations

import pandas as pd
import pytest

from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
    build_daily_ar_panel,
)


def _make_panel_frame() -> pd.DataFrame:
    rows = []
    for market in ("CN", "US"):
        for event_phase in ("announce", "effective"):
            for event_id, ar_scale in ((1, 0.01), (2, 0.02)):
                for rel in range(-3, 4):
                    rows.append(
                        {
                            "event_id": event_id,
                            "market": market,
                            "event_phase": event_phase,
                            "event_type": "addition",
                            "relative_day": rel,
                            "ar": ar_scale * (1 if rel == 0 else 0.5),
                        }
                    )
    rows.append(
        {
            "event_id": 99,
            "market": "US",
            "event_phase": "announce",
            "event_type": "deletion",
            "relative_day": 0,
            "ar": 0.9,
        }
    )
    return pd.DataFrame(rows)


def test_build_daily_ar_panel_filters_additions_only():
    raw = _make_panel_frame()
    out = build_daily_ar_panel(raw)
    assert (out["event_type"] == "addition").all()
    assert "relative_day" in out.columns
    assert set(out["market"].unique()) == {"CN", "US"}
    assert set(out["event_phase"].unique()) == {"announce", "effective"}


def test_build_daily_ar_panel_adds_cumulative_car():
    raw = _make_panel_frame()
    out = build_daily_ar_panel(raw)
    assert "car" in out.columns
    first = (
        out.sort_values(["event_id", "market", "event_phase", "relative_day"])
        .groupby(["event_id", "market", "event_phase"], as_index=False)
        .head(1)
    )
    pd.testing.assert_series_equal(
        first["ar"].reset_index(drop=True),
        first["car"].reset_index(drop=True),
        check_names=False,
    )


def test_build_daily_ar_panel_requires_columns():
    with pytest.raises(ValueError, match="missing columns"):
        build_daily_ar_panel(pd.DataFrame({"ar": [0.0]}))


def test_compute_average_paths_schema():
    raw = _make_panel_frame()
    ar_panel = build_daily_ar_panel(raw)
    from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
        compute_average_paths,
    )
    avg = compute_average_paths(ar_panel)
    expected_columns = {
        "market",
        "event_phase",
        "relative_day",
        "n_events",
        "ar_mean",
        "ar_se",
        "ar_t",
        "car_mean",
        "car_se",
        "car_t",
    }
    assert expected_columns.issubset(set(avg.columns))


def test_compute_average_paths_computes_mean_correctly():
    raw = _make_panel_frame()
    ar_panel = build_daily_ar_panel(raw)
    from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
        compute_average_paths,
    )
    avg = compute_average_paths(ar_panel)
    cn_announce_day_zero = avg.loc[
        (avg["market"] == "CN")
        & (avg["event_phase"] == "announce")
        & (avg["relative_day"] == 0)
    ]
    assert cn_announce_day_zero["ar_mean"].iloc[0] == pytest.approx(0.015, abs=1e-9)
    assert cn_announce_day_zero["n_events"].iloc[0] == 2


def test_compute_window_summary_produces_expected_rows():
    raw = _make_panel_frame()
    ar_panel = build_daily_ar_panel(raw)
    from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
        compute_window_summary,
    )
    summary = compute_window_summary(ar_panel, windows=[(-1, 1), (-3, 3)])
    assert {
        "market",
        "event_phase",
        "window_start",
        "window_end",
        "car_mean",
        "car_se",
        "car_t",
        "p_value",
        "n_events",
    }.issubset(summary.columns)
    assert len(summary) == 8


def test_compute_window_summary_respects_window_bounds():
    rows = []
    for rel in range(-5, 6):
        rows.append(
            {
                "event_id": 1,
                "market": "CN",
                "event_phase": "announce",
                "event_type": "addition",
                "relative_day": rel,
                "ar": 0.01,
            }
        )
    ar_panel = build_daily_ar_panel(pd.DataFrame(rows))
    from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
        compute_window_summary,
    )
    summary = compute_window_summary(ar_panel, windows=[(-1, 1)])
    assert summary["car_mean"].iloc[0] == pytest.approx(0.03, abs=1e-9)
