from __future__ import annotations

import warnings

import pandas as pd

from index_inclusion_research.analysis.cross_market_asymmetry.time_series import (
    build_rolling_car,
    export_time_series_tables,
    render_rolling_figure,
    summarize_structural_break,
)


def _make_events_with_dates():
    rows = []
    for i, year in enumerate(range(2005, 2025)):
        for market in ("CN", "US"):
            for phase in ("announce", "effective"):
                for rel in (-1, 0, 1):
                    rows.append(
                        {
                            "event_id": i * 10 + (0 if market == "CN" else 1),
                            "market": market,
                            "event_type": "addition",
                            "event_phase": phase,
                            "relative_day": rel,
                            "ar": 0.01,
                            "event_date": f"{year}-06-15",
                        }
                    )
    return pd.DataFrame(rows)


def test_build_rolling_car_schema():
    panel = _make_events_with_dates()
    out = build_rolling_car(panel, window_years=5, step_years=1)
    expected = {
        "market",
        "event_phase",
        "window_end_year",
        "car_mean",
        "car_se",
        "car_t",
        "n_events",
    }
    assert expected.issubset(out.columns)


def test_build_rolling_car_respects_window():
    panel = _make_events_with_dates()
    out = build_rolling_car(panel, window_years=5, step_years=5)
    assert set(out["window_end_year"].unique()).issuperset({2009, 2014, 2019, 2024})


def test_summarize_structural_break_returns_pre_post():
    panel = _make_events_with_dates()
    rolling = build_rolling_car(panel, window_years=3, step_years=1)
    out = summarize_structural_break(rolling, split_year=2015)
    assert {"market", "event_phase", "period", "car_mean", "car_se"}.issubset(out.columns)
    assert set(out["period"].unique()) == {"pre", "post"}


def test_render_rolling_figure_writes_png(tmp_path):
    panel = _make_events_with_dates()
    rolling = build_rolling_car(panel)
    out = render_rolling_figure(rolling, output_dir=tmp_path)
    assert out["figure"].exists()
    assert out["aum_overlay"] is False


def test_render_rolling_figure_skips_empty_legend_warning(tmp_path):
    rolling = pd.DataFrame(
        columns=[
            "market",
            "event_phase",
            "window_end_year",
            "car_mean",
        ]
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = render_rolling_figure(rolling, output_dir=tmp_path)
    assert out["figure"].exists()
    assert all(
        "No artists with labels found to put in legend" not in str(w.message)
        for w in caught
    )


def test_export_time_series_tables_writes_csvs(tmp_path):
    panel = _make_events_with_dates()
    rolling = build_rolling_car(panel)
    break_df = summarize_structural_break(rolling)
    paths = export_time_series_tables(rolling, break_df, output_dir=tmp_path)
    assert paths["rolling"].exists()
    assert paths["break"].exists()
