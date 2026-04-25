"""Tests for ``index_inclusion_research.chart_data``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from index_inclusion_research.chart_data import (
    CHART_BUILDERS,
    build_car_heatmap_chart_data,
    build_car_path_chart_data,
    build_chart_data,
    build_gap_decomposition_chart_data,
    build_heterogeneity_size_chart_data,
    build_price_pressure_chart_data,
    build_time_series_rolling_chart_data,
)


@pytest.fixture()
def empty_root(tmp_path: Path) -> Path:
    """Return a root directory with no result files."""
    return tmp_path


@pytest.fixture()
def populated_root(tmp_path: Path) -> Path:
    """Return a root directory with minimal CSV fixtures."""
    tables = tmp_path / "results" / "real_tables"
    tables.mkdir(parents=True)

    # AR path data — production exporters write ``relative_day``.
    ar_path = tables / "cma_ar_path.csv"
    ar_path.write_text(
        "market,event_phase,relative_day,ar_mean\n"
        "CN,announce,-2,0.001\n"
        "CN,announce,-1,0.003\n"
        "CN,announce,0,0.015\n"
        "CN,announce,1,0.002\n"
        "US,announce,-2,-0.001\n"
        "US,announce,-1,0.002\n"
        "US,announce,0,0.010\n"
        "US,announce,1,0.001\n"
    )

    # CAR path data
    car_path = tables / "cma_car_path.csv"
    car_path.write_text(
        "market,event_phase,relative_day,car_mean\n"
        "CN,announce,-2,0.001\n"
        "CN,announce,-1,0.004\n"
        "CN,announce,0,0.019\n"
        "CN,announce,1,0.021\n"
        "US,announce,-2,-0.001\n"
        "US,announce,-1,0.001\n"
        "US,announce,0,0.011\n"
        "US,announce,1,0.012\n"
    )

    # Event study summary (for heatmap)
    event = tables / "event_study_summary.csv"
    event.write_text(
        "market,event_phase,window,inclusion,mean_car,p_value\n"
        "CN,announce,\"[-1,+1]\",1,0.0185,0.005\n"
        "CN,effective,\"[-1,+1]\",1,0.0032,0.540\n"
        "US,announce,\"[-1,+1]\",1,0.0088,0.018\n"
        "US,effective,\"[-1,+1]\",1,-0.0021,0.720\n"
        "CN,announce,\"[-3,+3]\",1,0.0210,0.003\n"
        "CN,effective,\"[-3,+3]\",1,0.0045,0.310\n"
        "US,announce,\"[-3,+3]\",1,0.0110,0.042\n"
        "US,effective,\"[-3,+3]\",1,-0.0035,0.580\n"
        "CN,announce,\"[-5,+5]\",1,0.0230,0.001\n"
        "CN,effective,\"[-5,+5]\",1,0.0060,0.220\n"
        "US,announce,\"[-5,+5]\",1,0.0125,0.055\n"
        "US,effective,\"[-5,+5]\",1,-0.0040,0.490\n"
    )

    # Time series summary (for price pressure)
    ts = tables / "time_series_event_study_summary.csv"
    ts.write_text(
        "market,event_phase,announce_year,inclusion,mean_car_m1_p1\n"
        "CN,announce,2018,1,0.020\n"
        "CN,announce,2019,1,0.018\n"
        "CN,announce,2020,1,0.022\n"
        "US,announce,2018,1,0.012\n"
        "US,announce,2019,1,0.010\n"
        "US,announce,2020,1,0.008\n"
    )

    return tmp_path


# ── registry tests ───────────────────────────────────────────────────


class TestChartRegistry:
    def test_known_chart_ids(self) -> None:
        assert set(CHART_BUILDERS) == {
            "car_path",
            "price_pressure",
            "car_heatmap",
            "gap_decomposition",
            "heterogeneity_size",
            "time_series_rolling",
        }

    def test_build_chart_data_returns_none_for_unknown(self, empty_root: Path) -> None:
        assert build_chart_data("nonexistent", empty_root) is None


# ── car_path ─────────────────────────────────────────────────────────


class TestBuildCarPathChartData:
    def test_empty_root(self, empty_root: Path) -> None:
        result = build_car_path_chart_data(empty_root)
        assert result == {"series": [], "days": []}

    def test_populated_returns_series(self, populated_root: Path) -> None:
        result = build_car_path_chart_data(populated_root)
        assert isinstance(result["series"], list)
        assert len(result["series"]) > 0

    def test_series_structure(self, populated_root: Path) -> None:
        result = build_car_path_chart_data(populated_root)
        for s in result["series"]:
            assert "name" in s
            assert "data" in s
            assert "color" in s
            assert isinstance(s["data"], list)
            for point in s["data"]:
                assert len(point) == 2

    def test_days_are_sorted_integers(self, populated_root: Path) -> None:
        result = build_car_path_chart_data(populated_root)
        days = result["days"]
        assert days == sorted(days)
        assert all(isinstance(d, int) for d in days)

    def test_json_serializable(self, populated_root: Path) -> None:
        result = build_car_path_chart_data(populated_root)
        json.dumps(result)


# ── price_pressure ───────────────────────────────────────────────────


class TestBuildPricePressureChartData:
    def test_empty_root(self, empty_root: Path) -> None:
        result = build_price_pressure_chart_data(empty_root)
        assert result == {"series": [], "years": []}

    def test_populated_returns_series(self, populated_root: Path) -> None:
        result = build_price_pressure_chart_data(populated_root)
        assert len(result["series"]) > 0

    def test_years_are_sorted(self, populated_root: Path) -> None:
        result = build_price_pressure_chart_data(populated_root)
        years = result["years"]
        assert years == sorted(years)

    def test_json_serializable(self, populated_root: Path) -> None:
        result = build_price_pressure_chart_data(populated_root)
        json.dumps(result)


# ── car_heatmap ──────────────────────────────────────────────────────


class TestBuildCarHeatmapChartData:
    def test_empty_root(self, empty_root: Path) -> None:
        result = build_car_heatmap_chart_data(empty_root)
        assert result["data"] == []

    def test_populated_returns_heatmap_data(self, populated_root: Path) -> None:
        result = build_car_heatmap_chart_data(populated_root)
        assert len(result["data"]) > 0
        assert len(result["annotations"]) > 0
        assert len(result["row_labels"]) > 0
        assert len(result["col_labels"]) > 0

    def test_annotations_have_p_values(self, populated_root: Path) -> None:
        result = build_car_heatmap_chart_data(populated_root)
        for ann in result["annotations"]:
            assert "p_value" in ann
            assert "stars" in ann
            assert "car_pct" in ann

    def test_vmax_is_positive(self, populated_root: Path) -> None:
        result = build_car_heatmap_chart_data(populated_root)
        assert result["vmax"] > 0

    def test_json_serializable(self, populated_root: Path) -> None:
        result = build_car_heatmap_chart_data(populated_root)
        json.dumps(result)


# ── gap_decomposition ────────────────────────────────────────────────


@pytest.fixture()
def gap_decomposition_root(tmp_path: Path) -> Path:
    tables = tmp_path / "results" / "real_tables"
    tables.mkdir(parents=True)
    (tables / "cma_gap_summary.csv").write_text(
        "market,metric,mean,n_events\n"
        "CN,announce_jump,0.0175,118\n"
        "CN,gap_drift,0.0076,118\n"
        "CN,effective_jump,0.0042,118\n"
        "CN,post_effective_reversal,-0.0180,118\n"
        "US,announce_jump,0.0147,318\n"
        "US,gap_drift,-0.0033,318\n"
        "US,effective_jump,0.0008,318\n"
        "US,post_effective_reversal,-0.0021,318\n"
    )
    return tmp_path


class TestBuildGapDecompositionChartData:
    def test_empty_root(self, empty_root: Path) -> None:
        result = build_gap_decomposition_chart_data(empty_root)
        assert result == {"markets": [], "segments": [], "series": []}

    def test_populated_returns_4_segments_per_market(self, gap_decomposition_root: Path) -> None:
        result = build_gap_decomposition_chart_data(gap_decomposition_root)
        assert len(result["series"]) == 4
        assert all(len(s["data"]) == 2 for s in result["series"])
        assert "中国 A 股" in result["markets"]

    def test_json_serializable(self, gap_decomposition_root: Path) -> None:
        json.dumps(build_gap_decomposition_chart_data(gap_decomposition_root))


# ── heterogeneity_size ───────────────────────────────────────────────


@pytest.fixture()
def heterogeneity_root(tmp_path: Path) -> Path:
    tables = tmp_path / "results" / "real_tables"
    tables.mkdir(parents=True)
    (tables / "cma_heterogeneity_size.csv").write_text(
        "market,bucket,asymmetry_index,n_events\n"
        "CN,Q1,1.45,24\n"
        "CN,Q2,0.77,23\n"
        "CN,Q3,0.50,24\n"
        "CN,Q4,0.20,23\n"
        "CN,Q5,-0.10,24\n"
        "US,Q1,0.30,60\n"
        "US,Q2,0.10,60\n"
        "US,Q3,0.05,60\n"
        "US,Q4,0.00,60\n"
        "US,Q5,-0.05,60\n"
    )
    return tmp_path


class TestBuildHeterogeneitySizeChartData:
    def test_empty_root(self, empty_root: Path) -> None:
        result = build_heterogeneity_size_chart_data(empty_root)
        assert result == {"buckets": [], "series": []}

    def test_populated_5_buckets_2_markets(self, heterogeneity_root: Path) -> None:
        result = build_heterogeneity_size_chart_data(heterogeneity_root)
        assert result["buckets"] == ["Q1", "Q2", "Q3", "Q4", "Q5"]
        assert len(result["series"]) == 2
        for s in result["series"]:
            assert len(s["data"]) == 5
            assert len(s["n_events"]) == 5

    def test_json_serializable(self, heterogeneity_root: Path) -> None:
        json.dumps(build_heterogeneity_size_chart_data(heterogeneity_root))


# ── time_series_rolling ──────────────────────────────────────────────


@pytest.fixture()
def time_series_rolling_root(tmp_path: Path) -> Path:
    tables = tmp_path / "results" / "real_tables"
    tables.mkdir(parents=True)
    (tables / "cma_time_series_rolling.csv").write_text(
        "market,event_phase,car_mean,n_events,window_end_year\n"
        "US,announce,0.012,82,2014\n"
        "US,announce,0.010,90,2018\n"
        "US,announce,0.008,95,2022\n"
        "US,effective,0.005,82,2014\n"
        "US,effective,0.001,90,2018\n"
        "US,effective,-0.003,95,2022\n"
        "CN,announce,0.018,30,2018\n"
        "CN,announce,0.020,40,2022\n"
        "CN,effective,0.005,30,2018\n"
        "CN,effective,0.003,40,2022\n"
    )
    return tmp_path


class TestBuildTimeSeriesRollingChartData:
    def test_empty_root(self, empty_root: Path) -> None:
        result = build_time_series_rolling_chart_data(empty_root)
        assert result == {"series": [], "years": []}

    def test_populated_returns_4_market_phase_series(self, time_series_rolling_root: Path) -> None:
        result = build_time_series_rolling_chart_data(time_series_rolling_root)
        assert len(result["series"]) == 4
        assert result["years"][0] == 2014
        assert result["years"][-1] == 2022

    def test_json_serializable(self, time_series_rolling_root: Path) -> None:
        json.dumps(build_time_series_rolling_chart_data(time_series_rolling_root))
