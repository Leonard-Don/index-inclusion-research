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
    build_price_pressure_chart_data,
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
        assert set(CHART_BUILDERS) == {"car_path", "price_pressure", "car_heatmap"}

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
