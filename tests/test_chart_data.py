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
    build_cma_gap_length_distribution_chart_data,
    build_cma_mechanism_heatmap_chart_data,
    build_event_counts_chart_data,
    build_gap_decomposition_chart_data,
    build_heterogeneity_size_chart_data,
    build_main_regression_chart_data,
    build_mechanism_regression_chart_data,
    build_price_pressure_chart_data,
    build_rdd_scatter_chart_data,
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
            "main_regression",
            "mechanism_regression",
            "event_counts",
            "cma_mechanism_heatmap",
            "cma_gap_length_distribution",
            "rdd_scatter",
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


# ── main_regression forest plot ──────────────────────────────────────


@pytest.fixture()
def main_regression_root(tmp_path: Path) -> Path:
    tables = tmp_path / "results" / "real_tables"
    tables.mkdir(parents=True)
    (tables / "regression_coefficients.csv").write_text(
        "market,event_phase,specification,dependent_variable,parameter,coefficient,std_error,t_stat,p_value\n"
        "CN,announce,main_car,car_m1_p1,const,-0.13,0.035,-3.79,0.0001\n"
        "CN,announce,main_car,car_m1_p1,treatment_group,0.0101,0.0026,3.88,0.0001\n"
        "CN,effective,main_car,car_m1_p1,treatment_group,-0.005,0.0032,-1.58,0.114\n"
        "US,announce,main_car,car_m1_p1,treatment_group,0.013,0.0041,3.18,0.0014\n"
        "US,effective,main_car,car_m1_p1,treatment_group,-0.0007,0.0028,-0.26,0.792\n"
        "CN,announce,turnover_mechanism,turnover_change,treatment_group,-0.0002,0.0003,-0.69,0.49\n"
    )
    return tmp_path


class TestBuildMainRegressionChartData:
    def test_empty_root(self, empty_root: Path) -> None:
        result = build_main_regression_chart_data(empty_root)
        assert result == {"rows": []}

    def test_returns_only_main_car_treatment_rows(self, main_regression_root: Path) -> None:
        result = build_main_regression_chart_data(main_regression_root)
        # 4 quadrants for main_car, treatment_group only
        assert len(result["rows"]) == 4
        for r in result["rows"]:
            assert r["specification"] == "main_car"
        assert {r["market"] for r in result["rows"]} == {"CN", "US"}
        assert {r["phase"] for r in result["rows"]} == {"announce", "effective"}

    def test_ci_bounds_computed_from_se(self, main_regression_root: Path) -> None:
        result = build_main_regression_chart_data(main_regression_root)
        for r in result["rows"]:
            assert r["ci_lo"] < r["coef"] < r["ci_hi"]

    def test_json_serializable(self, main_regression_root: Path) -> None:
        json.dumps(build_main_regression_chart_data(main_regression_root))

    def test_mechanism_regression_filters_to_turnover_mechanism(
        self, main_regression_root: Path
    ) -> None:
        result = build_mechanism_regression_chart_data(main_regression_root)
        # Fixture has 1 turnover_mechanism row only (CN announce)
        assert len(result["rows"]) == 1
        assert result["rows"][0]["specification"] == "turnover_mechanism"
        assert result["rows"][0]["market"] == "CN"

    def test_mechanism_regression_empty_root_returns_empty(
        self, empty_root: Path
    ) -> None:
        assert build_mechanism_regression_chart_data(empty_root) == {"rows": []}


# ── event_counts ─────────────────────────────────────────────────────


@pytest.fixture()
def event_counts_root(tmp_path: Path) -> Path:
    tables = tmp_path / "results" / "real_tables"
    tables.mkdir(parents=True)
    (tables / "event_counts_by_year.csv").write_text(
        "market,announce_year,inclusion,n_events\n"
        "CN,2020,0,21\n"
        "CN,2020,1,21\n"
        "CN,2021,1,28\n"
        "US,2020,1,30\n"
        "US,2021,1,40\n"
        "US,2022,1,50\n"
    )
    return tmp_path


class TestBuildEventCountsChartData:
    def test_empty_root(self, empty_root: Path) -> None:
        assert build_event_counts_chart_data(empty_root) == {"series": [], "years": []}

    def test_filters_to_treated_only_and_aligns_years(
        self, event_counts_root: Path
    ) -> None:
        result = build_event_counts_chart_data(event_counts_root)
        assert result["years"] == [2020, 2021, 2022]
        # 2 markets × len(years) cells
        names = sorted(s["name"] for s in result["series"])
        assert "中国 A 股" in names and "美国" in names
        cn = next(s for s in result["series"] if s["market"] == "CN")
        # CN has events only in 2020 and 2021 → 21, 28, 0 across years
        assert cn["data"] == [21, 28, 0]
        us = next(s for s in result["series"] if s["market"] == "US")
        assert us["data"] == [30, 40, 50]

    def test_json_serializable(self, event_counts_root: Path) -> None:
        json.dumps(build_event_counts_chart_data(event_counts_root))


# ── cma_mechanism_heatmap ────────────────────────────────────────────


@pytest.fixture()
def cma_mechanism_root(tmp_path: Path) -> Path:
    tables = tmp_path / "results" / "real_tables"
    tables.mkdir(parents=True)
    rows = []
    for market in ("CN", "US"):
        for phase in ("announce", "effective"):
            for outcome in ("car_1_1", "turnover_change", "volume_change",
                             "volatility_change", "price_limit_hit_share"):
                rows.append(
                    f"{market},{phase},{outcome},no_fe,0.01,0.005,2.0,0.045,200,0.05"
                )
    (tables / "cma_mechanism_panel.csv").write_text(
        "market,event_phase,outcome,spec,coef,se,t,p_value,n_obs,r_squared\n"
        + "\n".join(rows) + "\n"
    )
    return tmp_path


class TestBuildCmaMechanismHeatmap:
    def test_empty_root(self, empty_root: Path) -> None:
        result = build_cma_mechanism_heatmap_chart_data(empty_root)
        assert result["data"] == []

    def test_populated_4x5_cells(self, cma_mechanism_root: Path) -> None:
        result = build_cma_mechanism_heatmap_chart_data(cma_mechanism_root)
        assert len(result["row_labels"]) == 4
        assert len(result["col_labels"]) == 5
        assert len(result["data"]) == 20  # 4 quadrants × 5 outcomes
        for ann in result["annotations"]:
            assert "t" in ann and "p_value" in ann and "stars" in ann
        assert result["vmax"] >= 2.0

    def test_json_serializable(self, cma_mechanism_root: Path) -> None:
        json.dumps(build_cma_mechanism_heatmap_chart_data(cma_mechanism_root))


# ── cma_gap_length_distribution ──────────────────────────────────────


@pytest.fixture()
def cma_gap_length_root(tmp_path: Path) -> Path:
    tables = tmp_path / "results" / "real_tables"
    tables.mkdir(parents=True)
    (tables / "cma_gap_event_level.csv").write_text(
        "event_id,market,gap_length_days\n"
        "cn-1,CN,14\n"
        "cn-2,CN,14\n"
        "cn-3,CN,14\n"
        "us-1,US,7\n"
        "us-2,US,7\n"
        "us-3,US,9\n"
        "us-4,US,4\n"
    )
    return tmp_path


class TestBuildCmaGapLengthDistribution:
    def test_empty_root(self, empty_root: Path) -> None:
        result = build_cma_gap_length_distribution_chart_data(empty_root)
        assert result == {"series": [], "lengths": []}

    def test_populated_distribution(self, cma_gap_length_root: Path) -> None:
        result = build_cma_gap_length_distribution_chart_data(cma_gap_length_root)
        assert result["lengths"] == [4, 7, 9, 14]
        cn = next(s for s in result["series"] if s["market"] == "CN")
        us = next(s for s in result["series"] if s["market"] == "US")
        # CN only has 14
        assert cn["data"] == [0, 0, 0, 3]
        # US has 4(1), 7(2), 9(1), 14(0)
        assert us["data"] == [1, 2, 1, 0]

    def test_json_serializable(self, cma_gap_length_root: Path) -> None:
        json.dumps(build_cma_gap_length_distribution_chart_data(cma_gap_length_root))


# ── rdd_scatter ──────────────────────────────────────────────────────


@pytest.fixture()
def rdd_scatter_root(tmp_path: Path) -> Path:
    rdd_dir = tmp_path / "results" / "literature" / "hs300_rdd"
    rdd_dir.mkdir(parents=True)
    (rdd_dir / "event_level_with_running.csv").write_text(
        "running_variable,car_m1_p1,inclusion,cutoff\n"
        "320,0.012,1,300\n"
        "350,0.018,1,300\n"
        "400,0.022,1,300\n"
        "280,0.001,0,300\n"
        "290,-0.003,0,300\n"
    )
    return tmp_path


class TestBuildRddScatterChartData:
    def test_empty_root(self, empty_root: Path) -> None:
        result = build_rdd_scatter_chart_data(empty_root)
        assert result["series"] == []
        assert result["cutoff"] is None

    def test_populated_returns_two_series(self, rdd_scatter_root: Path) -> None:
        result = build_rdd_scatter_chart_data(rdd_scatter_root)
        assert len(result["series"]) == 2
        assert result["cutoff"] == 300.0
        treated = next(s for s in result["series"] if s["inclusion"] == 1)
        control = next(s for s in result["series"] if s["inclusion"] == 0)
        assert len(treated["data"]) == 3
        assert len(control["data"]) == 2
        for x, y in treated["data"] + control["data"]:
            assert isinstance(x, float)
            assert isinstance(y, float)

    def test_json_serializable(self, rdd_scatter_root: Path) -> None:
        json.dumps(build_rdd_scatter_chart_data(rdd_scatter_root))

    def test_chart_data_matches_csv_source_of_truth(self, main_regression_root: Path) -> None:
        """Lock the chart_data builder to the regression_coefficients CSV.

        If the CSV source ever drifts from what the forest plot displays
        (different rounding, column rename, filter change), this test
        fails loudly so the PNG / forest plot don't silently disagree.
        """
        import pandas as _pd

        result = build_main_regression_chart_data(main_regression_root)
        csv_path = (
            main_regression_root
            / "results"
            / "real_tables"
            / "regression_coefficients.csv"
        )
        df = _pd.read_csv(csv_path)
        df = df.loc[
            (df["specification"] == "main_car")
            & (df["parameter"] == "treatment_group")
        ].reset_index(drop=True)
        assert len(result["rows"]) == len(df)
        for chart_row, (_, csv_row) in zip(result["rows"], df.iterrows(), strict=True):
            csv_coef = round(float(csv_row["coefficient"]), 6)
            csv_se = float(csv_row["std_error"])
            assert chart_row["coef"] == csv_coef
            assert chart_row["ci_lo"] == round(csv_coef - 1.96 * csv_se, 6)
            assert chart_row["ci_hi"] == round(csv_coef + 1.96 * csv_se, 6)
            assert chart_row["p_value"] == round(float(csv_row["p_value"]), 6)
            assert chart_row["market"] == csv_row["market"]
            assert chart_row["phase"] == csv_row["event_phase"]
