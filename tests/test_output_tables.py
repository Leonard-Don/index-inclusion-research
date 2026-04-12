from __future__ import annotations

import pandas as pd

from index_inclusion_research.outputs import (
    build_asymmetry_summary,
    build_data_source_table,
    build_event_counts_by_year_table,
    build_identification_scope_table,
    build_sample_scope_table,
    build_time_series_event_study_summary,
)


def test_build_data_source_table_summarises_core_inputs() -> None:
    events = pd.DataFrame(
        [
            {
                "market": "CN",
                "ticker": "000001",
                "announce_date": "2024-01-01",
                "effective_date": "2024-01-15",
                "source": "CSIndex sample adjustment attachment dated 2024-01-01",
                "source_url": "https://www.csindex.com.cn/",
            },
            {
                "market": "US",
                "ticker": "A",
                "announce_date": "2024-02-01",
                "effective_date": "2024-02-15",
                "source": "Wikipedia S&P 500 changes table with S&P Dow Jones citation dates",
                "source_url": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            },
        ]
    )
    prices = pd.DataFrame(
        [
            {"market": "CN", "ticker": "000001", "date": "2024-01-02"},
            {"market": "US", "ticker": "A", "date": "2024-02-02"},
        ]
    )
    benchmarks = pd.DataFrame(
        [
            {"market": "CN", "date": "2024-01-02"},
            {"market": "US", "date": "2024-02-02"},
        ]
    )

    summary = build_data_source_table(events, prices=prices, benchmarks=benchmarks)
    assert set(summary["数据集"]) >= {"事件样本", "日频价格", "基准收益"}
    event_row = summary.loc[summary["数据集"] == "事件样本"].iloc[0]
    assert event_row["市场范围"] == "中国 A 股 + 美国"
    assert event_row["事件数"] == 2


def test_build_sample_scope_table_includes_long_window_layer() -> None:
    events = pd.DataFrame(
        [
            {"market": "CN", "ticker": "000001", "announce_date": "2024-01-01", "effective_date": "2024-01-15"},
        ]
    )
    panel = pd.DataFrame(
        [
            {
                "event_id": "e1",
                "event_phase": "announce",
                "market": "CN",
                "event_ticker": "000001",
                "date": "2024-01-02",
            },
            {
                "event_id": "e1",
                "event_phase": "effective",
                "market": "CN",
                "event_ticker": "000001",
                "date": "2024-01-15",
            },
        ]
    )
    long_event_level = pd.DataFrame(
        [
            {"event_id": "e1", "event_phase": "announce", "market": "CN", "event_ticker": "000001", "event_date": "2024-01-01"},
        ]
    )

    scope = build_sample_scope_table(events, panel, long_event_level=long_event_level)
    assert "长窗口保留分析" in scope["样本层"].tolist()


def test_build_identification_scope_marks_demo_rdd_as_method_only() -> None:
    events = pd.DataFrame([{"market": "CN"}, {"market": "US"}])
    panel = pd.DataFrame(
        [
            {"event_id": "e1", "event_phase": "announce"},
            {"event_id": "e1", "event_phase": "effective"},
        ]
    )
    rdd_summary = pd.DataFrame([{"n_obs": 80}])

    scope = build_identification_scope_table(events, panel, rdd_summary=rdd_summary, rdd_mode="demo")
    rdd_row = scope.loc[scope["分析层"] == "中国 RDD 扩展"].iloc[0]
    assert rdd_row["证据状态"] == "方法展示"
    assert "不应与正式实证结果混用" in rdd_row["当前口径"]


def test_extended_output_tables_are_built_with_expected_columns() -> None:
    events = pd.DataFrame(
        [
            {"event_id": "e1", "market": "CN", "ticker": "000001", "announce_date": "2024-05-31", "effective_date": "2024-06-14", "inclusion": 1, "batch_id": "csi300-2024-05"},
            {"event_id": "e2", "market": "CN", "ticker": "000002", "announce_date": "2024-05-31", "effective_date": "2024-06-14", "inclusion": 0, "batch_id": "csi300-2024-05"},
            {"event_id": "e3", "market": "US", "ticker": "ABC", "announce_date": "2025-01-10", "effective_date": "2025-01-24", "inclusion": 1, "batch_id": "sp500-20250124"},
        ]
    )
    event_level = pd.DataFrame(
        [
            {"event_id": "e1", "market": "CN", "event_phase": "announce", "inclusion": 1, "treatment_group": 1, "announce_date": "2024-05-31", "car_m1_p1": 0.03, "car_m3_p3": 0.04, "car_m5_p5": 0.05, "turnover_change": 0.01, "volume_change": 0.02},
            {"event_id": "e2", "market": "CN", "event_phase": "announce", "inclusion": 0, "treatment_group": 1, "announce_date": "2024-05-31", "car_m1_p1": -0.01, "car_m3_p3": -0.02, "car_m5_p5": -0.03, "turnover_change": -0.01, "volume_change": -0.02},
            {"event_id": "e3", "market": "US", "event_phase": "effective", "inclusion": 1, "treatment_group": 1, "announce_date": "2025-01-10", "car_m1_p1": 0.02, "car_m3_p3": 0.01, "car_m5_p5": 0.00, "turnover_change": 0.03, "volume_change": 0.01},
        ]
    )
    long_event_level = pd.DataFrame(
        [
            {"event_id": "e1", "market": "CN", "event_phase": "announce", "inclusion": 1, "treatment_group": 1, "car_p0_p120": 0.08},
            {"event_id": "e2", "market": "CN", "event_phase": "announce", "inclusion": 0, "treatment_group": 1, "car_p0_p120": -0.02},
        ]
    )

    counts = build_event_counts_by_year_table(events)
    time_series = build_time_series_event_study_summary(event_level)
    asymmetry = build_asymmetry_summary(event_level, long_event_level=long_event_level)

    assert {"market", "announce_year", "inclusion", "n_events"}.issubset(counts.columns)
    assert {"market", "inclusion", "event_phase", "announce_year", "mean_car_m1_p1"}.issubset(time_series.columns)
    assert {"market", "event_phase", "addition_car_m1_p1", "deletion_car_m1_p1", "asymmetry_car_m1_p1"}.issubset(asymmetry.columns)
