from __future__ import annotations

import pandas as pd

from index_inclusion_research.outputs import (
    build_data_source_table,
    build_identification_scope_table,
    build_sample_scope_table,
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
