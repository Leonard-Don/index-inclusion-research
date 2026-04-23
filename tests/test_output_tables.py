from __future__ import annotations

import pandas as pd

from index_inclusion_research.outputs import (
    build_asymmetry_summary,
    build_data_source_table,
    build_event_counts_by_year_table,
    build_identification_scope_table,
    build_robustness_event_study_summary,
    build_robustness_regression_summary,
    build_robustness_retention_summary,
    build_sample_filter_summary,
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
    assert rdd_row["证据等级"] == "L1"
    assert rdd_row["证据状态"] == "方法展示"
    assert "不应与正式实证结果混用" in rdd_row["当前口径"]
    assert rdd_row["来源摘要"] == "demo 伪排名样本"


def test_build_identification_scope_marks_missing_rdd_as_pending_formal_input() -> None:
    events = pd.DataFrame([{"market": "CN"}, {"market": "US"}])
    panel = pd.DataFrame(
        [
            {"event_id": "e1", "event_phase": "announce"},
            {"event_id": "e1", "event_phase": "effective"},
        ]
    )

    scope = build_identification_scope_table(events, panel, rdd_summary=pd.DataFrame(), rdd_mode="missing")
    rdd_row = scope.loc[scope["分析层"] == "中国 RDD 扩展"].iloc[0]
    assert rdd_row["证据等级"] == "L0"
    assert rdd_row["证据状态"] == "待补正式样本"
    assert "hs300_rdd_candidates.csv" in rdd_row["当前口径"]
    assert rdd_row["来源摘要"] == "待补候选样本"


def test_build_identification_scope_marks_reconstructed_rdd_as_public_proxy() -> None:
    events = pd.DataFrame([{"market": "CN"}, {"market": "US"}])
    panel = pd.DataFrame(
        [
            {"event_id": "e1", "event_phase": "announce"},
            {"event_id": "e1", "event_phase": "effective"},
        ]
    )
    rdd_summary = pd.DataFrame([{"n_obs": 311}])

    scope = build_identification_scope_table(events, panel, rdd_summary=rdd_summary, rdd_mode="reconstructed")
    rdd_row = scope.loc[scope["分析层"] == "中国 RDD 扩展"].iloc[0]
    assert rdd_row["证据等级"] == "L2"
    assert rdd_row["证据状态"] == "公开重建样本"
    assert "官方历史候选排名表" in rdd_row["当前口径"]
    assert rdd_row["来源摘要"] == "公开重建候选样本文件"


def test_build_identification_scope_prefers_structured_rdd_status_when_provided() -> None:
    events = pd.DataFrame([{"market": "CN"}, {"market": "US"}])
    panel = pd.DataFrame(
        [
            {"event_id": "e1", "event_phase": "announce"},
            {"event_id": "e1", "event_phase": "effective"},
        ]
    )
    rdd_summary = pd.DataFrame([{"n_obs": 311}])

    scope = build_identification_scope_table(
        events,
        panel,
        rdd_summary=rdd_summary,
        rdd_status={
            "mode": "reconstructed",
            "evidence_tier": "L2",
            "evidence_status": "公开重建样本",
            "source_label": "公开重建候选样本文件",
            "note": "基于公开数据重建的边界样本，可进入公开数据版证据链，但不应表述为中证官方历史候选排名表。",
            "message": "当前正在使用公开数据重建的候选样本文件。",
        },
    )
    rdd_row = scope.loc[scope["分析层"] == "中国 RDD 扩展"].iloc[0]
    assert rdd_row["证据等级"] == "L2"
    assert rdd_row["证据状态"] == "公开重建样本"
    assert rdd_row["来源摘要"] == "公开重建候选样本文件"


def test_build_identification_scope_populates_source_summary_for_all_rows() -> None:
    events = pd.DataFrame([{"market": "CN"}, {"market": "US"}])
    panel = pd.DataFrame(
        [
            {"event_id": "e1", "event_phase": "announce"},
            {"event_id": "e1", "event_phase": "effective"},
        ]
    )
    matched_panel = pd.DataFrame([{"event_id": "e1"}])
    rdd_summary = pd.DataFrame([{"n_obs": 311}])

    scope = build_identification_scope_table(
        events,
        panel,
        matched_panel=matched_panel,
        rdd_summary=rdd_summary,
        rdd_mode="reconstructed",
    )

    assert scope["来源摘要"].notna().all()
    assert scope.loc[scope["分析层"] == "短窗口事件研究", "来源摘要"].iloc[0] == "正式事件样本 + 短窗口事件面板"
    assert scope.loc[scope["分析层"] == "长窗口保留分析", "来源摘要"].iloc[0] == "正式事件样本 + 长窗口保留面板"
    assert scope.loc[scope["分析层"] == "匹配对照组回归", "来源摘要"].iloc[0] == "正式事件样本 + 匹配对照面板"


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
    assert {"market", "inclusion", "event_phase", "announce_year", "mean_car_m1_p1", "se_car_m1_p1", "ci_low_95_car_m1_p1", "ci_high_95_car_m1_p1"}.issubset(time_series.columns)
    assert {"market", "event_phase", "addition_car_m1_p1", "deletion_car_m1_p1", "asymmetry_car_m1_p1"}.issubset(asymmetry.columns)


def test_robustness_output_tables_are_built_with_expected_columns() -> None:
    short_event_level = pd.DataFrame(
        [
            {"event_id": "e1", "market": "US", "event_phase": "announce", "event_ticker": "AAA", "event_date": "2024-01-01", "inclusion": 1, "treatment_group": 1, "car_m1_p1": 0.03, "car_m3_p3": 0.04, "car_m5_p5": 0.05},
            {"event_id": "e2", "market": "US", "event_phase": "announce", "event_ticker": "AAA", "event_date": "2024-03-01", "inclusion": 1, "treatment_group": 1, "car_m1_p1": 0.02, "car_m3_p3": 0.03, "car_m5_p5": 0.04},
            {"event_id": "e3", "market": "US", "event_phase": "announce", "event_ticker": "BBB", "event_date": "2024-12-01", "inclusion": 1, "treatment_group": 1, "car_m1_p1": 0.01, "car_m3_p3": 0.02, "car_m5_p5": 0.03},
            {"event_id": "e4", "market": "CN", "event_phase": "effective", "event_ticker": "000001", "event_date": "2024-05-01", "inclusion": 1, "treatment_group": 1, "car_m1_p1": 0.005, "car_m3_p3": 0.006, "car_m5_p5": 0.007},
            {"event_id": "e5", "market": "CN", "event_phase": "effective", "event_ticker": "000002", "event_date": "2024-10-01", "inclusion": 1, "treatment_group": 1, "car_m1_p1": 0.004, "car_m3_p3": 0.005, "car_m5_p5": 0.006},
        ]
    )
    long_event_level = pd.DataFrame(
        [
            {"event_id": "e1", "market": "US", "event_phase": "announce", "event_ticker": "AAA", "event_date": "2024-01-01", "inclusion": 1, "treatment_group": 1, "car_p0_p20": 0.03, "car_p0_p120": 0.05},
            {"event_id": "e2", "market": "US", "event_phase": "announce", "event_ticker": "AAA", "event_date": "2024-03-01", "inclusion": 1, "treatment_group": 1, "car_p0_p20": 0.02, "car_p0_p120": 0.03},
            {"event_id": "e3", "market": "US", "event_phase": "announce", "event_ticker": "BBB", "event_date": "2024-12-01", "inclusion": 1, "treatment_group": 1, "car_p0_p20": 0.01, "car_p0_p120": 0.02},
            {"event_id": "e4", "market": "CN", "event_phase": "effective", "event_ticker": "000001", "event_date": "2024-05-01", "inclusion": 1, "treatment_group": 1, "car_p0_p20": 0.003, "car_p0_p120": 0.02},
            {"event_id": "e5", "market": "CN", "event_phase": "effective", "event_ticker": "000002", "event_date": "2024-10-01", "inclusion": 1, "treatment_group": 1, "car_p0_p20": 0.004, "car_p0_p120": 0.01},
        ]
    )
    regression_dataset = pd.DataFrame(
        [
            {"event_id": "e1", "comparison_id": "e1", "market": "US", "event_phase": "announce", "event_ticker": "AAA", "event_date": "2024-01-01", "treatment_group": 1, "car_m1_p1": 0.03, "log_mkt_cap": 10.0, "pre_event_return": 0.01},
            {"event_id": "c1", "comparison_id": "e1", "market": "US", "event_phase": "announce", "event_ticker": "CCC", "event_date": "2024-01-01", "treatment_group": 0, "car_m1_p1": 0.01, "log_mkt_cap": 10.1, "pre_event_return": 0.00},
            {"event_id": "e2", "comparison_id": "e2", "market": "US", "event_phase": "announce", "event_ticker": "AAA", "event_date": "2024-03-01", "treatment_group": 1, "car_m1_p1": 0.02, "log_mkt_cap": 10.2, "pre_event_return": 0.02},
            {"event_id": "c2", "comparison_id": "e2", "market": "US", "event_phase": "announce", "event_ticker": "DDD", "event_date": "2024-03-01", "treatment_group": 0, "car_m1_p1": 0.00, "log_mkt_cap": 10.3, "pre_event_return": 0.01},
            {"event_id": "e3", "comparison_id": "e3", "market": "US", "event_phase": "announce", "event_ticker": "BBB", "event_date": "2024-12-01", "treatment_group": 1, "car_m1_p1": 0.01, "log_mkt_cap": 10.4, "pre_event_return": 0.00},
            {"event_id": "c3", "comparison_id": "e3", "market": "US", "event_phase": "announce", "event_ticker": "EEE", "event_date": "2024-12-01", "treatment_group": 0, "car_m1_p1": -0.01, "log_mkt_cap": 10.5, "pre_event_return": -0.01},
            {"event_id": "e4", "comparison_id": "e4", "market": "CN", "event_phase": "effective", "event_ticker": "000001", "event_date": "2024-05-01", "treatment_group": 1, "car_m1_p1": 0.005, "log_mkt_cap": 9.5, "pre_event_return": 0.01},
            {"event_id": "c4", "comparison_id": "e4", "market": "CN", "event_phase": "effective", "event_ticker": "000003", "event_date": "2024-05-01", "treatment_group": 0, "car_m1_p1": -0.002, "log_mkt_cap": 9.6, "pre_event_return": 0.00},
            {"event_id": "e5", "comparison_id": "e5", "market": "CN", "event_phase": "effective", "event_ticker": "000002", "event_date": "2024-10-01", "treatment_group": 1, "car_m1_p1": 0.004, "log_mkt_cap": 9.7, "pre_event_return": 0.02},
            {"event_id": "c5", "comparison_id": "e5", "market": "CN", "event_phase": "effective", "event_ticker": "000004", "event_date": "2024-10-01", "treatment_group": 0, "car_m1_p1": -0.001, "log_mkt_cap": 9.8, "pre_event_return": 0.01},
        ]
    )

    sample_filters = build_sample_filter_summary(short_event_level, long_event_level=long_event_level, regression_dataset=regression_dataset)
    robustness_events = build_robustness_event_study_summary(short_event_level, long_event_level=long_event_level)
    robustness_regressions = build_robustness_regression_summary(regression_dataset)
    robustness_retention = build_robustness_retention_summary(long_event_level)

    assert {"sample_filter", "n_treated_events", "share_of_baseline"}.issubset(sample_filters.columns)
    assert {"sample_filter", "market", "window", "se_car", "ci_low_95", "ci_high_95"}.issubset(robustness_events.columns)
    assert {"estimation", "covariance", "market", "coefficient", "std_error", "n_obs"}.issubset(robustness_regressions.columns)
    assert {"sample_filter", "retention_ratio_valid", "retention_note"}.issubset(robustness_retention.columns)
