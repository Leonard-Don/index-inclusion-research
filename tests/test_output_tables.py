from __future__ import annotations

from pathlib import Path

import pandas as pd

import index_inclusion_research.figures_tables as figures_tables
from index_inclusion_research.figures_tables import _should_save_dataframe
from index_inclusion_research.figures_tables import main as figures_tables_main
from index_inclusion_research.loaders import save_dataframe
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

EXPECTED_ROBUSTNESS_EVENT_STUDY_COLUMNS = (
    "market",
    "event_phase",
    "inclusion",
    "window",
    "window_slug",
    "sample_filter",
    "n_events",
    "mean_car",
    "std_car",
    "se_car",
    "ci_low_95",
    "ci_high_95",
    "t_stat",
    "p_value",
)

EXPECTED_ASYMMETRY_SUMMARY_COLUMNS = (
    "market",
    "event_phase",
    "n_additions",
    "n_deletions",
    "addition_car_m1_p1",
    "deletion_car_m1_p1",
    "asymmetry_car_m1_p1",
    "addition_turnover_change",
    "deletion_turnover_change",
    "addition_volume_change",
    "deletion_volume_change",
    "addition_car_p0_p120",
    "deletion_car_p0_p120",
    "asymmetry_car_p0_p120",
)

EXPECTED_EVENT_COUNTS_BY_YEAR_COLUMNS = (
    "market",
    "announce_year",
    "inclusion",
    "n_events",
    "n_tickers",
    "n_batches",
)

EXPECTED_TIME_SERIES_EVENT_STUDY_COLUMNS = {
    "market",
    "inclusion",
    "event_phase",
    "announce_year",
    "n_events",
    "mean_car_m1_p1",
    "se_car_m1_p1",
    "ci_low_95_car_m1_p1",
    "ci_high_95_car_m1_p1",
    "mean_car_m3_p3",
    "se_car_m3_p3",
    "ci_low_95_car_m3_p3",
    "ci_high_95_car_m3_p3",
    "mean_car_m5_p5",
    "se_car_m5_p5",
    "ci_low_95_car_m5_p5",
    "ci_high_95_car_m5_p5",
    "mean_car_p0_p20",
    "se_car_p0_p20",
    "ci_low_95_car_p0_p20",
    "ci_high_95_car_p0_p20",
    "mean_car_p0_p120",
    "se_car_p0_p120",
    "ci_low_95_car_p0_p120",
    "ci_high_95_car_p0_p120",
}


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


def test_build_robustness_event_study_summary_empty_input_round_trips_via_save_dataframe(
    tmp_path: Path,
) -> None:
    """Empty robustness event-study summary must round-trip through CSV.

    Why: ``figures_tables.main`` writes this helper's output via
    ``save_dataframe(robustness_event_summary, ... / 'robustness_event_study_summary.csv')``.
    Returning a bare ``pd.DataFrame()`` here causes ``to_csv`` to emit a
    single newline that ``pd.read_csv`` (used by audit and dashboard
    consumers that mirror ``event_study_summary.csv``) refuses with
    ``EmptyDataError``. Anchoring the empty path on the populated-path
    column set lets a "no events" run round-trip through the same
    downstream consumers as a populated run, mirroring the
    ``summarize_event_level_metrics`` empty-schema fix (commit ``61bc4be``).
    """
    summary = build_robustness_event_study_summary(pd.DataFrame())
    assert summary.empty
    assert list(summary.columns) == list(EXPECTED_ROBUSTNESS_EVENT_STUDY_COLUMNS), (
        "empty robustness event-study summary must expose populated schema, got "
        f"{list(summary.columns)!r}"
    )

    output_path = tmp_path / "robustness_event_study_summary.csv"
    save_dataframe(summary, output_path)
    reloaded = pd.read_csv(output_path)
    assert reloaded.empty
    assert list(reloaded.columns) == list(EXPECTED_ROBUSTNESS_EVENT_STUDY_COLUMNS)


def test_build_robustness_event_study_summary_all_controls_preserves_schema() -> None:
    """When every variant filters away all treated rows, schema still holds.

    Why: ``summarize_event_level_metrics`` filters on ``treatment_group == 1``,
    so a short_event_level frame consisting entirely of controls produces
    empty per-variant summaries. Without preserving schema in that branch,
    the concatenated robustness frame collapses to zero columns and breaks
    the same CSV consumers as the empty-input path.
    """
    short_event_level = pd.DataFrame(
        [
            {
                "event_id": "c1",
                "market": "US",
                "event_phase": "announce",
                "event_ticker": "CTRL",
                "event_date": "2024-01-01",
                "inclusion": 0,
                "treatment_group": 0,
                "car_m1_p1": 0.0,
            }
        ]
    )
    summary = build_robustness_event_study_summary(short_event_level)
    assert summary.empty
    assert list(summary.columns) == list(EXPECTED_ROBUSTNESS_EVENT_STUDY_COLUMNS), (
        "all-controls robustness event-study summary must expose populated schema, got "
        f"{list(summary.columns)!r}"
    )


def test_build_robustness_event_study_summary_populated_column_order_is_stable() -> None:
    """Populated path must keep the canonical column order the empty schema mirrors.

    Why: the empty-path schema constant duplicates the populated-path column
    order. If the populated path silently reorders, the two paths diverge and
    consumers comparing positional columns across populated and "no events"
    runs break.
    """
    short_event_level = pd.DataFrame(
        [
            {
                "event_id": "e1",
                "market": "US",
                "event_phase": "announce",
                "event_ticker": "AAA",
                "event_date": "2024-01-01",
                "inclusion": 1,
                "treatment_group": 1,
                "car_m1_p1": 0.03,
            }
        ]
    )
    summary = build_robustness_event_study_summary(short_event_level)
    assert list(summary.columns) == list(EXPECTED_ROBUSTNESS_EVENT_STUDY_COLUMNS)


def test_build_robustness_event_study_summary_long_only_input_is_not_dropped() -> None:
    """Empty short-window inputs must not suppress populated long-window summaries."""
    long_event_level = pd.DataFrame(
        [
            {
                "event_id": "e1",
                "market": "US",
                "event_phase": "announce",
                "event_ticker": "AAA",
                "event_date": "2024-01-01",
                "inclusion": 1,
                "treatment_group": 1,
                "car_p0_p120": 0.07,
            }
        ]
    )

    summary = build_robustness_event_study_summary(pd.DataFrame(), long_event_level)

    assert list(summary.columns) == list(EXPECTED_ROBUSTNESS_EVENT_STUDY_COLUMNS)
    assert not summary.empty
    assert "p0_p120" in set(summary["window_slug"])


def test_build_time_series_event_study_summary_empty_input_round_trips_via_save_dataframe(
    tmp_path: Path,
) -> None:
    """Empty time-series event-study summary must round-trip through CSV.

    Why: ``figures_tables.main`` writes this helper's output via
    ``save_dataframe(time_series_summary, ... / 'time_series_event_study_summary.csv')``.
    Returning a bare ``pd.DataFrame()`` here causes ``to_csv`` to emit a
    single newline that ``pd.read_csv`` (used by ``chart_data``,
    ``dashboard_figures``, ``dashboard_metrics``, and ``dashboard_sections``)
    refuses with ``EmptyDataError``. Anchoring the empty path on the
    populated-path column set lets a "no events" run round-trip through the
    same downstream consumers as a populated run, mirroring the
    ``summarize_event_level_metrics`` empty-schema fix (commit ``61bc4be``).
    """
    summary = build_time_series_event_study_summary(pd.DataFrame())
    assert summary.empty
    assert EXPECTED_TIME_SERIES_EVENT_STUDY_COLUMNS.issubset(summary.columns), (
        "empty time-series event-study summary must expose populated schema, got "
        f"{list(summary.columns)!r}"
    )

    output_path = tmp_path / "time_series_event_study_summary.csv"
    save_dataframe(summary, output_path)
    reloaded = pd.read_csv(output_path)
    assert reloaded.empty
    assert EXPECTED_TIME_SERIES_EVENT_STUDY_COLUMNS.issubset(reloaded.columns)


def test_build_time_series_event_study_summary_no_value_columns_preserves_schema() -> None:
    """When the input has announce_date but no CAR columns, schema still holds.

    Why: the helper drops to its no-value-column branch when none of the
    canonical ``car_*`` slugs are present in the input frame, which collapses
    the populated path to zero columns and breaks the same CSV consumers as
    the empty-input path.
    """
    event_level = pd.DataFrame(
        [
            {
                "event_id": "e1",
                "market": "CN",
                "event_phase": "announce",
                "inclusion": 1,
                "treatment_group": 1,
                "announce_date": "2024-05-31",
            }
        ]
    )
    summary = build_time_series_event_study_summary(event_level)
    assert summary.empty
    assert EXPECTED_TIME_SERIES_EVENT_STUDY_COLUMNS.issubset(summary.columns), (
        "no-value-column time-series event-study summary must expose populated schema, got "
        f"{list(summary.columns)!r}"
    )


def test_build_asymmetry_summary_empty_input_round_trips_via_save_dataframe(
    tmp_path: Path,
) -> None:
    """Empty asymmetry summary must round-trip through CSV.

    Why: ``figures_tables.main`` writes this helper's output via
    ``save_dataframe(asymmetry_summary, ... / 'asymmetry_summary.csv')``.
    Returning a bare ``pd.DataFrame()`` here causes ``to_csv`` to emit a
    single newline that ``pd.read_csv`` (used by ``dashboard_home``,
    ``dashboard_metrics``, and ``dashboard_sections``) refuses with
    ``EmptyDataError``. Anchoring the empty path on the populated-path
    column set lets a "no events" run round-trip through the same
    downstream consumers as a populated run, mirroring the
    ``build_time_series_event_study_summary`` empty-schema fix
    (commit ``d6517e6``).
    """
    summary = build_asymmetry_summary(pd.DataFrame())
    assert summary.empty
    assert list(summary.columns) == list(EXPECTED_ASYMMETRY_SUMMARY_COLUMNS), (
        "empty asymmetry summary must expose populated schema, got "
        f"{list(summary.columns)!r}"
    )

    output_path = tmp_path / "asymmetry_summary.csv"
    save_dataframe(summary, output_path)
    reloaded = pd.read_csv(output_path)
    assert reloaded.empty
    assert list(reloaded.columns) == list(EXPECTED_ASYMMETRY_SUMMARY_COLUMNS)


def test_build_asymmetry_summary_all_controls_preserves_schema() -> None:
    """When every event_level row is a control, schema still holds.

    Why: ``build_asymmetry_summary`` filters on ``treatment_group == 1``,
    so an event_level frame consisting entirely of controls produces an
    empty ``treated`` frame after filtering. Without preserving schema in
    that branch, the returned frame collapses to zero columns and breaks
    the same CSV consumers as the empty-input path.
    """
    event_level = pd.DataFrame(
        [
            {
                "event_id": "c1",
                "market": "US",
                "event_phase": "announce",
                "inclusion": 0,
                "treatment_group": 0,
                "car_m1_p1": 0.0,
            }
        ]
    )
    summary = build_asymmetry_summary(event_level)
    assert summary.empty
    assert list(summary.columns) == list(EXPECTED_ASYMMETRY_SUMMARY_COLUMNS), (
        "all-controls asymmetry summary must expose populated schema, got "
        f"{list(summary.columns)!r}"
    )


def test_build_event_counts_by_year_table_empty_input_round_trips_via_save_dataframe(
    tmp_path: Path,
) -> None:
    """Empty event-counts-by-year table must round-trip through CSV.

    Why: ``figures_tables.main`` writes this helper's output via
    ``save_dataframe(event_counts_by_year, ... / 'event_counts_by_year.csv')``.
    Returning a bare ``pd.DataFrame()`` here causes ``to_csv`` to emit a
    single newline that ``pd.read_csv`` (used by audit and dashboard
    consumers that mirror ``event_counts.csv``) refuses with
    ``EmptyDataError``. Anchoring the empty path on the populated-path
    column set lets a "no events" run round-trip through the same
    downstream consumers as a populated run, mirroring the
    ``build_asymmetry_summary`` empty-schema fix (commit ``37e47e0``).
    """
    counts = build_event_counts_by_year_table(pd.DataFrame())
    assert counts.empty
    assert list(counts.columns) == list(EXPECTED_EVENT_COUNTS_BY_YEAR_COLUMNS), (
        "empty event-counts-by-year table must expose populated schema, got "
        f"{list(counts.columns)!r}"
    )

    output_path = tmp_path / "event_counts_by_year.csv"
    save_dataframe(counts, output_path)
    reloaded = pd.read_csv(output_path)
    assert reloaded.empty
    assert list(reloaded.columns) == list(EXPECTED_EVENT_COUNTS_BY_YEAR_COLUMNS)


def test_build_event_counts_by_year_table_populated_column_order_is_stable() -> None:
    """Populated path must keep the canonical column order the empty schema mirrors.

    Why: the empty-path schema constant duplicates the populated-path column
    order. If the populated path silently reorders, the two paths diverge and
    consumers comparing positional columns across populated and "no events"
    runs break.
    """
    events = pd.DataFrame(
        [
            {
                "event_id": "e1",
                "market": "CN",
                "ticker": "000001",
                "announce_date": "2024-05-31",
                "effective_date": "2024-06-14",
                "inclusion": 1,
                "batch_id": "csi300-2024-05",
            }
        ]
    )
    counts = build_event_counts_by_year_table(events)
    assert list(counts.columns) == list(EXPECTED_EVENT_COUNTS_BY_YEAR_COLUMNS)


def test_header_only_event_counts_by_year_table_is_accepted_by_figures_tables_gate() -> None:
    """The production save gate must keep header-only event-counts-by-year artifacts.

    A header-only frame is still empty in pandas terms, so a bare
    ``if not frame.empty`` guard skips the artifact and leaves dashboard CSV
    readers without a readable ``event_counts_by_year.csv``. The generator
    gate must therefore treat schema-only frames as writeable.
    """
    counts = build_event_counts_by_year_table(pd.DataFrame())
    assert counts.empty
    assert list(counts.columns) == list(EXPECTED_EVENT_COUNTS_BY_YEAR_COLUMNS)
    assert _should_save_dataframe(counts)
    assert not _should_save_dataframe(pd.DataFrame())


def test_figures_tables_main_writes_header_only_event_counts_by_year(tmp_path: Path) -> None:
    """No-event CLI runs must still emit a readable event_counts_by_year.csv."""
    input_dir = tmp_path / "inputs"
    output_dir = tmp_path / "tables"
    figures_dir = tmp_path / "figures"
    missing_dir = tmp_path / "missing"
    input_dir.mkdir()

    events_path = input_dir / "events.csv"
    pd.DataFrame(
        columns=[
            "market",
            "index_name",
            "ticker",
            "announce_date",
            "effective_date",
        ]
    ).to_csv(events_path, index=False)

    figures_tables_main([
        "--profile",
        "sample",
        "--events",
        str(events_path),
        "--panel",
        str(missing_dir / "panel.csv"),
        "--prices",
        str(missing_dir / "prices.csv"),
        "--benchmarks",
        str(missing_dir / "benchmarks.csv"),
        "--metadata",
        str(missing_dir / "metadata.csv"),
        "--matched-panel",
        str(missing_dir / "matched_panel.csv"),
        "--average-paths",
        str(missing_dir / "average_paths.csv"),
        "--event-summary",
        str(missing_dir / "event_summary.csv"),
        "--regression-coefs",
        str(missing_dir / "regression_coefficients.csv"),
        "--regression-models",
        str(missing_dir / "regression_models.csv"),
        "--rdd-summary",
        str(missing_dir / "rdd_summary.csv"),
        "--rdd-output-dir",
        str(missing_dir / "rdd"),
        "--long-window-output-dir",
        str(missing_dir / "long"),
        "--figures-dir",
        str(figures_dir),
        "--tables-dir",
        str(output_dir),
        "--results-manifest",
        str(output_dir / "results_manifest.csv"),
    ])

    reloaded = pd.read_csv(output_dir / "event_counts_by_year.csv")
    assert reloaded.empty
    assert list(reloaded.columns) == list(EXPECTED_EVENT_COUNTS_BY_YEAR_COLUMNS)


def test_header_only_asymmetry_summary_is_saved_by_figures_tables_gate() -> None:
    """The production save gate must keep header-only asymmetry artifacts.

    A header-only asymmetry frame is still empty in pandas terms, so a bare
    ``if not frame.empty`` guard skips the artifact and leaves dashboard CSV
    readers without a readable ``asymmetry_summary.csv``. The generator gate
    must therefore treat schema-only frames as writeable.
    """
    summary = build_asymmetry_summary(pd.DataFrame())
    assert summary.empty
    assert list(summary.columns) == list(EXPECTED_ASYMMETRY_SUMMARY_COLUMNS)
    assert _should_save_dataframe(summary)
    assert not _should_save_dataframe(pd.DataFrame())


def test_figures_tables_main_writes_header_only_robustness_event_study_summary(
    tmp_path: Path,
) -> None:
    """No-event/no-panel CLI runs must still emit a readable robustness_event_study_summary.csv.

    Why: ``figures_tables.main`` writes ``build_robustness_event_study_summary``'s
    output via ``save_dataframe``. The cycle 9 helper fix (commit ``22448a2``)
    preserves a header-only schema for fully empty inputs, but a bare
    ``if not frame.empty`` save gate skips that schema and leaves audit and
    dashboard consumers without a readable ``robustness_event_study_summary.csv``.
    The production save gate must therefore route through ``_should_save_dataframe``
    so a "no events" run round-trips through the same downstream consumers as a
    populated run, mirroring the event_counts_by_year integration fix
    (commit ``af32c62``).
    """
    input_dir = tmp_path / "inputs"
    output_dir = tmp_path / "tables"
    figures_dir = tmp_path / "figures"
    missing_dir = tmp_path / "missing"
    input_dir.mkdir()

    events_path = input_dir / "events.csv"
    pd.DataFrame(
        columns=[
            "market",
            "index_name",
            "ticker",
            "announce_date",
            "effective_date",
        ]
    ).to_csv(events_path, index=False)

    figures_tables_main([
        "--profile",
        "sample",
        "--events",
        str(events_path),
        "--panel",
        str(missing_dir / "panel.csv"),
        "--prices",
        str(missing_dir / "prices.csv"),
        "--benchmarks",
        str(missing_dir / "benchmarks.csv"),
        "--metadata",
        str(missing_dir / "metadata.csv"),
        "--matched-panel",
        str(missing_dir / "matched_panel.csv"),
        "--average-paths",
        str(missing_dir / "average_paths.csv"),
        "--event-summary",
        str(missing_dir / "event_summary.csv"),
        "--regression-coefs",
        str(missing_dir / "regression_coefficients.csv"),
        "--regression-models",
        str(missing_dir / "regression_models.csv"),
        "--rdd-summary",
        str(missing_dir / "rdd_summary.csv"),
        "--rdd-output-dir",
        str(missing_dir / "rdd"),
        "--long-window-output-dir",
        str(missing_dir / "long"),
        "--figures-dir",
        str(figures_dir),
        "--tables-dir",
        str(output_dir),
        "--results-manifest",
        str(output_dir / "results_manifest.csv"),
    ])

    reloaded = pd.read_csv(output_dir / "robustness_event_study_summary.csv")
    assert reloaded.empty
    assert list(reloaded.columns) == list(EXPECTED_ROBUSTNESS_EVENT_STUDY_COLUMNS)
    assert not (output_dir / "time_series_event_study_summary.csv").exists()


def test_header_only_time_series_summary_is_saved_by_figures_tables_gate() -> None:
    """The production save gate must keep header-only time-series artifacts."""
    summary = build_time_series_event_study_summary(pd.DataFrame())
    assert summary.empty
    assert EXPECTED_TIME_SERIES_EVENT_STUDY_COLUMNS.issubset(summary.columns)
    assert _should_save_dataframe(summary)
    assert not _should_save_dataframe(pd.DataFrame())


def test_figures_tables_main_writes_header_only_time_series_event_study_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Panel-path CLI runs must emit readable header-only time-series summaries."""
    input_dir = tmp_path / "inputs"
    output_dir = tmp_path / "tables"
    figures_dir = tmp_path / "figures"
    missing_dir = tmp_path / "missing"
    input_dir.mkdir()

    events_path = input_dir / "events.csv"
    pd.DataFrame(
        [
            {
                "event_id": "e1",
                "market": "CN",
                "index_name": "沪深300",
                "ticker": "000001",
                "announce_date": "2024-05-31",
                "effective_date": "2024-06-14",
            }
        ]
    ).to_csv(events_path, index=False)

    panel_path = input_dir / "panel.csv"
    pd.DataFrame(
        [
            {
                "event_id": "e1",
                "market": "CN",
                "event_phase": "announce",
                "inclusion": 1,
                "relative_day": 0,
                "turnover": 0.0,
                "volume": 0.0,
                "event_date_raw": "2024-05-31",
                "mapped_market_date": "2024-05-31",
                "event_date": "2024-05-31",
                "date": "2024-05-31",
            }
        ]
    ).to_csv(panel_path, index=False)

    def _fake_compute_event_study(panel: pd.DataFrame, windows: list[tuple[int, int]]):
        del panel, windows
        return (
            pd.DataFrame(
                columns=[
                    "event_id",
                    "market",
                    "event_phase",
                    "inclusion",
                    "treatment_group",
                    "announce_date",
                ]
            ),
            pd.DataFrame(),
            pd.DataFrame(),
        )

    monkeypatch.setattr(figures_tables, "compute_event_study", _fake_compute_event_study)

    figures_tables.main([
        "--profile",
        "sample",
        "--events",
        str(events_path),
        "--panel",
        str(panel_path),
        "--prices",
        str(missing_dir / "prices.csv"),
        "--benchmarks",
        str(missing_dir / "benchmarks.csv"),
        "--metadata",
        str(missing_dir / "metadata.csv"),
        "--matched-panel",
        str(missing_dir / "matched_panel.csv"),
        "--average-paths",
        str(missing_dir / "average_paths.csv"),
        "--event-summary",
        str(missing_dir / "event_summary.csv"),
        "--regression-coefs",
        str(missing_dir / "regression_coefficients.csv"),
        "--regression-models",
        str(missing_dir / "regression_models.csv"),
        "--rdd-summary",
        str(missing_dir / "rdd_summary.csv"),
        "--rdd-output-dir",
        str(missing_dir / "rdd"),
        "--long-window-output-dir",
        str(missing_dir / "long"),
        "--figures-dir",
        str(figures_dir),
        "--tables-dir",
        str(output_dir),
        "--results-manifest",
        str(output_dir / "results_manifest.csv"),
    ])

    reloaded = pd.read_csv(output_dir / "time_series_event_study_summary.csv")
    assert reloaded.empty
    assert EXPECTED_TIME_SERIES_EVENT_STUDY_COLUMNS.issubset(reloaded.columns)
