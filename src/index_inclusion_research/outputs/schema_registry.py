from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

TIME_SERIES_EVENT_STUDY_GROUP_COLUMNS: tuple[str, ...] = (
    "market",
    "inclusion",
    "event_phase",
    "announce_year",
)

TIME_SERIES_EVENT_STUDY_VALUE_COLUMNS: tuple[str, ...] = (
    "car_m1_p1",
    "car_m3_p3",
    "car_m5_p5",
    "car_p0_p20",
    "car_p0_p120",
)


def _time_series_event_study_columns() -> tuple[str, ...]:
    columns: list[str] = [*TIME_SERIES_EVENT_STUDY_GROUP_COLUMNS, "n_events"]
    for value_column in TIME_SERIES_EVENT_STUDY_VALUE_COLUMNS:
        columns.extend(
            [
                f"mean_{value_column}",
                f"se_{value_column}",
                f"ci_low_95_{value_column}",
                f"ci_high_95_{value_column}",
            ]
        )
    return tuple(columns)


OUTPUT_TABLE_SCHEMAS: Mapping[str, tuple[str, ...]] = {
    "data_sources": (
        "数据集",
        "来源",
        "市场范围",
        "起始日期",
        "结束日期",
        "行数",
        "股票数",
        "事件数",
        "备注",
    ),
    "sample_scope": (
        "样本层",
        "市场范围",
        "事件数",
        "事件相位窗口数",
        "股票数",
        "观测值",
        "起始日期",
        "结束日期",
        "说明",
    ),
    "identification_scope": (
        "分析层",
        "市场范围",
        "样本基础",
        "主要输出",
        "证据等级",
        "证据状态",
        "当前口径",
        "来源摘要",
    ),
    "event_counts_by_year": (
        "market",
        "announce_year",
        "inclusion",
        "n_events",
        "n_tickers",
        "n_batches",
    ),
    "time_series_event_study_summary": _time_series_event_study_columns(),
    "asymmetry_summary": (
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
    ),
    "sample_filter_summary": (
        "sample_filter",
        "n_treated_events",
        "n_short_event_phase_windows",
        "n_long_event_phase_windows",
        "n_regression_comparisons",
        "n_regression_rows",
        "share_of_baseline",
        "note",
    ),
    "robustness_event_study_summary": (
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
        # Additive event-date-clustered robustness columns (NaN when the
        # optional ``pyfixest`` dependency is missing or a cell has too few
        # event-date clusters). The iid ``se_car``/``t_stat``/``p_value``
        # above stay PRIMARY and unchanged.
        "se_car_clustered",
        "p_value_clustered",
    ),
    "robustness_regression_summary": (
        "market",
        "event_phase",
        "specification",
        "dependent_variable",
        "parameter",
        "coefficient",
        "std_error",
        "t_stat",
        "p_value",
        "estimation",
        "n_obs",
        "r_squared",
        "adj_r_squared",
        "covariance",
    ),
    "robustness_retention_summary": (
        "market",
        "event_phase",
        "inclusion",
        "n_events",
        "short_window_slug",
        "long_window_slug",
        "short_mean_car",
        "long_mean_car",
        "car_reversal",
        "retention_ratio",
        "retention_ratio_valid",
        "retention_note",
        "sample_filter",
    ),
}


def output_table_columns(table_name: str) -> tuple[str, ...]:
    try:
        return OUTPUT_TABLE_SCHEMAS[table_name]
    except KeyError as exc:
        known = ", ".join(sorted(OUTPUT_TABLE_SCHEMAS))
        raise KeyError(f"Unknown output table schema {table_name!r}. Known schemas: {known}") from exc


def empty_output_table(table_name: str) -> pd.DataFrame:
    return pd.DataFrame(columns=list(output_table_columns(table_name)))
