from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from index_inclusion_research.analysis import (
    filter_nonoverlap_event_windows,
    run_regressions,
    summarize_event_level_metrics,
    winsorize_event_level_metrics,
)
from index_inclusion_research.literature import compute_retention_summary

plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

MARKET_LABELS = {
    "CN": "中国 A 股",
    "US": "美国",
}

MARKET_COLORS = {
    "CN": "#a63b28",
    "US": "#0f5c6e",
}

PHASE_LABELS = {
    "announce": "公告日",
    "effective": "生效日",
}

PHASE_LINESTYLES = {
    "announce": "-",
    "effective": "--",
}

INCLUSION_LABELS = {
    1: "调入样本",
    0: "调出样本",
}

INCLUSION_STYLES = {
    1: {"alpha": 1.0, "marker": "o", "linewidth": 2.4},
    0: {"alpha": 0.55, "marker": "s", "linewidth": 1.8},
}


def _lighten(color: str, factor: float = 0.45) -> tuple[float, float, float]:
    import matplotlib.colors as mcolors

    base = mcolors.to_rgb(color)
    return tuple(channel + (1 - channel) * factor for channel in base)


def _ensure_directory(path: str | Path) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target


def _market_scope_from_values(values: pd.Series | list[str]) -> str:
    labels = [MARKET_LABELS.get(str(value), str(value)) for value in pd.Series(values).dropna().astype(str).unique()]
    return " + ".join(sorted(labels)) if labels else "NA"


def _date_range_from_frame(frame: pd.DataFrame, date_columns: list[str]) -> tuple[object, object]:
    available = [column for column in date_columns if column in frame.columns]
    if frame.empty or not available:
        return pd.NA, pd.NA
    dates = pd.concat([pd.to_datetime(frame[column], errors="coerce") for column in available], ignore_index=True).dropna()
    if dates.empty:
        return pd.NA, pd.NA
    return dates.min().date().isoformat(), dates.max().date().isoformat()


def _safe_int(value: object) -> object:
    if value is None or pd.isna(value):
        return pd.NA
    return int(value)


def _display_value(value: object) -> str:
    if value is None or pd.isna(value):
        return "NA"
    return str(value)


def _summarise_event_sources(events: pd.DataFrame) -> str:
    if events.empty:
        return "NA"
    source_text = " ".join(events.get("source", pd.Series(dtype=str)).dropna().astype(str).tolist())
    sources: list[str] = []
    if "CSIndex" in source_text or "中证" in source_text:
        sources.append("中证指数官网调整公告附件")
    if "Wikipedia" in source_text or "wikipedia.org" in " ".join(events.get("source_url", pd.Series(dtype=str)).dropna().astype(str).tolist()):
        sources.append("Wikipedia 标普500变更表（含 S&P Dow Jones 引用日期）")
    if not sources:
        unique_sources = events.get("source", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()
        if len(unique_sources) > 3:
            return "；".join(unique_sources[:3]) + "等"
        return "；".join(unique_sources) if unique_sources else "NA"
    return "；".join(sources)


def build_data_source_table(
    events: pd.DataFrame,
    prices: pd.DataFrame = pd.DataFrame(),
    benchmarks: pd.DataFrame = pd.DataFrame(),
    metadata: pd.DataFrame = pd.DataFrame(),
    panel: pd.DataFrame = pd.DataFrame(),
    matched_panel: pd.DataFrame = pd.DataFrame(),
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    if not events.empty:
        event_start, event_end = _date_range_from_frame(events, ["announce_date", "effective_date"])
        rows.append(
            {
                "数据集": "事件样本",
                "来源": _summarise_event_sources(events),
                "市场范围": _market_scope_from_values(events["market"]),
                "起始日期": event_start,
                "结束日期": event_end,
                "行数": _safe_int(len(events)),
                "股票数": _safe_int(events["ticker"].nunique()) if "ticker" in events.columns else pd.NA,
                "事件数": _safe_int(len(events)),
                "备注": "A 股样本为沪深300调入/调出事件，美股样本为 S&P 500 加入/剔除事件。",
            }
        )

    if not prices.empty:
        price_start, price_end = _date_range_from_frame(prices, ["date"])
        rows.append(
            {
                "数据集": "日频价格",
                "来源": "Yahoo Finance（经 yfinance 抓取）",
                "市场范围": _market_scope_from_values(prices["market"]),
                "起始日期": price_start,
                "结束日期": price_end,
                "行数": _safe_int(len(prices)),
                "股票数": _safe_int(prices["ticker"].nunique()) if "ticker" in prices.columns else pd.NA,
                "事件数": pd.NA,
                "备注": "包含事件股票与匹配对照组候选股票的日度价格、成交量、换手率与市值近似值。",
            }
        )

    if not benchmarks.empty:
        benchmark_start, benchmark_end = _date_range_from_frame(benchmarks, ["date"])
        rows.append(
            {
                "数据集": "基准收益",
                "来源": "Yahoo Finance（经 yfinance 抓取）",
                "市场范围": _market_scope_from_values(benchmarks["market"]),
                "起始日期": benchmark_start,
                "结束日期": benchmark_end,
                "行数": _safe_int(len(benchmarks)),
                "股票数": pd.NA,
                "事件数": pd.NA,
                "备注": "美国使用 S&P 500 指数收益，中国使用沪深300指数收益。",
            }
        )

    if not metadata.empty:
        rows.append(
            {
                "数据集": "证券元数据",
                "来源": "Yahoo Finance（sharesOutstanding / sector 近似口径）",
                "市场范围": _market_scope_from_values(metadata["market"]) if "market" in metadata.columns else "NA",
                "起始日期": pd.NA,
                "结束日期": pd.NA,
                "行数": _safe_int(len(metadata)),
                "股票数": _safe_int(metadata["ticker"].nunique()) if "ticker" in metadata.columns else pd.NA,
                "事件数": pd.NA,
                "备注": "用于构造市值与换手率近似值，更适合课程论文与机制分析。",
            }
        )

    if not panel.empty:
        panel_start, panel_end = _date_range_from_frame(panel, ["date"])
        rows.append(
            {
                "数据集": "事件窗口面板",
                "来源": "由真实事件样本、日频价格与基准收益拼接生成",
                "市场范围": _market_scope_from_values(panel["market"]),
                "起始日期": panel_start,
                "结束日期": panel_end,
                "行数": _safe_int(len(panel)),
                "股票数": _safe_int(panel["event_ticker"].nunique()) if "event_ticker" in panel.columns else pd.NA,
                "事件数": _safe_int(panel["event_id"].nunique()) if "event_id" in panel.columns else pd.NA,
                "备注": "用于事件研究、长窗口保留分析与平均路径图。",
            }
        )

    if not matched_panel.empty:
        matched_start, matched_end = _date_range_from_frame(matched_panel, ["date"])
        rows.append(
            {
                "数据集": "匹配回归面板",
                "来源": "由匹配后的真实事件样本、对照组与基准收益拼接生成",
                "市场范围": _market_scope_from_values(matched_panel["market"]),
                "起始日期": matched_start,
                "结束日期": matched_end,
                "行数": _safe_int(len(matched_panel)),
                "股票数": _safe_int(matched_panel["event_ticker"].nunique()) if "event_ticker" in matched_panel.columns else pd.NA,
                "事件数": _safe_int(matched_panel["matched_to_event_id"].fillna(matched_panel["event_id"]).nunique())
                if {"matched_to_event_id", "event_id"}.intersection(matched_panel.columns)
                else pd.NA,
                "备注": "用于匹配对照组回归、机制回归与匹配诊断。",
            }
        )

    return pd.DataFrame(rows)


def build_sample_scope_table(
    events: pd.DataFrame,
    panel: pd.DataFrame,
    matched_panel: pd.DataFrame = pd.DataFrame(),
    long_panel: pd.DataFrame = pd.DataFrame(),
    long_event_level: pd.DataFrame = pd.DataFrame(),
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if not events.empty:
        event_start, event_end = _date_range_from_frame(events, ["announce_date", "effective_date"])
        rows.append(
            {
                "样本层": "事件样本",
                "市场范围": _market_scope_from_values(events["market"]),
                "事件数": _safe_int(len(events)),
                "事件相位窗口数": _safe_int(len(events) * 2),
                "股票数": _safe_int(events["ticker"].nunique()) if "ticker" in events.columns else pd.NA,
                "观测值": pd.NA,
                "起始日期": event_start,
                "结束日期": event_end,
                "说明": "每个真实事件同时对应公告日与生效日两个事件时点。",
            }
        )
    if not panel.empty:
        panel_start, panel_end = _date_range_from_frame(panel, ["date"])
        rows.append(
            {
                "样本层": "事件研究面板",
                "市场范围": _market_scope_from_values(panel["market"]),
                "事件数": _safe_int(panel["event_id"].nunique()) if "event_id" in panel.columns else pd.NA,
                "事件相位窗口数": _safe_int(panel[["event_id", "event_phase"]].drop_duplicates().shape[0])
                if {"event_id", "event_phase"}.issubset(panel.columns)
                else pd.NA,
                "股票数": _safe_int(panel["event_ticker"].nunique()) if "event_ticker" in panel.columns else pd.NA,
                "观测值": _safe_int(len(panel)),
                "起始日期": panel_start,
                "结束日期": panel_end,
                "说明": "窗口默认覆盖相对交易日 [-20,+20]，用于短窗口事件研究与平均路径图。",
            }
        )
    if not matched_panel.empty:
        matched_start, matched_end = _date_range_from_frame(matched_panel, ["date"])
        comparison_count = (
            matched_panel["matched_to_event_id"].fillna(matched_panel["event_id"]).nunique()
            if {"matched_to_event_id", "event_id"}.intersection(matched_panel.columns)
            else pd.NA
        )
        rows.append(
            {
                "样本层": "匹配回归面板",
                "市场范围": _market_scope_from_values(matched_panel["market"]),
                "事件数": _safe_int(comparison_count),
                "事件相位窗口数": _safe_int(matched_panel[["event_id", "event_phase"]].drop_duplicates().shape[0])
                if {"event_id", "event_phase"}.issubset(matched_panel.columns)
                else pd.NA,
                "股票数": _safe_int(matched_panel["ticker"].nunique())
                if "ticker" in matched_panel.columns
                else (_safe_int(matched_panel["event_id"].nunique()) if "event_id" in matched_panel.columns else pd.NA),
                "观测值": _safe_int(len(matched_panel)),
                "起始日期": matched_start,
                "结束日期": matched_end,
                "说明": "每个真实事件默认匹配 3 个对照股票，用于主回归与机制回归。",
            }
        )
    if not long_event_level.empty or not long_panel.empty:
        long_source = long_panel if not long_panel.empty else long_event_level
        long_start, long_end = _date_range_from_frame(long_source, ["date", "event_date"])
        rows.append(
            {
                "样本层": "长窗口保留分析",
                "市场范围": _market_scope_from_values(long_source["market"]),
                "事件数": _safe_int(long_event_level["event_id"].nunique()) if "event_id" in long_event_level.columns else pd.NA,
                "事件相位窗口数": _safe_int(long_source[["event_id", "event_phase"]].drop_duplicates().shape[0])
                if {"event_id", "event_phase"}.issubset(long_source.columns)
                else pd.NA,
                "股票数": _safe_int(long_source["event_ticker"].nunique()) if "event_ticker" in long_source.columns else pd.NA,
                "观测值": _safe_int(len(long_source)),
                "起始日期": long_start,
                "结束日期": long_end,
                "说明": "在同一真实事件面板上计算 [0,+5]、[0,+20]、[0,+60] 与 [0,+120] 的 CAR。",
            }
        )
    return pd.DataFrame(rows)


def build_identification_scope_table(
    events: pd.DataFrame,
    panel: pd.DataFrame,
    matched_panel: pd.DataFrame = pd.DataFrame(),
    rdd_summary: pd.DataFrame = pd.DataFrame(),
    rdd_mode: str = "unavailable",
) -> pd.DataFrame:
    event_count = _safe_int(len(events)) if not events.empty else pd.NA
    panel_windows = (
        _safe_int(panel[["event_id", "event_phase"]].drop_duplicates().shape[0])
        if not panel.empty and {"event_id", "event_phase"}.issubset(panel.columns)
        else pd.NA
    )
    matched_rows = _safe_int(len(matched_panel)) if not matched_panel.empty else pd.NA
    rdd_n_obs = _safe_int(rdd_summary["n_obs"].max()) if not rdd_summary.empty and "n_obs" in rdd_summary.columns else pd.NA

    rdd_status = "待补正式样本"
    rdd_note = "尚未提供有效的 hs300_rdd_candidates.csv，当前中国主线的正式证据仍以事件研究与匹配回归为主。"
    if rdd_mode == "real":
        rdd_status = "正式边界样本"
        rdd_note = "基于真实候选排名变量，可作为更强识别证据。"
    if rdd_mode == "demo":
        rdd_status = "方法展示"
        rdd_note = "当前使用 demo 伪排名变量，展示的是断点回归方法框架，不应与正式实证结果混用。"
    elif rdd_mode == "unavailable":
        rdd_status = "未生成"
        rdd_note = "尚未生成 RDD 扩展结果。"

    rows = [
        {
            "分析层": "短窗口事件研究",
            "市场范围": "中国 A 股 + 美国",
            "样本基础": f"{_display_value(event_count)} 个真实调入/调出事件、{_display_value(panel_windows)} 个事件相位窗口",
            "主要输出": "CAR[-1,+1]、CAR[-3,+3]、CAR[-5,+5]、平均路径图",
            "证据状态": "正式样本",
            "当前口径": "直接回答事件附近是否存在显著超额收益。",
        },
        {
            "分析层": "长窗口保留分析",
            "市场范围": "中国 A 股 + 美国",
            "样本基础": "沿用真实事件窗口面板",
            "主要输出": "CAR[0,+5]、CAR[0,+20]、CAR[0,+60]、CAR[0,+120]、retention ratio",
            "证据状态": "正式样本",
            "当前口径": "用于区分短期价格压力与部分永久性需求曲线效应。",
        },
        {
            "分析层": "匹配对照组回归",
            "市场范围": "中国 A 股 + 美国",
            "样本基础": f"{_display_value(matched_rows)} 条匹配面板观测值",
            "主要输出": "主回归处理组系数、换手率/成交量/波动率机制回归",
            "证据状态": "正式样本",
            "当前口径": "在市值与纳入前收益控制下，对事件研究结果进行进一步识别。",
        },
        {
            "分析层": "中国 RDD 扩展",
            "市场范围": "中国 A 股（沪深300）",
            "样本基础": f"{_display_value(rdd_n_obs)} 个断点附近观测值",
            "主要输出": "local linear RD 系数、断点主图与分箱图",
            "证据状态": rdd_status,
            "当前口径": rdd_note,
        },
    ]
    return pd.DataFrame(rows)


def plot_average_paths(average_paths: pd.DataFrame, output_dir: str | Path) -> None:
    target_dir = _ensure_directory(output_dir)
    if average_paths.empty:
        return

    for (market, event_phase), group in average_paths.groupby(["market", "event_phase"], dropna=False):
        fig, ax = plt.subplots(figsize=(9.5, 6))
        base_color = MARKET_COLORS.get(str(market), "#30424f")
        linestyle = PHASE_LINESTYLES.get(str(event_phase), "-")
        for inclusion, inclusion_group in group.groupby("inclusion", dropna=False):
            inclusion_group = inclusion_group.sort_values("relative_day").copy()
            label = INCLUSION_LABELS.get(int(inclusion), str(inclusion))
            style = INCLUSION_STYLES.get(int(inclusion), {"alpha": 0.9, "marker": "o", "linewidth": 2.0})
            line_color = base_color if int(inclusion) == 1 else _lighten(base_color)
            if {"ci_low_95", "ci_high_95"}.issubset(inclusion_group.columns):
                ax.fill_between(
                    inclusion_group["relative_day"],
                    inclusion_group["ci_low_95"],
                    inclusion_group["ci_high_95"],
                    color=line_color,
                    alpha=0.14 if int(inclusion) == 1 else 0.10,
                    linewidth=0,
                )
            ax.plot(
                inclusion_group["relative_day"],
                inclusion_group["mean_car"],
                marker=style["marker"],
                linewidth=style["linewidth"],
                linestyle=linestyle,
                color=line_color,
                alpha=style["alpha"],
                label=label,
            )
        ax.axvline(0, color=base_color, linestyle=linestyle, linewidth=1.2, alpha=0.85)
        market_label = MARKET_LABELS.get(str(market), str(market))
        phase_label = PHASE_LABELS.get(str(event_phase), str(event_phase))
        ax.set_title(f"{market_label}{phase_label}平均累计异常收益路径", color=base_color, pad=12)
        ax.set_xlabel("相对交易日")
        ax.set_ylabel("平均累计异常收益")
        ax.legend(title=f"{market_label} · {phase_label}", frameon=False)
        ax.grid(alpha=0.24)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        fig.tight_layout()
        fig.savefig(target_dir / f"{market.lower()}_{event_phase}_car_path.png", dpi=180)
        plt.close(fig)


def export_descriptive_tables(
    events: pd.DataFrame,
    panel: pd.DataFrame,
    output_dir: str | Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    target_dir = _ensure_directory(output_dir)
    event_counts = (
        events.groupby(["market", "index_name"], dropna=False)
        .agg(n_events=("event_id", "nunique"), n_tickers=("ticker", "nunique"))
        .reset_index()
    )
    panel_coverage = (
        panel.groupby(["market", "event_phase", "inclusion"], dropna=False)
        .agg(
            n_event_windows=("event_id", "nunique"),
            avg_window_obs=("relative_day", "size"),
            avg_turnover=("turnover", "mean"),
            avg_volume=("volume", "mean"),
        )
        .reset_index()
    )
    event_counts.to_csv(target_dir / "event_counts.csv", index=False)
    panel_coverage.to_csv(target_dir / "panel_coverage.csv", index=False)
    return event_counts, panel_coverage


def build_event_counts_by_year_table(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    work = events.copy()
    work["announce_year"] = pd.to_datetime(work["announce_date"], errors="coerce").dt.year
    return (
        work.groupby(["market", "announce_year", "inclusion"], dropna=False)
        .agg(
            n_events=("event_id", "nunique"),
            n_tickers=("ticker", "nunique"),
            n_batches=("batch_id", "nunique"),
        )
        .reset_index()
        .sort_values(["market", "announce_year", "inclusion"])
        .reset_index(drop=True)
    )


def build_time_series_event_study_summary(event_level: pd.DataFrame) -> pd.DataFrame:
    if event_level.empty or "announce_date" not in event_level.columns:
        return pd.DataFrame()
    work = event_level.copy()
    if "treatment_group" in work.columns:
        work = work.loc[work["treatment_group"] == 1].copy()
    work["announce_year"] = pd.to_datetime(work["announce_date"], errors="coerce").dt.year
    value_columns = [column for column in ["car_m1_p1", "car_m3_p3", "car_m5_p5", "car_p0_p20", "car_p0_p120"] if column in work.columns]
    if not value_columns:
        return pd.DataFrame()
    aggregations: dict[str, tuple[str, str]] = {"n_events": ("event_id", "nunique")}
    for column in value_columns:
        aggregations[f"mean_{column}"] = (column, "mean")
        aggregations[f"std_{column}"] = (column, lambda series: series.std(ddof=1))
    summary = (
        work.groupby(["market", "inclusion", "event_phase", "announce_year"], dropna=False)
        .agg(**aggregations)
        .reset_index()
        .sort_values(["market", "inclusion", "event_phase", "announce_year"])
        .reset_index(drop=True)
    )
    n_obs = summary["n_events"].where(summary["n_events"] > 1)
    for column in value_columns:
        se_column = f"se_{column}"
        std_column = f"std_{column}"
        summary[se_column] = summary[std_column] / (n_obs**0.5)
        summary[f"ci_low_95_{column}"] = summary[f"mean_{column}"] - 1.96 * summary[se_column]
        summary[f"ci_high_95_{column}"] = summary[f"mean_{column}"] + 1.96 * summary[se_column]
        summary = summary.drop(columns=[std_column])
    return summary


def build_asymmetry_summary(
    event_level: pd.DataFrame,
    long_event_level: pd.DataFrame = pd.DataFrame(),
) -> pd.DataFrame:
    if event_level.empty:
        return pd.DataFrame()
    treated = event_level.copy()
    if "treatment_group" in treated.columns:
        treated = treated.loc[treated["treatment_group"] == 1].copy()
    if treated.empty:
        return pd.DataFrame()

    long_treated = long_event_level.copy()
    if not long_treated.empty and "treatment_group" in long_treated.columns:
        long_treated = long_treated.loc[long_treated["treatment_group"] == 1].copy()

    rows: list[dict[str, object]] = []
    for (market, event_phase), group in treated.groupby(["market", "event_phase"], dropna=False):
        additions = group.loc[group["inclusion"] == 1]
        deletions = group.loc[group["inclusion"] == 0]
        long_group = (
            long_treated.loc[(long_treated["market"] == market) & (long_treated["event_phase"] == event_phase)]
            if not long_treated.empty
            else pd.DataFrame()
        )
        long_additions = long_group.loc[long_group["inclusion"] == 1] if not long_group.empty else pd.DataFrame()
        long_deletions = long_group.loc[long_group["inclusion"] == 0] if not long_group.empty else pd.DataFrame()
        rows.append(
            {
                "market": market,
                "event_phase": event_phase,
                "n_additions": int(additions["event_id"].nunique()),
                "n_deletions": int(deletions["event_id"].nunique()),
                "addition_car_m1_p1": additions["car_m1_p1"].mean() if "car_m1_p1" in additions.columns else pd.NA,
                "deletion_car_m1_p1": deletions["car_m1_p1"].mean() if "car_m1_p1" in deletions.columns else pd.NA,
                "asymmetry_car_m1_p1": (
                    additions["car_m1_p1"].mean() - deletions["car_m1_p1"].mean()
                    if "car_m1_p1" in additions.columns and "car_m1_p1" in deletions.columns
                    else pd.NA
                ),
                "addition_turnover_change": additions["turnover_change"].mean() if "turnover_change" in additions.columns else pd.NA,
                "deletion_turnover_change": deletions["turnover_change"].mean() if "turnover_change" in deletions.columns else pd.NA,
                "addition_volume_change": additions["volume_change"].mean() if "volume_change" in additions.columns else pd.NA,
                "deletion_volume_change": deletions["volume_change"].mean() if "volume_change" in deletions.columns else pd.NA,
                "addition_car_p0_p120": long_additions["car_p0_p120"].mean() if "car_p0_p120" in long_additions.columns else pd.NA,
                "deletion_car_p0_p120": long_deletions["car_p0_p120"].mean() if "car_p0_p120" in long_deletions.columns else pd.NA,
                "asymmetry_car_p0_p120": (
                    long_additions["car_p0_p120"].mean() - long_deletions["car_p0_p120"].mean()
                    if "car_p0_p120" in long_additions.columns and "car_p0_p120" in long_deletions.columns
                    else pd.NA
                ),
            }
        )
    return pd.DataFrame(rows)


def _attach_comparison_id(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    work = frame.copy()
    if "comparison_id" not in work.columns:
        if "matched_to_event_id" in work.columns:
            work["comparison_id"] = work["matched_to_event_id"].where(work["matched_to_event_id"].notna(), work["event_id"])
        else:
            work["comparison_id"] = work["event_id"]
    return work


def _filter_nonoverlap_regression_dataset(dataset: pd.DataFrame, *, days: int = 120) -> pd.DataFrame:
    if dataset.empty:
        return dataset.copy()
    work = _attach_comparison_id(dataset)
    treated = work.loc[work["treatment_group"] == 1].copy()
    if treated.empty:
        return work.iloc[0:0].copy()
    filtered_treated = filter_nonoverlap_event_windows(treated, days=days, event_id_col="comparison_id")
    keep_keys = filtered_treated.loc[:, ["comparison_id", "event_phase"]].drop_duplicates()
    if keep_keys.empty:
        return work.iloc[0:0].copy()
    merged = work.merge(keep_keys.assign(_keep=True), on=["comparison_id", "event_phase"], how="left")
    return merged.loc[merged["_keep"].eq(True)].drop(columns="_keep").copy()


def build_sample_filter_summary(
    short_event_level: pd.DataFrame,
    long_event_level: pd.DataFrame = pd.DataFrame(),
    regression_dataset: pd.DataFrame = pd.DataFrame(),
    *,
    overlap_window_days: int = 120,
) -> pd.DataFrame:
    if short_event_level.empty:
        return pd.DataFrame()

    short_work = short_event_level.copy()
    if "treatment_group" in short_work.columns:
        short_work = short_work.loc[short_work["treatment_group"] == 1].copy()
    long_work = long_event_level.copy()
    if not long_work.empty and "treatment_group" in long_work.columns:
        long_work = long_work.loc[long_work["treatment_group"] == 1].copy()
    regression_work = _attach_comparison_id(regression_dataset)

    short_nonoverlap = filter_nonoverlap_event_windows(short_work, days=overlap_window_days)
    long_nonoverlap = filter_nonoverlap_event_windows(long_work, days=overlap_window_days) if not long_work.empty else long_work
    regression_nonoverlap = _filter_nonoverlap_regression_dataset(regression_work, days=overlap_window_days) if not regression_work.empty else regression_work

    baseline_phase_windows = max(len(short_work), 1)

    def _row(sample_filter: str, short_frame: pd.DataFrame, long_frame: pd.DataFrame, reg_frame: pd.DataFrame, note: str) -> dict[str, object]:
        return {
            "sample_filter": sample_filter,
            "n_treated_events": int(short_frame["event_id"].nunique()) if not short_frame.empty else 0,
            "n_short_event_phase_windows": int(len(short_frame)),
            "n_long_event_phase_windows": int(len(long_frame)) if not long_frame.empty else 0,
            "n_regression_comparisons": int(reg_frame["comparison_id"].nunique()) if not reg_frame.empty else 0,
            "n_regression_rows": int(len(reg_frame)) if not reg_frame.empty else 0,
            "share_of_baseline": len(short_frame) / baseline_phase_windows,
            "note": note,
        }

    rows = [
        _row("baseline", short_work, long_work, regression_work, "基准样本，不做重叠过滤或极值处理。"),
        _row("winsorized_1pct", short_work, long_work, regression_work, "仅对事件级 CAR 做 1% / 99% winsorize，不改变样本量。"),
        _row("nonoverlap_120d", short_nonoverlap, long_nonoverlap, regression_nonoverlap, "剔除同一 ticker、同一事件阶段下 ±120 日历日内重叠的事件窗口。"),
    ]
    return pd.DataFrame(rows)


def build_robustness_event_study_summary(
    short_event_level: pd.DataFrame,
    long_event_level: pd.DataFrame = pd.DataFrame(),
    *,
    overlap_window_days: int = 120,
    winsor_quantile: float = 0.01,
) -> pd.DataFrame:
    if short_event_level.empty:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    variants = [
        ("baseline", short_event_level, long_event_level),
        (
            "winsorized_1pct",
            winsorize_event_level_metrics(short_event_level, quantile=winsor_quantile),
            winsorize_event_level_metrics(long_event_level, quantile=winsor_quantile) if not long_event_level.empty else long_event_level,
        ),
        (
            "nonoverlap_120d",
            filter_nonoverlap_event_windows(short_event_level, days=overlap_window_days),
            filter_nonoverlap_event_windows(long_event_level, days=overlap_window_days) if not long_event_level.empty else long_event_level,
        ),
    ]
    for sample_filter, short_frame, long_frame in variants:
        short_summary = summarize_event_level_metrics(short_frame, sample_filter=sample_filter)
        if not short_summary.empty:
            frames.append(short_summary)
        long_summary = summarize_event_level_metrics(long_frame, sample_filter=sample_filter)
        if not long_summary.empty:
            frames.append(long_summary)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_robustness_regression_summary(
    regression_dataset: pd.DataFrame,
    *,
    main_car_slug: str = "m1_p1",
    overlap_window_days: int = 120,
) -> pd.DataFrame:
    if regression_dataset.empty:
        return pd.DataFrame()

    work = _attach_comparison_id(regression_dataset)
    variants = [
        ("baseline_ols", "nonrobust", work),
        ("hc3", "HC3", work),
        ("nonoverlap_120d", "HC3", _filter_nonoverlap_regression_dataset(work, days=overlap_window_days)),
    ]
    frames: list[pd.DataFrame] = []
    for estimation, cov_type, dataset_variant in variants:
        coefficients, model_stats = run_regressions(
            dataset_variant,
            main_car_slug=main_car_slug,
            cov_type=cov_type,
            estimation=estimation,
        )
        if coefficients.empty or model_stats.empty:
            continue
        coef_focus = coefficients.loc[
            (coefficients["specification"] == "main_car") & (coefficients["parameter"] == "treatment_group")
        ].copy()
        model_focus = model_stats.loc[model_stats["specification"] == "main_car"].copy()
        merged = coef_focus.merge(
            model_focus.loc[:, ["market", "event_phase", "estimation", "n_obs", "r_squared", "adj_r_squared"]],
            on=["market", "event_phase", "estimation"],
            how="left",
        )
        merged["covariance"] = "OLS" if cov_type == "nonrobust" else cov_type
        frames.append(merged)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_robustness_retention_summary(
    long_event_level: pd.DataFrame,
    *,
    overlap_window_days: int = 120,
    winsor_quantile: float = 0.01,
    short_window_slug: str = "p0_p20",
    long_window_slug: str = "p0_p120",
) -> pd.DataFrame:
    if long_event_level.empty:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    variants = [
        ("baseline", long_event_level),
        ("winsorized_1pct", winsorize_event_level_metrics(long_event_level, quantile=winsor_quantile)),
        ("nonoverlap_120d", filter_nonoverlap_event_windows(long_event_level, days=overlap_window_days)),
    ]
    for sample_filter, frame in variants:
        summary = compute_retention_summary(
            frame,
            short_window_slug=short_window_slug,
            long_window_slug=long_window_slug,
        )
        if summary.empty:
            continue
        summary["sample_filter"] = sample_filter
        frames.append(summary)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def export_latex_tables(frames: dict[str, pd.DataFrame], output_dir: str | Path) -> None:
    target_dir = _ensure_directory(output_dir)
    for name, frame in frames.items():
        if frame.empty:
            continue
        latex = frame.to_latex(index=False, float_format=lambda value: f"{value:0.4f}")
        (target_dir / f"{name}.tex").write_text(latex, encoding="utf-8")
