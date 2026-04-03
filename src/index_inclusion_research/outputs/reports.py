from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

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
    1: "纳入样本",
    0: "匹配对照组",
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
                "备注": "A 股样本为沪深300新增成分股，美股样本为 S&P 500 新纳入成分股。",
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
                "来源": "由匹配后的纳入样本、对照组与基准收益拼接生成",
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
                "说明": "每个纳入事件同时对应公告日与生效日两个事件时点。",
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
                "说明": "每个纳入事件默认匹配 3 个对照股票，用于主回归与机制回归。",
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
            "样本基础": f"{_display_value(event_count)} 个真实纳入事件、{_display_value(panel_windows)} 个事件相位窗口",
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
            "主要输出": "主回归纳入系数、换手率/成交量/波动率机制回归",
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
            label = INCLUSION_LABELS.get(int(inclusion), str(inclusion))
            style = INCLUSION_STYLES.get(int(inclusion), {"alpha": 0.9, "marker": "o", "linewidth": 2.0})
            line_color = base_color if int(inclusion) == 1 else _lighten(base_color)
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


def export_latex_tables(frames: dict[str, pd.DataFrame], output_dir: str | Path) -> None:
    target_dir = _ensure_directory(output_dir)
    for name, frame in frames.items():
        if frame.empty:
            continue
        latex = frame.to_latex(index=False, float_format=lambda value: f"{value:0.4f}")
        (target_dir / f"{name}.tex").write_text(latex, encoding="utf-8")
