from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from index_inclusion_research.dashboard_media import build_figure_entry
from index_inclusion_research.dashboard_types import (
    FigureEntry,
    FormatPValue,
    FormatShare,
    RddStatusLoader,
    RelativePathBuilder,
)


def dashboard_figure_dir(root: Path) -> Path:
    target = root / "results" / "real_figures"
    target.mkdir(parents=True, exist_ok=True)
    return target


def figure_cache_is_fresh(targets: list[Path], sources: list[Path]) -> bool:
    existing_targets = [path for path in targets if path.exists()]
    if len(existing_targets) != len(targets):
        return False
    if os.environ.get("DASHBOARD_ECHARTS_TEST_STUB") == "1":
        return True
    existing_sources = [path for path in sources if path.exists()]
    if not existing_sources:
        return False
    latest_source = max(path.stat().st_mtime for path in existing_sources)
    oldest_target = min(path.stat().st_mtime for path in existing_targets)
    return oldest_target >= latest_source


def significance_stars(p_value: float) -> str:
    if p_value < 0.01:
        return "***"
    if p_value < 0.05:
        return "**"
    if p_value < 0.10:
        return "*"
    return ""


def _price_pressure_figure_entry(path: Path, to_relative: RelativePathBuilder) -> FigureEntry:
    entry = build_figure_entry(
        path,
        to_relative=to_relative,
        label="短窗口 CAR 时间变化图",
        caption="图意：按公告年份追踪调入事件的 CAR[-1,+1]。阅读重点：观察美股公告日效应是否随时间减弱，以及中国样本是否呈现不同的阶段性变化。",
        layout_class="wide",
    )
    entry["echart_id"] = "price_pressure"
    return entry


def create_price_pressure_figures(
    root: Path,
    *,
    to_relative: RelativePathBuilder,
) -> list[FigureEntry]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    target_dir = dashboard_figure_dir(root)
    summary_path = root / "results" / "real_tables" / "time_series_event_study_summary.csv"
    summary = pd.read_csv(summary_path)
    summary = summary.loc[summary["inclusion"] == 1].copy()
    if summary.empty:
        return []

    market_labels = {"CN": "中国 A 股", "US": "美国"}
    phase_labels = {"announce": "公告日", "effective": "生效日"}
    market_colors = {"CN": "#a63b28", "US": "#0f5c6e"}
    phase_linestyles = {"announce": "-", "effective": "--"}

    figure_path = target_dir / "price_pressure_time_series.png"
    if figure_cache_is_fresh([figure_path], [summary_path]):
        return [_price_pressure_figure_entry(figure_path, to_relative)]
    fig, ax = plt.subplots(figsize=(11.6, 5.2))
    for (market, event_phase), group in summary.groupby(["market", "event_phase"], dropna=False):
        group = group.sort_values("announce_year").copy()
        if {"ci_low_95_car_m1_p1", "ci_high_95_car_m1_p1"}.issubset(group.columns):
            ax.fill_between(
                group["announce_year"],
                group["ci_low_95_car_m1_p1"],
                group["ci_high_95_car_m1_p1"],
                color=market_colors.get(market, "#30424f"),
                alpha=0.12 if event_phase == "announce" else 0.08,
                linewidth=0,
            )
        ax.plot(
            group["announce_year"],
            group["mean_car_m1_p1"],
            color=market_colors.get(market, "#30424f"),
            linestyle=phase_linestyles.get(event_phase, "-"),
            marker="o",
            linewidth=2.2,
            markersize=6,
            label=f"{market_labels.get(market, market)}{phase_labels.get(event_phase, event_phase)}",
        )
    ax.axhline(0, color="#92a0aa", linewidth=1.0, linestyle="--")
    ax.set_title("短窗口 CAR 的时间变化", fontsize=16, pad=14)
    ax.set_xlabel("公告年份")
    ax.set_ylabel("平均 CAR[-1,+1]")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(frameon=False, ncol=2)
    fig.tight_layout()
    fig.savefig(figure_path, dpi=220)
    plt.close(fig)

    return [_price_pressure_figure_entry(figure_path, to_relative)]


def create_identification_figures(
    root: Path,
    *,
    load_rdd_status: RddStatusLoader,
    to_relative: RelativePathBuilder,
) -> list[FigureEntry]:
    import matplotlib
    import numpy as np

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    rdd_status = load_rdd_status()
    if rdd_status["mode"] not in {"real", "reconstructed"}:
        return []

    rdd_dir = root / "results" / "literature" / "hs300_rdd" / "figures"
    rdd_dir.mkdir(parents=True, exist_ok=True)
    event_level_path = root / "results" / "literature" / "hs300_rdd" / "event_level_with_running.csv"
    if not event_level_path.exists():
        return []
    panel = pd.read_csv(event_level_path)
    subset = panel.loc[panel["event_phase"] == "announce", ["distance_to_cutoff", "car_m1_p1"]].dropna().copy()
    if subset.empty:
        return []

    left = subset.loc[subset["distance_to_cutoff"] < 0].sort_values("distance_to_cutoff")
    right = subset.loc[subset["distance_to_cutoff"] >= 0].sort_values("distance_to_cutoff")

    def _binned(frame: pd.DataFrame, bins: int) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=["center", "mean"])
        frame = frame.copy()
        frame["bin"] = pd.cut(frame["distance_to_cutoff"], bins=bins, duplicates="drop")
        grouped = (
            frame.groupby("bin", observed=True)
            .agg(center=("distance_to_cutoff", "mean"), mean=("car_m1_p1", "mean"))
            .reset_index(drop=True)
        )
        return grouped

    def _fit_line(frame: pd.DataFrame):
        if len(frame) < 2 or frame["distance_to_cutoff"].nunique() < 2:
            return None, None
        x = frame["distance_to_cutoff"].to_numpy(dtype=float)
        y = frame["car_m1_p1"].to_numpy(dtype=float)
        design = np.column_stack([np.ones_like(x), x])
        intercept, slope = np.linalg.lstsq(design, y, rcond=None)[0]
        x_values = np.linspace(x.min(), x.max(), 80)
        y_values = intercept + slope * x_values
        return x_values, y_values

    left_bins = _binned(left, bins=min(6, max(len(left) // 4, 3)))
    right_bins = _binned(right, bins=min(6, max(len(right) // 4, 3)))
    left_x, left_y = _fit_line(left)
    right_x, right_y = _fit_line(right)

    figure_path = rdd_dir / "car_m1_p1_rdd_main.png"
    if figure_cache_is_fresh([figure_path], [event_level_path]):
        cached_entry = build_figure_entry(
            figure_path,
            to_relative=to_relative,
            caption="中国样本 RDD 主图。图意：以公告日 CAR[-1,+1] 为例展示断点两侧分箱均值与局部拟合线。阅读重点：聚焦 0 附近是否存在离散跳跃，而不是只看两侧散点的总体波动。",
        )
        cached_entry["echart_id"] = "rdd_scatter"
        return _assemble_identification_figures(
            root, to_relative=to_relative, lead_entry=cached_entry, rdd_dir=rdd_dir
        )
    fig, ax = plt.subplots(figsize=(10.8, 6.0))
    ax.axvline(0, color="#5c6b77", linestyle="--", linewidth=1.2)
    ax.scatter(left["distance_to_cutoff"], left["car_m1_p1"], color="#d7b49e", alpha=0.24, s=28)
    ax.scatter(right["distance_to_cutoff"], right["car_m1_p1"], color="#9cc7cf", alpha=0.24, s=28)
    if not left_bins.empty:
        ax.scatter(left_bins["center"], left_bins["mean"], color="#a63b28", s=72, label="断点左侧分箱均值", zorder=3)
    if not right_bins.empty:
        ax.scatter(right_bins["center"], right_bins["mean"], color="#0f5c6e", s=72, label="断点右侧分箱均值", zorder=3)
    if left_x is not None:
        ax.plot(left_x, left_y, color="#a63b28", linewidth=2.4)
    if right_x is not None:
        ax.plot(right_x, right_y, color="#0f5c6e", linewidth=2.4)
    ax.set_title("中国样本 RDD 主图：公告日 CAR[-1,+1] 断点回归", fontsize=16, pad=14)
    ax.set_xlabel("距断点距离")
    ax.set_ylabel("CAR[-1,+1]")
    ax.grid(alpha=0.22)
    ax.legend(frameon=False, loc="upper left")
    fig.tight_layout()
    fig.savefig(figure_path, dpi=220)
    plt.close(fig)
    rdd_entry = build_figure_entry(
        figure_path,
        to_relative=to_relative,
        caption="中国样本 RDD 主图。图意：以公告日 CAR[-1,+1] 为例展示断点两侧分箱均值与局部拟合线。阅读重点：聚焦 0 附近是否存在离散跳跃，而不是只看两侧散点的总体波动。",
    )
    rdd_entry["echart_id"] = "rdd_scatter"

    return _assemble_identification_figures(
        root, to_relative=to_relative, lead_entry=rdd_entry, rdd_dir=rdd_dir
    )


# Outcome-name → caption + thumb label for the RDD secondary-outcome bin
# scatter PNGs that hs300_rdd.run_analysis emits alongside the main car_m1_p1
# figure. Surfacing them as track thumbs gives reviewers a visual robustness
# check ("does the discontinuity also show up on wider windows / volume /
# turnover, not just car_m1_p1?") without leaving the main page.
_RDD_SECONDARY_OUTCOMES: tuple[tuple[str, str, str, str], ...] = (
    (
        "car_m3_p3_rdd_bins.png",
        "RDD 稳健性 · CAR[-3,+3]",
        (
            "RDD 稳健性副图：把 outcome 换成 CAR[-3,+3] 看断点是否依然存在。"
            "阅读重点：分箱均值在 0 附近的跳跃方向应与主图 CAR[-1,+1] 一致；"
            "若幅度衰减说明效应集中在更短窗口。"
        ),
        "wide",
    ),
    (
        "turnover_change_rdd_bins.png",
        "RDD 稳健性 · 换手变化",
        (
            "RDD 稳健性副图：outcome=换手率变化。"
            "阅读重点：处理组在 0 右侧应有正向集中（被动需求），"
            "若同时出现 CAR 跳跃和换手集中，价格压力解释更稳。"
        ),
        "wide",
    ),
    (
        "volume_change_rdd_bins.png",
        "RDD 稳健性 · 成交量变化",
        (
            "RDD 稳健性副图：outcome=成交量变化。"
            "阅读重点：和换手率副图同方向看；"
            "成交量集中度通常是事件需求冲击的最直接信号。"
        ),
        "wide",
    ),
)


def _assemble_identification_figures(
    root: Path,
    *,
    to_relative: RelativePathBuilder,
    lead_entry: FigureEntry,
    rdd_dir: Path,
) -> list[FigureEntry]:
    """Compose the identification track's display_figures: rdd lead +
    L3 coverage timeline + 3 RDD secondary outcome thumbs + robustness
    forest. The track template renders display_figures[0] as the feature
    image and display_figures[1:6] as a thumb-grid; we hand it up to 6
    entries so the grid fills with timeline + secondary outcomes +
    robustness panel.
    """
    figures: list[FigureEntry] = [lead_entry]
    coverage_entry = _l3_coverage_figure_entry(root, to_relative=to_relative)
    if coverage_entry is not None:
        figures.append(coverage_entry)
    for filename, label, caption, layout_class in _RDD_SECONDARY_OUTCOMES:
        path = rdd_dir / filename
        if not path.exists():
            continue
        entry = build_figure_entry(
            path,
            to_relative=to_relative,
            caption=caption,
            label=label,
            layout_class=layout_class,
        )
        figures.append(entry)
    robustness_entry = _rdd_robustness_figure_entry(root, to_relative=to_relative, rdd_dir=rdd_dir)
    if robustness_entry is not None:
        figures.append(robustness_entry)
    return figures


def _rdd_robustness_figure_entry(
    root: Path,
    *,
    to_relative: RelativePathBuilder,
    rdd_dir: Path,
) -> FigureEntry | None:
    """Forest-plot PNG of the RDD robustness panel (main / donut / placebo /
    polynomial). Reads results/literature/hs300_rdd/rdd_robustness.csv
    written by run_rdd_robustness; emits a figure entry with echart_id
    'rdd_robustness' so future thumb-grid template upgrades can swap to
    interactive ECharts without changing the figure pipeline.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    robust_path = root / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv"
    if not robust_path.exists():
        return None

    df = pd.read_csv(robust_path)
    if df.empty or "spec" not in df.columns:
        return None

    figure_path = rdd_dir / "rdd_robustness_forest.png"
    if figure_cache_is_fresh([figure_path], [robust_path]):
        cached = build_figure_entry(
            figure_path,
            to_relative=to_relative,
            caption=_rdd_robustness_caption(df),
            label="RDD 稳健性 · 设定面板",
            layout_class="wide",
        )
        cached["echart_id"] = "rdd_robustness"
        return cached

    spec_kind_order = ["main", "donut", "placebo", "polynomial"]
    df = df.assign(
        _kind_rank=df["spec_kind"].map(
            lambda k: spec_kind_order.index(k) if k in spec_kind_order else 99
        )
    ).sort_values(["_kind_rank", "spec"], ascending=[False, False])

    color_map = {
        "main": "#0f5c6e",
        "donut": "#5d4f8a",
        "placebo": "#5c6b77",
        "polynomial": "#a63b28",
    }
    colors = [color_map.get(str(k), "#30424f") for k in df["spec_kind"]]

    fig, ax = plt.subplots(figsize=(9.6, 4.4))
    y = list(range(len(df)))
    taus = df["tau"].astype(float).to_numpy()
    ses = df["std_error"].astype(float).to_numpy()
    ax.errorbar(
        taus, y,
        xerr=1.96 * ses,
        fmt="none",
        ecolor="#5c6b77",
        elinewidth=1.4,
        capsize=4,
    )
    ax.scatter(
        taus, y,
        s=70,
        c=colors,
        edgecolors="#18212b",
        linewidths=0.8,
        zorder=3,
    )
    ax.axvline(0, color="#9ba3ad", linestyle="--", linewidth=1.0)
    ax.set_yticks(y)
    ax.set_yticklabels(df["spec"].tolist(), fontsize=10)
    ax.set_xlabel("τ (RDD 处理效应)")
    ax.set_title("HS300 RDD 稳健性 · car_m1_p1 ± 1.96·SE", fontsize=14, pad=10)
    ax.grid(axis="x", alpha=0.18)
    ax.tick_params(axis="x", labelsize=9)
    fig.tight_layout()
    fig.savefig(figure_path, dpi=220)
    plt.close(fig)

    entry = build_figure_entry(
        figure_path,
        to_relative=to_relative,
        caption=_rdd_robustness_caption(df),
        label="RDD 稳健性 · 设定面板",
        layout_class="wide",
    )
    entry["echart_id"] = "rdd_robustness"
    return entry


def _rdd_robustness_caption(df: pd.DataFrame) -> str:
    main = df.loc[df["spec_kind"] == "main"].head(1)
    if main.empty:
        return (
            "RDD 稳健性面板：把 main 局部线性的 τ 与 donut / placebo / polynomial "
            "比较。阅读重点：placebo τ 应靠近 0；donut 与 polynomial 是 main 设定的偏离度。"
        )
    tau = float(main.iloc[0]["tau"])
    p = float(main.iloc[0]["p_value"])
    n = int(main.iloc[0]["n_obs"])
    return (
        f"RDD 稳健性面板：main 局部线性 τ={tau * 100:.2f}% (p={p:.3f}, n={n})。"
        "阅读重点：placebo cutoff 的 τ 应靠近 0（识别合理的反向证据）；"
        "donut 与 polynomial 偏离 main 越大，主结果对设定越敏感。"
    )


_L3_COVERAGE_TARGET_BATCHES = 20  # ~10-year window, see docs/hs300_rdd_l3_collection_audit.md
_L3_COVERAGE_TARGET_YEARS = 10


def _l3_coverage_figure_entry(
    root: Path,
    *,
    to_relative: RelativePathBuilder,
) -> FigureEntry | None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    candidates_path = root / "data" / "raw" / "hs300_rdd_candidates.csv"
    if not candidates_path.exists():
        return None

    figure_dir = root / "results" / "literature" / "hs300_rdd" / "figures"
    figure_dir.mkdir(parents=True, exist_ok=True)
    figure_path = figure_dir / "l3_coverage_timeline.png"

    candidates = pd.read_csv(candidates_path)
    if candidates.empty:
        return None
    summary = (
        candidates.assign(
            n_inclusion=candidates["inclusion"].fillna(0).astype(int),
            n_reserve=(candidates["inclusion"].fillna(0).astype(int) == 0).astype(int),
        )
        .groupby("batch_id", as_index=False)
        .agg(
            announce_date=("announce_date", "first"),
            n_inclusion=("n_inclusion", "sum"),
            n_reserve=("n_reserve", "sum"),
        )
    )
    if summary.empty:
        return None
    summary["announce_date"] = pd.to_datetime(summary["announce_date"], errors="coerce")
    summary = summary.dropna(subset=["announce_date"]).sort_values("announce_date")
    if summary.empty:
        return None

    n_batches = int(summary.shape[0])
    earliest = summary["announce_date"].min()
    latest = summary["announce_date"].max()
    span_years = max((latest - earliest).days / 365.25, 0.0)

    if figure_cache_is_fresh([figure_path], [candidates_path]):
        coverage_entry = build_figure_entry(
            figure_path,
            to_relative=to_relative,
            caption=_l3_coverage_caption(n_batches, span_years),
        )
        coverage_entry["label"] = "L3 批次覆盖时间线"
        return coverage_entry

    fig, ax = plt.subplots(figsize=(10.8, 4.4))
    dates = summary["announce_date"]
    inclusions = summary["n_inclusion"].to_numpy(dtype=float)
    reserves = summary["n_reserve"].to_numpy(dtype=float)
    bar_width = 120  # days
    ax.bar(dates, inclusions, width=bar_width, color="#0f5c6e", label="官方调入 (treated)")
    ax.bar(dates, reserves, width=bar_width, bottom=inclusions, color="#d7b49e", label="备选对照 (control)")

    threshold_x_start = pd.Timestamp(latest) - pd.DateOffset(years=_L3_COVERAGE_TARGET_YEARS)
    threshold_x_end = pd.Timestamp(latest)
    ax.axvspan(
        threshold_x_start,  # type: ignore[arg-type]  # matplotlib accepts Timestamp on date axis; stub typed as float
        threshold_x_end,  # type: ignore[arg-type]
        ymin=0,
        ymax=1,
        color="#0f5c6e",
        alpha=0.05,
        zorder=0,
        label=f"主表门槛窗口 ({_L3_COVERAGE_TARGET_YEARS} 年 · {_L3_COVERAGE_TARGET_BATCHES} 批次)",
    )
    annotated_progress = (
        f"覆盖：{n_batches} / {_L3_COVERAGE_TARGET_BATCHES} 批次 "
        f"· {span_years:.1f} / {_L3_COVERAGE_TARGET_YEARS} 年"
    )
    ax.set_title("HS300 RDD L3 批次覆盖时间线", fontsize=15, pad=12)
    ax.set_xlabel(annotated_progress, fontsize=11, labelpad=8)
    ax.set_ylabel("候选行数")
    ax.grid(axis="y", alpha=0.22)
    ax.legend(frameon=False, loc="upper left", fontsize=9)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(figure_path, dpi=220)
    plt.close(fig)

    coverage_entry = build_figure_entry(
        figure_path,
        to_relative=to_relative,
        caption=_l3_coverage_caption(n_batches, span_years),
    )
    coverage_entry["label"] = "L3 批次覆盖时间线"
    return coverage_entry


def _l3_coverage_caption(n_batches: int, span_years: float) -> str:
    return (
        f"L3 候选样本批次覆盖。图意：当前 {n_batches} / {_L3_COVERAGE_TARGET_BATCHES} 个批次，"
        f"跨度 {span_years:.1f} / {_L3_COVERAGE_TARGET_YEARS} 年；阴影区域是论文级因果声明的目标窗口。"
        "阅读重点：每个批次都要同时拥有调入与备选对照才能进入 L3 主表，2014-2019 缺口需通过外部档案补齐。"
    )


def _sample_design_figure_entries(target_dir: Path, to_relative: RelativePathBuilder) -> list[FigureEntry]:
    timeline_path = target_dir / "sample_event_timeline.png"
    heatmap_path = target_dir / "sample_car_heatmap.png"
    main_path = target_dir / "main_regression_coefficients.png"
    mechanism_path = target_dir / "mechanism_regression_coefficients.png"
    match_path = target_dir / "match_diagnostics_overview.png"
    heatmap_entry = build_figure_entry(
            heatmap_path,
            to_relative=to_relative,
            label="真实样本短窗口 CAR 热力图",
            caption="图意：把三组短窗口 CAR 压缩到同一张热力图中。阅读重点：优先比较美国公告日和中国生效日单元格的方向、幅度与显著性差异。",
            layout_class="wide",
        )
    heatmap_entry["echart_id"] = "car_heatmap"
    main_entry = build_figure_entry(
        main_path,
        to_relative=to_relative,
        label="主回归处理组系数图",
        caption="图意：展示主回归中处理组变量系数与 95% 置信区间。阅读重点：比较不同市场、不同事件阶段的方向是否一致，以及置信区间是否跨越 0。",
    )
    main_entry["echart_id"] = "main_regression"
    mechanism_entry = build_figure_entry(
        mechanism_path,
        to_relative=to_relative,
        label="机制回归系数图",
        caption="图意：把换手率、成交量与波动率三类机制回归放在同一张图中。阅读重点：观察中国 A 股与美国在公告日和生效日的机制方向是否一致。",
    )
    mechanism_entry["echart_id"] = "mechanism_regression"
    timeline_entry = build_figure_entry(
        timeline_path,
        to_relative=to_relative,
        label="真实调入调出事件时间线",
        caption="图意：按市场与事件阶段展开所有真实调入/调出事件。阅读重点：观察样本是否集中于少数批次，以及公告日与生效日是否在时间轴上形成清晰层次。",
        layout_class="wide",
    )
    timeline_entry["echart_id"] = "event_counts"
    return [
        timeline_entry,
        heatmap_entry,
        main_entry,
        mechanism_entry,
        build_figure_entry(
            match_path,
            to_relative=to_relative,
            label="匹配诊断图",
            caption="图意：同时展示匹配状态分布与匹配质量指标。阅读重点：先看匹配成功率，再看三对照构造与行业口径放宽占比，从而判断对照组设计是否稳定。",
            layout_class="wide",
        ),
    ]


def create_sample_design_figures(
    root: Path,
    *,
    to_relative: RelativePathBuilder,
    format_p_value: FormatPValue,
    format_share: FormatShare,
) -> list[FigureEntry]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm

    plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    target_dir = dashboard_figure_dir(root)
    timeline_path = target_dir / "sample_event_timeline.png"
    heatmap_path = target_dir / "sample_car_heatmap.png"
    main_path = target_dir / "main_regression_coefficients.png"
    mechanism_path = target_dir / "mechanism_regression_coefficients.png"
    match_path = target_dir / "match_diagnostics_overview.png"
    if figure_cache_is_fresh(
        [timeline_path, heatmap_path, main_path, mechanism_path, match_path],
        [
            root / "results" / "real_tables" / "event_study_summary.csv",
            root / "results" / "real_regressions" / "regression_coefficients.csv",
            root / "results" / "real_regressions" / "match_diagnostics.csv",
            root / "data" / "raw" / "real_events.csv",
        ],
    ):
        return _sample_design_figure_entries(target_dir, to_relative)
    event = pd.read_csv(root / "results" / "real_tables" / "event_study_summary.csv")
    regression = pd.read_csv(root / "results" / "real_regressions" / "regression_coefficients.csv")
    diagnostics = pd.read_csv(root / "results" / "real_regressions" / "match_diagnostics.csv")
    real_events = pd.read_csv(root / "data" / "raw" / "real_events.csv")

    market_labels = {"CN": "中国 A 股", "US": "美国"}
    phase_labels = {"announce": "公告日", "effective": "生效日"}
    spec_labels = {
        "turnover_mechanism": "换手率变化",
        "volume_mechanism": "成交量变化",
        "volatility_mechanism": "波动率变化",
    }
    market_colors = {"CN": "#a63b28", "US": "#0f5c6e"}

    long_events = real_events.loc[:, ["market", "ticker", "announce_date", "effective_date"]].copy()
    announce = long_events.rename(columns={"announce_date": "event_date"}).assign(event_phase="announce")
    effective = long_events.rename(columns={"effective_date": "event_date"}).assign(event_phase="effective")
    timeline = pd.concat([announce, effective], ignore_index=True)
    timeline["event_date"] = pd.to_datetime(timeline["event_date"])
    timeline["row_label"] = timeline["market"].map(market_labels) + " · " + timeline["event_phase"].map(phase_labels)
    row_order = ["中国 A 股 · 公告日", "中国 A 股 · 生效日", "美国 · 公告日", "美国 · 生效日"]
    row_positions = {label: idx for idx, label in enumerate(row_order)}
    fig, ax = plt.subplots(figsize=(12.2, 4.8))
    for market in ["CN", "US"]:
        for phase, marker in [("announce", "o"), ("effective", "s")]:
            subset = timeline.loc[(timeline["market"] == market) & (timeline["event_phase"] == phase)].copy()
            if subset.empty:
                continue
            y_values = [row_positions[label] for label in subset["row_label"]]
            ax.scatter(
                subset["event_date"],
                y_values,
                s=72,
                marker=marker,
                color=market_colors[market],
                alpha=0.88,
                edgecolor="white",
                linewidth=0.7,
                label=f"{market_labels[market]}{phase_labels[phase]}",
            )
    ax.set_yticks(range(len(row_order)))
    ax.set_yticklabels(row_order, fontsize=11)
    year_span = timeline["event_date"].dt.year.max() - timeline["event_date"].dt.year.min()
    locator_interval = 1 if year_span <= 6 else 2
    ax.xaxis.set_major_locator(mdates.YearLocator(base=locator_interval))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.tick_params(axis="x", labelrotation=0)
    ax.set_title("真实调入调出事件时间线", fontsize=16, pad=18)
    ax.set_xlabel("事件日期")
    ax.set_ylabel("样本分层")
    ax.grid(axis="x", alpha=0.25)
    handles, labels = ax.get_legend_handles_labels()
    unique = dict(zip(labels, handles, strict=False))
    fig.legend(
        unique.values(),
        unique.keys(),
        ncol=4,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.965),
        frameon=False,
        fontsize=10,
        columnspacing=1.4,
        handletextpad=0.6,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.89))
    fig.savefig(timeline_path, dpi=220)
    plt.close(fig)

    window_order = ["[-1,+1]", "[-3,+3]", "[-5,+5]"]
    heat = event.loc[event["inclusion"] == 1].copy()
    heat["row_label"] = heat["market"].map(market_labels) + " · " + heat["event_phase"].map(phase_labels)
    heat_matrix = (
        heat.pivot_table(index="row_label", columns="window", values="mean_car", aggfunc="first")
        .reindex(index=row_order, columns=window_order)
    )
    p_matrix = (
        heat.pivot_table(index="row_label", columns="window", values="p_value", aggfunc="first")
        .reindex(index=row_order, columns=window_order)
    )
    fig, ax = plt.subplots(figsize=(10.2, 5.8))
    cmap = LinearSegmentedColormap.from_list("car_heat", ["#9c2f55", "#f7f2ea", "#0f5c6e"])
    vmax = max(abs(float(heat_matrix.min().min())), abs(float(heat_matrix.max().max())), 0.01)
    norm = TwoSlopeNorm(vmin=-vmax, vcenter=0, vmax=vmax)
    image = ax.imshow(heat_matrix.values, cmap=cmap, norm=norm, aspect="auto")
    ax.set_xticks(range(len(window_order)))
    ax.set_xticklabels(window_order, fontsize=11)
    ax.set_yticks(range(len(row_order)))
    ax.set_yticklabels(row_order, fontsize=11)
    ax.set_title("真实样本短窗口 CAR 热力图", fontsize=16, pad=14)
    ax.set_xlabel("事件窗口")
    ax.set_ylabel("市场与事件阶段")
    for i, row_label in enumerate(row_order):
        for j, window in enumerate(window_order):
            car = float(heat_matrix.loc[row_label, window])  # type: ignore[arg-type]
            p_value = float(p_matrix.loc[row_label, window])  # type: ignore[arg-type]
            color = "white" if abs(car) > vmax * 0.45 else "#18212b"
            ax.text(
                j,
                i,
                f"{car:.2%}\n{significance_stars(p_value)}",
                ha="center",
                va="center",
                fontsize=11,
                color=color,
                fontweight="bold",
            )
    color_bar = fig.colorbar(image, ax=ax, shrink=0.92)
    color_bar.ax.set_ylabel("平均 CAR", rotation=90, labelpad=12)
    fig.tight_layout()
    fig.savefig(heatmap_path, dpi=220)
    plt.close(fig)

    main = regression.loc[
        (regression["parameter"] == "treatment_group") & (regression["specification"] == "main_car")
    ].copy()
    main["label"] = main["market"].map(market_labels) + " · " + main["event_phase"].map(phase_labels)
    main["ci"] = 1.96 * main["std_error"]
    main_order = ["中国 A 股 · 公告日", "中国 A 股 · 生效日", "美国 · 公告日", "美国 · 生效日"]
    main = main.set_index("label").reindex(main_order).reset_index()
    fig, ax = plt.subplots(figsize=(10.2, 5.6))
    y_positions = list(range(len(main)))
    colors = ["#a63b28" if "中国" in label else "#0f5c6e" for label in main["label"]]
    ax.axvline(0, color="#8894a0", linewidth=1.2, linestyle="--")
    for idx, (_, row) in enumerate(main.iterrows()):
        ax.errorbar(
            row["coefficient"],
            idx,
            xerr=row["ci"],
            fmt="o",
            color=colors[idx],
            ecolor=colors[idx],
            elinewidth=2,
            capsize=4,
            markersize=8,
        )
        offset = 0.002 if row["coefficient"] >= 0 else -0.002
        ax.text(
            row["coefficient"] + offset,
            idx + 0.18,
            format_p_value(float(row["p_value"])),
            color=colors[idx],
            fontsize=10,
            ha="left" if row["coefficient"] >= 0 else "right",
        )
    ax.set_yticks(y_positions)
    ax.set_yticklabels(main["label"], fontsize=11)
    ax.set_xlabel("处理组变量估计系数")
    ax.set_title("主回归处理组系数与 95% 置信区间", fontsize=16, pad=14)
    ax.grid(axis="x", alpha=0.25)
    ax.invert_yaxis()
    fig.tight_layout()
    fig.savefig(main_path, dpi=220)
    plt.close(fig)

    mechanism = regression.loc[
        (regression["parameter"] == "treatment_group") & (regression["specification"] != "main_car")
    ].copy()
    mechanism["metric_label"] = mechanism["specification"].map(spec_labels)
    mechanism["phase_label"] = mechanism["event_phase"].map(phase_labels)
    mechanism["ci"] = 1.96 * mechanism["std_error"]
    fig, axes = plt.subplots(1, 2, figsize=(12.8, 6.0))
    for ax, market in zip(axes, ["CN", "US"], strict=True):
        subset = mechanism.loc[mechanism["market"] == market].copy()
        subset["label"] = subset["phase_label"] + " · " + subset["metric_label"]
        order = [
            "公告日 · 换手率变化",
            "公告日 · 成交量变化",
            "公告日 · 波动率变化",
            "生效日 · 换手率变化",
            "生效日 · 成交量变化",
            "生效日 · 波动率变化",
        ]
        subset = subset.set_index("label").reindex(order).dropna(subset=["coefficient"]).reset_index()
        color = "#a63b28" if market == "CN" else "#0f5c6e"
        ax.axvline(0, color="#8894a0", linewidth=1.2, linestyle="--")
        for idx, (_, row) in enumerate(subset.iterrows()):
            ax.errorbar(
                row["coefficient"],
                idx,
                xerr=row["ci"],
                fmt="o",
                color=color,
                ecolor=color,
                elinewidth=1.8,
                capsize=4,
                markersize=7,
            )
            ax.text(
                row["coefficient"] + (0.01 if row["coefficient"] >= 0 else -0.01),
                idx + 0.16,
                significance_stars(float(row["p_value"])),
                color=color,
                fontsize=11,
                ha="left" if row["coefficient"] >= 0 else "right",
            )
        ax.set_yticks(range(len(subset)))
        ax.set_yticklabels(subset["label"], fontsize=10)
        ax.set_title(market_labels[market], fontsize=14)
        ax.grid(axis="x", alpha=0.25)
        ax.invert_yaxis()
    fig.supxlabel("处理组变量估计系数", fontsize=12)
    fig.suptitle("机制回归处理组系数与 95% 置信区间", fontsize=16, y=0.98)
    fig.tight_layout()
    fig.savefig(mechanism_path, dpi=220)
    plt.close(fig)

    matched_total = len(diagnostics)
    status_counts = diagnostics["status"].value_counts().sort_values(ascending=False)
    sector_relaxed = diagnostics["sector_relaxed"].where(diagnostics["sector_relaxed"].notna(), False).astype(bool)
    metrics = {
        "匹配成功率": (diagnostics["status"] == "matched").mean(),
        "完整三对照比例": (diagnostics["selected_controls"] == 3).mean(),
        "行业口径放宽占比": sector_relaxed.mean(),
    }
    fig, axes = plt.subplots(1, 2, figsize=(11.8, 4.8))
    ax = axes[0]
    colors = ["#0f5c6e" if status == "matched" else "#c36a2d" for status in status_counts.index]
    ax.bar(status_counts.index, status_counts.values, color=colors, width=0.6)
    for idx, value in enumerate(status_counts.values):
        ax.text(idx, value + 0.6, f"{int(value)}", ha="center", va="bottom", fontsize=11)
    ax.set_title("匹配状态分布", fontsize=14)
    ax.set_ylabel("事件数")
    ax.grid(axis="y", alpha=0.22)
    ax.set_axisbelow(True)
    ax.text(
        0.02,
        0.94,
        f"总事件数：{matched_total}",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=10,
        color="#445463",
    )

    ax = axes[1]
    metric_labels = list(metrics.keys())
    metric_values = list(metrics.values())
    y_positions = list(range(len(metric_labels)))
    ax.barh(y_positions, metric_values, color=["#0f5c6e", "#1f7a8c", "#a63b28"], height=0.52)
    for idx, value in enumerate(metric_values):
        ax.text(value + 0.015, idx, format_share(value), va="center", fontsize=11, color="#223546")
    ax.set_yticks(y_positions)
    ax.set_yticklabels(metric_labels, fontsize=11)
    ax.set_xlim(0, 1.05)
    ax.set_title("匹配质量概览", fontsize=14)
    ax.set_xlabel("比例")
    ax.grid(axis="x", alpha=0.22)
    ax.set_axisbelow(True)
    fig.suptitle("匹配诊断图", fontsize=16, y=0.99)
    fig.tight_layout()
    fig.savefig(match_path, dpi=220)
    plt.close(fig)

    return _sample_design_figure_entries(target_dir, to_relative)
