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
