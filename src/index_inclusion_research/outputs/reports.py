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

PHASE_LABELS = {
    "announce": "公告日",
    "effective": "生效日",
}

INCLUSION_LABELS = {
    1: "纳入样本",
    0: "匹配对照组",
}


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
        for inclusion, inclusion_group in group.groupby("inclusion", dropna=False):
            label = INCLUSION_LABELS.get(int(inclusion), str(inclusion))
            ax.plot(
                inclusion_group["relative_day"],
                inclusion_group["mean_car"],
                marker="o",
                linewidth=1.8,
                label=label,
            )
        ax.axvline(0, color="black", linestyle="--", linewidth=1)
        market_label = MARKET_LABELS.get(str(market), str(market))
        phase_label = PHASE_LABELS.get(str(event_phase), str(event_phase))
        ax.set_title(f"{market_label}{phase_label}平均累计异常收益路径")
        ax.set_xlabel("相对交易日")
        ax.set_ylabel("平均累计异常收益")
        ax.legend()
        ax.grid(alpha=0.3)
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
