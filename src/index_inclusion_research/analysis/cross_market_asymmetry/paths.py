from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from scipy import stats  # noqa: E402

REQUIRED_PANEL_COLUMNS: tuple[str, ...] = (
    "event_id",
    "market",
    "event_phase",
    "event_type",
    "relative_day",
    "ar",
)

DEFAULT_WINDOWS: tuple[tuple[int, int], ...] = (
    (-1, 1),
    (-3, 3),
    (-5, 5),
    (-20, -1),
    (2, 20),
    (0, 60),
)


def _require_columns(frame: pd.DataFrame, required: tuple[str, ...]) -> None:
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(f"missing columns in panel: {missing}")


def build_daily_ar_panel(panel: pd.DataFrame) -> pd.DataFrame:
    _require_columns(panel, REQUIRED_PANEL_COLUMNS)
    work = panel.loc[panel["event_type"] == "addition"].copy()
    work = work.loc[work["event_phase"].isin(("announce", "effective"))].copy()
    work = work.sort_values(
        ["event_id", "market", "event_phase", "relative_day"]
    ).reset_index(drop=True)
    work["car"] = work.groupby(["event_id", "market", "event_phase"])["ar"].cumsum()
    return work


def compute_average_paths(ar_panel: pd.DataFrame) -> pd.DataFrame:
    grouped = ar_panel.groupby(
        ["market", "event_phase", "relative_day"], as_index=False
    ).agg(
        n_events=("event_id", "nunique"),
        ar_mean=("ar", "mean"),
        ar_std=("ar", "std"),
        car_mean=("car", "mean"),
        car_std=("car", "std"),
    )
    grouped["ar_se"] = grouped["ar_std"] / grouped["n_events"].pow(0.5)
    grouped["car_se"] = grouped["car_std"] / grouped["n_events"].pow(0.5)
    grouped["ar_t"] = grouped["ar_mean"] / grouped["ar_se"].replace(0.0, pd.NA)
    grouped["car_t"] = grouped["car_mean"] / grouped["car_se"].replace(0.0, pd.NA)
    return grouped.drop(columns=["ar_std", "car_std"])


def compute_window_summary(
    ar_panel: pd.DataFrame,
    windows: list[tuple[int, int]] | tuple[tuple[int, int], ...] = DEFAULT_WINDOWS,
) -> pd.DataFrame:
    rows = []
    for (lo, hi) in windows:
        sub = ar_panel.loc[
            (ar_panel["relative_day"] >= lo) & (ar_panel["relative_day"] <= hi)
        ]
        per_event = sub.groupby(
            ["event_id", "market", "event_phase"], as_index=False
        )["ar"].sum()
        per_event = per_event.rename(columns={"ar": "car_window"})
        summary = per_event.groupby(["market", "event_phase"], as_index=False).agg(
            n_events=("event_id", "nunique"),
            car_mean=("car_window", "mean"),
            car_std=("car_window", "std"),
        )
        summary["car_se"] = summary["car_std"] / summary["n_events"].pow(0.5)
        summary["car_t"] = summary["car_mean"] / summary["car_se"].replace(0.0, pd.NA)
        summary["p_value"] = summary["car_t"].apply(
            lambda t: float(2 * (1 - stats.norm.cdf(abs(t))))
            if pd.notna(t)
            else pd.NA
        )
        summary["window_start"] = lo
        summary["window_end"] = hi
        rows.append(summary.drop(columns=["car_std"]))
    out = pd.concat(rows, ignore_index=True)
    return out[
        [
            "market",
            "event_phase",
            "window_start",
            "window_end",
            "car_mean",
            "car_se",
            "car_t",
            "p_value",
            "n_events",
        ]
    ]


_QUADRANTS: tuple[tuple[str, str], ...] = (
    ("CN", "announce"),
    ("CN", "effective"),
    ("US", "announce"),
    ("US", "effective"),
)


def _plot_quadrant(ax, data: pd.DataFrame, x_col: str, y_col: str, title: str) -> None:
    ax.plot(data[x_col], data[y_col], color="#1f6feb", linewidth=2)
    ax.axhline(0.0, color="#999", linestyle="--", linewidth=0.8)
    ax.axvline(0.0, color="#999", linestyle="--", linewidth=0.8)
    ax.set_title(title, fontsize=10)
    ax.set_xlabel("relative_day")


def _render_grid(avg: pd.DataFrame, value_col: str, ylabel: str) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(10, 7), sharex=True, sharey=True)
    for (market, phase), ax in zip(_QUADRANTS, axes.flat, strict=True):
        sub = avg.loc[
            (avg["market"] == market) & (avg["event_phase"] == phase)
        ].sort_values("relative_day")
        _plot_quadrant(ax, sub, "relative_day", value_col, f"{market} · {phase}")
        if ax in axes[:, 0]:
            ax.set_ylabel(ylabel)
    fig.suptitle(f"CMA {ylabel} path comparison", fontsize=12)
    fig.tight_layout()
    return fig


def render_path_figures(avg: pd.DataFrame, *, output_dir: Path) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ar_fig = _render_grid(avg, "ar_mean", "AR")
    ar_path = output_dir / "cma_ar_path_comparison.png"
    ar_fig.savefig(ar_path, dpi=150)
    plt.close(ar_fig)
    car_fig = _render_grid(avg, "car_mean", "CAR")
    car_path = output_dir / "cma_car_path_comparison.png"
    car_fig.savefig(car_path, dpi=150)
    plt.close(car_fig)
    return {"ar": ar_path, "car": car_path}


def export_path_tables(
    ar_panel: pd.DataFrame,
    avg: pd.DataFrame,
    window_summary: pd.DataFrame,
    *,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ar_rows = avg[
        ["market", "event_phase", "relative_day", "n_events", "ar_mean", "ar_se", "ar_t"]
    ]
    car_rows = avg[
        ["market", "event_phase", "relative_day", "n_events", "car_mean", "car_se", "car_t"]
    ]
    ar_path = output_dir / "cma_ar_path.csv"
    car_path = output_dir / "cma_car_path.csv"
    win_path = output_dir / "cma_window_summary.csv"
    ar_rows.to_csv(ar_path, index=False)
    car_rows.to_csv(car_path, index=False)
    window_summary.to_csv(win_path, index=False)
    return {"ar_path": ar_path, "car_path": car_path, "window_summary": win_path}
