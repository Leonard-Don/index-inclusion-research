from __future__ import annotations

from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
import statsmodels.api as sm

matplotlib.use("Agg")
from matplotlib import pyplot as plt

plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

LEFT_COLOR = "#a63b28"
RIGHT_COLOR = "#0f5c6e"
LEFT_SOFT = "#d9b39d"
RIGHT_SOFT = "#9fc7cf"

OUTCOME_LABELS = {
    "car_m1_p1": "CAR[-1,+1]",
    "car_m3_p3": "CAR[-3,+3]",
    "car_m5_p5": "CAR[-5,+5]",
    "turnover_change": "换手率变化",
    "volume_change": "成交量变化",
    "volatility_change": "波动率变化",
}


def choose_bandwidth(centered_running: pd.Series) -> float:
    clean = centered_running.dropna().astype(float)
    if clean.empty:
        return 0.0
    sigma = clean.std(ddof=1)
    n_obs = len(clean)
    if n_obs <= 1 or pd.isna(sigma) or sigma == 0:
        return float(clean.abs().max())
    bandwidth = 1.84 * sigma * (n_obs ** (-1 / 5))
    return float(max(bandwidth, clean.abs().quantile(0.4)))


def fit_local_linear_rdd(
    frame: pd.DataFrame,
    outcome_col: str,
    running_col: str = "distance_to_cutoff",
    treatment_col: str = "inclusion",
    bandwidth: float | None = None,
) -> dict[str, float | int | str]:
    work = frame[[outcome_col, running_col, treatment_col]].replace([np.inf, -np.inf], np.nan).dropna().copy()
    if work.empty:
        return {
            "outcome": outcome_col,
            "bandwidth": np.nan,
            "n_obs": 0,
            "n_left": 0,
            "n_right": 0,
            "tau": np.nan,
            "std_error": np.nan,
            "t_stat": np.nan,
            "p_value": np.nan,
            "r_squared": np.nan,
        }

    inferred_bandwidth = choose_bandwidth(work[running_col]) if bandwidth is None else float(bandwidth)
    local = work.loc[work[running_col].abs() <= inferred_bandwidth].copy()
    if local.empty:
        local = work.copy()
        inferred_bandwidth = float(local[running_col].abs().max())

    local["interaction"] = local[treatment_col] * local[running_col]
    design = sm.add_constant(local[[treatment_col, running_col, "interaction"]], has_constant="add")
    model = sm.OLS(local[outcome_col], design).fit(cov_type="HC1")
    return {
        "outcome": outcome_col,
        "bandwidth": inferred_bandwidth,
        "n_obs": int(model.nobs),
        "n_left": int((local[running_col] < 0).sum()),
        "n_right": int((local[running_col] >= 0).sum()),
        "tau": float(model.params[treatment_col]),
        "std_error": float(model.bse[treatment_col]),
        "t_stat": float(model.tvalues[treatment_col]),
        "p_value": float(model.pvalues[treatment_col]),
        "r_squared": float(model.rsquared),
    }


def run_rdd_suite(
    frame: pd.DataFrame,
    outcome_cols: list[str],
    running_col: str = "distance_to_cutoff",
    treatment_col: str = "inclusion",
    bandwidth: float | None = None,
) -> pd.DataFrame:
    rows = [
        fit_local_linear_rdd(
            frame=frame,
            outcome_col=outcome_col,
            running_col=running_col,
            treatment_col=treatment_col,
            bandwidth=bandwidth,
        )
        for outcome_col in outcome_cols
    ]
    return pd.DataFrame(rows)


def plot_rdd_bins(
    frame: pd.DataFrame,
    outcome_col: str,
    output_path: str | Path,
    running_col: str = "distance_to_cutoff",
    treatment_col: str = "inclusion",
    bandwidth: float | None = None,
    bins_per_side: int = 6,
) -> None:
    work = frame[[outcome_col, running_col, treatment_col]].replace([np.inf, -np.inf], np.nan).dropna().copy()
    if work.empty:
        return

    inferred_bandwidth = choose_bandwidth(work[running_col]) if bandwidth is None else float(bandwidth)
    local = work.loc[work[running_col].abs() <= inferred_bandwidth].copy()
    if local.empty:
        return

    left = local.loc[local[running_col] < 0].copy()
    right = local.loc[local[running_col] >= 0].copy()

    def _bin_side(side: pd.DataFrame) -> pd.DataFrame:
        if side.empty:
            return pd.DataFrame(columns=["bin_center", "mean_outcome"])
        ranks = pd.qcut(side[running_col], q=min(bins_per_side, len(side)), duplicates="drop")
        return (
            side.assign(bin=ranks)
            .groupby("bin", dropna=False, observed=False)
            .agg(bin_center=(running_col, "mean"), mean_outcome=(outcome_col, "mean"))
            .reset_index(drop=True)
        )

    left_bins = _bin_side(left)
    right_bins = _bin_side(right)
    outcome_label = OUTCOME_LABELS.get(outcome_col, outcome_col)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(8.5, 6))
    if not left_bins.empty:
        ax.scatter(
            left_bins["bin_center"],
            left_bins["mean_outcome"],
            label="断点左侧样本",
            color=LEFT_COLOR,
            s=72,
        )
    if not right_bins.empty:
        ax.scatter(
            right_bins["bin_center"],
            right_bins["mean_outcome"],
            label="断点右侧样本",
            color=RIGHT_COLOR,
            s=72,
        )
    if not left.empty:
        ax.scatter(left[running_col], left[outcome_col], color=LEFT_SOFT, alpha=0.18, s=22)
    if not right.empty:
        ax.scatter(right[running_col], right[outcome_col], color=RIGHT_SOFT, alpha=0.18, s=22)
    ax.axvline(0, color="#5c6b77", linestyle="--", linewidth=1.2)
    ax.set_title(f"{outcome_label} 断点回归分箱图", pad=12)
    ax.set_xlabel("距断点距离")
    ax.set_ylabel(outcome_label)
    ax.legend(frameon=False)
    ax.grid(alpha=0.24)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    fig.savefig(output, dpi=180)
    plt.close(fig)
