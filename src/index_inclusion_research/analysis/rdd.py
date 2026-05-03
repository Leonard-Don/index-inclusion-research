from __future__ import annotations

from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats

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
            "intercept": np.nan,
            "running_slope": np.nan,
            "interaction_slope": np.nan,
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
        "intercept": float(model.params["const"]),
        "running_slope": float(model.params[running_col]),
        "interaction_slope": float(model.params["interaction"]),
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


def compute_mccrary_density_test(
    frame: pd.DataFrame,
    *,
    running_col: str = "distance_to_cutoff",
    bandwidth: float | None = None,
    bin_size: float | None = None,
    n_bootstrap: int = 500,
    seed: int = 42,
) -> dict[str, float | int]:
    """Histogram-based density discontinuity test (McCrary 2008 spirit).

    Tests whether the density of the running variable jumps at the cutoff.
    A significant positive jump indicates units may be sorting in
    (manipulation); a significant negative jump indicates units sorting out.

    Returns a dict with `log_density_diff`, `std_error`, `z_stat`, `p_value`,
    `n_obs`, `bandwidth`, `bin_size`. NaN-filled when the sample is empty
    or both sides of the cutoff have zero local mass.

    This is not the original triangular-kernel local-linear estimator from
    McCrary (2008); it is a histogram approximation suitable for the
    relatively coarse running variables used in index-inclusion RDDs.
    """
    distances = pd.to_numeric(frame[running_col], errors="coerce").dropna().to_numpy(dtype=float)
    n_obs = int(distances.size)
    result: dict[str, float | int] = {
        "n_obs": n_obs,
        "bandwidth": float("nan"),
        "bin_size": float("nan"),
        "log_density_diff": float("nan"),
        "std_error": float("nan"),
        "z_stat": float("nan"),
        "p_value": float("nan"),
    }
    if n_obs == 0:
        return result

    sigma = float(np.std(distances, ddof=1)) if n_obs > 1 else 0.0
    if bandwidth is None:
        if sigma > 0:
            bandwidth = max(1.84 * sigma * (n_obs ** (-1 / 5)), 1e-6)
        else:
            bandwidth = float(np.max(np.abs(distances)) or 1.0)
    if bin_size is None:
        bin_size = max(bandwidth / 5.0, 1e-6)

    def _density_diff(sample: np.ndarray, current_bin: float) -> float:
        n = sample.size
        if n == 0:
            return float("nan")
        n_left = int(np.sum((sample >= -current_bin) & (sample < 0.0)))
        n_right = int(np.sum((sample >= 0.0) & (sample < current_bin)))
        if n_left == 0 or n_right == 0:
            return float("nan")
        density_left = n_left / (current_bin * n)
        density_right = n_right / (current_bin * n)
        return float(np.log(density_right) - np.log(density_left))

    # Auto-widen bin if discrete running variable produces empty cells,
    # up to the full bandwidth. Useful for ordinal / rank-style running
    # variables (HS300 adjustment list order).
    point = _density_diff(distances, bin_size)
    while not np.isfinite(point) and bin_size < bandwidth:
        bin_size = min(bin_size * 2.0, bandwidth)
        point = _density_diff(distances, bin_size)

    result["bandwidth"] = float(bandwidth)
    result["bin_size"] = float(bin_size)
    if not np.isfinite(point):
        return result

    rng = np.random.default_rng(seed)
    boot = np.empty(n_bootstrap, dtype=float)
    for i in range(n_bootstrap):
        sample = rng.choice(distances, size=n_obs, replace=True)
        boot[i] = _density_diff(sample, bin_size)
    boot = boot[np.isfinite(boot)]
    if boot.size < 5:
        result["log_density_diff"] = point
        return result

    se = float(np.std(boot, ddof=1))
    if se <= 0:
        result["log_density_diff"] = point
        result["std_error"] = se
        return result

    z = float(point / se)
    p = float(2.0 * stats.norm.sf(abs(z)))
    result.update(
        {
            "log_density_diff": float(point),
            "std_error": se,
            "z_stat": z,
            "p_value": p,
        }
    )
    return result


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
