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


def fit_donut_rdd(
    frame: pd.DataFrame,
    outcome_col: str,
    *,
    running_col: str = "distance_to_cutoff",
    treatment_col: str = "inclusion",
    bandwidth: float | None = None,
    donut_radius: float = 0.01,
) -> dict[str, float | int | str]:
    """Local-linear RDD fit with a donut hole around the cutoff.

    Drops observations with ``|running| < donut_radius`` to neutralize
    manipulation suspicion (Barreca et al. 2011). Returns the same dict
    shape as ``fit_local_linear_rdd`` plus a ``donut_radius`` field.
    """
    work = frame.copy()
    if running_col in work.columns:
        outside = work[running_col].abs() >= donut_radius
        work = work.loc[outside]
    result = fit_local_linear_rdd(
        work,
        outcome_col=outcome_col,
        running_col=running_col,
        treatment_col=treatment_col,
        bandwidth=bandwidth,
    )
    result["donut_radius"] = float(donut_radius)
    return result


def fit_placebo_rdd(
    frame: pd.DataFrame,
    outcome_col: str,
    *,
    running_col: str = "distance_to_cutoff",
    treatment_col: str = "inclusion",
    bandwidth: float | None = None,
    cutoff_shift: float = 0.05,
) -> dict[str, float | int | str]:
    """Placebo RDD: shift the cutoff to a non-existent boundary.

    Creates a synthetic running variable centered at ``cutoff_shift`` (so
    the actual cutoff is no longer at 0) and a synthetic placebo treatment
    indicator that flips at the shifted cutoff. A placebo τ that is
    significant suggests the original RDD picks up something other than
    the real boundary effect; a placebo τ near 0 supports the main
    estimate's identification.

    Returns the same dict shape plus ``cutoff_shift``.
    """
    work = frame.copy()
    if running_col in work.columns:
        work[running_col] = pd.to_numeric(work[running_col], errors="coerce") - cutoff_shift
    placebo_treatment_col = f"_placebo_treatment_{abs(cutoff_shift):.4f}"
    work[placebo_treatment_col] = (work[running_col] >= 0).astype(int)
    result = fit_local_linear_rdd(
        work,
        outcome_col=outcome_col,
        running_col=running_col,
        treatment_col=placebo_treatment_col,
        bandwidth=bandwidth,
    )
    result["cutoff_shift"] = float(cutoff_shift)
    return result


def fit_polynomial_rdd(
    frame: pd.DataFrame,
    outcome_col: str,
    *,
    running_col: str = "distance_to_cutoff",
    treatment_col: str = "inclusion",
    bandwidth: float | None = None,
    polynomial_order: int = 2,
) -> dict[str, float | int | str]:
    """Polynomial RDD: include higher-order terms of the running variable
    on each side. ``polynomial_order=1`` is the linear local-linear baseline;
    ``=2`` adds quadratic terms; higher orders are over-fit risks at small n.
    """
    if polynomial_order < 1:
        raise ValueError("polynomial_order must be >= 1")
    work = frame[[outcome_col, running_col, treatment_col]].replace([np.inf, -np.inf], np.nan).dropna().copy()
    nan_result: dict[str, float | int | str] = {
        "outcome": outcome_col,
        "bandwidth": np.nan,
        "polynomial_order": int(polynomial_order),
        "n_obs": 0,
        "n_left": 0,
        "n_right": 0,
        "tau": np.nan,
        "std_error": np.nan,
        "t_stat": np.nan,
        "p_value": np.nan,
        "r_squared": np.nan,
    }
    if work.empty:
        return nan_result

    inferred_bandwidth = (
        choose_bandwidth(work[running_col]) if bandwidth is None else float(bandwidth)
    )
    local = work.loc[work[running_col].abs() <= inferred_bandwidth].copy()
    if local.empty:
        local = work.copy()
        inferred_bandwidth = float(local[running_col].abs().max())

    design_cols: dict[str, pd.Series] = {treatment_col: local[treatment_col]}
    for order in range(1, polynomial_order + 1):
        col = running_col if order == 1 else f"{running_col}_pow{order}"
        design_cols[col] = local[running_col].pow(order)
        design_cols[f"{treatment_col}_x_{col}"] = local[treatment_col] * local[running_col].pow(order)
    design_frame = pd.DataFrame(design_cols, index=local.index)
    design = sm.add_constant(design_frame, has_constant="add")
    model = sm.OLS(local[outcome_col], design).fit(cov_type="HC1")
    return {
        "outcome": outcome_col,
        "bandwidth": inferred_bandwidth,
        "polynomial_order": int(polynomial_order),
        "n_obs": int(model.nobs),
        "n_left": int((local[running_col] < 0).sum()),
        "n_right": int((local[running_col] >= 0).sum()),
        "tau": float(model.params[treatment_col]),
        "std_error": float(model.bse[treatment_col]),
        "t_stat": float(model.tvalues[treatment_col]),
        "p_value": float(model.pvalues[treatment_col]),
        "r_squared": float(model.rsquared),
    }


def run_rdd_robustness(
    frame: pd.DataFrame,
    outcome_col: str = "car_m1_p1",
    *,
    running_col: str = "distance_to_cutoff",
    treatment_col: str = "inclusion",
    bandwidth: float | None = None,
    donut_radius: float = 0.01,
    placebo_shifts: tuple[float, ...] = (-0.05, 0.05),
    polynomial_orders: tuple[int, ...] = (2,),
) -> pd.DataFrame:
    """Combined RDD robustness panel for one outcome.

    Stacks the main local-linear fit with: donut-hole, placebo cutoffs at
    each shift, and higher-order polynomial fits. All specs use the SAME
    bandwidth the main fit picks (auto-chosen if ``bandwidth=None``) so
    the placebo / donut / polynomial estimates are comparable to the main
    estimate at the same window width — without this, an auto-bandwidth
    that moves with each shifted-cutoff sample produces apples-to-oranges
    τ that mislead reviewers into reading specification noise as evidence.

    Output frame has one row per spec, with ``spec`` / ``spec_kind`` columns
    plus the same numerics as ``fit_local_linear_rdd``.
    """
    rows: list[dict[str, float | int | str]] = []

    main = fit_local_linear_rdd(
        frame,
        outcome_col=outcome_col,
        running_col=running_col,
        treatment_col=treatment_col,
        bandwidth=bandwidth,
    )
    main["spec"] = "main · 局部线性"
    main["spec_kind"] = "main"
    rows.append(main)

    # Lock all subsequent specs to the bandwidth the main fit landed on.
    locked_bandwidth = (
        float(main["bandwidth"]) if not pd.isna(main["bandwidth"]) else bandwidth
    )

    donut = fit_donut_rdd(
        frame,
        outcome_col=outcome_col,
        running_col=running_col,
        treatment_col=treatment_col,
        bandwidth=locked_bandwidth,
        donut_radius=donut_radius,
    )
    donut["spec"] = f"donut(±{donut_radius:g})"
    donut["spec_kind"] = "donut"
    rows.append(donut)

    for shift in placebo_shifts:
        placebo = fit_placebo_rdd(
            frame,
            outcome_col=outcome_col,
            running_col=running_col,
            treatment_col=treatment_col,
            bandwidth=locked_bandwidth,
            cutoff_shift=shift,
        )
        sign = "+" if shift > 0 else ""
        placebo["spec"] = f"placebo cutoff {sign}{shift:g}"
        placebo["spec_kind"] = "placebo"
        rows.append(placebo)

    for order in polynomial_orders:
        poly = fit_polynomial_rdd(
            frame,
            outcome_col=outcome_col,
            running_col=running_col,
            treatment_col=treatment_col,
            bandwidth=locked_bandwidth,
            polynomial_order=order,
        )
        poly["spec"] = f"polynomial order={order}"
        poly["spec_kind"] = "polynomial"
        rows.append(poly)

    return pd.DataFrame(rows)


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
