from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import statsmodels.api as sm  # noqa: E402
from scipy import stats  # noqa: E402

_METRIC_COLUMNS: tuple[str, ...] = (
    "gap_length_days",
    "pre_announce_runup",
    "announce_jump",
    "gap_drift",
    "effective_jump",
    "post_effective_reversal",
)


def _window_sum(
    panel: pd.DataFrame,
    event_id: int,
    phase: str,
    lo: int,
    hi: int,
) -> float:
    sub = panel.loc[
        (panel["event_id"] == event_id)
        & (panel["event_phase"] == phase)
        & (panel["relative_day"] >= lo)
        & (panel["relative_day"] <= hi)
    ]
    if sub.empty:
        return float("nan")
    return float(sub["ar"].sum())


def compute_gap_metrics(events: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    work_events = events.loc[events["event_type"] == "addition"].copy()
    work_events["announce_date"] = pd.to_datetime(work_events["announce_date"])
    work_events["effective_date"] = pd.to_datetime(work_events["effective_date"])
    work_panel = panel.loc[panel["event_type"] == "addition"].copy()

    rows: list[dict[str, object]] = []
    for _, ev in work_events.iterrows():
        event_id = ev["event_id"]
        gap_days = (ev["effective_date"] - ev["announce_date"]).days
        pre_runup = _window_sum(work_panel, event_id, "announce", -20, -1)
        announce_jump = _window_sum(work_panel, event_id, "announce", -1, 1)
        effective_jump = _window_sum(work_panel, event_id, "effective", -1, 1)
        post_reversal = _window_sum(work_panel, event_id, "effective", 2, 20)
        gap_hi = max(gap_days - 1, 2)
        gap_drift = _window_sum(work_panel, event_id, "announce", 2, gap_hi)
        rows.append(
            {
                "event_id": event_id,
                "market": ev["market"],
                "ticker": ev["ticker"],
                "announce_date": ev["announce_date"].date().isoformat(),
                "effective_date": ev["effective_date"].date().isoformat(),
                "gap_length_days": gap_days,
                "pre_announce_runup": pre_runup,
                "announce_jump": announce_jump,
                "gap_drift": gap_drift,
                "effective_jump": effective_jump,
                "post_effective_reversal": post_reversal,
            }
        )
    return pd.DataFrame(rows)


def summarize_gap_metrics(gap: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for market, sub in gap.groupby("market"):
        for metric in _METRIC_COLUMNS:
            values = sub[metric].dropna()
            n = len(values)
            mean = float(values.mean()) if n else float("nan")
            median = float(values.median()) if n else float("nan")
            se = float(values.std(ddof=1) / (n**0.5)) if n > 1 else float("nan")
            t = mean / se if se == se and se not in (0.0, None) else float("nan")
            p = float(2 * (1 - stats.norm.cdf(abs(t)))) if t == t else float("nan")
            rows.append(
                {
                    "market": market,
                    "metric": metric,
                    "mean": mean,
                    "median": median,
                    "se": se,
                    "t": t,
                    "p_value": p,
                    "n_events": n,
                }
            )
    return pd.DataFrame(rows)


def render_gap_figures(
    gap: pd.DataFrame,
    summary: pd.DataFrame,
    *,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    fig1, ax1 = plt.subplots(figsize=(8, 5))
    for market, sub in gap.groupby("market"):
        ax1.hist(sub["gap_length_days"].dropna(), alpha=0.5, label=market, bins=20)
    ax1.set_xlabel("gap_length_days")
    ax1.set_ylabel("count")
    ax1.set_title("Announce-to-Effective gap length by market")
    ax1.legend()
    fig1.tight_layout()
    dist_path = output_dir / "cma_gap_length_distribution.png"
    fig1.savefig(dist_path, dpi=150)
    plt.close(fig1)

    segments = ("announce_jump", "gap_drift", "effective_jump", "post_effective_reversal")
    means = (
        summary.loc[summary["metric"].isin(segments)]
        .pivot_table(index="market", columns="metric", values="mean")
        .reindex(columns=list(segments))
    )
    fig2, ax2 = plt.subplots(figsize=(9, 5))
    means.plot.bar(ax=ax2)
    ax2.set_ylabel("mean CAR")
    ax2.set_title("Announce → Gap → Effective → Post decomposition")
    ax2.axhline(0.0, color="#999", linestyle="--", linewidth=0.7)
    fig2.tight_layout()
    decomp_path = output_dir / "cma_gap_decomposition.png"
    fig2.savefig(decomp_path, dpi=150)
    plt.close(fig2)

    return {"gap_distribution": dist_path, "gap_decomposition": decomp_path}


def export_gap_tables(
    gap: pd.DataFrame,
    summary: pd.DataFrame,
    *,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    event_path = output_dir / "cma_gap_event_level.csv"
    summary_path = output_dir / "cma_gap_summary.csv"
    gap.to_csv(event_path, index=False)
    summary.to_csv(summary_path, index=False)
    return {"event_level": event_path, "summary": summary_path}


def compute_pre_runup_bootstrap_test(
    gap_event_level: pd.DataFrame,
    *,
    n_boot: int = 5000,
    seed: int = 0,
) -> dict[str, float]:
    """Bootstrap test for H0: mean(CN pre_announce_runup) == mean(US pre_announce_runup).

    Resamples each market with replacement n_boot times, builds the empirical
    distribution of (CN_mean - US_mean), and reports a percentile two-sided
    p-value plus 95% CI. Returns NaN values when either market has < 2 events.
    """
    cn_values = (
        gap_event_level.loc[gap_event_level["market"] == "CN", "pre_announce_runup"]
        .dropna()
        .to_numpy()
    )
    us_values = (
        gap_event_level.loc[gap_event_level["market"] == "US", "pre_announce_runup"]
        .dropna()
        .to_numpy()
    )
    n_cn = int(cn_values.size)
    n_us = int(us_values.size)
    base = {
        "cn_mean": float(cn_values.mean()) if n_cn else float("nan"),
        "us_mean": float(us_values.mean()) if n_us else float("nan"),
        "diff_mean": float("nan"),
        "boot_p_value": float("nan"),
        "boot_ci_low": float("nan"),
        "boot_ci_high": float("nan"),
        "n_cn": n_cn,
        "n_us": n_us,
        "n_boot": 0,
    }
    if n_cn < 2 or n_us < 2:
        return base

    rng = np.random.default_rng(seed)
    cn_idx = rng.integers(0, n_cn, size=(n_boot, n_cn))
    us_idx = rng.integers(0, n_us, size=(n_boot, n_us))
    diffs = cn_values[cn_idx].mean(axis=1) - us_values[us_idx].mean(axis=1)
    diff_mean = float(cn_values.mean() - us_values.mean())
    if diff_mean > 0:
        p_one = float((diffs <= 0).mean())
    elif diff_mean < 0:
        p_one = float((diffs >= 0).mean())
    else:
        p_one = 0.5
    return {
        "cn_mean": float(cn_values.mean()),
        "us_mean": float(us_values.mean()),
        "diff_mean": diff_mean,
        "boot_p_value": float(min(2 * p_one, 1.0)),
        "boot_ci_low": float(np.percentile(diffs, 2.5)),
        "boot_ci_high": float(np.percentile(diffs, 97.5)),
        "n_cn": n_cn,
        "n_us": n_us,
        "n_boot": n_boot,
    }


def export_pre_runup_bootstrap_table(
    result: dict[str, float],
    *,
    output_dir: Path,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "cma_pre_runup_bootstrap.csv"
    pd.DataFrame([result]).to_csv(out_path, index=False)
    return out_path


def compute_gap_drift_cross_market_regression(
    gap_event_level: pd.DataFrame,
) -> dict[str, float]:
    """OLS HC3 regression: gap_drift ~ const + cn_dummy + gap_length_days.

    Tests H4 (short-sell constraints) by asking whether CN events have higher
    announce-to-effective drift than US events after controlling for the gap
    window length. Returns NaN-filled dict for empty / single-market /
    underdetermined inputs.
    """
    sub = gap_event_level.dropna(
        subset=["gap_drift", "gap_length_days", "market"]
    ).copy()
    sub = sub.loc[sub["market"].isin({"CN", "US"})]
    n_obs = int(len(sub))
    base = {
        "cn_coef": float("nan"),
        "cn_se": float("nan"),
        "cn_t": float("nan"),
        "cn_p_value": float("nan"),
        "gap_length_coef": float("nan"),
        "gap_length_p_value": float("nan"),
        "n_obs": n_obs,
        "r_squared": float("nan"),
    }
    if n_obs < 4 or sub["market"].nunique() < 2:
        return base
    X = pd.DataFrame(
        {
            "cn": (sub["market"] == "CN").astype(float).to_numpy(),
            "gap_length_days": sub["gap_length_days"].astype(float).to_numpy(),
        }
    )
    X = sm.add_constant(X, has_constant="add")
    y = sub["gap_drift"].astype(float).to_numpy()
    if float(y.var(ddof=0)) == 0.0 or n_obs <= X.shape[1] + 1:
        return base
    try:
        model = sm.OLS(y, X).fit(cov_type="HC3")
    except (np.linalg.LinAlgError, ValueError):
        return base
    return {
        "cn_coef": float(model.params.get("cn", float("nan"))),
        "cn_se": float(model.bse.get("cn", float("nan"))),
        "cn_t": float(model.tvalues.get("cn", float("nan"))),
        "cn_p_value": float(model.pvalues.get("cn", float("nan"))),
        "gap_length_coef": float(
            model.params.get("gap_length_days", float("nan"))
        ),
        "gap_length_p_value": float(
            model.pvalues.get("gap_length_days", float("nan"))
        ),
        "n_obs": int(model.nobs),
        "r_squared": float(model.rsquared),
    }


def export_gap_drift_cross_market_regression_table(
    result: dict[str, float],
    *,
    output_dir: Path,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "cma_gap_drift_market_regression.csv"
    pd.DataFrame([result]).to_csv(out_path, index=False)
    return out_path
