from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import statsmodels.api as sm  # noqa: E402

_VALID_DIMS: tuple[str, ...] = ("size", "liquidity", "sector", "gap_bucket")
EPS = 1e-4
H7_MIN_EVENTS_PER_SECTOR = 10


class _SupportsFTest(Protocol):
    def f_test(self, r_matrix: np.ndarray) -> Any: ...


def _pre_event_mean(panel: pd.DataFrame, col: str, lo: int = -20, hi: int = -1) -> pd.Series:
    sub = panel.loc[(panel["relative_day"] >= lo) & (panel["relative_day"] <= hi)]
    return sub.groupby(["event_id", "market"])[col].mean()


def _within_market_quintile(values: pd.Series, markets: pd.Series) -> pd.Series:
    result = pd.Series(index=values.index, dtype="object")
    for market in markets.dropna().unique():
        mask = markets == market
        sub = values.loc[mask]
        if sub.dropna().nunique() <= 1:
            result.loc[mask] = "Q1"
            continue
        try:
            bins = pd.qcut(
                sub,
                5,
                labels=["Q1", "Q2", "Q3", "Q4", "Q5"],
                duplicates="drop",
            )
            result.loc[mask] = bins.astype(str).to_numpy()
        except ValueError:
            ranks = sub.rank(method="first")
            bins = pd.qcut(ranks, 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"])
            result.loc[mask] = bins.astype(str).to_numpy()
    return result


def build_heterogeneity_panel(
    panel: pd.DataFrame,
    *,
    dim: str,
    gap_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if dim not in _VALID_DIMS:
        raise ValueError(f"unknown heterogeneity dim: {dim}")
    work = panel.loc[panel["event_type"] == "addition"].copy()
    events = work[["event_id", "market"]].drop_duplicates().reset_index(drop=True)

    if dim == "size":
        pre = _pre_event_mean(work, "mkt_cap")
        lookup = pd.MultiIndex.from_frame(events[["event_id", "market"]])
        values = pre.reindex(lookup).reset_index(drop=True)
        events["bucket"] = _within_market_quintile(values, events["market"])
    elif dim == "liquidity":
        pre = _pre_event_mean(work, "turnover")
        lookup = pd.MultiIndex.from_frame(events[["event_id", "market"]])
        values = pre.reindex(lookup).reset_index(drop=True)
        events["bucket"] = _within_market_quintile(values, events["market"])
    elif dim == "sector":
        sector = (
            work.groupby("event_id")["sector"].first().reindex(events["event_id"]).fillna("Unknown")
        )
        events["bucket"] = sector.astype(str).to_numpy()
    elif dim == "gap_bucket":
        if gap_frame is None:
            raise ValueError("gap_frame required for dim=gap_bucket")
        gap_lookup = gap_frame.set_index("event_id")["gap_length_days"]
        gl = gap_lookup.reindex(events["event_id"]).reset_index(drop=True)
        events["bucket"] = np.where(
            gl <= 10,
            "≤10",
            np.where(gl <= 20, "11-20", ">20"),
        )

    events["dim"] = dim
    return events[["event_id", "market", "dim", "bucket"]]


def _event_window_car(panel: pd.DataFrame, phase: str, lo: int, hi: int) -> pd.Series:
    sub = panel.loc[
        (panel["event_phase"] == phase)
        & (panel["relative_day"] >= lo)
        & (panel["relative_day"] <= hi)
    ]
    return sub.groupby(["event_id", "market"])["ar"].sum()


def compute_cell_statistics(
    panel: pd.DataFrame,
    buckets: pd.DataFrame,
    *,
    gap_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    announce = (
        _event_window_car(panel, "announce", -1, 1).rename("announce_car").reset_index()
    )
    effective = (
        _event_window_car(panel, "effective", -1, 1).rename("effective_car").reset_index()
    )
    merged = buckets.merge(announce, on=["event_id", "market"], how="left").merge(
        effective, on=["event_id", "market"], how="left"
    )
    if gap_frame is not None:
        if "gap_drift" in gap_frame.columns:
            gap_map = gap_frame[["event_id", "gap_drift"]]
        else:
            gap_map = gap_frame[["event_id", "gap_length_days"]].rename(
                columns={"gap_length_days": "gap_drift"}
            )
        merged = merged.merge(gap_map, on="event_id", how="left")
    else:
        merged["gap_drift"] = 0.0

    stats = merged.groupby(["market", "dim", "bucket"], as_index=False).agg(
        announce_car=("announce_car", "mean"),
        effective_car=("effective_car", "mean"),
        gap_drift=("gap_drift", "mean"),
        n_events=("event_id", "nunique"),
    )
    stats["asymmetry_index"] = (stats["effective_car"] + stats["gap_drift"]) / (
        stats["announce_car"].abs() + EPS
    )
    return stats


def render_heterogeneity_matrix(
    stats: pd.DataFrame,
    *,
    dim: str,
    output_dir: Path,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    sub = stats.loc[stats["dim"] == dim] if "dim" in stats.columns else stats
    pivot = sub.pivot_table(
        index="bucket", columns="market", values="asymmetry_index", aggfunc="first"
    )
    fig, ax = plt.subplots(figsize=(6, 4))
    im = ax.imshow(pivot.values, cmap="RdBu_r", aspect="auto")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    fig.colorbar(im, ax=ax, label="asymmetry_index")
    ax.set_title(f"Heterogeneity ({dim})")
    fig.tight_layout()
    out_path = output_dir / f"cma_heterogeneity_matrix_{dim}.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def export_heterogeneity_tables(
    tables: dict[str, pd.DataFrame],
    *,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out: dict[str, Path] = {}
    for dim, frame in tables.items():
        out_path = output_dir / f"cma_heterogeneity_{dim}.csv"
        frame.to_csv(out_path, index=False)
        out[dim] = out_path
    return out


def compute_h7_sector_interaction(
    mechanism_panel: pd.DataFrame,
    *,
    min_obs_per_sector: int = H7_MIN_EVENTS_PER_SECTOR,
    outcome: str = "car_1_1",
) -> pd.DataFrame:
    """Estimate within-market sector interaction checks for H7.

    The H7 spread table is descriptive. This regression table adds a
    model-based companion by asking whether sector buckets change the
    treatment effect and announce/effective phase pattern within each
    market.
    """
    required = {"market", "event_phase", "treatment_group", "sector", outcome}
    missing = sorted(required - set(mechanism_panel.columns))
    if missing:
        return pd.DataFrame(
            [
                {
                    "market": "ALL",
                    "status": "missing",
                    "signal": "insufficient",
                    "n_obs": 0,
                    "sector_count": 0,
                    "eligible_sectors": "",
                    "collapsed_sector_rows": 0,
                    "joint_p_value": float("nan"),
                    "min_term_p_value": float("nan"),
                    "max_abs_t": float("nan"),
                    "top_term": "",
                    "r_squared": float("nan"),
                    "note": f"missing columns: {', '.join(missing)}",
                }
            ]
        )

    rows: list[dict[str, object]] = []
    for market in sorted(mechanism_panel["market"].dropna().astype(str).unique()):
        rows.append(
            _fit_h7_market_sector_interaction(
                mechanism_panel,
                market=market,
                min_obs_per_sector=min_obs_per_sector,
                outcome=outcome,
            )
        )
    return pd.DataFrame(rows)


def _fit_h7_market_sector_interaction(
    mechanism_panel: pd.DataFrame,
    *,
    market: str,
    min_obs_per_sector: int,
    outcome: str,
) -> dict[str, object]:
    sub = mechanism_panel.loc[
        mechanism_panel["market"].astype(str) == market,
        ["market", "event_phase", "treatment_group", "sector", outcome],
    ].copy()
    sub = sub.dropna(subset=["event_phase", "treatment_group", "sector", outcome])
    sub["sector"] = sub["sector"].astype(str).str.strip()
    sub = sub.loc[(sub["sector"] != "") & (sub["sector"].str.lower() != "unknown")]
    n_obs = int(len(sub))
    base: dict[str, object] = {
        "market": market,
        "status": "warn",
        "signal": "insufficient",
        "n_obs": n_obs,
        "sector_count": 0,
        "eligible_sectors": "",
        "collapsed_sector_rows": 0,
        "joint_p_value": float("nan"),
        "min_term_p_value": float("nan"),
        "max_abs_t": float("nan"),
        "top_term": "",
        "r_squared": float("nan"),
        "note": "",
    }
    if n_obs < max(12, min_obs_per_sector * 2):
        return {**base, "note": "too few rows after sector/outcome cleanup"}
    if sub["treatment_group"].nunique() < 2:
        return {**base, "note": "treatment/control variation missing"}
    if sub["event_phase"].nunique() < 2:
        return {**base, "note": "announce/effective phase variation missing"}

    counts = sub["sector"].value_counts()
    eligible = sorted(counts.loc[counts >= min_obs_per_sector].index.astype(str))
    if len(eligible) < 2:
        return {
            **base,
            "sector_count": len(eligible),
            "eligible_sectors": " | ".join(eligible),
            "note": f"fewer than 2 sectors with n>={min_obs_per_sector}",
        }
    sub["sector_bucket"] = np.where(sub["sector"].isin(eligible), sub["sector"], "Other")
    collapsed_rows = int((sub["sector_bucket"] == "Other").sum())
    sector_count = int(sub["sector_bucket"].nunique())
    if sector_count < 2:
        return {
            **base,
            "sector_count": sector_count,
            "eligible_sectors": " | ".join(eligible),
            "collapsed_sector_rows": collapsed_rows,
            "note": "sector buckets collapsed to a single group",
        }

    treatment = sub["treatment_group"].astype(float).reset_index(drop=True)
    effective = (
        sub["event_phase"].astype(str).str.lower().eq("effective").astype(float).reset_index(drop=True)
    )
    X = pd.DataFrame(
        {
            "treatment_group": treatment,
            "is_effective": effective,
            "treatment_x_effective": treatment * effective,
        }
    )
    sector_dummies = pd.get_dummies(
        sub["sector_bucket"].reset_index(drop=True),
        prefix="sector",
        drop_first=True,
        dtype=float,
    )
    interaction_terms: list[str] = []
    for column in sector_dummies.columns:
        X[column] = sector_dummies[column]
        tx_name = f"treatment_x_{column}"
        px_name = f"effective_x_{column}"
        X[tx_name] = treatment * sector_dummies[column]
        X[px_name] = effective * sector_dummies[column]
        interaction_terms.extend([tx_name, px_name])

    y = sub[outcome].astype(float).reset_index(drop=True)
    finite = np.isfinite(X.to_numpy(dtype=float)).all(axis=1) & np.isfinite(y.to_numpy())
    X = X.loc[finite].reset_index(drop=True)
    y = y.loc[finite].reset_index(drop=True)
    n_fit = int(len(y))
    if n_fit <= X.shape[1] + 2:
        return {
            **base,
            "n_obs": n_fit,
            "sector_count": sector_count,
            "eligible_sectors": " | ".join(eligible),
            "collapsed_sector_rows": collapsed_rows,
            "note": "regression underdetermined after dummy expansion",
        }
    if float(y.var(ddof=0)) == 0.0:
        return {
            **base,
            "n_obs": n_fit,
            "sector_count": sector_count,
            "eligible_sectors": " | ".join(eligible),
            "collapsed_sector_rows": collapsed_rows,
            "note": "constant outcome",
        }

    X_const = sm.add_constant(X, has_constant="add")
    try:
        model = sm.OLS(y, X_const).fit(cov_type="HC3")
    except (np.linalg.LinAlgError, ValueError) as exc:
        return {
            **base,
            "n_obs": n_fit,
            "sector_count": sector_count,
            "eligible_sectors": " | ".join(eligible),
            "collapsed_sector_rows": collapsed_rows,
            "note": f"OLS failed: {type(exc).__name__}",
        }

    present_terms = [term for term in interaction_terms if term in X_const.columns]
    joint_p = _joint_p_value(model, list(X_const.columns), present_terms)
    term_p = model.pvalues.reindex(present_terms).dropna()
    term_t = model.tvalues.reindex(present_terms).dropna()
    min_term_p = float(term_p.min()) if not term_p.empty else float("nan")
    if term_t.empty:
        max_abs_t = float("nan")
        top_term = ""
    else:
        top_term = str(term_t.abs().idxmax())
        max_abs_t = float(term_t.loc[top_term])
    signal = "support" if joint_p == joint_p and joint_p < 0.10 else "weak"
    return {
        "market": market,
        "status": "pass",
        "signal": signal,
        "n_obs": n_fit,
        "sector_count": sector_count,
        "eligible_sectors": " | ".join(eligible),
        "collapsed_sector_rows": collapsed_rows,
        "joint_p_value": joint_p,
        "min_term_p_value": min_term_p,
        "max_abs_t": max_abs_t,
        "top_term": top_term,
        "r_squared": float(model.rsquared),
        "note": (
            "OLS HC3: car_1_1 ~ treatment + phase + sector + "
            "treatment×sector + phase×sector"
        ),
    }


def _joint_p_value(model: _SupportsFTest, columns: list[str], terms: list[str]) -> float:
    if not terms:
        return float("nan")
    R = np.zeros((len(terms), len(columns)))
    index = {name: pos for pos, name in enumerate(columns)}
    for row_idx, term in enumerate(terms):
        R[row_idx, index[term]] = 1.0
    try:
        test = model.f_test(R)
    except (ValueError, np.linalg.LinAlgError):
        return float("nan")
    try:
        return float(test.pvalue)
    except (TypeError, ValueError):
        return float("nan")


def export_h7_sector_interaction(
    frame: pd.DataFrame,
    *,
    output_dir: Path,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "cma_h7_sector_interaction.csv"
    frame.to_csv(out_path, index=False)
    return out_path
