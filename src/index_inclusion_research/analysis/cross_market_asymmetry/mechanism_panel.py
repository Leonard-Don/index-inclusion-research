from __future__ import annotations

import logging
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import statsmodels.api as sm  # noqa: E402

logger = logging.getLogger(__name__)

PRICE_LIMIT_THRESHOLD = 0.099

OUTCOMES: tuple[str, ...] = (
    "car_1_1",
    "turnover_change",
    "volume_change",
    "volatility_change",
    "price_limit_hit_share",
)
SPECS: tuple[str, ...] = ("no_fe", "controls", "controls_fe")


def _window_mean(frame: pd.DataFrame, col: str, lo: int, hi: int) -> float:
    sub = frame.loc[
        (frame["relative_day"] >= lo) & (frame["relative_day"] <= hi), col
    ]
    return float(sub.mean()) if len(sub) else float("nan")


def _window_std(frame: pd.DataFrame, col: str, lo: int, hi: int) -> float:
    sub = frame.loc[
        (frame["relative_day"] >= lo) & (frame["relative_day"] <= hi), col
    ]
    return float(sub.std(ddof=1)) if len(sub) > 1 else float("nan")


def _window_sum(frame: pd.DataFrame, col: str, lo: int, hi: int) -> float:
    sub = frame.loc[
        (frame["relative_day"] >= lo) & (frame["relative_day"] <= hi), col
    ]
    return float(sub.sum()) if len(sub) else float("nan")


def build_mechanism_panel(matched_panel: pd.DataFrame) -> pd.DataFrame:
    work = matched_panel.loc[matched_panel["event_type"] == "addition"].copy()
    rows: list[dict[str, object]] = []
    for (event_id, market, phase), group in work.groupby(
        ["event_id", "market", "event_phase"]
    ):
        car_1_1 = _window_sum(group, "ar", -1, 1)
        turnover_change = _window_mean(group, "turnover", 0, 5) - _window_mean(
            group, "turnover", -20, -1
        )
        volume_change = _window_mean(group, "volume", 0, 5) - _window_mean(
            group, "volume", -20, -1
        )
        volatility_change = _window_std(group, "ret", 0, 5) - _window_std(
            group, "ret", -20, -1
        )
        pre_sub = group.loc[
            (group["relative_day"] >= -5) & (group["relative_day"] <= 5), "ret"
        ].abs()
        limit_share = (
            float((pre_sub >= PRICE_LIMIT_THRESHOLD).mean())
            if len(pre_sub)
            else float("nan")
        )
        pre_mktcap = _window_mean(group, "mkt_cap", -20, -1)
        pre_turnover = _window_mean(group, "turnover", -20, -1)
        rows.append(
            {
                "event_id": event_id,
                "market": market,
                "event_phase": phase,
                "treatment_group": int(group["treatment_group"].iloc[0]),
                "sector": group["sector"].iloc[0]
                if "sector" in group.columns
                else pd.NA,
                "car_1_1": car_1_1,
                "turnover_change": turnover_change,
                "volume_change": volume_change,
                "volatility_change": volatility_change,
                "price_limit_hit_share": limit_share,
                "log_mktcap_pre": np.log(pre_mktcap)
                if pre_mktcap == pre_mktcap and pre_mktcap > 0
                else float("nan"),
                "pre_turnover": pre_turnover,
            }
        )
    return pd.DataFrame(rows)


def estimate_quadrant_regression(
    panel: pd.DataFrame,
    *,
    market: str,
    event_phase: str,
    outcome: str,
    spec: str = "no_fe",
) -> dict[str, object]:
    sub = panel.loc[
        (panel["market"] == market) & (panel["event_phase"] == event_phase)
    ].copy()
    sub = sub.dropna(subset=[outcome, "treatment_group"])
    empty_result = {
        "market": market,
        "event_phase": event_phase,
        "outcome": outcome,
        "spec": spec,
        "coef": float("nan"),
        "se": float("nan"),
        "t": float("nan"),
        "p_value": float("nan"),
        "n_obs": int(len(sub)),
        "r_squared": float("nan"),
    }
    if sub.empty or sub["treatment_group"].nunique() < 2:
        return empty_result
    design_cols: list[str] = ["treatment_group"]
    if spec in ("controls", "controls_fe"):
        for col in ("log_mktcap_pre", "pre_turnover"):
            if col in sub.columns and sub[col].notna().any():
                design_cols.append(col)
    # Drop rows whose design columns have NaN/inf — statsmodels' OLS rejects
    # those before HC3 even runs ("exog contains inf or nans").
    finite_design = np.isfinite(sub[design_cols].astype(float).to_numpy()).all(axis=1)
    sub = sub.loc[finite_design].copy()
    if sub.empty or sub["treatment_group"].nunique() < 2:
        return {**empty_result, "n_obs": int(len(sub))}
    X = sub[design_cols].astype(float)
    if spec == "controls_fe" and "sector" in sub.columns:
        sector_dummies = pd.get_dummies(
            sub["sector"], prefix="sector", drop_first=True, dtype=float
        )
        X = pd.concat([X, sector_dummies], axis=1)
    X = sm.add_constant(X, has_constant="add")
    y = sub[outcome].astype(float)
    n_obs = len(sub)
    # HC3 needs leverage h<1 for every obs, which fails if n_obs <= n_params;
    # require >=2 residual df. Constant y also kills R² (centered_tss==0).
    if n_obs <= X.shape[1] + 1 or float(y.var(ddof=0)) == 0.0:
        return {**empty_result, "n_obs": n_obs}
    try:
        model = sm.OLS(y, X).fit(cov_type="HC3")
    except (np.linalg.LinAlgError, ValueError) as exc:
        logger.debug(
            "OLS fit failed for %s/%s/%s/%s: %s",
            market, event_phase, outcome, spec, exc,
        )
        return {**empty_result, "n_obs": n_obs}
    return {
        "market": market,
        "event_phase": event_phase,
        "outcome": outcome,
        "spec": spec,
        "coef": float(model.params.get("treatment_group", float("nan"))),
        "se": float(model.bse.get("treatment_group", float("nan"))),
        "t": float(model.tvalues.get("treatment_group", float("nan"))),
        "p_value": float(model.pvalues.get("treatment_group", float("nan"))),
        "n_obs": int(model.nobs),
        "r_squared": float(model.rsquared),
    }


def assemble_mechanism_comparison_table(panel: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    markets = panel["market"].dropna().unique()
    phases = panel["event_phase"].dropna().unique()
    for market in markets:
        for phase in phases:
            for outcome in OUTCOMES:
                for spec in SPECS:
                    rows.append(
                        estimate_quadrant_regression(
                            panel,
                            market=market,
                            event_phase=phase,
                            outcome=outcome,
                            spec=spec,
                        )
                    )
    return pd.DataFrame(rows)


def render_mechanism_heatmap(
    table: pd.DataFrame,
    *,
    output_dir: Path,
    spec: str = "no_fe",
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    sub = table.loc[table["spec"] == spec].copy()
    sub["quadrant"] = sub["market"].astype(str) + "·" + sub["event_phase"].astype(str)
    pivot = sub.pivot_table(
        index="outcome", columns="quadrant", values="t", aggfunc="first"
    )
    fig, ax = plt.subplots(figsize=(9, 5))
    if pivot.empty or pivot.isna().all().all():
        ax.text(0.5, 0.5, "no data", ha="center", va="center", transform=ax.transAxes)
        ax.set_axis_off()
    else:
        im = ax.imshow(pivot.values, cmap="RdBu_r", vmin=-5, vmax=5, aspect="auto")
        ax.set_xticks(range(len(pivot.columns)))
        ax.set_xticklabels(pivot.columns, rotation=30, ha="right")
        ax.set_yticks(range(len(pivot.index)))
        ax.set_yticklabels(pivot.index)
        fig.colorbar(im, ax=ax, label="treatment t-stat")
    ax.set_title(f"Mechanism signed-t heatmap ({spec})")
    fig.tight_layout()
    out_path = output_dir / "cma_mechanism_heatmap.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


CHANNEL_SIG_LEVEL = 0.10


def compute_h5_limit_predictive_regression(
    mech_panel: pd.DataFrame,
    *,
    market: str = "CN",
) -> dict[str, float]:
    """OLS HC3 regression: car_1_1 ~ const + price_limit_hit_share + log_mktcap_pre.

    Tests H5 (涨跌停限制) by asking whether per-event price-limit-hit
    exposure predicts the announce-day CAR within the CN sub-sample
    (only CN has price-limit rules). Reports limit_coef and HC3 p-value
    plus n_obs, r_squared. NaN-safe guards for empty / underdetermined.
    """
    sub = mech_panel.dropna(
        subset=["car_1_1", "price_limit_hit_share", "log_mktcap_pre", "market"]
    ).copy()
    sub = sub.loc[sub["market"] == market]
    n_obs = int(len(sub))
    base = {
        "limit_coef": float("nan"),
        "limit_se": float("nan"),
        "limit_t": float("nan"),
        "limit_p_value": float("nan"),
        "n_obs": n_obs,
        "r_squared": float("nan"),
    }
    if n_obs < 5:
        return base

    X = pd.DataFrame(
        {
            "price_limit_hit_share": sub["price_limit_hit_share"].astype(float).to_numpy(),
            "log_mktcap_pre": sub["log_mktcap_pre"].astype(float).to_numpy(),
        }
    )
    finite = np.isfinite(X.to_numpy()).all(axis=1)
    sub = sub.loc[finite].copy()
    X = X.loc[finite]
    n_obs = int(len(sub))
    if n_obs <= X.shape[1] + 2:
        return {**base, "n_obs": n_obs}

    X = sm.add_constant(X, has_constant="add")
    y = sub["car_1_1"].astype(float).to_numpy()
    if float(y.var(ddof=0)) == 0.0:
        return {**base, "n_obs": n_obs}
    try:
        model = sm.OLS(y, X).fit(cov_type="HC3")
    except (np.linalg.LinAlgError, ValueError):
        return {**base, "n_obs": n_obs}
    return {
        "limit_coef": float(model.params.get("price_limit_hit_share", float("nan"))),
        "limit_se": float(model.bse.get("price_limit_hit_share", float("nan"))),
        "limit_t": float(model.tvalues.get("price_limit_hit_share", float("nan"))),
        "limit_p_value": float(model.pvalues.get("price_limit_hit_share", float("nan"))),
        "n_obs": int(model.nobs),
        "r_squared": float(model.rsquared),
    }


def export_h5_limit_predictive_regression_table(
    result: dict[str, float],
    *,
    output_dir: Path,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "cma_h5_limit_predictive_regression.csv"
    pd.DataFrame([result]).to_csv(out_path, index=False)
    return out_path


def compute_channel_concentration_table(
    table: pd.DataFrame,
    *,
    spec: str = "no_fe",
) -> pd.DataFrame:
    """Combine turnover_change and volume_change into one row per quadrant.

    Reads the wide mechanism comparison table (one row per market × phase ×
    outcome × spec) and pivots both turnover_change and volume_change onto
    a single row per (market, event_phase). The resulting frame is used by
    H3 to require BOTH channels to be significant before claiming
    institutional-vs-retail concentration; single-channel evidence is then
    explicitly reported as such.
    """
    sub = table.loc[
        (table["spec"] == spec)
        & (table["outcome"].isin({"turnover_change", "volume_change"}))
    ].copy()
    if sub.empty:
        return pd.DataFrame(
            columns=[
                "market", "event_phase",
                "turnover_coef", "turnover_t", "turnover_p",
                "volume_coef", "volume_t", "volume_p",
                "turnover_sig", "volume_sig", "both_channels_sig",
            ]
        )
    rows: list[dict[str, object]] = []
    for (market, phase), group in sub.groupby(["market", "event_phase"], dropna=False):
        by_outcome = {row["outcome"]: row for _, row in group.iterrows()}
        turnover = by_outcome.get("turnover_change")
        volume = by_outcome.get("volume_change")
        turnover_p = float(turnover["p_value"]) if turnover is not None else float("nan")
        volume_p = float(volume["p_value"]) if volume is not None else float("nan")
        turnover_sig = bool(turnover_p == turnover_p and turnover_p < CHANNEL_SIG_LEVEL)
        volume_sig = bool(volume_p == volume_p and volume_p < CHANNEL_SIG_LEVEL)
        rows.append(
            {
                "market": market,
                "event_phase": phase,
                "turnover_coef": float(turnover["coef"]) if turnover is not None else float("nan"),
                "turnover_t": float(turnover["t"]) if turnover is not None else float("nan"),
                "turnover_p": turnover_p,
                "volume_coef": float(volume["coef"]) if volume is not None else float("nan"),
                "volume_t": float(volume["t"]) if volume is not None else float("nan"),
                "volume_p": volume_p,
                "turnover_sig": turnover_sig,
                "volume_sig": volume_sig,
                "both_channels_sig": turnover_sig and volume_sig,
            }
        )
    return pd.DataFrame(rows)


def export_channel_concentration_table(
    table: pd.DataFrame,
    *,
    output_dir: Path,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "cma_h3_channel_concentration.csv"
    table.to_csv(out_path, index=False)
    return out_path


def export_mechanism_tables(
    table: pd.DataFrame,
    *,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "cma_mechanism_panel.csv"
    tex_path = output_dir / "cma_mechanism_panel.tex"
    table.to_csv(csv_path, index=False)
    with tex_path.open("w") as fh:
        fh.write("% auto-generated CMA mechanism panel\n")
        fh.write("\\begin{tabular}{lllrrrrrr}\n")
        fh.write("\\toprule\n")
        fh.write("market & phase & outcome & spec & coef & se & t & p & N \\\\\n")
        fh.write("\\midrule\n")
        for _, row in table.iterrows():
            coef = row.get("coef", float("nan"))
            se = row.get("se", float("nan"))
            t = row.get("t", float("nan"))
            p = row.get("p_value", float("nan"))
            n_obs = int(row.get("n_obs", 0))
            fh.write(
                f"{row['market']} & {row['event_phase']} & {row['outcome']} & {row['spec']} & "
                f"{coef:.4f} & {se:.4f} & {t:.3f} & {p:.3f} & {n_obs} \\\\\n"
            )
        fh.write("\\bottomrule\n\\end{tabular}\n")
    return {"csv": csv_path, "tex": tex_path}
