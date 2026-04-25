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
