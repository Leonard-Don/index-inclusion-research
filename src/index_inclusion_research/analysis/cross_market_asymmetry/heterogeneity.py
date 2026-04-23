from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

_VALID_DIMS: tuple[str, ...] = ("size", "liquidity", "sector", "gap_bucket")
EPS = 1e-4


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
