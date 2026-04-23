from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
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
