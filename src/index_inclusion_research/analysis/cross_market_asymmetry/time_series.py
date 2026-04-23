from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402


def build_rolling_car(
    panel: pd.DataFrame,
    *,
    window_years: int = 5,
    step_years: int = 1,
) -> pd.DataFrame:
    work = panel.loc[panel["event_type"] == "addition"].copy()
    work["event_date"] = pd.to_datetime(work["event_date"])
    work["year"] = work["event_date"].dt.year
    car_window = work.loc[(work["relative_day"] >= -1) & (work["relative_day"] <= 1)]
    per_event = (
        car_window.groupby(
            ["event_id", "market", "event_phase", "year"], as_index=False
        )["ar"]
        .sum()
        .rename(columns={"ar": "car_window"})
    )

    if per_event.empty:
        return pd.DataFrame(
            columns=[
                "market",
                "event_phase",
                "window_start_year",
                "window_end_year",
                "car_mean",
                "car_se",
                "car_t",
                "n_events",
            ]
        )
    min_year = int(per_event["year"].min())
    max_year = int(per_event["year"].max())
    rows = []
    end_year = min_year + window_years - 1
    while end_year <= max_year:
        start_year = end_year - window_years + 1
        sub = per_event.loc[
            (per_event["year"] >= start_year) & (per_event["year"] <= end_year)
        ]
        agg = (
            sub.groupby(["market", "event_phase"])
            .agg(
                car_mean=("car_window", "mean"),
                car_std=("car_window", "std"),
                n_events=("event_id", "nunique"),
            )
            .reset_index()
        )
        agg["car_se"] = agg["car_std"] / agg["n_events"].pow(0.5)
        agg["car_t"] = agg["car_mean"] / agg["car_se"].replace(0.0, pd.NA)
        agg["window_end_year"] = end_year
        agg["window_start_year"] = start_year
        rows.append(agg.drop(columns=["car_std"]))
        end_year += step_years
    if not rows:
        return pd.DataFrame(
            columns=[
                "market",
                "event_phase",
                "window_start_year",
                "window_end_year",
                "car_mean",
                "car_se",
                "car_t",
                "n_events",
            ]
        )
    return pd.concat(rows, ignore_index=True)


def summarize_structural_break(
    rolling: pd.DataFrame,
    *,
    split_year: int = 2010,
) -> pd.DataFrame:
    rolling = rolling.copy()
    if rolling.empty:
        return pd.DataFrame(
            columns=["market", "event_phase", "period", "car_mean", "car_se", "n_events"]
        )
    rolling["period"] = rolling["window_end_year"].apply(
        lambda y: "pre" if y < split_year else "post"
    )
    out = rolling.groupby(["market", "event_phase", "period"], as_index=False).agg(
        car_mean=("car_mean", "mean"),
        car_se=("car_se", "mean"),
        n_events=("n_events", "sum"),
    )
    return out


def render_rolling_figure(
    rolling: pd.DataFrame,
    *,
    output_dir: Path,
    aum_frame: pd.DataFrame | None = None,
) -> dict[str, object]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(10, 5))
    for (market, phase), sub in rolling.groupby(["market", "event_phase"]):
        sub = sub.sort_values("window_end_year")
        ax.plot(
            sub["window_end_year"],
            sub["car_mean"],
            marker="o",
            label=f"{market}·{phase}",
        )
    ax.axhline(0.0, color="#999", linestyle="--", linewidth=0.7)
    ax.set_xlabel("rolling window end year")
    ax.set_ylabel("CAR[-1,+1] mean")
    ax.set_title("Rolling CAR by market and event phase")
    ax.legend()

    aum_overlay = False
    if aum_frame is not None and not aum_frame.empty:
        ax2 = ax.twinx()
        for market, sub in aum_frame.groupby("market"):
            ax2.plot(
                sub["year"],
                sub["aum_trillion"],
                linestyle="--",
                alpha=0.5,
                label=f"AUM {market}",
            )
        ax2.set_ylabel("passive AUM (trillion)")
        ax2.legend(loc="lower right")
        aum_overlay = True

    fig.tight_layout()
    fig_path = output_dir / "cma_time_series_rolling.png"
    fig.savefig(fig_path, dpi=150)
    plt.close(fig)
    return {"figure": fig_path, "aum_overlay": aum_overlay}


def export_time_series_tables(
    rolling: pd.DataFrame,
    break_df: pd.DataFrame,
    *,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rolling_path = output_dir / "cma_time_series_rolling.csv"
    break_path = output_dir / "cma_time_series_break.csv"
    rolling.to_csv(rolling_path, index=False)
    break_df.to_csv(break_path, index=False)
    return {"rolling": rolling_path, "break": break_path}
