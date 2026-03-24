from __future__ import annotations

import numpy as np
import pandas as pd


def summarise_mechanism_changes(event_level: pd.DataFrame) -> pd.DataFrame:
    included = event_level.loc[event_level["inclusion"] == 1].copy()
    if included.empty:
        return pd.DataFrame()
    summary = (
        included.groupby(["market", "event_phase"], dropna=False)
        .agg(
            n_events=("event_id", "nunique"),
            mean_turnover_change=("turnover_change", "mean"),
            mean_volume_change=("volume_change", "mean"),
            mean_volatility_change=("volatility_change", "mean"),
        )
        .reset_index()
    )
    return summary


def compute_retention_summary(
    event_level: pd.DataFrame,
    short_window_slug: str = "p0_p20",
    long_window_slug: str = "p0_p120",
) -> pd.DataFrame:
    included = event_level.loc[event_level["inclusion"] == 1].copy()
    short_col = f"car_{short_window_slug}"
    long_col = f"car_{long_window_slug}"
    required = {"market", "event_phase", short_col, long_col}
    if included.empty or not required.issubset(included.columns):
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for (market, event_phase), group in included.groupby(["market", "event_phase"], dropna=False):
        short_mean = group[short_col].mean()
        long_mean = group[long_col].mean()
        retention_ratio = np.nan
        if pd.notna(short_mean) and abs(short_mean) > 1e-9:
            retention_ratio = long_mean / short_mean
        rows.append(
            {
                "market": market,
                "event_phase": event_phase,
                "n_events": int(group["event_id"].nunique()),
                "short_window_slug": short_window_slug,
                "long_window_slug": long_window_slug,
                "short_mean_car": short_mean,
                "long_mean_car": long_mean,
                "car_reversal": short_mean - long_mean,
                "retention_ratio": retention_ratio,
            }
        )
    return pd.DataFrame(rows)


def compute_did_summary(
    panel: pd.DataFrame,
    pre_window: tuple[int, int] = (-5, -1),
    post_window: tuple[int, int] = (0, 5),
) -> pd.DataFrame:
    if panel.empty:
        return pd.DataFrame()

    work = panel.copy()
    work["comparison_id"] = work["matched_to_event_id"].where(work["matched_to_event_id"].notna(), work["event_id"])
    metric_specs = {
        "abnormal_return": ("ar", lambda series: series.mean()),
        "turnover": ("turnover", lambda series: series.mean()),
        "log_volume": ("volume", lambda series: np.log1p(series).mean()),
    }

    event_rows: list[dict[str, object]] = []
    for (comparison_id, event_phase, inclusion), group in work.groupby(
        ["comparison_id", "event_phase", "inclusion"],
        dropna=False,
    ):
        first = group.iloc[0]
        row: dict[str, object] = {
            "comparison_id": comparison_id,
            "market": first["market"],
            "event_phase": event_phase,
            "inclusion": int(inclusion),
        }
        pre_mask = (group["relative_day"] >= pre_window[0]) & (group["relative_day"] <= pre_window[1])
        post_mask = (group["relative_day"] >= post_window[0]) & (group["relative_day"] <= post_window[1])
        pre_group = group.loc[pre_mask]
        post_group = group.loc[post_mask]
        for metric_name, (column, aggregator) in metric_specs.items():
            row[f"pre_{metric_name}"] = aggregator(pre_group[column]) if not pre_group.empty else np.nan
            row[f"post_{metric_name}"] = aggregator(post_group[column]) if not post_group.empty else np.nan
            row[f"diff_{metric_name}"] = row[f"post_{metric_name}"] - row[f"pre_{metric_name}"]
        event_rows.append(row)

    event_level = pd.DataFrame(event_rows)
    if event_level.empty:
        return pd.DataFrame()

    summary_rows: list[dict[str, object]] = []
    for (market, event_phase), group in event_level.groupby(["market", "event_phase"], dropna=False):
        treated = group.loc[group["inclusion"] == 1]
        control = group.loc[group["inclusion"] == 0]
        for metric_name in metric_specs:
            treated_mean = treated[f"diff_{metric_name}"].mean()
            control_mean = control[f"diff_{metric_name}"].mean()
            summary_rows.append(
                {
                    "market": market,
                    "event_phase": event_phase,
                    "metric": metric_name,
                    "treated_post_minus_pre": treated_mean,
                    "control_post_minus_pre": control_mean,
                    "did_estimate": treated_mean - control_mean,
                    "n_treated": int(treated["comparison_id"].nunique()),
                    "n_control": int(control["comparison_id"].nunique()),
                }
            )

    return pd.DataFrame(summary_rows)
