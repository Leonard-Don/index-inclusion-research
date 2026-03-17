from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats


@dataclass(frozen=True)
class WindowDefinition:
    start: int
    end: int

    @property
    def label(self) -> str:
        return f"[{self.start},+{self.end}]" if self.end >= 0 else f"[{self.start},{self.end}]"

    @property
    def slug(self) -> str:
        return f"m{abs(self.start)}_p{self.end}" if self.start < 0 else f"p{self.start}_p{self.end}"


def _normalise_windows(car_windows: list[list[int]] | list[tuple[int, int]]) -> list[WindowDefinition]:
    return [WindowDefinition(int(start), int(end)) for start, end in car_windows]


def compute_event_level_metrics(
    panel: pd.DataFrame,
    car_windows: list[list[int]] | list[tuple[int, int]],
) -> pd.DataFrame:
    windows = _normalise_windows(car_windows)
    rows: list[dict[str, object]] = []
    grouping = ["event_id", "event_phase"]
    for (_, _), group in panel.groupby(grouping, dropna=False):
        group = group.sort_values("relative_day").copy()
        first = group.iloc[0]
        metrics: dict[str, object] = {
            "event_id": first["event_id"],
            "matched_to_event_id": first.get("matched_to_event_id", pd.NA),
            "market": first["market"],
            "index_name": first["index_name"],
            "event_ticker": first["event_ticker"],
            "event_phase": first["event_phase"],
            "event_type": first["event_type"],
            "inclusion": first["inclusion"],
            "event_date": first["event_date"],
            "sector": first.get("sector", pd.NA),
        }
        for window in windows:
            mask = (group["relative_day"] >= window.start) & (group["relative_day"] <= window.end)
            metrics[f"car_{window.slug}"] = group.loc[mask, "ar"].sum()

        pre_group = group.loc[(group["relative_day"] >= -5) & (group["relative_day"] <= -1)]
        post_group = group.loc[(group["relative_day"] >= 0) & (group["relative_day"] <= 5)]
        pre_return_group = group.loc[(group["relative_day"] >= -20) & (group["relative_day"] <= -1)]
        metrics["pre_event_return"] = (
            (1.0 + pre_return_group["ret"].fillna(0.0)).prod() - 1.0 if not pre_return_group.empty else 0.0
        )
        reference_mkt_cap = group.loc[group["relative_day"] == -1, "mkt_cap"]
        if reference_mkt_cap.empty:
            reference_mkt_cap = group.loc[group["relative_day"] == 0, "mkt_cap"]
        metrics["log_mkt_cap"] = np.log(reference_mkt_cap.iloc[0]) if not reference_mkt_cap.empty and reference_mkt_cap.iloc[0] > 0 else np.nan
        metrics["turnover_change"] = post_group["turnover"].mean() - pre_group["turnover"].mean()
        metrics["volume_change"] = np.log1p(post_group["volume"].mean()) - np.log1p(pre_group["volume"].mean())
        metrics["volatility_change"] = post_group["ret"].std(ddof=0) - pre_group["ret"].std(ddof=0)
        rows.append(metrics)
    return pd.DataFrame(rows)


def compute_event_study(
    panel: pd.DataFrame,
    car_windows: list[list[int]] | list[tuple[int, int]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    windows = _normalise_windows(car_windows)
    event_level = compute_event_level_metrics(panel, car_windows)

    path_frame = panel.sort_values(["event_id", "event_phase", "relative_day"]).copy()
    path_frame["car_path"] = path_frame.groupby(["event_id", "event_phase"], dropna=False)["ar"].cumsum()
    average_paths = (
        path_frame.groupby(["market", "event_phase", "inclusion", "relative_day"], dropna=False)
        .agg(mean_ar=("ar", "mean"), mean_car=("car_path", "mean"), n_obs=("ar", "size"))
        .reset_index()
    )

    summary_rows: list[dict[str, object]] = []
    for (market, event_phase, inclusion), group in event_level.groupby(["market", "event_phase", "inclusion"], dropna=False):
        for window in windows:
            column = f"car_{window.slug}"
            values = group[column].dropna()
            t_stat = np.nan
            p_value = np.nan
            if len(values) > 1:
                t_stat, p_value = stats.ttest_1samp(values, popmean=0.0, nan_policy="omit")
            summary_rows.append(
                {
                    "market": market,
                    "event_phase": event_phase,
                    "inclusion": inclusion,
                    "window": window.label,
                    "window_slug": window.slug,
                    "n_events": int(values.count()),
                    "mean_car": values.mean() if not values.empty else np.nan,
                    "std_car": values.std(ddof=1) if len(values) > 1 else np.nan,
                    "t_stat": t_stat,
                    "p_value": p_value,
                }
            )

    summary = pd.DataFrame(summary_rows)
    return event_level, summary, average_paths
