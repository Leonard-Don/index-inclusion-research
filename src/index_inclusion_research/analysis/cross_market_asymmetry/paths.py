from __future__ import annotations

import pandas as pd

REQUIRED_PANEL_COLUMNS: tuple[str, ...] = (
    "event_id",
    "market",
    "event_phase",
    "event_type",
    "relative_day",
    "ar",
)


def _require_columns(frame: pd.DataFrame, required: tuple[str, ...]) -> None:
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(f"missing columns in panel: {missing}")


def build_daily_ar_panel(panel: pd.DataFrame) -> pd.DataFrame:
    _require_columns(panel, REQUIRED_PANEL_COLUMNS)
    work = panel.loc[panel["event_type"] == "addition"].copy()
    work = work.loc[work["event_phase"].isin(("announce", "effective"))].copy()
    work = work.sort_values(
        ["event_id", "market", "event_phase", "relative_day"]
    ).reset_index(drop=True)
    work["car"] = work.groupby(["event_id", "market", "event_phase"])["ar"].cumsum()
    return work


def compute_average_paths(ar_panel: pd.DataFrame) -> pd.DataFrame:
    grouped = ar_panel.groupby(
        ["market", "event_phase", "relative_day"], as_index=False
    ).agg(
        n_events=("event_id", "nunique"),
        ar_mean=("ar", "mean"),
        ar_std=("ar", "std"),
        car_mean=("car", "mean"),
        car_std=("car", "std"),
    )
    grouped["ar_se"] = grouped["ar_std"] / grouped["n_events"].pow(0.5)
    grouped["car_se"] = grouped["car_std"] / grouped["n_events"].pow(0.5)
    grouped["ar_t"] = grouped["ar_mean"] / grouped["ar_se"].replace(0.0, pd.NA)
    grouped["car_t"] = grouped["car_mean"] / grouped["car_se"].replace(0.0, pd.NA)
    return grouped.drop(columns=["ar_std", "car_std"])
