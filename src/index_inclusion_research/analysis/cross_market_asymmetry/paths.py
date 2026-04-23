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
