from __future__ import annotations

import re

import pandas as pd


def _slugify(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "-", str(value)).strip("-").lower() or "na"


def build_event_sample(events: pd.DataFrame, duplicate_window_days: int = 30) -> pd.DataFrame:
    prepared = events.copy()
    prepared["event_type"] = prepared["event_type"].fillna("addition")
    prepared["inclusion"] = prepared["inclusion"].fillna(1).astype(int)
    if "treatment_group" not in prepared.columns:
        prepared["treatment_group"] = 1
    prepared["treatment_group"] = prepared["treatment_group"].fillna(1).astype(int)
    prepared = prepared.sort_values(
        ["market", "ticker", "index_name", "announce_date", "effective_date", "inclusion", "treatment_group"]
    ).reset_index(drop=True)

    dedupe_columns = [
        "market",
        "index_name",
        "ticker",
        "announce_date",
        "effective_date",
        "event_type",
        "inclusion",
    ]
    prepared["is_exact_duplicate"] = prepared.duplicated(dedupe_columns, keep=False)
    prepared = prepared.loc[~prepared.duplicated(dedupe_columns, keep="first")].copy()
    prepared["has_nearby_event_conflict"] = False

    for _, group in prepared.groupby(["market", "ticker", "index_name"], dropna=False):
        previous_announce = None
        previous_effective = None
        for index, row in group.iterrows():
            if previous_announce is not None:
                announce_gap = abs((row["announce_date"] - previous_announce).days)
                effective_gap = abs((row["effective_date"] - previous_effective).days)
                if min(announce_gap, effective_gap) <= duplicate_window_days:
                    prepared.loc[index, "has_nearby_event_conflict"] = True  # type: ignore[index]
            previous_announce = row["announce_date"]
            previous_effective = row["effective_date"]

    prepared["event_sequence"] = (
        prepared.groupby(["market", "ticker", "index_name"]).cumcount() + 1
    )
    prepared["event_id"] = prepared.apply(
        lambda row: (
            f"{row['market']}-"
            f"{_slugify(row['index_name'])}-"
            f"{_slugify(row['ticker'])}-"
            f"{row['announce_date']:%Y%m%d}-"
            f"{row['effective_date']:%Y%m%d}-"
            f"{'add' if int(row['inclusion']) == 1 else 'del'}-"
            f"{int(row['event_sequence']):02d}"
        ),
        axis=1,
    )
    return prepared.drop(columns=["event_sequence"]).reset_index(drop=True)
