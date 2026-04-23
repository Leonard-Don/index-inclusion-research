from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

REQUIRED_EVENT_COLUMNS = [
    "market",
    "index_name",
    "ticker",
    "announce_date",
    "effective_date",
]
OPTIONAL_EVENT_COLUMNS = [
    "event_type",
    "source",
    "source_url",
    "sector",
    "note",
    "inclusion",
    "matched_to_event_id",
    "batch_id",
    "security_name",
    "treatment_group",
]

REQUIRED_PRICE_COLUMNS = [
    "market",
    "ticker",
    "date",
    "close",
    "ret",
    "volume",
    "turnover",
    "mkt_cap",
]

REQUIRED_BENCHMARK_COLUMNS = ["market", "date", "benchmark_ret"]


def ensure_required_columns(frame: pd.DataFrame, required: Iterable[str], frame_name: str) -> None:
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {missing}")


def normalise_market_codes(frame: pd.DataFrame) -> pd.DataFrame:
    normalised = frame.copy()
    normalised["market"] = normalised["market"].astype(str).str.upper().str.strip()
    return normalised


def parse_date_columns(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    parsed = frame.copy()
    for column in columns:
        if column in parsed.columns:
            parsed[column] = pd.to_datetime(parsed[column], errors="coerce").dt.normalize()
    return parsed
