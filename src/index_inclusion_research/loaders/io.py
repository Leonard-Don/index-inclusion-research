from __future__ import annotations

from pathlib import Path

import pandas as pd

from .contracts import (
    OPTIONAL_EVENT_COLUMNS,
    REQUIRED_BENCHMARK_COLUMNS,
    REQUIRED_EVENT_COLUMNS,
    REQUIRED_PRICE_COLUMNS,
    ensure_required_columns,
    normalise_market_codes,
    parse_date_columns,
)


def _prepare_event_defaults(events: pd.DataFrame) -> pd.DataFrame:
    prepared = events.copy()
    if "event_type" not in prepared.columns:
        prepared["event_type"] = "inclusion"
    if "inclusion" not in prepared.columns:
        prepared["inclusion"] = 1
    for optional in OPTIONAL_EVENT_COLUMNS:
        if optional not in prepared.columns:
            prepared[optional] = pd.NA
    prepared["inclusion"] = prepared["inclusion"].fillna(1).astype(int)
    return prepared


def load_events(path: str | Path) -> pd.DataFrame:
    events = pd.read_csv(path)
    ensure_required_columns(events, REQUIRED_EVENT_COLUMNS, "events")
    events = normalise_market_codes(events)
    events = parse_date_columns(events, ["announce_date", "effective_date"])
    events = _prepare_event_defaults(events)
    if events["announce_date"].isna().any() or events["effective_date"].isna().any():
        raise ValueError("events contains invalid announce_date or effective_date values")
    return events


def load_prices(path: str | Path) -> pd.DataFrame:
    prices = pd.read_csv(path)
    ensure_required_columns(prices, REQUIRED_PRICE_COLUMNS, "prices")
    prices = normalise_market_codes(prices)
    prices = parse_date_columns(prices, ["date"])
    if prices["date"].isna().any():
        raise ValueError("prices contains invalid date values")
    return prices.sort_values(["market", "ticker", "date"]).reset_index(drop=True)


def load_benchmarks(path: str | Path) -> pd.DataFrame:
    benchmarks = pd.read_csv(path)
    ensure_required_columns(benchmarks, REQUIRED_BENCHMARK_COLUMNS, "benchmarks")
    benchmarks = normalise_market_codes(benchmarks)
    benchmarks = parse_date_columns(benchmarks, ["date"])
    if benchmarks["date"].isna().any():
        raise ValueError("benchmarks contains invalid date values")
    return benchmarks.sort_values(["market", "date"]).reset_index(drop=True)


def save_dataframe(frame: pd.DataFrame, path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
