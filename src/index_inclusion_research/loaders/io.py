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


ADDITION_EVENT_TYPES = {"addition", "inclusion", "add", "调入", "纳入"}
DELETION_EVENT_TYPES = {"deletion", "removal", "exclusion", "delete", "剔除", "调出"}


def _infer_inclusion_from_event_type(event_type: object) -> int | None:
    if event_type is None or pd.isna(event_type):
        return None
    value = str(event_type).strip().lower()
    if value in ADDITION_EVENT_TYPES:
        return 1
    if value in DELETION_EVENT_TYPES:
        return 0
    return None


def _prepare_event_defaults(events: pd.DataFrame) -> pd.DataFrame:
    prepared = events.copy()
    if "event_type" not in prepared.columns:
        prepared["event_type"] = "addition"
    inferred_inclusion = prepared["event_type"].map(_infer_inclusion_from_event_type)
    if "inclusion" not in prepared.columns:
        prepared["inclusion"] = inferred_inclusion
    else:
        prepared["inclusion"] = prepared["inclusion"].fillna(inferred_inclusion)
    if "treatment_group" not in prepared.columns:
        prepared["treatment_group"] = 1
    for optional in OPTIONAL_EVENT_COLUMNS:
        if optional not in prepared.columns:
            prepared[optional] = pd.NA
    prepared["inclusion"] = prepared["inclusion"].fillna(1).astype(int)
    prepared["treatment_group"] = prepared["treatment_group"].fillna(1).astype(int)
    return prepared


def load_events(path: str | Path) -> pd.DataFrame:
    events = pd.read_csv(path, low_memory=False)
    ensure_required_columns(events, REQUIRED_EVENT_COLUMNS, "events")
    events = normalise_market_codes(events)
    events["ticker"] = events["ticker"].astype(str).str.strip()
    cn_mask = events["market"] == "CN"
    events.loc[cn_mask, "ticker"] = events.loc[cn_mask, "ticker"].str.zfill(6)
    events = parse_date_columns(events, ["announce_date", "effective_date"])
    events = _prepare_event_defaults(events)
    if events["announce_date"].isna().any() or events["effective_date"].isna().any():
        raise ValueError("events contains invalid announce_date or effective_date values")
    return events


def load_prices(path: str | Path) -> pd.DataFrame:
    prices = pd.read_csv(path, low_memory=False)
    ensure_required_columns(prices, REQUIRED_PRICE_COLUMNS, "prices")
    prices = normalise_market_codes(prices)
    prices["ticker"] = prices["ticker"].astype(str).str.strip()
    cn_mask = prices["market"] == "CN"
    prices.loc[cn_mask, "ticker"] = prices.loc[cn_mask, "ticker"].str.zfill(6)
    for column in ["close", "ret", "volume", "turnover", "mkt_cap"]:
        prices[column] = pd.to_numeric(prices[column], errors="coerce")
    prices = parse_date_columns(prices, ["date"])
    if prices["date"].isna().any():
        raise ValueError("prices contains invalid date values")
    return prices.sort_values(["market", "ticker", "date"]).reset_index(drop=True)


def load_benchmarks(path: str | Path) -> pd.DataFrame:
    benchmarks = pd.read_csv(path, low_memory=False)
    ensure_required_columns(benchmarks, REQUIRED_BENCHMARK_COLUMNS, "benchmarks")
    benchmarks = normalise_market_codes(benchmarks)
    benchmarks["benchmark_ret"] = pd.to_numeric(benchmarks["benchmark_ret"], errors="coerce")
    benchmarks = parse_date_columns(benchmarks, ["date"])
    if benchmarks["date"].isna().any():
        raise ValueError("benchmarks contains invalid date values")
    return benchmarks.sort_values(["market", "date"]).reset_index(drop=True)


def save_dataframe(frame: pd.DataFrame, path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
