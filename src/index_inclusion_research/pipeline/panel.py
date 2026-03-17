from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class EventPhase:
    name: str
    date_column: str


DEFAULT_PHASES = [
    EventPhase("announce", "announce_date"),
    EventPhase("effective", "effective_date"),
]


def map_to_trading_date(event_date: pd.Timestamp, trading_dates: Iterable[pd.Timestamp]) -> pd.Timestamp | pd.NaT:
    dates = pd.DatetimeIndex(pd.to_datetime(list(trading_dates))).sort_values().unique()
    if len(dates) == 0 or pd.isna(event_date):
        return pd.NaT
    position = dates.searchsorted(pd.Timestamp(event_date), side="left")
    if position < len(dates):
        return pd.Timestamp(dates[position])
    return pd.Timestamp(dates[-1])


def _build_market_calendars(prices: pd.DataFrame, benchmarks: pd.DataFrame) -> dict[str, pd.DatetimeIndex]:
    calendars: dict[str, pd.DatetimeIndex] = {}
    for market, group in benchmarks.groupby("market"):
        calendars[market] = pd.DatetimeIndex(group["date"].sort_values().unique())
    for market, group in prices.groupby("market"):
        if market not in calendars:
            calendars[market] = pd.DatetimeIndex(group["date"].sort_values().unique())
    return calendars


def _map_to_ticker_date(event_date: pd.Timestamp, ticker_dates: pd.Series) -> pd.Timestamp | pd.NaT:
    return map_to_trading_date(event_date, ticker_dates.tolist())


def build_event_panel(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    benchmarks: pd.DataFrame,
    window_pre: int = 20,
    window_post: int = 20,
    phases: list[EventPhase] | None = None,
) -> pd.DataFrame:
    phases = phases or DEFAULT_PHASES
    calendars = _build_market_calendars(prices, benchmarks)
    prices_by_key = {
        (market, ticker): group.sort_values("date").reset_index(drop=True)
        for (market, ticker), group in prices.groupby(["market", "ticker"], dropna=False)
    }
    benchmark_by_market = {
        market: group[["date", "benchmark_ret"]].drop_duplicates("date").set_index("date")
        for market, group in benchmarks.groupby("market")
    }

    rows: list[dict[str, object]] = []
    for event in events.itertuples(index=False):
        market = event.market
        ticker = event.ticker
        ticker_prices = prices_by_key.get((market, ticker))
        if ticker_prices is None or market not in calendars:
            continue

        for phase in phases:
            raw_event_date = getattr(event, phase.date_column, pd.NaT)
            mapped_market_date = map_to_trading_date(raw_event_date, calendars[market])
            mapped_ticker_date = _map_to_ticker_date(mapped_market_date, ticker_prices["date"])
            if pd.isna(mapped_ticker_date):
                continue

            event_position = ticker_prices["date"].searchsorted(mapped_ticker_date)
            start = max(0, event_position - window_pre)
            stop = min(len(ticker_prices), event_position + window_post + 1)
            window_frame = ticker_prices.iloc[start:stop].copy()
            if window_frame.empty:
                continue

            window_frame["relative_day"] = range(start - event_position, stop - event_position)
            benchmark_frame = benchmark_by_market.get(market)
            if benchmark_frame is not None:
                window_frame = window_frame.join(benchmark_frame, on="date", how="left")
            else:
                window_frame["benchmark_ret"] = np.nan
            window_frame["ar"] = window_frame["ret"] - window_frame["benchmark_ret"]
            window_frame["event_id"] = event.event_id
            window_frame["event_phase"] = phase.name
            window_frame["event_type"] = getattr(event, "event_type", "inclusion")
            window_frame["market"] = market
            window_frame["index_name"] = event.index_name
            window_frame["source"] = getattr(event, "source", pd.NA)
            window_frame["event_date_raw"] = raw_event_date
            window_frame["event_date"] = mapped_ticker_date
            window_frame["mapped_market_date"] = mapped_market_date
            window_frame["inclusion"] = getattr(event, "inclusion", 1)
            window_frame["matched_to_event_id"] = getattr(event, "matched_to_event_id", pd.NA)
            window_frame["event_ticker"] = ticker
            if "sector" not in window_frame.columns:
                window_frame["sector"] = getattr(event, "sector", pd.NA)
            else:
                event_sector = getattr(event, "sector", pd.NA)
                window_frame["sector"] = window_frame["sector"].where(window_frame["sector"].notna(), event_sector)
            rows.extend(window_frame.to_dict(orient="records"))

    if not rows:
        return pd.DataFrame()

    panel = pd.DataFrame(rows)
    column_order = [
        "event_id",
        "matched_to_event_id",
        "market",
        "index_name",
        "event_ticker",
        "event_phase",
        "event_type",
        "inclusion",
        "event_date_raw",
        "mapped_market_date",
        "event_date",
        "date",
        "relative_day",
        "close",
        "ret",
        "benchmark_ret",
        "ar",
        "volume",
        "turnover",
        "mkt_cap",
        "sector",
        "source",
    ]
    return panel[column_order].sort_values(
        ["market", "event_phase", "event_id", "relative_day"]
    ).reset_index(drop=True)
