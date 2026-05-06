from __future__ import annotations

import argparse
import logging
import math
import re
import time
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import akshare as ak
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

from index_inclusion_research import paths
from index_inclusion_research.loaders import load_events, save_dataframe

ROOT = paths.project_root()
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
CN_EVENT_SOURCE = ROOT / "data" / "raw" / "cn_csi300_changes.csv"


@dataclass(frozen=True)
class SecurityMetadata:
    yahoo_symbol: str
    ticker: str
    market: str
    sector: str | None
    shares_outstanding: float | None


def _parse_reference_dates(soup: BeautifulSoup) -> dict[str, str | None]:
    reference_dates: dict[str, str | None] = {}
    month_pattern = (
        r"(January|February|March|April|May|June|July|August|September|October|"
        r"November|December)\s+\d{1,2},\s+\d{4}"
    )
    for li in soup.select("ol.references li"):
        ref_id_raw = li.get("id")
        if not isinstance(ref_id_raw, str) or not ref_id_raw:
            continue
        ref_id = ref_id_raw
        text = " ".join(li.get_text(" ", strip=True).split())
        match = re.search(month_pattern, text)
        if match:
            reference_dates[ref_id] = match.group(0)
            continue
        match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", text)
        reference_dates[ref_id] = match.group(0) if match else None
    return reference_dates


def _normalise_us_yahoo_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")


def _cn_code_to_yahoo_symbol(code: str) -> str:
    code = str(code).zfill(6)
    if code.startswith(("6", "9", "688")):
        suffix = ".SS"
    else:
        suffix = ".SZ"
    return f"{code}{suffix}"


def _download_table_html(url: str) -> str:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    response.raise_for_status()
    return response.text


def build_us_events(start_year: int = 2010, end_year: int = 2025) -> tuple[pd.DataFrame, pd.DataFrame]:
    html = _download_table_html(WIKI_URL)
    soup = BeautifulSoup(html, "lxml")
    reference_dates = _parse_reference_dates(soup)
    tables = pd.read_html(StringIO(html))
    constituents = tables[0].copy()
    constituents["Symbol"] = constituents["Symbol"].astype(str)
    sector_lookup = constituents.set_index("Symbol")["GICS Sector"].to_dict()

    changes_table = soup.select("table.wikitable.sortable")[1]
    rows: list[dict[str, object]] = []
    body_rows = changes_table.select("tr")[2:]
    for tr in body_rows:
        tds = tr.find_all("td", recursive=False)
        if len(tds) != 6:
            continue
        values = [td.get_text(" ", strip=True) for td in tds]
        if not values[1]:
            continue
        effective_date = pd.to_datetime(values[0], errors="coerce")
        if pd.isna(effective_date):
            continue
        if not (start_year <= effective_date.year <= end_year):
            continue
        ref_ids = [
            href.lstrip("#")
            for a in tr.select('sup.reference a[href^="#cite_note"]')
            if isinstance((href := a.get("href")), str)
        ]
        announce_candidates = [reference_dates.get(ref_id) for ref_id in ref_ids if reference_dates.get(ref_id)]
        announce_date = (
            pd.to_datetime(announce_candidates[0], errors="coerce")  # type: ignore[call-overload]
            if announce_candidates
            else effective_date
        )
        if pd.isna(announce_date):
            announce_date = effective_date
        lead_days = int((effective_date.normalize() - announce_date.normalize()).days)
        if lead_days < 0 or lead_days > 60:
            announce_date = effective_date
        added_ticker = values[1].strip()
        removed_ticker = values[3].strip()
        if added_ticker:
            rows.append(
                {
                    "market": "US",
                    "index_name": "S&P500",
                    "ticker": added_ticker,
                    "announce_date": announce_date.normalize(),
                    "effective_date": effective_date.normalize(),
                    "event_type": "addition",
                    "inclusion": 1,
                    "batch_id": f"sp500-{effective_date:%Y%m%d}",
                    "source": "Wikipedia S&P 500 changes table with S&P Dow Jones citation dates",
                    "source_url": WIKI_URL,
                    "note": values[5],
                    "sector": sector_lookup.get(added_ticker, pd.NA),
                    "security_name": values[2].strip(),
                }
            )
        if removed_ticker:
            rows.append(
                {
                    "market": "US",
                    "index_name": "S&P500",
                    "ticker": removed_ticker,
                    "announce_date": announce_date.normalize(),
                    "effective_date": effective_date.normalize(),
                    "event_type": "deletion",
                    "inclusion": 0,
                    "batch_id": f"sp500-{effective_date:%Y%m%d}",
                    "source": "Wikipedia S&P 500 changes table with S&P Dow Jones citation dates",
                    "source_url": WIKI_URL,
                    "note": values[5],
                    "sector": sector_lookup.get(removed_ticker, pd.NA),
                    "security_name": values[4].strip(),
                }
            )

    return pd.DataFrame(rows), constituents


def build_cn_events(path: str | Path = CN_EVENT_SOURCE) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CN event source file not found: {csv_path}")
    required_columns = {"batch_id", "security_name", "event_type", "inclusion", "source", "source_url"}
    raw_events = pd.read_csv(csv_path)
    missing = [column for column in required_columns if column not in raw_events.columns]
    if missing:
        raise ValueError(f"CN event source is missing required columns: {missing}")
    events = load_events(csv_path)
    return events.loc[events["index_name"].astype(str).str.upper() == "CSI300"].copy()


def _sample_us_controls(constituents: pd.DataFrame, excluded: set[str], per_sector: int = 4) -> pd.DataFrame:
    controls: list[pd.DataFrame] = []
    filtered = constituents.loc[~constituents["Symbol"].isin(excluded)].copy()
    for _, sector_group in filtered.groupby("GICS Sector", dropna=False):
        controls.append(sector_group.head(per_sector))
    return pd.concat(controls, ignore_index=True).drop_duplicates(subset=["Symbol"])


def _sample_cn_controls(excluded: set[str], n_controls: int = 40) -> pd.DataFrame:
    current_cons = ak.index_stock_cons_csindex("000300").copy()
    current_cons["成分券代码"] = current_cons["成分券代码"].astype(str).str.zfill(6)
    filtered = current_cons.loc[~current_cons["成分券代码"].isin(excluded)].copy()
    return filtered.head(n_controls)


def _chunked_download_history(symbols: list[str], start: str, end: str, chunk_size: int = 20) -> dict[str, pd.DataFrame]:
    history_map: dict[str, pd.DataFrame] = {}
    for start_idx in range(0, len(symbols), chunk_size):
        chunk = symbols[start_idx : start_idx + chunk_size]
        if not chunk:
            continue
        frame = yf.download(
            tickers=chunk,
            start=start,
            end=end,
            auto_adjust=False,
            progress=False,
            threads=True,
            group_by="ticker",
        )
        if frame.empty:
            continue
        if len(chunk) == 1:
            history_map[chunk[0]] = frame.reset_index()
        else:
            for symbol in chunk:
                if symbol not in frame.columns.get_level_values(0):
                    continue
                symbol_frame = frame[symbol].reset_index()
                history_map[symbol] = symbol_frame
        time.sleep(0.25)
    return history_map


def _fetch_metadata(yahoo_symbol: str, fallback_sector: str | None) -> SecurityMetadata:
    ticker = yf.Ticker(yahoo_symbol)
    shares = None
    sector = fallback_sector
    try:
        fast_info = ticker.fast_info
        shares = fast_info.get("shares")
    except (KeyError, AttributeError, OSError, ValueError) as exc:
        logger.debug("yfinance fast_info unavailable for %s: %s", yahoo_symbol, exc)
        shares = None
    return SecurityMetadata(
        yahoo_symbol=yahoo_symbol,
        ticker="",
        market="",
        sector=sector,
        shares_outstanding=float(shares) if shares and not math.isnan(float(shares)) else None,
    )


def _build_price_rows(
    security_frame: pd.DataFrame,
    history_map: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    metadata_rows: list[dict[str, object]] = []
    price_rows: list[dict[str, object]] = []
    for row in security_frame.itertuples(index=False):
        yahoo_symbol = row.yahoo_symbol
        history = history_map.get(yahoo_symbol)  # type: ignore[arg-type]
        if history is None or history.empty:
            continue
        metadata = _fetch_metadata(yahoo_symbol, getattr(row, "sector", None))  # type: ignore[arg-type]
        shares = metadata.shares_outstanding
        sector = metadata.sector
        history = history.rename(
            columns={"Date": "date", "Close": "close", "Adj Close": "adj_close", "Volume": "volume"}
        ).copy()
        history["date"] = pd.to_datetime(history["date"]).dt.normalize()
        history = history.sort_values("date").reset_index(drop=True)
        price_base = history["adj_close"] if "adj_close" in history.columns else history["close"]
        history["ret"] = price_base.pct_change(fill_method=None)
        max_abs_ret = history["ret"].abs().max(skipna=True)
        extreme_count = int((history["ret"].abs() > 2.0).sum())
        severe_count = int((history["ret"].abs() > 5.0).sum())
        if pd.notna(max_abs_ret) and (max_abs_ret > 5.0 or severe_count > 0 or extreme_count >= 5):
            continue
        metadata_rows.append(
            {
                "market": row.market,
                "ticker": row.ticker,
                "yahoo_symbol": yahoo_symbol,
                "sector": sector,
                "shares_outstanding": shares,
            }
        )
        history["mkt_cap"] = history["close"] * shares if shares else pd.NA
        history["turnover"] = history["volume"] / shares if shares else pd.NA
        history["market"] = row.market
        history["ticker"] = row.ticker
        history["sector"] = sector
        history = history.loc[:, ["market", "ticker", "date", "close", "ret", "volume", "turnover", "mkt_cap", "sector"]]
        price_rows.extend(history.to_dict(orient="records"))  # type: ignore[arg-type]
        time.sleep(0.02)
    return pd.DataFrame(price_rows), pd.DataFrame(metadata_rows)


def _download_benchmarks(start: str, end: str) -> pd.DataFrame:
    benchmark_specs = {
        "US": "^GSPC",
        "CN": "000300.SS",
    }
    rows: list[dict[str, object]] = []
    for market, yahoo_symbol in benchmark_specs.items():
        history = yf.download(
            tickers=yahoo_symbol,
            start=start,
            end=end,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        if history.empty:
            continue
        if isinstance(history.columns, pd.MultiIndex):
            history.columns = [column[0] if isinstance(column, tuple) else column for column in history.columns]
        history = history.reset_index()
        first_column = history.columns[0]
        history = history.rename(columns={first_column: "date", "Close": "close", "Adj Close": "adj_close"})
        history["date"] = pd.to_datetime(history["date"]).dt.normalize()
        price_base = history["adj_close"] if "adj_close" in history.columns else history["close"]
        history["benchmark_ret"] = price_base.pct_change(fill_method=None)
        for item in history.loc[:, ["date", "benchmark_ret"]].dropna().to_dict(orient="records"):
            rows.append({"market": market, "date": item["date"], "benchmark_ret": item["benchmark_ret"]})
    return pd.DataFrame(rows)


def build_real_dataset(
    start: str,
    end: str,
    us_start_year: int = 2010,
    us_end_year: int = 2025,
    cn_events_path: str | Path = CN_EVENT_SOURCE,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    us_events, us_constituents = build_us_events(start_year=us_start_year, end_year=us_end_year)
    cn_events = build_cn_events(cn_events_path)
    events = pd.concat([cn_events, us_events], ignore_index=True)
    events["announce_date"] = pd.to_datetime(events["announce_date"]).dt.normalize()
    events["effective_date"] = pd.to_datetime(events["effective_date"]).dt.normalize()
    events = events.sort_values(["market", "effective_date", "inclusion", "ticker"]).reset_index(drop=True)

    us_event_tickers = set(us_events["ticker"].astype(str))
    cn_event_tickers = set(cn_events["ticker"].astype(str))

    us_controls = _sample_us_controls(us_constituents, excluded=us_event_tickers)
    cn_controls = _sample_cn_controls(excluded=cn_event_tickers)

    us_universe = pd.concat(
        [
            us_events.loc[:, ["ticker", "sector"]].drop_duplicates().assign(market="US"),
            us_controls.loc[:, ["Symbol", "GICS Sector"]]
            .rename(columns={"Symbol": "ticker", "GICS Sector": "sector"})
            .assign(market="US"),
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["market", "ticker"])
    us_universe["yahoo_symbol"] = us_universe["ticker"].map(_normalise_us_yahoo_symbol)

    cn_event_sector = cn_events.loc[:, ["ticker", "sector"]].drop_duplicates()
    cn_controls = cn_controls.rename(columns={"成分券代码": "ticker"})
    cn_universe = pd.concat(
        [
            cn_event_sector.assign(market="CN"),
            cn_controls.loc[:, ["ticker"]].assign(sector=pd.NA, market="CN"),  # type: ignore[arg-type]
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["market", "ticker"])
    cn_universe["yahoo_symbol"] = cn_universe["ticker"].map(_cn_code_to_yahoo_symbol)

    universe = pd.concat([cn_universe, us_universe], ignore_index=True)
    symbols = universe["yahoo_symbol"].dropna().drop_duplicates().tolist()
    history_map = _chunked_download_history(symbols, start=start, end=end)
    prices, metadata = _build_price_rows(universe, history_map)
    benchmarks = _download_benchmarks(start=start, end=end)
    return events, prices, benchmarks, metadata


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download a real cross-market index-inclusion sample.")
    parser.add_argument("--start", default="2009-01-01", help="Price history start date.")
    parser.add_argument("--end", default="2026-01-15", help="Price history end date.")
    parser.add_argument("--us-start-year", type=int, default=2010, help="Earliest S&P 500 change year to include.")
    parser.add_argument("--us-end-year", type=int, default=2025, help="Latest S&P 500 change year to include.")
    parser.add_argument("--cn-events", default=str(CN_EVENT_SOURCE), help="CSV path for CSI300 adjustment events.")
    parser.add_argument("--events-output", default="data/raw/real_events.csv", help="Events CSV output path.")
    parser.add_argument("--prices-output", default="data/raw/real_prices.csv", help="Prices CSV output path.")
    parser.add_argument("--benchmarks-output", default="data/raw/real_benchmarks.csv", help="Benchmarks CSV output path.")
    parser.add_argument("--metadata-output", default="data/raw/real_metadata.csv", help="Security metadata CSV output path.")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    events, prices, benchmarks, metadata = build_real_dataset(
        start=args.start,
        end=args.end,
        us_start_year=args.us_start_year,
        us_end_year=args.us_end_year,
        cn_events_path=args.cn_events,
    )
    save_dataframe(events, args.events_output)
    save_dataframe(prices, args.prices_output)
    save_dataframe(benchmarks, args.benchmarks_output)
    save_dataframe(metadata, args.metadata_output)
    print(f"Saved {len(events)} real events to {args.events_output}")
    print(f"Saved {len(prices)} real price rows to {args.prices_output}")
    print(f"Saved {len(benchmarks)} benchmark rows to {args.benchmarks_output}")


if __name__ == "__main__":
    main()
