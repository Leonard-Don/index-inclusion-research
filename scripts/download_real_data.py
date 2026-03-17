from __future__ import annotations

import argparse
import math
import re
import sys
import time
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import akshare as ak
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

from index_inclusion_research.loaders import save_dataframe

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

CN_EVENT_GROUPS = [
    {
        "announce_date": "2024-11-29",
        "effective_date": "2024-12-16",
        "source": "CSIndex sample adjustment attachment dated 2024-11-29",
        "source_url": "https://www.csindex.com.cn/",
        "index_name": "CSI300",
        "securities": [
            ("000686", "东北证券"),
            ("002625", "光启技术"),
            ("000708", "中信特钢"),
            ("601058", "赛轮轮胎"),
            ("300442", "润泽科技"),
            ("600839", "四川长虹"),
            ("300207", "欣旺达"),
            ("000975", "山金国际"),
            ("601168", "西部矿业"),
            ("300068", "南都电源"),
            ("688608", "恒玄科技"),
            ("601100", "恒立液压"),
            ("688506", "百利天恒"),
            ("688188", "柏楚电子"),
            ("601898", "中煤能源"),
        ],
    },
    {
        "announce_date": "2025-11-28",
        "effective_date": "2025-12-15",
        "source": "CSIndex sample adjustment attachment dated 2025-11-28",
        "source_url": "https://www.csindex.com.cn/",
        "index_name": "CSI300",
        "securities": [
            ("688097", "博众精工"),
            ("302132", "中航成飞"),
            ("300339", "润和软件"),
            ("601727", "上海电气"),
            ("300394", "天孚通信"),
            ("000423", "东阿阿胶"),
            ("601808", "中海油服"),
            ("603296", "华勤技术"),
            ("002028", "思源电气"),
            ("002179", "中航光电"),
            ("300476", "胜宏科技"),
        ],
    },
]


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
        ref_id = li.get("id")
        if not ref_id:
            continue
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


def build_us_events(start_year: int = 2024, end_year: int = 2025) -> tuple[pd.DataFrame, pd.DataFrame]:
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
        ref_ids = [a.get("href").lstrip("#") for a in tr.select('sup.reference a[href^="#cite_note"]')]
        announce_candidates = [reference_dates.get(ref_id) for ref_id in ref_ids if reference_dates.get(ref_id)]
        announce_date = (
            pd.to_datetime(announce_candidates[0], errors="coerce")
            if announce_candidates
            else effective_date
        )
        added_ticker = values[1].strip()
        rows.append(
            {
                "market": "US",
                "index_name": "S&P500",
                "ticker": added_ticker,
                "announce_date": announce_date.normalize(),
                "effective_date": effective_date.normalize(),
                "event_type": "inclusion",
                "source": "Wikipedia S&P 500 changes table with S&P Dow Jones citation dates",
                "source_url": WIKI_URL,
                "note": values[5],
                "sector": sector_lookup.get(added_ticker),
                "security_name": values[2].strip(),
            }
        )

    return pd.DataFrame(rows), constituents


def build_cn_events() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for group in CN_EVENT_GROUPS:
        for code, name in group["securities"]:
            rows.append(
                {
                    "market": "CN",
                    "index_name": group["index_name"],
                    "ticker": str(code).zfill(6),
                    "announce_date": pd.Timestamp(group["announce_date"]),
                    "effective_date": pd.Timestamp(group["effective_date"]),
                    "event_type": "inclusion",
                    "source": group["source"],
                    "source_url": group["source_url"],
                    "note": "Constituent addition extracted from official CSIndex adjustment attachment.",
                    "sector": pd.NA,
                    "security_name": name,
                }
            )
    return pd.DataFrame(rows)


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
    except Exception:
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
        history = history_map.get(yahoo_symbol)
        if history is None or history.empty:
            continue
        metadata = _fetch_metadata(yahoo_symbol, getattr(row, "sector", None))
        shares = metadata.shares_outstanding
        sector = metadata.sector
        metadata_rows.append(
            {
                "market": row.market,
                "ticker": row.ticker,
                "yahoo_symbol": yahoo_symbol,
                "sector": sector,
                "shares_outstanding": shares,
            }
        )
        history = history.rename(columns={"Date": "date", "Close": "close", "Volume": "volume"}).copy()
        history["date"] = pd.to_datetime(history["date"]).dt.normalize()
        history = history.sort_values("date").reset_index(drop=True)
        history["ret"] = history["close"].pct_change(fill_method=None)
        history["mkt_cap"] = history["close"] * shares if shares else pd.NA
        history["turnover"] = history["volume"] / shares if shares else pd.NA
        history["market"] = row.market
        history["ticker"] = row.ticker
        history["sector"] = sector
        history = history.loc[:, ["market", "ticker", "date", "close", "ret", "volume", "turnover", "mkt_cap", "sector"]]
        price_rows.extend(history.to_dict(orient="records"))
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
        history["benchmark_ret"] = history["close"].pct_change()
        for item in history.loc[:, ["date", "benchmark_ret"]].dropna().to_dict(orient="records"):
            rows.append({"market": market, "date": item["date"], "benchmark_ret": item["benchmark_ret"]})
    return pd.DataFrame(rows)


def build_real_dataset(start: str, end: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    us_events, us_constituents = build_us_events()
    cn_events = build_cn_events()
    events = pd.concat([cn_events, us_events], ignore_index=True)
    events["announce_date"] = pd.to_datetime(events["announce_date"]).dt.normalize()
    events["effective_date"] = pd.to_datetime(events["effective_date"]).dt.normalize()
    events = events.sort_values(["market", "effective_date", "ticker"]).reset_index(drop=True)

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
            cn_controls.loc[:, ["ticker"]].assign(sector=pd.NA, market="CN"),
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Download a real cross-market index-inclusion sample.")
    parser.add_argument("--start", default="2024-01-01", help="Price history start date.")
    parser.add_argument("--end", default="2026-01-15", help="Price history end date.")
    parser.add_argument("--events-output", default="data/raw/real_events.csv", help="Events CSV output path.")
    parser.add_argument("--prices-output", default="data/raw/real_prices.csv", help="Prices CSV output path.")
    parser.add_argument("--benchmarks-output", default="data/raw/real_benchmarks.csv", help="Benchmarks CSV output path.")
    parser.add_argument("--metadata-output", default="data/raw/real_metadata.csv", help="Security metadata CSV output path.")
    args = parser.parse_args()

    events, prices, benchmarks, metadata = build_real_dataset(start=args.start, end=args.end)
    save_dataframe(events, args.events_output)
    save_dataframe(prices, args.prices_output)
    save_dataframe(benchmarks, args.benchmarks_output)
    save_dataframe(metadata, args.metadata_output)
    print(f"Saved {len(events)} real events to {args.events_output}")
    print(f"Saved {len(prices)} real price rows to {args.prices_output}")
    print(f"Saved {len(benchmarks)} benchmark rows to {args.benchmarks_output}")


if __name__ == "__main__":
    main()
