"""Fill missing CN sector labels in the real-data CSVs.

The downloaded CN price panel already contains usable shares / market-cap
data, but early real-data snapshots left ``sector`` blank for A-share
events. This module adds a reproducible enrichment step that fetches an
industry label from CNInfo via ``akshare`` and falls back to Yahoo Finance
metadata through ``yfinance``.
"""

from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Callable
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EVENTS = ROOT / "data" / "raw" / "real_events.csv"
DEFAULT_METADATA = ROOT / "data" / "raw" / "real_metadata.csv"

SectorFetcher = Callable[[str, str], str | None]


def _cn_code_to_yahoo_symbol(code: object) -> str:
    ticker = str(code).strip()
    if ticker.endswith(".0"):
        ticker = ticker[:-2]
    ticker = ticker.zfill(6)
    suffix = ".SS" if ticker.startswith(("6", "9", "688")) else ".SZ"
    return f"{ticker}{suffix}"


def _is_missing_sector(value: object) -> bool:
    if value is None or pd.isna(value):
        return True
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "none", "<na>", "unknown"}


def _normalise_sector(value: object) -> str | None:
    if _is_missing_sector(value):
        return None
    return str(value).strip()


def fetch_yfinance_sector(ticker: str, yahoo_symbol: str) -> str | None:
    """Fetch a broad sector label from Yahoo Finance metadata."""
    try:
        import yfinance as yf
    except ImportError:
        logger.debug("yfinance not installed; cannot enrich sector for %s", ticker)
        return None
    try:
        info = yf.Ticker(yahoo_symbol).get_info()
    except Exception as exc:  # noqa: BLE001 — external metadata fetch is best-effort
        logger.debug("yfinance info unavailable for %s/%s: %s", ticker, yahoo_symbol, exc)
        return None
    return _normalise_sector(info.get("sector") or info.get("industry"))


def fetch_cninfo_sector(ticker: str, _yahoo_symbol: str) -> str | None:
    """Fetch a CN industry label from CNInfo via akshare."""
    try:
        import akshare as ak
    except ImportError:
        logger.debug("akshare not installed; cannot enrich sector for %s", ticker)
        return None
    try:
        profile = ak.stock_profile_cninfo(symbol=str(ticker).zfill(6))
    except Exception as exc:  # noqa: BLE001 — external metadata fetch is best-effort
        logger.debug("CNInfo profile unavailable for %s: %s", ticker, exc)
        return None
    if profile is None or profile.empty or "所属行业" not in profile.columns:
        return None
    return _normalise_sector(profile["所属行业"].iloc[0])


def fetch_cn_sector(ticker: str, yahoo_symbol: str) -> str | None:
    """Default sector fetcher: CNInfo first, Yahoo Finance fallback."""
    return fetch_cninfo_sector(ticker, yahoo_symbol) or fetch_yfinance_sector(
        ticker, yahoo_symbol
    )


def _prepare_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "sector" not in out.columns:
        out["sector"] = pd.NA
    out["market"] = out["market"].astype(str).str.strip().str.upper()
    out["ticker"] = out["ticker"].astype(str).str.strip()
    cn_mask = out["market"] == "CN"
    out.loc[cn_mask, "ticker"] = out.loc[cn_mask, "ticker"].str.zfill(6)
    return out


def _collect_existing_sector_map(frames: list[pd.DataFrame]) -> dict[str, str]:
    sector_map: dict[str, str] = {}
    for frame in frames:
        prepared = _prepare_frame(frame)
        cn = prepared.loc[prepared["market"] == "CN", ["ticker", "sector"]]
        for row in cn.itertuples(index=False):
            sector = _normalise_sector(row.sector)
            if sector:
                sector_map[str(row.ticker)] = sector
    return sector_map


def _collect_missing_tickers(frames: list[pd.DataFrame]) -> dict[str, str]:
    missing: dict[str, str] = {}
    for frame in frames:
        prepared = _prepare_frame(frame)
        cn = prepared.loc[prepared["market"] == "CN"].copy()
        if cn.empty:
            continue
        missing_mask = cn["sector"].map(_is_missing_sector)
        for row in cn.loc[missing_mask].itertuples(index=False):
            yahoo_symbol = getattr(row, "yahoo_symbol", None)
            if _is_missing_sector(yahoo_symbol):
                yahoo_symbol = _cn_code_to_yahoo_symbol(row.ticker)
            missing[str(row.ticker)] = str(yahoo_symbol)
    return missing


def fetch_missing_sector_map(
    tickers_to_symbols: dict[str, str],
    *,
    fetcher: SectorFetcher = fetch_cn_sector,
    limit: int | None = None,
    sleep_seconds: float = 0.05,
) -> tuple[dict[str, str], list[str]]:
    """Fetch sectors for missing CN tickers.

    Returns ``(sector_map, failed_tickers)``. The map only includes tickers
    with a non-empty sector label.
    """
    sector_map: dict[str, str] = {}
    failed: list[str] = []
    items = sorted(tickers_to_symbols.items())
    if limit is not None:
        items = items[:limit]
    for ticker, yahoo_symbol in items:
        sector = _normalise_sector(fetcher(ticker, yahoo_symbol))
        if sector:
            sector_map[ticker] = sector
        else:
            failed.append(ticker)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return sector_map, failed


def fill_missing_cn_sectors(
    frame: pd.DataFrame,
    sector_map: dict[str, str],
) -> tuple[pd.DataFrame, int]:
    """Return ``frame`` with missing CN sectors filled from ``sector_map``."""
    out = _prepare_frame(frame)
    cn_missing = (out["market"] == "CN") & out["sector"].map(_is_missing_sector)
    fill_values = out.loc[cn_missing, "ticker"].map(sector_map)
    fill_values = fill_values.loc[fill_values.notna()]
    out.loc[fill_values.index, "sector"] = fill_values
    return out, int(len(fill_values))


def enrich_sector_frames(
    events: pd.DataFrame,
    metadata: pd.DataFrame,
    *,
    fetcher: SectorFetcher = fetch_cn_sector,
    limit: int | None = None,
    sleep_seconds: float = 0.05,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    """Fill missing CN sectors in events + metadata using one shared map."""
    prepared_events = _prepare_frame(events)
    prepared_metadata = _prepare_frame(metadata)
    existing = _collect_existing_sector_map([prepared_events, prepared_metadata])
    missing = _collect_missing_tickers([prepared_events, prepared_metadata])
    to_fetch = {ticker: symbol for ticker, symbol in missing.items() if ticker not in existing}
    fetched, failed = fetch_missing_sector_map(
        to_fetch, fetcher=fetcher, limit=limit, sleep_seconds=sleep_seconds
    )
    sector_map = {**existing, **fetched}
    enriched_events, events_filled = fill_missing_cn_sectors(prepared_events, sector_map)
    enriched_metadata, metadata_filled = fill_missing_cn_sectors(
        prepared_metadata, sector_map
    )
    summary = {
        "existing_sectors": len(existing),
        "missing_tickers": len(missing),
        "fetched_sectors": len(fetched),
        "failed_tickers": failed,
        "events_filled": events_filled,
        "metadata_filled": metadata_filled,
    }
    return enriched_events, enriched_metadata, summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fill missing CN sector values in real_events.csv and real_metadata.csv."
    )
    parser.add_argument("--events", default=str(DEFAULT_EVENTS), help="Input events CSV.")
    parser.add_argument(
        "--metadata", default=str(DEFAULT_METADATA), help="Input metadata CSV."
    )
    parser.add_argument(
        "--events-output",
        default=None,
        help="Output events CSV. Defaults to overwriting --events when --force is set.",
    )
    parser.add_argument(
        "--metadata-output",
        default=None,
        help="Output metadata CSV. Defaults to overwriting --metadata when --force is set.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Fetch only the first N missing tickers, useful for smoke runs.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.05,
        help="Pause between metadata calls.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow writing output CSVs.",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Fetch and report coverage without writing CSVs.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    events_path = Path(args.events)
    metadata_path = Path(args.metadata)
    if not events_path.exists():
        print(f"[enrich-cn-sectors] events file not found: {events_path}")
        return 1
    if not metadata_path.exists():
        print(f"[enrich-cn-sectors] metadata file not found: {metadata_path}")
        return 1
    events = pd.read_csv(events_path, dtype={"ticker": str})
    metadata = pd.read_csv(metadata_path, dtype={"ticker": str})
    enriched_events, enriched_metadata, summary = enrich_sector_frames(
        events,
        metadata,
        limit=args.limit,
        sleep_seconds=args.sleep_seconds,
    )
    print(
        "[enrich-cn-sectors] "
        f"fetched={summary['fetched_sectors']} "
        f"failed={len(summary['failed_tickers'])} "
        f"events_filled={summary['events_filled']} "
        f"metadata_filled={summary['metadata_filled']}"
    )
    if args.check_only:
        print("[enrich-cn-sectors] check-only mode — not writing output.")
        return 0
    if not args.force:
        print("[enrich-cn-sectors] refusing to write without --force.")
        return 1
    events_output = Path(args.events_output) if args.events_output else events_path
    metadata_output = Path(args.metadata_output) if args.metadata_output else metadata_path
    events_output.parent.mkdir(parents=True, exist_ok=True)
    metadata_output.parent.mkdir(parents=True, exist_ok=True)
    enriched_events.to_csv(events_output, index=False)
    enriched_metadata.to_csv(metadata_output, index=False)
    print(f"[enrich-cn-sectors] wrote {events_output}")
    print(f"[enrich-cn-sectors] wrote {metadata_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
