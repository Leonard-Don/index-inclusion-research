"""Reconstruct ``data/processed/hs300_weight_change.csv`` for H6 verdict.

H6 (指数权重可预测性) wants per-event ``weight_change`` — the share of
the index that an added/removed name represents at the batch boundary.
The existing reconstructed candidates CSV only carries a rank-mapped
``running_variable`` (top of batch = 600..tail = 1), which is NOT a
real market-cap proxy. This module rebuilds a real
``mkt_cap_proxy = close × shares_outstanding`` per ticker per batch and
turns that into ``weight_proxy = mkt_cap_proxy / sum(mkt_cap_proxy)``
within each batch's top-300 inclusion bucket.

The module is split into pure functions and a thin CLI. Pure functions
take pandas frames and a ``MarketCapFetcher`` callable so tests can
swap in synthetic data; the CLI wires them to the existing akshare /
yfinance helpers in ``reconstruct_hs300_rdd_candidates``.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

from index_inclusion_research import paths

ROOT = paths.project_root()
DEFAULT_INPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv"
DEFAULT_OUTPUT = ROOT / "data" / "processed" / "hs300_weight_change.csv"
DEFAULT_LOCAL_PRICES = ROOT / "data" / "raw" / "real_prices.csv"


# ── pure functions ───────────────────────────────────────────────────


# Signature for a market-cap fetcher.  Receives a single (market, ticker,
# date) tuple and returns ``mkt_cap`` (price × shares) or ``None`` on
# failure. Production wiring uses akshare for CN and yfinance for US;
# tests inject a closure backed by a fixture dict.
MarketCapFetcher = Callable[[str, str, str], float | None]


def _normalise_key(market: object, ticker: object) -> tuple[str, str]:
    market_text = str(market).strip().upper()
    ticker_text = str(ticker).strip()
    if ticker_text.endswith(".0"):
        ticker_text = ticker_text[:-2]
    if market_text == "CN":
        ticker_text = ticker_text.zfill(6)
    return market_text, ticker_text


def attach_market_caps(
    candidates: pd.DataFrame,
    *,
    fetcher: MarketCapFetcher,
    only_inclusion: bool = True,
) -> pd.DataFrame:
    """Attach a ``mkt_cap_proxy`` column to ``candidates``.

    For each candidate row, calls ``fetcher(market, ticker, announce_date)``.
    Failures propagate as NaN. If ``only_inclusion`` is True, rows with
    ``inclusion != 1`` are skipped (NaN).
    """
    work = candidates.copy()
    caps: list[float | None] = []
    for _, row in work.iterrows():
        if only_inclusion and int(row.get("inclusion", 0)) != 1:
            caps.append(None)
            continue
        market = str(row["market"])
        ticker = str(row["ticker"])
        announce_date = str(row["announce_date"])
        try:
            value = fetcher(market, ticker, announce_date)
        except Exception:  # noqa: BLE001 — fetcher contract: errors → None
            logger.exception(
                "weight_change fetcher failed for %s/%s/%s",
                market, ticker, announce_date,
            )
            value = None
        caps.append(value if value is not None and value > 0 else None)
    work["mkt_cap_proxy"] = caps
    return work


def compute_weight_proxy(candidates_with_caps: pd.DataFrame) -> pd.DataFrame:
    """Compute ``weight_proxy`` per (batch_id, ticker) on inclusion=1 rows.

    Per-batch denominator = sum of ``mkt_cap_proxy`` across rows with
    ``inclusion=1`` and a non-NaN cap. Output retains only inclusion=1
    rows that have a positive denominator and a non-NaN cap.
    """
    if candidates_with_caps.empty:
        return pd.DataFrame(
            columns=[
                "batch_id", "market", "ticker",
                "announce_date", "effective_date",
                "mkt_cap_proxy", "batch_total_mkt_cap", "weight_proxy",
            ]
        )
    df = candidates_with_caps.loc[
        (candidates_with_caps["inclusion"] == 1)
        & candidates_with_caps["mkt_cap_proxy"].notna()
    ].copy()
    if df.empty:
        return df.assign(batch_total_mkt_cap=pd.Series(dtype=float),
                         weight_proxy=pd.Series(dtype=float))
    totals = df.groupby("batch_id")["mkt_cap_proxy"].transform("sum")
    df["batch_total_mkt_cap"] = totals
    df["weight_proxy"] = df["mkt_cap_proxy"] / totals
    keep_cols = [
        "batch_id", "market", "ticker",
        "announce_date", "effective_date",
        "mkt_cap_proxy", "batch_total_mkt_cap", "weight_proxy",
    ]
    return df[keep_cols].reset_index(drop=True)


def compute_weight_change_table(
    candidates: pd.DataFrame,
    *,
    fetcher: MarketCapFetcher,
) -> pd.DataFrame:
    """End-to-end pipeline: candidates + fetcher → weight_change frame."""
    enriched = attach_market_caps(candidates, fetcher=fetcher)
    return compute_weight_proxy(enriched)


def build_local_market_cap_fetcher(
    prices_path: str | Path = DEFAULT_LOCAL_PRICES,
    *,
    lookback_days: int = 10,
) -> MarketCapFetcher:
    """Build a ``MarketCapFetcher`` from a local real_prices.csv file.

    The fetcher returns the latest positive ``mkt_cap`` observation at or
    before ``announce_date`` within ``lookback_days``. This keeps the H6
    reconstruction reproducible from the project-local real price panel
    instead of re-querying one external API call per candidate.
    """
    path = Path(prices_path)
    required = ["market", "ticker", "date", "mkt_cap"]
    prices = pd.read_csv(path, usecols=required, dtype={"ticker": str})
    prices["market"] = prices["market"].astype(str).str.strip().str.upper()
    prices["ticker"] = prices["ticker"].astype(str).str.strip()
    cn_mask = prices["market"] == "CN"
    prices.loc[cn_mask, "ticker"] = prices.loc[cn_mask, "ticker"].str.zfill(6)
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.normalize()
    prices["mkt_cap"] = pd.to_numeric(prices["mkt_cap"], errors="coerce")
    prices = prices.loc[
        prices["date"].notna()
        & prices["mkt_cap"].notna()
        & (prices["mkt_cap"] > 0)
    ].copy()
    prices = prices.sort_values(["market", "ticker", "date"])

    lookup: dict[tuple[str, str], tuple[pd.DatetimeIndex, list[float]]] = {}
    for (market, ticker), group in prices.groupby(["market", "ticker"], sort=False):
        lookup[(str(market), str(ticker))] = (
            pd.DatetimeIndex(group["date"]),
            [float(value) for value in group["mkt_cap"]],
        )

    lookback_delta = pd.Timedelta(days=lookback_days)

    def _fetch(market: str, ticker: str, announce_date: str) -> float | None:
        key = _normalise_key(market, ticker)
        entry = lookup.get(key)
        if entry is None:
            return None
        dates, values = entry
        target = pd.to_datetime(announce_date, errors="coerce")
        if pd.isna(target):
            return None
        target = pd.Timestamp(target).normalize()
        position = dates.searchsorted(target, side="right") - 1
        if position < 0:
            return None
        if dates[position] < target - lookback_delta:
            return None
        return values[position]

    return _fetch


def export_weight_change_table(
    frame: pd.DataFrame,
    *,
    output_path: Path = DEFAULT_OUTPUT,
) -> Path:
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out_path, index=False)
    return out_path


# ── production fetchers (lazy-imported to keep tests fast) ───────────


def _akshare_cn_market_cap(ticker: str, announce_date: str) -> float | None:
    """Fetch CN ticker market cap via akshare.

    Best-effort: returns None if the API or network is unreachable. Heavy
    rate limits in real use; callers should batch + cache.
    """
    try:
        import akshare as ak
    except ImportError:
        logger.debug("akshare not installed; skipping CN market cap")
        return None
    try:
        # 2-day window so we tolerate weekends + holidays
        end = pd.Timestamp(announce_date)
        start = (end - pd.Timedelta(days=5)).strftime("%Y%m%d")
        df = ak.stock_zh_a_hist(
            symbol=ticker,
            period="daily",
            start_date=start,
            end_date=end.strftime("%Y%m%d"),
            adjust="",
        )
        if df is None or df.empty:
            return None
        close = float(df["收盘"].iloc[-1])
    except (KeyError, ValueError, OSError) as exc:
        logger.debug("akshare price fetch failed for %s/%s: %s", ticker, announce_date, exc)
        return None
    try:
        info = ak.stock_individual_info_em(symbol=ticker)
        if info is None or info.empty:
            return None
        shares_row = info.loc[info["item"].astype(str).str.contains("流通股", na=False)]
        if shares_row.empty:
            return None
        shares = float(shares_row["value"].iloc[0])
    except (KeyError, ValueError, OSError) as exc:
        logger.debug("akshare shares fetch failed for %s: %s", ticker, exc)
        return None
    return close * shares if close > 0 and shares > 0 else None


def _yfinance_us_market_cap(ticker: str, announce_date: str) -> float | None:
    """Fetch US ticker market cap via yfinance fast_info."""
    try:
        import yfinance as yf
    except ImportError:
        logger.debug("yfinance not installed; skipping US market cap")
        return None
    try:
        info = yf.Ticker(ticker).fast_info
        return float(info.get("market_cap") or 0.0) or None
    except (KeyError, AttributeError, OSError, ValueError) as exc:
        logger.debug("yfinance market_cap unavailable for %s: %s", ticker, exc)
        return None


def production_market_cap_fetcher(market: str, ticker: str, announce_date: str) -> float | None:
    """Default fetcher used by the CLI: akshare for CN, yfinance for US."""
    if market == "CN":
        return _akshare_cn_market_cap(ticker, announce_date)
    if market == "US":
        return _yfinance_us_market_cap(ticker, announce_date)
    return None


# ── CLI ──────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Reconstruct hs300_weight_change.csv by attaching real "
            "(price × shares) market caps to the reconstructed RDD "
            "candidates and computing per-batch weight proxies."
        )
    )
    parser.add_argument(
        "--input", default=str(DEFAULT_INPUT),
        help=f"Source candidates CSV (default: {DEFAULT_INPUT}).",
    )
    parser.add_argument(
        "--output", default=str(DEFAULT_OUTPUT),
        help=f"Where to write the weight_change frame (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Overwrite an existing output file.",
    )
    parser.add_argument(
        "--local-prices",
        default=str(DEFAULT_LOCAL_PRICES),
        help=(
            "Local real_prices.csv used for market-cap lookup before falling "
            f"back to live APIs (default: {DEFAULT_LOCAL_PRICES})."
        ),
    )
    parser.add_argument(
        "--no-local-prices",
        action="store_true",
        help="Skip the local real_prices.csv lookup and use live API fetchers.",
    )
    parser.add_argument(
        "--fallback-network",
        action="store_true",
        help="When local prices miss a row, fall back to akshare/yfinance.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=10,
        help="Maximum days before announce_date to accept a local mkt_cap row.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Optional limit on number of candidate rows to process (for smoke runs).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    input_path = Path(args.input)
    output_path = Path(args.output)
    if not input_path.exists():
        print(f"[compute-h6-weight-change] input file not found: {input_path}")
        return 1
    if output_path.exists() and not args.force:
        print(
            f"[compute-h6-weight-change] refusing to overwrite without --force: {output_path}"
        )
        return 1

    candidates = pd.read_csv(input_path, dtype={"ticker": str})
    if args.limit:
        candidates = candidates.head(args.limit)

    fetcher: MarketCapFetcher = production_market_cap_fetcher
    source_message = "live akshare (CN) + yfinance (US)"
    local_prices_path = Path(args.local_prices)
    if not args.no_local_prices and local_prices_path.exists():
        local_fetcher = build_local_market_cap_fetcher(
            local_prices_path, lookback_days=args.lookback_days
        )
        source_message = (
            f"local prices {local_prices_path} "
            f"(lookback={args.lookback_days}d)"
        )
        if args.fallback_network:
            source_message += " with live API fallback"

            def _fetch_with_fallback(
                market: str, ticker: str, announce_date: str
            ) -> float | None:
                local_value = local_fetcher(market, ticker, announce_date)
                if local_value is not None:
                    return local_value
                return production_market_cap_fetcher(market, ticker, announce_date)

            fetcher = _fetch_with_fallback
        else:
            fetcher = local_fetcher
    elif not args.no_local_prices:
        print(
            f"[compute-h6-weight-change] local prices not found: {local_prices_path}; "
            "falling back to live APIs."
        )

    print(
        f"[compute-h6-weight-change] processing {len(candidates)} rows; "
        f"market caps from {source_message}."
    )
    weight_frame = compute_weight_change_table(candidates, fetcher=fetcher)
    if weight_frame.empty:
        print(
            "[compute-h6-weight-change] no rows survived market-cap fetch — "
            "all calls returned None. Check network connectivity / API rate limits."
        )
        return 1
    out = export_weight_change_table(weight_frame, output_path=output_path)
    print(
        f"[compute-h6-weight-change] wrote {len(weight_frame)} rows to {out} "
        f"({weight_frame['market'].value_counts().to_dict()})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
