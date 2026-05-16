"""Build a CN passive-AUM proxy series by aggregating ETF TNA at year-end.

The existing ``data/raw/passive_aum.csv`` carries a top-down CN series
derived from total public-fund AUM × current index-fund share
(:mod:`download_passive_aum_cn`). That series is useful for the H2
verdict's market-aggregate trend, but it has two limitations the
project audit flagged:

1. It collapses every passive vehicle (broad-market, sector, smart-beta,
   bond ETFs, index OEFs) into one number, so it cannot be matched to
   the inclusion-event panel (which only studies CSI300 / CSI500).
2. The index-fund share is held constant at the current snapshot — i.e.
   it back-applies today's ETF penetration to 2021-22 and likely
   understates the post-2024 acceleration.

This module produces a complementary, **bottom-up** proxy that is
narrower but more transparent: year-end Total Net Assets (TNA) of all
public ETFs tracking a given equity index, aggregated by index.

Data path
---------

For each year-end snapshot ``Y-12-31`` (or the nearest preceding
trading day), we fetch:

* SH-listed ETF shares via :func:`akshare.fund_etf_scale_sse`
  ``(date=Y-12-31)``
* SZ-listed ETF shares via :func:`akshare.fund_scale_daily_szse`
  ``(start_date=Y-12-22, end_date=Y-12-31, symbol="ETF")``
* The unit NAV at that date via :func:`akshare.fund_etf_fund_info_em`
  ``(fund=code, start_date=Y-12-15, end_date=Y-12-31)``

then TNA = shares × NAV (RMB). Only ETFs whose 6-digit code is in a
curated tracking list per index are counted, so e.g. S&P 500 ETFs do
not pollute the CSI500 row.

The output schema is fixed:

.. code-block:: text

    index_name, snapshot_date, total_tna_cny_billions, etf_count, source, note

This file is consumed by the H2 verdict path in
``analysis/cross_market_asymmetry/verdicts/_h_functions._h2`` as the
CN-side analogue to the US Z.1 passive AUM trend. See
``docs/limitations.md`` §3 and ``data/raw/README.md`` for caveats.
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from index_inclusion_research import paths

logger = logging.getLogger(__name__)

ROOT = paths.project_root()
DEFAULT_OUTPUT = ROOT / "data" / "raw" / "cn_passive_aum_proxy.csv"

SOURCE_TAG = "akshare:fund_etf_scale_sse+fund_scale_daily_szse+fund_etf_fund_info_em:aggregated_by_underlying_index"
NOTE_DEFAULT = (
    "Proxy: sum of year-end TNA (shares*NAV) of curated ETF list tracking the index. "
    "NAV comes from fund_etf_fund_info_em with fund_etf_hist_em close-price fallback "
    "when the NAV endpoint returns empty on transient failure. Not direct passive AUM "
    "disclosure; ETF universe expands over time so older snapshots understate true "
    "passive-tracking AUM. See docs/limitations.md §3."
)


# Curated ETF tickers per CN equity index. Codes are 6-digit
# strings matching the ``基金代码`` column on both exchanges. Codes that
# do not yet exist at an earlier snapshot date will simply be missing
# from that year's aggregate — this is the honest behaviour because the
# ETF was not yet launched.
#
# Source for tickers: cross-referenced against 中证指数公司 product pages
# and East Money fund directories (https://fund.eastmoney.com/). Bond
# ETFs, sector slices, smart-beta variants are intentionally excluded so
# the proxy represents broad passive tracking AUM, not the whole ETF
# market.
CSI300_TICKERS: tuple[str, ...] = (
    # Shanghai 510/515/516/517 series
    "510300",  # 华泰柏瑞沪深300ETF (the long-standing flagship)
    "510310",  # 易方达沪深300ETF
    "510330",  # 华夏沪深300ETF
    "510350",  # 工银瑞信沪深300ETF
    "510360",  # 广发沪深300ETF
    "510370",  # 兴业沪深300ETF
    "510380",  # 国寿安保沪深300ETF
    "515130",  # 博时沪深300ETF
    "515300",  # 华富沪深300ETF (HS300)
    "515310",  # 添富沪深300ETF
    "515330",  # 天弘沪深300ETF
    "515350",  # 民生沪深300ETF
    "515360",  # 方正富邦沪深300ETF
    "515380",  # 泰康沪深300ETF
    "515390",  # 华安沪深300ETF
    "515660",  # 国联安沪深300ETF
    # Shenzhen 159 series
    "159673",  # 鹏华沪深300ETF
    "159912",  # 富国沪深300ETF (深300ETF)
    "159919",  # 嘉实沪深300ETF (the SZ-side flagship; very large)
    "159925",  # 南方沪深300ETF
    "159330",  # 申万菱信沪深300ETF基金
    "159300",  # 摩根资产沪深300ETF
)

CSI500_TICKERS: tuple[str, ...] = (
    # Shanghai (true CSI500 trackers — excludes A500, S&P500, 标普500, etc.)
    "510500",  # 南方中证500ETF (the long-standing flagship)
    "510510",  # 广发中证500ETF
    "510530",  # 工银瑞信中证500ETF
    "510550",  # 方正富邦中证500ETF
    "510560",  # 国寿安保中证500ETF
    "510570",  # 兴业中证500ETF
    "510580",  # 易方达中证500ETF (ZZ500ETF)
    "510590",  # 平安中证500ETF
    "512500",  # 华夏中证500ETF
    # Shenzhen 159 series
    "159337",  # 摩根资产中证500ETF基金
    "159922",  # 嘉实中证500ETF
)

INDEX_TICKERS: dict[str, tuple[str, ...]] = {
    "CSI300": CSI300_TICKERS,
    "CSI500": CSI500_TICKERS,
}


@dataclass(frozen=True)
class SnapshotConfig:
    """One ``snapshot_date`` we want a TNA aggregate for.

    ``year_end_date`` is the canonical reporting date written into the
    CSV ``snapshot_date`` column. ``probe_window_start`` and
    ``probe_window_end`` are the *trading-day* search window passed to
    AKShare; we look up the latest trading day in this window to handle
    year-end holidays (e.g. 2022-12-31 falls on a weekend so we use
    2022-12-30 under the hood, but still report 2022-12-31 as the
    snapshot date).
    """

    year_end_date: str  # ISO YYYY-MM-DD; the canonical year-end label
    probe_window_start: str  # YYYYMMDD; akshare format
    probe_window_end: str  # YYYYMMDD; akshare format


DEFAULT_SNAPSHOTS: tuple[SnapshotConfig, ...] = (
    SnapshotConfig("2020-12-31", "20201221", "20201231"),
    SnapshotConfig("2021-12-31", "20211220", "20211231"),
    SnapshotConfig("2022-12-31", "20221220", "20221230"),
    SnapshotConfig("2023-12-31", "20231218", "20231229"),
    SnapshotConfig("2024-12-31", "20241223", "20241231"),
)


@dataclass(frozen=True)
class TnaRow:
    """One aggregated row that maps 1:1 to a CSV line."""

    index_name: str
    snapshot_date: str
    total_tna_cny_billions: float
    etf_count: int
    source: str = SOURCE_TAG
    note: str = NOTE_DEFAULT


@dataclass
class FetchStats:
    """Per-snapshot run stats, surfaced in stdout output for transparency."""

    snapshot_date: str
    sh_total_etfs: int = 0
    sz_total_etfs: int = 0
    nav_lookups_ok: int = 0
    nav_lookups_failed: int = 0
    failed_codes: list[str] = field(default_factory=list)


def _to_iso(value: object) -> str:
    """Coerce an akshare date cell (may be ``str``/``date``/``Timestamp``) to ISO."""
    if isinstance(value, str):
        return value
    try:
        return pd.Timestamp(value).date().isoformat()
    except Exception:
        return str(value)


def _fetch_sh_shares(window_start: str, window_end: str) -> pd.DataFrame:
    """Return SH ETF shares snapshot at the latest trading day in window.

    Returns a DataFrame with columns ``code, shares, snapshot_iso``.
    Empty DataFrame if AKShare returns nothing usable.
    """
    import akshare as ak

    # SSE endpoint accepts a single date. Walk back from window_end until
    # we hit a trading day (~5 attempts covers any year-end holiday).
    end_date = pd.Timestamp(window_end)
    start_date = pd.Timestamp(window_start)
    while end_date >= start_date:
        attempt = end_date.strftime("%Y%m%d")
        try:
            df = ak.fund_etf_scale_sse(date=attempt)
        except Exception as exc:
            logger.debug("fund_etf_scale_sse(%s) failed: %s", attempt, exc)
            df = pd.DataFrame()
        if df is None or df.empty:
            end_date -= pd.Timedelta(days=1)
            continue
        out = df.rename(
            columns={"基金代码": "code", "基金份额": "shares", "统计日期": "snapshot_iso"}
        )[["code", "shares", "snapshot_iso"]].copy()
        out["code"] = out["code"].astype(str).str.zfill(6)
        out["shares"] = pd.to_numeric(out["shares"], errors="coerce")
        out["snapshot_iso"] = out["snapshot_iso"].map(_to_iso)
        return out.dropna(subset=["shares"]).reset_index(drop=True)
    return pd.DataFrame(columns=["code", "shares", "snapshot_iso"])


def _fetch_sz_shares(window_start: str, window_end: str) -> pd.DataFrame:
    """Return SZ ETF shares snapshot at the latest trading day in window."""
    import akshare as ak

    try:
        df = ak.fund_scale_daily_szse(
            start_date=window_start, end_date=window_end, symbol="ETF"
        )
    except Exception as exc:
        logger.debug("fund_scale_daily_szse(%s,%s) failed: %s", window_start, window_end, exc)
        return pd.DataFrame(columns=["code", "shares", "snapshot_iso"])
    if df is None or df.empty:
        return pd.DataFrame(columns=["code", "shares", "snapshot_iso"])
    work = df.rename(
        columns={"基金代码": "code", "基金份额": "shares", "日期": "snapshot_iso"}
    )[["code", "shares", "snapshot_iso"]].copy()
    work["code"] = work["code"].astype(str).str.zfill(6)
    work["shares"] = pd.to_numeric(work["shares"], errors="coerce")
    work["snapshot_iso"] = work["snapshot_iso"].map(_to_iso)
    work = work.dropna(subset=["shares"])
    if work.empty:
        return work
    # Keep the latest trading day inside the window.
    latest = work["snapshot_iso"].max()
    return work.loc[work["snapshot_iso"] == latest].reset_index(drop=True)


def _fetch_nav_via_info_em(code: str, window_start: str, window_end: str) -> float | None:
    """Try the primary NAV endpoint (fundf10 / fund_etf_fund_info_em)."""
    import akshare as ak

    try:
        df = ak.fund_etf_fund_info_em(
            fund=code, start_date=window_start, end_date=window_end
        )
    except Exception as exc:
        logger.debug("fund_etf_fund_info_em(%s) errored: %s", code, exc)
        return None
    if df is None or df.empty or "单位净值" not in df.columns:
        return None
    df = df.dropna(subset=["单位净值"]).copy()
    if df.empty:
        return None
    df["净值日期"] = pd.to_datetime(df["净值日期"], errors="coerce")
    df = df.dropna(subset=["净值日期"]).sort_values("净值日期")
    if df.empty:
        return None
    try:
        return float(df["单位净值"].iloc[-1])
    except (TypeError, ValueError):
        return None


def _fetch_nav_via_hist_em(code: str, window_start: str, window_end: str) -> float | None:
    """Fallback NAV: ETF close price (premium/discount typically <0.5%)."""
    import akshare as ak

    try:
        df = ak.fund_etf_hist_em(
            symbol=code,
            period="daily",
            start_date=window_start,
            end_date=window_end,
            adjust="",
        )
    except Exception as exc:
        logger.debug("fund_etf_hist_em(%s) errored: %s", code, exc)
        return None
    if df is None or df.empty or "收盘" not in df.columns:
        return None
    df = df.dropna(subset=["收盘"]).copy()
    if df.empty:
        return None
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"]).sort_values("日期")
    if df.empty:
        return None
    try:
        return float(df["收盘"].iloc[-1])
    except (TypeError, ValueError):
        return None


def _fetch_nav_at_date(
    code: str,
    window_start: str,
    window_end: str,
    *,
    retries: int = 3,
    polite_sleep: float = 0.15,
) -> float | None:
    """Return unit NAV at the latest trading day on/before ``window_end``.

    Tries :func:`akshare.fund_etf_fund_info_em` first (true NAV from
    fundf10), falling back to :func:`akshare.fund_etf_hist_em` close
    price when the NAV endpoint returns empty — they live on different
    hosts so a transient outage in one rarely takes both down at once.

    A small ``polite_sleep`` after each attempt keeps the per-snapshot
    request rate well under akshare's typical rate limit.
    """
    for attempt in range(retries + 1):
        nav = _fetch_nav_via_info_em(code, window_start, window_end)
        time.sleep(polite_sleep)
        if nav is not None and nav > 0:
            return nav
        nav = _fetch_nav_via_hist_em(code, window_start, window_end)
        time.sleep(polite_sleep)
        if nav is not None and nav > 0:
            return nav
        if attempt < retries:
            time.sleep(0.5 * (attempt + 1))
    return None


def build_tna_for_snapshot(
    snapshot: SnapshotConfig,
    index_tickers: dict[str, tuple[str, ...]],
) -> tuple[list[TnaRow], FetchStats]:
    """Aggregate TNA at one snapshot date across all configured indices."""
    stats = FetchStats(snapshot_date=snapshot.year_end_date)
    sh_shares = _fetch_sh_shares(snapshot.probe_window_start, snapshot.probe_window_end)
    sz_shares = _fetch_sz_shares(snapshot.probe_window_start, snapshot.probe_window_end)
    stats.sh_total_etfs = int(len(sh_shares))
    stats.sz_total_etfs = int(len(sz_shares))
    shares = pd.concat([sh_shares, sz_shares], ignore_index=True)
    shares_by_code = dict(zip(shares["code"], shares["shares"], strict=False))

    rows: list[TnaRow] = []
    for index_name, tickers in index_tickers.items():
        total_tna_rmb = 0.0
        etf_count = 0
        for code in tickers:
            code_str = str(code).zfill(6)
            shares_at_date = shares_by_code.get(code_str)
            if shares_at_date is None or pd.isna(shares_at_date):
                # ETF not present in either exchange snapshot at this date;
                # treat as "not launched yet" rather than zero.
                continue
            nav = _fetch_nav_at_date(
                code_str,
                snapshot.probe_window_start,
                snapshot.probe_window_end,
            )
            if nav is None or nav <= 0:
                stats.nav_lookups_failed += 1
                stats.failed_codes.append(code_str)
                continue
            stats.nav_lookups_ok += 1
            total_tna_rmb += float(shares_at_date) * float(nav)
            etf_count += 1
        rows.append(
            TnaRow(
                index_name=index_name,
                snapshot_date=snapshot.year_end_date,
                total_tna_cny_billions=total_tna_rmb / 1e9,
                etf_count=etf_count,
            )
        )
    return rows, stats


def assemble_proxy_frame(
    snapshots: tuple[SnapshotConfig, ...] = DEFAULT_SNAPSHOTS,
    index_tickers: dict[str, tuple[str, ...]] | None = None,
) -> tuple[pd.DataFrame, list[FetchStats]]:
    """Return ``(DataFrame, list[FetchStats])`` for the full snapshot set."""
    if index_tickers is None:
        index_tickers = INDEX_TICKERS
    all_rows: list[TnaRow] = []
    all_stats: list[FetchStats] = []
    for snapshot in snapshots:
        logger.info(
            "[cn-passive-aum-proxy] snapshot %s (window %s..%s)",
            snapshot.year_end_date,
            snapshot.probe_window_start,
            snapshot.probe_window_end,
        )
        rows, stats = build_tna_for_snapshot(snapshot, index_tickers)
        all_rows.extend(rows)
        all_stats.append(stats)
    df = pd.DataFrame(
        [
            {
                "index_name": row.index_name,
                "snapshot_date": row.snapshot_date,
                "total_tna_cny_billions": row.total_tna_cny_billions,
                "etf_count": row.etf_count,
                "source": row.source,
                "note": row.note,
            }
            for row in all_rows
        ]
    )
    if not df.empty:
        df = df.sort_values(["index_name", "snapshot_date"]).reset_index(drop=True)
    return df, all_stats


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate CN equity-index ETF TNA at year-end snapshots as a "
            "passive-AUM proxy (CSI300 / CSI500). Writes data/raw/cn_passive_aum_proxy.csv."
        )
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=f"Output CSV path (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Fetch and print but don't write the CSV.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    df, stats_list = assemble_proxy_frame()
    if df.empty:
        print("[cn-passive-aum-proxy] no rows produced — aborting.")
        return 1
    print("[cn-passive-aum-proxy] aggregated TNA (CNY billions):")
    for _, row in df.iterrows():
        print(
            f"  {row['index_name']:<8} {row['snapshot_date']}:"
            f"  {row['total_tna_cny_billions']:>10.3f}  ({int(row['etf_count'])} ETFs)"
        )
    print()
    for stats in stats_list:
        print(
            f"  [{stats.snapshot_date}] SH ETFs={stats.sh_total_etfs},"
            f" SZ ETFs={stats.sz_total_etfs},"
            f" NAV ok/fail={stats.nav_lookups_ok}/{stats.nav_lookups_failed}"
        )
        if stats.failed_codes:
            print(f"    failed codes (no NAV): {', '.join(stats.failed_codes[:10])}")

    if args.check_only:
        print("[cn-passive-aum-proxy] check-only — not writing.")
        return 0

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"[cn-passive-aum-proxy] wrote {output_path}: {len(df)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
