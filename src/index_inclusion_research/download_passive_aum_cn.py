"""Download a CN passive-fund AUM yearly series via AKShare.

CN passive AUM is not published as a single yearly series. We construct
it from two AKShare endpoints:

* ``fund_aum_trend_em`` — quarterly all-industry public-fund AUM (RMB).
* ``fund_aum_hist_em``  — current snapshot of per-company AUM split by
  product type (含 ``指数型`` 列, i.e. index funds).

We compute today's index-fund-share-of-total ratio from ``fund_aum_hist_em``
and apply it uniformly to each quarter in ``fund_aum_trend_em`` to obtain
an indicative quarterly CN passive AUM series, then collapse to the
year-end (12-31) row per calendar year.

Output rows are appended (idempotent) to ``data/raw/passive_aum.csv``
in the existing ``market, year, aum_trillion`` schema. CN values are
**trillions of RMB**, not USD. ``_h2`` consumes US data only for
verdict computation; the CN rows lift evidence coverage from ``warn``
to ``pass`` and unblock the H2 evidence manifest, but absolute
RMB-vs-USD comparison is not meaningful — see ``docs/limitations.md``
and the ``H2_passive_aum`` row in
``results/real_tables/cma_evidence_manifest.csv``.

Caveats (also flagged in stdout output):

* The index-fund share is held constant at the current snapshot. ETF
  growth was concentrated in 2024-2025, so applying today's share
  backwards understates the recent acceleration. This is acknowledged
  as the best AKShare-only approximation; ground-truth historical
  shares need 中国基金业协会 (AMAC) annual reports or paid sources.
* The trend endpoint starts at 2021-Q2; pre-2021 data is not available
  through AKShare and would require AMAC PDFs / Wind extracts.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from index_inclusion_research import paths

logger = logging.getLogger(__name__)

ROOT = paths.project_root()
DEFAULT_OUTPUT = ROOT / "data" / "raw" / "passive_aum.csv"

# Threshold: skip ratio refresh if total AUM looks insanely small (
# AKShare endpoint sometimes returns truncated rows during outages).
MIN_REASONABLE_TOTAL_YIYUAN = 100_000.0  # ~10 trillion RMB


def fetch_index_fund_share() -> float:
    """Return the index-fund share of total public-fund AUM (snapshot)."""
    import akshare as ak

    breakdown = ak.fund_aum_hist_em()
    total_yiyuan = float(breakdown["总规模"].sum())
    index_yiyuan = float(breakdown["指数型"].sum())
    if total_yiyuan < MIN_REASONABLE_TOTAL_YIYUAN:
        raise RuntimeError(
            f"fund_aum_hist_em total ({total_yiyuan:.2f} 亿 RMB) below sanity "
            f"threshold; refusing to compute index-fund share."
        )
    if index_yiyuan <= 0:
        raise RuntimeError("fund_aum_hist_em 指数型 column is zero or negative")
    return index_yiyuan / total_yiyuan


def fetch_total_aum_quarterly() -> pd.DataFrame:
    """Return ``DataFrame[date: datetime, value_rmb: float]`` of all-industry AUM."""
    import akshare as ak

    trend = ak.fund_aum_trend_em()
    if trend.empty or not {"date", "value"}.issubset(trend.columns):
        raise RuntimeError("fund_aum_trend_em returned an unexpected schema")
    out = trend.rename(columns={"value": "value_rmb"}).copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return out


def build_cn_yearly_passive_aum(
    quarterly: pd.DataFrame,
    index_share: float,
) -> pd.DataFrame:
    """Collapse the quarterly all-industry series to year-end rows scaled by share.

    Returns a DataFrame in the canonical ``market, year, aum_trillion`` schema
    (RMB trillion).
    """
    work = quarterly.copy()
    work["passive_rmb"] = work["value_rmb"].astype(float) * float(index_share)
    work["year"] = work["date"].dt.year
    work["month"] = work["date"].dt.month

    year_end = work.loc[work["month"] == 12].copy()
    if year_end.empty:
        # Fallback: latest available quarter per year (e.g. partial-year tail).
        year_end = work.sort_values("date").groupby("year", as_index=False).tail(1)

    out = (
        year_end[["year", "passive_rmb"]]
        .rename(columns={"passive_rmb": "aum_trillion"})
        .assign(market="CN")
        .loc[:, ["market", "year", "aum_trillion"]]
        .reset_index(drop=True)
    )
    out["aum_trillion"] = out["aum_trillion"] / 1e12  # RMB → RMB trillion
    out["year"] = out["year"].astype(int)
    return out


def merge_into_csv(
    new_rows: pd.DataFrame,
    output_path: Path,
    *,
    overwrite_cn: bool = True,
) -> pd.DataFrame:
    """Merge new CN rows into ``output_path``; preserve other markets verbatim.

    With ``overwrite_cn=True`` (default), any existing CN rows are replaced.
    The output is sorted by ``(market, year)``.
    """
    if output_path.exists():
        existing = pd.read_csv(output_path)
    else:
        existing = pd.DataFrame(columns=["market", "year", "aum_trillion"])

    if overwrite_cn:
        existing = existing.loc[existing["market"].astype(str).str.upper() != "CN"]
    merged = pd.concat([existing, new_rows], ignore_index=True)
    merged = merged.sort_values(["market", "year"]).reset_index(drop=True)
    merged["year"] = merged["year"].astype(int)
    return merged


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download CN passive-fund AUM yearly series from AKShare and merge "
            "into data/raw/passive_aum.csv."
        )
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=f"Output passive_aum.csv path (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Fetch and compute, print preview, but don't write the CSV.",
    )
    parser.add_argument(
        "--keep-existing-cn",
        action="store_true",
        help="If existing CSV has CN rows, leave them alone (default: replace).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    print("[download-passive-aum-cn] fetching fund_aum_hist_em (snapshot)...")
    share = fetch_index_fund_share()
    print(f"  index-fund share of total = {share:.4f}")

    print("[download-passive-aum-cn] fetching fund_aum_trend_em (quarterly)...")
    quarterly = fetch_total_aum_quarterly()
    print(f"  {len(quarterly)} quarters, {quarterly['date'].min().date()} → {quarterly['date'].max().date()}")

    cn_yearly = build_cn_yearly_passive_aum(quarterly, share)
    if cn_yearly.empty:
        print("[download-passive-aum-cn] no year-end rows produced — aborting.")
        return 1

    print("[download-passive-aum-cn] CN passive AUM (trillion RMB), year-end:")
    for _, row in cn_yearly.iterrows():
        print(f"  {int(row['year'])}: {row['aum_trillion']:.3f}")
    print(
        "  ⚠ CN values are RMB trillions; US rows in the same CSV are USD "
        "trillions. _h2 verdict only uses the US series, so this asymmetry "
        "does not pollute the verdict — but absolute CN-vs-US AUM "
        "comparisons require unit reconciliation."
    )
    print(
        "  ⚠ Index-fund share is the current snapshot held constant across "
        "all years (ETF growth concentrated in 2024-2025; share back-applied "
        "understates recent acceleration)."
    )

    if args.check_only:
        print("[download-passive-aum-cn] check-only — not writing.")
        return 0

    output_path = Path(args.output)
    merged = merge_into_csv(
        cn_yearly,
        output_path,
        overwrite_cn=not args.keep_existing_cn,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)
    cn_count = int((merged["market"].astype(str).str.upper() == "CN").sum())
    us_count = int((merged["market"].astype(str).str.upper() == "US").sum())
    print(f"[download-passive-aum-cn] wrote {output_path}: {us_count} US rows, {cn_count} CN rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
