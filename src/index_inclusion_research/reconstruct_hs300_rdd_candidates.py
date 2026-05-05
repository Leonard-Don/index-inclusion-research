from __future__ import annotations

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import akshare as ak
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

from index_inclusion_research import paths
from index_inclusion_research.analysis.rdd_candidates import (
    build_candidate_batch_audit,
    summarize_candidate_audit,
    validate_candidate_frame,
)
from index_inclusion_research.analysis.rdd_reconstruction import (
    build_reconstructed_candidate_frame,
    load_cached_proxy_market_caps,
    load_cn_reconstruction_batches,
    reconstruct_batch_membership,
)
from index_inclusion_research.loaders import save_dataframe

ROOT = paths.project_root()

DEFAULT_EVENTS = ROOT / "data" / "raw" / "real_events.csv"
DEFAULT_PRICES = ROOT / "data" / "raw" / "real_prices.csv"
DEFAULT_METADATA = ROOT / "data" / "raw" / "real_metadata.csv"
DEFAULT_OUTPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv"
DEFAULT_AUDIT_OUTPUT = ROOT / "results" / "literature" / "hs300_rdd_reconstruction" / "candidate_batch_audit.csv"
DEFAULT_SUMMARY_OUTPUT = ROOT / "results" / "literature" / "hs300_rdd_reconstruction" / "reconstruction_summary.md"


def _cn_code_to_yahoo_symbol(code: str) -> str:
    code = str(code).zfill(6)
    return f"{code}.SS" if code.startswith(("6", "9", "688")) else f"{code}.SZ"


def _relative_or_absolute(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT.resolve()))
    except ValueError:
        return str(path)


def _load_current_constituents() -> pd.DataFrame:
    current = ak.index_stock_cons_csindex("000300").copy()
    current["成分券代码"] = current["成分券代码"].astype(str).str.zfill(6)
    return current.loc[:, ["成分券代码", "成分券名称"]].rename(columns={"成分券代码": "ticker", "成分券名称": "security_name"})


def _fetch_missing_proxy_market_caps(
    tickers: list[str],
    *,
    target_date: str,
    share_cache: dict[str, float | None] | None = None,
) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame(columns=["ticker", "proxy_market_cap"])

    symbols = [_cn_code_to_yahoo_symbol(ticker) for ticker in tickers]
    history = yf.download(
        tickers=symbols,
        start=(pd.Timestamp(target_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d"),
        end=(pd.Timestamp(target_date) + pd.Timedelta(days=2)).strftime("%Y-%m-%d"),
        auto_adjust=False,
        progress=False,
        threads=True,
        group_by="ticker",
    )
    close_map: dict[str, float] = {}
    for ticker, symbol in zip(tickers, symbols, strict=True):
        try:
            history_frame = history[symbol] if len(symbols) > 1 else history
        except (KeyError, ValueError) as exc:
            logger.debug("yfinance history missing for %s: %s", symbol, exc)
            history_frame = pd.DataFrame()
        if history_frame.empty or "Close" not in history_frame.columns:
            continue
        close_series = history_frame["Close"].dropna()
        if close_series.empty:
            continue
        close_series.index = pd.to_datetime(close_series.index)
        close_on_or_before = close_series.loc[close_series.index <= pd.Timestamp(target_date)]
        if close_on_or_before.empty:
            continue
        close_map[ticker] = float(close_on_or_before.iloc[-1])

    def _fetch_shares(symbol: str) -> float | None:
        try:
            shares = yf.Ticker(symbol).fast_info.get("shares")
        except (KeyError, AttributeError, OSError, ValueError) as exc:
            logger.debug("yfinance shares unavailable for %s: %s", symbol, exc)
            shares = None
        if shares in (None, 0):
            return None
        return float(shares)

    cache = share_cache if share_cache is not None else {}
    shares_map: dict[str, float] = {}
    uncached_symbols = [
        (ticker, symbol)
        for ticker, symbol in zip(tickers, symbols, strict=True)
        if ticker in close_map and ticker not in cache
    ]
    with ThreadPoolExecutor(max_workers=min(24, len(symbols) or 1)) as executor:
        future_map = {
            executor.submit(_fetch_shares, symbol): ticker
            for ticker, symbol in uncached_symbols
        }
        for future in as_completed(future_map):
            shares = future.result()
            cache[future_map[future]] = shares

    for ticker in tickers:
        shares = cache.get(ticker)
        if shares is None or shares == 0:
            continue
        shares_map[ticker] = shares

    rows: list[dict[str, object]] = []
    for ticker in tickers:
        close_value = close_map.get(ticker)
        shares = shares_map.get(ticker)
        if close_value is None or shares is None:
            continue
        rows.append({"ticker": ticker, "proxy_market_cap": close_value * shares})
    return pd.DataFrame(rows)


def _normalize_announce_dates(raw_dates: list[str] | None) -> list[str]:
    normalized: list[str] = []
    for raw in raw_dates or []:
        for part in str(raw).split(","):
            value = part.strip()
            if not value:
                continue
            normalized.append(pd.Timestamp(value).strftime("%Y-%m-%d"))
    return normalized


def _select_reconstruction_batches(
    batches: list,
    *,
    announce_dates: list[str] | None,
    all_batches: bool,
) -> list:
    ordered = sorted(batches, key=lambda item: item.announce_date)
    if all_batches:
        return ordered
    normalized = _normalize_announce_dates(announce_dates)
    if not normalized:
        raise ValueError("Either --announce-date or --all-batches is required.")
    batch_map = {batch.announce_date: batch for batch in ordered}
    missing = [announce_date for announce_date in normalized if announce_date not in batch_map]
    if missing:
        available = ", ".join(batch.announce_date for batch in ordered)
        raise ValueError(
            f"Requested announce dates were not found: {', '.join(missing)}. Available batches: {available}"
        )
    selected: list = []
    seen: set[str] = set()
    for announce_date in normalized:
        if announce_date in seen:
            continue
        seen.add(announce_date)
        selected.append(batch_map[announce_date])
    return selected


def _resolve_selected_batches(
    current_constituents: set[str],
    batches: list,
    *,
    announce_dates: list[str] | None,
    all_batches: bool,
) -> tuple[list, dict[str, str]]:
    selected = _select_reconstruction_batches(
        batches,
        announce_dates=announce_dates,
        all_batches=all_batches,
    )
    if not all_batches:
        return selected, {}

    reconstructable_desc: list = []
    skipped: dict[str, str] = {}
    coverage_broken = False
    for batch in sorted(selected, key=lambda item: item.announce_date, reverse=True):
        if coverage_broken:
            skipped[batch.announce_date] = "Skipped because a later coverage gap already prevents consistent rollback to this batch."
            continue
        try:
            reconstruct_batch_membership(
                current_constituents,
                batches,
                target_announce_date=batch.announce_date,
            )
        except ValueError as exc:
            coverage_broken = True
            skipped[batch.announce_date] = str(exc)
            continue
        reconstructable_desc.append(batch)
    reconstructable = list(reversed(reconstructable_desc))
    if not reconstructable:
        raise ValueError("No batches could be reconstructed from the current constituent anchor.")
    return reconstructable, skipped


def _build_summary_text(
    *,
    selected_batches: list,
    output_path: Path,
    audit_path: Path,
    audit_summary: dict[str, int | None],
    candidate_count: int,
    missing_after_fetch: dict[str, list[str]],
    skipped_batches: dict[str, str],
) -> str:
    announce_dates = [batch.announce_date for batch in selected_batches]
    batch_ids = [batch.batch_id for batch in selected_batches]
    if len(selected_batches) == 1:
        selection_lines = [
            f"- 公告批次：`{announce_dates[0]}`",
            f"- 批次标识：`{batch_ids[0]}`",
        ]
    else:
        selection_lines = [
            f"- 重建批次数：`{len(selected_batches)}`",
            f"- 公告日期范围：`{announce_dates[0]}` 至 `{announce_dates[-1]}`",
            f"- 批次列表：`{', '.join(announce_dates)}`",
        ]
    lines = [
        "# HS300 RDD 公开重建候选样本摘要",
        "",
        *selection_lines,
        f"- 标准化输出：`{_relative_or_absolute(output_path)}`",
        f"- 批次审计：`{_relative_or_absolute(audit_path)}`",
        f"- 输出行数：`{candidate_count}`",
        f"- 候选批次数：`{audit_summary.get('candidate_batches')}`",
        f"- 调入样本数：`{audit_summary.get('treated_rows')}`",
        f"- 对照候选数：`{audit_summary.get('control_rows')}`",
        f"- 覆盖 cutoff 两侧的批次数：`{audit_summary.get('crossing_batches')}`",
        "",
        "口径说明：",
        "- 当前文件不是中证官方 reserve list，而是根据真实调样批次、当前成分股逆推和公开市值代理口径重建的边界样本。",
        "- `running_variable` 由边界样本内的代理市值降序排名线性映射到 cutoff=300 两侧，便于现有 RDD 流程直接读取。",
        "- 更适合课程论文、方法复现和公开数据版本的稳健性补充，不应表述为官方历史候选排名表。",
    ]
    if skipped_batches:
        lines.extend(
            [
                "",
                "未纳入本次重建的更早批次：",
            ]
        )
        for announce_date, reason in skipped_batches.items():
            lines.append(f"- `{announce_date}`：{reason}")
    if missing_after_fetch:
        lines.extend(
            [
                "",
                "未成功补齐代理市值的股票：",
            ]
        )
        for announce_date, tickers in missing_after_fetch.items():
            lines.append(f"- `{announce_date}`：`{', '.join(tickers[:40])}`")
    return "\n".join(lines) + "\n"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconstruct an HS300 RDD candidate file from public proxy data.")
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--announce-date",
        action="append",
        dest="announce_dates",
        help="Target CN review announce date. Repeat the flag or pass a comma-separated list for multiple batches.",
    )
    selection.add_argument(
        "--all-batches",
        action="store_true",
        help="Reconstruct every available CN review batch in real_events.csv into one aggregated candidate file.",
    )
    parser.add_argument("--events", default=str(DEFAULT_EVENTS), help="Path to real_events.csv.")
    parser.add_argument("--prices", default=str(DEFAULT_PRICES), help="Path to real_prices.csv cache.")
    parser.add_argument("--metadata", default=str(DEFAULT_METADATA), help="Path to real_metadata.csv cache.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Path for the reconstructed candidate CSV.")
    parser.add_argument("--audit-output", default=str(DEFAULT_AUDIT_OUTPUT), help="Path for the reconstructed audit CSV.")
    parser.add_argument("--summary-output", default=str(DEFAULT_SUMMARY_OUTPUT), help="Path for the reconstruction summary markdown.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output files.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    events_path = Path(args.events)
    prices_path = Path(args.prices)
    metadata_path = Path(args.metadata)
    output_path = Path(args.output)
    audit_path = Path(args.audit_output)
    summary_path = Path(args.summary_output)

    for path in [output_path, audit_path, summary_path]:
        if path.exists() and not args.force:
            parser.error(f"Refusing to overwrite existing file without --force: {path}")

    events = pd.read_csv(events_path, dtype={"ticker": str})
    batches = load_cn_reconstruction_batches(events)
    current = _load_current_constituents()
    current_constituents = set(current["ticker"])
    selected_batches, skipped_batches = _resolve_selected_batches(
        current_constituents,
        batches,
        announce_dates=args.announce_dates,
        all_batches=bool(args.all_batches),
    )
    name_map = {
        **current.set_index("ticker")["security_name"].to_dict(),
        **events.loc[events["ticker"].notna(), ["ticker", "security_name"]]
        .assign(ticker=lambda frame: frame["ticker"].astype(str).str.zfill(6))
        .drop_duplicates("ticker", keep="last")
        .set_index("ticker")["security_name"]
        .to_dict(),
    }

    candidate_frames: list[pd.DataFrame] = []
    missing_after_fetch: dict[str, list[str]] = {}
    share_cache: dict[str, float | None] = {}
    for selected_batch in selected_batches:
        target_batch, pre_review, post_review = reconstruct_batch_membership(
            current_constituents,
            batches,
            target_announce_date=selected_batch.announce_date,
        )
        union = sorted(pre_review | post_review)
        cached_caps = load_cached_proxy_market_caps(
            prices_path,
            metadata_path,
            tickers=set(union),
            target_date=target_batch.announce_date,
        )
        covered = set(cached_caps["ticker"]) if not cached_caps.empty else set()
        missing = [ticker for ticker in union if ticker not in covered]
        fetched_caps = _fetch_missing_proxy_market_caps(
            missing,
            target_date=target_batch.announce_date,
            share_cache=share_cache,
        )
        proxy_caps = pd.concat([cached_caps, fetched_caps], ignore_index=True).drop_duplicates(
            subset=["ticker"],
            keep="last",
        )
        candidate_caps = pd.DataFrame(
            [{"ticker": ticker, "security_name": name_map.get(ticker, ticker)} for ticker in union]
        ).merge(proxy_caps, on="ticker", how="left")
        candidate_frame = build_reconstructed_candidate_frame(
            candidate_caps,
            batch=target_batch,
            post_review_membership=post_review,
        )
        validated_batch = validate_candidate_frame(candidate_frame)
        candidate_frames.append(validated_batch)
        missing_rows = sorted(set(union) - set(validated_batch["ticker"]))
        if missing_rows:
            missing_after_fetch[target_batch.announce_date] = missing_rows

    validated = validate_candidate_frame(pd.concat(candidate_frames, ignore_index=True))
    audit = build_candidate_batch_audit(validated)
    audit_summary = summarize_candidate_audit(audit)

    save_dataframe(validated, output_path)
    save_dataframe(audit, audit_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        _build_summary_text(
            selected_batches=selected_batches,
            output_path=output_path,
            audit_path=audit_path,
            audit_summary=audit_summary,
            candidate_count=len(validated),
            missing_after_fetch=missing_after_fetch,
            skipped_batches=skipped_batches,
        ),
        encoding="utf-8",
    )

    if len(selected_batches) == 1:
        print(f"Reconstructed {len(validated)} candidate rows for {selected_batches[0].announce_date}")
    else:
        print(
            f"Reconstructed {len(validated)} candidate rows across {len(selected_batches)} batches "
            f"({selected_batches[0].announce_date} to {selected_batches[-1].announce_date})"
        )
    print(
        "Candidate audit: "
        f"{audit_summary.get('candidate_batches')} batches, "
        f"{audit_summary.get('treated_rows')} included rows, "
        f"{audit_summary.get('control_rows')} control rows, "
        f"{audit_summary.get('crossing_batches')} cutoff-crossing batches"
    )
    if skipped_batches:
        print(f"Skipped earlier batches due to anchor coverage gaps: {', '.join(skipped_batches)}")
    print(f"Saved reconstructed candidates to {output_path}")
    print(f"Saved batch audit to {audit_path}")
    print(f"Saved reconstruction summary to {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
