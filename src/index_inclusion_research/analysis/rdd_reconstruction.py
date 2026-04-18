from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


CSI300_CUTOFF = 300
CSI300_RUNNING_VARIABLE_INTERCEPT = (2 * CSI300_CUTOFF) + 1


@dataclass(frozen=True)
class ReconstructionBatch:
    announce_date: str
    effective_date: str
    batch_id: str
    additions: frozenset[str]
    deletions: frozenset[str]


def load_cn_reconstruction_batches(events: pd.DataFrame) -> list[ReconstructionBatch]:
    cn_events = events.loc[events["market"].astype(str).str.upper() == "CN"].copy()
    if cn_events.empty:
        raise ValueError("No CN events are available for CSI300 reconstruction.")

    required_columns = {
        "announce_date",
        "effective_date",
        "batch_id",
        "ticker",
        "inclusion",
    }
    missing = [column for column in required_columns if column not in cn_events.columns]
    if missing:
        raise ValueError(f"CN events are missing required reconstruction columns: {missing}")

    work = cn_events.copy()
    work["announce_date"] = pd.to_datetime(work["announce_date"]).dt.strftime("%Y-%m-%d")
    work["effective_date"] = pd.to_datetime(work["effective_date"]).dt.strftime("%Y-%m-%d")
    work["ticker"] = work["ticker"].astype(str).str.zfill(6)
    work["inclusion"] = pd.to_numeric(work["inclusion"]).astype(int)

    batches: list[ReconstructionBatch] = []
    group_columns = ["announce_date", "effective_date", "batch_id"]
    for (announce_date, effective_date, batch_id), group in work.groupby(group_columns, sort=True):
        additions = frozenset(group.loc[group["inclusion"] == 1, "ticker"])
        deletions = frozenset(group.loc[group["inclusion"] == 0, "ticker"])
        if not additions and not deletions:
            continue
        batches.append(
            ReconstructionBatch(
                announce_date=str(announce_date),
                effective_date=str(effective_date),
                batch_id=str(batch_id),
                additions=additions,
                deletions=deletions,
            )
        )
    return batches


def reconstruct_batch_membership(
    current_constituents: set[str],
    batches: list[ReconstructionBatch],
    *,
    target_announce_date: str,
    expected_size: int = CSI300_CUTOFF,
) -> tuple[ReconstructionBatch, set[str], set[str]]:
    if not current_constituents:
        raise ValueError("Current constituent set is empty.")

    ordered = sorted(batches, key=lambda item: item.announce_date)
    target = next((batch for batch in ordered if batch.announce_date == target_announce_date), None)
    if target is None:
        available = ", ".join(batch.announce_date for batch in ordered)
        raise ValueError(f"Target announce date {target_announce_date} was not found. Available batches: {available}")

    post_review = {str(ticker).zfill(6) for ticker in current_constituents}
    for batch in sorted([item for item in ordered if item.announce_date > target_announce_date], key=lambda item: item.announce_date, reverse=True):
        post_review.difference_update(batch.additions)
        post_review.update(batch.deletions)

    pre_review = set(post_review)
    pre_review.difference_update(target.additions)
    pre_review.update(target.deletions)

    if len(post_review) != expected_size:
        raise ValueError(f"Reconstructed post-review membership for {target_announce_date} has {len(post_review)} names, expected {expected_size}.")
    if len(pre_review) != expected_size:
        raise ValueError(f"Reconstructed pre-review membership for {target_announce_date} has {len(pre_review)} names, expected {expected_size}.")

    return target, pre_review, post_review


def build_reconstructed_candidate_frame(
    candidate_caps: pd.DataFrame,
    *,
    batch: ReconstructionBatch,
    post_review_membership: set[str],
    cutoff: int = CSI300_CUTOFF,
) -> pd.DataFrame:
    required_columns = {"ticker", "security_name", "proxy_market_cap"}
    missing = [column for column in required_columns if column not in candidate_caps.columns]
    if missing:
        raise ValueError(f"Candidate market-cap frame is missing required columns: {missing}")

    work = candidate_caps.copy()
    work["ticker"] = work["ticker"].astype(str).str.zfill(6)
    work["proxy_market_cap"] = pd.to_numeric(work["proxy_market_cap"], errors="coerce")
    work = work.dropna(subset=["proxy_market_cap"]).sort_values(["proxy_market_cap", "ticker"], ascending=[False, True]).reset_index(drop=True)
    if work["ticker"].duplicated().any():
        duplicates = sorted(work.loc[work["ticker"].duplicated(), "ticker"].unique())
        raise ValueError(f"Candidate market-cap frame contains duplicate tickers: {duplicates}")
    if len(work) < cutoff:
        raise ValueError(f"Candidate market-cap frame only has {len(work)} ranked names, fewer than cutoff {cutoff}.")

    work["descending_rank"] = range(1, len(work) + 1)
    running_variable_intercept = (2 * cutoff) + 1
    work["running_variable"] = running_variable_intercept - work["descending_rank"]
    work["cutoff"] = cutoff
    work["inclusion"] = work["ticker"].isin(post_review_membership).astype(int)
    work["market"] = "CN"
    work["index_name"] = "CSI300"
    work["batch_id"] = batch.announce_date
    work["announce_date"] = batch.announce_date
    work["effective_date"] = batch.effective_date
    work["event_type"] = work["inclusion"].map({1: "reconstructed_post_member", 0: "reconstructed_pre_only_member"})
    work["source"] = "CSI300 constituent-union public reconstruction"
    work["source_url"] = "https://www.csindex.com.cn/zh-CN/indices/index-detail/000300"
    work["note"] = (
        "Reconstructed from current CSI300 constituents, known later review reversals, and public market-cap proxies; "
        "not an official CSIndex reserve list."
    )
    ordered_columns = [
        "batch_id",
        "market",
        "index_name",
        "ticker",
        "security_name",
        "announce_date",
        "effective_date",
        "running_variable",
        "cutoff",
        "inclusion",
        "event_type",
        "source",
        "source_url",
        "note",
        "proxy_market_cap",
        "descending_rank",
    ]
    return work.loc[:, ordered_columns].copy()


def load_cached_proxy_market_caps(
    prices_path: Path,
    metadata_path: Path,
    *,
    tickers: set[str],
    target_date: str,
) -> pd.DataFrame:
    prices = pd.read_csv(prices_path, dtype={"ticker": str}, usecols=["market", "ticker", "date", "close", "mkt_cap"])
    prices["date"] = pd.to_datetime(prices["date"])
    prices["ticker"] = prices["ticker"].astype(str).str.zfill(6)
    day_prices = prices.loc[
        (prices["market"].astype(str).str.upper() == "CN")
        & (prices["ticker"].isin(tickers))
        & (prices["date"] <= pd.Timestamp(target_date))
        ,
        ["ticker", "date", "close", "mkt_cap"],
    ].copy()
    if day_prices.empty:
        return pd.DataFrame(columns=["ticker", "proxy_market_cap"])
    day_prices = (
        day_prices.sort_values(["ticker", "date"])
        .drop_duplicates(subset=["ticker"], keep="last")
        .copy()
    )

    metadata = pd.read_csv(metadata_path, dtype={"ticker": str}, usecols=["ticker", "shares_outstanding"])
    metadata["ticker"] = metadata["ticker"].astype(str).str.zfill(6)
    merged = day_prices.merge(metadata, on="ticker", how="left")
    merged["mkt_cap"] = pd.to_numeric(merged["mkt_cap"], errors="coerce")
    merged["close"] = pd.to_numeric(merged["close"], errors="coerce")
    merged["shares_outstanding"] = pd.to_numeric(merged["shares_outstanding"], errors="coerce")
    merged["proxy_market_cap"] = merged["mkt_cap"].where(
        merged["mkt_cap"].notna(),
        merged["close"] * merged["shares_outstanding"],
    )
    return merged.loc[:, ["ticker", "proxy_market_cap"]].dropna().copy()
