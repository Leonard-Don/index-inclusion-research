from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .panel import map_to_trading_date


def _compute_security_snapshot(
    prices: pd.DataFrame,
    market: str,
    ticker: str,
    reference_date: pd.Timestamp,
    lookback_days: int,
) -> dict[str, object] | None:
    history = prices.loc[(prices["market"] == market) & (prices["ticker"] == ticker)].sort_values("date")
    if history.empty:
        return None
    mapped_reference = map_to_trading_date(reference_date, history["date"].tolist())
    if pd.isna(mapped_reference):
        return None

    history = history.loc[history["date"] <= mapped_reference].copy()
    if history.empty:
        return None
    window = history.tail(lookback_days + 1).copy()
    if window.empty:
        return None
    latest = window.iloc[-1]
    pre_window = window.iloc[:-1]
    pre_return = (1.0 + pre_window["ret"].fillna(0.0)).prod() - 1.0 if not pre_window.empty else 0.0
    pre_volatility = pre_window["ret"].std(ddof=0) if len(pre_window) > 1 else 0.0
    return {
        "market": market,
        "ticker": ticker,
        "reference_date": mapped_reference,
        "mkt_cap": latest.get("mkt_cap", np.nan),
        "sector": latest.get("sector", pd.NA),
        "pre_event_return": pre_return,
        "pre_event_volatility": pre_volatility,
    }


def _distance_score(target: dict[str, object], candidate: dict[str, object]) -> float:
    target_cap = target.get("mkt_cap")
    candidate_cap = candidate.get("mkt_cap")
    if pd.isna(target_cap) or pd.isna(candidate_cap) or target_cap <= 0 or candidate_cap <= 0:
        return math.inf
    size_distance = abs(np.log(target_cap) - np.log(candidate_cap))
    return_distance = abs(float(target["pre_event_return"]) - float(candidate["pre_event_return"]))
    vol_distance = abs(float(target["pre_event_volatility"]) - float(candidate["pre_event_volatility"]))
    sector_distance = 0.0 if target.get("sector") == candidate.get("sector") else 0.25
    return float(size_distance + return_distance + 0.5 * vol_distance + sector_distance)


def build_matched_sample(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    lookback_days: int = 20,
    num_controls: int = 3,
    reference_date_column: str = "announce_date",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    treated = events.copy()
    if "treatment_group" not in treated.columns:
        treated["treatment_group"] = 1
    treated["treatment_group"] = treated["treatment_group"].fillna(1).astype(int)
    treated = treated.loc[treated["treatment_group"] == 1].copy()
    treated_tickers = set(treated["ticker"].astype(str))
    history_by_key = {
        (market, ticker): group.sort_values("date").reset_index(drop=True)
        for (market, ticker), group in prices.groupby(["market", "ticker"], dropna=False)
    }
    tickers_by_market = {
        market: sorted(group["ticker"].astype(str).unique().tolist())
        for market, group in prices.groupby("market")
    }
    snapshot_cache: dict[tuple[str, str, str, int], dict[str, object] | None] = {}

    def get_snapshot(market: str, ticker: str, reference_date: pd.Timestamp) -> dict[str, object] | None:
        cache_key = (market, str(ticker), pd.Timestamp(reference_date).date().isoformat(), lookback_days)
        if cache_key in snapshot_cache:
            return snapshot_cache[cache_key]
        history = history_by_key.get((market, ticker))
        if history is None:
            snapshot_cache[cache_key] = None
            return None
        mapped_snapshot = _compute_security_snapshot(
            prices=history,
            market=market,
            ticker=ticker,
            reference_date=reference_date,
            lookback_days=lookback_days,
        )
        snapshot_cache[cache_key] = mapped_snapshot
        return mapped_snapshot

    matched_rows: list[dict[str, object]] = []
    diagnostics: list[dict[str, object]] = []
    for event in treated.itertuples(index=False):
        reference_date = getattr(event, reference_date_column)
        target_snapshot = get_snapshot(
            market=event.market,
            ticker=event.ticker,
            reference_date=reference_date,
        )
        if target_snapshot is None:
            diagnostics.append(
                {
                    "event_id": event.event_id,
                    "status": "skipped_missing_target_snapshot",
                    "selected_controls": 0,
                }
            )
            continue

        candidates: list[dict[str, object]] = []
        for candidate_ticker in tickers_by_market.get(event.market, []):
            if candidate_ticker == event.ticker or str(candidate_ticker) in treated_tickers:
                continue
            candidate_snapshot = get_snapshot(
                market=event.market,
                ticker=candidate_ticker,
                reference_date=reference_date,
            )
            if candidate_snapshot is None:
                continue
            candidates.append(candidate_snapshot)

        if not candidates:
            diagnostics.append(
                {
                    "event_id": event.event_id,
                    "status": "skipped_no_candidates",
                    "selected_controls": 0,
                }
            )
            continue

        candidate_frame = pd.DataFrame(candidates)
        if pd.notna(target_snapshot.get("sector")) and "sector" in candidate_frame:
            sector_candidates = candidate_frame.loc[candidate_frame["sector"] == target_snapshot["sector"]].copy()
        else:
            sector_candidates = candidate_frame.iloc[0:0].copy()
        relaxed_sector = sector_candidates.empty
        if relaxed_sector:
            sector_candidates = candidate_frame.copy()

        target = target_snapshot
        sector_candidates["distance"] = sector_candidates.apply(
            lambda row, target=target: _distance_score(target, row.to_dict()),
            axis=1,
        )
        sector_candidates = sector_candidates.replace([np.inf, -np.inf], np.nan).dropna(subset=["distance"])
        selected = sector_candidates.nsmallest(num_controls, "distance")

        diagnostics.append(
            {
                "event_id": event.event_id,
                "status": "matched" if not selected.empty else "skipped_no_valid_match",
                "selected_controls": int(len(selected)),
                "sector_relaxed": relaxed_sector,
            }
        )
        for rank, candidate in enumerate(selected.itertuples(index=False), start=1):
            matched_rows.append(
                {
                    **event._asdict(),
                    "event_id": f"{event.event_id}-ctrl-{rank:02d}",
                    "ticker": candidate.ticker,
                    "treatment_group": 0,
                    "matched_to_event_id": event.event_id,
                    "note": f"Matched control {rank} for {event.event_id}",
                }
            )

    treated_rows = treated.copy()
    treated_rows["matched_to_event_id"] = treated_rows["event_id"]
    matched_events = pd.concat([treated_rows, pd.DataFrame(matched_rows)], ignore_index=True, sort=False)
    if matched_events.empty:
        matched_events = treated_rows
    matched_events = matched_events.sort_values(
        ["market", "matched_to_event_id", "treatment_group", "inclusion"],
        ascending=[True, True, False, False],
    )
    return matched_events.reset_index(drop=True), pd.DataFrame(diagnostics)
